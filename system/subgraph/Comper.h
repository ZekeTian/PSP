//########################################################################
//## Copyright 2018 Da Yan http://www.cs.uab.edu/yanda
//##
//## Licensed under the Apache License, Version 2.0 (the "License");
//## you may not use this file except in compliance with the License.
//## You may obtain a copy of the License at
//##
//## //http://www.apache.org/licenses/LICENSE-2.0
//##
//## Unless required by applicable law or agreed to in writing, software
//## distributed under the License is distributed on an "AS IS" BASIS,
//## WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//## See the License for the specific language governing permissions and
//## limitations under the License.
//########################################################################

/**
 *  线程封装类
 */

#ifndef COMPER_H_
#define COMPER_H_

#include "util/global.h"
#include "util/ioser.h"
#include "util/lshmap.h"
#include "util/MinHash.h"
#include "util/conque_p.h"
#include <deque> //for task queue
#include <unistd.h> //for usleep()
#include "TaskMap.h"
#include "adjCache.h"
#include "string.h"
#include <thread>
#include "Aggregator.h"
#include <fstream>
#include <unordered_set>
#include "util/logger.h"
#include "util/MemoryUtil.h"

using namespace std;

template <class TaskT, class AggregatorT = DummyAgg>
class Comper {
public:
    typedef Comper<TaskT, AggregatorT> ComperT;

	typedef TaskT TaskType;
    typedef AggregatorT AggregatorType;

    typedef typename AggregatorT::FinalType FinalT;

	typedef typename TaskT::VertexType VertexT;
	typedef typename TaskT::SubgraphT SubgraphT;
	typedef typename TaskT::ContextType ContextT;

	typedef typename VertexT::KeyType KeyT;
	typedef typename VertexT::ValueType ValueT;
	typedef typename VertexT::HashType HashT;

    typedef vector<VertexT*> VertexVec;

    typedef deque<TaskT *> TaskQueue;
    typedef TaskMap<TaskT> TaskMapT; // 任务 Map 类型
    typedef hash_map<KeyT, VertexT*> VTable;
    typedef typename VTable::iterator TableIter;

    /**
     * 任务 id 的哈希函数，任务 id 组成形式：<查询组 id, 顶点 id>
     */
    struct task_hash
    {
        // template <class T1, class T2>
        std::size_t operator()(const std::pair<KeyT, KeyT> &p) const
        {
            std::hash<KeyT> hasher1;
            size_t seed = hasher1(p.first);

            std::hash<KeyT> hasher2;
            seed ^= hasher2(p.second) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
            return seed;
        }
    };

    /**
     * 当前线程的任务队列
     * 该队列是一个双端队列，队头、队尾可能进行的操作如下：
     * 队头：从队头取任务执行；从磁盘文件中读取任务，然后从队头压入
     * 队尾：从就绪任务队列中取出任务，然后从队尾压入；从队尾取出任务，然后写到磁盘文件中
     */
    TaskQueue q_task; //written only by the thread itself, no need to be conque 当前线程的任务队列
    
    TaskQueue q_delay_task; // 存储延迟执行的任务，即公共部分匹配结果较多的任务

    MinHash minhash;
    // lshmap<KeyT, Label, TaskT *> generated_task_map; // 第二级哈希按照查询图的根顶点标签进行，可以保证根顶点标签相同的查询图能够合并
    lshmap<pair<KeyT, KeyT>, KeyT, TaskT *, task_hash> generated_task_map; // 第一级哈希按照任务 id 进行；第二级哈希按照查询组 id 进行，保证只进行查询组内部的任务合并

    hash_map<pair<KeyT, KeyT>, int, task_hash> comm_match_reference_count; // 公共匹配结果引用计数，Key：公共匹配结果 id（<查询组 id, 任务种子顶点 id>），Value：使用了该公共匹配结果的任务数量

    /**
     * 当前线程的 id
     */
    int thread_rank; // 线程 id

    /**
     * 存放任务的 map
     */
    TaskMapT map_task;
    
    
    thread_counter counter;

    thread main_thread;

    /**
     * 用户自定义函数，用于生成任务。在实现该函数时，内部要调用 add_task()，从而将生成的任务加入到任务队列 q_task 中
     */
    //UDF1
    virtual void task_spawn(VertexT * v) = 0; //call add_task() inside, will flush tasks to disk if queue overflows 在内部要调用 add_task()，从而将生成的任务加入到任务队列 q_task 中
    
    /**
     * 执行任务
     * @param g         当前任务在数据图上能关联到的子图
     * @param context   执行任务时，额外的信息
     * @param frontier  执行任务需要的顶点
     */
    //UDF2
    virtual bool compute(SubgraphT & g, ContextT & context, vector<VertexT *> & frontier) = 0;

    //task's pull wrappper (to serve UDF2 wrapper)
    TaskT* cur_task;
    void pull(KeyT id){
    	cur_task->to_pull.push_back(id);
	}

    /**
     * 清空原来拉取的顶点，从而便于下次顶点的拉取
     */
    void clear_pull()
    {
        cur_task->to_pull.clear();
    }

    /**
     * 如果任务未结束，则返回 true，表示在下一轮迭代中仍然需要进行计算；否则，返回 false（即任务在此轮迭代中结束）。
     * 该函数会在 comper 的 push_task_from_taskmap 和 pop_task 中调用
     *
     * 返回 false 的场景：
     *    （1）当前任务的子图不满足条件，没有必要再继续执行。（此时，该任务提前结束）
     *    （2）当前任务的子图已达到最终条件，不需要再继续执行。（此时，该任务正常结束）
     */
    //UDF2 wrapper
    bool compute(TaskT* task)
    {
        if (1 == task->context)
        {
            global_load_num += task->subG.roots.size();
        }

    	cur_task = task; //so that in UDF2, one can directly call pull(key), no need to get the task object
    	// task->to_pull.clear(); //clear it for pulling new vertices 改成：在存在新拉取顶点之前，清除原来的数据
    	return compute(task->subG, task->context, task->frontier_vertexes);
    }

    ofstream fout;

    /**
     * Comper 线程启动
     */
    void start(int thread_id)
    {
    	thread_rank= map_task.thread_rank = thread_id;
    	//------
        // 生成一个文件，文件名格式为：目录/worker的id_线程id
		char file[1000], no[40];
		long long fileSeqNo = 1;
		strcpy(file, REPORT_DIR.c_str());
		sprintf(no, "/%d_%d", _my_rank, thread_rank);
		strcat(file, no);
		fout.open(file);
		//------
    	main_thread = thread(&ComperT::run, this);
    }

    Comper()
    {

    }

    virtual ~Comper()
    {
    	main_thread.join();
    	fout.close();
    }

    /**
     * 该函数有两个功能：
     * （1）如果磁盘文件不为空，从磁盘文件中加载任务，并将任务加入到任务队列中。
     * （2）判断磁盘文件是否为空，如果磁盘文件为空，则返回 false；否则，返回 true
     */
    //load tasks from a file (from "global_file_list" to the task queue)
    //returns false if "global_file_list" is empty
    bool file2queue()
    {
    	string file;
    	bool succ = global_file_list.dequeue(file);
    	if(!succ) return false; //"global_file_list" is empty
    	else
    	{
    		global_file_num --;
    		ofbinstream in(file.c_str());
    		while(!in.eof())
    		{
    			TaskT* task;
    			in >> task;
    			add_task(task); // 从文件中反序列化出 task 对象后，将其加入到任务队列中
    		}
    		in.close();

            if (remove(file.c_str()) != 0) {
                cout<<"Error removing file: "<<file<<endl;
                perror("Error printed by perror");
            }
    		return true;
    	}
    }

    /**
     * 根据本地顶点列表中的顶点生成任务，并将生成的任务放进任务队列 q_task 。
     * 本地顶点列表能再生成新的任务，则返回 true；否则，返回 false。
     */
    //load tasks from local-table
	//returns false if local-table is exhausted
    bool locTable2queue() // 生成任务成功，则返回 true；否则，返回 false
	{
		size_t begin, end; //[begin, end) are the assigned vertices (their positions in local-table)
		//note that "end" is exclusive
		VTable & ltable = *(VTable *)global_local_table;
		int size = ltable.size(); // 本地顶点列表的大小
		//======== critical section on "global_vertex_pos"
        // 防止多个线程同时对全局本地顶点列表（global_local_table）进行访问，因此先加锁进行同步
		global_vertex_pos_lock.lock();
		if(global_vertex_pos < size) // 还能继续从本地顶点列表中生成任务
		{
			begin = global_vertex_pos; //starting element
			end = begin + TASK_BATCH_NUM; // 一次取出一个批次的任务
			if(end > size) end = size;
			global_vertex_pos = end; //next position to spawn
		}
		else begin = -1; //meaning that local-table is exhausted 本地顶点列表中不能再生成新的任务
		global_vertex_pos_lock.unlock();
		//======== spawn tasks from local-table[begin, end)
        // 从本地顶点列表的 [begin, end) 区间内取出顶点生成任务
		if(begin == -1) return false; // 本地顶点列表中不能再生成新的任务，则返回 false
		else
		{
            VertexVec & gb_vertexes = *(VertexVec*) global_vertexes; // 取出当前 worker 的本地顶点列表
			for(int i=begin; i<end; i++)
			{//call UDF to spawn tasks
				task_spawn(gb_vertexes[i]); // 逐个顶点生成任务，会调用 add_task() ，将生成的任务放进任务队列 q_task 
			}
			return true; // 能生成新的任务，则返回 true
		}
	}

    AggregatorT* get_aggregator() //get aggregator
    //cannot use the same name as in global.h (will be understood as the local one, recursive definition)
    {
    	return (AggregatorT*)global_aggregator;
    }


    /**
     * 弹出一个任务并处理它，如果有必要则会加入 task_map 中（如果需要拉取远程顶点，则会将该任务加入到挂起任务 map 中，即 task-map）。
     * 如果任务队列 q_task 为空，并且本地顶点列表中不需要处理顶点（不能生成新任务），则返回 false（即没有任务可以执行）； 有任务可以执行，
     * 
     * 该函数可以用于删除无法继续执行的任务，从而可以使得任务队列、任务 map 为空，让 Comper 空闲，最后使得 worker 空闲，结束整个计算过程。
     * 任务是否可以继续执行，是由开发人员在 Comper 的 Compute 函数里面定义。Compute 如果返回 true，继续执行任务；返回 false，不再继续执行任务。
     * 返回 false 的场景：
     *    （1）当前任务的子图不满足条件，没有必要再继续执行。（此时，该任务提前结束）
     *    （2）当前任务的子图已达到最终条件，不需要再继续执行。（此时，该任务正常结束）
     *
     *
     * 该函数的具体过程如下：
     * 如果任务队列中任务数量较少，则：
     *    先从磁盘文件中取出任务，放进任务队列中
     *    如果磁盘文件为空，则从就绪任务队列中不断取任务执行，直到就绪任务队列为空或者任务队列任务数量达到阈值
     *       （因为在执行就绪任务队列中的任务时，部分任务可能需要在下轮迭代中继续执行，而这些任务会被放进任务队列中，因此任务队列中的任务数量会增加）
     *    如果就绪任务队列为空，则本地顶点列表生成任务
     * 如果任务队列为空，且无法通过磁盘文件、本地顶点列表生成任务，则返回 false (即没有任务可以执行)
     * 从任务队列中弹出一个任务 t。在 while 循环中，不断拉取任务 t 需要的顶点、执行任务 t ，直到任务 t 结束或者任务 t 被挂起（请求远程顶点）为止
     * 函数结束，返回 true 
     */
    //part 2's logic: get a task, process it, and add to task-map if necessary
    //- returns false if (a) q_task is empty and
    // *** (b) locTable2queue() did not process a vertex (may process vertices but tasks are pruned)
    //condition to call: 调用的两种情况
    //1. map_task has space 
    //2. vcache has space
    bool pop_task()
    {
    	bool task_spawn_called = false;
    	bool push_called = false;
    	//fill the queue when there is space
    	// if(q_task.size() <= TASK_BATCH_NUM) // 任务队列中的任务数量较少，需要向任务队列中添加任务，从而保持任务队列中有足够的任务，保证线程尽可能地处于工作状态
    	if(global_mem_tasknum_vec[thread_rank] <= TASK_BATCH_NUM) // 任务队列中的任务数量较少，需要向任务队列中添加任务，从而保持任务队列中有足够的任务，保证线程尽可能地处于工作状态
    	{
            // 添加任务的优先级：磁盘文件 > 就绪任务队列中 > 本地顶点列表
            // 先从磁盘文件加载任务，如果磁盘文件为空，则从 task-map 中生成任务
    		if(!file2queue()) //priority <1>: fill from file on local disk 如果能从磁盘文件中加载任务，返回 true；否则，返回 false
    		{//"global_file_list" is empty 磁盘文件为空
    			if(!push_task_from_taskmap()) //priority <2>: fetch a task from task-map
    			{//CASE 1: task-map's "task_buf" is empty // 就绪任务队列为空，则从本地顶点列表中生成新的任务
    				task_spawn_called = locTable2queue(); // 从本地顶点列表中生成任务，生成的任务会加入到 q_task 中，如果能生成任务，则返回 true
    			}
    			else
    			{//CASE 2: try to move TASK_BATCH_NUM tasks from task-map to the queue
                    // 就绪任务队列不为空，则从 task_map 中取出 TASK_BATCH_NUM 个任务，放进当前的任务队列 q_task 中
                    // 上面 if(!push_task_from_taskmap()) 中的 push_task_from_taskmap() 函数中执行了 compute 计算任务
    				push_called = true; // 从就绪队列中取出任务并压入到队列 q_task 中
    				while(q_task.size() < 2 * TASK_BATCH_NUM)
    				{//i starts from 1 since push_task_from_taskmap() has been called once 
    					if(!push_task_from_taskmap()) break; //task-map's "task_buf" is empty, no more try 就绪队列为空，则不再从中取任务
    				}
    			}
    		} // 否则，磁盘文件不为空，已经通过 if(!file2queue()) 中的 file2queue() 将磁盘文件中的任务添加到任务队列中
    	}
    	//==================================
    	if(q_delay_task.size() == 0 && q_task.size() == 0){
			if(task_spawn_called) return true;
			else if(push_called) return true;
			else return false; // 磁盘文件为空、本地顶点列表中没有顶点需要继续处理（不能生成新任务），且任务队列为空，则返回 false（实际就是没有任务可以继续执行）
		}
    	//fetch task from Comper's task queue head 从任务队列中取出任务
    	TaskT * task = NULL;
    	if (q_delay_task.size() > 0) // 优先从 q_delay_task 队列中取任务执行
        {
            task = q_delay_task.front(); // 尝试取队首任务执行
            task->retry_count++;
            
            if (task->comm_match_memory_size < (get_free_memory_byte() / num_compers) /** 内存足够，直接执行 */
                || task->retry_count >= MAX_RETRY_NUM /** 尝试 MAX_RETRY_NUM 次后，则该任务不再等待，直接执行 */
                || q_task.size() == 0 /** 普通任务队列为空，则从延迟任务队列中取任务 */)
            {
                q_delay_task.pop_front(); // 取出队首任务，执行
            }    
            else
                task = NULL; // q_delay_task 中的队首任务暂时不能执行
        }

        if (NULL == task) // 如果 q_delay_task 不能取出任务执行，则再从 q_task 队列中取任务
        {
            task = q_task.front();
            q_task.pop_front();
        }

    	//task.to_pull should've been set
    	//[*] process "to_pull" to get "frontier_vertexes" and return "how many" vertices to pull
    	//if "how many" = 0, call task.compute(.) to set task.to_pull (and unlock old "to_pull") and go to [*]
    	bool go = true; //whether to continue another round of task.compute() 标记任务是否需要另外一轮计算
    	//init-ed to be true since:  初始为 true 有如下两个原因：
    	//1. if it is newly spawned, should allow it to run 如果该任务是刚生成的，则应该允许其运行
    	//2. if compute(.) returns false, should be filtered already, won't be popped
        // 循环中不断拉取任务需要的顶点、执行 task 任务，直到任务结束或者任务被挂起（请求远程顶点）为止
    	
        // generated_task_set.erase(task); // task 即将拉取顶点，则不能再继续合并，应当从 generated_task_set 中删除
        // lshmap_bucket<Label, TaskT *> &lsh_bucket = generated_task_map.get_bucket_by_key(task->subG.vertexes[0].id);
        // lsh_bucket.erase(task->subG.vertexes[0].value.l);

        pair<KeyT, KeyT> key = make_pair(task->group_id, task->subG.vertexes[0].id);
        if (generated_task_map.contains(key))
        {
            lshmap_bucket<KeyT, TaskT *> &lsh_bucket = generated_task_map.get_bucket_by_key(key);
            lsh_bucket.erase(task->group_id);
            generated_task_map.erase_index(key);
        }

        while(task->pull_all(counter, map_task)) //may call add2map(.) task 拉取顶点，如果该任务需要的顶点已经全部拉取到本地，则返回 true，可以继续下一轮迭代；否则，拉取远程顶点，返回 false
    	{
            size_t prev_size = q_delay_task.size();
    		go = compute(task);
            if (q_delay_task.size() == prev_size) // 任务没有延迟，则正常执行，解锁缓存的远程顶点
    		{
                task->unlock_all();
            }
            else
            {
                break; // 任务延迟执行，退出循环
            }

    		if(go == false) // 任务在当前迭代中结束
    		{
    			global_tasknum_vec[thread_rank]++;
                global_mem_tasknum_vec[thread_rank] = q_task.size() + map_task.size; // 记录当前线程的任务数量
                global_load_num -= task->subG.roots.size();

                // 公共匹配结果引用计数减 1
                if (NULL != task->subG.comm_match)
                {
                    comm_match_reference_count[key]--;
                    if (0 == comm_match_reference_count[key])
                    {
                        comm_match_reference_count.erase(key);
                        delete task->subG.comm_match; // 该任务的公共匹配结果 comm_match 不再需要被其它任务使用，则将从其内存中释放掉
                    }    
                }

				delete task;
    			break; // 任务结束，退出循环
    		}
    	}
    	//now task is waiting for resps (task_map => task_buf => push_task_from_taskmap()), or finished
		return true; // （1）任务在拉取远程顶点，等待响应结果。（2）任务已经结束
    }

    //=== for handling task streaming on disk ===
    char fname[1000], num[20];
    long long fileSeqNo = 1;
    void set_fname() //will proceed file seq #
    {
    	strcpy(fname, TASK_DISK_BUFFER_DIR.c_str());
    	sprintf(num, "/%d_", _my_rank);
    	strcat(fname, num);
    	sprintf(num, "%d_", thread_rank);
    	strcat(fname, num);
    	sprintf(num, "%lld", fileSeqNo);
    	strcat(fname, num);
    	fileSeqNo++;
    }

    /**
     * 将 task 放进当前线程的任务队列中，如果线程队列任务达到上限，则将该任务存储到磁盘文件中
     */
    //tasks are added to q_task only through this function !!!
    //it flushes tasks as a file to disk when q_task's size goes beyond 3 * TASK_BATCH_NUM
    void add_task(TaskT * task)
    {
    	if(q_task.size() == 3 * TASK_BATCH_NUM && NULL == task->subG.comm_match) // 任务比较多，且未寻找公共部分结果
    	{
            // 当队列中的任务数量等于 3 * TASK_BATCH_NUM 时，则从队列中取出 TASK_BATCH_NUM 个任务保存到文件中
    		set_fname();
    		ifbinstream out(fname);
    		//------
    		while(q_task.size() > 2 * TASK_BATCH_NUM)
    		{
    			//get task at the tail
    			TaskT * t = q_task.back();
    			q_task.pop_back();
                
                // generated_task_set.erase(t); // 对于即将保存到文件中的 task，应当从 generated_task_set 中删除，从而避免后面继续与其它 task 合并
                // lshmap_bucket<Label, TaskT *> &lsh_bucket = generated_task_map.get_bucket_by_key(t->subG.vertexes[0].id);
                // lsh_bucket.erase(t->subG.vertexes[0].value.l);

                pair<KeyT, KeyT> key = make_pair(t->group_id, t->subG.vertexes[0].id);
                if (generated_task_map.contains(key))
                {
                    lshmap_bucket<KeyT, TaskT *> &lsh_bucket = generated_task_map.get_bucket_by_key(key);
                    lsh_bucket.erase(t->group_id);
                    generated_task_map.erase_index(key);
                }

                global_load_num += t->subG.roots.size();

    			//stream to file
    			out << t;
    			//release from memory
    			delete t;
    		}
    		out.close();
    		//------
    		//register with "global_file_list"
    		global_file_list.enqueue(fname);
    		global_file_num ++;
    	}
    	//--- deprecated:
    	//task->comper = this;//important !!! set task.comper before entering processing
    	//---------------
    	q_task.push_back(task);

        global_mem_tasknum_vec[thread_rank] = q_task.size() + map_task.size; // 记录当前线程的任务数量
    }

    /**
     * 将 task 放进 q_delay_task 队列中，延迟该任务的执行，
     */
    void delay_task(TaskT * task)
    {
        q_delay_task.push_back(task);
    }

    /**
     * 从就绪任务队列中取出任务，并且执行。如果任务需要在下一轮迭代中继续执行，则压入到当前线程的任务队列 q_task 中。
     * 如果能取出任务，则返回 true；否则，返回 false
     */
    //part 1's logic: fetch a task from task-map's "task_buf", process it, and add to q_task (flush to disk if necessary)
    bool push_task_from_taskmap() //returns whether a task is fetched from taskmap
    {
    	TaskT * task = map_task.get(); // 实际从 task_buf 中取出任务
    	if(task == NULL) return false; //no task to fetch from q_task
    	task->set_pulled(); //reset task's frontier_vertexes (to replace NULL entries)
        
        size_t prev_size = q_delay_task.size();
    	bool go = compute(task); //set new "to_pull" 执行该任务的计算
    	if (q_delay_task.size() == prev_size) // 任务没有延迟，则正常执行，解锁缓存的远程顶点
        {
            task->unlock_all();
        }

    	if(go != false)
        {
            if (q_delay_task.size() == prev_size) // 任务没有延迟，则再添加到 q_task 任务队列中
                add_task(task); //add task to queue 任务需要在下一轮迭代中继续执行，因此仍然需要将任务加入到队列中
        }
        else
        {
        	global_tasknum_vec[thread_rank]++; // 任务结束，相应 comper 的任务数量加 1
            global_mem_tasknum_vec[thread_rank] = q_task.size() + map_task.size; // 记录当前线程的任务数量
            global_load_num -= task->subG.roots.size();

            // 公共匹配结果引用计数减 1
            if (NULL != task->subG.comm_match)
            {
                pair<KeyT, KeyT> key = make_pair(task->group_id, task->subG.vertexes[0].id);
                comm_match_reference_count[key]--;
                if (0 == comm_match_reference_count[key])
                {
                    comm_match_reference_count.erase(key);
                    delete task->subG.comm_match; // 该任务的公共匹配结果 comm_match 不再需要被其它任务使用，则将从其内存中释放掉
                }
            }

        	delete task;
        }
		return true;
    }

    /**
     * Work 内部进行任务窃取
     */
    void inner_work_steal(conque_p<TaskT>* work_steal_queue)
    {
        if (0 == q_task.size() && !work_steal_queue->empty())
        {   // 负载较轻的线程从 work_steal_queue 中取任务执行
            while (q_task.size() < 10 * TASK_GET_NUM)
            {
                TaskT* t = work_steal_queue->dequeue();
                if (NULL != t)
                    add_task(t);
                else
                    break;
                
                // cout << "inner work " << _my_rank << " steal, dst thread " << thread_rank << " task num: " << q_task.size() << endl;
            }
        }
        else if (q_task.size() > 2 * num_compers * TASK_GET_NUM)
        {   // 负载较重的线程将任务转移到 work_steal_queue，以便其它线程执行
            // int remain_num = q_task.size() / num_compers;
            int remain_num = num_compers * TASK_GET_NUM;
            while (q_task.size() > remain_num)
            {
                TaskT * t = q_task.back();
                q_task.pop_back();
                work_steal_queue->enqueue(t);
                
                pair<KeyT, KeyT> key = make_pair(t->group_id, t->subG.vertexes[0].id);
                if (generated_task_map.contains(key))
                {
                    lshmap_bucket<KeyT, TaskT *> &lsh_bucket = generated_task_map.get_bucket_by_key(key);
                    lsh_bucket.erase(t->group_id);
                    generated_task_map.erase_index(key);
                }
                // cout << "inner work " << _my_rank << " steal, src thread " << thread_rank << " task num: " << q_task.size() << endl;
            }
            global_mem_tasknum_vec[thread_rank] = q_task.size() + map_task.size; // 记录当前线程的任务数量
        }
    }

    //=========================

    //combining part 1 and part 2
    void run()
	{
        // conque_p<TaskT>* task_queue_ptr = (conque_p<TaskT>*)global_task_queue; // Worker 内部全局任务队列，用于存储工作窃取的任务
    	while(global_end_label == false) //otherwise, thread terminates
		{
            // if (global_inner_work_steal_label)
            // {
            //     inner_work_steal(task_queue_ptr);
            // }

            // 任务没有结束，则继续处理
    		bool nothing_processed_by_pop; //called pop_task(), but cannot get a task to process, and not called a task_spawn(v)
    		bool blocked; //blocked from calling pop_task()
    		bool nothing_to_push; //nothing to fetch from taskmap's buf (but taskmap's map may not be empty) 标记就绪任务队列是否为空，如果为空，则为 true；否则，为 false（注意，挂起任务 map 可能不为空）
			for(int i=0; i<TASK_GET_NUM; i++) // TASK_GET_NUM 表示一次取出的任务数量，默认为 1
			{
				nothing_processed_by_pop = false; // 标记是否有任务可以继续处理，如果没有可以继续处理的任务，则标记为 true；如果有任务需要继续处理，则标记为 false
				blocked = false;
				//check whether we can continue to pop a task (may add things to vcache) 检查缓存表大小
				if(global_cache_size < VCACHE_LIMIT + VCACHE_OVERSIZE_LIMIT) //(1 + alpha) * vcache_limit
				{
                    // 判断线程的任务 map 是否还能继续放任务（在 pop_task() 内部，可能会调用 add2map ，即会向挂起任务 map 中加入任务，因此调用前先检查大小）
					if(map_task.size < TASKMAP_LIMIT)
					{
                        // 线程的任务 map 可以继续放任务，则调用 pop_task 处理任务
						if(!pop_task()) nothing_processed_by_pop = true; //only the last iteration is useful, others will be set back to false 任务队列 q_task 为空，不能生成新任务，没有可以继续处理的任务
					}
					else blocked = true; //only the last iteration is useful, others will be set back to false 
				}
				else blocked = true; //only the last iteration is useful, others will be set back to false 只有在最后一轮迭代中有效
			}
			//------
			for(int i=0; i<TASK_RECV_NUM; i++)
			{
				nothing_to_push = false;
				//unconditionally:
				if(!push_task_from_taskmap()) nothing_to_push = true; //only the last iteration is useful, others will be set back to false 就绪任务队列为空
			}
			//------
			if(nothing_to_push)
			{
                // 就绪任务队列为空
				if(blocked) usleep(WAIT_TIME_WHEN_IDLE); //avoid busy-wait when idle 就绪任务队列为空，同时缓存表或任务 map 容量达到上限，则此时当前线程需要阻塞一段时间
				else if(nothing_processed_by_pop)
				{
                    // 任务队列 q_task 为空、不能生成新任务、任务 map 中没有任务，则将当前线程设置为空闲状态
					if(map_task.size == 0) //needed because "push_task_from_taskmap()" does not check whether map_task's map is empty
					{
						unique_lock<mutex> lck(mtx_go);
						idle_set[thread_rank] = true;
						global_num_idle++;
						while(idle_set[thread_rank]){
							cv_go.wait(lck);
						}
					}
					//usleep(WAIT_TIME_WHEN_IDLE); //avoid busy-wait when idle
				}
			}
		}
	}
};

#endif
