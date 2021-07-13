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
 *  任务，一个 Comper 会处理多个 task 
 */

#ifndef TASK_H_
#define TASK_H_

#include "Subgraph.h"
#include "adjCache.h"
#include "util/serialization.h"
#include "util/ioser.h"
#include "Comper.h"
#include "TaskMap.h"

using namespace std;

template <class VertexT, class ContextT = char>
class Task {
public:
	typedef VertexT VertexType;
	typedef typename VertexT::KeyType KeyT;
	typedef typename VertexT::ValueType ValueT;
	typedef typename VertexT::HashType HashT;
	typedef ContextT ContextType;

    typedef Task<VertexT, ContextT> TaskT;
	typedef Subgraph<VertexT> SubgraphT;

	typedef AdjCache<TaskT> CTable;
	typedef hash_map<KeyT, VertexT*> VTable;

	typedef TaskMap<TaskT> TaskMapT;


    /**
     * 任务关联的子图
     */
	SubgraphT subG;

    /**
     * 任务关联的查询组
     */
    KeyT group_id;

    /**
     * 当前任务公共部分匹配的近似内存大小
     */
    unsigned long long comm_match_memory_size;

    /**
     * 任务尝试执行的次数
     */
    int retry_count;

    /**
     * 可用于保存任务需要的一些额外信息
     */
	ContextT context;

	HashT hash;

	//internally used:
    /**
     * 下一轮迭代中需要拉取的顶点的 id
     */
	vector<KeyT> to_pull; //vertices to be pulled for use in next round 下一轮迭代中需要拉取的顶点的 id
	//- to_pull needs to be swapped to old_to_pull, before calling compute(.) that writes to_pull
	//- unlock vertices in old_to_pull

    /**
     * 存储拉取到的顶点，这些顶点在下轮迭代中使用。如果其中第 i 个顶点是远程顶点，需要通过发送消息拉取，则在 frontier_vertexes 中第 i 个位置为 NULL
     */
	vector<VertexT *> frontier_vertexes; //remote vertices are replaced with NULL

    /**
     * 已经拉取到本地的顶点数量（即在待拉取的顶点中，有 met_counter 个顶点已经拉取到本地）
     */
	atomic<int> met_counter; //how many requested vertices are at local, updated by comper/RespServer, not for use by users (system only)

	Task()
    {
        comm_match_memory_size = 0;
        retry_count = 0;        
    }

	Task(const Task & o)
	{
		//defined so that "met_counter" is no longer a problem when using vector<TaskT>
		subG = o.subG;
		context = o.context;
		hash = o.hash;
		to_pull = o.to_pull;
        comm_match_memory_size = o.comm_match_memory_size;
        retry_count = o.retry_count;
        group_id = o.group_id;
	}

	inline size_t req_size()
	{
		return frontier_vertexes.size();
	}

	/*//deprecated, now Task has no UDF, all UDFs are moved to Comper
	//will be set by Comper before calling compute(), so that add_task() can access comper's add_task()
	virtual bool compute(SubgraphT & g, ContextType & context, vector<VertexType *> & frontier) = 0;
	*/

	//to be used by users in UDF compute(.)
	void pull(KeyT id){
		to_pull.push_back(id);
	}

	
    /**
     * 拉取顶点。
     * 如果该任务需要从远程 worker 中拉取顶点（或已经从响应中获取到了顶点数据，但是该任务当前迭代不能继续处理），则返回 false；
     * 如果该任务需要的顶点已经全部拉取到本地，则返回 true，可以继续下一轮迭代。
     */
	//after task.compute(.) returns, process "to_pull" to:
	//set "frontier_vertexes"
	bool pull_all(thread_counter & counter, TaskMapT & taskmap) //returns whether "no need to wait for remote-pulling"
	{//called by Comper, giving its thread_counter and thread_id
		long long task_id = taskmap.peek_next_taskID(); 
		CTable & vcache = *(CTable *)global_vcache; // 本地缓存表
		VTable & ltable = *(VTable *)global_local_table; // 本地顶点表
		met_counter = 0;
		bool remote_detected = false; //two cases: whether add2map(.) has been called or not 标记 task_id 任务是否已经被挂起，true 表示该任务已经挂起，false 表示未挂起（同时也可以表示是否需要发送远程请求）
        // 如果需要将 task_id 任务挂起，则会调用 add2map，将其放入挂起任务 map 中；如果 task_id 任务在之前请求远程顶点时已经被挂起，则不需要调用 
		int size = to_pull.size();
		frontier_vertexes.resize(size);

        // 逐一获取顶点
		for(int i=0; i<size; i++)
		{
			KeyT key= to_pull[i];
			if(hash(key) == _my_rank) //in local-table 在本地顶点列表中
			{
				frontier_vertexes[i] = ltable[key];
				met_counter++;
			}
			else //remote 远程顶点（有两种情况：一种是，该远程顶点已经被拉取过，并放入到缓存表中；另外一种是，该远程顶点未被拉取过，需要进行远程拉取）
			{
                // 拉取远程顶点时，先判断该任务是否需要挂起，如果该任务已经挂起，则不需要再将其挂起
				if(remote_detected) //no need to call add2map(.) again 不需要再次调用 add2map（即不需要再次将 task_id 任务挂起）
				{
                    // task_id 任务已经挂起
					frontier_vertexes[i] = vcache.lock_and_get(key, counter, task_id); // 这里的 lock_and_get 函数中不会调用 add2map 
					if(frontier_vertexes[i] != NULL) met_counter++; // 第 i 个顶点已经拉取到本地，则 met_counter 加 1
				}
				else
				{
                    // task_id 任务未被挂起
					frontier_vertexes[i] = vcache.lock_and_get(key, counter, task_id,
											taskmap, this); // 如果在缓存中找到对应顶点，则直接返回顶点；否则，需要向远程 worker 拉取顶点，会调用 add2map
					if(frontier_vertexes[i] != NULL) met_counter++;
					else //add2map(.) is called
					{
                        // 调用了 add2map()，将 task_id 任务挂起，需要将 remote_detected 设为 true，标记该任务被挂起
                        // 这样在下次拉取远程顶点时，先判断该任务是否已经被挂起，如果该任务已经处于挂起状态，则不需要调用 add2map 
						remote_detected = true;
					}
				}
			}
		}

        // 判断是否需要远程请求，如果需要远程请求，则还需要判断是否需要更新任务的状态
		if(remote_detected)
		{
            // 需要发送远程请求获取数据
			//so far, all pull reqs are processed, and pending resps could've arrived (not wakening the task)
			//------
            // 从挂起任务 map 中获取 task_id 任务，判断是否成功获取到
			conmap2t_bucket<long long, TaskT *> & bucket = taskmap.task_map.get_bucket(task_id); // 挂起任务 map
			bucket.lock();
			hash_map<long long, TaskT *> & kvmap = bucket.get_map();
			auto it = kvmap.find(task_id);

			if(it != kvmap.end())
			{
                // 挂起任务 map 中存在 task_id 任务，
				if(met_counter == req_size())  // 如果所有顶点都已经拉取到，则该任务需要从挂起状态转换成就绪状态
				{//ready for task move, delete
					kvmap.erase(it); // 将 task 从挂起任务列表中删除，并加入到就绪任务队列中
					taskmap.task_buf.enqueue(this); 
				}
				//else, RespServer will do the move 所需要的顶点未拉取完，RespServer 负责摘取远程顶点
			}
			//else, RespServer has already did the move 在挂起任务 map 中未获取到 task_id 任务，则有可能 RespServer 中已经接收完 task_id 任务所需要的远程顶点数据，然后将该任务从挂起任务 map 中移到就绪任务队列中（因此，在挂起任务 map 中未找到该任务）
			bucket.unlock();
			return false;//either has pending resps, or all resps are received but the task is now in task_buf (to be processed, but not this time) 
            // 远程请求未返回 或 远程请求已经返回但是当前任务在就绪任务队列中（不能立马执行）
		}
		else return true; //all v-local, continue to run the task for another iteration 所有顶点数据已全部拉取本地，可以继续下一轮迭代
	}

    /**
     * 任务结束时调用此函数，解锁本任务所使用的远程顶点（更新缓存表中这些顶点的状态）
     */
	void unlock_all()
	{
        // 获取当前 worker 的缓存表，更新缓存表中的顶点状态，主要是顶点的计数器（即使用顶点的任务数量）
		CTable & vcache = *(CTable *)global_vcache;

        // 遍历当前任务所使用的所有顶点，判断是否是远程顶点，如果是远程顶点，则在缓存表中更新其状态
		for(int i=0; i<frontier_vertexes.size(); i++)
		{
			VertexT * v = frontier_vertexes[i];
            // 因为是更新缓存表中顶点的状态，而缓存表中是缓存的远程顶点，因此需要先判断顶点是否为远程顶点
            // 即 hash(v->id) != _my_rank 通过判断是否为远程顶点
			if(hash(v->id) != _my_rank) vcache.unlock(v->id); // 如果是远程顶点，则需要在缓存表中更新相应的状态，即：将使用该顶点的任务数量减 1，如果减到 0，是该顶点会从 Γ-tables 移到 Z-tables 中
		}
	}

    /**
     * 远程顶点数据返回后，将远程顶点设置到拉取顶点列表（frontier_vertexes）中
     * 在 Comper.push_task_from_taskmap() 中调用
     */
	//task_map => task_buf => push_task_from_taskmap() (where it is called)
	void set_pulled() //called after vertex-pull, to replace NULL's in "frontier_vertexes"
	{
        // 遍历拉取顶点列表（frontier_vertexes），如果是远程顶点，则从缓存表中取出该远程顶点，然后放进拉取顶点列表（frontier_vertexes）中
		CTable & vcache = *(CTable *)global_vcache; 
		for(int i=0; i<to_pull.size(); i++)
		{
			if(frontier_vertexes[i] == NULL)
			{
				frontier_vertexes[i] = vcache.get(to_pull[i]);
			}
		}
	}

	friend ibinstream& operator<<(ibinstream& m, const Task& v)
	{
		m << v.subG;
		m << v.context;
		m << v.to_pull;
        m << v.group_id;

		return m;
	}

	friend obinstream& operator>>(obinstream& m, Task& v)
	{
		m >> v.subG;
		m >> v.context;
		m >> v.to_pull;
        m >> v.group_id;

        v.comm_match_memory_size = 0;
        v.retry_count = 0;

		return m;
	}

	friend ifbinstream& operator<<(ifbinstream& m, const Task& v)
	{
		m << v.subG;
		m << v.context;
		m << v.to_pull;
        m << v.group_id;

		return m;
	}

	friend ofbinstream& operator>>(ofbinstream& m, Task& v)
	{
		m >> v.subG;
		m >> v.context;
		m >> v.to_pull;
        m >> v.group_id;

        v.comm_match_memory_size = 0;
        v.retry_count = 0;

		return m;
	}
};

#endif /* TASK_H_ */
