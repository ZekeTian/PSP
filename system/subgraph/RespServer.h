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
 * 响应服务器，负责接收请求处理后返回的响应结果（即负责接收远程顶点数据），其逻辑与 ReqServer 类似。
 */

//this is the server of managing vcache
#ifndef RESPSERVER_H_
#define RESPSERVER_H_

//it receives batches of resps, add new comers to vcache
//- if capacity is less than limit, just insert
//- otherwise, try to replace; if cannot work, then insert (overflow); finally, try again to trim to limit
//this is repeated till no msg-batch is probed, in which case it sleeps for "WAIT_TIME_WHEN_IDLE" usec and probe again

#include "util/global.h"
#include "util/serialization.h"
#include "util/communication.h"
#include <unistd.h> //for usleep()
#include <thread>
#include "adjCache.h"
#include "Comper.h"

using namespace std;

template <class ComperT>
class RespServer {
public:
	typedef RespServer<ComperT> RespServerT;

	typedef typename ComperT::TaskType TaskT;
	typedef typename ComperT::TaskMapT TaskMapT;

	typedef typename TaskT::VertexType VertexT;
	typedef typename VertexT::KeyType KeyT;
	typedef typename VertexT::ValueType ValueT;
	typedef typename VertexT::HashType HashT;
	typedef AdjCache<TaskT> CTable; // 缓存表数据类型

	CTable & cache_table; // 缓存表，对应论文中的 Γ-tables、R-tables、Z-table
	//thread_counter counter; //used for trim-to-vcache-limit
	thread main_thread;

    /**
     * 读取响应结果返回的消息数据（即接收返回的远程顶点），并将消息反序列化为对象
     */
	void thread_func(char * buf, int size, int mpi_src)
	{
		//get Comper objects of all threads
		TaskMapT ** taskmap_vec = (TaskMapT **)global_taskmap_vec; // 取出当前 worker 中所有线程的任务列表
		//insert received batch
		obinstream m(buf, size);
		VertexT* v;
		while(m.end() == false)
		{
			m >> v; // 反序列化为顶点
			//step 1: move vertex from pcache to vcache
			vector<long long> tid_collector;
			cache_table.insert(v->id, v, tid_collector); // 将 v 顶点请求缓存列表 pcache（R-table）中移到线程正在使用的顶点 map 中（即代码中的 vcache，论文中的 Γ-table）
			// 同时，会将请求过 v 顶点的任务的 id 保存在 tid_collector 中
            //now "tid_collector" contains the ID of the tasks that requested for v
			//why use tid_collector?
			//>> pcache.erase(.) is called by vcache.insert(.), where AdjValue-constructor is called
			//>> (1) need to insert to vcache first, and then notify; (2) vcache.insert(.) triggers pcache.erase(.), erase the thread-list before vertex-insertion
			//>> Solution to above: let vcache.insert(.) return the thread list (vec.swap), and notify them in the current function
			//------
			//step 2: notify those tasks 通知之前拉取过过 v 顶点的任务，更新这些任务的计数器（或状态）
            // 因为因为已经接收到远程顶点数据，所以对应任务的 counter 应该加 1（即已拉取到的顶点数量加 1）
            // 并且如果该任务已经拉取到所有需要的顶点，则该任务需要从挂起状态变成就绪状态。以上更新的过程均在 tmap->update() 函数中完成
			for(int i=0; i<tid_collector.size(); i++)
			{
                // 根据任务号获取线程号
				long long task_id = tid_collector[i];
				int thread_id = (task_id >> 48);

                // 根据线程号获取该线程的任务列表，并在此任务列表中更新该任务
				//get the thread's Comper object
				TaskMapT* tmap = taskmap_vec[thread_id];
				tmap->update(task_id); //add the task's counter, move to conque if ready (to be fetched by Comper)
			}
			req_counter[mpi_src]--; // 一个顶点对应一次请求，因为已经接收到一个顶点，因此向 mpi_src 号 worker 发送的请求次数减 1
		}
		/* we let GC do that now // GC 做缓存清理的工作
		//try to trim to capacity-limit
		size_t oversize = global_cache_size - VCACHE_LIMIT;
		if(oversize > 0) cache_table.shrink(oversize, counter);
		*/
	}

    void run()
    {
    	bool first = true;
    	thread t; // 用于读取返回的响应结果，限将消息数据反序列化为顶点
    	//------
    	while(global_end_label == false) //otherwise, thread terminates
    	{
            // 不断地探测消息，即不断地检测是否有响应结果返回给当前 worker
    		int has_msg;
    		MPI_Status status;
    		MPI_Iprobe(MPI_ANY_SOURCE, RESP_CHANNEL, MPI_COMM_WORLD, &has_msg, &status);
    		if(!has_msg) usleep(WAIT_TIME_WHEN_IDLE);
    		else
    		{
                // 有响应结果返回给当前 worker，则接收响应结果
    			int size;
    			MPI_Get_count(&status, MPI_CHAR, &size); // get size of the msg-batch (# of bytes)
    			char * buf = new char[size]; //space for receiving this msg-batch, space will be released by obinstream in thread_func(.)
    			MPI_Recv(buf, size, MPI_CHAR, status.MPI_SOURCE, status.MPI_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    			if(!first) t.join(); //wait for previous CPU op to finish; t can be extended to a vector of threads later if necessary
    			t = thread(&RespServerT::thread_func, this, buf, size, status.MPI_SOURCE); // 读取返回的远程顶点数据
    			first = false;
    		}
    	}
    	if(!first) t.join();
    }

    RespServer(CTable & cache_tab) : cache_table(cache_tab) //get cache_table from Worker
    {
    	main_thread = thread(&RespServerT::run, this);
    }

    ~RespServer()
	{
    	main_thread.join();
	}
};

#endif
