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
 * 任务 Map ，其包含就绪任务队列、挂起任务 Map，并且会与一个线程关联。
 * 需要注意的是，刚开始就绪任务队列和挂起任务 map 均为空，然后当任务需要拉取远程顶点时，那么该任务就会放进的挂起任务 map 中，
 * 等该挂起任务获取到所有需要的顶点数据后，会将该任务从挂起任务 map 中移到就绪任务队列中。具体过程如下：
 *  需要拉取远程 worker 顶点的任务 --（发送顶点拉取请求，处于挂起状态）--> 放进挂起任务 map --（获取到所有需要的远程顶点，处于就绪状态）--> 放进就绪任务队列中
 */

#ifndef TASKMAP_H_
#define TASKMAP_H_

#include <atomic>
#include "util/conque_p.h"
#include "util/global.h"
#include <iostream>
#include "util/conmap2t.h"

using namespace std;

template <class TaskT>
class TaskMap {
public:
    // 就绪 task 队列，满足执行条件，但是由于正在执行其它线程而暂时停止
	conque_p<TaskT> task_buf; //for keeping ready tasks
	//- pushed by Comper
	//- popped by RespServer

	typedef conmap2t<long long, TaskT *> TMap; // TMap[task_id] = task 任务 map 类型，key 为 task_id，value 为对应的 task

    /**
     * 挂起任务 map （类型为：<任务 id, 任务>），即存储不符合执行条件的任务，比如说在等待远程顶点返回的任务。其对应论文中的 Task Table T_task
     */
	TMap task_map; //for keeping pending tasks
	//- added by Comper
	//- removed by RespServer

    /**
     * 下一个任务的顺序号
     */
	unsigned int seqno; //sequence number for tasks, go back to 0 when used up 任务的顺序号，从 0 开始

    /**
     * 关联线程的线程号
     */
	int thread_rank; //associated with which thread? 线程号，记录关联的线程

    /**
     * 任务的总数量，其值 = 就绪任务队列大小 + 挂起任务 map 大小
     * 当调用 add2map 时，有新的挂起任务加入，则任务数量加 1
     * 当调用 get() 时，从就绪任务队列中取出任务，则任务数量减 1
     *    数量变化过程：新挂起任务加入到挂起任务 map 中（数量 +1） ----> 任务就绪后，任务放入就绪队列中 ----> 从就绪队列中取出任务（数量 -1）
     * 注意：从挂起任务 map 到就绪任务队列这个过程中，只是任务的状态发生变化，但是数量没有变化，因此 size 不会发生变化
     */
	atomic<int> size; // 任务的总数量，其值 = 就绪任务队列大小 + 挂起任务 map 大小
	//updated by Comper, read by Worker's status_sync
	//> ++ by add2map(.)
	//> -- by get(.)

    /**
     * 取出下一个任务的 id ，但是任务的顺序号 seqno 不会新增，在 Task.pull_all() 中使用。
     */
	long long peek_next_taskID() //not take it
	{
		long long id = thread_rank;
		id = (id << 48); //first 16-bit is thread_id 前 16 位作为线程号，后 48 位作为任务号。线程号左移 48 位，相当于是：线程号 * (2^48)，然后在这个结果上加上任务的顺序号即可得到任务的 id。
        // 第 i 号线程的线程号左移 48 位后的结果与第 i+1 号线程左移的结果之间会相差 2^48 ，因此相邻线程之间会有足够的容量存储任务号。
        // 实际上，线程最多可以有 2^16 个，而每个线程又可以有 2^48 个任务，容量足够。
        // 即：任务的 id 总共有 64 位，前 16 位是线程号，后 48 位是该任务在当前线程内部的一个顺序号（seqno），最后这两部分作为整体形成该任务的任务号
        // 这种设计可以很方便地从任务号中获取该任务对应的线程号，即只需要将任务号右移 48 位便可得到线程号
		return (id + seqno);
	}

    /**
     * 获取下一个任务的 id ，但是任务的顺序号 seqno 会新增，添加新任务时调用该函数。
     */
	long long get_next_taskID() //take it
	{
		long long id = thread_rank;
		id = (id << 48); //first 16-bit is thread_id
		id += seqno;
		seqno++;
		return id;
	}

	TaskMap() //need to set thread_rank right after the object creation
	{
		seqno = 0;
		size = 0;
	}

	TaskMap(int thread_id)
	{
		thread_rank = thread_id;
		seqno = 0;
		size = 0;
	}

    /**
     * 将任务添加到挂起任务 map 中（在 adjCache.lock_and_get 中被调用，主要是将请求远程顶点的任务加入到挂起任务 map 中）
     * 
     */
	//called by Comper, need to provide counter = task.pull_all(.)
	//only called if counter > 0
	void add2map(TaskT * task)
	{
		size++;
        // 生成一个任务号后，将新任务放入挂起任务 map 中
		//add to task_map
		long long tid = get_next_taskID(); // 生成一个任务号
		conmap2t_bucket<long long, TaskT *> & bucket = task_map.get_bucket(tid);  // 放入挂起任务 map 中
		bucket.lock();
		bucket.insert(tid, task);
		bucket.unlock();
	}//no need to add "v -> tasks" track, should've been handled by lock&get(tid) -> request(tid) 在 adjCache.lock_and_get 函数中，pcache.request 会将任务与顶点关联

    /**
     * 被 RespServer （RespServer.thread_func）调用，用于更新 task 的 counter（可能会更新挂起任务 map）。
     * 因为 RespServer 已经接收到了该任务需要的一个远程顶点数据，那么任务的 counter 应该自增（即该任务已经拉取到本地的顶点数量加 1）。
     * 当该任务所需要的远程顶点已经全部获取到，那么就会该任务从 task_map（挂起任务 Map）中移动 task_buf（就绪任务队列）中。因为此时该任务已经满足条件，不需要再被挂起。
     *
     * @param task_id   任务 id
     */
	//called by RespServer
	//- counter--
	//- if task is ready, move it from "task_map" to "task_buf"
	void update(long long task_id)
	{
        // 从 task_map 中取出 task_id 任务
		conmap2t_bucket<long long, TaskT *> & bucket = task_map.get_bucket(task_id);
		bucket.lock();
		hash_map<long long, TaskT *> & kvmap = bucket.get_map();
		auto it = kvmap.find(task_id);
		assert(it != kvmap.end()); //#DEBUG# to make sure key is found
		TaskT * task = it->second;

        // 该任务对应的 met_counter 加 1，如果该任务所需要的顶点数据已经全部获取到，则将该任务从挂起 map 中移到就绪队列中
		task->met_counter++;
		if(task->met_counter == task->req_size())
		{
			task_buf.enqueue(task);
			kvmap.erase(it);
		}
		bucket.unlock();
	}

    /**
     * 返回就绪队列中下一个任务，如果就绪队列为空，返回 NULL。
     */
	TaskT* get() //get the next ready-task in "task_buf", returns NULL if no more
	{
		//note:
		//called by Comper after inserting each task to "task_map"
		//Comper will fetch as many as possible (call get() repeatedly), till NULL is returned

		TaskT* ret = task_buf.dequeue();
		if(ret != NULL) size--;
		return ret;
	}
};

#endif /* TASKMAP_H_ */
