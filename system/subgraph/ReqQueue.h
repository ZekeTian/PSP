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
 * 请求队列，存储需要发送的请求，并且向远程 worker 发送请求
 */


#ifndef REQQUEUE_H_
#define REQQUEUE_H_

#include "util/global.h"
#include "util/serialization.h"
#include "util/communication.h"
#include "util/conque.h"
#include <atomic>
#include <thread>
#include <unistd.h> //for usleep()
using namespace std;

template <class VertexT>
class ReqQueue {
public:
	typedef typename VertexT::KeyType KeyT;
	typedef typename VertexT::ValueType ValueT;
	typedef typename VertexT::HashType HashT;

    /**
     * 请求队列，存储着 worker 请求拉取的顶点的 id
     */
    typedef conque<KeyT> Buffer; // 请求队列，存储着 worker 中请求拉取的顶点的 id
    
    /**
     * 请求队列列表类型
     */
    typedef vector<Buffer> Queue;

    /**
     * 请求队列列表，存储着各个 worker 的请求队列。每个 worker 都有一个请求队列，因此该列表中共有 nums_worker 个队列
     * 列表中第 0 个队列存储的是，当前 worker 向 0 号 worker 请求的顶点的 id。
     * 当前worker --请求--> i号worker，当前 worker 向第 i 号 worker 发送请求，则列表中第 i 个队列存储着这个请求中所有需要拉取的顶点 id
     */
    Queue q; // 请求队列列表

    /**
     * ReqQueue 中的主线程，负责发送请求，获取远程 worker 的顶点
     */
    thread main_thread;

    /**
     * 将需要请求的顶点的 id 进行序列化，保存到流 m 中，从而便于后面发送消息获取相应的顶点
     */
    void get_msgs(int i, ibinstream & m)
	{
    	if(i == _my_rank) return;
    	Buffer & buf = q[i]; // 取出当前 worker 向第 i 号 worker 请求的顶点数据
		// 遍历向第 i 号 worker 发送的请求，逐一将 buf 中存储的顶点 id 序列化到 m 流中
        KeyT temp; //for fetching KeyT items
        while(req_counter[i] < REQUEST_BOUND && buf.dequeue(temp))
        { //fetching till reach list-head
            m << temp;
            req_counter[i]++; // 向第 i 号 worker 发送的请求次数自增
            if(m.size() > MAX_BATCH_SIZE) break; //cut at batch size boundary 一旦达到一次请求的顶点数量的上限，则此次请求不会再继续发送 buf 中的数据（会在下次循环到 i 号 worker 时继续发送 buf 中剩余的数据）
        }
	}

    /**
     * 循环遍历请求队列列表 q，从中取出请求数据，然后序列化到流中，最后通过消息发送请求数据，获取远程 worker 的顶点。
     * 逻辑与 RespQueue 中类似，也是一边将请求数据序列化到流中，一边发送请求数据
     */
    void thread_func() //managing requests to tgt_rank
    {
    	int i = 0; //target worker to send 消息发送的目标 worker（即待请求数据的 worker）
    	bool sth_sent = false; //if sth is sent in one round, set it as true
    	ibinstream* m0 = new ibinstream;
    	ibinstream* m1 = new ibinstream;
    	//m0 and m1 are alternating
    	thread t(&ReqQueue::get_msgs, this, 0, ref(*m0)); //assisting thread
    	bool use_m0 = true; //tag for alternating
        clock_t last_tick = clock();

        // 不断循环遍历请求队列 q ，从中取出请求数据，然后序列化到流中，最后通过消息发送请求数据
    	while(global_end_label == false) //otherwise, thread terminates
    	{
    		t.join(); //m0 or m1 becomes ready to send
    		int j = i+1; //  j 是下次发送消息的目标 worker
    		if(j == _num_workers) j = 0;
    		if(use_m0) //even
    		{
    			//use m0, set m1
    			t = thread(&ReqQueue::get_msgs, this, j, ref(*m1));
				if(m0->size() > 0)
				{
					sth_sent = true;
					//send reqs to tgt
					MPI_Send(m0->get_buf(), m0->size(), MPI_CHAR, i, REQ_CHANNEL, MPI_COMM_WORLD);
					//------
					delete m0;
					m0 = new ibinstream;
				}
				use_m0 = false;
    		}
    		else //odd
    		{
    			//use m1, set m0
    			t = thread(&ReqQueue::get_msgs, this, j, ref(*m0));
				if(m1->size() > 0)
				{
					sth_sent = true;
					//send reqs to tgt
					MPI_Send(m1->get_buf(), m1->size(), MPI_CHAR, i, REQ_CHANNEL, MPI_COMM_WORLD);
					//------
					delete m1;
					m1 = new ibinstream;
				}
				use_m0 = true;
    		}
    		//------------------------
    		i = j;
    		if(j == 0)
    		{
    			if(!sth_sent) usleep(WAIT_TIME_WHEN_IDLE);
    			/*else{
                    sth_sent = false;
                    clock_t time_passed = clock() - last_tick; //processing time
                    clock_t gap = polling_ticks - time_passed; //remaining time before next polling
                    if(gap > 0) usleep(gap * 1000000 / CLOCKS_PER_SEC);
                }
                last_tick = clock();*/
    		}
    	}
    	t.join();
    	delete m0;
    	delete m1;
    }

    ReqQueue()
    {
    	q.resize(_num_workers);
    	main_thread = thread(&ReqQueue::thread_func, this);
    }

    ~ReqQueue()
    {
    	main_thread.join();
    }

    HashT hash;

    void add(KeyT vid)
    {
    	int tgt = hash(vid);
    	Buffer & buf = q[tgt];
    	buf.enqueue(vid);
    }
};

#endif
