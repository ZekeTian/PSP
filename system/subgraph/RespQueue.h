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
 * 响应队列，负责存储请求处理后的响应结果，并将响应结果发送给请求 worker 
 */

#ifndef RESPQUEUE_H_
#define RESPQUEUE_H_

#include "util/global.h"
#include "util/serialization.h"
#include "util/communication.h"
#include "util/conque_p.h"
#include <atomic>
#include <thread>
#include <unistd.h> //for usleep()
using namespace std;

template <class VertexT>
class RespQueue { //part of ReqServer
public:
	typedef typename VertexT::KeyType KeyT;
	typedef typename VertexT::ValueType ValueT;
	typedef typename VertexT::HashType HashT;

    typedef conque_p<VertexT> Buffer; // 并发队列类型，用于存储各个进程的响应结果
    typedef vector<Buffer> Queue; // 队列列表，其长度等于 worker 的数量（从而保证一个 worker 拥有一个响应队列）

    /**
     * 队列列表，含有 nums_worker 个队列，通过 RespQueue.add() 对里面的队列进行赋值
     */
    Queue q;

    /**
     * 负责不断循环取出队列列表 q 中的队列，然后将序列化的结果以消息的形式发送给请求源
     */
    thread main_thread;

    /**
     * 将响应结果序列化，从而转换成可以发送的消息
     * @param i 第 i 号 worker
     * @param m 存储响应结果（即顶点）序列化后的数据
     */
    void get_msgs(int i, ibinstream & m)
	{
    	if(i == _my_rank) return;
    	Buffer & buf = q[i]; // 取出第 i 号 worker 的响应结果队列，从而将其响应结果序列化成消息
		VertexT* temp; //for fetching VertexT items
		while(temp = buf.dequeue()) //fetching till reach list-head
		{
			m << *temp;
		}
	}

    /**
     * 循环遍历队列列表 q 中的队列，然后将队列中的数据序列化，之后利用消息传递发送给相应的请求 worker
     */
    void thread_func() //managing requests to tgt_rank
    {
    	int i = 0; //target worker to send 消息发送的目标 worker
    	bool sth_sent = false; //if sth is sent in one round, set it as true 标记是否需要发送消息
        // 用 m0、m1 两个流的目的，是为了让顶点对象序列化的过程、发送消息（响应结果）的过程的并行执行
        // 其中，若 m0 是用于对象序列化，则 m1 是用于发送消息；反之，则两者角色交换。
    	ibinstream* m0 = new ibinstream;
    	ibinstream* m1 = new ibinstream;

        // 第一次发送队列列表中 0 号 worker 的队列数据
        // 因此在进入循环之前，提前将 0 号 worker 队列序列化到 m0 中，然后在进入循环时，一边发送消息，一边序列化下次发送的队列（即第 i+1 号 worker 的队列）
    	thread t(&RespQueue::get_msgs, this, 0, ref(*m0)); //assisting thread 辅助线程，用于将响应队列中的顶点对象序列化，从而转换成消息进行发送
    	bool use_m0 = true; //tag for alternating 标记是否使用 m0 中的数据发送消息，如果为 true 则发送 m0 中的消息（m1 用于序列化）；否则，发送 m1 中的消息（m0 用于序列化）
        
        // 不断循环地遍历队列列表 q ，逐个取出其中的队列，然后将队列中的数据序列化，用于消息发送（需要注意的是，当前次循环中序列化的结果将在下次循环时发送）
        while(global_end_label == false) //otherwise, thread terminates
    	{
			t.join();//m0 or m1 becomes ready to send 用于序列化对象的线程已经将对象序列化完毕后，执行下面的消息发送
			int j = i+1; // j 是下次发送消息的目标 worker
			if(j == _num_workers) j = 0; // 若已经遍历了一遍队列列表 q，则从头开始遍历

            // m0、m1 交替用于发送消息，从而使得发送消息和序列化的两个过程并行执行
			if(use_m0) //even 
			{
				//use m0, set m1 （使用 m0 的数据发送消息，用 m1 序列化对象）
				t = thread(&RespQueue::get_msgs, this, j, ref(*m1)); // 将 j 号队列的顶点对象序列化成消息，并将消息数据放入流 m1 中
				// 因为是将 j 号 worker 队列中的顶点序列化到 m1 中，所以下次在用 m1 中数据发送消息时，目标 worker 应该为 j 号 worker（但是在后面将 j 赋值给了 i，因此 i 也是目标 worker）
                // 在线程 t 进行对象序列化的过程中，主线程 main_thread 在发送消息，两者并行执行
                if(m0->size() > 0)
				{
					sth_sent = true;
					//send reqs to tgt 将请求的响应结果发送给请求源（i 号 worker）
					MPI_Send(m0->get_buf(), m0->size(), MPI_CHAR, i, RESP_CHANNEL, MPI_COMM_WORLD);
					delete m0;
					m0 = new ibinstream;
				}
				use_m0 = false; // 当前次使用 m0 中的数据发送消息，m1 存储序列化结果，则下次使用 m1 发送消息
			}
			else
			{   
				//use m1, set m0 （使用 m1 的数据发送消息，用 m0 序列化对象
				t = thread(&RespQueue::get_msgs, this, j, ref(*m0));
				if(m1->size() > 0)
				{
					sth_sent = true;
					//send reqs to tgt
					MPI_Send(m1->get_buf(), m1->size(), MPI_CHAR, i, RESP_CHANNEL, MPI_COMM_WORLD);
					//------
					delete m1;
					m1 = new ibinstream;
				}
				use_m0 = true;  // 当前次使用 m1 中的数据发送消息，m0 存储序列化结果，则下次使用 m0 发送消息
			}
			//------------------------
			i = j; // 因为 j 是下次消息发送的目标 worker，因此在进行下次循环前，将 j 赋值给 i，保证了 i 是消息发送的目标 worker
			if(j == 0) // j = 0 时，已经将所有 worker 的队列（即队列列表 q）遍历了一遍，即已经为所有 worker 发送了一次响应结果
			{
				if(!sth_sent) usleep(WAIT_TIME_WHEN_IDLE); // 如果没有消息需要发送，则挂起一段时间
				else sth_sent = false; // 有消息需要发送，则将 sth_sent 重置为 false，从而开始下一轮消息列表
			}
    	}
    	t.join();
		delete m0;
		delete m1;
    }

    RespQueue()
    {
    	q.resize(_num_workers);
    	main_thread = thread(&RespQueue::thread_func, this);
    }

    ~RespQueue()
    {
    	main_thread.join();
    }

    /**
     * 将响应结果加入到对应请求 worker 的响应队列中
     *
     * @param v   请求获取的顶点，即响应结果
     * @param tgt 请求源 id ，即向当前 worker 请求的 worker id 
     */
    void add(VertexT * v, int tgt)
    {
    	Buffer & buf = q[tgt];
    	buf.enqueue(v);
    }
};

#endif
