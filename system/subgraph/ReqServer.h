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
 * 请求服务器，接收批量请求，在本地顶点列表 local_table 中查找需要拉取的顶点，并返回结果。
 * 该类中的线程会不断地重复运行，直到没有新的批量请求消息。如果没有新的请求消息，则该线程会休眠，等待新的消息。
 */

//this is the server of key-value store
#ifndef REQSERVER_H_
#define REQSERVER_H_

//it receives batches of reqs, looks up local_table for vertex objects, and responds
//this is repeated till no msg-batch is probed, in which case it sleeps for "WAIT_TIME_WHEN_IDLE" usec and probe again

#include "util/global.h"
#include "util/serialization.h"
#include "util/communication.h"
#include <unistd.h> //for usleep()
#include <thread>
#include "RespQueue.h"
using namespace std;

template <class VertexT>
class ReqServer {
public:
	typedef typename VertexT::KeyType KeyT;
	typedef typename VertexT::ValueType ValueT;
	typedef typename VertexT::HashType HashT;
	typedef hash_map<KeyT, VertexT*> VTable;

	VTable & local_table;
    /**
     * 响应队列，负责存储请求处理后的响应结果。
     * 该队列对象中封装了 num_workers 个队列（每个队列对应一个 worker），同时还维护一个线程用于将响应数据返回给请求的 worker。
     */
	RespQueue<VertexT> q_resp; //will create _num_workers responding threads

    /**
     * 请求服务器的主线程，用于接收消息。
     */
	thread main_thread;


    /**
     * 读取消息 buf 中的顶点 id（即从流中反序列化得到顶点的 id），获取相应的顶点，并将顶点放进响应队列中
     * @param buf   消息数据
     * @param size  消息大小
     * @param src   消息源（即请求源，向当前 worker 发送请求的 worker 的 id， src-wroker --发送请求--> 当前 worker，src-worker 为请求源，src 为 src-worker 的 id）
     */
	//*//unsafe version, users need to guarantee that adj-list vertex-item must exist
	//faster
	void thread_func(char * buf, int size, int src)
	{
		obinstream m(buf, size);
		KeyT vid;
		while(m.end() == false)
		{
			m >> vid; // 读取消息中顶点的 id（即从流中反序列化得到顶点的 id） ，从而在顶点 map 中通过 id 获取对应的顶点
			VertexT * v = local_table[vid];
			q_resp.add(local_table[vid], src); // 将对应顶点加入到 RespQueue 中的请求 worker 响应队列中，从而最终返回给请求的 worker
		}
	}
	//*/

	/*//safe version
	void thread_func(char * buf, int size, int src)
	{
		obinstream m(buf, size);
		KeyT vid;
		while(m.end() == false)
		{
			m >> vid;
			auto it = local_table.find(vid);
			if(it == local_table.end())
			{
				cout<<_my_rank<<": [ERROR] Vertex "<<vid<<" does not exist but is requested"<<endl;
				MPI_Abort(MPI_COMM_WORLD, -1);
			}
			q_resp.add(it->second, src);
		}
	}
	//*/

    /**
     * 在加载完数据图并且设置了 worker 的本地顶点列表后，会创建 ReqServer ，从而会启动一个线程执行 run 函数
     */
    void run() //called after graph is loaded and local_table is set
    {
    	bool first = true; // 标记是否是第一次进入 while 循环中
    	thread t;
    	//------
        // 不断地探测消息，即不断地检测是否有发给当前 worker 的请求，如果有则需要处理相应的 worker
    	while(global_end_label == false) //otherwise, thread terminates 还需要 worker 继续处理任务，因此需要继续运行
    	{
    		int has_msg;
    		MPI_Status status;
    		MPI_Iprobe(MPI_ANY_SOURCE, REQ_CHANNEL, MPI_COMM_WORLD, &has_msg, &status); // 探测接收的消息
            // MPI_Iprobe 是非阻塞型的，无论是否探测到消息都立即返回。如果探测到消息，则 has_msg 为 true，否则为 false。

    		if(!has_msg) usleep(WAIT_TIME_WHEN_IDLE); // 没有消息时，挂起请求线程 WAIT_TIME_WHEN_IDLE 微秒（默认 100）
    		else
    		{   // 有消息（即有其它 worker 的请求）时，则处理请求。即取出请求的顶点，并将这些顶点放入响应队列（RespQueue）中
    			int size;
    			MPI_Get_count(&status, MPI_CHAR, &size); // get size of the msg-batch (# of bytes) 获取接收到的消息大小
    			char * buf = new char[size]; //space for receiving this msg-batch, space will be released by obinstream in thread_func(.) 用于存储接收到的消息
    			MPI_Recv(buf, size, MPI_CHAR, status.MPI_SOURCE, status.MPI_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE); // 接收消息
    			// 判断是否是第一次进入循环，如果是第一次进入循环，则线程 t 未创建，不需要 join；否则 ，需要 join 线程
                if(!first) t.join(); //wait for previous CPU op to finish; t can be extended to a vector of threads later if necessary

                // 创建一个新的线程，负责读取消息 buf 中的顶点 id，然后获取相应顶点，并将顶点加入到响应队列中
    			t = thread(&ReqServer<VertexT>::thread_func, this, buf, size, status.MPI_SOURCE); //insert to q_resp[status.MPI_SOURCE]
    			first = false;
    		}
    	}
    	if(!first) t.join();
    }

    /**
     * 初始化 local_table ，创建并启动一个线程
     */
    ReqServer(VTable & loc_table) : local_table(loc_table) //get local_table from Worker 冒号后面的 “local_table(loc_table)” 是对成员变量 local_table 的初始化
    {
    	main_thread = thread(&ReqServer<VertexT>::run, this);
    }

    ~ReqServer()
    {
    	main_thread.join();
    }
};

#endif
