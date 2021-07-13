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

#ifndef AGGSYNC_H_
#define AGGSYNC_H_

#include "util/global.h"
#include "util/communication.h"
#include <unistd.h> //for usleep()
#include <thread>
using namespace std;

template <class AggregatorT>
class AggSync {
public:
	typedef typename AggregatorT::PartialType PartialT;
	typedef typename AggregatorT::FinalType FinalT;

	thread main_thread;

    /**
     * 结束标志同步。只要有一个 worker 的 endTag 为 true，则返回 true
     */
	bool endTag_sync() //returns true if any worker has endTag = true
	{
		bool endTag = global_end_label;
		if(_my_rank != MASTER_RANK)
		{
            // 非 master 的 worker 
			send_data(endTag, MASTER_RANK, AGG_CHANNEL); // 向 master 发自己的结束标志
			bool ret = recv_data<bool>(MASTER_RANK, AGG_CHANNEL); // 接收 master 发过来的结束标志
			return ret;
		}
		else
		{
            // master worker
			bool all_end = endTag;
            // 接收其它 worker 发过来的结束标志，对这些结束标志作 “或” 操作（即有一个 worker 结束标志为 true，则最终 all_end 也为 true，返回结果也为 true）
			for(int i=0; i<_num_workers; i++)
			{
				if(i != MASTER_RANK) all_end = (recv_data<bool>(i, AGG_CHANNEL) || all_end);
			}

            // 向其它 worker 发送最终的结束标志
			for(int i=0; i<_num_workers; i++)
			{
				if(i != MASTER_RANK) send_data(all_end, i, AGG_CHANNEL);
			}
			return all_end;
		}
	}

    /**
     * 对数据进行聚合并同步
     * 
     * @param sync_is_end 标记是否结束
     **/
	void agg_sync(bool sync_is_end)
	{
		AggregatorT* agg = (AggregatorT*)global_aggregator;
		if (agg != NULL)
		{
			if (_my_rank != MASTER_RANK)
			{ 	//send partialT to aggregator
                // 非 master worker ，获取自己内部各个 comper 聚合后的数据（放在 part 中），然后将该结果发给 master
				PartialT part;
				agg->finishPartial(part); // 获取内部聚合后的数据
				send_data(part, MASTER_RANK, AGG_CHANNEL); // 发给 master
				//scattering FinalT
				agg_rwlock.wrlock();
				*((FinalT*)global_agg) = recv_data<FinalT>(MASTER_RANK, AGG_CHANNEL); // 接收 master 发过来的最终聚合的数据
				agg_rwlock.unlock();
			}
			else
			{
                // master worker
                // 接收其它 worker 发过来的局部聚合数据，并将它们聚合在一起
				for (int i = 0; i < _num_workers; i++)
				{
					if(i != MASTER_RANK)
					{
						PartialT part = recv_data<PartialT>(i, AGG_CHANNEL); // 接收其它 worker 发过来的局部数据
						agg->stepFinal(part); // 将接收的数据进行聚合
					}
				}

                // 将 master 内部的聚合数据与最终的数据进行进行聚合
				FinalT final; // 最终的聚合结果
				agg->finishFinal(final); // 进行聚合
                if (sync_is_end) agg->printFinal(final); // 结束时，输出最终结果

				//cannot set "global_agg=final" since MASTER_RANK works as a slave, and agg->finishFinal() may change
				agg_rwlock.wrlock();
				*((FinalT*)global_agg) = final; //deep copy
				agg_rwlock.unlock();

                // 将最终的聚合结果发给其它 worker
				for(int i=0; i<_num_workers; i++)
				{
					if(i != MASTER_RANK)
						send_data(final, i, AGG_CHANNEL);
				}
			}
		}
		//------ call agg UDF: init(prev)
		agg_rwlock.rdlock();
		agg->init_aggSync(*((FinalT*)global_agg)); // 下一轮聚合前，对聚合器进行再一次的初始化
		agg_rwlock.unlock();
	}

    void run()
    {
    	while(true) //global_end_label is checked with "any_end"
    	{
    		bool any_end = endTag_sync();
    		if(any_end)
    		{
                // 至少有一个 worker 的结束标志为 true 时（即至少有一个 worker 工作结束），不再调用  agg_sync 进行聚合，而是阻塞等待所有 worker 执行完任务。
    			while(global_end_label == false); //block till main_thread sets global_end_label = true
    			return;
    		}
    		else{
                // 所有 worker 都处于工作状态，则进行同步
    			agg_sync(false);
    			usleep(AGG_SYNC_TIME_GAP); //polling frequency 聚合一次后，空闲一段时间，再进行下一次聚合
    		}
    	}
    }

    AggSync()
    {
    	main_thread = thread(&AggSync<AggregatorT>::run, this);
    }

    ~AggSync()
    {
    	main_thread.join();
    	agg_sync(true); //to make sure results of all tasks are aggregated 在销毁聚合器时再进行最后一次聚合，确保所有结果都已经聚合在一起
    }
};

#endif
