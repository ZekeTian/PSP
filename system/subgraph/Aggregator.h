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
 *   聚合器，需要指定三个模板参数：<ValueT>, <PartialT>, <FinalT>
 *      <ValueT>：本地聚合器（各个 worker 的聚合器）的待聚合数据类型（即 task 执行完后、聚合前原始的数据类型）
 * 		<PartialT>：本地聚合器的数据类型（即各个 task 聚合后的数据类型）
 * 		<FinalT>：最终的数据类型（即各个 worker 聚合后的数据类型）
 */

#ifndef AGGREGATOR_H_
#define AGGREGATOR_H_

#include <stddef.h>
#include <util/rwlock.h>
using namespace std;

template <class ValueT, class PartialT, class FinalT>
class Aggregator {
public:
	rwlock lock;
    typedef PartialT PartialType;
    typedef FinalT FinalType;

    virtual void init() = 0;

    /**
     * 下一轮聚合之前，对聚合器进行再一次初始化（可用于更新本地 worker 的聚合器数据）。
     *
     * @param prev  上一轮聚合后的结果
     */
    virtual void init_udf(FinalT & prev) = 0;
    //called right after agg-sync, prev is the previously sync-ed value
    void init_aggSync(FinalT & prev)
	{
		lock.wrlock();
		init_udf(prev);
		lock.unlock();
	}

    /**
     * 各个 worker 聚合各自 comper 任务执行完后的数据（即 worker 内部的数据聚合），在 Comper 的 compute 执行结束后调用该函数
     *
     * @param context Comper 执行完任务后的最终结果
     */
    virtual void aggregate_udf(ValueT & context) = 0;
    void aggregate(ValueT & context)
    {
    	lock.wrlock();
    	aggregate_udf(context);
    	lock.unlock();
    }

    /**
     * master 聚合其它 worker 的结果（此时 master 不会聚合 master 本身的数据）。
     * 该过程即为：将其它 worker 的内部聚合结果聚合到 master 的 final_result（全局最终聚合结果） 中
     *
     * @param part	其它 worker 内部聚合的结果
     */
    virtual void stepFinal_udf(PartialT & part) = 0;
    void stepFinal(PartialT & part)
    {
    	lock.wrlock();
    	stepFinal_udf(part);
    	lock.unlock();
    }

    /**
     * 返回各个 worker 内部聚合后的数据（即 aggregate_udf 聚合后的数据）
     *
     * @param collector 输出数据类型，将当前 worker 内部聚合的结果放进 collector 中，从而返回结果
     */
    virtual void finishPartial_udf(PartialT & collector) = 0;
    virtual void finishPartial(PartialT & collector)
    {
    	lock.rdlock();
    	finishPartial_udf(collector);
    	lock.unlock();
    }

    /**
     * master worker 将自己的数据与其它 worker 聚合后的数据（即经过 stepFinal_udf 聚合后的数据，final_result）进行最后的一次聚合，获得最终的聚合结果
     *
     * @param collector 输出参数类型，即将最终所有 worker 聚合的结果放进 collector 中，从而返回结果
     */
    virtual void finishFinal_udf(FinalT & collector) = 0;
    virtual void finishFinal(FinalT & collector)
    {
    	lock.rdlock();
    	finishFinal_udf(collector);
    	lock.unlock();
    }

    /**
     * 输出最终的聚合结果
     * 
     * @param result 最终的结果
     **/
    virtual void printFinal_udf(const FinalT & result) = 0;
    virtual void printFinal(const FinalT & result)
    {
    	lock.rdlock();
    	printFinal_udf(result);
    	lock.unlock();
    }

};

class DummyAgg : public Aggregator<char, char, char> {
public:
    virtual void init()
    {
    }
    virtual void init(char& prev)
	{
	}
    virtual void aggregate_udf(char & v)
    {
    }
    virtual void stepFinal_udf(char & part)
    {
    }
    virtual void finishPartial_udf(char & collector)
    {
    }
    virtual void finishFinal_udf(char & collector)
    {
    }
};

#endif /* AGGREGATOR_H_ */
