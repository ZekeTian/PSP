#ifndef GMATCHAGG_H_
#define GMATCHAGG_H_

#include "subg-dev.h"

/**
 * 统计子图匹配的数量
 */
class GMatchAgg : public Aggregator<hash_map<VertexID,size_t>, hash_map<VertexID,size_t>, hash_map<VertexID,size_t>> //all args are counts
{
private:
    hash_map<VertexID,size_t> part_count;
    hash_map<VertexID,size_t> sum_count;

public:
    virtual void init()
    {
        // sum = count = 0;
         for(auto &count0 : part_count){
            part_count[count0.first] = 0;
        }

        for(auto &count0 : sum_count){
            sum_count[count0.first] = 0;
        }
    }

    virtual void init_udf(hash_map<VertexID,size_t> &prev)
    {
        // sum = 0;
         for(auto &count0 : sum_count){
            sum_count[count0.first] = 0;
        }
    }

    virtual void aggregate_udf(hash_map<VertexID,size_t> &task_count)
    {
        // count += task_count;
        for(auto &count : task_count){
            part_count[count.first]+=count.second;
        }
    }

    virtual void stepFinal_udf(hash_map<VertexID,size_t> &partial_count)
    {
        // sum += partial_count; //add all other machines' counts (not master's)
        for(auto &p_count : partial_count){
            sum_count[p_count.first] += p_count.second;
        }
    }

    virtual void finishPartial_udf(hash_map<VertexID,size_t> &collector)
    {
        // collector = count;
        for(auto &p_count : part_count){
           collector[p_count.first] += p_count.second;
        }
    }

    virtual void finishFinal_udf(hash_map<VertexID,size_t> &collector)
    {
        // sum += count; //add master itself's count
        // if (_my_rank == MASTER_RANK)
        //     cout << "结果：the # of matched graph = " << sum << endl;
        // collector = sum;

        for(auto &count0 : part_count){
            sum_count[count0.first] += count0.second;
        }

        // 边聚合边输出
        // if (_my_rank == MASTER_RANK){
        //     for(auto &count1 : sum_count){
        //         cout << "结果：the root "<<count1.first <<" of matched graph = " << count1.second << endl;
        //     }
        // }

        for(auto &p_count : sum_count){
           collector[p_count.first] = p_count.second;
        }
    }

    virtual void printFinal_udf(const hash_map<VertexID,size_t> &result)
    {
        if (_my_rank == MASTER_RANK){
            for(auto &count1 : result){
                cout << "the number of matching results for query"<< count1.first 
                     <<" is " << count1.second << endl;
            }
        }
    }
};

/** 
class GMatchAgg : public Aggregator<size_t, size_t, size_t> //all args are counts
{
private:
    size_t count;
    size_t sum;

public:
    virtual void init()
    {
        sum = count = 0;
    }

    virtual void init_udf(size_t &prev)
    {
        sum = 0;
    }

    virtual void aggregate_udf(size_t &task_count)
    {
        count += task_count;
    }

    virtual void stepFinal_udf(size_t &partial_count)
    {
        sum += partial_count; //add all other machines' counts (not master's)
    }

    virtual void finishPartial_udf(size_t &collector)
    {
        collector = count;
    }

    virtual void finishFinal_udf(size_t &collector)
    {
        sum += count; //add master itself's count
        if (_my_rank == MASTER_RANK)
            cout << "结果：the # of matched graph = " << sum << endl;
        collector = sum;
    }
};
*/

#endif