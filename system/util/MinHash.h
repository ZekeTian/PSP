#ifndef MINHASH_H
#define MINHASH_H

#include "GMatchSubgraph.h"
#include <vector>
#include <stdlib.h>
#include <time.h>
#include <climits>


# if __WORDSIZE == 64
#  define UINT64_C(c)	c ## UL
# else
#  define UINT64_C(c)	c ## ULL
# endif
#define SIGNATURE_SIZE 5

class MinHash
{
public:

    MinHash(int sig_num = SIGNATURE_SIZE)
    {
        signature_size = sig_num;
        // 随机初始化哈希函数
        // srand((unsigned)time(NULL));
        // srand(666);
        srand(111);
        for (int i = 0; i < sig_num; i++)
        {
            coef_a.push_back(rand() % LARGE_PRIME);
            coef_b.push_back(rand() % LARGE_PRIME);
        }
        
    }

    // 标签类型是 char 时使用
    // vector<int> signature(const vector<Label> &data_set)
    // {
    //     vector<VertexID> sig(signature_size, INT_MAX);
    //     // 逐行遍历，即逐个元素遍历
    //     for (const auto& i : data_set)
    //     {
    //         // 逐个使用哈希函数计算值
    //         for (int j = 0; j < coef_a.size(); j++)
    //         {
    //             VertexID mod = (coef_a[j] * i + coef_b[j]) % LARGE_PRIME;
    //             if (mod < sig[j])
    //             {
    //                 sig[j] = mod;
    //             }
    //         }
    //     }

    //     return sig;
    // }


    /**
     * 获取数据集合 data_set 的签名信息
     * 
     * @param   data_set 数据集合
     * 
     * @return  返回 data_set 的签名信息
     */
    vector<int> signature(const vector<VertexID> &data_set)
    {
        vector<int> sig(signature_size, INT_MAX);
        // 逐行遍历，即逐个元素遍历
        for (const auto& i : data_set)
        {
            // 逐个使用哈希函数计算值
            for (int j = 0; j < coef_a.size(); j++)
            {
                int mod = (coef_a[j] * i + coef_b[j]) % LARGE_PRIME;
                if (mod < sig[j])
                {
                    sig[j] = mod;
                }
            }
        }

        return sig;
    }

    /**
     * 获取数据集合 data_set 的签名信息
     * 
     * @param   data_set 数据集合
     * 
     * @return  返回 data_set 的签名信息
     */
    vector<int> signature(const vector<AdjItem> &data_set)
    {
        vector<int> sig(SIGNATURE_SIZE, INT_MAX);
        // 逐行遍历，即逐个元素遍历
        for (const auto& v : data_set)
        {
            // 逐个使用哈希函数计算值
            for (int j = 0; j < coef_a.size(); j++)
            {
                int mod = (coef_a[j] * v.id + coef_b[j]) % LARGE_PRIME;
                if (mod < sig[j])
                {
                    sig[j] = mod;
                }
            }
        }

        return sig;
    }

    /**
     * 获取数据集合 data_set 的哈希值
     * 
     * @param   data_set 数据集合
     * 
     * @return  返回 data_set 的哈希值
     */
    size_t hash1(const vector<VertexID> &data_set)
    {
        vector<VertexID> sig = signature(data_set);

        // 直接对 sig 进行哈希，然后将结果作为最终的哈希值
        return vector_hash(sig);
    }

    /**
     * 对邻接表进行 MinHash 计算，直接返回哈希值
     */
    size_t hash(const vector<AdjItem> &data_set)
    {
        vector<VertexID> sig = signature(data_set);

        // 在 sig 中选择最小值作为最终的哈希值
        size_t s = INT_MAX;
        for (const auto &i : sig)
        {
            if (s > i)
                s = i;
        }

        return s > 0 ? s : -s;
    }

    /**
     * 对邻接表（顶点 id）进行 MinHash 计算，直接返回哈希值
     */
    size_t hash(const vector<VertexID> &data_set)
    {
        vector<VertexID> sig = signature(data_set);

        // 在 sig 中选择最小值作为最终的哈希值
        size_t s = INT_MAX;
        for (const auto &i : sig)
        {
            if (s > i)
                s = i;
        }

        return s > 0 ? s : -s;
    }

    /**
     * 按照标签对邻接表进行 MinHash 计算，直接返回哈希值
     */
    size_t labeled_hash(const vector<AdjItem> &data_set)
    {
        hash_map<Label, int> label_sig; // 每个标签组对应的特征值
        
        // 逐行遍历，即逐个元素遍历（标签内）
        for (const auto& v : data_set)
        {
            if (label_sig.find(v.l) == label_sig.end())
            {
                label_sig[v.l] = INT_MAX;
            }

            // 逐个使用哈希函数计算值，更新 v.l 对应的特征
            for (int j = 0; j < coef_a.size(); j++)
            {
                int mod = (coef_a[j] * v.id + coef_b[j]) % LARGE_PRIME;
                if (mod < label_sig[v.l])
                {
                    label_sig[v.l] = mod;
                }
            }
        }

        // 只对 map 中的 value 进行哈希
        // size_t seed = 0;
        // std::hash<VertexID> hasher;
        // for (const auto &item : label_sig)
        // {
        //     hash_combine_impl(seed, hasher(item.second));
        // }

        // return seed;

        // 对 map 中的二元组对进行哈希，会与标签相关联
        return map_hash(label_sig);
    }

private:
    const int LARGE_PRIME = 2147483647; // 2^31 - 1
    int signature_size = 5;

    // 哈希函数系数(coefficient)，哈希函数形式：ax + b
    vector<int> coef_a;
    vector<int> coef_b;

    // from boost lib
    inline void hash_combine_impl(uint64_t& h, uint64_t k)
    {
        const uint64_t m = UINT64_C(0xc6a4a7935bd1e995);
        const int r = 47;

        k *= m;
        k ^= k >> r;
        k *= m;

        h ^= k;
        h *= m;

        // Completely arbitrary number, to prevent 0's
        // from hashing to 0.
        h += 0xe6546b64;
    }

    /**
     * 二元组哈希
     */
    template <class A, class B>
    inline size_t hash_value(std::pair<A, B> const& v)
    {
        size_t seed = 0;
        hash_combine_impl(seed, v.first);
        hash_combine_impl(seed, v.second);

        return seed;
    }

    /**
     * 对 map 进行哈希
     */
    inline size_t map_hash(const hash_map<Label, VertexID> &data_set)
    {
        size_t seed = 0;
        for (const auto &i : data_set)
        {
            hash_combine_impl(seed, hash_value(i));
        }

        return seed > 0 ? seed : -seed;
    }

    /**
     * 对 vector 进行哈希
     */
    inline size_t vector_hash(const vector<VertexID> &data_set)
    {
        size_t seed = 0;
        std::hash<VertexID> hasher;
        for (const auto &i : data_set)
        {
            hash_combine_impl(seed, hasher(i));
        }

        return seed > 0 ? seed : -seed;
    }
};

#endif /* MINHASH_H */
