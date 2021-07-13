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
 * LSH map，两级哈希，用于对任务进行分组。
 * 第 1 级哈希，通过 minhash 获取任务 key 获得 lshmap 中相应的 bucket
 * 第 2 级哈希，通过 key 获得 bucket 中相应的元素
 */

#ifndef LSHMAP_H
#define LSHMAP_H

#define LSHMAP_BUCKET_NUM 1013 //should be proportional to the number of threads on a machine 哈希表中 bucket 的个数

#include <util/global.h>
#include <vector>
#include <unordered_set>
using namespace std;

template <typename K, typename V>
struct lshmap_bucket
{
    typedef hash_map<K, V> KVMap;
    mutex mtx;
    KVMap bucket;

    inline void lock()
    {
        mtx.lock();
    }

    inline void unlock()
    {
        mtx.unlock();
    }

    KVMap &get_map()
    {
        return bucket;
    }

    //returns true if inserted
    //false if an entry with this key alreqdy exists
    bool insert(K key, V &val)
    {
        auto ret = bucket.insert(
            std::pair<K, V>(key, val));
        return ret.second;
    }

    //returns whether deletion is successful
    bool erase(K key)
    {
        size_t num_erased = bucket.erase(key);
        return (num_erased == 1);
    }
};

// K1: 索引，K2: 二级哈希表索引，V3: 二级哈希表中存储的值
template <class K1, class K2, class V, class _HashFn = __gnu_cxx::hash<K1>>
struct lshmap
{
public:
    typedef lshmap_bucket<K2, V> bucket;
    typedef hash_map<K1, int, _HashFn> IndexMap;
    bucket *buckets;
    IndexMap *indexes; // 记录 bucket 索引

    lshmap()
    {
        buckets = new bucket[LSHMAP_BUCKET_NUM];
        indexes = new IndexMap;
    }

    bool contains(K1 key)
    {
        return indexes->find(key) != indexes->end();
    }

    /**
     * 通过 key 获取 bucket
     */
    bucket &get_bucket_by_key(K1 key)
    {
        return buckets[indexes->find(key)->second];
    }

    /**
     * 通过签名信息获取 bucket 的 id
     */
    size_t get_bucket_by_signature(const vector<VertexID> &sig)
    {
        // https://stackoverflow.com/questions/20511347/a-good-hash-function-for-a-vector
        std::size_t seed = sig.size();
        for (const auto &i : sig)
        {
            seed ^= i + 0x9e3779b9 + (seed << 6) + (seed >> 2);
        }
        
        if (seed < 0)
        {
            seed = -seed;
        }

        return seed % LSHMAP_BUCKET_NUM;
    }

    void insert_index(K1 key, size_t pos)
    {
        indexes->insert(make_pair(key, pos));
    }

    void erase_index(K1 key)
    {
        indexes->erase(key);
    }

    bucket &pos(size_t pos)
    {
        return buckets[pos];
    }

    ~lshmap()
    {
        delete[] buckets;
        delete indexes;
    }
};

#endif
