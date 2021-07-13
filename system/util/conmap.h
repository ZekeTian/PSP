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
 * 并发 map，两级哈希。
 * 第 1 级哈希，通过 key 获得 conmap 中相应的 bucket
 * 第 2 级哈希，通过 key 获得 bucket 中相应的元素
 * 
 * 该 Map 中封装了论文中的 Γ-tables（线程正在使用的顶点 map）、 Z-table（记录目前没有任务使用的顶点的 id）
 */

#ifndef CONMAP_H
#define CONMAP_H

#define CONMAP_BUCKET_NUM 10000 //should be proportional to the number of threads on a machine 哈希表中 bucket 的个数

//idea: 2-level hashing
//1. id % CONMAP_BUCKET_NUM -> bucket_index
//2. bucket[bucket_index] -> give id, get content

//now, we can dump zero-cache, since GC can directly scan buckets one by one

#include <util/global.h>
#include <vector>
#include <unordered_set>
using namespace std;

/**
 * conmap 中的一个 bucket ，里面含有一个哈希表 bucket（对应论文中的 Γ-tables）和一个集合 zeros（对应论文中的 Z-tables）
 */
template <typename K, typename V> struct conmap_bucket
{
	typedef hash_map<K, V> KVMap;
	typedef unordered_set<K> KSet;
	mutex mtx;
	KVMap bucket; // 存储线程正在使用的顶点，对应论文中的 Γ-tables
	KSet zeros; // 记录目前没有 task 使用的顶点的 id（即 zeros 里面的顶点都没有被其它任务使用，可以安全地从内存中删除），对应论文中的 Z-tables

	inline void lock()
	{
		mtx.lock();
	}

	inline void unlock()
	{
		mtx.unlock();
	}

    /**
     * 获取线程正在使用的顶点 map
     */
	KVMap & get_map()
	{
		return bucket;
	}

    /**
     * 将 id 为 key，值 为 val 的顶点插入到线程正在使用的顶点 map 中。
     * 如果插入成功，返回 true；否则，返回 false（即 key 顶点已经存在 map 中）
     *
     * @param key   待插入的顶点的 id 值 
     * @param val   待插入的顶点的值
     */
	//returns true if inserted
	//false if an entry with this key alreqdy exists
	bool insert(K key, V & val)
	{
		auto ret = bucket.insert(
			std::pair<K, V>(key, val)
		);
		return ret.second;
	}

    /**
     * 将 key 顶点从线程正在使用的顶点 map 中删除。如果删除成功，返回 true；否则，返回 false。
     * 
     * @param key   待删除的顶点的 id 
     */
	//returns whether deletion is successful
	bool erase(K key)
	{
		size_t num_erased = bucket.erase(key);
		return (num_erased == 1);
	}
};

/**
 * 并发 map，含有多个 bucket
 */
template <typename K, typename V> struct conmap
{
public:
	typedef conmap_bucket<K, V> bucket;
	bucket* buckets;

	conmap()
	{
		buckets = new bucket[CONMAP_BUCKET_NUM];
	}

	bucket & get_bucket(K key)
	{
		return buckets[key % CONMAP_BUCKET_NUM];
	}

    /**
     * 返回下标为 pos 的 bucket
     */
	bucket & pos(size_t pos)
	{
		return buckets[pos];
	}

	~conmap()
	{
		delete[] buckets;
	}
};

#endif
