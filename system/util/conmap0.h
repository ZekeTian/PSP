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
 * 并发的 map ，设计思路与 conmap 一样，其封装的是论文中的 R-table，即顶点请求缓存列表
 */

#ifndef CONMAP0_H
#define CONMAP0_H

#include "conmap.h"
//idea: 2-level hashing
//1. id % CONMAP_BUCKET_NUM -> bucket_index
//2. bucket[bucket_index] -> give id, get content

#include <util/global.h>
#include <vector>
#include <unordered_set>
using namespace std;

template <typename K, typename V> struct conmap0_bucket
{
	typedef hash_map<K, V> KVMap;
	KVMap bucket; // R-table，存储顶点请求缓存列表，数据类型：<KeyType, TaskIDVec>，key 为被请求顶点的 id，value 为请求 key 顶点的任务列表

	KVMap & get_map()
	{
		return bucket;
	}

	//returns true if inserted
	//false if an entry with this key alreqdy exists
	bool insert(K key, V & val)
	{
		auto ret = bucket.insert(
			std::pair<K, V>(key, val)
		);
		return ret.second;
	}

	//returns whether deletion is successful
	bool erase(K key)
	{
		size_t num_erased = bucket.erase(key);
		return (num_erased == 1);
	}
};

template <typename K, typename V> struct conmap0
{
public:
	typedef conmap0_bucket<K, V> bucket;
	bucket* buckets;

	conmap0()
	{
		buckets = new bucket[CONMAP_BUCKET_NUM];
	}

	bucket & get_bucket(K key)
	{
		return buckets[key % CONMAP_BUCKET_NUM];
	}

	bucket & pos(size_t pos)
	{
		return buckets[pos];
	}

	~conmap0()
	{
		delete[] buckets;
	}
};

#endif
