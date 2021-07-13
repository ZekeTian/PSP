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
 * 缓存表封装类，用来缓存远程顶点，主要对应论文中如下的三个表：
 * Γ-tables：已拉取到的远程顶点缓存表，记录已拉取到本地的远程顶点，即这里面的顶点已经从远程 worker 中拉取到本地（实际上也是线程正在使用的远程顶点 map）
 * R-tables：正在请求的远程顶点缓存表，记录的是那些正在请求的远程顶点，即：这些顶点正在向远程 worker 请求，但是还没有返回。
 * Z-tables：无任务使用的远程顶点缓存表，记录目前没有任务使用的远程顶点
 */

#ifndef ADJCACHE_H_
#define ADJCACHE_H_

/*
The cache of data objects wraps cuckoo hashmap: http://efficient.github.io/libcuckoo
Maintaining a lock_counter with each table entry
Two sub-structures:
1. zeroCache: keeps only those entrys with lock_counter == 0, for ease of eviction，即论文中的 Z-table
2. pullCache: keeps objects to pull from remote，即论文中的 R-tables
*/

#include <cassert>
#include <queue>
#include <atomic>
#include "ReqQueue.h" //for inserting reqs, triggered by lock_and_get(.)
#include "TaskMap.h" //an input to lock_and_get(.)
#include "util/conmap.h"
#include "util/conmap0.h"

using namespace std;

//====== global counter ======
atomic<int> global_cache_size(0); //cache size: "already-in" + "to-pull" 缓存表中已缓存的数量（即论文中 Γ-table 和 R-table 缓存的数据量）
int COMMIT_FREQ = 10; //delta that needs to be committed from each local counter to "global_cache_size"
//parameter to fine-tune !!!

//====== thread counter ======
struct thread_counter
{
    int count;
    
    thread_counter()
    {
        count = 0;
    }
    
    void increment() //by 1, called when a vertex is requested over pull-cache for the 1st time 当顶点是第一次请求时，那么值为 1
    {
        count++;
        if(count >= COMMIT_FREQ)
        {
            global_cache_size += count;
            count = 0; //clear local counter
        }
    }
    
    void decrement() //by 1, called when a vertex is erased by vcache
    {
        count--;
        if(count <= - COMMIT_FREQ)
        {
            global_cache_size += count;
            count = 0; //clear local counter
        }
    }
};

//====== pull-cache (to keep objects to pull from remote) ======
//tracking tasks that request for each vertex
/**
 * 记录需要远程拉取顶点的任务，其与 KeyType 一起搭配使用，放进 map 中（即形如 conmap0<KeyType, TaskIDVec>），从而可以记录请求 key 顶点的任务列表。
 */
struct TaskIDVec
{
	vector<long long> list; //for ease of erase 记录任务的 id，便于后面清除。因为 TaskIDVec 会与 KeyType 一起搭配使用，放进 map 中，所以其实际上就是记录请求顶点 key 的任务。

	TaskIDVec(){}

	TaskIDVec(long long first_tid)
	{
		list.push_back(first_tid);
	}
};

/**
 * 顶点拉取请求的缓存表（即论文中的 R-table），用于保存顶点的拉取请求
 */
//pull cache
template <class KeyType>
class PullCache
{
public:
    typedef conmap0<KeyType, TaskIDVec> PCache; //value is just lock-counter map
    //we do not need a state to indicate whether the vertex request is sent or not 不需要使用一个状态位来标记顶点请求是否已经发送
    //a request will be added to the sending stream (ReqQueue) if an entry is newly inserted 如果插入了一个新元素，则一个会在 ReqQueue 中添加一个请求
    //otherwise, it may be on-the-fly, or waiting to be sent, but we only merge the reqs to avoid repeated sending 如果插入的元素不是新元素（即已经在 PCache 中），那么就不会重复向 ReqQueue 中添加新请求（即合并请求，避免重复发送）
    PCache pcache; // 顶点请求缓存列表（即论文中的 R-table），请求是按照顶点 key 来区分的（不与任务挂钩），key 不同则请求不同，key 相同则为同一个请求
    
    /**
     * 将顶点 key 从顶点请求缓存列表中删除，并返回请求过 key 顶点的任务数量（其值实际上就是 tid_collector.size），以及曾请求拉取过 key 顶点的任务列表（保存在 tid_collector）
     *
     * @param key               待删除的顶点的 key
     * @param tid_collector     输出类型参数，其表示：请求 key 顶点的任务列表（即存储了请求 key 顶点的任务）
     */
    //subfunctions to be called by adjCache
    size_t erase(KeyType key, vector<long long> & tid_collector) //returns lock-counter, to be inserted along with received adj-list into vcache
    {
    	conmap0_bucket<KeyType, TaskIDVec> & bucket = pcache.get_bucket(key);
    	hash_map<KeyType, TaskIDVec> & kvmap = bucket.get_map();
    	auto it = kvmap.find(key);
    	assert(it != kvmap.end()); //#DEBUG# to make sure key is found 确保 key 顶点在 pcache 中存在
    	TaskIDVec & ids = it->second; // 请求 key 顶点的任务列表，其大小就是论文 R-table 中的 lock-count，即请求 key 顶点的任务数量
    	size_t ret = ids.list.size(); //record the lock-counter before deleting the element 
		assert(ret > 0); //#DEBUG# we do not allow counter to be 0 here 因为 key 顶点在 pcache 中，则请求顶点的任务数一定会大于 0
		tid_collector.swap(ids.list); // tid_collector 与 ids.list 交换，从而可以将 ids.list 的值通过 tid_collector 返回出去
		kvmap.erase(it); //to erase the element 将顶点 key 从顶点请求缓存列表中删除
        return ret;
    }
    
    /**
     * 判断一个拉取 key 顶点的请求是否为新请求（即根据顶点的 key 来判断是否新请求）。 
     * 如果是一个新请求（即在顶点拉取请求缓存列表中未找到拉取 key 顶点的请求，也就是说之前没有任务请求过 key 顶点），则返回 true；
     * 否则，返回 false（即该请求已经在顶点拉取请求缓存列表 pcache 中。也就是说之前已经有任务需要请求 key 顶点，因此已经将 key 顶点的拉取请求放进了请求缓存列表中）
     *
     * 同时记录需要请求 key 顶点的任务，目的是为了合并多个相同的请求，即多个任务同时拉取 key 顶点，那么多个任务的请求实际上可以合并成一个请求。
     * 换言之，即多个任务同时请求 key 顶点，没有必要每个任务都请求一次 key 顶点，当前 worker 可以只请求一次 key 顶点，等获取到 key 顶点后，这些任务可以一起使用。
     *
     * @param key       被拉取的顶点的 key，根据该值区分是否为一个新请求
     * @param counter   
     * @param task_id   任务 id，即此次拉取 key 顶点的任务的 id
     */
    bool request(KeyType key, thread_counter & counter, long long task_id) //returns "whether a req is newly inserted" (i.e., not just add lock-counter)
    {
        // 根据顶点 key 在顶点拉取请求缓存列表（pcache）中获取对应的请求
    	conmap0_bucket<KeyType, TaskIDVec> & bucket = pcache.get_bucket(key); // 获取顶点 key 对应的 bucket
		hash_map<KeyType, TaskIDVec> & kvmap = bucket.get_map(); // 获取 bucket 内部存储请求的 map
		auto it = kvmap.find(key); // 获取 key 对应的请求
    	if(it != kvmap.end())
    	{
            // 获取到了 key 对应的请求
    		TaskIDVec & ids = it->second; // 取出 map 中 Key 对应的值 
    		ids.list.push_back(task_id); // 因为 task_id 任务需要拉取 key 顶点，因此需要将 task_id 加入到 key 顶点的任务列表中
    		return false;
    	}
    	else
    	{
            // 在 pcache 中没有获取到顶点 key 对应的请求，则直接在 pcache 中放入该请求，
            counter.increment();
    		kvmap[key].list.push_back(task_id); // 因为 kvmap 中不包含 key ，因此使用 kvmap[key] 时会在 map 中插入一个 key 的元素，value 取默认值（在这里 value 默认值即为 TaskIDVec 是），然后返回 value （即不会返回 NULL）。
            // 简而言之，上面那行代码中的 kvmap[key] 会在 map 中插入一个 key 的元素，并将其返回
            // 该行代码的作用等同于：先 new 一个 TaskIDVec，然后将 task_id 放入 TaskIDVec 中，最后再将 TaskIDVec 放入 map 中
    		return true;
    	}
    }
};


/**
 * 对象缓存，在 RespServer、Task、Worker 中均有使用
 */
//====== object cache ======

template <class TaskT>
class AdjCache
{
public:

	typedef typename TaskT::VertexType ValType; // 顶点数据类型 Vertex<KeyT, ValueT, HashT> 
	typedef typename ValType::KeyType KeyType; // 顶点的 Key 数据类型

	typedef TaskMap<TaskT> TaskMapT; // 存放任务的 map 数据类型

	ReqQueue<ValType> q_req; // 请求队列
    
    /**
     * conmap 中 map 存放的元素类型，其实际上就是论文中 Γ-tables 存储的元素类型
     */
    //an object wrapper, expanded with lock-counter
    struct AdjValue
    {
        ValType * value;//note that it is a pointer !!! 顶点数据
        int counter; //lock-counter, bounded by the number of active tasks in a machine 记录 value 这个顶点被多少个任务使用（即使用 value 顶点的任务数量），受机器中活跃任务数量的限制
    };
    
    //internal conmap for cached objects
    typedef conmap<KeyType, AdjValue> ValCache; // 并发 map 类型，是一个两级哈希 map，里面包含 Γ-tables、Z-table
    ValCache vcache; // 保存缓存对象，封装了论文中的 Γ-tables（已拉取到的远程顶点缓存表，即线程正在使用的顶点 map）、 Z-table（记录目前没有任务使用的顶点的 id）
    PullCache<KeyType> pcache; // 顶点拉取请求缓存表，封装了论文中的 R-table
    
    ~AdjCache()
    {
    	for(int i=0; i<CONMAP_BUCKET_NUM; i++)
    	{
    		conmap_bucket<KeyType, AdjValue> & bucket = vcache.pos(i);
    		bucket.lock();
    		hash_map<KeyType, AdjValue> & kvmap = bucket.get_map();
    		for(auto it = kvmap.begin(); it != kvmap.end(); it++)
    		{
    			delete it->second.value; //release cached vertices
    		}
    		bucket.unlock();
    	}
    }
    
    /**
     * 被 comper（即计算线程）调用，用于获取 key 对应的远程顶点。在此函数中，会调用 add2map ，即会将任务放进挂起任务 map 中。在 Task.pull_all 中被调用。
     * 如果在本地缓存中没有找到 key 对应的顶点，返回 NULL，并且会向远程 worker 发送请求，获取顶点。
     * 如果找到，则返回该顶点。
     *
     * 注意：此函数中会调用 add2map
     *
     * 此函数在 Task.pull_all 中被调用，调用场景：
     * task_id 任务在请求远程顶点时，本地缓存中没有相应的远程顶点，需要向远程 worker 发送请求，从而获取顶点数据。
     * 如果 task_id 任务是第一次向远程 worker 发送顶点请求，该任务需要更改状态，即需要调用本函数，将该任务挂起。
     *
     * @param key       待获取的顶点 key
     * @param counter   
     * @param task_id   任务的 id
     * @param taskmap   任务对应的 map（保存了该任务的就绪任务和挂起任务） 
     * @param task      任务
     */
    //to be called by computing threads, returns:
    //1. NULL, if not found; but req will be posed
    //2. vcache[key].value, if found; vcache[counter] is incremented by 1
    ValType * lock_and_get(KeyType & key, thread_counter & counter, long long task_id,
    		TaskMapT & taskmap, TaskT* task) //used when vcache miss happens, for adding the task to task_map 会调用 add2map ，即会将任务放进挂起任务 map 中
    {
    	ValType * ret; // 返回值
    	conmap_bucket<KeyType, AdjValue> & bucket = vcache.get_bucket(key); // 获取顶点所在的 bucket 
    	bucket.lock();
    	hash_map<KeyType, AdjValue> & kvmap = bucket.get_map(); // 获取 bucket 中的 map，便于下面通过顶点的 key 取出顶点
    	auto it = kvmap.find(key); // 取出顶点数据
    	if(it == kvmap.end())
		{   // 远程顶点
            // 在 vcache 中不能取出 key 对应的顶点数据，则调用 add2map，将任务放入挂起任务 map 中
			taskmap.add2map(task); // 因为该任务需要通过发送请求来获取远程顶点，因此需要将该任务放进挂起任务 map 中

            // 因为是远程顶点，所以需要发送请求，即要在请求队列中添加一个新请求。
            // 但是在添加之前，先判断该请求是否是一个新请求（即判断之前是否有任务请求过顶点 key）
            // 如果是新请求，则之前没有任务请求过该顶点，那么请求队列中也不会存在该请求，因此需要插入到队列中。
            // 否则，该顶点在之前已经被其它任务请求过，即已经存在请求队列中，不需要再插入。
			bool new_req = pcache.request(key, counter, task_id); // 判断在顶点拉取请求缓存表中是否能找 key 顶点对应的请求
			if(new_req) q_req.add(key); // 如果是一个新请求，则将该请求加入到请求队列中
			ret = NULL;
		}
    	else
    	{   // 在本地缓存中找到远程顶点
            // 在 vcache 中能取出 key 对应的顶点数据
        	AdjValue & vpair = it->second;
        	if(vpair.counter == 0) bucket.zeros.erase(key); //zero-cache.remove 当前顶点之前在 zeros 中（即之前没有被任务使用过），则需要将其从 zeros 中删除（因为该顶点会在本次任务中使用）
        	vpair.counter++; // 该顶点会在当前任务中使用，所以该顶点对应的 counter 加 1
        	ret = vpair.value; // 返回该顶点
    	}
    	bucket.unlock();
    	return ret;
    }
    
    /**
     * 被 comper（即计算线程）调用，用于获取 key 对应的远程顶点。在此函数中，不会调用 add2map ，即不会将任务放进挂起任务 map 中。在 Task.pull_all 中被调用。
     * 如果没有找到 key 对应的顶点，返回 NULL，并且会向远程 worker 发送请求，获取顶点。
     * 如果找到，则返回该顶点。
     * 
     * 注意：此函数中不调用 add2map
     * 
     * 此函数在 Task.pull_all 中被调用，调用场景：
     * task_id 任务在之前请求远程顶点时，已经被放进挂起任务 map 中，则后面该任务在请求远程顶点时，不必再将其放入挂起任务 map 中
     */
    //to be called by computing threads, returns:
	//1. NULL, if not found; but req will be posed
	//2. vcache[key].value, if found; vcache[counter] is incremented by 1
	ValType * lock_and_get(KeyType & key, thread_counter & counter, long long task_id) // 不调用 add2map ，即不会将任务放进挂起任务 map 中
	{
		ValType * ret;
		conmap_bucket<KeyType, AdjValue> & bucket = vcache.get_bucket(key);
		bucket.lock();
		hash_map<KeyType, AdjValue> & kvmap = bucket.get_map();
		auto it = kvmap.find(key);
		if(it == kvmap.end())
		{
			bool new_req = pcache.request(key, counter, task_id);
			if(new_req) q_req.add(key);
			ret = NULL;
		}
		else
		{
			AdjValue & vpair = it->second;
			if(vpair.counter == 0) bucket.zeros.erase(key); //zero-cache.remove
			vpair.counter++;
			ret = vpair.value;
		}
		bucket.unlock();
		return ret;
	}

    /**
     * 从本地缓存中取出 key 对应的顶点。调用前需要确保 key 顶点在 map 中存在。
     *
     * @param key   待获取的顶点的 id 
     */
	//must lock, since the hash_map may be updated by other key-insertion
	ValType * get(KeyType & key) //called only if you are sure of cache hit
	{
		conmap_bucket<KeyType, AdjValue> & bucket = vcache.get_bucket(key);
		bucket.lock();
		hash_map<KeyType, AdjValue> & kvmap = bucket.get_map();
		auto it = kvmap.find(key);
		assert(it != kvmap.end());
		ValType * val = it->second.value;
		bucket.unlock();
		return val;
	}

    /**
     * 解锁 key 顶点，当计算线程使用完了 key 顶点后调用该函数。
     * 同时，使用该顶点的任务数减 1。如果使用该顶点的任务数为 0（即没有任务会使用该顶点），则将顶点移到 zero 缓存中。
     */
    //to be called by computing threads, after finishing using an object
    //will not check existence, assume previously locked and so must be in the hashmap
    void unlock(KeyType & key)
    {
    	conmap_bucket<KeyType, AdjValue> & bucket = vcache.get_bucket(key);
    	bucket.lock();
    	hash_map<KeyType, AdjValue> & kvmap = bucket.get_map();
		auto it = kvmap.find(key);
		assert(it != kvmap.end());
    	it->second.counter--; // 因为该计算线程已经使用完 key 顶点，因此使用 key 顶点的任务数需要减 1
    	if(it->second.counter == 0) bucket.zeros.insert(key); //zero-cache.insert 因为已经没有任务使用 key 顶点，因此将该顶点放入 zeros 缓存中
    	bucket.unlock();
    }

    /**
     * 将 id 为 key，值为 value 的顶点从请求缓存列表中删除，然后插入到已拉取到的远程顶点缓存表 中（即 Γ-tables）。
     * 该函数被通信线程 RespServer 调用（RespServer.thread_func() 函数），即当通信线程接收到返回的远程顶点数据后，调用该函数。
     * 
     * 该函数的过程实质上也就是将 key 顶点从请求缓存列表 pcache（R-table）中移到已拉取到的远程顶点缓存表 中（即代码中的 vcache，论文中的 Γ-table）
     * 
     * @param key               输入参数类型，顶点 id
     * @param value             输入参数类型，顶点值     
     * @param tid_collector     输出参数类型，保存请求过 key 顶点的任务 id
     */
    //to be called by communication threads: pass in "ValType *", "new"-ed outside
    //** obtain lock-counter from pcache
    //** these locks are transferred from pcache to the vcache
    void insert(KeyType key, ValType * value, vector<long long> & tid_collector) //tid_collector gets the task-id list
	{
    	conmap_bucket<KeyType, AdjValue> & bucket = vcache.get_bucket(key);
		bucket.lock();
		AdjValue vpair;
		vpair.value = value;
		vpair.counter = pcache.erase(key, tid_collector); // 将 key 顶点从请求缓存列表中删除（因为该顶点已经获取到了），同时返回请求过 key 顶点的任务（保存在 tid_collector）及其数量
		bool inserted = bucket.insert(key, vpair); // 将顶点数据插入到已拉取到的远程顶点缓存表 中（即 Γ-tables）
		assert(inserted);//#DEBUG# to make sure item is not already in vcache 必须要确保插入成功，即确保已拉取到的远程顶点缓存表 中不存在顶点 key。
        // 因为顶点 key 是通过远程请求获取到的，那说明本地正在使用的顶点 map 中一定不存在该顶点（如果存在的话，那么也不会通过远程请求获取）
		//#DEBUG# this should be the case if logic is correct:
		//#DEBUG# not_in_vcache -> pull -> insert
		bucket.unlock();
	}
    
    //note:
    //1. lock_and_get & insert operates on pull-cache （lock_and_get 和 insert 函数是在 pull-cache 上操作）
    //2. pcache.erase/request(key) should have no conflict, since they are called in vcache's lock(key) section
    
    int pos = 0; //starting position of bucket in conmap （conmap 中当前 bucket 的下标，也就是 Z-tables、Γ-tables 中当前 bucket 的下标，在删除 Z-tables、Γ-tables 的缓存数据时使用，即删除 pos 处 bucket ）

    //try to delete "num_to_delete" elements, called only if capacity VCACHE_LIMIT is reached
    //insert(.) just moves data from pcache to vcache, does not change total cache capacity
    //calling erase followed by insert is not good, as some elements soon to be locked (by other tasks) may be erased
    //we use the strategy of "batch-insert" + "trim-to-limit" (best-effort)
    //if we fail to trim to capacity limit after checking one round of zero-cache, we just return
    /**
     * 根据 Z-tables 删除 Γ-tables 中无任务使用的顶点的缓存数据（即在 Γ-tables 中，有些顶点没有被任务使用，则这些顶点的缓存数据可以删除掉，从而释放内存空间）。
     * 返回的是删除失败的顶点个数
     * 
     * @param num_to_delete     试图删除的顶点个数
     * @param counter           
     */
    size_t shrink(size_t num_to_delete, thread_counter & counter) //return value = how many elements failed to trim
    {
    	int start_pos = pos;
		//------
		while(num_to_delete > 0) //still need to delete element(s)
		{
			conmap_bucket<KeyType, AdjValue> & bucket = vcache.pos(pos);
			bucket.lock();
            // 遍历 Z-tables（其是一个集合，记录了目前没有 task 使用的顶点），取出顶点 key ，然后在 Z-tables、Γ-tables 中将顶点 key 的缓存数据删除
			auto it = bucket.zeros.begin();
			while(it != bucket.zeros.end())
			{
				KeyType key = *it; // zeros 中顶点的 key
				hash_map<KeyType, AdjValue> & kvmap = bucket.get_map(); // 已拉取到的远程顶点缓存表，对应论文中的 Γ-tables
				auto it1 = kvmap.find(key);
				assert(it1 != kvmap.end()); //#DEBUG# to make sure key is found, this is where libcuckoo's bug prompts
				AdjValue & vpair = it1->second;
				if(vpair.counter == 0) //to make sure item is not locked 确保 key 顶点确实没有任务使用，从而将该顶点从缓存表中删除
				{
                    // 删除 Z-tables、Γ-tables 中缓存的数据
					counter.decrement();
					delete vpair.value;
					kvmap.erase(it1);
					it = bucket.zeros.erase(it); //update it 返回 it 位置下一个元素的迭代器，并更新 it
					num_to_delete--;
					if(num_to_delete == 0) //there's no need to look at more candidates
					{
						bucket.unlock();
						pos++; //current bucket has been checked, next time, start from next bucket 下一次调用本函数时，从 Z-tables 的下一个 bucket 开始删除
						return 0; // 因为已经将需要删除的顶点全部删除，则删除失败的
					}
				}
			}
			bucket.unlock();
			//------
			//move set index forward in a circular manner
			pos++;
			if(pos >= CONMAP_BUCKET_NUM) pos -= CONMAP_BUCKET_NUM; // 已经遍历到最后一个 bucket （此时 pos = CONMAP_BUCKET_NUM，而不会大于 CONMAP_BUCKET_NUM），则将 pos 重置为 0
			if(pos == start_pos) break; //finish one round of checking all zero-sets, no more checking （已经将 Z-tables、Γ-tables 中所有的 bucket 完整地遍历一遍，不再进行下一轮遍历，结束删除操作）
            // 判断是否遍历了一遍 bucket 的标志是 “pos == start_pos”，而不是 “pos == start_pos” 。
            // 这是因为本函数可能会从上面的 return 处结束掉，在这种情况下，下次进入本函数时，pos 不为 0（即起始位置不为 0），所以应该与 start_pos 进行比较，而不是与 0 进行比较
            // 对 Z-tables、Γ-tables 中所有的 bucket 只遍历一遍，是因为遍历完一遍后，Z-tables 中已经删空了，因此不需要再进行删除。
            // 当 num_to_delete 的值较大，大于 Z-tables 的元素个数时，那么最终返回的 num_to_delete 就不会为 0，即有 num_to_delete 个 “删除失败” 的顶点。
		}
		return num_to_delete; // num_to_delete 最终是删除失败的顶点个数
    }

};

#endif
