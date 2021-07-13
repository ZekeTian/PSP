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
 * 全局文件，本文件主要有如下的功能：
 *    （1）存储一些全局使用的变量，通常是 worker 变量的全局化（即将 Worker 类中的属性定义域扩大到全局），从而 worker 中任何地方都可以使用
 *    （2）定义全局使用的函数
 *    （3）设置系统的配置参数
 */

#ifndef GLOBAL_H
#define GLOBAL_H

#include <mpi.h>
#include <stddef.h>
#include <string.h>
#include <limits.h>
#include <math.h>
#include <assert.h> //for ease of debug
#include <sys/stat.h>
#include <ext/hash_set>
#include <ext/hash_map>

#include <unistd.h>
#include <dirent.h>
#include <sys/stat.h>

#include <atomic>
#include <mutex>
#include "conque.h"

#include "rwlock.h"

//============================
#include <time.h>

int POLLING_TIME; //unit: usec, user-configurable, used by sender
//set in init_worker()
static clock_t polling_ticks; // = POLLING_TIME * CLOCKS_PER_SEC / 1000000;

#define SLEEP_PARAM 2000 // POLLING_TIME = SLEEP_PARAM * _num_workers
//this ratio is tested on Azure
//if too small, communication is unbalanced and congestion happens
//if too big, low bandwidth utilization

#define MAX_RETRY_NUM 200 // 公共匹配结果数量较多的任务尝试执行的次数。一旦超过这个次数，无论内存是否能够存储下公共匹配结果，该任务都必须要执行

#define MAX_BATCH_SIZE 1000 //number of bytes sent in a batch 每次请求的最大顶点数量

#define WAIT_TIME_WHEN_IDLE 100 //unit: usec, user-configurable, used by recv-er

#define STATUS_SYNC_TIME_GAP 100000 //unit: usec, used by Worker main-thread

#define AGG_SYNC_TIME_GAP 1000000 //unit: usec, used by AggSync main-thread

#define PROGRESS_SYNC_TIME_GAP 1000000 //unit: usec, used by Profiler main-thread

#define TASK_BATCH_NUM 150 //minimal number of tasks processed as a unit
#define TASKMAP_LIMIT 8 * TASK_BATCH_NUM //number of tasks allowed in a task map （task map 中存放任务数的上限）

#define VCACHE_LIMIT 2000000 //how many vertices allowed in vcache (pull-cache + adj-cache)
#define VCACHE_OVERSIZE_FACTOR 0.2
#define VCACHE_OVERSIZE_LIMIT VCACHE_LIMIT * VCACHE_OVERSIZE_FACTOR

#define MAX_STEAL_TASK_NUM 10*TASK_BATCH_NUM //how many tasks to steal at a time at most 一次最多可以窃取的任务数量
#define MIN_TASK_NUM_BEFORE_STEALING 10*TASK_BATCH_NUM //how many tasks should be remaining (or task stealing is triggered) 各个 worker 中应该保留的最小任务数量，如果低于这个值，则该 worker 的负载较轻，则需要从负载较重的 worker 中窃取任务执行

#define MINI_BATCH_NUM 10 //used by spawning from local 从本地顶点中一次生成的最小任务数量
#define REQUEST_BOUND 50000 //the maximal number of requests could be sent between each two workers //tuned on GigE

#define GRAPH_LOAD_CHANNEL 200
#define REQ_CHANNEL 201
#define RESP_CHANNEL 202
#define STATUS_CHANNEL 203
#define AGG_CHANNEL 204
#define PROGRESS_CHANNEL 205
#define PARTITION_CHANNEL 206

// configuration
/**
 * Specify the partition type.
 * 0: Hash Partition; 1: LSH Partition
 */
#define PARTITION_TYPE 1

/**
 * Specify the LSH type.
 * 0: no label restriction; 1: with label restriction
 */
#define LSH_TYPE 1

/**
 * Specify the task scheduling type.
 * 0: Basic; 1: LSH 
 */
#define TASK_SCHEDULE_TYPE 1
/**
 * Set ENABLE_QUERY_GROUP_COMBINE to decide whether to enable task combine in the query group
 * 0: Disable; 1: Enable
 */
#define ENABLE_QUERY_GROUP_COMBINE 1

/**
 * Set ENABLE_QUERY_GRAPH_COMBINE to decide whether to enable task combine in the query graph
 * 0: Disable; 1: Enable
 */
#define ENABLE_QUERY_GRAPH_COMBINE 1

/**
 * Set ENABLE_LEAPFORG_JOIN to decide whether to use leapfrog join in the intersection
 * 0: Disable; 1: Enable 
 */

#define ENABLE_LEAPFORG_JOIN 1

void* global_trimmer = NULL;
void* global_taskmap_vec; //set by Worker using its compers, used by RespServer 当前 worker 中各个 Comper 的任务列表（对应 worker 中的 taskmap_vec，是其的全局化变量），若当前 worker 有 n 个线程，则有 n 个任务列表
void* global_vcache;
void* global_local_table; // worker 中存储本地顶点的 map（对应 worker 中 local_table）
void* global_query_graph_table; // 所有查询图中的顶点
void* global_label_query_graph_group; // 查询图分组，key：查询组的标签，value：对应标签的查询组，用于加速任务的生成
void* global_lsh_query_graph_group; // 查询图分组，key：查询组 id，value：对应的查询组
void* global_lsh_query_graph_group_table; // 查询图分组表(lsh分组)，key：查询图 id，value：key 查询图对应的查询组
void* global_query_vertex_order; // 查询顶点顺序
void* global_query_plan_vertex_order; // 查询计划顶点顺序
void* global_query_combine_tag;//查询点每个位置是否可以作为能减少遍历的位置
void* global_comm_combine_tag_table;
void* global_ishasChild;//在公共结构中的点在数据途中是否有点

void* global_query_plan_vertex_order_list;
void* global_query_plan_vertex_table; // 查询计划点表
void* global_query_vertex_label; // 查询图中顶点的标签集合
void* global_same_layer_edge; // 查询图中同层顶点之间的边关系
void* global_partition_table; // 顶点分区表
void* global_query_vertex_tag;//每个查询点与之前已经排序的顶点是否有相同的标签，如果有该位置赋值为1，反之为0
void* global_query_last;// 标记查查询图的最后一层是否只有一个点。如果有该位置赋值为1，反之为0
atomic<int> global_num_idle(0); // 当前 worker 中空闲 comper 的数量
atomic<bool> global_inner_work_steal_label(false); // 标记 Worker 内部是否需要进行任务窃取
void* global_task_queue; // Worker 内部全局任务队列，用于存储工作窃取的任务

conque<string> global_file_list; //tasks buffered on local disk; each element is a file name 磁盘中保存的 task 文件名
atomic<int> global_file_num; //number of files in global_file_list 磁盘中保存的 task 文件数量
atomic<int> global_load_num; // 当前 worker 负载数量

void* global_query_vertexes; // 查询图中的顶点
void* global_vertexes; // 当前 worker 存储的顶点列表（对应 worker 的 vertexes），是为了将顶点列表的作用域扩大到全局，在 worker 的构造方法中初始化，在 worker 和  comper 中均有使用。
int global_vertex_pos; //next vertex position in global_vertexes to spawn a task 顶点列表中下一个用来生成任务的顶点位置
mutex global_vertex_pos_lock; //lock for global_vertex_pos

#define TASK_GET_NUM 1
#define TASK_RECV_NUM 1
//try "TASK_GET_NUM" times of fetching and processing tasks
//try "TASK_RECV_NUM" times of inserting processed tasks to task-queue
//============================

#define hash_map __gnu_cxx::hash_map
#define hash_set __gnu_cxx::hash_set

using namespace std;

atomic<bool> global_end_label(false); // 标记所有 worker 是否都已经处理完任务，如果已经处理完所有任务，则为 true，结束迭代计算

// atomic<bool> global_query_last(false);// 标记查查询图的最后一层是否只有一个点。
//============================
///worker info
#define MASTER_RANK 0

int _my_rank;
int _num_workers;
inline int get_worker_id()
{
    return _my_rank;
}
inline int get_num_workers()
{
    return _num_workers;
}

void init_worker(int * argc, char*** argv)
{
	int provided;
	MPI_Init_thread(argc, argv, MPI_THREAD_MULTIPLE, &provided); // 在一个进程中开启多个线程，进程中开启的所有线程都是可以进行MPI Call
	if(provided != MPI_THREAD_MULTIPLE)
	{
	    printf("MPI do not Support Multiple thread\n");
	    exit(0);
	}
	MPI_Comm_size(MPI_COMM_WORLD, &_num_workers);
	MPI_Comm_rank(MPI_COMM_WORLD, &_my_rank);
    POLLING_TIME = SLEEP_PARAM * _num_workers;
    polling_ticks = POLLING_TIME * CLOCKS_PER_SEC / 1000000;
}

void worker_finalize()
{
    MPI_Finalize();
}

void worker_barrier()
{
    MPI_Barrier(MPI_COMM_WORLD); //only usable before creating threads
}

//------------------------
// worker parameters

struct WorkerParams {
    string input_path; // 数据图的数据
    string query_graph_path; // 查询图的路径
    bool force_write;
    bool native_dispatcher; //true if input is the output of a previous blogel job

    WorkerParams()
    {
    	force_write = true;
        native_dispatcher = false;
    }
};

//============================
//general types
typedef int VertexID;
typedef int Label; // 顶点标签类型，其本质须为数值类型（如 char、int）

void* global_aggregator = NULL; // worker 的聚合器

void* global_agg = NULL; //for aggregator, FinalT of previous round
rwlock agg_rwlock;

//============================
string TASK_DISK_BUFFER_DIR;
string REPORT_DIR;

//disk operations
void _mkdir(const char *dir) {//taken from: http://nion.modprobe.de/blog/archives/357-Recursive-directory-creation.html
	char tmp[256];
	char *p = NULL;
	size_t len;

	snprintf(tmp, sizeof(tmp), "%s", dir);
	len = strlen(tmp);
	if(tmp[len - 1] == '/') tmp[len - 1] = '\0';
	for(p = tmp + 1; *p; p++)
		if(*p == '/') {
				*p = 0;
				mkdir(tmp, S_IRWXU);
				*p = '/';
		}
	mkdir(tmp, S_IRWXU);
}

void _rmdir(string path){
    DIR* dir = opendir(path.c_str());
    struct dirent * file;
    while ((file = readdir(dir)) != NULL) {
        if(strcmp(file->d_name, ".") == 0 || strcmp(file->d_name, "..") == 0)
        	continue;
        string filename = path + "/" + file->d_name;
        remove(filename.c_str());
    }
    if (rmdir(path.c_str()) == -1) {
    	perror ("The following error occurred");
        exit(-1);
    }
}

atomic<bool>* idle_set; //to indicate whether a comper has notified worker of its idleness 当前 worker 中所有 comper 的工作状态，在 comper 中设置为 true，在 worker 中设置为 false
mutex mtx_go; // 全局互斥锁，在 Worker 和 Comper 中使用
condition_variable cv_go; // 全局条件变量，在 Worker 和 Comper 中使用
mutex mtx_work_steal; // 互斥锁，用于工作窃取

//used by profiler
atomic<size_t>* global_tasknum_vec; //set by Worker using its compers, updated by comper, read by profiler 各个 Comper 的任务数量
atomic<size_t>* global_mem_tasknum_vec; // 各个 Comper 中内存存储的任务数量（已经生成的，任务队列、就绪任务、阻塞任务）
atomic<size_t>* global_task_graph_size_vec; // 各个 Comper 中当前正在执行的任务子图大小
atomic<size_t>* global_task_graph_id_vec; // 各个 Comper 中当前正在执行的任务子图 id
atomic<size_t> num_stolen(0); //number of tasks stolen by the current worker since previous profiling barrier 当前 worker 窃取到任务总数

atomic<size_t>* req_counter; //to count how many requests were sent to each worker 请求次数列表，统计当前 worker 向其它 worker 发送的请求次数。如：其中第 1 个值表示当前 wroker 向第 1 号 worker 发送的请求总次数

int num_compers; // 一个 worker 所拥有的 comper 数量

//============= to allow long long to be ID =============
namespace __gnu_cxx {
    template <>
    struct hash<long long> {
        size_t operator()(long long key) const
        {
            return (size_t)key;
        }
    };
}
//====================================================

#endif
