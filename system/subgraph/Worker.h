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
 * Worker（进程），根据 global.h 中的配置参数选择使用分区类型、任务调度方式
 */

#ifndef WORKER_H_
#define WORKER_H_

#include <iostream>

#include "util/global.h"
#include "util/ydhdfs.h"
#include "util/communication.h"
#include "util/logger.h"
#include "util/lshmap.h"
#include "util/MinHash.h"
#include "comm-struct.h"
#include "Subgraph.h"
#include "util/conque_p.h"
#include "Trimmer.h"
#include "adjCache.h"
#include "ReqServer.h"
#include "RespServer.h"
#include "Comper.h"
#include "GC.h"
#include "AggSync.h"
#include "Profiler.h"
#include <unistd.h> //sleep(sec)
#include <queue> //for std::priority_queue
#include <algorithm> // for lsh partition

using namespace std;

template <class Comper>
class Worker
{
public:
	typedef typename Comper::TaskType TaskT;
    typedef typename Comper::AggregatorType AggregatorT;
	typedef typename Comper::TaskMapT TaskMapT;

    typedef typename TaskT::VertexType VertexT;
    typedef typename TaskT::SubgraphT SubgraphT;
    typedef typename TaskT::ContextType ContextT;


    typedef typename VertexT::KeyType KeyT;
	typedef typename VertexT::ValueType ValueT;
	typedef typename VertexT::HashType HashT;

    typedef vector<VertexT*> VertexVec; // 顶点列表数据类型

    typedef hash_map<KeyT, VertexT*> VTable;
    typedef typename VTable::iterator TableIter;

    typedef AdjCache<TaskT> CTable;

    typedef typename AggregatorT::PartialType PartialT;
    typedef typename AggregatorT::FinalType FinalT;

    typedef GC<TaskT> GCT;

    typedef Trimmer<VertexT> TrimmerT;

    //=======================================================
    //worker's data structures

    /**
     * 顶点的哈希函数类
     */
    HashT hash;

    /**
     * local_table 是当前 worker 存储的本地顶点，存储的顶点和 vertexes 一样，只不过用 Map 存储。
     * 用 Map 存储的原因，是为了在处理其它 worker 的顶点拉取请求时，可以通过顶点 id 快速查找相应顶点
     */
    VTable local_table; //key-value store of my vertex

    /**
     * 用于查询图分组的 LSH
     */
    lshmap<KeyT, string, QueryGroup*> query_graph_group_lsh; 

    /**
     * 用 map 存储查询图中的顶点，与 query_vertexes 中的数据一样。
     */
    VTable query_graph_table;

    /**
     * 所有查询图的查询顶点顺序，key：查询图根顶点 id，value：该查询图对应的查询顶点顺序
     */
    hash_map<KeyT, QueryVertexVec> order_list_table;

    /**
     * 所有查询图的查询计划顶点顺序，key：查询图根顶点 id，value：该查询图对应的查询计划顶点顺序
     */
    hash_map<KeyT, QueryPlanVertexVec> plan_order_list_table;

	hash_map<KeyT, QueryPlanVertexVecList> plan_list_table;
	hash_map<KeyT, vector<query_judge>> query_combine_tag_table;
    hash_map<KeyT, vector<query_judge>> comm_query_combine_tag_table;
	hash_map<KeyT,vector<int>> query_vertex_tag_table;;
	hash_map<KeyT,bool> query_last_tabel;
    /**
     * 查询图中顶点的标签集合
     */

    hash_set<Label> query_vertex_label;

    /**
     * 数据图中各个顶点标签出现的频率
     */
    hash_map<Label, int> data_vertex_label_frequency;

    /**
     * 远程顶点的缓存
     */
    CTable * cache_table; //cached remote vertices, it creates ReqQueue for appeding reqs 

    /**
     * 存储本地顶点，与 local_table 中数据一样，只不过使用的数据结构不同
     */
	VertexVec vertexes;

	/**
	 * 存储所有查询图中的顶点，一个 VertexVec 代表一个查询图
	 */
    vector<VertexVec> query_vertexes;

    /**
     * 存储各个 comper 的 task
     */
    TaskMapT** taskmap_vec;

    /**
     * worker 空闲状态标志位
     */
    bool local_idle; //indicate whether the current worker is idle  
    //logic with work stealing:
    //if it tries to steal from all workers but failed to get any job, this should be set
    //after master's idle-condition sync, if job does not terminate, should steal for another round
    //ToDo："local_idle" may need to change to "one per thread", or aggregated over 

    /**
     * 当前 worker 含有的 comper （线程）
     */
    Comper* compers; //dynamic array of compers 

    //=======================================================
    //Trimmer
    void setTrimmer(TrimmerT* trimmer)
	{
    	global_trimmer = trimmer;
	}

    //=======================================================
    //constructor & destructor

    Worker(int comper_num, string local_disk_path = "buffered_tasks", string report_path = "report")
    {
    	num_compers = comper_num;
    	TASK_DISK_BUFFER_DIR = local_disk_path;
    	_mkdir(TASK_DISK_BUFFER_DIR.c_str());
    	REPORT_DIR = report_path;
    	_mkdir(REPORT_DIR.c_str());
    	//------
    	global_end_label = false;
    	local_idle = false;
    	global_trimmer = NULL;
    	global_aggregator = NULL;
    	global_agg = NULL;
    	req_counter = new atomic<size_t>[_num_workers]; // 存储向其它 worker 发送的请求次数
    	for(int i=0; i<_num_workers; i++) req_counter[i] = 0; //must be before the next line
    	global_vcache = cache_table = new CTable;
    	global_local_table = &local_table;

    	global_query_graph_table = &query_graph_table;
		global_vertexes = &vertexes; // 将本地顶点列表引用保存到 global_vertexes 中，从而扩大作用域，确保顶点列表在当前 worker 中全局可用
		global_query_vertexes= &query_vertexes;
		global_query_vertex_order = &order_list_table;
		global_query_plan_vertex_order = &plan_order_list_table;
		global_query_plan_vertex_order_list = &plan_list_table;
		global_query_combine_tag= &query_combine_tag_table;
        global_comm_combine_tag_table=&comm_query_combine_tag_table;
		global_query_vertex_tag = &query_vertex_tag_table;
		global_query_last = &query_last_tabel;

		global_query_vertex_label = &query_vertex_label;
        global_same_layer_edge = new hash_map<KeyT, EdgeVector>;
        global_label_query_graph_group = new hash_map<Label, vector<QueryGroup*>>; // 查询图分组，key：查询组的标签，value：对应标签的查询组，用于加速任务的生成
        global_lsh_query_graph_group = new hash_map<KeyT, QueryGroup*>; // 查询图分组，key：查询组 id，value：对应查询组
        global_lsh_query_graph_group_table = new hash_map<KeyT, QueryGroup*>; // 查询图分组表，key：查询图 id，value：key 查询图对应的查询组
        global_query_plan_vertex_table = new hash_map<KeyT, QueryPlanVertex>; // 查询计划点表

#if PARTITION_TYPE == 1
        global_partition_table = new hash_map<KeyT, int>; // 分区表
#endif
        global_task_queue = new conque_p<TaskT>; // Worker 内部全局任务队列，用于存储工作窃取的任务

		idle_set = new atomic<bool>[comper_num]; // 当前 worker 中各个 comper 的状态，true 表示线程处于空闲状态
		for(int i=0; i<comper_num; i++) idle_set[i] = false;
    }

    void setAggregator(AggregatorT* ag)
    {
        global_aggregator = ag;
        global_agg = new FinalT;
        ag -> init();
    }

    AggregatorT* get_aggregator() //get aggregator
    //cannot use the same name as in global.h (will be understood as the local one, recursive definition)
	{
		return (AggregatorT*)global_aggregator;
	}

	virtual ~Worker()
	{
        for(int i=0;i<vertexes.size();i++)
        {
            delete vertexes[i];
        }
        for (int i = 0; i < query_vertexes.size(); i++)
        {
            for (int j = 0; j < query_vertexes[i].size(); j++)
            {
                delete query_vertexes[i][j];
            }
        }

		delete[] compers;
		delete[] taskmap_vec;
		delete[] global_tasknum_vec;
        delete[] global_mem_tasknum_vec;
		delete[] global_task_graph_size_vec;
		delete[] global_task_graph_id_vec;
		delete[] idle_set;
		delete[] req_counter;
		delete cache_table;

        // 删除同层顶点之间的边关系
        hash_map<KeyT, EdgeVector>* edge = (hash_map<KeyT, EdgeVector>*)global_same_layer_edge;
        delete edge;
        hash_map<Label, vector<QueryGroup*>>* label_query_graph_group = (hash_map<Label, vector<QueryGroup*>>*)global_label_query_graph_group;
        delete label_query_graph_group;
        hash_map<KeyT, QueryGroup*>* lsh_query_graph_group = (hash_map<KeyT, QueryGroup*>*)global_lsh_query_graph_group;
        delete lsh_query_graph_group;
        hash_map<int, QueryGroup*>* lsh_query_graph_group_table = (hash_map<KeyT, QueryGroup*>*)global_lsh_query_graph_group_table;
        delete lsh_query_graph_group_table;

        hash_map<KeyT, QueryPlanVertex>* query_plan_vertex_table = (hash_map<KeyT, QueryPlanVertex>*)global_query_plan_vertex_table; // 查询计划点表
        delete query_plan_vertex_table;

#if PARTITION_TYPE == 1
        hash_map<KeyT, int>* partition_table = (hash_map<KeyT, int>*)global_partition_table; // 分区表
        delete partition_table;
#endif

        conque_p<TaskT>* task_queue_ptr = (conque_p<TaskT>*)global_task_queue; // Worker 内部全局任务队列，用于存储工作窃取的任务
        delete task_queue_ptr;

		//ToDo: release aggregator
        if (global_agg != NULL)
            delete (FinalT*)global_agg;
	}

	//=======================================================
	//graph loading:

	//user-defined loading function
	virtual VertexT* toVertex(char* line) = 0;

    /**
     * 加载数据图
     * @param inpath 输入文件路径
     * @param vVec   存储读入的顶点数据
     */
	void load_graph(const char* inpath, VertexVec & vVec)
	{
		TrimmerT* trimmer = NULL;
		if(global_trimmer != NULL) trimmer = (TrimmerT*)global_trimmer;
		//------
		hdfsFS fs = getHdfsFS();
		hdfsFile in = getRHandle(inpath, fs);
		LineReader reader(fs, in);
		while(true)
		{
			reader.readLine();
			if (!reader.eof())
			{
				VertexT * v = toVertex(reader.getLine()); // 将行内容转换成顶点数据类型
				if(trimmer) trimmer->trim(*v);
				vVec.push_back(v); // 将顶点放入顶点列表中
                data_vertex_label_frequency[v->value.l]++;// 相应数据点标签频率加 1
			}
			else
				break;
		}
		hdfsCloseFile(fs, in);
		hdfsDisconnect(fs);
	}

        /**
     * 检测查询组内查询图的公共子图部分
     * 
     * @param query_graph_group   查询组
     */
    virtual void detect_common_subgraph(hash_map<KeyT, QueryGroup*> &query_graph_group) = 0;

    /**
     * 生成查询计划
     * @param query_graph_table         查询图顶点 map
     * @param vec                       查询图顶点列表
     * @param order_list                生成的查询顶点顺序，输出参数
     * @param plan_order_list           生成的查询计划顶点顺序，输出参数
	 * @param plan_list				    查询计划顶点列表，输出参数
     * @param query_plan_vertex_table   查询计划顶点表，输出参数
     * @param edges                     同层顶点之间的边关系
     */
    virtual void generateQueryPlan(VTable &query_graph_table, VertexVec &vec, 
        QueryVertexVec &order_list, QueryPlanVertexVec &plan_order_list,
        QueryPlanVertexVecList &plan_list, hash_map<KeyT, QueryPlanVertex> &query_plan_vertex_table,
        vector<int> &query_vertex_tag_list,bool &query_last, EdgeVector &edge_vec) = 0;

	/**
	 * 导入查询图数据
	 */
    void load_query_graph(const char* inpath, vector<VertexVec> & gVec)
    {
        hdfsFS fs = getHdfsFS();
        hdfsFile in = getRHandle(inpath, fs);
        LineReader reader(fs, in);
        char *line = NULL;
        int count = 0; // 已经读取的查询图数量
        vector<VertexT *> vec; // 存储单个查询图的顶点
        gVec.push_back(vec);

        while(true)
        {
            reader.readLine();
            if (!reader.eof())
            {
                line = reader.getLine();
                if (*line == '\r' || *line == '\0')
                {
                    count++;
                }
                else 
                {
                    if (gVec.size() <= count)
                    {
                        gVec.push_back(vec);
                    }
                    
                    VertexT * v = toVertex(line);
                    gVec[count].push_back(v);
                }
            }
            else
                break;
        }
        hdfsCloseFile(fs, in);
        hdfsDisconnect(fs);
    }

    /**
     * 将查询图数据发送给其它 worker
     */
    void sync_query_graph(VertexVec & gVec)
    {
        graph_to_all(gVec, GRAPH_LOAD_CHANNEL);
    }

    /**
     * 设置查询图的数据
     * 将查询顶点列表保存成 map 类型
     */
    void set_query_graph_data(vector<VertexVec> & gVec)
    {
        for(int i=0; i<gVec.size(); i++)
        {
            for(int j=0; j<gVec[i].size(); j++)
            {
                VertexT * v = gVec[i][j];
                query_graph_table[v->id] = v;
            }
        }
    }


   /**
     * 使用 LSH 对顶点进行划分（内部会进行排序，即遍历是存在先后顺序的）
     * 
     * @param   v                   顶点数据
     * @param   partition_table     分区表
     * @param   label_count_vec     各个分区中不同标签的数量
     * @param   partition_size      各个分区的大小
     * @param   maximum_capacity    每个分区的最大容量
     * @return                      返回 v 的分区号      
     */
    int lsh_partition_order(VertexT *v, hash_map<KeyT, int> *partition_table,
            MinHash &minhash,
            const vector<int> &partition_size,
            const int maximum_capacity)
    {
        int best_part_id = -1;        // 顶点 v 最佳分区的 id

        // 计算顶点 v 的 MinHash
#if LSH_TYPE == 0
        size_t sig = minhash.hash(v->value.adj); // 无标签限制的 LSH
#elif LSH_TYPE == 1
        size_t sig = minhash.labeled_hash(v->value.adj); // 含有标签限制的 LSH
#endif

        best_part_id = sig % _num_workers;

        // 进行负载均衡
        // 求出当前顶点 v 可能被划分到的分区的平均大小
        // hash_set<int> v_partition_id; // 顶点 v 可能被划分到的分区
        // size_t partition_total_size = 0; // 分区总大小
        // size_t partition_avg_size = 0; // 分区平均大小
        // for (const auto& s : sig)
        // {
        //     int p_id = s % _num_workers;
        //     if (p_id < 0)
        //         p_id = -p_id;
        //     if (v_partition_id.find(p_id) == v_partition_id.end())  
        //     {
        //         v_partition_id.insert(p_id);
        //         partition_total_size += partition_size[p_id];
        //     }
        // }
        // partition_avg_size = partition_total_size / v_partition_id.size();

        // // 对 sig 进行从小到大的排序
        // sort(sig.begin(), sig.end());

        // // 从哈希值最小的开始确定分区号
        // for (const auto& s : sig)
        // {
        //     int p_id = s % _num_workers;
        //     if (p_id < 0)
        //         p_id = -p_id;
        //     int p_size = partition_size[p_id];  // 分区 p_id 中的顶点数量

        //     // 判断该哈希值对应的分区的负载是否较轻(是否小于平均值)，如果较轻，则放入该分区；否则，继续下一个哈希值
        //     if (p_size <= partition_avg_size)
        //     {
        //         best_part_id = p_id;
        //         break;
        //     }
        // }

        return best_part_id;
    }

    /**
     * 使用 LSH 对顶点进行划分
     * 
     * @param   v                   顶点数据
     * @param   partition_table     分区表
     * @param   label_count_vec     各个分区中不同标签的数量
     * @param   partition_size      各个分区的大小
     * @param   maximum_capacity    每个分区的最大容量
     * @param   label_flag          标记标签的数量是否归一化，如果为 true 则规一化；否则，不规一化，直接使用数量。默认归一化。
     * @return                      返回 v 的分区号      
     */
    int lsh_partition(VertexT *v, hash_map<KeyT, int> *partition_table,
            MinHash &minhash,
            const vector<int> &partition_size,
            const int maximum_capacity)
    {
        int best_part_id = -1;        // 顶点 v 最佳分区的 id
        int best_part_load = INT_MAX; // 当前最佳分区时对应的负载
        // vector<KeyT> adj_vec(v->value.adj.size());
        // for (int i= 0; i < adj_vec.size(); ++i)
        // {
        //     adj_vec[i] = v->value.adj[i].id;
        // }
        
        // 计算顶点 v 的 MinHash
        vector<int> sig = minhash.signature(v->value.adj);
        for (const auto& s : sig)
        {
            int p_id = s % _num_workers;
            if (p_id < 0)
                p_id = -p_id;
            int p_size = partition_size[p_id];  // 分区 p_id 中的顶点数量

            // 比较负载，取负载最小的分区
            if (p_size < best_part_load)
            {
                best_part_load = p_size;
                best_part_id = p_id;
            }
        }

        return best_part_id;
    }

    /**
     * 同步图，即对读入的顶点数据进入分区，然后将其放入到分区表中相应的分区内，之后再将分区表中的数据发给相应的 worker。
     * 同时，当前 worker 也会接收其它 worker 发过来的分区数据（其它 worker 发过来的数据就是当前 worker 分区需要保存的数据）。
     * 简而言之，同步图的过程就是数据归位的过程，即对读入的顶点数据进行分区后，让这些数据回归到各自应该所在的分区内（即 worker）。
     *
     * @param vVec 本地的顶点数据，在分区之前，保存的是当前 worker 读取的顶点数据（作为输入参数）；分区后，保存的是当前 worker 所对应分区的数据（作为输出参数）
     */
	void sync_graph(VertexVec & vVec)
	{
		//ResetTimer(4);
		//set send buffer
        vector<VertexVec> _loaded_parts(_num_workers); // 分区表，二维向量，一个元素即为一个 worker 的顶点集（即一个分区）
#if PARTITION_TYPE == 0
        // 使用 Hash 划分，根据顶点 id 的哈希值将顶点放在相应分区内
		for (int i = 0; i < vVec.size(); i++) {
			VertexT* v = vVec[i];
			_loaded_parts[hash(v->id)].push_back(v);
		}
#elif PARTITION_TYPE == 1
        // 使用 LSH 在线划分数据
        hash_map<KeyT, int> *partition_table = (hash_map<KeyT, int>*)global_partition_table; // 记录顶点的分区，key：顶点 id，value：顶点分区号
        vector<hash_map<Label, int> > label_count_vec(_num_workers);            // 各个分区中不同标签的数量
        vector<int> partition_size(_num_workers, 0);                           // 记录各个分区的大小
        float alpha = 1.01f;                                                   // 分区最大容量调节因子
        const int maximum_capacity = (int)(alpha * vVec.size() / _num_workers); // 每个分区的最大容量
        MinHash minhash(1);

        for (int i = 0; i < vVec.size(); i++)
        {
            VertexT *v = vVec[i];
            int part_id = lsh_partition_order(v, partition_table, minhash, partition_size, maximum_capacity);
            // int part_id = lsh_partition(v, partition_table, minhash, partition_size, maximum_capacity);
            partition_table->insert(make_pair(v->id, part_id));
            ++partition_size[part_id]; // 相应分区的顶点数量加 1
            hash_map<Label, int> &label_count_map = label_count_vec[part_id];
            ++label_count_map[v->value.l];

            _loaded_parts[part_id].push_back(v); // 放入相应的分区
        }

        // if (_my_rank == MASTER_RANK)
        // {
        //     cout << "各个分区的标签数量分布情况：" << endl;
        //     for (int i = 0; i < label_count_vec.size(); i++)
        //     {
        //         hash_map<Label, int> &label_count_map = label_count_vec[i];
        //         cout << i << " 号分区：";
        //         for (const auto &map_item : label_count_map)
        //         {
        //             cout << map_item.first << "->" << map_item.second << " ";
        //         }
        //         cout << endl;
        //     }
        // }

        // 保存分区结果
        // int p_id = 0;
        // for (const auto &part : _loaded_parts)
        // {
        //     char out_name[64];
        //     sprintf(out_name, "partition%d.txt", p_id);
        //     ofstream out(out_name, ios::app);
        //     for (const auto &v : part)
        //     {
        //         out << v->id << " " << v->value.l << endl;
        //     }
        //     ++p_id;
        //     out.close();
        // }
        allreduce(*partition_table, PARTITION_CHANNEL); // 将分区表规约后，发送给所有 Worker
#endif
		//exchange vertices to add
		all_to_all(_loaded_parts, GRAPH_LOAD_CHANNEL); // 将分区中的顶点数据发给相应的 worker，同时也接收其它 worker 发过来的数据（_loaded_parts 在发数据前是保存发送的数据，在发完数据后其用来保存接收的数据）

		vVec.clear();
        // 在 all_to_all 接收完当前 worker 的分区顶点数据后，__loaded_parts 存储的是其它 worker 发送给当前 worker 的分区顶点数据（简而言之，即 __loaded_parts 保存当前 worker 的分区数据）
        // 注意：all_to_all 调用前，_loaded_parts 保存发送的数据；调用后，保存接收的分区数据
		//collect vertices to add 将 _loaded_parts 二维向量的数据全部放入到一维向量 vVec （即本地顶点列表）中
		for (int i = 0; i < _num_workers; i++) {
			vVec.insert(vVec.end(), _loaded_parts[i].begin(), _loaded_parts[i].end());
		}
		_loaded_parts.clear();
		//StopTimer(4);
		//PrintTimer("Reduce Time",4);
	};

	void set_local_table(VertexVec & vVec)
	{
		for(int i=0; i<vVec.size(); i++)
		{
			VertexT * v = vVec[i];
			local_table[v->id] = v;
		}
	}

	//=======================================================
	void create_compers()
	{
		compers = new Comper[num_compers];
		//set global_taskmap_vec
		taskmap_vec = new TaskMapT*[num_compers];
		global_tasknum_vec = new atomic<size_t>[num_compers]; // 各个 Comper 的任务数量
        global_mem_tasknum_vec = new atomic<size_t>[num_compers]; // 各个 Comper 的任务数量
		global_task_graph_size_vec = new atomic<size_t>[num_compers]; // 各个 Comper 的任务子图大小
		global_task_graph_id_vec = new atomic<size_t>[num_compers]; // 各个 Comper 的任务子图 id
		global_taskmap_vec = taskmap_vec; // 各个 Comper 的任务列表
		for(int i=0; i<num_compers; i++)
		{
			taskmap_vec[i] = &(compers[i].map_task);
			global_tasknum_vec[i] = 0;
            global_mem_tasknum_vec[i] = 0;
			global_task_graph_size_vec[i] = 0;
			global_task_graph_id_vec[i] = 0;            
			compers[i].start(i);
		}
	}

    /**
     *  Master Worker 更新所有 Worker 中的 global_end_label 状态，当所有 worker 都空闲时，global_end_label 为 true
     */
	//called by the main worker thread, to sync computation-progress, and aggregator
	void status_sync(bool sth2steal)
	{
        // 本 worker 只有当其不需要进行工作窃取并且其所有 comper 都空闲时，才表明该 worker 空闲
		bool worker_idle = (sth2steal == false) && (global_num_idle.load(memory_order_relaxed) == num_compers);
		if(_my_rank != MASTER_RANK)
		{
            // 本 worker 将自己的工作状态发送给 Master 
			send_data(worker_idle, MASTER_RANK, STATUS_CHANNEL);
			bool all_idle = recv_data<bool>(MASTER_RANK, STATUS_CHANNEL); // 从 Master 中接收所有 worker 的工作状态
			if(all_idle) global_end_label = true;
		}
		else
		{
            // Master Worker 负责接收所有工作 worker 的状态
			bool all_idle = worker_idle;
			for(int i=0; i<_num_workers; i++)
			{
				if(i != MASTER_RANK) all_idle = (recv_data<bool>(i, STATUS_CHANNEL) && all_idle);
			}
			if(all_idle) global_end_label = true; // 只有当所有 worker 的工作状态为 true 时，all_idle 才为 true，即此时所有 worker 都已经工作结束
			for(int i=0; i<_num_workers; i++)
			{
                // 将所有 worker 的工作状态 all_idle 发给所有 worker
				if(i != MASTER_RANK) send_data(all_idle, i, STATUS_CHANNEL);
			}
		}
	}

	//=======================================================
    /**
     * 获取当前 worker 中剩余的任务数量，剩余数量 = 本地顶点列表中剩余的顶点数量 + 文件中的顶点数量
     */
	//task stealing
	size_t get_remaining_task_num()
	//not counting number of active tasks in memory (for simplicity)
	{
		global_vertex_pos_lock.lock();
		int table_remain = local_table.size() - global_vertex_pos; // 本地顶点列表中的剩余的顶点数量
		global_vertex_pos_lock.unlock();
		return table_remain + global_file_num * TASK_BATCH_NUM;
	}

    /**
     * 工作窃取计划 src_rank --任务--> dst_rank
     */
	struct steal_plan
	{
		int src_rank; // 被窃取任务的 worker
		int dst_rank; // 窃取任务的 worker
	};

    /**
     * 最大堆中的元素（保存 worker 的 id 和剩余任务数量），最大堆按照 worker 中剩余的任务数量确定元素大小
     */
	struct max_heap_entry
	{
		size_t num_remain; // worker 中剩余的任务数量
		int rank; // worker 的 id

		bool operator<(const max_heap_entry& o) const
		{
			return num_remain < o.num_remain;
		}
	};
    
    /**
     * 与 max_heap_entry 含义一样，只是元素顺序的确定方式相反
     */
	struct min_heap_entry
	{
		size_t num_remain;
		int rank;

		bool operator<(const min_heap_entry& o) const
		{
			return num_remain > o.num_remain;
		}
	};

	//UDF for stealing seed tasks
	virtual void task_spawn(VertexT * v, vector<TaskT> & tvec) = 0;

	/*//=== deprecated, 50 vertices may just spawn 0 task or 2 tasks (etc.), so the quota of 50 is wasted during plan generation
	//get tasks from local-table
	//returns false if local-table is exhausted
	bool locTable2vec(vector<TaskT> & tvec)
	{
		size_t begin, end; //[begin, end) are the assigned vertices (their positions in local-table)
		//note that "end" is exclusive
		int size = local_table.size();
		//======== critical section on "global_vertex_pos"
		global_vertex_pos_lock.lock();
		if(global_vertex_pos < size)
		{
			begin = global_vertex_pos; //starting element
			end = begin + TASK_BATCH_NUM;
			if(end > size) end = size;
			global_vertex_pos = end; //next position to spawn
		}
		else begin = -1; //meaning that local-table is exhausted
		global_vertex_pos_lock.unlock();
		//======== spawn tasks from local-table[begin, end)
		if(begin == -1) return false;
		else
		{
			VertexVec & gb_vertexes = *(VertexVec*) global_vertexes;
			for(int i=begin; i<end; i++)
			{//call UDF to spawn tasks
				task_spawn(gb_vertexes[i], tvec);
			}
			return true;
		}
	}
	*/

    /**
     * 从 worker 的本地顶点中获取任务，如果本地顶点列表为空，则返回 false
     */
	//get tasks from local-table
	//returns false if local-table is exhausted
	bool locTable2vec(vector<TaskT> & tvec)
	{
		size_t begin, end; //[begin, end) are the assigned vertices (their positions in local-table)
		//note that "end" is exclusive
		int size = local_table.size();
		//======== critical section on "global_vertex_pos"
		while(tvec.size() < TASK_BATCH_NUM)
		{
			global_vertex_pos_lock.lock();
			if(global_vertex_pos < size)
			{
				begin = global_vertex_pos; //starting element
				end = begin + MINI_BATCH_NUM;
				if(end > size) end = size;
				global_vertex_pos = end; //next position to spawn
			}
			else begin = -1; //meaning that local-table is exhausted 本地顶点已经用完
			global_vertex_pos_lock.unlock();
			//======== spawn tasks from local-table[begin, end)
			if(begin == -1) return false; // 本地顶点已经用完，返回 false
			else
			{
                // 从当前 worker 的顶点列表中取出 [begin, end) 区间的顶点
				VertexVec & gb_vertexes = *(VertexVec*) global_vertexes; // 获取当前 worker 的顶点列表
				for(int i=begin; i<end; i++)
				{//call UDF to spawn tasks
					task_spawn(gb_vertexes[i], tvec); // 通过顶点生成的任务存储在 tvec 列表中
				}
			}
		}
		return true;
	}

    /**
     * 从文件中获取任务，如果文件列表为空，则返回 false，否则返回 true
     */
	//get tasks from disk files
	//returns false if "global_file_list" is empty
	bool file2vec(vector<TaskT> & tvec)
	{
		string file;
		bool succ = global_file_list.dequeue(file);
		if(!succ) return false; //"global_file_list" is empty
		else
		{
			global_file_num --;
			ofbinstream in(file.c_str());
			TaskT dummy;
			while(!in.eof())
			{
				TaskT task;
                global_load_num -= task.subG.roots.size();

				tvec.push_back(dummy);
				in >> tvec.back();
			}
			in.close();
			//------
			if (remove(file.c_str()) != 0) {
				cout<<"Error removing file: "<<file<<endl;
				perror("Error printed by perror");
			}
			return true;
		}
	}

	//=== for handling task streaming on disk ===
	char fname[1000], num[20];
	long long fileSeqNo = 1;
	void set_fname() //will proceed file seq #
	{
        // fname 格式：任务文件目录/worker 的 id _ comper数量 _ 文件号
		strcpy(fname, TASK_DISK_BUFFER_DIR.c_str());
		sprintf(num, "/%d_", _my_rank);
		strcat(fname, num);
		sprintf(num, "%d_", num_compers); //compers have rank 0, 1, ... comper_num-1; so there's no conflict
		strcat(fname, num);
		sprintf(num, "%lld", fileSeqNo);
		strcat(fname, num);
		fileSeqNo++;
	}

    /**
     * 确定是否需要工作窃取，如果当前 worker 需要从其它 worker 中窃取任务，则返回 true；否则返回 false。
     *
     * 工作窃取的思路：根据 worker 剩余的任务数量，将 worker 放进最大堆或最小堆。
     * 如果 worker 剩余的任务数量大于阈值，则放进最大堆（此时，该 worker 负载较重）；否则，放进最小堆（此时，该 worker 负载较轻）。
     * 然后不断循环地从最大堆、最小堆中取出堆顶元素，然后将负载最重的 worker （最大堆的堆顶元素）的负载转移给负载最轻的 worker（最小堆的堆顶元素）。
     * 直到负载最重、最轻的两个 worker 剩余的任务数量相差较小，此时可以认为所有 worker 的负载较均衡，因此停止工作窃取。
     **/
	bool steal_planning() //whether there's something to steal from/to others
	{
		vector<int> my_steal_list; // 当前 worker 的任务窃取列表，存储的是 worker 的 id。一个 worker 如果需要任务窃取，那么其只可能是任务窃取者(dst)、任务被窃取者(src)的一个，不可能既是任务窃取者又是任务被窃取者。因此 list 里面存储的 worker 的 id 符号全部一样，不能既有正又有负
		//====== set my_steal_list
		if(_my_rank != MASTER_RANK)
		{
            // slave worker 将自己剩余的任务数量发送给 master worker
			send_data(get_remaining_task_num(), MASTER_RANK, STATUS_CHANNEL);
			recv_data<vector<int> >(MASTER_RANK, STATUS_CHANNEL, my_steal_list);
		}
		else
		{
            // master worker 收集 slave worker 发送来的消息，即收集各个 slave worker 剩余的任务数量
			//collect remaining workloads
			vector<size_t> remain_vec(_num_workers);
			for(int i=0; i<_num_workers; i++)
			{
				if(i != MASTER_RANK) remain_vec[i] = recv_data<size_t>(i, STATUS_CHANNEL); // 各个 slave worker 剩余的任务数量
				else remain_vec[i] = get_remaining_task_num();
			}
			//------ 依据阈值 MIN_TASK_NUM_BEFORE_STEALING 将所有 worker 划分到最小堆与最大堆中，最小堆中的 worker 负载相对较轻，最大堆中的 worker 负载相对较重
			priority_queue<max_heap_entry> max_heap; // 最大堆
			priority_queue<min_heap_entry> min_heap; // 最小堆
			for(int i=0; i<_num_workers; i++)
			{
                // MIN_TASK_NUM_BEFORE_STEALING = 10*TASK_BATCH_NUM
				if(remain_vec[i] > MIN_TASK_NUM_BEFORE_STEALING) // 如果 i 号 worker 剩余的任务数量大于阈值（负载较重），则将该 i 号 worker 放入最大堆中
				{
					max_heap_entry en;
					en.num_remain = remain_vec[i];
					en.rank = i;
					max_heap.push(en);
				}
				else if(remain_vec[i] < MIN_TASK_NUM_BEFORE_STEALING) // 如果 i 号 worker 剩余的任务数量小于阈值（负载较轻），则将其放入最小堆中
				{
					min_heap_entry en;
					en.num_remain = remain_vec[i];
					en.rank = i;
					min_heap.push(en);
				}
			}
			//------
			//plan generation
			vector<int> steal_num(_num_workers, 0); //each element should not exceed MAX_STEAL_TASK_NUM 存储每个 worker 窃取的任务总数，但是不能超过 MAX_STEAL_TASK_NUM
			vector<steal_plan> plans; // 任务窃取计划，只存储窃取任务 worker 、被窃取任务 worker 的 id 
			while(!max_heap.empty() && !min_heap.empty()) // 只有当最大堆和最小堆中都有 worker 时才需要通过工作窃取来实现负载均衡（如果有一个为空，则说明所有的 worker 负载都较重或者较轻）
			{
				max_heap_entry max = max_heap.top(); // 取出负载最重的 worker， max.num_remain > 10*TASK_BATCH_NUM
				max_heap.pop();
				min_heap_entry min = min_heap.top(); // 取出负载最轻的 worker，min.num_remain < 10*TASK_BATCH_NUM
				min_heap.pop();
				if(max.num_remain - TASK_BATCH_NUM < min.num_remain) break; // worker 之间负载相差较小，负载相对均衡，无需进行工作窃取。当 max.num_remain - min.num_remain < TASK_BATCH_NUM 时，负载最重的 worker 与负载最轻的 worker 两者之间的负载相差并不大，此时各个 worker 之间的负载相差较小，没有必要进行工作窃取
				else
				{   // worker 之间的负载相差较大，此时需要通过工作窃取实现负载均衡。每次工作窃取，是直接窃取数量为 TASK_BATCH_NUM 的一批任务
					max.num_remain -= TASK_BATCH_NUM; // 负载较重的 worker 被负载轻的 worker 窃取 TASK_BATCH_NUM 个任务
					min.num_remain += TASK_BATCH_NUM;
					steal_num[min.rank] += TASK_BATCH_NUM; // 记录 min 号 worker 窃取任务的总数量 
					
                    //--- 生成工作窃取计划
					steal_plan plan;
					plan.src_rank = max.rank; // 被窃取任务的 worker
					plan.dst_rank = min.rank; // 窃取任务的 worker
					plans.push_back(plan);
					//---
					if(max.num_remain > MIN_TASK_NUM_BEFORE_STEALING) max_heap.push(max); // 刚被窃取任务的 max 号 worker 剩余的任务数量依然大于阈值，则依然还需要其它 worker 窃取其任务，因此该 worker 依然加入到最大堆中
					// MAX_STEAL_TASK_NUM 的默认值与 MIN_TASK_NUM_BEFORE_STEALING 的默认值相同，均为 10*TASK_BATCH_NUM
                    if(steal_num[min.rank] + TASK_BATCH_NUM <= MAX_STEAL_TASK_NUM &&
							min.num_remain < MIN_TASK_NUM_BEFORE_STEALING) // 刚窃取任务的 min 号 worker 剩余的任务数量小于阈值而且还能继续窃取任务，则加入到最小堆；否则说明该 worker 已经窃取到足够的任务数量，无需再进行窃取
						min_heap.push(min);
				}
			}
			//------
			if(plans.size() > 0) cout<<plans.size()<<" stealing plans generated at the master"<<endl;//@@@@@@
			//calculating stealing tasks
			//a negative tag (-x-1) means receiving 根据正负号来区分是任务的窃取者还是被窃取者，负号是被窃取者，正号是窃取者
			vector<vector<int> > steal_lists(_num_workers); //steal_list[i] = stealing tasks 任务窃取列表
			for(int i=0; i<plans.size(); i++)
			{
				steal_plan & plan = plans[i];
				steal_lists[plan.dst_rank].push_back(-plan.src_rank-1); // 窃取任务的 worker 存储被窃取任务的 worker 的 id 值
				steal_lists[plan.src_rank].push_back(plan.dst_rank); // 被窃取任务的 worker 存储窃取任务的 worker 的 id 值
			}
			//------
			//distribute the plans to machines 各个 worker 的任务窃取列表初始化完毕后，master 将其发送给相应的 worker
			for(int i=0; i<_num_workers; i++)
			{
				if(i == _my_rank) steal_lists[i].swap(my_steal_list); // 如果是当前 worker（master） ，则 swap ，从而将当前 worker 的任务窃取列表保存下来
				else
				{
					send_data(steal_lists[i], i, STATUS_CHANNEL); // 将其它 worker 的任务窃取列表发送给相应的 worker
				}
			}
		}
		//====== execute my_steal_list
		if(my_steal_list.size() == 0) return false; // 不需要窃取任务

#if TASK_SCHEDULE_TYPE == 0
        // 原始的基础调度
		for(int i=0; i<my_steal_list.size(); i++)
		{
			int other = my_steal_list[i];
			if(other < 0)
			{   // 从被窃取的 worker 中接收窃取的任务
				vector<TaskT> tvec; // 窃取的任务
				recv_data<vector<TaskT> >(-other-1, STATUS_CHANNEL, tvec);
                // 判断是否真正需要处理窃取的任务。如果原来被窃取任务的 worker 在生成工作窃取计划时完成了一部分任务，那么其任务数量会减少，负载减轻，
                // 可能就不再需要将任务调度给其它 worker。像这种情况下，接收到的 vector 为空
				if(tvec.size() > 0) // 大于 0 则说明需要进行任务窃取
				{
					set_fname();
					ifbinstream out(fname);
					//------ 将窃取的任务列表序列化保存到文件中
					for(int i=0; i<tvec.size(); i++)
					{
						out << tvec[i];
					}
					out.close();
					num_stolen += tvec.size();
					//------
					//register with "global_file_list"
					global_file_list.enqueue(fname);
					global_file_num ++;
				}
			}
			else
			{
				vector<TaskT> tvec; // 当前 worker 被窃取的任务列表
                // 在生成工作窃取计划时，当前 worker 完成了一部分任务，那么其任务数量会减少，负载减轻，因此需要再次判断其剩余的任务数量
                // 如果剩余的任务数量依然大于阈值，则需要进行任务窃取
				if(get_remaining_task_num() > MIN_TASK_NUM_BEFORE_STEALING)
				//check this since time has passed, and more tasks may have been processed
				//send empty task-vec if no longer a task heavy-hitter
					if(locTable2vec(tvec) == false) file2vec(tvec); // 先从本地顶点中获取任务，如果从本地无法生成任务，则从文件中生成任务
				send_data(tvec, other, STATUS_CHANNEL); //send even if it's empty
			}
		}
#elif TASK_SCHEDULE_TYPE == 1
        // 使用 LSH 调度
        MinHash minhash(1);
        // 确定窃取者（即空闲的 Worker）和被窃取者（即较忙的 Worker）
        hash_set<int> idle_worker_set;
        hash_set<int> busy_worker_set;
        for(int i=0; i<my_steal_list.size(); i++)
        {
            int other = my_steal_list[i];
            if (other < 0) 
                busy_worker_set.insert(other);
            else
                idle_worker_set.insert(other);

        }
        int idle_worker_num = idle_worker_set.size();
        
        // 生成窃取的任务
        vector<vector<TaskT>> stealed_task(idle_worker_num); // 存储生成的窃取任务
		for(int i=0; i<my_steal_list.size(); i++)
		{
			int other = my_steal_list[i];
            if (other >= 0)
			{
				vector<TaskT> tvec; // 当前 worker 被窃取的任务列表
                // 在生成工作窃取计划时，当前 worker 完成了一部分任务，那么其任务数量会减少，负载减轻，因此需要再次判断其剩余的任务数量
                // 如果剩余的任务数量依然大于阈值，则需要进行任务窃取
				if(get_remaining_task_num() > MIN_TASK_NUM_BEFORE_STEALING)
				//check this since time has passed, and more tasks may have been processed
				//send empty task-vec if no longer a task heavy-hitter
					if(locTable2vec(tvec) == false) file2vec(tvec); // 先从本地顶点中获取任务，如果从本地无法生成任务，则从文件中生成任务
			
                // 对生成的窃取任务进行一次 LSH 划分，确定其要分配到的 Worker
                for (const auto &t : tvec)
                {
                    size_t sig = minhash.hash(t.to_pull);
                    stealed_task[sig % idle_worker_num].push_back(t);
                }
            }
		}

        // 接收窃取的任务
        for (const auto &other : busy_worker_set)
        {
            // 从被窃取的 worker 中接收窃取的任务
            vector<TaskT> tvec; // 窃取的任务
            recv_data<vector<TaskT> >(-other-1, STATUS_CHANNEL, tvec);
            // 判断是否真正需要处理窃取的任务。如果原来被窃取任务的 worker 在生成工作窃取计划时完成了一部分任务，那么其任务数量会减少，负载减轻，
            // 可能就不再需要将任务调度给其它 worker。像这种情况下，接收到的 vector 为空
            if(tvec.size() > 0) // 大于 0 则说明需要进行任务窃取
            {
                set_fname();
                ifbinstream out(fname);
                //------ 将窃取的任务列表序列化保存到文件中
                for(int i=0; i<tvec.size(); i++)
                {
                    out << tvec[i];
                }
                out.close();
                num_stolen += tvec.size();
                //------
                //register with "global_file_list"
                global_file_list.enqueue(fname);
                global_file_num ++;
            }
        }

        // 发送窃取的任务
        int i = 0;
        for (const auto &other : idle_worker_set)
		{
            vector<TaskT> &tvec = stealed_task[i++]; // 当前 worker 被窃取的任务列表
            send_data(tvec, other, STATUS_CHANNEL); //send even if it's empty
		}
#endif

		return true;
	}

	//=======================================================
	//program entry point
    void run(const WorkerParams& params)
    {
        //check path + init
        if (_my_rank == MASTER_RANK)
        {
            if (dirCheck(params.input_path.c_str()) == -1)
                return;
        }
        init_timers();

		//dispatch splits
		ResetTimer(WORKER_TIMER);

		// 初始化顶点查询顺序（在导入图数据之前导入查询图数据）
        // 导入查询图数据
		if (_my_rank == MASTER_RANK)
		{
		    load_query_graph(params.query_graph_path.c_str(), query_vertexes);
		}

		// LOG_WARN("同步查询图前，查询图顶点数量：%d\n", (int)query_vertexes.size());
        broadcast_data(query_vertexes);
		// LOG_WARN("同步查询图后，查询图顶点数量：%d\n", (int)query_vertexes.size());
        set_query_graph_data(query_vertexes);
        
		// 遍历查询图中所有的查询点，获取查询点的标签集合，从而在导入数据时剪枝
        for (int i = 0; i < query_vertexes.size(); i++)
        {	
            for (int j = 0; j < query_vertexes[i].size(); j++)		
            {
                query_vertex_label.insert(query_vertexes[i][j]->value.l);
            }   
        }

		vector<vector<string> >* arrangement;
		if (_my_rank == MASTER_RANK) {
			arrangement = params.native_dispatcher ? dispatchLocality(params.input_path.c_str()) : dispatchRan(params.input_path.c_str());
			//reportAssignment(arrangement);//DEBUG !!!!!!!!!!
			masterScatter(*arrangement);
			vector<string>& assignedSplits = (*arrangement)[0];
			//reading assigned splits (map)
            // 在读取数据时，不同 worker 负责读取不同区域的数据
            // assignedSplits 是当前 worker 需要负责读取的部分数据
			for (vector<string>::iterator it = assignedSplits.begin();
				 it != assignedSplits.end(); it++)
                // 正式导入图。在 load_graph 函数内部，先调用 Worker 的 toVertex 函数（需要重写）将输入数据转换成顶点，如果设置了 trimer 则还会进行剪枝
                load_graph(it->c_str(), vertexes); 
			delete arrangement;
		} else {
			vector<string> assignedSplits;
			slaveScatter(assignedSplits);
			//reading assigned splits (map)
			for (vector<string>::iterator it = assignedSplits.begin();
				 it != assignedSplits.end(); it++)
				load_graph(it->c_str(), vertexes);
		}
       
        // 各个 worker 读完数据后再对数据进行分区
		//send vertices according to hash_id (reduce)
        // vertexes 是当前 worker 读取的顶点，因为读取到的顶点不一定是分区到当前 worker ，因此通过 sync_graph 操作将顶点进行分区并发送给相应的分区（即 worker）
		sync_graph(vertexes); // 作用，数据归位，将根据顶点 id 的哈希值进行分区，然后将顶点数据发给其真正应该所处的 worker
        broadcast_data(data_vertex_label_frequency);

		//init global_vertex_pos
		global_vertex_pos = 0;

		//use "vertexes" to set local_table
		set_local_table(vertexes); // 将顶点向量转换成顶点 Map

		//barrier for data loading
		worker_barrier();
		StopTimer(WORKER_TIMER);
		PrintTimer("Load Time", WORKER_TIMER);

		//ReqQueue already set, by Worker::cache_table
		//>> by this time, ReqQueue occupies about 0.3% CPU

		ResetTimer(WORKER_TIMER); // 开始计时
		//call status_sync() periodically
        // 遍历查询图列表，逐个生成查询计划
        hash_map<KeyT, EdgeVector> &edge = *(hash_map<KeyT, EdgeVector>*)global_same_layer_edge;
        hash_map<Label, vector<QueryGroup*>> &label_query_graph_group = *(hash_map<Label, vector<QueryGroup*>>*)global_label_query_graph_group; // 查询图分组，key：查询组的标签，value：对应标签的查询组，用于加速任务的生成
        hash_map<KeyT, QueryGroup*> &lsh_query_graph_group = *(hash_map<KeyT, QueryGroup*>*)global_lsh_query_graph_group; // LSH 查询图分组，key：分组 id，value：查询组
        hash_map<KeyT, QueryGroup*> &lsh_query_graph_group_table = *(hash_map<KeyT, QueryGroup*>*)global_lsh_query_graph_group_table; // 查询图分组表，key：查询图 id，value：查询组，用于记录每个查询图对应的分组
        int group_id = 0;
        QueryGroup group; // 查询分组
        MinHash minhash(1);
        for (int i = 0; i < query_vertexes.size(); i++)
        {
            QueryVertexVec order_list; // 查询顶点顺序
            QueryPlanVertexVec plan_order_list; // 查询计划顶点顺序（二维数组）
            QueryPlanVertexVecList plan_list; // 查询计划顶点列表（一维数组）
			vector<int> query_vertex_tag_list;
            vector<query_judge> query_combine_tag;
            EdgeVector edge_vec; // 同层边
			bool query_last;

			generateQueryPlan(query_graph_table, query_vertexes[i], order_list, 
                    plan_order_list, plan_list, *(hash_map<KeyT, QueryPlanVertex>*)global_query_plan_vertex_table,
                    query_vertex_tag_list, query_last, edge_vec); // 生成查询计划


            KeyT query_graph_id = plan_order_list[0][0].id; // 将查询计划的根顶点作为查询图的唯一标记
            order_list_table.insert(make_pair(query_graph_id, order_list));
            plan_order_list_table.insert(make_pair(query_graph_id, plan_order_list));

			query_vertex_tag_table.insert(make_pair(query_graph_id,query_vertex_tag_list));
			query_last_tabel.insert(make_pair(query_graph_id,query_last));

            plan_list_table.insert(make_pair(query_graph_id, plan_list));
			
            query_combine_tag_table.insert(make_pair(query_graph_id, query_combine_tag));
            edge[query_graph_id] = edge_vec;

#if ENABLE_QUERY_GROUP_COMBINE == 0
            // 不进行查询组内的查询图合并（即相当于把每个查询图作为一个查询组）
            ++group_id;
            QueryGroup* group_ptr = new QueryGroup;
            group_ptr->group_id = group_id;
            group_ptr->query_vertexs.push_back(query_graph_id);
            label_query_graph_group[plan_order_list[0][0].l].push_back(group_ptr);
            lsh_query_graph_group_table[query_graph_id] = group_ptr;
            lsh_query_graph_group[group_id] = group_ptr;
#elif ENABLE_QUERY_GROUP_COMBINE == 1
            // 判断是否有非树边，如果有非树边，则此查询图不利用 LSH 进行分组合并
            // if (edge_vec[1].size() > 0)
            // {
            //     ++group_id;
            //     QueryGroup* group_ptr = new QueryGroup;
            //     group_ptr->group_id = group_id;
            //     group_ptr->query_vertexs.push_back(query_graph_id);
            //     lsh_query_graph_group_table[query_graph_id] = group_ptr;
            //     lsh_query_graph_group[group_id] = group_ptr;
            //     continue;
            // }
            
            // LSH 分组
            vector<Label> query_plan_labels; // 查询计划的标签顺序
            for (const QueryPlanVertex& qpv : plan_order_list[1])
            // for (const QueryPlanVertex& qpv : plan_list)
            {
                query_plan_labels.push_back(qpv.l);
            }
            vector<KeyT> sig = minhash.signature(query_plan_labels);
            size_t pos = query_graph_group_lsh.get_bucket_by_signature(sig);
            hash_map<string, QueryGroup*> &lsh_bucket = query_graph_group_lsh.pos(pos).get_map();
            string label_str; // 查询图根顶点的标签，如果查询图含有非树边，则含有前缀 "-"；否则，无该前缀
            // 根据当前查询图是否有非树边来取出相应的查询组
            if (edge_vec[1].size() > 0)
                label_str = "-" + to_string(plan_order_list[0][0].l);
            else
                label_str = to_string(plan_order_list[0][0].l);

            auto it = lsh_bucket.find(label_str);
            if (it == lsh_bucket.end())
            {   // 新的查询分组
                ++group_id;
                QueryGroup* group_ptr = new QueryGroup;
                lsh_bucket[label_str] = group_ptr;
                group_ptr->group_id = group_id;
                group_ptr->query_vertexs.push_back(query_graph_id);
                label_query_graph_group[plan_order_list[0][0].l].push_back(group_ptr);
                lsh_query_graph_group_table[query_graph_id] = group_ptr;
                lsh_query_graph_group[group_id] = group_ptr;
            }
            else
            {   // 已有的查询分组
                it->second->query_vertexs.push_back(query_graph_id);
                lsh_query_graph_group_table[query_graph_id] = it->second;
            }
#endif
            // cout << "查询图；(" << plan_order_list[0][0].id << ", " << plan_order_list[0][0].l << ") pos: " << pos << endl;
        }

#if ENABLE_QUERY_GROUP_COMBINE == 1
        detect_common_subgraph(lsh_query_graph_group); // 开启组内的查询图合并之后，才需要检测公共子图
#endif

        // 创建并启动一个 ReqServer 线程，负责接收请求和处理请求
		//set up ReqServer (containing RespQueue), let it know local_table for responding reqs
		ReqServer<VertexT> server_req(local_table);

		//set up computing threads
		create_compers(); //side effect: set global_comper_vec

        // 创建并启动一个 RespServer 线程，负责接收其它线程的响应
		//set up RespServer, let it know cache_table so that it can update it when getting resps
		RespServer<Comper> server_resp(*cache_table); //it would read global_comper_vec

		//set up vcache GC
		GCT gc(*cache_table);

		//set up AggSync 聚合器线程
		AggSync<AggregatorT> * agg_thread; //the thread that runs agg_sync()
		if(global_aggregator != NULL) agg_thread = new AggSync<AggregatorT>;

		Profiler* profiler = new Profiler;
		if (_my_rank == MASTER_RANK)
        {
            cout << "running" << endl;
        }

		while(global_end_label == false)
		{
			clock_t last_tick = clock();
			bool sth2steal = steal_planning();
            status_sync(sth2steal); // 在 worker 之间同步 global_end_label 状态，从而确定是否需要下一轮迭代
            //------
            //reset idle status of Worker, compers will add back if idle
            mtx_go.lock();
            global_inner_work_steal_label = (global_num_idle >= 1); // 标记 Worker 内部是否需要工作窃取
            for(int i=0; i<num_compers; i++) idle_set[i] = false; // 在 Comper 中设置为 true，在 worker 中设置为 false
            global_num_idle = 0;
            cv_go.notify_all(); //release threads to compute tasks
            mtx_go.unlock();
            usleep(STATUS_SYNC_TIME_GAP);
		}

		StopTimer(WORKER_TIMER);
		PrintTimer("Task Time", WORKER_TIMER);
        // PrintTimer("Communication Time", COMMUNICATION_TIMER);
        // PrintTimer("Serialization Time", SERIALIZATION_TIMER);
        // PrintTimer("Transfer Time", TRANSFER_TIMER);


        // 释放分组内存
        for (const auto &item: lsh_query_graph_group)
        {
            QueryGroup* group_ptr = item.second;
            if (group_ptr != NULL)
            {
                delete group_ptr;
                group_ptr = NULL;
            }
        }

        // 处理完所有计算任务后，结束迭代计算。如果设置了聚合器，则释放聚合器的内存（调用聚合器的析构方法，在析构方法中会调用 agg_sync，从而真正聚合数据）
		if(global_aggregator != NULL) delete agg_thread; //make sure destructor of agg_thread is called to do agg_sync() before exiting run()
		delete profiler;
    }
};

#endif
