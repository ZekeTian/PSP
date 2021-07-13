#ifndef GMATCHTCOMPER_H_
#define GMATCHTCOMPER_H_

#include "subg-dev.h"
#include "GMatchAgg.h"
#include "graph_matching.h"
#include "util/MemoryUtil.h"

class GMatchComper : public Comper<GMatchTask, GMatchAgg>
{
public:
    inline int max(int a, int b)
    {
        return a > b ? a : b;
    }

    inline int min(int a, int b)
    {
        return a < b ? a : b;
    }

    /**
     * 判断种子顶点 v 是否可以与已有 task 合并，如果可以，则返回能够合并的 task；否则，返回 NULL。
     * 
     * @param   query_graph_id，查询图 id
     * @param   v，种子顶点
     * @param   next_level_plan_vertex_map，下一层查询计划点对应的 map。key：查询点的标签值，value：该类标签对应的 benefit 值
     * 
     * @return  如果 v 可以与已有 task 合并，则返回能够合并的 task；否则，返回 NULL。
     */
    GMatchTask * can_combine(VertexID query_graph_id, VertexT * v, hash_map<Label, int> &next_level_plan_vertex_map)
    {
        hash_set<VertexID> nbs_set;
        int total_benefit = 0; // 种子顶点 v 的邻居点的总收益值
        vector<AdjItem> &nbs = v->value.adj;
        for (const auto &adj : nbs)
        {
            if (next_level_plan_vertex_map.find(adj.l) != next_level_plan_vertex_map.end())
            {
                nbs_set.insert(adj.id);
                total_benefit += next_level_plan_vertex_map[adj.l];
            }
        }

        int common_benefit = 0; // 两个种子顶点的共同邻居的收益值
        // for (const auto &t : generated_task_set)
        // {
        //     if (t->subG.query_vertexs[0] != query_graph_id)
        //     {
        //         continue; // 当前 task 无法匹配到 query_graph_id 查询图，则不需要判断是否需要合并
        //     }

        //     VertexT &root = t->subG.roots[0];
            
        //     // 将 v 的邻居集与 root 的邻居集作交集，根据交集结果数量判断是否合并
        //     common_benefit = 0;
        //     vector<AdjItem> &root_nbs = root.value.adj;
        //     for (const auto &adj : root_nbs)
        //     {
        //         if (nbs_set.find(adj.id) != nbs_set.end())
        //         {
        //             common_benefit += next_level_plan_vertex_map[adj.l];
        //         }
        //     }

        //     // 共同邻居的收益值超过一半，则可以任务合并, 暂定阈值为 0.5，可以根据实际调整
        //     // if (common_benefit > 1 * total_benefit) // 不合并 task
        //     if (common_benefit > 0.5 * total_benefit) // 合并 task
        //     {
        //         return t;
        //     }
        // }

        return NULL;
    }

    virtual void task_spawn(VertexT *v)
    {   // 获取根顶点，判断当前顶点 v 能否成为根顶点的候选点
        const hash_map<VertexID, QueryPlanVertexVec> &plan_order_map = *(hash_map<VertexID, QueryPlanVertexVec> *)global_query_plan_vertex_order;
        const hash_map<VertexID, VertexT*> &query_graph_table = *(hash_map<VertexID, VertexT*> *)global_query_graph_table; // 查询图顶点表
        const hash_map<VertexID, QueryPlanVertex> &query_plan_vertex_table = *(hash_map<VertexID, QueryPlanVertex>*)global_query_plan_vertex_table; // 查询计划点表
        const hash_map<Label, vector<QueryGroup*>> &label_query_graph_group = *(hash_map<Label, vector<QueryGroup*>>*)global_label_query_graph_group; // 查询图分组，key：查询组的标签，value：对应标签的查询组，用于加速任务的生成

        if (label_query_graph_group.find(v->value.l) == label_query_graph_group.end())
        {
            return;
        }

        // 遍历 v 标签对应的查询组中的所有查询图
        const vector<QueryGroup*> &query_group_vec = label_query_graph_group.find(v->value.l)->second;
        for (const QueryGroup* group : query_group_vec)
        {
            const vector<VertexID>& query_vertexs = group->query_vertexs;
            // 判断当前组查询图的标签是否符合条件
            const VertexT* root = query_graph_table.find(query_vertexs[0])->second;
            // if (v->value.l != root->value.l)
            //     continue; // 数据点 v 与当前组查询图的标签不一致，则继续下一组

            // 以分组为单位生成任务
            hash_set<VertexID> query_graph_id_set; // 当前数据点 v 能够匹配到的查询图根顶点 id
            int max_step = 0;
            // 遍历查询组内所有的查询图，逐个判断是否可以生成 task
            for (const auto& qv : query_vertexs)
            {
                const VertexT &root = *(query_graph_table.find(qv)->second);
                if (v->value.l == root.value.l 
                    && v->value.adj.size() >= root.value.adj.size()) // LDF 过滤
                {
                    if (nlf_filter(v, root)) // NLF 过滤
                    {
                        query_graph_id_set.insert(root.id);
                        if (plan_order_map.find(root.id)->second.size() > max_step)
                        {
                            max_step = plan_order_map.find(root.id)->second.size();
                        }
                    }
                }
            }

            if (query_graph_id_set.empty())
                continue;

            // 获取各个查询图根顶点的子查询点标签，从而便于拉取顶点
            hash_map<Label, bool> child_query_vertex; // key：子查询点标签，value：标记子查询点是否需要拉取
            vector<const GMatchVertex*> only_parent_query_vertex;
            for (const auto& root_id : query_graph_id_set)
            {
                get_child_query_vertex(query_plan_vertex_table.find(root_id)->second, child_query_vertex, 
                                        query_graph_table, query_plan_vertex_table, only_parent_query_vertex);
            }

            // 判断该 bucket 中是否含有任务，如果含有任务，则与其合并，不需要单独生成任务
            GMatchTask *t = NULL;
            bool flag_combine = false; // 标记是否可以合并，true 表示可以合并；false 表示不可以合并。
            
            // 遍历邻接表，确定需要拉取的邻居点
            vector<AdjItem> &nbs = v->value.adj;

#if ENABLE_QUERY_GRAPH_COMBINE == 1
            // 进行查询图内部的任务合并
            vector<VertexID> next_level_data_vertex; // 匹配下一层所需的数据点 id
            for (const auto &adj_v : nbs)
            {
                // 判断当前顶点是否需要拉取
                hash_map<Label, bool>::iterator child_query_vertex_it = child_query_vertex.find(adj_v.l);
                if (child_query_vertex_it != child_query_vertex.end())
                {
                    next_level_data_vertex.push_back(adj_v.id);
                }
            }
            // 根据该 task 拉取的顶点生成相应的签名信息，从而获取 bucket
            vector<VertexID> sig = minhash.signature(next_level_data_vertex);
            size_t pos = generated_task_map.get_bucket_by_signature(sig);
            hash_map<KeyT, GMatchTask *> &lsh_bucket = generated_task_map.pos(pos).get_map();
            auto lsh_it = lsh_bucket.find(group->group_id);

            if (lsh_it == lsh_bucket.end())
            {
                flag_combine = false;
            }
            else
            {   
                t = lsh_it->second;
                flag_combine = true;
                // flag_combine = (t->subG.query_vertexs[0] == root_id);
            }
#endif
            if (!flag_combine)
            {   // 种子顶点 v 不能与已有 task 合并，则需要生成新的 task
                t = new GMatchTask;
                t->subG.max_step = 0;
                // 新 task ，直接插入 query_graph_id_set 的元素
                t->subG.query_vertexs.insert(t->subG.query_vertexs.end(), query_graph_id_set.begin(), query_graph_id_set.end());
            }
            else
            {   // 合并的 task ，则将新种子顶点能够匹配的查询图与原有的查询图进行合并
                query_graph_id_set.insert(t->subG.query_vertexs.begin(), t->subG.query_vertexs.end());
                t->subG.query_vertexs.assign(query_graph_id_set.begin(), query_graph_id_set.end());
            }

            // 将 v 加入到任务子图中，同时拉取相应的邻居点
            if (!t->subG.hasVertex(v->id))
            {
                t->subG.roots.push_back(*v); // 将根顶点添加到子图的根顶点列表中
                addNode(t->subG, *v); // 将根顶点添加到子图中
            }            

            t->subG.max_step = max(max_step, t->subG.max_step);            
            t->context = 1;       // context is step number
            t->group_id = group->group_id;

            // 拉取相应标签的顶点
            hash_map<VertexID, set<VertexID>> & candidate_vertexs = t->subG.candidate_vertexs;
            for (const auto &adj_v : nbs)
            {
                // 判断当前顶点是否需要拉取
                hash_map<Label, bool>::iterator child_query_vertex_it = child_query_vertex.find(adj_v.l);
                if (child_query_vertex_it != child_query_vertex.end())
                {   
                    // 判断当前顶点匹配到的查询点是否只有父节点
                    if (child_query_vertex_it->second)
                    {   // 如果只有父节点，则不需要拉取，直接加入到任务子图中
                        if (!t->subG.hasVertex(adj_v.id))
                        {
                            vector<VertexID> vec;
                            addNode(t->subG, adj_v.id, adj_v.l, vec);
                        }
                        // 添加边
                        addEdge_safe(t->subG, v->id, adj_v.id);
                        for (const auto &opqv : only_parent_query_vertex)
                        {
                            if (opqv->value.l == adj_v.l)
                                candidate_vertexs[opqv->id].insert(adj_v.id);
                        }
                    }
                    else
                    {   // 如果含有兄弟节点或子节点，则需要拉取
                        t->pull(adj_v.id);
                    }
                }
            }
#if ENABLE_QUERY_GRAPH_COMBINE == 0
            add_task(t); // 不进行查询图内的任务合并时，直接添加任务即可
#elif ENABLE_QUERY_GRAPH_COMBINE == 1
            // 进行查询图内的任务合并时，则需要判断是否需要合并
            if (!flag_combine)
            {   // 种子顶点 v 会生成新的 task，则添加该新的 task
                lsh_bucket.insert(make_pair(group->group_id, t));
                pair<KeyT, KeyT> t_key = make_pair(group->group_id, t->subG.vertexes[0].id);
                generated_task_map.insert_index(t_key, pos);
                add_task(t);
            }
#endif
        }
    }

    /**
     * 根据 LDF、NLF 对 data_vertex 进行过滤，判断其是否可能成为当前层查询点的一个候选点。
     * 如果其至少能成为一个查询点的候选点，则返回 true，并将其分类存入 vet_map 中；否则，返回 false。
     *
     * @param    data_vertex 	        数据图上的数据点，即待过滤确定的点
     * @param    current_level_vertex   查询图中当前层的查询点，即下次迭代待匹配的点
     * @param    vet_map                用于存储分类结果的 map，输出参数
     */
    void filter_vertex(VertexT *data_vertex, vector<QueryVertex> &current_level_vertex, 
                       hash_map<VertexID, vector<AdjItem>> &adj_map, const hash_set<Label> &query_vertex_label,
                       hash_map<VertexID, list<VertexT *>> &vet_map)
    {
        // 计算数据图、查询图中顶点的 NLF 信息
        hash_map<Label, int> data_nlf;
        vector<AdjItem> &data_adj = data_vertex->value.adj;
        vector<AdjItem> trimed_data_adj; // 剪枝后的数据点邻接表
        for (int i = 0;  i < data_adj.size(); ++i) 
        {
            data_nlf[data_adj[i].l]++; // 相应标签频率加 1
            if (query_vertex_label.find(data_adj[i].l) != query_vertex_label.end())
                trimed_data_adj.push_back(data_adj[i]);
        }

        hash_map<VertexID, list<VertexT *>>::iterator it;
        for (int i = 0; i < current_level_vertex.size(); i++)
        {
            QueryVertex &query_vertex = current_level_vertex[i];
            // 根据 LDF 进行过滤
            if (data_vertex->value.l == query_vertex.value.l 
                && data_vertex->value.adj.size() >= query_vertex.value.adj.size())
            {
                hash_map<Label, int> query_nlf;
                vector<AdjItem> &query_adj = query_vertex.value.adj;
                for (int i = 0;  i < query_adj.size(); ++i) 
                {
                    query_nlf[query_adj[i].l]++; // 相应标签频率加 1
                }

                // 根据 NLF 进行过滤
                if (nlf(data_nlf, query_nlf))
                {
                    if (adj_map.find(data_vertex->id) == adj_map.end())
                        adj_map.insert(make_pair(data_vertex->id, trimed_data_adj));

                    //                    cout << "data id: " << data_vertex->id << ", query id: " << query_vertex.id << endl;
                    it = vet_map.find(query_vertex.id);
                    if (it == vet_map.end())
                    {
                        list<VertexT *> vec;
                        vec.push_back(data_vertex);
                        vet_map.insert(make_pair(query_vertex.id, vec));
                    }
                    else 
                    {
                        it->second.push_back(data_vertex);
                    }
                }
            }
        }
    }

    void check_edge(list<VertexT *> &v1_candidate_list, list<VertexT *> &v2_candidate_list)
    {
        // 检查 v1
        hash_set<VertexID> v2_set;
        for (const auto &v2 : v2_candidate_list)
        {
            v2_set.insert(v2->id);
        }
        for (list<VertexT *>::iterator it = v1_candidate_list.begin(); it != v1_candidate_list.end(); /** 空 */)
        {
            // v1 的邻接表与 v2_candidate_list 作交集，如果为空，则从 v1_candidate_list 中删除 v1
            bool is_candidate = false;
            for (const auto &adj_v : (*it)->value.adj)
            {
                if (v2_set.find(adj_v.id) != v2_set.end())
                {
                    is_candidate = true; // v1 的邻接表与 v2_candidate_list 交集有结果，则当前 it 指向的顶点是 v1 的一个候���点
                    break;
                }
            }
            if (is_candidate)
            {   // it 指向的顶点是 v1 的一个候选点，则继续检查下一个 v1 的候选点
                it++; 
            }
            else
            {   // it 指向的顶点不是 v1 的一个候选点，则将其从 v1 的候选点列表删除
                it = v1_candidate_list.erase(it);
            }
        }

        // 检查 v2
        hash_set<VertexID> v1_set;
        for (const auto &v1 : v1_candidate_list)
        {
            v1_set.insert(v1->id);
        }
        for (list<VertexT *>::iterator it = v2_candidate_list.begin(); it != v2_candidate_list.end(); /** 空 */)
        {
            // v2 的邻接表与 v1_candidate_list 作交集，如果为空，则从 v2_candidate_list 中删除 v2
            bool is_candidate = false;
            for (const auto &adj_v : (*it)->value.adj)
            {
                if (v1_set.find(adj_v.id) != v1_set.end())
                {
                    is_candidate = true; // 邻接表与 v2_candidate_list 交集有结果，则当前 it 指向的顶点是 v2 的一个候选点
                    break;
                }
            }
            if (is_candidate)
            {   // it 指向的顶点是 v2 的一个候选点，则继续检查下一个 v2 的候选点
                it++; 
            }
            else
            {   // it 指向的顶点不是 v2 的一个候选点，则将其从 v2 的候选点列表删除
                it = v2_candidate_list.erase(it);
            }
        }
    }

    /**
     * 检查同层节点之间的边关系（v1-v2），删除非法候选点
     * 
     * @param v1_candidate_list    v1 顶点的候选点列表    
     * @param v2_candidate_list    v2 顶点的候选点列表
     */
     void check_edge(list<VertexT *> &v1_candidate_list, list<VertexT *> &v2_candidate_list,
                    hash_map<VertexID, vector<AdjItem>> &adj_map)
    {
        // 检查 v1
        vector<AdjItem> v2_set;
        for (const auto &v2 : v2_candidate_list)
        {
            AdjItem adj;
            adj.id = v2->id;
            v2_set.push_back(adj);
        }
        sort(v2_set.begin(), v2_set.end(), adj_comp);

        for (list<VertexT *>::iterator it = v1_candidate_list.begin(); it != v1_candidate_list.end(); /** 空 */)
        {
            // v1 的邻接表与 v2_candidate_list 作交集，如果为空，则从 v1_candidate_list 中删除 v1
            // vector<AdjItem> &adj_vec = adj_map[(*it)->id];
            vector<AdjItem> &adj_vec = (*it)->value.adj;
            vector<vector<AdjItem> *> all_list;
            all_list.push_back(&v2_set);
            all_list.push_back(&adj_vec);

            if (leapfrog_contains_common(all_list))
            {   // it 指向的顶点是 v1 的一个候选点，则继续检查下一个 v1 的候选点
                it++; 
            }
            else
            {   // it 指向的顶点不是 v1 的一个候选点，则将其从 v1 的候选点列表删除
                it = v1_candidate_list.erase(it);
                if (0 == v1_candidate_list.size()) return;
            }
        }

        // 检查 v2
        vector<AdjItem> v1_set;
        for (const auto &v1 : v1_candidate_list)
        {
            AdjItem adj;
            adj.id = v1->id;
            v1_set.push_back(adj);
        }
        sort(v1_set.begin(), v1_set.end(), adj_comp);

        for (list<VertexT *>::iterator it = v2_candidate_list.begin(); it != v2_candidate_list.end(); /** 空 */)
        {
            // v2 的邻接表与 v1_candidate_list 作交集，如果为空，则从 v2_candidate_list 中删除 v2
            // vector<AdjItem> &adj_vec = adj_map[(*it)->id];
            vector<AdjItem> &adj_vec = (*it)->value.adj;
            vector<vector<AdjItem> *> all_list;
            all_list.push_back(&v1_set);
            all_list.push_back(&adj_vec);
            
            if (leapfrog_contains_common(all_list))
            {   // it 指向的顶点是 v2 的一个候选点，则继续检查下一个 v2 的候选点
                it++; 
            }
            else
            {   // it 指向的顶点不是 v2 的一个候选点，则将其从 v2 的候选点列表删除
                it = v1_candidate_list.erase(it);
                if (0 == v2_candidate_list.size()) return;
            }
        }
    }

    void write_result(SubgraphT &task_graph)
    {
        char out_name[64];
        // sprintf(out_name, "out-%d-%d.txt", _my_rank, thread_rank);
        sprintf(out_name, "sort-single-out.txt");
        ofstream out(out_name, ios::app);
        // for (const auto &r : task_graph.roots)
        // {
        //     out << r.id << "\n";
        // }
        vector<GMatchVertex> &vet_list = task_graph.vertexes;
        for (const auto &v : vet_list)
        {
            out << "(" << v.id << ", " << v.value.l << ") 邻接表：" << endl;
            const vector<AdjItem> &adjs = v.value.adj;
            for (const auto &a : adjs)
            {
                out <<  "(" << a.id << ", " << a.l << ") ";
            }
            out << endl;
        }

        out << "---------------------------------------------------" << "\n";
        out.close();
    }

    /**
     * 执行单个任务
     */
    bool single_compute(SubgraphT &task_graph, ContextT &context, vector<VertexT *> &frontier)
    {
        const hash_map<VertexID, QueryGroup*> &lsh_query_graph_group = *(hash_map<VertexID, QueryGroup*>*)global_lsh_query_graph_group; // key：查询组 id，value：对应的查询组
        const hash_map<VertexID, QueryPlanVertexVec> &plan_order_map = *(hash_map<VertexID, QueryPlanVertexVec> *)global_query_plan_vertex_order;
        const hash_map<VertexID, QueryPlanVertexVecList> &q = *(hash_map<VertexID, QueryPlanVertexVecList> *)global_query_plan_vertex_order_list;
        const hash_map<VertexID, vector<query_judge>> &combine_tag_table = *(hash_map<VertexID, vector<query_judge>> *)global_query_combine_tag;
        
        const hash_map<VertexID, vector<int>> & query_tag_all = *(hash_map<VertexID, vector<int>> *)global_query_vertex_tag;
        const hash_map<VertexID, bool> & query_last_all= *(hash_map<VertexID, bool> *)global_query_last;

        const hash_map<VertexID, QueryVertexVec> &order_list_table = *(hash_map<VertexID, QueryVertexVec> *)global_query_vertex_order;
        const hash_map<VertexID, EdgeVector> &edge_vec_table = *(hash_map<VertexID, EdgeVector>*)global_same_layer_edge;
        const hash_map<VertexID, QueryPlanVertex> &query_plan_vertex_table = *(hash_map<VertexID, QueryPlanVertex>*)global_query_plan_vertex_table; // 查询计划点表
        const hash_map<VertexID, VertexT*> &query_graph_table = *(hash_map<VertexID, VertexT*> *)global_query_graph_table; // 查询图顶点表
        bool is_end = (context >= task_graph.max_step - 1); // 标记是否结束
        
        hash_map<VertexID, vector<AdjItem>> adj_map; // 符合条件的顶点的邻接表
        const hash_set<Label> &query_vertex_label = *(hash_set<Label> *)global_query_vertex_label; // 查询图中所有查询点的标签集合
        // 查询图对应的分组
        const hash_map<VertexID, QueryGroup*> &lsh_query_graph_group_table = *(hash_map<VertexID, QueryGroup*>*)global_lsh_query_graph_group_table; // 查询图分组表，key：查询图 id，value：查询组，用于记录每个查询图对应的分组
        const vector<VertexID> &matched_query_graph_id =  task_graph.query_vertexs;

        // cout << "query graph: " << matched_query_graph_id[0] 
        //      << ", comm match size: " << task_graph.comm_match.size()
        //      << ", context: " << (int)context
        //      << ", max step: " << task_graph.max_step << endl;
        // 判断当前任务是否含有公共部分结果，并且是否可以结束
        // if (task_graph.comm_match.size() > 0 && context >= task_graph.max_step)
        if (NULL != task_graph.comm_match && context >= task_graph.max_step)
        {
            vector<VertexT> &tg_vertexes = task_graph.vertexes;

            // cout << "公共部分匹配结果" << endl;
            // for (const auto &match : task_graph.comm_match)
            // {
            //     for (const auto &r : match)
            //     {
            //         cout << r << " ";
            //     }
            //     cout << endl;
            // }

#if ENABLE_DEBUG
            // 输出任务子图
            cout << endl << "======= 匹配的查询点： ";
            for (const auto &v : task_graph.query_vertexs)
            {
                cout << v << " ";
            }
            cout << " ===============" << endl;

            cout << "======= 种子顶点： ";
            for (const auto &v : task_graph.roots)
            {
                cout << v.id << " ";
            }
            cout << " ===============" << endl;

            cout << "任务子图结构如下：" << endl;
            for (const auto &v : tg_vertexes)
            {
                cout << "(" << v.id << "," << v.value.l << ")" << "邻接表：";
                for (const auto &adj_v : v.value.adj)
                {
                    cout << "(" << adj_v.id << "," << adj_v.l << ") ";
                }
                cout << endl;
            }
#endif
            VertexID query_graph_id = task_graph.query_vertexs[0];                         // 获得查询图的 id
            const QueryGroup* group_ptr = lsh_query_graph_group_table.find(query_graph_id)->second;//查询组
            GMatchSubgraph comm_subgraph = group_ptr->comm_subgraph;            //公共子图
            vector<QueryVertex> comm_subgraph_vertexes = comm_subgraph.vertexes;//公共子图的点
       
            //先将每个查询点的匹配数量设置为0
            hash_map<VertexID,size_t> each_query_count;
            for(auto &each_query :q){
                // cout <<"查询图id："<<each_query.first <<endl;
                each_query_count[each_query.first] = 0;
            }

            vector<VertexID> GMatchQ;           //保存结果的数据
            QueryPlanVertexVecList query_graph; //查询图
            size_t count = 0;
            bool query_last = false;

            vector<vector<VertexID>> &comm_matchQ = *task_graph.comm_match;                               // 保存公共部分匹配
            vector<VertexID> MatchQ;
            const vector<query_judge> comm_tag  = combine_tag_table.find(query_graph_id)->second;  //查询点的状态
            vector<vector<VertexID>> matchQList;
            query_graph = q.find(query_graph_id)->second;
            const vector<query_judge> query_state =  combine_tag_table.find(query_graph_id)->second;
            count = 0;

            //一开始回溯
            vector<GMatchVertex> &root= task_graph.roots;
            // cout << "-------the root size() = "<<root.size() <<endl;
            // cout << "-------the root"<<query_graph_id <<" task_graph.size =" << task_graph.vertexes.size() << endl;
            // graph_matching_comm4(query_graph,task_graph,comm_matchQ,GMatchQ,count,query_state);
            // cout<<"公共匹配数量："<<comm_matchQ.size()<<endl;
            if(comm_matchQ.size() > 0)
            {
                graph_matching_comm_root(query_graph,task_graph,comm_matchQ,GMatchQ,count,query_state);
            
                // 强制从头开始匹配
                // graph_matching_root(query_graph,task_graph, GMatchQ, count,query_state);
            }
            
            // count = comm_matchQ.size();
            // graph_matching2(query_graph,task_graph,GMatchQ,count,query_state); 

            each_query_count[query_graph_id] = count;    
            // each_query_count[0] = task_graph.vertexes.size();

            global_task_graph_size_vec[thread_rank] = task_graph.vertexes.size();
            global_task_graph_id_vec[thread_rank] = task_graph.vertexes[0].id;
            
            // 聚合结果
            GMatchAgg *agg = get_aggregator();
            agg->aggregate(each_query_count);

            // 结束任务
            return false;
        }


        vector<const QueryPlanVertexVecList*> matched_q;
        vector<const vector<query_judge>*> matched_combine_tag;
        vector<const QueryVertexVec*> matched_query_order;
        hash_map<pair<VertexID, VertexID>, VertexID, pair_hash> matched_edges; // 该任务子图能够匹配到所有的查询图的当前层边关系，Key：边 e，Value：边 e 所属的查询图 id
        vector<QueryVertex> current_level_vertex;     // 该任务子图能够匹配到所有的查询图的当前层查询点
        // 获取该任务子图能够匹配的所有查询图的查询计划
        for (const auto &id : matched_query_graph_id)
        {
            matched_q.push_back(&(q.find(id)->second));
            matched_combine_tag.push_back(&(combine_tag_table.find(id)->second));
            const QueryVertexVec &order = order_list_table.find(id)->second;
            matched_query_order.push_back(&order);
            if (order.size() > context)
            {
                current_level_vertex.insert(current_level_vertex.end(), order[context].begin(), order[context].end());
            }

            const EdgeVector &edge_vec = edge_vec_table.find(id)->second;
            if (edge_vec.size() > context)
            {
                for (const auto&e : edge_vec[context])
                {
                    matched_edges.insert(make_pair(e, id));
                }
            }
        }
        
        // 判断是否是最后一层
        hash_set<VertexID> pull_vertex;   // 需要拉取的顶点 id
        // 若当前层是倒数第二层，则判断最后一层的查询点是否都需要拉取
        if (context == task_graph.max_step - 2)
        {
            vector<QueryVertex> last_level_query_vertex; // 最后一层的查询点
            for (auto & order : matched_query_order)
            {
                if (order->size() == task_graph.max_step)
                {
                    last_level_query_vertex.insert(last_level_query_vertex.end() , order->back().begin(), order->back().end());
                }
            }

            bool flag = true;
            for (const auto &q_v : last_level_query_vertex)
            {
                const QueryPlanVertex &qpv = query_plan_vertex_table.find(q_v.id)->second;
                flag = flag && (q_v.value.adj.size() == qpv.parent.size());
            }
            is_end = flag;
        }

        // 遍历拉取到的顶点，利用 NLF 对其进行初步过滤、分类
        hash_map<VertexID, list<VertexT *>> vet_map; // 存储分类后的顶点，key 为：查询图中当前层的查询点 id，value 为：能够匹配到 key id 的数据图顶点（即当前拉取的顶点）
        for (int i = 0; i < frontier.size(); i++)
        {
            // 利用 LDF、NLF 过滤
            filter_vertex(frontier[i], current_level_vertex, adj_map, query_vertex_label, vet_map);
        }

        // 检查同层关系
        // 对每条边进行检查，去除不满足边关系的非法候选点，从而对 NLF 过滤后的顶点进行进一步过滤
        for (const auto &edges_item : matched_edges)
        {
            // 取出边两端顶点的候选点列表
            const pair<VertexID, VertexID>& e = edges_item.first; // 边 e
            const VertexID& qg_id = edges_item.second; // 边 e 所属的查询图 id
            hash_map<VertexID, list<VertexT *>>::iterator v1_it = vet_map.find(e.first);
            hash_map<VertexID, list<VertexT *>>::iterator v2_it = vet_map.find(e.second);
            if (v1_it == vet_map.end() || v2_it == vet_map.end())
            {
                // e.first 或 e.second 查询点的候选点列表为空，则说明此 task 不能匹配到这条边所属的查询图，在候选查询图集合中需要将该查询图删除
                vector<VertexID>& query_graph_id_set = task_graph.query_vertexs; // 当前 task 可能匹配到的查询图 id
                // 在 query_vertexs 中将边所属的查询图 id 删除
                for (auto qg_it = query_graph_id_set.begin(); qg_it != query_graph_id_set.end(); /** empty */)
                {
                    if (*qg_it == qg_id)
                    {
                        qg_it = query_graph_id_set.erase(qg_it);
                        break;
                    }
                    else
                    {
                        qg_it++;
                    }
                }

                if (query_graph_id_set.empty())
                {
                    hash_map<VertexID,size_t> count;
                    GMatchAgg *agg = get_aggregator();
                    agg->aggregate(count);
                    // agg->aggregate(time_counter);
                    return false;
                }
            }

            if (v1_it != vet_map.end() && v2_it != vet_map.end())
            {
                // 只有两端顶点的候选列表不为空时，才需要通过 check_edge 去删除一些非法的候选点
                if (v1_it->second.size() > 0 && v2_it->second.size() > 0)
                {
                    check_edge(v1_it->second, v2_it->second, adj_map);
                    // check_edge(v1_it->second, v2_it->second);
                }
                else
                {
                    vector<VertexID>& query_graph_id_set = task_graph.query_vertexs; // 当前 task 可能匹配到的查询图 id
                    // 在 query_vertexs 中将边所属的查询图 id 删除
                    for (auto qg_it = query_graph_id_set.begin(); qg_it != query_graph_id_set.end(); /** empty */)
                    {
                        if (*qg_it == qg_id)
                        {
                            qg_it = query_graph_id_set.erase(qg_it);
                            break;
                        }
                        else
                        {
                            qg_it++;
                        }
                    }

                    if (query_graph_id_set.empty())
                    {
                        hash_map<VertexID,size_t> count;
                        GMatchAgg *agg = get_aggregator();
                        agg->aggregate(count);
                        // agg->aggregate(time_counter);
                        return false;
                    }
                }
            }
        }

        // 变换 vet_map 结构 (hash_map<VertexID, list<VertexT *>> ==> hash_map<VertexT *, vector<VertexID>>)
        hash_map<VertexT *, vector<VertexID>, vertex_hash> vet_map2;
        hash_map<VertexT *, vector<VertexID>, vertex_hash>::iterator it;
        hash_map<VertexID, set<VertexID>> & candidate_vertexs = task_graph.candidate_vertexs;
        for (const auto &item : vet_map)
        {
            for (const auto &v : item.second)
            {
                candidate_vertexs[item.first].insert(v->id);
                it = vet_map2.find(v);
                if (it == vet_map2.end())
                {
                    vector<VertexID> vec;
                    vec.push_back(item.first);
                    vet_map2.insert(make_pair(v, vec));
                }
                else 
                {
                    it->second.push_back(item.first);
                }
            }
        }

        // 判断过滤后的候选点 map 是否为空
        if (0 == vet_map2.size() && !is_end)
        {   // 当前步不能结束但是map 为空，则说明当前层的查询点没有合法的候选点（即不能匹配到当前层查询点），因此当前 task 需要提前结束任务
            LOG_DEBUG("%d 种子顶点任务提前结束\n", task_graph.vertexes[0].id);
            vector<VertexID>& query_graph_id_set = task_graph.query_vertexs;
            for (auto qg_it = query_graph_id_set.begin(); qg_it != query_graph_id_set.end(); /** empty */)
            {
                if (plan_order_map.find(*qg_it)->second.size() - 1 >= context)
                {
                    qg_it = query_graph_id_set.erase(qg_it);
                }
                else
                {
                    qg_it++;
                }
            }

            if (query_graph_id_set.empty())
            {
                hash_map<VertexID,size_t> count;
                GMatchAgg *agg = get_aggregator();
                agg->aggregate(count);
                // agg->aggregate(time_counter);
                return false;
            }
        }

        // 将过滤后的候选点加入任务子图中，同时确定下一次要拉取的顶点
        // WORKER_LOG(1, "第 %d 层拉取的顶点：\n", (context + 1));
        hash_map<Label, bool>::iterator child_query_vertex_it;
        for (const auto &item : vet_map2)
        {
            hash_map<Label, bool> child_query_vertex; // key：子查询点标签，value：标记子查询点是否需要拉取
            vector<pair<Label, bool>> child_query_vertex_vec;
            vector<const GMatchVertex*> only_parent_query_vertex;
            // 获取顶点子节点的标签及度数
            if (context < task_graph.max_step - 1) // 判断当前层是否是最后一层，如果是最后一层，不需要再拉取顶点，因此也不需要获取子节点的标签
            {
                // 不是最后一层，则需要获取子节点标签，从而确定需要拉取的顶点
                for (const auto &q_v : item.second) 
                {
                    const auto &clvm_it = query_plan_vertex_table.find(q_v);

                    const QueryPlanVertex &p_v = query_plan_vertex_table.find(q_v)->second;
                    // 获取 p_v 的子查询点的标签
                    get_child_query_vertex(p_v, child_query_vertex, 
                                           query_graph_table, query_plan_vertex_table, only_parent_query_vertex);
                }
                for (const auto &cqv : child_query_vertex)
                    child_query_vertex_vec.push_back(make_pair(cqv.first, cqv.second));
            }

            // 判断当前顶点是否在任务子图中，不在则需要将其加入到任务子图中
            item.first->value.matched_query = item.second;
            if (!task_graph.hasVertex(item.first->id))
            {
                addNode(task_graph, *item.first);
            }

            // 遍历邻接表，根据邻接表数据在任务子图中加入相应的边。同时，根据子节点标签拉取相应的顶点数据
            vector<AdjItem> &nbs = adj_map[item.first->id];
            for (const auto &adj_v : nbs)
            {
                bool has_flag = task_graph.hasVertex(adj_v.id);

                if (task_graph.hasVertex(adj_v.id))
                {
                    // 添加边
                    addEdge_safe(task_graph, item.first->id, adj_v.id);
                }

                child_query_vertex_it = child_query_vertex.find(adj_v.l);

                // 判断当前顶点是否需要拉取
                if (child_query_vertex_it != child_query_vertex.end())
                {
                    // 判断当前顶点匹配到的查询点是否只有父节点
                    if (child_query_vertex_it->second)
                    {   // 如果只有父节点，则不需要拉取，直接加入到任务子图中
                        if (!task_graph.hasVertex(adj_v.id))
                        {
                            vector<VertexID> vec;
                            addNode(task_graph, adj_v.id, adj_v.l, vec);
                        }
                        // 添加边
                        addEdge_safe(task_graph, item.first->id, adj_v.id);

                        for (const auto &opqv : only_parent_query_vertex)
                        {
                            if (opqv->value.l == adj_v.l)
                                candidate_vertexs[opqv->id].insert(adj_v.id);
                        }
                    }
                    else
                    {   // 如果含有兄弟节点或子节点，则需要拉取
                        pull_vertex.insert(adj_v.id);
                    }
                }
            }
        }

        // WORKER_LOG(1, "\n\n");

        // 判断是否结束
        if (is_end)
        {
            vector<VertexT> &tg_vertexes = task_graph.vertexes;

#if ENABLE_DEBUG
            // 输出任务子图
            cout << endl << "======= 匹配的查询点： ";
            for (const auto &v : task_graph.query_vertexs)
            {
                cout << v << " ";
            }
            cout << " ===============" << endl;

            cout << "======= 种子顶点： ";
            for (const auto &v : task_graph.roots)
            {
                cout << v.id << " ";
            }
            cout << " ===============" << endl;

            cout << "任务子图结构如下：" << endl;
            for (const auto &v : tg_vertexes)
            {
                cout << "(" << v.id << "," << v.value.l << ")" << "邻接表：";
                for (const auto &adj_v : v.value.adj)
                {
                    cout << "(" << adj_v.id << "," << adj_v.l << ") ";
                }
                cout << endl;
            }
#endif
            VertexID query_graph_id = task_graph.query_vertexs[0]; // 获得查询图的 id
            const QueryGroup* group_ptr = lsh_query_graph_group_table.find(query_graph_id)->second;//查询组
            const GMatchSubgraph &comm_subgraph = group_ptr->comm_subgraph;            //公共子图
            const vector<QueryVertex> &comm_subgraph_vertexes = comm_subgraph.vertexes;//公共子图的点

            //先将每个查询点的匹配数量设置为0
            hash_map<VertexID,size_t> each_query_count;
            for(auto &each_query :q){
                // cout <<"查询图id："<<each_query.first <<endl;
                each_query_count[each_query.first] = 0;
            }

            vector<VertexID> GMatchQ;                                 //保存结果的数据
            const QueryPlanVertexVecList &query_graph = q.find(query_graph_id)->second; //查询图
            size_t count = 0;
            // bool query_last = false;
            const vector<query_judge> &query_state =  combine_tag_table.find(query_graph_id)->second;
            count = 0;

            // 判断是否存在公共匹配结果
            if(NULL == task_graph.comm_match) // 无公共匹配结果，则直接回溯即可
            {
                // cout<<"query_graph.size() = "<<query_graph.size()<<endl;
                // cout<<"查询图查询点：";
                // for(int i=0; i<query_graph.size();++i){
                //     cout<<" "<<query_graph[i].id<<query_graph[i].l;
                // }
                // cout<<endl;
                //  cout<<"--------------------"<<endl;
                // cout<<"query_state.size() = "<<query_state.size()<<endl;
                // for(int i=0; i<query_state.size();++i){
                //     cout<<" "<<query_state[i].id;
                // }
                // cout<<endl;
                // graph_matching3(query_graph,task_graph,GMatchQ,count,query_state); 
                // cout<<"进入graph_matching_root2"<<endl;
                

                // cout <<"无公共匹配结果，则直接回溯即可"<<endl;
                graph_matching_root(query_graph,task_graph, GMatchQ, count,query_state);
                

                // cout<<"结束"<<endl;
            }                
            else if (task_graph.comm_match->size() > 0) // 存在公共匹配结果，则利用公共匹配结果继续匹配
            {

                vector<vector<VertexID>> &comm_matchQ = *task_graph.comm_match; // 保存公共部 分 匹配
                //拉取几步后回溯
                // 使用公共部分继续匹配
                graph_matching_comm_root(query_graph,task_graph,comm_matchQ,GMatchQ,count,query_state);
                
                // 强制从头开始匹配
                // graph_matching_root(query_graph,task_graph, GMatchQ, count,query_state);
                // count = comm_matchQ.size();
            }
            
            // graph_matching2(query_graph,task_graph,GMatchQ,count,query_state); 
           
            each_query_count[query_graph_id] = count;
            // each_query_count[0] = task_graph.vertexes.size();
            global_task_graph_size_vec[thread_rank] = task_graph.vertexes.size();
            global_task_graph_id_vec[thread_rank] = task_graph.vertexes[0].id;

            // 将有效种子顶点写入到文件中，以便检查结果
            // write_result(task_graph);
            
            // 聚合结果
            GMatchAgg *agg = get_aggregator();
            agg->aggregate(each_query_count);

            // 结束任务
            return false;
        }
        else
        {
            if (pull_vertex.empty())
            {
                LOG_DEBUG("%d 种子顶点任务提前结束\n", task_graph.vertexes[0].id);
                vector<VertexID>& query_graph_id_set = task_graph.query_vertexs;
                for (auto qg_it = query_graph_id_set.begin(); qg_it != query_graph_id_set.end(); /** empty */)
                {
                    if (plan_order_map.find(*qg_it)->second.size() - 1 >= context)
                    {
                        qg_it = query_graph_id_set.erase(qg_it);
                    }
                    else
                    {
                        qg_it++;
                    }
                }

                if (query_graph_id_set.empty())
                {
                    hash_map<VertexID,size_t> count;
                    GMatchAgg *agg = get_aggregator();
                    agg->aggregate(count);
                    // agg->aggregate(time_counter);
                    return false;
                }
            }

            // 拉取下一层顶点
            clear_pull(); // 清空之前拉取的顶点，从而存储下一次需要拉取的顶点
            for (auto it = pull_vertex.begin(); it != pull_vertex.end(); it++)
            {
                pull(*it);
            }

            context++;
            return true;
        }
    }

    /**
     * 通过回溯计算公共子图的部分。
     * 如果任务能够继续执行，则返回 true；否则（任务被延迟执行），返回 false
     */
    bool comm_compute1(SubgraphT &task_graph, ContextT &context, vector<VertexT *> &frontier)
    {
        hash_map<VertexID, QueryGroup*> &lsh_query_graph_group_table = *(hash_map<VertexID, QueryGroup*>*)global_lsh_query_graph_group_table; // 查询图分组表，key：查询图 id，value：查询组，用于记录每个查询图对应的分组
        // hash_map<VertexID, vector<query_judge>> &combine_tag_table = *(hash_map<VertexID, vector<query_judge>> *)global_query_combine_tag;
        hash_map<VertexID, vector<query_judge>> &comm_combine_tag_table = *(hash_map<VertexID, vector<query_judge>> *)global_comm_combine_tag_table;
        vector<VertexID> &query_vertexs = task_graph.query_vertexs;
        VertexID query_graph_id = query_vertexs[0];                         // 获得查询图的 id
        QueryGroup* group_ptr = lsh_query_graph_group_table[query_graph_id];//查询组
        GMatchSubgraph &comm_subgraph = group_ptr->comm_subgraph;            //公共子图
        vector<QueryVertex> &comm_subgraph_vertexes = comm_subgraph.vertexes;

        bool flag_execute = false; // 标记任务是否继续执行
        if (cur_task->comm_match_memory_size > 0)
        { // 从延迟任务队列 q_delay_task 中取出的任务，则当前任务必须要执行，无论内存是否能够存储下公共匹配结果
            flag_execute = true;
        }
        else
        {
            hash_map<Label, int> query_vertex_map; // key：公共子图中查询点的标签，value：查询点候选点集合的下标
            for (int i = 1; i < comm_subgraph_vertexes.size(); i++)
            {
                QueryVertex &qv = comm_subgraph_vertexes[i];
                query_vertex_map[qv.value.l] = i;
            }

            // 估计匹配结果数量
            hash_map<VertexID, vector<int>> candidate_map;
            long long comm_match_size = 0; // 公共匹配结果数量
            for (auto &r : task_graph.roots)
            {
                candidate_map[r.id].resize(comm_subgraph_vertexes.size());
                candidate_map[r.id][0] = 1;

                vector<AdjItem> &adj = r.value.adj;

                for (int i = 0; i < adj.size(); i++)
                {
                    const auto &it = query_vertex_map.find(adj[i].l);
                    if (it != query_vertex_map.end())
                    {
                        candidate_map[r.id][it->second]++;
                    }
                }

                long long leng = 1;
                for(int j = 0; j < candidate_map[r.id].size(); j++ ){
                    leng = leng * candidate_map[r.id][j];
                }
                comm_match_size = comm_match_size + leng;
            }

            cur_task->comm_match_memory_size = sizeof(vector<vector<VertexID>>) + 
                comm_match_size * (sizeof(vector<VertexID>) + sizeof(VertexID) * comm_subgraph_vertexes.size());
            
            flag_execute = (cur_task->comm_match_memory_size < (get_free_memory_byte() / num_compers));

            if (!flag_execute)
            {
                delay_task(cur_task); // 当前内存不能满足任务的执行，则延迟任务
                return false;
            }
        }

        // 判断剩余的内存是否足够存储公共匹配结果
        if (cur_task->comm_match_memory_size > (get_free_memory_byte() / num_compers))
            return flag_execute; // 内存不足，不进行后面的公共部分匹配

        if (NULL == task_graph.comm_match)
            task_graph.comm_match = new vector<vector<VertexID>>;

        SubgraphT comm_task_graph = task_graph; // 公共查询图对应的公共任务子图
        hash_map<VertexID, hash_set<VertexID>> root_adj_map; // 种子顶点邻居点 map，Key：种子顶点 ID，Value：种子顶点邻居点集合
        for (const auto &r : comm_task_graph.roots)
        {
            hash_set<VertexID> &adj_set = root_adj_map[r.id];
            for (const auto &adj_v : r.value.adj)
            {
                adj_set.insert(adj_v.id);
            }
        }

        hash_map<Label, VertexID> query_vertex_map; // key：公共子图中查询点的标签，value：查询点 id
        for (int i = 1; i < comm_subgraph_vertexes.size(); i++)
        {
            QueryVertex &qv = comm_subgraph_vertexes[i];
            query_vertex_map[qv.value.l] = qv.id;
        }

        // 将拉取到的顶点添加到种子顶点的邻接表中
        for (int i = 0; i < frontier.size(); i++)
        {
            const auto &it = query_vertex_map.find(frontier[i]->value.l);
            if (it != query_vertex_map.end())
            {
                addNode(comm_task_graph, *(frontier[i]));

                vector<AdjItem> &adj_vec = frontier[i]->value.adj;
                for (const auto &adj_v : adj_vec)
                {
                    if (comm_task_graph.hasVertex(adj_v.id))
                        addEdge_safe(comm_task_graph, adj_v.id, frontier[i]->id);
                }
                // for (const auto &item : root_adj_map)
                // {
                //     if (item.second.find(frontier[i]->id) != item.second.end())
                //         addEdge(comm_task_graph, item.first, frontier[i]->id);
                // }
            }
        }

        // cout << "公共查询图：" << endl;
        // vector<VertexT> &tg2_vertexes = comm_subgraph.vertexes;
        // for (const auto &v : tg2_vertexes)
        // {
        //     cout << "(" << v.id << "," << v.value.l << ")" << "邻接表：";
        //     for (const auto &adj_v : v.value.adj)
        //     {
        //         cout << "(" << adj_v.id << "," << adj_v.l << ") ";
        //     }
        //     cout << endl;
        // }

        // cout << "公共部分的任务子图：" << endl;
        // vector<VertexT> &tg3_vertexes = comm_task_graph.vertexes;
        // for (const auto &v : tg3_vertexes)
        // {
        //     cout << "(" << v.id << "," << v.value.l << ")" << "邻接表：";
        //     for (const auto &adj_v : v.value.adj)
        //     {
        //         cout << "(" << adj_v.id << "," << adj_v.l << ") ";
        //     }
        //     cout << endl;
        // }

        // 回溯枚举出所有公共匹配结果
        // cout <<"进入到回溯"<<endl;
        vector<vector<VertexID>> &comm_matchQ = *task_graph.comm_match; // 保存公共部分匹配
        vector<VertexID> MatchQ;
        VertexID group_id = group_ptr->group_id;
        vector<query_judge> comm_tag  = comm_combine_tag_table[group_id];  //查询点的状态
        vector<vector<VertexID *>> matchQList;

        // double start_time2 = get_current_time();
        // comm_matching2(comm_task_graph,comm_subgraph,comm_matchQ,MatchQ,matchQList,comm_tag);

        // 星形公共查询图
        // cout << "开始匹配公共部分" << endl;
        // comm_matching_root(comm_task_graph,cosmm_subgraph,comm_matchQ,MatchQ,matchQList,comm_tag);
        // cout << "结束匹配公共部分" << endl;

        // 含有非树边的公共查询图
        comm_matching(comm_task_graph,comm_subgraph_vertexes,comm_matchQ,MatchQ,comm_tag);

        // double stop_time2 = get_current_time();
        return flag_execute;
    }

    /**
     * 通过组合计算公共子图的部分
     */
    void comm_compute(SubgraphT &task_graph, ContextT &context, vector<VertexT *> &frontier)
    {
        hash_map<VertexID, QueryGroup*> &lsh_query_graph_group_table = *(hash_map<VertexID, QueryGroup*>*)global_lsh_query_graph_group_table; // 查询图分组表，key：查询图 id，value：查询组，用于记录每个查询图对应的分组
        hash_map<VertexID, vector<query_judge>> &combine_tag_table = *(hash_map<VertexID, vector<query_judge>> *)global_query_combine_tag;
        vector<VertexID> &query_vertexs = task_graph.query_vertexs;
        VertexID query_graph_id = query_vertexs[0];                         // 获得查询图的 id
        QueryGroup* group_ptr = lsh_query_graph_group_table[query_graph_id];//查询组
        GMatchSubgraph &comm_subgraph = group_ptr->comm_subgraph;            //公共子图
        vector<QueryVertex> &comm_subgraph_vertexes = comm_subgraph.vertexes;

        hash_map<Label, int> query_vertex_map; // key：公共子图中查询点的标签，value：查询点候选点集合的下标
        for (int i = 1; i < comm_subgraph_vertexes.size(); i++)
        {
            QueryVertex &qv = comm_subgraph_vertexes[i];
            query_vertex_map[qv.value.l] = i;
        }

        // 遍历拉取到的顶点，获取各个查询点的候选点集合
        hash_map<VertexID, vector<vector<VertexID>>> candidate_map;
        long long comm_match_size = 0; // 公共匹配结果数量
        for (auto &r : task_graph.roots)
        {
            candidate_map[r.id].resize(comm_subgraph_vertexes.size());
            candidate_map[r.id][0].push_back(r.id);

            vector<AdjItem> &adj = r.value.adj;

            for (int i = 0; i < adj.size(); i++)
            {
                const auto &it = query_vertex_map.find(adj[i].l);
                if (it != query_vertex_map.end())
                {
                    candidate_map[r.id][it->second].push_back(adj[i].id);
                }
            }

            long long leng = 1;
            for(int j = 0; j < candidate_map[r.id].size(); j++ ){
                leng = leng * candidate_map[r.id][j].size();
            }
            comm_match_size = comm_match_size + leng;
        }

        unsigned long long need_memory_size = sizeof(vector<vector<VertexID>>) + 
            comm_match_size * (sizeof(vector<VertexID>) + sizeof(VertexID) * comm_subgraph_vertexes.size());
        if (need_memory_size > get_free_memory_byte())
        {
            return; // 内存不足，不进行公共部分匹配
        }

        // 组合出所有公共匹配结果
        if (NULL == task_graph.comm_match)
            task_graph.comm_match = new vector<vector<VertexID>>();
        // int[comm_subgraph_vertexes.size()] line;
        // vector<line> comm_matchQ2;
        vector<vector<VertexID>> &comm_matchQ = *task_graph.comm_match; // 保存公共部分匹配
        // vector<vector<VertexID>> :: iterator it = comm_matchQ.begin(); 

        comm_matchQ.resize(comm_match_size);
        // comm_matchQ.resize(leng, vector<VertexID>(cansize));
        // for(int n =0;n<comm_matchQ.size(); n++){
        //    comm_matchQ[n].resize(cansize);
        // }

        
        long long allcommS = 0;
        vector<VertexID> MatchQ;
        vector<query_judge> comm_tag  = combine_tag_table[query_graph_id];  //查询点的状态
        // double start_time2 = get_current_time();
        // cout <<"查询点根节点" <<comm_subgraph_vertexes[0].id<< " "<<comm_subgraph_vertexes[0].value.l <<"后第二个查询的候选个数size-----:"<<candidate[1].size() <<endl;
        for (auto &r : task_graph.roots){
            comm_matching3(comm_subgraph_vertexes,comm_matchQ,MatchQ,comm_tag,candidate_map[r.id],allcommS);
        }
        
       
        // for(int n =0;n<comm_matchQ.size(); n++){
        //     print_vec(comm_matchQ[n]);
        // }
        // double stop_time2 = get_current_time();
        // cout << "----------comm_matching3 time----- :"<<stop_time2-start_time2<<endl;
    }


    /**
     * 执行多个任务（包含公共）
     */
    bool multi_compute(SubgraphT &task_graph, ContextT &context, vector<VertexT *> &frontier)
    {
        const hash_map<VertexID, QueryGroup*> &lsh_query_graph_group = *(hash_map<VertexID, QueryGroup*>*)global_lsh_query_graph_group; // key：查询组 id，value：对应的查询组
        const hash_map<VertexID, QueryPlanVertexVec> &plan_order_map = *(hash_map<VertexID, QueryPlanVertexVec> *)global_query_plan_vertex_order;
        const hash_map<VertexID, QueryPlanVertexVecList> &q = *(hash_map<VertexID, QueryPlanVertexVecList> *)global_query_plan_vertex_order_list;
        const hash_map<VertexID, vector<query_judge>> &combine_tag_table = *(hash_map<VertexID, vector<query_judge>> *)global_query_combine_tag;
        
        const hash_map<VertexID, vector<int>> & query_tag_all = *(hash_map<VertexID, vector<int>> *)global_query_vertex_tag;
        const hash_map<VertexID, bool> & query_last_all= *(hash_map<VertexID, bool> *)global_query_last;
        const hash_map<VertexID, QueryVertexVec> &order_list_table = *(hash_map<VertexID, QueryVertexVec> *)global_query_vertex_order;
        const hash_map<VertexID, EdgeVector> &edge_vec_table = *(hash_map<VertexID, EdgeVector>*)global_same_layer_edge;
        const hash_map<VertexID, QueryPlanVertex> &query_plan_vertex_table = *(hash_map<VertexID, QueryPlanVertex>*)global_query_plan_vertex_table; // 查询计划点表
        const hash_map<VertexID, VertexT*> &query_graph_table = *(hash_map<VertexID, VertexT*> *)global_query_graph_table; // 查询图顶点表
        bool is_end = (context == task_graph.max_step - 1);                    // 标记任务是否结束。有两种情况会在本次迭代中正常结束任务：（1）当前层是最后一层 （2）当前层是倒数第二层，同时最后一层的顶点都不需要拉取
        
        hash_map<VertexID, vector<AdjItem>> adj_map; // 符合条件的顶点的邻接表
        const hash_set<Label> &query_vertex_label = *(hash_set<Label> *)global_query_vertex_label; // 查询图中所有查询点的标签集合
        
        // 查询图对应的分组
        const hash_map<VertexID, QueryGroup*> &lsh_query_graph_group_table = *(hash_map<VertexID, QueryGroup*>*)global_lsh_query_graph_group_table; // 查询图分组表，key：查询图 id，value：查询组，用于记录每个查询图对应的分组
        const vector<VertexID> &matched_query_graph_id =  task_graph.query_vertexs;
        vector<const QueryPlanVertexVecList*> matched_q;
        vector<const vector<query_judge>*> matched_combine_tag;
        hash_map<pair<VertexID, VertexID>, VertexID, pair_hash> matched_edges; // 该任务子图能够匹配到所有的查询图的当前层边关系，Key：边 e，Value：边 e 所属的查询图 id
        vector<QueryVertex> current_level_vertex;     // 该任务子图能够匹配到所有的查询图的当前层查询点
        
        // 对拉取的顶点进行 LDF、NLF 过滤和分类
        // 获取当前层的查询点
        for (const auto &id : matched_query_graph_id)
        {
            matched_q.push_back(&(q.find(id)->second));
            matched_combine_tag.push_back(&(combine_tag_table.find(id)->second));
            const QueryVertexVec &order = order_list_table.find(id)->second;
            if (order.size() > context)
            {
                current_level_vertex.insert(current_level_vertex.end(), order[context].begin(), order[context].end());
            }

            const EdgeVector &edge_vec = edge_vec_table.find(id)->second;
            if (edge_vec.size() > context)
            {
                for (const auto&e : edge_vec[context])
                {
                    matched_edges.insert(make_pair(e, id));
                }
            }
        }
        // 遍历拉取到的顶点，利用 NLF 对其进行初步过滤、分类
        hash_map<VertexID, list<VertexT *>> vet_map; // 存储分类后的顶点，key 为：查询图中当前层的查询点 id，value 为：能够匹配到 key id 的数据图顶点（即当前拉取的顶点）
        for (int i = 0; i < frontier.size(); i++)
        {   // 利用 LDF、NLF 过滤
            filter_vertex(frontier[i], current_level_vertex, adj_map, query_vertex_label, vet_map);
        }

        // cout << "过滤前任务：";
        // for (const auto &query_graph_item : task_graph.query_vertexs)
        // {
        //     cout << query_graph_item << " ";
        // }
        // cout << endl;

        // 检查同层关系
        // 对每条边进行检查，去除不满足边关系的非法候选点，从而对 NLF 过滤后的顶点进行进一步过滤
        for (const auto &edges_item : matched_edges)
        {
            // 取出边两端顶点的候选点列表
            const pair<VertexID, VertexID>& e = edges_item.first; // 边 e
            const VertexID& qg_id = edges_item.second; // 边 e 所属的查询图 id
            hash_map<VertexID, list<VertexT *>>::iterator v1_it = vet_map.find(e.first);
            hash_map<VertexID, list<VertexT *>>::iterator v2_it = vet_map.find(e.second);
            if (v1_it == vet_map.end() || v2_it == vet_map.end())
            {
                // e.first 或 e.second 查询点的候选点列表为空，则说明此 task 不能匹配到这条边所属的查询图，在候选查询图集合中需要将该查询图删除
                vector<VertexID>& query_graph_id_set = task_graph.query_vertexs; // 当前 task 可能匹配到的查询图 id
                // 在 query_vertexs 中将边所属的查询图 id 删除
                for (auto qg_it = query_graph_id_set.begin(); qg_it != query_graph_id_set.end(); /** empty */)
                {
                    if (*qg_it == qg_id)
                    {
                        qg_it = query_graph_id_set.erase(qg_it);
                        break;
                    }
                    else
                    {
                        qg_it++;
                    }
                }

                if (query_graph_id_set.empty())
                {                   
                    hash_map<VertexID,size_t> count;
                    GMatchAgg *agg = get_aggregator();
                    agg->aggregate(count);

                    return false;
                }
            }

            if (v1_it != vet_map.end() && v2_it != vet_map.end())
            {
                // 只有两端顶点的候选列表不为空时，才需要通过 check_edge 去删除一些非法的候选点
                if (v1_it->second.size() > 0 && v2_it->second.size() > 0)
                {
                    check_edge(v1_it->second, v2_it->second, adj_map);
                    // check_edge(v1_it->second, v2_it->second);
                }
                else
                {
                    vector<VertexID>& query_graph_id_set = task_graph.query_vertexs; // 当前 task 可能匹配到的查询图 id
                    // 在 query_vertexs 中将边所属的查询图 id 删除
                    for (auto qg_it = query_graph_id_set.begin(); qg_it != query_graph_id_set.end(); /** empty */)
                    {
                        if (*qg_it == qg_id)
                        {
                            qg_it = query_graph_id_set.erase(qg_it);
                            break;
                        }
                        else
                        {
                            qg_it++;
                        }
                    }

                    if (query_graph_id_set.empty())
                    {
                        hash_map<VertexID,size_t> count;
                        GMatchAgg *agg = get_aggregator();
                        agg->aggregate(count);

                        return false;
                    }
                }
            }
        }

        // 获取过滤后依然能够匹配到的查询点
        hash_map<VertexID, VertexID> filtered_matched_query_vertex; // 存储过滤后依然能够匹配到的查询点，Key：查询点 ID，Value：查询点所属的查询图 ID
        for (const auto &id : task_graph.query_vertexs)
        {
            const QueryVertexVec &order = order_list_table.find(id)->second;
            if (order.size() > context)
            {
                for (const auto &qv : order[context])
                    filtered_matched_query_vertex.insert(make_pair(qv.id, id));
            }
        }

        // 变换 vet_map 结构 (hash_map<VertexID, list<VertexT *>> ==> hash_map<VertexT *, vector<VertexID>>)
        hash_map<VertexT *, hash_map<VertexID, vector<VertexID>>, vertex_hash> vet_map2; // key：数据点，value：该数据点能够匹配到的查询图的查询点（key：查询图 ID，value：查询点）
        for (const auto &item : vet_map)
        {
            if (filtered_matched_query_vertex.find(item.first) == filtered_matched_query_vertex.end())
                continue;
            for (const auto &v : item.second)
            {
                VertexID q_id = item.first; // 查询点 ID
                VertexID qg_id = filtered_matched_query_vertex[q_id]; // 查询图 ID
                vet_map2[v][qg_id].push_back(q_id);
            }
        }

        // 判断过滤后的候选点 map 是否为空
        if (0 == vet_map2.size() && !is_end)
        {   // 当前步不能结束但是map 为空，则说明当前层的查询点没有合法的候选点（即不能匹配到当前层查询点），因此当前 task 需要提前结束任务
            LOG_DEBUG("%d 种子顶点任务提前结束\n", task_graph.vertexes[0].id);
            vector<VertexID>& query_graph_id_set = task_graph.query_vertexs;
            for (auto qg_it = query_graph_id_set.begin(); qg_it != query_graph_id_set.end(); /** empty */)
            {
                if (plan_order_map.find(*qg_it)->second.size() - 1 >= context)
                {
                    qg_it = query_graph_id_set.erase(qg_it);
                }
                else
                {
                    qg_it++;
                }
            }

            if (query_graph_id_set.empty())
            {
                hash_map<VertexID,size_t> count;
                GMatchAgg *agg = get_aggregator();
                agg->aggregate(count);

                return false;
            }
        }

        // 将任务按照查询图进行拆分
        hash_map<VertexID, GMatchTask*> divied_task_map; // 按照查询图分解后的任务 map，Key：查询图 ID，Value：任务
        // 判断是否能够匹配到多个查询图
        if (1 == task_graph.query_vertexs.size())
        {
            VertexID qg_id = task_graph.query_vertexs[0];
            cur_task->subG.max_step = plan_order_map.find(qg_id)->second.size();
            divied_task_map[qg_id] = cur_task; // 如果经过过滤后，只能匹配到一个查询图，则直接将当前任务放进 map，不需要创建新的任务
        }
        else
        {
            // 匹配出公共部分结果
            bool flag_execute = comm_compute1(task_graph, context, frontier);
            if (!flag_execute)
            {
                return true; // 任务只是延迟执行，但是不能结束，所以返回 true
            }

            pair<KeyT, KeyT> t_key = make_pair(cur_task->group_id, task_graph.vertexes[0].id);
            if (NULL != task_graph.comm_match && 0 == task_graph.comm_match->size())
            {
                comm_match_reference_count[t_key] = 1;
                return false;
            } 

            // double start_time2 = get_current_time();
            // 按照查询图依次形成各自的任务
            vector<VertexID> final_match_query_graph = task_graph.query_vertexs;
            for (const auto &id : final_match_query_graph)
            {
                vector<VertexID> new_query_vertexs = {id};
                task_graph.query_vertexs.swap(new_query_vertexs);
                
                GMatchTask *t = new GMatchTask;
                t->subG = task_graph;
                t->group_id = cur_task->group_id;
                t->context = context;
                t->subG.max_step = plan_order_map.find(id)->second.size();
                
                divied_task_map[id] = t;
            }
            // double end_time2 = get_current_time();
            comm_match_reference_count[t_key] = final_match_query_graph.size() + 1;
            // cout << "divide time---: " << (end_time2 - start_time2) << "s" <<  endl;
        }

        // cout << "过滤后任务：";
        // for (const auto &query_graph_item : divied_task_map)
        // {
        //     cout << query_graph_item.first << " ";
        // }
        // cout << endl;

        // 将过滤后的候选点加入任务子图中，同时确定下一次要拉取的顶点
        hash_map<VertexID, hash_set<VertexID>> pull_vertex_map; // 各个任务需要拉取的顶点，Key：查询点 ID，Value：该任务需要摘取的顶点 ID 集合
        hash_map<Label, bool>::iterator child_query_vertex_it;
        for (const auto &item : vet_map2)
        {
            hash_map<VertexID, hash_map<Label, bool>> child_query_vertex_map; // Key：查询图 ID，Value：ID 对应查询图中当前层的子查询点
            hash_map<VertexID, vector<const GMatchVertex*>> only_parent_query_vertex_map; // Key：查询图 ID，Value：ID 对应查询图中当前层的子查询点集合中，只含有父节点的部分

            for (const auto & query_graph_item : item.second)
            {
                VertexID qg_id = query_graph_item.first; // 查询图 ID
                SubgraphT &current_task_graph = divied_task_map[qg_id]->subG;
                hash_set<VertexID> &pull_vertex = pull_vertex_map[qg_id];
                hash_map<Label, bool> &child_query_vertex = child_query_vertex_map[qg_id]; // key：子查询点标签，value：标记子查询点是否需要拉取
                vector<const GMatchVertex*> &only_parent_query_vertex = only_parent_query_vertex_map[qg_id];
                // 获取顶点子节点的标签及度数
                if (context < current_task_graph.max_step - 1) // 判断当前层是否是最后一层，如果是最后一层，不需要再拉取顶点，因此也不需要获取子节点的标签
                {
                    // 不是最后一层，则需要获取子节点标签，从而确定需要拉取的顶点
                    for (const auto &q_v : query_graph_item.second) 
                    {
                        const QueryPlanVertex &p_v = query_plan_vertex_table.find(q_v)->second;
                        // 获取 p_v 的子查询点的标签
                        get_child_query_vertex(p_v, child_query_vertex, 
                                            query_graph_table, query_plan_vertex_table, only_parent_query_vertex);
                    }
                }

                // 判断当前顶点是否在任务子图中，不在则需要将其加入到任务子图中
                if (!current_task_graph.hasVertex(item.first->id))
                {
                    addNode(current_task_graph, *item.first);
                }
                
                // 遍历邻接表，根据邻接表数据在任务子图中加入相应的边。同时，根据子节点标签拉取相应的顶点数据
                vector<AdjItem> &nbs = adj_map[item.first->id];
                for (const auto &adj_v : nbs)
                {
                    bool has_flag = current_task_graph.hasVertex(adj_v.id);

                    if (current_task_graph.hasVertex(adj_v.id))
                    {
                        // 添加边
                        addEdge_safe(current_task_graph, item.first->id, adj_v.id);
                    }

                    child_query_vertex_it = child_query_vertex.find(adj_v.l);
                    
                    // 判断当前顶点是否需要拉取
                    if (child_query_vertex_it != child_query_vertex.end())
                    {
                        // 判断当前顶点匹配到的查询点是否只有父节点
                        if (child_query_vertex_it->second)
                        {   // 如果只有父节点，则不需要拉取，直接加入到任务子图中
                            if (!current_task_graph.hasVertex(adj_v.id))
                            {
                                vector<VertexID> vec;
                                addNode(current_task_graph, adj_v.id, adj_v.l, vec);
                            }
                            // 添加边
                            addEdge_safe(current_task_graph, item.first->id, adj_v.id);

                            // for (const auto &opqv : only_parent_query_vertex)
                            // {
                            //     if (opqv->value.l == adj_v.l)
                            //         current_task_graph.candidate_vertexs[opqv->id].insert(adj_v.id);
                            // }
                        }
                        else
                        {   
                            // 如果含有兄弟节点或子节点，则需要拉取
                            pull_vertex.insert(adj_v.id);
                        }
                    }
                }
            }
        }

        // 检查每个任务是否结束
        for (const auto &task_item : divied_task_map)
        {
            VertexID id = task_item.first;
            GMatchTask *task_ptr = task_item.second;
            hash_set<VertexID> pull_vertex;   // 需要拉取的顶点 id
            // 若当前层是倒数第二层，则判断最后一层的查询点是否都需要拉取
            const QueryVertexVec &order = order_list_table.find(id)->second;
            bool is_end = (context == task_ptr->subG.max_step - 1); // 标记任务是否结束。有两种情况会在本次迭代中正常结束任务：（1）当前层是最后一层 （2）当前层是倒数第二层，同时最后一层的顶点都不需要拉取

            if (context == task_ptr->subG.max_step - 2)
            {
                const vector<QueryVertex> &last_level_query_vertex = order.back(); // 最后一层的查询点

                bool flag = true;
                for (const auto &q_v : last_level_query_vertex)
                {
                    const QueryPlanVertex &qpv = query_plan_vertex_table.find(q_v.id)->second;
                    flag = flag && (q_v.value.adj.size() == qpv.parent.size());
                }
                is_end = flag;
            }
            if (is_end)
            {
                task_ptr->context = task_ptr->subG.max_step; // 如果任务需要结束，则将 context 标记成 max_step;
            }
        }

        // 判断任务拆分后，是否只有一个任务
        if (1 == divied_task_map.size())
        {
            LOG_DEBUG("任务拆分后，只剩下一个查询图\n");
            // 任务不能结束，则继续拉取
            if (cur_task->context < cur_task->subG.max_step)
            {
                VertexID qg_id = cur_task->subG.query_vertexs[0]; // 查询图 ID
                hash_set<VertexID> &pull_vertex = pull_vertex_map[qg_id];
                if (pull_vertex.empty())
                {
                    hash_map<VertexID,size_t> count;
                    GMatchAgg *agg = get_aggregator();
                    agg->aggregate(count);
                    return false;
                }

                // 拉取下一层顶点
                cur_task->to_pull.clear(); // 清空之前拉取的顶点，从而存储下次需要拉取的顶点
                for (const auto &v_id : pull_vertex)
                    cur_task->to_pull.push_back(v_id);

                cur_task->context++;
                return true;
            }

            LOG_DEBUG("剩余一个任务后，继续匹配\n");
            // 当只有一个任务时，且该任务需要结束，则直接回溯枚举
            SubgraphT &current_task_graph = cur_task->subG;
            vector<VertexT> &tg_vertexes = current_task_graph.vertexes;

#if ENABLE_DEBUG
            // 输出任务子图
            cout << endl << "======= 匹配的查询点： ";
            for (const auto &v : current_task_graph.query_vertexs)
            {
                cout << v << " ";
            }
            cout << " ===============" << endl;

            cout << "======= 种子顶点： ";
            for (const auto &v : current_task_graph.roots)
            {
                cout << v.id << " ";
            }
            cout << " ===============" << endl;

            cout << "任务子图结构如下：" << endl;
            for (const auto &v : tg_vertexes)
            {
                cout << "(" << v.id << "," << v.value.l << ")" << "邻接表：";
                for (const auto &adj_v : v.value.adj)
                {
                    cout << "(" << adj_v.id << "," << adj_v.l << ") ";
                }
                cout << endl;
            }
#endif
            VertexID query_graph_id = current_task_graph.query_vertexs[0]; // 获得查询图的 id
            const QueryGroup* group_ptr = lsh_query_graph_group_table.find(query_graph_id)->second;//查询组
            const GMatchSubgraph &comm_subgraph = group_ptr->comm_subgraph;            //公共子图
            const vector<QueryVertex> &comm_subgraph_vertexes = comm_subgraph.vertexes;//公共子图的点
            
#if ENABLE_DEBUG
            //输出公共子图
            vector<VertexT> &tg2_vertexes = comm_subgraph.vertexes;
            cout << "公共结构如下：" << endl;
            for (const auto &v : tg2_vertexes)
            {
                cout << "(" << v.id << "," << v.value.l << ")" << "邻接表：";
                for (const auto &adj_v : v.value.adj)
                {
                    cout << "(" << adj_v.id << "," << adj_v.l << ") ";
                }
                cout << endl;
            }
#endif
       
            //先将每个查询点的匹配数量设置为0
            hash_map<VertexID,size_t> each_query_count;
            for(auto &each_query :q){
                each_query_count[each_query.first] = 0;
            }
            vector<VertexID> GMatchQ;           //保存结果的数据
            QueryPlanVertexVecList query_graph; //查询图
            size_t count = 0;
            bool query_last = false;

            query_graph = q.find(query_graph_id)->second;
            vector<query_judge> query_state =  combine_tag_table.find(query_graph_id)->second;
            count = 0;
            // cout << "query_graph_id: " << query_graph_id <<endl;
            // graph_matching3(query_graph,current_task_graph,GMatchQ,count,query_state);

            
            // cout << "直接匹配"<<endl;
            graph_matching_root(query_graph,task_graph, GMatchQ, count,query_state);

            each_query_count[query_graph_id] = count;
           
            // each_query_count[0] = current_task_graph.vertexes.size();
            global_task_graph_size_vec[thread_rank] = current_task_graph.vertexes.size();
            global_task_graph_id_vec[thread_rank] = current_task_graph.vertexes[0].id;

            // 将有效种子顶点写入到文件中，以便检查结果
            // write_result(current_task_graph);
            
            // 聚合结果
            GMatchAgg *agg = get_aggregator();
            agg->aggregate(each_query_count);

            // 结束任务
            return false;
        }

        // 含有多个任务时，则逐个对这些任务进行处理
        for (const auto &task_item : divied_task_map)
        {
            hash_set<VertexID> &pull_vertex = pull_vertex_map[task_item.first];
            GMatchTask *task_ptr = task_item.second;
            SubgraphT &current_task_graph = task_ptr->subG;
            // 任务不能结束，同时又无法拉取顶点继续扩展，则该任务不需要处理，不需要添加到任务队列中
            if (task_ptr->context < task_ptr->subG.max_step && pull_vertex.empty())
            {
                LOG_DEBUG("拆分任务后，%d 种子顶点任务提前结束，不添加到任务队列中\n", current_task_graph.vertexes[0].id);
                delete task_ptr;
            }
            else
            {
                add_task(task_ptr); // 拆分得到的任务可以继续执行，则添加到任务队列中
                // 拉取下一层顶点
                task_ptr->to_pull.clear(); // 清空之前拉取的顶点，从而存储下次需要拉取的顶点
                for (const auto &v_id : pull_vertex)
                    task_ptr->to_pull.push_back(v_id);

                task_ptr->context++;

                LOG_DEBUG("拆分任务后，%d 种子顶点任务的 context = %d \n", current_task_graph.vertexes[0].id, task_ptr->context);        
            }
        }
        return false; // 将当前任务拆分成多个任务后，结束掉当前任务
    }

    /**
     * frontier 是 task 中上一步拉取的顶点
     */
    virtual bool compute(SubgraphT &task_graph, ContextT &context, vector<VertexT *> &frontier)
    {   
        const vector<VertexID> &matched_query_graph_id =  task_graph.query_vertexs;
        // 判断是否含有多个任务
        // if (1 == matched_query_graph_id.size())
        //     return single_compute(task_graph, context, frontier); // 只有一个任务，则直接在该任务上按照单任务执行即可
        // else
        //     return multi_compute(task_graph, context, frontier);
        
        bool flag;
        if (1 == matched_query_graph_id.size())
            flag = single_compute(task_graph, context, frontier); // 只有一个任务，则直接在该任务上按照单任务执行即可
        else{
            // double start_time1 = get_current_time();
            flag = multi_compute(task_graph, context, frontier);
            // double stop_time1 = get_current_time();   
            // cout << "comm size: " << task_graph.comm_match.size() << " ---a task all time----  :" <<   stop_time1- start_time1 <<endl;
            // cout <<endl;
        }
        return flag;
    }
 
};

#endif