#ifndef GMATCHWORKER_H_
#define GMATCHWORKER_H_

#include "subg-dev.h"
#include "GMatchComper.h"
#include <limits>

class GMatchWorker : public Worker<GMatchComper>
{
public:
    GMatchWorker(int num_compers) : Worker<GMatchComper>(num_compers) {}

    virtual VertexT *toVertex(char *line)
    {
        VertexT *v = new VertexT;
        char *pch;
        pch = strtok(line, " \t");
        v->id = atoi(pch);
        pch = strtok(NULL, " \t");
        v->value.l = atoi(pch); // 标签为 int 类型

        // v->value.l = *pch; // 标签为 char 类型
        // v->value.l = 'a'; // 全部为同一个标签，从而退化成无标签匹配
        vector<AdjItem> &nbs = v->value.adj;
        AdjItem temp;
        while ((pch = strtok(NULL, " ")) != NULL)
        {
            temp.id = atoi(pch);
            pch = strtok(NULL, " ");
            temp.l = atoi(pch); // 标签为 int 类型
            // temp.l = *pch; // 标签为 char 类型
            // temp.l = 'a'; // 全部为同一个标签，从而退化成无标签匹配
            nbs.push_back(temp);
        }

        sort(nbs.begin(), nbs.end(), adj_comp);

        return v;
    }

    virtual void task_spawn(VertexT *v, vector<GMatchTask> &tcollector)
    {
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
                VertexT &root = *(query_graph_table.find(qv)->second);
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
                continue; // 当前组的查询图不能生成任务

            // 为当前组生成 task 
            GMatchTask t;
            t.subG.roots.push_back(*v);
            t.subG.query_vertexs.insert(t.subG.query_vertexs.end(), query_graph_id_set.begin(), query_graph_id_set.end());
            t.subG.max_step = max_step;
            addNode(t.subG, *v);
            t.context = 1;
            t.group_id = group->group_id;


            // 获取各个查询图根顶点的子查询点标签，从而便于拉取顶点
            hash_map<Label, bool> child_query_vertex; // key：子查询点标签，value：标记子查询点是否需要拉取
            vector<const GMatchVertex*> only_parent_query_vertex;
            for (const auto& root_id : query_graph_id_set)
            {
                get_child_query_vertex(query_plan_vertex_table.find(root_id)->second, child_query_vertex, 
                                       query_graph_table, query_plan_vertex_table, only_parent_query_vertex);
            }

            // 遍历邻接表，确定需要拉取的邻居点
            vector<AdjItem> &nbs = v->value.adj;
            // 拉取相应标签的顶点
            hash_map<VertexID, set<VertexID>> & candidate_vertexs = t.subG.candidate_vertexs;
            for (const auto &adj_v : nbs)
            {
                // 判断当前顶点是否需要拉取
                hash_map<Label, bool>::iterator child_query_vertex_it = child_query_vertex.find(adj_v.l);
                if (child_query_vertex_it != child_query_vertex.end())
                {   
                    // 判断当前顶点匹配到的查询点是否只有父节点
                    if (child_query_vertex_it->second)
                    {   // 如果只有父节点，则不需要拉取，直接加入到任务子图中
                        if (!t.subG.hasVertex(adj_v.id))
                        {
                            vector<VertexID> vec;
                            addNode(t.subG, adj_v.id, adj_v.l, vec);
                        }
                        // 添加边
                        addEdge_safe(t.subG, v->id, adj_v.id);

                        for (const auto &opqv : only_parent_query_vertex)
                        {
                            if (opqv->value.l == adj_v.l)
                                candidate_vertexs[opqv->id].insert(adj_v.id);
                        }
                    }
                    else
                    {   // 如果含有兄弟节点或子节点，则需要拉取
                        t.pull(adj_v.id);
                    }
                }
            }
            tcollector.push_back(t);           
        }
    }

    /**
     * 将邻接表 adj 与顶点列表 vec 进行交集运算
     * 
     * @param adj       邻接表数据
     * @param vec       顶点列表
     * @param result    邻接表数据与 vec 的交集结果，输出参数
     */
    void intersect_vertex(const vector<AdjItem> &adj, const vector<VertexT> &vec, vector<AdjItem> &result)
    {
        for (const auto &v : vec)
        {
            // 邻接表与 vec 作交集
            for (const auto &nb : adj)
            {
                if (nb.id == v.id)
                {
                    result.push_back(nb);
                }
            }
        }
    }

    //为每个查询图的每个查询点生成该点的查询计划
    void addPlan_list (QueryPlanVertex &v,hash_map<VertexID,int> &Id_map_index,hash_set<Label> &matchLable,
                hash_map<Label,vector<VertexID>> &matchLable_list,vector<query_judge> &query_combine_tag,
                QueryPlanVertexVecList &plan_list)
    {
        query_judge query_x;
        query_x.id =v.id;
        //查找每个查询点的父节点
        vector<AdjItem> v_parents = v.parent;
        vector<AdjItem> v_partner = v.partner;
        vector<int> parent_indext;
        for(int k = 0;k <v_parents.size(); k++){
            //如果
            if(Id_map_index.find(v_parents[k].id) != Id_map_index.end()){
                query_x.parents_indext.push_back(Id_map_index[v_parents[k].id]);
                parent_indext.push_back(Id_map_index[v_parents[k].id]);
            }
        }
        for(int k = 0;k < v_partner.size();k++){
            if(Id_map_index.find(v_partner[k].id) != Id_map_index.end()){
                query_x.parents_indext.push_back(Id_map_index[v_partner[k].id]);
                query_x.notree_indext.push_back(Id_map_index[v_partner[k].id]);
            }
        }

        //每个查询点的祖先节点
        // for(int k =0 ;k<query_x.parents_indext.size(); k++){
        //     int p_indext = query_x.parents_indext[k];
        //     vector<int> an_indext = query_combine_tag[p_indext].ancestor_indext;
        //     vector<VertexID> an_ids = query_combine_tag[p_indext].ancestor_ids;
        //     if(an_indext.size()!=0){
        //         query_x.ancestor_indext = an_indext;
        //         query_x.ancestor_ids = an_ids;
        //     }
        //     query_x.ancestor_indext.push_back(p_indext);
        //     query_x.ancestor_ids.push_back(plan_list[p_indext].id);
        // }
        
        // query_x.ancestor_ids.push_back(v.id);
       
        //输出父节点下标 
        // cout << "父节点下标： " ;
        // for(int k= 0;k<query_x.parents_indext.size();k++){
        //     cout << query_x.parents_indext[k] << "  ";
        // }
        // cout << endl;
        
        // cout << "非树边： " ;
        // for(int k= 0;k<query_x.notree_indext.size();k++){
        //     cout << query_x.notree_indext[k]<<"  ";
        // }
        // cout << endl;

        // 输出祖先节点下标 
        // cout << "祖先下标： " ;
        // for(int k= 0;k<query_x.ancestor_indext.size();k++){
        //     cout << query_x.ancestor_indext[k]<<"  ";
        // }
        // cout << endl;
        
        // //输出祖先节点id
        // cout << "祖先ID： " ;
        // for(int k= 0;k<query_x.ancestor_ids.size();k++){
        //     cout << query_x.ancestor_ids[k]<<"  ";
        // }
        // cout << endl;
        

        //计算该点的下标，并将该下标保存在与id的映射中
        int v_index = plan_list.size();
        Id_map_index[v.id] = v_index;
        // cout << "该点的下标是： " << v_index << endl;

        //判断该查询点的标签是否在前面出现过，如果出现则将该位置设置为ture，否则设置为false
        //与该位置标签相同的点记录其下标,并输出
        if(matchLable.find(v.l) != matchLable.end()){
            // cout << "该点与排序前的有相同标签" <<endl;
            query_x.reLabel=true;
            vector<VertexID> label_list = matchLable_list.find(v.l)->second;
            // cout << "相同节点下标： ";
            for(int k =0 ;k < label_list.size();k++){
                query_x.reLabel_indext.push_back(Id_map_index[label_list[k]]);
                // cout << query_x.reLabel_indext[k];
            }
            // cout << endl;
        }else{
            // cout << "该点与排序前的没有相同标签" <<endl;
            query_x.reLabel=false;
            matchLable.insert(v.l);
        }
        matchLable_list[v.l].push_back(v.id);   

        //将查询点的信息放入 query_combine_tag
        query_combine_tag.push_back(query_x);  
        plan_list.push_back(v);     
    }

// 存在公共部分的最后一个点特殊处理
void MatchingQueryPlancomm(QueryPlanVertexVecList order,hash_map<VertexID, vector<query_judge>> &combine_tag_table,
    VertexID indextID){
    // cout << "查询顶点为" << order[0].id <<"查询计划"<<endl;
    // QueryPlanVertexVecList order = it->second;
    vector<query_judge> query_combine_tag;                                              //该查询点的查询信息
    hash_map<VertexID,int> Id_map_index;                                                //记录每个查询点对应的下标
    hash_set<Label> matchLable;                                                         //记录查询点的标签种类
    hash_map<Label,vector<VertexID>> matchLable_list;
    QueryPlanVertexVecList plan_list;
    for(int j = 0;j<order.size();j++){
        QueryPlanVertex v = order[j];
        // cout << "v点id: " << v.id << " ---  v点标签L： " << v.l <<endl;
        addPlan_list(v,Id_map_index,matchLable,matchLable_list,query_combine_tag,plan_list);  
    }
    if(order[order.size()-2].child.size()==1 && order[order.size()-1].parent.size()==1 && !query_combine_tag[order.size()-1].reLabel){
        query_combine_tag[order.size()-2].tag_last = true;
    }
    combine_tag_table[indextID]=query_combine_tag;
}

//利用公共部分回溯的时候需要检查的信息
void comm_check_information(hash_map<KeyT, QueryGroup*> &query_graph_group,hash_map<VertexID, vector<query_judge>> &comm_combine_tag_table,
hash_map<VertexID, vector<query_judge>> &combine_tag_table,hash_map<VertexID, QueryPlanVertexVecList> &q,
hash_map<int, vector<QueryPlanVertex>> &comm_query_plan_map){
    for (const auto& group : query_graph_group)
    {
        vector<VertexID>& query_vertexs = group.second->query_vertexs; // 当前查询组包含的查询图
        if(query_vertexs.size()>1){
            vector<query_judge> &comm_query = comm_combine_tag_table[group.first];
            vector<QueryPlanVertex> &comm_order = comm_query_plan_map[group.first];
            for(VertexID queryID:query_vertexs){
            // cout<<"查询图id---queryID:"<<queryID<<endl;
           
            vector<query_judge> &query = combine_tag_table[queryID];
           
            QueryPlanVertexVecList &order = q[queryID];
            // cout << "8888"<<endl;
            hash_set<int> reindext;
            //根节点是否需要检查
            // cout <<comm_order[0].child.size()<<endl;
            // cout << order[0].child.size()<<endl;
            if((comm_order[0].child.size()) != (order[0].child.size())){
                //  cout << "6666"<<endl;
                query[0].ifcheck_indext.push_back(0);
                //  cout << "7777"<<endl;
                reindext.insert(0);     
            }
        //    cout << "9999"<<endl;
            for(int i =1;i<comm_query.size();i++){
                // 利用公共结果继续回溯的时候需要检查的点的下标
                if((comm_order[i].child.size()) != (order[i].child.size())){
                    if(reindext.find(i)==reindext.end()){
                        query[0].ifcheck_indext.push_back(i);
                        // cout<<"11111需要检查的点下标:"<<i<<endl;
                        reindext.insert(i);
                    }
                    
                }
                //单查询图与公共子图之间缺少的非树边
                vector<int> &notree_e = query[i].notree_indext;
                vector<int> &comm_notree_e = comm_query[i].notree_indext;
                if(notree_e.size() != comm_notree_e.size()){                       
                    for(int j:notree_e){
                        bool flag=true;
                        for(int m :comm_notree_e){
                            if(j==m){
                                flag=false;
                                break;
                            }
                        }
                        if(flag){
                            comm_notree eage;
                            eage.pointx=j;
                            eage.pointy=i;
                            query[0].notree_e.push_back(eage);
                            // cout<<"需要检查的非树边:( "<<j <<" , "<< i <<" )" <<endl;
                            if(reindext.find(j)==reindext.end()){
                                // cout<<"需要检查的点下标:"<<j<<endl;
                                query[0].ifcheck_indext.push_back(j);
                                reindext.insert(j);
                            }
                            if(reindext.find(i)==reindext.end()){
                                // cout<<"需要检查的点下标:"<<i<<endl;
                                query[0].ifcheck_indext.push_back(i);
                                reindext.insert(i);
                            }
                            
                            
                        }
                    }
                }
            }
        }
    
        }
        
    }
}



//没有公共部分的最后一个点特殊处理
void MatchingQueryPlan(QueryPlanVertexVec &plan_order_list,QueryPlanVertexVecList &plan_list,
    hash_map<VertexID, vector<query_judge>> &combine_tag_table,bool &query_last ){
    // 最后一层的顶点数量
    int lastSize = plan_order_list[plan_order_list.size()-1].size();
    int lastPsize = plan_order_list[plan_order_list.size()-1][0].parent.size(); 
    vector<query_judge> query_combine_tag;                                              //该查询点的查询信息
    hash_map<VertexID,int> Id_map_index;                                                //记录每个查询点对应的下标
    hash_set<Label> matchLable;                                                         //记录查询点的标签种类
    hash_map<Label,vector<VertexID>> matchLable_list;
    bool tag_1 = false;

    //cout << "lastSize： " << lastSize << ":" <<"lastPsize: " << lastPsize <<endl;
    if(lastSize == 1 & lastPsize == 1){
        // cout << "有最后的点" << endl;
        QueryPlanVertex childv = plan_order_list[plan_order_list.size()-1][0];  //最后一个点
        vector<AdjItem> parent = childv.parent;                                 //最后一个点父节点
        VertexID parentID = parent[0].id;                                       //最后一个点父节点id
        QueryPlanVertex parentv;                                                //保存最后一个节点
        for(int k=0;k < plan_order_list.size();k++)                             
        {
            if(k == plan_order_list.size()-2){                                  //进入倒数第二层
                for(int j=0;j <plan_order_list[k].size();j++){                  
                    if(plan_order_list[k][j].id !=  parentID){
                       addPlan_list(plan_order_list[k][j],Id_map_index,matchLable,matchLable_list,query_combine_tag,plan_list); 
                    }else{
                        parentv  = plan_order_list[k][j];
                    }
                    
                }
                addPlan_list(parentv,Id_map_index,matchLable,matchLable_list,query_combine_tag,plan_list);
            }else{
                for(int j=0;j <plan_order_list[k].size();j++)
                {
                    addPlan_list(plan_order_list[k][j],Id_map_index,matchLable,matchLable_list,query_combine_tag,plan_list);
                }
            }
        }
        tag_1=true;//最后一个点可以特殊处理
        // query_last = true;
    }else{
        // cout << "没有最后的点" << endl;
        for(int k=0;k < plan_order_list.size();k++)
        {
            for(int j=0;j <plan_order_list[k].size();j++)
            {
                addPlan_list(plan_order_list[k][j],Id_map_index,matchLable,matchLable_list,query_combine_tag,plan_list);
            }
        }
        // query_last=false;    
    }
    // 将查询计划放入combine_tag_table
    if(tag_1 & !query_combine_tag[plan_list.size()-1].reLabel){
        query_combine_tag[plan_list.size()-2].tag_last = true;
    }
    combine_tag_table[plan_order_list[0][0].id]=query_combine_tag;
}
    /**
     * 检测查询组内查询图的公共子图部分
     * 
     * @param query_graph_group   查询组
     */
    virtual void detect_common_subgraph(hash_map<KeyT, QueryGroup*> &query_graph_group)
    {
        // cout << "进入detect_common_subgraph" <<endl;
        hash_map<VertexID, vector<query_judge>> &combine_tag_table = *(hash_map<VertexID, vector<query_judge>> *)global_query_combine_tag;
        hash_map<VertexID, vector<query_judge>> &comm_combine_tag_table = *(hash_map<VertexID, vector<query_judge>> *)global_comm_combine_tag_table;
        hash_map<VertexID, VertexT*> &query_graph_table = *(hash_map<VertexID, VertexT*> *)global_query_graph_table; // 查询图顶点表
        hash_map<VertexID, vector<int>> &ifhasChild = *(hash_map<VertexID, vector<int>> *)global_ishasChild;
        hash_map<VertexID, QueryPlanVertexVec> &plan_order_map = *(hash_map<VertexID, QueryPlanVertexVec> *)global_query_plan_vertex_order;
        hash_map<VertexID, QueryPlanVertexVecList> &q = *(hash_map<VertexID, QueryPlanVertexVecList> *)global_query_plan_vertex_order_list;
        hash_map<VertexID, QueryGroup*> &lsh_query_graph_group_table = *(hash_map<VertexID, QueryGroup*>*)global_lsh_query_graph_group_table; // 查询图分组表，key：查询图 id，value：查询组，用于记录每个查询图对应的分组
        hash_map<VertexID, EdgeVector> &edge_vec_table = *(hash_map<VertexID, EdgeVector>*)global_same_layer_edge; // 非树边表
        hash_map<int, vector<QueryPlanVertex>> comm_query_plan_map; // 公共查询图的查询计划，key：查询组号，value：查询组对应的查询计划

        // 逐个查询组检测公共子结构
        for (const auto& group : query_graph_group)
        {
            vector<VertexID>& query_vertexs = group.second->query_vertexs; // 当前查询组包含的查询图
            if (query_vertexs.size() == 1)
            {
                continue;
            }
            
            // 检测公共子结构
            vector<AdjItem> common_adj; // 当前组查询图根顶点的的共同点（星状结构）
            vector<AdjItem> &adj1 = query_graph_table[query_vertexs[0]]->value.adj;
            hash_set<Label> labels_set; // 最终交集结果
            for (const auto &v : adj1)
                labels_set.insert(v.l);

            // 确定公共标签
            for (int i = 1; i < query_vertexs.size(); i++)
            {
                vector<AdjItem> &adj2 = query_graph_table[query_vertexs[i]]->value.adj;
                vector<Label> intersection_result; // 中间交集结果
                for (const auto &v : adj2)
                {
                    if (labels_set.find(v.l) != labels_set.end())
                    {
                        intersection_result.push_back(v.l);
                    }
                }
                labels_set.clear();
                labels_set.insert(intersection_result.begin(), intersection_result.end());
            }

            // 构建公共子结构
            vector<QueryPlanVertex> query_plan_vec(1 + labels_set.size()); // 存储公共查询点对应的查询计划点
            GMatchSubgraph &comm_subgraph = group.second->comm_subgraph;
            hash_map<VertexID, VertexID> &map_table = group.second->map_table;
            VertexID v_id = 1; // 公共子结构的查询点 id 
            GMatchVertex temp_v;
            temp_v.id = v_id;
            temp_v.value.l = query_graph_table[query_vertexs[0]]->value.l;
            comm_subgraph.addVertex(temp_v);
            query_plan_vec[v_id - 1].id = temp_v.id;
            query_plan_vec[v_id - 1].l = temp_v.value.l;

            hash_map<Label, VertexID> id_tabel; // 公共子结构的 id 表
            for (const auto &item : labels_set)
            {
                id_tabel[item] = ++v_id;
                temp_v.id = v_id;
                temp_v.value.l = item;
                comm_subgraph.addVertex(temp_v);
                addEdge_safe(comm_subgraph, 1, v_id);

                // 查询点对应的查询计划点
                query_plan_vec[v_id - 1].id = temp_v.id;
                query_plan_vec[v_id - 1].l = temp_v.value.l;

                AdjItem tmp_adj;
                tmp_adj.id = v_id;
                tmp_adj.l = item;
                query_plan_vec[0].child.push_back(tmp_adj);
                tmp_adj.id = query_plan_vec[0].id;
                tmp_adj.l = query_plan_vec[0].l;
                query_plan_vec[v_id - 1].parent.push_back(tmp_adj);
            }

            // 确定公共结构与每个查询图的映射关系
            for (int i = 0; i < query_vertexs.size(); i++)
            {
                vector<AdjItem> &adj2 = query_graph_table[query_vertexs[i]]->value.adj;
                size_t second_level_num = plan_order_map[query_vertexs[i]][1].size(); // 第二层的查询点数量
                QueryPlanVertexVecList &order = q[query_vertexs[i]];
                QueryPlanVertexVecList second_level(order.begin() + 1, order.begin() + second_level_num + 1); // 第二层的查询计划点
                order.erase(order.begin() + 1, order.begin() + second_level_num + 1);
                hash_map<Label, QueryPlanVertex> cur_query_graph_comm_vet; // 当前查询图第二层公共的查询计划点

                for (auto it = second_level.begin(); it != second_level.end(); /**/)
                {
                    auto it2 = id_tabel.find(it->l);
                    if (it2 != id_tabel.end() /* 当前标签属于公共点 */
                        && cur_query_graph_comm_vet.find(it->l) == cur_query_graph_comm_vet.end() /* 当前标签没有记录 */)
                    {
                        group.second->map_table[it->id] = it2->second;
                        cur_query_graph_comm_vet[it->l] = *it;
                        it = second_level.erase(it); // 在 second_level 中删除公共点，从而使得 second_level 中全部是特定点
                    }
                    else
                    {
                        it++;
                    }
                }
                // 重排序，将公共部分顶点的顺序放在最前面
                int pos = 1;
                for (const auto& v : cur_query_graph_comm_vet)
                {
                    order.insert(order.begin() + (pos++), v.second); // 插入第二层公共部分
                }
                order.insert(order.begin() + pos, second_level.begin(), second_level.end()); // 插入第二层非公共部分
            }

            // 确定公共的非树边
            bool has_non_tree_edge = true; // 标记是否有非树边
            hash_set<pair<Label, Label>, pair_hash> comm_non_tree_edge; // 公共的非树边集合（边由两个端点的标签组成）
            for (int i = 0; i < query_vertexs.size(); i++)
            {
                const hash_set<pair<VertexID, VertexID>, pair_hash> &edge = edge_vec_table[query_vertexs[i]][1];
                if (edge.empty())
                {
                    has_non_tree_edge = false;
                    break;
                }

                if (comm_non_tree_edge.empty())
                {
                    // 将 id 转换成标签
                    for (const auto &e : edge)
                    {
                        comm_non_tree_edge.insert(
                            make_pair(query_graph_table[e.first]->value.l, query_graph_table[e.second]->value.l));
                    }
                }
                else
                {
                    // 更新公共的非树边集合
                    hash_set<pair<Label, Label>, pair_hash> new_non_tree_edge;
                    for (const auto &e : edge)
                    {
                        pair<Label, Label> label_e = make_pair(query_graph_table[e.first]->value.l, query_graph_table[e.second]->value.l); // 标签组成的边
                        if (comm_non_tree_edge.find(label_e) != comm_non_tree_edge.end())
                            new_non_tree_edge.insert(label_e);
                    }
                    comm_non_tree_edge.swap(new_non_tree_edge);
                    if (comm_non_tree_edge.empty()) 
                    {
                        has_non_tree_edge = false;
                        break;
                    }
                }
            }

            if (has_non_tree_edge)
            {
                // 从公共子图中取出公共非树边相应的顶点，然后在公共子图中添加边
                for (const auto &e : comm_non_tree_edge)
                {
                    VertexID id1 = id_tabel.find(e.first)->second;
                    VertexID id2 = id_tabel.find(e.second)->second;
                    addEdge_safe(comm_subgraph, id1, id2);

                    AdjItem tmp_adj;
                    tmp_adj.l = e.second;
                    tmp_adj.id = id2;
                    query_plan_vec[id1 - 1].partner.push_back(tmp_adj);
                    tmp_adj.l = e.first;
                    tmp_adj.id = id1;
                    query_plan_vec[id2 - 1].partner.push_back(tmp_adj);
                }
            }

            comm_query_plan_map.insert(make_pair(group.first, query_plan_vec));
        }
        

        // for (const auto &item : comm_query_plan_map)
        // {
        //     cout << "查询组 " << item.first << " 的查询计划点：" << endl;

        //     for (const auto &pv : item.second)
        //     {
        //         cout << "查询计划点：(" << pv.id << "," << pv.l << ")" << endl;
        //         cout << "父节点：";
        //         for (const auto &v : pv.parent)
        //         {
        //             cout << "(" << v.id << "," << v.l << ") ";
        //         }
        //         cout << endl;
        //         cout << "兄弟节点：";
        //         for (const auto &v : pv.partner)
        //         {
        //             cout << "(" << v.id << "," << v.l << ") ";
        //         }
        //         cout << endl;
        //         cout << "子节点：";
        //         for (const auto &v : pv.child)
        //         {
        //             cout << "(" << v.id << "," << v.l << ") ";
        //         }
        //         cout << endl;
        //     }
        // }

        //每个查询图的查询点的计划
        hash_map<VertexID, QueryPlanVertexVecList> :: iterator it;
        it = q.begin();
        while(it != q.end()){
            QueryPlanVertexVecList order = it->second;
            VertexID indextID = it ->first;
            // QueryGroup* group_ptr = lsh_query_graph_group_table[it->first];            //查询组
            MatchingQueryPlancomm(order,combine_tag_table,indextID);
            ++it;
        }

        //公共查询组的查询计划
//      hash_map<int, vector<QueryPlanVertex>> comm_query_plan_map;
        hash_map<int, vector<QueryPlanVertex>>:: iterator it2;
        it2 = comm_query_plan_map.begin();
        while(it2 != comm_query_plan_map.end()){

            QueryPlanVertexVecList order = it2->second;
            VertexID indextID = it2 ->first;
            MatchingQueryPlancomm(order,comm_combine_tag_table,indextID);

            ++it2;
        }

        comm_check_information(query_graph_group,comm_combine_tag_table,combine_tag_table,q,comm_query_plan_map);
        
    }

    /**
     * 生成查询计划，确定查询顶点顺序
     */
    virtual void generateQueryPlan(VTable &query_graph_table, VertexVec &vec, 
        QueryVertexVec &order_list, QueryPlanVertexVec &plan_order_list,
        QueryPlanVertexVecList &plan_list, hash_map<KeyT, QueryPlanVertex> &query_plan_vertex_table,
        vector<int> &query_vertex_tag_list, bool &query_last, EdgeVector &edge_vec)
    {
        // 选择最佳根顶点，广度遍历查询图
        hash_map<VertexID, vector<query_judge>> &combine_tag_table = *(hash_map<VertexID, vector<query_judge>> *)global_query_combine_tag;
        QueryVertex *root; // 根顶点
        float best_cost = numeric_limits<float>::max();
        float cost = 0.0f;
        for (const auto &v : vec)
        {
            cost = 1.0f * data_vertex_label_frequency[v->value.l] / v->value.adj.size();
            if (cost < best_cost)
            {
                best_cost = cost;
                root = v;
            }
        }
        
        vector<QueryVertex> first_level;                    // 记录第一层的顶点，实际即记录根顶点
        int pop_num = 0;                                    // 记录每层出队元素的个数
        int current_level_vertex_num = 1;                   // 记录当前层的顶点个数，第一层只有根顶点，所以默认为 1
        int next_level_vertex_num = 0;                      // 记录下一层的顶点个数
        int level_num = 0;                                  // 层数
        hash_set<KeyT> visisted;                            // 标记已访问过的顶点
        queue<QueryVertex *> q;                             // 辅助队列

        q.push(root); // 查询图的根顶点入队
        visisted.insert(root->id);
        order_list.push_back(first_level);

        while (!q.empty())
        {
            QueryVertex *v = q.front();
            q.pop();
            pop_num++;
            order_list[level_num].push_back(*v);

            // 遍历 v 的邻接表，将未访问的顶点加入到队列中
            vector<AdjItem> &nbs = v->value.adj;; // 存储 v 的邻居点
            for (int i = 0; i < nbs.size(); i++)
            {
                if (visisted.find(nbs[i].id) == visisted.end())
                {
                    q.push(query_graph_table[nbs[i].id]);
                    visisted.insert(nbs[i].id);
                    next_level_vertex_num++;
                }
            }

            // 判断是否遍历完一层
            if (pop_num == current_level_vertex_num)
            {
                // 已遍历完当前层，则将计数重置，准备遍历下一层
                pop_num = 0;
                current_level_vertex_num = next_level_vertex_num;
                next_level_vertex_num = 0;

                // 判断是否为最后一层
                if (!q.empty())
                {
                    level_num++;
                    // 不是最后一层，则向 order 中添加一个 vector 用于保存下一层的顶点
                    vector<QueryVertex> next_level; // 存储下一层的查询顶点
                    order_list.push_back(next_level);
                }
            }
        }

        // 生成查询计划
        for (int i = 0; i < order_list.size(); i++)
        {
            vector<QueryVertex> &order = order_list[i];
            vector<QueryPlanVertex> plan_vertex_list;

            hash_set<pair<VertexID, VertexID>, pair_hash> edge;
            for (int j = 0; j < order.size(); j++)
            {
                // 将查询图的中查询点转换成查询计划点
                QueryVertex &v = order[j];
                QueryPlanVertex qpv;
                qpv.id = v.id;
                qpv.l = v.value.l;

                // 确定父节点
                if (i > 0)
                {
                    intersect_vertex(v.value.adj, order_list[i - 1], qpv.parent);
                }

                // 确定中间的边关系
                if (i != 0)
                {
                    intersect_vertex(v.value.adj, order_list[i], qpv.partner);
                    // 加入边
                    for (const auto &item : qpv.partner)
                    {
                        // 插入边的时候，按照 (小id,大id) 的规则插入，从而避免插入重复的边
                        if (v.id < item.id)
                            edge.insert(make_pair(v.id, item.id));
                        else
                            edge.insert(make_pair(item.id, v.id));
                    }
                }

                // 确定子节点
                if (i < order_list.size() - 1)
                {
                    intersect_vertex(v.value.adj, order_list[i + 1], qpv.child);
                }

                plan_vertex_list.push_back(qpv);
                query_plan_vertex_table.insert(make_pair(qpv.id, qpv));
            }

            plan_order_list.push_back(plan_vertex_list);
           
            edge_vec.push_back(edge);
        }

        //查询计划公共
        // cout << "查询计划点序" <<endl;
#if ENABLE_QUERY_GROUP_COMBINE == 1
        for(int k=0;k < plan_order_list.size();k++)
        {
            for(int j=0;j <plan_order_list[k].size();j++)
            {
                int degree = plan_order_list[k][j].child.size()+plan_order_list[k][j].parent.size()+plan_order_list[k][j].partner.size();
                plan_order_list[k][j].degree = degree;
                plan_list.push_back(plan_order_list[k][j]);
            //    cout << plan_order_list[k][j].id<<" ";
            }
        }
        // cout <<endl;
#endif       
       
        // 没有公共部分
#if ENABLE_QUERY_GROUP_COMBINE == 0
        MatchingQueryPlan(plan_order_list,plan_list,combine_tag_table,query_last);
#endif

#if ENABLE_DEBUG
        // 输出查询计划
        for (const auto &pv_list : plan_order_list) 
        {
            for (const auto &pv : pv_list)
            {
                cout << "查询计划点：(" << pv.id << "," << pv.l << ")" << endl;
                cout << "父节点：";
                for (const auto &v : pv.parent)
                {
                    cout << "(" << v.id << "," << v.l << ") ";
                }
                cout << endl;
                 cout << "兄弟节点：";
                for (const auto &v : pv.partner)
                {
                    cout << "(" << v.id << "," << v.l << ") ";
                }
                cout << endl;
                 cout << "子节点：";
                for (const auto &v : pv.child)
                {
                    cout << "(" << v.id << "," << v.l << ") ";
                }
                cout << endl;
            }
        }

#endif

    }
};

#endif