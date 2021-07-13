#ifndef GRAPHMATCHING_H_
#define GRAPHMATCHING_H_

#include "comm-struct.h"
#include "subg-dev.h"
#include "util/leapfrog.h"

// 输出列表结果
void print_vec(vector<VertexID> vec)
{
    for (int i = 0; i < vec.size(); ++i)
    {
        cout << vec[i] << " ";
    }
    cout << endl;
}

void print_vec(hash_set<VertexID> vec)
{
    for(VertexID id : vec)
    { 
        cout << id << " ";
    }
    cout << endl;
}

//-------------------
// add a node to graph: only id and label of v, not its edges
// must make sure g.hasVertex(v.id) == true !!!!!!

void addNode(GMatchSubgraph &g, GMatchVertex &v)
{
    GMatchVertex temp_v;
    temp_v.id = v.id;
    temp_v.value.l = v.value.l;
    temp_v.value.matched_query = v.value.matched_query;
    g.addVertex(temp_v);
}

void addNode(GMatchSubgraph &g, VertexID id, Label l, vector<VertexID> matched_query)
{
    GMatchVertex temp_v;
    temp_v.id = id;
    temp_v.value.l = l;
    temp_v.value.matched_query = matched_query;
    g.addVertex(temp_v);
}

void addEdge(GMatchSubgraph &g, VertexID id1, VertexID id2)
{
    GMatchVertex *v1, *v2;
    v1 = g.getVertex(id1);
    v2 = g.getVertex(id2);
    AdjItem temp_adj;
    temp_adj.id = v2->id;
    temp_adj.l = v2->value.l;
    v1->value.adj.push_back(temp_adj); // push_back 的时候，会将数据拷贝一份然后保存，因此下面的 temp_adj 在修改值时并不会影响 v1 这里的 Vector
    temp_adj.id = v1->id;
    temp_adj.l = v1->value.l;
    v2->value.adj.push_back(temp_adj);
}

/**
 * 相比 addEdge，本函数会在 add 之前检查插入的边是否已经存在，如果已经存在则不会重复插入边，即避免插入相同的边
 */
void addEdge_safe(GMatchSubgraph &g, VertexID id1, VertexID id2) //avoid redundancy
{
    GMatchVertex *v1, *v2;
    v1 = g.getVertex(id1);
    v2 = g.getVertex(id2);
    AdjItem adj1;
    adj1.id = v1->id;
    adj1.l = v1->value.l;
    AdjItem adj2;
    adj2.id = v2->id;
    adj2.l = v2->value.l;

    bool flag_add = false; // 标记是否添加边
    vector<AdjItem> &adj1_list = v1->value.adj;
    vector<AdjItem> &adj2_list = v2->value.adj;

    vector<AdjItem>::iterator pos1 = lower_bound(adj1_list.begin(), adj1_list.end(), adj2, adj_comp);
    if (pos1 != adj1_list.end())
    {
        if (pos1->id != id2) // 在邻接表中没有找到相应顶点，则添加边
            flag_add = true;
    }
    else
        flag_add = true;

    if (flag_add)
    {
        vector<AdjItem>::iterator pos2 = lower_bound(adj2_list.begin(), adj2_list.end(), adj1, adj_comp);
        
        if (v1->id == v2->id)
        {
            adj1_list.insert(pos1, adj2);
        }
        else
        {
            adj1_list.insert(pos1, adj2);
            adj2_list.insert(pos2, adj1);
        }
    }
}


/**
 * 获取查询计划点 qpv 的子查询点标签，并对这些子查询点标签标记是否需要拉取
 * 
 * @param   qpv                         查询计划点
 * @param   child_query_vertex          qpv 的子查询点标签 map，key：子查询点标签，value：标记子查询点是否需要拉，输出参数类型
 * @param   query_graph_table           查询图顶点表
 * @param   query_plan_vertex_table     查询计划点表
 */
void get_child_query_vertex(const QueryPlanVertex &qpv, hash_map<Label, bool> &child_query_vertex,
                            const hash_map<VertexID, GMatchVertex*> &query_graph_table, const hash_map<VertexID, QueryPlanVertex> &query_plan_vertex_table,
                            vector<const GMatchVertex*> &only_parent_query_vertex)
{
    hash_map<Label, bool>::iterator child_query_vertex_it;
    // 遍历 p_v 的子节点，获取标签
    for (const auto &adj_v : qpv.child)
    {
        // 判断该查询点是否只有父节点
        const GMatchVertex* adj_q_v = query_graph_table.find(adj_v.id)->second; // adj_v 对应的查询点
        const QueryPlanVertex &adj_p_v = query_plan_vertex_table.find(adj_v.id)->second; // adj_v 对应的查询计划点
        bool is_only_parent = (adj_q_v->value.adj.size() == adj_p_v.parent.size()); // 标记该查询点是否只有父节点

        if (is_only_parent) only_parent_query_vertex.push_back(adj_q_v);

        child_query_vertex_it = child_query_vertex.find(adj_v.l);
        if (child_query_vertex_it == child_query_vertex.end())
        {
            child_query_vertex.insert(make_pair(adj_v.l, is_only_parent));
        }
        else
        {
            child_query_vertex_it->second = child_query_vertex_it->second && is_only_parent;
        }
    }
}


bool nlf(hash_map<Label, int> &data_vertex_nlf, hash_map<Label, int> &query_vertex_nlf)
{
    if (data_vertex_nlf.size() >= query_vertex_nlf.size())
    {
        hash_map<Label, int>::iterator data_it;
        // 遍历查询点的邻居，判断数据点的邻居频率是否满足条件
        for (hash_map<Label, int>::iterator it = query_vertex_nlf.begin(); it != query_vertex_nlf.end(); ++it)
        {
            data_it = data_vertex_nlf.find(it->first);
            if (data_it == data_vertex_nlf.end() || data_it->second < it->second)
            {
                // 在数据点的邻居中未找到查询点相应标签的邻居，或者虽然找到，但是其频率小查询图中的频率
                return false;
            }
        }
    } 
    else 
    {
        // 数据图中顶点的 NLF 大小比查询图中顶点 query_vertex 的 NLF 小，则该顶点不可能成为 query_vertex 的候选点
        return false;
    }

    return true; // 数据点满足查询点的 NLF 条件，说明其能成为 query_vertex 的候选点
}


/**
 * 根据 NLF 对 data_vertex 进行过滤，判断其是否可能成为 query_vertex 的一个候选点。如果其能成为一个候选点，则返回 true；否则，返回 false。
 *
 * 注意：调用此函数前需要确保 data_vertex 与 query_vertex 的标签相同
 *
 * @param    data_vertex 	数据图上的数据点，即待过滤确定的点
 * @param    query_vertex   查询图中的查询点，即待匹配的点
 *
 * @return   如果 data_vertex 能成为 query_vertex 的候选点，则返回 true；否则，返回 false
 */
bool nlf_filter(GMatchVertex *data_vertex, QueryVertex &query_vertex,                        
                hash_map<VertexID, vector<AdjItem>> &adj_map, hash_set<Label> &query_vertex_label)
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

    hash_map<Label, int> query_nlf;
    vector<AdjItem> &query_adj = query_vertex.value.adj;
    for (int i = 0;  i < query_adj.size(); ++i) 
    {
        query_nlf[query_adj[i].l]++; // 相应标签频率加 1
    }
    
    bool flag = nlf(data_nlf, query_nlf);
    if (flag && adj_map.find(data_vertex->id) == adj_map.end())
        adj_map.insert(make_pair(data_vertex->id, trimed_data_adj));

    return flag;
}

bool nlf_filter(GMatchVertex *data_vertex, const QueryVertex &query_vertex)
{
    // 计算数据图、查询图中顶点的 NLF 信息
    hash_map<Label, int> data_nlf;
    vector<AdjItem> &data_adj = data_vertex->value.adj;
    for (int i = 0;  i < data_adj.size(); ++i) 
    {
        data_nlf[data_adj[i].l]++; // 相应标签频率加 1
    }

    hash_map<Label, int> query_nlf;
    const vector<AdjItem> &query_adj = query_vertex.value.adj;
    for (int i = 0;  i < query_adj.size(); ++i) 
    {
        query_nlf[query_adj[i].l]++; // 相应标签频率加 1
    }
    
    return nlf(data_nlf, query_nlf);
}


bool nlf_filter2(GMatchVertex *data_vertex, vector<AdjItem> query_adj)
{
    // 计算数据图、查询图中顶点的 NLF 信息
    hash_map<Label, int> data_nlf;
    vector<AdjItem> &data_adj = data_vertex->value.adj;
    for (int i = 0;  i < data_adj.size(); ++i) 
    {
        data_nlf[data_adj[i].l]++; // 相应标签频率加 1
    }

    hash_map<Label, int> query_nlf;
    
    for (int i = 0;  i < query_adj.size(); ++i) 
    {
        query_nlf[query_adj[i].l]++; // 相应标签频率加 1
    }
    
    return nlf(data_nlf , query_nlf);
}


//--------------------------------------------------------------------------------------------------------------
// 根据父节点的邻居表找到该查询点(存在重复标签 leapfrog_join)  
void ComputeCandvre(GMatchSubgraph & task_graph, const vector<int> &relabel_indext, vector<VertexID> & GMatchQ,
    const Label &u_l, vector<vector<VertexID>> & all_list,int list_indext,VertexID &v_id){
    GMatchVertex * v =  task_graph.getVertex(v_id);
    vector<AdjItem> & v_adj = v->value.adj;
    for(int j = 0; j <v_adj.size();++j){
        if (v_adj[j].l== u_l){
            bool flag = true;
            for(int i = 0; i <relabel_indext.size(); ++i){   
                if(GMatchQ[relabel_indext[i]] == v_adj[j].id){
                    flag = false;
                    break;
                }
            }
            if(flag) all_list[list_indext].push_back(v_adj[j].id);    
        }
    }
}

// vector<VertexID> & firstCandidate
void ComputeCandvre(GMatchSubgraph & task_graph, const vector<int> &relabel_indext, vector<VertexID> & GMatchQ,
    const Label &u_l, vector<VertexID> & firstCandidate,VertexID &v_id){
    GMatchVertex * v =  task_graph.getVertex(v_id);
    vector<AdjItem> & v_adj = v->value.adj;
    for(int j = 0; j <v_adj.size();++j){
        if (v_adj[j].l== u_l){
            bool flag = true;
            for(int i = 0; i <relabel_indext.size(); ++i){   
                if(GMatchQ[relabel_indext[i]] == v_adj[j].id){
                    flag = false;
                    break;
                }
            }
            if(flag) firstCandidate.push_back(v_adj[j].id);    
        }
    }
}

//vector<AdjItem> & firstCandidate
void ComputeCandvre(GMatchSubgraph & task_graph, const vector<int> &relabel_indext, vector<VertexID> & GMatchQ,
    const Label &u_l, vector<AdjItem> & firstCandidate,VertexID &v_id){
    GMatchVertex * v =  task_graph.getVertex(v_id);
    vector<AdjItem> & v_adj = v->value.adj;
    for(int j = 0; j <v_adj.size();++j){
        if (v_adj[j].l== u_l){
            bool flag = true;
            for(int i = 0; i <relabel_indext.size(); ++i){   
                if(GMatchQ[relabel_indext[i]] == v_adj[j].id){
                    flag = false;
                    break;
                }
            }
            if(flag) firstCandidate.push_back(v_adj[j]);    
        }
    }
}

// 根据父节点的邻居表找到该查询点(不存在重复标签 leapfrog_join)  
void ComputeCandv(GMatchSubgraph & task_graph, vector<VertexID> & GMatchQ,
    const Label &u_l, vector<vector<VertexID>> & all_list,int list_indext,VertexID &v_id){
    GMatchVertex * v =  task_graph.getVertex(v_id);
    vector<AdjItem> & v_adj = v->value.adj;
    for(int j = 0; j <v_adj.size();++j){
        if (v_adj[j].l== u_l){
            all_list[list_indext].push_back(v_adj[j].id);      
        }
    }
}

void ComputeCandv(GMatchSubgraph & task_graph, vector<VertexID> & GMatchQ,
    const Label &u_l, vector<VertexID> & firstCandidate,VertexID &v_id){
    GMatchVertex * v =  task_graph.getVertex(v_id);
    vector<AdjItem> & v_adj = v->value.adj;
    for(int j = 0; j <v_adj.size();++j){
        if (v_adj[j].l== u_l){
            firstCandidate.push_back(v_adj[j].id);      
        }
    }
}

void ComputeCandv(GMatchSubgraph & task_graph, vector<VertexID> & GMatchQ,
    const Label &u_l, vector<AdjItem> & firstCandidate,VertexID &v_id){
    GMatchVertex * v =  task_graph.getVertex(v_id);
    vector<AdjItem> & v_adj = v->value.adj;
    for(int j = 0; j <v_adj.size();++j){
        if (v_adj[j].l== u_l){
            firstCandidate.push_back(v_adj[j]);      
        }
    }
}


// 根据父节点的邻居表找到该查询点
void ComputeCandvP(GMatchSubgraph & task_graph,const vector<int> &p_indext,vector<VertexID> & GMatchQ,
    vector<VertexID> &first_List,vector<VertexID> &second_List){   
    for(int i = 1; i < p_indext.size(); ++i){
        GMatchVertex * v =  task_graph.getVertex(GMatchQ[p_indext[i]]);
        vector<AdjItem> & v_adj = v->value.adj;
        for(int j = 0; j <v_adj.size();++j){
            if(find(first_List.begin(), first_List.end(),v_adj[j].id)!=first_List.end()){
                second_List.push_back(v_adj[j].id);
            }
        }
        if(second_List.size()==0){
            first_List.clear();
            break;
        }else{
            first_List.clear();
            // first_List.resize(second_List.size());
            first_List.swap(second_List);
        }
    }  
            
    
}

//计算每个查询点的候选点
void ComputeLC(const vector<QueryPlanVertex> & query_graph,GMatchSubgraph & task_graph,int &Mindext,
    vector<VertexID> & GMatchQ,const vector<query_judge> &query_state,vector<VertexID> &intersection_result){
    // 查询点信息
    const QueryPlanVertex &u = query_graph[Mindext];          //查询点
    const VertexID &u_id = u.id;                              //取得查询点的id
    const Label &u_l = u.l;                                   //取得查询点标签

    //查询计划中保存的信息
    const query_judge &query_x = query_state[Mindext];         //取得查询点的各个状态
    const vector<int> &p_indext = query_x.parents_indext;      //取得父节点的下标
    const bool &relabel = query_x.reLabel;                     //排序前是否有相同的标签
    const vector<int> &relabel_indext = query_x.reLabel_indext;//相同标签的下标
    int p_num = p_indext.size();                         //父节点数量
    
    vector<VertexID> first_List;
    vector<VertexID> second_List;
    //如果有重复标签
    if(relabel){
        // 对于第一个父节点处理
        VertexID v_id = GMatchQ[p_indext[0]];
        GMatchVertex * v =  task_graph.getVertex(v_id);
        vector<AdjItem> & v_adj = v->value.adj;
        for(int j = 0; j <v_adj.size();++j){
            if (v_adj[j].l== u_l){
                bool flag = true;
                for(int i = 0; i <relabel_indext.size(); ++i){   
                    if(GMatchQ[relabel_indext[i]] == v_adj[j].id){
                        flag = false;
                        break;
                    }
                }
                if(flag) first_List.push_back(v_adj[j].id);    
            }
        }
        //其他点的处理
        if(p_indext.size()>1 && first_List.size()>0){
            ComputeCandvP(task_graph,p_indext,GMatchQ,first_List,second_List);
        }
        // intersection_result.resize(first_List.size());
        intersection_result.swap(first_List);   
    }else{
        VertexID v_id = GMatchQ[p_indext[0]];
        GMatchVertex * v =  task_graph.getVertex(v_id);
        vector<AdjItem> & v_adj = v->value.adj;
        for(int j = 0; j <v_adj.size();++j){
            if (v_adj[j].l== u_l){
                first_List.push_back(v_adj[j].id);      
            }
        }
        if(p_indext.size()>1 && first_List.size()>0){
            ComputeCandvP(task_graph,p_indext,GMatchQ,first_List,second_List) ;          
        }

        // intersection_result.resize(first_List.size());
        intersection_result.swap(first_List);

        
    }
}


// 每个查询点的候选点(leapfrog_join)
void ComputeLC_join(const vector<QueryPlanVertex> & query_graph,GMatchSubgraph & task_graph,int& Mindext,
    vector<VertexID> & GMatchQ, const vector<query_judge> &query_state,vector<VertexID> &intersection_result)
{
    // 查询点信息
    const QueryPlanVertex &u = query_graph[Mindext];            //查询点
    const VertexID &u_id = u.id;                                //取得查询点的id
    const Label &u_l = u.l;                                     //取得查询点标签

    //查询计划中保存的查询点的信息
    const query_judge &query_x = query_state[Mindext];          //取得查询点的各个状态
    const vector<int> &p_indext = query_x.parents_indext;       //取得父节点的下标
    const bool &relabel = query_x.reLabel;                      //排序前是否有相同的标签
    const vector<int> &relabel_indext = query_x.reLabel_indext; //相同标签的下标
    int p_num = p_indext.size();                                //父节点数量
    
    // vector<VertexID> intersection_result;
    vector<vector<VertexID>> all_list;                          //需要做交集的集合
    all_list.resize(p_num);

    //如果有重复标签
    if(relabel){
        // 对于第一个父节点处理
        VertexID v_id = GMatchQ[p_indext[0]];
        ComputeCandvre(task_graph,relabel_indext,GMatchQ,u_l,all_list,0,v_id);                      
        //其他点的处理
        if(p_indext.size()>1 && all_list[0].size()>0){
            bool tag_3 =true;
            for(int i = 1 ; i < p_indext.size();++i){
                ComputeCandv(task_graph,GMatchQ,u_l, all_list,i,GMatchQ[p_indext[i]]);
                if(all_list[i].size()==0){
                    tag_3=false;
                    break;
                }
            }  
            if(tag_3){
                leapfrog_join(all_list,intersection_result); 
            }  
        }else{
            intersection_result.resize(all_list[0].size());
            intersection_result.swap(all_list[0]);

        }
    }
    
    
    else{
        VertexID v_id = GMatchQ[p_indext[0]];
        ComputeCandv(task_graph,GMatchQ,u_l, all_list,0,v_id);
        if(p_indext.size()>1 && all_list[0].size()>0){
            bool tag_3 =true;
            for(int i = 1 ; i < p_indext.size();++i){
                ComputeCandv(task_graph,GMatchQ,u_l, all_list,i,GMatchQ[p_indext[i]]);
                if(all_list[i].size()==0){
                    tag_3=false;
                    break;
                }
            }  
            if(tag_3){
                leapfrog_join(all_list,intersection_result); 
            }
             
        }else{

            intersection_result.resize(all_list[0].size());
            intersection_result.swap(all_list[0]);

        }
    }
}


void ComputeLC_join2(const vector<QueryPlanVertex> & query_graph,GMatchSubgraph & task_graph,int& Mindext,
    vector<VertexID> & GMatchQ, const vector<query_judge> &query_state,vector<VertexID> &intersection_result)
{
    // 查询点信息
    const QueryPlanVertex &u = query_graph[Mindext];            //查询点
    const VertexID &u_id = u.id;                                //取得查询点的id
    const Label &u_l = u.l;                                     //取得查询点标签

    //查询计划中保存的查询点的信息
    const query_judge &query_x = query_state[Mindext];          //取得查询点的各个状态
    const vector<int> &p_indext = query_x.parents_indext;       //取得父节点的下标
    const bool &relabel = query_x.reLabel;                      //排序前是否有相同的标签
    const vector<int> &relabel_indext = query_x.reLabel_indext; //相同标签的下标
    int p_num = p_indext.size();                                //父节点数量
    
    // vector<VertexID> intersection_result;
    vector<AdjItem> firstCandidate;                            //对第一个点候选点处理的结果
    vector<vector<AdjItem> *> all_list;                         //需要做交集的集合
    // all_list.resize(p_num);
    
    for(int i = 0; i < p_indext.size(); ++i){
        VertexID v_id = GMatchQ[p_indext[i]];
       
        if(p_num == 1){
            // cout <<"1111"<<endl;
            if(relabel){ 
                ComputeCandvre(task_graph,relabel_indext,GMatchQ,u_l,intersection_result,v_id);
            }else{
                ComputeCandv(task_graph,GMatchQ,u_l, intersection_result,v_id);
            }
            break;
            //  cout <<"1111"<<endl;
        }
       
        else{
            // cout <<"2222"<<endl;
            if(i==0){
                if(relabel){ 
                    ComputeCandvre(task_graph,relabel_indext,GMatchQ,u_l,firstCandidate,v_id);
                }else{
                    ComputeCandv(task_graph,GMatchQ,u_l, firstCandidate,v_id);
                }
                if(firstCandidate.size()!=0){
                    all_list.push_back(&firstCandidate);
                }else{
                    break;
                }
                
            }else{
                GMatchVertex * v =  task_graph.getVertex(v_id);
                if(v->value.adj.size()!=0){
                    all_list.push_back(&(v->value.adj));
                } else{
                    all_list.clear();
                    break;
                }
            }
            // cout <<"2222"<<endl;
              
        }
        
    }

    // cout <<"3333"<<endl;
    if(all_list.size()>1){
        // cout << "aa" << endl;
        leapfrog_join(all_list,intersection_result);
        // cout << "bb" << endl;

    }
    // cout <<"3333"<<endl;  
    
    // cout <<"查询点："<<u_id<<endl;
    // for(VertexID id :intersection_result){
    //     cout << id<<" ";
    // }
    // cout << endl;

}

void comm_matching(GMatchSubgraph & task_graph,vector<QueryVertex> &comm_subgraph_vertexes,vector<vector<VertexID>> &comm_matchQ,
         vector<VertexID> & MatchQ,vector<query_judge> &comm_tag)
{
    int Mindext = MatchQ.size();
    if(Mindext == comm_subgraph_vertexes.size()){       //进入到最后一个点的时候
        // print_vec(MatchQ);
        comm_matchQ.push_back(MatchQ);
    }
    else if(Mindext ==0){
        vector<GMatchVertex> & root= task_graph.roots;           //能够匹配的根节点
        // vector<QueryVertex> & comm_ver = comm_subgraph.vertexes;  //公共结构中的点
        for(int i=0;i<root.size();i++) {
            GMatchVertex &v = root[i];
            vector<AdjItem> & v_adj = v.value.adj;//根节点邻居表 
            MatchQ.push_back(v.id); 
            comm_matching(task_graph,comm_subgraph_vertexes,comm_matchQ,MatchQ,comm_tag);
            MatchQ.pop_back(); 
        }
    }else{
        QueryVertex &u = comm_subgraph_vertexes[Mindext];           //查询点
        VertexID &u_id = u.id;                                      //取得查询点的id
        Label &u_l = u.value.l;                                     //取得查询点标签

        //查询计划中保存的信息
        query_judge &query_x = comm_tag[Mindext];         //取得查询点的各个状态
        vector<int> &p_indext = query_x.parents_indext;      //取得父节点的下标
        bool &relabel = query_x.reLabel;                     //排序前是否有相同的标签
        vector<int> &relabel_indext = query_x.reLabel_indext;//相同标签的下标
        int p_num = p_indext.size();                         //父节点数量
        
        // vector<VertexID> intersection_result;
        vector<VertexID> vcandidate;
        vector<vector<VertexID>> all_list;                    //需要做交集的集合
        all_list.resize(p_num);
       //如果有重复标签
        if(relabel){
            // 对于第一个父节点处理
            VertexID v_id = MatchQ[p_indext[0]];
            ComputeCandvre(task_graph,relabel_indext,MatchQ,u_l,all_list,0,v_id);                      
            //其他点的处理
            if(p_indext.size()>1 && all_list[0].size()>0){
                bool tag_3 =true;
                for(int i = 1 ; i < p_indext.size();++i){
                    ComputeCandv(task_graph,MatchQ,u_l, all_list,i,MatchQ[p_indext[i]]);
                    if(all_list[i].size()==0){
                        tag_3=false;
                        break;
                    }
                }  
                if(tag_3){
                    leapfrog_join(all_list,vcandidate); 
                }  
            }else{
                vcandidate.resize(all_list[0].size());
                vcandidate.swap(all_list[0]);

            }
        }else{
            VertexID v_id = MatchQ[p_indext[0]];
            ComputeCandv(task_graph,MatchQ,u_l, all_list,0,v_id);
            if(p_indext.size()>1 && all_list[0].size()>0){
                bool tag_3 =true;
                for(int i = 1 ; i < p_indext.size();++i){
                    ComputeCandv(task_graph,MatchQ,u_l, all_list,i,MatchQ[p_indext[i]]);
                    if(all_list[i].size()==0){
                        tag_3=false;
                        break;
                    }
                }  
                if(tag_3){
                    leapfrog_join(all_list,vcandidate); 
                }
                
            }else{

                vcandidate.resize(all_list[0].size());
                vcandidate.swap(all_list[0]);

            }
        }
        if(vcandidate.size()==0);
        else{
            for(VertexID id : vcandidate)
            {
                MatchQ.push_back(id);
                comm_matching(task_graph,comm_subgraph_vertexes,comm_matchQ,MatchQ,comm_tag);
                MatchQ.pop_back();    
            }
        }    
    }  
}


// 公共部分回溯
void comm_matching2(GMatchSubgraph & task_graph,GMatchSubgraph &comm_subgraph,vector<vector<VertexID>> &comm_matchQ,
         vector<VertexID> & MatchQ,vector<vector<VertexID *>> &matchQList,vector<query_judge> &comm_tag)
{
    int Mindext = MatchQ.size();
    if(Mindext == comm_subgraph.vertexes.size()){       //进入到最后一个点的时候
        // print_vec(MatchQ);
        comm_matchQ.push_back(MatchQ);
    }
    else{  //其他点
        query_judge &query_x = comm_tag[Mindext];
        vector<int> &relabel_indext = query_x.reLabel_indext;
        bool relabel = query_x.reLabel;
        if(!relabel){
            //不存在重复点的时候
           for(int i = 0;i<matchQList[Mindext].size();++i){
                MatchQ.push_back(*matchQList[Mindext][i]); 
                comm_matching2(task_graph,comm_subgraph,comm_matchQ,MatchQ,matchQList,comm_tag);
                //matchQList[Mindext].pop_back();
                MatchQ.pop_back();
            }       
        }else{   
            //重复点的下标
            for(int j =0 ;j<relabel_indext.size();j++){
               for(int k = 0;k<matchQList[Mindext].size();++k){
                   if(MatchQ[relabel_indext[j]]!=*matchQList[Mindext][k]){
                        MatchQ.push_back(*matchQList[Mindext][k]); 
                        comm_matching2(task_graph,comm_subgraph,comm_matchQ,MatchQ,matchQList,comm_tag);
                        //matchQList[Mindext].pop_back();
                        MatchQ.pop_back();
                   }   
                }  
            }
        }  
        
    }
}

void comm_matching3(vector<QueryVertex> &comm_ver,vector<vector<VertexID>> &comm_matchQ,
vector<VertexID> & MatchQ,vector<query_judge> &comm_tag,vector<vector<VertexID>> &candidate,long long &allcommS)
{
    
    int Mindext = MatchQ.size();
    // vector<VertexID> & candidatelist = candidate[Mindext];
    if(Mindext == comm_ver.size()){       //进入到最后一个点的时候
        // print_vec(MatchQ);
        // comm_matchQ[allcommS].resize(comm_ver.size());
        // copy(MatchQ.begin(),MatchQ.end(),comm_matchQ[allcommS].begin());
        // comm_matchQ[allcommS].insert(comm_matchQ[allcommS].begin(),MatchQ.begin(),MatchQ.end());
        comm_matchQ[allcommS].assign(MatchQ.begin(),MatchQ.end());
        ++allcommS;
        // comm_matchQ.push_back(MatchQ);
    }
    else if(Mindext==0){ //第一个点
        // vector<GMatchVertex> &root= task_graph.roots;           //能够匹配的根节点
        // vector<QueryVertex> comm_ver = comm_subgraph.vertexes;  //公共结构中的点
        for(VertexID i : candidate[Mindext]) {
            // GMatchVertex &v = root[i];
            MatchQ.push_back(i); 
            comm_matching3(comm_ver,comm_matchQ,MatchQ,comm_tag,candidate,allcommS);
            MatchQ.pop_back(); 
        }
    }else{  //其他点
        query_judge & query_x = comm_tag[Mindext];
        vector<int> & relabel_indext = query_x.reLabel_indext;
        bool relabel = query_x.reLabel;
        if(!relabel){//不存在重复点的时候
           for(VertexID i : candidate[Mindext]){
                MatchQ.push_back(i); 
                comm_matching3(comm_ver,comm_matchQ,MatchQ,comm_tag,candidate,allcommS);
                MatchQ.pop_back();
            }       
        }else{   
            //重复点的下标
            for(int j =0 ;j<relabel_indext.size();j++){
               for(VertexID k : candidate[Mindext]){
                   if(MatchQ[relabel_indext[j]]!=k){
                        MatchQ.push_back(k);
                        comm_matching3(comm_ver,comm_matchQ,MatchQ,comm_tag,candidate,allcommS);
                        MatchQ.pop_back();
                   }   
                }  
            }
        }     
    }
}



//根据公共部分回溯匹配得到最终结果
size_t graph_matching_comm(const vector<QueryPlanVertex> & query_graph,GMatchSubgraph & task_graph,
        vector<vector<VertexID>> &comm_matchQ,vector<VertexID> & GMatchQ, size_t & count,
        const vector<query_judge> &query_state)
{
    int Mindext = GMatchQ.size();
    if(Mindext == query_graph.size()){       //进入到最后一个点的时候
        // print_vec(GMatchQ);
        count ++;
    } 
    else{
        //查询点的信息
        vector<VertexID> vcandidate;
        
#if ENABLE_LEAPFORG_JOIN == 0
        // 普通做交集
        ComputeLC(query_graph,task_graph,Mindext,GMatchQ,query_state,vcandidate);
#elif ENABLE_LEAPFORG_JOIN == 1
        // leapfrog_join
        ComputeLC_join(query_graph, task_graph,Mindext,GMatchQ,query_state,vcandidate);
#endif

        //如果候选点数量为0；
        if(vcandidate.size()==0);
        else if (Mindext == query_graph.size()-1) {
            // for(VertexID id : vcandidate)
            // {
            //     GMatchQ.push_back(id);
            //     // print_vec(GMatchQ);
            //     count++;
            //     GMatchQ.pop_back();    
            // }
            count = count + vcandidate.size();
        }
        else if(query_state[Mindext].tag_last){
            for(VertexID id : vcandidate)
            {                             
                GMatchQ.push_back(id);
                GMatchVertex * v =  task_graph.getVertex(id);
                Label ue_l = query_graph[Mindext+1].l;
                vector<AdjItem> & v_adj = v->value.adj;
                for(int j = 0 ; j <v_adj.size();j++){                   
                    if(v_adj[j].l == ue_l){
                        // GMatchQ.push_back(v_adj[j].id);
                        // print_vec(GMatchQ);
                        count++;
                        // GMatchQ.pop_back();
                    }                   
                }
                GMatchQ.pop_back(); 
            }    
        }
        else{
            for(VertexID id : vcandidate)
            {
                GMatchQ.push_back(id);
                graph_matching_comm(query_graph,task_graph,comm_matchQ,GMatchQ,count,query_state);
                GMatchQ.pop_back();    
            }
        }    
    }

}



// 单个子图匹配结果
size_t graph_matching3(const vector<QueryPlanVertex> & query_graph,GMatchSubgraph & task_graph, 
        vector<VertexID> & GMatchQ, size_t & count, const vector<query_judge> &query_state)
{
    int Mindext = GMatchQ.size();           //当前匹配到的查询点的下标
    // cout <<"第"<< Mindext<<"个顶点"<<endl;
    if(Mindext == query_graph.size() ){       //进入到最后一个点的时候
        ++ count;
        // print_vec(GMatchQ);
    }
    else{

        vector<VertexID> vcandidate;    //保存查询点得到的候选点

#if ENABLE_LEAPFORG_JOIN == 0
        // 普通做交集
        ComputeLC( query_graph,task_graph,Mindext,GMatchQ,query_state,vcandidate);
#elif ENABLE_LEAPFORG_JOIN == 1
        // leapfrog_join
        // cout <<"ComputeLC_join2"<<endl;
        ComputeLC_join2(query_graph, task_graph,Mindext,GMatchQ,query_state,vcandidate);
        // cout <<"ComputeLC_join3"<<endl;
#endif

        //如果候选点数量为0；
        // cout <<"结果1"<<endl;
        if(vcandidate.size()==0);

        else if (Mindext == query_graph.size()-1) {//如果查询点是最后的点候选点
            for(VertexID id : vcandidate)
            {
                GMatchQ.push_back(id);
                // print_vec(GMatchQ);
                count ++;
                GMatchQ.pop_back();    
            }
            // count = count + vcandidate.size();
        }

        else if(query_state[Mindext].tag_last){ //如果最后点可以特殊处理
            for(VertexID id : vcandidate)
            {                             
                GMatchQ.push_back(id);
                GMatchVertex * v =  task_graph.getVertex(id);
                Label ue_l = query_graph[Mindext+1].l;
                vector<AdjItem> & v_adj = v->value.adj;
                for(int j = 0 ; j <v_adj.size();j++){                   
                    if(v_adj[j].l == ue_l){
                        GMatchQ.push_back(v_adj[j].id);
                        // print_vec(GMatchQ);
                        count++;
                        GMatchQ.pop_back();

                        // count++;
                    }                   
                }
                GMatchQ.pop_back(); 
            }    
        }
        else{
            for(VertexID id : vcandidate)//不是最后的点
            {
                GMatchQ.push_back(id);
                graph_matching3(query_graph,task_graph, GMatchQ, count,query_state);
                GMatchQ.pop_back();    
            }
        }    
        // cout <<"结果2"<<endl;
    }
}


size_t graph_matching_root(const vector<QueryPlanVertex> & query_graph,GMatchSubgraph & task_graph, 
        vector<VertexID> & GMatchQ, size_t & count, const vector<query_judge> &query_state)
{
    // cout <<"进入graph_matching_root"<<endl;
    vector<GMatchVertex> &root= task_graph.roots;    
    for(int i=0;i<root.size();i++) {
        // cout <<"111"<<endl;
        GMatchVertex & v = root[i];
        // cout <<"根节点root: " << v.id <<endl;
        GMatchQ.push_back(v.id); 
        graph_matching3(query_graph,task_graph, GMatchQ, count,query_state);
        GMatchQ.pop_back();
        // cout <<"222"<<endl;
    }
}


void comm_matching_root(GMatchSubgraph & task_graph,GMatchSubgraph &comm_subgraph,vector<vector<VertexID>> &comm_matchQ,
    vector<VertexID> & MatchQ,vector<vector<VertexID *>> &matchQList,vector<query_judge> &comm_tag){
    cout <<"进入comm_matching_root"<<endl;
    vector<GMatchVertex> & root= task_graph.roots;           //能够匹配的根节点
    vector<QueryVertex> & comm_ver = comm_subgraph.vertexes;  //公共结构中的点
    for(int i=0;i<root.size();i++) {
        matchQList.clear();
        matchQList.resize(comm_subgraph.vertexes.size());
        GMatchVertex &v = root[i];
        vector<AdjItem> & v_adj = v.value.adj;//根节点邻居表

        //查询星点候选点
        for(int j=0;j<v_adj.size();j++){      //遍历邻居表得到需要的点
            for(int k=1;k<comm_ver.size();k++){
                if(v_adj[j].l==comm_ver[k].value.l){
                    matchQList[k].push_back(&v_adj[j].id);
                }
            }
        }
        
        MatchQ.push_back(v.id); 
        comm_matching2(task_graph,comm_subgraph,comm_matchQ,MatchQ,matchQList,comm_tag);
        MatchQ.pop_back(); 
    }
}


size_t graph_matching_comm_root(const vector<QueryPlanVertex> & query_graph,GMatchSubgraph & task_graph,
        vector<vector<VertexID>> &comm_matchQ,vector<VertexID> & GMatchQ, size_t & count,
        const vector<query_judge> &query_state)
{
    // cout <<"进入graph_matching_comm_root"<<endl;
    int Mindext = GMatchQ.size();
    const vector<comm_notree> &notree_e = query_state[0].notree_e;
    if(notree_e.size()==0){
        // 非树边不存在 
        // cout<<"comm_matchQ[0].size():"<<comm_matchQ[0].size()<<endl;
        // cout<<"query_graph.size():"<<query_graph.size()<<endl;
        // cout<<"query_state[0].ifcheck_indext.size():"<<query_state[0].ifcheck_indext.size()<<endl;
        if(comm_matchQ[0].size()==query_graph.size() && query_state[0].ifcheck_indext.size()==0){
            // 查询图 == 公共子图
            count=comm_matchQ.size();
            return count;
        }else{
            // 查询图 != 公共子图
            for(int i = 0; i <comm_matchQ.size();i++){           
                vector<VertexID> &match = comm_matchQ[i];
                bool tag_2 = true;
                for(int j = 0; j <match.size();++j){
                    if(!task_graph.hasVertex(match[j])){
                        tag_2 = false;
                        break;
                    }
                }
                if(tag_2){
                  
                    GMatchQ.assign(match.begin(),match.end());
                    graph_matching_comm(query_graph,task_graph,comm_matchQ,GMatchQ,count,query_state);
                }

            }
        }     
    }else{                 
        //非树边存在 && 查询图 != 公共子图
        for(int i = 0; i <comm_matchQ.size();i++){          
            vector<VertexID> &match = comm_matchQ[i];
            const vector<int> &ifcheck_indext = query_state[0].ifcheck_indext;
            bool flag;                      //标示该非树边是否存在
            bool tag_2 = true;              //标识共公共部分的点在该子图中是否存在
            for(int j = 0; j <ifcheck_indext.size();++j){
                if(!task_graph.hasVertex(match[ifcheck_indext[j]])){
                    tag_2 = false;
                    break;
                }
            }
            if(tag_2){
                for(int j=0;j<notree_e.size();j++){  
                    flag = false;   
                    GMatchVertex * v =  task_graph.getVertex(match[notree_e[j].pointx]);
                    vector<AdjItem> & v_adj = v->value.adj;
                    for(int m =0;m<v_adj.size();m++){
                        if(v_adj[m].id==match[notree_e[j].pointy]){
                            flag = true; 
                            break;
                        };
                    }
                    if(!flag) break; //该非树边不存在，结束           
                }
                if(flag){
                    GMatchQ.assign(match.begin(),match.end());
                    graph_matching_comm(query_graph,task_graph,comm_matchQ,GMatchQ,count,query_state);
                }
            }
        }
    }
}

#endif