#ifndef COMMSTRUCT_H_
#define COMMSTRUCT_H_

#include "GMatchSubgraph.h"
#include "subgraph/Task.h"

using namespace std;
using namespace __gnu_cxx;
namespace __gnu_cxx
{
    template <>
    struct hash<const string>
    {
        size_t operator()(const string &s) const
        {
            return hash<const char *>()(s.c_str());
        } //__stl_hash_string
    };
    template <>
    struct hash<string>
    {
        size_t operator()(const string &s) const
        {
            return hash<const char *>()(s.c_str());
        }
    };
} // namespace __gnu_cxx

/**
 * 二元组的哈希函数
 */
struct pair_hash
{
    template <class T1, class T2>
    std::size_t operator()(const std::pair<T1, T2> &p) const
    {
        // auto h1 = std::hash<T1>{}(p.first);
        // auto h2 = std::hash<T2>{}(p.second);
        // return h1 ^ h2;

        // 参考 https://www.boost.org/doc/libs/1_55_0/doc/html/hash/reference.html#boost.hash_combine
        std::hash<T1> hasher1;
        size_t seed = hasher1(p.first);

        std::hash<T2> hasher2;
        seed ^= hasher2(p.second) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
        return seed;
    }
};

// 任务的定义
typedef Task<GMatchVertex, char> GMatchTask; //context = step

struct MatchvStruct
{
    VertexID candidate;
    VertexID matchquery;
};

/**
 * 查询计划点，生成查询计划时使用
 */
struct QueryPlanVertex
{
    VertexID id;
    Label l;
    vector<AdjItem> parent;  // 父节点
    vector<AdjItem> partner; // 兄弟节点
    vector<AdjItem> child;   // 子节点
    int degree; //该点的度数
};

/**
 * 对每个查询点的判断
 * 
 */
// struct query_judge
// {
// 	bool tag;
// 	vector<VertexID> child_list;
// 	vector<int> child_Index;
// };

// 查询分组结构
struct QueryGroup
{
    VertexID group_id;                       // 查询组 id
    GMatchSubgraph comm_subgraph;            // 公共子图
    vector<VertexID> query_vertexs;          // 该查询组中包含的查询图
    hash_map<VertexID, VertexID> map_table;  // 映射表，key：查询图的顶点 id，value：公共子结构的顶点 id
};

struct comm_notree
{
    int pointx;
    int pointy;
};
struct query_judge
{
	// bool last_tag;//是否有最后一个点
	// vector<bool> reLable;//与之前的点是否有重复的标签
	// vector<VertexID> order_before_index;//排序之前的点标签的下标
    VertexID id;
	vector<int> ancestor_indext;		//该查询点的祖先下标
	vector<VertexID> ancestor_ids;		//查询点祖先id
	vector<int> parents_indext;			//该查询点排序前的父节点
	bool reLabel;						//该标签在之前的排序中是否有相同的点
	vector<int> reLabel_indext;			//与其相同点的下标
    vector<int> notree_indext;          //非树边的下标
    
    vector<int> ifcheck_indext;         //是否有需要检查的候选点下标
    vector<comm_notree> notree_e;       //公共结构非树边两点下标

    bool tag_last = false;
    
};

typedef Vertex<VertexID, GMatchValue> QueryVertex;
typedef vector<vector<QueryPlanVertex>> QueryPlanVertexVec; // 存储查询计划点的顺序（由广度遍历得到）
typedef vector<QueryPlanVertex> QueryPlanVertexVecList;
typedef vector<vector<QueryVertex>> QueryVertexVec;                       // 存储查询点的顺序（由广度遍历得到）
typedef vector<hash_set<pair<VertexID, VertexID>, pair_hash>> EdgeVector; // 存储查询图中同层顶点之间的边


#endif