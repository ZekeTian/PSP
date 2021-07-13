#ifndef GMATCHSUBGRAPH_H_
#define GMATCHSUBGRAPH_H_

#include "subgraph/Vertex.h"
#include "subgraph/Subgraph.h"

struct AdjItem
{
    VertexID id;
    Label l;
};

struct GMatchValue
{
    Label l;
    vector<AdjItem> adj;            // 邻接表
    vector<VertexID> matched_query; // 可能匹配到的查询点
};

typedef Vertex<VertexID, GMatchValue> GMatchVertex;
typedef Subgraph<GMatchVertex> GMatchSubgraph;

struct vertex_hash
{
    std::size_t operator()(const GMatchVertex *p) const
    {
        return p->id;
    }
};

inline bool adj_comp(AdjItem a, AdjItem b)
{
    return a.id < b.id;
}

// 以下函数是对 >> 、<< 运算符的重载，在序列化对象时使用
obinstream &operator>>(obinstream &m, AdjItem &v)
{
    m >> v.id;
    m >> v.l;
    return m;
}

ibinstream &operator<<(ibinstream &m, const AdjItem &v)
{
    m << v.id;
    m << v.l;
    return m;
}

ofbinstream &operator>>(ofbinstream &m, AdjItem &v)
{
    m >> v.id;
    m >> v.l;
    return m;
}

ifbinstream &operator<<(ifbinstream &m, const AdjItem &v)
{
    m << v.id;
    m << v.l;
    return m;
}

//--------- GMatchValue ---------
obinstream &operator>>(obinstream &m, GMatchValue &Val)
{
    m >> Val.l;
    m >> Val.adj;
    m >> Val.matched_query;
    return m;
}

ibinstream &operator<<(ibinstream &m, const GMatchValue &Val)
{
    m << Val.l;
    m << Val.adj;
    m << Val.matched_query;
    return m;
}

ofbinstream &operator>>(ofbinstream &m, GMatchValue &Val)
{
    m >> Val.l;
    m >> Val.adj;
    m >> Val.matched_query;
    return m;
}

ifbinstream &operator<<(ifbinstream &m, const GMatchValue &Val)
{
    m << Val.l;
    m << Val.adj;
    m << Val.matched_query;
    return m;
}

#endif