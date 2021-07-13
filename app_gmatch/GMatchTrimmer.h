#ifndef GMATCHTRIMMER_H_
#define GMATCHTRIMMER_H_

#include "comm-struct.h"
#include "subg-dev.h"

/**
 *	裁剪器，可以对顶点的邻接表进行预处理（一般是用于去除一些无用的顶点）
 */
class GMatchTrimmer : public Trimmer<GMatchVertex>
{
    virtual void trim(GMatchVertex &v)
    {
        // 获取查询图中所有查询点的标签集合
        hash_set<Label> &query_vertex_label = *(hash_set<Label> *)global_query_vertex_label;

        // 邻接表剪枝
        vector<AdjItem> &val = v.value.adj;
        vector<AdjItem> new_val;
        for (int i = 0; i < val.size(); i++)
        {
            if (query_vertex_label.find(val[i].l) != query_vertex_label.end())
            {
                new_val.push_back(val[i]);
            }
        }
        val.swap(new_val);
    }
};

#endif