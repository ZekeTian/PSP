/**
 * 利用 Leapfrog Join 算法对有序集合求交集
 */ 

#ifndef LEAPFROG_H
#define LEAPFROG_H

#include "GMatchSubgraph.h"
#include <vector>
#include <algorithm>

inline bool compare(const vector<AdjItem> *p1, const vector<AdjItem> *p2)
{
    return (*p1)[0].id < (*p2)[0].id;
}

inline bool compare2(const vector<VertexID> p1, const vector<VertexID> p2)
{
    return p1[0] < p2[0];
}

/**
 * 一元关系的 leapfrog join 算法，即对多个有序列表求交
 * 
 * @param all_list      待求交集的列表
 * @param result        交集结果
 */
void leapfrog_join(vector<vector<AdjItem> *> &all_list, vector<AdjItem> &result)
{
    // 将 all_list 中的列表按照列表的首元素进行排序
    sort(all_list.begin(), all_list.end(), compare);
    
    int list_num = all_list.size();
    AdjItem search_value = (*all_list.back())[0];
    int p = 0; // 当前正在执行查找的 list 在 all_list 中的下标
    vector<AdjItem>::iterator search_pos = all_list.back()->begin(); // search_value 在 p 号列表中的位置，同时其也表示当前值的位置
    vector<AdjItem>::iterator current_pos[list_num]; // 各个 list 当前的位置
    for (int i = 0; i < list_num; ++i)
        current_pos[i] = all_list[i]->begin();

    AdjItem current_value; // 正在执行查找操作的列表中当前的值
    while (true)
    {
        current_value = *(current_pos[p]);
        if (current_value.id == search_value.id)
        {
            result.push_back(search_value);
            current_pos[p]++; // 当前列表中搜索下一个
        }
        else
        {
            vector<AdjItem>::iterator pos = lower_bound(all_list[p]->begin(), all_list[p]->end(), search_value, adj_comp);
            current_pos[p] = pos;
        }

        if (current_pos[p] == all_list[p]->end())
            return; // p 号列表全部查找完毕，结束查找
        
        search_value = *(current_pos[p]); // 更新搜索值
        p = (p + 1) % list_num; // 转向下一个列表的查找
    }
}

/**
 * 一元关系的 leapfrog join 算法，即对多个有序列表求交
 * 
 * @param all_list      待求交集的列表
 * @param result        交集结果
 */
void leapfrog_join(vector<vector<AdjItem> *> &all_list, vector<VertexID> &result)
{
    // 将 all_list 中的列表按照列表的首元素进行排序
    // cout << "cc0" << endl;
    sort(all_list.begin(), all_list.end(), compare);
    // cout << "cc1" << endl;
    int list_num = all_list.size();
    AdjItem search_value = (*all_list.back())[0];
    int p = 0; // 当前正在执行查找的 list 在 all_list 中的下标
    vector<AdjItem>::iterator search_pos = all_list.back()->begin(); // search_value 在 p 号列表中的位置，同时其也表示当前值的位置
    vector<AdjItem>::iterator current_pos[list_num]; // 各个 list 当前的位置
    for (int i = 0; i < list_num; ++i)
        current_pos[i] = all_list[i]->begin();

    AdjItem current_value; // 正在执行查找操作的列表中当前的值
    while (true)
    {
        current_value = *(current_pos[p]);
        if (current_value.id == search_value.id)
        {
            result.push_back(search_value.id);
            current_pos[p]++; // 当前列表中搜索下一个
        }
        else
        {
            vector<AdjItem>::iterator pos = lower_bound(all_list[p]->begin(), all_list[p]->end(), search_value, adj_comp);
            current_pos[p] = pos;
        }

        if (current_pos[p] == all_list[p]->end())
            return; // p 号列表全部查找完毕，结束查找
        
        search_value = *(current_pos[p]); // 更新搜索值
        p = (p + 1) % list_num; // 转向下一个列表的查找
    }

    //  cout << "cc11" << endl;

}


/**
 * 一元关系的 leapfrog join 算法，即对多个有序列表求交
 * 
 * @param all_list      待求交集的列表
 * @param result        交集结果
 */
void leapfrog_join(vector<vector<VertexID>> &all_list, vector<VertexID> &result)
{
    // 将 all_list 中的列表按照列表的首元素进行排序
    sort(all_list.begin(), all_list.end(), compare2);
    
    int list_num = all_list.size();
    VertexID search_value = (all_list.back())[0];
    int p = 0; // 当前正在执行查找的 list 在 all_list 中的下标
    vector<VertexID>::iterator search_pos = all_list.back().begin(); // search_value 在 p 号列表中的位置，同时其也表示当前值的位置
    vector<VertexID>::iterator current_pos[list_num]; // 各个 list 当前的位置
    for (int i = 0; i < list_num; ++i)
        current_pos[i] = all_list[i].begin();

    int current_value; // 正在执行查找操作的列表中当前的值
    while (true)
    {
        current_value = *(current_pos[p]);
        if (current_value == search_value)
        {
            result.push_back(search_value);
            current_pos[p]++; // 当前列表中搜索下一个
        }
        else
        {
            vector<VertexID>::iterator pos = lower_bound(all_list[p].begin(), all_list[p].end(), search_value);
            current_pos[p] = pos;
        }

        if (current_pos[p] == all_list[p].end())
            return; // p 号列表全部查找完毕，结束查找
        
        search_value = *(current_pos[p]); // 更新搜索值
        p = (p + 1) % list_num; // 转向下一个列表的查找
    }
}


/**
 * 利用 leapfrog join 算法，判断多个列表是否有共同元素（即相当于是看多个列表的交集结果是否为空）
 * 
 * @param all_list      待确定关系的列表
 */
bool leapfrog_contains_common(vector<vector<AdjItem> *> &all_list)
{
    // 将 all_list 中的列表按照列表的首元素进行排序
    sort(all_list.begin(), all_list.end(), compare);
    
    int list_num = all_list.size();
    AdjItem search_value = (*all_list.back())[0];
    int p = 0; // 当前正在执行查找的 list 在 all_list 中的下标
    vector<AdjItem>::iterator search_pos = all_list.back()->begin(); // search_value 在 p 号列表中的位置，同时其也表示当前值的位置
    vector<AdjItem>::iterator current_pos[list_num]; // 各个 list 当前的位置
    for (int i = 0; i < list_num; ++i)
        current_pos[i] = all_list[i]->begin();

    AdjItem current_value; // 正在执行查找操作的列表中当前的值
    while (true)
    {
        current_value = *(current_pos[p]);
        if (current_value.id == search_value.id)
        {   // 有公共元素，则返回 true
            return true;
        }
        else
        {
            vector<AdjItem>::iterator pos = lower_bound(all_list[p]->begin(), all_list[p]->end(), search_value, adj_comp);
            current_pos[p] = pos;
        }

        if (current_pos[p] == all_list[p]->end())
            return false; // p 号列表全部查找完毕，未找到公共元素，结束查找
        
        search_value = *(current_pos[p]); // 更新搜索值
        p = (p + 1) % list_num; // 转向下一个列表的查找
    }

    return false;
}

#endif /* LEAPFROG_H */