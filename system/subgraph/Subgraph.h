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
 * 子图封装类
 */

#ifndef SUBGRAPH_H_
#define SUBGRAPH_H_

#include "util/serialization.h"
#include "util/ioser.h"
#include <list>

template <class VertexT>
class Subgraph{
public:
	typedef typename VertexT::KeyType KeyT;
	typedef typename VertexT::ValueType ValueT;

	typedef hash_map<KeyT, int> VertexMap;
	
    /**
     * 用于存储顶点在 vertexes 的下标，即 vmap[id] 的值为 id 顶点在 vertexes 中的下标。
     * 借助该 map 可以快速地根据顶点 id 获取到顶点的值。
     */
    VertexMap vmap; //vmap[vid] = index of the vertex in "vertexes" defined below 
	vector<VertexT> vertexes; //store the nodes of this subgraph
    vector<VertexT> roots; // 存储根顶点（种子顶点，数据点）
    vector<KeyT> query_vertexs; // 存储查询图根顶点的 id
    vector<vector<KeyT>> *comm_match; // 存储公共部分匹配
    // vector<VertexID *> *comm_match; 
    hash_map<KeyT, set<KeyT>> candidate_vertexs; // 候选点，key：查询点，value：key 对应的候选点id（数据点id）
    hash_map<KeyT, set<KeyT>> mapping; // 数据点到查询点的映射，key：数据点，value：key 能够匹配到的查询点

    /**
     * 拉取该子图最多需要执行的 step 数量
     */
    int max_step;

    Subgraph() {
        comm_match = NULL; // 初始化为 NULL
    }

	void addVertex(VertexT & vertex){
		vmap[vertex.id] = vertexes.size();
		vertexes.push_back(vertex); //deep copy
	}

    void addRoot(VertexT & root) {
        roots.push_back(root);
    }

	//adding edge = get neigbors' values and cope with the adj-lists in them

	bool hasVertex(KeyT vid){
		return vmap.find(vid) != vmap.end();
	}

	VertexT * getVertex(KeyT id){
		typename VertexMap::iterator it = vmap.find(id);
		if(it != vmap.end())
			return &(vertexes[it->second]);
		return NULL;
	}

	//only write "vertexes" to disk / binary-stream
	//when serialized back, "vmap" is reconstructed
	friend ibinstream& operator<<(ibinstream& m, const Subgraph& v)
	{
		m << v.vertexes;
        m << v.roots;
        m << v.query_vertexs;
        // m << v.comm_match;
        m << v.candidate_vertexs;
        m << v.mapping;
        m << v.max_step;
		return m;
	}

	friend obinstream& operator>>(obinstream& m, Subgraph& v)
	{
		vector<VertexT> & vertexes = v.vertexes;
		vector<VertexT> & roots = v.roots;
		vector<KeyT> & query_vertexs = v.query_vertexs;
        // vector<vector<KeyT>> & comm_match = v.comm_match;
        v.comm_match = NULL;
		hash_map<KeyT, set<KeyT>> & candidate_vertexs = v.candidate_vertexs;
		hash_map<KeyT, set<KeyT>> & mapping = v.mapping;
        int & max_step = v.max_step;
		VertexMap & vmap = v.vmap;
		m >> vertexes;
        m >> roots;
        m >> query_vertexs;
        // m >> comm_match;
        m >> candidate_vertexs;
        m >> mapping;
        m >> max_step;

		for(int i = 0 ; i < vertexes.size(); i++)
			vmap[vertexes[i].id] = i;
		return m;
	}

	friend ifbinstream& operator<<(ifbinstream& m, const Subgraph& v)
	{
		m << v.vertexes;
        m << v.roots;
        m << v.query_vertexs;
        // m << v.comm_match;
        m << v.candidate_vertexs;
        m << v.mapping;
        m << v.max_step;
		return m;
	}

	friend ofbinstream& operator>>(ofbinstream& m, Subgraph& v)
	{
		vector<VertexT> & vertexes = v.vertexes;
        vector<VertexT> & roots = v.roots;
        vector<KeyT> & query_vertexs = v.query_vertexs;
        // vector<vector<KeyT>> & comm_match = v.comm_match;
        v.comm_match = NULL;
        hash_map<KeyT, set<KeyT>> & candidate_vertexs = v.candidate_vertexs;
        hash_map<KeyT, set<KeyT>> & mapping = v.mapping;
        int & max_step = v.max_step;
		VertexMap & vmap = v.vmap;
		m >> vertexes;
        m >> roots;
        m >> query_vertexs;
        // m >> comm_match;
        m >> candidate_vertexs;
        m >> mapping;
        m >> max_step;
		for(int i = 0 ; i < vertexes.size(); i++)
			vmap[vertexes[i].id] = i;
		return m;
	}
};

#endif /* SUBGRAPH_H_ */
