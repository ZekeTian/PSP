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
 * 并发 set，主要用于存储已经生成的 task
 */

#ifndef CONSET_H
#define CONSET_H

#define CONSET_BUCKET_NUM 10000

#include <util/global.h>
#include <unordered_set>
using namespace std;

template <typename T> struct conset
{
public:
    mutex mtx;
    unordered_set<T> *data_set; // 存储数据

    conset()
    {
        data_set = new unordered_set<T>;
    }

    inline void lock()
    {
        mtx.lock();
    }

    inline void unlock()
    {
        mtx.unlock();
    }

    void insert(T & val)
    {
        data_set->insert(val);
    }

    void erase(T & val)
    {
        data_set->erase(val);
    }

    ~conset()
    {
        delete data_set;
    }
};

#endif
