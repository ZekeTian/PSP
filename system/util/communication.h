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

//Acknowledgements: this code is implemented by referencing pregel-mpi (https://code.google.com/p/pregel-mpi/) by Chuntao Hong.

#ifndef COMMUNICATION_H
#define COMMUNICATION_H

#include <mpi.h>
#include "timer.h"
#include "serialization.h"
#include "global.h"

//============================================
//binstream-level send/recv
void send_ibinstream(ibinstream& m, int dst, int tag)
{
    MPI_Send(m.get_buf(), m.size(), MPI_CHAR, dst, tag, MPI_COMM_WORLD);
}

obinstream recv_obinstream(int src, int tag)
{
	MPI_Status status;
	MPI_Probe(src, tag, MPI_COMM_WORLD, &status);
	int size;
	MPI_Get_count(&status, MPI_CHAR, &size); // get size of the msg-batch (# of bytes)
	char * buf = new char[size];
	MPI_Recv(buf, size, MPI_CHAR, status.MPI_SOURCE, status.MPI_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    return obinstream(buf, size);
}

//============================================
//obj-level send/recv
template <class T>
void send_data(const T& data, int dst, int tag)
{
    ibinstream m;
    m << data;
    send_ibinstream(m, dst, tag);
}

template <class T>
T recv_data(int src, int tag)
{
    obinstream um = recv_obinstream(src, tag);
    T data;
    um >> data;
    return data;
}

template <class T>
void recv_data(int src, int tag, T& data)
{
    obinstream um = recv_obinstream(src, tag);
    um >> data;
}

//============================================
//all-to-all

template <class T>
void all_to_all(vector<vector<T*> > & to_exchange, int tag)
{
    StartTimer(COMMUNICATION_TIMER);
    int np = get_num_workers();
    int me = get_worker_id();
    for (int i = 0; i < np; i++)
    {
        int partner = (i - me + np) % np;
        if (me != partner)
        {
            if (me < partner)
            {
                StartTimer(SERIALIZATION_TIMER);
                //send
                ibinstream * m = new ibinstream;
                *m << to_exchange[partner];
                for(int k = 0; k < to_exchange[partner].size();k++)
                    delete to_exchange[partner][k];
                vector<T*>().swap(to_exchange[partner]);
                StopTimer(SERIALIZATION_TIMER);
                StartTimer(TRANSFER_TIMER);
                send_ibinstream(*m, partner, tag);
                delete m;
                StopTimer(TRANSFER_TIMER);
                //receive
                StartTimer(TRANSFER_TIMER);
                obinstream um = recv_obinstream(partner, tag);
                StopTimer(TRANSFER_TIMER);
                StartTimer(SERIALIZATION_TIMER);
                um >> to_exchange[partner];
                StopTimer(SERIALIZATION_TIMER);
            }
            else
            {
                StartTimer(TRANSFER_TIMER);
                //receive
                obinstream um = recv_obinstream(partner, tag);
                StopTimer(TRANSFER_TIMER);
                StartTimer(SERIALIZATION_TIMER);
                //send
                ibinstream * m = new ibinstream;
                *m << to_exchange[partner];
                for(int k = 0; k < to_exchange[partner].size();k++)
                    delete to_exchange[partner][k];
                vector<T*>().swap(to_exchange[partner]);
                StopTimer(SERIALIZATION_TIMER);
                StartTimer(TRANSFER_TIMER);
                send_ibinstream(*m, partner, tag);
                delete m;
                StopTimer(TRANSFER_TIMER);
                um >> to_exchange[partner];
            }
        }
    }
    StopTimer(COMMUNICATION_TIMER);
}

//数据点的发送<顶点,分区号>
template <class T>
void dataPart_to_all( hash_map<T, T> & vertex_map,int tag)
{
    StartTimer(COMMUNICATION_TIMER);
    int np = get_num_workers();
    int me = get_worker_id();
    for (int i = 0; i < np; i++)
    {
        int partner = (i - me + np) % np;
        if (me != partner)//判断是否是当前worker的id
        {
            if (me < partner)//大于当前worker_id传送数据
            {
                StartTimer(SERIALIZATION_TIMER);
                //send
                ibinstream * m = new ibinstream;

                *m << vertex_map;
                StopTimer(SERIALIZATION_TIMER);

                StartTimer(TRANSFER_TIMER);
                //发送数据

                send_ibinstream(*m, partner, tag);
                delete m;
                StopTimer(TRANSFER_TIMER);

                //receive
                StartTimer(TRANSFER_TIMER);
                //接收数据
                obinstream um = recv_obinstream(partner, tag);
                StopTimer(TRANSFER_TIMER);
                StartTimer(SERIALIZATION_TIMER);
                //保存传递过来的数据
                um >> vertex_map;
                StopTimer(SERIALIZATION_TIMER);
            }
            else//小于当前worker_id传送数据
            {
                StartTimer(TRANSFER_TIMER);
                //receive
                obinstream um = recv_obinstream(partner, tag);
                StopTimer(TRANSFER_TIMER);
                StartTimer(SERIALIZATION_TIMER);
                //send
                ibinstream * m = new ibinstream;
                *m << vertex_map;
                StopTimer(SERIALIZATION_TIMER);
                StartTimer(TRANSFER_TIMER);
                send_ibinstream(*m, partner, tag);
                delete m;
                StopTimer(TRANSFER_TIMER);
                um >> vertex_map;
            }
        }
    }
    StopTimer(COMMUNICATION_TIMER);
}

//============================================
// broadcast

/**
 * 广播数据
 */
template <class T>
void broadcast_data(vector<T> & to_send)
{
    StartTimer(COMMUNICATION_TIMER);
    int np = get_num_workers();
    int me = get_worker_id();

    // 判断当前进程是否为 master
    if (me == MASTER_RANK)
    {
        // 如果为 master 则进行广播发送数据
        StartTimer(SERIALIZATION_TIMER);
        //send
        ibinstream * m = new ibinstream;
        *m << to_send;
        StopTimer(SERIALIZATION_TIMER);

        StartTimer(TRANSFER_TIMER);
        size_t length = m->size();
        // 广播长度
        MPI_Bcast(&length, 1, MPI_UNSIGNED_LONG_LONG, MASTER_RANK, MPI_COMM_WORLD);
        // 广播数据
        MPI_Bcast(m->get_buf(), m->size(), MPI_CHAR, MASTER_RANK, MPI_COMM_WORLD);
        delete m;
        StopTimer(TRANSFER_TIMER);
    }
    else
    {
        // 否则，其它进程只负责接收数据
        StartTimer(TRANSFER_TIMER);
        // 接收长度
        size_t length;
        MPI_Bcast(&length, 1, MPI_UNSIGNED_LONG_LONG, MASTER_RANK, MPI_COMM_WORLD);

        // 分配内存空间，负责接收数据
        char * buf = new char[length];
        MPI_Bcast(buf, length, MPI_CHAR, MASTER_RANK, MPI_COMM_WORLD);
        obinstream um = obinstream(buf, length);
        StopTimer(TRANSFER_TIMER);

        // 将接收的数据反序列化
        StartTimer(SERIALIZATION_TIMER);
        um >> to_send;
        StopTimer(SERIALIZATION_TIMER);
    }

    StopTimer(COMMUNICATION_TIMER);
}


template <class K, class V>
void broadcast_data(hash_map<K, V>& to_send)
{
    StartTimer(COMMUNICATION_TIMER);
    int np = get_num_workers();
    int me = get_worker_id();

    // 判断当前进程是否为 master
    if (me == MASTER_RANK)
    {
        // 如果为 master 则进行广播发送数据
        StartTimer(SERIALIZATION_TIMER);
        //send
        ibinstream * m = new ibinstream;
        *m << to_send;
        StopTimer(SERIALIZATION_TIMER);

        StartTimer(TRANSFER_TIMER);
        size_t length = m->size();
        // 广播长度
        MPI_Bcast(&length, 1, MPI_UNSIGNED_LONG_LONG, MASTER_RANK, MPI_COMM_WORLD);
        // 广播数据
        MPI_Bcast(m->get_buf(), m->size(), MPI_CHAR, MASTER_RANK, MPI_COMM_WORLD);
        delete m;
        StopTimer(TRANSFER_TIMER);
    }
    else
    {
        // 否则，其它进程只负责接收数据
        StartTimer(TRANSFER_TIMER);
        // 接收长度
        size_t length;
        MPI_Bcast(&length, 1, MPI_UNSIGNED_LONG_LONG, MASTER_RANK, MPI_COMM_WORLD);

        // 分配内存空间，负责接收数据
        char * buf = new char[length];
        MPI_Bcast(buf, length, MPI_CHAR, MASTER_RANK, MPI_COMM_WORLD);
        obinstream um = obinstream(buf, length);
        StopTimer(TRANSFER_TIMER);

        // 将接收的数据反序列化
        StartTimer(SERIALIZATION_TIMER);
        um >> to_send;
        StopTimer(SERIALIZATION_TIMER);
    }

    StopTimer(COMMUNICATION_TIMER);
}


//============================================
// allreduce
template <class K, class V>
void allreduce(hash_map<K, V> & to_send, int tag)
{
    StartTimer(COMMUNICATION_TIMER);
    int np = get_num_workers();
    int me = get_worker_id();

    // 树形规约
    int remain = np, sum = me, half, rm;
    while(remain != 1)
    {
        half = remain / 2;
        rm = remain% 2 ;
        if(me < half)
        {
            StartTimer(TRANSFER_TIMER);
            //receive
            obinstream um = recv_obinstream(me + half + rm, tag);
            StopTimer(TRANSFER_TIMER);
            StartTimer(SERIALIZATION_TIMER);
            hash_map<K, V> tmp_map;
            um >> tmp_map;
            to_send.insert(tmp_map.begin(), tmp_map.end());
        }
        else if(me >= half+rm && me < remain)
        {
            //send
            ibinstream * m = new ibinstream;
            *m << to_send;
            StopTimer(SERIALIZATION_TIMER);
            StartTimer(TRANSFER_TIMER);
            send_ibinstream(*m, me - half - rm, tag);
            delete m;
            StopTimer(TRANSFER_TIMER);
        }
        remain = half+rm;
    }

    // 判断当前进程是否为 master
    if (me != MASTER_RANK)
    {
       to_send.clear(); // 非 master 节点清空数据，从而便于接收 master 广播的数据
    }
    broadcast_data(to_send);
}


//查询图的传输
template <class T>
void graph_to_all(vector<T*> & to_exchange, int tag)
{
    StartTimer(COMMUNICATION_TIMER);
    int np = get_num_workers();
    int me = get_worker_id();
    for (int i = 0; i < np; i++)
    {
        int partner = (i - me + np) % np;
        if (me != partner)//判断是否是当前worker的id
        {
            if (me < partner)//大于当前worker_id传送数据
            {
                StartTimer(SERIALIZATION_TIMER);
                //send
                ibinstream * m = new ibinstream;
                *m << to_exchange;
                StopTimer(SERIALIZATION_TIMER);

                StartTimer(TRANSFER_TIMER);
                //发送数据
                send_ibinstream(*m, partner, tag);
                delete m;
                StopTimer(TRANSFER_TIMER);

                //receive
                StartTimer(TRANSFER_TIMER);
                //接收数据
                obinstream um = recv_obinstream(partner, tag);
                StopTimer(TRANSFER_TIMER);
                StartTimer(SERIALIZATION_TIMER);
                //保存传递过来的数据
                um >> to_exchange;
                StopTimer(SERIALIZATION_TIMER);
            }
            else//小于当前worker_id传送数据
            {
                StartTimer(TRANSFER_TIMER);
                //receive
                obinstream um = recv_obinstream(partner, tag);
                StopTimer(TRANSFER_TIMER);
                StartTimer(SERIALIZATION_TIMER);
                //send
                ibinstream * m = new ibinstream;
                *m << to_exchange;
                StopTimer(SERIALIZATION_TIMER);
                StartTimer(TRANSFER_TIMER);
                send_ibinstream(*m, partner, tag);
                delete m;
                StopTimer(TRANSFER_TIMER);
                um >> to_exchange;
            }
        }
    }
    StopTimer(COMMUNICATION_TIMER);
}

//the following functions are only usable before creating threads
//in fact, just used for graph loading
//============================================
//scatter
template <class T>
void masterScatter(vector<T>& to_send)
{ //scatter
    StartTimer(COMMUNICATION_TIMER);
    int* sendcounts = new int[_num_workers];
    int recvcount;
    int* sendoffset = new int[_num_workers];

    ibinstream m;
    StartTimer(SERIALIZATION_TIMER);
    int size = 0;
    for (int i = 0; i < _num_workers; i++) {
        if (i == _my_rank) {
            sendcounts[i] = 0;
        } else {
            m << to_send[i];
            sendcounts[i] = m.size() - size;
            size = m.size();
        }
    }
    StopTimer(SERIALIZATION_TIMER);

    StartTimer(TRANSFER_TIMER);
    //  MPI_Scatter(待发送数据缓冲区地址，数据个数，数据类型，接收缓冲区地址，数据个数，
    //  数据类型，发送消息的进程的标识，通信域）
    MPI_Scatter(sendcounts, 1, MPI_INT, &recvcount, 1, MPI_INT, MASTER_RANK, MPI_COMM_WORLD);
    StopTimer(TRANSFER_TIMER);

    for (int i = 0; i < _num_workers; i++) {
        sendoffset[i] = (i == 0 ? 0 : sendoffset[i - 1] + sendcounts[i - 1]);
    }
    char* sendbuf = m.get_buf(); //ibinstream will delete it
    char* recvbuf;

    StartTimer(TRANSFER_TIMER);
    // MPI_Scatterv(sendbuf发送数据的指针,sendcounts发送给各进程数据的个数表,sendoffset发送给各进程数据关于 sendbuf 的偏移表，注意当 sendcounts[] 不全为 1 时 displs 是以,
    // MPI_CHAR发送数据类型,recvbuf指向接收数据的指针, recvcount接收数据个数, MPI_CHAR接收数据类型, MASTER_RANK发送数据源进程号, MPI_COMM_WORLD通信域)
    MPI_Scatterv(sendbuf, sendcounts, sendoffset, MPI_CHAR, recvbuf, recvcount, MPI_CHAR, MASTER_RANK, MPI_COMM_WORLD);
    StopTimer(TRANSFER_TIMER);

    delete[] sendcounts;
    delete[] sendoffset;
    StopTimer(COMMUNICATION_TIMER);
}

template <class T>
void slaveScatter(T& to_get)
{ //scatter
    StartTimer(COMMUNICATION_TIMER);
    int* sendcounts;
    int recvcount;
    int* sendoffset;

    StartTimer(TRANSFER_TIMER);
    MPI_Scatter(sendcounts, 1, MPI_INT, &recvcount, 1, MPI_INT, MASTER_RANK, MPI_COMM_WORLD);

    char* sendbuf;
    char* recvbuf = new char[recvcount]; //obinstream will delete it

    MPI_Scatterv(sendbuf, sendcounts, sendoffset, MPI_CHAR, recvbuf, recvcount, MPI_CHAR, MASTER_RANK, MPI_COMM_WORLD);
    StopTimer(TRANSFER_TIMER);

    StartTimer(SERIALIZATION_TIMER);
    obinstream um(recvbuf, recvcount);
    um >> to_get;
    StopTimer(SERIALIZATION_TIMER);
    StopTimer(COMMUNICATION_TIMER);
}

#endif
