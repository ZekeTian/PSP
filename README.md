# PSP: Parallel Subgraph query Processing

We study how to exploit the inherent parallelism of parallel computing systems for both intraquery and interquery parallelism during subgraph query processing over large graphs. Different from widely used hash-based parallelism techniques, we utilize and extend locality sensitive hashing based on the structures of subgraph queries to merge multiple tasks during a single subgraph query processing or multiple subgraph queries. 



## Prerequisities

- Linux
- GCC 4.8.5 or higher version
- MPICH3
- JDK 7 or higher version
- Hadoop 2.7.7 or higher version



## Data Format

The data input into the program needs to be in adjacency list format, as follows:

```
vertex-ID label \t  neighbor1-ID  neighbor1-label  neighbor2-ID  neighbor2-label  neighbor3-ID   neighbor3-label ...
```

In the **sample_data** directory of the project, there are sample data graph and query graph.

If you need to input multiple query graphs, please put multiple query graphs in one file. The id of the vertex of each query graph must be unique, and the query graphs are separated by blank lines. **In the query graph file, please use CRLF for line breaks.**

Sample:

```
0	7	3 8 5 1
1	9	2 20 3 8
2	20	1 9
3	8	0 7 1 9
4	95	5 1
5	1	0 7 4 95

6	27	8 5
7	12	8 5
8	5	6 27 7 12

9	27	12 5
10	1	12 5
11	12	12 5
12	5	9 27 10 1 11 12
```



## Configuration

In the `ydhdfs2.h` file in the `$PSP_PATH/system/util` (**$PSP_PATH** is the root directory of the project), you need to specify the hostname and port of the master machine (aka NameNode) of the Hadoop cluster. 

```c++
// $PSP_PATH/system/util/ydhdfs2.h
hdfsBuilderSetNameNode(bld, "master"); // master machine's hostname, default is master
hdfsBuilderSetNameNodePort(bld, 9000); // port, default is 9000
```



## Compile

In the root directory of the project, execute the following command, you can get an executable file `run`.

```shell
cd app_gmatch
make
```



## Run

Before running the code, please check the following:

- All prerequisites have been met
- The `ydhdfs2.h` file has been updated with your master machine’s hostname, port
- Data graph and query graph have been converted into adjacency list format
- The data graph and query graph have been uploaded to HDFS
- All executable files have been sent to each machine in the cluster (if you want to run on multiple machines )

### Running on multiple machines

You can use the `mpiexec` command to run the program on multiple machines, the command is as follows:

```shell
mpiexec -n <N> -f <hosts> ./run <data_graph_path> <query_graph_path> <t>
```

where **N** is the number of machines/processes, **hosts** is the hosts file, **data_graph_path** is the HDFS path of the data graph, **query_graph_path** is the HDFS path of the query graph, and **t** is the number of threads per machine/process.

### Running on a single machine

If you only want to test on a single machine, you can execute the following command:

```shell
./run <data_graph_path> <query_graph_path> <t>
```

### Results of the run

When you test on sample data, you can get the following results.

```shell
Load Time : xxxxx seconds
running
Task Time : xxxxx seconds
the number of matching results for query1 is 7
```

where **Load Time** is the time to load data, **Task Time** is the total time the program runs. For the sample query graph, the id of the root vertex of the query plan is 1 (i.e. 1 in *query**1***), and the number of matching results on the sample data graph is 7. If you input multiple query graphs, please distinguish the number of matching results from different query graphs by the id of the root vertex of the query plan.

