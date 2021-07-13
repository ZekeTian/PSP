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


#include "comm-struct.h"
#include "subg-dev.h"
#include "GMatchTrimmer.h"
#include "GMatchAgg.h"
#include "GMatchWorker.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int main(int argc, char *argv[])
{
    init_worker(&argc, &argv);
    WorkerParams param;
    param.input_path = argv[1]; // the path of data graph in HDFS
    param.query_graph_path = argv[2]; // the path of query graph in HDFS
    int thread_num = atoi(argv[3]); // number of threads per process
    param.force_write = true;
    param.native_dispatcher = false;
    //------
    GMatchTrimmer trimmer;
    GMatchAgg aggregator;
    GMatchWorker worker(thread_num);
    // worker.setTrimmer(&trimmer);
    worker.setAggregator(&aggregator);
    worker.run(param);
    worker_finalize();
    return 0;
}