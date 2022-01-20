/**************************************************************************************
 *  Copyright (c) 2019- Gabriele Mencagli
 *  
 *  This file is part of WindFlow.
 *  
 *  WindFlow is free software dual licensed under the GNU LGPL or MIT License.
 *  You can redistribute it and/or modify it under the terms of the
 *    * GNU Lesser General Public License as published by
 *      the Free Software Foundation, either version 3 of the License, or
 *      (at your option) any later version
 *    OR
 *    * MIT License: https://github.com/ParaGroup/WindFlow/blob/vers3.x/LICENSE.MIT
 *  
 *  WindFlow is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Lesser General Public License for more details.
 *  You should have received a copy of the GNU Lesser General Public License and
 *  the MIT License along with WindFlow. If not, see <http://www.gnu.org/licenses/>
 *  and <http://opensource.org/licenses/MIT/>.
 **************************************************************************************
 */

/*  
 *  Test 1 of merge between MultiPipes with both CPU and GPU operators.
 *  
 *  +---------------------------------+
 *  |  +-----+    +-----+    +-----+  |
 *  |  |  S  |    |  F  |    |  M  |  |
 *  |  | CPU +--->+ GPU +--->+ GPU |  +---+
 *  |  | (*) |    | (*) |    | (*) |  |   |      +----------------------+
 *  |  +-----+    +-----+    +-----+  |   |      |  +-----+    +-----+  |
 *  +---------------------------------+   |      |  |  M  |    |  S  |  |
 *                                        +----->+  | GPU +--->+ CPU |  |
 *  +---------------------------------+   |      |  | (*) |    | (*) |  |
 *  |  +-----+    +-----+    +-----+  |   |      |  +-----+    +-----+  |
 *  |  |  S  |    |  M  |    |  M  |  |   |      +----------------------+
 *  |  | CPU +--->+ CPU +--->+ CPU |  +---+
 *  |  | (*) |    | (*) |    | (*) |  |
 *  |  +-----+    +-----+    +-----+  |
 *  +---------------------------------+
 */ 

// include
#include<random>
#include<iostream>
#include<ff/ff.hpp>
#include<windflow.hpp>
#include<windflow_gpu.hpp>
#include"merge_common_gpu.hpp"

using namespace std;
using namespace wf;

// global variable for the result
extern atomic<long> global_sum;

// main
int main(int argc, char *argv[])
{
    int option = 0;
    size_t runs = 1;
    size_t stream_len = 0;
    size_t n_keys = 1;
    // initalize global variable
    global_sum = 0;
    // arguments from command line
    if (argc != 7) {
        cout << argv[0] << " -r [runs] -l [stream_length] -k [n_keys]" << endl;
        exit(EXIT_SUCCESS);
    }
    while ((option = getopt(argc, argv, "r:l:k:")) != -1) {
        switch (option) {
            case 'r': runs = atoi(optarg);
                     break;
            case 'l': stream_len = atoi(optarg);
                     break;
            case 'k': n_keys = atoi(optarg);
                     break;
            default: {
                cout << argv[0] << " -r [runs] -l [stream_length] -k [n_keys]" << endl;
                exit(EXIT_SUCCESS);
            }
        }
    }
    // set random seed
    mt19937 rng;
    rng.seed(std::random_device()());
    size_t min = 1;
    size_t max = 4;
    std::uniform_int_distribution<std::mt19937::result_type> dist_p(min, max);
    std::uniform_int_distribution<std::mt19937::result_type> dist_b(100, 200);
    int map1_degree, map2_degree, map3_degree, map4_degree, filter_degree, sink_degree;
    size_t source1_degree = dist_p(rng);
    size_t source2_degree = dist_p(rng);
    long last_result = 0;
    // executes the runs in DEFAULT mode
    for (size_t i=0; i<runs; i++) {
        map1_degree = dist_p(rng);
        map2_degree = dist_p(rng);
        map3_degree = dist_p(rng);
        map4_degree = dist_p(rng);
        filter_degree = dist_p(rng);
        sink_degree = dist_p(rng);
        cout << "Run " << i << endl;
        cout << "+---------------------------------+" << endl;
        cout << "|  +-----+    +-----+    +-----+  |" << endl;
        cout << "|  |  S  |    |  F  |    |  M  |  |" << endl;
        cout << "|  | CPU +--->+ GPU +--->+ GPU |  +---+" << endl;
        cout << "|  | (" << source1_degree << ") |    | (" << filter_degree << ") |    | (" << map1_degree << ") |  |   |      +----------------------+" << endl;
        cout << "|  +-----+    +-----+    +-----+  |   |      |  +-----+    +-----+  |" << endl;
        cout << "+---------------------------------+   |      |  |  M  |    |  S  |  |" << endl;
        cout << "                                      +----->+  | GPU +--->+ CPU |  |" << endl;
        cout << "+---------------------------------+   |      |  | (" << map2_degree << ") |    | (" << sink_degree << ") |  |" << endl;
        cout << "|  +-----+    +-----+    +-----+  |   |      |  +-----+    +-----+  |" << endl;
        cout << "|  |  S  |    |  M  |    |  M  |  |   |      +----------------------+" << endl;
        cout << "|  | CPU +--->+ CPU +--->+ CPU |  +---+" << endl;
        cout << "|  | (" << source2_degree << ") |    | (" << map3_degree << ") |    | (" << map4_degree << ") |  |" << endl;
        cout << "|  +-----+    +-----+    +-----+  |" << endl;
        cout << "+---------------------------------+" << endl;
        // compute the total parallelism degree of the PipeGraph
        size_t check_degree = source1_degree;
        if (source1_degree != filter_degree) {
            check_degree += filter_degree;
        }
        if (filter_degree != map1_degree) {
            check_degree += map1_degree;
        }
        check_degree += source2_degree;
        if (source2_degree != map3_degree) {
            check_degree += map3_degree;
        }
        if (map3_degree != map4_degree) {
            check_degree += map4_degree;
        }
        check_degree += map2_degree;
        if (map2_degree != sink_degree) {
            check_degree += sink_degree;
        }
        // prepare the test
        PipeGraph graph("test_merge_gpu_1", Execution_Mode_t::DEFAULT, Time_Policy_t::EVENT_TIME);
        // prepare the first MultiPipe
        Source_Positive_Functor source_functor_positive(stream_len, n_keys, true);
        Source source1 = Source_Builder(source_functor_positive)
                            .withName("source1")
                            .withParallelism(source1_degree)
                            .withOutputBatchSize(dist_b(rng))
                            .build();
        MultiPipe &pipe1 = graph.add_source(source1);
        Filter_Functor_GPU filter_functor_gpu; 
        Filter_GPU filter = FilterGPU_Builder(filter_functor_gpu)
                                .withName("filtergpu")
                                .withParallelism(filter_degree)
                                .build();
        pipe1.chain(filter);
        Map_Functor_GPU map_functor_gpu1; 
        Map_GPU mapgpu1 = MapGPU_Builder(map_functor_gpu1)
                                .withName("mapgpu1")
                                .withParallelism(map1_degree)
                                .build();
        pipe1.chain(mapgpu1);
        // prepare the second MultiPipe
        Source_Negative_Functor source_negative_positive(stream_len, n_keys, true);
        Source source2 = Source_Builder(source_negative_positive)
                            .withName("source2")
                            .withParallelism(source2_degree)
                            .withOutputBatchSize(dist_b(rng))
                            .build();
        MultiPipe &pipe2 = graph.add_source(source2);
        Map_Functor map_functor3; 
        Map map3 = Map_Builder(map_functor3)
                        .withName("map3")
                        .withParallelism(map3_degree)
                        .withOutputBatchSize(dist_b(rng))
                        .build();
        pipe2.chain(map3);
        Map_Functor map_functor4; 
        Map map4 = Map_Builder(map_functor4)
                        .withName("map4")
                        .withParallelism(map4_degree)
                        .withOutputBatchSize(dist_b(rng))
                        .build();
        pipe2.chain(map4);
        // prepare the third MultiPipe
        MultiPipe &pipe3 = pipe1.merge(pipe2);
        Map_Functor_GPU map_functor_gpu2;
        Map_GPU mapgpu2 = MapGPU_Builder(map_functor_gpu2)
                                .withName("mapgpu2")
                                .withParallelism(map2_degree)
                                .build();
        pipe3.chain(mapgpu2);
        Sink_Functor sink_functor;
        Sink sink = Sink_Builder(sink_functor)
                        .withName("sink")
                        .withParallelism(sink_degree)
                        .build();
        pipe3.chain_sink(sink);
        assert(graph.getNumThreads() == check_degree);
        // run the application
        graph.run();
        if (i == 0) {
            last_result = global_sum;
            cout << "Result is --> " << GREEN << "OK" << DEFAULT_COLOR << " value " << global_sum.load() << endl;
        }
        else {
            if (last_result == global_sum) {
                cout << "Result is --> " << GREEN << "OK" << DEFAULT_COLOR << " value " << global_sum.load() << endl;
            }
            else {
                cout << "Result is --> " << RED << "FAILED" << DEFAULT_COLOR << " value " << global_sum.load() << endl;
                abort();
            }
        }
        global_sum = 0;
    }
    return 0;
}
