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
 *  Test 2 tracing support and interaction with web dashboard.
 *  
 *                                                          +---------------------+
 *                                                          |  +-----+   +-----+  |
 *                                                          |  |  S  |   |  F  |  |
 *                                                          |  | CPU |   | CPU |  |
 *                                                          |  | (*) +-->+ (*) |  +-+
 *                                                          |  +-----+   +-----+  | |
 *                                                          +---------------------+ |
 *                                                                                  |
 *                              +---------------------+                             |   +-----------+
 *                              |  +-----+   +-----+  |                             |   |  +-----+  |
 *                           +->+  |  M  |   |  M  |  +-+                           |   |  |  S  |  |
 *                           |  |  | GPU |   | GPU |  | |                           |   |  | CPU |  |
 *                           |  |  | (*) +-->+ (*) |  | |                           +-->+  | (*) |  |
 *                           |  |  +-----+   +-----+  | |                           |   |  +-----+  |
 *  +---------------------+  |  +---------------------+ |   +---------------------+ |   +-----------+
 *  |  +-----+   +-----+  |  |                          |   |  +-----+   +-----+  | |
 *  |  |  S  |   |  F  |  |  |                          |   |  | FM  |   |  F  |  | |
 *  |  | CPU |   | GPU |  |  |                          |   |  | CPU |   | GPU |  | |
 *  |  | (*) +-->+ (*) |  +--+                          +-->+  | (*) +-->+ (*) |  +-+
 *  |  +-----+   +-----+  |  |                          |   |  +-----+   +-----+  |
 *  +---------------------+  |       +-----------+      |   +---------------------+
 *                           |       |  +-----+  |      |
 *                           |       |  |  F  |  |      |
 *                           |       |  | CPU |  |      |
 *                           +------>+  | (*) |  +------+
 *                                   |  +-----+  |
 *                                   +-----------+
 *  
 */ 

// include
#include<random>
#include<iostream>
#include<windflow.hpp>
#include<windflow_gpu.hpp>
#include"misc_common.hpp"

using namespace std;
using namespace wf;

// main
int main(int argc, char *argv[])
{
    int option = 0;
    size_t stream_len = 0;
    size_t n_keys = 1;
    // arguments from command line
    if (argc != 5) {
        cout << argv[0] << " -l [stream_length] -k [n_keys]" << endl;
        exit(EXIT_SUCCESS);
    }
    while ((option = getopt(argc, argv, "l:k:")) != -1) {
        switch (option) {
            case 'l': stream_len = atoi(optarg);
                     break;
            case 'k': n_keys = atoi(optarg);
                     break;
            default: {
                cout << argv[0] << " -l [stream_length] -k [n_keys]" << endl;
                exit(EXIT_SUCCESS);
            }
        }
    }
    // set random seed
    mt19937 rng;
    rng.seed(std::random_device()());
    size_t min = 1;
    size_t max = 9;
    std::uniform_int_distribution<std::mt19937::result_type> dist_p(min, max);
    std::uniform_int_distribution<std::mt19937::result_type> dist_b(100, 200);
    int map1_degree, map2_degree, flatmap1_degree, filter1_degree, filter2_degree, filter3_degree, filter4_degree, sink1_degree;
    size_t source1_degree = dist_p(rng);
    size_t source2_degree = dist_p(rng);
    long last_result = 0;
    map1_degree = dist_p(rng);
    map2_degree = dist_p(rng);
    flatmap1_degree = dist_p(rng);
    filter1_degree = dist_p(rng);
    filter2_degree = dist_p(rng);
    filter3_degree = dist_p(rng);
    filter4_degree = dist_p(rng);
    map1_degree = dist_p(rng);
    map2_degree = dist_p(rng);
    sink1_degree = dist_p(rng);
    cout << "                                                          +---------------------+" << std::endl;
    cout << "                                                          |  +-----+   +-----+  |" << std::endl;
    cout << "                                                          |  |  S  |   |  F  |  |" << std::endl;
    cout << "                                                          |  | CPU |   | GPU |  |" << std::endl;
    cout << "                                                          |  | (" << source2_degree << ") +-->+ (" << filter2_degree << ") |  +-+" << std::endl;
    cout << "                                                          |  +-----+   +-----+  | |" << std::endl;
    cout << "                                                          +---------------------+ |" << std::endl;
    cout << "                                                                                  |" << std::endl;
    cout << "                              +---------------------+                             |   +-----------+" << std::endl;
    cout << "                              |  +-----+   +-----+  |                             |   |  +-----+  |" << std::endl;
    cout << "                           +->+  |  M  |   |  M  |  +-+                           |   |  |  S  |  |" << std::endl;
    cout << "                           |  |  | GPU |   | GPU |  | |                           |   |  | CPU |  |" << std::endl;
    cout << "                           |  |  | (" << map1_degree << ") +-->+ (" << map2_degree << ") |  | |                           +-->+  | (" << sink1_degree << ") |  |" << std::endl;
    cout << "                           |  |  +-----+   +-----+  | |                           |   |  +-----+  |" << std::endl;
    cout << "  +---------------------+  |  +---------------------+ |   +---------------------+ |   +-----------+" << std::endl;
    cout << "  |  +-----+   +-----+  |  |                          |   |  +-----+   +-----+  | |" << std::endl;
    cout << "  |  |  S  |   |  F  |  |  |                          |   |  | FM  |   |  F  |  | |" << std::endl;
    cout << "  |  | CPU |   | GPU |  |  |                          |   |  | CPU |   | GPU |  | |" << std::endl;
    cout << "  |  | (" << source1_degree << ") +-->+ (" << filter1_degree << ") |  +--+                          +-->+  | (" << flatmap1_degree << ") +-->+ (" << filter4_degree << ") |  +-+" << std::endl;
    cout << "  |  +-----+   +-----+  |  |                          |   |  +-----+   +-----+  |" << std::endl;
    cout << "  +---------------------+  |       +-----------+      |   +---------------------+" << std::endl;
    cout << "                           |       |  +-----+  |      |" << std::endl;
    cout << "                           |       |  |  F  |  |      |" << std::endl;
    cout << "                           |       |  | CPU |  |      |" << std::endl;
    cout << "                           +------>+  | (" << filter3_degree << ") |  +------+" << std::endl;
    cout << "                                   |  +-----+  |" << std::endl;
    cout << "                                   +-----------+" << std::endl;
    // compute the total parallelism degree of the PipeGraph
    size_t check_degree = source1_degree;
    check_degree += filter1_degree;
    check_degree += map1_degree;
    if (map1_degree != map2_degree) {
        check_degree += map2_degree;
    }
    check_degree += filter3_degree;
    check_degree += flatmap1_degree;
    if (flatmap1_degree != filter4_degree) {
        check_degree += filter4_degree;
    }
    check_degree += source2_degree;
    check_degree += filter2_degree;
    check_degree += sink1_degree;
    // prepare the test
    PipeGraph graph("test_tracing_2", Execution_Mode_t::DEFAULT, Time_Policy_t::EVENT_TIME);
    // prepare the first MultiPipe
    Source_Positive_Functor source_functor_positive(stream_len, n_keys, true);
    Source source1 = Source_Builder(source_functor_positive)
                        .withName("source1")
                        .withParallelism(source1_degree)
                        .withOutputBatchSize(dist_b(rng))
                        .build();
    MultiPipe &pipe1 = graph.add_source(source1);
    Filter_Functor_GPU_KB filter_functor_gpu1;
    Filter_GPU filtergpu1 = FilterGPU_Builder(filter_functor_gpu1)
                                .withName("filter1")
                                .withParallelism(filter1_degree)
                                .withKeyBy([] __host__ __device__ (const tuple_t &t) -> size_t { return t.key; })
                                .build();
    pipe1.chain(filtergpu1);
    // split
    pipe1.split_gpu<tuple_t>(2);
    // prepare the second MultiPipe
    MultiPipe &pipe2 = pipe1.select(0);
    Map_Functor_GPU_KB map_functor_gpu1;
    Map_GPU mapgpu1 = MapGPU_Builder(map_functor_gpu1)
                            .withName("map1")
                            .withParallelism(map1_degree)
                            .withKeyBy([] __host__ __device__ (const tuple_t &t) -> size_t { return t.key; })
                            .build();
    pipe2.chain(mapgpu1);
    Map_Functor_GPU map_functor_gpu2;
    Map_GPU map2 = MapGPU_Builder(map_functor_gpu2)
                        .withName("map2")
                        .withParallelism(map2_degree)
                        .build();
    pipe2.chain(map2);
    // prepare the third MultiPipe
    MultiPipe &pipe3 = pipe1.select(1);
    Filter_Functor_KB filter_functor2;
    Filter filter2 = Filter_Builder(filter_functor2)
                            .withName("filter2")
                            .withParallelism(filter3_degree)
                            .withKeyBy([](const tuple_t &t) -> size_t { return t.key; })
                            .withOutputBatchSize(dist_b(rng))
                            .build();
    pipe3.chain(filter2);
    // prepare the fourth MultiPipe
    MultiPipe &pipe4 = pipe2.merge(pipe3);
    FlatMap_Functor flatmap_functor1;
    FlatMap flatmap1 = FlatMap_Builder(flatmap_functor1)
                            .withName("flatmap1")
                            .withParallelism(flatmap1_degree)
                            .withOutputBatchSize(dist_b(rng))
                            .build();
    pipe4.chain(flatmap1);
    Filter_Functor_GPU filter_functor_gpu3;
    Filter_GPU filter3 = FilterGPU_Builder(filter_functor_gpu3)
                            .withName("filter3")
                            .withParallelism(filter4_degree)
                            .build();
    pipe4.chain(filter3);
    // prepare the fifth MultiPipe
    Source_Negative_Functor source_functor_negative(stream_len, n_keys, true);
    Source source2 = Source_Builder(source_functor_negative)
                        .withName("source2")
                        .withParallelism(source2_degree)
                        .withOutputBatchSize(dist_b(rng))
                        .build();
    MultiPipe &pipe5 = graph.add_source(source2); 
    Filter_Functor_GPU_KB filter_functor_gpu4;
    Filter_GPU filter4 = FilterGPU_Builder(filter_functor_gpu4)
                                .withName("filter4")
                                .withParallelism(filter2_degree)
                                .withKeyBy([] __host__ __device__ (const tuple_t &t) -> size_t { return t.key; })
                                .build();
    pipe5.chain(filter4);
    // prepare the sixth MultiPipe
    MultiPipe &pipe6 = pipe4.merge(pipe5);
    Sink_Functor sink_functor;
    Sink sink = Sink_Builder(sink_functor)
                    .withName("sink")
                    .withParallelism(sink1_degree)
                    .build();
    pipe6.chain_sink(sink);
    assert(graph.getNumThreads() == check_degree);
    // run the application
    graph.run();
    return 0;
}
