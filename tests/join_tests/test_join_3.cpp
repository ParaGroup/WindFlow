/**************************************************************************************
 *  Copyright (c) 2023- Gabriele Mencagli and Yuriy Rymarchuk
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
 *  Test 3 of merge between MultiPipes.
 *  
 *  +-----------+
 *  |  +-----+  |
 *  |  |  S  |  |
 *  |  | (*) |  +-------------+
 *  |  +-----+  |             |    +-----------+
 *  +-----------+             |    |  +-----+  |
 *                            |    |  |  J  |  |
 *  +---------------------+   +--->+  | (*) |  +---+
 *  |  +-----+   +-----+  |   |    |  +-----+  |   |
 *  |  |  S  |   |  M  |  |   |    +-----------+   |    +---------------------+
 *  |  | (*) +-->+ (*) |  +---+                    |    |  +-----+   +-----+  |
 *  |  +-----+   +-----+  |                        |    |  | FM  |   |  S  |  |
 *  +---------------------+                        +--->+  | (*) +-->+ (*) |  |
 *                                                 |    |  +-----+   +-----+  |
 *  +---------------------+                        |    +---------------------+
 *  |  +-----+   +-----+  |                        |
 *  |  |  S  |   |  M  |  |                        |
 *  |  | (*) +-->+ (*) |  +------------------------+
 *  |  +-----+   +-----+  |
 *  +---------------------+
 */ 

// include
#include<random>
#include<iostream>
#include<windflow.hpp>
#include"join_common.hpp"

using namespace std;
using namespace chrono;
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
    int64_t lower_bound = 0;
    int64_t upper_bound = 0;
    // initalize global variable
    global_sum = 0;
    // arguments from command line
    if (argc != 11) {
        cout << argv[0] << " -r [runs] -l [stream_length] -k [n_keys] -L [lower bound in usec] -U [upper bound in usec]" << endl;
        exit(EXIT_SUCCESS);
    }
    while ((option = getopt(argc, argv, "r:l:k:L:U:")) != -1) {
        switch (option) {
            case 'r': runs = atoi(optarg);
                     break;
            case 'l': stream_len = atoi(optarg);
                     break;
            case 'k': n_keys = atoi(optarg);
                     break;
            case 'L': lower_bound = atoi(optarg);
                    break;
            case 'U': upper_bound = atoi(optarg);
                    break;
            default: {
                cout << argv[0] << " -r [runs] -l [stream_length] -k [n_keys] -L [lower bound in usec] -U [upper bound in usec]" << endl;
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
    std::uniform_int_distribution<std::mt19937::result_type> dist_b(0, 10);
    int map1_degree, map2_degree, join_degree, flatmap_degree, sink_degree;
    size_t source1_degree = 1; //dist_p(rng);
    size_t source2_degree = 1; //dist_p(rng);
    size_t source3_degree = 1; //dist_p(rng);
    long last_result = 0;
    // executes the runs in DEFAULT mode
    for (size_t i=0; i<runs; i++) {
        map1_degree = dist_p(rng);
        map2_degree = dist_p(rng);
        flatmap_degree = dist_p(rng);
        join_degree = 3; //dist_p(rng);
        sink_degree = dist_p(rng);
        cout << "Run " << i << endl;
        cout << "+-----------+" << endl;
        cout << "|  +-----+  |" << endl;
        cout << "|  |  S  |  |" << endl;
        cout << "|  | (" << source1_degree <<") |  +-------------+" << endl;
        cout << "|  +-----+  |             |    +-----------+" << endl;
        cout << "+-----------+             |    |  +-----+  |" << endl;
        cout << "                          |    |  |  J  |  |" << endl;
        cout << "+---------------------+   +--->+  | (" << join_degree << ") |  +---+" << endl;
        cout << "|  +-----+   +-----+  |   |    |  +-----+  |   |" << endl;
        cout << "|  |  S  |   |  M  |  |   |    +-----------+   |    +---------------------+" << endl;
        cout << "|  | (" << source2_degree << ") +-->+ (" << map1_degree << ") |  +---+                    |    |  +-----+   +-----+  |" << endl;
        cout << "|  +-----+   +-----+  |                        |    |  |  M  |   |  S  |  |" << endl;
        cout << "+---------------------+                        +--->+  | (" << flatmap_degree << ") +-->+ (" << sink_degree << ") |  |" << endl;
        cout << "                                               |    |  +-----+   +-----+  |" << endl;
        cout << "+---------------------+                        |    +---------------------+" << endl;
        cout << "|  +-----+   +-----+  |                        |" << endl;
        cout << "|  |  S  |   |  M  |  |                        |" << endl;
        cout << "|  | (" << source3_degree << ") +-->+ (" << map2_degree << ") |  +------------------------+" << endl;
        cout << "|  +-----+   +-----+  |" << endl;
        cout << "+---------------------+" << endl;
        // compute the total parallelism degree of the PipeGraph
        size_t check_degree = source1_degree;
        check_degree += source2_degree;
        if (source2_degree != map1_degree) {
            check_degree += map1_degree;
        }
        check_degree += join_degree;
        check_degree += source3_degree;
        if (source3_degree != map2_degree) {
            check_degree += map2_degree;
        }
        check_degree += flatmap_degree;
        if (flatmap_degree != sink_degree) {
            check_degree += sink_degree;
        }
        // prepare the test
        PipeGraph graph("test_join_3 (DEFAULT)", Execution_Mode_t::DEFAULT, Time_Policy_t::EVENT_TIME);
        // prepare the first MultiPipe
        Source_Positive_Functor source_functor1(stream_len, n_keys, true);
        Source source1 = Source_Builder(source_functor1)
                                .withName("source")
                                .withParallelism(source1_degree)
                                .withOutputBatchSize(dist_b(rng))
                                .build();
        MultiPipe &pipe1 = graph.add_source(source1);
        // prepare the second MultiPipe
        Source_Positive_Functor source_functor2(stream_len, n_keys, true);
        Source source2 = Source_Builder(source_functor2)
                                .withName("source2")
                                .withParallelism(source2_degree)
                                .withOutputBatchSize(dist_b(rng))
                                .build();
        MultiPipe &pipe2 = graph.add_source(source2);
        // map 1
        Map_Functor map_functor1;
        Map map1 = Map_Builder(map_functor1)
                        .withName("map1")
                        .withParallelism(map1_degree)
                        .withOutputBatchSize(dist_b(rng))
                        .build();
        pipe2.chain(map1);
        // prepare the third MultiPipe
        MultiPipe &pipe3 = pipe1.merge(pipe2);
        Join_Functor join_functor;
        Interval_Join join = Interval_Join_Builder(join_functor)
                                    .withName("join")
                                    .withParallelism(join_degree)
                                    .withOutputBatchSize(dist_b(rng))
                                    .withKeyBy([](const tuple_t &t) -> size_t { return t.key; })
                                    .withBoundaries(microseconds(lower_bound), microseconds(upper_bound))
                                    .withDPSMode()
                                    .build();
        pipe3.add(join);
        // prepare the fourth MultiPipe
        Source_Negative_Functor source_functor3(stream_len, n_keys, true);
        Source source3 = Source_Builder(source_functor3)
                                .withName("source3")
                                .withParallelism(source3_degree)
                                .withOutputBatchSize(dist_b(rng))
                                .build();
        MultiPipe &pipe4 = graph.add_source(source3);
        Map_Functor map_functor2;
        Map map2 = Map_Builder(map_functor2)
                        .withName("map2")
                        .withParallelism(map2_degree)
                        .withOutputBatchSize(dist_b(rng))
                        .build();
        pipe4.chain(map2);
        // prepare the fifth MultiPipe
        MultiPipe &pipe5 = pipe3.merge(pipe4);
        FlatMap_Functor flatmap_functor;
        FlatMap flatmap = FlatMap_Builder(flatmap_functor)
                            .withName("flatmap")
                            .withParallelism(flatmap_degree)
                            .withKeyBy([](const tuple_t &t) -> size_t { return t.key; })
                            .withOutputBatchSize(dist_b(rng))
                            .build();
        pipe5.chain(flatmap);
        Sink_Functor sink_functor;
        Sink sink = Sink_Builder(sink_functor)
                            .withName("sink")
                            .withParallelism(sink_degree)
                            .build();
        pipe5.chain_sink(sink);
        assert(graph.getNumThreads() == check_degree);
        // run the application
        graph.run();
        cout << "Result is --> " << GREEN << "OK" << DEFAULT_COLOR << " value " << global_sum.load() << endl;
        global_sum = 0;
    }
    // executes the runs in DETERMINISTIC mode
    for (size_t i=0; i<0; i++) {
        map1_degree = dist_p(rng);
        map2_degree = dist_p(rng);
        flatmap_degree = dist_p(rng);
        join_degree = dist_p(rng);
        sink_degree = dist_p(rng);
        cout << "Run " << i << endl;
        cout << "+-----------+" << endl;
        cout << "|  +-----+  |" << endl;
        cout << "|  |  S  |  |" << endl;
        cout << "|  | (" << source1_degree <<") |  +-------------+" << endl;
        cout << "|  +-----+  |             |    +-----------+" << endl;
        cout << "+-----------+             |    |  +-----+  |" << endl;
        cout << "                          |    |  |  J  |  |" << endl;
        cout << "+---------------------+   +--->+  | (" << join_degree << ") |  +---+" << endl;
        cout << "|  +-----+   +-----+  |   |    |  +-----+  |   |" << endl;
        cout << "|  |  S  |   |  M  |  |   |    +-----------+   |    +---------------------+" << endl;
        cout << "|  | (" << source2_degree << ") +-->+ (" << map1_degree << ") |  +---+                    |    |  +-----+   +-----+  |" << endl;
        cout << "|  +-----+   +-----+  |                        |    |  |  M  |   |  S  |  |" << endl;
        cout << "+---------------------+                        +--->+  | (" << flatmap_degree << ") +-->+ (" << sink_degree << ") |  |" << endl;
        cout << "                                               |    |  +-----+   +-----+  |" << endl;
        cout << "+---------------------+                        |    +---------------------+" << endl;
        cout << "|  +-----+   +-----+  |                        |" << endl;
        cout << "|  |  S  |   |  M  |  |                        |" << endl;
        cout << "|  | (" << source3_degree << ") +-->+ (" << map2_degree << ") |  +------------------------+" << endl;
        cout << "|  +-----+   +-----+  |" << endl;
        cout << "+---------------------+" << endl;
        // compute the total parallelism degree of the PipeGraph
        size_t check_degree = source1_degree;
        check_degree += source2_degree;
        if (source2_degree != map1_degree) {
            check_degree += map1_degree;
        }
        check_degree += join_degree;
        check_degree += source3_degree;
        if (source3_degree != map2_degree) {
            check_degree += map2_degree;
        }
        check_degree += flatmap_degree;
        if (flatmap_degree != sink_degree) {
            check_degree += sink_degree;
        }
        // prepare the test
        PipeGraph graph("test_join_3 (DETERMINISTIC)", Execution_Mode_t::DETERMINISTIC, Time_Policy_t::EVENT_TIME);
        // prepare the first MultiPipe
        Source_Positive_Functor source_functor1(stream_len, n_keys, false);
        Source source1 = Source_Builder(source_functor1)
                                .withName("source")
                                .withParallelism(source1_degree)
                                .build();
        MultiPipe &pipe1 = graph.add_source(source1);
        // prepare the second MultiPipe
        Source_Positive_Functor source_functor2(stream_len, n_keys, false);
        Source source2 = Source_Builder(source_functor2)
                                .withName("source2")
                                .withParallelism(source2_degree)
                                .build();
        MultiPipe &pipe2 = graph.add_source(source2);
        // map 1
        Map_Functor map_functor1;
        Map map1 = Map_Builder(map_functor1)
                        .withName("map1")
                        .withParallelism(map1_degree)
                        .build();
        pipe2.chain(map1);
        // prepare the third MultiPipe
        MultiPipe &pipe3 = pipe1.merge(pipe2);
        Join_Functor join_functor;
        Interval_Join join = Interval_Join_Builder(join_functor)
                                    .withName("join")
                                    .withParallelism(join_degree)
                                    .withKeyBy([](const tuple_t &t) -> size_t { return t.key; })
                                    .withBoundaries(microseconds(lower_bound), microseconds(upper_bound))
                                    .withDPSMode()
                                    .build();
        pipe3.add(join);
        // prepare the fourth MultiPipe
        Source_Negative_Functor source_functor3(stream_len, n_keys, false);
        Source source3 = Source_Builder(source_functor3)
                                .withName("source3")
                                .withParallelism(source3_degree)
                                .build();
        MultiPipe &pipe4 = graph.add_source(source3);
        Map_Functor map_functor2;
        Map map2 = Map_Builder(map_functor2)
                        .withName("map2")
                        .withParallelism(map2_degree)
                        .build();
        pipe4.chain(map2);
        // prepare the fifth MultiPipe
        MultiPipe &pipe5 = pipe3.merge(pipe4);
        FlatMap_Functor flatmap_functor;
        FlatMap flatmap = FlatMap_Builder(flatmap_functor)
                            .withName("flatmap")
                            .withParallelism(flatmap_degree)
                            .withKeyBy([](const tuple_t &t) -> size_t { return t.key; })
                            .build();
        pipe5.chain(flatmap);
        Sink_Functor sink_functor;
        Sink sink = Sink_Builder(sink_functor)
                            .withName("sink")
                            .withParallelism(sink_degree)
                            .build();
        pipe5.chain_sink(sink);
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
