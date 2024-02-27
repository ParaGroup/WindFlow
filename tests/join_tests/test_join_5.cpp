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
 *  Test 7 of general graphs of operators.
 *  
 *                                +---------------------+
 *                                |  +-----+   +-----+  |
 *                                |  |  F  |   |  M  |  +---+
 *                           +--->+  | (*) +-->+ (*) |  |   |   +---------------------+
 *                           |    |  +-----+   +-----+  |   |   |  +-----+   +-----+  |
 *                           |    +---------------------+   +-->+  |  J  |   | FM  |  |
 *                           |                              |   |  | (*) +-->+ (*) |  +---+
 *                           |    +---------------------+   |   |  +-----+   +-----+  |   |
 *                           |    |  +-----+   +-----+  |   |   +---------------------+   |
 *                           |    |  |  F  |   |  M  |  |   |                             |
 *                           +--->+  | (*) +-->+ (*) |  +---+                             |
 *                           |    |  +-----+   +-----+  |                                 |
 *                           |    +---------------------+                                 |
 *                           |                                                            |
 *  +---------------------+  |    +---------------------+                                 |   +---------------------+
 *  |  +-----+   +-----+  |  |    |  +-----+   +-----+  |                                 |   |  +-----+   +-----+  |
 *  |  |  S  |   |  M  |  |  |    |  |  F  |   |  M  |  +--+                              |   |  |  J  |   |  S  |  |
 *  |  | (*) +-->+ (*) |  +--+--->+  | (*) +-->+ (*) |  |  |                              +-->+  | (*) +-->+ (*) |  |
 *  |  +-----+   +-----+  |  |    |  +-----+   +-----+  |  |                              |   |  +-----+   +-----+  |
 *  +---------------------+  |    +---------------------+  |                              |   +---------------------+
 *                           |                             |                              |
 *                           |    +---------------------+  |    +---------------------+   |
 *                           |    |  +-----+   +-----+  |  |    |  +-----+   +-----+  |   |
 *                           |    |  |  F  |   |  M  |  |  |    |  |  M  |   | FM  |  |   |
 *                           +--->+  | (*) +-->+ (*) |  +--+--->+  | (*) +-->+ (*) |  +---+
 *                           |    |  +-----+   +-----+  |  |    |  +-----+   +-----+  |
 *                           |    +---------------------+  |    +---------------------+
 *                           |                             |
 *                           |    +---------------------+  |
 *                           |    |  +-----+   +-----+  |  |
 *                           |    |  |  F  |   |  M  |  |  |
 *                           +--->+  | (*) +-->+ (*) |  +--+
 *                                |  +-----+   +-----+  |
 *                                +---------------------+
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
        cout << argv[0] << " -r [runs] -l [stream_length] -k [n_keys] -L [lower bound in msec] -U [upper bound in msec]" << endl;
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
                cout << argv[0] << " -r [runs] -l [stream_length] -k [n_keys] -L [lower bound in msec] -U [upper bound in msec]" << endl;
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
    int map1_degree, map2_degree, map3_degree, map4_degree, map5_degree, map6_degree, join1_degree, join2_degree, map8_degree, flatmap1_degree, flatmap2_degree, sink1_degree;
    int filter1_degree, filter2_degree, filter3_degree, filter4_degree, filter5_degree;
    size_t source_degree = dist_p(rng);
    long last_result = 0;
    // executes the runs in DEFAULT mode
    for (size_t i=0; i<runs; i++) {
        map1_degree = dist_p(rng);
        map2_degree = dist_p(rng);
        map3_degree = dist_p(rng);
        map4_degree = dist_p(rng);
        map5_degree = dist_p(rng);
        map6_degree = dist_p(rng);
        join1_degree = dist_p(rng);
        join2_degree = dist_p(rng);
        map8_degree = dist_p(rng);
        flatmap1_degree = dist_p(rng);
        flatmap2_degree = dist_p(rng);
        filter1_degree = dist_p(rng);
        filter2_degree = dist_p(rng);
        filter3_degree = dist_p(rng);
        filter4_degree = dist_p(rng);
        filter5_degree = dist_p(rng);
        sink1_degree = dist_p(rng);
        cout << "Run " << i << endl;
        cout << "                              +---------------------+" << endl;
        cout << "                              |  +-----+   +-----+  |" << endl;
        cout << "                              |  |  F  |   |  M  |  +---+" << endl;
        cout << "                         +--->+  | (" << filter1_degree << ") +-->+ (" << map2_degree << ") |  |   |   +---------------------+" << endl;
        cout << "                         |    |  +-----+   +-----+  |   |   |  +-----+   +-----+  |" << endl;
        cout << "                         |    +---------------------+   +-->+  |  J  |   | FM  |  |" << endl;
        cout << "                         |                              |   |  | (" << join1_degree << ") +-->+ (" << flatmap1_degree << ") |  +---+" << endl;
        cout << "                         |    +---------------------+   |   |  +-----+   +-----+  |   |" << endl;
        cout << "                         |    |  +-----+   +-----+  |   |   +---------------------+   |" << endl;
        cout << "                         |    |  |  F  |   |  M  |  |   |                             |" << endl;
        cout << "                         +--->+  | (" << filter2_degree << ") +-->+ (" << map3_degree << ") |  +---+                             |" << endl;
        cout << "                         |    |  +-----+   +-----+  |                                 |" << endl;
        cout << "                         |    +---------------------+                                 |" << endl;
        cout << "                         |                                                            |" << endl;
        cout << "+---------------------+  |    +---------------------+                                 |   +---------------------+" << endl;
        cout << "|  +-----+   +-----+  |  |    |  +-----+   +-----+  |                                 |   |  +-----+   +-----+  |" << endl;
        cout << "|  |  S  |   |  M  |  |  |    |  |  F  |   |  M  |  +--+                              |   |  |  J  |   |  S  |  |" << endl;
        cout << "|  | (" << source_degree << ") +-->+ (" << map1_degree << ") |  +--+--->+  | (" << filter3_degree << ") +-->+ (" << map4_degree << ") |  |  |                              +-->+  | (" << join2_degree << ") +-->+ (" << sink1_degree << ") |  |" << endl;
        cout << "|  +-----+   +-----+  |  |    |  +-----+   +-----+  |  |                              |   |  +-----+   +-----+  |" << endl;
        cout << "+---------------------+  |    +---------------------+  |                              |   +---------------------+" << endl;
        cout << "                         |                             |                              |" << endl;
        cout << "                         |    +---------------------+  |    +---------------------+   |" << endl;
        cout << "                         |    |  +-----+   +-----+  |  |    |  +-----+   +-----+  |   |" << endl;
        cout << "                         |    |  |  F  |   |  M  |  |  |    |  |  M  |   | FM  |  |   |" << endl;
        cout << "                         +--->+  | (" << filter4_degree << ") +-->+ (" << map5_degree << ") |  +--+--->+  | (" << map8_degree << ") +-->+ (" << flatmap2_degree << ") |  +---+" << endl;
        cout << "                         |    |  +-----+   +-----+  |  |    |  +-----+   +-----+  |" << endl;
        cout << "                         |    +---------------------+  |    +---------------------+" << endl;
        cout << "                         |                             |" << endl;
        cout << "                         |    +---------------------+  |" << endl;
        cout << "                         |    |  +-----+   +-----+  |  |" << endl;
        cout << "                         |    |  |  F  |   |  M  |  |  |" << endl;
        cout << "                         +--->+  | (" << filter5_degree << ") +-->+ (" << map6_degree << ") |  +--+" << endl;
        cout << "                              |  +-----+   +-----+  |" << endl;
        cout << "                              +---------------------+" << endl;
        // compute the total parallelism degree of the PipeGraph
        size_t check_degree = source_degree;
        check_degree += map1_degree;
        check_degree += filter1_degree;
        check_degree += map2_degree;
        check_degree += filter2_degree;
        check_degree += map3_degree;
        check_degree += filter3_degree;
        check_degree += map4_degree;
        check_degree += filter4_degree;
        check_degree += map5_degree;
        check_degree += filter5_degree;
        check_degree += map6_degree;
        check_degree += join1_degree;
        check_degree += map8_degree;
        check_degree += flatmap1_degree;
        check_degree += flatmap2_degree;
        check_degree += join2_degree;
        check_degree += sink1_degree;
        // prepare the test
        PipeGraph graph("test_join_5 (DEFAULT)", Execution_Mode_t::DEFAULT, Time_Policy_t::EVENT_TIME);
        // prepare the first MultiPipe
        Source_Positive_Functor source_functor_positive(stream_len, n_keys, true);
        Source source = Source_Builder(source_functor_positive)
                            .withName("source")
                            .withParallelism(source_degree)
                            .withOutputBatchSize(dist_b(rng))
                            .build();
        MultiPipe &pipe1 = graph.add_source(source);
        Map_Functor map_functor1;
        Map map1 = Map_Builder(map_functor1)
                        .withName("map1")
                        .withParallelism(map1_degree)
                        .withKeyBy([](const tuple_t &t) -> size_t { return t.key; })
                        .withOutputBatchSize(dist_b(rng))
                        .build();
        pipe1.chain(map1);
        // split
        pipe1.split([](const tuple_t &t) {
            if (t.value % 2 == 0) {
                if (t.value % 4 == 0) {
                    return 0;
                }
                else {
                    return 1;
                }
            }
            else {
                if (t.value % 3 == 0) {
                    return 2;
                }
                else if (t.value % 7 == 0) {
                    return 3;
                }
                else {
                    return 4;
                }
            }
        }, 5);
        // prepare the second MultiPipe
        MultiPipe &pipe2 = pipe1.select(0);
        Filter_Functor_KB filter_functor1(8);
        Filter filter1 = Filter_Builder(filter_functor1)
                        .withName("filter1")
                        .withParallelism(filter1_degree)
                        .withKeyBy([](const tuple_t &t) -> size_t { return t.key; })
                        .withOutputBatchSize(dist_b(rng))
                        .build();
        pipe2.chain(filter1);
        Map_Functor map_functor2;
        Map map2 = Map_Builder(map_functor2)
                        .withName("map2")
                        .withParallelism(map2_degree)
                        .withKeyBy([](const tuple_t &t) -> size_t { return t.key; })
                        .withOutputBatchSize(dist_b(rng))
                        .build();
        pipe2.chain(map2);
        // prepare the third MultiPipe
        MultiPipe &pipe3 = pipe1.select(1);
        Filter_Functor_KB filter_functor2(6);
        Filter filter2 = Filter_Builder(filter_functor2)
                        .withName("filter2")
                        .withParallelism(filter2_degree)
                        .withKeyBy([](const tuple_t &t) -> size_t { return t.key; })
                        .withOutputBatchSize(dist_b(rng))
                        .build();
        pipe3.chain(filter2);
        Map_Functor map_functor3;
        Map map3 = Map_Builder(map_functor3)
                        .withName("map3")
                        .withParallelism(map3_degree)
                        .withKeyBy([](const tuple_t &t) -> size_t { return t.key; })
                        .withOutputBatchSize(dist_b(rng))
                        .build();
        pipe3.chain(map3);
        // prepare the fourth MultiPipe
        MultiPipe &pipe4 = pipe1.select(2);
        Filter_Functor_KB filter_functor3(7);
        Filter filter3 = Filter_Builder(filter_functor3)
                        .withName("filter3")
                        .withParallelism(filter3_degree)
                        .withKeyBy([](const tuple_t &t) -> size_t { return t.key; })
                        .withOutputBatchSize(dist_b(rng))
                        .build();
        pipe4.chain(filter3);
        Map_Functor map_functor4;
        Map map4 = Map_Builder(map_functor4)
                        .withName("map4")
                        .withParallelism(map4_degree)
                        .withKeyBy([](const tuple_t &t) -> size_t { return t.key; })
                        .withOutputBatchSize(dist_b(rng))
                        .build();
        pipe4.chain(map4);
        // prepare the fifth MultiPipe
        MultiPipe &pipe5 = pipe1.select(3);
        Filter_Functor_KB filter_functor4(7);
        Filter filter4 = Filter_Builder(filter_functor4)
                        .withName("filter4")
                        .withParallelism(filter4_degree)
                        .withKeyBy([](const tuple_t &t) -> size_t { return t.key; })
                        .withOutputBatchSize(dist_b(rng))
                        .build();
        pipe5.chain(filter4);
        Map_Functor map_functor5;
        Map map5 = Map_Builder(map_functor5)
                        .withName("map5")
                        .withParallelism(map5_degree)
                        .withKeyBy([](const tuple_t &t) -> size_t { return t.key; })
                        .withOutputBatchSize(dist_b(rng))
                        .build();
        pipe5.chain(map5);
        // prepare the sixth MultiPipe
        MultiPipe &pipe6 = pipe1.select(4);
        Filter_Functor_KB filter_functor5(27);
        Filter filter5 = Filter_Builder(filter_functor5)
                        .withName("filter5")
                        .withParallelism(filter5_degree)
                        .withKeyBy([](const tuple_t &t) -> size_t { return t.key; })
                        .withOutputBatchSize(dist_b(rng))
                        .build();
        pipe6.chain(filter5);
        Map_Functor map_functor6;
        Map map6 = Map_Builder(map_functor6)
                        .withName("map6")
                        .withParallelism(map6_degree)
                        .withKeyBy([](const tuple_t &t) -> size_t { return t.key; })
                        .withOutputBatchSize(dist_b(rng))
                        .build();
        pipe6.chain(map6);
        // prepare the seventh MultiPipe
        MultiPipe &pipe7 = pipe2.merge(pipe3);
        Join_Functor join_functor1;
        Interval_Join join1 = Interval_Join_Builder(join_functor1)
                                    .withName("join1")
                                    .withParallelism(join1_degree)
                                    .withOutputBatchSize(dist_b(rng))
                                    .withKeyBy([](const tuple_t &t) -> size_t { return t.key; })
                                    .withBoundaries(milliseconds(lower_bound), milliseconds(upper_bound))
                                    .withKPMode()
                                    .build();
        pipe7.add(join1);
        FlatMap_Functor flatmap_functor1;
        FlatMap flatmap1 = FlatMap_Builder(flatmap_functor1)
                            .withName("flatmap1")
                            .withParallelism(flatmap1_degree)
                            .withKeyBy([](const tuple_t &t) -> size_t { return t.key; })
                            .withOutputBatchSize(dist_b(rng))
                            .build();
        pipe7.chain(flatmap1);
        // prepare the eightth MultiPipe
        MultiPipe &pipe8 = pipe6.merge(pipe5, pipe4);
        Map_Functor map_functor8;
        Map map8 = Map_Builder(map_functor8)
                        .withName("map8")
                        .withParallelism(map8_degree)
                        .withKeyBy([](const tuple_t &t) -> size_t { return t.key; })
                        .withOutputBatchSize(dist_b(rng))
                        .build();
        pipe8.chain(map8);
        // map 10
        FlatMap_Functor flatmap_functor2;
        FlatMap flatmap2 = FlatMap_Builder(flatmap_functor2)
                            .withName("flatmap2")
                            .withParallelism(flatmap2_degree)
                            .withKeyBy([](const tuple_t &t) -> size_t { return t.key; })
                            .withOutputBatchSize(dist_b(rng))
                            .build();
        pipe8.chain(flatmap2);        
        // prepare the ninth MultiPipe
        MultiPipe &pipe9 = pipe7.merge(pipe8);
        Distinct_Join_Functor join_functor2;
        Interval_Join join2 = Interval_Join_Builder(join_functor2)
                                    .withName("join2")
                                    .withParallelism(join2_degree)
                                    .withOutputBatchSize(dist_b(rng))
                                    .withKeyBy([](const tuple_t &t) -> size_t { return t.key; })
                                    .withBoundaries(milliseconds(lower_bound), milliseconds(upper_bound))
                                    .withDPSMode()
                                    .build();
        pipe9.add(join2);
        Sink_Functor sink_functor;
        Sink sink = Sink_Builder(sink_functor)
                        .withName("sink")
                        .withParallelism(sink1_degree)
                        .withKeyBy([](const tuple_t &t) -> size_t { return t.key; })
                        .build();
        pipe9.chain_sink(sink);
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
    // executes the runs in DETERMINISTIC mode
    for (size_t i=0; i<runs; i++) {
        map1_degree = dist_p(rng);
        map2_degree = dist_p(rng);
        map3_degree = dist_p(rng);
        map4_degree = dist_p(rng);
        map5_degree = dist_p(rng);
        map6_degree = dist_p(rng);
        join1_degree = dist_p(rng);
        join2_degree = dist_p(rng);
        map8_degree = dist_p(rng);
        flatmap1_degree = dist_p(rng);
        flatmap2_degree = dist_p(rng);
        filter1_degree = dist_p(rng);
        filter2_degree = dist_p(rng);
        filter3_degree = dist_p(rng);
        filter4_degree = dist_p(rng);
        filter5_degree = dist_p(rng);
        sink1_degree = dist_p(rng);
        cout << "Run " << i << endl;
        cout << "                              +---------------------+" << endl;
        cout << "                              |  +-----+   +-----+  |" << endl;
        cout << "                              |  |  F  |   |  M  |  +---+" << endl;
        cout << "                         +--->+  | (" << filter1_degree << ") +-->+ (" << map2_degree << ") |  |   |   +---------------------+" << endl;
        cout << "                         |    |  +-----+   +-----+  |   |   |  +-----+   +-----+  |" << endl;
        cout << "                         |    +---------------------+   +-->+  |  J  |   | FM  |  |" << endl;
        cout << "                         |                              |   |  | (" << join1_degree << ") +-->+ (" << flatmap1_degree << ") |  +---+" << endl;
        cout << "                         |    +---------------------+   |   |  +-----+   +-----+  |   |" << endl;
        cout << "                         |    |  +-----+   +-----+  |   |   +---------------------+   |" << endl;
        cout << "                         |    |  |  F  |   |  M  |  |   |                             |" << endl;
        cout << "                         +--->+  | (" << filter2_degree << ") +-->+ (" << map3_degree << ") |  +---+                             |" << endl;
        cout << "                         |    |  +-----+   +-----+  |                                 |" << endl;
        cout << "                         |    +---------------------+                                 |" << endl;
        cout << "                         |                                                            |" << endl;
        cout << "+---------------------+  |    +---------------------+                                 |   +---------------------+" << endl;
        cout << "|  +-----+   +-----+  |  |    |  +-----+   +-----+  |                                 |   |  +-----+   +-----+  |" << endl;
        cout << "|  |  S  |   |  M  |  |  |    |  |  F  |   |  M  |  +--+                              |   |  |  J  |   |  S  |  |" << endl;
        cout << "|  | (" << source_degree << ") +-->+ (" << map1_degree << ") |  +--+--->+  | (" << filter3_degree << ") +-->+ (" << map4_degree << ") |  |  |                              +-->+  | (" << join2_degree << ") +-->+ (" << sink1_degree << ") |  |" << endl;
        cout << "|  +-----+   +-----+  |  |    |  +-----+   +-----+  |  |                              |   |  +-----+   +-----+  |" << endl;
        cout << "+---------------------+  |    +---------------------+  |                              |   +---------------------+" << endl;
        cout << "                         |                             |                              |" << endl;
        cout << "                         |    +---------------------+  |    +---------------------+   |" << endl;
        cout << "                         |    |  +-----+   +-----+  |  |    |  +-----+   +-----+  |   |" << endl;
        cout << "                         |    |  |  F  |   |  M  |  |  |    |  |  M  |   | FM  |  |   |" << endl;
        cout << "                         +--->+  | (" << filter4_degree << ") +-->+ (" << map5_degree << ") |  +--+--->+  | (" << map8_degree << ") +-->+ (" << flatmap2_degree << ") |  +---+" << endl;
        cout << "                         |    |  +-----+   +-----+  |  |    |  +-----+   +-----+  |" << endl;
        cout << "                         |    +---------------------+  |    +---------------------+" << endl;
        cout << "                         |                             |" << endl;
        cout << "                         |    +---------------------+  |" << endl;
        cout << "                         |    |  +-----+   +-----+  |  |" << endl;
        cout << "                         |    |  |  F  |   |  M  |  |  |" << endl;
        cout << "                         +--->+  | (" << filter5_degree << ") +-->+ (" << map6_degree << ") |  +--+" << endl;
        cout << "                              |  +-----+   +-----+  |" << endl;
        cout << "                              +---------------------+" << endl;
        // compute the total parallelism degree of the PipeGraph
        size_t check_degree = source_degree;
        check_degree += map1_degree;
        check_degree += filter1_degree;
        check_degree += map2_degree;
        check_degree += filter2_degree;
        check_degree += map3_degree;
        check_degree += filter3_degree;
        check_degree += map4_degree;
        check_degree += filter4_degree;
        check_degree += map5_degree;
        check_degree += filter5_degree;
        check_degree += map6_degree;
        check_degree += join1_degree;
        check_degree += map8_degree;
        check_degree += flatmap1_degree;
        check_degree += flatmap2_degree;
        check_degree += join2_degree;
        check_degree += sink1_degree;
        // prepare the test
        PipeGraph graph("test_join_5 (DETERMINISTIC)", Execution_Mode_t::DETERMINISTIC, Time_Policy_t::EVENT_TIME);
        // prepare the first MultiPipe
        Source_Positive_Functor source_functor_positive(stream_len, n_keys, false);
        Source source = Source_Builder(source_functor_positive)
                            .withName("source")
                            .withParallelism(source_degree)
                            .build();
        MultiPipe &pipe1 = graph.add_source(source);
        Map_Functor map_functor1;
        Map map1 = Map_Builder(map_functor1)
                        .withName("map1")
                        .withParallelism(map1_degree)
                        .withKeyBy([](const tuple_t &t) -> size_t { return t.key; })
                        .build();
        pipe1.chain(map1);
        // split
        pipe1.split([](const tuple_t &t) {
            if (t.value % 2 == 0) {
                if (t.value % 4 == 0) {
                    return 0;
                }
                else {
                    return 1;
                }
            }
            else {
                if (t.value % 3 == 0) {
                    return 2;
                }
                else if (t.value % 7 == 0) {
                    return 3;
                }
                else {
                    return 4;
                }
            }
        }, 5);
        // prepare the second MultiPipe
        MultiPipe &pipe2 = pipe1.select(0);
        Filter_Functor_KB filter_functor1(8);
        Filter filter1 = Filter_Builder(filter_functor1)
                        .withName("filter1")
                        .withParallelism(filter1_degree)
                        .withKeyBy([](const tuple_t &t) -> size_t { return t.key; })
                        .build();
        pipe2.chain(filter1);
        Map_Functor map_functor2;
        Map map2 = Map_Builder(map_functor2)
                        .withName("map2")
                        .withParallelism(map2_degree)
                        .withKeyBy([](const tuple_t &t) -> size_t { return t.key; })
                        .build();
        pipe2.chain(map2);
        // prepare the third MultiPipe
        MultiPipe &pipe3 = pipe1.select(1);
        Filter_Functor_KB filter_functor2(6);
        Filter filter2 = Filter_Builder(filter_functor2)
                        .withName("filter2")
                        .withParallelism(filter2_degree)
                        .withKeyBy([](const tuple_t &t) -> size_t { return t.key; })
                        .build();
        pipe3.chain(filter2);
        Map_Functor map_functor3;
        Map map3 = Map_Builder(map_functor3)
                        .withName("map3")
                        .withParallelism(map3_degree)
                        .withKeyBy([](const tuple_t &t) -> size_t { return t.key; })
                        .build();
        pipe3.chain(map3);
        // prepare the fourth MultiPipe
        MultiPipe &pipe4 = pipe1.select(2);
        Filter_Functor_KB filter_functor3(7);
        Filter filter3 = Filter_Builder(filter_functor3)
                        .withName("filter3")
                        .withParallelism(filter3_degree)
                        .withKeyBy([](const tuple_t &t) -> size_t { return t.key; })
                        .build();
        pipe4.chain(filter3);
        Map_Functor map_functor4;
        Map map4 = Map_Builder(map_functor4)
                        .withName("map4")
                        .withParallelism(map4_degree)
                        .withKeyBy([](const tuple_t &t) -> size_t { return t.key; })
                        .build();
        pipe4.chain(map4);
        // prepare the fifth MultiPipe
        MultiPipe &pipe5 = pipe1.select(3);
        Filter_Functor_KB filter_functor4(7);
        Filter filter4 = Filter_Builder(filter_functor4)
                        .withName("filter4")
                        .withParallelism(filter4_degree)
                        .withKeyBy([](const tuple_t &t) -> size_t { return t.key; })
                        .build();
        pipe5.chain(filter4);
        Map_Functor map_functor5;
        Map map5 = Map_Builder(map_functor5)
                        .withName("map5")
                        .withParallelism(map5_degree)
                        .withKeyBy([](const tuple_t &t) -> size_t { return t.key; })
                        .build();
        pipe5.chain(map5);
        // prepare the sixth MultiPipe
        MultiPipe &pipe6 = pipe1.select(4);
        Filter_Functor_KB filter_functor5(27);
        Filter filter5 = Filter_Builder(filter_functor5)
                        .withName("filter5")
                        .withParallelism(filter5_degree)
                        .withKeyBy([](const tuple_t &t) -> size_t { return t.key; })
                        .build();
        pipe6.chain(filter5);
        Map_Functor map_functor6;
        Map map6 = Map_Builder(map_functor6)
                        .withName("map6")
                        .withParallelism(map6_degree)
                        .withKeyBy([](const tuple_t &t) -> size_t { return t.key; })
                        .build();
        pipe6.chain(map6);
        // prepare the seventh MultiPipe
        MultiPipe &pipe7 = pipe2.merge(pipe3);
        Join_Functor join_functor1;
        Interval_Join join1 = Interval_Join_Builder(join_functor1)
                                    .withName("join1")
                                    .withParallelism(join1_degree)
                                    .withKeyBy([](const tuple_t &t) -> size_t { return t.key; })
                                    .withBoundaries(milliseconds(lower_bound), milliseconds(upper_bound))
                                    .withKPMode()
                                    .build();
        pipe7.add(join1);
        FlatMap_Functor flatmap_functor1;
        FlatMap flatmap1 = FlatMap_Builder(flatmap_functor1)
                            .withName("flatmap1")
                            .withParallelism(flatmap1_degree)
                            .withKeyBy([](const tuple_t &t) -> size_t { return t.key; })
                            .build();
        pipe7.chain(flatmap1);
        // prepare the eightth MultiPipe
        MultiPipe &pipe8 = pipe6.merge(pipe5, pipe4);
        Map_Functor map_functor8;
        Map map8 = Map_Builder(map_functor8)
                        .withName("map8")
                        .withParallelism(map8_degree)
                        .withKeyBy([](const tuple_t &t) -> size_t { return t.key; })
                        .build();
        pipe8.chain(map8);
        // map 10
        FlatMap_Functor flatmap_functor2;
        FlatMap flatmap2 = FlatMap_Builder(flatmap_functor2)
                            .withName("flatmap2")
                            .withParallelism(flatmap2_degree)
                            .withKeyBy([](const tuple_t &t) -> size_t { return t.key; })
                            .build();
        pipe8.chain(flatmap2);        
        // prepare the ninth MultiPipe
        MultiPipe &pipe9 = pipe7.merge(pipe8);
        Distinct_Join_Functor join_functor2;
        Interval_Join join2 = Interval_Join_Builder(join_functor2)
                                    .withName("join2")
                                    .withParallelism(join2_degree)
                                    .withKeyBy([](const tuple_t &t) -> size_t { return t.key; })
                                    .withBoundaries(milliseconds(lower_bound), milliseconds(upper_bound))
                                    .withDPSMode()
                                    .build();
        pipe9.add(join2);
        Sink_Functor sink_functor;
        Sink sink = Sink_Builder(sink_functor)
                        .withName("sink")
                        .withParallelism(sink1_degree)
                        .withKeyBy([](const tuple_t &t) -> size_t { return t.key; })
                        .build();
        pipe9.chain_sink(sink);
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
