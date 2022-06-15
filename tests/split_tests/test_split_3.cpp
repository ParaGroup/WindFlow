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
 *  Test 3 split of MultiPipes.
 *  
 *                                                  +---------------------+
 *                                                  |  +-----+   +-----+  |
 *                                                  |  |  M  |   |  S  |  |
 *                      +---------------------+  +->+  | (*) +-->+ (*) |  |
 *                      |  +-----+   +-----+  |  |  |  +-----+   +-----+  |
 *                 +--->+  |  M  |   |  F  |  |  |  +---------------------+
 *  +-----------+  |    |  | (*) +-->+ (*) |  +--+
 *  |  +-----+  |  |    |  +-----+   +-----+  |  |  +-----------+
 *  |  |  S  |  |  |    +---------------------+  |  |  +-----+  |
 *  |  | (*) |  +--+                             +->+  |  S  |  |
 *  |  +-----+  |  |                                |  | (*) |  |
 *  +-----------+  |                                |  +-----+  |
 *                 |    +-----------------------+   +-----------+
 *                 |    |  +------+    +-----+  |
 *                 |    |  |  FM  |    |  S  |  |
 *                 +--->+  | (*)  +--->+ (*) |  |
 *                      |  +------+    +-----+  |
 *                      +-----------------------+
 */ 

// include
#include<random>
#include<iostream>
#include<windflow.hpp>
#include"split_common.hpp"

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
    size_t max = 9;
    std::uniform_int_distribution<std::mt19937::result_type> dist_p(min, max);
    std::uniform_int_distribution<std::mt19937::result_type> dist_b(0, 10);
    int map1_degree, map2_degree, filter_degree, flatmap_degree, sink1_degree, sink2_degree, sink3_degree;
    size_t source_degree = dist_p(rng);
    long last_result = 0;
    // executes the runs in DEFAULT mode
    for (size_t i=0; i<runs; i++) {
        map1_degree = dist_p(rng);
        map2_degree = dist_p(rng);
        filter_degree = dist_p(rng);
        flatmap_degree =  dist_p(rng);
        sink1_degree = dist_p(rng);
        sink2_degree = dist_p(rng);
        sink3_degree = dist_p(rng);
        cout << "Run " << i << endl;
        cout << "                                                +---------------------+" << endl;
        cout << "                                                |  +-----+   +-----+  |" << endl;
        cout << "                                                |  |  M  |   |  S  |  |" << endl;
        cout << "                    +---------------------+  +->+  | (" << map2_degree << ") +-->+ (" << sink1_degree << ") |  |" << endl;
        cout << "                    |  +-----+   +-----+  |  |  |  +-----+   +-----+  |" << endl;
        cout << "               +--->+  |  M  |   |  F  |  |  |  +---------------------+" << endl;
        cout << "+-----------+  |    |  | (" << map1_degree << ") +-->+ (" << filter_degree << ") |  +--+" << endl;
        cout << "|  +-----+  |  |    |  +-----+   +-----+  |  |  +-----------+" << endl;
        cout << "|  |  S  |  |  |    +---------------------+  |  |  +-----+  |" << endl;
        cout << "|  | (" << source_degree << ") |  +--+                             +->+  |  S  |  |" << endl;
        cout << "|  +-----+  |  |                                |  | (" << sink2_degree << ") |  |" << endl;
        cout << "+-----------+  |                                |  +-----+  |" << endl;
        cout << "               |    +-----------------------+   +-----------+" << endl;
        cout << "               |    |  +------+    +-----+  |" << endl;
        cout << "               |    |  |  FM  |    |  S  |  |" << endl;
        cout << "               +--->+  | (" << flatmap_degree << ")  +--->+ (" << sink3_degree << ") |  |" << endl;
        cout << "                    |  +------+    +-----+  |" << endl;
        cout << "                    +-----------------------+" << endl;
        // compute the total parallelism degree of the PipeGraph
        size_t check_degree = source_degree;
        check_degree += map1_degree;
        check_degree += filter_degree;
        check_degree += map2_degree;
        if (map2_degree != sink1_degree) {
            check_degree += sink1_degree;
        }
        check_degree += sink2_degree;
        check_degree += flatmap_degree;
        if (flatmap_degree != sink3_degree) {
            check_degree += sink3_degree;
        }
        // preapre the test
        PipeGraph graph("test_split_3 (DEFAULT)", Execution_Mode_t::DEFAULT, Time_Policy_t::EVENT_TIME);
        // prepare the first MultiPipe
        Source_Functor source_functor(stream_len, n_keys, true);
        Source source = Source_Builder(source_functor)
                                .withName("source")
                                .withParallelism(source_degree)
                                .withOutputBatchSize(dist_b(rng))
                                .build();
        MultiPipe &pipe1 = graph.add_source(source);
        // split
        pipe1.split([](const tuple_t &t) {
            if (t.value % 2 == 0) {
                return 0;
            }
            else {
                return 1;
            }
        }, 2);
        // prepare the second MultiPipe
        MultiPipe &pipe2 = pipe1.select(0);
        Map_Functor map_functor1;
        Map map1 = Map_Builder(map_functor1)
                            .withName("map1")
                            .withParallelism(map1_degree)
                            .withOutputBatchSize(dist_b(rng))
                            .build();
        pipe2.chain(map1);
        Filter_Functor_KB filter_functor;
        Filter filter = Filter_Builder(filter_functor)
                                .withName("filter")
                                .withParallelism(filter_degree)
                                .withKeyBy([](const tuple_t &t) -> size_t { return t.key; })
                                .withOutputBatchSize(dist_b(rng))
                                .build();
        pipe2.chain(filter);
        // split
        pipe2.split([](const tuple_t &t) {
            if (t.value % 4 == 0) {
                return 0;
            }
            else {
                return 1;
            }
        }, 2);     
        // prepare the third MultiPipe
        MultiPipe &pipe3 = pipe2.select(0);
        Map_Functor map_functor2;
        Map map2 = Map_Builder(map_functor2)
                            .withName("map2")
                            .withParallelism(map2_degree)
                            .withOutputBatchSize(dist_b(rng))
                            .build();
        pipe3.chain(map2);
        Sink_Functor sink_functor1;
        Sink sink1 = Sink_Builder(sink_functor1)
                            .withName("sink1")
                            .withParallelism(sink1_degree)
                            .build();
        pipe3.chain_sink(sink1);
        // prepare the fourth MultiPipe
        MultiPipe &pipe4 = pipe2.select(1);
        Sink_Functor sink_functor2;
        Sink sink2 = Sink_Builder(sink_functor2)
                            .withName("sink2")
                            .withParallelism(sink2_degree)
                            .build();
        pipe4.chain_sink(sink2);
        // prepare the fifth MultiPipe
        MultiPipe &pipe5 = pipe1.select(1);
        FlatMap_Functor flatmap_functor;
        FlatMap flatmap = FlatMap_Builder(flatmap_functor)
                                .withName("flatmap")
                                .withParallelism(flatmap_degree)
                                .withOutputBatchSize(dist_b(rng))
                                .build();
        pipe5.chain(flatmap);
        Sink_Functor sink_functor3;
        Sink sink3 = Sink_Builder(sink_functor3)
                            .withName("sink3")
                            .withParallelism(sink3_degree)
                            .build();
        pipe5.chain_sink(sink3);
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
        filter_degree = dist_p(rng);
        flatmap_degree =  dist_p(rng);
        sink1_degree = dist_p(rng);
        sink2_degree = dist_p(rng);
        sink3_degree = dist_p(rng);
        cout << "Run " << i << endl;
        cout << "                                                +---------------------+" << endl;
        cout << "                                                |  +-----+   +-----+  |" << endl;
        cout << "                                                |  |  M  |   |  S  |  |" << endl;
        cout << "                    +---------------------+  +->+  | (" << map2_degree << ") +-->+ (" << sink1_degree << ") |  |" << endl;
        cout << "                    |  +-----+   +-----+  |  |  |  +-----+   +-----+  |" << endl;
        cout << "               +--->+  |  M  |   |  F  |  |  |  +---------------------+" << endl;
        cout << "+-----------+  |    |  | (" << map1_degree << ") +-->+ (" << filter_degree << ") |  +--+" << endl;
        cout << "|  +-----+  |  |    |  +-----+   +-----+  |  |  +-----------+" << endl;
        cout << "|  |  S  |  |  |    +---------------------+  |  |  +-----+  |" << endl;
        cout << "|  | (" << source_degree << ") |  +--+                             +->+  |  S  |  |" << endl;
        cout << "|  +-----+  |  |                                |  | (" << sink2_degree << ") |  |" << endl;
        cout << "+-----------+  |                                |  +-----+  |" << endl;
        cout << "               |    +-----------------------+   +-----------+" << endl;
        cout << "               |    |  +------+    +-----+  |" << endl;
        cout << "               |    |  |  FM  |    |  S  |  |" << endl;
        cout << "               +--->+  | (" << flatmap_degree << ")  +--->+ (" << sink3_degree << ") |  |" << endl;
        cout << "                    |  +------+    +-----+  |" << endl;
        cout << "                    +-----------------------+" << endl;
        // compute the total parallelism degree of the PipeGraph
        size_t check_degree = source_degree;
        check_degree += map1_degree;
        check_degree += filter_degree;
        check_degree += map2_degree;
        if (map2_degree != sink1_degree) {
            check_degree += sink1_degree;
        }
        check_degree += sink2_degree;
        check_degree += flatmap_degree;
        if (flatmap_degree != sink3_degree) {
            check_degree += sink3_degree;
        }
        // preapre the test
        PipeGraph graph("test_split_3 (DETERMINISTIC)", Execution_Mode_t::DETERMINISTIC, Time_Policy_t::EVENT_TIME);
        // prepare the first MultiPipe
        Source_Functor source_functor(stream_len, n_keys, false);
        Source source = Source_Builder(source_functor)
                                .withName("source")
                                .withParallelism(source_degree)
                                .build();
        MultiPipe &pipe1 = graph.add_source(source);
        // split
        pipe1.split([](const tuple_t &t) {
            if (t.value % 2 == 0) {
                return 0;
            }
            else {
                return 1;
            }
        }, 2);
        // prepare the second MultiPipe
        MultiPipe &pipe2 = pipe1.select(0);
        Map_Functor map_functor1;
        Map map1 = Map_Builder(map_functor1)
                            .withName("map1")
                            .withParallelism(map1_degree)
                            .build();
        pipe2.chain(map1);
        Filter_Functor_KB filter_functor;
        Filter filter = Filter_Builder(filter_functor)
                                .withName("filter")
                                .withParallelism(filter_degree)
                                .withKeyBy([](const tuple_t &t) -> size_t { return t.key; })
                                .build();
        pipe2.chain(filter);
        // split
        pipe2.split([](const tuple_t &t) {
            if (t.value % 4 == 0) {
                return 0;
            }
            else {
                return 1;
            }
        }, 2);     
        // prepare the third MultiPipe
        MultiPipe &pipe3 = pipe2.select(0);
        Map_Functor map_functor2;
        Map map2 = Map_Builder(map_functor2)
                            .withName("map2")
                            .withParallelism(map2_degree)
                            .build();
        pipe3.chain(map2);
        Sink_Functor sink_functor1;
        Sink sink1 = Sink_Builder(sink_functor1)
                            .withName("sink1")
                            .withParallelism(sink1_degree)
                            .build();
        pipe3.chain_sink(sink1);
        // prepare the fourth MultiPipe
        MultiPipe &pipe4 = pipe2.select(1);
        Sink_Functor sink_functor2;
        Sink sink2 = Sink_Builder(sink_functor2)
                            .withName("sink2")
                            .withParallelism(sink2_degree)
                            .build();
        pipe4.chain_sink(sink2);
        // prepare the fifth MultiPipe
        MultiPipe &pipe5 = pipe1.select(1);
        FlatMap_Functor flatmap_functor;
        FlatMap flatmap = FlatMap_Builder(flatmap_functor)
                                .withName("flatmap")
                                .withParallelism(flatmap_degree)
                                .build();
        pipe5.chain(flatmap);
        Sink_Functor sink_functor3;
        Sink sink3 = Sink_Builder(sink_functor3)
                            .withName("sink3")
                            .withParallelism(sink3_degree)
                            .build();
        pipe5.chain_sink(sink3);
        assert(graph.getNumThreads() == check_degree);
        // run the application
        graph.run();
        if (last_result == global_sum) {
                cout << "Result is --> " << GREEN << "OK" << DEFAULT_COLOR << " value " << global_sum.load() << endl;
        }
        else {
            cout << "Result is --> " << RED << "FAILED" << DEFAULT_COLOR << " value " << global_sum.load() << endl;
            abort();
        }
        global_sum = 0;
    }
    return 0;
}
