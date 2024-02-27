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
 *  Test 8 of the P_Keyed_Windows operator with time-based windows and incremental
 *  window function.
 *  
 *  +-----------------------------------------------------------------+
 *  |  +-----+   +-----+   +------+   +-----+   +--------+   +-----+  |
 *  |  |  S  |   |  F  |   |  FM  |   |  M  |   | PKW_TB |   |  S  |  |
 *  |  | (*) +-->+ (*) +-->+  (*) +-->+ (*) +-->+   (*)  +-->+ (*) |  |
 *  |  +-----+   +-----+   +------+   +-----+   +--------+   +-----+  |
 *  +-----------------------------------------------------------------+
 */ 

// includes
#include<string>
#include<random>
#include<iostream>
#include<math.h>
#include<windflow.hpp>
#include<persistent/windflow_rocksdb.hpp>
#include"rocksdb_common.hpp"

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
    size_t win_len = 0;
    size_t win_slide = 0;
    size_t n_keys = 1;
    // initalize global variable
    global_sum = 0;
    // arguments from command line
    if (argc != 11) {
        cout << argv[0] << " -r [runs] -l [stream_length] -k [n_keys] -w [win length usec] -s [win slide usec]" << endl;
        exit(EXIT_SUCCESS);
    }
    while ((option = getopt(argc, argv, "r:l:k:w:s:")) != -1) {
        switch (option) {
            case 'r': runs = atoi(optarg);
                     break;
            case 'l': stream_len = atoi(optarg);
                     break;
            case 'k': n_keys = atoi(optarg);
                     break;
            case 'w': win_len = atoi(optarg);
                     break;
            case 's': win_slide = atoi(optarg);
                     break;
            default: {
                cout << argv[0] << " -r [runs] -l [stream_length] -k [n_keys] -w [win length usec] -s [win slide usec]" << endl;
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
    int filter_degree, flatmap_degree, map_degree, kw_degree;
    size_t source_degree, sink_degree;
    long last_result = 0;
    source_degree = 1; // dist_p(rng);
    // executes the runs in DEFAULT mode
    for (size_t i=0; i<runs; i++) {
        filter_degree = dist_p(rng);
        flatmap_degree = dist_p(rng);
        map_degree = dist_p(rng);
        kw_degree = dist_p(rng);
        sink_degree = 1; // dist_p(rng);
        cout << "Run " << i << endl;
        cout << "+-----------------------------------------------------------------+" << endl;
        cout << "|  +-----+   +-----+   +------+   +-----+   +--------+   +-----+  |" << endl;
        cout << "|  |  S  |   |  F  |   |  FM  |   |  M  |   | PKW_TB |   |  S  |  |" << endl;
        cout << "|  | (" << source_degree << ") +-->+ (" << filter_degree << ") +-->+  (" << flatmap_degree << ") +-->+ (" << map_degree << ") +-->+  (" << kw_degree << ")   +-->+ (" << sink_degree << ") |  |" << endl;
        cout << "|  +-----+   +-----+   +------+   +-----+   +--------+   +-----+  |" << endl;
        cout << "+-----------------------------------------------------------------+" << endl;
        auto tuple_serializer = [](tuple_t &t) -> std::string {
            return std::to_string(t.key) + "," + std::to_string(t.value);
        };
        auto tuple_deserializer = [](std::string &s) -> tuple_t {
            tuple_t t;
            t.key = atoi(s.substr(0, s.find(",")).c_str());
            t.value = atoi(s.substr(s.find(",")+1, s.length()-1).c_str());
            return t;
        };
        auto result_serializer = [](result_t &r) -> std::string {
            return std::to_string(r.key) + "," + std::to_string(r.value) + ";" + std::to_string(r.wid);
        };
        auto result_deserializer = [](std::string &s) -> result_t {
            result_t r;
            r.key = atoi(s.substr(0, s.find(",")).c_str());
            r.value = atoi(s.substr(s.find(",")+1, s.find(";")).c_str());
            r.wid = atoi(s.substr(s.find(";")+1, s.length()-1).c_str());
            return r;
        };
        // prepare the test
        PipeGraph graph("test_rocksdb_8", Execution_Mode_t::DEFAULT, Time_Policy_t::EVENT_TIME);
        Source_Functor source_functor(stream_len, n_keys, true);
        Source source = Source_Builder(source_functor)
                            .withName("source")
                            .withParallelism(source_degree)
                            .withOutputBatchSize(dist_b(rng))
                            .build();
        MultiPipe &mp = graph.add_source(source);
        Filter_Functor_KB filter_functor(2);
        Filter filter = Filter_Builder(filter_functor)
                            .withName("filter")
                            .withParallelism(filter_degree)
                            .withKeyBy([](const tuple_t &t) -> size_t { return t.key; })
                            .withOutputBatchSize(dist_b(rng))
                            .build();
        mp.chain(filter);
        FlatMap_Functor flatmap_functor;
        FlatMap flatmap = FlatMap_Builder(flatmap_functor)
                                .withName("flatmap")
                                .withParallelism(flatmap_degree)
                                .withOutputBatchSize(dist_b(rng))
                                .build();
        mp.chain(flatmap);
        Map_Functor map_functor;
        Map map = Map_Builder(map_functor)
                        .withName("map")
                        .withParallelism(map_degree)
                        .withOutputBatchSize(dist_b(rng))
                        .build();
        mp.chain(map);
        Win_Functor_INC win_functor;
#if 1
        P_Keyed_Windows kwins = P_Keyed_Windows_Builder(win_functor)
                                    .withName("p_keyed_wins")
                                    .withParallelism(kw_degree)
                                    .withKeyBy([](const tuple_t &t) -> size_t { return t.key; })
                                    .withTBWindows(microseconds(win_len), microseconds(win_slide))
                                    .withTupleSerializerAndDeserializer(tuple_serializer, tuple_deserializer)
                                    .withResultSerializerAndDeserializer(result_serializer, result_deserializer)
                                    .build();
#else
        Keyed_Windows kwins = Keyed_Windows_Builder(win_functor)
                                    .withName("keyed_wins")
                                    .withParallelism(kw_degree)
                                    .withKeyBy([](const tuple_t &t) -> size_t { return t.key; })
                                    .withTBWindows(microseconds(win_len), microseconds(win_slide))
                                    .build();
#endif
        mp.add(kwins);
        WinSink_Functor sink_functor;
        Sink sink = Sink_Builder(sink_functor)
                        .withName("sink")
                        .withParallelism(sink_degree)
                        .build();
        mp.chain_sink(sink);
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
