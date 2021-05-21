/******************************************************************************
 *  This program is free software; you can redistribute it and/or modify it
 *  under the terms of the GNU Lesser General Public License version 3 as
 *  published by the Free Software Foundation.
 *  
 *  This program is distributed in the hope that it will be useful, but WITHOUT
 *  ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 *  FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public
 *  License for more details.
 *  
 *  You should have received a copy of the GNU Lesser General Public License
 *  along with this program; if not, write to the Free Software Foundation,
 *  Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 ******************************************************************************
 */

/*  
 *  Test 1 split of MultiPipes.
 *  
 *                                 +-------------------------------+
 *                                 |  +-----+   +-----+   +-----+  |
 *                            +--->+  |  F  |   | FM  |   |  S  |  |
 *                            |    |  | (*) +-->+ (*) +-->+ (*) |  |
 *                            |    |  +-----+   +-----+   +-----+  |
 *  +---------------------+   |    +-------------------------------+
 *  |  +-----+   +-----+  |   |
 *  |  |  S  |   |  M  |  |   |
 *  |  | (*) +-->+ (*) |  +---+
 *  |  +-----+   +-----+  |   |
 *  +---------------------+   |    +-----------+
 *                            |    |  +-----+  |
 *                            |    |  |  S  |  |
 *                            +--->+  | (*) |  |
 *                                 |  +-----+  |
 *                                 +-----------+
 */ 

// include
#include<random>
#include<iostream>
#include<ff/ff.hpp>
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
    int map_degree, flatmap_degree, filter_degree, sink1_degree, sink2_degree;
    size_t source_degree = dist_p(rng);
    long last_result = 0;
    // executes the runs in DEFAULT mode
    for (size_t i=0; i<runs; i++) {
        map_degree = dist_p(rng);
        flatmap_degree = dist_p(rng);
        filter_degree = dist_p(rng);
        sink1_degree = dist_p(rng);
        sink2_degree = dist_p(rng);
        cout << "Run " << i << endl;
        cout << "                               +-------------------------------+" << endl;
        cout << "                               |  +-----+   +-----+   +-----+  |" << endl;
        cout << "                          +--->+  |  F  |   | FM  |   |  S  |  |" << endl;
        cout << "                          |    |  | (" << filter_degree << ") +-->+ (" << flatmap_degree << ") +-->+ (" << sink1_degree << ") |  |" << endl;
        cout << "                          |    |  +-----+   +-----+   +-----+  |" << endl;
        cout << "+---------------------+   |    +-------------------------------+" << endl;
        cout << "|  +-----+   +-----+  |   |" << endl;
        cout << "|  |  S  |   |  M  |  |   |" << endl;
        cout << "|  | (" << source_degree << ") +-->+ (" << map_degree << ") |  +---+" << endl;
        cout << "|  +-----+   +-----+  |   |" << endl;
        cout << "+---------------------+   |    +-----------+" << endl;
        cout << "                          |    |  +-----+  |" << endl;
        cout << "                          |    |  |  S  |  |" << endl;
        cout << "                          +--->+  | (" << sink2_degree << ") |  |" << endl;
        cout << "                              |  +-----+  |" << endl;
        cout << "                              +-----------+" << endl;
        // compute the total parallelism degree of the PipeGraph
        size_t check_degree = source_degree;
        if (source_degree != map_degree) {
            check_degree += map_degree;
        }
        check_degree += filter_degree;
        if (filter_degree != flatmap_degree) {
            check_degree += flatmap_degree;
        }
        if (flatmap_degree != sink1_degree) {
            check_degree += sink1_degree;
        }
        check_degree += sink2_degree;
        // pepare the test
        PipeGraph graph("test_split_1 (DEFAULT)", Execution_Mode_t::DEFAULT, Time_Policy_t::EVENT_TIME);
        // prepare the first MultiPipe
        Source_Functor source_functor(stream_len, n_keys, true);
        Source source = Source_Builder(source_functor)
                                .withName("source")
                                .withParallelism(source_degree)
                                .withOutputBatchSize(dist_b(rng))
                                .build();
        MultiPipe &pipe1 = graph.add_source(source);
        Map_Functor map_functor1;
        Map map1 = Map_Builder(map_functor1)
                            .withName("map1")
                            .withParallelism(map_degree)
                            .withOutputBatchSize(dist_b(rng))
                            .build();
        pipe1.chain(map1);
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
        Filter_Functor_KB filter_functor;
        Filter filter = Filter_Builder(filter_functor)
                                .withName("filter")
                                .withParallelism(filter_degree)
                                .withKeyBy([](const tuple_t &t) -> size_t { return t.key; })
                                .withOutputBatchSize(dist_b(rng))
                                .build();
        pipe2.chain(filter);
        FlatMap_Functor flatmap_functor;
        FlatMap flatmap = FlatMap_Builder(flatmap_functor)
                                .withName("flatmap")
                                .withParallelism(flatmap_degree)
                                .withOutputBatchSize(dist_b(rng))
                                .build();
        pipe2.chain(flatmap);
        Sink_Functor sink_functor;
        Sink sink = Sink_Builder(sink_functor)
                            .withName("sink1")
                            .withParallelism(sink1_degree)
                            .build();
        pipe2.chain_sink(sink);
        // prepare the third MultiPipe
        MultiPipe &pipe3 = pipe1.select(1);
        Sink_Functor sink_functor2;
        Sink sink2 = Sink_Builder(sink_functor2)
                            .withName("sink2")
                            .withParallelism(sink2_degree)
                            .build();
        pipe3.chain_sink(sink2);
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
        map_degree = dist_p(rng);
        flatmap_degree = dist_p(rng);
        filter_degree = dist_p(rng);
        sink1_degree = dist_p(rng);
        sink2_degree = dist_p(rng);
        cout << "Run " << i << endl;
        cout << "                               +-------------------------------+" << endl;
        cout << "                               |  +-----+   +-----+   +-----+  |" << endl;
        cout << "                          +--->+  |  F  |   | FM  |   |  S  |  |" << endl;
        cout << "                          |    |  | (" << filter_degree << ") +-->+ (" << flatmap_degree << ") +-->+ (" << sink1_degree << ") |  |" << endl;
        cout << "                          |    |  +-----+   +-----+   +-----+  |" << endl;
        cout << "+---------------------+   |    +-------------------------------+" << endl;
        cout << "|  +-----+   +-----+  |   |" << endl;
        cout << "|  |  S  |   |  M  |  |   |" << endl;
        cout << "|  | (" << source_degree << ") +-->+ (" << map_degree << ") |  +---+" << endl;
        cout << "|  +-----+   +-----+  |   |" << endl;
        cout << "+---------------------+   |    +-----------+" << endl;
        cout << "                          |    |  +-----+  |" << endl;
        cout << "                          |    |  |  S  |  |" << endl;
        cout << "                          +--->+  | (" << sink2_degree << ") |  |" << endl;
        cout << "                              |  +-----+  |" << endl;
        cout << "                              +-----------+" << endl;
        // compute the total parallelism degree of the PipeGraph
        size_t check_degree = source_degree;
        if (source_degree != map_degree) {
            check_degree += map_degree;
        }
        check_degree += filter_degree;
        if (filter_degree != flatmap_degree) {
            check_degree += flatmap_degree;
        }
        if (flatmap_degree != sink1_degree) {
            check_degree += sink1_degree;
        }
        check_degree += sink2_degree;
        // pepare the test
        PipeGraph graph("test_split_1 (DETERMINISTIC)", Execution_Mode_t::DETERMINISTIC, Time_Policy_t::EVENT_TIME);
        // prepare the first MultiPipe
        Source_Functor source_functor(stream_len, n_keys, false);
        Source source = Source_Builder(source_functor)
                                .withName("source")
                                .withParallelism(source_degree)
                                .build();
        MultiPipe &pipe1 = graph.add_source(source);
        Map_Functor map_functor1;
        Map map1 = Map_Builder(map_functor1)
                            .withName("map1")
                            .withParallelism(map_degree)
                            .build();
        pipe1.chain(map1);
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
        Filter_Functor_KB filter_functor;
        Filter filter = Filter_Builder(filter_functor)
                                .withName("filter")
                                .withParallelism(filter_degree)
                                .withKeyBy([](const tuple_t &t) -> size_t { return t.key; })
                                .build();
        pipe2.chain(filter);
        FlatMap_Functor flatmap_functor;
        FlatMap flatmap = FlatMap_Builder(flatmap_functor)
                                .withName("flatmap")
                                .withParallelism(flatmap_degree)
                                .build();
        pipe2.chain(flatmap);
        Sink_Functor sink_functor;
        Sink sink = Sink_Builder(sink_functor)
                            .withName("sink1")
                            .withParallelism(sink1_degree)
                            .build();
        pipe2.chain_sink(sink);
        // prepare the third MultiPipe
        MultiPipe &pipe3 = pipe1.select(1);
        Sink_Functor sink_functor2;
        Sink sink2 = Sink_Builder(sink_functor2)
                            .withName("sink2")
                            .withParallelism(sink2_degree)
                            .build();
        pipe3.chain_sink(sink2);
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
