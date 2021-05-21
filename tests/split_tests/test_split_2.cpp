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
 *  Test 2 of the split of MultiPipe instances.
 *  
 *                                 +-------------------------------+
 *                                 |  +-----+   +-----+   +-----+  |
 *                            +--->+  |  F  |   |  M  |   |  S  |  |
 *                            |    |  | (*) +-->+ (*) +-->+ (1) |  |
 *                            |    |  +-----+   +-----+   +-----+  |
 *  +---------------------+   |    +-------------------------------+
 *  |  +-----+   +-----+  |   |    +-----------+
 *  |  |  S  |   |  M  |  |   |    |  +-----+  |
 *  |  | (*) +-->+ (*) |  +---+--->+  |  S  |  |
 *  |  +-----+   +-----+  |   |    |  | (1) |  |
 *  +---------------------+   |    |  +-----+  |
 *                            |    +-----------+
 *                            |    +-----------------------+
 *                            |    |  +------+    +-----+  |
 *                            |    |  |  FM  |    |  S  |  |
 *                            +--->+  | (*)  +--->+ (1) |  |
 *                                 |  +------+    +-----+  |
 *                                 +-----------------------+
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
    std::uniform_int_distribution<std::mt19937::result_type> dist6(min, max);
    int map1_degree, map2_degree, filter_degree, flatmap_degree;
    size_t source_degree = dist6(rng);
    long last_result = 0;
    // executes the runs
    for (size_t i=0; i<runs; i++) {
        map1_degree = dist6(rng);
        map2_degree = dist6(rng);
        filter_degree = dist6(rng);
        flatmap_degree = dist6(rng);
        cout << "Run " << i << endl;
        cout << "                               +-------------------------------+" << endl;
        cout << "                               |  +-----+   +-----+   +-----+  |" << endl;
        cout << "                          +--->+  |  F  |   |  M  |   |  S  |  |" << endl;
        cout << "                          |    |  | (" << filter_degree << ") +-->+ (" << map2_degree << ") +-->+ (1) |  |" << endl;
        cout << "                          |    |  +-----+   +-----+   +-----+  |" << endl;
        cout << "+---------------------+   |    +-------------------------------+" << endl;
        cout << "|  +-----+   +-----+  |   |    +-----------+" << endl;
        cout << "|  |  S  |   |  M  |  |   |    |  +-----+  |" << endl;
        cout << "|  | (" << source_degree << ") +-->+ (" << map1_degree << ") |  +---+--->+  |  S  |  |" << endl;
        cout << "|  +-----+   +-----+  |   |    |  | (1) |  |" << endl;
        cout << "+---------------------+   |    |  +-----+  |" << endl;
        cout << "                          |    +-----------+" << endl;
        cout << "                          |    +-----------------------+" << endl;
        cout << "                          |    |  +------+    +-----+  |" << endl;
        cout << "                          |    |  |  FM  |    |  S  |  |" << endl;
        cout << "                          +--->+  | (" << flatmap_degree << ")  +--->+ (1) |  |" << endl;
        cout << "                               |  +------+    +-----+  |" << endl;
        cout << "                               +-----------------------+" << endl;
        // compute the total parallelism degree of the PipeGraph
        size_t check_degree = source_degree;
        if (source_degree != map1_degree)
            check_degree += map1_degree;
        check_degree += filter_degree;
        if (filter_degree != map2_degree)
            check_degree += map2_degree;
        if (map2_degree != 1)
            check_degree++;
        check_degree++;
        check_degree += flatmap_degree;
        if (flatmap_degree != 1)
            check_degree++;
        // prepare the test
        PipeGraph graph("test_split_2");
        // prepare the first MultiPipe
        // source
        Source_Functor source_functor(stream_len, n_keys);
        Source source = Source_Builder(source_functor)
                                .withName("source")
                                .withParallelism(source_degree)
                                .build();
        MultiPipe &pipe1 = graph.add_source(source);
        // map 1
        Map_Functor1 map_functor1;
        Map map1 = Map_Builder(map_functor1)
                            .withName("map1")
                            .withParallelism(map1_degree)
                            .build();
        pipe1.chain(map1);
        // split
        pipe1.split([](const tuple_t &t) {
            if (t.value % 2 == 0) {
                return 0;
            }
            else {
                if (t.value % 5 == 0) {
                    return 1;
                }
                else {
                    return 2;
                }
            }
        }, 3);
        // prepare the second MultiPipe
        MultiPipe &pipe2 = pipe1.select(0);
        // filter
        Filter_Functor filter_functor;
        Filter filter = Filter_Builder(filter_functor)
                                .withName("filter")
                                .withParallelism(filter_degree)
                                .build();
        pipe2.chain(filter);
        // map 2
        Map_Functor2 map_functor2;
        Map map2 = Map_Builder(map_functor2)
                            .withName("map2")
                            .withParallelism(map2_degree)
                            .build();
        pipe2.chain(map2);
        // sink
        Sink_Functor sink_functor1(n_keys);
        Sink sink1 = Sink_Builder(sink_functor1)
                            .withName("sink1")
                            .withParallelism(1)
                            .build();
        pipe2.chain_sink(sink1);
        // prepare the third MultiPipe
        MultiPipe &pipe3 = pipe1.select(1);
        // sink
        Sink_Functor sink_functor2(n_keys);
        Sink sink2 = Sink_Builder(sink_functor2)
                            .withName("sink2")
                            .withParallelism(1)
                            .build();
        pipe3.chain_sink(sink2);
        // prepare the fourth MultiPipe
        MultiPipe &pipe4 = pipe1.select(2);
        // flatmap
        FlatMap_Functor flatmap_functor;
        FlatMap flatmap = FlatMap_Builder(flatmap_functor)
                                    .withName("flatmap")
                                    .withParallelism(flatmap_degree)
                                    .build();
        pipe4.chain(flatmap);
        // sink
        Sink_Functor sink_functor3(n_keys);
        Sink sink3 = Sink_Builder(sink_functor3)
                            .withName("sink3")
                            .withParallelism(1)
                            .build();
        pipe4.chain_sink(sink3);
        assert(graph.getNumThreads() == check_degree);
        // run the application
        graph.run();
        if (i == 0) {
            last_result = global_sum;
            cout << "Result is --> " << GREEN << "OK" << "!!!" << DEFAULT_COLOR << endl;
        }
        else {
            if (last_result == global_sum) {
                cout << "Result is --> " << GREEN << "OK" << "!!!" << DEFAULT_COLOR << endl;
            }
            else {
                cout << "Result is --> " << RED << "FAILED" << "!!!" << DEFAULT_COLOR << endl;
            }
        }
        global_sum = 0;
    }
    return 0;
}
