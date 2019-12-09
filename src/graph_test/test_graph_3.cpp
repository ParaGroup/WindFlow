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
 *  Test of general graphs of MultiPipe instances:
 *                                                          +---------------------+
 *                                                          |  +-----+   +-----+  |
 *                                                          |  |  S  |   |  F  |  |
 *                                                          |  | (*) +-->+ (*) |  +-+
 *                                                          |  +-----+   +-----+  | |
 *                                                          +---------------------+ |
 *                                                                                  |
 *                              +---------------------+                             |   +----------+
 *                              |  +-----+   +-----+  |                             |   | +-----+  |
 *                           +->+  |  F  |   |  M  |  +-+                           |   | |  S  |  |
 *                           |  |  | (*) +-->+ (*) |  | |                           +-->+ | (1) |  |
 *                           |  |  +-----+   +-----+  | |                           |   | +-----+  |
 *  +---------------------+  |  +---------------------+ |   +---------------------+ |   +----------+
 *  |  +-----+   +-----+  |  |                          |   |  +-----+   +-----+  | |
 *  |  |  S  |   |  M  |  |  |                          |   |  |  M  |   |  F  |  | |
 *  |  | (*) +-->+ (*) |  +--+                          +-->+  | (*) +-->+ (*) |  +-+
 *  |  +-----+   +-----+  |  |                          |   |  +-----+   +-----+  |
 *  +---------------------+  |       +-----------+      |   +---------------------+
 *                           |       |  +-----+  |      |
 *                           |       |  |  F  |  |      |
 *                           +------>+  | (*) |  +------+
 *                                   |  +-----+  |
 *                                   +-----------+
 */

// include
#include <random>
#include <iostream>
#include <ff/ff.hpp>
#include <windflow.hpp>
#include "graph_common.hpp"

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
    int map1_degree, map2_degree, map3_degree, filter1_degree, filter2_degree, filter3_degree, filter4_degree;
    size_t source1_degree = dist6(rng);
    size_t source2_degree = dist6(rng);
    long last_result = 0;
    // executes the runs
    for (size_t i=0; i<runs; i++) {
        map1_degree = dist6(rng);
        map2_degree = dist6(rng);
        map3_degree = dist6(rng);
        filter1_degree = dist6(rng);
        filter2_degree = dist6(rng);
        filter3_degree = dist6(rng);
        filter4_degree = dist6(rng);
        cout << "Run " << i << endl;
        cout << "                                                        +---------------------+" << endl;
        cout << "                                                        |  +-----+   +-----+  |" << endl;
        cout << "                                                        |  |  S  |   |  F  |  |" << endl;
        cout << "                                                        |  | (" << source2_degree << ") +-->+ (" << filter4_degree << ") |  +-+" << endl;
        cout << "                                                        |  +-----+   +-----+  | |" << endl;
        cout << "                                                        +---------------------+ |" << endl;
        cout << "                                                                                |" << endl;
        cout << "                            +---------------------+                             |   +----------+" << endl;
        cout << "                            |  +-----+   +-----+  |                             |   | +-----+  |" << endl;
        cout << "                         +->+  |  F  |   |  M  |  +-+                           |   | |  S  |  |" << endl;
        cout << "                         |  |  | (" << filter1_degree << ") +-->+ (" << map2_degree << ") |  | |                           +-->+ | (1) |  |" << endl;
        cout << "                         |  |  +-----+   +-----+  | |                           |   | +-----+  |" << endl;
        cout << "+---------------------+  |  +---------------------+ |   +---------------------+ |   +----------+" << endl;
        cout << "|  +-----+   +-----+  |  |                          |   |  +-----+   +-----+  | |" << endl;
        cout << "|  |  S  |   |  M  |  |  |                          |   |  |  M  |   |  F  |  | |" << endl;
        cout << "|  | (" << source1_degree << ") +-->+ (" << map1_degree << ") |  +--+                          +-->+  | (" << map3_degree << ") +-->+ (" << filter3_degree << ") |  +-+" << endl;
        cout << "|  +-----+   +-----+  |  |                          |   |  +-----+   +-----+  |" << endl;
        cout << "+---------------------+  |       +-----------+      |   +---------------------+" << endl;
        cout << "                         |       |  +-----+  |      |" << endl;
        cout << "                         |       |  |  F  |  |      |" << endl;
        cout << "                         +------>+  | (" << filter2_degree << ") |  +------+" << endl;
        cout << "                                 |  +-----+  |" << endl;
        cout << "                                 +-----------+" << endl;
        // compute the total parallelism degree of the PipeGraph
        size_t check_degree = source1_degree;
        if (source1_degree != map1_degree)
            check_degree += map1_degree;
        check_degree += filter1_degree;
        if (filter1_degree != map2_degree)
            check_degree += map2_degree;
        check_degree += filter2_degree;
        check_degree += map3_degree;
        if (map3_degree != filter3_degree)
            check_degree += filter3_degree;
        check_degree += source2_degree;
        if (source2_degree != filter4_degree)
            check_degree += filter4_degree;
        check_degree++;
        // prepare the test
        PipeGraph graph("test_graph_3");
        // prepare the first MultiPipe
        // source
        Source_Positive_Functor source_functor_positive(stream_len, n_keys);
        Source source1 = Source_Builder(source_functor_positive)
                            .withName("pipe1_source")
                            .withParallelism(source1_degree)
                            .build();
        MultiPipe &pipe1 = graph.add_source(source1);
        // map 1
        Map_Functor map_functor1;
        Map map1 = Map_Builder(map_functor1)
                        .withName("pipe1_map")
                        .withParallelism(map1_degree)
                        .build();
        pipe1.chain(map1);
        // split
        pipe1.split([](const tuple_t &t) {
            if (t.value % 2 == 0)
                return 0;
            else
                return 1;
        }, 2);
        // prepare the second MultiPipe
        MultiPipe &pipe2 = pipe1.select(0);
        // filter
        Filter_Functor filter_functor1;
        Filter filter1 = Filter_Builder(filter_functor1)
                                .withName("pipe2_filter")
                                .withParallelism(filter1_degree)
                                .build();
        pipe2.chain(filter1);
        // map 2
        Map_Functor map_functor2;
        Map map2 = Map_Builder(map_functor2)
                        .withName("pipe2_map")
                        .withParallelism(map2_degree)
                        .build();
        pipe2.chain(map2);
        // prepare the third MultiPipe
        MultiPipe &pipe3 = pipe1.select(1);
        // filter
        Filter_Functor filter_functor2;
        Filter filter2 = Filter_Builder(filter_functor2)
                                .withName("pipe3_filter")
                                .withParallelism(filter2_degree)
                                .build();
        pipe3.chain(filter2);
        // merge of pipe2 and pipe3
        MultiPipe &pipe4 = pipe2.merge(pipe3);
        // prepare the fourth MultiPipe
        // map 3
        Map_Functor map_functor3;
        Map map3 = Map_Builder(map_functor3)
                        .withName("pipe4_map")
                        .withParallelism(map3_degree)
                        .build();
        pipe4.chain(map3);
        // filter
        Filter_Functor filter_functor3;
        Filter filter3 = Filter_Builder(filter_functor3)
                                .withName("pipe4_filter")
                                .withParallelism(filter3_degree)
                                .build();
        pipe4.chain(filter3);
        // prepare the fifth MultiPipe
        // source
        Source_Negative_Functor source_functor_negative(stream_len, n_keys);
        Source source2 = Source_Builder(source_functor_negative)
                            .withName("pipe5_source")
                            .withParallelism(source2_degree)
                            .build();
        MultiPipe &pipe5 = graph.add_source(source2);
        // filter
        Filter_Functor filter_functor4;
        Filter filter4 = Filter_Builder(filter_functor4)
                                .withName("pipe5_filter")
                                .withParallelism(filter4_degree)
                                .build();
        pipe5.chain(filter4);
        // merge of the fourth and fifth MultiPipe
        MultiPipe &pipe6 = pipe4.merge(pipe5);
        // prepare the sixth MultiPipe
        // sink
        Sink_Functor sink_functor(n_keys);
        Sink sink = Sink_Builder(sink_functor)
                        .withName("pipe6_sink")
                        .withParallelism(1)
                        .build();
        pipe6.chain_sink(sink);
        assert(graph.getNumThreads() == check_degree);
        // run the application
        graph.run();
        if (i == 0) {
            last_result = global_sum;
            cout << "Result is --> " << GREEN << "OK" << "!!!" << DEFAULT << endl;
        }
        else {
            if (last_result == global_sum) {
                cout << "Result is --> " << GREEN << "OK" << "!!!" << DEFAULT << endl;
            }
            else {
                cout << "Result is --> " << RED << "FAILED" << "!!!" << DEFAULT << endl;
            }
        }
        global_sum = 0;
    }
	return 0;
}
