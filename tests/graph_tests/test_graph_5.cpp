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
 *  Test 5 of general graphs of MultiPipe instances.
 *  
 *  +---------------------+                         +-----------+
 *  |  +-----+   +-----+  |                         |  +-----+  |
 *  |  |  S  |   |  M  |  |                         |  |  S  |  |
 *  |  | (*) +-->+ (*) |  +--+   +-----------+  +-->+  | (1) |  |
 *  |  +-----+   +-----+  |  |   |  +-----+  |  |   |  +-----+  |
 *  +---------------------+  |   |  |  F  |  |  |   +-----------+
 *                           +-->+  | (*) |  +--+
 *  +---------------------+  |   |  +-----+  |  |   +-----------+
 *  |  +-----+   +-----+  |  |   +-----------+  |   |  +-----+  |
 *  |  |  S  |   |  M  |  |  |                  |   |  |  S  |  |
 *  |  | (*) +-->+ (*) |  +--+                  +-->+  | (1) |  |
 *  |  +-----+   +-----+  |                         |  +-----+  |
 *  +---------------------+                         +-----------+
 *
 *  +-------------------------------+
 *  |  +-----+   +-----+   +-----+  |
 *  |  |  S  |   |  M  |   |  S  |  |
 *  |  | (*) +-->+ (*) +-->+ (1) |  |
 *  |  +-----+   +-----+   +-----+  |
 *  +-------------------------------+
 */ 

// include
#include<random>
#include<iostream>
#include<ff/ff.hpp>
#include<windflow.hpp>
#include"graph_common.hpp"

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
    int map1_degree, map2_degree, map3_degree, filter_degree;
    size_t source1_degree = dist6(rng);
    size_t source2_degree = dist6(rng);
    size_t source3_degree = dist6(rng);
    long last_result = 0;
    // executes the runs
    for (size_t i=0; i<runs; i++) {
        map1_degree = dist6(rng);
        map2_degree = dist6(rng);
        map3_degree = dist6(rng);
        filter_degree = dist6(rng);
        cout << "Run " << i << endl;
        cout << "+---------------------+                         +-----------+" << endl;
        cout << "|  +-----+   +-----+  |                         |  +-----+  |" << endl;
        cout << "|  |  S  |   |  M  |  |                         |  |  S  |  |" << endl;
        cout << "|  | (" << source1_degree << ") +-->+ (" << map1_degree << ") |  +--+   +-----------+  +-->+  | (1) |  |" << endl;
        cout << "|  +-----+   +-----+  |  |   |  +-----+  |  |   |  +-----+  |" << endl;
        cout << "+---------------------+  |   |  |  F  |  |  |   +-----------+" << endl;
        cout << "                         +-->+  | (" << filter_degree << ") |  +--+" << endl;
        cout << "+---------------------+  |   |  +-----+  |  |   +-----------+" << endl;
        cout << "|  +-----+   +-----+  |  |   +-----------+  |   |  +-----+  |" << endl;
        cout << "|  |  S  |   |  M  |  |  |                  |   |  |  S  |  |" << endl;
        cout << "|  | (" << source2_degree << ") +-->+ (" << map2_degree << ") |  +--+                  +-->+  | (1) |  |" << endl;
        cout << "|  +-----+   +-----+  |                         |  +-----+  |" << endl;
        cout << "+---------------------+                         +-----------+" << endl;
        cout << "" << endl;
        cout << "+-------------------------------+" << endl;
        cout << "|  +-----+   +-----+   +-----+  |" << endl;
        cout << "|  |  S  |   |  M  |   |  S  |  |" << endl;
        cout << "|  | (" << source3_degree << ") +-->+ (" << map3_degree << ") +-->+ (1) |  |" << endl;
        cout << "|  +-----+   +-----+   +-----+  |" << endl;
        cout << "+-------------------------------+" << endl;
        // compute the total parallelism degree of the PipeGraph
        size_t check_degree = source1_degree;
        if (source1_degree != map1_degree)
            check_degree += map1_degree;
        check_degree += source2_degree;
        if (source2_degree != map2_degree)
            check_degree += map2_degree;
        check_degree += filter_degree;
        check_degree += 2;
        check_degree += source3_degree;
        if (source3_degree != map3_degree)
            check_degree += map3_degree;
        if (map3_degree != 1)
            check_degree++;
        // prepare the test
        PipeGraph graph("test_graph_5");
        // prepare the first MultiPipe
        // source 1
        Source_Positive_Functor source_functor_positive(stream_len, n_keys);
        Source source1 = Source_Builder(source_functor_positive)
                            .withName("source1")
                            .withParallelism(source1_degree)
                            .build();
        MultiPipe &pipe1 = graph.add_source(source1);
        // map 1
        Map_Functor map_functor1;
        Map map1 = Map_Builder(map_functor1)
                        .withName("map1")
                        .withParallelism(map1_degree)
                        .build();
        pipe1.chain(map1);
        // prepare the second MultiPipe
        // source 2
        Source_Negative_Functor source_functor_negative(stream_len, n_keys);
        Source source2 = Source_Builder(source_functor_negative)
                            .withName("source2")
                            .withParallelism(source2_degree)
                            .build();
        MultiPipe &pipe2 = graph.add_source(source2);
        // map 2
        Map_Functor map_functor2;
        Map map2 = Map_Builder(map_functor2)
                        .withName("map2")
                        .withParallelism(map2_degree)
                        .build();
        pipe2.chain(map2);
        // prepare the third MultiPipe
        MultiPipe &pipe3 = pipe1.merge(pipe2);
        // filter
        Filter_Functor filter_functor;
        Filter filter = Filter_Builder(filter_functor)
                        .withName("filter")
                        .withParallelism(filter_degree)
                        .build();
        pipe3.chain(filter);
        // split
        pipe3.split([](const tuple_t &t) {
            if (t.value >= 0) {
                return 0;
            }
            else {
                return 1;
            }
        }, 2);
        // prepare the fourth MultiPipe
        MultiPipe &pipe4 = pipe3.select(0);
        // sink
        Sink_Functor sink_functor1(n_keys);
        Sink sink1 = Sink_Builder(sink_functor1)
                        .withName("sink1")
                        .withParallelism(1)
                        .build();
        pipe4.chain_sink(sink1);
        // prepare the fifth MultiPipe
        MultiPipe &pipe5 = pipe3.select(1);
        // sink
        Sink_Functor sink_functor2(n_keys);
        Sink sink2 = Sink_Builder(sink_functor2)
                        .withName("sink2")
                        .withParallelism(1)
                        .build();
        pipe5.chain_sink(sink2);
        // prepare the sixth MultiPipe
        // source 3
        Source_Positive_Functor source_functor_positive2(stream_len, n_keys);
        Source source3 = Source_Builder(source_functor_positive2)
                            .withName("source3")
                            .withParallelism(source3_degree)
                            .build();
        MultiPipe &pipe6 = graph.add_source(source3);  
        // map 3
        Map_Functor map_functor3;
        Map map3 = Map_Builder(map_functor3)
                        .withName("map3")
                        .withParallelism(map3_degree)
                        .build();
        pipe6.chain(map3);
        // sink
        Sink_Functor sink_functor3(n_keys);
        Sink sink3 = Sink_Builder(sink_functor3)
                        .withName("sink3")
                        .withParallelism(1)
                        .build();
        pipe6.chain_sink(sink3);
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
