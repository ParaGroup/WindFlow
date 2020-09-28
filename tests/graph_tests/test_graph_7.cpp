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
 *  Test 7 of general graphs of MultiPipe instances.
 *  
 *                               +---------------------+
 *                               |  +-----+   +-----+  |
 *                               |  |  F  |   |  M  |  +---+
 *                          +--->+  | (*) +-->+ (*) |  |   |   +---------------------+
 *                          |    |  +-----+   +-----+  |   |   |  +-----+   +-----+  |
 *                          |    +---------------------+   +-->+  |  M  |   |  M  |  |
 *                          |                              |   |  | (*) +-->+ (*) |  +---+
 *                          |    +---------------------+   |   |  +-----+   +-----+  |   |
 *                          |    |  +-----+   +-----+  |   |   +---------------------+   |
 *                          |    |  |  F  |   |  M  |  |   |                             |
 *                          +--->+  | (*) +-->+ (*) |  +---+                             |
 *                          |    |  +-----+   +-----+  |                                 |
 *                          |    +---------------------+                                 |
 *                          |                                                            |
 * +---------------------+  |    +---------------------+                                 |   +-----------+
 * |  +-----+   +-----+  |  |    |  +-----+   +-----+  |                                 |   |  +-----+  |
 * |  |  S  |   |  M  |  |  |    |  |  F  |   |  M  |  +--+                              |   |  |  S  |  |
 * |  | (*) +-->+ (*) |  +--+--->+  | (*) +-->+ (*) |  |  |                              +-->+  | (1) |  |
 * |  +-----+   +-----+  |  |    |  +-----+   +-----+  |  |                              |   |  +-----+  |
 * +---------------------+  |    +---------------------+  |                              |   +-----------+
 *                          |                             |                              |
 *                          |    +---------------------+  |    +---------------------+   |
 *                          |    |  +-----+   +-----+  |  |    |  +-----+   +-----+  |   |
 *                          |    |  |  F  |   |  M  |  |  |    |  |  M  |   |  M  |  |   |
 *                          +--->+  | (*) +-->+ (*) |  +--+--->+  | (*) +-->+ (*) |  +---+
 *                          |    |  +-----+   +-----+  |  |    |  +-----+   +-----+  |
 *                          |    +---------------------+  |    +---------------------+
 *                          |                             |
 *                          |    +---------------------+  |
 *                          |    |  +-----+   +-----+  |  |
 *                          |    |  |  F  |   |  M  |  |  |
 *                          +--->+  | (*) +-->+ (*) |  +--+
 *                               |  +-----+   +-----+  |
 *                               +---------------------+
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
    int map1_degree, map2_degree, map3_degree, map4_degree, map5_degree, map6_degree, map7_degree, map8_degree, map9_degree, map10_degree;
    int filter1_degree, filter2_degree, filter3_degree, filter4_degree, filter5_degree;
    size_t source_degree = dist6(rng);
    long last_result = 0;
    // executes the runs
    for (size_t i=0; i<runs; i++) {
        map1_degree = dist6(rng);
        map2_degree = dist6(rng);
        map3_degree = dist6(rng);
        map4_degree = dist6(rng);
        map5_degree = dist6(rng);
        map6_degree = dist6(rng);
        map7_degree = dist6(rng);
        map8_degree = dist6(rng);
        map9_degree = dist6(rng);
        map10_degree = dist6(rng);
        filter1_degree = dist6(rng);
        filter2_degree = dist6(rng);
        filter3_degree = dist6(rng);
        filter4_degree = dist6(rng);
        filter5_degree = dist6(rng);
        cout << "Run " << i << endl;
        cout << "                              +---------------------+" << endl;
        cout << "                              |  +-----+   +-----+  |" << endl;
        cout << "                              |  |  F  |   |  M  |  +---+" << endl;
        cout << "                         +--->+  | (" << filter1_degree << ") +-->+ (" << map2_degree << ") |  |   |   +---------------------+" << endl;
        cout << "                         |    |  +-----+   +-----+  |   |   |  +-----+   +-----+  |" << endl;
        cout << "                         |    +---------------------+   +-->+  |  M  |   |  M  |  |" << endl;
        cout << "                         |                              |   |  | (" << map7_degree << ") +-->+ (" << map8_degree << ") |  +---+" << endl;
        cout << "                         |    +---------------------+   |   |  +-----+   +-----+  |   |" << endl;
        cout << "                         |    |  +-----+   +-----+  |   |   +---------------------+   |" << endl;
        cout << "                         |    |  |  F  |   |  M  |  |   |                             |" << endl;
        cout << "                         +--->+  | (" << filter2_degree << ") +-->+ (" << map3_degree << ") |  +---+                             |" << endl;
        cout << "                         |    |  +-----+   +-----+  |                                 |" << endl;
        cout << "                         |    +---------------------+                                 |" << endl;
        cout << "                         |                                                            |" << endl;
        cout << "+---------------------+  |    +---------------------+                                 |   +-----------+" << endl;
        cout << "|  +-----+   +-----+  |  |    |  +-----+   +-----+  |                                 |   |  +-----+  |" << endl;
        cout << "|  |  S  |   |  M  |  |  |    |  |  F  |   |  M  |  +--+                              |   |  |  S  |  |" << endl;
        cout << "|  | (" << source_degree << ") +-->+ (" << map1_degree << ") |  +--+--->+  | (" << filter3_degree << ") +-->+ (" << map4_degree << ") |  |  |                              +-->+  | (1) |  |" << endl;
        cout << "|  +-----+   +-----+  |  |    |  +-----+   +-----+  |  |                              |   |  +-----+  |" << endl;
        cout << "+---------------------+  |    +---------------------+  |                              |   +-----------+" << endl;
        cout << "                         |                             |                              |" << endl;
        cout << "                         |    +---------------------+  |    +---------------------+   |" << endl;
        cout << "                         |    |  +-----+   +-----+  |  |    |  +-----+   +-----+  |   |" << endl;
        cout << "                         |    |  |  F  |   |  M  |  |  |    |  |  M  |   |  M  |  |   |" << endl;
        cout << "                         +--->+  | (" << filter4_degree << ") +-->+ (" << map5_degree << ") |  +--+--->+  | (" << map9_degree << ") +-->+ (" << map10_degree << ") |  +---+" << endl;
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
        check_degree += map7_degree;
        check_degree += map8_degree;
        check_degree += map9_degree;
        check_degree += map10_degree;
        check_degree++;
        // prepare the test
        PipeGraph graph("test_graph_7");
        // prepare the first MultiPipe
        // source
        Source_Positive_Functor source_functor_positive(stream_len, n_keys);
        Source source = Source_Builder(source_functor_positive)
                            .withName("source")
                            .withParallelism(source_degree)
                            .build();
        MultiPipe &pipe1 = graph.add_source(source);
        // map 1
        Map_Functor map_functor1;
        Map map1 = Map_Builder(map_functor1)
                        .withName("map1")
                        .withParallelism(map1_degree)
                        .enable_KeyBy()
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
        // filter 1
        Filter_Functor filter_functor1;
        Filter filter1 = Filter_Builder(filter_functor1)
                        .withName("filter1")
                        .withParallelism(filter1_degree)
                        .enable_KeyBy()
                        .build();
        pipe2.chain(filter1);
        // map 2
        Map_Functor map_functor2;
        Map map2 = Map_Builder(map_functor2)
                        .withName("map2")
                        .withParallelism(map2_degree)
                        .enable_KeyBy()
                        .build();
        pipe2.chain(map2);
        // prepare the third MultiPipe
        MultiPipe &pipe3 = pipe1.select(1);
        // filter 2
        Filter_Functor filter_functor2;
        Filter filter2 = Filter_Builder(filter_functor2)
                        .withName("filter2")
                        .withParallelism(filter2_degree)
                        .enable_KeyBy()
                        .build();
        pipe3.chain(filter2);
        // map 3
        Map_Functor map_functor3;
        Map map3 = Map_Builder(map_functor3)
                        .withName("map3")
                        .withParallelism(map3_degree)
                        .enable_KeyBy()
                        .build();
        pipe3.chain(map3);
        // prepare the fourth MultiPipe
        MultiPipe &pipe4 = pipe1.select(2);
        // filter 3
        Filter_Functor filter_functor3;
        Filter filter3 = Filter_Builder(filter_functor3)
                        .withName("filter3")
                        .withParallelism(filter3_degree)
                        .enable_KeyBy()
                        .build();
        pipe4.chain(filter3);
        // map 4
        Map_Functor map_functor4;
        Map map4 = Map_Builder(map_functor4)
                        .withName("map4")
                        .withParallelism(map4_degree)
                        .enable_KeyBy()
                        .build();
        pipe4.chain(map4);
        // prepare the fifth MultiPipe
        MultiPipe &pipe5 = pipe1.select(3);
        // filter 4
        Filter_Functor filter_functor4;
        Filter filter4 = Filter_Builder(filter_functor4)
                        .withName("filter4")
                        .withParallelism(filter4_degree)
                        .enable_KeyBy()
                        .build();
        pipe5.chain(filter4);
        // map 5
        Map_Functor map_functor5;
        Map map5 = Map_Builder(map_functor5)
                        .withName("map5")
                        .withParallelism(map5_degree)
                        .enable_KeyBy()
                        .build();
        pipe5.chain(map5);
        // prepare the sixth MultiPipe
        MultiPipe &pipe6 = pipe1.select(4);
        // filter 5
        Filter_Functor filter_functor5;
        Filter filter5 = Filter_Builder(filter_functor5)
                        .withName("filter5")
                        .withParallelism(filter5_degree)
                        .enable_KeyBy()
                        .build();
        pipe6.chain(filter5);
        // map 6
        Map_Functor map_functor6;
        Map map6 = Map_Builder(map_functor6)
                        .withName("map6")
                        .withParallelism(map6_degree)
                        .enable_KeyBy()
                        .build();
        pipe6.chain(map6);
        // prepare the seventh MultiPipe
        MultiPipe &pipe7 = pipe2.merge(pipe3);
        // map 7
        Map_Functor map_functor7;
        Map map7 = Map_Builder(map_functor7)
                        .withName("map7")
                        .withParallelism(map7_degree)
                        .enable_KeyBy()
                        .build();
        pipe7.chain(map7);
        // map 8
        Map_Functor map_functor8;
        Map map8 = Map_Builder(map_functor8)
                        .withName("map8")
                        .withParallelism(map8_degree)
                        .enable_KeyBy()
                        .build();
        pipe7.chain(map8);
        // prepare the eightth MultiPipe
        MultiPipe &pipe8 = pipe6.merge(pipe5, pipe4);
        // map 9
        Map_Functor map_functor9;
        Map map9 = Map_Builder(map_functor9)
                        .withName("map9")
                        .withParallelism(map9_degree)
                        .enable_KeyBy()
                        .build();
        pipe8.chain(map9);
        // map 10
        Map_Functor map_functor10;
        Map map10 = Map_Builder(map_functor10)
                        .withName("map10")
                        .withParallelism(map10_degree)
                        .enable_KeyBy()
                        .build();
        pipe8.chain(map10);        
        // prepare the ninth MultiPipe
        MultiPipe &pipe9 = pipe7.merge(pipe8);
        // sink
        Sink_Functor sink_functor(n_keys);
        Sink sink = Sink_Builder(sink_functor)
                        .withName("sink")
                        .withParallelism(1)
                        .enable_KeyBy()
                        .build();
        pipe9.chain_sink(sink);
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
