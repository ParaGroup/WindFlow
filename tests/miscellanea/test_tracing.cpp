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
 *  This test runs the graph below one time with a random number of replicas for each
 *  operator. It generates the tracing log files of the WindFlow library and interacts
 *  with the WindFlow Web Dashboard.
 *  
 *                                +---------------------+
 *                                |  +-----+   +-----+  |
 *                                |  |  F  |   |  M  |  |
 *                           +--->+  | (*) +-->+ (*) |  +---------------------------+
 *                           |    |  +-----+   +-----+  |                           |
 *                           |    +---------------------+                           |
 *                           |                                    +-----------+     |
 *                           |                                    |  +-----+  |     |    +-----------+
 *                           |                                    |  |  M  |  |     |    |  +-----+  |
 *                           |                               +----+  | (*) |  +-----+--->+  |  S  |  |
 *  +---------------------+  |    +---------------------+    |    |  +-----+  |     |    |  | (1) |  |
 *  |  +-----+   +-----+  |  |    |  +-----+   +-----+  |    |    +-----------+     |    |  +-----+  |
 *  |  |  S  |   |  M  |  |  |    |  |  F  |   |  M  |  |    |                      |    +-----------+
 *  |  | (*) +-->+ (*) |  +--+--->+  | (*) +-->+ (*) |  +----+                      |
 *  |  +-----+   +-----+  |  |    |  +-----+   +-----+  |    |                      |
 *  +---------------------+  |    +---------------------+    |    +-----------+     |
 *                           |                               |    |  +-----+  |     |
 *                           |    +---------------------+    +----+  |  M  |  +-----+
 *                           |    |  +-----+   +-----+  |         |  | (*) |  |
 *                           |    |  |  F  |   |  S  |  |         |  +-----+  |
 *                           +--->+  | (*) +-->+ (1) |  |         +-----------+
 *                                |  +-----+   +-----+  |
 *                                +---------------------+
 */ 

// include
#include<random>
#include<iostream>
#include<ff/ff.hpp>
#include<windflow.hpp>
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
    std::uniform_int_distribution<std::mt19937::result_type> dist6(min, max);
    int map1_degree, map2_degree, map3_degree, map4_degree, map5_degree;
    int filter1_degree, filter2_degree, filter3_degree;
    size_t source_degree = dist6(rng);
    map1_degree = dist6(rng);
    map2_degree = dist6(rng);
    map3_degree = dist6(rng);
    map4_degree = dist6(rng);
    map5_degree = dist6(rng);
    filter1_degree = dist6(rng);
    filter2_degree = dist6(rng);
    filter3_degree = dist6(rng);
    cout << "                                 +---------------------+" << endl;
    cout << "                                 |  +-----+   +-----+  |" << endl;
    cout << "                                 |  |  F  |   |  M  |  |" << endl;
    cout << "                            +--->+  | (" << filter1_degree << ") +-->+ (" << map2_degree << ") |  +---------------------------+" << endl;
    cout << "                            |    |  +-----+   +-----+  |                           |" << endl;
    cout << "                            |    +---------------------+                           |" << endl;
    cout << "                            |                                    +-----------+     |" << endl;
    cout << "                            |                                    |  +-----+  |     |    +-----------+" << endl;
    cout << "                            |                                    |  |  M  |  |     |    |  +-----+  |" << endl;
    cout << "                            |                               +----+  | (" << map4_degree << ") |  +-----+--->+  |  S  |  |" << endl;
    cout << "   +---------------------+  |    +---------------------+    |    |  +-----+  |     |    |  | (1) |  |" << endl;
    cout << "   |  +-----+   +-----+  |  |    |  +-----+   +-----+  |    |    +-----------+     |    |  +-----+  |" << endl;
    cout << "   |  |  S  |   |  M  |  |  |    |  |  F  |   |  M  |  |    |                      |    +-----------+" << endl;
    cout << "   |  | (" << source_degree << ") +-->+ (" << map1_degree << ") |  +--+--->+  | (" << filter2_degree << ") +-->+ (" << map3_degree << ") |  +----+                      |" << endl;
    cout << "   |  +-----+   +-----+  |  |    |  +-----+   +-----+  |    |                      |" << endl;
    cout << "   +---------------------+  |    +---------------------+    |    +-----------+     |" << endl;
    cout << "                            |                               |    |  +-----+  |     |" << endl;
    cout << "                            |    +---------------------+    +----+  |  M  |  +-----+" << endl;
    cout << "                            |    |  +-----+   +-----+  |         |  | (" << map5_degree << ") |  |" << endl;
    cout << "                            |    |  |  F  |   |  S  |  |         |  +-----+  |" << endl;
    cout << "                            +--->+  | (" << filter3_degree << ") +-->+ (1) |  |         +-----------+" << endl;
    cout << "                                 |  +-----+   +-----+  |" << endl;
    cout << "                                 +---------------------+" << endl;
    // compute the total parallelism degree of the PipeGraph
    size_t check_degree = source_degree;
    if (source_degree != map1_degree)
        check_degree += map1_degree;
    check_degree += filter1_degree;
    if (filter1_degree != map2_degree)
        check_degree += map2_degree;
    check_degree += filter2_degree;
    if (filter2_degree != map3_degree)
        check_degree += map3_degree;
    check_degree += filter3_degree;
    if (filter3_degree != 1)
        check_degree ++;
    check_degree += map4_degree;
    check_degree += map5_degree;
    check_degree++;
    // prepare the test
    PipeGraph graph("test_tracing");
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
                    .build();
    pipe1.chain(map1);
    // split
    pipe1.split([](const tuple_t &t) {
        if (t.value % 2)
            return 0;
        else {
            if (t.value % 3 == 0)
                return 1;
            else
                return 2;
        }
    }, 3);
    // prepare the second MultiPipe
    MultiPipe &pipe2 = pipe1.select(0);
    // filter 1
    Filter_Functor filter_functor1;
    Filter filter1 = Filter_Builder(filter_functor1)
                    .withName("filter1")
                    .withParallelism(filter1_degree)
                    .build();
    pipe2.chain(filter1);
    // map 2
    Map_Functor map_functor2;
    Map map2 = Map_Builder(map_functor2)
                    .withName("map2")
                    .withParallelism(map2_degree)
                    .build();
    pipe2.chain(map2);
    // prepare the third MultiPipe
    MultiPipe &pipe3 = pipe1.select(1);
    // filter 2
    Filter_Functor filter_functor2;
    Filter filter2 = Filter_Builder(filter_functor2)
                    .withName("filter2")
                    .withParallelism(filter2_degree)
                    .build();
    pipe3.chain(filter2);
    // map 3
    Map_Functor map_functor3;
    Map map3 = Map_Builder(map_functor3)
                    .withName("map3")
                    .withParallelism(map3_degree)
                    .build();
    pipe3.chain(map3);
    // prepare the fourth MultiPipe
    MultiPipe &pipe4 = pipe1.select(2);
    // filter 3
    Filter_Functor filter_functor3;
    Filter filter3 = Filter_Builder(filter_functor3)
                    .withName("filter3")
                    .withParallelism(filter3_degree)
                    .build();
    pipe4.chain(filter3);
    // sink 1
    Sink_Functor sink_functor1(n_keys);
    Sink sink1 = Sink_Builder(sink_functor1)
                    .withName("sink1")
                    .withParallelism(1)
                    .build();
    pipe4.chain_sink(sink1);
    // split
    pipe3.split([](const tuple_t &t) {
        if (t.value % 7)
            return 0;
        else {
            return 1;
        }
    }, 2);        
    // prepare the fifth MultiPipe
    MultiPipe &pipe5 = pipe3.select(0);
    // map 4
    Map_Functor map_functor4;
    Map map4 = Map_Builder(map_functor4)
                    .withName("map4")
                    .withParallelism(map4_degree)
                    .build();
    pipe5.chain(map4);
    // prepare the sixth MultiPipe
    MultiPipe &pipe6 = pipe3.select(1);
    // map 5
    Map_Functor map_functor5;
    Map map5 = Map_Builder(map_functor5)
                    .withName("map5")
                    .withParallelism(map5_degree)
                    .build();
    pipe6.chain(map5);
    // prepare the seventh MultiPipe
    MultiPipe &pipe7 = pipe2.merge(pipe5, pipe6);
    // sink 2
    Sink_Functor sink_functor2(n_keys);
    Sink sink2 = Sink_Builder(sink_functor2)
                    .withName("sink2")
                    .withParallelism(1)
                    .build();
    pipe7.chain_sink(sink2);
    assert(graph.getNumThreads() == check_degree);
    // run the application
    graph.run();
    cout << "Result is --> " << GREEN << "OK" << "!!!" << DEFAULT_COLOR << endl;
    return 0;
}
