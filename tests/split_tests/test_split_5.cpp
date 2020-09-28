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
 *  Test 5 of the split of MultiPipe instances.
 *  
 *                                                  +---------------------+
 *                                                  |  +-----+   +-----+  |
 *                                                  |  |  M  |   |  S  |  |
 *                      +-----------+            +-->  | (*) +-->+ (1) |  |
 *                      |  +-----+  |            |  |  +-----+   +-----+  |
 *                 +--->+  |  M  |  |            |  +---------------------+
 *  +-----------+  |    |  | (*) |  +---------->-+   +---------------------------+
 *  |  +-----+  |  |    |  +-----+  |            |   |      KF(*)                |
 *  |  |  S  |  |  |    +-----------+            |   | +-------------+           |
 *  |  | (*) |  +--+                             |   | | +---------+ |           |
 *  |  +-----+  |  |                             +-->+ | | PF (*,*)| |   +-----+ |
 *  +-----------+  |                                 | | +---------+ |   |  S  | |
 *                 |    +-----------------------+    | |             +-->+ (1) | |
 *                 |    |  +------+    +-----+  |    | | +---------+ |   +-----+ |
 *                 |    |  |  FM  |    |  S  |  |    | | | PF (*,*)| |           |
 *                 +--->+  | (*)  +--->+ (1) |  |    | | +---------+ |           |
 *                      |  +------+    +-----+  |    | +-------------+           |
 *                      +-----------------------+    +---------------------------+
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
    int map1_degree, map2_degree, flatmap_degree, plq_degree, wlq_degree, kf_degree;
    size_t source_degree = 1;
    long last_result = 0;
    // executes the runs
    for (size_t i=0; i<runs; i++) {
        map1_degree = dist6(rng);
        map2_degree = dist6(rng);
        flatmap_degree = dist6(rng);
        plq_degree = dist6(rng);
        wlq_degree = dist6(rng);
        kf_degree = dist6(rng);
        cout << "Run " << i << endl;
        cout << "                                                +---------------------+" << endl;
        cout << "                                                |  +-----+   +-----+  |" << endl;
        cout << "                                                |  |  M  |   |  S  |  |" << endl;
        cout << "                    +-----------+            +-->  | (" << map2_degree << ") +-->+ (1) |  |" << endl;
        cout << "                    |  +-----+  |            |  |  +-----+   +-----+  |" << endl;
        cout << "               +--->+  |  M  |  |            |  +---------------------+" << endl;
        cout << "+-----------+  |    |  | (" << map1_degree << ") |  +---------->-+   +---------------------------+" << endl;
        cout << "|  +-----+  |  |    |  +-----+  |            |   |      KF(" << kf_degree << ")                |" << endl;
        cout << "|  |  S  |  |  |    +-----------+            |   | +-------------+           |" << endl;
        cout << "|  | (" << source_degree << ") |  +--+                             |   | | +---------+ |           |" << endl;
        cout << "|  +-----+  |  |                             +-->+ | | PF (" << plq_degree << "," << wlq_degree << ")| |   +-----+ |" << endl;
        cout << "+-----------+  |                                 | | +---------+ |   |  S  | |" << endl;
        cout << "               |    +-----------------------+    | |             +-->+ (1) | |" << endl;
        cout << "               |    |  +------+    +-----+  |    | | +---------+ |   +-----+ |" << endl;
        cout << "               |    |  |  FM  |    |  S  |  |    | | | PF (" << plq_degree << "," << wlq_degree << ")| |           |" << endl;
        cout << "               +--->+  | (" << flatmap_degree << ")  +--->+ (1) |  |    | | +---------+ |           |" << endl;
        cout << "                    |  +------+    +-----+  |    | +-------------+           |" << endl;
        cout << "                    +-----------------------+    +---------------------------+" << endl;
        // compute the total parallelism degree of the PipeGraph
        size_t check_degree = source_degree;
        check_degree += map1_degree;
        check_degree += map2_degree;
        if (map2_degree != 1)
            check_degree++;
        if (plq_degree == 1 && wlq_degree == 1)
            check_degree += kf_degree;
        else
            check_degree += (kf_degree) * (plq_degree + wlq_degree);
        check_degree++;
        check_degree += flatmap_degree;
        if (flatmap_degree != 1)
            check_degree++;
        // prepare the test
        PipeGraph graph("test_split_5", Mode::DETERMINISTIC);
        // prepare the first MultiPipe
        // source
        Source_Functor source_functor(stream_len, n_keys);
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
        // map 1
        Map_Functor1 map_functor1;
        Map map1 = Map_Builder(map_functor1)
                        .withName("map1")
                        .withParallelism(map1_degree)
                        .build();
        pipe2.chain(map1);
        // split
        pipe2.split([](const tuple_t &t) {
            if (t.value % 5 == 0)
                return 0;
            else
                return 1;
        }, 2);     
        // prepare the third MultiPipe
        MultiPipe &pipe3 = pipe2.select(0);
        // map 2
        Map_Functor2 map_functor2;
        Map map2 = Map_Builder(map_functor2)
                            .withName("map2")
                            .withParallelism(map2_degree)
                            .build();
        pipe3.chain(map2);
        // sink
        Sink_Functor sink_functor1(n_keys);
        Sink sink1 = Sink_Builder(sink_functor1)
                            .withName("sink1")
                            .withParallelism(1)
                            .build();
        pipe3.chain_sink(sink1);
        // prepare the fourth MultiPipe
        MultiPipe &pipe4 = pipe2.select(1);
        // user-defined pane function (Non-Incremental Query)
        auto F = [](size_t pid, const Iterable<tuple_t> &input, tuple_t &pane_result) {
            long sum = 0;
            // print the window content
            for (auto t : input) {
                int val = t.value;
                sum += val;
            }
            pane_result.value = sum;
        };
        // user-defined window function (Non-Incremental Query)
        auto G = [](size_t wid, const Iterable<tuple_t> &input, tuple_t &win_result) {
            long sum = 0;
            // print the window content
            for (auto t : input) {
                int val = t.value;
                sum += val;
            }
            win_result.value = sum;
        };
        // creation of the Pane_Farm and Win_Farm operators
        Pane_Farm pf = PaneFarm_Builder(F, G).withCBWindows(100, 1)
                                        .withParallelism(plq_degree, wlq_degree)
                                        .withName("pf")
                                        .prepare4Nesting()
                                        .build();
        Key_Farm kf = KeyFarm_Builder(pf).withParallelism(kf_degree)
                                        .withName("kf")
                                        .build();
        pipe4.add(kf);
        // sink
        Sink_Functor sink_functor2(n_keys);
        Sink sink2 = Sink_Builder(sink_functor2)
                            .withName("sink2")
                            .withParallelism(1)
                            .build();
        pipe4.chain_sink(sink2);
        // prepare the fifth MultiPipe
        MultiPipe &pipe5 = pipe1.select(1);
        // flatmap
        FlatMap_Functor flatmap_functor;
        FlatMap flatmap = FlatMap_Builder(flatmap_functor)
                                .withName("flatmap")
                                .withParallelism(flatmap_degree)
                                .build();
        pipe5.chain(flatmap);
        // sink
        Sink_Functor sink_functor3(n_keys);
        Sink sink3 = Sink_Builder(sink_functor3)
                            .withName("sink3")
                            .withParallelism(1)
                            .build();
        pipe5.chain_sink(sink3);
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
