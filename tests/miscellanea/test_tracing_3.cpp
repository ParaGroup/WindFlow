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
 *  Test 3 tracing support and interaction with web dashboard.
 *  
 *  +-----------------------------------------------------------------+
 *  |  +-----+   +-----+   +------+   +-----+   +--------+   +-----+  |
 *  |  |  S  |   |  F  |   |  FM  |   |  M  |   | MRW_CB |   |  S  |  |
 *  |  | (*) +-->+ (*) +-->+  (*) +-->+ (*) +-->+  (*,*) +-->+ (*) |  |
 *  |  +-----+   +-----+   +------+   +-----+   +--------+   +-----+  |
 *  +-----------------------------------------------------------------+
 */ 

// includes
#include<string>
#include<random>
#include<iostream>
#include<math.h>
#include<ff/ff.hpp>
#include<windflow.hpp>
#include"misc_common.hpp"

using namespace std;
using namespace chrono;
using namespace wf;

// main
int main(int argc, char *argv[])
{
    int option = 0;
    size_t stream_len = 0;
    size_t win_len = 0;
    size_t win_slide = 0;
    size_t n_keys = 1;
    // arguments from command line
    if (argc != 9) {
        cout << argv[0] << " -l [stream_length] -k [n_keys] -w [win length usec] -s [win slide usec]" << endl;
        exit(EXIT_SUCCESS);
    }
    while ((option = getopt(argc, argv, "l:k:w:s:")) != -1) {
        switch (option) {
            case 'l': stream_len = atoi(optarg);
                     break;
            case 'k': n_keys = atoi(optarg);
                     break;
            case 'w': win_len = atoi(optarg);
                     break;
            case 's': win_slide = atoi(optarg);
                     break;
            default: {
                cout << argv[0] << " -l [stream_length] -k [n_keys] -w [win length usec] -s [win slide usec]" << endl;
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
    int filter_degree, flatmap_degree, map_degree, wmap_degree, reduce_degree;
    size_t source_degree, sink_degree;
    long last_result = 0;
    source_degree = 1; // dist_p(rng);
    filter_degree = dist_p(rng);
    flatmap_degree = dist_p(rng);
    map_degree = dist_p(rng);
    wmap_degree = dist_p(rng);
    reduce_degree = dist_p(rng);
    sink_degree = dist_p(rng);
    std::cout << "+-----------------------------------------------------------------+" << std::endl;
    std::cout << "|  +-----+   +-----+   +------+   +-----+   +--------+   +-----+  |" << std::endl;
    std::cout << "|  |  S  |   |  F  |   |  FM  |   |  M  |   | MRW_TB |   |  S  |  |" << std::endl;
    std::cout << "|  | (" << source_degree << ") +-->+ (" << filter_degree << ") +-->+  (" << flatmap_degree << ") +-->+ (" << map_degree << ") +-->+  (" << wmap_degree << "," << reduce_degree << ") +-->+ (" << sink_degree << ") |  |" << std::endl;
    std::cout << "|  +-----+   +-----+   +------+   +-----+   +--------+   +-----+  |" << std::endl;
    std::cout << "+-----------------------------------------------------------------+" << std::endl;
    // prepare the test
    PipeGraph graph("test_tracing_3", Execution_Mode_t::DEFAULT, Time_Policy_t::EVENT_TIME);
    Source_Positive_Functor source_functor(stream_len, n_keys, true);
    Source source = Source_Builder(source_functor)
                        .withName("source")
                        .withParallelism(source_degree)
                        .withOutputBatchSize(dist_b(rng))
                        .build();
    MultiPipe &mp = graph.add_source(source);
    Filter_Functor_KB filter_functor;
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
    Stage1_Functor wmap_functor;
    Stage2_Functor reduce_functor;
    MapReduce_Windows mrwins = MapReduce_Windows_Builder(wmap_functor, reduce_functor)
                                    .withName("mr_wins")
                                    .withParallelism(wmap_degree, reduce_degree)
                                    .withKeyBy([](const tuple_t &t) -> size_t { return t.key; })
                                    .withTBWindows(microseconds(win_len), microseconds(win_slide))
                                    .withOutputBatchSize(dist_b(rng))
                                    .build();
    mp.add(mrwins);
    Sink_Functor2 sink_functor;
    Sink sink = Sink_Builder(sink_functor)
                    .withName("sink")
                    .withParallelism(sink_degree)
                    .build();
    mp.chain_sink(sink);
    // run the application
    graph.run();
}
