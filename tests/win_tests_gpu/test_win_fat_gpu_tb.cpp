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
 *  Test of the FFAT_Aggregator operator with time-based windows. This test
 *  also includes some basic GPU operators.
 *  
 * +--------------------------------------------------------------------------+
 * |  +-----+     +-----+     +-----+     +-----+     +--------+     +-----+  |
 * |  |  S  |     |  F  |     |  M  |     |  M  |     | FAT_TB |     |  S  |  |
 * |  | CPU +---->+ CPU +---->+ GPU +---->+ GPU +---->+  CPU   +---->+ CPU |  |
 * |  | (*) |     | (*) |     | (*) |     | (*) |     |  (*)   |     | (*) |  |
 * |  +-----+     +-----+     +-----+     +-----+     +--------+     +-----+  |
 * +--------------------------------------------------------------------------+
 */ 

// includes
#include<string>
#include<random>
#include<iostream>
#include<math.h>
#include<ff/ff.hpp>
#include<windflow.hpp>
#include<windflow_gpu.hpp>
#include"win_common_gpu.hpp"

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
    std::uniform_int_distribution<std::mt19937::result_type> dist_b(100, 200);
    int filter_degree, map1_degree, map2_degree, map3_degree, fat_degree;
    size_t source_degree, sink_degree;
    long last_result = 0;
    source_degree = 1; // dist_p(rng);
    // executes the runs in DEFAULT mode
    for (size_t i=0; i<runs; i++) {
        filter_degree = dist_p(rng);
        map1_degree = dist_p(rng);
        map2_degree = dist_p(rng);
        map3_degree = dist_p(rng);
        fat_degree = dist_p(rng);
        sink_degree = dist_p(rng);
        cout << "Run " << i << endl;
        std::cout << "+--------------------------------------------------------------------------+" << std::endl;
        std::cout << "|  +-----+     +-----+     +-----+     +-----+     +--------+     +-----+  |" << std::endl;
        std::cout << "|  |  S  |     |  F  |     |  M  |     |  M  |     | FAT_TB |     |  S  |  |" << std::endl;
        std::cout << "|  | CPU +---->+ CPU +---->+ GPU +---->+ GPU +---->+  CPU   +---->+ CPU |  |" << std::endl;
        std::cout << "|  | (" << source_degree << ") |     | (" << filter_degree << ") |     | (" << map1_degree << ") |     | (" << map2_degree << ") |     |  (" << fat_degree << ")   |     | (" << sink_degree << ") |  |" << std::endl;
        std::cout << "|  +-----+     +-----+     +-----+     +-----+     +--------+     +-----+  |" << std::endl;
        std::cout << "+--------------------------------------------------------------------------+" << std::endl;
        // prepare the test
        PipeGraph graph("test_win_fat_gpu_tb", Execution_Mode_t::DEFAULT, Time_Policy_t::EVENT_TIME);
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
        Map_Functor map_functor1;
        Map_GPU mapgpu1 = MapGPU_Builder(map_functor1)
                            .withName("mapgpu1")
                            .withParallelism(map1_degree)
                            .build();
        mp.chain(mapgpu1);
        Map_Functor_KB map_functor_gpu2;
        Map_GPU mapgpu2 = MapGPU_Builder(map_functor_gpu2)
                                .withName("mapgpu2")
                                .withParallelism(map2_degree)
                                .withKeyBy([] __host__ __device__ (const tuple_t &t) -> size_t { return t.key; })
                                .build();
        mp.chain(mapgpu2);
        Lift_Functor lift_functor;
        Comb_Functor comb_functor;
        FFAT_Aggregator fatagg = FFAT_Aggregator_Builder(lift_functor, comb_functor)
                                    .withName("ffat_agg")
                                    .withParallelism(fat_degree)
                                    .withKeyBy([](const tuple_t &t) -> size_t { return t.key; })
                                    .withTBWindows(microseconds(win_len), microseconds(win_slide))
                                    .withOutputBatchSize(dist_b(rng))
                                    .build();
        mp.add(fatagg);
        Sink_Functor sink_functor;
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
