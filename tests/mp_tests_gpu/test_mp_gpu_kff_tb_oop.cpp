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
 *  Test of the MultiPipe construct with out-of-order streams
 *  
 *  +-----+   +-----+   +------+   +-----+   +--------+   +-----+
 *  |  S  |   |  F  |   |  FM  |   |  M  |   | KFF_TB |   |  S  |
 *  | (1) +-->+ (*) +-->+  (*) +-->+ (*) +-->+  (*)   +-->+ (1) |
 *  +-----+   +-----+   +------+   +-----+   +--------+   +-----+
 */ 

// includes
#include<string>
#include<iostream>
#include<random>
#include<math.h>
#include<ff/ff.hpp>
#include<windflow.hpp>
#include<windflow_gpu.hpp>
#include"mp_common.hpp"

using namespace std;
using namespace chrono;
using namespace wf;

// global variable
extern long global_received;

// main
int main(int argc, char *argv[])
{
    int option = 0;
    size_t runs = 1;
    size_t stream_len = 0;
    size_t win_len = 0;
    size_t win_slide = 0;
    size_t n_keys = 1;
    size_t batch_len = 1;
    // initalize global variable
    global_received = 0;
    // arguments from command line
    if (argc != 13) {
        cout << argv[0] << " -r [runs] -l [stream_length] -k [n_keys] -w [win length usec] -s [win slide usec] -b [batch len]" << endl;
        exit(EXIT_SUCCESS);
    }
    while ((option = getopt(argc, argv, "r:l:k:w:s:b:")) != -1) {
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
            case 'b': batch_len = atoi(optarg);
                     break;
            default: {
                cout << argv[0] << " -r [runs] -l [stream_length] -k [n_keys] -w [win length usec] -s [win slide usec] -b [batch len]" << endl;
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
    int filter_degree, flatmap_degree, map_degree, kff_degree;
    size_t source_degree = 1;
    long last_results = 0;
    // executes the runs
    for (size_t i=0; i<runs; i++) {
        filter_degree = dist6(rng);
        flatmap_degree = dist6(rng);
        map_degree = dist6(rng);
        kff_degree = 1; //dist6(rng);
        cout << "Run " << i << endl;
        cout << "+-----+   +-----+   +------+   +-----+   +--------+   +-----+" << endl;
        cout << "|  S  |   |  F  |   |  FM  |   |  M  |   | KFF_TB |   |  S  |" << endl;
        cout << "| (" << source_degree << ") +-->+ (" << filter_degree << ") +-->+  (" << flatmap_degree << ") +-->+ (" << map_degree << ") +-->+  (" << kff_degree << ")   +-->+ (1) |" << endl;
        cout << "+-----+   +-----+   +------+   +-----+   +--------+   +-----+" << endl;
        // prepare the test
        PipeGraph graph("test_kff_tb_gpu");
        // source
        Source_Functor source_functor(stream_len, n_keys);
        auto *source = Source_Builder<decltype(source_functor)>(source_functor)
                                .withName("source")
                                .withParallelism(source_degree)
                                .build_ptr();
        MultiPipe &mp = graph.add_source(*source);
        // filter
        Filter_Functor filter_functor;
        auto *filter = Filter_Builder<decltype(filter_functor)>(filter_functor)
                                .withName("filter")
                                .withParallelism(filter_degree)
                                .build_ptr();
        mp.chain(*filter);
        // flatmap
        FlatMap_Functor flatmap_functor;
        auto *flatmap = FlatMap_Builder<decltype(flatmap_functor)>(flatmap_functor)
                                .withName("flatmap")
                                .withParallelism(flatmap_degree)
                                .build_ptr();
        mp.chain(*flatmap);
        // map
        Map_Functor map_functor;
        auto *map = Map_Builder<decltype(map_functor)>(map_functor).withName("map").withParallelism(map_degree).build_ptr();
        mp.chain(*map);
        // kff
        // user-defined lift function
        auto liftFunction = [](const tuple_t &t, output_t &out) {
            out.key = t.key;
            out.id = t.id;
            out.ts = t.ts;
            out.value = t.value;
        };
        // user-defined combine function
        auto combineFunction = [] __host__ __device__ (const output_t &o1, const output_t &o2, output_t &out) {
            out.value = o1.value + o2.value;
        };
        // creation of the Key_FFAT_GPU operator
        auto *kff_gpu = KeyFFATGPU_Builder<decltype(liftFunction), decltype(combineFunction)>(liftFunction, combineFunction)
                                    .withTBWindows(microseconds(win_len), microseconds(win_slide), /* delay */ seconds(1)) // huge delay because the timestamps in this example does not respect the real generation speed
                                    .withParallelism(kff_degree)
                                    .withBatch(batch_len)
                                    .withName("kff")
                                    .build_ptr();
        mp.add(*kff_gpu);
        // sink
        Sink_Functor sink_functor(n_keys);
        auto *sink = Sink_Builder<decltype(sink_functor)>(sink_functor)
                            .withName("sink")
                            .withParallelism(1)
                            .build_ptr();
        mp.chain_sink(*sink);
        // run the application
        graph.run();
        if (i == 0) {
            last_results = global_received;
            cout << "Result is --> " << GREEN << "OK" << "!!!" << DEFAULT_COLOR << endl;
            cout << "Number of dropped tuples: " << kff_gpu->getNumDroppedTuples() << endl;
        }
        else {
            if (last_results == global_received) {
                cout << "Result is --> " << GREEN << "OK" << "!!!" << DEFAULT_COLOR << endl;
                cout << "Number of dropped tuples: " << kff_gpu->getNumDroppedTuples() << endl;
            }
            else {
                cout << "Result is --> " << RED << "FAILED" << "!!!" << DEFAULT_COLOR << endl;
                cout << "Number of dropped tuples: " << kff_gpu->getNumDroppedTuples() << endl;
            }
        }
    }
    return 0;
}
