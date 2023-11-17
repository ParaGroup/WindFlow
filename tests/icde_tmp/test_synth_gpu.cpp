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
 *  Test of the FFAT_Windows_GPU operator with a given aggregate function.
 */ 

// includes
#include<string>
#include<random>
#include<iostream>
#include<math.h>
#include<ff/ff.hpp>
#include<windflow.hpp>
#include<windflow_gpu.hpp>
#include"common.hpp"
#include"aggregates.hpp"

#define BUFFER_SIZE 32768

using namespace std;
using namespace chrono;
using namespace wf;

// global variables
atomic<long> sent_tuples;

// main
int main(int argc, char *argv[])
{
    int option = 0;
    size_t n = 1;
    size_t w = 0;
    size_t s = 0;
    size_t b = 0;
    size_t r = 1;
    string agg;
    // initialize global variable
    sent_tuples = 0;
    // arguments from command line
    if (argc != 13) {
        cout << argv[0] << " -n [num sources] -w [win length usec] -s [win slide usec] -b [batch size] -r [num wins per batch] -a [aggregation]" << endl;
        exit(EXIT_SUCCESS);
    }
    while ((option = getopt(argc, argv, "n:w:s:b:r:a:")) != -1) {
        switch (option) {
            case 'n': n = atoi(optarg);
                break;
            case 'w': w = atoi(optarg);
                break;
            case 's': s = atoi(optarg);
                break;
            case 'b': b = atoi(optarg);
                break;
            case 'r': r = atoi(optarg);
                break;
            case 'a': agg = string(optarg);
                break;
            default: {
                cout << argv[0] << " -n [num sources] -w [win length usec] -s [win slide usec] -b [batch size] -r [num wins per batch] -a [aggregation]" << endl;
                exit(EXIT_SUCCESS);
            }
        }
    }

    // create and initialize the input stream shared by all sources
    input_t *buffer = (input_t *) malloc(sizeof(input_t) * BUFFER_SIZE);
    init_inputBuffer(buffer, BUFFER_SIZE, 1);

    // application starting time
    volatile unsigned long app_start_time = current_time_nsecs();

    // prepare the application
    PipeGraph graph("test_synth_gpu", Execution_Mode_t::DEFAULT, Time_Policy_t::EVENT_TIME);

    Source_Functor source_functor(buffer, BUFFER_SIZE, app_start_time);
    Source source = Source_Builder(source_functor)
                      .withName("source")
                      .withParallelism(n)
                      .withOutputBatchSize(b)
                      .build();
    MultiPipe &mp = graph.add_source(source);

    if (agg == "sum") {
        Lift_SUM_GPU lift;
        Combine_SUM_GPU comb;
        Ffat_Windows_GPU ffat_gpu = Ffat_WindowsGPU_Builder(lift, comb)
                                          .withName("sum_gpu")
                                          .withTBWindows(microseconds(w), microseconds(s))
                                          .withNumWinPerBatch(r)
                                          .build();
        mp.add(ffat_gpu);
        Sink_Functor<output_v1_t> sink_functor;
        Sink sink = Sink_Builder(sink_functor)
                     .withName("sink")
                     .withParallelism(1)
                     .build();
        mp.chain_sink(sink);
    }
    else if (agg == "count") {
        Lift_COUNT_GPU lift;
        Combine_COUNT_GPU comb;
        Ffat_Windows_GPU ffat_gpu = Ffat_WindowsGPU_Builder(lift, comb)
                                           .withName("count_gpu")
                                           .withTBWindows(microseconds(w), microseconds(s))
                                           .withNumWinPerBatch(r)
                                           .build();
        mp.add(ffat_gpu);
        Sink_Functor<output_v1_t> sink_functor;
        Sink sink = Sink_Builder(sink_functor)
                      .withName("sink")
                      .withParallelism(1)
                      .build();
        mp.chain_sink(sink);
    }
    else if (agg == "max") {
        Lift_MAX_GPU lift;
        Combine_MAX_GPU comb;
        Ffat_Windows_GPU ffat_gpu = Ffat_WindowsGPU_Builder(lift, comb)
                                            .withName("max_gpu")
                                            .withTBWindows(microseconds(w), microseconds(s))
                                            .withNumWinPerBatch(r)
                                            .build();
        mp.add(ffat_gpu);
        Sink_Functor<output_v1_t> sink_functor;
        Sink sink = Sink_Builder(sink_functor)
                       .withName("sink")
                       .withParallelism(1)
                       .build();
        mp.chain_sink(sink);
    }
    else if (agg == "max_count") {
        Lift_MAX_COUNT_GPU lift;
        Combine_MAX_COUNT_GPU comb;
        Ffat_Windows_GPU ffat_gpu = Ffat_WindowsGPU_Builder(lift, comb)
                                            .withName("maxcount_gpu")
                                            .withTBWindows(microseconds(w), microseconds(s))
                                            .withNumWinPerBatch(r)
                                            .build();
        mp.add(ffat_gpu);
        Sink_Functor<output_v2_t> sink_functor;
        Sink sink = Sink_Builder(sink_functor)
                       .withName("sink")
                       .withParallelism(1)
                       .build();
        mp.chain_sink(sink);
    }
    else if (agg == "min") {
        Lift_MIN_GPU lift;
        Combine_MIN_GPU comb;
        Ffat_Windows_GPU ffat_gpu = Ffat_WindowsGPU_Builder(lift, comb)
                                            .withName("min_gpu")
                                            .withTBWindows(microseconds(w), microseconds(s))
                                            .withNumWinPerBatch(r)
                                            .build();
        mp.add(ffat_gpu);
        Sink_Functor<output_v1_t> sink_functor;
        Sink sink = Sink_Builder(sink_functor)
                       .withName("sink")
                       .withParallelism(1)
                       .build();
        mp.chain_sink(sink);
    }
    else if (agg == "min_count") {
        Lift_MIN_COUNT_GPU lift;
        Combine_MIN_COUNT_GPU comb;
        Ffat_Windows_GPU ffat_gpu = Ffat_WindowsGPU_Builder(lift, comb)
                                            .withName("mincount_gpu")
                                            .withTBWindows(microseconds(w), microseconds(s))
                                            .withNumWinPerBatch(r)
                                            .build();
        mp.add(ffat_gpu);
        Sink_Functor<output_v2_t> sink_functor;
        Sink sink = Sink_Builder(sink_functor)
                       .withName("sink")
                       .withParallelism(1)
                       .build();
        mp.chain_sink(sink);
    }
    else if (agg == "avg") {
        Lift_AVG_GPU lift;
        Combine_AVG_GPU comb;
        Ffat_Windows_GPU ffat_gpu = Ffat_WindowsGPU_Builder(lift, comb)
                                            .withName("avg_gpu")
                                            .withTBWindows(microseconds(w), microseconds(s))
                                            .withNumWinPerBatch(r)
                                            .build();
        mp.add(ffat_gpu);
        Sink_Functor<output_v2_t> sink_functor;
        Sink sink = Sink_Builder(sink_functor)
                       .withName("sink")
                       .withParallelism(1)
                       .build();
        mp.chain_sink(sink);
    }
    else if (agg == "geom") {
        Lift_GEOM_GPU lift;
        Combine_GEOM_GPU comb;
        Ffat_Windows_GPU ffat_gpu = Ffat_WindowsGPU_Builder(lift, comb)
                                            .withName("geom_gpu")
                                            .withTBWindows(microseconds(w), microseconds(s))
                                            .withNumWinPerBatch(r)
                                            .build();
        mp.add(ffat_gpu);
        Sink_Functor<output_v2_t> sink_functor;
        Sink sink = Sink_Builder(sink_functor)
                       .withName("sink")
                       .withParallelism(1)
                       .build();
        mp.chain_sink(sink);
    }
    else if (agg == "sstd") {
        Lift_SSTD_GPU lift;
        Combine_SSTD_GPU comb;
        Ffat_Windows_GPU ffat_gpu = Ffat_WindowsGPU_Builder(lift, comb)
                                            .withName("sstd_gpu")
                                            .withTBWindows(microseconds(w), microseconds(s))
                                            .withNumWinPerBatch(r)
                                            .build();
        mp.add(ffat_gpu);
        Sink_Functor<output_v3_t> sink_functor;
        Sink sink = Sink_Builder(sink_functor)
                       .withName("sink")
                       .withParallelism(1)
                       .build();
        mp.chain_sink(sink);
    }
    else if (agg == "pstd") {
        Lift_PSTD_GPU lift;
        Combine_PSTD_GPU comb;
        Ffat_Windows_GPU ffat_gpu = Ffat_WindowsGPU_Builder(lift, comb)
                                            .withName("Pstd_gpu")
                                            .withTBWindows(microseconds(w), microseconds(s))
                                            .withNumWinPerBatch(r)
                                            .build();
        mp.add(ffat_gpu);
        Sink_Functor<output_v3_t> sink_functor;
        Sink sink = Sink_Builder(sink_functor)
                       .withName("sink")
                       .withParallelism(1)
                       .build();
        mp.chain_sink(sink);       
    }
    else {
        cout << "Not a valid aggregate!" << endl;
        abort();
    }

    volatile unsigned long start_time_main_usecs = current_time_usecs();
    // run the application
    graph.run();
    volatile unsigned long end_time_main_usecs = current_time_usecs();

    // compute statistics
    double elapsed_time_seconds = (end_time_main_usecs - start_time_main_usecs) / (1000000.0);
    double throughput = sent_tuples / elapsed_time_seconds;
    cout << "Measured throughput: " << (int) throughput << " tuples/second" << endl;
    cout << "Measured throughput: " << ((int) throughput * (sizeof(batch_item_gpu_t<input_t>) + 8)) / (1024*1024) << " MB/s" << endl;
    return 0;
}
