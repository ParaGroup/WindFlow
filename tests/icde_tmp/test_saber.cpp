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
 *  Test for the comparison between WindFlow and Saber.
 */ 

// includes
#include<string>
#include<random>
#include<iostream>
#include<math.h>
#include<ff/ff.hpp>
#include<windflow.hpp>
#include<windflow_gpu.hpp>
#include"common_saber.hpp"

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
    size_t src_degree = 1;
    size_t win_len = 0;
    size_t win_slide = 0;
    size_t batch_size = 0;
    size_t win_batch_size = 1;
    // initialize global variable
    sent_tuples = 0;
    // arguments from command line
    if (argc != 11) {
        cout << argv[0] << " -n [num sources] -w [win length] -s [win slide] -b [batch size] -r [num wins per batch]" << endl;
        exit(EXIT_SUCCESS);
    }
    while ((option = getopt(argc, argv, "n:k:w:s:b:r:")) != -1) {
        switch (option) {
            case 'n': src_degree = atoi(optarg);
                     break;
            case 'w': win_len = atoi(optarg);
                     break;
            case 's': win_slide = atoi(optarg);
                     break;
            case 'b': batch_size = atoi(optarg);
                     break;
            case 'r': win_batch_size = atoi(optarg);
                     break;
            default: {
                cout << argv[0] << " -n [num sources] -w [win length] -s [win slide] -b [batch size] -r [num wins per batch]" << endl;
                exit(EXIT_SUCCESS);
            }
        }
    }
    // create and initialize the input stream shared by all sources
    input_t *buffer = (input_t *) malloc(sizeof(input_t) * BUFFER_SIZE);
    init_inputBuffer(buffer, BUFFER_SIZE);
    // application starting time
    volatile unsigned long app_start_time = current_time_nsecs();

    // prepare the test
    PipeGraph graph("test_saber", Execution_Mode_t::DEFAULT, Time_Policy_t::EVENT_TIME);

    Source_Functor source_functor(buffer, BUFFER_SIZE, app_start_time);
    Source source = Source_Builder(source_functor)
                        .withName("source")
                        .withParallelism(src_degree)
                        .withOutputBatchSize(batch_size)
                        .build();
    MultiPipe &mp = graph.add_source(source);
    Lift_Functor_GPU lift_functor;
    Comb_Functor_GPU comb_functor;
    Ffat_Windows_GPU ffat_gpu = Ffat_WindowsGPU_Builder(lift_functor, comb_functor)
                                        .withName("fat_gpu")
                                        .withCBWindows(win_len, win_slide)
                                        .withNumWinPerBatch(win_batch_size)
                                        .build();
    mp.add(ffat_gpu);
    Sink_Functor sink_functor;
    Sink sink = Sink_Builder(sink_functor)
                    .withName("sink")
                    .withParallelism(1)
                    .build();
    mp.chain_sink(sink);

    volatile unsigned long start_time_main_usecs = current_time_usecs();
    // run the application
    graph.run();
    volatile unsigned long end_time_main_usecs = current_time_usecs();
    double elapsed_time_seconds = (end_time_main_usecs - start_time_main_usecs) / (1000000.0);
    double throughput = sent_tuples / elapsed_time_seconds;
    cout << "Measured throughput: " << (int) throughput << " tuples/second" << endl;
    cout << "Measured throughput: " << ((int) throughput * (sizeof(batch_item_gpu_t<input_t>))) / (1024*1024) << " MB/s" << endl;
    return 0;
}
