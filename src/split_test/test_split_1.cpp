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
 *  First Test Program of the Split of a MultiPipe
 *  
 *  MP1|->(MP2, MP3)
 */ 

// include
#include <random>
#include <iostream>
#include <ff/ff.hpp>
#include <windflow.hpp>
#include "split_common.hpp"

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
    size_t max = 10;
    std::uniform_int_distribution<std::mt19937::result_type> dist6(min, max);
    int map1_degree, map2_degree, filter_degree;
    size_t source_degree = dist6(rng);
    long last_result = 0;
    // executes the runs
    for (size_t i=0; i<runs; i++) {
        map1_degree = dist6(rng);
        map2_degree = dist6(rng);
        filter_degree = dist6(rng);
        cout << "Run " << i << " Source(" << source_degree <<")->Map(" << map1_degree << ")-|" << endl;
        cout << "      ->Filter(" << filter_degree << ")->Map(" << map2_degree << ")->Sink(1)" << endl;
        cout << "      ->Sink(1)" << endl;

        // prepare the first MultiPipe
        MultiPipe pipe1("pipe1");
        // source
        Source_Functor source_functor(stream_len, n_keys);
        Source source = Source_Builder(source_functor)
                                .withName("pipe1_source")
                                .withParallelism(source_degree)
                                .build();
        pipe1.add_source(source);
        // map 1
        Map_Functor1 map_functor1;
        Map map1 = Map_Builder(map_functor1)
                            .withName("pipe1_map")
                            .withParallelism(map1_degree)
                            .build();
        pipe1.add(map1);

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
        Filter_Functor filter_functor;
        Filter filter = Filter_Builder(filter_functor)
                                .withName("pipe2_filter")
                                .withParallelism(filter_degree)
                                .build();
        pipe2.add(filter);
        // map 2
        Map_Functor2 map_functor2;
        Map map2 = Map_Builder(map_functor2)
                            .withName("pipe2_map")
                            .withParallelism(map2_degree)
                            .build();
        pipe2.chain(map2);
        // sink
        Sink_Functor sink_functor(n_keys);
        Sink sink = Sink_Builder(sink_functor)
                            .withName("pipe2_sink")
                            .withParallelism(1)
                            .build();
        pipe2.add_sink(sink);

        // prepare the third MultiPipe
        MultiPipe &pipe3 = pipe1.select(1);
        // sink
        Sink_Functor sink_functor2(n_keys);
        Sink sink2 = Sink_Builder(sink_functor2)
                            .withName("pipe3_sink")
                            .withParallelism(1)
                            .build();
        pipe3.add_sink(sink2);

        // run the application
        pipe1.run_and_wait_end();
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
