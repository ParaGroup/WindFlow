/* *****************************************************************************
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
 *  Test of the MultiPipe construct
 *  
 *  Composition: Source -> Filter -> FlatMap -> Map -> WF_TB -> Sink
 */ 

// includes
#include <string>
#include <iostream>
#include <random>
#include <math.h>
#include <ff/ff.hpp>
#include <windflow.hpp>
#include "mp_common.hpp"

using namespace std;
using namespace chrono;
using namespace wf;

// global variable for the result
extern long global_sum;

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
    size_t max = 10;
    std::uniform_int_distribution<std::mt19937::result_type> dist6(min, max);
    int source_degree, degree1, degree2, wf_degree;
    source_degree = 1;
    long last_result = 0;
    // executes the runs
    for (size_t i=0; i<runs; i++) {
    	degree1 = dist6(rng);
    	degree2 = dist6(rng);
    	wf_degree = dist6(rng);
    	cout << "Run " << i << " Source(" << source_degree <<")->Filter(" << degree1 << ")-?->FlatMap(" << degree2 << ")-c->Map(" << degree2 << ")->Win_Farm_TB(" << wf_degree << ")-?->Sink(1)" << endl;
	    // prepare the test
	    PipeGraph graph("test_wf_tb_ch");
	    // source
	    Source_Functor source_functor(stream_len, n_keys);
	    Source source = Source_Builder(source_functor)
	    					.withName("test_wf_tb_ch_source")
	    					.withParallelism(source_degree)
	    					.build();
	    MultiPipe &mp = graph.add_source(source);
	    // filter
	    Filter_Functor filter_functor;
	    Filter filter = Filter_Builder(filter_functor)
	    						.withName("test_wf_tb_ch_filter")
	    						.withParallelism(degree1)
	    						.build();
	    mp.chain(filter);
	    // flatmap
	    FlatMap_Functor flatmap_functor;
	    FlatMap flatmap = FlatMap_Builder(flatmap_functor)
	    						.withName("test_wf_tb_ch_flatmap")
	    						.withParallelism(degree2)
	    						.build();
	    mp.chain(flatmap);
	    // map
	    Map_Functor map_functor;
	    Map map = Map_Builder(map_functor)
	    				.withName("test_wf_tb_ch_map")
	    				.withParallelism(degree2)
	    				.build();
	    mp.chain(map);
	    // wf
	    Win_Farm wf = WinFarm_Builder(wf_function)
	    					.withName("test_wf_tb_ch_wf")
	    					.withParallelism(wf_degree)
	    					.withTBWindows(microseconds(win_len), microseconds(win_slide))
	    					.build();
	    mp.add(wf);
	    // sink
	    Sink_Functor sink_functor(n_keys);
	    Sink sink = Sink_Builder(sink_functor)
	    					.withName("test_wf_tb_ch_sink")
	    					.withParallelism(1)
	    					.build();
	    mp.chain_sink(sink);
	   	// run the application
	   	graph.run();
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
    }
	return 0;
}
