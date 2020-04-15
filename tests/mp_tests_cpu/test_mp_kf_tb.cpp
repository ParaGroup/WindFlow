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
 *  Test of the MultiPipe construct:
 *  
 *  +-----+   +-----+   +------+   +-----+   +-------+   +-----+
 *  |  S  |   |  F  |   |  FM  |   |  M  |   | KF_TB |   |  S  |
 *  | (1) +-->+ (*) +-->+  (*) +-->+ (*) +-->+  (*)  +-->+ (1) |
 *  +-----+   +-----+   +------+   +-----+   +-------+   +-----+
 */ 

// includes
#include<string>
#include<iostream>
#include<random>
#include<math.h>
#include<ff/ff.hpp>
#include<windflow.hpp>
#include"mp_common.hpp"

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
    size_t max = 9;
    std::uniform_int_distribution<std::mt19937::result_type> dist6(min, max);
    int filter_degree, flatmap_degree, map_degree, kf_degree;
    size_t source_degree = 1;
    long last_result = 0;
    // executes the runs
    for (size_t i=0; i<runs; i++) {
    	filter_degree = dist6(rng);
    	flatmap_degree = dist6(rng);
    	map_degree = dist6(rng);
    	kf_degree = dist6(rng);
		cout << "Run " << i << endl;
		cout << "+-----+   +-----+   +------+   +-----+   +-------+   +-----+" << endl;
		cout << "|  S  |   |  F  |   |  FM  |   |  M  |   | KF_TB |   |  S  |" << endl;
		cout << "| (" << source_degree << ") +-->+ (" << filter_degree << ") +-->+  (" << flatmap_degree << ") +-->+ (" << map_degree << ") +-->+  (" << kf_degree << ")  +-->+ (1) |" << endl;
		cout << "+-----+   +-----+   +------+   +-----+   +-------+   +-----+" << endl;
		// prepare the test
	    PipeGraph graph("test_kf_tb", Mode::DETERMINISTIC);
	    // source
	    Source_Functor source_functor(stream_len, n_keys);
	    Source source = Source_Builder(source_functor)
	    					.withName("source")
	    					.withParallelism(source_degree)
	    					.build();
	    MultiPipe &mp = graph.add_source(source);
	    // filter
	    Filter_Functor filter_functor;
	    Filter filter = Filter_Builder(filter_functor)
	    					.withName("filter")
	    					.withParallelism(filter_degree)
	    					.build();
	    mp.chain(filter);
	    // flatmap
	    FlatMap_Functor flatmap_functor;
	    FlatMap flatmap = FlatMap_Builder(flatmap_functor)
	    						.withName("flatmap")
	    						.withParallelism(flatmap_degree)
	    						.build();
	    mp.chain(flatmap);
	    // map
	    Map_Functor map_functor;
	    Map map = Map_Builder(map_functor)
	    				.withName("map")
	    				.withParallelism(map_degree)
	    				.build();
	    mp.chain(map);
	    // kf
	    Key_Farm kf = KeyFarm_Builder(kf_function)
	    					.withName("kf")
	    					.withParallelism(kf_degree)
	    					.withTBWindows(microseconds(win_len), microseconds(win_slide))
	    					.build();
	    mp.add(kf);
	    // sink
	    Sink_Functor sink_functor(n_keys);
	    Sink sink = Sink_Builder(sink_functor)
	    					.withName("sink")
	    					.withParallelism(1)
	    					.build();
	    mp.chain_sink(sink);
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
    }
	return 0;
}
