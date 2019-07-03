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
 *  Spatial Query Application
 *  
 *  Version where the Skyline operator is implemented by a Pane_Farm pattern.
 */ 

// includes
#include <set>
#include <string>
#include <iostream>
#include <algorithm>
#include <getopt.h>
#include <ff/ff.hpp>
#include <windflow.hpp>
#include "tuple_t.hpp"
#include "skytree.hpp"
#include "dkm.hpp"
#include "sq_printer.hpp"
#include "sq_generator.hpp"

using namespace ff;
using namespace std;

// global barrier to synchronize the beginning of the execution
pthread_barrier_t startBarrier;

// main
int main(int argc, char *argv[])
{
	int option = 0;
	int option_index = 0;
	size_t rate = 1;
	size_t stream_len = 1;
	size_t win_len = 1;
	size_t slide_len = 1;
	size_t skyline_plq_degree = 1;
	size_t skyline_wlq_degree = 1;
	opt_level_t opt_level = LEVEL0;
	static struct option long_opts[] = {
		{"sky-plq-n", 1, nullptr, 'n'},
		{"sky-wlq-n", 1, nullptr, 'm'},
		{"opt", 1, nullptr, 'o'},
		{nullptr, 0, nullptr, 0}
	};
	// arguments from command line
	if (argc != 15) {
		cout << argv[0] << " -r [rate tuples/sec] -l [stream_length] -w [win_length ms] -s [slide_length ms] --sky-plq-n [par_degree] --sky-wlq-n [par_degree] --opt [level]" << endl;
		exit(EXIT_SUCCESS);
	}
	while ((option = getopt_long(argc, argv, "r:l:w:s:n:m:o:", long_opts, &option_index)) != -1) {
		switch (option) {
			case 'r': rate = atoi(optarg);
					 break;
			case 'l': stream_len = atoi(optarg);
					 break;
			case 'w': win_len = atoi(optarg);
					 break;
			case 's': slide_len = atoi(optarg);
					 break;
			case 'n': skyline_plq_degree = atoi(optarg);
					 break;
			case 'm': skyline_wlq_degree = atoi(optarg);
					 break;
			case 'o': {
					 int level = atoi(optarg);
					 if (level == 0) opt_level = LEVEL0;
					 if (level == 1) opt_level = LEVEL1;
					 if (level == 2) opt_level = LEVEL2;
					 break;
			}
			default: {
				cout << argv[0] << " -r [rate tuples/sec] -l [stream_length] -w [win_length ms] -s [slide_length ms] --sky-plq-n [par_degree] --sky-wlq-n [par_degree] --opt [level]" << endl;
				exit(EXIT_SUCCESS);
			}
        }
    }
    // initialize the startBarrier
    pthread_barrier_init(&startBarrier, NULL, 2);
	// create the pipeline of the application
	ff_pipeline pipe;
	// create the generator of the stream
	SQGenerator *generator = new SQGenerator(rate, stream_len);
	pipe.add_stage(generator);
	// create the first stage (Skyline Operator)
	Pane_Farm sky_pf = PaneFarm_Builder(SkyLineFunction, SkyLineMergeNIC).withTBWindows(milliseconds(win_len), milliseconds(slide_len))
					   													 .withParallelism(skyline_plq_degree, skyline_wlq_degree)
					   													 .withName("skyline")
					   													 .withOptLevel(opt_level)
					   													 .build();
	pipe.add_stage(&sky_pf);
	// create the consumer stage (printing statistics every second)
	SQPrinter<Skyline> *printer = new SQPrinter<Skyline>(1000, win_len);
	pipe.add_stage(printer);
	// run the application
	cout << BOLDCYAN << "Starting Spatial Query Application (" << pipe.cardinality() << " threads)" << DEFAULT << endl;
	if (pipe.run_and_wait_end() < 0) {
		cerr << "Error execution of ff_pipe" << endl;
		return -1;
	}
	else {
		cout << BOLDCYAN << "...end" << DEFAULT << endl;
		//pipe.ffStats(cout);
		return 0;
	}
}
