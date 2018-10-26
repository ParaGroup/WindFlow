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
 *  Version with a Win_Farm for the Skyline operator and a Win_Farm
 *  for the DKM operator.
 */ 

// includes
#include <set>
#include <string>
#include <iostream>
#include <algorithm>
#include <getopt.h>
#include <ff/node.hpp>
#include <ff/mapper.hpp>
#include <ff/pipeline.hpp>
#include <win_farm.hpp>
#include <pane_farm.hpp>
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
	size_t skyline_pardegree = 1;
	static struct option long_opts[] = {
		{"sky-n", 1, nullptr, 'n'},
		{nullptr, 0, nullptr, 0}
	};
	// arguments from command line
	if (argc != 11) {
		cout << argv[0] << " -r [rate tuples/sec] -l [stream_length] -w [win_length ms] -s [slide_length ms] --sky-n [par_degree]" << endl;
		exit(EXIT_SUCCESS);
	}
	while ((option = getopt_long(argc, argv, "r:l:w:s:n:", long_opts, &option_index)) != -1) {
		switch (option) {
			case 'r': rate = atoi(optarg);
					 break;
			case 'l': stream_len = atoi(optarg);
					 break;
			case 'w': win_len = atoi(optarg);
					 break;
			case 's': slide_len = atoi(optarg);
					 break;
			case 'n': skyline_pardegree = atoi(optarg);
					 break;
			default: {
				cout << argv[0] << " -r [rate tuples/sec] -l [stream_length] -w [win_length ms] -s [slide_length ms] --sky-n [par_degree]" << endl;
				exit(EXIT_SUCCESS);
			}
        }
    }
    // check the consistency of the windowing parameters
    if (slide_len > 1000 || 1000 % slide_len != 0) {
    	cout << RED << "Incorrect use of win_len and slide_len parameters for this application" << DEFAULT << endl;
    	exit(1);
    }
    // compute the length of the count-based tumbling window used by the DKM operator
    size_t tumbling_win_len = 1000/slide_len;
    // initialize the startBarrier
    pthread_barrier_init(&startBarrier, NULL, 2);
	// create the pipeline of the application
	ff_pipeline pipe;
	// create the generator of the stream
	SQGenerator *generator = new SQGenerator(rate, stream_len);
	pipe.add_stage(generator);
	// create the first stage (Skyline Operator)
	Win_Seq sky_seq = WinSeq_Builder(SkyLineFunction).withTBWindow(milliseconds(win_len), milliseconds(slide_len))
													 .withName("skyline")
													 .build();
	Win_Farm sky_wf = WinFarm_Builder(SkyLineFunction).withTBWindow(milliseconds(win_len), milliseconds(slide_len))
													  .withParallelism(skyline_pardegree)
													  .withName("skyline")
													  .build();
	if (skyline_pardegree == 1) { // first stage is sequential
		pipe.add_stage(&sky_seq);
	}
	else { // first stage is a Win_Farm	
		pipe.add_stage(&sky_wf);
	}
	// create the consumer stage (printing statistics every second)
	SQPrinter<Skyline> *printer = new SQPrinter<Skyline>(1000, win_len);
	pipe.add_stage(printer);
	pipe.setFixedSize(false); // all the queues are unbounded
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
