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
 *  Test of the Generator (Spatial Query Application)
 *  
 *  This file implements a simple test to check the maximum rate of the generator.
 */ 

// includes
#include <iomanip>
#include <iostream>
#include <getopt.h>
#include <ff/node.hpp>
#include <ff/mapper.hpp>
#include <ff/pipeline.hpp>
#include "tuple_t.hpp"
#include "sq_generator.hpp"

using namespace ff;
using namespace std;

// global barrier to synchronize the beginning of the execution
pthread_barrier_t startBarrier;

// client class
class Client: public ff_node_t<tuple_t>
{
private:
	size_t cnt_sample; // number of elapsed samples
	size_t received; // total number of received results
	size_t sample_received; // number of results received in the last sample
	volatile unsigned long last_time;
	volatile unsigned long start_time;

public:
	// constructor
	Client(): cnt_sample(0),
	          received(0),
	          sample_received(0) {}

	// destructor
	~Client() {}

	// svc_init method (utilized by the FastFlow runtime)
	int svc_init()
	{
		// entering the startBarrier
		pthread_barrier_wait(&startBarrier);
		last_time = current_time_usecs();
		start_time = last_time;
		return 0;
	}

	// svc method (utilized by the FastFlow runtime)
	tuple_t *svc(tuple_t *r) {
		received++;
		sample_received++;
		unsigned long current_time = current_time_usecs();
		unsigned long elapsed_time = current_time - start_time;
		if(current_time - last_time >= 1000000) {
			cnt_sample++;
			cout << "->Time: " << setprecision(5) << ((double) elapsed_time)/1000000 << "sec received [" << sample_received << "] results" << endl;
			sample_received = 0;
			last_time = current_time_usecs();
		}
		delete r;
		return GO_ON;
	}

    // svc_end method (utilized by the FastFlow runtime)
    void svc_end()
    {
    	cout << "Received results: " << received << endl;
    }
};

// main
int main(int argc, char *argv[])
{
	int option = 0;
	size_t rate = 1;
	size_t stream_len = 1;
	// arguments from command line
	if (argc != 5) {
		cout << argv[0] << " -r [rate tuples/sec] -l [stream_length]" << endl;
		exit(EXIT_SUCCESS);
	}
	while ((option = getopt(argc, argv, "r:l:")) != -1) {
		switch (option) {
			case 'r': rate = atoi(optarg);
					 break;
			case 'l': stream_len = atoi(optarg);
					 break;
			default: {
				cout << argv[0] << " -r [rate tuples/sec] -l [stream_length]" << endl;
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
	// create the client
	Client *client = new Client();
	pipe.add_stage(client);
	// run the application
	cout << BOLDCYAN << "Starting generation test..." << DEFAULT << endl << endl;
	if (pipe.run_and_wait_end() < 0) {
		cerr << "Error execution of ff_pipe" << endl;
		return -1;
	}
	else {
		cout << BOLDCYAN << "...end" << DEFAULT << endl;
		return 0;
	}
	return 0;
}
