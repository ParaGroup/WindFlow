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
 *  Stream generator used by the spatial query application. The generator produces
 *  tuples with DIM attributes (floats) whose values are generated uniformly in a
 *  given interval. The inter-arrival time between consecutive input tuples can be
 *  constant or generated according to an exponential distribution (macro EXP).
 *  
 *  The generator is implemented as a FastFlow node in order to be directly used
 *  by the WindFlow application.
 */ 

#ifndef SQ_GEN_H
#define SQ_GEN_H

// includes
#include <ff/node.hpp>
#include <basic.hpp>
#include "random.hpp"

using namespace std;
using namespace chrono;
using namespace ff;
using namespace wf;

// global barrier to synchronize the beginning of the execution
extern pthread_barrier_t startBarrier;

// class implementing the generator node in FastFlow
class SQGenerator: public ff_node_t<tuple_t>
{
private:
	double rate; // input rate in tuples per second
	long num_tuples; // no. of tuples to be generated
	tuple_t **tuples;
	RandomGenerator random;

public:
	// constructor
	SQGenerator(double _rate, long _num_tuples): rate(_rate), num_tuples(_num_tuples), random(1)
	{
#if defined(PREGENERATE)
		// pregenerate all the tuples to save time during the generation loop
		cout << "SQGenerator: starting pre-generation of tuples..." << endl;
		tuples = (tuple_t **) malloc(sizeof(tuple_t *) * num_tuples);
		for (int i=0; i<num_tuples; i++) {
			tuples[i] = (tuple_t *) malloc(sizeof(tuple_t));
			tuples[i]->id = i;
			for (int j=0; j<DIM; j++)
				(tuples[i]->elems)[j] = random.randf() * 1000;
		}
		cout << "...completed!"
#endif
	}

	// destructor
	~SQGenerator()
	{
#if defined(PREGENERATE)
		free(tuples);
#endif
	}

	// svc_init method (utilized by the FastFlow runtime)
	int svc_init() {
		// entering the startBarrier
		pthread_barrier_wait(&startBarrier);
		return 0;
	}

	// svc method (utilized by the FastFlow runtime)
	tuple_t *svc(tuple_t *)
	{
		// set configuration variables to preliminary values
		double avgTA = (((double) 1000000000) / rate); // average inter-arrival time in nanoseconds
		volatile unsigned long start_time = current_time_nsecs();
		volatile unsigned long next_send_time = 0; //the time (in nanoseconds) at which the next tuple has to be sent
		tuple_t *tuple = NULL;
		// start the generation loop
		for (int i=0; i<num_tuples; i++) {
#if !defined(PREGENERATE)
			tuple = new tuple_t();
			tuple->key = 0;
			tuple->id = i;
			for (int j=0; j<DIM; j++)
				(tuple->elems)[j] = random.randf() * 1000;
#endif
			volatile unsigned long end_wait = start_time + next_send_time;
			volatile unsigned long curr_t = current_time_nsecs();
			while (curr_t < end_wait) curr_t = current_time_nsecs(); // waiting loop
#if defined(PREGENERATE)
			tuples[i]->ts = next_send_time/1000; // timestamps are in microseconds in the WindFlow library
			ff_send_out(tuples[i]); // send tuple outside
#else
			tuple->ts = next_send_time/1000; // timestamps are in microseconds in the WindFlow library
			ff_send_out(tuple); // send tuple outside
#endif
			// generate the time for the next tuple
#if !defined(EXP)
			unsigned long offset = avgTA;
#else
			unsigned long offset = random.expntl(avgTA);
#endif
			if(offset < 1000) // since WindFlow allows timestamps in microseconds, 1M tuples/sec is the maximum rate
				offset = 1000;
			next_send_time += offset;
		}
		return EOS;
	}

    // svc_end method (utilized by the FastFlow runtime)
    void svc_end() {}
};

#endif
