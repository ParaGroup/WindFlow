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
 *  Stream printer used by the spatial query application. This class implements a
 *  FastFlow node in charge of receiving results and printing them to the user
 *  with a given output frequency.
 */ 

#ifndef SQ_PRINTER_H
#define SQ_PRINTER_H

// includes
#include <iomanip>
#include <ff/node.hpp>
#include <basic.hpp>

using namespace std;

// global barrier to synchronize the beginning of the execution
extern pthread_barrier_t startBarrier;

// printer class to collect outputs
template<typename result_t>
class SQPrinter: public ff_node_t<result_t>
{
private:
	size_t sample_len_ms; // length of the sampling interval in milliseconds
	size_t warmup_ms; // length of the initial warmup period in milliseconds
	size_t cnt_sample; // number of elapsed samples
	size_t cnt_sample_warmup; // number of elapsed samples after the initial warmup period
	size_t received; // total number of received results
	size_t received_warmup; // total number of received results after the warmup period
	size_t sample_received; // number of results received in the last sample
	double avg_ta_us; // average time between two consecutive results in microseconds
	bool initial_warmup; // flag that is true if we are in the warmup period, false otherwise
	volatile unsigned long last_ta;
	volatile unsigned long last_time;
	volatile unsigned long start_time;
#if defined(LOG_DIR)
	ofstream logfile;
	vector<result_t> results;
#endif

public:
	// constructor
	SQPrinter(size_t _sample_len_ms, size_t _warmup_ms):
	          sample_len_ms(_sample_len_ms),
	          warmup_ms(_warmup_ms),
	          cnt_sample(0),
	          cnt_sample_warmup(0),
	          received(0),
	          received_warmup(0),
	          sample_received(0),
	          avg_ta_us(0),
	          initial_warmup(true) {}

	// destructor
	~SQPrinter() {}

	// svc_init method (utilized by the FastFlow runtime)
	int svc_init()
	{
#if defined(LOG_DIR)
        string filename = string(STRINGIFY(LOG_DIR)) + "/output.log";
        logfile.open(filename);
#endif
		// entering the startBarrier
		pthread_barrier_wait(&startBarrier);
		start_time = current_time_usecs();
		last_time = start_time;
		return 0;
	}

	// svc method (utilized by the FastFlow runtime)
	result_t *svc(result_t *r) {
		if (received != std::get<1>(r->getControlFields())) {
			cout << RED << "Result received out-of-order: something is wrong!" << DEFAULT << endl;
			exit(1);
		}
		received++;
		sample_received++;
		unsigned long current_time = current_time_usecs();
		unsigned long elapsed_time = current_time - start_time;
		// check if the warmup period is over
		if (initial_warmup && (((double) elapsed_time)/1000 > warmup_ms)) {
			initial_warmup = false;
			last_ta = current_time_usecs();
		}
		// get the inter-arrival time statistics (if the warmup period is over)
		if (!initial_warmup) {
			received_warmup++;
			unsigned long elapsed_ta = current_time_usecs() - last_ta;
			avg_ta_us += (1.0 / received_warmup) * (((double) elapsed_ta) - avg_ta_us);
			last_ta = current_time_usecs();
		}
		// check if a sample is complete
		if(current_time - last_time >= sample_len_ms * 1000) {
			cnt_sample++;
			cout << "->Time: " << setprecision(5) << ((double) elapsed_time)/1000000 << "sec received [" << sample_received << "] results" << endl;
			sample_received = 0;
			last_time = current_time_usecs();
		}
#if defined(LOG_DIR)
		results.push_back(*r);
#endif
		delete r;
		return ff_node_t<result_t>::GO_ON;
	}

    // svc_end method (utilized by the FastFlow runtime)
    void svc_end()
    {
    	// final print
		unsigned long elapsed_time = current_time_usecs() - start_time;
		cout << "->Time: " << setprecision(5) << ((double) elapsed_time)/1000000 << "sec received [" << sample_received << "] results" << endl;
    	// print summary of statistics
    	cout << endl;
    	cout << CYAN << "*******************SUMMARY*******************" << DEFAULT << endl;
    	cout << CYAN << "Received results: " << received << DEFAULT << endl;
    	cout << CYAN << "Inter-arrival time: " << avg_ta_us << " usec" << DEFAULT << endl;
    	cout << CYAN << "Output rate: " << 1000/avg_ta_us << " results/sec" << DEFAULT << endl;
    	cout << CYAN << "*********************************************" << DEFAULT << endl;
#if defined(LOG_DIR)/*
    	// print log file of results
    	ostringstream stream;
    	size_t wid = 0;
    	for (auto &r: results) {
    		vector<array<float, DIM>> a = r.getCentroids();
    		stream << "Window " << wid++ << " centroids {";
    		for (size_t i=0; i<N_CENTROIDS-1; i++) {
    			stream << "[";
    			for (size_t j=0; j<DIM-1; j++)
    				stream << a[i][j] << ", ";
    			stream << a[i][DIM-1] << "], ";
    		}
    		stream << "[";
    		for (size_t j=0; j<DIM-1; j++)
    			stream << a[N_CENTROIDS-1][j] << ", ";
    		stream << a[N_CENTROIDS-1][DIM-1] << "]}" << endl;
    	}
    	logfile << stream.str();
    	logfile.close();*/
    	long sum_size = 0;
    	for(auto &s: results)
    		sum_size += s.size();
    	cout << "Average skyline size: " << ((double) sum_size) / results.size() << endl;
#endif
    }
};

#endif
