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
 *  Test application of the Yahoo! Streaming Benchmark (WindFlow version)
 *  
 *  The application is a pipeline of six stages:
 *  EventSource (generator of events at full speed)
 *  Filter
 *  Project
 *  Join
 *  Window Aggregate (implemented by a Win_MapReduce instance)
 *  Sink
 */ 

// include
#include <iostream>
#include <iterator>
#include <ff/ff.hpp>
#include <windflow.hpp>
#include <yahoo_app.hpp>
#include <ysb_nodes.hpp>
#include <campaign_generator.hpp>

// global variable: starting time of the execution
extern volatile unsigned long start_time_usec;

// global variable: number of generated events
extern atomic<int> sentCounter;

// global variable: number of received results
extern atomic<int> rcvResults;

// global variable: sum of the latency values
extern atomic<long> latency_sum;

// global variable: vector of latency result and its mutex
extern vector<long> latency_values;
extern mutex mutex_latency;

// main
int main(int argc, char *argv[])
{
	int option = 0;
	unsigned long exec_time_sec = 0;
	size_t pardegree1 = 1;
    size_t pardegree2 = 1;
    // initialize global variables
    sentCounter = 0;
    rcvResults = 0;
    latency_sum = 0;
	// arguments from command line
	if (argc != 7) {
		cout << argv[0] << " -l [execution_seconds] -n [par_degree] -m [par_degree]" << endl;
		exit(EXIT_SUCCESS);
	}
	while ((option = getopt(argc, argv, "l:n:m:")) != -1) {
		switch (option) {
			case 'l': exec_time_sec = atoi(optarg);
					 break;
			case 'n': pardegree1 = atoi(optarg);
					 break;
            case 'm': pardegree2 = atoi(optarg);
                     break;
			default: {
				cout << argv[0] << " -l [execution_seconds] -n [par_degree] -m [par_degree]" << endl;
				exit(EXIT_SUCCESS);
			}
        }
    }
    // create the campaigns
    CampaignGenerator campaign_gen;
    // create the application pipeline
    MultiPipe application("ysb");
    // create source operator
    YSBSource source_functor(exec_time_sec, campaign_gen.getArrays(), campaign_gen.getAdsCompaign());
    Source source = Source_Builder(source_functor).withName("ysb_source").withParallelism(pardegree1).build();
    // create filter operator
    YSBFilter filter_functor;
    Filter filter = Filter_Builder(filter_functor).withName("ysb_filter").withParallelism(pardegree1).build();
    YSBJoin join_functor(campaign_gen.getHashMap(), campaign_gen.getRelationalTable());
    FlatMap join = FlatMap_Builder(join_functor).withName("ysb_join").withParallelism(pardegree1).build();
    // create the aggregation operator
    Win_MapReduce aggregation = WinMapReduce_Builder(aggregateFunctionINC, reduceFunctionINC).withTBWindow(seconds(10), seconds(10)).withName("ysb_wmr").withParallelism(pardegree2, 1).build();
    // create the sink operator
    YSBSink sink_functor;
    Sink sink = Sink_Builder(sink_functor).withName("ysb_sink").withParallelism(1).build();
    // set the starting time of the application
    volatile unsigned long start_time_main_us = current_time_usecs();
    start_time_usec = start_time_main_us;
    application.add_source(source);
    application.chain(filter);
    //application.chain(project);
    application.chain(join);
    application.add(aggregation);
    application.chain_sink(sink);
	application.run_and_wait_end();
	volatile unsigned long end_time_main_us = current_time_usecs();
	double elapsed_time_sec = (end_time_main_us - start_time_main_us) / (1000000.0);
    cout << "[Main] Total generated messages are " << sentCounter << endl;
    cout << "[Main] Total received results are " << rcvResults << endl;
    cout << "[Main] Latency (usec) " << latency_sum/rcvResults << endl;
	cout << "[Main] Total elapsed time (seconds) " << elapsed_time_sec << endl;
    // write latencies into a file
    // ofstream out("latencies.log");
    // copy(latency_values.begin(), latency_values.end(), ostream_iterator<long>(out, "\n"));
	return 0;
}
