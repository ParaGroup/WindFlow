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
 *  Test application of the Yahoo! Streaming Benchmark (FastFlow+WindFlow version)
 *  
 *  The application is a pipeline of six stages:
 *  EventSource (generator of events at full speed)
 *  Filter
 *  Project
 *  Join
 *  Window Aggregate
 *  Sink
 *  
 *  All the stages are sequential (single thread) except the Window_Aggregate that can be
 *  parallel inside.
 */ 

// include
#include <iostream>
#include <ff/ff.hpp>
#include <windflow.hpp>
#include <yahoo_app.hpp>
#include <raw_ysb_nodes_buf.hpp>
#include <campaign_generator.hpp>

// global variable starting time of the execution
extern volatile unsigned long start_time_usec;

// main
int main(int argc, char *argv[])
{
	int option = 0;
	unsigned long exec_time_sec = 0;
	size_t pardegree = 1;
	size_t buffered_cnt = 1;
	// arguments from command line
	if (argc != 7) {
		cout << argv[0] << " -l [execution_seconds] -n [par_degree] -b [buffered_cnt]" << endl;
		exit(EXIT_SUCCESS);
	}
	while ((option = getopt(argc, argv, "l:n:b:")) != -1) {
		switch (option) {
			case 'l': exec_time_sec = atoi(optarg);
					 break;
			case 'n': pardegree = atoi(optarg);
					 break;
			case 'b': buffered_cnt = atoi(optarg);
					 break;
			default: {
				cout << argv[0] << " -l [execution_seconds] -n [par_degree] -b [num_buffered]" << endl;
				exit(EXIT_SUCCESS);
			}
        }
    }
    // create the campaigns
    CampaignGenerator campaign_gen;
    // create the application pipeline
    ff_pipeline pipe;
    // create the first stage (EventSource)
    EventSource source(exec_time_sec, campaign_gen.getArrays(), campaign_gen.getAdsCompaign(), buffered_cnt);
    pipe.add_stage(&source);
    // create the filter operator (Filter)
    RawFilter filter(0, buffered_cnt);
    pipe.add_stage(&filter);
    // create the project operator (Project)
    Project project;
    pipe.add_stage(&project);
    // create the join operator (Join)
    Join join(campaign_gen.getHashMap(), campaign_gen.getRelationalTable());
    pipe.add_stage(&join);
    // create the aggregation operator (WindFlow)
    if (pardegree == 1) {
    	auto *aggregation = WinSeq_Builder(aggregateFunctionINC).withTBWindow(seconds(10), seconds(10)).withName("aggregation_inc").build_ptr(); // incremental version (sequential)
    	pipe.add_stage(aggregation);
    }
    else {
    	auto *aggregation = KeyFarm_Builder(aggregateFunctionINC).withTBWindow(seconds(10), seconds(10)).withName("par_aggregation_inc").withParallelism(pardegree).build_ptr(); // incremental version (parallel)
    	pipe.add_stage(aggregation);
    }
    // create the sink operator
    RawSink sink;
    pipe.add_stage(&sink);
    cout << "Starting ff_pipe with cardinality " << pipe.cardinality() << "..." << endl;
    volatile unsigned long start_time_main_us = current_time_usecs();
    start_time_usec = start_time_main_us;
	if (pipe.run_and_wait_end() < 0) {
		cerr << "Error execution of ff_pipe" << endl;
		return -1;
	}
	else {
		cout << "...end ff_pipe" << endl;
	}
	volatile unsigned long end_time_main_us = current_time_usecs();
	double elapsed_time_sec = (end_time_main_us - start_time_main_us) / (1000000.0);
	cout << "[Main] Total elapsed time (seconds) " << elapsed_time_sec << endl;
	return 0;
}
