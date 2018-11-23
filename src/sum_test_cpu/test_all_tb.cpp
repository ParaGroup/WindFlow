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
 *  Inc., 59 Temple Place - Suite 330, Boston, MAff_Pipe 02111-1307, USA.
 ******************************************************************************
 */

/*  
 *  Suite of tests of the sum query with all the possible compositions and nestings
 *  of patterns instantiated with time-based windows.
 */	

// includes
#include <string>
#include <iostream>
#include <ff/mapper.hpp>
#include <ff/pipeline.hpp>
#include <win_seq.hpp>
#include <win_mapreduce.hpp>
#include <win_farm.hpp>
#include <key_farm.hpp>
#include <pane_farm.hpp>
#include <sum_tb.hpp>

using namespace ff;
using namespace std;

// main
int main(int argc, char *argv[])
{
	int option = 0;
	size_t stream_len = 0;
	size_t win_len = 0;
	size_t win_slide = 0;
	size_t num_keys = 1;
	size_t emitter_degree = 1;
	size_t degree1 = 1;
	size_t degree2 = 1;
	size_t degree3 = 1;
	unsigned long value = 0;
	opt_level_t opt_level = LEVEL0;
	// arguments from command line
	if (argc != 19) {
		cout << argv[0] << " -l [stream_length] -k [num keys] -w [win length usec] -s [win slide usec] -e [pardegree] -r [pardegree] -n [pardegree] -m [pardegree] -o [opt_level]" << endl;
		exit(EXIT_SUCCESS);
	}
	while ((option = getopt(argc, argv, "l:k:w:s:e:r:n:m:o:")) != -1) {
		switch (option) {
			case 'l': stream_len = atoi(optarg);
					 break;
			case 'k': num_keys = atoi(optarg);
					 break;
			case 'w': win_len = atoi(optarg);
					 break;
			case 's': win_slide = atoi(optarg);
					 break;
			case 'e': emitter_degree = atoi(optarg);
					 break;						 
			case 'r': degree1 = atoi(optarg);
					 break;					 
			case 'n': degree2 = atoi(optarg);
					 break;
			case 'm': degree3 = atoi(optarg);
					 break;
			case 'o': {
					 int level = atoi(optarg);
					 if (level == 0) opt_level = LEVEL0;
					 if (level == 1) opt_level = LEVEL1;
					 if (level == 2) opt_level = LEVEL2;
					 break;
			}
			default: {
		cout << argv[0] << " -l [stream_length] -k [num keys] -w [win length usec] -s [win slide usec] -e [pardegree] -r [pardegree] -n [pardegree] -m [pardegree] -o [opt_level]" << endl;
				exit(EXIT_SUCCESS);
			}
        }
    }
	// user-defined window function (Non-Incremental Query)
	auto seqFNIC = [](size_t key, size_t wid, Iterable<tuple_t> &input, output_t &result) {
		long sum = 0;
		// print the window content
		string win = string("Key: ") + to_string(key) + " window " + to_string(wid) + " [";
		for (auto t : input) {
			int val = t.value;
			win += to_string(val) + ", ";
			sum += val;
		}
		win = win + "] -> Sum "+ to_string(sum);
		result.value = sum;
		return 0;
	};
	// user-defined window function (Incremental Query)
	auto seqFINC = [](size_t key, size_t wid, const tuple_t &t, output_t &result) {
		result.value += t.value;
		return 0;
	};
	// user-defined pane function (Non-Incremental Query)
	auto plqFNIC = [](size_t key, size_t pid, Iterable<tuple_t> &input, output_t &pane_result) {
		long sum = 0;
		// print the window content
		string pane = string("Key: ") + to_string(key) + " pane " + to_string(pid) + " [";
		for (auto t : input) {
			int val = t.value;
			pane += to_string(val) + ", ";
			sum += val;
		}
		pane = pane + "] -> Sum "+ to_string(sum);
		pane_result.value = sum;
		return 0;
	};
	// user-defined window function (Non-Incremental Query)
	auto wlqFNIC = [](size_t key, size_t wid, Iterable<output_t> &input, output_t &win_result) {
		long sum = 0;
		// print the window content
		string pane = string("Key: ") + to_string(key) + " pane " + to_string(wid) + " [";
		for (auto t : input) {
			int val = t.value;
			pane += to_string(val) + ", ";
			sum += val;
		}
		pane = pane + "] -> Sum "+ to_string(sum);
		win_result.value = sum;
		return 0;
	};
    // user-defined pane function (Incremental Query)
	auto plqFINC = [](size_t key, size_t pid, const tuple_t &t, output_t &pane_result) {
		pane_result.value += t.value;
		return 0;
	};
    // user-defined window function (Incremental Query)
	auto wlqFINC = [](size_t key, size_t wid, const output_t &r, output_t &win_result) {
		win_result.value += r.value;
		return 0;
	};
	// user-defined map and reduce functions (Non-Incremental Query)
	auto wmFNIC = [](size_t key, size_t wid, Iterable<tuple_t> &input, tuple_t &win_result) {
		long sum = 0;
		// print the window content
		string window = string("Key: ") + to_string(key) + " window " + to_string(wid) + " [";
		for (auto t : input) {
			int val = t.value;
			window += to_string(val) + ", ";
			sum += val;
		}
		window = window + "] -> Sum "+ to_string(sum);
		win_result.value = sum;
		return 0;
	};
	// user-defined map and reduce functions (Incremental Query)
	auto wmFINC = [](size_t key, size_t wid, const tuple_t &t, tuple_t &win_result) {
		win_result.value += t.value;
		return 0;
	};
	// function routingF used by all the Key_Farm instances
	auto routingF = [](size_t k, size_t n) { return k%n; };

	// Test 1 SEQ(NIC)
	{
		Generator generator(stream_len, num_keys);
		Consumer consumer(num_keys);
		Win_Seq seqNIC = WinSeq_Builder(seqFNIC).withTBWindow(microseconds(win_len), microseconds(win_slide))
											   .withName("seq_nic")
											   .build();
		ff_Pipe<tuple_t, output_t> pipe1(generator, seqNIC, consumer);
		cout << "Run Test 1 SEQ(NIC): number of threads " << pipe1.cardinality() << endl;
		pipe1.run_and_wait_end();
		value = consumer.getTotalSum();
		cout << "Done" << GREEN << " OK!" << DEFAULT << endl;
	}

	// Test 2 SEQ(INC)
	{
		Generator generator(stream_len, num_keys);
		Consumer consumer(num_keys);
		Win_Seq seqINC = WinSeq_Builder(seqFINC).withTBWindow(microseconds(win_len), microseconds(win_slide))
											   .withName("seq_inc")
											   .build();
		ff_Pipe<tuple_t, output_t> pipe2(generator, seqINC, consumer);
		cout << "Run Test 2 SEQ(INC): number of threads " << pipe2.cardinality() << endl;
		pipe2.run_and_wait_end();
		if (value == consumer.getTotalSum())
			cout << "Done" << GREEN << " OK!" << DEFAULT << endl;
		else {
			cout << RED << " Error!!" << DEFAULT << endl;
			exit(1);
		}
	}

	// Test 3 WF(SEQ(NIC))
	{
		Generator generator(stream_len, num_keys);
		Consumer consumer(num_keys);
		Win_Farm wfNIC = WinFarm_Builder(seqFNIC).withTBWindow(microseconds(win_len), microseconds(win_slide))
											   .withParallelism(degree1)
											   .withName("wf_nic")
											   .withEmitters(emitter_degree)
											   .build();
		ff_Pipe<tuple_t, output_t> pipe3(generator, wfNIC, consumer);
		cout << "Run Test 3 WF(SEQ(NIC)): number of threads " << pipe3.cardinality() << endl;
		pipe3.run_and_wait_end();
		if (value == consumer.getTotalSum())
			cout << "Done" << GREEN << " OK!" << DEFAULT << endl;
		else {
			cout << RED << " Error!!" << DEFAULT << endl;
			exit(1);
		}
	}

	// Test 4 WF(SEQ(INC))
	{
		Generator generator(stream_len, num_keys);
		Consumer consumer(num_keys);
		Win_Farm wfINC = WinFarm_Builder(seqFINC).withTBWindow(microseconds(win_len), microseconds(win_slide))
											   .withParallelism(degree1)
											   .withName("wf_inc")
											   .withEmitters(emitter_degree)
											   .build();
		ff_Pipe<tuple_t, output_t> pipe4(generator, wfINC, consumer);
		cout << "Run Test 4 WF(SEQ(INC)): number of threads " << pipe4.cardinality() << endl;
		pipe4.run_and_wait_end();
		if (value == consumer.getTotalSum())
			cout << "Done" << GREEN << " OK!" << DEFAULT << endl;
		else {
			cout << RED << " Error!!" << DEFAULT << endl;
			exit(1);
		}
	}

	// Test 5 KF(SEQ(NIC))
	{
		Generator generator(stream_len, num_keys);
		Consumer consumer(num_keys);
		Key_Farm kfNIC = KeyFarm_Builder(seqFNIC).withTBWindow(microseconds(win_len), microseconds(win_slide))
											   .withParallelism(degree1)
											   .withName("kf_nic")
											   .build();
		ff_Pipe<tuple_t, output_t> pipe5(generator, kfNIC, consumer);
		cout << "Run Test 5 KF(SEQ(NIC)): number of threads " << pipe5.cardinality() << endl;
		pipe5.run_and_wait_end();
		if (value == consumer.getTotalSum())
			cout << "Done" << GREEN << " OK!" << DEFAULT << endl;
		else {
			cout << RED << " Error!!" << DEFAULT << endl;
			exit(1);
		}
	}

	// Test 6 KF(SEQ(INC))
	{
		Generator generator(stream_len, num_keys);
		Consumer consumer(num_keys);
		Key_Farm kfINC = KeyFarm_Builder(seqFINC).withTBWindow(microseconds(win_len), microseconds(win_slide))
											   .withParallelism(degree1)
											   .withName("kf_inc")
											   .build();
		ff_Pipe<tuple_t, output_t> pipe6(generator, kfINC, consumer);
		cout << "Run Test 6 KF(SEQ(INC)): number of threads " << pipe6.cardinality() << endl;
		pipe6.run_and_wait_end();
		if (value == consumer.getTotalSum())
			cout << "Done" << GREEN << " OK!" << DEFAULT << endl;
		else {
			cout << RED << " Error!!" << DEFAULT << endl;
			exit(1);
		}
	}

	// Test 7 PF(PLQ(NIC), WLQ(NIC))
	{
		Generator generator(stream_len, num_keys);
		Consumer consumer(num_keys);
		Pane_Farm pfNICNIC = PaneFarm_Builder(plqFNIC, wlqFNIC).withTBWindow(microseconds(win_len), microseconds(win_slide))
											   .withParallelism(degree2, degree3)
											   .withName("pf_nic_nic")
											   .withOpt(opt_level)
											   .build();
		ff_Pipe<tuple_t, output_t> pipe7(generator, pfNICNIC, consumer);
		cout << "Run Test 7 PF(PLQ(NIC), WLQ(NIC)): number of threads " << pipe7.cardinality() << endl;
		pipe7.run_and_wait_end();
		if (value == consumer.getTotalSum())
			cout << "Done" << GREEN << " OK!" << DEFAULT << endl;
		else {
			cout << RED << " Error!!" << DEFAULT << endl;
			exit(1);
		}
	}

	// Test 8 PF(PLQ(NIC), WLQ(INC))
	{
		Generator generator(stream_len, num_keys);
		Consumer consumer(num_keys);
		Pane_Farm pfNICINC = PaneFarm_Builder(plqFNIC, wlqFINC).withTBWindow(microseconds(win_len), microseconds(win_slide))
											   .withParallelism(degree2, degree3)
											   .withName("pf_nic_inc")
											   .withOpt(opt_level)
											   .build();
		ff_Pipe<tuple_t, output_t> pipe8(generator, pfNICINC, consumer);
		cout << "Run Test 8 PF(PLQ(NIC), WLQ(INC)): number of threads " << pipe8.cardinality() << endl;
		pipe8.run_and_wait_end();
		if (value == consumer.getTotalSum())
			cout << "Done" << GREEN << " OK!" << DEFAULT << endl;
		else {
			cout << RED << " Error!!" << DEFAULT << endl;
			exit(1);
		}
	}

	// Test 9 PF(PLQ(INC), WLQ(NIC))
	{
		Generator generator(stream_len, num_keys);
		Consumer consumer(num_keys);
		Pane_Farm pfINCNIC = PaneFarm_Builder(plqFINC, wlqFNIC).withTBWindow(microseconds(win_len), microseconds(win_slide))
											   .withParallelism(degree2, degree3)
											   .withName("pf_inc_nic")
											   .withOpt(opt_level)
											   .build();
		ff_Pipe<tuple_t, output_t> pipe9(generator, pfINCNIC, consumer);
		cout << "Run Test 9 PF(PLQ(INC), WLQ(NIC)): number of threads " << pipe9.cardinality() << endl;
		pipe9.run_and_wait_end();
		if (value == consumer.getTotalSum())
			cout << "Done" << GREEN << " OK!" << DEFAULT << endl;
		else {
			cout << RED << " Error!!" << DEFAULT << endl;
			exit(1);
		}
	}

	// Test 10 PF(PLQ(INC), WLQ(INC))
	{
		Generator generator(stream_len, num_keys);
		Consumer consumer(num_keys);
		Pane_Farm pfINCINC = PaneFarm_Builder(plqFINC, wlqFINC).withTBWindow(microseconds(win_len), microseconds(win_slide))
											   .withParallelism(degree2, degree3)
											   .withName("pf_inc_inc")
											   .withOpt(opt_level)
											   .build();
		ff_Pipe<tuple_t, output_t> pipe10(generator, pfINCINC, consumer);
		cout << "Run Test 10 PF(PLQ(INC), WLQ(INC)): number of threads " << pipe10.cardinality() << endl;
		pipe10.run_and_wait_end();
		if (value == consumer.getTotalSum())
			cout << "Done" << GREEN << " OK!" << DEFAULT << endl;
		else {
			cout << RED << " Error!!" << DEFAULT << endl;
			exit(1);
		}
	}

	// Test 11 WM(MAP(NIC), REDUCE(NIC))
	if (degree2 > 1) {
		Generator generator(stream_len, num_keys);
		Consumer consumer(num_keys);
		Win_MapReduce wmNICNIC = WinMapReduce_Builder(wmFNIC, wmFNIC).withTBWindow(microseconds(win_len), microseconds(win_slide))
											   .withParallelism(degree2, degree3)
											   .withName("wm_nic_nic")
											   .withOpt(opt_level)
											   .build();
		ff_Pipe<tuple_t, output_t> pipe11(generator, wmNICNIC, consumer);
		cout << "Run Test 11 WM(MAP(NIC), REDUCE(NIC)): number of threads " << pipe11.cardinality() << endl;
		pipe11.run_and_wait_end();
		if (value == consumer.getTotalSum())
			cout << "Done" << GREEN << " OK!" << DEFAULT << endl;
		else {
			cout << RED << " Error!!" << DEFAULT << endl;
			exit(1);
		}
	}

	// Test 12 WM(MAP(NIC), REDUCE(INC))
	if (degree2 > 1) {
		Generator generator(stream_len, num_keys);
		Consumer consumer(num_keys);
		Win_MapReduce wmNICINC = WinMapReduce_Builder(wmFNIC, wmFINC).withTBWindow(microseconds(win_len), microseconds(win_slide))
											   .withParallelism(degree2, degree3)
											   .withName("wm_nic_inc")
											   .withOpt(opt_level)
											   .build();
		ff_Pipe<tuple_t, output_t> pipe12(generator, wmNICINC, consumer);
		cout << "Run Test 12 WM(MAP(NIC), REDUCE(INC)): number of threads " << pipe12.cardinality() << endl;
		pipe12.run_and_wait_end();
		if (value == consumer.getTotalSum())
			cout << "Done" << GREEN << " OK!" << DEFAULT << endl;
		else {
			cout << RED << " Error!!" << DEFAULT << endl;
			exit(1);
		}
	}

	// Test 13 WM(MAP(INC), REDUCE(NIC))
	if (degree2 > 1) {
		Generator generator(stream_len, num_keys);
		Consumer consumer(num_keys);
		Win_MapReduce wmINCNIC = WinMapReduce_Builder(wmFINC, wmFNIC).withTBWindow(microseconds(win_len), microseconds(win_slide))
											   .withParallelism(degree2, degree3)
											   .withName("wm_inc_nic")
											   .withOpt(opt_level)
											   .build();
		ff_Pipe<tuple_t, output_t> pipe13(generator, wmINCNIC, consumer);
		cout << "Run Test 13 WM(MAP(INC), REDUCE(NIC)): number of threads " << pipe13.cardinality() << endl;
		pipe13.run_and_wait_end();
		if (value == consumer.getTotalSum())
			cout << "Done" << GREEN << " OK!" << DEFAULT << endl;
		else {
			cout << RED << " Error!!" << DEFAULT << endl;
			exit(1);
		}
	}

	// Test 14 WM(MAP(INC), REDUCE(INC))
	if (degree2 > 1) {
		Generator generator(stream_len, num_keys);
		Consumer consumer(num_keys);
		Win_MapReduce wmINCINC = WinMapReduce_Builder(wmFINC, wmFINC).withTBWindow(microseconds(win_len), microseconds(win_slide))
											   .withParallelism(degree2, degree3)
											   .withName("wm_inc_inc")
											   .withOpt(opt_level)
											   .build();
		ff_Pipe<tuple_t, output_t> pipe14(generator, wmINCINC, consumer);
		cout << "Run Test 14 WM(MAP(INC), REDUCE(INC)): number of threads " << pipe14.cardinality() << endl;
		pipe14.run_and_wait_end();
		if (value == consumer.getTotalSum())
			cout << "Done" << GREEN << " OK!" << DEFAULT << endl;
		else {
			cout << RED << " Error!!" << DEFAULT << endl;
			exit(1);
		}
	}

	// Test 15 WF(PF(PLQ(NIC), WLQ(NIC)))
	{
		Generator generator(stream_len, num_keys);
		Consumer consumer(num_keys);
		Pane_Farm pfNICNIC = PaneFarm_Builder(plqFNIC, wlqFNIC).withTBWindow(microseconds(win_len), microseconds(win_slide))
											   .withParallelism(degree2, degree3)
											   .withName("pf_nic_nic")
											   .withOpt(opt_level)
											   .build();
		Win_Farm wf = WinFarm_Builder(pfNICNIC).withParallelism(degree1)
											   .withName("wf_pf_nic_nic")
											   .withOpt(opt_level)
											   .build();
		ff_Pipe<tuple_t, output_t> pipe15(generator, wf, consumer);
		cout << "Run Test 15 WF(PF(PLQ(NIC), WLQ(NIC))): number of threads " << pipe15.cardinality() << endl;
		pipe15.run_and_wait_end();
		if (value == consumer.getTotalSum())
			cout << "Done" << GREEN << " OK!" << DEFAULT << endl;
		else {
			cout << RED << " Error!!" << DEFAULT << endl;
			exit(1);
		}
	}

	// Test 16 WF(PF(PLQ(NIC), WLQ(INC)))
	{
		Generator generator(stream_len, num_keys);
		Consumer consumer(num_keys);
		Pane_Farm pfNICINC = PaneFarm_Builder(plqFNIC, wlqFINC).withTBWindow(microseconds(win_len), microseconds(win_slide))
											   .withParallelism(degree2, degree3)
											   .withName("pf_nic_inc")
											   .withOpt(opt_level)
											   .build();
		Win_Farm wf = WinFarm_Builder(pfNICINC).withParallelism(degree1)
											   .withName("wf_pf_nic_inc")
											   .withOpt(opt_level)
											   .build();
		ff_Pipe<tuple_t, output_t> pipe16(generator, wf, consumer);
		cout << "Run Test 16 WF(PF(PLQ(NIC), WLQ(INC))): number of threads " << pipe16.cardinality() << endl;
		pipe16.run_and_wait_end();
		if (value == consumer.getTotalSum())
			cout << "Done" << GREEN << " OK!" << DEFAULT << endl;
		else {
			cout << RED << " Error!!" << DEFAULT << endl;
			exit(1);
		}
	}

	// Test 17 WF(PF(PLQ(INC), WLQ(NIC)))
	{
		Generator generator(stream_len, num_keys);
		Consumer consumer(num_keys);
		Pane_Farm pfINCNIC = PaneFarm_Builder(plqFINC, wlqFNIC).withTBWindow(microseconds(win_len), microseconds(win_slide))
											   .withParallelism(degree2, degree3)
											   .withName("pf_inc_nic")
											   .withOpt(opt_level)
											   .build();
		Win_Farm wf = WinFarm_Builder(pfINCNIC).withParallelism(degree1)
											   .withName("wf_pf_inc_nic")
											   .withOpt(opt_level)
											   .build();
		ff_Pipe<tuple_t, output_t> pipe17(generator, wf, consumer);
		cout << "Run Test 17 WF(PF(PLQ(INC), WLQ(NIC))): number of threads " << pipe17.cardinality() << endl;
		pipe17.run_and_wait_end();
		if (value == consumer.getTotalSum())
			cout << "Done" << GREEN << " OK!" << DEFAULT << endl;
		else {
			cout << RED << " Error!!" << DEFAULT << endl;
			exit(1);
		}
	}

	// Test 18 WF(PF(PLQ(INC), WLQ(INC)))
	{
		Generator generator(stream_len, num_keys);
		Consumer consumer(num_keys);
		Pane_Farm pfINCINC = PaneFarm_Builder(plqFINC, wlqFINC).withTBWindow(microseconds(win_len), microseconds(win_slide))
											   .withParallelism(degree2, degree3)
											   .withName("pf_inc_inc")
											   .withOpt(opt_level)
											   .build();
		Win_Farm wf = WinFarm_Builder(pfINCINC).withParallelism(degree1)
											   .withName("wf_pf_inc_inc")
											   .withOpt(opt_level)
											   .build();
		ff_Pipe<tuple_t, output_t> pipe18(generator, wf, consumer);
		cout << "Run Test 18 WF(PF(PLQ(INC), WLQ(INC))): number of threads " << pipe18.cardinality() << endl;
		pipe18.run_and_wait_end();
		if (value == consumer.getTotalSum())
			cout << "Done" << GREEN << " OK!" << DEFAULT << endl;
		else {
			cout << RED << " Error!!" << DEFAULT << endl;
			exit(1);
		}
	}

	// Test 19 WF(WM(MAP(NIC), REDUCE(NIC)))
	if (degree2 > 1) {
		Generator generator(stream_len, num_keys);
		Consumer consumer(num_keys);
		Win_MapReduce wmNICNIC = WinMapReduce_Builder(wmFNIC, wmFNIC).withTBWindow(microseconds(win_len), microseconds(win_slide))
											   .withParallelism(degree2, degree3)
											   .withName("wm_nic_nic")
											   .withOpt(opt_level)
											   .build();
		Win_Farm wf = WinFarm_Builder(wmNICNIC).withParallelism(degree1)
											   .withName("wf_wm_nic_nic")
											   .withOpt(opt_level)
											   .build();
		ff_Pipe<tuple_t, output_t> pipe19(generator, wf, consumer);
		cout << "Run Test 19 WF(WM(MAP(NIC), REDUCE(NIC))): number of threads " << pipe19.cardinality() << endl;
		pipe19.run_and_wait_end();
		if (value == consumer.getTotalSum())
			cout << "Done" << GREEN << " OK!" << DEFAULT << endl;
		else {
			cout << RED << " Error!!" << DEFAULT << endl;
			exit(1);
		}
	}

	// Test 20 WF(WM(MAP(NIC), REDUCE(INC)))
	if (degree2 > 1) {
		Generator generator(stream_len, num_keys);
		Consumer consumer(num_keys);
		Win_MapReduce wmNICINC = WinMapReduce_Builder(wmFNIC, wmFINC).withTBWindow(microseconds(win_len), microseconds(win_slide))
											   .withParallelism(degree2, degree3)
											   .withName("wm_nic_inc")
											   .withOpt(opt_level)
											   .build();
		Win_Farm wf = WinFarm_Builder(wmNICINC).withParallelism(degree1)
											   .withName("wf_wm_nic_inc")
											   .withOpt(opt_level)
											   .build();
		ff_Pipe<tuple_t, output_t> pipe20(generator, wf, consumer);
		cout << "Run Test 20 WF(WM(MAP(NIC), REDUCE(INC))): number of threads " << pipe20.cardinality() << endl;
		pipe20.run_and_wait_end();
		if (value == consumer.getTotalSum())
			cout << "Done" << GREEN << " OK!" << DEFAULT << endl;
		else {
			cout << RED << " Error!!" << DEFAULT << endl;
			exit(1);
		}
	}

	// Test 21 WF(WM(MAP(INC), REDUCE(NIC)))
	if (degree2 > 1) {
		Generator generator(stream_len, num_keys);
		Consumer consumer(num_keys);
		Win_MapReduce wmINCNIC = WinMapReduce_Builder(wmFINC, wmFNIC).withTBWindow(microseconds(win_len), microseconds(win_slide))
											   .withParallelism(degree2, degree3)
											   .withName("wm_inc_nic")
											   .withOpt(opt_level)
											   .build();
		Win_Farm wf = WinFarm_Builder(wmINCNIC).withParallelism(degree1)
											   .withName("wf_wm_inc_nic")
											   .withOpt(opt_level)
											   .build();
		ff_Pipe<tuple_t, output_t> pipe21(generator, wf, consumer);
		cout << "Run Test 21 WF(WM(MAP(INC), REDUCE(NIC))): number of threads " << pipe21.cardinality() << endl;
		pipe21.run_and_wait_end();
		if (value == consumer.getTotalSum())
			cout << "Done" << GREEN << " OK!" << DEFAULT << endl;
		else {
			cout << RED << " Error!!" << DEFAULT << endl;
			exit(1);
		}
	}

	// Test 22 WF(WM(MAP(INC), REDUCE(INC)))
	if (degree2 > 1) {
		Generator generator(stream_len, num_keys);
		Consumer consumer(num_keys);
		Win_MapReduce wmINCINC = WinMapReduce_Builder(wmFINC, wmFINC).withTBWindow(microseconds(win_len), microseconds(win_slide))
											   .withParallelism(degree2, degree3)
											   .withName("wm_inc_inc")
											   .withOpt(opt_level)
											   .build();
		Win_Farm wf = WinFarm_Builder(wmINCINC).withParallelism(degree1)
											   .withName("wf_wm_inc_inc")
											   .withOpt(opt_level)
											   .build();
		ff_Pipe<tuple_t, output_t> pipe22(generator, wf, consumer);
		cout << "Run Test 22 WF(WM(MAP(INC), REDUCE(INC))): number of threads " << pipe22.cardinality() << endl;
		pipe22.run_and_wait_end();
		if (value == consumer.getTotalSum())
			cout << "Done" << GREEN << " OK!" << DEFAULT << endl;
		else {
			cout << RED << " Error!!" << DEFAULT << endl;
			exit(1);
		}
	}

	// Test 23 KF(PF(PLQ(NIC), WLQ(NIC)))
	{
		Generator generator(stream_len, num_keys);
		Consumer consumer(num_keys);
		Pane_Farm pfNICNIC = PaneFarm_Builder(plqFNIC, wlqFNIC).withTBWindow(microseconds(win_len), microseconds(win_slide))
											   .withParallelism(degree2, degree3)
											   .withName("pf_nic_nic")
											   .withOpt(opt_level)
											   .build();
		Key_Farm kf = KeyFarm_Builder(pfNICNIC).withParallelism(degree1)
											   .withName("kf_pf_nic_nic")
											   .withOpt(opt_level)
											   .build();
		ff_Pipe<tuple_t, output_t> pipe23(generator, kf, consumer);
		cout << "Run Test 23 KF(PF(PLQ(NIC), WLQ(NIC))): number of threads " << pipe23.cardinality() << endl;
		pipe23.run_and_wait_end();
		if (value == consumer.getTotalSum())
			cout << "Done" << GREEN << " OK!" << DEFAULT << endl;
		else {
			cout << RED << " Error!!" << DEFAULT << endl;
			exit(1);
		}
	}

	// Test 24 KF(PF(PLQ(NIC), WLQ(INC)))
	{
		Generator generator(stream_len, num_keys);
		Consumer consumer(num_keys);
		Pane_Farm pfNICINC = PaneFarm_Builder(plqFNIC, wlqFINC).withTBWindow(microseconds(win_len), microseconds(win_slide))
											   .withParallelism(degree2, degree3)
											   .withName("pf_nic_inc")
											   .withOpt(opt_level)
											   .build();
		Key_Farm kf = KeyFarm_Builder(pfNICINC).withParallelism(degree1)
											   .withName("kf_pf_nic_inc")
											   .withOpt(opt_level)
											   .build();
		ff_Pipe<tuple_t, output_t> pipe24(generator, kf, consumer);
		cout << "Run Test 24 KF(PF(PLQ(NIC), WLQ(INC))): number of threads " << pipe24.cardinality() << endl;
		pipe24.run_and_wait_end();
		if (value == consumer.getTotalSum())
			cout << "Done" << GREEN << " OK!" << DEFAULT << endl;
		else {
			cout << RED << " Error!!" << DEFAULT << endl;
			exit(1);
		}
	}

	// Test 25 KF(PF(PLQ(INC), WLQ(NIC)))
	{
		Generator generator(stream_len, num_keys);
		Consumer consumer(num_keys);
		Pane_Farm pfINCNIC = PaneFarm_Builder(plqFINC, wlqFNIC).withTBWindow(microseconds(win_len), microseconds(win_slide))
											   .withParallelism(degree2, degree3)
											   .withName("pf_inc_nic")
											   .withOpt(opt_level)
											   .build();
		Key_Farm kf = KeyFarm_Builder(pfINCNIC).withParallelism(degree1)
											   .withName("kf_pf_inc_nic")
											   .withOpt(opt_level)
											   .build();
		ff_Pipe<tuple_t, output_t> pipe25(generator, kf, consumer);
		cout << "Run Test 25 KF(PF(PLQ(INC), WLQ(NIC))): number of threads " << pipe25.cardinality() << endl;
		pipe25.run_and_wait_end();
		if (value == consumer.getTotalSum())
			cout << "Done" << GREEN << " OK!" << DEFAULT << endl;
		else {
			cout << RED << " Error!!" << DEFAULT << endl;
			exit(1);
		}
	}

	// Test 26 KF(PF(PLQ(INC), WLQ(INC)))
	{
		Generator generator(stream_len, num_keys);
		Consumer consumer(num_keys);
		Pane_Farm pfINCINC = PaneFarm_Builder(plqFINC, wlqFINC).withTBWindow(microseconds(win_len), microseconds(win_slide))
											   .withParallelism(degree2, degree3)
											   .withName("pf_inc_inc")
											   .withOpt(opt_level)
											   .build();
		Key_Farm kf = KeyFarm_Builder(pfINCINC).withParallelism(degree1)
											   .withName("kf_pf_inc_inc")
											   .withOpt(opt_level)
											   .build();
		ff_Pipe<tuple_t, output_t> pipe26(generator, kf, consumer);
		cout << "Run Test 26 KF(PF(PLQ(INC), WLQ(INC))): number of threads " << pipe26.cardinality() << endl;
		pipe26.run_and_wait_end();
		if (value == consumer.getTotalSum())
			cout << "Done" << GREEN << " OK!" << DEFAULT << endl;
		else {
			cout << RED << " Error!!" << DEFAULT << endl;
			exit(1);
		}
		cout << "Final value: " << consumer.getTotalSum() << endl;
	}

	// Test 27 KF(WM(MAP(NIC), REDUCE(NIC)))
	if (degree2 > 1) {
		Generator generator(stream_len, num_keys);
		Consumer consumer(num_keys);
		Win_MapReduce wmNICNIC = WinMapReduce_Builder(wmFNIC, wmFNIC).withTBWindow(microseconds(win_len), microseconds(win_slide))
											   .withParallelism(degree2, degree3)
											   .withName("wm_nic_nic")
											   .withOpt(opt_level)
											   .build();
		Key_Farm kf = KeyFarm_Builder(wmNICNIC).withParallelism(degree1)
											   .withName("kf_wm_nic_nic")
											   .withOpt(opt_level)
											   .build();
		ff_Pipe<tuple_t, output_t> pipe27(generator, kf, consumer);
		cout << "Run Test 27 KF(WM(MAP(NIC), REDUCE(NIC))): number of threads " << pipe27.cardinality() << endl;
		pipe27.run_and_wait_end();
		if (value == consumer.getTotalSum())
			cout << "Done" << GREEN << " OK!" << DEFAULT << endl;
		else {
			cout << RED << " Error!!" << DEFAULT << endl;
			exit(1);
		}
	}

	// Test 28 KF(WM(MAP(NIC), REDUCE(INC)))
	if (degree2 > 1) {
		Generator generator(stream_len, num_keys);
		Consumer consumer(num_keys);
		Win_MapReduce wmNICINC = WinMapReduce_Builder(wmFNIC, wmFINC).withTBWindow(microseconds(win_len), microseconds(win_slide))
											   .withParallelism(degree2, degree3)
											   .withName("wm_nic_inc")
											   .withOpt(opt_level)
											   .build();
		Key_Farm kf = KeyFarm_Builder(wmNICINC).withParallelism(degree1)
											   .withName("kf_wm_nic_inc")
											   .withOpt(opt_level)
											   .build();
		ff_Pipe<tuple_t, output_t> pipe28(generator, kf, consumer);
		cout << "Run Test 28 KF(WM(MAP(NIC), REDUCE(INC))): number of threads " << pipe28.cardinality() << endl;
		pipe28.run_and_wait_end();
		if (value == consumer.getTotalSum())
			cout << "Done" << GREEN << " OK!" << DEFAULT << endl;
		else {
			cout << RED << " Error!!" << DEFAULT << endl;
			exit(1);
		}
	}

	// Test 29 KF(WM(MAP(INC), REDUCE(NIC)))
	if (degree2 > 1) {
		Generator generator(stream_len, num_keys);
		Consumer consumer(num_keys);
		Win_MapReduce wmINCNIC = WinMapReduce_Builder(wmFINC, wmFNIC).withTBWindow(microseconds(win_len), microseconds(win_slide))
											   .withParallelism(degree2, degree3)
											   .withName("wm_inc_nic")
											   .withOpt(opt_level)
											   .build();
		Key_Farm kf = KeyFarm_Builder(wmINCNIC).withParallelism(degree1)
											   .withName("kf_wm_inc_nic")
											   .withOpt(opt_level)
											   .build();
		ff_Pipe<tuple_t, output_t> pipe29(generator, kf, consumer);
		cout << "Run Test 29 KF(WM(MAP(INC), REDUCE(NIC))): number of threads " << pipe29.cardinality() << endl;
		pipe29.run_and_wait_end();
		if (value == consumer.getTotalSum())
			cout << "Done" << GREEN << " OK!" << DEFAULT << endl;
		else {
			cout << RED << " Error!!" << DEFAULT << endl;
			exit(1);
		}
	}

	// Test 30 KF(WM(MAP(INC), REDUCE(INC)))
	if (degree2 > 1) {
		Generator generator(stream_len, num_keys);
		Consumer consumer(num_keys);
		Win_MapReduce wmINCINC = WinMapReduce_Builder(wmFINC, wmFINC).withTBWindow(microseconds(win_len), microseconds(win_slide))
											   .withParallelism(degree2, degree3)
											   .withName("wm_inc_inc")
											   .withOpt(opt_level)
											   .build();
		Key_Farm kf = KeyFarm_Builder(wmINCINC).withParallelism(degree1)
											   .withName("kf_wm_inc_inc")
											   .withOpt(opt_level)
											   .build();
		ff_Pipe<tuple_t, output_t> pipe30(generator, kf, consumer);
		cout << "Run Test 30 KF(WM(MAP(INC), REDUCE(INC))): number of threads " << pipe30.cardinality() << endl;
		pipe30.run_and_wait_end();
		if (value == consumer.getTotalSum())
			cout << "Done" << GREEN << " OK!" << DEFAULT << endl;
		else {
			cout << RED << " Error!!" << DEFAULT << endl;
			exit(1);
		}
	}

	cout << "All tests done correctly!" << endl;
	return 0;
}
