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
 *  of GPU patterns instantiated with count-based windows.
 */	

// includes
#include <string>
#include <iostream>
#include <ff/ff.hpp>
#include <windflow.hpp>
#include <windflow_gpu.hpp>
#include <sum_cb.hpp>

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
	size_t batch_len = 1;
	unsigned long value = 0;
	opt_level_t opt_level = LEVEL0;
	// arguments from command line
	if (argc != 21) {
		cout << argv[0] << " -l [stream_length] -k [num keys] -w [win length] -s [win slide] -b [batch_len] -e [pardegree] -r [pardegree] -n [pardegree] -m [pardegree] -o [opt_level]" << endl;
		exit(EXIT_SUCCESS);
	}
	while ((option = getopt(argc, argv, "l:k:w:s:b:e:r:n:m:o:")) != -1) {
		switch (option) {
			case 'l': stream_len = atoi(optarg);
					 break;
			case 'k': num_keys = atoi(optarg);
					 break;
			case 'w': win_len = atoi(optarg);
					 break;
			case 's': win_slide = atoi(optarg);
					 break;
			case 'b': batch_len = atoi(optarg);
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
				cout << argv[0] << " -l [stream_length] -k [num keys] -w [win length] -s [win slide] -b [batch_len] -e [pardegree] -r [pardegree] -n [pardegree] -m [pardegree] -o [opt_level]" << endl;
				exit(EXIT_SUCCESS);
			}
        }
    }
	// user-defined window function (Non-Incremental Query on GPU)
	auto seqFGPU = [] __host__ __device__ (size_t wid, const tuple_t *data, output_t *res, size_t size, char *memory) {
		long sum = 0;
		for (size_t i=0; i<size; i++) {
			sum += data[i].value;
		}
		res->value = sum;
	};
	// user-defined pane function (Non-Incremental Query on GPU)
	auto plqFGPU = [] __host__ __device__ (size_t pid, const tuple_t *data, output_t *res, size_t size, char *memory) {
		long sum = 0;
		for (size_t i=0; i<size; i++) {
			sum += data[i].value;
		}
		res->value = sum;
	};
	// user-defined pane function (Non-Incremental Query on CPU)
	auto plqFNIC = [](size_t pid, Iterable<tuple_t> &input, output_t &pane_result) {
		long sum = 0;
		for (auto t: input) {
			int val = t.value;
			sum += val;
		}
		pane_result.value = sum;
	};
    // user-defined pane function (Incremental Query on CPU)
	auto plqFINC = [](size_t pid, const tuple_t &t, output_t &pane_result) {
		pane_result.value += t.value;
	};
	// user-defined window function (Non-Incremental Query on GPU)
	auto wlqFGPU = [] __host__ __device__ (size_t wid, const output_t *data, output_t *res, size_t size, char *memory) {
		long sum = 0;
		for (size_t i=0; i<size; i++) {
			sum += data[i].value;
		}
		res->value = sum;
	};
	// user-defined window function (Non-Incremental Query on CPU)
	auto wlqFNIC = [](size_t wid, Iterable<output_t> &input, output_t &win_result) {
		long sum = 0;
		for (auto t: input) {
			int val = t.value;
			sum += val;
		}
		win_result.value = sum;
	};
	// user-defined window function (Incremental Query on CPU)
	auto wlqFINC = [](size_t wid, const output_t &r, output_t &win_result) {
		win_result.value += r.value;
	};
	// user-defined map function (Non-Incremental Query on GPU)
	auto mapFGPU = [] __host__ __device__ (size_t wid, const tuple_t *data, tuple_t *res, size_t size, char *memory) {
		long sum = 0;
		for (size_t i=0; i<size; i++) {
			sum += data[i].value;
		}
		res->value = sum;
	};
	// user-defined map function (Non-Incremental Query on CPU)
	auto mapFNIC = [](size_t wid, Iterable<tuple_t> &input, tuple_t &win_result) {
		long sum = 0;
		for (auto t: input) {
			int val = t.value;
			sum += val;
		}
		win_result.value = sum;
	};
	// user-defined map functions (Incremental Query on CPU)
	auto mapFINC = [](size_t wid, const tuple_t &t, tuple_t &win_result) {
		win_result.value += t.value;
	};
	// user-defined reduce function (Non-Incremental Query on GPU)
	auto reduceFGPU = [] __host__ __device__ (size_t wid, const tuple_t *data, tuple_t *res, size_t size, char *memory) {
		long sum = 0;
		for (size_t i=0; i<size; i++) {
			sum += data[i].value;
		}
		res->value = sum;
	};
	// user-defined reduce function (Non-Incremental Query on CPU)
	auto reduceFNIC = [](size_t wid, Iterable<tuple_t> &input, tuple_t &win_result) {
		long sum = 0;
		for (auto t: input) {
			int val = t.value;
			sum += val;
		}
		win_result.value = sum;
	};
	// user-defined reduce functions (Incremental Query on CPU)
	auto reduceFINC = [](size_t wid, const tuple_t &t, tuple_t &win_result) {
		win_result.value += t.value;
	};
	// function routingF used by all the Key_Farm_GPU instances
	auto routingF = [](size_t k, size_t n) { return k%n; };

	// Test 1 SEQ_GPU
	Generator generator1(stream_len, num_keys);
	Consumer consumer1(num_keys);
	auto *seq1 = WinSeqGPU_Builder<decltype(seqFGPU)>(seqFGPU).withCBWindow(win_len, win_slide)
																.withBatch(batch_len)
										   						.withName("seq_gpu")
										   						.build_ptr();
	ff_Pipe<tuple_t, output_t> pipe1(generator1, *seq1, consumer1);
	cout << "Run Test 1 SEQ_GPU: number of threads " << pipe1.cardinality() << endl;
	pipe1.run_and_wait_end();
	value = consumer1.getTotalSum();
	cout << "Done" << GREEN << " OK!" << DEFAULT << endl;

	// Test 2 WF(SEQ_GPU)
	Generator generator2(stream_len, num_keys);
	Consumer consumer2(num_keys);
	auto *wf2 = WinFarmGPU_Builder<decltype(seqFGPU)>(seqFGPU).withCBWindow(win_len, win_slide)
															  .withBatch(batch_len)
										   					  .withParallelism(degree1)
										   					  .withName("wf_gpu")
										   					  .withEmitters(emitter_degree)
										   					  .build_ptr();
	ff_Pipe<tuple_t, output_t> pipe2(generator2, *wf2, consumer2);
	cout << "Run Test 2 WF(SEQ_GPU): number of threads " << pipe2.cardinality() << endl;
	pipe2.run_and_wait_end();
	if (value == consumer2.getTotalSum())
		cout << "Done" << GREEN << " OK!" << DEFAULT << endl;
	else {
		cout << RED << " Error!!" << DEFAULT << endl;
		exit(1);
	}

	// Test 3 KF(SEQ_GPU)
	Generator generator3(stream_len, num_keys);
	Consumer consumer3(num_keys);
	auto *kf3 = KeyFarmGPU_Builder<decltype(seqFGPU)>(seqFGPU).withCBWindow(win_len, win_slide)
																.withBatch(batch_len)
										   					    .withParallelism(degree1)
										   					    .set_KeyBy(routingF)
										   						.withName("kf_gpu")
										   						.build_ptr();
	ff_Pipe<tuple_t, output_t> pipe3(generator3, *kf3, consumer3);
	cout << "Run Test 3 KF(SEQ_GPU): number of threads " << pipe3.cardinality() << endl;
	pipe3.run_and_wait_end();
	if (value == consumer3.getTotalSum())
		cout << "Done" << GREEN << " OK!" << DEFAULT << endl;
	else {
		cout << RED << " Error!!" << DEFAULT << endl;
		exit(1);
	}

	// Test 4 PF(PLQ_GPU, WLQ(NIC))
	Generator generator4(stream_len, num_keys);
	Consumer consumer4(num_keys);
	auto *pf4 = PaneFarmGPU_Builder<decltype(plqFGPU), decltype(wlqFNIC)>(plqFGPU, wlqFNIC).withCBWindow(win_len, win_slide)
																								.withBatch(batch_len)
										   														.withParallelism(degree2, degree3)
										   														.withName("pf_gpu_nic")
										   														.withOptLevel(opt_level)
										   														.build_ptr();
	ff_Pipe<tuple_t, output_t> pipe4(generator4, *pf4, consumer4);
	cout << "Run Test 4 PF(PLQ_GPU, WLQ(NIC)): number of threads " << pipe4.cardinality() << endl;
	pipe4.run_and_wait_end();
	if (value == consumer4.getTotalSum())
		cout << "Done" << GREEN << " OK!" << DEFAULT << endl;
	else {
		cout << RED << " Error!!" << DEFAULT << endl;
		exit(1);
	}

	// Test 5 PF(PLQ_GPU, WLQ(INC))
	Generator generator5(stream_len, num_keys);
	Consumer consumer5(num_keys);
	auto *pf5 = PaneFarmGPU_Builder<decltype(plqFGPU), decltype(wlqFINC)>(plqFGPU, wlqFINC).withCBWindow(win_len, win_slide)
																								.withBatch(batch_len)
										   														.withParallelism(degree2, degree3)
										   														.withName("pf_gpu_inc")
										   														.withOptLevel(opt_level)
										   														.build_ptr();
	ff_Pipe<tuple_t, output_t> pipe5(generator5, *pf5, consumer5);
	cout << "Run Test 5 PF(PLQ_GPU, WLQ(INC)): number of threads " << pipe5.cardinality() << endl;
	pipe5.run_and_wait_end();
	if (value == consumer5.getTotalSum())
		cout << "Done" << GREEN << " OK!" << DEFAULT << endl;
	else {
		cout << RED << " Error!!" << DEFAULT << endl;
		exit(1);
	}

	// Test 6 PF(PLQ(NIC), WLQ_GPU)
	Generator generator6(stream_len, num_keys);
	Consumer consumer6(num_keys);
	auto *pf6 = PaneFarmGPU_Builder<decltype(plqFNIC), decltype(wlqFGPU)>(plqFNIC, wlqFGPU).withCBWindow(win_len, win_slide)
																								.withBatch(batch_len)
										   														.withParallelism(degree2, degree3)
										   														.withName("pf_nic_gpu")
										   														.withOptLevel(opt_level)
										   														.build_ptr();
	ff_Pipe<tuple_t, output_t> pipe6(generator6, *pf6, consumer6);
	cout << "Run Test 6 PF(PLQ(NIC), WLQ_GPU): number of threads " << pipe6.cardinality() << endl;
	pipe6.run_and_wait_end();
	if (value == consumer6.getTotalSum())
		cout << "Done" << GREEN << " OK!" << DEFAULT << endl;
	else {
		cout << RED << " Error!!" << DEFAULT << endl;
		exit(1);
	}

	// Test 7 PF(PLQ(INC), WLQ_GPU)
	Generator generator7(stream_len, num_keys);
	Consumer consumer7(num_keys);
	auto *pf7 = PaneFarmGPU_Builder<decltype(plqFINC), decltype(wlqFGPU)>(plqFINC, wlqFGPU).withCBWindow(win_len, win_slide)
																								.withBatch(batch_len)
										   														.withParallelism(degree2, degree3)
										   														.withName("pf_inc_gpu")
										   														.withOptLevel(opt_level)
										   														.build_ptr();
	ff_Pipe<tuple_t, output_t> pipe7(generator7, *pf7, consumer7);
	cout << "Run Test 7 PF(PLQ(INC), WLQ_GPU): number of threads " << pipe7.cardinality() << endl;
	pipe7.run_and_wait_end();
	if (value == consumer7.getTotalSum())
		cout << "Done" << GREEN << " OK!" << DEFAULT << endl;
	else {
		cout << RED << " Error!!" << DEFAULT << endl;
		exit(1);
	}

	// Test 8 WM(MAP_GPU, REDUCE(NIC))
	Generator generator8(stream_len, num_keys);
	Consumer consumer8(num_keys);
	auto *wm8 = WinMapReduceGPU_Builder<decltype(mapFGPU), decltype(reduceFNIC)>(mapFGPU, reduceFNIC).withCBWindow(win_len, win_slide)
																										  .withBatch(batch_len)
										   																  .withParallelism(max<size_t>(degree2, 2), degree3)
										   															      .withName("wm_gpu_nic")
										   																  .withOptLevel(opt_level)
										   																  .build_ptr();
	ff_Pipe<tuple_t, output_t> pipe8(generator8, *wm8, consumer8);
	if (degree2 > 1) {
		cout << "Run Test 8 WM(MAP_GPU, REDUCE(NIC)): number of threads " << pipe8.cardinality() << endl;
		pipe8.run_and_wait_end();
		if (value == consumer8.getTotalSum())
			cout << "Done" << GREEN << " OK!" << DEFAULT << endl;
		else {
			cout << RED << " Error!!" << DEFAULT << endl;
			exit(1);
		}
	}

	// Test 9 WM(MAP_GPU, REDUCE(INC))
	Generator generator9(stream_len, num_keys);
	Consumer consumer9(num_keys);
	auto *wm9 = WinMapReduceGPU_Builder<decltype(mapFGPU), decltype(reduceFINC)>(mapFGPU, reduceFINC).withCBWindow(win_len, win_slide)
																										  .withBatch(batch_len)
										   																  .withParallelism(max<size_t>(degree2, 2), degree3)
										   																  .withName("wm_gpu_inc")
										   																  .withOptLevel(opt_level)
										   																  .build_ptr();
	ff_Pipe<tuple_t, output_t> pipe9(generator9, *wm9, consumer9);
	if (degree2 > 1) {
		cout << "Run Test 9 WM(MAP_GPU, REDUCE(INC)): number of threads " << pipe9.cardinality() << endl;
		pipe9.run_and_wait_end();
		if (value == consumer9.getTotalSum())
			cout << "Done" << GREEN << " OK!" << DEFAULT << endl;
		else {
			cout << RED << " Error!!" << DEFAULT << endl;
			exit(1);
		}
	}

	// Test 10 WM(MAP(NIC), REDUCE_GPU)
	Generator generator10(stream_len, num_keys);
	Consumer consumer10(num_keys);
	auto *wm10 = WinMapReduceGPU_Builder<decltype(mapFNIC), decltype(reduceFGPU)>(mapFNIC, reduceFGPU).withCBWindow(win_len, win_slide)
																										  .withBatch(batch_len)
										   																  .withParallelism(max<size_t>(degree2, 2), degree3)
										   																  .withName("wm_nic_gpu")
										   															      .withOptLevel(opt_level)
										   																  .build_ptr();
	ff_Pipe<tuple_t, output_t> pipe10(generator10, *wm10, consumer10);
	if (degree2 > 1) {
		cout << "Run Test 10 WM(MAP(NIC), REDUCE_GPU): number of threads " << pipe10.cardinality() << endl;
		pipe10.run_and_wait_end();
		if (value == consumer10.getTotalSum())
			cout << "Done" << GREEN << " OK!" << DEFAULT << endl;
		else {
			cout << RED << " Error!!" << DEFAULT << endl;
			exit(1);
		}
	}

	// Test 11 WM(MAP(INC), REDUCE_GPU)
	Generator generator11(stream_len, num_keys);
	Consumer consumer11(num_keys);
	auto *wm11 = WinMapReduceGPU_Builder<decltype(mapFINC), decltype(reduceFGPU)>(mapFINC, reduceFGPU).withCBWindow(win_len, win_slide)
																										  .withBatch(batch_len)
										   																  .withParallelism(max<size_t>(degree2, 2), degree3)
										   																  .withName("wm_inc_gpu")
										   															      .withOptLevel(opt_level)
										   																  .build_ptr();
	ff_Pipe<tuple_t, output_t> pipe11(generator11, *wm11, consumer11);
	if (degree2 > 1) {
		cout << "Run Test 11 WM(MAP(INC), REDUCE_GPU): number of threads " << pipe11.cardinality() << endl;
		pipe11.run_and_wait_end();
		if (value == consumer11.getTotalSum())
			cout << "Done" << GREEN << " OK!" << DEFAULT << endl;
		else {
			cout << RED << " Error!!" << DEFAULT << endl;
			exit(1);
		}
	}

	// Test 12 WF(PF(PLQ_GPU, WLQ(NIC)))
	Generator generator12(stream_len, num_keys);
	Consumer consumer12(num_keys);
	auto *pf12 = PaneFarmGPU_Builder<decltype(plqFGPU), decltype(wlqFNIC)>(plqFGPU, wlqFNIC).withCBWindow(win_len, win_slide)
																							    .withBatch(batch_len)
										   														.withParallelism(degree2, degree3)
										   														.withName("pf_gpu_nic")
										   														.withOptLevel(opt_level)
										   														.build_ptr();
	auto *wf12 = WinFarmGPU_Builder<decltype(*pf12)>(*pf12).withParallelism(degree1)
										   			 		     .withName("wf_pf_gpu_nic")
										   			 			 .withOptLevel(opt_level)
										   			 			 .build_ptr();
	ff_Pipe<tuple_t, output_t> pipe12(generator12, *wf12, consumer12);
	cout << "Run Test 12 WF(PF(PLQ_GPU, WLQ(NIC))): number of threads " << pipe12.cardinality() << endl;
	pipe12.run_and_wait_end();
	if (value == consumer12.getTotalSum())
		cout << "Done" << GREEN << " OK!" << DEFAULT << endl;
	else {
		cout << RED << " Error!!" << DEFAULT << endl;
		exit(1);
	}

	// Test 13 WF(PF(PLQ_GPU, WLQ(INC)))
	Generator generator13(stream_len, num_keys);
	Consumer consumer13(num_keys);
	auto *fp13 = PaneFarmGPU_Builder<decltype(plqFGPU), decltype(wlqFINC)>(plqFGPU, wlqFINC).withCBWindow(win_len, win_slide)
																								.withBatch(batch_len)
										   														.withParallelism(degree2, degree3)
										   														.withName("pf_gpu_inc")
										   														.withOptLevel(opt_level)
										   														.build_ptr();
	auto *wf13 = WinFarmGPU_Builder<decltype(*fp13)>(*fp13).withParallelism(degree1)
										   			 		     .withName("wf_pf_gpu_inc")
										   			 			 .withOptLevel(opt_level)
										   			 			 .build_ptr();
	ff_Pipe<tuple_t, output_t> pipe13(generator13, *wf13, consumer13);
	cout << "Run Test 13 WF(PF(PLQ_GPU, WLQ(INC))): number of threads " << pipe13.cardinality() << endl;
	pipe13.run_and_wait_end();
	if (value == consumer13.getTotalSum())
		cout << "Done" << GREEN << " OK!" << DEFAULT << endl;
	else {
		cout << RED << " Error!!" << DEFAULT << endl;
		exit(1);
	}

	// Test 14 WF(PF(PLQ(NIC), WLQ_GPU))
	Generator generator14(stream_len, num_keys);
	Consumer consumer14(num_keys);
	auto *pf14 = PaneFarmGPU_Builder<decltype(plqFNIC), decltype(wlqFGPU)>(plqFNIC, wlqFGPU).withCBWindow(win_len, win_slide)
																								.withBatch(batch_len)
										   														.withParallelism(degree2, degree3)
										   														.withName("pf_nic_gpu")
										   														.withOptLevel(opt_level)
										   														.build_ptr();
	auto *wf14 = WinFarmGPU_Builder<decltype(*pf14)>(*pf14).withParallelism(degree1)
										   			 		     .withName("wf_pf_nic_gpu")
										   			 			 .withOptLevel(opt_level)
										   			 			 .build_ptr();
	ff_Pipe<tuple_t, output_t> pipe14(generator14, *wf14, consumer14);
	cout << "Run Test 14 WF(PF(PLQ(NIC), WLQ_GPU)): number of threads " << pipe14.cardinality() << endl;
	pipe14.run_and_wait_end();
	if (value == consumer14.getTotalSum())
		cout << "Done" << GREEN << " OK!" << DEFAULT << endl;
	else {
		cout << RED << " Error!!" << DEFAULT << endl;
		exit(1);
	}

	// Test 15 WF(PF(PLQ(INC), WLQ_GPU))
	Generator generator15(stream_len, num_keys);
	Consumer consumer15(num_keys);
	auto *pf15 = PaneFarmGPU_Builder<decltype(plqFINC), decltype(wlqFGPU)>(plqFINC, wlqFGPU).withCBWindow(win_len, win_slide)
																								.withBatch(batch_len)
										   														.withParallelism(degree2, degree3)
										   														.withName("pf_inc_gpu")
										   														.withOptLevel(opt_level)
										   														.build_ptr();
	auto *wf15 = WinFarmGPU_Builder<decltype(*pf15)>(*pf15).withParallelism(degree1)
										   			 		     .withName("wf_pf_inc_gpu")
										   			 			 .withOptLevel(opt_level)
										   			 			 .build_ptr();
	ff_Pipe<tuple_t, output_t> pipe15(generator15, *wf15, consumer15);
	cout << "Run Test 15 WF(PF(PLQ(INC), WLQ_GPU)): number of threads " << pipe15.cardinality() << endl;
	pipe15.run_and_wait_end();
	if (value == consumer15.getTotalSum())
		cout << "Done" << GREEN << " OK!" << DEFAULT << endl;
	else {
		cout << RED << " Error!!" << DEFAULT << endl;
		exit(1);
	}

	// Test 16 WF(WM(MAP_GPU, REDUCE(NIC)))
	Generator generator16(stream_len, num_keys);
	Consumer consumer16(num_keys);
	auto *wm16 = WinMapReduceGPU_Builder<decltype(mapFGPU), decltype(reduceFNIC)>(mapFGPU, reduceFNIC).withCBWindow(win_len, win_slide)
																										  .withBatch(batch_len)
										   																  .withParallelism(max<size_t>(degree2, 2), degree3)
										   																  .withName("wm_gpu_nic")
										   																  .withOptLevel(opt_level)
										   																  .build_ptr();
	auto *wf16 = WinFarmGPU_Builder<decltype(*wm16)>(*wm16).withParallelism(degree1)
										   			 		     .withName("wf_wm_gpu_nic")
										   			 			 .withOptLevel(opt_level)
										   			 			 .build_ptr();
	ff_Pipe<tuple_t, output_t> pipe16(generator16, *wf16, consumer16);
	if (degree2 > 1) {
		cout << "Run Test 16 WF(WM(MAP_GPU, REDUCE(NIC))): number of threads " << pipe16.cardinality() << endl;
		pipe16.run_and_wait_end();
		if (value == consumer16.getTotalSum())
			cout << "Done" << GREEN << " OK!" << DEFAULT << endl;
		else {
			cout << RED << " Error!!" << DEFAULT << endl;
			exit(1);
		}
	}

	// Test 17 WF(WM(MAP_GPU, REDUCE(INC)))
	Generator generator17(stream_len, num_keys);
	Consumer consumer17(num_keys);
	auto *wm17 = WinMapReduceGPU_Builder<decltype(mapFGPU), decltype(reduceFINC)>(mapFGPU, reduceFINC).withCBWindow(win_len, win_slide)
																										  .withBatch(batch_len)
										   																  .withParallelism(max<size_t>(degree2, 2), degree3)
										   																  .withName("wm_gpu_nic")
										   																  .withOptLevel(opt_level)
										   																  .build_ptr();
	auto *wf17 = WinFarmGPU_Builder<decltype(*wm17)>(*wm17).withParallelism(degree1)
										   			 		     .withName("wf_wm_gpu_nic")
										   			 			 .withOptLevel(opt_level)
										   			 			 .build_ptr();
	ff_Pipe<tuple_t, output_t> pipe17(generator17, *wf17, consumer17);
	if (degree2 > 1) {
		cout << "Run Test 17 WF(WM(MAP_GPU, REDUCE(INC))): number of threads " << pipe16.cardinality() << endl;
		pipe17.run_and_wait_end();
		if (value == consumer17.getTotalSum())
			cout << "Done" << GREEN << " OK!" << DEFAULT << endl;
		else {
			cout << RED << " Error!!" << DEFAULT << endl;
			exit(1);
		}
	}

	// Test 18 WF(WM(MAP(NIC), REDUCE_GPU))
	Generator generator18(stream_len, num_keys);
	Consumer consumer18(num_keys);
	auto *wm18 = WinMapReduceGPU_Builder<decltype(mapFNIC), decltype(reduceFGPU)>(mapFNIC, reduceFGPU).withCBWindow(win_len, win_slide)
																									  .withBatch(batch_len)
										   															  .withParallelism(max<size_t>(degree2, 2), degree3)
										   															  .withName("wm_nic_gpu")
										   															  .withOptLevel(opt_level)
										   															  .build_ptr();
	auto *wf18 = WinFarmGPU_Builder<decltype(*wm18)>(*wm18).withParallelism(degree1)
										   			 		     .withName("wf_wn_nic_gpu")
										   			 			 .withOptLevel(opt_level)
										   			 			 .build_ptr();
	ff_Pipe<tuple_t, output_t> pipe18(generator18, *wf18, consumer18);
	if (degree2 > 1) {
		cout << "Run Test 18 WF(WM(MAP(NIC), REDUCE_GPU)): number of threads " << pipe18.cardinality() << endl;
		pipe18.run_and_wait_end();
		if (value == consumer18.getTotalSum())
			cout << "Done" << GREEN << " OK!" << DEFAULT << endl;
		else {
			cout << RED << " Error!!" << DEFAULT << endl;
			exit(1);
		}
	}

	// Test 19 WF(WM(MAP(INC), REDUCE_GPU))
	Generator generator19(stream_len, num_keys);
	Consumer consumer19(num_keys);
	auto *wm19 = WinMapReduceGPU_Builder<decltype(mapFINC), decltype(reduceFGPU)>(mapFINC, reduceFGPU).withCBWindow(win_len, win_slide)
																									  .withBatch(batch_len)
										   														      .withParallelism(max<size_t>(degree2, 2), degree3)
										   														      .withName("wm_inc_gpu")
										   														      .withOptLevel(opt_level)
										   														      .build_ptr();
	auto *wf19 = WinFarmGPU_Builder<decltype(*wm19)>(*wm19).withParallelism(degree1)
										   			 		     .withName("wf_wn_inc_gpu")
										   			 			 .withOptLevel(opt_level)
										   			 			 .build_ptr();
	ff_Pipe<tuple_t, output_t> pipe19(generator19, *wf19, consumer19);
	if (degree2 > 1) {
		cout << "Run Test 19 WF(WM(MAP(INC), REDUCE_GPU)): number of threads " << pipe19.cardinality() << endl;
		pipe19.run_and_wait_end();
		if (value == consumer19.getTotalSum())
			cout << "Done" << GREEN << " OK!" << DEFAULT << endl;
		else {
			cout << RED << " Error!!" << DEFAULT << endl;
			exit(1);
		}
	}

	// Test 20 KF(PF(PLQ_GPU, WLQ(NIC)))
	Generator generator20(stream_len, num_keys);
	Consumer consumer20(num_keys);
	auto *pf20 = PaneFarmGPU_Builder<decltype(plqFGPU), decltype(wlqFNIC)>(plqFGPU, wlqFNIC).withCBWindow(win_len, win_slide)
																								.withBatch(batch_len)
										   														.withParallelism(degree2, degree3)
										   														.withName("pf_gpu_nic")
										   														.withOptLevel(opt_level)
										   														.build_ptr();
	auto *kf20 = KeyFarmGPU_Builder<decltype(*pf20)>(*pf20).withParallelism(degree1)
										   			 		     .withName("kf_pf_gpu_nic")
										   			 			 .withOptLevel(opt_level)
										   			 			 .build_ptr();
	ff_Pipe<tuple_t, output_t> pipe20(generator20, *kf20, consumer20);
	cout << "Run Test 20 KF(PF(PLQ_GPU, WLQ(NIC))): number of threads " << pipe20.cardinality() << endl;
	pipe20.run_and_wait_end();
	if (value == consumer20.getTotalSum())
		cout << "Done" << GREEN << " OK!" << DEFAULT << endl;
	else {
		cout << RED << " Error!!" << DEFAULT << endl;
		exit(1);
	}

	// Test 21 KF(PF(PLQ_GPU, WLQ(INC)))
	Generator generator21(stream_len, num_keys);
	Consumer consumer21(num_keys);
	auto *pf21 = PaneFarmGPU_Builder<decltype(plqFGPU), decltype(wlqFINC)>(plqFGPU, wlqFINC).withCBWindow(win_len, win_slide)
																								.withBatch(batch_len)
										   														.withParallelism(degree2, degree3)
										   														.withName("pf_gpu_inc")
										   														.withOptLevel(opt_level)
										   														.build_ptr();
	auto *kf21 = KeyFarmGPU_Builder<decltype(*pf21)>(*pf21).withParallelism(degree1)
										   			 		     .withName("kf_pf_gpu_inc")
										   			 			 .withOptLevel(opt_level)
										   			 			 .build_ptr();
	ff_Pipe<tuple_t, output_t> pipe21(generator21, *kf21, consumer21);
	cout << "Run Test 21 KF(PF(PLQ_GPU, WLQ(INC))): number of threads " << pipe21.cardinality() << endl;
	pipe21.run_and_wait_end();
	if (value == consumer21.getTotalSum())
		cout << "Done" << GREEN << " OK!" << DEFAULT << endl;
	else {
		cout << RED << " Error!!" << DEFAULT << endl;
		exit(1);
	}

	// Test 22 KF(PF(PLQ(NIC), WLQ_GPU))
	Generator generator22(stream_len, num_keys);
	Consumer consumer22(num_keys);
	auto *pf22 = PaneFarmGPU_Builder<decltype(plqFNIC), decltype(wlqFGPU)>(plqFNIC, wlqFGPU).withCBWindow(win_len, win_slide)
																								.withBatch(batch_len)
										   														.withParallelism(degree2, degree3)
										   														.withName("pf_nic_gpu")
										   														.withOptLevel(opt_level)
										   														.build_ptr();
	auto *kf22 = KeyFarmGPU_Builder<decltype(*pf22)>(*pf22).withParallelism(degree1)
										   			 		     .withName("kf_pf_nic_gpu")
										   			 			 .withOptLevel(opt_level)
										   			 			 .build_ptr();
	ff_Pipe<tuple_t, output_t> pipe22(generator22, *kf22, consumer22);
	cout << "Run Test 22 KF(PF(PLQ(NIC), WLQ_GPU)): number of threads " << pipe22.cardinality() << endl;
	pipe22.run_and_wait_end();
	if (value == consumer22.getTotalSum())
		cout << "Done" << GREEN << " OK!" << DEFAULT << endl;
	else {
		cout << RED << " Error!!" << DEFAULT << endl;
		exit(1);
	}

	// Test 23 KF(PF(PLQ(INC), WLQ_GPU))
	Generator generator23(stream_len, num_keys);
	Consumer consumer23(num_keys);
	auto *pf23 = PaneFarmGPU_Builder<decltype(plqFINC), decltype(wlqFGPU)>(plqFINC, wlqFGPU).withCBWindow(win_len, win_slide)
																								.withBatch(batch_len)
										   														.withParallelism(degree2, degree3)
										   														.withName("pf_inc_gpu")
										   														.withOptLevel(opt_level)
										   														.build_ptr();
	auto *kf23 = KeyFarmGPU_Builder<decltype(*pf23)>(*pf23).withParallelism(degree1)
										   			 		     .withName("kf_pf_inc_gpu")
										   			 			 .withOptLevel(opt_level)
										   			 			 .build_ptr();
	ff_Pipe<tuple_t, output_t> pipe23(generator23, *kf23, consumer23);
	cout << "Run Test 23 KF(PF(PLQ(INC), WLQ_GPU)): number of threads " << pipe23.cardinality() << endl;
	pipe23.run_and_wait_end();
	if (value == consumer23.getTotalSum())
		cout << "Done" << GREEN << " OK!" << DEFAULT << endl;
	else {
		cout << RED << " Error!!" << DEFAULT << endl;
		exit(1);
	}
	cout << "Final value: " << consumer23.getTotalSum() << endl;

	// Test 24 KF(WM(MAP_GPU, REDUCE(NIC)))
	Generator generator24(stream_len, num_keys);
	Consumer consumer24(num_keys);
	auto *wm24 = WinMapReduceGPU_Builder<decltype(mapFGPU), decltype(reduceFNIC)>(mapFGPU, reduceFNIC).withCBWindow(win_len, win_slide)
																										  .withBatch(batch_len)
										   																  .withParallelism(max<size_t>(degree2, 2), degree3)
										   																  .withName("wm_gpu_nic")
										   																  .withOptLevel(opt_level)
										   																  .build_ptr();
	auto *kf24 = KeyFarmGPU_Builder<decltype(*wm24)>(*wm24).withParallelism(degree1)
										   			 		     .withName("kf_wm_gpu_nic")
										   			 			 .withOptLevel(opt_level)
										   			 			 .build_ptr();
	ff_Pipe<tuple_t, output_t> pipe24(generator24, *kf24, consumer24);
	if (degree2 > 1) {
		cout << "Run Test 24 KF(WM(MAP_GPU, REDUCE(NIC))): number of threads " << pipe24.cardinality() << endl;
		pipe24.run_and_wait_end();
		if (value == consumer24.getTotalSum())
			cout << "Done" << GREEN << " OK!" << DEFAULT << endl;
		else {
			cout << RED << " Error!!" << DEFAULT << endl;
			exit(1);
		}
	}

	// Test 25 KF(WM(MAP_GPU, REDUCE(INC)))
	Generator generator25(stream_len, num_keys);
	Consumer consumer25(num_keys);
	auto *wm25 = WinMapReduceGPU_Builder<decltype(mapFGPU), decltype(reduceFINC)>(mapFGPU, reduceFINC).withCBWindow(win_len, win_slide)
																										  .withBatch(batch_len)
										   														          .withParallelism(max<size_t>(degree2, 2), degree3)
										   														          .withName("wm_gpu_inc")
										   														          .withOptLevel(opt_level)
										   														          .build_ptr();
	auto *kf25 = KeyFarmGPU_Builder<decltype(*wm25)>(*wm25).withParallelism(degree1)
										   			 		     .withName("kf_wm_gpu_inc")
										   			 			 .withOptLevel(opt_level)
										   			 			 .build_ptr();
	ff_Pipe<tuple_t, output_t> pipe25(generator25, *kf25, consumer25);
	if (degree2 > 1) {
		cout << "Run Test 25 KF(WM(MAP_GPU, REDUCE(INC))): number of threads " << pipe25.cardinality() << endl;
		pipe25.run_and_wait_end();
		if (value == consumer25.getTotalSum())
			cout << "Done" << GREEN << " OK!" << DEFAULT << endl;
		else {
			cout << RED << " Error!!" << DEFAULT << endl;
			exit(1);
		}
	}

	// Test 26 KF(WM(MAP(NIC), REDUCE_GPU))
	Generator generator26(stream_len, num_keys);
	Consumer consumer26(num_keys);
	auto *wm26 = WinMapReduceGPU_Builder<decltype(mapFNIC), decltype(reduceFGPU)>(mapFNIC, reduceFGPU).withCBWindow(win_len, win_slide)
																									  .withBatch(batch_len)
										   														      .withParallelism(max<size_t>(degree2, 2), degree3)
										   														      .withName("wm_nic_gpu")
										   														      .withOptLevel(opt_level)
										   														      .build_ptr();
	auto *kf26 = KeyFarmGPU_Builder<decltype(*wm26)>(*wm26).withParallelism(degree1)
										   			 		     .withName("kf_wn_nic_gpu")
										   			 			 .withOptLevel(opt_level)
										   			 			 .build_ptr();
	ff_Pipe<tuple_t, output_t> pipe26(generator26, *kf26, consumer26);
	if (degree2 > 1) {
		cout << "Run Test 26 KF(WM(MAP(NIC), REDUCE_GPU)): number of threads " << pipe26.cardinality() << endl;
		pipe26.run_and_wait_end();
		if (value == consumer26.getTotalSum())
			cout << "Done" << GREEN << " OK!" << DEFAULT << endl;
		else {
			cout << RED << " Error!!" << DEFAULT << endl;
			exit(1);
		}
	}

	// Test 27 KF(WM(MAP(INC), REDUCE_GPU))
	Generator generator27(stream_len, num_keys);
	Consumer consumer27(num_keys);
	auto *wm27 = WinMapReduceGPU_Builder<decltype(mapFINC), decltype(reduceFGPU)>(mapFINC, reduceFGPU).withCBWindow(win_len, win_slide)
																									  .withBatch(batch_len)
										   														      .withParallelism(max<size_t>(degree2, 2), degree3)
										   														      .withName("wm_inc_gpu")
										   														      .withOptLevel(opt_level)
										   														      .build_ptr();
	auto *kf27 = KeyFarmGPU_Builder<decltype(*wm27)>(*wm27).withParallelism(degree1)
										   			 		     .withName("kf_wn_inc_gpu")
										   			 			 .withOptLevel(opt_level)
										   			 			 .build_ptr();
	ff_Pipe<tuple_t, output_t> pipe27(generator27, *kf27, consumer27);
	if (degree2 > 1) {
		cout << "Run Test 27 KF(WM(MAP(INC), REDUCE_GPU)): number of threads " << pipe27.cardinality() << endl;
		pipe27.run_and_wait_end();
		if (value == consumer27.getTotalSum())
			cout << "Done" << GREEN << " OK!" << DEFAULT << endl;
		else {
			cout << RED << " Error!!" << DEFAULT << endl;
			exit(1);
		}
	}

	cout << "All tests done correctly!" << endl;
	return 0;
}
