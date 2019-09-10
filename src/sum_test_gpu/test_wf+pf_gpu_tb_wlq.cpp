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
 *  Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 ******************************************************************************
 */

/*  
 *  Test Program of the nesting between Win_Farm_GPU and Pane_Farm_GPU (CB windows with a
 *  WLQ stage on the GPU)
 *  
 *  Test program of the nesting between the Win_Farm_GPU and the Pane_Farm_GPU pattern
 *  instantiated with a non-incremental PLQ phase on the CPU while the WLQ phase is executed
 *  on the GPU. The query computes the sum of the value attribute of all the tuples in the window.
 *  The sliding window specification uses the time-based model.
 */ 

// includes
#include <string>
#include <iostream>
#include <ff/ff.hpp>
#include <windflow.hpp>
#include <windflow_gpu.hpp>
#include <sum_tb.hpp>

using namespace std;
using namespace chrono;
using namespace ff;
using namespace wf;

// main
int main(int argc, char *argv[])
{
	int option = 0;
	size_t stream_len = 0;
	size_t win_len = 0;
	size_t win_slide = 0;
	size_t num_keys = 1;
	size_t wf_degree = 1;
	size_t plq_degree = 1;
	size_t wlq_degree = 1;
	size_t batch_len = 1;
	// arguments from command line
	if (argc != 17) {
		cout << argv[0] << " -l [stream_length] -k [num keys] -w [win length usec] -s [win slide usec] -r [WF pardegree] -n [PLQ pardegree] -m [WLQ pardegree] -b [batch_len]" << endl;
		exit(EXIT_SUCCESS);
	}
	while ((option = getopt(argc, argv, "l:k:w:s:r:n:m:b:")) != -1) {
		switch (option) {
			case 'l': stream_len = atoi(optarg);
					 break;
			case 'k': num_keys = atoi(optarg);
					 break;
			case 'w': win_len = atoi(optarg);
					 break;
			case 's': win_slide = atoi(optarg);
					 break;
			case 'r': wf_degree = atoi(optarg);
					 break;
			case 'n': plq_degree = atoi(optarg);
					 break;
			case 'm': wlq_degree = atoi(optarg);
					 break;
			case 'b': batch_len = atoi(optarg);
					 break;
			default: {
				cout << argv[0] << " -l [stream_length] -k [num keys] -w [win length usec] -s [win slide usec] -r [WF pardegree] -n [PLQ pardegree] -m [WLQ pardegree] -b [batch_len]" << endl;
				exit(EXIT_SUCCESS);
			}
        }
    }
	// user-defined pane function (Non-Incremental Query)
	auto F = [](size_t pid, Iterable<tuple_t> &input, output_t &pane_result) {
		long sum = 0;
		for (auto t: input) {
			int val = t.value;
			sum += val;
		}
		pane_result.value = sum;
	};
	// user-defined window function (Non-Incremental Query on GPU)
	auto G = [] __host__ __device__ (size_t wid, const output_t *data, output_t *res, size_t size, char *memory) {
		long sum = 0;
		for (size_t i=0; i<size; i++) {
			sum += data[i].value;
		}
		res->value = sum;
	};
	// creation of the Pane_Farm_GPU pattern
	auto *pf_gpu = PaneFarmGPU_Builder<decltype(F), decltype(G)>(F, G).withTBWindows(microseconds(win_len), microseconds(win_slide))
													     .withParallelism(plq_degree, wlq_degree)
													     .withBatch(batch_len)
													     .withName("test_sum")
													     .withOptLevel(LEVEL)
													     .build_ptr();
	// creation of the Win_Farm_GPU pattern
	auto *wf_gpu = WinFarmGPU_Builder<decltype(*pf_gpu)>(*pf_gpu).withParallelism(wf_degree)
													             .withName("test_sum")
													             .withOptLevel(LEVEL)
													             .build_ptr();
	// creation of the pipeline
	Generator generator(stream_len, num_keys);
	Consumer consumer(num_keys);
	ff_Pipe<tuple_t, output_t> pipe(generator, *wf_gpu, consumer);
	cout << "Starting ff_pipe with cardinality " << pipe.cardinality() << "..." << endl;
	if (pipe.run_and_wait_end() < 0) {
		cerr << "Error execution of ff_pipe" << endl;
		return -1;
	}
	else {
		cout << "...end ff_pipe" << endl;
		return 0;
	}
	return 0;
}
