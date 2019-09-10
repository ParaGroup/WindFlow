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
 *  Test of the MultiPipe construct (keys of type string)
 *  
 *  Composition: Source(1) -> Filter(*) -> FlatMap(*) -> Map(*) -> PF_CB(*) -> Sink(1)
 */ 

// includes
#include <string>
#include <iostream>
#include <random>
#include <math.h>
#include <ff/ff.hpp>
#include <windflow.hpp>

using namespace std;
using namespace chrono;
using namespace wf;

// defines
#define RATIO 0.46566128e-9

// global variable for the result
long global_sum;

// generation of pareto-distributed pseudo-random numbers
double pareto(double alpha, double kappa)
{
	double u;
	long seed = random();
	u = (seed) * RATIO;
	return (kappa / pow(u, (1. / alpha)));
}

// struct of the input tuple
struct tuple_t
{
	string key;
	uint64_t id;
	uint64_t ts;
	uint64_t value;

	// constructor
	tuple_t(string _key, uint64_t _id, uint64_t _ts, uint64_t _value): key(_key), id(_id), ts(_ts), value(_value) {}

	// default constructor
	tuple_t(): key("undefined"), id(0), ts(0), value(0) {}

	// getControlFields method
	tuple<string, uint64_t, uint64_t> getControlFields() const
	{
		return tuple<string, uint64_t, uint64_t>(key, id, ts);
	}

	// setControlFields method
	void setControlFields(string _key, uint64_t _id, uint64_t _ts)
	{
		key = _key;
		id = _id;
		ts = _ts;
	}
};

// struct of the output data type
struct output_t
{
	string key;
	uint64_t id;
	uint64_t ts;
	uint64_t value;

	// default constructor
	output_t(): key("undefined"), id(0), ts(0), value(0) {}

	// getControlFields method
	tuple<string, uint64_t, uint64_t> getControlFields() const
	{
		return tuple<string, uint64_t, uint64_t>(key, id, ts);
	}

	// setControlFields method
	void setControlFields(string _key, uint64_t _id, uint64_t _ts)
	{
		key = _key;
		id = _id;
		ts = _ts;
	}
};

// source functor
class Source_Functor
{
private:
	size_t len; // stream length per key
	size_t keys; // number of keys
	uint64_t next_ts;

public:
	// constructor
	Source_Functor(size_t _len, size_t _keys): len(_len), keys(_keys), next_ts(0)
	{
		srand(0);
	}

	// operator()
	void operator()(Shipper<tuple_t> &shipper)
	{
		// generation of the input stream
		for (size_t i=0; i<len; i++) {
			for (size_t k=0; k<keys; k++) {
				string key = "key_" + to_string(k);
				tuple_t t(key, i, next_ts, i);
				double x = (1000 * 0.05) / 1.05;
				next_ts += ceil(pareto(1.05, x));
				//next_ts[k] += 1000;
				shipper.push(t);
			}
		}
	}
};

// filter functor
class Filter_Functor
{
public:
	// operator()
	bool operator()(tuple_t &t)
	{
		// drop odd numbers
		if (t.value % 2 == 0)
			return true;
		else
			return false;
	}
};

// flatmap functor
class FlatMap_Functor
{
public:
	// operator()
	void operator()(const tuple_t &t, Shipper<tuple_t> &shipper)
	{
		// generate three items per input
		for (size_t i=0; i<3; i++) {
			tuple_t t2 = t;
			t2.value = t.value + i;
			t2.ts = t.ts+i; // <---- important to have deterministic results
			shipper.push(t2);
		}
	}
};

// map functor
class Map_Functor
{
public:
	// operator()
	void operator()(tuple_t &t)
	{
		// double the value
		t.value = t.value * 2;
	}
};

// Pane_Farm (PLQ) function (non-incremental)
void plq_function(size_t pid, Iterable<tuple_t> &input, output_t &pane_result) {
	long sum = 0;
	// print the window content
	for (auto t : input) {
		int val = t.value;
		sum += val;
	}
	pane_result.value = sum;
};

// Pane_Farm (WLQ) function (non-incremental)
void wlq_function(size_t wid, Iterable<output_t> &input, output_t &win_result) {
	long sum = 0;
	// print the window content
	for (auto t : input) {
		int val = t.value;
		sum += val;
	}
	win_result.value = sum;
};

// sink functor
class Sink_Functor
{
private:
	size_t received; // counter of received results
	long totalsum;

public:
	// constructor
	Sink_Functor(): received(0), totalsum(0) {}

	// operator()
	void operator()(optional<output_t> &out)
	{
		if (out) {
			received++;
			totalsum += (*out).value;
		}
		else {
			LOCKED_PRINT("Received " << received << " window results, total sum " << totalsum << endl;)
			global_sum = totalsum;
		}
	}
};

// main
int main(int argc, char *argv[])
{
	int option = 0;
	size_t runs = 1;
	size_t stream_len = 0;
	size_t win_len = 0;
	size_t win_slide = 0;
	size_t n_keys = 1;
	// initalize global variable
	global_sum = 0;
	// arguments from command line
	if (argc != 11) {
		cout << argv[0] << " -r [runs] -l [stream_length] -k [n_keys] -w [win length] -s [win slide]" << endl;
		exit(EXIT_SUCCESS);
	}
	while ((option = getopt(argc, argv, "r:l:k:w:s:")) != -1) {
		switch (option) {
			case 'r': runs = atoi(optarg);
					 break;
			case 'l': stream_len = atoi(optarg);
					 break;
			case 'k': n_keys = atoi(optarg);
					 break;
			case 'w': win_len = atoi(optarg);
					 break;
			case 's': win_slide = atoi(optarg);
					 break;
			default: {
				cout << argv[0] << " -r [runs] -l [stream_length] -k [n_keys] -w [win length] -s [win slide]" << endl;
				exit(EXIT_SUCCESS);
			}
        }
    }
    // set random seed
    mt19937 rng;
    rng.seed(std::random_device()());
    size_t min = 1;
    size_t max = 10;
    std::uniform_int_distribution<std::mt19937::result_type> dist6(min, max);
    int filter_degree, flatmap_degree, map_degree, plq_degree, wlq_degree;
    size_t source_degree = dist6(rng);
    source_degree = 1;
    long last_result = 0;
    // executes the runs
    for (size_t i=0; i<runs; i++) {
    	filter_degree = dist6(rng);
    	flatmap_degree = dist6(rng);
    	map_degree = dist6(rng);
    	plq_degree = dist6(rng);
    	wlq_degree = dist6(rng);
    	cout << "Run " << i << " Source(" << source_degree <<")->Filter(" << filter_degree << ")->FlatMap(" << flatmap_degree << ")->Map(" << map_degree << ")->Pane_Farm_CB(" << plq_degree << "," << wlq_degree << ")->Sink(1)" << endl;
	    // prepare the test
	    MultiPipe application("test_pf_cb");
	    // source
	    Source_Functor source_functor(stream_len, n_keys);
	    Source source = Source_Builder(source_functor).withName("test_pf_cb_source").withParallelism(source_degree).build();
	    application.add_source(source);
	    // filter
	    Filter_Functor filter_functor;
	    Filter filter = Filter_Builder(filter_functor).withName("test_pf_cb_filter").withParallelism(filter_degree).build();
	    application.add(filter);
	    // flatmap
	    FlatMap_Functor flatmap_functor;
	    FlatMap flatmap = FlatMap_Builder(flatmap_functor).withName("test_pf_cb_flatmap").withParallelism(flatmap_degree).build();
	    application.add(flatmap);
	    // map
	    Map_Functor map_functor;
	    Map map = Map_Builder(map_functor).withName("test_pf_cb_map").withParallelism(map_degree).build();
	    application.add(map);
	    // pf
	    Pane_Farm pf = PaneFarm_Builder(plq_function, wlq_function).withName("test_pf_cb_pf").withParallelism(plq_degree, wlq_degree).withCBWindows(win_len, win_slide).build();
	    application.add(pf);
	    // sink
	    Sink_Functor sink_functor;
	    Sink sink = Sink_Builder(sink_functor).withName("test_pf_cb_sink").withParallelism(1).build();
	    application.add_sink(sink);
	   	// run the application
	   	application.run_and_wait_end();
	   	if (i == 0) {
	   		last_result = global_sum;
	   		cout << "Result is --> " << GREEN << "OK" << "!!!" << DEFAULT << endl;
	   	}
	   	else {
	   		if (last_result == global_sum) {
	   			cout << "Result is --> " << GREEN << "OK" << "!!!" << DEFAULT << endl;
	   		}
	   		else {
	   			cout << "Result is --> " << RED << "FAILED" << "!!!" << DEFAULT << endl;
	   		}
	   	}
    }

	return 0;
}
