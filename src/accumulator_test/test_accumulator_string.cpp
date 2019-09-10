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
 *  Test Program of the Accumulator Pattern
 *  
 *  Test program of the Accumulator pattern instantiated in different ways: through a function,
 *  lambda or with a functor object.
 */ 

// includes
#include <string>
#include <iostream>
#include <ff/ff.hpp>
#include <windflow.hpp>

using namespace std;
using namespace ff;
using namespace wf;

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
	tuple_t(): key("undefined_key"), id(0), ts(0), value(0) {}

	// destructor
	~tuple_t() {}

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
struct result_t
{
	string key;
	uint64_t id;
	uint64_t ts;
	uint64_t value;

	// default constructor
	result_t(): key("undefined_key"), id(0), ts(0), value(0) {}

	// destructor
	~result_t() {}

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

// class Generator: first stage that produces a stream of integers
class Generator: public ff_node_t<tuple_t>
{
private:
	size_t len; // stream length per key
	size_t n_keys; // number of keys
	vector<string> keys; // strings representing the keys

public:
	// constructor
	Generator(size_t _len, size_t _n_keys): len(_len), n_keys(_n_keys)
	{
		for (int i=0; i<n_keys; i++) {
			string key = "key_" + to_string(i);
			keys.push_back(key);
		}
	}

	// destructor
	~Generator() {}

	// svc method
	tuple_t *svc(tuple_t *in)
	{
		// generation of the input stream
		for (size_t i=0; i<len; i++) {
			for (size_t k=0; k<n_keys; k++) {
				tuple_t *t = new tuple_t(keys[k], i, 0, i);
				ff_send_out(t);
			}
		}
		return EOS;
	}
};
// class Consumer
class Consumer: public ff_node_t<result_t>
{
private:
	size_t received;
	size_t sum;
public:
	// constructor
	Consumer(): received(0), sum(0) {}

	// svc method
	result_t *svc(result_t *r)
	{
		received++;
		sum += r->value;
		delete r;
		return this->GO_ON;
	}

	// svc_end method
	void svc_end()
	{
		cout << "Received " << received << " results, total value " << sum << endl;
	}
};

// accumulator function
void acc_function(const tuple_t &t, result_t &r)
{
	r.value += t.value;
}

// accumulator functor
class Acc_Functor
{
private:
	int state;
public:
	Acc_Functor(int _state=1): state(_state) {}

	void operator()(const tuple_t &t, result_t &r)
	{
		r.value += t.value;
	}
};

// main
int main(int argc, char *argv[])
{
	int option = 0;
	size_t stream_len = 0;
	size_t pardegree = 0;
	size_t num_keys = 1;
	// arguments from command line
	if (argc != 7) {
		cout << argv[0] << " -l [stream_length] -k [num keys] -n [pardegree]" << endl;
		exit(EXIT_SUCCESS);
	}
	while ((option = getopt(argc, argv, "l:k:n:")) != -1) {
		switch (option) {
			case 'l': stream_len = atoi(optarg);
					 break;
			case 'k': num_keys = atoi(optarg);
					 break;
			case 'n': pardegree = atoi(optarg);
					 break;
			default: {
				cout << argv[0] << " -l [stream_length] -k [num keys] -n [pardegree]" << endl;
				exit(EXIT_SUCCESS);
			}
        }
    }
    // TEST 1: accumulator with lambda
    cout << CYAN << "(Test 1) Accumulator with lambda" << DEFAULT << endl;
	Generator generator1(stream_len, num_keys);
	// filter lambda
	auto acc_lambda = [](const tuple_t &t, result_t &r) {
		r.value += t.value;
	};
	// creation of the Accumulator pattern
	Accumulator acc1 = Accumulator_Builder(acc_lambda).withParallelism(pardegree)
	                                                  .withName("accumulator")
						                              .build();
	Consumer consumer1;
	// creation of the pipeline
	ff_Pipe<tuple_t> pipe1(generator1, acc1, consumer1);
	cout << "Starting ff_pipe with cardinality " << pipe1.cardinality() << "..." << endl;
	if (pipe1.run_and_wait_end() < 0) {
		cerr << "Error execution of ff_pipe" << endl;
	}
	else {
		cout << "...end ff_pipe" << endl;
	}

    // TEST 2: accumulator with function
    cout << CYAN << "(Test 2) Accumulator with function" << DEFAULT << endl;
	Generator generator2(stream_len, num_keys);
	// creation of the Accumulator pattern
	Accumulator acc2 = Accumulator_Builder(acc_function).withParallelism(pardegree)
	                                                    .withName("accumulator")
						                                .build();
	Consumer consumer2;
	// creation of the pipeline
	ff_Pipe<tuple_t> pipe2(generator2, acc2, consumer2);
	cout << "Starting ff_pipe with cardinality " << pipe2.cardinality() << "..." << endl;
	if (pipe2.run_and_wait_end() < 0) {
		cerr << "Error execution of ff_pipe" << endl;
	}
	else {
		cout << "...end ff_pipe" << endl;
	}

    // TEST 3: accumulator with functor
    cout << CYAN << "(Test 3) Accumulator with functor" << DEFAULT << endl;
    Acc_Functor functor;
	Generator generator3(stream_len, num_keys);
	// creation of the Accumulator pattern
	Accumulator acc3 = Accumulator_Builder(functor).withParallelism(pardegree)
	                                               .withName("accumulator")
						                           .build();
	Consumer consumer3;
	// creation of the pipeline
	ff_Pipe<tuple_t> pipe3(generator3, acc3, consumer3);
	cout << "Starting ff_pipe with cardinality " << pipe3.cardinality() << "..." << endl;
	if (pipe3.run_and_wait_end() < 0) {
		cerr << "Error execution of ff_pipe" << endl;
	}
	else {
		cout << "...end ff_pipe" << endl;
	}

	return 0;
}
