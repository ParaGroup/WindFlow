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
 *  Test Program of the Filter Pattern
 *  
 *  Test program of the Filter pattern instantiated in different ways: through a function,
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
	size_t key;
	uint64_t id;
	uint64_t ts;
	uint64_t value;

	// constructor
	tuple_t(size_t _key, uint64_t _id, uint64_t _ts, uint64_t _value): key(_key), id(_id), ts(_ts), value(_value) {}

	// default constructor
	tuple_t(): key(0), id(0), ts(0), value(0) {}

	// getControlFields method
	tuple<size_t, uint64_t, uint64_t> getControlFields() const
	{
		return tuple<size_t, uint64_t, uint64_t>(key, id, ts);
	}

	// setControlFields method
	void setControlFields(size_t _key, uint64_t _id, uint64_t _ts)
	{
		key = _key;
		id = _id;
		ts = _ts;
	}
};

// class Generator
class Generator: public ff_node_t<tuple_t>
{
private:
	size_t stream_len;
public:
	// constructor
	Generator(size_t _stream_len): stream_len(_stream_len) {}

	// svc method
	tuple_t *svc(tuple_t *)
	{
		for(size_t i=1; i<=stream_len; i++) {
			tuple_t *t = new tuple_t(0, i, 0, i);
			this->ff_send_out(t);
		}
		return this->EOS;
	}
};

// class Consumer
class Consumer: public ff_node_t<tuple_t>
{
private:
	size_t received;
	size_t sum;
public:
	// constructor
	Consumer(): received(0), sum(0) {}

	// svc method
	tuple_t *svc(tuple_t *t)
	{
		received++;
		sum += t->value;
		delete t;
		return this->GO_ON;
	}

	// svc_end method
	void svc_end()
	{
		cout << "Received " << received << " results, total value " << sum << endl;
	}
};

// filter function
bool predicate_function(tuple_t &t)
{
	if (t.value % 3 == 0)
		return true;
	else
		return false;	
}

// filter functor
template<typename T>
class Predicate_Functor
{
private:
	int state;
public:
	Predicate_Functor(int _state=1): state(_state) {}

	bool operator()(T &t)
	{
	if (t.value % 3 == 0)
		return true;
	else
		return false;
	}
};

// main
int main(int argc, char *argv[])
{
	int option = 0;
	size_t stream_len = 0;
	size_t pardegree = 0;
	// arguments from command line
	if (argc != 5) {
		cout << argv[0] << " -l [stream_length] -n [pardegree]" << endl;
		exit(EXIT_SUCCESS);
	}
	while ((option = getopt(argc, argv, "l:n:")) != -1) {
		switch (option) {
			case 'l': stream_len = atoi(optarg);
					 break;
			case 'n': pardegree = atoi(optarg);
					 break;
			default: {
				cout << argv[0] << " -l [stream_length] -n [pardegree]" << endl;
				exit(EXIT_SUCCESS);
			}
        }
    }
    // TEST 1: filter with lambda
    cout << CYAN << "(Test 1) Filter with lambda" << DEFAULT << endl;
	Generator generator1(stream_len);
	// filter lambda
	auto predicate_lambda = [](tuple_t &t) {
		if (t.value % 3 == 0)
			return true;
		else
			return false;
	};
	// creation of the Filter pattern
	Filter filter1 = Filter_Builder(predicate_lambda).withParallelism(pardegree)
	                                                 .withName("filter")
						                             .build();
	Consumer consumer1;
	// creation of the pipeline
	ff_Pipe<tuple_t> pipe1(generator1, filter1, consumer1);
	cout << "Starting ff_pipe with cardinality " << pipe1.cardinality() << "..." << endl;
	if (pipe1.run_and_wait_end() < 0) {
		cerr << "Error execution of ff_pipe" << endl;
	}
	else {
		cout << "...end ff_pipe" << endl;
	}

    // TEST 2: filter with function
    cout << CYAN << "(Test 2) Filter with function" << DEFAULT << endl;
	Generator generator2(stream_len);
	// creation of the Filter pattern
	Filter filter2 = Filter_Builder(predicate_function).withParallelism(pardegree)
	                                                   .withName("filter")
						                               .build();
	Consumer consumer2;
	// creation of the pipeline
	ff_Pipe<tuple_t> pipe2(generator2, filter2, consumer2);
	cout << "Starting ff_pipe with cardinality " << pipe2.cardinality() << "..." << endl;
	if (pipe2.run_and_wait_end() < 0) {
		cerr << "Error execution of ff_pipe" << endl;
	}
	else {
		cout << "...end ff_pipe" << endl;
	}

    // TEST 3: filter with functor
    cout << CYAN << "(Test 3) Filter with functor" << DEFAULT << endl;
    Predicate_Functor<tuple_t> functor;
	Generator generator3(stream_len);
	// creation of the Filter pattern
	Filter filter3 = Filter_Builder(functor).withParallelism(pardegree)
	                                          .withName("filter")
						                      .build();
	Consumer consumer3;
	// creation of the pipeline
	ff_Pipe<tuple_t> pipe3(generator3, filter3, consumer3);
	cout << "Starting ff_pipe with cardinality " << pipe3.cardinality() << "..." << endl;
	if (pipe3.run_and_wait_end() < 0) {
		cerr << "Error execution of ff_pipe" << endl;
	}
	else {
		cout << "...end ff_pipe" << endl;
	}

	return 0;
}
