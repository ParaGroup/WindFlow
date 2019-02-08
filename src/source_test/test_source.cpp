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
 *  Test Program of the Source Pattern
 *  
 *  Test program of the Source pattern instantiated in different ways: through a function,
 *  lambda or a functor object.
 */ 

// includes
#include <string>
#include <iostream>
#include <ff/ff.hpp>
#include <windflow.hpp>

using namespace ff;
using namespace std;

// global variable (stream length)
size_t stream_len_global;

// struct tuple_t
struct tuple_t
{
	int id;
	int val;

	// constructor I
	tuple_t() {}

	// constructor II
	tuple_t(int _id, int _val): id(_id), val(_val) {}

	// destructor
	~tuple_t() {}
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
		sum += t->val;
		delete t;
		return this->GO_ON;
	}

	// svc_end method
	void svc_end()
	{
		cout << "Received " << received << " results, total value " << sum << endl;
	}
};

// generation function (single-loop version)
void generator_function(Shipper<tuple_t> &shipper)
{
	for (size_t index=0; index<stream_len_global; index++) {
		tuple_t t;
		t.id = index;
		t.val = index;
		shipper.push(t);
	}
}

// generator functor (single-loop version)
class Generator_Functor
{
private:
	size_t stream_len;
public:
	Generator_Functor(size_t _stream_len): stream_len(_stream_len) {}

	void operator()(Shipper<tuple_t> &shipper)
	{
		for (size_t index=0; index<stream_len; index++) {
			tuple_t t;
			t.id = index;
			t.val = index;
			shipper.push(t);
		}
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
    stream_len_global = stream_len;

    // TEST 1: source with lambda (single-loop version)
    cout << CYAN << "(Test 1) Source with lambda (single-loop version)" << DEFAULT << endl;
    // generation lambda (single-loop version)
	auto generator_lambda = [stream_len](Shipper<tuple_t> &shipper) {
		for (size_t index=0; index<stream_len; index++) {
			tuple_t t;
			t.id = index;
			t.val = index;
			shipper.push(t);
		}
	};
	// creation of the Source pattern
	Source source1 = Source_Builder(generator_lambda).withParallelism(pardegree)
	                                      			 .withName("source")
						                  			 .build();
	Consumer consumer1;
	// creation of the pipeline
	ff_Pipe<tuple_t> pipe1(source1, consumer1);
	cout << "Starting ff_pipe with cardinality " << pipe1.cardinality() << "..." << endl;
	if (pipe1.run_and_wait_end() < 0) {
		cerr << "Error execution of ff_pipe" << endl;
	}
	else {
		cout << "...end ff_pipe" << endl;
	}

    // TEST 2: source with function (single-loop version)
    cout << CYAN << "(Test 2) Source with function (single-loop version)" << DEFAULT << endl;
    stream_len_global = stream_len;
	// creation of the Source pattern
	Source source2 = Source_Builder(generator_function).withParallelism(pardegree)
	                                      			   .withName("source")
						                  			   .build();
	Consumer consumer2;
	// creation of the pipeline
	ff_Pipe<tuple_t> pipe2(source2, consumer2);
	cout << "Starting ff_pipe with cardinality " << pipe2.cardinality() << "..." << endl;
	if (pipe2.run_and_wait_end() < 0) {
		cerr << "Error execution of ff_pipe" << endl;
	}
	else {
		cout << "...end ff_pipe" << endl;
	}

    // TEST 3: source with functor (single-loop version)
    cout << CYAN << "(Test 3) Source with functor (single-loop version)" << DEFAULT << endl;
    Generator_Functor functor(stream_len);
	// creation of the Source pattern
	Source source3 = Source_Builder(functor).withParallelism(pardegree)
	                                      	.withName("source")
						                  	.build();
	Consumer consumer3;
	// creation of the pipeline
	ff_Pipe<tuple_t> pipe3(source3, consumer3);
	cout << "Starting ff_pipe with cardinality " << pipe3.cardinality() << "..." << endl;
	if (pipe3.run_and_wait_end() < 0) {
		cerr << "Error execution of ff_pipe" << endl;
	}
	else {
		cout << "...end ff_pipe" << endl;
	}

	return 0;
}
