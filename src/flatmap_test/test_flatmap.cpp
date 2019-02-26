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
 *  Test Program of the FlatMap Pattern
 *  
 *  Test program of the FlatMap pattern instantiated in different ways: through a function,
 *  lambda or with a functor object.
 */ 

// includes
#include <string>
#include <iostream>
#include <time.h>
#include <stdlib.h>
#include <ff/ff.hpp>
#include <windflow.hpp>

using namespace ff;
using namespace std;

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

// struct result_t
struct result_t
{
	int id;
	int val;

	// constructor
	result_t() {}

	// destructor
	~result_t() {}
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
			tuple_t *t = new tuple_t(i, i);
			this->ff_send_out(t);
		}
		return this->EOS;
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
		sum += r->val;
		delete r;
		return this->GO_ON;
	}

	// svc_end method
	void svc_end()
	{
		cout << "Received " << received << " results, total value " << sum << endl;
	}
};

// flatmap function
void flatmap_function(const tuple_t &t, Shipper<result_t> &shipper)
{
	// random number between 0 and 9
	size_t n = rand() % 10;
	for (size_t i=0; i<n; i++) {
		result_t r;
		r.id = t.id;
		r.val = t.val;
		shipper.push(r);
	}
}

// flatmap functor
class FlatMap_Functor
{
private:
	int state;
public:
	FlatMap_Functor(int _state=1): state(_state) {}

	void operator()(const tuple_t &t, Shipper<result_t> &shipper)
	{
		// random number between 0 and 9
		size_t n = rand() % 10;
		for (size_t i=0; i<n; i++) {
			result_t r;
			r.id = t.id;
			r.val = t.val;
			shipper.push(r);
		}
	}
};

// main
int main(int argc, char *argv[])
{
	srand(0);
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

    // TEST 1: flatmap with lambda
    cout << CYAN << "(Test 1) FlatMap with lambda" << DEFAULT << endl;
	Generator generator1(stream_len);
	// flatmap lambda
	auto flatmap_lambda = [](const tuple_t &t, Shipper<result_t> &shipper) {
		// random number between 0 and 9
		size_t n = rand() % 10;
		for (size_t i=0; i<n; i++) {
			result_t r;
			r.id = t.id;
			r.val = t.val;
			shipper.push(r);
		}
	};
	// creation of the FlatMap pattern
	FlatMap flatmap1 = FlatMap_Builder(flatmap_lambda).withParallelism(pardegree)
	                                                  .withName("flatmap")
						                              .build();
	Consumer consumer1;
	// creation of the pipeline
	ff_Pipe<tuple_t> pipe1(generator1, flatmap1, consumer1);
	cout << "Starting ff_pipe with cardinality " << pipe1.cardinality() << "..." << endl;
	if (pipe1.run_and_wait_end() < 0) {
		cerr << "Error execution of ff_pipe" << endl;
	}
	else {
		cout << "...end ff_pipe" << endl;
	}
    // TEST 2: flatmap with function
    cout << CYAN << "(Test 2) FlatMap with function" << DEFAULT << endl;
	Generator generator2(stream_len);
	// creation of the FlatMap pattern
	FlatMap flatmap2 = FlatMap_Builder(flatmap_function).withParallelism(pardegree)
	                                                    .withName("flatmap")
						                                .build();
	Consumer consumer2;
	// creation of the pipeline
	ff_Pipe<tuple_t> pipe2(generator2, flatmap2, consumer2);
	cout << "Starting ff_pipe with cardinality " << pipe2.cardinality() << "..." << endl;
	if (pipe2.run_and_wait_end() < 0) {
		cerr << "Error execution of ff_pipe" << endl;
	}
	else {
		cout << "...end ff_pipe" << endl;
	}

    // TEST 3: flatmap with functor
    cout << CYAN << "(Test 3) FlatMap with functor" << DEFAULT << endl;
    FlatMap_Functor functor;
	Generator generator3(stream_len);
	// creation of the FlatMap pattern
	FlatMap flatmap3 = FlatMap_Builder(functor).withParallelism(pardegree)
	                                           .withName("flatmap")
						                       .build();
	Consumer consumer3;
	// creation of the pipeline
	ff_Pipe<tuple_t> pipe3(generator3, flatmap3, consumer3);
	cout << "Starting ff_pipe with cardinality " << pipe3.cardinality() << "..." << endl;
	if (pipe3.run_and_wait_end() < 0) {
		cerr << "Error execution of ff_pipe" << endl;
	}
	else {
		cout << "...end ff_pipe" << endl;
	}
	return 0;
}
