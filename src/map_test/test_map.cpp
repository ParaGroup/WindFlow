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
 *  Test Program of the Map Pattern
 *  
 *  Test program of the Map pattern instantiated in different ways: through a function,
 *  lambda or with a functor object.
 */ 

// includes
#include <string>
#include <iostream>
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

// class Consumer (in-place)
class Consumer_IP: public ff_node_t<tuple_t>
{
private:
	size_t received;
	size_t sum;
public:
	// constructor
	Consumer_IP(): received(0), sum(0) {}

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

// class Consumer (not in-place)
class Consumer_NIP: public ff_node_t<result_t>
{
private:
	size_t received;
	size_t sum;
public:
	// constructor
	Consumer_NIP(): received(0), sum(0) {}

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

// map function (NIP)
void map_function_nip(const tuple_t &t, result_t &r)
{
	r.id = t.id;
	r.val = t.val * 2;	
}

// map function (IP)
void map_function_ip(tuple_t &t)
{
	t.val = t.val * 2;	
}

// map functor (NIP)
template<typename T, typename R>
class Map_Functor_NIP
{
private:
	int state;
public:
	Map_Functor_NIP(int _state=1): state(_state) {}

	void operator()(const T &t, R &r)
	{
		r.id = t.id;
		r.val = t.val * 2 * state;		
	}
};

// map functor (IP)
template<typename T>
class Map_Functor_IP
{
private:
	int state;
public:
	Map_Functor_IP(int _state=1): state(_state) {}

	void operator()(T &t) {
	
		t.val = t.val * 2 * state;
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
    // TEST 1: map with not in-place lambda
    cout << CYAN << "(Test 1) Map with not in-place lambda" << DEFAULT << endl;
	Generator generator1(stream_len);
	// map lambda (NIP)
	auto map_lambda_nip = [](const tuple_t &t, result_t &r) {
		r.id = t.id;
		r.val = t.val * 2;
	};
	// creation of the Map pattern
	Map map1 = Map_Builder(map_lambda_nip).withParallelism(pardegree)
	                                      .withName("map")
						                  .build();
	Consumer_NIP consumer1;
	// creation of the pipeline
	ff_Pipe<tuple_t, result_t> pipe1(generator1, map1, consumer1);
	cout << "Starting ff_pipe with cardinality " << pipe1.cardinality() << "..." << endl;
	if (pipe1.run_and_wait_end() < 0) {
		cerr << "Error execution of ff_pipe" << endl;
	}
	else {
		cout << "...end ff_pipe" << endl;
	}

    // TEST 2: map with not in-place function
    cout << CYAN << "(Test 2) Map with not in-place function" << DEFAULT << endl;
	Generator generator2(stream_len);
	// creation of the Map pattern
	Map map2 = Map_Builder(map_function_nip).withParallelism(pardegree)
	                                        .withName("map")
						                    .build();
	Consumer_NIP consumer2;
	// creation of the pipeline
	ff_Pipe<tuple_t, result_t> pipe2(generator2, map2, consumer2);
	cout << "Starting ff_pipe with cardinality " << pipe2.cardinality() << "..." << endl;
	if (pipe2.run_and_wait_end() < 0) {
		cerr << "Error execution of ff_pipe" << endl;
	}
	else {
		cout << "...end ff_pipe" << endl;
	}

    // TEST 3: map with not in-place functor
    cout << CYAN << "(Test 3) Map with not in-place functor" << DEFAULT << endl;
    Map_Functor_NIP<tuple_t, result_t> functor_nip;
	Generator generator3(stream_len);
	// creation of the Map pattern
	Map map3 = Map_Builder(functor_nip).withParallelism(pardegree)
	                                   .withName("map")
						               .build();
	Consumer_NIP consumer3;
	// creation of the pipeline
	ff_Pipe<tuple_t, result_t> pipe3(generator3, map3, consumer3);
	cout << "Starting ff_pipe with cardinality " << pipe3.cardinality() << "..." << endl;
	if (pipe3.run_and_wait_end() < 0) {
		cerr << "Error execution of ff_pipe" << endl;
	}
	else {
		cout << "...end ff_pipe" << endl;
	}

	// TEST 4: map with in-place lambda
    cout << CYAN << "(Test 4) Map with in-place lambda" << DEFAULT << endl;
	Generator generator4(stream_len);
	// map lambda (IP)
	auto map_lambda_ip = [](tuple_t &t) {
		t.val = t.val * 2;
	};
	// creation of the Map pattern
	Map map4 = Map_Builder(map_lambda_ip).withParallelism(pardegree)
	                                     .withName("map")
						                 .build();
	Consumer_IP consumer4;
	// creation of the pipeline
	ff_Pipe<tuple_t, result_t> pipe4(generator4, map4, consumer4);
	cout << "Starting ff_pipe with cardinality " << pipe4.cardinality() << "..." << endl;
	if (pipe4.run_and_wait_end() < 0) {
		cerr << "Error execution of ff_pipe" << endl;
	}
	else {
		cout << "...end ff_pipe" << endl;
	}

    // TEST 5: map with in-place function
    cout << CYAN << "(Test 5) Map with in-place function" << DEFAULT << endl;
	Generator generator5(stream_len);
	// creation of the Map pattern
	Map map5 = Map_Builder(map_function_ip).withParallelism(pardegree)
	                                       .withName("map")
						                   .build();
	Consumer_IP consumer5;
	// creation of the pipeline
	ff_Pipe<tuple_t, result_t> pipe5(generator5, map5, consumer5);
	cout << "Starting ff_pipe with cardinality " << pipe5.cardinality() << "..." << endl;
	if (pipe5.run_and_wait_end() < 0) {
		cerr << "Error execution of ff_pipe" << endl;
	}
	else {
		cout << "...end ff_pipe" << endl;
	}

    // TEST 6: map with in-place functor
    cout << CYAN << "(Test 6) Map with in-place functor" << DEFAULT << endl;
    Map_Functor_IP<tuple_t> functor_ip;
	Generator generator6(stream_len);
	// creation of the Map pattern
	Map map6 = Map_Builder(functor_ip).withParallelism(pardegree)
	                                  .withName("map")
						              .build();
	Consumer_IP consumer6;
	// creation of the pipeline
	ff_Pipe<tuple_t, result_t> pipe6(generator6, map6, consumer6);
	cout << "Starting ff_pipe with cardinality " << pipe6.cardinality() << "..." << endl;
	if (pipe6.run_and_wait_end() < 0) {
		cerr << "Error execution of ff_pipe" << endl;
	}
	else {
		cout << "...end ff_pipe" << endl;
	}

	return 0;
}
