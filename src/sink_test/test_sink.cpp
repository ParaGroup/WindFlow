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
 *  Test Program of the Sink Pattern
 *  
 *  Test program of the Sink pattern instantiated in different ways: through a function,
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

// sink functor
class Sink_Functor
{
private:
	size_t received;
	unsigned long sum;

public:
	Sink_Functor(): received(0), sum(0) {}

	~Sink_Functor() {}

	void operator()(optional<tuple_t> &t)
	{
		if (t) {
			received++;
			sum += (*t).val;
		}
		else
			cout << "Received " << received << " results, total value " << sum << endl;
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
    // TEST 1: sink with functor
    cout << CYAN << "(Test 1) Sink with functor" << DEFAULT << endl;
	Generator generator1(stream_len);
	Sink_Functor functor;
	// creation of the Sink pattern
	Sink sink1 = Sink_Builder(functor).withParallelism(pardegree)
	                                  .withName("sink")
						              .build();
	// creation of the pipeline
	ff_Pipe<tuple_t> pipe1(generator1, sink1);
	cout << "Starting ff_pipe with cardinality " << pipe1.cardinality() << "..." << endl;
	if (pipe1.run_and_wait_end() < 0) {
		cerr << "Error execution of ff_pipe" << endl;
	}
	else {
		cout << "...end ff_pipe" << endl;
	}

    // TEST 2: sink with lambda
    cout << CYAN << "(Test 1) Sink with lambda" << DEFAULT << endl;
	Generator generator2(stream_len);
	size_t received = 0;
	unsigned long sum = 0;
	auto sink_lambda = [received, sum](optional<tuple_t> &t) mutable -> void {
		if (t) {
			received++;
			sum += (*t).val;
		}
		else
			cout << "Received " << received << " results, total value " << sum << endl;		
	};
	// creation of the Sink pattern
	Sink sink2 = Sink_Builder(sink_lambda).withParallelism(pardegree)
	                                      .withName("sink")
						                  .build();
	// creation of the pipeline
	ff_Pipe<tuple_t> pipe2(generator2, sink2);
	cout << "Starting ff_pipe with cardinality " << pipe2.cardinality() << "..." << endl;
	if (pipe2.run_and_wait_end() < 0) {
		cerr << "Error execution of ff_pipe" << endl;
	}
	else {
		cout << "...end ff_pipe" << endl;
	}

	return 0;
}
