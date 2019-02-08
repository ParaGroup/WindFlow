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
 *  Test of the Pipe construct
 *  
 *  Composition: Source(*) -> Filter(*) -> Sink(*)
 */ 

// includes
#include <string>
#include <iostream>
#include <ff/ff.hpp>
#include <windflow.hpp>

using namespace ff;
using namespace std;

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

	// getInfo method
	tuple<size_t, uint64_t, uint64_t> getInfo() const
	{
		return tuple<size_t, uint64_t, uint64_t>(key, id, ts);
	}

	// setInfo method
	void setInfo(size_t _key, uint64_t _id, uint64_t _ts)
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
	size_t stream_len;
public:
	// constructor
	Source_Functor(size_t _stream_len): stream_len(_stream_len) {}

	// operator()
	void operator()(Shipper<tuple_t> &shipper)
	{
		for(size_t i=1; i<=stream_len; i++) {
			tuple_t t(0, i, i, i);
			shipper.push(t);
		}
		LOCKED_PRINT("Generated " << stream_len << " items" << endl;)
	}
};

// filter functor
class Filter_Functor
{
public:
	bool operator()(tuple_t &t)
	{
		// drop odd numbers
		if (t.value % 2 == 0)
			return true;
		else
			return false;
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

	void operator()(optional<tuple_t> &t)
	{
		if (t) {
			received++;
			sum += (*t).value;
		}
		else
			LOCKED_PRINT("Received " << received << " results, total value " << sum << endl;)
	}
};

// main
int main(int argc, char *argv[])
{
	int option = 0;
	size_t stream_len = 0;
	size_t pardegree_source = 1;
	size_t pardegree_filter = 1;
	size_t pardegree_sink = 1;
	// arguments from command line
	if (argc != 9) {
		cout << argv[0] << " -l [stream_length] -n [sources] -f [filters] -m [sinks]" << endl;
		exit(EXIT_SUCCESS);
	}
	while ((option = getopt(argc, argv, "l:n:f:m:")) != -1) {
		switch (option) {
			case 'l': stream_len = atoi(optarg);
					 break;
			case 'n': pardegree_source = atoi(optarg);
					 break;
			case 'f': pardegree_filter = atoi(optarg);
					 break;
			case 'm': pardegree_sink = atoi(optarg);
					 break;
			default: {
				cout << argv[0] << " -l [stream_length] -n [sources] -f [filters] -m [sinks]" << endl;
				exit(EXIT_SUCCESS);
			}
        }
    }
    // prepare the test
    Pipe application("test2");
    // source
    Source_Functor source_functor(stream_len);
    Source source = Source_Builder(source_functor).withName("test2_source").withParallelism(pardegree_source).build();
    application.add_source(source);
    // filter
    Filter_Functor filter_functor;
    Filter filter = Filter_Builder(filter_functor).withName("test2_filter").withParallelism(pardegree_filter).build();
    application.add(filter);
    // sink
    Sink_Functor sink_functor;
    Sink sink = Sink_Builder(sink_functor).withName("test2_sink").withParallelism(pardegree_sink).build();
    application.add_sink(sink);
   	// run the application
   	application.run_and_wait_end();
	return 0;
}
