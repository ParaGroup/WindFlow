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
 *  Generator and Consumer nodes of a stream of integers. The stream is designed to
 *  be used with count-based sliding windows.
 */ 

// includes
#include <string>
#include <ff/node.hpp>
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

	// destructor
	~tuple_t() {}

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

// struct of the output data type
struct output_t
{
	size_t key;
	uint64_t id;
	uint64_t ts;
	uint64_t value;

	// default constructor
	output_t(): key(0), id(0), ts(0), value(0) {}

	// destructor
	~output_t() {}

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

// class Generator: first stage that produces a stream of integers
class Generator: public ff_node_t<tuple_t>
{
private:
	size_t len; // stream length per key
	size_t keys; // number of keys

public:
	// constructor
	Generator(size_t _len, size_t _keys): len(_len), keys(_keys) {}

	// destructor
	~Generator() {}

	// svc method
	tuple_t *svc(tuple_t *in)
	{
		// generation of the input stream
		for (size_t i=0; i<len; i++) {
			for (size_t k=0; k<keys; k++) {
				tuple_t *t = new tuple_t(k, i, 0, i);
				ff_send_out(t);
			}
		}
		return EOS;
	}
};

// class Consumer: last stage that prints the query results
class Consumer: public ff_node_t<output_t>
{
private:
	size_t received; // counter of received results
	unsigned long totalsum;
	size_t keys;
	size_t *check_counters;

public:
	// constructor
	Consumer(size_t _keys): received(0), totalsum(0), keys(_keys), check_counters(new size_t[_keys])
	{
		std::fill_n(check_counters, keys, 0);
	}

	// destructor
	~Consumer()
	{
		delete[] check_counters;
	}

	// svc method
	output_t *svc(output_t *out)
	{
		received++;
		totalsum += out->value;
		// check the ordering of results
		if (check_counters[out->key] != out->id)
			cout << "Results received out-of-order!" << endl;
		//else cout << "Received result window " << out->id << " of key " << out->key << " with value " << out->value << endl;
		check_counters[out->key]++;
		delete out;
		return GO_ON;
	}

	// svc_end method
	void svc_end ()
	{
#if !defined(ALL_TESTS)
		cout << "Received " << received << " window results, total sum " << totalsum << endl;
#endif
	}

	// method to get the total sum of the windows
	unsigned long getTotalSum() const  {
		return totalsum;
	}
};
