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
 *  Data types and operator functors for running the MultiPipe tests on CPU
 */ 

// includes
#include<cmath>
#include<string>

// defines
#define RATIO 0.46566128e-9

using namespace std;
using namespace wf;

// global variables
long global_sum;
long global_received;

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
    size_t key;
    uint64_t id;
    uint64_t ts;
    int64_t value;

    // constructor
    tuple_t(size_t _key,
            uint64_t _id,
            uint64_t _ts,
            int64_t _value):
            key(_key),
            id(_id),
            ts(_ts),
            value(_value)
    {}

    // default constructor
    tuple_t():
            key(0),
            id(0),
            ts(0),
            value(0)
    {}

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

// struct of the output data type
struct output_t
{
    size_t key;
    uint64_t id;
    uint64_t ts;
    int64_t value;

    // constructor
    output_t(size_t _key,
             uint64_t _id,
             uint64_t _ts,
             int64_t _value):
             key(_key),
             id(_id),
             ts(_ts),
             value(_value)
    {}

    // default constructor
    output_t():
             key(0),
             id(0),
             ts(0),
             value(0)
    {}

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

// source functor for generating numbers
class Source_Functor
{
private:
    size_t len; // stream length per key
    size_t keys; // number of keys
    size_t k;
    size_t sent;
    vector<uint64_t> ids;
    uint64_t next_ts;

public:
    // Constructor
    Source_Functor(size_t _len,
                   size_t _keys):
                   len(_len),
                   keys(_keys),
                   k(0),
                   sent(0),
                   ids(_keys, 0),
                   next_ts(0)
    {
        srand(0);
    }

    bool operator()(tuple_t &t)
    {
        t.setControlFields(k, ids[k], next_ts);
        t.value = ids[k]++;
        sent++;
        k = (k+1) % keys;
        double x = (1000 * 0.05) / 1.05;
        next_ts += ceil(pareto(1.05, x));
        if (sent < len*keys)
            return true;
        else
            return false;
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
            t2.ts = t.ts+i; // important to have deterministic results
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

// Win_Farm function (non-incremental)
void wf_function(size_t wid, const Iterable<tuple_t> &input, output_t &result) {
    long sum = 0;
    // print the window content
    for (auto t : input) {
        int value = t.value;
        sum += value;
    }
    result.value = sum;
};

// Key_Farm function (non-incremental)
void kf_function(size_t wid, const Iterable<tuple_t> &input, output_t &result) {
    long sum = 0;
    // print the window content
    for (auto t : input) {
        int value = t.value;
        sum += value;
    }
    result.value = sum;
};

// Key_FFAT lift function
void liftFunction(const tuple_t &t, output_t &out) {
    out.key = t.key;
    out.id = t.id;
    out.ts = t.ts;
    out.value = t.value;
};

// Key_FFAT combine function
void combineFunction(const output_t &o1, const output_t &o2, output_t &out) {
    out.value = o1.value + o2.value;
};

// Pane_Farm (PLQ) function (non-incremental)
void plq_function(size_t pid, const Iterable<tuple_t> &input, output_t &pane_result) {
    long sum = 0;
    // print the window content
    for (auto t : input) {
        int val = t.value;
        sum += val;
    }
    pane_result.value = sum;
};

// Pane_Farm (WLQ) function (non-incremental)
void wlq_function(size_t wid, const Iterable<output_t> &input, output_t &win_result) {
    long sum = 0;
    // print the window content
    for (auto t : input) {
        int val = t.value;
        sum += val;
    }
    win_result.value = sum;
};

// Win_MapReduce (MAP) function (non-incremental)
void wmap_function(size_t wid, const Iterable<tuple_t> &input, output_t &result) {
    long sum = 0;
    // print the window content
    for (auto t : input) {
        int val = t.value;
        sum += val;
    }
    result.value = sum;
};

// Win_MapReduce (REDUCE) function (non-incremental)
void reduce_function(size_t wid, const Iterable<output_t> &input, output_t &win_result) {
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
    size_t keys;
    vector<size_t> check_counters;

public:
    // constructor
    Sink_Functor(size_t _keys):
                 received(0),
                 totalsum(0),
                 keys(_keys),
                 check_counters(_keys, 0)
    {}

    // operator()
    void operator()(optional<output_t> &out)
    {
        if (out) {
            received++;
            totalsum += (*out).value;
            // check the ordering of results
            //if (check_counters[(*out).key] != (*out).id)
                //cout << "Results received out-of-order!" << endl;
            //else cout << "Received result window " << *out->id << " of key " << out->key << " with value " << (*out).value << endl;
            check_counters[(*out).key]++;   
        }
        else {
            cout << "Received " << received << " results, total sum " << totalsum << endl;
            global_sum = totalsum;
            global_received = received;
        }
    }
};
