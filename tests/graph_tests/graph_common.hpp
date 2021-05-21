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
 *  Data types and operator functors for running the Graph tests.
 */ 

// includes
#include<cmath>
#include<string>

// defines
#define RATIO 0.46566128e-9

using namespace std;
using namespace wf;

// global variable for the result
atomic<long> global_sum;

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
            value(_value) {}

    // default constructor
    tuple_t():
            key(0),
            id(0),
            ts(0),
            value(0) {}

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

// source functor for generating positive numbers
class Source_Positive_Functor
{
private:
    size_t len; // stream length per key
    size_t keys; // number of keys
    size_t k;
    size_t sent;
    vector<uint64_t> ids;
    vector<int64_t> values;
    uint64_t next_ts;

public:
    // Constructor
    Source_Positive_Functor(size_t _len,
                        size_t _keys):
                        len(_len),
                        keys(_keys),
                        k(0),
                        sent(0),
                        ids(_keys, 0),
                        values(_keys, 0),
                        next_ts(0)
    {
        srand(0);
    }

    bool operator()(tuple_t &t)
    {
        t.setControlFields(k, ids[k]++, next_ts);
        t.value = values[k];
        values[k]++;
        sent++;
        k = (k+1) % keys;
        double x = (1000 * 0.05) / 1.05;
        next_ts += ceil(pareto(1.05, x));
        if (sent < keys*len) {
            return true;
        }
        else {
            return false;
        }
    }
};

// source functor for generating negative numbers
class Source_Negative_Functor
{
private:
    size_t len; // stream length per key
    size_t keys; // number of keys
    size_t k;
    size_t sent;
    vector<uint64_t> ids;
    vector<int64_t> values;
    uint64_t next_ts;

public:
    // Constructor
    Source_Negative_Functor(size_t _len,
                        size_t _keys):
                        len(_len),
                        keys(_keys),
                        k(0),
                        sent(0),
                        ids(_keys, 0),
                        values(_keys, 0),
                        next_ts(0)
    {
        srand(0);
    }

    bool operator()(tuple_t &t)
    {
        t.setControlFields(k, ids[k]++, next_ts);
        t.value = values[k];
        values[k]--;
        sent++;
        k = (k+1) % keys;
        double x = (1000 * 0.05) / 1.05;
        next_ts += ceil(pareto(1.05, x));
        if (sent < keys*len) {
            return true;
        }
        else {
            return false;
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
        return true;
    }
};

// map functor 
class Map_Functor
{
public:
    // operator()
    void operator()(tuple_t &t)
    {
        t.value = t.value;
    }
};

// sink functor
class Sink_Functor
{
private:
    size_t received; // counter of received results
    long totalsum;

public:
    // constructor
    Sink_Functor(size_t _keys): received(0), totalsum(0) {}

    // operator()
    void operator()(optional<tuple_t> &out)
    {
        if (out) {
            received++;
            totalsum += (*out).value;
        }
        else {
            cout << "Received " << received << " results, total sum " << totalsum << endl;
            global_sum.fetch_add(totalsum);
        }
    }
};
