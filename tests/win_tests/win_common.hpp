/*******************************************************************************
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
 *  Data types and operator functors used by the tests of window-based operators.
 */ 

// includes
#include<cmath>
#include<string>

using namespace std;
using namespace wf;

// Global variable for the result
atomic<long> global_sum;

// Struct of the input tuple
struct tuple_t
{
    size_t key;
    uint64_t id;
    int64_t value;

    // Constructor I
    tuple_t():
            key(0),
            id(0),
            value(0) {}

    // Constructor II
    tuple_t(size_t _key,
            uint64_t _id):
            key(_key),
            id(_id),
            value(0) {}
};

// Struct of the output result
struct result_t
{
    size_t key;
    uint64_t id;
    int64_t value;

    // Constructor I
    result_t():
             key(0),
             id(0),
             value(0) {}

    // Constructor II
    result_t(size_t _key,
             uint64_t _id):
             key(_key),
             id(_id),
             value(0) {}
};

// Source functor for generating positive numbers
class Source_Positive_Functor
{
private:
    size_t len; // stream length per key
    size_t keys; // number of keys
    uint64_t next_ts; // next timestamp
    bool generateWS; // true if watermarks must be generated

public:
    // Constructor
    Source_Positive_Functor(size_t _len,
                            size_t _keys,
                            bool _generateWS):
                            len(_len),
                            keys(_keys),
                            next_ts(0),
                            generateWS(_generateWS) {}

    // operator()
    void operator()(Source_Shipper<tuple_t> &shipper)
    {
        static thread_local std::mt19937 generator;
        std::uniform_int_distribution<int> distribution(0, 500);
        for (size_t i=1; i<=len; i++) { // generation loop
            for (size_t k=0; k<keys; k++) {
                tuple_t t(k, 0);
                t.value = i;
                shipper.pushWithTimestamp(std::move(t), next_ts);
                if (generateWS) {
                    shipper.configureWatermark(next_ts);
                }
                auto offset = (distribution(generator)+1);
                next_ts += offset;
            }
        }
    }
};

// Source functor for generating negative numbers
class Source_Negative_Functor
{
private:
    size_t len; // stream length per key
    size_t keys; // number of keys
    vector<int> values; // list of values
    uint64_t next_ts; // next timestamp
    bool generateWS; // true if watermarks must be generated

public:
    // Constructor
    Source_Negative_Functor(size_t _len,
                            size_t _keys,
                            bool _generateWS):
                            len(_len),
                            keys(_keys),
                            values(_keys, 0),
                            next_ts(0),
                            generateWS(_generateWS) {}

    // operator()
    void operator()(Source_Shipper<tuple_t> &shipper)
    {
        static thread_local std::mt19937 generator;
        std::uniform_int_distribution<int> distribution(0, 500);
        for (size_t i=1; i<=len; i++) { // generation loop
            for (size_t k=0; k<keys; k++) {
                values[k]--;
                tuple_t t(k, 0);
                t.value = values[k];
                shipper.pushWithTimestamp(std::move(t), next_ts);
                if (generateWS) {
                    shipper.configureWatermark(next_ts);
                }
                auto offset = (distribution(generator)+1);
                next_ts += offset;
            }
        }
    }
};

// Filter functor
class Filter_Functor
{
public:
    // operator()
    bool operator()(tuple_t &t)
    {
        if (t.value % 2 == 0) {
            return true;
        }
        else {
            return false;
        }
    }
};

// Filter functor with keyby distribution
class Filter_Functor_KB
{
public:
    // operator()
    bool operator()(tuple_t &t, RuntimeContext &rc)
    {
        assert(t.key % rc.getParallelism() == rc.getReplicaIndex());
        if (t.value % 2 == 0) {
            return true;
        }
        else {
            return false;
        }
    }
};

// Map functor 
class Map_Functor
{
public:
    // operator()
    void operator()(tuple_t &t)
    {
        t.value = t.value + 2;
    }
};

// FlatMap functor 
class FlatMap_Functor
{
public:
    // operator()
    void operator()(const tuple_t &t, Shipper<tuple_t> &shipper)
    {
        for (size_t i=0; i<3; i++) {
            shipper.push(t);
        }
    }
};

// Window-based functor
class Win_Functor
{
public:
#if 0
    // operator() non-incremental version
    void operator()(const Iterable<tuple_t> &win, result_t &result)
    {
        result.value = 0;
        for (size_t i=0; i<win.size(); i++) {
            result.value += win[i].value;
        }
    }
#else
    // operator() incremental version
    void operator()(const tuple_t &tuple, result_t &result)
    {
        result.value += tuple.value;
    }
#endif
};

// Stage1 functor
class Stage1_Functor
{
public:
#if 1
    // operator() non-incremental version
    void operator()(const Iterable<tuple_t> &win, tuple_t &result)
    {
        result.value = 0;
        for (size_t i=0; i<win.size(); i++) {
            result.value += win[i].value;
        }
    }
#else
    // operator() incremental version
    void operator()(const tuple_t &tuple, tuple_t &result)
    {
        result.value += tuple.value;
    }
#endif
};

// Stage2 functor
class Stage2_Functor
{
public:
#if 0
    // operator() non-incremental version
    void operator()(const Iterable<tuple_t> &win, result_t &result)
    {
        result.value = 0;
        for (size_t i=0; i<win.size(); i++) {
            result.value += win[i].value;
        }
    }
#else
    // operator() incremental version
    void operator()(const tuple_t &tuple, result_t &result)
    {
        result.value += tuple.value;
    }
#endif
};

// Lift functor
class Lift_Functor
{
public:
    // operator()
    void operator()(const tuple_t &tuple, result_t &result)
    {
        result.value = tuple.value;
    }
};

// Combine functor
class Comb_Functor
{
public:
    // operator()
    void operator()(const result_t &input1, const result_t &input2, result_t &output)
    {
        output.value = input1.value + input2.value;
    }
};

// Sink functor
class Sink_Functor
{
private:
    size_t received; // counter of received results
    long totalsum;

public:
    // Constructor
    Sink_Functor():
                 received(0),
                 totalsum(0) {}

    // operator()
    void operator()(optional<result_t> &out)
    {
        if (out) {
            received++;
            totalsum += (*out).value;
        }
        else {
            // printf("Received: %ld results, total sum: %ld\n", received, totalsum);
            global_sum.fetch_add(totalsum);
        }
    }
};
