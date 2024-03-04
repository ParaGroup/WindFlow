/**************************************************************************************
 *  Copyright (c) 2023- Gabriele Mencagli and Yuriy Rymarchuk
 *  
 *  This file is part of WindFlow.
 *  
 *  WindFlow is free software dual licensed under the GNU LGPL or MIT License.
 *  You can redistribute it and/or modify it under the terms of the
 *    * GNU Lesser General Public License as published by
 *      the Free Software Foundation, either version 3 of the License, or
 *      (at your option) any later version
 *    OR
 *    * MIT License: https://github.com/ParaGroup/WindFlow/blob/vers3.x/LICENSE.MIT
 *  
 *  WindFlow is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Lesser General Public License for more details.
 *  You should have received a copy of the GNU Lesser General Public License and
 *  the MIT License along with WindFlow. If not, see <http://www.gnu.org/licenses/>
 *  and <http://opensource.org/licenses/MIT/>.
 **************************************************************************************
 */

/*  
 *  Data types and operator functors used by the graph tests.
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
    int64_t value;
};

#if 1
template<>
struct std::hash<tuple_t>
{
    size_t operator()(const tuple_t &t) const
    {
        size_t h1 = std::hash<int>()(t.value);
        size_t h2 = std::hash<int>()(t.key);
        return h1 ^ h2;
    }
};
#endif

struct res_t
{
    size_t key;
    int64_t value;
    size_t from;
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
        generator.seed(1234);
        std::uniform_int_distribution<int> distribution(0, 250);
        for (size_t i=1; i<=len; i++) { // generation loop
            for (size_t k=0; k<keys; k++) {
                tuple_t t;
                t.key = k;
                t.value = i;
                shipper.pushWithTimestamp(std::move(t), next_ts);
                if (generateWS) {
                    shipper.setNextWatermark(next_ts);
                }
                auto offset = (distribution(generator)+1);
                next_ts += offset*1000; // in ms
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
        generator.seed(4321);
        std::uniform_int_distribution<int> distribution(0, 250);
        for (size_t i=1; i<=len; i++) { // generation loop
            for (size_t k=0; k<keys; k++) {
                values[k]--;
                tuple_t t;
                t.key = k;
                t.value = values[k];
                shipper.pushWithTimestamp(std::move(t), next_ts);
                if (generateWS) {
                    shipper.setNextWatermark(next_ts);
                }
                auto offset = (distribution(generator)+1);
                next_ts += offset*1000; // in ms
            }
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

// Join functor
class Join_Functor
{
public:
    // operator()
    optional<tuple_t> operator()(const tuple_t &a, const tuple_t &b)
    {
        tuple_t out;
        out.value = a.value - b.value;
        out.key = a.key;
        return out;
    }
};

// Distinct Join functor
class Distinct_Join_Functor
{
public:
    // operator()
    optional<tuple_t> operator()(const tuple_t &a, const tuple_t &b)
    {
        if (a.value != b.value) {
            tuple_t out;
            out.value = a.value * b.value;
            out.key = a.key;
            return out;
        }
        return {};
    }
};

// Filter functor with keyby distribution
class Filter_Functor_KB
{
private:
    int mod;

public:
    // constructor
    Filter_Functor_KB(int _mod): mod(_mod) {}

    // operator()
    bool operator()(tuple_t &t, RuntimeContext &rc)
    {
        assert(t.key % rc.getParallelism() == rc.getReplicaIndex());
        if (t.value % mod == 0) {
            return true;
        }
        else {
            return false;
        }
    }
};

// Filter functor
class Filter_Functor
{
private:
    int mod;

public:
    // constructor
    Filter_Functor(int _mod): mod(_mod) {}

    // operator()
    bool operator()(tuple_t &t)
    {
        if (t.value % mod == 0) {
            return true;
        }
        else {
            return false;
        }
    }
};

// FlatMap functor 
class FlatMap_Functor
{
public:
    // operator()
    void operator()(const tuple_t &t, Shipper<tuple_t> &shipper)
    {
        for (size_t i=0; i<2; i++) {
            shipper.push(t);
        }
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
    void operator()(optional<tuple_t> &out, RuntimeContext &rc)
    {
        if (out) {
            received++;
            totalsum += (*out).value;
            //printf("%lu %lu\n", (*out).key, rc.getCurrentTimestamp());
        }
        else {
            //printf("Received: %ld results, total sum: %ld\n", received, totalsum);
            global_sum.fetch_add(totalsum);
        }
    }
};
