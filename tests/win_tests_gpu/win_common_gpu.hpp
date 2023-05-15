/**************************************************************************************
 *  Copyright (c) 2019- Gabriele Mencagli
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
 *  Data types and operator functors used by the tests of window-based operators.
 *  These tests use both CPU and GPU operators.
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
    __host__ __device__ result_t():
                                 key(0),
                                 id(0),
                                 value(0) {}

    // Constructor II
    __host__ __device__  result_t(size_t _key,
                                  uint64_t _id):
                                  key(_key),
                                  id(_id),
                                  value(0) {}
};

// Struct of the state used by Map_GPU operators
struct map_state_t
{
    int64_t counter;

    // Constructor
    __device__ map_state_t():
                           counter(0) {}
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
                    shipper.setNextWatermark(next_ts);
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
                    shipper.setNextWatermark(next_ts);
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

// Map functor on GPU
class Map_Functor_GPU
{
public:
    // operator()
    __host__ __device__ void operator()(tuple_t &t)
    {
        t.value = t.value + 2;
    }
};

// Map functor on GPU with keyby distribution
class Map_Functor_GPU_KB
{
public:
    // operator()
    __host__ __device__ void operator()(tuple_t &t, map_state_t &state)
    {
        state.counter++;
        t.value++;
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
#if 0
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
#if 1
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
class Lift_Functor_GPU
{
public:
    // operator()
    __host__ __device__ void operator()(const tuple_t &tuple, result_t &result)
    {
        result.value = tuple.value;
    }
};

// Combine functor on GPU
class Comb_Functor_GPU
{
public:
    // operator()
    __host__ __device__ void operator()(const result_t &input1, const result_t &input2, result_t &output)
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
    void operator()(optional<result_t> &out, RuntimeContext &rc)
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
