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
 *  Data types and operator functors used by the graph tests with persistent operators.
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

    // Constructor
    tuple_t(): key(0), value(0) {}
};

// Struct of the state object
struct state_t
{
    int64_t value;
    int count;

    // Constructor
    state_t(): value(0), count(0) {}
};

// Struct of the window result
struct result_t
{
    size_t key;
    int64_t value;
    uint64_t wid;

    // Constructor I
    result_t(): key(0), value(0), wid(0) {}

    // Constructor II
    result_t(size_t _key,
             uint64_t _wid):
             key(_key),
             value(0),
             wid(_wid) {}
};

// Source functor for generating numbers
class Source_Functor
{
private:
    size_t len; // stream length per key
    size_t keys; // number of keys
    uint64_t next_ts; // next timestamp
    bool generateWS; // true if watermarks must be generated

public:
    // Constructor
    Source_Functor(size_t _len,
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
                tuple_t t;
                t.key = k;
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

// Filter functor (stateless)
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

// Filter functor with keyby distribution (stateless)
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

// Filter functor with keyby distribution (stateful)
class Filter_Functor_KB_State
{
private:
    int mod;
    std::unordered_map<size_t, state_t> hashT;

public:
    // constructor
    Filter_Functor_KB_State(int _mod): mod(_mod) {}

    // operator()
    bool operator()(tuple_t &t, RuntimeContext &rc)
    {
        assert(t.key % rc.getParallelism() == rc.getReplicaIndex());
        auto it = hashT.find(t.key);
        if (it == hashT.end()) {
            auto p = hashT.insert(std::make_pair(t.key, state_t())); // create the state of the key
            it = p.first;
        }
        if (t.value % mod == 0) {
            ((*it).second).value += t.value;
            ((*it).second).count++;
            t.value = ((*it).second).value;
            return true;
        }
        else {
            return false;
        }
    }
};

// P_Filter functor with keyby distribution
class P_Filter_Functor_KB
{
private:
    int mod;

public:
    // constructor
    P_Filter_Functor_KB(int _mod): mod(_mod) {}

    // operator()
    bool operator()(tuple_t &t, state_t &state, RuntimeContext &rc)
    {
        assert(t.key % rc.getParallelism() == rc.getReplicaIndex());
        if (t.value % mod == 0) {
            state.value += t.value;
            state.count++;
            t.value = state.value;
            return true;
        }
        else {
            return false;
        }
    }
};

// Map functor (stateless)
class Map_Functor
{
public:
    // operator()
    void operator()(tuple_t &t)
    {
        t.value = t.value + 2;
    }
};

// Map functor with keyby distribution (stateless)
class Map_Functor_KB
{
public:
    // operator()
    void operator()(tuple_t &t, RuntimeContext &rc)
    {
        assert(t.key % rc.getParallelism() == rc.getReplicaIndex());
        t.value = t.value + 2;
    }
};

// Map functor with keyby distribution (stateful)
class Map_Functor_KB_State
{
private:
    std::unordered_map<size_t, state_t> hashT;

public:
    // operator()
    void operator()(tuple_t &t, RuntimeContext &rc)
    {
        assert(t.key % rc.getParallelism() == rc.getReplicaIndex());
        auto it = hashT.find(t.key);
        if (it == hashT.end()) {
            auto p = hashT.insert(std::make_pair(t.key, state_t())); // create the state of the key
            it = p.first;
        }
        ((*it).second).value += t.value;
        ((*it).second).count++;
        t.value = ((*it).second).value;
    }
};

// P_Map functor with keyby distribution
class P_Map_Functor_KB
{
public:
    // operator()
    void operator()(tuple_t &t, state_t &state, RuntimeContext &rc)
    {
        assert(t.key % rc.getParallelism() == rc.getReplicaIndex());
        state.value += t.value;
        state.count++;
        t.value = state.value;
    }
};

// FlatMap functor (stateless)
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

// FlatMap functor with keyby distribution (stateless)
class FlatMap_Functor_KB
{
public:
    // operator()
    void operator()(const tuple_t &t, Shipper<tuple_t> &shipper, RuntimeContext &rc)
    {
        assert(t.key % rc.getParallelism() == rc.getReplicaIndex());
        for (size_t i=0; i<3; i++) {
            shipper.push(t);
        }
    }
};

// FlatMap functor with keyby distribution (stateful)
class FlatMap_Functor_KB_State
{
private:
    std::unordered_map<size_t, state_t> hashT;

public:
    // operator()
    void operator()(const tuple_t &t, Shipper<tuple_t> &shipper, RuntimeContext &rc)
    {
        assert(t.key % rc.getParallelism() == rc.getReplicaIndex());
        auto it = hashT.find(t.key);
        if (it == hashT.end()) {
            auto p = hashT.insert(std::make_pair(t.key, state_t())); // create the state of the key
            it = p.first;
        }
        ((*it).second).value += t.value;
        ((*it).second).count++;
        tuple_t result = t;
        result.value = ((*it).second).value;
        shipper.push(std::move(result));
    }
};

// P_FlatMap functor with keyby distribution
class P_FlatMap_Functor_KB
{
public:
    // operator()
    void operator()(const tuple_t &t, state_t &state, Shipper<tuple_t> &shipper, RuntimeContext &rc)
    {
        assert(t.key % rc.getParallelism() == rc.getReplicaIndex());
        state.value += t.value;
        state.count++;
        tuple_t result = t;
        result.value = state.value;
        shipper.push(std::move(result));
    }
};

// Reduce functor with keyby distribution (stateful)
class Reduce_Functor_KB
{
public:
    // operator()
    void operator()(const tuple_t &t, tuple_t &state, RuntimeContext &rc)
    {
        assert(t.key % rc.getParallelism() == rc.getReplicaIndex());
        state.value += t.value;
        state.key = t.key;
    }
};

// P_Reduce functor with keyby distribution
class P_Reduce_Functor_KB
{
public:
    // operator()
    void operator()(const tuple_t &t, tuple_t &state, RuntimeContext &rc)
    {
        assert(t.key % rc.getParallelism() == rc.getReplicaIndex());
        state.value += t.value;
        state.key = t.key;
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
        }
        else {
            // printf("Received: %ld results, total sum: %ld\n", received, totalsum);
            global_sum.fetch_add(totalsum);
        }
    }
};

// P_Sink functor
class P_Sink_Functor
{
private:
    size_t received; // counter of received results
    long totalsum;

public:
    // Constructor
    P_Sink_Functor():
                 received(0),
                 totalsum(0) {}

    // operator()
    void operator()(optional<tuple_t> &out, state_t &state, RuntimeContext &rc)
    {
        if (out) {
            state.value += (*out).value;
            received++;
            totalsum += (*out).value;
        }
        else {
            // printf("Received: %ld results, total sum: %ld\n", received, totalsum);
            global_sum.fetch_add(totalsum);
        }
    }
};

// Window-based functor (incremental version)
class Win_Functor_INC
{
public:
    // operator() incremental version
    void operator()(const tuple_t &tuple, result_t &result)
    {
        result.value += tuple.value;
    }
};

// Window-based functor (non-incremental version)
class Win_Functor_NINC
{
public:
    // operator() non-incremental version
    void operator()(const Iterable<tuple_t> &win, result_t &result)
    {
        result.value = 0;
        for (size_t i=0; i<win.size(); i++) {
            result.value += win[i].value;
        }
    }
};

// WinSink functor
class WinSink_Functor
{
private:
    size_t received; // counter of received results
    long totalsum;

public:
    // Constructor
    WinSink_Functor():
                    received(0),
                    totalsum(0) {}

    // operator()
    void operator()(optional<result_t> &out, RuntimeContext &rc)
    {
        if (out) {
            received++;
            totalsum += (*out).value;
            std::cout << "Ricevuto risultato finestra wid: " << (*out).wid << ", chiave: " << (*out).key << ", valore: " << (*out).value << std::endl;
        }
        else {
            // printf("Received: %ld results, total sum: %ld\n", received, totalsum);
            global_sum.fetch_add(totalsum);
        }
    }
};
