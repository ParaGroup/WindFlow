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
 *  Data types and operator functors used by the merge tests with GPU operators
 *  and keyby distributions.
 */ 

// includes
#include<cmath>
#include<string>

using namespace std;
using namespace wf;

// Global variable for the result
atomic<long> global_sum;

// Global variable for the number of keys
const size_t num_keys = 26;

// Global array of possible keys
const char list_keys[] = {
    'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
    'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z'
};

// Struct of the input tuple
struct tuple_t
{
    char key;
    int64_t value;
};

// Struct of the state used by Map_GPU operators
struct map_state_t
{
    int64_t counter;

    // Constructor
    __device__ map_state_t():
                           counter(0) {}
};

// Struct of the state used by Filter_GPU operators
struct filter_state_t
{
    int64_t counter;

    // Constructor
    __device__ filter_state_t():
                              counter(0) {}
};

// Function to check whether a character is a vowel
__host__ __device__ bool isVowel(char _c)
{
    if ((_c == 'a' || _c == 'e' || _c == 'i' || _c == 'o' || _c == 'u')) {
        return true;
    }
    else {
        return false;
    }
}

// Source functor for generating positive numbers
class Source_Functor
{
private:
    size_t len; // stream length per key
    uint64_t next_ts; // next timestamp
    bool generateWS; // true if watermarks must be generated

public:
    // Constructor
    Source_Functor(size_t _len,
                   bool _generateWS):
                   len(_len),
                   next_ts(0),
                   generateWS(_generateWS) {}

    // operator()
    void operator()(Source_Shipper<tuple_t> &shipper)
    {
        static thread_local std::mt19937 generator;
        std::uniform_int_distribution<int> distribution(0, 500);
        size_t k_id = 0;
        for (size_t i=1; i<=len; i++) { // generation loop
            tuple_t t;
            t.key = list_keys[k_id];
            t.value = i;
            shipper.pushWithTimestamp(std::move(t), next_ts);
            k_id = (k_id + 1) % num_keys;
            if (generateWS) {
                shipper.configureWatermark(next_ts);
            }
            auto offset = (distribution(generator)+1);
            next_ts += offset;
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
        if (t.value % 2 == 0) {
            t.value = t.value + 2;
        }
        else {
            t.value = t.value + 3;
        }
    }
};

// Map functor on GPU
class Map_Functor_GPU
{
public:
    // operator()
    __device__ void operator()(tuple_t &t)
    {
        if (isVowel(t.key)) {
            t.value++;
        }
    }
};

// Map functor on GPU with keyby distribution
class Map_Functor_GPU_KB
{
public:
    // operator()
    __device__ void operator()(tuple_t &t, map_state_t &state)
    {
        if (isVowel(t.key)) {
            state.counter++;
            t.value += state.counter;
        }
        else {
            state.counter--;
            t.value += state.counter;
        }
    }
};

// Filter functor on GPU with keyby distribution
class Filter_Functor_GPU_KB
{
public:
    // operator()
    __device__ bool operator()(tuple_t &t, filter_state_t &state)
    {
        state.counter++;
        if (t.value % 2 == 0) {
            return true;
        }
        else {
            return false;
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
    void operator()(optional<tuple_t> &out)
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
