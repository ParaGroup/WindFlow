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
 *  Data types and operator functors used by the split tests with GPU operators.
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

// Source functor
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
                    shipper.configureWatermark(next_ts);
                }
                auto offset = (distribution(generator)+1);
                next_ts += offset;
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
        if (t.value % 2 == 0) {
            t.value = t.value + 2;
        }
        else {
            t.value = t.value + 3;
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

// Sink functor (version 2)
class Sink_Functor_v2
{
private:
    size_t received; // counter of received results
    long totalsum;
    int target;

public:
    // Constructor
    Sink_Functor_v2(int _target):
                    received(0),
                    totalsum(0),
                    target(_target) {}

    // operator()
    void operator()(optional<tuple_t> &out)
    {
        if (out) {
            if ((*out).key % 2 != target) {
                abort();
            }
            received++;
            totalsum += (*out).value;
        }
        else {
            // printf("Received: %ld results, total sum: %ld\n", received, totalsum);
            global_sum.fetch_add(totalsum);
        }
    }
};
