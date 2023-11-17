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
 *  Data types and operator functors used by the test for the comparison
 *  between WindFlow and Saber.
 *  
 *  We report below some configuration parameters used by Saber (experiment Fig. 11b):
 *  batch size -> 1048576
 *  winType -> CB
 *  window length -> 1024
 *  window slide -> variable from 2 to 1024
 *  attributes per tuple (except timestamp) -> 6 (float, int, ..., int)
 *  groups -> 0
 *  tuples_per_insert -> 32768
 */ 

// includes
#include<cmath>
#include<string>
#include<fstream>

using namespace std;
using namespace wf;

// global variables
extern atomic<long> sent_tuples;

// input tuple
struct input_t
{
    long t;
    float _1;
    int _2;
    int _3;
    int _4;
    int _5;
    int _6;

    // Default Constructor
    __host__ __device__ input_t(): t(0), _1(0), _2(0), _3(0), _4(0), _5(0), _6(0) {}

} __attribute__((aligned(1)));

// output tuple
struct output_t
{
    long t;
    float _1;
    int _2;

    // Default Constructor
    __host__ __device__ output_t(): t(0), _1(0), _2(0) {}

    // Constructor
    __host__ __device__ output_t(uint64_t _id) {}
} __attribute__((aligned(1)));

// class Lift_Functor on GPU
class Lift_Functor_GPU
{
public:
    // operator()
    __host__ __device__ void operator()(const input_t &tuple, output_t &result)
    {
        result.t = tuple.t;
        result._1 = tuple._1;
        result._2 = tuple._2;
    }
};

// Combine functor on GPU
class Comb_Functor_GPU
{
public:
    // operator()
    __host__ __device__ void operator()(const output_t &input1, const output_t &input2, output_t &result)
    {
        result.t = (input1.t < input2.t) ? input1.t : input2.t;
        result._1 = input1._1 + input2._1;
        result._2 = input1._2 + input2._2;
    }
};

// function to inizialize the input buffer of tuples
void init_inputBuffer(input_t *_buffer, size_t _size)
{
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<float> dis(0, 1.0);
    for (size_t i=0; i<_size; i++) {
        _buffer[i].t = 1;
        _buffer[i]._1 = dis(gen);
        _buffer[i]._2 = 1;
        _buffer[i]._3 = 1;
        _buffer[i]._4 = 1;
        _buffer[i]._5 = 1;
        _buffer[i]._6 = 1;
    }
}

// Source functor
class Source_Functor
{
private:
    unsigned long app_start_time; // application start time
    unsigned long generated_tuples; // tuples counter
    input_t *buffer; // buffer of tuples to be read
    size_t size_buffer; // number of tuples in the buffer

public:
    // Constructor
    Source_Functor(input_t *_buffer,
                   size_t _size_buffer,
                   unsigned long _app_start_time):
                   buffer(_buffer),
                   size_buffer(_size_buffer),
                   app_start_time(_app_start_time),
                   generated_tuples(0) {}

    // operator()
    void operator()(Source_Shipper<input_t> &shipper)
    {
        bool endGeneration = false;
        unsigned long current_time = current_time_nsecs();
        uint64_t timestamp = 0;
        size_t idx = 0;
        while (!endGeneration) {
            shipper.pushWithTimestamp(buffer[idx], timestamp); // send the tuple
            idx = (idx + 1) % size_buffer;
            shipper.setNextWatermark(timestamp);
            timestamp++;
            generated_tuples++;
            if (generated_tuples % 10000 == 0) {
                current_time = current_time_nsecs();
            }
            // check EOS
            if (current_time - app_start_time >= 120e9) {
                sent_tuples.fetch_add(generated_tuples);
                endGeneration = true;
            }
        }
    }
};

// Source functor (Version 2)
class Source_Functor_V2
{
private:
    unsigned long app_start_time; // application start time
    unsigned long generated_tuples; // tuples counter
    input_t *buffer; // buffer of tuples to be read
    size_t size_buffer; // number of tuples in the buffer
    size_t delay; // delay to generate timestamps

public:
    // Constructor
    Source_Functor_V2(input_t *_buffer,
                      size_t _size_buffer,
                      unsigned long _app_start_time,
                      size_t _delay):
                      buffer(_buffer),
                      size_buffer(_size_buffer),
                      app_start_time(_app_start_time),
                      delay(_delay),
                      generated_tuples(0) {}

    // operator()
    void operator()(Source_Shipper<input_t> &shipper)
    {
        bool endGeneration = false;
        unsigned long current_time = current_time_nsecs();
        uint64_t timestamp = 0;
        size_t idx = 0;
        while (!endGeneration) {
            shipper.pushWithTimestamp(buffer[idx], timestamp); // send the tuple
            idx = (idx + 1) % size_buffer;
            shipper.setNextWatermark(timestamp);
            timestamp += delay;
            generated_tuples++;
            if (generated_tuples % 10000 == 0) {
                current_time = current_time_nsecs();
            }
            // check EOS
            if (current_time - app_start_time >= 120e9) {
                sent_tuples.fetch_add(generated_tuples);
                endGeneration = true;
            }
        }
    }
};

// Sink functor
class Sink_Functor
{
private:
    size_t received;

public:
    // Constructor
    Sink_Functor(): received(0) {}

    // operator()
    void operator()(optional<output_t> &out)
    {
        if (out) {
            received++;
        }
    }
};
