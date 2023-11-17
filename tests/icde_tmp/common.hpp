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
 *  Data types and operator functors used by the synthetic tests of the
 *  FFAT_Windows_GPU operator.
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
    int key;
    int _1;
    float _2;
    int _3;

    // Default Constructor
    __host__ __device__ input_t(): key(0), _1(0), _2(0), _3(0) {}
};

// output tuple (version 1)
struct output_v1_t
{
    int key;
    int _1;

    // Default Constructor
    __host__ __device__ output_v1_t(int _key, uint64_t _wid): key(_key), _1(0) {}

    // Default Constructor
    __host__ __device__ output_v1_t(uint64_t _wid): key(0), _1(0) {}

    // Default Constructor
    __host__ __device__ output_v1_t(): key(0), _1(0) {}
};

// output tuple (version 2)
struct output_v2_t
{
    int key;
    int _1;
    float _2;

    // Default Constructor
    __host__ __device__ output_v2_t(int _key, uint64_t _wid): key(_key), _1(0), _2(0) {}

    __host__ __device__ output_v2_t(uint64_t _wid): key(0), _1(0), _2(0) {}

    // Default Constructor
    __host__ __device__ output_v2_t(): key(0), _1(0), _2(0) {}
};

// output tuple (version 3)
struct output_v3_t
{
    int key;
    int _1;
    float _2;
    int _3;
    float _4;

    // Default Constructor
    __host__ __device__ output_v3_t(int _key, uint64_t _wid): key(_key), _1(0), _2(0), _3(0), _4(0) {}

    // Default Constructor
    __host__ __device__ output_v3_t(uint64_t _wid): key(0), _1(0), _2(0), _3(0), _4(0) {}

    // Default Constructor
    __host__ __device__ output_v3_t(): key(0), _1(0), _2(0), _3(0), _4(0) {}
};

// inizialize the input buffer of tuples
void init_inputBuffer(input_t *_buffer, size_t _size, size_t _num_keys)
{
    std::random_device rd;
    std::mt19937 gen(rd());
    std::random_device rd2;
    std::mt19937 gen2(rd2());
    std::random_device rd3;
    std::mt19937 gen3(rd3());
    std::uniform_int_distribution<> distrib_keys(0, _num_keys-1);
    std::uniform_int_distribution<> distrib_int_values(0, 1000);
    std::uniform_real_distribution<> distrib_fp_values(0, 1000);
    for (size_t i=0; i<_size; i++) {
        _buffer[i].key = distrib_keys(gen);
        _buffer[i]._1 = distrib_int_values(gen2);
        _buffer[i]._2 = distrib_fp_values(gen3);
        _buffer[i]._3 = distrib_int_values(gen2);
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
        uint64_t start_time = current_time_usecs();
        uint64_t timestamp = 0;
        size_t idx = 0;
        while (!endGeneration) {
            timestamp = (current_time_usecs() - start_time);
            shipper.pushWithTimestamp(buffer[idx], timestamp); // send the tuple
            idx = (idx + 1) % size_buffer;
            shipper.setNextWatermark(timestamp);
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
    size_t n_keys; // total number of keys;
    size_t local_n_keys; // total number of keys assigned to this replica

public:
    // Constructor
    Source_Functor_V2(input_t *_buffer,
                      size_t _size_buffer,
                      unsigned long _app_start_time,
                      size_t _n_keys):
                      app_start_time(_app_start_time),
                      generated_tuples(0),
                      buffer(_buffer),
                      size_buffer(_size_buffer),
                      n_keys(_n_keys) {}

    // operator()
    void operator()(Source_Shipper<input_t> &shipper, RuntimeContext &ctx)
    {
        local_n_keys = n_keys / ctx.getParallelism();
        bool endGeneration = false;
        unsigned long current_time = current_time_nsecs();
        uint64_t start_time = current_time_usecs();
        uint64_t timestamp = 0;
        size_t idx = 0;
        while (!endGeneration) {
            timestamp = (current_time_usecs() - start_time);
            input_t t = buffer[idx];
            if (local_n_keys > 0) {
                t.key = (local_n_keys * ctx.getReplicaIndex()) + (t.key % ctx.getParallelism());
            }
            shipper.pushWithTimestamp(std::move(t), timestamp); // send the tuple
            idx = (idx + 1) % size_buffer;
            shipper.setNextWatermark(timestamp);
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

// Source functor (delayed)
class Source_Functor_Delayed
{
private:
    unsigned long app_start_time; // application start time
    unsigned long generated_tuples; // tuples counter
    input_t *buffer; // buffer of tuples to be read
    size_t size_buffer; // number of tuples in the buffer
    unsigned long delay_usec; // average delay of tuples (in microseconds)

public:
    // Constructor
    Source_Functor_Delayed(input_t *_buffer,
                           size_t _size_buffer,
                           unsigned long _app_start_time,
                           unsigned long _delay_usec):
                           buffer(_buffer),
                           size_buffer(_size_buffer),
                           app_start_time(_app_start_time),
                           generated_tuples(0),
                           delay_usec(_delay_usec) {}

    // operator()
    void operator()(Source_Shipper<input_t> &shipper)
    {
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> distrib_delay(0, (2*delay_usec)-1);
        bool endGeneration = false;
        unsigned long current_time = current_time_nsecs();
        uint64_t start_time = current_time_usecs();
        uint64_t timestamp = 0;
        size_t idx = 0;
        while (!endGeneration) {
            timestamp = (current_time_usecs() - start_time);
            shipper.pushWithTimestamp(buffer[idx], timestamp + distrib_delay(gen)); // send the tuple
            idx = (idx + 1) % size_buffer;
            shipper.setNextWatermark(timestamp);
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
template<typename result_t>
class Sink_Functor
{
private:
    size_t received;

public:
    // Constructor
    Sink_Functor(): received(0) {}

    // operator()
    void operator()(optional<result_t> &out)
    {
        if (out) {
            received++;
        }
        else {
            std::cout << "Received " << received << " window results" << std::endl;
        }
    }
};
