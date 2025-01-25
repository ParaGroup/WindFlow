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
 *    * MIT License: https://github.com/ParaGroup/WindFlow/blob/master/LICENSE.MIT
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

// include
#include<random>
#include<iostream>
#include<windflow.hpp>

using namespace std;
using namespace wf;

// global variable for the result
atomic<long> sent_tuples;

// Struct of the input tuple
struct tuple_t
{
    int64_t key;
    int64_t value;
};

// Source functor for generating positive numbers
class Source_Functor
{
private:
    uint64_t next_ts; // next timestamp
    unsigned long app_start_time;
    unsigned long current_time;
    long generated_tuples;
    size_t batch_size;

public:
    // Constructor
    Source_Functor(const unsigned long _app_start_time,
                   size_t _batch_size):
                   next_ts(0),
                   batch_size(_batch_size),
                   app_start_time(_app_start_time),
                   current_time(_app_start_time),
                   generated_tuples(0) {}

    // operator()
    void operator()(Source_Shipper<tuple_t> &shipper)
    {
        // static thread_local std::mt19937 generator;
        // std::uniform_int_distribution<int> distribution(0, 500);
        current_time = current_time_nsecs(); // get the current time
        while (current_time - app_start_time <= 60e9) { // generation loop
            tuple_t t;
            t.key = 10;
            t.value = 10;
            shipper.pushWithTimestamp(std::move(t), next_ts);
            shipper.setNextWatermark(next_ts);
            auto offset = 250; // (distribution(generator)+1);
            next_ts += offset;
            generated_tuples++;
            if ((batch_size > 0) && (generated_tuples % (100000 * batch_size) == 0)) {
                current_time = current_time_nsecs(); // get the new current time
            }
            if (batch_size == 0) {
                current_time = current_time_nsecs(); // get the new current time
            }
        }
        sent_tuples.fetch_add(generated_tuples); // save the number of generated tuples
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
    void operator()(optional<reference_wrapper<tuple_t>> out, RuntimeContext &rc)
    {
        if (out) {
            received++;
            totalsum += ((*out).get()).value;
        }
    }
};

// main
int main(int argc, char *argv[])
{
    int option = 0;
    // initalize global variable
    sent_tuples = 0;
    size_t pardegree = 1;
    size_t batch_size = 0;
    // arguments from command line
    if (argc != 5) {
        cout << argv[0] << " -n [par] -b  [batch_size]" << endl;
        exit(EXIT_SUCCESS);
    }
    while ((option = getopt(argc, argv, "n:b:")) != -1) {
        switch (option) {
            case 'n': pardegree = atoi(optarg);
                     break;
            case 'b': batch_size = atoi(optarg);
                     break;
            default: {
                cout << argv[0] << " -n [par] -b  [batch_size]" << endl;
                exit(EXIT_SUCCESS);
            }
        }
    }
    // prepare the test
    PipeGraph graph("test_simple", Execution_Mode_t::DEFAULT, Time_Policy_t::EVENT_TIME);
    // application starting time
    unsigned long app_start_time = current_time_nsecs();
    // prepare the first MultiPipe
    Source_Functor source_functor(app_start_time, batch_size);
    Source source = Source_Builder(source_functor)
                        .withName("source1")
                        .withParallelism(pardegree)
                        .withOutputBatchSize(batch_size)
                        .build();
    MultiPipe &pipe = graph.add_source(source);
    Sink_Functor sink_functor;
    Sink sink = Sink_Builder(sink_functor)
                    .withName("sink")
                    .withParallelism(pardegree)
                    .build();
    pipe.chain_sink(sink);
    /// evaluate topology execution time
    volatile unsigned long start_time_main_usecs = current_time_usecs();
    graph.run();
    volatile unsigned long end_time_main_usecs = current_time_usecs();
    double elapsed_time_seconds = (end_time_main_usecs - start_time_main_usecs) / (1000000.0);
    double throughput = sent_tuples / elapsed_time_seconds;
    cout << "Measured throughput: " << (int) throughput << " tuples/second" << endl;
    return 0;
}
