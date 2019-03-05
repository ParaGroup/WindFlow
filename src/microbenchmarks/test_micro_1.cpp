/******************************************************************************
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
 *  First microbenchmark
 *  
 *  Multi-Pipe instance with Source->Map->Filter->FlatMap->Sink operators
 */

// include
#include <random>
#include <iostream>
#include <ff/ff.hpp>
#include <windflow.hpp>

// global variable: number of generated tuples
atomic<int> sentCounter;

// global variable: number of received results
atomic<int> rcvResults;

// global variable: sum of the latency values
atomic<long> latency_sum;

// struct of the input tuple
struct tuple_t
{
    size_t key;
    uint64_t id;
    uint64_t ts;
    int value;

    // constructor
    tuple_t(size_t _key, uint64_t _id, uint64_t _ts, int _value): key(_key), id(_id), ts(_ts), value(_value) {}

    // default constructor
    tuple_t(): key(0), id(0), ts(0), value(0) {}

    // getInfo method
    tuple<size_t, uint64_t, uint64_t> getInfo() const
    {
        return tuple<size_t, uint64_t, uint64_t>(key, id, ts);
    }

    // setInfo method
    void setInfo(size_t _key, uint64_t _id, uint64_t _ts)
    {
        key = _key;
        id = _id;
        ts = _ts;
    }
};

// source functor
class Source_Functor
{
private:
    size_t idx;
    unsigned long execution_time_sec;
    volatile unsigned long start_time_ns;

public:
    // constructor
    Source_Functor(unsigned long _execution_time_sec, unsigned long _start_time_ns): idx(0), execution_time_sec(_execution_time_sec), start_time_ns(_start_time_ns) {}

    // operator()
    bool operator()(tuple_t &t)
    {
        t.key = 0;
        t.id = idx;
        t.value = idx++;
        t.ts = current_time_nsecs() - start_time_ns;
        double elapsed_time_sec = (current_time_nsecs() - start_time_ns) / 1000000000.0;
        if (elapsed_time_sec >= execution_time_sec) {
            sentCounter.fetch_add(idx);
            return false;
        }
        else
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
        // double the value
        t.value = t.value + 1;
    }
};

// filter functor
class Filter_Functor
{
public:
    // operator()
    bool operator()(tuple_t &t)
    {
        // drop odd numbers
        if (t.id % 2 == 0)
            return true;
        else
            return false;
    }
};

// flatmap functor
class FlatMap_Functor
{
public:
    void operator()(const tuple_t &t, Shipper<tuple_t> &shipper)
    {
        tuple_t t1 = t;
        t1.value = t1.value * 2;
        shipper.push(t1);
        tuple_t t2 = t;
        t2.value= t2.value * 3;
        shipper.push(t2);
    }
};

// sink functor
class Sink_Functor
{
private:
    size_t received; // counter of received results
    unsigned long avgLatencyNs;
    volatile unsigned long start_time_ns;

public:
    // constructor
    Sink_Functor(unsigned long _start_time_ns): received(0), avgLatencyNs(0), start_time_ns(_start_time_ns) {}

    // operator()
    void operator()(optional<tuple_t> &out)
    {
        if (out) {
            received++;
            // update the latency
            avgLatencyNs += current_time_nsecs() - ((*out).ts + start_time_ns);
        }
        else {
            long value_lat = (long) avgLatencyNs;
            latency_sum.fetch_add(value_lat);
            rcvResults.fetch_add(received);
        }
    }
};

// main
int main(int argc, char *argv[])
{
	int option = 0;
    unsigned long exec_time_sec = 0;
    size_t pardegree = 1;
    // initialize global variables
    sentCounter = 0;
    rcvResults = 0;
    latency_sum = 0;
    // arguments from command line
    if (argc != 5) {
        cout << argv[0] << " -l [execution_seconds] -n [pardegree]" << endl;
        exit(EXIT_SUCCESS);
    }
    while ((option = getopt(argc, argv, "l:n:")) != -1) {
        switch (option) {
            case 'l': exec_time_sec = atoi(optarg);
                     break;
            case 'n': pardegree = atoi(optarg);
                     break;
            default: {
                cout << argv[0] << " -l [execution_seconds] -n [pardegree]" << endl;
                exit(EXIT_SUCCESS);
            }
        }
    }
    // prepare the Multi-Pipe
    Pipe pipe("micro_1");
    // set the starting time of the application
    volatile unsigned long start_time_ns = current_time_nsecs();
    // source
    Source_Functor source_functor(exec_time_sec, start_time_ns);
    Source source = Source_Builder(source_functor).withName("micro_1_source").withParallelism(pardegree).build();
    pipe.add_source(source);
    // map
    Map_Functor map_functor;
    Map map = Map_Builder(map_functor).withName("micro_1_map").withParallelism(pardegree).build();
    pipe.chain(map);
    // filter
    Filter_Functor filter_functor;
    Filter filter = Filter_Builder(filter_functor).withName("micro_1_filter").withParallelism(pardegree).build();
    pipe.chain(filter);
    // flatmap
    FlatMap_Functor flatmap_functor;
    FlatMap flatmap = FlatMap_Builder(flatmap_functor).withName("micro_1_flatmap").withParallelism(pardegree).build();
    pipe.chain(flatmap);
    // sink
    Sink_Functor sink_functor(start_time_ns);
    Sink sink = Sink_Builder(sink_functor).withName("micro_1_sink").withParallelism(pardegree).build();
    pipe.chain_sink(sink);
    // run the application
    pipe.run_and_wait_end();
    volatile unsigned long end_time_main_ns = current_time_nsecs();
    double elapsed_time_sec = (end_time_main_ns - start_time_ns) / (1000000000.0);
    cout << "[Main] Total generated messages are " << sentCounter << endl;
    cout << "[Main] Total received results are " << rcvResults << endl;
    cout << "[Main] Latency (nsec) " << ((double) latency_sum)/rcvResults << endl;
    cout << "[Main] Total elapsed time (seconds) " << elapsed_time_sec << endl;
	return 0;
}
