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
 *  Third Test Program of the Union between Multi-MultiPipe instances
 *  
 *  Test program of the union of union of Multi-MultiPipe instances
 */

// include
#include <random>
#include <iostream>
#include <ff/ff.hpp>
#include <windflow.hpp>

// defines
#define RATIO 0.46566128e-9

// global variable for the result
long global_sum;

// generation of pareto-distributed pseudo-random numbers
double pareto(double alpha, double kappa)
{
    double u;
    long seed = random();
    u = (seed) * RATIO;
    return (kappa / pow(u, (1. / alpha)));
}

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

// source functor for generating even numbers
class Source_Even_Functor
{
private:
    size_t len; // stream length per key
    size_t keys; // number of keys
    vector<uint64_t> counters;
    vector<uint64_t> next_ts;

public:
    // constructor
    Source_Even_Functor(size_t _len, size_t _keys): len(_len), keys(_keys), counters(_keys, 0), next_ts(_keys, 0)
    {
        srand(0);
    }

    // operator()
    void operator()(Shipper<tuple_t> &shipper)
    {
        // generation of the input stream
        for (size_t i=0; i<len; i++) {
            for (size_t k=0; k<keys; k++) {
                tuple_t t(k, i, next_ts[k], counters[k]);
                double x = (1000 * 0.05) / 1.05;
                next_ts[k] += ceil(pareto(1.05, x));
                counters[k] += 2;
                //next_ts[k] += 1000;
                shipper.push(t);
            }
        }
    }
};

// source functor for generating odd numbers
class Source_Odd_Functor
{
private:
    size_t len; // stream length per key
    size_t keys; // number of keys
    vector<uint64_t> counters;
    vector<uint64_t> next_ts;

public:
    // constructor
    Source_Odd_Functor(size_t _len, size_t _keys): len(_len), keys(_keys), counters(_keys, 1), next_ts(_keys, 0)
    {
        srand(0);
    }

    // operator()
    void operator()(Shipper<tuple_t> &shipper)
    {
        // generation of the input stream
        for (size_t i=0; i<len; i++) {
            for (size_t k=0; k<keys; k++) {
                tuple_t t(k, i, next_ts[k], counters[k]);
                double x = (1000 * 0.05) / 1.05;
                next_ts[k] += ceil(pareto(1.05, x));
                counters[k] += 2;
                //next_ts[k] += 1000;
                shipper.push(t);
            }
        }
    }
};

// source functor for generating negative numbers
class Source_Negative_Functor
{
private:
    size_t len; // stream length per key
    size_t keys; // number of keys
    vector<int> counters;
    vector<uint64_t> next_ts;

public:
    // constructor
    Source_Negative_Functor(size_t _len, size_t _keys): len(_len), keys(_keys), counters(_keys, 0), next_ts(_keys, 0)
    {
        srand(0);
    }

    // operator()
    void operator()(Shipper<tuple_t> &shipper)
    {
        // generation of the input stream
        for (size_t i=0; i<len; i++) {
            for (size_t k=0; k<keys; k++) {
                tuple_t t(k, i, next_ts[k], counters[k]);
                double x = (1000 * 0.05) / 1.05;
                next_ts[k] += ceil(pareto(1.05, x));
                counters[k] -= 2;
                //next_ts[k] += 1000;
                shipper.push(t);
            }
        }
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
        if (t.value % 3 == 0)
            return true;
        else
            return false;
    }
};

// map functor 1
class Map_Functor1
{
public:
    // operator()
    void operator()(tuple_t &t)
    {
        // double the value
        t.value = t.value * 2;
    }
};

// map functor 2
class Map_Functor2
{
public:
    // operator()
    void operator()(tuple_t &t)
    {
        // double the value
        t.value = t.value * 3;
    }
};

// map functor 3
class Map_Functor3
{
public:
    // operator()
    void operator()(tuple_t &t)
    {
        // double the value
        t.value = t.value * (-2);
    }
};

// map functor 4
class Map_Functor4
{
public:
    // operator()
    void operator()(tuple_t &t)
    {
        // double the value
        t.value = t.value * 4;
    }
};

// sink functor
class Sink_Functor
{
private:
    size_t received; // counter of received results
    long totalsum;

public:
    // constructor
    Sink_Functor(size_t _keys): received(0), totalsum(0) {}

    // operator()
    void operator()(optional<tuple_t> &out)
    {
        if (out) {
            received++;
            totalsum += (*out).value;
        }
        else {
            LOCKED_PRINT("Received " << received << " window results, total sum " << totalsum << endl;)
            global_sum = totalsum;
        }
    }
};

// main
int main(int argc, char *argv[])
{
	int option = 0;
    size_t runs = 1;
    size_t stream_len = 0;
    size_t n_keys = 1;
    // initalize global variable
    global_sum = 0;
    // arguments from command line
    if (argc != 7) {
        cout << argv[0] << " -r [runs] -l [stream_length] -k [n_keys]" << endl;
        exit(EXIT_SUCCESS);
    }
    while ((option = getopt(argc, argv, "r:l:k:")) != -1) {
        switch (option) {
            case 'r': runs = atoi(optarg);
                     break;
            case 'l': stream_len = atoi(optarg);
                     break;
            case 'k': n_keys = atoi(optarg);
                     break;
            default: {
                cout << argv[0] << " -r [runs] -l [stream_length] -k [n_keys]" << endl;
                exit(EXIT_SUCCESS);
            }
        }
    }
    // set random seed
    mt19937 rng;
    rng.seed(std::random_device()());
    size_t min = 1;
    size_t max = 10;
    std::uniform_int_distribution<std::mt19937::result_type> dist6(min, max);
    int map1_degree, map2_degree, filter_degree, map3_degree, map4_degree;
    size_t source1_degree = dist6(rng);
    size_t source2_degree = dist6(rng);
    size_t source3_degree = dist6(rng);
    long last_result = 0;
    // executes the runs
    for (size_t i=0; i<runs; i++) {
        map1_degree = dist6(rng);
        map2_degree = dist6(rng);
        map3_degree = dist6(rng);
        map4_degree = dist6(rng);
        filter_degree = dist6(rng);
        cout << "Run " << i << " Source1(" << source1_degree <<")->Map(" << map1_degree << ")-|" << endl;
        cout << "      Source2(" << source2_degree << ")->Map(" << map2_degree << ")-|" << endl;
        cout << "      Source3(" << source3_degree <<")->Filter(" << filter_degree << ")->Map(" << map3_degree << ")-|->Map(" << map4_degree << ")->Sink(1)" << endl;
        // prepare the first Multi-MultiPipe
        MultiPipe pipe1("pipe1");
        // source 1
        Source_Even_Functor source_functor1(stream_len, n_keys);
        Source source1 = Source_Builder(source_functor1).withName("pipe1_source").withParallelism(source1_degree).build();
        pipe1.add_source(source1);
        // map 1
        Map_Functor1 map_functor1;
        Map map1 = Map_Builder(map_functor1).withName("pipe1_map").withParallelism(map1_degree).build();
        pipe1.chain(map1);

        // prepare the second Multi-MultiPipe
        MultiPipe pipe2("pipe2");
        // source 2
        Source_Odd_Functor source_functor2(stream_len, n_keys);
        Source source2 = Source_Builder(source_functor2).withName("pipe2_source").withParallelism(source2_degree).build();
        pipe2.add_source(source2);
        // map 2
        Map_Functor2 map_functor2;
        Map map2 = Map_Builder(map_functor2).withName("pipe2_map").withParallelism(map2_degree).build();
        pipe2.chain(map2);

        // create the first union
        MultiPipe pipe3 = pipe1.unionMultiPipes("pipe3", pipe2);

        // prepare the fourth Multi-MultiPipe
        MultiPipe pipe4("pipe4");
        // source 3
        Source_Negative_Functor source_functor3(stream_len, n_keys);
        Source source3 = Source_Builder(source_functor3).withName("pipe4_source").withParallelism(source3_degree).build();
        pipe4.add_source(source3);
        // filter
        Filter_Functor filter_functor;
        Filter filter = Filter_Builder(filter_functor).withName("pipe4_filter").withParallelism(filter_degree).build();
        pipe4.chain(filter);
        // map 3
        Map_Functor3 map_functor3;
        Map map3 = Map_Builder(map_functor3).withName("pipe4_map").withParallelism(map3_degree).build();
        pipe4.chain(map3);

        // create the second union
        MultiPipe pipe5 = pipe3.unionMultiPipes("pipe5", pipe4);
        // map 4
        Map_Functor4 map_functor4;
        Map map4 = Map_Builder(map_functor4).withName("pipe5_map").withParallelism(map4_degree).build();
        pipe5.chain(map4);
        // sink
        Sink_Functor sink_functor(n_keys);
        Sink sink = Sink_Builder(sink_functor).withName("pipe5_sink").withParallelism(1).build();
        pipe5.chain_sink(sink);
        // run the application
        pipe5.run_and_wait_end();
        if (i == 0) {
            last_result = global_sum;
            cout << "Result is --> " << GREEN << "OK" << "!!!" << DEFAULT << endl;
        }
        else {
            if (last_result == global_sum) {
                cout << "Result is --> " << GREEN << "OK" << "!!!" << DEFAULT << endl;
            }
            else {
                cout << "Result is --> " << RED << "FAILED" << "!!!" << DEFAULT << endl;
            }
        }
    }
	return 0;
}
