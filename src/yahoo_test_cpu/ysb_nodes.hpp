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
 *  Classes of the Yahoo! Streaming Benchmark (WindFlow version)
 *  
 *  This version of the Yahoo! Streaming Benchmark is the one modified in the
 *  StreamBench project available in GitHub:
 *  https://github.com/lsds/StreamBench
 */ 

#ifndef YSB_NODES
#define YSB_NODES

// include
#include <tuple>
#include <unordered_map>
#include <ff/ff.hpp>
#include <windflow.hpp>
#include <yahoo_app.hpp>
#include <campaign_generator.hpp>

using namespace ff;

// global variable: starting time of the execution
volatile unsigned long start_time_usec;

// global variable: number of generated events
atomic<long> sentCounter;

// global variable: number of received results
atomic<long> rcvResults;

// global variable: sum of the latency values
atomic<long> latency_sum;

// global variable: vector of latency results and its mutex
vector<long> latency_values;
mutex mutex_latency;

// Source functor
class YSBSource
{
private:
    unsigned long execution_time_sec; // total execution time of the benchmark
    unsigned long **ads_arrays;
    unsigned int adsPerCampaign; // number of ads per campaign
    size_t num_sent;
    volatile unsigned long current_time_us;
    unsigned int value;

public:
    // constructor
    YSBSource(unsigned long _time_sec, unsigned long **_ads_arrays, unsigned int _adsPerCampaign):
              execution_time_sec(_time_sec), ads_arrays(_ads_arrays), adsPerCampaign(_adsPerCampaign), num_sent(0), value(0) {}

    // destructor
    ~YSBSource() {}

#if 0
    // generation function
    void operator()(Shipper<event_t> &shipper)
    {
        // generation loop
        while (true) {
            current_time_us = current_time_usecs();
            double elapsed_time_sec = (current_time_us - start_time_usec) / 1000000.0;
            if (elapsed_time_sec >= execution_time_sec)
                break;
            else {
                // generation of a new tuple
                event_t out;
                // fill the tuple's fields
                out.ts = current_time_usecs() - start_time_usec;
                out.user_id = 0; // not meaningful
                out.page_id = 0; // not meaningful
                out.ad_id = ads_arrays[(value % 100000) % (N_CAMPAIGNS * adsPerCampaign)][1];
                out.ad_type = (value % 100000) % 5;
                out.event_type = (value % 100000) % 3;
                out.ip = 1; // not meaningful
                value++;
                shipper.push(out);
                num_sent++;
            }
        }
        cout << "[EventSource] Generated " << num_sent << " events" << endl;
    }
#endif

    // generation function
    bool operator()(event_t &event)
    {
        current_time_us = current_time_usecs();
        // fill the event's fields
        event.ts = current_time_usecs() - start_time_usec;
        event.user_id = 0; // not meaningful
        event.page_id = 0; // not meaningful
        event.ad_id = ads_arrays[(value % 100000) % (N_CAMPAIGNS * adsPerCampaign)][1];
        event.ad_type = (value % 100000) % 5;
        event.event_type = (value % 100000) % 3;
        event.ip = 1; // not meaningful
        value++;
        num_sent++;
        double elapsed_time_sec = (current_time_us - start_time_usec) / 1000000.0;
        if (elapsed_time_sec >= execution_time_sec) {
            //cout << "[EventSource] Generated " << num_sent << " events" << endl;
            sentCounter.fetch_add(num_sent);
            return false;
        }
        else
            return true;
    }
};

// Filter functor
class YSBFilter
{
private:
    unsigned int event_type; // forward only tuples with event_type

public:
    // constructor
    YSBFilter(unsigned int _event_type=0): event_type(_event_type) {}

    // destrutor
    ~YSBFilter() {}

    // filter function
    bool operator()(event_t &event)
    {
        if(event.event_type == event_type)
            return true;
        else {
            return false;
        }        
    }
};

// Projector functor
class YSBProject
{
private:
    // here...

public:
    // constructor
    YSBProject() {}

    // destrutor
    ~YSBProject() {}

    // project function
    void operator()(const event_t &event, projected_event_t &out)
    {
        out.ts = event.ts;
        out.ad_id = event.ad_id;
    }
};

// Join functor
class YSBJoin
{
private:
    unordered_map<unsigned long, unsigned int> &map; // hashmap
    campaign_record *relational_table; // relational table

public:
    // constructor
    YSBJoin(unordered_map<unsigned long, unsigned int> &_map, campaign_record *_relational_table):
            map(_map), relational_table(_relational_table) {}

    // destrutor
    ~YSBJoin() {}

    // join function
    void operator()(const event_t &event, Shipper<joined_event_t> &shipper)
    {
        // check inside the hashmap
        auto it = map.find(event.ad_id);
        if (it == map.end()) {
            return;
        }
        else {
            joined_event_t *out = new joined_event_t();
            out->ts = event.ts;
            out->ad_id = event.ad_id;
            campaign_record record = relational_table[(*it).second];
            out->relational_ad_id = record.ad_id;
            out->cmp_id = record.cmp_id;
            shipper.push(out);
        }        
    }
};

// Sink functor
class YSBSink
{
private:
    size_t received;
    unsigned long avgLatencyUs;
    vector<long> local_latencies;

public:
    // constructor
    YSBSink(): received(0), avgLatencyUs(0) {}

    // destrutor
    ~YSBSink() {}

    // sink function
    void operator()(optional<win_result> &res)
    {
        if (res) {
            if ((*res).count > 0) {
                received++;
                auto info = (*res).getControlFields();
                // update the latency
                long latency = current_time_usecs() - ((*res).lastUpdate + start_time_usec);
                avgLatencyUs += latency;
                local_latencies.push_back(latency);
            }
        }
        else {
            int value_lat = (int) avgLatencyUs;
            latency_sum.fetch_add(value_lat);
            rcvResults.fetch_add(received);
            // write latency results in the global vector
            /*
            mutex_latency.lock();
            for (auto lat: local_latencies) {
                latency_values.push_back(lat);
            }
            mutex_latency.unlock();
            */
        }
    }
};

#endif
