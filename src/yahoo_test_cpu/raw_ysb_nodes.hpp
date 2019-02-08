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
 *  Classes of the Yahoo! Streaming Benchmark (FastFlow+WindFlow version)
 *  
 *  This version of the Yahoo! Streaming Benchmark is the one modified in the
 *  StreamBench project available in GitHub:
 *  https://github.com/lsds/StreamBench
 */ 

#ifndef RAW_YSB_NODES
#define RAW_YSB_NODES

// include
#include <tuple>
#include <unordered_map>
#include <ff/node.hpp>
#include <yahoo_app.hpp>
#include <campaign_generator.hpp>

using namespace ff;

// global variable starting time of the execution
volatile unsigned long start_time_usec;

// source operator
class EventSource: public ff_node_t<event_t, event_t>
{
private:
    unsigned long execution_time_sec; // total execution time of the benchmark
    unsigned long **ads_arrays;
    unsigned int adsPerCampaign; // number of ads per campaign
    size_t num_sent;

public:
    // constructor
    EventSource(unsigned long _time_sec, unsigned long **_ads_arrays, unsigned int _adsPerCampaign): execution_time_sec(_time_sec), ads_arrays(_ads_arrays), adsPerCampaign(_adsPerCampaign), num_sent(0) {}

    // destructor
    ~EventSource() {}

    // svc_init
    int svc_init()
    {
        //cout << "[EventSource] started..." << endl;
        return 0;
    }

    // svc
    event_t *svc(event_t *)
    {
        volatile unsigned long current_time_us;
        unsigned int value = 0;
        // generation loop
        while (true) {
            current_time_us = current_time_usecs();
            double elapsed_time_sec = (current_time_us - start_time_usec) / 1000000.0;
            if (elapsed_time_sec >= execution_time_sec)
                break;
            else {
                // generation of a new tuple
                event_t *out = new event_t();
                // fill the tuple's fields
                out->ts = current_time_usecs() - start_time_usec;
                out->user_id = 0; // not meaningful
                out->page_id = 0; // not meaningful
                out->ad_id = ads_arrays[(value % 100000) % (100 * adsPerCampaign)][1];
                out->ad_type = (value % 100000) % 5;
                out->event_type = (value % 100000) % 3;
                out->ip = 1; // not meaningful
                value++;
                ff_send_out(out);
                num_sent++;
            }
        }
        return this->EOS;
    }

    // svc_end
    void svc_end()
    {
        //cout << "[EventSource] ...end" << endl;
        cout << "[EventSource] Generated " << num_sent << " events" << endl;
    }
};

// filter operator
class RawFilter: public ff_node_t<event_t, event_t>
{
private:
    unsigned int event_type; // forward only tuples with event_type

public:
    // constructor
    RawFilter(unsigned int _event_type=0): event_type(_event_type) {}

    // destrutor
    ~RawFilter() {}

    // svc_init
    int svc_init()
    {
        //cout << "[Filter] started..." << endl;
        return 0;
    }

    // svc
    event_t *svc(event_t *t)
    {
        if(t->event_type == event_type)
            return t;
        else {
            delete t;
            return this->GO_ON;
        }
    }

    // svc_end
    void svc_end()
    {
        //cout << "[Filter] ...end" << endl;
    }
};

// projector operator
class Project: public ff_node_t<event_t, projected_event_t>
{
private:
    // here...

public:
    // constructor
    Project() {}

    // destrutor
    ~Project() {}

    // svc_init
    int svc_init()
    {
        //cout << "[Project] started..." << endl;
        return 0;
    }

    // svc
    projected_event_t *svc(event_t *t)
    {
        projected_event_t *out = new projected_event_t();
        out->ts = t->ts;
        out->ad_id = t->ad_id;
        delete t;
        return out;
    }

    // svc_end
    void svc_end()
    {
        //cout << "[Project] ...end" << endl;
    }
};

// join operator
class Join: public ff_node_t<projected_event_t, joined_event_t>
{
private:
    unordered_map<unsigned long, unsigned int> &map; // hashmap
    campaign_record *relational_table; // relational table

public:
    // constructor
    Join(unordered_map<unsigned long, unsigned int> &_map, campaign_record *_relational_table): map(_map), relational_table(_relational_table) {}

    // destrutor
    ~Join() {}

    // svc_init
    int svc_init()
    {
        //cout << "[Join] started..." << endl;
        return 0;
    }

    // svc
    joined_event_t *svc(projected_event_t *t)
    {
        // check inside the hashmap
        auto it = map.find(t->ad_id);
        if (it == map.end()) {
            delete t;
            return this->GO_ON; // no output
        }
        else {
            joined_event_t *out = new joined_event_t();
            out->ts = t->ts;
            out->ad_id = t->ad_id;
            campaign_record record = relational_table[(*it).second];
            out->relational_ad_id = record.ad_id;
            out->cmp_id = record.cmp_id;
            delete t;
            return out;
        }
    }

    // svc_end
    void svc_end()
    {
        //cout << "[Join] ...end" << endl;
    }
};

// Sink operator
class RawSink: public ff_node_t<win_result, win_result>
{
private:
    size_t received;
    unsigned long avgLatencyUs;

public:
    // constructor
    RawSink(): received(0), avgLatencyUs(0) {}

    // destrutor
    ~RawSink() {}

    // svc_init
    int svc_init()
    {
        //cout << "[Sink] started..." << endl;
        return 0;
    }

    // svc
    win_result *svc(win_result *r)
    {
        received++;
        auto info = r->getInfo();
        // update the latency
        avgLatencyUs += current_time_usecs() - (r->lastUpdate + start_time_usec);
        //cout << "[Sink] Ricevuto risultato per cmp_id: " << std::get<0>(info) << " wid: " << std::get<1>(info) << " ts: " << std::get<2>(info) << endl;
        delete r;
        return this->GO_ON;
    }

    // svc_end
    void svc_end()
    {
        cout << "[Sink] Received " << received << " results, average latency (usec) " << avgLatencyUs/((double) received) << endl;
    }
};

#endif
