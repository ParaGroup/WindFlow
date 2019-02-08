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

/** 
 *  @file    sink.hpp
 *  @author  Gabriele Mencagli
 *  @date    11/01/2019
 *  
 *  @brief Sink pattern absorbing the input stream
 *  
 *  @section Sink (Description)
 *  
 *  This file implements the Sink pattern in charge of absorbing the items of
 *  a data stream.
 *  
 *  The template argument tuple_t must be default constructible, with a copy constructor and copy assignment
 *  operator, and it must provide and implement the setInfo() and getInfo() methods.
 */ 

#ifndef SINK_H
#define SINK_H

// includes
#include <string>
#if __cplusplus < 201703L //not C++17
    #include <experimental/optional>
    using namespace std::experimental;
#else
    #include <optional>
#endif
#include <ff/multinode.hpp>
#include <ff/farm.hpp>
#include <builders.hpp>

using namespace ff;

/** 
 *  \class Sink
 *  
 *  \brief Sink pattern absorbing the input stream
 *  
 *  This class implements the Sink pattern absorbing a data stream of items.
 */ 
template<typename tuple_t>
class Sink: public ff_farm
{
public:
    /// type of the sink function
    using sink_func_t = function<void(optional<tuple_t> &)>;
private:
    // class Sink_Node
    class Sink_Node: public ff_monode_t<tuple_t>
    {
    private:
        sink_func_t sink_fun; // sink function
        string name; // string of the unique name of the pattern
#if defined(LOG_DIR)
        unsigned long rcvTuples = 0;
        double avg_td_us = 0;
        double avg_ts_us = 0;
        volatile unsigned long startTD, startTS, endTD, endTS;
        ofstream logfile;
#endif
    public:
        // Constructor
        Sink_Node(sink_func_t _sink_fun, string _name): sink_fun(_sink_fun), name(_name) {}

        // svc_init method (utilized by the FastFlow runtime)
        int svc_init()
        {
#if defined(LOG_DIR)
            name += "_node_" + to_string(ff_monode_t<tuple_t>::get_my_id()) + ".log";
            string filename = string(STRINGIFY(LOG_DIR)) + "/" + name;
            logfile.open(filename);
#endif
            return 0;
        }

        // svc method (utilized by the FastFlow runtime)
        tuple_t *svc(tuple_t *t)
        {
#if defined (LOG_DIR)
            startTS = current_time_nsecs();
            if (rcvTuples == 0)
                startTD = current_time_nsecs();
            rcvTuples++;
#endif
            // create optional to the input tuple
            optional<tuple_t> opt = make_optional(move(*t));
            // call the sink function
            sink_fun(opt);
            // delete the received item
            delete t;
#if defined(LOG_DIR)
            endTS = current_time_nsecs();
            endTD = current_time_nsecs();
            double elapsedTS_us = ((double) (endTS - startTS)) / 1000;
            avg_ts_us += (1.0 / rcvTuples) * (elapsedTS_us - avg_ts_us);
            double elapsedTD_us = ((double) (endTD - startTD)) / 1000;
            avg_td_us += (1.0 / rcvTuples) * (elapsedTD_us - avg_td_us);
            startTD = current_time_nsecs();
#endif           
            return this->GO_ON;
        }

        // method to manage the EOS (utilized by the FastFlow runtime)
        void eosnotify(ssize_t id)
        {
            // create empty optional
            optional<tuple_t> opt;
            // call the sink function for the last time (empty optional)
            sink_fun(opt);
        }

        // svc_end method (utilized by the FastFlow runtime)
        void svc_end()
        {
#if defined (LOG_DIR)
            ostringstream stream;
            stream << "************************************LOG************************************\n";
            stream << "No. of received tuples: " << rcvTuples << "\n";
            stream << "Average service time: " << avg_ts_us << " usec \n";
            stream << "Average inter-departure time: " << avg_td_us << " usec \n";
            stream << "***************************************************************************\n";
            logfile << stream.str();
            logfile.close();
#endif
        }
    };

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _func sink function
     *  \param _pardegree parallelism degree of the Sink pattern
     *  \param _name string with the unique name of the Sink pattern
     */ 
    Sink(sink_func_t _func, size_t _pardegree, string _name)
    {
        // check the validity of the parallelism degree
        if (_pardegree == 0) {
            cerr << RED << "WindFlow Error: parallelism degree cannot be zero" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // vector of Sink_Node instances
        vector<ff_node *> w;
        for (size_t i=0; i<_pardegree; i++) {
            auto *seq = new Sink_Node(_func, _name);
            w.push_back(seq);
        }
        ff_farm::add_workers(w);
        // when the Map will be destroyed we need aslo to destroy the emitter and workers
        ff_farm::cleanup_all();
    }

//@cond DOXY_IGNORE

    // -------------------------------------- deleted methods ----------------------------------------
    template<typename T>
    int add_emitter(T *e)                                                                    = delete;
    template<typename T>
    int add_emitter(const T &e)                                                              = delete;
    template<typename T>
    int change_emitter(T *e, bool cleanup=false)                                             = delete;
    template<typename T>
    int change_emitter(const T &e, bool cleanup=false)                                       = delete;
    void set_ordered(const size_t MemoryElements=DEF_OFARM_ONDEMAND_MEMORY)                  = delete;
    int add_workers(std::vector<ff_node *> &w)                                               = delete;
    int add_collector(ff_node *c, bool cleanup=false)                                        = delete;
    int wrap_around(bool multi_input=false)                                                  = delete;
    int remove_collector()                                                                   = delete;
    void cleanup_workers()                                                                   = delete;
    void cleanup_all()                                                                       = delete;
    bool offload(void *task, unsigned long retry=((unsigned long)-1),
        unsigned long ticks=ff_loadbalancer::TICKS2WAIT)                                     = delete;
    bool load_result(void **task, unsigned long retry=((unsigned long)-1),
        unsigned long ticks=ff_gatherer::TICKS2WAIT)                                         = delete;
    bool load_result_nb(void **task)                                                         = delete;

private:
    using ff_farm::set_scheduling_ondemand;

//@endcond

};

#endif
