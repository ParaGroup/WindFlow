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
 *  @file    filter.hpp
 *  @author  Gabriele Mencagli
 *  @date    10/01/2019
 *  
 *  @brief Filter pattern dropping data items not respecting a given predicate
 *  
 *  @section Filter (Description)
 *  
 *  This file implements the Filter pattern able to drop all the input items that do not
 *  respect a given predicate given by the user.
 *  
 *  The template argument tuple_t must be default constructible, with a copy constructor and copy assignment
 *  operator, and it must provide and implement the setInfo() and getInfo() methods.
 */ 

#ifndef FILTER_H
#define FILTER_H

// includes
#include <string>
#include <ff/node.hpp>
#include <ff/farm.hpp>
#include <builders.hpp>

using namespace ff;

/** 
 *  \class Filter
 *  
 *  \brief Filter pattern dropping data items not respecting a given predicate
 *  
 *  This class implements the Filter pattern applying a given predicate to all the input
 *  items and dropping out all of them for which the predicate evaluates to false.
 */ 
template<typename tuple_t>
class Filter: public ff_farm
{
public:
    /// Type of the predicate function
    using filter_func_t = function<bool(tuple_t &)>;
private:
    // class Filter_Node
    class Filter_Node: public ff_node_t<tuple_t>
    {
    private:
        filter_func_t filter_func; // filter function (predicate)
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
        Filter_Node(filter_func_t _filter_func, string _name): filter_func(_filter_func), name(_name) {}

        // svc_init method (utilized by the FastFlow runtime)
        int svc_init()
        {
#if defined(LOG_DIR)
            name += "_node_" + to_string(ff_node_t<tuple_t>::get_my_id()) + ".log";
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
            // evaluate the predicate on the input item
            bool predicate = filter_func(*t);
#if defined(LOG_DIR)
            endTS = current_time_nsecs();
            endTD = current_time_nsecs();
            double elapsedTS_us = ((double) (endTS - startTS)) / 1000;
            avg_ts_us += (1.0 / rcvTuples) * (elapsedTS_us - avg_ts_us);
            double elapsedTD_us = ((double) (endTD - startTD)) / 1000;
            avg_td_us += (1.0 / rcvTuples) * (elapsedTD_us - avg_td_us);
            startTD = current_time_nsecs();
#endif
            if (!predicate) {
                delete t;
                return this->GO_ON;
            }
            else
                return t;
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
     *  \param _func filter function (boolean predicate)
     *  \param _pardegree parallelism degree of the Filter pattern
     *  \param _name string with the unique name of the Filter pattern
     */ 
    Filter(filter_func_t _func, size_t _pardegree, string _name)
    {
        // check the validity of the parallelism degree
        if (_pardegree == 0) {
            cerr << RED << "WindFlow Error: parallelism degree cannot be zero" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // vector of Filter_Node instances
        vector<ff_node *> w;
        for (size_t i=0; i<_pardegree; i++) {
            auto *seq = new Filter_Node(_func, _name);
            w.push_back(seq);
        }
        ff_farm::add_workers(w);
        // add default collector
        ff_farm::add_collector(nullptr);
        // when the Filter will be destroyed we need aslo to destroy the emitter, workers and collector
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
