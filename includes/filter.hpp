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
 *  The template argument tuple_t must be default constructible, with a copy constructor
 *  and copy assignment operator, and it must provide and implement the setInfo()
 *  and getInfo() methods.
 */ 

#ifndef FILTER_H
#define FILTER_H

// includes
#include <string>
#include <ff/node.hpp>
#include <ff/farm.hpp>
#include <context.hpp>
#include <standard.hpp>

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
    /// Type of the rich predicate function
    using rich_filter_func_t = function<bool(tuple_t &, RuntimeContext)>;
    /// function type to map the key onto an identifier starting from zero to pardegree-1
    using f_routing_t = function<size_t(size_t, size_t)>;
private:
    // friendships with other classes in the library
    friend class MultiPipe;
    bool keyed; // flag stating whether the Filter is configured with keyBy or not
    // class Filter_Node
    class Filter_Node: public ff_node_t<tuple_t>
    {
    private:
        filter_func_t filter_func; // filter function (predicate)
        rich_filter_func_t rich_filter_func; // rich filter function (predicate)
        string name; // string of the unique name of the pattern
        bool isRich; // flag stating whether the function to be used is rich (i.e. it receives the RuntimeContext object)
        RuntimeContext context; // RuntimeContext instance
#if defined(LOG_DIR)
        unsigned long rcvTuples = 0;
        double avg_td_us = 0;
        double avg_ts_us = 0;
        volatile unsigned long startTD, startTS, endTD, endTS;
        ofstream *logfile = nullptr;
#endif
    public:
        // Constructor I
        Filter_Node(filter_func_t _filter_func, string _name): filter_func(_filter_func), name(_name), isRich(false) {}

        // Constructor II
        Filter_Node(rich_filter_func_t _rich_filter_func, string _name, RuntimeContext _context): rich_filter_func(_rich_filter_func), name(_name), isRich(true), context(_context) {}

        // svc_init method (utilized by the FastFlow runtime)
        int svc_init()
        {
#if defined(LOG_DIR)
            logfile = new ofstream();
            name += "_node_" + to_string(ff_node_t<tuple_t>::get_my_id()) + ".log";
            string filename = string(STRINGIFY(LOG_DIR)) + "/" + name;
            logfile->open(filename);
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
            bool predicate;
            if (!isRich)
                predicate = filter_func(*t);
            else
                predicate = rich_filter_func(*t, context);
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
            *logfile << stream.str();
            logfile->close();
            delete logfile;
#endif
        }
    };

public:
    /** 
     *  \brief Constructor I
     *  
     *  \param _func filter function (boolean predicate)
     *  \param _pardegree parallelism degree of the Filter pattern
     *  \param _name string with the unique name of the Filter pattern
     */ 
    Filter(filter_func_t _func, size_t _pardegree, string _name): keyed(false)
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
        // add emitter
        ff_farm::add_emitter(new standard_emitter<tuple_t>());
        // add workers
        ff_farm::add_workers(w);
        // add default collector
        ff_farm::add_collector(nullptr);
        // when the Filter will be destroyed we need aslo to destroy the emitter, workers and collector
        ff_farm::cleanup_all();
    }

    /** 
     *  \brief Constructor II
     *  
     *  \param _func filter function (boolean predicate)
     *  \param _pardegree parallelism degree of the Filter pattern
     *  \param _name string with the unique name of the Filter pattern
     *  \param _routing routing function for the key-based distribution
     */ 
    Filter(filter_func_t _func, size_t _pardegree, string _name, f_routing_t _routing): keyed(true)
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
        // add emitter
        ff_farm::add_emitter(new standard_emitter<tuple_t>(_routing));
        // add workers
        ff_farm::add_workers(w);
        // add default collector
        ff_farm::add_collector(nullptr);
        // when the Filter will be destroyed we need aslo to destroy the emitter, workers and collector
        ff_farm::cleanup_all();
    }

    /** 
     *  \brief Constructor III
     *  
     *  \param _func rich filter function (boolean predicate)
     *  \param _pardegree parallelism degree of the Filter pattern
     *  \param _name string with the unique name of the Filter pattern
     */ 
    Filter(rich_filter_func_t _func, size_t _pardegree, string _name): keyed(false)
    {
        // check the validity of the parallelism degree
        if (_pardegree == 0) {
            cerr << RED << "WindFlow Error: parallelism degree cannot be zero" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // vector of Filter_Node instances
        vector<ff_node *> w;
        for (size_t i=0; i<_pardegree; i++) {
            auto *seq = new Filter_Node(_func, _name, RuntimeContext(_pardegree, i));
            w.push_back(seq);
        }
        // add emitter
        ff_farm::add_emitter(new standard_emitter<tuple_t>());
        // add workers
        ff_farm::add_workers(w);
        // add default collector
        ff_farm::add_collector(nullptr);
        // when the Filter will be destroyed we need aslo to destroy the emitter, workers and collector
        ff_farm::cleanup_all();
    }

    /** 
     *  \brief Constructor IV
     *  
     *  \param _func rich filter function (boolean predicate)
     *  \param _pardegree parallelism degree of the Filter pattern
     *  \param _name string with the unique name of the Filter pattern
     *  \param _routing routing function for the key-based distribution
     */ 
    Filter(rich_filter_func_t _func, size_t _pardegree, string _name, f_routing_t _routing): keyed(true)
    {
        // check the validity of the parallelism degree
        if (_pardegree == 0) {
            cerr << RED << "WindFlow Error: parallelism degree cannot be zero" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // vector of Filter_Node instances
        vector<ff_node *> w;
        for (size_t i=0; i<_pardegree; i++) {
            auto *seq = new Filter_Node(_func, _name, RuntimeContext(_pardegree, i));
            w.push_back(seq);
        }
        // add emitter
        ff_farm::add_emitter(new standard_emitter<tuple_t>(_routing));
        // add workers
        ff_farm::add_workers(w);
        // add default collector
        ff_farm::add_collector(nullptr);
        // when the Filter will be destroyed we need aslo to destroy the emitter, workers and collector
        ff_farm::cleanup_all();
    }

    /** 
     *  \brief Check whether the Filter has been instantiated with a key-based distribution or not
     *  \return true if the Filter is configured with keyBy
     */
    bool isKeyed() { return keyed; }
};

#endif
