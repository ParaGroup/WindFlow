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
 *  @brief Filter operator dropping data items not respecting a given predicate
 *  
 *  @section Filter (Description)
 *  
 *  This file implements the Filter operator able to drop all the input items that do not
 *  respect a given predicate given by the user.
 *  
 *  The template parameter tuple_t must be default constructible, with a copy constructor
 *  and copy assignment operator, and it must provide and implement the setControlFields()
 *  and getControlFields() methods.
 */ 

#ifndef FILTER_H
#define FILTER_H

/// includes
#include <string>
#include <iostream>
#include <ff/node.hpp>
#include <ff/pipeline.hpp>
#include <ff/farm.hpp>
#include <basic.hpp>
#include <context.hpp>
#include <standard_nodes.hpp>

namespace wf {

/** 
 *  \class Filter
 *  
 *  \brief Filter operator dropping data items not respecting a given predicate
 *  
 *  This class implements the Filter operator applying a given predicate to all the input
 *  items and dropping out all of them for which the predicate evaluates to false.
 */ 
template<typename tuple_t>
class Filter: public ff::ff_farm
{
public:
    /// type of the predicate function
    using filter_func_t = std::function<bool(tuple_t &)>;
    /// type of the rich predicate function
    using rich_filter_func_t = std::function<bool(tuple_t &, RuntimeContext &)>;
    /// type of the closing function
    using closing_func_t = std::function<void(RuntimeContext &)>;
    /// type of the function to map the key hashcode onto an identifier starting from zero to pardegree-1
    using routing_func_t = std::function<size_t(size_t, size_t)>;

private:
    // friendships with other classes in the library
    friend class MultiPipe;
    bool keyed; // flag stating whether the Filter is configured with keyBy or not
    // class Filter_Node
    class Filter_Node: public ff::ff_node_t<tuple_t>
    {
private:
        filter_func_t filter_func; // filter function (predicate)
        rich_filter_func_t rich_filter_func; // rich filter function (predicate)
        closing_func_t closing_func; // closing function
        std::string name; // string of the unique name of the operator
        bool isRich; // flag stating whether the function to be used is rich (i.e. it receives the RuntimeContext object)
        RuntimeContext context; // RuntimeContext
#if defined(LOG_DIR)
        unsigned long rcvTuples = 0;
        double avg_td_us = 0;
        double avg_ts_us = 0;
        volatile unsigned long startTD, startTS, endTD, endTS;
        std::ofstream *logfile = nullptr;
#endif

public:
        // Constructor I
        Filter_Node(filter_func_t _filter_func,
                    std::string _name,
                    RuntimeContext _context,
                    closing_func_t _closing_func):
                    filter_func(_filter_func),
                    name(_name),
                    isRich(false),
                    context(_context),
                    closing_func(_closing_func)
        {}

        // Constructor II
        Filter_Node(rich_filter_func_t _rich_filter_func, 
                    std::string _name,
                    RuntimeContext _context,
                    closing_func_t _closing_func):
                    rich_filter_func(_rich_filter_func),
                    name(_name), isRich(true),
                    context(_context),
                    closing_func(_closing_func)
        {}

        // svc_init method (utilized by the FastFlow runtime)
        int svc_init()
        {
#if defined(LOG_DIR)
            logfile = new std::ofstream();
            name += "_node_" + std::to_string(ff::ff_node_t<tuple_t>::get_my_id()) + ".log";
            std::string filename = std::string(STRINGIFY(LOG_DIR)) + "/" + name;
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
            // call the closing function
            closing_func(context);
#if defined (LOG_DIR)
            std::ostringstream stream;
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
     *  \param _pardegree parallelism degree of the Filter operator
     *  \param _name string with the unique name of the Filter operator
     *  \param _closing_func closing function
     */ 
    Filter(filter_func_t _func,
           size_t _pardegree,
           std::string _name,
           closing_func_t _closing_func):
           keyed(false)
    {
        // check the validity of the parallelism degree
        if (_pardegree == 0) {
            std::cerr << RED << "WindFlow Error: Filter has parallelism zero" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // vector of Filter_Node
        std::vector<ff_node *> w;
        for (size_t i=0; i<_pardegree; i++) {
            auto *seq = new Filter_Node(_func, _name, RuntimeContext(_pardegree, i), _closing_func);
            w.push_back(seq);
        }
        // add emitter
        ff::ff_farm::add_emitter(new Standard_Emitter<tuple_t>(_pardegree));
        // add workers
        ff::ff_farm::add_workers(w);
        // add default collector
        ff::ff_farm::add_collector(nullptr);
        // when the Filter will be destroyed we need aslo to destroy the emitter, workers and collector
        ff::ff_farm::cleanup_all();
    }

    /** 
     *  \brief Constructor II
     *  
     *  \param _func filter function (boolean predicate)
     *  \param _pardegree parallelism degree of the Filter operator
     *  \param _name string with the unique name of the Filter operator
     *  \param _closing_func closing function
     *  \param _routing_func function to map the key hashcode onto an identifier starting from zero to pardegree-1
     */ 
    Filter(filter_func_t _func,
           size_t _pardegree,
           std::string _name,
           closing_func_t _closing_func,
           routing_func_t _routing_func):
           keyed(true)
    {
        // check the validity of the parallelism degree
        if (_pardegree == 0) {
            std::cerr << RED << "WindFlow Error: Filter has parallelism zero" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // vector of Filter_Node
        std::vector<ff_node *> w;
        for (size_t i=0; i<_pardegree; i++) {
            auto *seq = new Filter_Node(_func, _name, RuntimeContext(_pardegree, i), _closing_func);
            w.push_back(seq);
        }
        // add emitter
        ff::ff_farm::add_emitter(new Standard_Emitter<tuple_t>(_routing_func, _pardegree));
        // add workers
        ff::ff_farm::add_workers(w);
        // add default collector
        ff::ff_farm::add_collector(nullptr);
        // when the Filter will be destroyed we need aslo to destroy the emitter, workers and collector
        ff::ff_farm::cleanup_all();
    }

    /** 
     *  \brief Constructor III
     *  
     *  \param _func rich filter function (boolean predicate)
     *  \param _pardegree parallelism degree of the Filter operator
     *  \param _name string with the unique name of the Filter operator
     *  \param _closing_func closing function
     */ 
    Filter(rich_filter_func_t _func,
           size_t _pardegree,
           std::string _name,
           closing_func_t _closing_func):
           keyed(false)
    {
        // check the validity of the parallelism degree
        if (_pardegree == 0) {
            std::cerr << RED << "WindFlow Error: Filter has parallelism zero" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // vector of Filter_Node
        std::vector<ff_node *> w;
        for (size_t i=0; i<_pardegree; i++) {
            auto *seq = new Filter_Node(_func, _name, RuntimeContext(_pardegree, i), _closing_func);
            w.push_back(seq);
        }
        // add emitter
        ff::ff_farm::add_emitter(new Standard_Emitter<tuple_t>(_pardegree));
        // add workers
        ff::ff_farm::add_workers(w);
        // add default collector
        ff::ff_farm::add_collector(nullptr);
        // when the Filter will be destroyed we need aslo to destroy the emitter, workers and collector
        ff::ff_farm::cleanup_all();
    }

    /** 
     *  \brief Constructor IV
     *  
     *  \param _func rich filter function (boolean predicate)
     *  \param _pardegree parallelism degree of the Filter operator
     *  \param _name string with the unique name of the Filter operator
     *  \param _closing_func closing function
     *  \param _routing_func function to map the key hashcode onto an identifier starting from zero to pardegree-1
     */ 
    Filter(rich_filter_func_t _func,
           size_t _pardegree,
           std::string _name,
           closing_func_t _closing_func,
           routing_func_t _routing_func):
           keyed(true)
    {
        // check the validity of the parallelism degree
        if (_pardegree == 0) {
            std::cerr << RED << "WindFlow Error: Filter has parallelism zero" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // vector of Filter_Node
        std::vector<ff_node *> w;
        for (size_t i=0; i<_pardegree; i++) {
            auto *seq = new Filter_Node(_func, _name, RuntimeContext(_pardegree, i), _closing_func);
            w.push_back(seq);
        }
        // add emitter
        ff::ff_farm::add_emitter(new Standard_Emitter<tuple_t>(_routing_func, _pardegree));
        // add workers
        ff::ff_farm::add_workers(w);
        // add default collector
        ff::ff_farm::add_collector(nullptr);
        // when the Filter will be destroyed we need aslo to destroy the emitter, workers and collector
        ff::ff_farm::cleanup_all();
    }

    /** 
     *  \brief Check whether the Filter has been instantiated with a key-based distribution or not
     *  \return true if the Filter is configured with keyBy
     */
    bool isKeyed() const
    {
        return keyed;
    }
};

} // namespace wf

#endif
