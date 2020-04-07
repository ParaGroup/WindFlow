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
 *  The template parameters tuple_t and result_t must be default constructible, with
 *  a copy constructor and copy assignment operator, and they must provide and implement
 *  the setControlFields() and getControlFields() methods.
 */ 

#ifndef FILTER_H
#define FILTER_H

/// includes
#include<string>
#include<iostream>
#include<ff/node.hpp>
#include<ff/pipeline.hpp>
#include<ff/multinode.hpp>
#include<ff/farm.hpp>
#include<basic.hpp>
#include<context.hpp>
#include<standard_emitter.hpp>

namespace wf {

/** 
 *  \class Filter
 *  
 *  \brief Filter operator dropping data items not respecting a given predicate
 *  
 *  This class implements the Filter operator applying a given predicate to all the input
 *  items and dropping out all of them for which the predicate evaluates to false.
 */ 
template<typename tuple_t, typename result_t>
class Filter: public ff::ff_farm
{
public:
    /// type of the predicate function (with boolean return type)
    using filter_func_t = std::function<bool(tuple_t &)>;
    /// type of the rich predicate function (with boolean return type)
    using rich_filter_func_t = std::function<bool(tuple_t &, RuntimeContext &)>;
    /// type of the predicate function (with optional return type)
    using filter_func_opt_t = std::function<std::optional<result_t>(const tuple_t &)>;
    /// type of the rich predicate function (with optional return type)
    using rich_filter_func_opt_t = std::function<std::optional<result_t>(const tuple_t &, RuntimeContext &)>;
    /// type of the predicate function (with optional return type containing a pointer)
    using filter_func_optptr_t = std::function<std::optional<result_t *>(const tuple_t &)>;
    /// type of the rich predicate function (with optional return type containing a pointer)
    using rich_filter_func_optptr_t = std::function<std::optional<result_t *>(const tuple_t &, RuntimeContext &)>;
    /// type of the closing function
    using closing_func_t = std::function<void(RuntimeContext &)>;
    /// type of the function to map the key hashcode onto an identifier starting from zero to pardegree-1
    using routing_func_t = std::function<size_t(size_t, size_t)>;

private:
    // friendships with other classes in the library
    friend class MultiPipe;
    bool used; // true if the operator has been added/chained in a MultiPipe
    bool keyed; // flag stating whether the Filter is configured with keyBy or not
    // class Filter_Node
    class Filter_Node: public ff::ff_minode_t<tuple_t, result_t>
    {
private:
        filter_func_t filter_func; // filter function (with boolean return type)
        rich_filter_func_t rich_filter_func; // rich filter function (with boolean return type)
        filter_func_opt_t filter_func_opt; // filter function (with optional return type)
        rich_filter_func_opt_t rich_filter_func_opt; // rich filter function (with optional return type)
        filter_func_optptr_t filter_func_optptr; // filter function (with optional return type containing a pointer)
        rich_filter_func_optptr_t rich_filter_func_optptr; // rich filter function (with optional return type containing a pointer)
        closing_func_t closing_func; // closing function
        std::string name; // string of the unique name of the operator
        size_t isOPT; // 0 = function returning a boolean, 1 = function returning an optional, 2 = function returning an optional containing a pointer
        bool isRich; // flag stating whether the function to be used is rich (i.e. it receives the RuntimeContext object)
        RuntimeContext context; // RuntimeContext
#if defined(TRACE_WINDFLOW)
        unsigned long rcvTuples = 0;
        double avg_td_us = 0;
        double avg_ts_us = 0;
        volatile unsigned long startTD, startTS, endTD, endTS;
        std::ofstream *logfile = nullptr;
#endif

public:
        // Constructor I
        template<typename T=std::string>
        Filter_Node(typename std::enable_if<std::is_same<T,T>::value && std::is_same<tuple_t,result_t>::value, filter_func_t>::type _filter_func,
                    T _name,
                    RuntimeContext _context,
                    closing_func_t _closing_func):
                    filter_func(_filter_func),
                    name(_name),
                    isOPT(0),
                    isRich(false),
                    context(_context),
                    closing_func(_closing_func)
        {}

        // Constructor II
        template<typename T=std::string>
        Filter_Node(typename std::enable_if<std::is_same<T,T>::value && std::is_same<tuple_t,result_t>::value, rich_filter_func_t>::type _rich_filter_func,
                    T _name,
                    RuntimeContext _context,
                    closing_func_t _closing_func):
                    rich_filter_func(_rich_filter_func),
                    name(_name),
                    isOPT(0),
                    isRich(true),
                    context(_context),
                    closing_func(_closing_func)
        {}

        // Constructor III
        template<typename T=std::string>
        Filter_Node(typename std::enable_if<std::is_same<T,T>::value && !std::is_same<tuple_t,result_t>::value, filter_func_opt_t>::type _filter_func_opt,
                    T _name,
                    RuntimeContext _context,
                    closing_func_t _closing_func):
                    filter_func_opt(_filter_func_opt),
                    name(_name),
                    isOPT(1),
                    isRich(false),
                    context(_context),
                    closing_func(_closing_func)
        {}

        // Constructor IV
        template<typename T=std::string>
        Filter_Node(typename std::enable_if<std::is_same<T,T>::value && !std::is_same<tuple_t,result_t>::value, rich_filter_func_opt_t>::type _rich_filter_func_opt,
                    T _name,
                    RuntimeContext _context,
                    closing_func_t _closing_func):
                    rich_filter_func_opt(_rich_filter_func_opt),
                    name(_name),
                    isOPT(1),
                    isRich(true),
                    context(_context),
                    closing_func(_closing_func)
        {}

        // Constructor V
        template<typename T=std::string>
        Filter_Node(typename std::enable_if<std::is_same<T,T>::value && !std::is_same<tuple_t,result_t>::value, filter_func_optptr_t>::type _filter_func_optptr,
                    T _name,
                    RuntimeContext _context,
                    closing_func_t _closing_func):
                    filter_func_optptr(_filter_func_optptr),
                    name(_name),
                    isOPT(2),
                    isRich(false),
                    context(_context),
                    closing_func(_closing_func)
        {}

        // Constructor VI
        template<typename T=std::string>
        Filter_Node(typename std::enable_if<std::is_same<T,T>::value && !std::is_same<tuple_t,result_t>::value, rich_filter_func_optptr_t>::type _rich_filter_func_optptr,
                    T _name,
                    RuntimeContext _context,
                    closing_func_t _closing_func):
                    rich_filter_func_optptr(_rich_filter_func_optptr),
                    name(_name),
                    isOPT(2),
                    isRich(true),
                    context(_context),
                    closing_func(_closing_func)
        {}

        // svc_init method (utilized by the FastFlow runtime)
        int svc_init()
        {
#if defined(TRACE_WINDFLOW)
            logfile = new std::ofstream();
            name += "_" + std::to_string(this->get_my_id()) + "_" + std::to_string(getpid()) + ".log";
#if defined(LOG_DIR)
            std::string filename = std::string(STRINGIFY(LOG_DIR)) + "/" + name;
            std::string log_dir = std::string(STRINGIFY(LOG_DIR));
#else
            std::string filename = "log/" + name;
            std::string log_dir = std::string("log");
#endif
            // create the log directory
            if (mkdir(log_dir.c_str(), 0777) != 0) {
                struct stat st;
                if((stat(log_dir.c_str(), &st) != 0) || !S_ISDIR(st.st_mode)) {
                    std::cerr << RED << "WindFlow Error: directory for log files cannot be created" << DEFAULT_COLOR << std::endl;
                    exit(EXIT_FAILURE);
                }
            }
            logfile->open(filename);
#endif
            return 0;
        }

        // svc method (utilized by the FastFlow runtime)
        result_t *svc(tuple_t *t)
        {
#if defined(TRACE_WINDFLOW)
            startTS = current_time_nsecs();
            if (rcvTuples == 0)
                startTD = current_time_nsecs();
            rcvTuples++;
#endif
            // we use the version returning a boolean
            if (isOPT == 0) {
                // evaluate the predicate on the input item
                bool predicate;
                if (!isRich) {
                    predicate = filter_func(*t);
                }
                else {
                    predicate = rich_filter_func(*t, context);
                }
#if defined(TRACE_WINDFLOW)
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
                else {
                    return t;
                }
            }
            // we use the version returning an optional
            else if (isOPT == 1) {
                // evaluate the predicate on the input item
                std::optional<result_t> out;
                if (!isRich) {
                    out = filter_func_opt(*t);
                }
                else {
                    out = rich_filter_func_opt(*t, context);
                }
                delete t;
#if defined(TRACE_WINDFLOW)
                endTS = current_time_nsecs();
                endTD = current_time_nsecs();
                double elapsedTS_us = ((double) (endTS - startTS)) / 1000;
                avg_ts_us += (1.0 / rcvTuples) * (elapsedTS_us - avg_ts_us);
                double elapsedTD_us = ((double) (endTD - startTD)) / 1000;
                avg_td_us += (1.0 / rcvTuples) * (elapsedTD_us - avg_td_us);
                startTD = current_time_nsecs();
#endif
                if (!out) {
                    return this->GO_ON;
                }
                else {
                    result_t *result = new result_t();
                    *result = *out;
                    return result;
                }
            }
            // we use the version returning an optional containing a pointer
            else {
                assert(isOPT == 2);
                // evaluate the predicate on the input item
                std::optional<result_t *> out;
                if (!isRich) {
                    out = filter_func_optptr(*t);
                }
                else {
                    out = rich_filter_func_optptr(*t, context);
                }
                delete t;
#if defined(TRACE_WINDFLOW)
                endTS = current_time_nsecs();
                endTD = current_time_nsecs();
                double elapsedTS_us = ((double) (endTS - startTS)) / 1000;
                avg_ts_us += (1.0 / rcvTuples) * (elapsedTS_us - avg_ts_us);
                double elapsedTD_us = ((double) (endTD - startTD)) / 1000;
                avg_td_us += (1.0 / rcvTuples) * (elapsedTD_us - avg_td_us);
                startTD = current_time_nsecs();
#endif
                if (!out) {
                    return this->GO_ON;
                }
                else {
                    return *out;
                }
            }
        }

        // svc_end method (utilized by the FastFlow runtime)
        void svc_end()
        {
            // call the closing function
            closing_func(context);
#if defined(TRACE_WINDFLOW)
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
     *  \param _func function with signature accepted by the Filter operator
     *  \param _pardegree parallelism degree of the Filter operator
     *  \param _name string with the unique name of the Filter operator
     *  \param _closing_func closing function
     */
    template<typename F_t>
    Filter(F_t _func,
           size_t _pardegree,
           std::string _name,
           closing_func_t _closing_func):
           keyed(false),
           used(false)
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
     *  \param _func function with signature accepted by the Filter operator
     *  \param _pardegree parallelism degree of the Filter operator
     *  \param _name string with the unique name of the Filter operator
     *  \param _closing_func closing function
     *  \param _routing_func function to map the key hashcode onto an identifier starting from zero to pardegree-1
     */ 
    template<typename F_t>
    Filter(F_t _func,
           size_t _pardegree,
           std::string _name,
           closing_func_t _closing_func,
           routing_func_t _routing_func):
           keyed(true),
           used(false)
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

    /** 
     *  \brief Check whether the Filter has been used in a MultiPipe
     *  \return true if the Filter has been added/chained to an existing MultiPipe
     */
    bool isUsed() const
    {
        return used;
    }

    /// deleted constructors/operators
    Filter(const Filter &) = delete; // copy constructor
    Filter(Filter &&) = delete; // move constructor
    Filter &operator=(const Filter &) = delete; // copy assignment operator
    Filter &operator=(Filter &&) = delete; // move assignment operator
};

} // namespace wf

#endif
