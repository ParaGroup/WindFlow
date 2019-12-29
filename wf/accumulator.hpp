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
 *  @file    accumulator.hpp
 *  @author  Gabriele Mencagli
 *  @date    13/02/2019
 *  
 *  @brief Accumulator operator executing "rolling" reduce/fold functions on data streams
 *  
 *  @section Accumulator (Description)
 *  
 *  This file implements the Accumulator operator able to execute "rolling" reduce/fold
 *  functions on data streams.
 *  
 *  The template parameters tuple_t and result_t must be default constructible, with a copy
 *  constructor and copy assignment operator, and thet must provide and implement the
 *  setControlFields() and getControlFields() methods.
 */ 

#ifndef ACCUMULATOR_H
#define ACCUMULATOR_H

/// includes
#include <string>
#include <iostream>
#include <unordered_map>
#include <ff/node.hpp>
#include <ff/pipeline.hpp>
#include <ff/farm.hpp>
#include <basic.hpp>
#include <context.hpp>
#include <standard_nodes.hpp>

namespace wf {

/** 
 *  \class Accumulator
 *  
 *  \brief Accumulator operator executing "rolling" reduce/fold functions on data streams
 *  
 *  This class implements the Accumulator operator able to execute "rolling" reduce/fold
 *  functions on data streams.
 */ 
template<typename tuple_t, typename result_t>
class Accumulator: public ff::ff_farm
{
public:
    /// type of the reduce/fold function
    using acc_func_t = std::function<void(const tuple_t &, result_t &)>;
    /// type of the rich reduce/fold function
    using rich_acc_func_t = std::function<void(const tuple_t &, result_t &, RuntimeContext &)>;
    /// type of the closing function
    using closing_func_t = std::function<void(RuntimeContext &)>;
    /// type of the function to map the key hashcode onto an identifier starting from zero to pardegree-1
    using routing_func_t = std::function<size_t(size_t, size_t)>;

private:
    tuple_t tmp; // never used
    // key data type
    using key_t = typename std::remove_reference<decltype(std::get<0>(tmp.getControlFields()))>::type;
    // friendships with other classes in the library
    friend class MultiPipe;
    bool used; // true if the operator has been added/chained in a MultiPipe
    // class Accumulator_Node
    class Accumulator_Node: public ff::ff_node_t<tuple_t, result_t>
    {
private:
        acc_func_t acc_func; // reduce/fold function
        rich_acc_func_t rich_acc_func; // rich reduce/fold function
        closing_func_t closing_func; // closing function
        std::string name; // string of the unique name of the operator
        bool isRich; // flag stating whether the function to be used is rich (i.e. it receives the RuntimeContext object)
        RuntimeContext context; // RuntimeContext
        result_t init_value; // initial value of the results
        // inner struct of a key descriptor
        struct Key_Descriptor
        {
            result_t result;

            // Constructor
            Key_Descriptor(result_t _init_value):
                           result(_init_value)
            {}
        };
        // hash table that maps key values onto key descriptors
        std::unordered_map<key_t, Key_Descriptor> keyMap;
#if defined(TRACE_WINDFLOW)
        unsigned long rcvTuples = 0;
        double avg_td_us = 0;
        double avg_ts_us = 0;
        volatile unsigned long startTD, startTS, endTD, endTS;
        std::ofstream *logfile = nullptr;
#endif

public:
        // Constructor I
        Accumulator_Node(acc_func_t _acc_func,
                        result_t _init_value,
                        std::string _name,
                        RuntimeContext _context,
                        closing_func_t _closing_func):
                        acc_func(_acc_func),
                        init_value(_init_value),
                        name(_name),
                        isRich(false),
                        context(_context),
                        closing_func(_closing_func)
        {}

        // Constructor II
        Accumulator_Node(rich_acc_func_t _rich_acc_func,
                         result_t _init_value,
                         std::string _name,
                         RuntimeContext _context,
                         closing_func_t _closing_func):
                         rich_acc_func(_rich_acc_func),
                         init_value(_init_value),
                         name(_name),
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
            // extract key from the input tuple
            auto key = std::get<0>(t->getControlFields()); // key
            // find the corresponding key descriptor
            auto it = keyMap.find(key);
            if (it == keyMap.end()) {
                // create the descriptor of that key
                keyMap.insert(std::make_pair(key, Key_Descriptor(init_value)));
                it = keyMap.find(key);
            }
            Key_Descriptor &key_d = (*it).second;
            // call the reduce/fold function on the input
            if (!isRich)
                acc_func(*t, key_d.result);
            else
                rich_acc_func(*t, key_d.result, context);
            // copy the result
            result_t *r = new result_t(key_d.result);
#if defined(TRACE_WINDFLOW)
            endTS = current_time_nsecs();
            endTD = current_time_nsecs();
            double elapsedTS_us = ((double) (endTS - startTS)) / 1000;
            avg_ts_us += (1.0 / rcvTuples) * (elapsedTS_us - avg_ts_us);
            double elapsedTD_us = ((double) (endTD - startTD)) / 1000;
            avg_td_us += (1.0 / rcvTuples) * (elapsedTD_us - avg_td_us);
            startTD = current_time_nsecs();
#endif
            return r;
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
     *  \param _func reduce/fold function
     *  \param _init_value initial value to be used by the fold function (for reduce the initial value is the one obtained by the default Constructor of result_t)
     *  \param _pardegree parallelism degree of the Accumulator operator
     *  \param _name string with the unique name of the Accumulator operator
     *  \param _closing_func closing function
     *  \param _routing_func function to map the key hashcode onto an identifier starting from zero to pardegree-1
     */ 
    Accumulator(acc_func_t _func,
                result_t _init_value,
                size_t _pardegree,
                std::string _name,
                closing_func_t _closing_func,
                routing_func_t _routing_func): used(false)
    {
        // check the validity of the parallelism degree
        if (_pardegree == 0) {
            std::cerr << RED << "WindFlow Error: Accumulator has parallelism zero" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // vector of Accumulator_Node
        std::vector<ff_node *> w;
        for (size_t i=0; i<_pardegree; i++) {
            auto *seq = new Accumulator_Node(_func, _init_value, _name, RuntimeContext(_pardegree, i), _closing_func);
            w.push_back(seq);
        }
        ff::ff_farm::add_emitter(new Standard_Emitter<tuple_t>(_routing_func, _pardegree));
        ff::ff_farm::add_workers(w);
        // add default collector
        ff::ff_farm::add_collector(nullptr);
        // when the Accumulator will be destroyed we need aslo to destroy the emitter, workers and collector
        ff::ff_farm::cleanup_all();
    }

    /** 
     *  \brief Constructor II
     *  
     *  \param _func rich reduce/fold function
     *  \param _init_value initial value to be used by the fold function (for reduce the initial value is the one obtained by the default Constructor of result_t)
     *  \param _pardegree parallelism degree of the Accumulator operator
     *  \param _name string with the unique name of the Accumulator operator
     *  \param _closing_func closing function
     *  \param _routing_func function to map the key hashcode onto an identifier starting from zero to pardegree-1
     */ 
    Accumulator(rich_acc_func_t _func,
                result_t _init_value,
                size_t _pardegree,
                std::string _name,
                closing_func_t _closing_func,
                routing_func_t _routing_func): used(false)
    {
        // check the validity of the parallelism degree
        if (_pardegree == 0) {
            std::cerr << RED << "WindFlow Error: Accumulator has parallelism zero" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // vector of Accumulator_Node
        std::vector<ff_node *> w;
        for (size_t i=0; i<_pardegree; i++) {
            auto *seq = new Accumulator_Node(_func, _init_value, _name, RuntimeContext(_pardegree, i), _closing_func);
            w.push_back(seq);
        }
        ff::ff_farm::add_emitter(new Standard_Emitter<tuple_t>(_routing_func, _pardegree));
        ff::ff_farm::add_workers(w);
        // add default collector
        ff::ff_farm::add_collector(nullptr);
        // when the Accumulator will be destroyed we need aslo to destroy the emitter, workers and collector
        ff::ff_farm::cleanup_all();
    }

    /** 
     *  \brief Check whether the Accumulator has been used in a MultiPipe
     *  \return true if the Accumulator has been added/chained to an existing MultiPipe
     */
    bool isUsed() const
    {
        return used;
    }

    /// deleted constructors/operators
    Accumulator(const Accumulator &) = delete; // copy constructor
    Accumulator(Accumulator &&) = delete; // move constructor
    Accumulator &operator=(const Accumulator &) = delete; // copy assignment operator
    Accumulator &operator=(Accumulator &&) = delete; // move assignment operator
};

} // namespace wf

#endif
