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
 *  @brief Accumulator pattern executing "rolling" reduce/fold functions on data streams
 *  
 *  @section Accumulator (Description)
 *  
 *  This file implements the Accumulator pattern able to execute "rolling" reduce/fold
 *  functions on data streams.
 *  
 *  The template parameters tuple_t and result_t must be default constructible, with a copy
 *  Constructor and copy assignment operator, and thet must provide and implement the
 *  setControlFields() and getControlFields() methods.
 */ 

#ifndef ACCUMULATOR_H
#define ACCUMULATOR_H

// includes
#include <string>
#include <unordered_map>
#include <ff/node.hpp>
#include <ff/farm.hpp>
#include <context.hpp>
#include <standard.hpp>

using namespace ff;
using namespace std;

/** 
 *  \class Accumulator
 *  
 *  \brief Accumulator pattern executing "rolling" reduce/fold functions on data streams
 *  
 *  This class implements the Accumulator pattern able to execute "rolling" reduce/fold
 *  functions on data streams.
 */ 
template<typename tuple_t, typename result_t>
class Accumulator: public ff_farm
{
public:
    /// type of the reduce/fold function
    using acc_func_t = function<void(const tuple_t &, result_t &)>;
    /// type of the rich reduce/fold function
    using rich_acc_func_t = function<void(const tuple_t &, result_t &, RuntimeContext &)>;
    /// type of the closing function
    using closing_func_t = function<void(RuntimeContext &)>;
    /// type of the function to map the key hashcode onto an identifier starting from zero to pardegree-1
    using routing_func_t = function<size_t(size_t, size_t)>;
private:
    tuple_t tmp; // never used
    // key data type
    using key_t = typename remove_reference<decltype(std::get<0>(tmp.getControlFields()))>::type;
    // friendships with other classes in the library
    friend class MultiPipe;
    // class Accumulator_Node
    class Accumulator_Node: public ff_node_t<tuple_t, result_t>
    {
    private:
        acc_func_t acc_func; // reduce/fold function
        rich_acc_func_t rich_acc_func; // rich reduce/fold function
        closing_func_t closing_func; // closing function
        string name; // string of the unique name of the pattern
        bool isRich; // flag stating whether the function to be used is rich (i.e. it receives the RuntimeContext object)
        RuntimeContext context; // RuntimeContext instance
        result_t init_value; // initial value of the results
        // inner struct of a key descriptor
        struct Key_Descriptor
        {
            result_t result;

            // Constructor
            Key_Descriptor(result_t _init_value): result(_init_value) {}
        };
        // hash table that maps key values onto key descriptors
        unordered_map<key_t, Key_Descriptor> keyMap;
#if defined(LOG_DIR)
        unsigned long rcvTuples = 0;
        double avg_td_us = 0;
        double avg_ts_us = 0;
        volatile unsigned long startTD, startTS, endTD, endTS;
        ofstream *logfile = nullptr;
#endif
    public:
        // Constructor I
        Accumulator_Node(acc_func_t _acc_func, result_t _init_value, string _name, RuntimeContext _context, closing_func_t _closing_func): acc_func(_acc_func), init_value(_init_value), name(_name), isRich(false), context(_context), closing_func(_closing_func) {}

        // Constructor II
        Accumulator_Node(rich_acc_func_t _rich_acc_func, result_t _init_value, string _name, RuntimeContext _context, closing_func_t _closing_func): rich_acc_func(_rich_acc_func), init_value(_init_value), name(_name), isRich(true), context(_context), closing_func(_closing_func) {}

        // svc_init method (utilized by the FastFlow runtime)
        int svc_init()
        {
#if defined(LOG_DIR)
            logfile = new ofstream();
            name += "_node_" + to_string(ff_node_t<tuple_t, result_t>::get_my_id()) + ".log";
            string filename = string(STRINGIFY(LOG_DIR)) + "/" + name;
            logfile->open(filename);
#endif
            return 0;
        }

        // svc method (utilized by the FastFlow runtime)
        result_t *svc(tuple_t *t)
        {
#if defined (LOG_DIR)
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
                keyMap.insert(make_pair(key, Key_Descriptor(init_value)));
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
#if defined(LOG_DIR)
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
     *  \param _func reduce/fold function
     *  \param _init_value initial value to be used by the fold function (for reduce the initial value is the one obtained by the default Constructor of result_t)
     *  \param _pardegree parallelism degree of the Accumulator pattern
     *  \param _name string with the unique name of the Accumulator pattern
     *  \param _closing_func closing function
     *  \param _routing_func function to map the key hashcode onto an identifier starting from zero to pardegree-1
     */ 
    Accumulator(acc_func_t _func, result_t _init_value, size_t _pardegree, string _name, closing_func_t _closing_func, routing_func_t _routing_func)
    {
        // check the validity of the parallelism degree
        if (_pardegree == 0) {
            cerr << RED << "WindFlow Error: parallelism degree cannot be zero" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // vector of Accumulator_Node instances
        vector<ff_node *> w;
        for (size_t i=0; i<_pardegree; i++) {
            auto *seq = new Accumulator_Node(_func, _init_value, _name, RuntimeContext(_pardegree, i), _closing_func);
            w.push_back(seq);
        }
        ff_farm::add_emitter(new Standard_Emitter<tuple_t>(_routing_func));
        ff_farm::add_workers(w);
        // add default collector
        ff_farm::add_collector(nullptr);
        // when the Accumulator will be destroyed we need aslo to destroy the emitter, workers and collector
        ff_farm::cleanup_all();
    }

    /** 
     *  \brief Constructor II
     *  
     *  \param _func rich reduce/fold function
     *  \param _init_value initial value to be used by the fold function (for reduce the initial value is the one obtained by the default Constructor of result_t)
     *  \param _pardegree parallelism degree of the Accumulator pattern
     *  \param _name string with the unique name of the Accumulator pattern
     *  \param _closing_func closing function
     *  \param _routing_func function to map the key hashcode onto an identifier starting from zero to pardegree-1
     */ 
    Accumulator(rich_acc_func_t _func, result_t _init_value, size_t _pardegree, string _name, closing_func_t _closing_func, routing_func_t _routing_func)
    {
        // check the validity of the parallelism degree
        if (_pardegree == 0) {
            cerr << RED << "WindFlow Error: parallelism degree cannot be zero" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // vector of Accumulator_Node instances
        vector<ff_node *> w;
        for (size_t i=0; i<_pardegree; i++) {
            auto *seq = new Accumulator_Node(_func, _init_value, _name, RuntimeContext(_pardegree, i), _closing_func);
            w.push_back(seq);
        }
        ff_farm::add_emitter(new Standard_Emitter<tuple_t>(_routing_func));
        ff_farm::add_workers(w);
        // add default collector
        ff_farm::add_collector(nullptr);
        // when the Accumulator will be destroyed we need aslo to destroy the emitter, workers and collector
        ff_farm::cleanup_all();
    }
};

#endif