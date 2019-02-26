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
 *  @brief Accumulator executing "rolling" reduce/fold functions on a data stream
 *  
 *  @section Accumulator (Description)
 *  
 *  This file implements the Accumulator pattern able to execute "rolling" reduce/fold
 *  functions on a data stream.
 *  
 *  The template arguments tuple_t and result_t must be default constructible, with a copy
 *  constructor and copy assignment operator, and thet must provide and implement the
 *  setInfo() and getInfo() methods.
 */ 

#ifndef ACCUMULATOR_H
#define ACCUMULATOR_H

// includes
#include <string>
#include <unordered_map>
#include <ff/node.hpp>
#include <ff/farm.hpp>
#include <ff/multinode.hpp>
#include <context.hpp>

using namespace ff;

//@cond DOXY_IGNORE

// class Accumulator_Emitter
template<typename tuple_t>
class Accumulator_Emitter: public ff_monode_t<tuple_t>
{
private:
    // function type to map the key onto an identifier starting from zero to pardegree-1
    using f_routing_t = function<size_t(size_t, size_t)>;
    // friendships with other classes in the library
    template<typename T1, typename T2>
    friend class Accumulator;
    friend class Pipe;
    f_routing_t routing; // routing function
    size_t pardegree; // parallelism degree (number of inner patterns)

    // private constructor
    Accumulator_Emitter(f_routing_t _routing, size_t _pardegree):
                        routing(_routing), pardegree(_pardegree) {}

    // svc_init method (utilized by the FastFlow runtime)
    int svc_init()
    {
        return 0;
    }

    // svc method (utilized by the FastFlow runtime)
    tuple_t *svc(tuple_t *t)
    {
        // extract the key from the input tuple
        size_t key = std::get<0>(t->getInfo()); // key
        // send the tuple to the proper destination
        this->ff_send_out_to(t, routing(key, pardegree));
        return this->GO_ON;
    }

    // svc_end method (FastFlow runtime)
    void svc_end() {}
};

//@endcond

/** 
 *  \class Accumulator
 *  
 *  \brief Accumulator pattern executing "rolling" reduce/fold functions on a data stream
 *  
 *  This class implements the Accumulator pattern able to execute "rolling" reduce/fold
 *  functions on a data stream.
 */ 
template<typename tuple_t, typename result_t>
class Accumulator: public ff_farm
{
public:
    /// reduce/fold function
    using acc_func_t = function<void(const tuple_t &, result_t &)>;
    /// rich reduce/fold function
    using rich_acc_func_t = function<void(const tuple_t &, result_t &, RuntimeContext)>;
    /// function type to map the key onto an identifier starting from zero to pardegree-1
    using f_routing_t = function<size_t(size_t, size_t)>;
private:
    // friendships with other classes in the library
    friend class Pipe;
    // class Accumulator_Node
    class Accumulator_Node: public ff_node_t<tuple_t, result_t>
    {
    private:
        acc_func_t acc_func; // reduce/fold function
        rich_acc_func_t rich_acc_func; // rich reduce/fold function
        string name; // string of the unique name of the pattern
        bool isRich; // flag stating whether the function to be used is rich (i.e. it receives the RuntimeContext object)
        RuntimeContext context; // RuntimeContext instance
        // inner struct of a key descriptor
        struct Key_Descriptor
        {
            result_t result;

            // constructor
            Key_Descriptor() {}
        };
        // hash table that maps key identifiers onto key descriptors
        unordered_map<size_t, Key_Descriptor> keyMap;
#if defined(LOG_DIR)
        unsigned long rcvTuples = 0;
        double avg_td_us = 0;
        double avg_ts_us = 0;
        volatile unsigned long startTD, startTS, endTD, endTS;
        ofstream *logfile = nullptr;
#endif
    public:
        // Constructor I
        Accumulator_Node(acc_func_t _acc_func, string _name): acc_func(_acc_func), name(_name), isRich(false) {}

        // Constructor II
        Accumulator_Node(rich_acc_func_t _rich_acc_func, string _name, RuntimeContext _context): rich_acc_func(_rich_acc_func), name(_name), isRich(true), context(_context) {}

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
            size_t key = std::get<0>(t->getInfo()); // key
            // find the corresponding key descriptor
            auto it = keyMap.find(key);
            if (it == keyMap.end()) {
                // create the descriptor of that key
                keyMap.insert(make_pair(key, Key_Descriptor()));
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
     *  \param _pardegree parallelism degree of the Accumulator pattern
     *  \param _name string with the unique name of the Accumulator pattern
     *  \param _routing function to map the key onto an identifier starting from zero to pardegree-1
     */ 
    Accumulator(acc_func_t _func, size_t _pardegree, string _name, f_routing_t _routing=[](size_t k, size_t n) { return k%n; })
    {
        // check the validity of the parallelism degree
        if (_pardegree == 0) {
            cerr << RED << "WindFlow Error: parallelism degree cannot be zero" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // vector of Accumulator_Node instances
        vector<ff_node *> w;
        for (size_t i=0; i<_pardegree; i++) {
            auto *seq = new Accumulator_Node(_func, _name);
            w.push_back(seq);
        }
        ff_farm::add_emitter(new Accumulator_Emitter<tuple_t>(_routing, _pardegree));
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
     *  \param _pardegree parallelism degree of the Accumulator pattern
     *  \param _name string with the unique name of the Accumulator pattern
     *  \param _routing function to map the key onto an identifier starting from zero to pardegree-1
     */ 
    Accumulator(rich_acc_func_t _func, size_t _pardegree, string _name, f_routing_t _routing=[](size_t k, size_t n) { return k%n; })
    {
        // check the validity of the parallelism degree
        if (_pardegree == 0) {
            cerr << RED << "WindFlow Error: parallelism degree cannot be zero" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // vector of Accumulator_Node instances
        vector<ff_node *> w;
        for (size_t i=0; i<_pardegree; i++) {
            auto *seq = new Accumulator_Node(_func, _name, RuntimeContext(_pardegree, i));
            w.push_back(seq);
        }
        ff_farm::add_emitter(new Accumulator_Emitter<tuple_t>(_routing, _pardegree));
        ff_farm::add_workers(w);
        // add default collector
        ff_farm::add_collector(nullptr);
        // when the Accumulator will be destroyed we need aslo to destroy the emitter, workers and collector
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
