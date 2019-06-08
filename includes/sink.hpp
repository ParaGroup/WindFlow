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
 *  The template parameter tuple_t must be default constructible, with a copy constructor
 *  and copy assignment operator, and it must provide and implement the setControlFields() and
 *  getControlFields() methods.
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
#include <context.hpp>

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
    /// type of the rich sink function
    using rich_sink_func_t = function<void(optional<tuple_t> &, RuntimeContext &)>;
    /// type of the closing function
    using closing_func_t = function<void(RuntimeContext &)>;
    /// type of the function to map the key hashcode onto an identifier starting from zero to pardegree-1
    using routing_func_t = function<size_t(size_t, size_t)>;
private:
    // friendships with other classes in the library
    friend class MultiPipe;
    bool keyed; // flag stating whether the Sink is configured with keyBy or not
    // class Sink_Node
    class Sink_Node: public ff_monode_t<tuple_t>
    {
    private:
        sink_func_t sink_fun; // sink function
        rich_sink_func_t rich_sink_func; // rich sink function
        closing_func_t closing_func; // closing function
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
        Sink_Node(sink_func_t _sink_fun, string _name, RuntimeContext _context, closing_func_t _closing_func): sink_fun(_sink_fun), name(_name), isRich(false), context(_context), closing_func(_closing_func) {}

        // Constructor II
        Sink_Node(rich_sink_func_t _rich_sink_fun, string _name, RuntimeContext _context, closing_func_t _closing_func): rich_sink_func(_rich_sink_fun), name(_name), isRich(true), context(_context), closing_func(_closing_func) {}

        // svc_init method (utilized by the FastFlow runtime)
        int svc_init()
        {
#if defined(LOG_DIR)
            logfile = new ofstream();
            name += "_node_" + to_string(ff_monode_t<tuple_t>::get_my_id()) + ".log";
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
            // create optional to the input tuple
            optional<tuple_t> opt = make_optional(move(*t));
            // call the sink function
            if (!isRich)
                sink_fun(opt);
            else
                rich_sink_func(opt, context);
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
            if (!isRich)
                sink_fun(opt);
            else
                rich_sink_func(opt, context);
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
     *  \param _func sink function
     *  \param _pardegree parallelism degree of the Sink pattern
     *  \param _name string with the unique name of the Sink pattern
     *  \param _closing_func closing function
     */ 
    Sink(sink_func_t _func, size_t _pardegree, string _name, closing_func_t _closing_func): keyed(false)
    {
        // check the validity of the parallelism degree
        if (_pardegree == 0) {
            cerr << RED << "WindFlow Error: parallelism degree cannot be zero" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // vector of Sink_Node instances
        vector<ff_node *> w;
        for (size_t i=0; i<_pardegree; i++) {
            auto *seq = new Sink_Node(_func, _name, RuntimeContext(_pardegree, i), _closing_func);
            w.push_back(seq);
        }
        // add emitter
        ff_farm::add_emitter(new Standard_Emitter<tuple_t>());
        // add workers
        ff_farm::add_workers(w);
        // when the Sink will be destroyed we need aslo to destroy the emitter and workers
        ff_farm::cleanup_all();
    }

    /** 
     *  \brief Constructor II
     *  
     *  \param _func sink function
     *  \param _pardegree parallelism degree of the Sink pattern
     *  \param _name string with the unique name of the Sink pattern
     *  \param _closing_func closing function
     *  \param _routing_func function to map the key hashcode onto an identifier starting from zero to pardegree-1
     */ 
    Sink(sink_func_t _func, size_t _pardegree, string _name, closing_func_t _closing_func, routing_func_t _routing_func): keyed(true)
    {
        // check the validity of the parallelism degree
        if (_pardegree == 0) {
            cerr << RED << "WindFlow Error: parallelism degree cannot be zero" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // vector of Sink_Node instances
        vector<ff_node *> w;
        for (size_t i=0; i<_pardegree; i++) {
            auto *seq = new Sink_Node(_func, _name, RuntimeContext(_pardegree, i), _closing_func);
            w.push_back(seq);
        }
        // add emitter
        ff_farm::add_emitter(new Standard_Emitter<tuple_t>(_routing_func));
        // add workers
        ff_farm::add_workers(w);
        // when the Sink will be destroyed we need aslo to destroy the emitter and workers
        ff_farm::cleanup_all();
    }

    /** 
     *  \brief Constructor III
     *  
     *  \param _func rich sink function
     *  \param _pardegree parallelism degree of the Sink pattern
     *  \param _name string with the unique name of the Sink pattern
     *  \param _closing_func closing function
     */ 
    Sink(rich_sink_func_t _func, size_t _pardegree, string _name, closing_func_t _closing_func): keyed(false)
    {
        // check the validity of the parallelism degree
        if (_pardegree == 0) {
            cerr << RED << "WindFlow Error: parallelism degree cannot be zero" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // vector of Sink_Node instances
        vector<ff_node *> w;
        for (size_t i=0; i<_pardegree; i++) {
            auto *seq = new Sink_Node(_func, _name, RuntimeContext(_pardegree, i), _closing_func);
            w.push_back(seq);
        }
        // add emitter
        ff_farm::add_emitter(new Standard_Emitter<tuple_t>());
        // add workers
        ff_farm::add_workers(w);
        // when the Sink will be destroyed we need aslo to destroy the emitter and workers
        ff_farm::cleanup_all();
    }

    /** 
     *  \brief Constructor IV
     *  
     *  \param _func rich sink function
     *  \param _pardegree parallelism degree of the Sink pattern
     *  \param _name string with the unique name of the Sink pattern
     *  \param _closing_func closing function
     *  \param _routing_func function to map the key hashcode onto an identifier starting from zero to pardegree-1
     */ 
    Sink(rich_sink_func_t _func, size_t _pardegree, string _name, closing_func_t _closing_func, routing_func_t _routing_func): keyed(true)
    {
        // check the validity of the parallelism degree
        if (_pardegree == 0) {
            cerr << RED << "WindFlow Error: parallelism degree cannot be zero" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // vector of Sink_Node instances
        vector<ff_node *> w;
        for (size_t i=0; i<_pardegree; i++) {
            auto *seq = new Sink_Node(_func, _name, RuntimeContext(_pardegree, i), _closing_func);
            w.push_back(seq);
        }
        // add emitter
        ff_farm::add_emitter(new Standard_Emitter<tuple_t>(_routing_func));
        // add workers
        ff_farm::add_workers(w);
        // when the Sink will be destroyed we need aslo to destroy the emitter and workers
        ff_farm::cleanup_all();
    }

    /** 
     *  \brief Check whether the Sink has been instantiated with a key-based distribution or not
     *  \return true if the Filter is configured with keyBy
     */
    bool isKeyed() { return keyed; }
};

#endif
