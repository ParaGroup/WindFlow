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
 *  @brief Sink operator absorbing the input stream
 *  
 *  @section Sink (Description)
 *  
 *  This file implements the Sink operator in charge of absorbing the items of
 *  a data stream.
 *  
 *  The template parameter tuple_t must be default constructible, with a copy Constructor
 *  and copy assignment operator, and it must provide and implement the setControlFields() and
 *  getControlFields() methods.
 */ 

#ifndef SINK_H
#define SINK_H

/// includes
#include <string>
#if __cplusplus < 201703L // not C++17
    #include <experimental/optional>
    namespace std { using namespace experimental; }
#else
    #include <optional>
#endif
#include <ff/node.hpp>
#include <ff/combine.hpp>
#include <ff/pipeline.hpp>
#include <ff/multinode.hpp>
#include <ff/farm.hpp>
#include <basic.hpp>
#include <context.hpp>
#include <transformations.hpp>
#include <standard_emitter.hpp>

namespace wf {

/** 
 *  \class Sink
 *  
 *  \brief Sink operator absorbing the input stream
 *  
 *  This class implements the Sink operator absorbing a data stream of items.
 */ 
template<typename tuple_t>
class Sink: public ff::ff_farm
{
public:
    /// type of the sink function
    using sink_func_t = std::function<void(std::optional<tuple_t> &)>;
    /// type of the rich sink function
    using rich_sink_func_t = std::function<void(std::optional<tuple_t> &, RuntimeContext &)>;
    /// type of the closing function
    using closing_func_t = std::function<void(RuntimeContext &)>;
    /// type of the function to map the key hashcode onto an identifier starting from zero to pardegree-1
    using routing_func_t = std::function<size_t(size_t, size_t)>;

private:
    // friendships with other classes in the library
    friend class MultiPipe;
    bool keyed; // flag stating whether the Sink is configured with keyBy or not
    bool used; // true if the operator has been added/chained in a MultiPipe
    // class Sink_Node
    class Sink_Node: public ff::ff_minode_t<tuple_t>
    {
    private:
        sink_func_t sink_fun; // sink function
        rich_sink_func_t rich_sink_func; // rich sink function
        closing_func_t closing_func; // closing function
        std::string name; // string of the unique name of the operator
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
        Sink_Node(sink_func_t _sink_fun,
                  std::string _name,
                  RuntimeContext _context,
                  closing_func_t _closing_func):
                  sink_fun(_sink_fun),
                  name(_name),
                  isRich(false),
                  context(_context),
                  closing_func(_closing_func)
        {}

        // Constructor II
        Sink_Node(rich_sink_func_t _rich_sink_fun,
                  std::string _name,
                  RuntimeContext _context,
                  closing_func_t _closing_func):
                  rich_sink_func(_rich_sink_fun),
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
        tuple_t *svc(tuple_t *t)
        {
#if defined(TRACE_WINDFLOW)
            startTS = current_time_nsecs();
            if (rcvTuples == 0)
                startTD = current_time_nsecs();
            rcvTuples++;
#endif
            // create optional to the input tuple
            std::optional<tuple_t> opt = std::make_optional(std::move(*t));
            // call the sink function
            if (!isRich)
                sink_fun(opt);
            else
                rich_sink_func(opt, context);
            // delete the received item
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
            return this->GO_ON;
        }

        // svc_end method (utilized by the FastFlow runtime)
        void svc_end()
        {
            // create empty optional
            std::optional<tuple_t> opt;
            // call the sink function for the last time (empty optional)
            if (!isRich) {
                sink_fun(opt);
            }
            else {
                rich_sink_func(opt, context);
            }
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
     *  \param _func sink function
     *  \param _pardegree parallelism degree of the Sink operator
     *  \param _name string with the unique name of the Sink operator
     *  \param _closing_func closing function
     */ 
    Sink(sink_func_t _func,
         size_t _pardegree,
         std::string _name,
         closing_func_t _closing_func):
         keyed(false), used(false)
    {
        // check the validity of the parallelism degree
        if (_pardegree == 0) {
            std::cerr << RED << "WindFlow Error: Sink has parallelism zero" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // std::vector of Sink_Node
        std::vector<ff_node *> w;
        for (size_t i=0; i<_pardegree; i++) {
            auto *seq = new Sink_Node(_func, _name, RuntimeContext(_pardegree, i), _closing_func);
            auto *seq_comb = new ff::ff_comb(seq, new dummy_mo(), true, true);
            w.push_back(seq_comb);
        }
        // add emitter
        ff::ff_farm::add_emitter(new Standard_Emitter<tuple_t>(_pardegree));
        // add workers
        ff::ff_farm::add_workers(w);
        // when the Sink will be destroyed we need aslo to destroy the emitter and workers
        ff::ff_farm::cleanup_all();
    }

    /** 
     *  \brief Constructor II
     *  
     *  \param _func sink function
     *  \param _pardegree parallelism degree of the Sink operator
     *  \param _name string with the unique name of the Sink operator
     *  \param _closing_func closing function
     *  \param _routing_func function to map the key hashcode onto an identifier starting from zero to pardegree-1
     */ 
    Sink(sink_func_t _func,
         size_t _pardegree,
         std::string _name,
         closing_func_t _closing_func,
         routing_func_t _routing_func):
         keyed(true), used(false)
    {
        // check the validity of the parallelism degree
        if (_pardegree == 0) {
            std::cerr << RED << "WindFlow Error: Sink has parallelism zero" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // std::vector of Sink_Node
        std::vector<ff_node *> w;
        for (size_t i=0; i<_pardegree; i++) {
            auto *seq = new Sink_Node(_func, _name, RuntimeContext(_pardegree, i), _closing_func);
            auto *seq_comb = new ff::ff_comb(seq, new dummy_mo(), true, true);
            w.push_back(seq_comb);
        }
        // add emitter
        ff::ff_farm::add_emitter(new Standard_Emitter<tuple_t>(_routing_func, _pardegree));
        // add workers
        ff::ff_farm::add_workers(w);
        // when the Sink will be destroyed we need aslo to destroy the emitter and workers
        ff::ff_farm::cleanup_all();
    }

    /** 
     *  \brief Constructor III
     *  
     *  \param _func rich sink function
     *  \param _pardegree parallelism degree of the Sink operator
     *  \param _name string with the unique name of the Sink operator
     *  \param _closing_func closing function
     */ 
    Sink(rich_sink_func_t _func,
         size_t _pardegree,
         std::string _name,
         closing_func_t _closing_func):
         keyed(false), used(false)
    {
        // check the validity of the parallelism degree
        if (_pardegree == 0) {
            std::cerr << RED << "WindFlow Error: Sink has parallelism zero" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // std::vector of Sink_Node
        std::vector<ff_node *> w;
        for (size_t i=0; i<_pardegree; i++) {
            auto *seq = new Sink_Node(_func, _name, RuntimeContext(_pardegree, i), _closing_func);
            auto *seq_comb = new ff::ff_comb(seq, new dummy_mo(), true, true);
            w.push_back(seq_comb);
        }
        // add emitter
        ff::ff_farm::add_emitter(new Standard_Emitter<tuple_t>(_pardegree));
        // add workers
        ff::ff_farm::add_workers(w);
        // when the Sink will be destroyed we need aslo to destroy the emitter and workers
        ff::ff_farm::cleanup_all();
    }

    /** 
     *  \brief Constructor IV
     *  
     *  \param _func rich sink function
     *  \param _pardegree parallelism degree of the Sink operator
     *  \param _name string with the unique name of the Sink operator
     *  \param _closing_func closing function
     *  \param _routing_func function to map the key hashcode onto an identifier starting from zero to pardegree-1
     */ 
    Sink(rich_sink_func_t _func,
         size_t _pardegree,
         std::string _name,
         closing_func_t _closing_func,
         routing_func_t _routing_func):
         keyed(true), used(false)
    {
        // check the validity of the parallelism degree
        if (_pardegree == 0) {
            std::cerr << RED << "WindFlow Error: Sink has parallelism zero" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // std::vector of Sink_Node
        std::vector<ff_node *> w;
        for (size_t i=0; i<_pardegree; i++) {
            auto *seq = new Sink_Node(_func, _name, RuntimeContext(_pardegree, i), _closing_func);
            auto *seq_comb = new ff::ff_comb(seq, new dummy_mo(), true, true);
            w.push_back(seq_comb);
        }
        // add emitter
        ff::ff_farm::add_emitter(new Standard_Emitter<tuple_t>(_routing_func, _pardegree));
        // add workers
        ff::ff_farm::add_workers(w);
        // when the Sink will be destroyed we need aslo to destroy the emitter and workers
        ff::ff_farm::cleanup_all();
    }

    /** 
     *  \brief Check whether the Sink has been instantiated with a key-based distribution or not
     *  \return true if the Filter is configured with keyBy
     */
    bool isKeyed() const
    {
        return keyed;
    }

    /** 
     *  \brief Check whether the Sink has been used in a MultiPipe
     *  \return true if the Sink has been added/chained to an existing MultiPipe
     */
    bool isUsed() const
    {
        return used;
    }

    /// deleted constructors/operators
    Sink(const Sink &) = delete; // copy constructor
    Sink(Sink &&) = delete; // move constructor
    Sink &operator=(const Sink &) = delete; // copy assignment operator
    Sink &operator=(Sink &&) = delete; // move assignment operator
};

} // namespace wf

#endif
