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
 *  @file    flatmap.hpp
 *  @author  Gabriele Mencagli
 *  @date    08/01/2019
 *  
 *  @brief FlatMap pattern executing a one-to-any transformation on the input stream
 *  
 *  @section FlatMap (Description)
 *  
 *  This file implements the FlatMap pattern able to execute a one-to-any transformation
 *  on each tuple of the input data stream. The transformation should be stateless and
 *  must produce zero, one or more than one output result for each input tuple consumed.
 *  
 *  The template parameters tuple_t and result_t must be default constructible, with
 *  a copy Constructor and copy assignment operator, and they must provide and implement
 *  the setControlFields() and getControlFields() methods.
 */ 

#ifndef FLATMAP_H
#define FLATMAP_H

/// includes
#include <string>
#include <ff/node.hpp>
#include <ff/farm.hpp>
#include <shipper.hpp>
#include <context.hpp>
#include <standard.hpp>

using namespace ff;
using namespace std;

/** 
 *  \class FlatMap
 *  
 *  \brief FlatMap pattern executing a one-to-any transformation on the input stream
 *  
 *  This class implements the FlatMap pattern executing a one-to-any transformation
 *  on each tuple of the input stream.
 */ 
template<typename tuple_t, typename result_t>
class FlatMap: public ff_farm
{
public:
    /// type of the flatmap function
    using flatmap_func_t = function<void(const tuple_t &, Shipper<result_t> &)>;
    /// type of the rich flatmap function
    using rich_flatmap_func_t = function<void(const tuple_t &, Shipper<result_t> &, RuntimeContext &)>;
    /// type of the closing function
    using closing_func_t = function<void(RuntimeContext &)>;
    /// type of the function to map the key hashcode onto an identifier starting from zero to pardegree-1
    using routing_func_t = function<size_t(size_t, size_t)>;

private:
    // friendships with other classes in the library
    friend class MultiPipe;
    bool keyed; // flag stating whether the FlatMap is configured with keyBy or not
    // class FlatMap_Node
    class FlatMap_Node: public ff_node_t<tuple_t, result_t>
    {
    private:
        flatmap_func_t flatmap_func; // flatmap function
        rich_flatmap_func_t rich_flatmap_func; // rich flatmap function
        closing_func_t closing_func; // closing function
        string name; // string of the unique name of the pattern
        bool isRich; // flag stating whether the function to be used is rich (i.e. it receives the RuntimeContext object)
        // shipper object used for the delivery of results
        Shipper<result_t> *shipper = nullptr;
        RuntimeContext context; // RuntimeContext
#if defined(LOG_DIR)
        unsigned long rcvTuples = 0;
        unsigned long delivered = 0;
        unsigned long selectivity = 0;
        double avg_td_us = 0;
        double avg_ts_us = 0;
        volatile unsigned long startTD, startTS, endTD, endTS;
        ofstream *logfile = nullptr;
#endif

    public:
        // Constructor I
        FlatMap_Node(flatmap_func_t _flatmap_func, string _name, RuntimeContext _context, closing_func_t _closing_func): flatmap_func(_flatmap_func), name(_name), isRich(false), context(_context), closing_func(_closing_func) {}

        // Constructor II
        FlatMap_Node(rich_flatmap_func_t _rich_flatmap_func, string _name, RuntimeContext _context, closing_func_t _closing_func): rich_flatmap_func(_rich_flatmap_func), name(_name), isRich(true), context(_context), closing_func(_closing_func) {}

        // svc_init method (utilized by the FastFlow runtime)
        int svc_init()
        {
            shipper = new Shipper<result_t>(*this);
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
            // call the flatmap function
            if (!isRich)
                flatmap_func(*t, *shipper);
            else
                rich_flatmap_func(*t, *shipper, context);
            delete t;
#if defined(LOG_DIR)
            selectivity += (shipper->delivered() - delivered);
            delivered = shipper->delivered();
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
            // call the closing function
            closing_func(context);
            delete shipper;
#if defined (LOG_DIR)
            ostringstream stream;
            stream << "************************************LOG************************************\n";
            stream << "No. of received tuples: " << rcvTuples << "\n";
            stream << "Output selectivity: " << delivered/((double) rcvTuples) << "\n";
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
     *  \param _func flatmap function
     *  \param _pardegree parallelism degree of the FlatMap pattern
     *  \param _name string with the unique name of the FlatMap pattern
     *  \param _closing_func closing function
     */ 
    FlatMap(flatmap_func_t _func, size_t _pardegree, string _name, closing_func_t _closing_func): keyed(false)
    {
        // check the validity of the parallelism degree
        if (_pardegree == 0) {
            cerr << RED << "WindFlow Error: parallelism degree cannot be zero" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // vector of FlatMap_Node
        vector<ff_node *> w;
        for (size_t i=0; i<_pardegree; i++) {
            auto *seq = new FlatMap_Node(_func, _name, RuntimeContext(_pardegree, i), _closing_func);
            w.push_back(seq);
        }
        // add emitter
        ff_farm::add_emitter(new Standard_Emitter<tuple_t>(_pardegree));
        // add workers
        ff_farm::add_workers(w);
        // add default collector
        ff_farm::add_collector(nullptr);
        // when the FlatMap will be destroyed we need aslo to destroy the emitter, workers and collector
        ff_farm::cleanup_all();
    }

    /** 
     *  \brief Constructor II
     *  
     *  \param _func flatmap function
     *  \param _pardegree parallelism degree of the FlatMap pattern
     *  \param _name string with the unique name of the FlatMap pattern
     *  \param _closing_func closing function
     *  \param _routing_func function to map the key hashcode onto an identifier starting from zero to pardegree-1
     */ 
    FlatMap(flatmap_func_t _func, size_t _pardegree, string _name, closing_func_t _closing_func, routing_func_t _routing_func): keyed(true)
    {
        // check the validity of the parallelism degree
        if (_pardegree == 0) {
            cerr << RED << "WindFlow Error: parallelism degree cannot be zero" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // vector of FlatMap_Node
        vector<ff_node *> w;
        for (size_t i=0; i<_pardegree; i++) {
            auto *seq = new FlatMap_Node(_func, _name, RuntimeContext(_pardegree, i), _closing_func);
            w.push_back(seq);
        }
        // add emitter
        ff_farm::add_emitter(new Standard_Emitter<tuple_t>(_routing_func, _pardegree));
        // add workers
        ff_farm::add_workers(w);
        // add default collector
        ff_farm::add_collector(nullptr);
        // when the FlatMap will be destroyed we need aslo to destroy the emitter, workers and collector
        ff_farm::cleanup_all();
    }

    /** 
     *  \brief Constructor III
     *  
     *  \param _func rich flatmap function
     *  \param _pardegree parallelism degree of the FlatMap pattern
     *  \param _name string with the unique name of the FlatMap pattern
     *  \param _closing_func closing function
     */ 
    FlatMap(rich_flatmap_func_t _func, size_t _pardegree, string _name, closing_func_t _closing_func): keyed(false)
    {
        // check the validity of the parallelism degree
        if (_pardegree == 0) {
            cerr << RED << "WindFlow Error: parallelism degree cannot be zero" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // vector of FlatMap_Node
        vector<ff_node *> w;
        for (size_t i=0; i<_pardegree; i++) {
            auto *seq = new FlatMap_Node(_func, _name, RuntimeContext(_pardegree, i), _closing_func);
            w.push_back(seq);
        }
        // add emitter
        ff_farm::add_emitter(new Standard_Emitter<tuple_t>(_pardegree));
        // add workers
        ff_farm::add_workers(w);
        // add default collector
        ff_farm::add_collector(nullptr);
        // when the FlatMap will be destroyed we need aslo to destroy the emitter, workers and collector
        ff_farm::cleanup_all();
    }

    /** 
     *  \brief Constructor IV
     *  
     *  \param _func rich flatmap function
     *  \param _pardegree parallelism degree of the FlatMap pattern
     *  \param _name string with the unique name of the FlatMap pattern
     *  \param _closing_func closing function
     *  \param _routing_func function to map the key hashcode onto an identifier starting from zero to pardegree-1
     */ 
    FlatMap(rich_flatmap_func_t _func, size_t _pardegree, string _name, closing_func_t _closing_func, routing_func_t _routing_func): keyed(true)
    {
        // check the validity of the parallelism degree
        if (_pardegree == 0) {
            cerr << RED << "WindFlow Error: parallelism degree cannot be zero" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // vector of FlatMap_Node
        vector<ff_node *> w;
        for (size_t i=0; i<_pardegree; i++) {
            auto *seq = new FlatMap_Node(_func, _name, RuntimeContext(_pardegree, i), _closing_func);
            w.push_back(seq);
        }
        // add emitter
        ff_farm::add_emitter(new Standard_Emitter<tuple_t>(_routing_func, _pardegree));
        // add workers
        ff_farm::add_workers(w);
        // add default collector
        ff_farm::add_collector(nullptr);
        // when the FlatMap will be destroyed we need aslo to destroy the emitter, workers and collector
        ff_farm::cleanup_all();
    }

    /** 
     *  \brief Check whether the FlatMap has been instantiated with a key-based distribution or not
     *  \return true if the FlatMap is configured with keyBy
     */
    bool isKeyed() { return keyed; }
};

#endif
