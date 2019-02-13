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
 *  @file    source.hpp
 *  @author  Gabriele Mencagli
 *  @date    10/01/2019
 *  
 *  @brief Source pattern generating the input stream
 *  
 *  @section Source (Description)
 *  
 *  This file implements the Source pattern in charge of generating the items of
 *  a data stream.
 *  
 *  The template argument tuple_t must be default constructible, with a copy constructor and copy assignment
 *  operator, and it must provide and implement the setInfo() and getInfo() methods.
 */ 

#ifndef SOURCE_H
#define SOURCE_H

// includes
#include <string>
#include <ff/node.hpp>
#include <ff/all2all.hpp>
#include <ff/multinode.hpp>
#include <dummy.hpp>
#include <shipper.hpp>
#include <builders.hpp>

using namespace ff;

/** 
 *  \class Source
 *  
 *  \brief Source pattern generating the input stream
 *  
 *  This class implements the Source pattern generating a data stream of items.
 */ 
template<typename tuple_t>
class Source: public ff_a2a
{
public:
    /// type of the generation function (one-to-one version, briefly "o2o")
    using source_o2o_func_t = function<bool(tuple_t &)>;
    /// type of the generation function (single-loop version)
    using source_sloop_func_t = function<void(Shipper<tuple_t>&)>;
    // friendships with other classes in the library
    friend class Pipe;
private:
    // class Source_Node
    class Source_Node: public ff_node_t<tuple_t>
    {
    private:
        source_o2o_func_t source_func_o2o; // generation function (one-to-one version)
        source_sloop_func_t source_func_sloop; // generation function (single-loop version)
        string name; // string of the unique name of the pattern
        bool isO2O; // flag stating whether we are using the one-to-one generation function
        bool isEND; // flag stating whether the Source_Node has completed to generate items (used only for the single-loop generation)
        Shipper<tuple_t> shipper; // shipper object used for the delivery of results (single-loop generation)
#if defined(LOG_DIR)
        unsigned long sentTuples = 0;
        ofstream logfile;
#endif
    public:
        // Constructor I (one-to-one version)
        Source_Node(source_o2o_func_t _source_func_o2o, string _name): source_func_o2o(_source_func_o2o), name(_name), isO2O(true), isEND(false), shipper(*this) {}

        // Constructor II (single-loop version)
        Source_Node(source_sloop_func_t _source_func_sloop, string _name): source_func_sloop(_source_func_sloop), name(_name), isO2O(false), isEND(false), shipper(*this) {}

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
        tuple_t *svc(tuple_t *)
        {
            // tuple-by-tuple version
            if (isO2O) {
                if (isEND)
                    return this->EOS;
                else {
                    // allocate the new tuple to be sent
                    tuple_t *t = new tuple_t();
                    isEND = !source_func_o2o(*t); // call the one-to-one generation function
#if defined (LOG_DIR)
                    sentTuples++;
#endif
                    return t;           
                }
            }
            // single-loop version
            else {
                source_func_sloop(shipper); // call the single-loop generation function
#if defined (LOG_DIR)
                sentTuples = shipper.delivered();
#endif
                isEND = true; // not necessary!
                return this->EOS;
            }
        }

        // svc_end method (utilized by the FastFlow runtime)
        void svc_end()
        {
#if defined (LOG_DIR)
            ostringstream stream;
            stream << "************************************LOG************************************\n";
            stream << "Generated tuples: " << sentTuples << "\n";
            stream << "***************************************************************************\n";
            logfile << stream.str();
            logfile.close();
#endif
        }
    };

public:
    /** 
     *  \brief Constructor I (one-to-one version)
     *  
     *  \param _func generation function (one-to-one version)
     *  \param _pardegree parallelism degree of the Source pattern
     *  \param _name string with the unique name of the Source pattern
     */ 
    Source(source_o2o_func_t _func, size_t _pardegree, string _name)
    {
        // check the validity of the parallelism degree
        if (_pardegree == 0) {
            cerr << RED << "WindFlow Error: parallelism degree cannot be zero" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // vector of Source_Node instances
        vector<ff_node *> first_set;
        for (size_t i=0; i<_pardegree; i++) {
            auto *seq = new Source_Node(_func, _name);
            first_set.push_back(seq);
        }
        ff_a2a::add_firstset(first_set, 0, true);
        vector<ff_node *> second_set;
        second_set.push_back(new dummy_collector());
        ff_a2a::add_secondset(second_set, true);
    }

    /** 
     *  \brief Constructor II (single-loop version)
     *  
     *  \param _func generation function (single-loop version)
     *  \param _pardegree parallelism degree of the Source pattern
     *  \param _name string with the unique name of the Source pattern
     */ 
    Source(source_sloop_func_t _func, size_t _pardegree, string _name)
    {
        // check the validity of the parallelism degree
        if (_pardegree == 0) {
            cerr << RED << "WindFlow Error: parallelism degree cannot be zero" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // vector of Source_Node instances
        vector<ff_node *> first_set;
        for (size_t i=0; i<_pardegree; i++) {
            auto *seq = new Source_Node(_func, _name);
            first_set.push_back(seq);
        }
        ff_a2a::add_firstset(first_set, 0, true);
        vector<ff_node *> second_set;
        second_set.push_back(new dummy_collector());
        ff_a2a::add_secondset(second_set, true);
    }

//@cond DOXY_IGNORE

    // -------------------------------------- deleted methods ----------------------------------------
    int add_firstset(const std::vector<ff_node *> & w, int ondemand=0, bool cleanup=false)  = delete;
    int add_secondset(const std::vector<ff_node *> & w, bool cleanup=false)                 = delete;
    //void skipfirstpop(bool sk)                                                              = delete;
    //void blocking_mode(bool blk=true)                                                       = delete;
    //void no_mapping()                                                                       = delete;
    ///void no_barrier()                                                                       = delete;
    //int set_output(ff_node *node)                                                           = delete;
    //void get_out_nodes(svector<ff_node*>&w)                                                 = delete;
    //virtual void get_in_nodes(svector<ff_node*>&w)                                                  = delete;
    int wrap_around()                                                                       = delete;

//@endcond

};

#endif
