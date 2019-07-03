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
 *  The template parameter tuple_t must be default constructible, with a copy
 *  Constructor and copy assignment operator, and it must provide and implement
 *  the setControlFields() and getControlFields() methods.
 */ 

#ifndef SOURCE_H
#define SOURCE_H

/// includes
#include <string>
#include <ff/node.hpp>
#include <ff/all2all.hpp>
#include <shipper.hpp>
#include <context.hpp>
#include <standard.hpp>

using namespace ff;
using namespace std;

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
    /// type of the generation function (item-by-item version, briefly "itemized")
    using source_item_func_t = function<bool(tuple_t &)>;
    /// type of the rich generation function (item-by-item version, briefly "itemized")
    using rich_source_item_func_t = function<bool(tuple_t &, RuntimeContext &)>;
    /// type of the generation function (single-loop version, briefly "loop")
    using source_loop_func_t = function<void(Shipper<tuple_t>&)>;
    /// type of the rich generation function (single-loop version, briefly "loop")
    using rich_source_loop_func_t = function<void(Shipper<tuple_t>&, RuntimeContext &)>;
    /// type of the closing function
    using closing_func_t = function<void(RuntimeContext &)>;

private:
    // friendships with other classes in the library
    friend class MultiPipe;
    // class Source_Node
    class Source_Node: public ff_node_t<tuple_t>
    {
    private:
        source_item_func_t source_func_item; // generation function (item-by-item version)
        rich_source_item_func_t rich_source_func_item; // rich generation function (item-by-item version)
        source_loop_func_t source_func_loop; // generation function (single-loop version)
        rich_source_loop_func_t rich_source_func_loop; // rich generation function (single-loop version)
        closing_func_t closing_func; // closing function
        string name; // string of the unique name of the pattern
        bool isItemized; // flag stating whether we are using the item-by-item version of the generation function
        bool isRich; // flag stating whether the function to be used is rich (i.e. it receives the RuntimeContext object)
        bool isEND; // flag stating whether the Source_Node has completed to generate items
        Shipper<tuple_t> *shipper = nullptr; // shipper object used for the delivery of results (single-loop version)
        RuntimeContext context; // RuntimeContext
#if defined(LOG_DIR)
        unsigned long sentTuples = 0;
        ofstream *logfile = nullptr;
#endif

    public:
        // Constructor I
        Source_Node(source_item_func_t _source_func_item, string _name, RuntimeContext _context, closing_func_t _closing_func): source_func_item(_source_func_item), name(_name), isItemized(true), isRich(false), isEND(false), context(_context), closing_func(_closing_func) {}

        // Constructor II
        Source_Node(rich_source_item_func_t _rich_source_func_item, string _name, RuntimeContext _context, closing_func_t _closing_func): rich_source_func_item(_rich_source_func_item), name(_name), isItemized(true), isRich(true), isEND(false), context(_context), closing_func(_closing_func) {}

        // Constructor III
        Source_Node(source_loop_func_t _source_func_loop, string _name, RuntimeContext _context, closing_func_t _closing_func): source_func_loop(_source_func_loop), name(_name), isItemized(false), isRich(false), isEND(false), context(_context), closing_func(_closing_func) {}

        // Constructor IV
        Source_Node(rich_source_loop_func_t _rich_source_func_loop, string _name, RuntimeContext _context, closing_func_t _closing_func): rich_source_func_loop(_rich_source_func_loop), name(_name), isItemized(false), isRich(true), isEND(false), context(_context), closing_func(_closing_func) {}

        // svc_init method (utilized by the FastFlow runtime)
        int svc_init()
        {
            shipper = new Shipper<tuple_t>(*this);
#if defined(LOG_DIR)
            logfile = new ofstream();
            name += "_node_" + to_string(ff_node_t<tuple_t>::get_my_id()) + ".log";
            string filename = string(STRINGIFY(LOG_DIR)) + "/" + name;
            logfile->open(filename);
#endif
            return 0;
        }

        // svc method (utilized by the FastFlow runtime)
        tuple_t *svc(tuple_t *)
        {
            // itemized version
            if (isItemized) {
                if (isEND)
                    return this->EOS;
                else {
                    // allocate the new tuple to be sent
                    tuple_t *t = new tuple_t();
                    if (!isRich)
                        isEND = !source_func_item(*t); // call the generation function
                    else
                        isEND = !rich_source_func_item(*t, context); // call the generation function
#if defined (LOG_DIR)
                    sentTuples++;
#endif
                    return t;
                }
            }
            // single-loop version
            else {
                if (!isRich)
                    source_func_loop(*shipper); // call the generation function
                else
                    rich_source_func_loop(*shipper, context); // call the generation function
#if defined (LOG_DIR)
                sentTuples = shipper->delivered();
#endif
                isEND = true; // not necessary!
                return this->EOS;
            }
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
            stream << "Generated tuples: " << sentTuples << "\n";
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
     *  \param _func generation function (item-by-item version)
     *  \param _pardegree parallelism degree of the Source pattern
     *  \param _name string with the unique name of the Source pattern
     *  \param _closing_func closing function
     */ 
    Source(source_item_func_t _func, size_t _pardegree, string _name, closing_func_t _closing_func)
    {
        // check the validity of the parallelism degree
        if (_pardegree == 0) {
            cerr << RED << "WindFlow Error: parallelism degree cannot be zero" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // vector of Source_Node
        vector<ff_node *> first_set;
        for (size_t i=0; i<_pardegree; i++) {
            auto *seq = new Source_Node(_func, _name, RuntimeContext(_pardegree, i), _closing_func);
            first_set.push_back(seq);
        }
        // add first set
        ff_a2a::add_firstset(first_set, 0, true);
        vector<ff_node *> second_set;
        second_set.push_back(new Standard_Collector());
        // add second set
        ff_a2a::add_secondset(second_set, true);
    }

    /** 
     *  \brief Constructor II
     *  
     *  \param _func rich generation function (item-by-item version)
     *  \param _pardegree parallelism degree of the Source pattern
     *  \param _name string with the unique name of the Source pattern
     *  \param _closing_func closing function
     */ 
    Source(rich_source_item_func_t _func, size_t _pardegree, string _name, closing_func_t _closing_func)
    {
        // check the validity of the parallelism degree
        if (_pardegree == 0) {
            cerr << RED << "WindFlow Error: parallelism degree cannot be zero" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // vector of Source_Node
        vector<ff_node *> first_set;
        for (size_t i=0; i<_pardegree; i++) {
            auto *seq = new Source_Node(_func, _name, RuntimeContext(_pardegree, i), _closing_func);
            first_set.push_back(seq);
        }
        // add first set
        ff_a2a::add_firstset(first_set, 0, true);
        vector<ff_node *> second_set;
        second_set.push_back(new Standard_Collector());
        // add second set
        ff_a2a::add_secondset(second_set, true);
    }

    /** 
     *  \brief Constructor III
     *  
     *  \param _func generation function (single-loop version)
     *  \param _pardegree parallelism degree of the Source pattern
     *  \param _name string with the unique name of the Source pattern
     *  \param _closing_func closing function
     */ 
    Source(source_loop_func_t _func, size_t _pardegree, string _name, closing_func_t _closing_func)
    {
        // check the validity of the parallelism degree
        if (_pardegree == 0) {
            cerr << RED << "WindFlow Error: parallelism degree cannot be zero" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // vector of Source_Node
        vector<ff_node *> first_set;
        for (size_t i=0; i<_pardegree; i++) {
            auto *seq = new Source_Node(_func, _name, RuntimeContext(_pardegree, i), _closing_func);
            first_set.push_back(seq);
        }
        // add first set
        ff_a2a::add_firstset(first_set, 0, true);
        vector<ff_node *> second_set;
        second_set.push_back(new Standard_Collector());
        // add second set
        ff_a2a::add_secondset(second_set, true);
    }

    /** 
     *  \brief Constructor IV
     *  
     *  \param _func rich generation function (single-loop version)
     *  \param _pardegree parallelism degree of the Source pattern
     *  \param _name string with the unique name of the Source pattern
     *  \param _closing_func closing function
     */ 
    Source(rich_source_loop_func_t _func, size_t _pardegree, string _name, closing_func_t _closing_func)
    {
        // check the validity of the parallelism degree
        if (_pardegree == 0) {
            cerr << RED << "WindFlow Error: parallelism degree cannot be zero" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // vector of Source_Node
        vector<ff_node *> first_set;
        for (size_t i=0; i<_pardegree; i++) {
            auto *seq = new Source_Node(_func, _name, RuntimeContext(_pardegree, i), _closing_func);
            first_set.push_back(seq);
        }
        // add first set
        ff_a2a::add_firstset(first_set, 0, true);
        vector<ff_node *> second_set;
        second_set.push_back(new Standard_Collector());
        // add second set
        ff_a2a::add_secondset(second_set, true);
    }
};

#endif
