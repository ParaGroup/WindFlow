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
 *  @brief Source operator generating the input stream
 *  
 *  @section Source (Description)
 *  
 *  This file implements the Source operator in charge of generating the items of
 *  a data stream.
 *  
 *  The template parameter tuple_t must be default constructible, with a copy
 *  Constructor and copy assignment operator, and it must provide and implement
 *  the setControlFields() and getControlFields() methods.
 */ 

#ifndef SOURCE_H
#define SOURCE_H

/// includes
#include<string>
#include<ff/node.hpp>
#include<ff/pipeline.hpp>
#include<ff/all2all.hpp>
#include<basic.hpp>
#include<shipper.hpp>
#include<context.hpp>
#include<transformations.hpp>

namespace wf {

/** 
 *  \class Source
 *  
 *  \brief Source operator generating the input stream
 *  
 *  This class implements the Source operator generating a data stream of items.
 */ 
template<typename tuple_t>
class Source: public ff::ff_a2a
{
public:
    /// type of the generation function (item-by-item version, briefly "itemized")
    using source_item_func_t = std::function<bool(tuple_t &)>;
    /// type of the rich generation function (item-by-item version, briefly "itemized")
    using rich_source_item_func_t = std::function<bool(tuple_t &, RuntimeContext &)>;
    /// type of the generation function (single-loop version, briefly "loop")
    using source_loop_func_t = std::function<bool(Shipper<tuple_t> &)>;
    /// type of the rich generation function (single-loop version, briefly "loop")
    using rich_source_loop_func_t = std::function<bool(Shipper<tuple_t> &, RuntimeContext &)>;
    /// type of the closing function
    using closing_func_t = std::function<void(RuntimeContext &)>;

private:
    // friendships with other classes in the library
    friend class MultiPipe;
    bool used; // true if the operator has been added/chained in a MultiPipe
    // class Source_Node
    class Source_Node: public ff::ff_node_t<tuple_t>
    {
    private:
        source_item_func_t source_func_item; // generation function (item-by-item version)
        rich_source_item_func_t rich_source_func_item; // rich generation function (item-by-item version)
        source_loop_func_t source_func_loop; // generation function (single-loop version)
        rich_source_loop_func_t rich_source_func_loop; // rich generation function (single-loop version)
        closing_func_t closing_func; // closing function
        std::string name; // string of the unique name of the operator
        bool isItemized; // flag stating whether we are using the item-by-item version of the generation function
        bool isRich; // flag stating whether the function to be used is rich (i.e. it receives the RuntimeContext object)
        bool isEND; // flag stating whether the Source_Node has completed to generate items
        Shipper<tuple_t> *shipper = nullptr; // shipper object used for the delivery of results (single-loop version)
        RuntimeContext context; // RuntimeContext
#if defined(TRACE_WINDFLOW)
        unsigned long sentTuples = 0;
        std::ofstream *logfile = nullptr;
#endif

    public:
        // Constructor I
        Source_Node(source_item_func_t _source_func_item,
                    std::string _name,
                    RuntimeContext _context,
                    closing_func_t _closing_func):
                    source_func_item(_source_func_item),
                    name(_name),
                    isItemized(true),
                    isRich(false),
                    isEND(false),
                    context(_context),
                    closing_func(_closing_func)
        {}

        // Constructor II
        Source_Node(rich_source_item_func_t _rich_source_func_item,
                    std::string _name,
                    RuntimeContext _context,
                    closing_func_t _closing_func):
                    rich_source_func_item(_rich_source_func_item),
                    name(_name),
                    isItemized(true),
                    isRich(true),
                    isEND(false), 
                    context(_context),
                    closing_func(_closing_func)
        {}

        // Constructor III
        Source_Node(source_loop_func_t _source_func_loop,
                    std::string _name,
                    RuntimeContext _context,
                    closing_func_t _closing_func):
                    source_func_loop(_source_func_loop),
                    name(_name),
                    isItemized(false),
                    isRich(false),
                    isEND(false),
                    context(_context),
                    closing_func(_closing_func)
        {}

        // Constructor IV
        Source_Node(rich_source_loop_func_t _rich_source_func_loop,
                    std::string _name,
                    RuntimeContext _context,
                    closing_func_t _closing_func):
                    rich_source_func_loop(_rich_source_func_loop),
                    name(_name),
                    isItemized(false),
                    isRich(true),
                    isEND(false),
                    context(_context),
                    closing_func(_closing_func)
        {}

        // svc_init method (utilized by the FastFlow runtime)
        int svc_init()
        {
            shipper = new Shipper<tuple_t>(*this);
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
        tuple_t *svc(tuple_t *)
        {
            // itemized version
            if (isItemized) {
                if (isEND) {
                    return this->EOS;
                }
                else {
                    // allocate the new tuple to be sent
                    tuple_t *t = new tuple_t();
                    if (!isRich) {
                        isEND = !source_func_item(*t); // call the generation function filling the tuple
                    }
                    else {
                        isEND = !rich_source_func_item(*t, context); // call the generation function filling the tuple
                    }
#if defined(TRACE_WINDFLOW)
                    sentTuples++;
#endif
                    return t;
                }
            }
            // single-loop version
            else {
                if (isEND) {
                    return this->EOS;
                }
                else {
                    if (!isRich) {
                        isEND = !source_func_loop(*shipper); // call the generation function sending some tuples through the shipper
                    }
                    else {
                        isEND = !rich_source_func_loop(*shipper, context); // call the generation function sending some tuples through the shipper
                    }
#if defined(TRACE_WINDFLOW)
                    sentTuples = shipper->delivered();
#endif
                    return this->GO_ON;
                }
            }
        }

        // svc_end method (utilized by the FastFlow runtime)
        void svc_end()
        {
            // call the closing function
            closing_func(context);
            delete shipper;
#if defined(TRACE_WINDFLOW)
            std::ostringstream stream;
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
     *  \brief Constructor
     *  
     *  \param _func function with signature accepted by the Source operator
     *  \param _pardegree parallelism degree of the Source operator
     *  \param _name string with the unique name of the Source operator
     *  \param _closing_func closing function
     */ 
    template<typename F_t>
    Source(F_t _func,
           size_t _pardegree,
           std::string _name,
           closing_func_t _closing_func):
           used(false)
    {
        // check the validity of the parallelism degree
        if (_pardegree == 0) {
            std::cerr << RED << "WindFlow Error: Source has parallelism zero" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // vector of Source_Node
        std::vector<ff_node *> first_set;
        for (size_t i=0; i<_pardegree; i++) {
            auto *seq = new Source_Node(_func, _name, RuntimeContext(_pardegree, i), _closing_func);
            first_set.push_back(seq);
        }
        // add first set
        ff::ff_a2a::add_firstset(first_set, 0, true);
        std::vector<ff_node *> second_set;
        second_set.push_back(new dummy_mi());
        // add second set
        ff::ff_a2a::add_secondset(second_set, true);
    }

    /** 
     *  \brief Check whether the Source has been used in a MultiPipe
     *  \return true if the Source has been added/chained to an existing MultiPipe
     */
    bool isUsed() const
    {
        return used;
    }

    /// deleted constructors/operators
    Source(const Source &) = delete; // copy constructor
    Source(Source &&) = delete; // move constructor
    Source &operator=(const Source &) = delete; // copy assignment operator
    Source &operator=(Source &&) = delete; // move assignment operator
};

} // namespace wf

#endif
