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
 *  Constructor and a copy assignment operator, and it must provide and implement
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
#if defined (TRACE_WINDFLOW)
    #include<stats_record.hpp>
#endif
#include<basic_operator.hpp>
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
class Source: public ff::ff_a2a, public Basic_Operator
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
    std::string name; // name of the Source
    size_t parallelism; // internal parallelism of the Source
    bool used; // true if the Source has been added/chained in a MultiPipe
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
        bool terminated; // true if the replica has finished its work
#if defined (TRACE_WINDFLOW)
        Stats_Record stats_record;
        double avg_td_us = 0;
        double avg_ts_us = 0;
        uint64_t source_func_calls = 0;
        uint64_t last_delivered_count = 0;
        volatile uint64_t startTD, startTS, endTD, endTS;
#endif

    public:
        // Constructor I
        Source_Node(source_item_func_t _source_func_item,
                    std::string _name,
                    RuntimeContext _context,
                    closing_func_t _closing_func):
                    source_func_item(_source_func_item),
                    closing_func(_closing_func),
                    name(_name),
                    isItemized(true),
                    isRich(false),
                    isEND(false),
                    context(_context),
                    terminated(false) {}

        // Constructor II
        Source_Node(rich_source_item_func_t _rich_source_func_item,
                    std::string _name,
                    RuntimeContext _context,
                    closing_func_t _closing_func):
                    rich_source_func_item(_rich_source_func_item),
                    closing_func(_closing_func),
                    name(_name),
                    isItemized(true),
                    isRich(true),
                    isEND(false), 
                    context(_context),
                    terminated(false) {}

        // Constructor III
        Source_Node(source_loop_func_t _source_func_loop,
                    std::string _name,
                    RuntimeContext _context,
                    closing_func_t _closing_func):
                    source_func_loop(_source_func_loop),
                    closing_func(_closing_func),
                    name(_name),
                    isItemized(false),
                    isRich(false),
                    isEND(false),
                    context(_context),
                    terminated(false) {}

        // Constructor IV
        Source_Node(rich_source_loop_func_t _rich_source_func_loop,
                    std::string _name,
                    RuntimeContext _context,
                    closing_func_t _closing_func):
                    rich_source_func_loop(_rich_source_func_loop),
                    closing_func(_closing_func),
                    name(_name),
                    isItemized(false),
                    isRich(true),
                    isEND(false),
                    context(_context),
                    terminated(false) {}

        // svc_init method (utilized by the FastFlow runtime)
        int svc_init() override
        {
            // create the shipper object used by this replica
            shipper = new Shipper<tuple_t>(*this);
#if defined (TRACE_WINDFLOW)
            stats_record = Stats_Record(name, std::to_string(this->get_my_id()), false, false);
#endif
            return 0;
        }

        // svc method (utilized by the FastFlow runtime)
        tuple_t *svc(tuple_t *) override
        {
#if defined (TRACE_WINDFLOW)
            startTS = current_time_nsecs();
            if (stats_record.outputs_sent == 0) {
                startTD = current_time_nsecs();
            }
            source_func_calls++;
#endif
            // itemized version
            if (isItemized) {
                if (isEND) {
                    terminated = true;
#if defined (TRACE_WINDFLOW)
                    stats_record.set_Terminated();
#endif
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
#if defined (TRACE_WINDFLOW)
                    endTS = current_time_nsecs();
                    endTD = current_time_nsecs();
                    double elapsedTS_us = ((double) (endTS - startTS)) / 1000;
                    avg_ts_us += (1.0 / source_func_calls) * (elapsedTS_us - avg_ts_us);
                    double elapsedTD_us = ((double) (endTD - startTD)) / 1000;
                    avg_td_us += (1.0 / source_func_calls) * (elapsedTD_us - avg_td_us);
                    stats_record.service_time = std::chrono::duration<double, std::micro>(avg_ts_us);
                    stats_record.eff_service_time = std::chrono::duration<double, std::micro>(avg_td_us);
                    stats_record.outputs_sent++;
                    stats_record.bytes_sent += sizeof(tuple_t);
                    startTD = current_time_nsecs();
#endif
                    return t;
                }
            }
            // single-loop version
            else {
                if (isEND) {
                    terminated = true;
#if defined (TRACE_WINDFLOW)
                    stats_record.set_Terminated();
#endif
                    return this->EOS;
                }
                else {
                    if (!isRich) {
                        isEND = !source_func_loop(*shipper); // call the generation function sending some tuples through the shipper
                    }
                    else {
                        isEND = !rich_source_func_loop(*shipper, context); // call the generation function sending some tuples through the shipper
                    }
#if defined (TRACE_WINDFLOW)
                    uint64_t delivered = (shipper->delivered() - last_delivered_count);
                    last_delivered_count = shipper->delivered();
                    endTS = current_time_nsecs();
                    endTD = current_time_nsecs();
                    double elapsedTS_us = ((double) (endTS - startTS)) / 1000;
                    avg_ts_us += (1.0 / source_func_calls) * (elapsedTS_us - avg_ts_us);
                    double elapsedTD_us = ((double) (endTD - startTD)) / 1000;
                    avg_td_us += (1.0 / source_func_calls) * (elapsedTD_us - avg_td_us);
                    stats_record.service_time = std::chrono::duration<double, std::micro>(avg_ts_us);
                    stats_record.eff_service_time = std::chrono::duration<double, std::micro>(avg_td_us);
                    stats_record.outputs_sent += delivered;
                    stats_record.bytes_sent += delivered * sizeof(tuple_t);
                    startTD = current_time_nsecs();
#endif
                    return this->GO_ON;
                }
            }
        }

        // svc_end method (utilized by the FastFlow runtime)
        void svc_end() override
        {
            // call the closing function
            closing_func(context);
            // delete the shipper object used by this replica
            delete shipper;
        }

        // method the check the termination of the replica
        bool isTerminated() const
        {
            return terminated;
        }

#if defined (TRACE_WINDFLOW)
        // method to return a copy of the Stats_Record of this node
        Stats_Record get_StatsRecord() const
        {
            return stats_record;
        }
#endif
    };

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _func function with signature accepted by the Source operator
     *  \param _parallelism internal parallelism of the Source operator
     *  \param _name name of the Source operator
     *  \param _closing_func closing function
     */ 
    template<typename F_t>
    Source(F_t _func,
           size_t _parallelism,
           std::string _name,
           closing_func_t _closing_func):
           name(_name),
           parallelism(_parallelism),
           used(false)
    {
        // check the validity of the parallelism value
        if (_parallelism == 0) {
            std::cerr << RED << "WindFlow Error: Source has parallelism zero" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // vector of Source_Node
        std::vector<ff_node *> first_set;
        for (size_t i=0; i<_parallelism; i++) {
            auto *seq = new Source_Node(_func, _name, RuntimeContext(_parallelism, i), _closing_func);
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
     *  \brief Get the name of the Source
     *  \return name of the Source
     */ 
    std::string getName() const override
    {
        return name;
    }

    /** 
     *  \brief Get the total parallelism within the Source
     *  \return total parallelism within the Source
     */ 
    size_t getParallelism() const override
    {
        return parallelism;
    }

    /** 
     *  \brief Return the routing mode of inputs to the Accumulator
     *  \return routing mode (always NONE for the Accumulator)
     */ 
    routing_modes_t getRoutingMode() const override
    {
        return routing_modes_t::NONE;
    }

    /** 
     *  \brief Check whether the Source has been used in a MultiPipe
     *  \return true if the Source has been added/chained to an existing MultiPipe
     */ 
    bool isUsed() const override
    {
        return used;
    }

    /** 
     *  \brief Check whether the operator has been terminated
     *  \return true if the operator has finished its work
     */ 
    virtual bool isTerminated() const override
    {
        bool terminated = true;
        // scan all the replicas to check their termination
        for(auto *w: this->getFirstSet()) {
            auto *node = static_cast<Source_Node *>(w);
            terminated = terminated && node->isTerminated(); 
        }
        return terminated;
    }

#if defined (TRACE_WINDFLOW)
    /// Dump the log file (JSON format) in the LOG_DIR directory
    void dump_LogFile() const override
    {
        // create and open the log file in the LOG_DIR directory
        std::ofstream logfile;
#if defined (LOG_DIR)
        std::string log_dir = std::string(STRINGIFY(LOG_DIR));
        std::string filename = std::string(STRINGIFY(LOG_DIR)) + "/" + std::to_string(getpid()) + "_" + name + ".json";
#else
        std::string log_dir = std::string("log");
        std::string filename = "log/" + std::to_string(getpid()) + "_" + name + ".json";
#endif
        // create the log directory
        if (mkdir(log_dir.c_str(), 0777) != 0) {
            struct stat st;
            if((stat(log_dir.c_str(), &st) != 0) || !S_ISDIR(st.st_mode)) {
                std::cerr << RED << "WindFlow Error: directory for log files cannot be created" << DEFAULT_COLOR << std::endl;
                exit(EXIT_FAILURE);
            }
        }
        logfile.open(filename);
        // create the rapidjson writer
        rapidjson::StringBuffer buffer;
        rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(buffer);
        // append the statistics of this operator
        this->append_Stats(writer);
        // serialize the object to file
        logfile << buffer.GetString();
        logfile.close();
    }

    // append the statistics of this operator
    void append_Stats(rapidjson::PrettyWriter<rapidjson::StringBuffer> &writer) const override
    {
        // create the header of the JSON file
        writer.StartObject();
        writer.Key("Operator_name");
        writer.String(name.c_str());
        writer.Key("Operator_type");
        writer.String("Source");
        writer.Key("Distribution");
        writer.String("NONE");
        writer.Key("isTerminated");
        writer.Bool(this->isTerminated());
        writer.Key("isWindowed");
        writer.Bool(false);
        writer.Key("isGPU");
        writer.Bool(false);
        writer.Key("Parallelism");
        writer.Uint(parallelism);
        writer.Key("Replicas");
        writer.StartArray();
        // get statistics from all the replicas of the operator
        for(auto *w: this->getFirstSet()) {
            auto *node = static_cast<Source_Node *>(w);
            Stats_Record record = node->get_StatsRecord();
            record.append_Stats(writer);
        }
        writer.EndArray();
        writer.EndObject();   
    }
#endif

    /// deleted constructors/operators
    Source(const Source &) = delete; // copy constructor
    Source(Source &&) = delete; // move constructor
    Source &operator=(const Source &) = delete; // copy assignment operator
    Source &operator=(Source &&) = delete; // move assignment operator
};

} // namespace wf

#endif
