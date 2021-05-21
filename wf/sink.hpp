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
 *  and acopy assignment operator, and it must provide and implement the setControlFields()
 *  and getControlFields() methods.
 */ 

#ifndef SINK_H
#define SINK_H

/// includes
#include<string>
#if __cplusplus < 201703L // not C++17
    #include<experimental/optional>
    namespace std { using namespace experimental; }
#else
    #include<optional>
#endif
#include<ff/node.hpp>
#include<ff/combine.hpp>
#include<ff/pipeline.hpp>
#include<ff/multinode.hpp>
#include<ff/farm.hpp>
#include<basic.hpp>
#include<context.hpp>
#if defined (TRACE_WINDFLOW)
    #include<stats_record.hpp>
#endif
#include<basic_operator.hpp>
#include<transformations.hpp>
#include<standard_emitter.hpp>

namespace wf {

/** 
 *  \class Sink
 *  
 *  \brief Sink operator absorbing the input stream
 *  
 *  This class implements the Sink operator absorbing a data stream of items.
 */ 
template<typename tuple_t>
class Sink: public ff::ff_farm, public Basic_Operator
{
public:
    /// type of the sink function (receiving a reference to an optional containing the input)
    using sink_func_t = std::function<void(std::optional<tuple_t> &)>;
    /// type of the rich sink function (receiving a reference to an optional containing the input)
    using rich_sink_func_t = std::function<void(std::optional<tuple_t> &, RuntimeContext &)>;
    /// type of the sink function (receiving an optional containing a reference wrapper to the input)
    using sink_func_ref_t = std::function<void(std::optional<std::reference_wrapper<tuple_t>>)>;
    /// type of the rich sink function (receiving an optional containing a reference wrapper to the input)
    using rich_sink_func_ref_t = std::function<void(std::optional<std::reference_wrapper<tuple_t>>, RuntimeContext &)>;
    /// type of the closing function
    using closing_func_t = std::function<void(RuntimeContext &)>;
    /// type of the function to map the key hashcode onto an identifier starting from zero to parallelism-1
    using routing_func_t = std::function<size_t(size_t, size_t)>;

private:
    // friendships with other classes in the library
    friend class MultiPipe;
    std::string name; // name of the Sink
    size_t parallelism; // internal parallelism of the Sink
    bool keyed; // flag stating whether the Sink is configured with keyBy or not
    bool used; // true if the Sink has been added/chained in a MultiPipe
    // class Sink_Node
    class Sink_Node: public ff::ff_minode_t<tuple_t>
    {
    private:
        sink_func_t sink_func; // sink function (receiving a reference to an optional containing the input)
        rich_sink_func_t rich_sink_func; // rich sink function (receiving a reference to an optional containing the input)
        sink_func_ref_t sink_func_ref; // sink function (receiving an optional containing a reference wrapper to the input)
        rich_sink_func_ref_t rich_sink_func_ref; // rich sink function (receiving an optional containing a reference wrapper to the input)
        closing_func_t closing_func; // closing function
        std::string name; // string of the unique name of the operator
        bool isRich; // flag stating whether the function to be used is rich (i.e. it receives the RuntimeContext object)
        bool isRef; // flag stating whether the function to be used receives an optional containing the input (by value) o a reference wrapper to it
        RuntimeContext context; // RuntimeContext
        size_t eos_received; // number of received EOS messages
        bool terminated; // true if the replica has finished its work
#if defined (TRACE_WINDFLOW)
        Stats_Record stats_record;
        double avg_td_us = 0;
        double avg_ts_us = 0;
        volatile uint64_t startTD, startTS, endTD, endTS;
#endif

    public:
        // Constructor I
        Sink_Node(sink_func_t _sink_func,
                  std::string _name,
                  RuntimeContext _context,
                  closing_func_t _closing_func):
                  sink_func(_sink_func),
                  closing_func(_closing_func),
                  name(_name),
                  isRich(false),
                  isRef(false),
                  context(_context),
                  eos_received(0),
                  terminated(false) {}

        // Constructor II
        Sink_Node(rich_sink_func_t _rich_sink_func,
                  std::string _name,
                  RuntimeContext _context,
                  closing_func_t _closing_func):
                  rich_sink_func(_rich_sink_func),
                  closing_func(_closing_func),
                  name(_name),
                  isRich(true),
                  isRef(false),
                  context(_context),
                  eos_received(0),
                  terminated(false) {}

        // Constructor III
        Sink_Node(sink_func_ref_t _sink_func_ref,
                  std::string _name,
                  RuntimeContext _context,
                  closing_func_t _closing_func):
                  sink_func_ref(_sink_func_ref),
                  closing_func(_closing_func),
                  name(_name),
                  isRich(false),
                  isRef(true),
                  context(_context),
                  eos_received(0),
                  terminated(false) {}

        // Constructor IV
        Sink_Node(rich_sink_func_ref_t _rich_sink_func_ref,
                  std::string _name,
                  RuntimeContext _context,
                  closing_func_t _closing_func):
                  rich_sink_func_ref(_rich_sink_func_ref),
                  closing_func(_closing_func),
                  name(_name),
                  isRich(true),
                  isRef(true),
                  context(_context),
                  eos_received(0),
                  terminated(false) {}

        // svc_init method (utilized by the FastFlow runtime)
        int svc_init() override
        {
#if defined (TRACE_WINDFLOW)
            stats_record = Stats_Record(name, std::to_string(this->get_my_id()), false, false);
#endif
            return 0;
        }

        // svc method (utilized by the FastFlow runtime)
        tuple_t *svc(tuple_t *t) override
        {
#if defined (TRACE_WINDFLOW)
            startTS = current_time_nsecs();
            if (stats_record.inputs_received == 0) {
                startTD = current_time_nsecs();
            }
            stats_record.inputs_received++;
            stats_record.bytes_received += sizeof(tuple_t);
#endif
            if (!isRef) { // the optional encapsulates the input by value
                // create optional containing a copy of the input tuple
                std::optional<tuple_t> opt = std::make_optional(std::move(*t)); // try to move the input if it is possible
                // call the sink function
                if (!isRich) {
                    sink_func(opt);
                }
                else {
                    rich_sink_func(opt, context);
                }
            }
            else { // the optional encapsulates the input by a reference wrapper
                // create optional containing a reference wrapper to the input tuple
                std::optional<std::reference_wrapper<tuple_t>> opt = std::make_optional(std::ref(*t));
                // call the sink function
                if (!isRich) {
                    sink_func_ref(opt);
                }
                else {
                    rich_sink_func_ref(opt, context);
                }
            }
            // delete the received item
            delete t;
#if defined (TRACE_WINDFLOW)
            endTS = current_time_nsecs();
            endTD = current_time_nsecs();
            double elapsedTS_us = ((double) (endTS - startTS)) / 1000;
            avg_ts_us += (1.0 / stats_record.inputs_received) * (elapsedTS_us - avg_ts_us);
            double elapsedTD_us = ((double) (endTD - startTD)) / 1000;
            avg_td_us += (1.0 / stats_record.inputs_received) * (elapsedTD_us - avg_td_us);
            stats_record.service_time = std::chrono::duration<double, std::micro>(avg_ts_us);
            stats_record.eff_service_time = std::chrono::duration<double, std::micro>(avg_td_us);
            startTD = current_time_nsecs();
#endif
            return this->GO_ON;
        }

        // method to manage the EOS (utilized by the FastFlow runtime)
        void eosnotify(ssize_t id) override
        {
            eos_received++;
            // check the number of received EOS messages
            if ((eos_received != this->get_num_inchannels()) && (this->get_num_inchannels() != 0)) { // workaround due to FastFlow
                return;
            }
            terminated = true;
#if defined (TRACE_WINDFLOW)
            stats_record.set_Terminated();
#endif
        }

        // svc_end method (utilized by the FastFlow runtime)
        void svc_end() override
        {
            if (!isRef) {
                // create empty optional
                std::optional<tuple_t> opt;
                // call the sink function for the last time (empty optional)
                if (!isRich) {
                    sink_func(opt);
                }
                else {
                    rich_sink_func(opt, context);
                }
            }
            else {
                // create empty optional
                std::optional<std::reference_wrapper<tuple_t>> opt;
                // call the sink function for the last time (empty optional)
                if (!isRich) {
                    sink_func_ref(opt);
                }
                else {
                    rich_sink_func_ref(opt, context);
                }
            }
            // call the closing function
            closing_func(context);
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
    std::vector<Sink_Node *> sink_workers; // vector of pointers to the Sink_Node instances

public:
    /** 
     *  \brief Constructor I
     *  
     *  \param _func _func function with signature accepted by the Sink operator
     *  \param _parallelism internal parallelism of the Sink operator
     *  \param _name string name of the Sink operator
     *  \param _closing_func closing function
     */ 
    template<typename F_t>
    Sink(F_t _func,
         size_t _parallelism,
         std::string _name,
         closing_func_t _closing_func):
         name(_name),
         parallelism(_parallelism),
         keyed(false),
         used(false)
    {
        // check the validity of the parallelism value
        if (_parallelism == 0) {
            std::cerr << RED << "WindFlow Error: Sink has parallelism zero" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // std::vector of Sink_Node
        std::vector<ff_node *> w;
        for (size_t i=0; i<_parallelism; i++) {
            auto *seq = new Sink_Node(_func, _name, RuntimeContext(_parallelism, i), _closing_func);
            sink_workers.push_back(seq);
            auto *seq_comb = new ff::ff_comb(seq, new dummy_mo(), true, true);
            w.push_back(seq_comb);
        }
        // add emitter
        ff::ff_farm::add_emitter(new Standard_Emitter<tuple_t>(_parallelism));
        // add workers
        ff::ff_farm::add_workers(w);
        // when the Sink will be destroyed we need aslo to destroy the emitter and workers
        ff::ff_farm::cleanup_all();
    }

    /** 
     *  \brief Constructor II
     *  
     *  \param _func _func function with signature accepted by the Sink operator
     *  \param _parallelism internal parallelism of the Sink operator
     *  \param _name string name of the Sink operator
     *  \param _closing_func closing function
     *  \param _routing_func function to map the key hashcode onto an identifier starting from zero to parallelism-1
     */ 
    template<typename F_t>
    Sink(F_t _func,
         size_t _parallelism,
         std::string _name,
         closing_func_t _closing_func,
         routing_func_t _routing_func):
         name(_name),
         parallelism(_parallelism),
         keyed(true),
         used(false)
    {
        // check the validity of the parallelism value
        if (_parallelism == 0) {
            std::cerr << RED << "WindFlow Error: Sink has parallelism zero" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // std::vector of Sink_Node
        std::vector<ff_node *> w;
        for (size_t i=0; i<_parallelism; i++) {
            auto *seq = new Sink_Node(_func, _name, RuntimeContext(_parallelism, i), _closing_func);
            sink_workers.push_back(seq);
            auto *seq_comb = new ff::ff_comb(seq, new dummy_mo(), true, true);
            w.push_back(seq_comb);
        }
        // add emitter
        ff::ff_farm::add_emitter(new Standard_Emitter<tuple_t>(_routing_func, _parallelism));
        // add workers
        ff::ff_farm::add_workers(w);
        // when the Sink will be destroyed we need aslo to destroy the emitter and workers
        ff::ff_farm::cleanup_all();
    }

    /** 
     *  \brief Get the name of the Sink
     *  \return name of the Sink
     */ 
    std::string getName() const override
    {
        return name;
    }

    /** 
     *  \brief Get the total parallelism within the Sink
     *  \return total parallelism within the Sink
     */ 
    size_t getParallelism() const override
    {
        return parallelism;
    }

    /** 
     *  \brief Return the routing mode of inputs to the Sink
     *  \return routing mode used by the Sink
     */ 
    routing_modes_t getRoutingMode() const override
    {
        if (keyed) {
            return routing_modes_t::KEYBY;
        }
        else {
            return routing_modes_t::FORWARD;
        }
    }

    /** 
     *  \brief Check whether the Sink has been used in a MultiPipe
     *  \return true if the Sink has been added/chained to an existing MultiPipe
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
        for(auto *w: sink_workers) {
            auto *node = static_cast<Sink_Node *>(w);
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
        writer.String("Sink");
        writer.Key("Distribution");
        writer.String(keyed ? "KEYBY" : "FORWARD");
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
        for(auto *w: sink_workers) {
            auto *node = static_cast<Sink_Node *>(w);
            Stats_Record record = node->get_StatsRecord();
            record.append_Stats(writer);
        }
        writer.EndArray();
        writer.EndObject();    
    }
#endif

    /// deleted constructors/operators
    Sink(const Sink &) = delete; // copy constructor
    Sink(Sink &&) = delete; // move constructor
    Sink &operator=(const Sink &) = delete; // copy assignment operator
    Sink &operator=(Sink &&) = delete; // move assignment operator
};

} // namespace wf

#endif
