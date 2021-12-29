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
 *  
 *  @brief Sink operator
 *  
 *  @section Sink (Description)
 *  
 *  This file implements the Sink operator able to absorb input streams.
 */ 

#ifndef SINK_H
#define SINK_H

/// includes
#include<string>
#include<functional>
#include<context.hpp>
#include<batch_t.hpp>
#include<single_t.hpp>
#if defined (WF_TRACING_ENABLED)
    #include<stats_record.hpp>
#endif
#include<basic_emitter.hpp>
#include<basic_operator.hpp>

namespace wf {

//@cond DOXY_IGNORE

// class Sink_Replica
template<typename sink_func_t>
class Sink_Replica: public ff::ff_monode
{
private:
    sink_func_t func; // functional logic used by the Sink replica
    using tuple_t = decltype(get_tuple_t_Sink(func)); // extracting the tuple_t type and checking the admissible signatures
    // static predicates to check the type of the functional logic to be invoked
    static constexpr bool isNonRichedNonWrapper = std::is_invocable<decltype(func), std::optional<tuple_t> &>::value;
    static constexpr bool isRichedNonWrapper = std::is_invocable<decltype(func), std::optional<tuple_t> &, RuntimeContext &>::value;
    static constexpr bool isNonRichedWrapper = std::is_invocable<decltype(func), std::optional<std::reference_wrapper<tuple_t>>>::value;
    static constexpr bool isRichedWrapper = std::is_invocable<decltype(func), std::optional<std::reference_wrapper<tuple_t>>, RuntimeContext &>::value;
    // check the presence of a valid functional logic
    static_assert(isNonRichedNonWrapper || isRichedNonWrapper || isNonRichedWrapper || isRichedWrapper,
        "WindFlow Compilation Error - Sink_Replica does not have a valid functional logic:\n");
    std::string opName; // name of the Sink containing the replica
    bool input_batching; // if true, the Sink replica expects to receive batches instead of individual inputs
    RuntimeContext context; // RuntimeContext object
    std::function<void(RuntimeContext &)> closing_func; // closing functional logic used by the Sink replica
    bool terminated; // true if the Sink replica has finished its work
    Execution_Mode_t execution_mode; // execution mode of the Sink replica
#if defined (WF_TRACING_ENABLED)
    Stats_Record stats_record;
    double avg_td_us = 0;
    double avg_ts_us = 0;
    volatile uint64_t startTD, startTS, endTD, endTS;
#endif

public:
    // Constructor
    Sink_Replica(sink_func_t _func,
                 std::string _opName,
                 RuntimeContext _context,
                 std::function<void(RuntimeContext &)> _closing_func):
                 func(_func),
                 opName(_opName),
                 input_batching(false),
                 context(_context),
                 closing_func(_closing_func),
                 terminated(false),
                 execution_mode(Execution_Mode_t::DEFAULT) {}

    // svc_init (utilized by the FastFlow runtime)
    int svc_init() override
    {
#if defined (WF_TRACING_ENABLED)
        stats_record = Stats_Record(opName, std::to_string(context.getReplicaIndex()), false, false);
#endif
        return 0;
    }

    // svc (utilized by the FastFlow runtime)
    void *svc(void *_in) override
    {
#if defined (WF_TRACING_ENABLED)
        startTS = current_time_nsecs();
        if (stats_record.inputs_received == 0) {
            startTD = current_time_nsecs();
        }
#endif
        if (input_batching) { // receiving a batch
            Batch_t<decltype(get_tuple_t_Sink(func))> *batch_input = reinterpret_cast<Batch_t<decltype(get_tuple_t_Sink(func))> *>(_in);
            if (batch_input->isPunct()) { // if it is a punctuaton
                deleteBatch_t(batch_input); // delete the punctuation
                return this->GO_ON;
            }
#if defined (WF_TRACING_ENABLED)
            stats_record.inputs_received += batch_input->getSize();
            stats_record.bytes_received += batch_input->getSize() * sizeof(tuple_t);
#endif
            for (size_t i=0; i<batch_input->getSize(); i++) { // process all the inputs within the received batch
                process_input(batch_input->getTupleAtPos(i), batch_input->getTimestampAtPos(i), batch_input->getWatermark(context.getReplicaIndex()));
            }
            deleteBatch_t(batch_input); // delete the input batch
        }
        else { // receiving a single input
            Single_t<decltype(get_tuple_t_Sink(func))> *input = reinterpret_cast<Single_t<decltype(get_tuple_t_Sink(func))> *>(_in);
            if (input->isPunct()) { // if it is a punctuaton
                deleteSingle_t(input); // delete the punctuation
                return this->GO_ON;
            }
#if defined (WF_TRACING_ENABLED)
            stats_record.inputs_received++;
            stats_record.bytes_received += sizeof(tuple_t);
#endif
            process_input(input->tuple, input->getTimestamp(), input->getWatermark(context.getReplicaIndex()));
            deleteSingle_t(input); // delete the input Single_t
        }
#if defined (WF_TRACING_ENABLED)
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

    // Process a single input
    void process_input(tuple_t &_tuple,
                       uint64_t _timestamp,
                       uint64_t _watermark)
    {
        
        if constexpr (isNonRichedNonWrapper) { // non-riched non-wrapper version
            std::optional<decltype(get_tuple_t_Sink(func))> opt_tuple = std::make_optional(std::move(_tuple)); // move the input tuple in the optional
            func(opt_tuple);
        }
        if constexpr (isRichedNonWrapper)  { // riched non-wrapper version
            context.setContextParameters(_timestamp, _watermark); // set the parameter of the RuntimeContext
            std::optional<decltype(get_tuple_t_Sink(func))> opt_tuple = std::make_optional(std::move(_tuple)); // move the input tuple in the optional
            func(opt_tuple, context);
        }
        if constexpr (isNonRichedWrapper) { // non-riched wrapper version
            std::optional<std::reference_wrapper<decltype(get_tuple_t_Sink(func))>> opt_wtuple = std::make_optional(std::ref(_tuple));
            func(opt_wtuple);
        }
        if constexpr (isRichedWrapper) { // riched wrapper version
            context.setContextParameters(_timestamp, _watermark); // set the parameter of the RuntimeContext
            std::optional<std::reference_wrapper<decltype(get_tuple_t_Sink(func))>> opt_wtuple = std::make_optional(std::ref(_tuple));
            func(opt_wtuple, context);
        }
    }

    // EOS management (utilized by the FastFlow runtime)
    void eosnotify(ssize_t id) override
    {
        terminated = true;
#if defined (WF_TRACING_ENABLED)
        stats_record.setTerminated();
#endif
    }

    // svc_end (utilized by the FastFlow runtime)
    void svc_end() override
    {
        
        if constexpr (isNonRichedNonWrapper) { // non-riched non-wrapper version
            std::optional<decltype(get_tuple_t_Sink(func))> opt_empty; // create empty optional
            func(opt_empty);
        }
        if constexpr (isRichedNonWrapper)  { // riched non-wrapper version
            std::optional<decltype(get_tuple_t_Sink(func))> opt_empty; // create empty optional
            func(opt_empty, context);
        }
        if constexpr (isNonRichedWrapper) { // non-riched wrapper version
            std::optional<std::reference_wrapper<decltype(get_tuple_t_Sink(func))>> wopt_empty;
            func(wopt_empty);
        }
        if constexpr (isRichedWrapper) { // riched wrapper version
            std::optional<std::reference_wrapper<decltype(get_tuple_t_Sink(func))>> wopt_empty;
            func(wopt_empty, context);
        }
        closing_func(context); // call the closing function
    }

    // Configure the Sink replica to receive batches instead of individual inputs
    void receiveBatches(bool _input_batching)
    {
        input_batching = _input_batching;
    }

    // Check the termination of the Sink replica
    bool isTerminated() const
    {
        return terminated;
    }

    // Set the execution mode of the Sink replica
    void setExecutionMode(Execution_Mode_t _execution_mode)
    {
        execution_mode = _execution_mode;
    }

#if defined (WF_TRACING_ENABLED)
    // Get a copy of the Stats_Record of the Sink replica
    Stats_Record getStatsRecord() const
    {
        return stats_record;
    }
#endif
};

//@endcond

/** 
 *  \class Sink
 *  
 *  \brief Sink operator
 *  
 *  This class implements the Sink operator able to absorb input streams
 */ 
template<typename sink_func_t, typename key_extractor_func_t>
class Sink: public Basic_Operator
{
private:
    friend class MultiPipe; // friendship with the MultiPipe class
    friend class PipeGraph; // friendship with the PipeGraph class
    sink_func_t func; // functional logic used by the Sink
    key_extractor_func_t key_extr; // logic to extract the key attribute from the tuple_t
    size_t parallelism; // parallelism of the Sink
    std::string name; // name of the Sink
    Routing_Mode_t input_routing_mode; // routing mode of inputs to the Sink
    bool input_batching; // if true, the Sink expects to receive batches instead of individual inputs
    std::vector<Sink_Replica<sink_func_t>*> replicas; // vector of pointers to the replicas of the Sink

    // Configure the Sink to receive batches instead of individual inputs
    void receiveBatches(bool _input_batching) override
    {
        for (auto *r: replicas) {
            r->receiveBatches(_input_batching);
        }
    }

    // Set the emitter used to route outputs from the Sink (cannot be called for the Sink)
    void setEmitter(Basic_Emitter *_emitter) override
    { 
        abort(); // <-- this method cannot be used!
    }

    // Check whether the Sink has terminated
    bool isTerminated() const override
    {
        bool terminated = true;
        for(auto *r: replicas) { // scan all the replicas to check their termination
            terminated = terminated && r->isTerminated();
        }
        return terminated;
    }

    // Set the execution mode of the Sink (i.e., the one of its PipeGraph)
    void setExecutionMode(Execution_Mode_t _execution_mode)
    {
        for (auto *r: replicas) {
            r->setExecutionMode(_execution_mode);
        }
    }

    // Get the logic to extract the key attribute from the tuple_t
    key_extractor_func_t getKeyExtractor() const
    {
        return key_extr;
    }

#if defined (WF_TRACING_ENABLED)
    // Dump the log file (JSON format) of statistics of the Sink
    void dumpStats() const override
    {
        std::ofstream logfile; // create and open the log file in the WF_LOG_DIR directory
#if defined (WF_LOG_DIR)
        std::string log_dir = std::string(STRINGIFY(WF_LOG_DIR));
        std::string filename = std::string(STRINGIFY(WF_LOG_DIR)) + "/" + std::to_string(getpid()) + "_" + name + ".json";
#else
        std::string log_dir = std::string("log");
        std::string filename = "log/" + std::to_string(getpid()) + "_" + name + ".json";
#endif
        if (mkdir(log_dir.c_str(), 0777) != 0) { // create the log directory
            struct stat st;
            if((stat(log_dir.c_str(), &st) != 0) || !S_ISDIR(st.st_mode)) {
                std::cerr << RED << "WindFlow Error: directory for log files cannot be created" << DEFAULT_COLOR << std::endl;
                exit(EXIT_FAILURE);
            }
        }
        logfile.open(filename);
        rapidjson::StringBuffer buffer; // create the rapidjson writer
        rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(buffer);
        this->appendStats(writer); // append the statistics of the Sink
        logfile << buffer.GetString();
        logfile.close();
    }

    // Append the statistics (JSON format) of the Sink to a PrettyWriter
    void appendStats(rapidjson::PrettyWriter<rapidjson::StringBuffer> &writer) const override
    {
        writer.StartObject(); // create the header of the JSON file
        writer.Key("Operator_name");
        writer.String(name.c_str());
        writer.Key("Operator_type");
        writer.String("Sink");
        writer.Key("Distribution");
        if (input_routing_mode == Routing_Mode_t::KEYBY) {
            writer.String("KEYBY");
        }
        else {
            writer.String("FORWARD");
        }
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
        for (auto *r: replicas) { // append the statistics from all the replicas of the Sink
            Stats_Record record = r->getStatsRecord();
            record.appendStats(writer);
        }
        writer.EndArray();
        writer.EndObject();
    }
#endif

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _func functional logic of the Sink (a function or a callable type)
     *  \param _key_extr key extractor (a function or a callable type)
     *  \param _parallelism internal parallelism of the Sink
     *  \param _name name of the Sink
     *  \param _input_routing_mode input routing mode of the Sink
     *  \param _closing_func closing functional logic of the Sink (a function or callable type)
     */ 
    Sink(sink_func_t _func,
         key_extractor_func_t _key_extr,
         size_t _parallelism,
         std::string _name,
         Routing_Mode_t _input_routing_mode,
         std::function<void(RuntimeContext &)> _closing_func):
         func(_func),
         key_extr(_key_extr),
         parallelism(_parallelism),
         name(_name),
         input_routing_mode(_input_routing_mode),
         input_batching(false)
    {
        if (parallelism == 0) { // check the validity of the parallelism value
            std::cerr << RED << "WindFlow Error: Sink has parallelism zero" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        for (size_t i=0; i<parallelism; i++) { // create the internal replicas of the Sink
            replicas.push_back(new Sink_Replica<sink_func_t>(_func, name, RuntimeContext(parallelism, i), _closing_func));
        }
    }

    /// Copy Constructor
    Sink(const Sink &_other):
         func(_other.func),
         key_extr(_other.key_extr),
         parallelism(_other.parallelism),
         name(_other.name),
         input_routing_mode(_other.input_routing_mode),
         input_batching(_other.input_batching)
    {
        for (size_t i=0; i<parallelism; i++) { // deep copy of the pointers to the Sink replicas
            replicas.push_back(new Sink_Replica<sink_func_t>(*(_other.replicas[i])));
        }
    }

    // Destructor
    ~Sink() override
    {
        for (auto *r: replicas) { // delete all the replicas
            delete r;
        }
    }

    /// Copy Assignment Operator
    Sink& operator=(const Sink &_other)
    {
        if (this != _other) {
            func = _other.func;
            key_extr = _other.key_extr;
            parallelism = _other.parallelism;
            name = _other.name;
            input_routing_mode = _other.input_routing_mode;
            input_batching = _other.input_batching;
            for (auto *r: replicas) { // delete all the replicas
                delete r;
            }
            replicas.clear();
            for (size_t i=0; i<parallelism; i++) { // deep copy of the pointers to the Sink replicas
                replicas.push_back(new Sink_Replica<sink_func_t>(*(_other.replicas[i])));
            }
        }
        return *this;
    }

    /// Move Assignment Operator
    Sink& operator=(Sink &&_other)
    {
        func = std::move(_other.func);
        key_extr = std::move(_other.key_extr);
        parallelism = _other.parallelism;
        name = std::move(_other.name);
        input_routing_mode = _other.input_routing_mode;
        input_batching = _other.input_batching;
        for (auto *r: replicas) { // delete all the replicas
            delete r;
        }
        replicas = std::move(_other.replicas);
        return *this;
    }

    /** 
     *  \brief Get the type of the Sink as a string
     *  \return type of the Sink
     */ 
    std::string getType() const override
    {
        return std::string("Sink");
    }

    /** 
     *  \brief Get the name of the Sink as a string
     *  \return name of the Sink
     */ 
    std::string getName() const override
    {
        return name;
    }

    /** 
     *  \brief Get the total parallelism of the Sink
     *  \return total parallelism of the Sink
     */  
    size_t getParallelism() const override
    {
        return parallelism;
    }

    /** 
     *  \brief Return the input routing mode of the Sink
     *  \return routing mode used to send inputs to the Sink
     */ 
    Routing_Mode_t getInputRoutingMode() const override
    {
        return input_routing_mode;
    }

    /** 
     *  \brief Return the size of the output batches that the Sink should produce (cannot be called for the Sink)
     *  \return output batch size in number of tuples
     */ 
    size_t getOutputBatchSize() const override
    {
        return 1;
    };
};

} // namespace wf

#endif
