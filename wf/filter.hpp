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
 *  @file    filter.hpp
 *  @author  Gabriele Mencagli
 *  
 *  @brief Filter operator
 *  
 *  @section Filter (Description)
 *  
 *  This file implements the Filter operator able to execute streaming transformations
 *  producing zero or one output per input.
 */ 

#ifndef FILTER_H
#define FILTER_H

/// includes
#include<string>
#include<functional>
#include<context.hpp>
#include<batch_t.hpp>
#include<single_t.hpp>
#if defined (TRACE_WINDFLOW)
    #include<stats_record.hpp>
#endif
#include<basic_emitter.hpp>
#include<basic_operator.hpp>

namespace wf {

//@cond DOXY_IGNORE

// class Filter_Replica
template<typename filter_func_t>
class Filter_Replica: public ff::ff_monode
{
private:
    filter_func_t func; // functional logic used by the Filter replica
    using tuple_t = decltype(get_tuple_t_Filter(func)); // extracting the tuple_t type and checking the admissible signatures
    using result_t = decltype(get_result_t_Filter(func)); // extracting the result_t type and checking the admissible signatures
    // static predicates to check the type of the functional logic to be invoked
    static constexpr bool isNonRiched = std::is_invocable<decltype(func), tuple_t &>::value;
    static constexpr bool isRiched = std::is_invocable<decltype(func), tuple_t &, RuntimeContext &>::value;
    // check the presence of a valid functional logic
    static_assert(isNonRiched || isRiched,
        "WindFlow Compilation Error - Filter_Replica does not have a valid functional logic:\n");
    std::string opName; // name of the Filter containing the replica
    bool input_batching; // if true, the Filter replica expects to receive batches instead of individual inputs
    RuntimeContext context; // RuntimeContext object
    std::function<void(RuntimeContext &)> closing_func; // closing functional logic used by the Filter replica
    bool terminated; // true if the Filter replica has finished its work
    Basic_Emitter *emitter; // pointer to the used emitter
    size_t dropped_inputs; // number of "consecutive" dropped inputs
    uint64_t last_time_punct; // last time used to send punctuations
    Execution_Mode_t execution_mode; // execution mode of the Filter replica
#if defined (TRACE_WINDFLOW)
    Stats_Record stats_record;
    double avg_td_us = 0;
    double avg_ts_us = 0;
    volatile uint64_t startTD, startTS, endTD, endTS;
#endif

public:
    // Constructor
    Filter_Replica(filter_func_t _func,
                   std::string _opName,
                   RuntimeContext _context,
                   std::function<void(RuntimeContext &)> _closing_func):
                   func(_func),
                   opName(_opName),
                   input_batching(false),
                   context(_context),
                   closing_func(_closing_func),
                   terminated(false),
                   emitter(nullptr),
                   dropped_inputs(0),
                   execution_mode(Execution_Mode_t::DEFAULT) {}

    // Copy Constructor
    Filter_Replica(const Filter_Replica &_other):
                   func(_other.func),
                   opName(_other.opName),
                   input_batching(_other.input_batching),
                   context(_other.context),
                   closing_func(_other.closing_func),
                   terminated(_other.terminated),
                   dropped_inputs(_other.dropped_inputs),
                   execution_mode(_other.execution_mode)
    {
        if (_other.emitter == nullptr) {
            emitter = nullptr;
        }
        else {
            emitter = (_other.emitter)->clone(); // clone the emitter if it exists
        }
#if defined (TRACE_WINDFLOW)
        stats_record = _other.stats_record;
#endif
    }

    // Move Constructor
    Filter_Replica(Filter_Replica &&_other):
                   func(std::move(_other.func)),
                   opName(std::move(_other.opName)),
                   input_batching(_other.input_batching),
                   context(std::move(_other.context)),
                   closing_func(std::move(_other.closing_func)),
                   terminated(_other.terminated),
                   emitter(std::exchange(_other.emitter, nullptr)),
                   dropped_inputs(_other.dropped_inputs),
                   execution_mode(_other.execution_mode)
    {
#if defined (TRACE_WINDFLOW)
        stats_record = std::move(_other.stats_record);
#endif
    }

    // Destructor
    ~Filter_Replica()
    {
        if (emitter != nullptr) {
            delete emitter;
        }
    }

    // Copy Assignment Operator
    Filter_Replica &operator=(const Filter_Replica &_other)
    {
        if (this != &_other) {
            func = _other.func;
            opName = _other.opName;
            input_batching = _other.input_batching;
            context = _other.context;
            closing_func = _other.closing_func;
            terminated = _other.terminated;
            if (emitter != nullptr) {
                delete emitter;
            }
            if (_other.emitter == nullptr) {
                emitter = nullptr;
            }
            else {
                emitter = (_other.emitter)->clone(); // clone the emitter if it exists
            }
            dropped_inputs = _other.dropped_inputs;
            execution_mode = _other.execution_mode;
#if defined (TRACE_WINDFLOW)
            stats_record = _other.stats_record;
#endif
        }
        return *this;
    }

    // Move Assignment Operator
    Filter_Replica &operator=(Filter_Replica &_other)
    {
        func = std::move(_other.func);
        opName = std::move(_other.opName);
        input_batching = _other.input_batching;
        context = std::move(_other.context);
        closing_func = std::move(_other.closing_func);
        terminated = _other.terminated;
        if (emitter != nullptr) {
            delete emitter;
        }
        emitter = std::exchange(_other.emitter, nullptr);
        dropped_inputs = _other.dropped_inputs;
        execution_mode = _other.execution_mode;
#if defined (TRACE_WINDFLOW)
        stats_record = std::move(_other.stats_record);
#endif
        return *this;
    }

    // svc_init (utilized by the FastFlow runtime)
    int svc_init() override
    {
#if defined (TRACE_WINDFLOW)
        stats_record = Stats_Record(opName, std::to_string(context.getReplicaIndex()), false, false);
#endif
        last_time_punct = current_time_usecs();
        return 0;
    }

    // svc (utilized by the FastFlow runtime)
    void *svc(void *_in) override
    {
#if defined (TRACE_WINDFLOW)
        startTS = current_time_nsecs();
        if (stats_record.inputs_received == 0) {
            startTD = current_time_nsecs();
        }
#endif
        if (input_batching) { // receiving a batch
            Batch_t<decltype(get_tuple_t_Filter(func))> *batch_input = reinterpret_cast<Batch_t<decltype(get_tuple_t_Filter(func))> *>(_in);
            if (batch_input->isPunct()) { // if it is a punctuaton
                emitter->generate_punctuation(batch_input->getWatermark(context.getReplicaIndex()), this); // propagate the received punctuation
                deleteBatch_t(batch_input); // delete the punctuation
                return this->GO_ON;
            }
#if defined (TRACE_WINDFLOW)
            stats_record.inputs_received += batch_input->getSize();
            stats_record.bytes_received += batch_input->getSize() * sizeof(tuple_t);
#endif
            for (size_t i=0; i<batch_input->getSize(); i++) { // process all the inputs within the received batch
                process_input(batch_input->getTupleAtPos(i), batch_input->getTimestampAtPos(i), batch_input->getWatermark(context.getReplicaIndex()));
            }
            deleteBatch_t(batch_input); // delete the input batch
        }
        else { // receiving a single input
            Single_t<decltype(get_tuple_t_Filter(func))> *input = reinterpret_cast<Single_t<decltype(get_tuple_t_Filter(func))> *>(_in);
            if (input->isPunct()) { // if it is a punctuaton
                emitter->generate_punctuation(input->getWatermark(context.getReplicaIndex()), this); // propagate the received punctuation
                deleteSingle_t(input); // delete the punctuation
                return this->GO_ON;
            }
#if defined (TRACE_WINDFLOW)
            stats_record.inputs_received++;
            stats_record.bytes_received += sizeof(tuple_t);
#endif
            process_input(input);
        }
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

    // Process a single input (version 1)
    void process_input(Single_t<tuple_t> *_input)
    {
        if constexpr (isNonRiched) { // inplace non-riched version
            if (func(_input->tuple)) {
#if defined (TRACE_WINDFLOW)
                stats_record.outputs_sent++;
                stats_record.bytes_sent += sizeof(result_t);
#endif
                emitter->emit_inplace(_input, this);
                dropped_inputs = 0;
            }
            else {
                dropped_inputs++;
                if ((execution_mode == Execution_Mode_t::DEFAULT) && (dropped_inputs % DEFAULT_WM_AMOUNT == 0)) { // punctuaction auto-generation logic
                    if (current_time_usecs() - last_time_punct >= DEFAULT_WM_INTERVAL_USEC) {
                        emitter->generate_punctuation(_input->getWatermark(context.getReplicaIndex()), this); // generation of a new punctuation
                        last_time_punct = current_time_usecs();
                    }
                }
                deleteSingle_t(_input); // delete the input Single_t
            }
        }
        if constexpr (isRiched)  { // inplace riched version
            context.setContextParameters(_input->getTimestamp(), _input->getWatermark(context.getReplicaIndex())); // set the parameter of the RuntimeContext
            if (func(_input->tuple, context)) {
#if defined (TRACE_WINDFLOW)
                stats_record.outputs_sent++;
                stats_record.bytes_sent += sizeof(result_t);
#endif
                emitter->emit_inplace(_input, this);
                dropped_inputs = 0;
            }
            else {
                dropped_inputs++;
                if ((execution_mode == Execution_Mode_t::DEFAULT) && (dropped_inputs % DEFAULT_WM_AMOUNT == 0)) { // punctuaction auto-generation logic
                    if (current_time_usecs() - last_time_punct >= DEFAULT_WM_INTERVAL_USEC) {
                        emitter->generate_punctuation(_input->getWatermark(context.getReplicaIndex()), this); // generation of a new punctuation
                        last_time_punct = current_time_usecs();
                    }
                }
                deleteSingle_t(_input); // delete the input Single_t
            }
        }
    }

    // Process a single input (version 2)
    void process_input(tuple_t &_tuple,
                       uint64_t _timestamp,
                       uint64_t _watermark)
    {
        if constexpr (isNonRiched) { // inplace non-riched version
            if (func(_tuple)) {
#if defined (TRACE_WINDFLOW)
                stats_record.outputs_sent++;
                stats_record.bytes_sent += sizeof(result_t);
#endif
                emitter->emit(&_tuple, 0, _timestamp, _watermark, this);
                dropped_inputs = 0;
            }
            else {
                dropped_inputs++;
                if ((execution_mode == Execution_Mode_t::DEFAULT) && (dropped_inputs % DEFAULT_WM_AMOUNT == 0)) { // punctuaction auto-generation logic
                    if (current_time_usecs() - last_time_punct >= DEFAULT_WM_INTERVAL_USEC) {
                        emitter->generate_punctuation(_watermark, this); // generation of a new punctuation
                        last_time_punct = current_time_usecs();
                    }
                }
            }
        }
        if constexpr (isRiched)  { // inplace riched version
            context.setContextParameters(_timestamp, _watermark); // set the parameter of the RuntimeContext
            if (func(_tuple, context)) {
#if defined (TRACE_WINDFLOW)
                stats_record.outputs_sent++;
                stats_record.bytes_sent += sizeof(result_t);
#endif
                emitter->emit(&_tuple, 0, _timestamp, _watermark, this);
                dropped_inputs = 0;
            }
            else {
                dropped_inputs++;
                if ((execution_mode == Execution_Mode_t::DEFAULT) && (dropped_inputs % DEFAULT_WM_AMOUNT == 0)) { // punctuaction auto-generation logic
                    if (current_time_usecs() - last_time_punct >= DEFAULT_WM_INTERVAL_USEC) {
                        emitter->generate_punctuation(_watermark, this); // generation of a new punctuation
                        last_time_punct = current_time_usecs();
                    }
                }
            }
        }
    }

    // EOS management (utilized by the FastFlow runtime)
    void eosnotify(ssize_t id) override
    {
        emitter->flush(this); // call the flush of the emitter
        terminated = true;
#if defined (TRACE_WINDFLOW)
        stats_record.setTerminated();
#endif
    }

    // svc_end (utilized by the FastFlow runtime)
    void svc_end() override
    {
        closing_func(context); // call the closing function
    }

    // Configure the Filter replica to receive batches instead of individual inputs
    void receiveBatches(bool _input_batching)
    {
        input_batching = _input_batching;
    }

    // Set the emitter used to route outputs from the Filter replica
    void setEmitter(Basic_Emitter *_emitter)
    {
        emitter = _emitter;
    }

    // Check the termination of the Filter replica
    bool isTerminated() const
    {
        return terminated;
    }

    // Set the execution mode of the Filter replica
    void setExecutionMode(Execution_Mode_t _execution_mode)
    {
        execution_mode = _execution_mode;
    }

#if defined (TRACE_WINDFLOW)
    // Get a copy of the Stats_Record of the Filter replica
    Stats_Record getStatsRecord() const
    {
        return stats_record;
    }
#endif
};

//@endcond

/** 
 *  \class Filter
 *  
 *  \brief Filter operator
 *  
 *  This class implements the Filter operator executing a streaming transformation producing
 *  zero or one output per input.
 */ 
template<typename filter_func_t, typename key_extractor_func_t>
class Filter: public Basic_Operator
{
private:
    friend class MultiPipe; // friendship with the MultiPipe class
    friend class PipeGraph; // friendship with the PipeGraph class
    filter_func_t func; // functional logic used by the Filter
    key_extractor_func_t key_extr; // logic to extract the key attribute from the tuple_t
    size_t parallelism; // parallelism of the Filter
    std::string name; // name of the Filter
    Routing_Mode_t input_routing_mode; // routing mode of inputs to the Filter
    bool input_batching; // if true, the Filter expects to receive batches instead of individual inputs
    size_t outputBatchSize; // batch size of the outputs produced by the Filter
    std::vector<Filter_Replica<filter_func_t>*> replicas; // vector of pointers to the replicas of the Filter

    // Configure the Filter to receive batches instead of individual inputs
    void receiveBatches(bool _input_batching) override
    {
        for (auto *r: replicas) {
            r->receiveBatches(_input_batching);
        }
    }

    // Set the emitter used to route outputs from the Map
    void setEmitter(Basic_Emitter *_emitter) override
    {
        replicas[0]->setEmitter(_emitter);
        for (size_t i=1; i<replicas.size(); i++) {
            replicas[i]->setEmitter(_emitter->clone());
        }
    }

    // Check whether the Filter has terminated
    bool isTerminated() const override
    {
        bool terminated = true;
        for(auto *r: replicas) { // scan all the replicas to check their termination
            terminated = terminated && r->isTerminated();
        }
        return terminated;
    }

    // Set the execution mode of the Filter
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

#if defined (TRACE_WINDFLOW)
    // Dump the log file (JSON format) of statistics of the Filter
    void dumpStats() const override
    {
        std::ofstream logfile; // create and open the log file in the LOG_DIR directory
#if defined (LOG_DIR)
        std::string log_dir = std::string(STRINGIFY(LOG_DIR));
        std::string filename = std::string(STRINGIFY(LOG_DIR)) + "/" + std::to_string(getpid()) + "_" + name + ".json";
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
        this->appendStats(writer); // append the statistics of the Filter
        logfile << buffer.GetString();
        logfile.close();
    }

    // Append the statistics (JSON format) of the Filter to a PrettyWriter
    void appendStats(rapidjson::PrettyWriter<rapidjson::StringBuffer> &writer) const override
    {
        writer.StartObject(); // create the header of the JSON file
        writer.Key("Operator_name");
        writer.String(name.c_str());
        writer.Key("Operator_type");
        writer.String("Filter");
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
        writer.Key("OutputBatchSize");
        writer.Uint(outputBatchSize);
        writer.Key("Replicas");
        writer.StartArray();
        for (auto *r: replicas) { // append the statistics from all the replicas of the Filter
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
     *  \param _func functional logic of the Filter (a function or a callable type)
     *  \param _key_extr key extractor (a function or a callable type)
     *  \param _parallelism internal parallelism of the Filter
     *  \param _name name of the Filter
     *  \param _input_routing_mode input routing mode of the Filter
     *  \param _outputBatchSize size (in num of tuples) of the batches produced by this operator (0 for no batching)
     *  \param _closing_func closing functional logic of the Filter (a function or callable type)
     */ 
    Filter(filter_func_t _func,
           key_extractor_func_t _key_extr,
           size_t _parallelism,
           std::string _name,
           Routing_Mode_t _input_routing_mode,
           size_t _outputBatchSize,
           std::function<void(RuntimeContext &)> _closing_func):
           func(_func),
           key_extr(_key_extr),
           parallelism(_parallelism),
           name(_name),
           input_routing_mode(_input_routing_mode),
           input_batching(false),
           outputBatchSize(_outputBatchSize)
    {
        if (parallelism == 0) { // check the validity of the parallelism value
            std::cerr << RED << "WindFlow Error: Filter has parallelism zero" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        for (size_t i=0; i<parallelism; i++) { // create the internal replicas of the Filter
            replicas.push_back(new Filter_Replica<filter_func_t>(_func, name, RuntimeContext(parallelism, i), _closing_func));
        }
    }

    /// Copy constructor
    Filter(const Filter &_other):
           func(_other.func),
           key_extr(_other.key_extr),
           parallelism(_other.parallelism),
           name(_other.name),
           input_routing_mode(_other.input_routing_mode),
           input_batching(_other.input_batching),
           outputBatchSize(_other.outputBatchSize)
    {
        for (size_t i=0; i<parallelism; i++) { // deep copy of the pointers to the Filter replicas
            replicas.push_back(new Filter_Replica<filter_func_t>(*(_other.replicas[i])));
        }
    }

    // Destructor
    ~Filter() override
    {
        for (auto *r: replicas) { // delete all the replicas
            delete r;
        }
    }

    /// Copy Assignment Operator
    Filter& operator=(const Filter &_other)
    {
        if (this != &_other) {
            func = _other.func;
            key_extr = _other.key_extr;
            parallelism = _other.parallelism;
            name = _other.name;
            input_routing_mode = _other.input_routing_mode;
            input_batching = _other.input_batching;
            outputBatchSize = _other.outputBatchSize;
            for (auto *r: replicas) { // delete all the replicas
                delete r;
            }
            replicas.clear();
            for (size_t i=0; i<parallelism; i++) { // deep copy of the pointers to the Filter replicas
                replicas.push_back(new Filter_Replica<filter_func_t>(*(_other.replicas[i])));
            }
        }
        return *this;
    }

    /// Move Assignment Operator
    Filter& operator=(Filter &&_other)
    {
        func = std::move(_other.func);
        key_extr = std::move(_other.key_extr);
        parallelism = _other.parallelism;
        name = std::move(_other.name);
        input_routing_mode = _other.input_routing_mode;
        input_batching = _other.input_batching;
        outputBatchSize = _other.outputBatchSize;
        for (auto *r: replicas) { // delete all the replicas
            delete r;
        }
        replicas = std::move(_other.replicas);
        return *this;
    }

    /** 
     *  \brief Get the type of the Filter as a string
     *  \return type of the Filter
     */ 
    std::string getType() const override
    {
        return std::string("Filter");
    }

    /** 
     *  \brief Get the name of the Filter as a string
     *  \return name of the Filter
     */ 
    std::string getName() const override
    {
        return name;
    }

    /** 
     *  \brief Get the total parallelism of the Filter
     *  \return total parallelism of the Filter
     */  
    size_t getParallelism() const override
    {
        return parallelism;
    }

    /** 
     *  \brief Return the input routing mode of the Filter
     *  \return routing mode used to send inputs to the Filter
     */ 
    Routing_Mode_t getInputRoutingMode() const override
    {
        return input_routing_mode;
    }

    /** 
     *  \brief Return the size of the output batches that the Filter should produce
     *  \return output batch size in number of tuples
     */ 
    size_t getOutputBatchSize() const override
    {
        return outputBatchSize;
    }
};

} // namespace wf

#endif
