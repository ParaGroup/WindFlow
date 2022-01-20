/**************************************************************************************
 *  Copyright (c) 2019- Gabriele Mencagli
 *  
 *  This file is part of WindFlow.
 *  
 *  WindFlow is free software dual licensed under the GNU LGPL or MIT License.
 *  You can redistribute it and/or modify it under the terms of the
 *    * GNU Lesser General Public License as published by
 *      the Free Software Foundation, either version 3 of the License, or
 *      (at your option) any later version
 *    OR
 *    * MIT License: https://github.com/ParaGroup/WindFlow/blob/vers3.x/LICENSE.MIT
 *  
 *  WindFlow is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Lesser General Public License for more details.
 *  You should have received a copy of the GNU Lesser General Public License and
 *  the MIT License along with WindFlow. If not, see <http://www.gnu.org/licenses/>
 *  and <http://opensource.org/licenses/MIT/>.
 **************************************************************************************
 */

/** 
 *  @file    reduce.hpp
 *  @author  Gabriele Mencagli
 *  
 *  @brief Reduce operator
 *  
 *  @section Reduce (Description)
 *  
 *  This file implements the Reduce operator able to execute rolling reduce associative
 *  and commutative functions over input streams.
 */ 

#ifndef REDUCE_H
#define REDUCE_H

/// includes
#include<string>
#include<functional>
#include<unordered_map>
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

// class Reduce_Replica
template<typename reduce_func_t, typename key_extractor_func_t>
class Reduce_Replica: public ff::ff_monode
{
private:
    reduce_func_t func; // functional logic used by the Reduce replica
    key_extractor_func_t key_extr; // logic to extract the key attribute from the tuple_t
    using tuple_t = decltype(get_tuple_t_Reduce(func)); // extracting the tuple_t type and checking the admissible signatures
    using state_t = decltype(get_state_t_Reduce(func)); // extracting the state_t type and checking the admissible signatures
    using key_t = decltype(get_key_t_KeyExtr(key_extr)); // extracting the key_t type and checking the admissible singatures
    // static predicates to check the type of the functional logic to be invoked
    static constexpr bool isNonRiched = std::is_invocable<decltype(func), const tuple_t &, state_t &>::value;
    static constexpr bool isRiched = std::is_invocable<decltype(func), const tuple_t &, state_t &, RuntimeContext &>::value;
    // check the presence of a valid functional logic
    static_assert(isNonRiched || isRiched,
        "WindFlow Compilation Error - Reduce_Replica does not have a valid functional logic:\n");
    std::string opName; // name of the Reduce containing the replica
    bool input_batching; // if true, the Reduce replica expects to receive batches instead of individual inputs
    RuntimeContext context; // RuntimeContext object
    std::function<void(RuntimeContext &)> closing_func; // closing functional logic used by the Reduce replica
    bool terminated; // true if the Reduce replica has finished its work
    Basic_Emitter *emitter; // pointer to the used emitter
    std::unordered_map<key_t, state_t> keyMap; // hashtable mapping key values onto the corresponding state objects
    state_t initial_state; // initial state to be copied for each new key
    Execution_Mode_t execution_mode; // execution mode of the Reduce replica
#if defined (WF_TRACING_ENABLED)
    Stats_Record stats_record;
    double avg_td_us = 0;
    double avg_ts_us = 0;
    volatile uint64_t startTD, startTS, endTD, endTS;
#endif

public:
    // Constructor
    Reduce_Replica(reduce_func_t _func,
                   key_extractor_func_t _key_extr,
                   std::string _opName,
                   RuntimeContext _context,
                   std::function<void(RuntimeContext &)> _closing_func,
                   state_t _initial_state):
                   func(_func),
                   key_extr(_key_extr),
                   opName(_opName),
                   input_batching(false),
                   context(_context),
                   closing_func(_closing_func),
                   terminated(false),
                   emitter(nullptr),
                   initial_state(_initial_state),
                   execution_mode(Execution_Mode_t::DEFAULT) {}

    // Copy Constructor
    Reduce_Replica(const Reduce_Replica &_other):
                   func(_other.func),
                   key_extr(_other.key_extr),
                   opName(_other.opName),
                   input_batching(_other.input_batching),
                   context(_other.context),
                   closing_func(_other.closing_func),
                   terminated(_other.terminated),
                   keyMap(_other.keyMap),
                   initial_state(_other.initial_state),
                   execution_mode(_other.execution_mode)
    {
        if (_other.emitter == nullptr) {
            emitter = nullptr;
        }
        else {
            emitter = (_other.emitter)->clone(); // clone the emitter if it exists
        }
#if defined (WF_TRACING_ENABLED)
        stats_record = _other.stats_record;
#endif
    }

    // Move Constructor
    Reduce_Replica(Reduce_Replica &&_other):
                   func(std::move(_other.func)),
                   key_extr(std::move(_other.key_extr)),
                   opName(std::move(_other.opName)),
                   input_batching(_other.input_batching),
                   context(std::move(_other.context)),
                   closing_func(std::move(_other.closing_func)),
                   terminated(_other.terminated),
                   emitter(std::exchange(_other.emitter, nullptr)),
                   keyMap(std::move(_other.keyMap)),
                   initial_state(std::move(initial_state)),
                   execution_mode(_other.execution_mode)
    {
#if defined (WF_TRACING_ENABLED)
        stats_record = std::move(_other.stats_record);
#endif
    }

    // Destructor
    ~Reduce_Replica()
    {
        if (emitter != nullptr) {
            delete emitter;
        }
    }

    // Copy Assignment Operator
    Reduce_Replica &operator=(const Reduce_Replica &_other)
    {
        if (this != &_other) {
            func = _other.func;
            key_extr = _other.key_extr;
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
            keyMap = _other.keyMap;
            initial_state = _other.initial_state;
            execution_mode = _other.execution_mode;     
#if defined (WF_TRACING_ENABLED)
            stats_record = _other.stats_record;
#endif
        }
        return *this;
    }

    // Move Assignment Operator
    Reduce_Replica &operator=(Reduce_Replica &_other)
    {
        func = std::move(_other.func);
        key_extr = std::move(_other.key_extr);
        opName = std::move(_other.opName);
        input_batching = _other.input_batching;
        context = std::move(_other.context);
        closing_func = std::move(_other.closing_func);
        terminated = _other.terminated;
        if (emitter != nullptr) {
            delete emitter;
        }
        emitter = std::exchange(_other.emitter, nullptr);
        keyMap = std::move(_other.keyMap);
        initial_state = std::move(initial_state);
        execution_mode = _other.execution_mode;
#if defined (WF_TRACING_ENABLED)
        stats_record = std::move(_other.stats_record);
#endif
        return *this;
    }

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
            Batch_t<decltype(get_tuple_t_Reduce(func))> *batch_input = reinterpret_cast<Batch_t<decltype(get_tuple_t_Reduce(func))> *>(_in);
            if (batch_input->isPunct()) { // if it is a punctuaton
                emitter->propagate_punctuation(batch_input->getWatermark(context.getReplicaIndex()), this); // propagate the received punctuation
                deleteBatch_t(batch_input); // delete the punctuaton
                return this->GO_ON;
            }
#if defined (WF_TRACING_ENABLED)
            stats_record.inputs_received += batch_input->getSize();
            stats_record.bytes_received += batch_input->getSize() * sizeof(tuple_t);
            stats_record.outputs_sent += batch_input->getSize();
            stats_record.bytes_sent += batch_input->getSize()* sizeof(state_t);
#endif
            for (size_t i=0; i<batch_input->getSize(); i++) { // process all the inputs within the batch
                process_input(batch_input->getTupleAtPos(i), batch_input->getTimestampAtPos(i), batch_input->getWatermark(context.getReplicaIndex()));
            }
            deleteBatch_t(batch_input); // delete the input batch
        }
        else { // receiving a single input
            Single_t<decltype(get_tuple_t_Reduce(func))> *input = reinterpret_cast<Single_t<decltype(get_tuple_t_Reduce(func))> *>(_in);
            if (input->isPunct()) { // if it is a punctuaton
                emitter->propagate_punctuation(input->getWatermark(context.getReplicaIndex()), this); // propagate the received punctuation
                deleteSingle_t(input); // delete the punctuaton
                return this->GO_ON;
            }
#if defined (WF_TRACING_ENABLED)
            stats_record.inputs_received++;
            stats_record.bytes_received += sizeof(tuple_t);
            stats_record.outputs_sent++;
            stats_record.bytes_sent += sizeof(state_t);
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
        auto key = key_extr(_tuple); // get the key attribute of the input tuple
        auto it = keyMap.find(key); // find the corresponding state (or allocate it if does not exist)
        if (it == keyMap.end()) {
            auto p = keyMap.insert(std::make_pair(key, state_t(initial_state))); // create the state of the key by copying the initial_state
            it = p.first;
        }
        if constexpr (isNonRiched) { // inplace non-riched version
            func(_tuple, (*it).second);
        }
        if constexpr (isRiched)  { // inplace riched version
            context.setContextParameters(_timestamp, _watermark); // set the parameter of the RuntimeContext
            func(_tuple, (*it).second, context);
        }
        decltype(get_state_t_Reduce(func)) result = (*it).second; // the result is a copy of the present state
        emitter->emit(&result, 0, _timestamp, _watermark, this);
    }

    // EOS management (utilized by the FastFlow runtime)
    void eosnotify(ssize_t id) override
    {
        emitter->flush(this); // call the flush of the emitter
        terminated = true;
#if defined (WF_TRACING_ENABLED)
        stats_record.setTerminated();
#endif
    }

    // svc_end (utilized by the FastFlow runtime)
    void svc_end() override
    {
        closing_func(context); // call the closing function
    }

    // Configure the Reduce replica to receive batches instead of individual inputs
    void receiveBatches(bool _input_batching)
    {
        input_batching = _input_batching;
    }

    // Set the emitter used to route outputs from the Reduce replica
    void setEmitter(Basic_Emitter *_emitter)
    {
        emitter = _emitter;
    }

    // Check the termination of the Reduce replica
    bool isTerminated() const
    {
        return terminated;
    }

    // Set the execution mode of the Reduce replica
    void setExecutionMode(Execution_Mode_t _execution_mode)
    {
        execution_mode = _execution_mode;
    }

#if defined (WF_TRACING_ENABLED)
    // Get a copy of the Stats_Record of the Reduce replica
    Stats_Record getStatsRecord() const
    {
        return stats_record;
    }
#endif
};

//@endcond

/** 
 *  \class Reduce
 *  
 *  \brief Reduce operator
 *  
 *  This class implements the Reduce operator executing a rolling reduce/fold function
 *  over the input stream.
 */ 
template<typename reduce_func_t, typename key_extractor_func_t>
class Reduce: public Basic_Operator
{
private:
    friend class MultiPipe; // friendship with the MultiPipe class
    friend class PipeGraph; // friendship with the PipeGraph class
    reduce_func_t func; // functional logic used by the Reduce
    key_extractor_func_t key_extr; // logic to extract the key attribute from the tuple_t
    using state_t = decltype(get_state_t_Reduce(func)); // extracting the state_t type and checking the admissible signatures
    size_t parallelism; // parallelism of the Reduce
    std::string name; // name of the Reduce
    bool input_batching; // if true, the Reduce expects to receive batches instead of individual inputs
    size_t outputBatchSize; // batch size of the outputs produced by the Reduce
    std::vector<Reduce_Replica<reduce_func_t, key_extractor_func_t>*> replicas; // vector of pointers to the replicas of the Reduce

    // Configure the Reduce to receive batches instead of individual inputs
    void receiveBatches(bool _input_batching) override
    {
        for (auto *r: replicas) {
            r->receiveBatches(_input_batching);
        }
    }

    // Set the emitter used to route outputs from the Reduce
    void setEmitter(Basic_Emitter *_emitter) override
    {
        replicas[0]->setEmitter(_emitter);
        for (size_t i=1; i<replicas.size(); i++) {
            replicas[i]->setEmitter(_emitter->clone());
        }
    }

    // Check whether the Reduce has terminated
    bool isTerminated() const override
    {
        bool terminated = true;
        for (auto *r: replicas) { // scan all the replicas to check their termination
            terminated = terminated && r->isTerminated();
        }
        return terminated;
    }

    // Set the execution mode of the Reduce
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
    // Dump the log file (JSON format) of statistics of the Reduce
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
        this->appendStats(writer); // append the statistics of the Reduce
        logfile << buffer.GetString();
        logfile.close();
    }

    // Append the statistics (JSON format) of the Reduce to a PrettyWriter
    void appendStats(rapidjson::PrettyWriter<rapidjson::StringBuffer> &writer) const override
    {
        writer.StartObject(); // create the header of the JSON file
        writer.Key("Operator_name");
        writer.String(name.c_str());
        writer.Key("Operator_type");
        writer.String("Reduce");
        writer.Key("Distribution");
        writer.String("KEYBY");
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
        for (auto *r: replicas) { // append the statistics from all the replicas of the Reduce
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
     *  \param _func functional logic of the Reduce (a function or a callable type)
     *  \param _key_extr key extractor (a function or a callable type)
     *  \param _parallelism internal parallelism of the Reduce
     *  \param _name name of the Reduce
     *  \param _outputBatchSize size (in num of tuples) of the batches produced by this operator (0 for no batching)
     *  \param _closing_func closing functional logic of the Reduce (a function or callable type)
     *  \param _initial_state initial value of the state to be used for each key
     */ 
    Reduce(reduce_func_t _func,
                key_extractor_func_t _key_extr,
                size_t _parallelism,
                std::string _name,
                size_t _outputBatchSize,
                std::function<void(RuntimeContext &)> _closing_func,
                state_t _initial_state):
                func(_func),
                key_extr(_key_extr),
                parallelism(_parallelism),
                name(_name),
                input_batching(false),
                outputBatchSize(_outputBatchSize)
    {
        if (parallelism == 0) { // check the validity of the parallelism value
            std::cerr << RED << "WindFlow Error: Reduce has parallelism zero" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        for (size_t i=0; i<parallelism; i++) { // create the internal replicas of the Reduce
            replicas.push_back(new Reduce_Replica<reduce_func_t, key_extractor_func_t>(_func,
                                                                                       _key_extr,
                                                                                       name,
                                                                                       RuntimeContext(parallelism, i),
                                                                                       _closing_func,
                                                                                       _initial_state));
        }
    }

    /// Copy Constructor
    Reduce(const Reduce &_other):
                func(_other.func),
                key_extr(_other.key_extr),
                parallelism(_other.parallelism),
                name(_other.name),
                input_batching(_other.input_batching),
                outputBatchSize(_other.outputBatchSize)
    {
        for (size_t i=0; i<parallelism; i++) { // deep copy of the pointers to the Reduce replicas
            replicas.push_back(new Reduce_Replica<reduce_func_t, key_extractor_func_t>(*(_other.replicas[i])));
        }
    }

    // Destructor
    ~Reduce() override
    {
        for (auto *r: replicas) { // delete all the replicas
            delete r;
        }
    }

    /// Copy Assignment Operator
    Reduce& operator=(const Reduce &_other)
    {
        if (this != &_other) {
            func = _other.func;
            key_extr = _other.key_extr;
            parallelism = _other.parallelism;
            name = _other.name;
            input_batching = _other.input_batching;
            outputBatchSize = _other.outputBatchSize;
            for (auto *r: replicas) { // delete all the replicas
                delete r;
            }
            replicas.clear();
            for (size_t i=0; i<parallelism; i++) { // deep copy of the pointers to the Reduce replicas
                replicas.push_back(new Reduce_Replica<reduce_func_t, key_extractor_func_t>(*(_other.replicas[i])));
            }
        }
        return *this;
    }

    /// Move Assignment Operator
    Reduce& operator=(Reduce &&_other)
    {
        func = std::move(_other.func);
        key_extr = std::move(_other.key_extr);
        parallelism = _other.parallelism;
        name = std::move(_other.name);
        input_batching = _other.input_batching;
        outputBatchSize = _other.outputBatchSize;
        for (auto *r: replicas) { // delete all the replicas
            delete r;
        }
        replicas = std::move(_other.replicas);
        return *this;
    }

    /** 
     *  \brief Get the type of the Reduce as a string
     *  \return type of the Reduce
     */ 
    std::string getType() const override
    {
        return std::string("Reduce");
    }

    /** 
     *  \brief Get the name of the Reduce as a string
     *  \return name of the Reduce
     */ 
    std::string getName() const override
    {
        return name;
    }

    /** 
     *  \brief Get the total parallelism of the Reduce
     *  \return total parallelism of the Reduce
     */  
    size_t getParallelism() const override
    {
        return parallelism;
    }

    /** 
     *  \brief Return the input routing mode of the Reduce
     *  \return routing mode used to send inputs to the Reduce
     */ 
    Routing_Mode_t getInputRoutingMode() const override
    {
        return Routing_Mode_t::KEYBY;
    }

    /** 
     *  \brief Return the size of the output batches that the Reduce should produce
     *  \return output batch size in number of tuples
     */ 
    size_t getOutputBatchSize() const override
    {
        return outputBatchSize;
    }
};

} // namespace wf

#endif
