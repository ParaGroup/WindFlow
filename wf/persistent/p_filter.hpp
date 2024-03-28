/**************************************************************************************
 *  Copyright (c) 2019- Gabriele Mencagli and Simone Frassinelli
 *  
 *  This file is part of WindFlow.
 *  
 *  WindFlow is free software dual licensed under the GNU LGPL or MIT License.
 *  You can redistribute it and/or modify it under the terms of the
 *    * GNU Lesser General Public License as published by
 *      the Free Software Foundation, either version 3 of the License, or
 *      (at your option) any later version
 *    OR
 *    * MIT License: https://github.com/ParaGroup/WindFlow/blob/master/LICENSE.MIT
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
 *  @file    p_filter.hpp
 *  @author  Gabriele Mencagli and Simone Frassinelli
 *  
 *  @brief P_Filter operator
 *  
 *  @section P_Filter (Description)
 *  
 *  This file implements the P_Filter operator able to execute streaming transformations
 *  producing zero or one output per input. This operator keeps the state on RocksDB.
 */ 

#ifndef P_FILTER_H
#define P_FILTER_H

/// includes
#include<string>
#include<functional>
#include<context.hpp>
#include<batch_t.hpp>
#include<single_t.hpp>
#if defined(WF_TRACING_ENABLED)
    #include<stats_record.hpp>
#endif
#include<basic_emitter.hpp>
#include<basic_operator.hpp>
#include<persistent/db_handle.hpp>

namespace wf {

//@cond DOXY_IGNORE

// class P_Filter_Replica
template<typename p_filter_func_t, typename keyextr_func_t>
class P_Filter_Replica: public Basic_Replica
{
private:
    p_filter_func_t func; // functional logic used by the P_Filter replica
    keyextr_func_t key_extr; // logic to extract the key attribute from the tuple_t
    using tuple_t = decltype(get_tuple_t_P_Filter(func)); // extracting the tuple_t type and checking the admissible signatures
    using state_t = decltype(get_state_t_P_Filter(func)); // extracting the state_t type and checking the admissible signatures
    using key_t = decltype(get_key_t_KeyExtr(key_extr)); // extracting the key_t type and checking the admissible singatures
    // static predicates to check the type of the functional logic to be invoked
    static constexpr bool isNonRiched = std::is_invocable<decltype(func), tuple_t &, state_t &>::value;
    static constexpr bool isRiched = std::is_invocable<decltype(func), tuple_t &, state_t &, RuntimeContext &>::value;
    // check the presence of a valid functional logic
    static_assert(isNonRiched || isRiched,
                  "WindFlow Compilation Error - P_Filter_Replica does not have a valid functional logic:\n");
    template<typename T1, typename T2> friend class P_Filter;
    bool copyOnWrite; // flag stating if a copy of each input must be done in case of in-place semantics
    DBHandle<state_t> *mydb; // pointer to the DBHandle object used to interact with RocksDB

public:
    // Constructor
    P_Filter_Replica(p_filter_func_t _func,
                     keyextr_func_t _key_extr,
                     std::string _opName,
                     std::string _dbpath,
                     RuntimeContext _context,
                     std::function<void(RuntimeContext &)> _closing_func,
                     std::function<std::string(state_t &)> _serialize,
                     std::function<state_t(std::string &)> _deserialize,
                     bool _deleteDb,
                     state_t _initial_state,
                     bool _copyOnWrite,
                     bool _sharedDb,
                     size_t _whoami):
                     Basic_Replica(_opName, _context, _closing_func, false),
                     func(_func),
                     key_extr(_key_extr),
                     copyOnWrite(_copyOnWrite)
    {
        _dbpath = _sharedDb ? _dbpath + "_shared" : _dbpath;
        mydb = new DBHandle<state_t>(_serialize,
                                     _deserialize,
                                     _deleteDb,
                                     _dbpath,
                                     _initial_state,
                                     _whoami);
    }

    // Copy Constructor
    P_Filter_Replica(const P_Filter_Replica &_other):
                     Basic_Replica(_other),
                     func(_other.func),
                     key_extr(_other.key_extr),
                     copyOnWrite(_other.copyOnWrite),
                     mydb((_other.mydb)->getCopy()) {}

    // Destructor
    ~P_Filter_Replica()
    {
        delete mydb;
    }

    // svc_init (utilized by the FastFlow runtime)
    int svc_init()
    {
        if (mydb->get_internal_db() == nullptr) {
            std::cerr << RED << "WindFlow Error: a P_Filter_Replica is using a null db (RocksDB)" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        return Basic_Replica::svc_init();
    }

    // svc (utilized by the FastFlow runtime)
    void *svc(void *_in) override
    {
        this->startStatsRecording();
        if (this->input_batching) { // receiving a batch
            Batch_t<tuple_t> *batch_input = reinterpret_cast<Batch_t<tuple_t> *>(_in);
            if (batch_input->isPunct()) { // if it is a punctuaton
                (this->emitter)->propagate_punctuation(batch_input->getWatermark((this->context).getReplicaIndex()), this); // propagate the received punctuation
                deleteBatch_t(batch_input); // delete the punctuation
                return this->GO_ON;
            }
#if defined(WF_TRACING_ENABLED)
            (this->stats_record).inputs_received += batch_input->getSize();
            (this->stats_record).bytes_received += batch_input->getSize() * sizeof(tuple_t);
#endif
            for (size_t i = 0; i < batch_input->getSize(); i++) { // process all the inputs within the received batch
                if (!copyOnWrite) {
                    process_input(batch_input->getTupleAtPos(i), batch_input->getTimestampAtPos(i), batch_input->getWatermark((this->context).getReplicaIndex()));
                }
                else {
                    tuple_t t = batch_input->getTupleAtPos(i);
                    process_input(t, batch_input->getTimestampAtPos(i), batch_input->getWatermark((this->context).getReplicaIndex()));
                }
            }
            deleteBatch_t(batch_input); // delete the input batch
        }
        else { // receiving a single input
            Single_t<tuple_t> *input = reinterpret_cast<Single_t<tuple_t> *>(_in);
            if (input->isPunct()) { // if it is a punctuaton
                (this->emitter)->propagate_punctuation(input->getWatermark((this->context).getReplicaIndex()), this); // propagate the received punctuation
                deleteSingle_t(input); // delete the punctuation
                return this->GO_ON;
            }
#if defined(WF_TRACING_ENABLED)
            (this->stats_record).inputs_received++;
            (this->stats_record).bytes_received += sizeof(tuple_t);
#endif
            if (!copyOnWrite) {
                process_input(input);
            }
            else {
                tuple_t t = input->tuple;
                process_input(t, input->getTimestamp(), input->getWatermark((this->context).getReplicaIndex()));
                deleteSingle_t(input); // delete the input Single_t
            }
        }
        this->endStatsRecording();
        return this->GO_ON;
    }

    // Process a single input (version "in-place")
    void process_input(Single_t<tuple_t> *_input) {
        auto key = key_extr(_input->tuple);
        state_t val = mydb->get(key);
        if constexpr (isNonRiched) { // non-riched version
            if (func(_input->tuple, val)) {
#if defined(WF_TRACING_ENABLED)
                (this->stats_record).outputs_sent++;
                (this->stats_record).bytes_sent += sizeof(tuple_t);
#endif
                (this->emitter)->emit_inplace(_input, this);
                this->dropped_inputs = 0;
            }
            else {
                this->dropped_inputs++;
                if ((this->execution_mode == Execution_Mode_t::DEFAULT) && (this->dropped_inputs % WF_DEFAULT_WM_AMOUNT == 0)) { // check punctuaction generation logic
                    if (current_time_usecs() - this->last_time_punct >= WF_DEFAULT_WM_INTERVAL_USEC) { // check the end of the sample
                        (this->emitter)->propagate_punctuation(_input->getWatermark((this->context).getReplicaIndex()), this);
                        this->last_time_punct = current_time_usecs();
                    }
                }
                deleteSingle_t(_input); // delete the input Single_t
            }
        }
        if constexpr (isRiched) { // riched version
            (this->context).setContextParameters(_input->getTimestamp(), _input->getWatermark((this->context).getReplicaIndex())); // set the parameter of the RuntimeContext
            if (func(_input->tuple, val, this->context)) {
#if defined(WF_TRACING_ENABLED)
                (this->stats_record).outputs_sent++;
                (this->stats_record).bytes_sent += sizeof(tuple_t);
#endif
                (this->emitter)->emit_inplace(_input, this);
                this->dropped_inputs = 0;
            }
            else {
                this->dropped_inputs++;
                if ((this->execution_mode == Execution_Mode_t::DEFAULT) && (this->dropped_inputs % WF_DEFAULT_WM_AMOUNT == 0)) { // check punctuaction generation logic
                    if (current_time_usecs() - this->last_time_punct >= WF_DEFAULT_WM_INTERVAL_USEC) { // check the end of the sample
                        (this->emitter)->propagate_punctuation(_input->getWatermark((this->context).getReplicaIndex()), this);
                        this->last_time_punct = current_time_usecs();
                    }
                }
                deleteSingle_t(_input); // delete the input Single_t
            }
        }
        mydb->put(val);
    }

    // Process a single input (version "non in-place")
    void process_input(tuple_t &_tuple,
                       uint64_t _timestamp,
                       uint64_t _watermark)
    {
        auto key = key_extr(_tuple);
        state_t val = mydb->get(key);
        if constexpr (isNonRiched) { // non-riched version
            if (func(_tuple, val)) {
#if defined(WF_TRACING_ENABLED)
                (this->stats_record).outputs_sent++;
                (this->stats_record).bytes_sent += sizeof(tuple_t);
#endif
                (this->emitter)->emit(&_tuple, 0, _timestamp, _watermark, this);
                this->dropped_inputs = 0;
            }
            else {
                this->dropped_inputs++;
                if ((this->execution_mode == Execution_Mode_t::DEFAULT) && (this->dropped_inputs % WF_DEFAULT_WM_AMOUNT == 0)) { // check punctuaction generation logic
                    if (current_time_usecs() - this->last_time_punct >= WF_DEFAULT_WM_INTERVAL_USEC) { // check the end of the sample
                        (this->emitter)->propagate_punctuation(_watermark, this);
                        this->last_time_punct = current_time_usecs();
                    }
                }
            }
        }
        if constexpr (isRiched) { // riched version
            (this->context).setContextParameters(_timestamp, _watermark); // set the parameter of the RuntimeContext
            if (func(_tuple, val, this->context)) {
#if defined(WF_TRACING_ENABLED)
                (this->stats_record).outputs_sent++;
                (this->stats_record).bytes_sent += sizeof(tuple_t);
#endif
                (this->emitter)->emit(&_tuple, 0, _timestamp, _watermark, this);
                this->dropped_inputs = 0;
            }
            else {
                this->dropped_inputs++;
                if ((this->execution_mode == Execution_Mode_t::DEFAULT) && (this->dropped_inputs % WF_DEFAULT_WM_AMOUNT == 0)) { // check punctuaction generation logic
                    if (current_time_usecs() - this->last_time_punct >= WF_DEFAULT_WM_INTERVAL_USEC) { // check the end of the sample
                        (this->emitter)->propagate_punctuation(_watermark, this);
                        this->last_time_punct = current_time_usecs();
                    }
                }
            }
        }
        mydb->put(val);
    }

    P_Filter_Replica(P_Filter_Replica &&) = delete; ///< Move constructor is deleted
    P_Filter_Replica &operator=(const P_Filter_Replica &) = delete; ///< Copy assignment operator is deleted
    P_Filter_Replica &operator=(P_Filter_Replica &&) = delete; ///< Move assignment operator is deleted
};

//@endcond

/**
 *  \class P_Filter
 *  
 *  \brief P_Filter operator
 *  
 *  This class implements the P_Filter operator executing a streaming transformation producing
 *  zero or one output per input. The operator keeps its state on RocksDB.
 */ 
template<typename p_filter_func_t, typename keyextr_func_t>
class P_Filter: public Basic_Operator
{
private:
    friend class MultiPipe;
    friend class PipeGraph;
    p_filter_func_t func; // functional logic used by the P_Filter
    keyextr_func_t key_extr;
    using tuple_t = decltype(get_tuple_t_P_Filter(func)); // extracting the tuple_t type and checking the admissible signatures
    using result_t = tuple_t;
    using state_t = decltype(get_state_t_P_Filter(func));
    std::vector<P_Filter_Replica<p_filter_func_t, keyextr_func_t> *> replicas; // vector of pointers to the replicas of the P_Filter
    bool sharedDb; // sharedBD flag
    static constexpr op_type_t op_type = op_type_t::BASIC;

    // Configure the P_Filter to receive batches instead of individual inputs
    void receiveBatches(bool _input_batching) override
    {
        for (auto *r: replicas) {
            r->receiveBatches(_input_batching);
        }
    }

    // Set the emitter used to route outputs from the P_Filter
    void setEmitter(Basic_Emitter *_emitter) override
    {
        replicas[0]->setEmitter(_emitter);
        for (size_t i = 1; i < replicas.size(); i++) {
            replicas[i]->setEmitter(_emitter->clone());
        }
    }

    // Check whether the P_Filter has terminated
    bool isTerminated() const override
    {
        bool terminated = true;
        for (auto *r: replicas) { // scan all the replicas to check their termination
            terminated = terminated && r->isTerminated();
        }
        return terminated;
    }

    // Set the execution mode of the P_Filter
    void setExecutionMode(Execution_Mode_t _execution_mode)
    {
        if (this->getOutputBatchSize() > 0 && _execution_mode != Execution_Mode_t::DEFAULT) {
            std::cerr << RED << "WindFlow Error: P_Filter is trying to produce a batch in non DEFAULT mode" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        for (auto *r: replicas) {
            r->setExecutionMode(_execution_mode);
        }
    }

    // Get the logic to extract the key attribute from the tuple_t
    keyextr_func_t getKeyExtractor() const
    {
        return key_extr;
    }

#if defined(WF_TRACING_ENABLED)
    // Append the statistics (JSON format) of the Filter to a PrettyWriter
    void appendStats(rapidjson::PrettyWriter<rapidjson::StringBuffer> &writer) const override
    {
        writer.StartObject(); // create the header of the JSON file
        writer.Key("Operator_name");
        writer.String((this->name).c_str());
        writer.Key("Operator_type");
        writer.String("P_Filter");
        writer.Key("Distribution");
        if (this->getInputRoutingMode() == Routing_Mode_t::KEYBY) {
            writer.String("KEYBY");
        }
        else if (this->getInputRoutingMode() == Routing_Mode_t::REBALANCING) {
            writer.String("REBALANCING");
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
        writer.Uint(this->parallelism);
        writer.Key("OutputBatchSize");
        writer.Uint(this->outputBatchSize);
        writer.Key("Replicas");
        writer.StartArray();
        for (auto *r: replicas) { // append the statistics from all the replicas of the P_Filter
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
     *  \param _func functional logic of the P_Filter (a function or any callable type)
     *  \param _key_extr key extractor (a function or any callable type)
     *  \param _parallelism internal parallelism of the P_Filter
     *  \param _name name of the P_Filter
     *  \param _dbpath pathname of SST files used by RocksDB
     *  \param _input_routing_mode input routing mode of the P_Filter
     *  \param _outputBatchSize size (in num of tuples) of the batches produced by this operator (0 for no batching)
     *  \param _closing_func closing functional logic of the P_Filter (a function or any callable type)
     *  \param _serialize serialize function of the RocksDB state (a function or any callable type)
     *  \param _deserialize deserialize function of the RocksDB state (a function or any callable type)
     *  \param _deleteDb Boolean flag stating whether SST files in _dbpath must be deleted before starting
     *  \param _initial_state initial value of the state for each key
     *  \param _sharedDb Boolean flag stating whether replicas of this operator should use the same RockDB states
     *  \param _options options to be provided to RocksDB
     *  \param _read_options read options to be provided to RocksDB
     *  \param _write_options read options to be provided to RocksDB
     */
    P_Filter(p_filter_func_t _func,
             keyextr_func_t _key_extr,
             size_t _parallelism,
             std::string _name,
             std::string _dbpath,
             Routing_Mode_t _input_routing_mode,
             size_t _outputBatchSize,
             std::function<void(RuntimeContext &)> _closing_func,
             std::function<std::string(state_t &)> _serialize,
             std::function<state_t(std::string &)> _deserialize,
             bool _deleteDb,
             state_t _initial_state,
             bool _sharedDb,
             rocksdb::Options _options,
             rocksdb::ReadOptions _read_options,
             rocksdb::WriteOptions _write_options):
             Basic_Operator(_parallelism, _name, _input_routing_mode, _outputBatchSize),
             func(_func),
             key_extr(_key_extr),
             sharedDb(_sharedDb)
    {
        bool copyOnWrite = (this->input_routing_mode == Routing_Mode_t::BROADCAST);
        for (size_t i = 0; i < this->parallelism; i++) { // create the internal replicas of the P_Filter
            std::string shared_dbpath = _dbpath + this->name;
            std::string normal_dbpath = shared_dbpath + "_" + std::to_string(i);
            replicas.push_back(new P_Filter_Replica<p_filter_func_t, keyextr_func_t>(_func,
                                                                                     _key_extr,
                                                                                     this->name,
                                                                                     sharedDb ? shared_dbpath : normal_dbpath,
                                                                                     RuntimeContext(this->parallelism, i),
                                                                                     _closing_func,
                                                                                     _serialize,
                                                                                     _deserialize,
                                                                                     _deleteDb,
                                                                                     _initial_state,
                                                                                     copyOnWrite,
                                                                                     sharedDb,
                                                                                     i));
        }
        assert(this->parallelism > 0);
        // initialize the internal DB of the replicas
        (replicas[0]->mydb)->initDB(nullptr, _options, _read_options, _write_options);
        for (size_t i = 1; i < this->parallelism; i++) {
            if (sharedDb) {
                (replicas[i]->mydb)->initDB((replicas[0]->mydb)->get_internal_db(), _options, _read_options, _write_options);
            }
            else {
                (replicas[i]->mydb)->initDB(nullptr, _options, _read_options, _write_options);
            }
        }
    }

    /// Copy constructor
    P_Filter(const P_Filter &_other):
             Basic_Operator(_other),
             func(_other.func),
             key_extr(_other.key_extr),
             sharedDb(_other.sharedDb)
    {
        for (size_t i=0; i<this->parallelism; i++) { // deep copy of the pointers to the Filter replicas
            replicas.push_back(new P_Filter_Replica<p_filter_func_t, keyextr_func_t>(*(_other.replicas[i])));
        }
    }

    // Destructor
    ~P_Filter() override
    {
        for (auto *r: replicas) { // delete all the replicas
            delete r;
        }
    }

    /** 
     *  \brief Get the type of the P_Filter as a string
     *  \return type of the Filter
     */ 
    std::string getType() const override {
        return std::string("P_Filter");
    }

    P_Filter(P_Filter &&) = delete; ///< Move constructor is deleted
    P_Filter &operator=(const P_Filter &) = delete; ///< Copy assignment operator is deleted
    P_Filter &operator=(P_Filter &&) = delete; ///< Move assignment operator is deleted
};

} // namespace wf

#endif
