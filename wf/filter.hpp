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
#if defined (WF_TRACING_ENABLED)
    #include<stats_record.hpp>
#endif
#include<basic_emitter.hpp>
#include<basic_operator.hpp>

namespace wf {

//@cond DOXY_IGNORE

// class Filter_Replica
template<typename filter_func_t>
class Filter_Replica: public Basic_Replica
{
private:
    filter_func_t func; // functional logic used by the Filter replica
    using tuple_t = decltype(get_tuple_t_Filter(func)); // extracting the tuple_t type and checking the admissible signatures
    // static predicates to check the type of the functional logic to be invoked
    static constexpr bool isNonRiched = std::is_invocable<decltype(func), tuple_t &>::value;
    static constexpr bool isRiched = std::is_invocable<decltype(func), tuple_t &, RuntimeContext &>::value;
    // check the presence of a valid functional logic
    static_assert(isNonRiched || isRiched,
        "WindFlow Compilation Error - Filter_Replica does not have a valid functional logic:\n");
    bool copyOnWrite; // flag stating if a copy of each input must be done in case of in-place semantics

public:
    // Constructor
    Filter_Replica(filter_func_t _func,
                   std::string _opName,
                   RuntimeContext _context,
                   std::function<void(RuntimeContext &)> _closing_func,
                   bool _copyOnWrite):
                   Basic_Replica(_opName, _context, _closing_func, false),
                   func(_func),
                   copyOnWrite(_copyOnWrite) {}

    // Copy Constructor
    Filter_Replica(const Filter_Replica &_other):
                   Basic_Replica(_other),
                   func(_other.func),
                   copyOnWrite(_other.copyOnWrite) {}

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
#if defined (WF_TRACING_ENABLED)
            (this->stats_record).inputs_received += batch_input->getSize();
            (this->stats_record).bytes_received += batch_input->getSize() * sizeof(tuple_t);
#endif
            for (size_t i=0; i<batch_input->getSize(); i++) { // process all the inputs within the received batch
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
#if defined (WF_TRACING_ENABLED)
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
    void process_input(Single_t<tuple_t> *_input)
    {
        if constexpr (isNonRiched) { // non-riched version
            if (func(_input->tuple)) {
#if defined (WF_TRACING_ENABLED)
                (this->stats_record).outputs_sent++;
                (this->stats_record).bytes_sent += sizeof(tuple_t);
#endif
                this->doEmit_inplace(this->emitter, _input, this);
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
        if constexpr (isRiched)  { // riched version
            (this->context).setContextParameters(_input->getTimestamp(), _input->getWatermark((this->context).getReplicaIndex())); // set the parameter of the RuntimeContext
            if (func(_input->tuple, this->context)) {
#if defined (WF_TRACING_ENABLED)
                (this->stats_record).outputs_sent++;
                (this->stats_record).bytes_sent += sizeof(tuple_t);
#endif
                this->doEmit_inplace(this->emitter, _input, this);
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
    }

    // Process a single input (version "non in-place")
    void process_input(tuple_t &_tuple,
                       uint64_t _timestamp,
                       uint64_t _watermark)
    {
        if constexpr (isNonRiched) { // non-riched version
            if (func(_tuple)) {
#if defined (WF_TRACING_ENABLED)
                (this->stats_record).outputs_sent++;
                (this->stats_record).bytes_sent += sizeof(tuple_t);
#endif
                this->doEmit(this->emitter, &_tuple, 0, _timestamp, _watermark, this);
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
        if constexpr (isRiched)  { // riched version
            (this->context).setContextParameters(_timestamp, _watermark); // set the parameter of the RuntimeContext
            if (func(_tuple, this->context)) {
#if defined (WF_TRACING_ENABLED)
                (this->stats_record).outputs_sent++;
                (this->stats_record).bytes_sent += sizeof(tuple_t);
#endif
                this->doEmit(this->emitter, &_tuple, 0, _timestamp, _watermark, this);
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
    }

    Filter_Replica(Filter_Replica &&) = delete; ///< Move constructor is deleted
    Filter_Replica &operator=(const Filter_Replica &) = delete; ///< Copy assignment operator is deleted
    Filter_Replica &operator=(Filter_Replica &&) = delete; ///< Move assignment operator is deleted
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
template<typename filter_func_t, typename keyextr_func_t>
class Filter: public Basic_Operator
{
private:
    friend class MultiPipe;
    friend class PipeGraph;
    filter_func_t func; // functional logic used by the Filter
    keyextr_func_t key_extr; // logic to extract the key attribute from the tuple_t
    using tuple_t = decltype(get_tuple_t_Filter(func)); // extracting the tuple_t type and checking the admissible signatures
    using result_t = tuple_t;
    std::vector<Filter_Replica<filter_func_t>*> replicas; // vector of pointers to the replicas of the Filter
    static constexpr op_type_t op_type = op_type_t::BASIC;

    // Configure the Filter to receive batches instead of individual inputs
    void receiveBatches(bool _input_batching) override
    {
        for (auto *r: replicas) {
            r->receiveBatches(_input_batching);
        }
    }

    // Set the emitter used to route outputs from the Filter
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
        if (this->getOutputBatchSize() > 0 && _execution_mode != Execution_Mode_t::DEFAULT) {
            std::cerr << RED << "WindFlow Error: Filter is trying to produce a batch in non DEFAULT mode" << DEFAULT_COLOR << std::endl;
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

#if defined (WF_TRACING_ENABLED)
    // Append the statistics (JSON format) of the Filter to a PrettyWriter
    void appendStats(rapidjson::PrettyWriter<rapidjson::StringBuffer> &writer) const override
    {
        writer.StartObject(); // create the header of the JSON file
        writer.Key("Operator_name");
        writer.String((this->name).c_str());
        writer.Key("Operator_type");
        writer.String("Filter");
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
     *  \param _func functional logic of the Filter (a function or any callable type)
     *  \param _key_extr key extractor (a function or any callable type)
     *  \param _parallelism internal parallelism of the Filter
     *  \param _name name of the Filter
     *  \param _input_routing_mode input routing mode of the Filter
     *  \param _outputBatchSize size (in num of tuples) of the batches produced by this operator (0 for no batching)
     *  \param _closing_func closing functional logic of the Filter (a function or any callable type)
     */ 
    Filter(filter_func_t _func,
           keyextr_func_t _key_extr,
           size_t _parallelism,
           std::string _name,
           Routing_Mode_t _input_routing_mode,
           size_t _outputBatchSize,
           std::function<void(RuntimeContext &)> _closing_func):
           Basic_Operator(_parallelism, _name, _input_routing_mode, _outputBatchSize),
           func(_func),
           key_extr(_key_extr)
    {
        bool copyOnWrite = (this->input_routing_mode == Routing_Mode_t::BROADCAST);
        for (size_t i=0; i<this->parallelism; i++) { // create the internal replicas of the Filter
            replicas.push_back(new Filter_Replica<filter_func_t>(_func, this->name, RuntimeContext(this->parallelism, i), _closing_func, copyOnWrite));
        }
    }

    /// Copy constructor
    Filter(const Filter &_other):
           Basic_Operator(_other),
           func(_other.func),
           key_extr(_other.key_extr)
    {
        for (size_t i=0; i<this->parallelism; i++) { // deep copy of the pointers to the Filter replicas
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

    /** 
     *  \brief Get the type of the Filter as a string
     *  \return type of the Filter
     */ 
    std::string getType() const override
    {
        return std::string("Filter");
    }

    Filter(Filter &&) = delete; ///< Move constructor is deleted
    Filter &operator=(const Filter &) = delete; ///< Copy assignment operator is deleted
    Filter &operator=(Filter &&) = delete; ///< Move assignment operator is deleted
};

} // namespace wf

#endif
