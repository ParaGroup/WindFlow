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
 *  @file    map.hpp
 *  @author  Gabriele Mencagli
 *  
 *  @brief Map operator
 *  
 *  @section Map (Description)
 *  
 *  This file implements the Map operator able to execute streaming transformations
 *  producing one output per input.
 */ 

#ifndef MAP_H
#define MAP_H

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

// class Map_Replica
template<typename map_func_t>
class Map_Replica: public Basic_Replica
{
private:
    map_func_t func; // functional logic used by the Map replica
    using tuple_t = decltype(get_tuple_t_Map(func)); // extracting the tuple_t type and checking the admissible signatures
    using result_t = decltype(get_result_t_Map(func)); // extracting the result_t type and checking the admissible signatures
    using return_t = decltype(get_return_t_Map(func)); // extracting the return type and checking the admissible signatures
    // static predicates to check the type of the functional logic to be invoked
    static constexpr bool isInPlaceNonRiched = std::is_invocable<decltype(func), tuple_t &>::value && std::is_same<return_t, void>::value;
    static constexpr bool isInPlaceRiched = std::is_invocable<decltype(func), tuple_t &, RuntimeContext &>::value && std::is_same<return_t, void>::value;
    static constexpr bool isNonInPlaceNonRiched = std::is_invocable<decltype(func), const tuple_t &>::value && !std::is_same<return_t, void>::value;
    static constexpr bool isNonInPlaceRiched = std::is_invocable<decltype(func), const tuple_t &, RuntimeContext &>::value && !std::is_same<return_t, void>::value;
    // check the presence of a valid functional logic
    static_assert(isInPlaceNonRiched || isInPlaceRiched || isNonInPlaceNonRiched || isNonInPlaceRiched,
        "WindFlow Compilation Error - Map_Replica does not have a valid functional logic:\n");
    bool copyOnWrite; // flag stating if a copy of each input must be done in case of in-place semantics

public:
    // Constructor
    Map_Replica(map_func_t _func,
                std::string _opName,
                RuntimeContext _context,
                std::function<void(RuntimeContext &)> _closing_func,
                bool _copyOnWrite):
                Basic_Replica(_opName, _context, _closing_func, false),
                func(_func),
                copyOnWrite(_copyOnWrite) {}

    // Copy Constructor
    Map_Replica(const Map_Replica &_other):
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
                (this->emitter)->propagate_punctuation(batch_input->getWatermark(this->context.getReplicaIndex()), this); // propagate the received punctuation
                deleteBatch_t(batch_input); // delete the punctuation
                return this->GO_ON;
            }
#if defined (WF_TRACING_ENABLED)
            (this->stats_record).inputs_received += batch_input->getSize();
            (this->stats_record).bytes_received += batch_input->getSize() * sizeof(tuple_t);
            (this->stats_record).outputs_sent += batch_input->getSize();
            (this->stats_record).bytes_sent += batch_input->getSize()* sizeof(result_t);
#endif
            for (size_t i=0; i<batch_input->getSize(); i++) { // process all the inputs within the received batch
                process_input(batch_input->getTupleAtPos(i), batch_input->getTimestampAtPos(i), batch_input->getWatermark((this->context).getReplicaIndex()));
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
            (this->stats_record).outputs_sent++;
            (this->stats_record).bytes_sent += sizeof(result_t);
#endif
            process_input(input);
        }
        this->endStatsRecording();
        return this->GO_ON;
    }

    // Process a single input (version 1)
    void process_input(Single_t<tuple_t> *_input)
    {
        if constexpr (isInPlaceNonRiched) { // inplace non-riched version
            if (copyOnWrite) {
                tuple_t t = _input->tuple;
                func(t);
                (this->emitter)->emit(&t, 0, _input->getTimestamp(), _input->getWatermark((this->context).getReplicaIndex()), this);
                deleteSingle_t(_input); // delete the input Single_t
            }
            else {
                func(_input->tuple);
                (this->emitter)->emit_inplace(_input, this);
            }
        }
        if constexpr (isInPlaceRiched)  { // inplace riched version
            (this->context).setContextParameters(_input->getTimestamp(), _input->getWatermark((this->context).getReplicaIndex())); // set the parameter of the RuntimeContext
            if (copyOnWrite) {
                tuple_t t = _input->tuple;
                func(t, this->context);
                (this->emitter)->emit(&t, 0, _input->getTimestamp(), _input->getWatermark((this->context).getReplicaIndex()), this);
                deleteSingle_t(_input); // delete the input Single_t
            }
            else {
                func(_input->tuple, this->context);
                (this->emitter)->emit_inplace(_input, this);
            }
        }
        if constexpr (isNonInPlaceNonRiched) { // non-inplace non-riched version
            result_t res = func(_input->tuple);
            (this->emitter)->emit(&res, 0, _input->getTimestamp(), _input->getWatermark((this->context).getReplicaIndex()), this);
            deleteSingle_t(_input); // delete the input Single_t
        }
        if constexpr (isNonInPlaceRiched) { // non-inplace riched version
            (this->context).setContextParameters(_input->getTimestamp(), _input->getWatermark((this->context).getReplicaIndex())); // set the parameter of the RuntimeContext
            result_t res = func(_input->tuple, this->context);
            (this->emitter)->emit(&res, 0, _input->getTimestamp(), _input->getWatermark((this->context).getReplicaIndex()), this);
            deleteSingle_t(_input); // delete the input Single_t
        }
    }

    // Process a single input (version 2)
    void process_input(tuple_t &_tuple,
                       uint64_t _timestamp,
                       uint64_t _watermark)
    {
        if constexpr (isInPlaceNonRiched) { // inplace non-riched version
            if (copyOnWrite) {
                tuple_t t = _tuple;
                func(t);
                (this->emitter)->emit(&t, 0, _timestamp, _watermark, this);
            }
            else {
                func(_tuple);
                (this->emitter)->emit(&_tuple, 0, _timestamp, _watermark, this);
            }
        }
        if constexpr (isInPlaceRiched)  { // inplace riched version
            (this->context).setContextParameters(_timestamp, _watermark); // set the parameter of the RuntimeContext
            if (copyOnWrite) {
                tuple_t t = _tuple;
                func(t, this->context);
                (this->emitter)->emit(&t, 0, _timestamp, _watermark, this);
            }
            else {
                func(_tuple, this->context);
                (this->emitter)->emit(&_tuple, 0, _timestamp, _watermark, this);
            }
        }
        if constexpr (isNonInPlaceNonRiched) { // non-inplace non-riched version
            result_t res = func(_tuple);
            (this->emitter)->emit(&res, 0, _timestamp, _watermark, this);
        }
        if constexpr (isNonInPlaceRiched) { // non-inplace riched version
            (this->context).setContextParameters(_timestamp, _watermark); // set the parameter of the RuntimeContext
            result_t res = func(_tuple, this->context);
            (this->emitter)->emit(&res, 0, _timestamp, _watermark, this);
        }
    }

    Map_Replica(Map_Replica &&) = delete; ///< Move constructor is deleted
    Map_Replica &operator=(const Map_Replica &) = delete; ///< Copy assignment operator is deleted
    Map_Replica &operator=(Map_Replica &&) = delete; ///< Move assignment operator is deleted
};

//@endcond

/** 
 *  \class Map
 *  
 *  \brief Map operator
 *  
 *  This class implements the Map operator executing streaming transformations producing
 *  one output per input.
 */ 
template<typename map_func_t, typename keyextr_func_t>
class Map: public Basic_Operator
{
private:
    friend class MultiPipe;
    friend class PipeGraph;
    map_func_t func; // functional logic used by the Map
    keyextr_func_t key_extr; // logic to extract the key attribute from the tuple_t
    std::vector<Map_Replica<map_func_t>*> replicas; // vector of pointers to the replicas of the Map

    // Configure the Map to receive batches instead of individual inputs
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

    // Check whether the Map has terminated
    bool isTerminated() const override
    {
        bool terminated = true;
        for(auto *r: replicas) { // scan all the replicas to check their termination
            terminated = terminated && r->isTerminated();
        }
        return terminated;
    }

    // Set the execution mode of the Map
    void setExecutionMode(Execution_Mode_t _execution_mode)
    {
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
    // Append the statistics (JSON format) of the Map to a PrettyWriter
    void appendStats(rapidjson::PrettyWriter<rapidjson::StringBuffer> &writer) const override
    {
        writer.StartObject(); // create the header of the JSON file
        writer.Key("Operator_name");
        writer.String((this->name).c_str());
        writer.Key("Operator_type");
        writer.String("Map");
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
        for (auto *r: replicas) { // append the statistics from all the replicas of the Map
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
     *  \param _func functional logic of the Map (a function or any callable type)
     *  \param _key_extr key extractor (a function or any callable type)
     *  \param _parallelism internal parallelism of the Map
     *  \param _name name of the Map
     *  \param _input_routing_mode input routing mode of the Map
     *  \param _outputBatchSize size (in num of tuples) of the batches produced by this operator (0 for no batching)
     *  \param _closing_func closing functional logic of the Map (a function or any callable type)
     */ 
    Map(map_func_t _func,
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
        for (size_t i=0; i<this->parallelism; i++) { // create the internal replicas of the Map
            replicas.push_back(new Map_Replica<map_func_t>(_func, this->name, RuntimeContext(this->parallelism, i), _closing_func, copyOnWrite));
        }
    }

    /// Copy constructor
    Map(const Map &_other):
        Basic_Operator(_other),
        func(_other.func),
        key_extr(_other.key_extr)
    {
        for (size_t i=0; i<this->parallelism; i++) { // deep copy of the pointers to the Map replicas
            replicas.push_back(new Map_Replica<map_func_t>(*(_other.replicas[i])));
        }
    }

    // Destructor
    ~Map() override
    {
        for (auto *r: replicas) { // delete all the replicas
            delete r;
        }
    }

    /** 
     *  \brief Get the type of the Map as a string
     *  \return type of the Map
     */ 
    std::string getType() const override
    {
        return std::string("Map");
    }

    Map(Map &&) = delete; ///< Move constructor is deleted
    Map &operator=(const Map &) = delete; ///< Copy assignment operator is deleted
    Map &operator=(Map &&) = delete; ///< Move assignment operator is deleted
};

} // namespace wf

#endif
