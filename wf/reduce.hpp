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
template<typename reduce_func_t, typename keyextr_func_t>
class Reduce_Replica: public Basic_Replica
{
private:
    reduce_func_t func; // functional logic used by the Reduce replica
    keyextr_func_t key_extr; // logic to extract the key attribute from the tuple_t
    using tuple_t = decltype(get_tuple_t_Reduce(func)); // extracting the tuple_t type and checking the admissible signatures
    using state_t = decltype(get_state_t_Reduce(func)); // extracting the state_t type and checking the admissible signatures
    using key_t = decltype(get_key_t_KeyExtr(key_extr)); // extracting the key_t type and checking the admissible singatures
    // static predicates to check the type of the functional logic to be invoked
    static constexpr bool isNonRiched = std::is_invocable<decltype(func), const tuple_t &, state_t &>::value;
    static constexpr bool isRiched = std::is_invocable<decltype(func), const tuple_t &, state_t &, RuntimeContext &>::value;
    // check the presence of a valid functional logic
    static_assert(isNonRiched || isRiched,
        "WindFlow Compilation Error - Reduce_Replica does not have a valid functional logic:\n");
    std::unordered_map<key_t, state_t> keyMap; // hashtable mapping key values onto the corresponding state objects
    state_t initial_state; // initial state to be copied for each new key

public:
    // Constructor
    Reduce_Replica(reduce_func_t _func,
                   keyextr_func_t _key_extr,
                   std::string _opName,
                   RuntimeContext _context,
                   std::function<void(RuntimeContext &)> _closing_func,
                   state_t _initial_state):
                   Basic_Replica(_opName, _context, _closing_func, false),
                   func(_func),
                   key_extr(_key_extr),
                   initial_state(_initial_state) {}

    // Copy Constructor
    Reduce_Replica(const Reduce_Replica &_other):
                   Basic_Replica(_other),
                   func(_other.func),
                   key_extr(_other.key_extr),
                   keyMap(_other.keyMap),
                   initial_state(_other.initial_state) {}

    // svc (utilized by the FastFlow runtime)
    void *svc(void *_in) override
    {
        this->startStatsRecording();
        if (this->input_batching) { // receiving a batch
            Batch_t<tuple_t> *batch_input = reinterpret_cast<Batch_t<tuple_t> *>(_in);
            if (batch_input->isPunct()) { // if it is a punctuaton
                (this->emitter)->propagate_punctuation(batch_input->getWatermark((this->context).getReplicaIndex()), this); // propagate the received punctuation
                deleteBatch_t(batch_input); // delete the punctuaton
                return this->GO_ON;
            }
#if defined (WF_TRACING_ENABLED)
            (this->stats_record).inputs_received += batch_input->getSize();
            (this->stats_record).bytes_received += batch_input->getSize() * sizeof(tuple_t);
            (this->stats_record).outputs_sent += batch_input->getSize();
            (this->stats_record).bytes_sent += batch_input->getSize()* sizeof(state_t);
#endif
            for (size_t i=0; i<batch_input->getSize(); i++) { // process all the inputs within the batch
                process_input(batch_input->getTupleAtPos(i), batch_input->getTimestampAtPos(i), batch_input->getWatermark((this->context).getReplicaIndex()));
            }
            deleteBatch_t(batch_input); // delete the input batch
        }
        else { // receiving a single input
            Single_t<tuple_t> *input = reinterpret_cast<Single_t<tuple_t> *>(_in);
            if (input->isPunct()) { // if it is a punctuaton
                (this->emitter)->propagate_punctuation(input->getWatermark((this->context).getReplicaIndex()), this); // propagate the received punctuation
                deleteSingle_t(input); // delete the punctuaton
                return this->GO_ON;
            }
#if defined (WF_TRACING_ENABLED)
            (this->stats_record).inputs_received++;
            (this->stats_record).bytes_received += sizeof(tuple_t);
            (this->stats_record).outputs_sent++;
            (this->stats_record).bytes_sent += sizeof(state_t);
#endif
            process_input(input->tuple, input->getTimestamp(), input->getWatermark((this->context).getReplicaIndex()));
            deleteSingle_t(input); // delete the input Single_t
        }
        this->endStatsRecording();
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
            (this->context).setContextParameters(_timestamp, _watermark); // set the parameter of the RuntimeContext
            func(_tuple, (*it).second, this->context);
        }
        state_t result = (*it).second; // the result is a copy of the present state
        (this->emitter)->emit(&result, 0, _timestamp, _watermark, this);
    }

    Reduce_Replica(Reduce_Replica &&) = delete; ///< Move constructor is deleted
    Reduce_Replica &operator=(const Reduce_Replica &) = delete; ///< Copy assignment operator is deleted
    Reduce_Replica &operator=(Reduce_Replica &&) = delete; ///< Move assignment operator is deleted
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
template<typename reduce_func_t, typename keyextr_func_t>
class Reduce: public Basic_Operator
{
private:
    friend class MultiPipe;
    friend class PipeGraph;
    reduce_func_t func; // functional logic used by the Reduce
    keyextr_func_t key_extr; // logic to extract the key attribute from the tuple_t
    using tuple_t = decltype(get_tuple_t_Reduce(func)); // extracting the tuple_t type and checking the admissible signatures
    using state_t = decltype(get_state_t_Reduce(func)); // extracting the state_t type and checking the admissible signatures
    using result_t = state_t;
    std::vector<Reduce_Replica<reduce_func_t, keyextr_func_t>*> replicas; // vector of pointers to the replicas of the Reduce
    static constexpr op_type_t op_type = op_type_t::BASIC;

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
        if (this->getOutputBatchSize() > 0 && _execution_mode != Execution_Mode_t::DEFAULT) {
            std::cerr << RED << "WindFlow Error: Reduce is trying to produce a batch in non DEFAULT mode" << DEFAULT_COLOR << std::endl;
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
    // Append the statistics (JSON format) of the Reduce to a PrettyWriter
    void appendStats(rapidjson::PrettyWriter<rapidjson::StringBuffer> &writer) const override
    {
        writer.StartObject(); // create the header of the JSON file
        writer.Key("Operator_name");
        writer.String((this->name).c_str());
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
        writer.Uint(this->parallelism);
        writer.Key("OutputBatchSize");
        writer.Uint(this->outputBatchSize);
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
     *  \param _func functional logic of the Reduce (a function or any callable type)
     *  \param _key_extr key extractor (a function or any callable type)
     *  \param _parallelism internal parallelism of the Reduce
     *  \param _name name of the Reduce
     *  \param _outputBatchSize size (in num of tuples) of the batches produced by this operator (0 for no batching)
     *  \param _closing_func closing functional logic of the Reduce (a function or any callable type)
     *  \param _initial_state initial value of the state to be used for each key
     */ 
    Reduce(reduce_func_t _func,
           keyextr_func_t _key_extr,
           size_t _parallelism,
           std::string _name,
           size_t _outputBatchSize,
           std::function<void(RuntimeContext &)> _closing_func,
           state_t _initial_state):
           Basic_Operator(_parallelism, _name, Routing_Mode_t::KEYBY, _outputBatchSize),
           func(_func),
           key_extr(_key_extr)
    {
        for (size_t i=0; i<this->parallelism; i++) { // create the internal replicas of the Reduce
            replicas.push_back(new Reduce_Replica<reduce_func_t, keyextr_func_t>(_func,
                                                                                       _key_extr,
                                                                                       this->name,
                                                                                       RuntimeContext(this->parallelism, i),
                                                                                       _closing_func,
                                                                                       _initial_state));
        }
    }

    /// Copy Constructor
    Reduce(const Reduce &_other):
           Basic_Operator(_other),
           func(_other.func),
           key_extr(_other.key_extr)
    {
        for (size_t i=0; i<this->parallelism; i++) { // deep copy of the pointers to the Reduce replicas
            replicas.push_back(new Reduce_Replica<reduce_func_t, keyextr_func_t>(*(_other.replicas[i])));
        }
    }

    // Destructor
    ~Reduce() override
    {
        for (auto *r: replicas) { // delete all the replicas
            delete r;
        }
    }

    /** 
     *  \brief Get the type of the Reduce as a string
     *  \return type of the Reduce
     */ 
    std::string getType() const override
    {
        return std::string("Reduce");
    }

    Reduce(Reduce &&) = delete; ///< Move constructor is deleted
    Reduce &operator=(const Reduce &) = delete; ///< Copy assignment operator is deleted
    Reduce &operator=(Reduce &&) = delete; ///< Move assignment operator is deleted
};

} // namespace wf

#endif
