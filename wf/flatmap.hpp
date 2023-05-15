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
 *  @file    flatmap.hpp
 *  @author  Gabriele Mencagli
 *  
 *  @brief FlatMap operator
 *  
 *  @section FlatMap (Description)
 *  
 *  This file implements the FlatMap operator able to execute streaming transformations
 *  producing zero, one or more than one output per input.
 */ 

#ifndef FLATMAP_H
#define FLATMAP_H

/// includes
#include<string>
#include<functional>
#include<shipper.hpp>
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

// class FlatMap_Replica
template<typename flatmap_func_t>
class FlatMap_Replica: public Basic_Replica
{
private:
    flatmap_func_t func; // functional logic used by the FlatMap replica
    using tuple_t = decltype(get_tuple_t_FlatMap(func)); // extracting the tuple_t type and checking the admissible signatures
    using result_t = decltype(get_result_t_FlatMap(func)); // extracting the result_t type and checking the admissible signatures
    // static predicates to check the type of the functional logic to be invoked
    static constexpr bool isNonRiched = std::is_invocable<decltype(func), const tuple_t &, Shipper<result_t> &>::value;
    static constexpr bool isRiched = std::is_invocable<decltype(func), const tuple_t &, Shipper<result_t> &, RuntimeContext &>::value;
    // check the presence of a valid functional logic
    static_assert(isNonRiched || isRiched,
        "WindFlow Compilation Error - FlatMap_Replica does not have a valid functional logic:\n");
    Shipper<result_t> *shipper; // pointer to the shipper object used by the FlatMap replica to send outputs
    uint64_t empty_rounds; // number of "consecutive" flatmap executions without produced outputs

public:
    // Constructor
    FlatMap_Replica(flatmap_func_t _func,
                    std::string _opName,
                    RuntimeContext _context,
                    std::function<void(RuntimeContext &)> _closing_func):
                    Basic_Replica(_opName, _context, _closing_func, false),
                    func(_func),
                    shipper(nullptr),
                    empty_rounds(0) {}

    // Copy Constructor
    FlatMap_Replica(const FlatMap_Replica &_other):
                    Basic_Replica(_other),
                    func(_other.func),
                    empty_rounds(0)
    {
        if (_other.shipper != nullptr) {
            shipper = new Shipper<result_t>(*_other.shipper); // create a copy of the shipper
            shipper->node = this; // change the node referred by the shipper
#if defined (WF_TRACING_ENABLED)
            shipper->setStatsRecord(&(this->stats_record)); // change the Stats_Record referred by the shipper
#endif
        }
        else {
            shipper = nullptr;
        }
    }

    // Destructor
    ~FlatMap_Replica()
    {
        if (shipper != nullptr) {
            delete shipper;
        }
    }

    // svc (utilized by the FastFlow runtime)
    void *svc(void *_in) override
    {
        this->startStatsRecording();
        if (this->input_batching) { // receiving a batch
            Batch_t<tuple_t> *batch_input = reinterpret_cast<Batch_t<tuple_t> *>(_in);
            if (batch_input->isPunct()) { // check if it is a punctuaton
                (shipper->emitter)->propagate_punctuation(batch_input->getWatermark((this->context).getReplicaIndex()), this); // propagate the received punctuation
                deleteBatch_t(batch_input); // delete the input batch
                return this->GO_ON;
            }
#if defined (WF_TRACING_ENABLED)
            (this->stats_record).inputs_received += batch_input->getSize();
            (this->stats_record).bytes_received += batch_input->getSize() * sizeof(tuple_t);
#endif
            for (size_t i=0; i<batch_input->getSize(); i++) { // process all the inputs within the received batch
                process_input(batch_input->getTupleAtPos(i), batch_input->getTimestampAtPos(i), batch_input->getWatermark((this->context).getReplicaIndex()));
            }
            deleteBatch_t(batch_input); // delete the input batch
        }
        else { // receiving a single input
            Single_t<tuple_t> *input = reinterpret_cast<Single_t<tuple_t> *>(_in);
            if (input->isPunct()) { // check if it is a punctuaton
                (shipper->emitter)->propagate_punctuation(input->getWatermark((this->context).getReplicaIndex()), this); // propagate the received punctuation
                deleteSingle_t(input); // delete the input Single_t
                return this->GO_ON;
            }
#if defined (WF_TRACING_ENABLED)
            (this->stats_record).inputs_received++;
            (this->stats_record).bytes_received += sizeof(tuple_t);
#endif
            process_input(input->tuple, input->getTimestamp(), input->getWatermark((this->context).getReplicaIndex()));
            deleteSingle_t(input); // delete the input Single_t
        }
        this->endStatsRecording();
        return this->GO_ON;
    }

    // Process a single input
    void process_input(const tuple_t &_tuple,
                       uint64_t _timestamp,
                       uint64_t _watermark)
    {
        shipper->setShipperParameters(_timestamp, _watermark); // set the parameter of the shipper
        uint64_t delivered = shipper->getNumDelivered();
        if constexpr (isNonRiched) { // non-riched version
            func(_tuple, *shipper);
        }
        if constexpr (isRiched)  { // riched version
            (this->context).setContextParameters(_timestamp, _watermark); // set the parameter of the RuntimeContext
            func(_tuple, *shipper, this->context);
        }
        if (delivered == shipper->getNumDelivered()) { // update empty_rounds
            empty_rounds++;
        }
        else {
            empty_rounds = 0;
        }
        if ((this->execution_mode == Execution_Mode_t::DEFAULT) && (empty_rounds % WF_DEFAULT_WM_AMOUNT == 0)) { // check punctuaction generation logic
            if (current_time_usecs() - this->last_time_punct >= WF_DEFAULT_WM_INTERVAL_USEC) { // check the end of the sample
                (shipper->emitter)->propagate_punctuation(_watermark, this);
                this->last_time_punct = current_time_usecs();
            }
        }
    }

    // EOS management (utilized by the FastFlow runtime)
    void eosnotify(ssize_t id) override
    {
        this->closing_func(this->context); // call the closing function here to allow the use of the shipper
        shipper->flush(); // call the flush of the shipper
        Basic_Replica::eosnotify(id);
    }

    // svc_end (utilized by the FastFlow runtime)
    void svc_end() override {} // it must be empty now (closing_func already called in eosnotify)

    // Set the emitter used to route outputs from the FlatMap replica
    void setEmitter(Basic_Emitter *_emitter) override
    {
        if (shipper != nullptr) { // if a shipper already exists, it is destroyed
            delete shipper;
        }
        shipper = new Shipper<result_t>(_emitter, this); // create the shipper
#if defined (WF_TRACING_ENABLED)
        shipper->setStatsRecord(&stats_record);
#endif
    }

    FlatMap_Replica(FlatMap_Replica &&) = delete; ///< Move constructor is deleted
    FlatMap_Replica &operator=(const FlatMap_Replica &) = delete; ///< Copy assignment operator is deleted
    FlatMap_Replica &operator=(FlatMap_Replica &&) = delete; ///< Move assignment operator is deleted
};

//@endcond

/** 
 *  \class FlatMap
 *  
 *  \brief FlatMap operator
 *  
 *  This class implements the FlatMap operator executing a streaming transformation producing
 *  zero, one or more than one output per input.
 */ 
template<typename flatmap_func_t, typename keyextr_func_t>
class FlatMap: public Basic_Operator
{
private:
    friend class MultiPipe;
    friend class PipeGraph;
    flatmap_func_t func; // functional logic used by the FlatMap
    keyextr_func_t key_extr; // logic to extract the key attribute from the tuple_t
    std::vector<FlatMap_Replica<flatmap_func_t>*> replicas; // vector of pointers to the replicas of the FlatMap

    // Configure the FlatMap to receive batches instead of individual inputs
    void receiveBatches(bool _input_batching) override
    {
        for (auto *r: replicas) {
            r->receiveBatches(_input_batching);
        }
    }

    // Set the emitter used to route outputs from the FlatMap
    void setEmitter(Basic_Emitter *_emitter) override
    {
        replicas[0]->setEmitter(_emitter);
        for (size_t i=1; i<replicas.size(); i++) {
            replicas[i]->setEmitter(_emitter->clone());
        }
    }

    // Check whether the FlatMap has terminated
    bool isTerminated() const override
    {
        bool terminated = true;
        for(auto *r: replicas) { // scan all the replicas to check their termination
            terminated = terminated && r->isTerminated();
        }
        return terminated;
    }

    // Set the execution mode of the FlatMap
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
    // Append the statistics (JSON format) of the FlatMap to a PrettyWriter
    void appendStats(rapidjson::PrettyWriter<rapidjson::StringBuffer> &writer) const override
    {
        writer.StartObject(); // create the header of the JSON file
        writer.Key("Operator_name");
        writer.String((this->name).c_str());
        writer.Key("Operator_type");
        writer.String("FlatMap");
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
        for (auto *r: replicas) { // append the statistics from all the replicas of the FlatMap
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
     *  \param _func functional logic of the FlatMap (a function or any callable type)
     *  \param _key_extr key extractor (a function or any callable type)
     *  \param _parallelism internal parallelism of the FlatMap
     *  \param _name name of the FlatMap
     *  \param _input_routing_mode input routing mode of the FlatMap
     *  \param _outputBatchSize size (in num of tuples) of the batches produced by this operator (0 for no batching)
     *  \param _closing_func closing functional logic of the FlatMap (a function or any callable type)
     */ 
    FlatMap(flatmap_func_t _func,
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
        for (size_t i=0; i<this->parallelism; i++) { // create the internal replicas of the FlatMap
            replicas.push_back(new FlatMap_Replica<flatmap_func_t>(_func, this->name, RuntimeContext(this->parallelism, i), _closing_func));
        }
    }

    /// Copy Constructor
    FlatMap(const FlatMap &_other):
            Basic_Operator(_other),
            func(_other.func),
            key_extr(_other.key_extr)
    {
        for (size_t i=0; i<this->parallelism; i++) { // deep copy of the pointers to the FlatMap replicas
            replicas.push_back(new FlatMap_Replica<flatmap_func_t>(*(_other.replicas[i])));
        }
    }

    // Destructor
    ~FlatMap() override
    {
        for (auto *r: replicas) { // delete all the replicas
            delete r;
        }
    }

    /** 
     *  \brief Get the type of the FlatMap as a string
     *  \return type of the FlatMap
     */ 
    std::string getType() const override
    {
        return std::string("FlatMap");
    }

    FlatMap(FlatMap &&) = delete; ///< Move constructor is deleted
    FlatMap &operator=(const FlatMap &) = delete; ///< Copy assignment operator is deleted
    FlatMap &operator=(FlatMap &&) = delete; ///< Move assignment operator is deleted
};

} // namespace wf

#endif
