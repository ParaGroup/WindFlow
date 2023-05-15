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
class Sink_Replica: public Basic_Replica
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
    bool copyOnWrite; // flag stating if a copy of each input must be done in case of in-place semantics

public:
    // Constructor
    Sink_Replica(sink_func_t _func,
                 std::string _opName,
                 RuntimeContext _context,
                 std::function<void(RuntimeContext &)> _closing_func,
                 bool _copyOnWrite):
                 Basic_Replica(_opName, _context, _closing_func, false),
                 func(_func),
                 copyOnWrite(_copyOnWrite) {}

    // Copy Constructor
    Sink_Replica(const Sink_Replica &_other):
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
                deleteBatch_t(batch_input); // delete the punctuation
                return this->GO_ON;
            }
#if defined (WF_TRACING_ENABLED)
            (this->stats_record).inputs_received += batch_input->getSize();
            (this->stats_record).bytes_received += batch_input->getSize() * sizeof(tuple_t);
#endif
            for (size_t i=0; i<batch_input->getSize(); i++) { // process all the inputs within the received batch
                if (copyOnWrite) {
                    tuple_t t = batch_input->getTupleAtPos(i);
                    process_input(t, batch_input->getTimestampAtPos(i), batch_input->getWatermark((this->context).getReplicaIndex()));
                }
                else {
                    process_input(batch_input->getTupleAtPos(i), batch_input->getTimestampAtPos(i), batch_input->getWatermark((this->context).getReplicaIndex()));
                }
            }
            deleteBatch_t(batch_input); // delete the input batch
        }
        else { // receiving a single input
            Single_t<tuple_t> *input = reinterpret_cast<Single_t<tuple_t> *>(_in);
            if (input->isPunct()) { // if it is a punctuaton
                deleteSingle_t(input); // delete the punctuation
                return this->GO_ON;
            }
#if defined (WF_TRACING_ENABLED)
            (this->stats_record).inputs_received++;
            (this->stats_record).bytes_received += sizeof(tuple_t);
#endif
            if (copyOnWrite) {
                tuple_t t = input->tuple;
                process_input(t, input->getTimestamp(), input->getWatermark((this->context).getReplicaIndex()));
            }
            else {
                process_input(input->tuple, input->getTimestamp(), input->getWatermark((this->context).getReplicaIndex()));
            }
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
        
        if constexpr (isNonRichedNonWrapper) { // non-riched non-wrapper version
            std::optional<tuple_t> opt_tuple = std::make_optional(std::move(_tuple)); // move the input tuple in the optional
            func(opt_tuple);
        }
        if constexpr (isRichedNonWrapper)  { // riched non-wrapper version
            (this->context).setContextParameters(_timestamp, _watermark); // set the parameter of the RuntimeContext
            std::optional<tuple_t> opt_tuple = std::make_optional(std::move(_tuple)); // move the input tuple in the optional
            func(opt_tuple, this->context);
        }
        if constexpr (isNonRichedWrapper) { // non-riched wrapper version
            std::optional<std::reference_wrapper<tuple_t>> opt_wtuple = std::make_optional(std::ref(_tuple));
            func(opt_wtuple);
        }
        if constexpr (isRichedWrapper) { // riched wrapper version
            (this->context).setContextParameters(_timestamp, _watermark); // set the parameter of the RuntimeContext
            std::optional<std::reference_wrapper<tuple_t>> opt_wtuple = std::make_optional(std::ref(_tuple));
            func(opt_wtuple, this->context);
        }
    }

    // svc_end (utilized by the FastFlow runtime)
    void svc_end() override
    {
        if constexpr (isNonRichedNonWrapper) { // non-riched non-wrapper version
            std::optional<tuple_t> opt_empty; // create empty optional
            func(opt_empty);
        }
        if constexpr (isRichedNonWrapper)  { // riched non-wrapper version
            std::optional<tuple_t> opt_empty; // create empty optional
            func(opt_empty, this->context);
        }
        if constexpr (isNonRichedWrapper) { // non-riched wrapper version
            std::optional<std::reference_wrapper<tuple_t>> wopt_empty;
            func(wopt_empty);
        }
        if constexpr (isRichedWrapper) { // riched wrapper version
            std::optional<std::reference_wrapper<tuple_t>> wopt_empty;
            func(wopt_empty, this->context);
        }
        Basic_Replica::svc_end();
    }

    Sink_Replica(Sink_Replica &&) = delete; ///< Move constructor is deleted
    Sink_Replica &operator=(const Sink_Replica &) = delete; ///< Copy assignment operator is deleted
    Sink_Replica &operator=(Sink_Replica &&) = delete; ///< Move assignment operator is deleted
};

//@endcond

/** 
 *  \class Sink
 *  
 *  \brief Sink operator
 *  
 *  This class implements the Sink operator able to absorb input streams
 */ 
template<typename sink_func_t, typename keyextr_func_t>
class Sink: public Basic_Operator
{
private:
    friend class MultiPipe;
    friend class PipeGraph;
    sink_func_t func; // functional logic used by the Sink
    using tuple_t = decltype(get_tuple_t_Sink(func)); // extracting the tuple_t type and checking the admissible signatures
    keyextr_func_t key_extr; // logic to extract the key attribute from the tuple_t
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

    // Set the execution mode of the Sink
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
    // Append the statistics (JSON format) of the Sink to a PrettyWriter
    void appendStats(rapidjson::PrettyWriter<rapidjson::StringBuffer> &writer) const override
    {
        writer.StartObject(); // create the header of the JSON file
        writer.Key("Operator_name");
        writer.String((this->name).c_str());
        writer.Key("Operator_type");
        writer.String("Sink");
        writer.Key("Distribution");
        if (this->input_routing_mode == Routing_Mode_t::KEYBY) {
            writer.String("KEYBY");
        }
        else if (this->input_routing_mode == Routing_Mode_t::REBALANCING) {
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
     *  \param _func functional logic of the Sink (a function or any callable type)
     *  \param _key_extr key extractor (a function or any callable type)
     *  \param _parallelism internal parallelism of the Sink
     *  \param _name name of the Sink
     *  \param _input_routing_mode input routing mode of the Sink
     *  \param _closing_func closing functional logic of the Sink (a function or any callable type)
     */ 
    Sink(sink_func_t _func,
         keyextr_func_t _key_extr,
         size_t _parallelism,
         std::string _name,
         Routing_Mode_t _input_routing_mode,
         std::function<void(RuntimeContext &)> _closing_func):
         Basic_Operator(_parallelism, _name, _input_routing_mode, 1 /* outputBatchSize fixed to 1 */),
         func(_func),
         key_extr(_key_extr)
    {
        for (size_t i=0; i<this->parallelism; i++) { // create the internal replicas of the Sink
            bool copyOnWrite = (this->input_routing_mode == Routing_Mode_t::BROADCAST);
            replicas.push_back(new Sink_Replica<sink_func_t>(_func, this->name, RuntimeContext(this->parallelism, i), _closing_func, copyOnWrite));
        }
    }

    /// Copy Constructor
    Sink(const Sink &_other):
         Basic_Operator(_other),
         func(_other.func),
         key_extr(_other.key_extr)
    {
        for (size_t i=0; i<this->parallelism; i++) { // deep copy of the pointers to the Sink replicas
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

    /** 
     *  \brief Get the type of the Sink as a string
     *  \return type of the Sink
     */ 
    std::string getType() const override
    {
        return std::string("Sink");
    }

    Sink(Sink &&) = delete; ///< Move constructor is deleted
    Sink &operator=(const Sink &) = delete; ///< Copy assignment operator is deleted
    Sink &operator=(Sink &&) = delete; ///< Move assignment operator is deleted
};

} // namespace wf

#endif
