/**************************************************************************************
 *  Copyright (c) 2023- Gabriele Mencagli and Yuriy Rymarchuk
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
 *  @file    interval_join.hpp
 *  @author  Gabriele Mencagli and Yuriy Rymarchuk
 *  
 *  @brief Interval Join operator
 *  
 *  @section Interval Join (Description)
 *  
 *  This file implements the Interval Join operator able to execute joins over two streams of tuples
 *  producing x output per input, where x is the number of asserted predicates in the given range.
 *  [ ...number of join conditions evalueted to true in the given range ]
 */ 

#ifndef INTERVAL_JOIN_H
#define INTERVAL_JOIN_H

/// includes
#include<iomanip>
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
#include<join_archive.hpp>
#include<iterable.hpp>

namespace wf {

//@cond DOXY_IGNORE

// class IJoin_Replica
template<typename join_func_t, typename keyextr_func_t>
class IJoin_Replica: public Basic_Replica
{
private:
    template<typename T1, typename T2> friend class Interval_Join;
    join_func_t func; // functional logic used by the Interval Join replica
    keyextr_func_t key_extr; // logic to extract the key attribute from the tuple_t
    using tuple_t = decltype(get_tuple_t_Join(func)); // extracting the tuple_t type and checking the admissible signatures
    using key_t = decltype(get_key_t_KeyExtr(key_extr)); // extracting the key_t type and checking the admissible singatures
    using result_t = decltype(get_result_t_Join(func)); // extracting the result_t type and checking the admissible signatures

    // static predicates to check the type of the functional logic to be invoked
    static constexpr bool isNonRiched = std::is_invocable<decltype(func), const tuple_t &, const tuple_t &>::value;
    static constexpr bool isRiched = std::is_invocable<decltype(func), const tuple_t &, const tuple_t &, RuntimeContext &>::value;
    // check the presence of a valid functional logic
    static_assert(isNonRiched || isRiched,
        "WindFlow Compilation Error - IJoin_Replica does not have a valid functional logic:\n");

    using wrapper_t = wrapper_tuple_t<tuple_t>; // alias for the wrapped tuple type
    using container_t = typename std::deque<wrapper_t>; // container type for underlying archive's buffer structure
    using iterator_t = typename container_t::iterator; // iterator type for accessing wrapped tuples in the archive
    
    using compare_func_t = std::function< bool(const wrapper_t &, const uint64_t &) >; // function type to compare wrapped tuple to an uint64

    struct Key_Descriptor // struct of a key descriptor
    {
        JoinArchive<tuple_t> archiveA; // archive of stream A tuples of this key
        JoinArchive<tuple_t> archiveB; // archive of stream B tuples of this key

        // Constructor
        Key_Descriptor(compare_func_t _compare_func):
                       archiveA(_compare_func),
                       archiveB(_compare_func) {}
    };

    compare_func_t compare_func; // function to compare wrapped to an uint64 that rapresent an timestamp ( or watermark )
    int64_t lower_bound; // lower bound of the interval ( ts - lower_bound )
    int64_t upper_bound; // upper bound of the interval ( ts + upper_bound )
    Interval_Join_Mode_t joinMode; // Interval Join operating mode
    std::unordered_map<key_t, Key_Descriptor> keyMap; // hash table that maps a descriptor for each key
    uint64_t last_time; // last received watermark or timestamp
    size_t ignored_tuples; // number of ignored tuples

    struct Buffer_Stats
    {
        uint64_t buff_size;
        uint64_t buff_count;

        // Constructor
        Buffer_Stats():
            buff_size(0),
            buff_count(0) {}
    };

    Buffer_Stats a_Buff;
    Buffer_Stats b_Buff;
    uint64_t last_sampled_size_time;

public:
    // Constructor
    IJoin_Replica(join_func_t _func,
                keyextr_func_t _key_extr,
                std::string _opName,
                RuntimeContext _context,
                std::function<void(RuntimeContext &)> _closing_func,
                int64_t _lower_bound,
                int64_t _upper_bound,
                Interval_Join_Mode_t _join_mode):
                Basic_Replica(_opName, _context, _closing_func, false),
                func(_func),
                key_extr(_key_extr),
                lower_bound(_lower_bound),
                upper_bound(_upper_bound),
                joinMode(_join_mode),
                ignored_tuples(0),
                last_time(0),
                a_Buff(Buffer_Stats()),
                b_Buff(Buffer_Stats()),
                last_sampled_size_time(current_time_nsecs())
    {
        compare_func = [](const wrapper_t &w1, const uint64_t &_idx) { // comparator function of wrapped tuples
            return w1.index < _idx;
        };
    }

    // Copy Constructor
    IJoin_Replica(const IJoin_Replica &_other):
                Basic_Replica(_other),
                func(_other.func),
                key_extr(_other.key_extr),
                lower_bound(_other.lower_bound),
                upper_bound(_other.upper_bound),
                joinMode(_other.joinMode),
                compare_func(_other.compare_func),
                ignored_tuples(_other.ignored_tuples),
                last_time(_other.last_time),
                a_Buff(_other.a_Buff),
                b_Buff(_other.b_Buff),
                last_sampled_size_time(current_time_nsecs()) {} //_other.last_sampled_size_time

    // svc (utilized by the FastFlow runtime)
    void *svc(void *_in) override
    {
        this->startStatsRecording();
        if (this->input_batching) { // receiving a batch
            Batch_t<tuple_t> *batch_input = reinterpret_cast<Batch_t<tuple_t> *>(_in);
            if (batch_input->isPunct()) { // if it is a punctuaton
                (this->emitter)->propagate_punctuation(batch_input->getWatermark((this->context).getReplicaIndex()), this); // propagate the received punctuation
                assert(last_time <= batch_input->getWatermark((this->context).getReplicaIndex())); // sanity check
                last_time = batch_input->getWatermark((this->context).getReplicaIndex());
                purgeWithPunct();
                deleteBatch_t(batch_input); // delete the punctuation
                return this->GO_ON;
            }
#if defined (WF_TRACING_ENABLED)
            (this->stats_record).inputs_received += batch_input->getSize();
            (this->stats_record).bytes_received += batch_input->getSize() * sizeof(tuple_t);
#endif
            for (size_t i=0; i<batch_input->getSize(); i++) { // process all the inputs within the received batch
                process_input(batch_input->getTupleAtPos(i), batch_input->getTimestampAtPos(i), batch_input->getWatermark((this->context).getReplicaIndex()), batch_input->getStreamTag());
            }
            deleteBatch_t(batch_input); // delete the input batch
        }
        else { // receiving a single input
            Single_t<tuple_t> *input = reinterpret_cast<Single_t<tuple_t> *>(_in);
            if (input->isPunct()) { // if it is a punctuaton
                (this->emitter)->propagate_punctuation(input->getWatermark((this->context).getReplicaIndex()), this); // propagate the received punctuation
                assert(last_time <= input->getWatermark((this->context).getReplicaIndex())); // sanity check
                last_time = input->getWatermark((this->context).getReplicaIndex());
                purgeWithPunct();
                deleteSingle_t(input); // delete the punctuation
                return this->GO_ON;
            }
#if defined (WF_TRACING_ENABLED)
            (this->stats_record).inputs_received++;
            (this->stats_record).bytes_received += sizeof(tuple_t);
#endif
            process_input(input->tuple, input->getTimestamp(), input->getWatermark((this->context).getReplicaIndex()), input->getStreamTag());
            deleteSingle_t(input); // delete the input Single_t
        }
        this->endStatsRecording();
        return this->GO_ON;
    }

    // Process a single input
    void process_input(tuple_t &_tuple,
                       uint64_t _timestamp,
                       uint64_t _watermark,
                       Join_Stream_t _tag)
    {
        if (this->execution_mode == Execution_Mode_t::DEFAULT && _timestamp < last_time) { // if the input is out-of-order
#if defined (WF_TRACING_ENABLED)
            stats_record.inputs_ignored++;
#endif
            ignored_tuples++;
            return;
        }

#if defined (WF_JOIN_STATS)
        if (joinMode == Interval_Join_Mode_t::DPS) {
            uint64_t delta = (current_time_nsecs() - last_sampled_size_time) / 1e06; //ms
            if ( delta >= 250 )
            {
                for (auto &k: keyMap) {
                    Key_Descriptor &key_d = (k.second);
                    a_Buff.buff_size += (key_d.archiveA).size();
                    b_Buff.buff_size += (key_d.archiveB).size();
                }
                a_Buff.buff_count++;
                b_Buff.buff_count++;
                last_sampled_size_time = current_time_nsecs();
            }
        }
#endif

        if (this->execution_mode == Execution_Mode_t::DEFAULT) {
            assert(last_time <= _watermark); // sanity check
            last_time = _watermark;
        } else {
            last_time = _timestamp < last_time ? last_time : _timestamp;
        }
        
        auto key = key_extr(_tuple); // get the key attribute of the input tuple
        auto it = keyMap.find(key); // find the corresponding key_descriptor (or allocate it if does not exist)
        if (it == keyMap.end()) {
            auto p = keyMap.insert(std::make_pair(key, Key_Descriptor(compare_func))); // create the state of the key
            it = p.first;
        }
        Key_Descriptor &key_d = (*it).second;
        
        uint64_t l_b = 0;
        if (isStreamA(_tag)) {
            if (!(-lower_bound > (int64_t)_timestamp))  { l_b = _timestamp + lower_bound; }
        } else {
            if (!(upper_bound > (int64_t)_timestamp))   { l_b = _timestamp - upper_bound; }
        }
        
        uint64_t u_b = 0;
        if (isStreamA(_tag)) {      
            if (!(-upper_bound > (int64_t)_timestamp))  { u_b = _timestamp + upper_bound; }
        } else {
            if (!(lower_bound > (int64_t)_timestamp))   { u_b = _timestamp - lower_bound; }
        }

        std::optional<result_t> output;
        std::pair<iterator_t, iterator_t> its = isStreamA(_tag) ? (key_d.archiveB).getJoinRange(l_b, u_b) : (key_d.archiveA).getJoinRange(l_b, u_b);
        Iterable<tuple_t> interval(its.first, its.second);
        for (size_t i=0; i<interval.size(); i++) {
            if constexpr (isNonRiched) { // inplace non-riched version
                output = isStreamA(_tag) ? func(_tuple, (interval.at(i))) : func(interval.at(i), _tuple);
            }
            if constexpr (isRiched)  { // inplace riched version
                (this->context).setContextParameters(_timestamp, _watermark); // set the parameter of the RuntimeContext
                output = isStreamA(_tag) ? func(_tuple, interval.at(i), this->context) : func(interval.at(i), _tuple, this->context);
            }
            if (output) {
                uint64_t ts = _timestamp >= interval.index_at(i) ? _timestamp : interval.index_at(i); // use the highest timestamp between two joined tuples
                (this->emitter)->emit(&(*output), 0, ts, _watermark, this);
#if defined (WF_TRACING_ENABLED)
                (this->stats_record).outputs_sent++;
                (this->stats_record).bytes_sent += sizeof(result_t);
#endif
            }
        }
        if (joinMode == Interval_Join_Mode_t::KP) {
            insertIntoBuffer(key_d, wrapper_t(_tuple, _timestamp), _tag);
        } else if (joinMode == Interval_Join_Mode_t::DPS) {
            if constexpr(if_defined_hash<tuple_t>) {
                size_t hash_idx = std::hash<tuple_t>()(_tuple) % this->context.getParallelism(); // compute the hash index of the tuple
                if (hash_idx == this->context.getReplicaIndex()) {
                    insertIntoBuffer(key_d, wrapper_t(_tuple, _timestamp), _tag);
                }
            } else {
                size_t hash_idx = std::hash<uint>()(_timestamp) % this->context.getParallelism(); // compute the hash index of the tuple using memory address
                if (hash_idx == this->context.getReplicaIndex()) {
                    insertIntoBuffer(key_d, wrapper_t(_tuple, _timestamp), _tag);
                }
            }
        }
        purgeBuffers(key_d);
    }

    inline bool isStreamA(Join_Stream_t stream) const
    {
        return stream == Join_Stream_t::A;
    }

    inline void insertIntoBuffer(Key_Descriptor &_key_d, wrapper_t _wt, Join_Stream_t stream)
    {
        (stream == Join_Stream_t::A) ? (_key_d.archiveA).insert(_wt) : (_key_d.archiveB).insert(_wt);
    }

    void purgeBuffers(Key_Descriptor &_key_d)
    {
        uint64_t idx_a = 0, idx_b = 0;
        if (!(upper_bound > (int64_t)last_time))  { idx_a = last_time - upper_bound; }
        if (!(-lower_bound > (int64_t)last_time)) { idx_b = last_time + lower_bound; }
        if (idx_a != 0) (_key_d.archiveA).purge(idx_a);
        if (idx_b != 0) (_key_d.archiveB).purge(idx_b);
    }

    void purgeWithPunct()
    {
        for (auto &k: keyMap) {
            Key_Descriptor &key_d = (k.second);
            purgeBuffers(key_d);
        }
    }

    double getBufferMeanSize(Join_Stream_t stream) const
    {
        double mean = 0.0;
        if (stream == Join_Stream_t::A) {
            mean = static_cast<double>(a_Buff.buff_size) / a_Buff.buff_count;
        } else {
            mean = static_cast<double>(b_Buff.buff_size) / b_Buff.buff_count;
        }
        return mean;
    }

    // Get the number of ignored tuples
    size_t getNumIgnoredTuples() const
    {
        return ignored_tuples;
    }

    IJoin_Replica(IJoin_Replica &&) = delete; ///< Move constructor is deleted
    IJoin_Replica &operator=(const IJoin_Replica &) = delete; ///< Copy assignment operator is deleted
    IJoin_Replica &operator=(IJoin_Replica &&) = delete; ///< Move assignment operator is deleted
};

//@endcond

/** 
 *  \class Interval Join
 *  
 *  \brief Interval Join operator
 *  
 *  This class implements the Interval Join operator able to execute joins over two streams of tuples
 *  producing x output per input, where x is the number of asserted predicates in the given range.
 */ 
template<typename join_func_t, typename keyextr_func_t>
class Interval_Join: public Basic_Operator
{
private:
    friend class MultiPipe;
    friend class PipeGraph;
    join_func_t func; // functional boolean condition logic used by the Interval Join
    keyextr_func_t key_extr; // logic to extract the key attribute from the tuple_t
    std::vector<IJoin_Replica<join_func_t, keyextr_func_t>*> replicas; // vector of pointers to the replicas of the Interval Join
    int64_t lower_bound; // lower bound of the interval, can be negative ( ts + lower_bound )
    int64_t upper_bound; // upper bound of the interval, can be negative ( ts + upper_bound )
    Interval_Join_Mode_t joinMode; // Interval Join operating mode

    // Configure the Interval Join to receive batches instead of individual inputs
    void receiveBatches(bool _input_batching) override
    {
        for (auto *r: replicas) {
            r->receiveBatches(_input_batching);
        }
    }

    // Set the emitter used to route outputs from the Interval Join
    void setEmitter(Basic_Emitter *_emitter) override
    {
        replicas[0]->setEmitter(_emitter);
        for (size_t i=1; i<replicas.size(); i++) {
            replicas[i]->setEmitter(_emitter->clone());
        }
    }

    // Check whether the Interval Join has terminated
    bool isTerminated() const override
    {
        bool terminated = true;
        for(auto *r: replicas) { // scan all the replicas to check their termination
            terminated = terminated && r->isTerminated();
        }
        return terminated;
    }

    // Set the execution mode of the Interval Join
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
        writer.String("Interval_Join");
        writer.Key("Distribution");
        if (this->getInputRoutingMode() == Routing_Mode_t::KEYBY) {
            writer.String("KEYBY");
        }
        else if (this->getInputRoutingMode() == Routing_Mode_t::BROADCAST) {
            writer.String("BROADCAST");
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
        writer.Key("Lower_Bound");
        writer.Int64(lower_bound);
        writer.Key("Uper_Bound");
        writer.Int64(upper_bound);
        writer.Key("Join_Mode");
        if (this->joinMode == Interval_Join_Mode_t::KP) {
            writer.String("Key-Parallelism");
        }
        else if (this->joinMode == Interval_Join_Mode_t::DPS) {
            writer.String("Data-Parallelism_Single-Buffer");
        }
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
     *  \param _func functional boolean condition logic of the Interval Join (a function or any callable type)
     *  \param _key_extr key extractor (a function or any callable type)
     *  \param _parallelism internal parallelism of the Interval Join
     *  \param _name name of the Interval Join
     *  \param _input_routing_mode input routing mode of the Interval Join
     *  \param _outputBatchSize size (in num of tuples) of the batches produced by this operator (0 for no batching)
     *  \param _closing_func closing functional logic of the Interval Join (a function or any callable type)
     *  \param _lower_bound lower bound of the interval ( ts - lower_bound )
     *  \param _upper_bound upper bound of the interval ( ts - upper_bound )
     *  \param _join_mode Interval Join operating mode
     */ 
    Interval_Join(join_func_t _func,
                  keyextr_func_t _key_extr,
                  size_t _parallelism,
                  std::string _name,
                  Routing_Mode_t _input_routing_mode,
                  size_t _outputBatchSize,
                  std::function<void(RuntimeContext &)> _closing_func,
                  int64_t _lower_bound,
                  int64_t _upper_bound,
                  Interval_Join_Mode_t _join_mode):
                  Basic_Operator(_parallelism, _name, _input_routing_mode, _outputBatchSize),
                  func(_func),
                  key_extr(_key_extr),
                  lower_bound(_lower_bound),
                  upper_bound(_upper_bound),
                  joinMode(_join_mode)
    {
        for (size_t i=0; i<this->parallelism; i++) { // create the internal replicas of the Interval Join
            replicas.push_back(new IJoin_Replica<join_func_t, keyextr_func_t>(_func, _key_extr, this->name, RuntimeContext(this->parallelism, i), _closing_func, _lower_bound, _upper_bound, _join_mode));
        }
    }

    /// Copy constructor
    Interval_Join(const Interval_Join &_other):
        Basic_Operator(_other),
        func(_other.func),
        key_extr(_other.key_extr),
        lower_bound(_other.lower_bound),
        upper_bound(_other.upper_bound),
        joinMode(_other.joinMode)
    {
        for (size_t i=0; i<this->parallelism; i++) { // deep copy of the pointers to the Interval Join replicas
            replicas.push_back(new IJoin_Replica<join_func_t, keyextr_func_t>(*(_other.replicas[i])));
        }
    }

    // Destructor
    ~Interval_Join() override
    {
#if defined (WF_JOIN_STATS)
        if (joinMode == Interval_Join_Mode_t::DPS && this->isTerminated()) {
            printBufferStats(Join_Stream_t::A);
            printBufferStats(Join_Stream_t::B);
        }
#endif
        for (auto *r: replicas) { // delete all the replicas
            delete r;
        }
    }

    void printBufferStats(Join_Stream_t stream) {
        std::cout << (stream == Join_Stream_t::A ? "A" : "B") << " Buffer Stats: " << std::endl;
        uint64_t num_replicas = replicas.size();
        double acc_size = 0.0;
        int i = 0;
        for (auto *r: replicas) {
            std::cout << (i+1) << " Replica mean -> " << r->getBufferMeanSize(stream) << std::endl;
            acc_size += r->getBufferMeanSize(stream);
            i++;
        }
        double mean_size = static_cast<double>(acc_size) / num_replicas;
        std::cout << "Mean Buffer Size -> " << mean_size << std::endl;
        // Check distribution
        /*
        double variance = 0;
        for (auto *r: replicas) {
            double diff = r->getBufferMeanSize(stream) - mean_size;
            variance += diff * diff;
        }
        variance = variance / num_replicas;
        double stddev = sqrt(variance);
        double threshold_balance = (0.3*mean_size);
        std::string check_balance = stddev < threshold_balance ? " ✔ " : " ✘ ";
        std::cout << std::fixed << std::setprecision(2);
        std::cout << "Variance -> " << variance << ", stddev -> " << stddev << " | Balance threshold -> " << stddev << "<" << threshold_balance << check_balance << std::endl;
        */
    }

    /** 
     *  \brief Get the type of the Interval Join as a string
     *  \return type of the Interval Join
     */ 
    std::string getType() const override
    {
        return std::string("Interval_Join");
    }

    Interval_Join(Interval_Join &&) = delete; ///< Move constructor is deleted
    Interval_Join &operator=(const Interval_Join &) = delete; ///< Copy assignment operator is deleted
    Interval_Join &operator=(Interval_Join &&) = delete; ///< Move assignment operator is deleted
};

} // namespace wf

#endif
