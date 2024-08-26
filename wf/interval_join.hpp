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
#include <cstdint>
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
    using result_t = decltype(get_result_t_Join(func)); // extracting the result_t type and checking the admissible signatures
    using key_t = decltype(get_key_t_KeyExtr(key_extr)); // extracting the key_t type and checking the admissible singatures

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

    struct Archive_Stats
    {
        size_t size;
        uint64_t size_count;

        Archive_Stats():
            size(0),
            size_count(0) {}

        void recordSize(uint64_t _size)
        {
            size += _size;
            size_count++;
        }

        double getArchiveMeanSize() const
        {
            double mean = static_cast<double>(size) / size_count;
            return std::isnan(mean) ? 0.0 : mean;
        }
    };
    uint64_t last_measured_size_time; // last time (ns) the archives size was measured

    struct Key_Descriptor // struct of a key descriptor
    {
        JoinArchive<tuple_t, compare_func_t> archiveA; // archive of stream A tuples of this key
        JoinArchive<tuple_t, compare_func_t> archiveB; // archive of stream B tuples of this key
        Archive_Stats archive_metrics;

        // Constructor
        Key_Descriptor(compare_func_t _compare_func):
                        archiveA(_compare_func),
                        archiveB(_compare_func),
                        archive_metrics(Archive_Stats()) {}
        
        void recordSize()
        {
            archive_metrics.recordSize((archiveA.size()+archiveB.size()));
        }
    };

    compare_func_t compare_func; // function to compare wrapped to an uint64 that rapresent an timestamp ( or watermark )
    int64_t lower_bound; // lower bound of the interval ( ts - lower_bound )
    int64_t upper_bound; // upper bound of the interval ( ts + upper_bound )
    Join_Mode_t joinMode; // Interval Join operating mode
    std::unordered_map<key_t, Key_Descriptor> keyMap; // hash table that maps a descriptor for each key
    uint64_t last_time; // last received watermark or timestamp
    size_t ignored_tuples; // number of ignored tuples

    size_t id_inner; // id_inner value
    size_t num_inner; // num_inner value

public:
    // Constructor
    IJoin_Replica(join_func_t _func,
                keyextr_func_t _key_extr,
                std::string _opName,
                RuntimeContext _context,
                std::function<void(RuntimeContext &)> _closing_func,
                int64_t _lower_bound,
                int64_t _upper_bound,
                Join_Mode_t _join_mode,
                size_t _id_inner,
                size_t _num_inner):
                Basic_Replica(_opName, _context, _closing_func, false),
                func(_func),
                key_extr(_key_extr),
                lower_bound(_lower_bound),
                upper_bound(_upper_bound),
                joinMode(_join_mode),
                ignored_tuples(0),
                last_time(0),
                id_inner(_id_inner),
                num_inner(_num_inner),
                last_measured_size_time(current_time_nsecs())
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
                id_inner(_other.id_inner),
                num_inner(_other.num_inner),
                last_measured_size_time(current_time_nsecs()) {} 

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

        auto key = key_extr(_tuple); // get the key attribute of the input tuple
        auto it = keyMap.find(key); // find the corresponding key_descriptor (or allocate it if does not exist)
        if (it == keyMap.end()) {
            auto p = keyMap.insert(std::make_pair(key, Key_Descriptor(compare_func))); // create the state of the key
            it = p.first;
        }
        Key_Descriptor &key_d = (*it).second;

        uint64_t l_b = 0;
        if (isStreamA(_tag)) {
            if (-lower_bound <= static_cast<int64_t>(_timestamp))  { l_b = _timestamp + lower_bound; }
        } else {
            if (upper_bound <= static_cast<int64_t>(_timestamp))   { l_b = _timestamp - upper_bound; }
        }
        
        uint64_t u_b = 0;
        if (isStreamA(_tag)) {      
            if (-upper_bound <= static_cast<int64_t>(_timestamp))  { u_b = _timestamp + upper_bound; }
        } else {
            if (lower_bound <= static_cast<int64_t>(_timestamp))   { u_b = _timestamp - lower_bound; }
        }

        std::optional<result_t> output;
        std::pair<iterator_t, iterator_t> its = isStreamA(_tag) ? (key_d.archiveB).getJoinRange(l_b, u_b) : (key_d.archiveA).getJoinRange(l_b, u_b);
        Iterable<tuple_t> interval(its.first, its.second);
        for (size_t i=0; i<interval.size(); i++) {
            if constexpr (isNonRiched) { // inplace non-riched version
                output = isStreamA(_tag) ? func(_tuple, interval.at(i)) : func(interval.at(i), _tuple);
            }
            if constexpr (isRiched)  { // inplace riched version
                (this->context).setContextParameters(_timestamp, _watermark); // set the parameter of the RuntimeContext
                output = isStreamA(_tag) ? func(_tuple, interval.at(i), this->context) : func(interval.at(i), _tuple, this->context);
            }
            if (output) {
                uint64_t ts = (_timestamp >= interval.index_at(i)) ? _timestamp : interval.index_at(i); // use the highest timestamp between two joined tuples
                this->doEmit(this->emitter, &(*output), 0, ts, _watermark, this);
#if defined (WF_TRACING_ENABLED)
                (this->stats_record).outputs_sent++;
                (this->stats_record).bytes_sent += sizeof(result_t);
#endif
            }
        }

        if (joinMode == Join_Mode_t::KP) {
            insertIntoBuffer(key_d, wrapper_t(_tuple, _timestamp), _tag);
        } else if (joinMode == Join_Mode_t::DP) {
            if constexpr(if_defined_hash<tuple_t>) {
                size_t hash = std::hash<tuple_t>()(_tuple);
                size_t hash_idx = (hash % num_inner); // compute the hash index of the tuple
                if (hash_idx == id_inner) {
                    insertIntoBuffer(key_d, wrapper_t(_tuple, _timestamp), _tag);
                }
            } else {
                size_t hash = fnv1a_hash(&_timestamp);
                size_t hash_idx = (hash % num_inner); // compute the hash index of the tuple
                if (hash_idx == id_inner) {
                    insertIntoBuffer(key_d, wrapper_t(_tuple, _timestamp), _tag);
                }
            }
        }

        if (this->execution_mode == Execution_Mode_t::DEFAULT) {
            assert(last_time <= _watermark); // sanity check
            if ( last_time < _watermark)
                purgeArchives(key_d); // purge the archives using the new watermark
            last_time = _watermark;
        } else {
            if (last_time < _timestamp) {
                last_time = _timestamp;
            }
        }

#if defined (WF_JOIN_MEASUREMENT)
        uint64_t delta = (current_time_nsecs() - last_measured_size_time) / 1e06; //ms
        if ( delta >= 200 ) {
            for (auto &k: keyMap) {
                Key_Descriptor &key_m_d = (k.second);
                (key_m_d.recordSize());
            }
            last_measured_size_time = current_time_nsecs();
        }
#endif

    }

    inline const size_t fnv1a_hash(const void* key, const size_t len = sizeof(uint64_t)) {
        const char* data = (char*)key;
        const size_t prime = 0x1000193;
        size_t hash = 0x811c9dc5;
        for(int i = 0; i < len; ++i) {
            uint8_t value = data[i];
            hash = hash ^ value;
            hash *= prime;
        }
        return hash;
    }

    inline bool isStreamA(Join_Stream_t stream) const
    {
        return stream == Join_Stream_t::A;
    }

    inline void insertIntoBuffer(Key_Descriptor &_key_d, wrapper_t _wt, Join_Stream_t stream)
    {
        isStreamA(stream) ? (_key_d.archiveA).insert(_wt) : (_key_d.archiveB).insert(_wt);
    }

    void purgeArchives(Key_Descriptor &_key_d)
    {
        uint64_t idx_a = 0, idx_b = 0;
        if ((upper_bound) <= static_cast<int64_t>(last_time))  { idx_a = last_time - upper_bound; }
        if (-(lower_bound) <= static_cast<int64_t>(last_time)) { idx_b = last_time + lower_bound; }
        (_key_d.archiveA).purge(idx_a);
        (_key_d.archiveB).purge(idx_b);
    }

    void purgeWithPunct()
    {
        for (auto &k: keyMap) {
            Key_Descriptor &key_d = (k.second);
            purgeArchives(key_d);
        }
    }

    double getArchiveMeanSize()
    {
        if(keyMap.empty()) return 0.0;
        double acc = 0;
        uint64_t n_key = 0;
        for (auto &k: keyMap) {
            Key_Descriptor &key_d = (k.second);
            auto mean_size = (key_d.archive_metrics).getArchiveMeanSize();
            acc += mean_size;
            n_key++;
        }
        return std::isnan(acc / n_key) ? 0.0 : acc / n_key;
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
    Join_Mode_t joinMode; // Interval Join operating mode

    using tuple_t = decltype(get_tuple_t_Join(func)); // extracting the tuple_t type and checking the admissible signatures
    using result_t = decltype(get_result_t_Join(func)); // extracting the result_t type and checking the admissible signatures
    static constexpr op_type_t op_type = op_type_t::BASIC;

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
        if (this->getOutputBatchSize() > 0 && _execution_mode != Execution_Mode_t::DEFAULT) {
            std::cerr << RED << "WindFlow Error: Interval Join is trying to produce a batch in non DEFAULT mode" << DEFAULT_COLOR << std::endl;
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
        if (this->joinMode == Join_Mode_t::KP) {
            writer.String("Key-Parallelism");
        }
        else if (this->joinMode == Join_Mode_t::DP) {
            writer.String("Data-Parallelism");
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
                  Join_Mode_t _join_mode):
                  Basic_Operator(_parallelism, _name, _input_routing_mode, _outputBatchSize),
                  func(_func),
                  key_extr(_key_extr),
                  lower_bound(_lower_bound),
                  upper_bound(_upper_bound),
                  joinMode(_join_mode)
    {
        for (size_t i=0; i<this->parallelism; i++) { // create the internal replicas of the Interval Join
            replicas.push_back(new IJoin_Replica<join_func_t, keyextr_func_t>(_func, _key_extr, this->name, RuntimeContext(this->parallelism, i), _closing_func, _lower_bound, _upper_bound, _join_mode, i, this->parallelism));
        }
        std::cout << "Tuple size -> " << sizeof(tuple_t) << std::endl;
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
#if defined(WF_JOIN_MEASUREMENT)
        if (this->isTerminated()) {
            printArchivePerKeyStats();
        }
#endif
        for (auto *r: replicas) { // delete all the replicas
            delete r;
        }
    }

    void printArchivePerKeyStats() {
        std::cout << "***" << std::endl;
        std::cout << "Archive Stats: " << std::endl;
        uint64_t num_replicas = replicas.size();
        double acc_mean = 0.0;
        int i = 0;
        for (auto *r: replicas) {
            auto mean = r->getArchiveMeanSize();
            std::cout << (i+1) << " Replica mean -> " << mean << std::endl;
            acc_mean += mean;
            i++;
        }
        double mean_size = acc_mean / num_replicas;
        std::cout << "Global Mean Archive Size -> " << mean_size << std::endl;
        
        // Check distribution
        double variance = 0;
        for (auto *r: replicas) {
            variance += std::pow(r->getArchiveMeanSize() - mean_size, 2);
        }
        variance /= num_replicas;
        
        double cv = variance != 0 ? sqrt(variance) / mean_size * 100 : 0.0; //coefficient of variation
        std::string check_balance = cv < 20 ? " ✔ " : " ✘ ";
        std::cout << std::fixed << std::setprecision(2);
        std::cout << "Variance -> " << variance << " | Coefficient of variation -> " << cv << "| Balanced Distribution ->" << check_balance << std::endl;
        std::cout << "***" << std::endl;
    }

    /** 
     *  \brief Get the type of the Interval Join as a string
     *  \return type of the Interval Join
     */ 
    std::string getType() const override
    {
        return joinMode == Join_Mode_t::KP ? std::string("Interval_Join_KP") : std::string("Interval_Join_DP");
    }

    Interval_Join(Interval_Join &&) = delete; ///< Move constructor is deleted
    Interval_Join &operator=(const Interval_Join &) = delete; ///< Copy assignment operator is deleted
    Interval_Join &operator=(Interval_Join &&) = delete; ///< Move assignment operator is deleted
};

} // namespace wf

#endif
