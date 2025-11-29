/**************************************************************************************
 *  Copyright (c) 2024- Gabriele Mencagli and Yuriy Rymarchuk
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
 *  @file    window_join.hpp
 *  @author  Gabriele Mencagli and Yuriy Rymarchuk
 *  
 *  @brief Window Join operator
 *  
 *  @section Window Join (Description)
 *  
 *  This file implements the Window Join operator able to execute joins over two streams of tuples
 *  producing x output per input, where x is the number of asserted predicates that lay in the same
 *  temporal window.
 */ 

#ifndef WINDOW_JOIN_H
#define WINDOW_JOIN_H

/// includes
#include<string>
#include<cstdint>
#include<iomanip>
#include<functional>
#include<context.hpp>
#include<batch_t.hpp>
#include<single_t.hpp>
#if defined (WF_TRACING_ENABLED)
    #include<stats_record.hpp>
#endif
#include<iterable.hpp>
#include<join_archive.hpp>
#include<window_structure.hpp>
#include<basic_emitter.hpp>
#include<basic_operator.hpp>

namespace wf {

//@cond DOXY_IGNORE

// class WJoin_Replica
template<typename join_func_t, typename keyextr_func_t>
class WJoin_Replica: public Basic_Replica
{
private:
    template<typename T1, typename T2> friend class Window_Join;
    join_func_t func; // functional logic used by the Window Join replica
    keyextr_func_t key_extr; // logic to extract the key attribute from the tuple_t
    using tuple_t = decltype(get_tuple_t_Join(func)); // extracting the tuple_t type and checking the admissible signatures
    using result_t = decltype(get_result_t_Join(func)); // extracting the result_t type and checking the admissible signatures
    using key_t = decltype(get_key_t_KeyExtr(key_extr)); // extracting the key_t type and checking the admissible singatures
    // static predicates to check the type of the functional logic to be invoked
    static constexpr bool isNonRiched = std::is_invocable<decltype(func), const tuple_t &, const tuple_t &>::value;
    static constexpr bool isRiched = std::is_invocable<decltype(func), const tuple_t &, const tuple_t &, RuntimeContext &>::value;
    // check the presence of a valid functional logic
    static_assert(isNonRiched || isRiched,
        "WindFlow Compilation Error - WJoin_Replica does not have a valid functional logic:\n");
    using wrapper_t = wrapper_tuple_t<tuple_t>; // alias for the wrapped tuple type
    using container_t = typename std::deque<wrapper_t>; // container type for underlying archive's buffer structure
    using iterator_t = typename container_t::iterator; // iterator type for accessing wrapped tuples in the archive
    using win_t = JoinWindow<key_t>; // window type used by the Window_Replica
    using compare_func_t = std::function<bool(const wrapper_t &, const uint64_t &)>; // function type to compare wrapped tuple to an uint64

    template<typename tuple_t, typename compare_func_t>
    struct Key_Descriptor // struct of a key descriptor
    {
        JoinArchive<tuple_t, compare_func_t> archiveA; // archive of stream A tuples of this key
        JoinArchive<tuple_t, compare_func_t> archiveB; // archive of stream B tuples of this key
        uint64_t partitioning_counter; // counter used in DP mode to establish which replica will save the given tuple
        int64_t last_purged_wm; // last purged wm

        // Constructor
        Key_Descriptor(compare_func_t _compare_func):
                       archiveA(_compare_func),
                       archiveB(_compare_func),
                       last_purged_wm(0),
                       partitioning_counter(0) {}
    };
    using key_d_t = Key_Descriptor<tuple_t, compare_func_t>; // key descriptor type

    compare_func_t compare_func; // function to compare wrapper to an uint64 that rapresents a timestamp or a watermark
    size_t ignored_tuples; // number of ignored tuples
    uint64_t win_len; // window size expressed in time unit
    uint64_t slide_len; // sliding length expressed in time unit
    uint64_t growing_lwid_num; // number of slides to reach full window size
    Join_Window_t join_win_type; // type of the window join
    Join_Mode_t join_mode; // Interval Join operating mode
    std::unordered_map<key_t, key_d_t> keyMap; // hash table that maps a descriptor for each key
    uint64_t last_wm; // last received watermark or timestamp
    size_t id_inner; // id_inner value
    size_t num_inner; // num_inner value

    // Checks if the given Join_Stream_t is Stream A
    bool isStreamA(Join_Stream_t stream) const
    {
        return stream == Join_Stream_t::A;
    }

    // Inserts a wrapper object into the buffer of a given key descriptor
    void insertIntoBuffer(key_d_t &_key_d,
                          wrapper_t _wt,
                          Join_Stream_t stream)
    {
        isStreamA(stream) ? (_key_d.archiveA).insert(_wt) : (_key_d.archiveB).insert(_wt);
    }

    // Purges the keyMap by removing any archived data associated with each key
    void purgeWithPunct()
    {
        for (auto &k: keyMap) {
            key_d_t &key_d = (k.second);
            purgeArchives(key_d, last_wm);
        }
    }

    // Purges the archives of the given key descriptor
    void purgeArchives(key_d_t &_key_d, uint64_t _check_point)
    {
        if (_key_d.last_purged_wm < _check_point) {
            (_key_d.archiveA).purge(_check_point);
            (_key_d.archiveB).purge(_check_point);
            _key_d.last_purged_wm = _check_point;
        }
    }

    // Purges the archives of the given key descriptor based on the last watermark and the first window id
    void purgeFiredWinTuples(key_d_t &_key_d, uint64_t _last_wm, long _first_w) {
        long startWID = 0;
        if (win_len > slide_len) {
            startWID = ceil(((int64_t) _last_wm - (int64_t) win_len + 1) / (double) slide_len);
        }
        else {
            startWID = floor((double)(_last_wm) / slide_len);
        }
        uint64_t purge_wm = startWID < 0 ? 0 : startWID * slide_len;
        purgeArchives(_key_d, purge_wm);
    }

public:
    // Constructor
    WJoin_Replica(join_func_t _func,
                  keyextr_func_t _key_extr,
                  std::string _opName,
                  RuntimeContext _context,
                  std::function<void(RuntimeContext &)> _closing_func,
                  uint64_t _win_len,
                  uint64_t _slide_len,
                  Join_Window_t _join_win_type,
                  Join_Mode_t _join_mode):
                  Basic_Replica(_opName, _context, _closing_func, false),
                  func(_func),
                  key_extr(_key_extr),
                  win_len(_win_len),
                  slide_len(_slide_len),
                  join_win_type(_join_win_type),
                  join_mode(_join_mode),
                  last_wm(0),
                  ignored_tuples(0)
    {
        compare_func = [](const wrapper_t &w1, const uint64_t &_idx) { // comparator function of wrapped tuples
            return w1.index < _idx;
        };
        num_inner = _context.getParallelism();
        id_inner = _context.getReplicaIndex();
        growing_lwid_num = 0;
        if (join_win_type == Join_Window_t::SLIDE && win_len >= slide_len) {
            growing_lwid_num = ceil((double) win_len / slide_len) - 1; // Number of slides to reach full window size
        }
    }

    // Copy Constructor
    WJoin_Replica(const WJoin_Replica &_other):
                  Basic_Replica(_other),
                  func(_other.func),
                  key_extr(_other.key_extr),
                  compare_func(_other.compare_func),
                  win_len(_other.win_len),
                  slide_len(_other.slide_len),
                  growing_lwid_num(_other.growing_lwid_num),
                  join_win_type(_other.join_win_type),
                  join_mode(_other.join_mode),
                  last_wm(_other.last_wm),
                  ignored_tuples(_other.ignored_tuples),
                  id_inner(_other.id_inner),
                  num_inner(_other.num_inner) {}

    // svc (utilized by the FastFlow runtime)
    void *svc(void *_in) override
    {
        this->startStatsRecording();
        if (this->input_batching) { // receiving a batch
            Batch_t<tuple_t> *batch_input = reinterpret_cast<Batch_t<tuple_t> *>(_in);
            if (batch_input->isPunct()) { // if it is a punctuaton
                (this->emitter)->propagate_punctuation(batch_input->getWatermark((this->context).getReplicaIndex()), this); // propagate the received punctuation
                assert(last_wm <= batch_input->getWatermark((this->context).getReplicaIndex())); // sanity check
                last_wm = batch_input->getWatermark((this->context).getReplicaIndex());
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
                assert(last_wm <= input->getWatermark((this->context).getReplicaIndex())); // sanity check
                last_wm = input->getWatermark((this->context).getReplicaIndex());
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
        if (this->execution_mode == Execution_Mode_t::DEFAULT && _timestamp < last_wm) {
#if defined (WF_TRACING_ENABLED)
            stats_record.inputs_ignored++;
#endif
            ignored_tuples++;
            return;
        }
        auto key = key_extr(_tuple); // get the key attribute of the input tuple
        auto it = keyMap.find(key);
        if (it == keyMap.end()) {
            auto p = keyMap.insert(std::make_pair(key, key_d_t(compare_func)));
            it = p.first;
        }
        key_d_t &key_d = (*it).second;
        bool should_store_tuple = false;
        uint64_t ts = _timestamp; // the timestamp of the current tuple
        if (ts < key_d.last_purged_wm) { // if the tuple is related to a closed window of the current key -> IGNORED
#if defined (WF_TRACING_ENABLED)
            stats_record.inputs_ignored++;
#endif
            ignored_tuples++;
            return;
        }
        long first_w = 0; // determine the wid of the first window containing t
        long last_w = -1; // determine the wid of the last window containing t
        if (win_len > slide_len) {
            first_w = ceil(((int64_t) ts - (int64_t) win_len + 1) / (double) slide_len);
            last_w = floor((double) (ts)/slide_len);
        }
        else {
            first_w = floor((double)(ts)/slide_len);
            last_w = first_w;
        }
        uint64_t emit_ts, emit_wm;
        uint64_t win_start, win_end;
        std::optional<result_t> output;
        std::pair<iterator_t, iterator_t> its;
        for (long wid = first_w; wid <= last_w; wid++) {
            win_start = wid < 0 ? 0 : wid * slide_len;
            win_end = wid < 0 ? (wid + (long) growing_lwid_num + 1) * slide_len : win_len;
            win_end += (win_start - 1);
            its = isStreamA(_tag) ? (key_d.archiveB).getJoinRange(win_start, win_end) : (key_d.archiveA).getJoinRange(win_start, win_end);
                Iterable<tuple_t> interval(its.first, its.second);
                for (size_t i=0; i<interval.size(); i++) {
                    if constexpr (isNonRiched) {
                        output = isStreamA(_tag) ? func(_tuple, interval.at(i)) : func(interval.at(i), _tuple);
                    }
                    if constexpr (isRiched)  { // inplace riched version
                        (this->context).setContextParameters(ts, _watermark);
                        output = isStreamA(_tag) ? func(_tuple, interval.at(i), this->context) : func(interval.at(i), _tuple, this->context);
                    }
                    if (output) {
                        if (this->execution_mode == Execution_Mode_t::DETERMINISTIC) {
                            emit_ts = (ts >= interval.index_at(i)) ? ts : interval.index_at(i);
                        }
                        else {
                            emit_ts = win_end;
                        }
                        emit_wm = _watermark;
                        this->doEmit(this->emitter, &(*output), 0, emit_ts, emit_wm, this); // emit the pair
#if defined (WF_TRACING_ENABLED)
                        (this->stats_record).outputs_sent++;
                        (this->stats_record).bytes_sent += sizeof(result_t);
#endif
                    }
                }
        }
        if (join_mode == Join_Mode_t::KP) { // KP
            should_store_tuple = true;
        }
        else { // DP
            key_d.partitioning_counter++;
            if (key_d.partitioning_counter % num_inner == id_inner) {
                should_store_tuple = true;
            }
        }
        if (should_store_tuple) {
            insertIntoBuffer(key_d, wrapper_t(_tuple, _timestamp), _tag);
        }
        if (this->execution_mode == Execution_Mode_t::DEFAULT) {
            assert(last_wm <= _watermark); // sanity check
            if (last_wm < _watermark) {
                last_wm = _watermark;
                purgeFiredWinTuples(key_d, last_wm, first_w); // purge the archives using the new watermark
            }
        }
        else {
            if (last_wm < _timestamp) {
                last_wm = _timestamp;
                purgeFiredWinTuples(key_d, last_wm, first_w); // purge the archives using the new timestamp
            }
        }
    }

    // Get the number of ignored tuples
    size_t getNumIgnoredTuples() const
    {
        return ignored_tuples;
    }

    WJoin_Replica(WJoin_Replica &&) = delete; ///< Move constructor is deleted
    WJoin_Replica &operator=(const WJoin_Replica &) = delete; ///< Copy assignment operator is deleted
    WJoin_Replica &operator=(WJoin_Replica &&) = delete; ///< Move assignment operator is deleted
};

//@endcond

/** 
 *  \class Window Join
 *  
 *  \brief Window Join operator
 *  
 *  The Window Join operator performs a join operation over two streams based on a specified window size and sliding length.
 *  It takes a functional Boolean condition logic and a key extractor logic as input. The operator operates in
 *  either Key-Parallelism (KP) or Data-Parallelism (DP) mode.
 */ 
template<typename join_func_t, typename keyextr_func_t>
class Window_Join: public Basic_Operator
{
private:
    friend class MultiPipe;
    friend class PipeGraph;
    join_func_t func; // functional boolean condition logic used by the Window Join
    keyextr_func_t key_extr; // logic to extract the key attribute from the tuple_t
    std::vector<WJoin_Replica<join_func_t, keyextr_func_t>*> replicas; // vector of pointers to the replicas of the Window Join
    uint64_t win_size; // window size expressed in usec
    uint64_t slide_len; // sliding length expressed in usec
    Join_Window_t join_win_type; // type of the join windows
    Join_Mode_t join_mode; // Window Join operating mode
    using tuple_t = decltype(get_tuple_t_Join(func)); // extracting the tuple_t type and checking the admissible signatures
    using result_t = decltype(get_result_t_Join(func)); // extracting the result_t type and checking the admissible signatures
    using key_t = decltype(get_key_t_KeyExtr(key_extr)); // extracting the key_t type and checking the admissible singatures
    static constexpr op_type_t op_type = op_type_t::BASIC;

    // Configure the Window Join to receive batches instead of individual inputs
    void receiveBatches(bool _input_batching) override
    {
        for (auto *r: replicas) {
            r->receiveBatches(_input_batching);
        }
    }

    // Set the emitter used to route outputs from the Window Join
    void setEmitter(Basic_Emitter *_emitter) override
    {
        replicas[0]->setEmitter(_emitter);
        for (size_t i=1; i<replicas.size(); i++) {
            replicas[i]->setEmitter(_emitter->clone());
        }
    }

    // Check whether the Window Join has terminated
    bool isTerminated() const override
    {
        bool terminated = true;
        for(auto *r: replicas) { // scan all the replicas to check their termination
            terminated = terminated && r->isTerminated();
        }
        return terminated;
    }

    // Set the execution mode of the Window Join
    void setExecutionMode(Execution_Mode_t _execution_mode)
    {
        if (this->getOutputBatchSize() > 0 && _execution_mode != Execution_Mode_t::DEFAULT) {
            std::cerr << RED << "WindFlow Error: Window Join is trying to produce a batch in non DEFAULT mode" << DEFAULT_COLOR << std::endl;
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
    // Append the statistics (JSON format) of the Window Join to a PrettyWriter
    void appendStats(rapidjson::PrettyWriter<rapidjson::StringBuffer> &writer) const override
    {
        writer.StartObject(); // create the header of the JSON file
        writer.Key("Operator_name");
        writer.String((this->name).c_str());
        writer.Key("Operator_type");
        writer.String("Window_Join");
        writer.Key("Distribution");
        if (this->getInputRoutingMode() == Routing_Mode_t::KEYBY) {
            writer.String("KEYBY");
        }
        else {
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
        writer.Key("Window_Size");
        writer.Uint(this->win_size);
        writer.Key("Sliding_Length");
        writer.Uint(this->slide_len);
        writer.Key("Join_Mode");
        if (this->join_mode == Join_Mode_t::KP) {
            writer.String("Key-Parallelism");
        }
        else {
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
     *  \param _func functional Boolean condition logic of the Window Join (a function or any callable type)
     *  \param _key_extr key extractor (a function or any callable type)
     *  \param _parallelism internal parallelism of the Window Join
     *  \param _name name of the Window Join
     *  \param _input_routing_mode input routing mode of the Window Join
     *  \param _outputBatchSize size (in num of tuples) of the batches produced by this operator (0 for no batching)
     *  \param _closing_func closing functional logic of the Window Join (a function or any callable type)
     *  \param _win_size window size expressed in time unit
     *  \param _slide sliding length expressed in time unit
     *  \param _join_mode Window Join operating mode
     */ 
    Window_Join(join_func_t _func,
                  keyextr_func_t _key_extr,
                  size_t _parallelism,
                  std::string _name,
                  Routing_Mode_t _input_routing_mode,
                  size_t _outputBatchSize,
                  std::function<void(RuntimeContext &)> _closing_func,
                  uint64_t _win_size,
                  uint64_t _slide_len,
                  Join_Window_t _join_win_type,
                  Join_Mode_t _join_mode):
                  Basic_Operator(_parallelism, _name, _input_routing_mode, _outputBatchSize),
                  func(_func),
                  key_extr(_key_extr),
                  win_size(_win_size),
                  slide_len(_slide_len),
                  join_win_type(_join_win_type),
                  join_mode(_join_mode)
    {
        for (size_t i=0; i<this->parallelism; i++) { // create the internal replicas of the Interval Join
            replicas.push_back(new WJoin_Replica<join_func_t, keyextr_func_t>(this->func,
                                                                              this->key_extr,
                                                                              this->name,
                                                                              RuntimeContext(this->parallelism, i),
                                                                              _closing_func,
                                                                              this->win_size,
                                                                              this->slide_len,
                                                                              this->join_win_type,
                                                                              this->join_mode));
        }
    }

    /// Copy constructor
    Window_Join(const Window_Join &_other):
                  Basic_Operator(_other),
                  func(_other.func),
                  key_extr(_other.key_extr),
                  win_size(_other.win_size),
                  slide_len(_other.slide_len),
                  join_win_type(_other.join_win_type),
                  join_mode(_other.join_mode)
    {
        for (size_t i=0; i<this->parallelism; i++) { // deep copy of the pointers to the Interval Join replicas
            replicas.push_back(new WJoin_Replica<join_func_t, keyextr_func_t>(*(_other.replicas[i])));
        }
    }

    // Destructor
    ~Window_Join() override
    {
        for (auto *r: replicas) { // delete all the replicas
            delete r;
        }
    }

    /** 
     *  \brief Get the type of the Window Join as a string
     *  \return type of the Window Join
     */ 
    std::string getType() const override
    {
        std::string join_mode_str = "Window_Join_";
        switch (join_mode) {
            case Join_Mode_t::KP:
                join_mode_str += "KP";
                break;
            case Join_Mode_t::DP:
                join_mode_str += "DP";
                break;
        }
        return join_mode_str;
    }

    Window_Join(Window_Join &&) = delete; ///< Move constructor is deleted
    Window_Join &operator=(const Window_Join &) = delete; ///< Copy assignment operator is deleted
    Window_Join &operator=(Window_Join &&) = delete; ///< Move assignment operator is deleted
};

} // namespace wf

#endif
