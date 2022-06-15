/**************************************************************************************
 *  Copyright (c) 2019- Gabriele Mencagli and Elia Ruggeri
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
 *  @file    ffat_replica.hpp
 *  @author  Gabriele Mencagli and Elia Ruggeri
 *  
 *  @brief FFAT_Replica implements the replica of the FFAT_Aggregator
 *  
 *  @section FFAT_Replica (Description)
 *  
 *  This file implements the FFAT_Replica representing the replica of the FFAT_Aggregator
 *  operator.
 */ 

#ifndef FFAT_REPLICA_H
#define FFAT_REPLICA_H

// includes
#include<cmath>
#include<string>
#include<functional>
#include<unordered_map>
#include<ff/multinode.hpp>
#include<flatfat.hpp>
#include<context.hpp>
#include<batch_t.hpp>
#include<single_t.hpp>
#if defined (WF_TRACING_ENABLED)
    #include<stats_record.hpp>
#endif
#include<basic_emitter.hpp>
#include<basic_operator.hpp>
#include<stream_archive.hpp>

namespace wf {

// class FFAT_Replica
template<typename lift_func_t, typename comb_func_t, typename key_extractor_func_t>
class FFAT_Replica: public ff::ff_monode
{
private:
    template<typename T1, typename T2, typename T3> friend class FFAT_Aggregator; // friendship with the FFAT_Aggregator class
    lift_func_t lift_func; // functional logic of the lift
    comb_func_t comb_func; // functional logic of the combine
    key_extractor_func_t key_extr; // logic to extract the key attribute from the tuple_t
    using tuple_t = decltype(get_tuple_t_Lift(lift_func)); // extracting the tuple_t type and checking the admissible signatures
    using result_t = decltype(get_result_t_Lift(lift_func)); // extracting the result_t type and checking the admissible signatures
    using key_t = decltype(get_key_t_KeyExtr(key_extr)); // extracting the key_t type and checking the admissible singatures
    using fat_t = FlatFAT<comb_func_t, key_t>; // type of the FlatFAT
    // static predicates to check the type of the functional logic to be invoked
    static constexpr bool isNonRichedLift = std::is_invocable<decltype(lift_func), const tuple_t &, result_t &>::value;
    static constexpr bool isRichedLift = std::is_invocable<decltype(lift_func), const tuple_t &, result_t &, RuntimeContext &>::value;
    static constexpr bool isNonRichedComb = std::is_invocable<decltype(comb_func), const result_t &, const result_t &, result_t &>::value;
    static constexpr bool isRichedComb = std::is_invocable<decltype(comb_func), const result_t &, const result_t &, result_t &, RuntimeContext &>::value;
    // check the presence of a valid functional logic
    static_assert(isNonRichedLift || isRichedLift || isNonRichedComb || isRichedComb,
        "WindFlow Compilation Error - FFAT_Replica does not have a valid functional logic:\n");
    struct Key_Descriptor // struct of a key descriptor
    {
        fat_t fat; // FlatFAT of this key
        std::vector<result_t> pending_tuples; // vector of pending tuples of this key
        std::deque<result_t> acc_results; // deque of acculumated results
        uint64_t last_quantum; // identifier of the last quantum
        uint64_t rcv_counter; // number of tuples received of this key
        uint64_t slide_counter; // counter of the tuples in the last slide
        uint64_t ts_rcv_counter; // counter of received tuples (count-based translation)
        uint64_t next_lwid; // next window to be opened of this key (lwid)
        uint64_t next_input_id; // identifier of the next tuple of this key

        // Constructor
        Key_Descriptor(comb_func_t *_comb_func,
                       key_t _key,
                       size_t _win_len,
                       RuntimeContext *_context):
                       fat(_comb_func, _key, false /* not commutative by default */, _win_len, _context),
                       last_quantum(0),
                       rcv_counter(0),
                       slide_counter(0),
                       ts_rcv_counter(0),
                       next_lwid(0),
                       next_input_id(0) {}
    };
    std::string opName; // name of the FFAT_Aggregator containing the replica
    bool input_batching; // if true, the FFAT_Replica expects to receive batches instead of individual inputs
    RuntimeContext context; // RuntimeContext object
    std::function<void(RuntimeContext &)> closing_func; // closing functional logic used by the FFAT_Replica
    bool terminated; // true if the replica has finished its work
    Basic_Emitter *emitter; // pointer to the used emitter
    uint64_t win_len; // window length (no. of tuples or in time units)
    uint64_t slide_len; // slide length (no. of tuples or in time units)
    uint64_t lateness; // triggering delay in time units (meaningful for TB windows in DEFAULT mode)
    Win_Type_t winType; // window type (CB or TB)
    uint64_t quantum; // quantum value (for time-based windows only)
    std::unordered_map<key_t, Key_Descriptor> keyMap; // hash table that maps a descriptor for each key
    size_t ignored_tuples; // number of ignored tuples
    Execution_Mode_t execution_mode; // execution mode of the FFAT_Aggregator replica
    uint64_t last_time; // last received timestamp or watermark
#if defined (WF_TRACING_ENABLED)
    Stats_Record stats_record;
    double avg_td_us = 0;
    double avg_ts_us = 0;
    volatile uint64_t startTD, startTS, endTD, endTS;
#endif

public:
    // Constructor
    FFAT_Replica(lift_func_t _lift_func,
                 comb_func_t _comb_func,
                 key_extractor_func_t _key_extr,
                 std::string _opName,
                 RuntimeContext _context,
                 std::function<void(RuntimeContext &)> _closing_func,
                 uint64_t _win_len,
                 uint64_t _slide_len,
                 uint64_t _lateness,
                 Win_Type_t _winType):
                 lift_func(_lift_func),
                 comb_func(_comb_func),
                 key_extr(_key_extr),
                 opName(_opName),
                 input_batching(false),
                 context(_context),
                 closing_func(_closing_func),
                 terminated(false),
                 emitter(nullptr),
                 win_len(_win_len),
                 slide_len(_slide_len),
                 lateness(_lateness),
                 winType(_winType),
                 ignored_tuples(0),
                 execution_mode(Execution_Mode_t::DEFAULT),
                 last_time(0)
    {
        if (winType == Win_Type_t::TB) { // set the quantum value (for time-based windows only)
            quantum = compute_gcd(win_len, slide_len);
            win_len = win_len / quantum;
            slide_len = slide_len / quantum;
        }
        else {
            quantum = 0; // zero, quantum is never used
        }        
    }

    // Copy Constructor
    FFAT_Replica(const FFAT_Replica &_other):
                 lift_func(_other.lift_func),
                 comb_func(_other.comb_func),
                 key_extr(_other.key_extr),
                 opName(_other.opName),
                 input_batching(_other.input_batching),
                 context(_other.context),
                 closing_func(_other.closing_func),
                 terminated(_other.terminated),
                 win_len(_other.win_len),
                 slide_len(_other.slide_len),
                 lateness(_other.lateness),
                 winType(_other.winType),
                 quantum(_other.quantum),
                 keyMap(_other.keyMap),
                 ignored_tuples(_other.ignored_tuples),
                 execution_mode(_other.execution_mode),
                 last_time(_other.last_time)
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
    FFAT_Replica(FFAT_Replica &&_other):
                 lift_func(std::move(_other.lift_func)),
                 comb_func(std::move(_other.comb_func)),
                 key_extr(std::move(_other.key_extr)),
                 opName(std::move(_other.opName)),
                 input_batching(_other.input_batching),
                 context(std::move(_other.context)),
                 closing_func(std::move(_other.closing_func)),
                 terminated(_other.terminated),
                 emitter(std::exchange(_other.emitter, nullptr)),
                 win_len(_other.win_len),
                 slide_len(_other.slide_len),
                 lateness(_other.lateness),
                 winType(_other.winType),
                 quantum(_other.quantum),
                 keyMap(std::move(_other.keyMap)),
                 ignored_tuples(_other.ignored_tuples),
                 execution_mode(_other.execution_mode),
                 last_time(_other.last_time)
    {
#if defined (WF_TRACING_ENABLED)
        stats_record = std::move(_other.stats_record);
#endif
    }

    // Destructor
    ~FFAT_Replica()
    {
        if (emitter != nullptr) {
            delete emitter;
        }
    }

    // Copy Assignment Operator
    FFAT_Replica &operator=(const FFAT_Replica &_other)
    {
        if (this != &_other) {
            lift_func = _other.lift_func;
            comb_func = _other.comb_func;
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
            win_len = _other.win_len;
            slide_len = _other.slide_len;
            lateness = _other.lateness;
            winType = _other.winType;
            quantum = _other.quantum;
            keyMap = _other.keyMap;
            ignored_tuples = _other.ignored_tuples;
            execution_mode = _other.execution_mode;
            last_time = _other.last_time; 
#if defined (WF_TRACING_ENABLED)
            stats_record = _other.stats_record;
#endif
        }
        return *this;
    }

    // Move Assignment Operator
    FFAT_Replica &operator=(FFAT_Replica &_other)
    {
        lift_func = std::move(_other.lift_func);
        comb_func = std::move(_other.comb_func);
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
        win_len = _other.win_len;
        slide_len = _other.slide_len;
        lateness = _other.lateness;
        winType = _other.winType;
        quantum = _other.quantum;
        keyMap = std::move(_other.keyMap);
        ignored_tuples = _other.ignored_tuples;
        execution_mode = _other.execution_mode;
        last_time = _other.last_time; 
#if defined (WF_TRACING_ENABLED)
        stats_record = std::move(_other.stats_record);
#endif
        return *this;
    }

    // svc_init method (utilized by the FastFlow runtime)
    int svc_init() override
    {
#if defined (WF_TRACING_ENABLED)
        stats_record = Stats_Record(opName, std::to_string(this->get_my_id()), true, false);
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
            Batch_t<decltype(get_tuple_t_Lift(lift_func))> *batch_input = reinterpret_cast<Batch_t<decltype(get_tuple_t_Lift(lift_func))> *>(_in);
            if (batch_input->isPunct()) { // if it is a punctuaton
                emitter->propagate_punctuation(batch_input->getWatermark(context.getReplicaIndex()), this); // propagate the received punctuation
                assert(last_time <= batch_input->getWatermark(context.getReplicaIndex())); // sanity check
                last_time = batch_input->getWatermark(context.getReplicaIndex());
                deleteBatch_t(batch_input); // delete the punctuation
                return this->GO_ON;
            }
#if defined (WF_TRACING_ENABLED)
            stats_record.inputs_received += batch_input->getSize();
            stats_record.bytes_received += batch_input->getSize() * sizeof(tuple_t);
#endif
            for (size_t i=0; i<batch_input->getSize(); i++) { // process all the inputs within the received batch
                if (winType == Win_Type_t::CB) { // count-based windows
                    process_input_cb(batch_input->getTupleAtPos(i), batch_input->getTimestampAtPos(i), batch_input->getWatermark(context.getReplicaIndex()));
                }
                else { // time-based windows
                    process_input_tb(batch_input->getTupleAtPos(i), batch_input->getTimestampAtPos(i), batch_input->getWatermark(context.getReplicaIndex()));
                }
            }
            deleteBatch_t(batch_input); // delete the input batch
        }
        else { // receiving a single input
            Single_t<decltype(get_tuple_t_Lift(lift_func))> *input = reinterpret_cast<Single_t<decltype(get_tuple_t_Lift(lift_func))> *>(_in);
            if (input->isPunct()) { // if it is a punctuaton
                emitter->propagate_punctuation(input->getWatermark(context.getReplicaIndex()), this); // propagate the received punctuation
                assert(last_time <= input->getWatermark(context.getReplicaIndex())); // sanity check
                last_time = input->getWatermark(context.getReplicaIndex());
                deleteSingle_t(input); // delete the punctuation
                return this->GO_ON;
            }
#if defined (WF_TRACING_ENABLED)
            stats_record.inputs_received++;
            stats_record.bytes_received += sizeof(tuple_t);
#endif
            if (winType == Win_Type_t::CB) { // count-based windows
                process_input_cb(input->tuple, input->getTimestamp(), input->getWatermark(context.getReplicaIndex()));
            }
            else { // time-based windows
                process_input_tb(input->tuple, input->getTimestamp(), input->getWatermark(context.getReplicaIndex()));
            }
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

    // Process a single input (count-based windows)
    void process_input_cb(tuple_t &_tuple,
                          uint64_t _timestamp,
                          uint64_t _watermark)
    {
        if (execution_mode == Execution_Mode_t::DEFAULT) {
            assert(last_time <= _watermark); // sanity check
            last_time = _watermark;
        }
        else {
            assert(last_time <= _timestamp); // sanity check
            last_time = _timestamp;
        }
        auto key = key_extr(_tuple); // get the key attribute of the input tuple
        auto it = keyMap.find(key); // find the corresponding key_descriptor (or allocate it if does not exist)
        if (it == keyMap.end()) {
            auto p = keyMap.insert(std::make_pair(key, Key_Descriptor(&comb_func, key, win_len, &context))); // create the state of the key
            it = p.first;
        }
        Key_Descriptor &key_d = (*it).second;
        key_d.next_input_id++; // set the progressive identifier of the tuple (per key basis)
        key_d.rcv_counter++;
        key_d.slide_counter++;
        result_t res = create_win_result_t<decltype(get_result_t_Lift(lift_func)), decltype(key)>(key);
        if constexpr (isRichedLift || isRichedComb) {
            context.setContextParameters(_timestamp, _watermark); // set the parameter of the RuntimeContext
        }
        if constexpr (isNonRichedLift) {
            lift_func(_tuple, res);
        }
        if constexpr (isRichedLift) {
            lift_func(_tuple, res, context);
        }
        (key_d.pending_tuples).push_back(res);
        // check whether the current window has been fired
        bool fired = false;
        uint64_t gwid;
        if (key_d.rcv_counter == win_len) { // first window when it is complete
            fired = true;
            uint64_t lwid = key_d.next_lwid;
            gwid = lwid;
            key_d.next_lwid++;
            key_d.slide_counter = 0;
        }
        else if ((key_d.rcv_counter > win_len) && (key_d.slide_counter % slide_len == 0)) { // other windows when the slide is complete
            fired = true;
            uint64_t lwid = key_d.next_lwid;
            gwid = lwid;
            key_d.next_lwid++;
            key_d.slide_counter = 0;
        }
        if (fired) { // if a window has been fired
            (key_d.fat).insert(key_d.pending_tuples); // add all the pending tuples to the FlatFAT
            (key_d.pending_tuples).clear(); // clear the vector of pending tuples
            result_t out = ((key_d.fat).getResult(gwid)); // get a copy of the result of the fired window
            (key_d.fat).remove(slide_len); // purge the tuples in the last slide from FlatFAT
            uint64_t used_ts = (execution_mode != Execution_Mode_t::DEFAULT) ? _timestamp : _watermark;
            uint64_t used_wm = (execution_mode != Execution_Mode_t::DEFAULT) ? 0 : _watermark;
            emitter->emit(&out, 0, used_ts, used_wm, this);
#if defined (WF_TRACING_ENABLED)
            stats_record.outputs_sent++;
            stats_record.bytes_sent += sizeof(result_t);
#endif
        }
    }

    // Process a single input (time-based windows)
    void process_input_tb(tuple_t &_tuple,
                          uint64_t _timestamp,
                          uint64_t _watermark)
    {
        if (execution_mode == Execution_Mode_t::DEFAULT) {
            assert(last_time <= _watermark); // sanity check
            last_time = _watermark;
        }
        else {
            assert(last_time <= _timestamp); // sanity check
            last_time = _timestamp;
        }
        auto key = key_extr(_tuple); // get the key attribute of the input tuple
        auto it = keyMap.find(key); // find the corresponding key_descriptor (or allocate it if does not exist)
        if (it == keyMap.end()) {
            auto p = keyMap.insert(std::make_pair(key, Key_Descriptor(&comb_func, key, win_len, &context))); // create the state of the key
            it = p.first;
        }
        Key_Descriptor &key_d = (*it).second;
        key_d.next_input_id++; // set the progressive identifier of the tuple (per key basis)
        uint64_t quantum_id = _timestamp / quantum; // compute the identifier of the quantum containing the input tuple
        // check if the tuple must be ignored
        if (quantum_id < key_d.last_quantum) {
#if defined (WF_TRACING_ENABLED)
            stats_record.inputs_ignored++;
#endif
            ignored_tuples++;
            return;
        }
        key_d.rcv_counter++;
        auto &acc_results = key_d.acc_results;
        int64_t distance = quantum_id - key_d.last_quantum;
        for (size_t i=acc_results.size(); i<=distance; i++) { // resize acc_results properly
            result_t r = create_win_result_t<decltype(get_result_t_Lift(lift_func)), decltype(key)>(key);
            acc_results.push_back(r);
        }
        result_t tmp = create_win_result_t<decltype(get_result_t_Lift(lift_func)), decltype(key)>(key);
        if constexpr (isRichedLift || isRichedComb) {
            context.setContextParameters(_timestamp, _watermark); // set the parameter of the RuntimeContext
        }
        if constexpr (isNonRichedLift) {
            lift_func(_tuple, tmp);
        }
        if constexpr (isRichedLift) {
            lift_func(_tuple, tmp, context);
        }
        size_t id = quantum_id - key_d.last_quantum; // compute the identifier of the corresponding quantum
        result_t tmp2 = create_win_result_t<decltype(get_result_t_Lift(lift_func)), decltype(key)>(key);
        if constexpr (isNonRichedComb) {
            comb_func(acc_results[id], tmp, tmp2);
        }
        if constexpr (isRichedComb) {
            comb_func(acc_results[id], tmp, tmp2, context);
        }
        acc_results[id] = tmp2;
        size_t n_completed = 0;
        for (size_t i=0; i<acc_results.size(); i++) { // check whether there are complete quantums by taking into account the lateness
            uint64_t final_ts = ((key_d.last_quantum+i+1) * quantum)-1;
            if (final_ts + lateness < _watermark) {
                n_completed++;
                processCompleteTBWindows(key_d, acc_results[i], key, _timestamp, _watermark);
                key_d.last_quantum++;
            }
            else {
                break;
            }
        }
        acc_results.erase(acc_results.begin(), acc_results.begin() + n_completed); // remove the accumulated results of all the complete quantums
    }

    // process a completed time-based window
    void processCompleteTBWindows(Key_Descriptor &key_d,
                                  result_t &r,
                                  key_t key,
                                  uint64_t _timestamp,
                                  uint64_t _watermark)
    {
        (key_d.pending_tuples).push_back(r);
        key_d.ts_rcv_counter++;
        key_d.slide_counter++;
        // check whether the current window has been fired
        bool fired = false;
        uint64_t gwid;
        if (key_d.ts_rcv_counter == win_len) { // first window when it is complete
            fired = true;
            uint64_t lwid = key_d.next_lwid;
            gwid = lwid;
            key_d.next_lwid++;
            key_d.slide_counter = 0;
        }
        else if ((key_d.ts_rcv_counter > win_len) && (key_d.slide_counter % slide_len == 0)) { // other windows when the slide is complete
            fired = true;
            uint64_t lwid = key_d.next_lwid;
            gwid = lwid;
            key_d.next_lwid++;
            key_d.slide_counter = 0;
        }
        if (fired) { // if a window has been fired
            (key_d.fat).insert(key_d.pending_tuples); // add all the pending tuples to the FlatFAT
            (key_d.pending_tuples).clear(); // clear the vector of pending tuples
            result_t out = ((key_d.fat).getResult(gwid)); // get a copy of the result of the fired window
            (key_d.fat).remove(slide_len); // purge the tuples in the last slide from FlatFAT
            uint64_t used_ts = (execution_mode != Execution_Mode_t::DEFAULT) ? _timestamp : _watermark;
            uint64_t used_wm = (execution_mode != Execution_Mode_t::DEFAULT) ? 0 : _watermark;
            emitter->emit(&out, 0, used_ts, used_wm, this);
#if defined (WF_TRACING_ENABLED)
            stats_record.outputs_sent++;
            stats_record.bytes_sent += sizeof(result_t);
#endif
        }
    }

    // method to manage the EOS (utilized by the FastFlow runtime)
    void eosnotify(ssize_t id) override
    {
        if (winType == Win_Type_t::CB) { // count-based eos logic
            eosnotifyCBWindows(id);
        }
        else {
            eosnotifyTBWindows(id); // count-based eos logic
        }
        emitter->flush(this); // call the flush of the emitter
        terminated = true;
#if defined (WF_TRACING_ENABLED)
        stats_record.setTerminated();
#endif
    }

    // Eosnotify with count-based windows
    void eosnotifyCBWindows(ssize_t id)
    {
        for (auto &k: keyMap) { // iterate over all the keys
            auto key = k.first; 
            auto &key_d = k.second;
            auto &fat = key_d.fat;
            fat.insert(key_d.pending_tuples); // add all the pending tuples to the FlatFAT
            while (!fat.is_Empty()) { // iterate over all the existing windows of the key
                uint64_t lwid = key_d.next_lwid;
                uint64_t gwid = lwid;
                key_d.next_lwid++;
                result_t out = fat.getResult(gwid); // get a copy of the result of the fired window
                fat.remove(slide_len); // purge the tuples in the last slide from FlatFAT
                uint64_t used_wm = (execution_mode != Execution_Mode_t::DEFAULT) ? 0 : last_time;
                emitter->emit(&out, 0, last_time, used_wm, this);
#if defined (WF_TRACING_ENABLED)
                stats_record.outputs_sent++;
                stats_record.bytes_sent += sizeof(result_t);
#endif
            }
        }
    }

    // Eosnotify with time-based windows
    void eosnotifyTBWindows(ssize_t id)
    {
        // iterate over all the keys
        for (auto &k: keyMap) { // iterate over all the keys
            auto key = k.first;
            auto &key_d = k.second;
            auto &fat = key_d.fat;
            auto &acc_results = key_d.acc_results;
            for (size_t i=0; i<acc_results.size(); i++) { // add all the accumulated results
                uint64_t used_wm = (execution_mode != Execution_Mode_t::DEFAULT) ? 0 : last_time;
                processCompleteTBWindows(key_d, acc_results[i], key, last_time, used_wm);
                key_d.last_quantum++;
            }
            fat.insert(key_d.pending_tuples); // add all the pending tuples to the FlatFAT
            while (!fat.is_Empty()) { // loop until the FlatFAT is empty
                uint64_t lwid = key_d.next_lwid;
                uint64_t gwid = lwid;
                key_d.next_lwid++;
                result_t out = fat.getResult(gwid); // get a copy the result of the fired window
                fat.remove(slide_len); // purge the tuples from Flat FAT
                uint64_t used_wm = (execution_mode != Execution_Mode_t::DEFAULT) ? 0 : last_time;
                emitter->emit(&out, 0, last_time, used_wm, this);
#if defined (WF_TRACING_ENABLED)
                stats_record.outputs_sent++;
                stats_record.bytes_sent += sizeof(result_t);
#endif
            }
        }
    }

    // svc_end method (utilized by the FastFlow runtime)
    void svc_end() override
    {
        closing_func(context); // call the closing function
    }

    // Configure the Window_Replica to receive batches instead of individual inputs
    void receiveBatches(bool _input_batching)
    {
        input_batching = _input_batching;
    }

    // Set the emitter used to route outputs from the FFAT_Aggregator replica
    void setEmitter(Basic_Emitter *_emitter)
    {
        emitter = _emitter;
    }

    // Check the termination of the Window_Replica
    bool isTerminated() const
    {
        return terminated;
    }

    // Set the execution mode of the FFAT_Aggregator replica
    void setExecutionMode(Execution_Mode_t _execution_mode)
    {
        execution_mode = _execution_mode;
    }

    // Get the number of ignored tuples
    size_t getNumIgnoredTuples() const
    {
        return ignored_tuples;
    }

#if defined (WF_TRACING_ENABLED)
    // Get a copy of the Stats_Record of the FFAT_Replica
    Stats_Record getStatsRecord() const
    {
        return stats_record;
    }
#endif
};

} // namespace wf

#endif
