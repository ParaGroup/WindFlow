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
 *  @file    window_replica.hpp
 *  @author  Gabriele Mencagli
 *  
 *  @brief Window_Replica is the replica of most of the window-based operators
 *  
 *  @section Window_Replica (Description)
 *  
 *  This file implements the Window_Replica representing the replica of most of the
 *  window-based operators (i.e., Keyed_Windows, Parallel_Windows, Paned_Windows and
 *  MapReduce_Windows).
 */ 

#ifndef WIN_REPLICA_H
#define WIN_REPLICA_H

// includes
#include<cmath>
#include<string>
#include<functional>
#include<unordered_map>
#include<context.hpp>
#include<batch_t.hpp>
#include<single_t.hpp>
#include<iterable.hpp>
#if defined (WF_TRACING_ENABLED)
    #include<stats_record.hpp>
#endif
#include<basic_emitter.hpp>
#include<basic_operator.hpp>
#include<stream_archive.hpp>
#include<window_structure.hpp>

namespace wf {

// class Window_Replica
template<typename win_func_t, typename keyextr_func_t>
class Window_Replica: public Basic_Replica
{
private:
    template<typename T1, typename T2> friend class Keyed_Windows;
    template<typename T1, typename T2> friend class Parallel_Windows;
    win_func_t func; // functional logic used by the Window_Replica
    keyextr_func_t key_extr; // logic to extract the key attribute from the tuple_t
    using tuple_t = decltype(get_tuple_t_Win(func)); // extracting the tuple_t type and checking the admissible signatures
    using result_t = decltype(get_result_t_Win(func)); // extracting the result_t type and checking the admissible signatures
    using key_t = decltype(get_key_t_KeyExtr(key_extr)); // extracting the key_t type and checking the admissible singatures
    // static predicates to check the type of the functional logic to be invoked
    static constexpr bool isNonIncNonRiched = std::is_invocable<decltype(func), const Iterable<tuple_t> &, result_t &>::value;
    static constexpr bool isNonIncRiched = std::is_invocable<decltype(func), const Iterable<tuple_t> &, result_t &, RuntimeContext &>::value;
    static constexpr bool isIncNonRiched = std::is_invocable<decltype(func), const tuple_t &, result_t &>::value;
    static constexpr bool isIncRiched = std::is_invocable<decltype(func), const tuple_t &, result_t &, RuntimeContext &>::value;
    // check the presence of a valid functional logic
    static_assert(isNonIncNonRiched || isNonIncRiched || isIncNonRiched || isIncRiched,
        "WindFlow Compilation Error - Window_Replica does not have a valid functional logic:\n");
    using wrapper_t = wrapper_tuple_t<tuple_t>; // alias for the wrapped tuple type
    using input_iterator_t = typename std::deque<wrapper_t>::iterator; // iterator type for accessing wrapped tuples in the archive
    using win_t = Window<tuple_t, result_t, key_t>; // window type used by the Window_Replica
    using compare_func_t = std::function<bool(const wrapper_t &, const wrapper_t &)>; // function type to compare two wrapped tuples

    struct Key_Descriptor // struct of a key descriptor
    {
        StreamArchive<tuple_t> archive; // archive of tuples of this key
        std::vector<win_t> wins; // open windows of this key
        uint64_t next_res_id; // identifier of the next result of this key (used if role is PLQ or MAP)
        uint64_t next_lwid; // next window to be opened of this key (lwid)
        int64_t last_lwid; // last window closed of this key (lwid)
        uint64_t next_input_id; // identifier of the next tuple of this key

        // Constructor
        Key_Descriptor(compare_func_t _compare_func,
                       uint64_t _next_res_id=0):
                       archive(_compare_func),
                       next_res_id(_next_res_id),
                       next_lwid(0),
                       last_lwid(-1),
                       next_input_id(0)
        {
            wins.reserve(WF_DEFAULT_VECTOR_CAPACITY);
        }
    };

    compare_func_t compare_func; // function to compare two wrapped tuples
    uint64_t win_len; // window length (in no. of tuples or in time units)
    uint64_t slide_len; // slide length (in no. of tuples or in time units)
    uint64_t lateness; // triggering delay in time units (meaningful for TB windows in DEFAULT mode)
    Win_Type_t winType; // window type (CB or TB)
    role_t role; // role of the Window_Replica
    std::unordered_map<key_t, Key_Descriptor> keyMap; // hash table that maps a descriptor for each key
    size_t id_inner; // id_inner value
    size_t num_inner; // num_inner value
    std::pair<size_t, size_t> map_indexes; // indexes useful is the role is MAP
    size_t ignored_tuples; // number of ignored tuples
    uint64_t last_time; // last received timestamp or watermark

public:
    // Constructor
    Window_Replica(win_func_t _func,
                   keyextr_func_t _key_extr,
                   std::string _opName,
                   RuntimeContext _context,
                   std::function<void(RuntimeContext &)> _closing_func,
                   uint64_t _win_len,
                   uint64_t _slide_len,
                   uint64_t _lateness,
                   Win_Type_t _winType,
                   role_t _role,
                   size_t _id_inner,
                   size_t _num_inner):
                   Basic_Replica(_opName, _context, _closing_func, true),
                   func(_func),
                   key_extr(_key_extr),
                   win_len(_win_len),
                   slide_len(_slide_len),
                   lateness(_lateness),
                   winType(_winType),
                   role(_role),
                   id_inner(_id_inner),
                   num_inner(_num_inner),
                   map_indexes(std::make_pair(0, 1)),
                   ignored_tuples(0),
                   last_time(0)
    {
        compare_func = [](const wrapper_t &w1, const wrapper_t &w2) { // comparator function of wrapped tuples
            return w1.index < w2.index;
        };
        if (role == role_t::MAP) { // set the map_indexes if role is MAP
            map_indexes.first = context.getReplicaIndex();
            map_indexes.second = context.getParallelism();
        }
    }

    // Copy Constructor
    Window_Replica(const Window_Replica &_other):
                   Basic_Replica(_other),
                   func(_other.func),
                   key_extr(_other.key_extr),
                   compare_func(_other.compare_func),
                   win_len(_other.win_len),
                   slide_len(_other.slide_len),
                   lateness(_other.lateness),
                   winType(_other.winType),
                   role(_other.role),
                   keyMap(_other.keyMap),
                   id_inner(_other.id_inner),
                   num_inner(_other.num_inner),
                   map_indexes(_other.map_indexes),
                   ignored_tuples(_other.ignored_tuples),
                   last_time(_other.last_time) {}

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
                deleteBatch_t(batch_input); // delete the punctuation
                return this->GO_ON;
            }
#if defined (WF_TRACING_ENABLED)
            (this->stats_record).inputs_received += batch_input->getSize();
            (this->stats_record).bytes_received += batch_input->getSize() * sizeof(tuple_t);
#endif
            assert(role != role_t::WLQ && role != role_t::REDUCE); // sanity check
            for (size_t i=0; i<batch_input->getSize(); i++) { // process all the inputs within the received batch
                process_input(batch_input->getTupleAtPos(i), 0, batch_input->getTimestampAtPos(i), batch_input->getWatermark((this->context).getReplicaIndex()));
            }
            deleteBatch_t(batch_input); // delete the input batch
        }
        else { // receiving a single input
            Single_t<tuple_t> *input = reinterpret_cast<Single_t<tuple_t> *>(_in);
            if (input->isPunct()) { // if it is a punctuaton
                (this->emitter)->propagate_punctuation(input->getWatermark((this->context).getReplicaIndex()), this); // propagate the received punctuation
                assert(last_time <= input->getWatermark((this->context).getReplicaIndex())); // sanity check
                last_time = input->getWatermark((this->context).getReplicaIndex());
                deleteSingle_t(input); // delete the punctuation
                return this->GO_ON;
            }
#if defined (WF_TRACING_ENABLED)
            (this->stats_record).inputs_received++;
            (this->stats_record).bytes_received += sizeof(tuple_t);
#endif
            if ((role == role_t::WLQ || role == role_t::REDUCE) && this->execution_mode != Execution_Mode_t::DEFAULT) { // special case
                assert(winType == Win_Type_t::CB); // sanity check
                process_input(input->tuple, input->getIdentifier(), input->getWatermark((this->context).getReplicaIndex()), 0);
            }
            else if ((role == role_t::WLQ || role == role_t::REDUCE) && this->execution_mode == Execution_Mode_t::DEFAULT) { // special case
                assert(winType == Win_Type_t::CB); // sanity check
                process_input(input->tuple, input->getIdentifier(), input->getWatermark((this->context).getReplicaIndex()), input->getWatermark((this->context).getReplicaIndex()));
            }
            else {
                process_input(input->tuple, 0, input->getTimestamp(), input->getWatermark((this->context).getReplicaIndex()));
            }
            deleteSingle_t(input); // delete the input Single_t
        }
        this->endStatsRecording();
        return this->GO_ON;
    }

    // Process a single input
    void process_input(tuple_t &_tuple,
                       uint64_t _identifier,
                       uint64_t _timestamp,
                       uint64_t _watermark)
    {
        if (this->execution_mode == Execution_Mode_t::DEFAULT) {
            assert(last_time <= _watermark); // sanity check
            last_time = _watermark;
        }
        else { // attention: timestamps can be disordered, even in DETERMINISTIC/PROBABILISTIC mode if role == WLQ or role == REDUCE
            if (last_time < _timestamp) {
                last_time = _timestamp;
            }
        }
        auto key = key_extr(_tuple); // get the key attribute of the input tuple
        size_t hashcode = std::hash<key_t>()(key); // compute the hashcode of the key
        auto it = keyMap.find(key); // find the corresponding key_descriptor (or allocate it if does not exist)
        if (it == keyMap.end()) {
            auto p = keyMap.insert(std::make_pair(key, Key_Descriptor(compare_func, role == role_t::MAP ? map_indexes.first : 0))); // create the state of the key
            it = p.first;
        }
        Key_Descriptor &key_d = (*it).second;
        _identifier = key_d.next_input_id++; // set the progressive identifier of the tuple (per key basis)
        uint64_t index = (winType == Win_Type_t::CB) ? _identifier : _timestamp; // index value is the identifier (CB) of the timestamp (TB) of the tuple
        // gwid of the first window of the key assigned to the replica
        uint64_t first_gwid_key = ((id_inner - (hashcode % num_inner) + num_inner) % num_inner);
        // initial identifer (CB) or timestamp (TB) of the keyed sub-stream arriving at the replica
        uint64_t initial_index = ((id_inner - (hashcode % num_inner) + num_inner) % num_inner) * (slide_len / num_inner);
        uint64_t min_boundary = (key_d.last_lwid >= 0) ? win_len + (key_d.last_lwid  * slide_len) : 0; // if the tuple is related to a closed window -> IGNORED
        if (index < initial_index + min_boundary) {
            if (key_d.last_lwid >= 0) {
#if defined (WF_TRACING_ENABLED)
                stats_record.inputs_ignored++;
#endif
                ignored_tuples++;
            }
            return;
        }
        long last_w = -1; // determine the lwid of the last window containing t
        if (win_len >= slide_len) { // sliding or tumbling windows
            last_w = ceil(((double) index + 1 - initial_index)/((double) slide_len)) - 1;
        }
        else { // hopping windows
            uint64_t n = floor((double) (index - initial_index) / slide_len);
            last_w = n;
        }
        auto &wins = key_d.wins;
        for (long lwid = key_d.next_lwid; lwid <= last_w; lwid++) { // create all the new opened windows
            uint64_t gwid = first_gwid_key + (lwid * num_inner); // translate lwid -> gwid
            if (winType == Win_Type_t::CB) {
                wins.push_back(win_t(key, lwid, gwid, Triggerer_CB(win_len, slide_len, lwid, initial_index), Win_Type_t::CB, win_len, slide_len));
            }
            else {
                wins.push_back(win_t(key, lwid, gwid, Triggerer_TB(win_len, slide_len, lwid, initial_index), Win_Type_t::TB, win_len, slide_len));
            }
            key_d.next_lwid++;
        }
        // check if the input must be discarded (only for role MAP)
        if (role == role_t::MAP && (_timestamp % map_indexes.second) != map_indexes.first) {
            return;
        }
        size_t cnt_fired = 0;
        if constexpr (isNonIncRiched || isNonIncNonRiched) {
            (key_d.archive).insert(wrapper_t(_tuple, index)); // insert the wrapped tuple in the archive of the key
        }
        for (auto &win: wins) { // evaluate all the open windows of the key
            win_event_t event = win.onTuple(_tuple, index, _timestamp); // get the event
            if (event == win_event_t::IN) { // window is not fired
                if constexpr (isIncNonRiched) { // incremental and non-riched
                    func(_tuple, win.getResult());
                }
                if constexpr (isIncRiched) { // incremental and riched
                    (this->context).setContextParameters(_timestamp, _watermark); // set the parameter of the RuntimeContext
                    func(_tuple, win.getResult(), this->context);
                }
            }
            else if (event == win_event_t::FIRED) { // window is fired
                if ((winType == Win_Type_t::CB) || (this->execution_mode != Execution_Mode_t::DEFAULT) || (win.getResultTimestamp() + lateness < _watermark)) {
                    std::optional<wrapper_t> t_s = win.getFirstTuple();
                    std::optional<wrapper_t> t_e = win.getLastTuple();
                    if constexpr (isNonIncNonRiched || isNonIncRiched) { // non-incremental
                        std::pair<input_iterator_t, input_iterator_t> its;
                        if (!t_s) { // empty window
                            its.first = (key_d.archive).end();
                            its.second = (key_d.archive).end();
                        }
                        else { // non-empty window
                            its = (key_d.archive).getWinRange(*t_s, *t_e);
                        }
                        Iterable<tuple_t> iter(its.first, its.second);
                        if constexpr (isNonIncNonRiched) { // non-riched
                            func(iter, win.getResult());
                        }
                        if constexpr (isNonIncRiched) { // riched
                            (this->context).setContextParameters(_timestamp, _watermark); // set the parameter of the RuntimeContext
                            func(iter, win.getResult(), this->context);
                        }
                        if (t_s) { // purge tuples from the archive
                            (key_d.archive).purge(*t_s);
                        }
                    }
                    cnt_fired++;
                    key_d.last_lwid++;
                    uint64_t used_ts = (this->execution_mode != Execution_Mode_t::DEFAULT) ? _timestamp : _watermark;
                    uint64_t used_wm = (this->execution_mode != Execution_Mode_t::DEFAULT) ? 0 : _watermark;
                    if (role == role_t::MAP) { // special case: role is MAP
                        (this->emitter)->emit(&(win.getResult()), key_d.next_res_id, used_ts, used_wm, this);
                        key_d.next_res_id += map_indexes.second;
                    }
                    else if (role == role_t::PLQ) { // special case: role is PLQ
                        uint64_t new_id = ((id_inner - (hashcode % num_inner) + num_inner) % num_inner) + (key_d.next_res_id * num_inner);
                        (this->emitter)->emit(&(win.getResult()), new_id, used_ts, used_wm, this);
                        key_d.next_res_id++;
                    }
                    else { // standard case
                        (this->emitter)->emit(&(win.getResult()), 0, used_ts, used_wm, this);
                    }
#if defined (WF_TRACING_ENABLED)
                    (this->stats_record).outputs_sent++;
                    (this->stats_record).bytes_sent += sizeof(result_t);
#endif
                }
            }
        }
        wins.erase(wins.begin(), wins.begin() + cnt_fired); // purge the fired windows
    }

    // method to manage the EOS (utilized by the FastFlow runtime)
    void eosnotify(ssize_t id) override
    {
        size_t count = 0;
        for (auto &k: keyMap) { // iterate over all the keys
            Key_Descriptor &key_d = (k.second);
            auto &wins = key_d.wins;
            for (auto &win: wins) { // iterate over all the windows of the key
                if constexpr (isNonIncNonRiched || isNonIncRiched) { // non-incremental
                    std::optional<wrapper_t> t_s = win.getFirstTuple();
                    std::optional<wrapper_t> t_e = win.getLastTuple();
                    std::pair<input_iterator_t, input_iterator_t> its;
                    if (!t_s) { // empty window
                        its.first = ((k.second).archive).end();
                        its.second = ((k.second).archive).end();
                    }
                    else { // non-empty window
                        if (!t_e) {
                            its = ((k.second).archive).getWinRange(*t_s);
                        }
                        else {
                            its = ((k.second).archive).getWinRange(*t_s, *t_e);
                        }
                    }
                    Iterable<tuple_t> iter(its.first, its.second);
                    if constexpr (isNonIncNonRiched) { // non-riched
                        func(iter, win.getResult());
                    }
                    if constexpr (isNonIncRiched) { // riched
                        func(iter, win.getResult(), this->context);
                    }
                }
                uint64_t used_wm = (this->execution_mode != Execution_Mode_t::DEFAULT) ? 0 : last_time;
                if (role == role_t::MAP) { // special case: role is MAP
                    (this->emitter)->emit(&(win.getResult()), key_d.next_res_id, last_time, used_wm, this);
                    key_d.next_res_id += map_indexes.second;
                }
                else if (role == role_t::PLQ) { // special case: role is PLQ
                    size_t hashcode = std::hash<typename std::remove_const<key_t>::type>()(k.first); // compute the hashcode of the key
                    uint64_t new_id = ((id_inner - (hashcode % num_inner) + num_inner) % num_inner) + (key_d.next_res_id * num_inner);
                    (this->emitter)->emit(&(win.getResult()), new_id, last_time, used_wm, this);
                    key_d.next_res_id++;
                }
                else { // standard case
                    (this->emitter)->emit(&(win.getResult()), 0, last_time, used_wm, this);
                }
#if defined (WF_TRACING_ENABLED)
                (this->stats_record).outputs_sent++;
                (this->stats_record).bytes_sent += sizeof(result_t);
#endif
            }
        }
        Basic_Replica::eosnotify(id);
    }

    // Get the number of ignored tuples
    size_t getNumIgnoredTuples() const
    {
        return ignored_tuples;
    }

    Window_Replica(Window_Replica &&) = delete; ///< Move constructor is deleted
    Window_Replica &operator=(const Window_Replica &) = delete; ///< Copy assignment operator is deleted
    Window_Replica &operator=(Window_Replica &&) = delete; ///< Move assignment operator is deleted
};

} // namespace wf

#endif
