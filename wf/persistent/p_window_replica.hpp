/**************************************************************************************
 *  Copyright (c) 2019- Gabriele Mencagli, Simone Frassinelli and Andrea Filippi
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
 *  @file    p_window_replica.hpp
 *  @author  Gabriele Mencagli, Simone Frassinelli and Andrea Filippi
 *  
 *  @brief P_Window_Replica is the replica of the P_Keyed_Windows operator
 *  
 *  @section P_Window_Replica (Description)
 *  
 *  This file implements the P_Window_Replica representing the replica of the
 *  P_Keyed_Windows operators, which processes windows with key-based parallelism
 *  keeping information on RocksDB.
 */ 

#ifndef P_WIN_REPLICA_H
#define P_WIN_REPLICA_H

// includes
#include<map>
#include<list>
#include<cmath>
#include<deque>
#include<regex>
#include<vector>
#include<string>
#include<cstddef>
#include<functional>
#include<unordered_map>
#include<string.h>
#include<context.hpp>
#include<batch_t.hpp>
#include<single_t.hpp>
#if defined(WF_TRACING_ENABLED)
    #include<stats_record.hpp>
#endif
#include<basic_emitter.hpp>
#include<basic_operator.hpp>
#include<persistent/db_handle.hpp>
#include<persistent/p_window_structure.hpp>
#include<persistent/cache/cache_lru.hpp>
#include<persistent/cache/cache_lfu.hpp>

namespace wf {

// class P_Window_Replica
template<typename win_func_t, typename keyextr_func_t>
class P_Window_Replica: public Basic_Replica
{
private:
    template<typename T1, typename T2> friend class P_Keyed_Windows;
    win_func_t func; // functional logic used by the P_Window_Replica
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
                  "WindFlow Compilation Error - P_Window_Replica does not have a valid functional logic:\n");
    using wrapper_t = wrapper_tuple_t<tuple_t>; // alias for the wrapped tuple type
    using input_iterator_t = typename std::deque<wrapper_t>::iterator; // iterator type for accessing wrapped tuples in the archive
    using win_t = P_Window<tuple_t, result_t, key_t>; // window type used by the P_Window_Replica
    using compare_func_t = std::function<bool(const wrapper_t &, const wrapper_t &)>; // function type to compare two wrapped tuples
    using index_t = decltype(wrapper_t::index); // type of the index field
    using compare_func_index_t = std::function<bool(const index_t &, const index_t &)>; // function type to compare two indexes
    using meta_frag_t = std::tuple<index_t, index_t, size_t>; // tuple type for fragment metadata (min, max, id)
    using window_buffer_t = std::deque<wrapper_t>; // buffer partially containing tuples useful for the next window
    size_t n_max_elements; // max capacity of volatile buffers representing fragments
    DBHandle<tuple_t> *mydb_wrappers; // pointer to the DBHandle object used to interact with RocksDB
    DBHandle<result_t> *mydb_results; // pointer to the DBHandle object used to interact with RocksDB

    struct Key_Descriptor // struct of a key descriptor
    {
        std::vector<win_t> wins; // open windows of this key
        std::deque<wrapper_t> actual_memory; // in-memoty buffer of tuples used by non-incremental logic only
        std::deque<meta_frag_t> frags; // fragments metadata of this key
        size_t frag_keys = 0; // counter of fragments produced for this key
        index_t min, max; // min and max indexes in the in-memory buffer
        uint64_t next_lwid = 0; // next window to be opened of this key (lwid)
        int64_t last_lwid = -1; // last window closed of this key (lwid)
        uint64_t next_input_id = 0; // identifier of the next tuple of this key
    };

    std::unordered_map<key_t, Key_Descriptor> keyMap; // hashtable mapping keys to Key_Descriptor structures
    compare_func_t compare_func = [](const wrapper_t &w1, const wrapper_t &w2) { return w1.index < w2.index; }; // function to compare two wrapped tuples
    compare_func_index_t geqt = [](const index_t &w1, const index_t &w2) { return w1 >= w2; }; // geq function between indexes
    compare_func_index_t leqt = [](const index_t &w1, const index_t &w2) { return w1 <= w2; }; // leq function between indexes
    compare_func_index_t compare_func_index = [](const index_t &w1, const index_t &w2) { return w1 < w2; }; // compare function between indexes
    uint64_t win_len; // window length (in no. of tuples or in time units)
    uint64_t slide_len; // slide length (in no. of tuples or in time units)
    uint64_t lateness; // triggering delay in time units (meaningful for TB windows in DEFAULT mode)
    Win_Type_t winType; // window type (CB or TB)
    size_t ignored_tuples; // number of ignored tuples
    uint64_t last_time; // last received timestamp or watermark
    Cache<key_t, window_buffer_t> *cache; // cache to avoid accessing too many fragments from the kVS

public:
    // check_range_mm method to check that a fragment is useful for a window computation
    inline bool check_range_mm(const wrapper_t &_minw,
                               const wrapper_t &_maxw,
                               const meta_frag_t &_info,
                               bool _only_one)
    {
        return _only_one ? leqt(_maxw.index, std::get<1>(_info)) : (geqt(_maxw.index, std::get<0>(_info)) && leqt(_minw.index, std::get<1>(_info)));
    }

    // set_mm method to set min and max indexes inside the in-memory buffer
    inline void set_mm(const index_t &_wt_index,
                       Key_Descriptor &_kd)
    {
        if (_kd.actual_memory.empty()) {
            _kd.max = _wt_index;
            _kd.min = _wt_index;
            return;
        }
        if (geqt(_wt_index, _kd.max)) {
            _kd.max = _wt_index;
        }
        if (leqt(_wt_index, _kd.min)) {
            _kd.min = _wt_index;
        }
    }

    // method to insert a new tuple in the in-memory buffer
    void insert(wrapper_t &&_wt,
                Key_Descriptor &_kd,
                key_t &_my_key)
    {
        if (_kd.actual_memory.size() + 1 > n_max_elements) {
            size_t new_frag_id = _kd.frag_keys++;
            meta_frag_t meta(_kd.min, _kd.max, new_frag_id);
            _kd.frags.push_back(meta);
            mydb_wrappers->put(_kd.actual_memory, _my_key, new_frag_id);
            _kd.actual_memory.clear();
        }
        set_mm(_wt.index, _kd); // update min/max new fragment
        _kd.actual_memory.push_back(std::move(_wt));
    }

    // method to insert a new tuple in the in-memory buffer
    void insert(const wrapper_t &_wt,
                Key_Descriptor &_kd,
                key_t &_my_key)
    {
        if (_kd.actual_memory.size() + 1 > n_max_elements) {
            size_t new_frag_id = _kd.frag_keys++;
            meta_frag_t meta(_kd.min, _kd.max, new_frag_id);
            _kd.frags.push_front(meta);
            mydb_wrappers->put(_kd.actual_memory, _my_key, new_frag_id);
            _kd.actual_memory.clear();
        }
        set_mm(_wt.index, _kd); // update min/max new fragment
        _kd.actual_memory.push_back(_wt);
    }

    // method to purge all tuples older than _wt
    size_t purge(const wrapper_t &_wt,
                 Key_Descriptor &_kd,
                 key_t &_my_key)
    {
        size_t sum = 0;
        if (compare_func_index(_kd.max, _wt.index)) {
            sum += _kd.actual_memory.size();
            _kd.actual_memory.clear();
        }
        if (_kd.frags.empty()) {
            return sum;
        }
        for (auto &info: _kd.frags) {
            if (compare_func_index(std::get<1>(info), _wt.index)) {
                mydb_wrappers->delete_key(_my_key, std::get<2>(info));
                std::get<2>(info) = -1;
                sum += n_max_elements;
            }
        }
        auto erased_it = std::remove_if(_kd.frags.begin(), _kd.frags.end(), [](meta_frag_t &x) { return std::get<2>(x) == (size_t)-1; });
        _kd.frags.erase(erased_it, _kd.frags.end());
        return sum;
    }

    // method to get the history of tuples useful for computing a windows
    window_buffer_t get_history_buffer(const size_t &_lwid,
                                       const wrapper_t &_w1,
                                       const wrapper_t &_w2,
                                       bool _from_w1_to_end,
                                       Key_Descriptor &_kd,
                                       key_t &_my_key)
    {
        window_buffer_t final_range;
        meta_frag_t mem_infos(_kd.min, _kd.max, 0);
        auto min = _w1;
        bool usable_cache = false;
        window_buffer_t cached_window;
        if (cache != nullptr) { // if cache is enabled
            std::optional<window_buffer_t> cached_window_res = cache->get(_my_key);
            if (cached_window_res) {
                // check if the cached window buffer contains some tuples useful for the next window lwid
                auto min_win = slide_len * _lwid;
                auto temp = cached_window_res.value().back();
                if (temp.index >= min_win) {
                    // cached window buffer partially overlaps with the next window lwid
                    min = temp;
                    usable_cache = true;
                    cached_window = *cached_window_res;
                }
            }
        }
        if (check_range_mm(min, _w2, mem_infos, _from_w1_to_end)) {
            // for (wrapper_t &wrap: _kd.actual_memory) {
            //    final_range.push_back(wrap);
            // }
            final_range.insert(final_range.end(), _kd.actual_memory.begin(), _kd.actual_memory.end());
        }
        for (auto &info: _kd.frags) {
            if (check_range_mm(min, _w2, info, _from_w1_to_end)) {
                std::deque<wrapper_t> to_push = mydb_wrappers->get_list_frag(_my_key, std::get<2>(info));
                // for (wrapper_t &wrap: to_push) {
                //    final_range.push_back(std::move(wrap));
                // }
                final_range.insert(final_range.end(), std::make_move_iterator(to_push.begin()), std::make_move_iterator(to_push.end()));
            }
        }
        std::sort(final_range.begin(), final_range.end(), compare_func); // sorting the archive before passing to the user function (NIC)
        if (usable_cache) { // delete tuples from fragments and reatach last cached window, to resolve duplicates
            final_range.erase(final_range.begin(),
                              std::find_if(final_range.begin(), final_range.end(), [&min](const wrapper_t& w) { return w.index > min.index; }));
            final_range.insert(final_range.begin(),
                               std::make_move_iterator(cached_window.begin()),
                               std::make_move_iterator(cached_window.end()));
        }
        return final_range;
    }

    // getEnd method
    input_iterator_t getEnd(Key_Descriptor &_kd)
    {
        return (_kd.actual_memory).end();
    }

    // Constructor
    P_Window_Replica(win_func_t _func,
                     keyextr_func_t _key_extr,
                     std::string _opName,
                     std::string _dbpath,
                     RuntimeContext _context,
                     std::function<void(RuntimeContext &)> _closing_func,
                     std::function<std::string(tuple_t &)> _tuple_serialize,
                     std::function<tuple_t(std::string &)> _tuple_deserialize,
                     std::function<std::string(result_t &)> _result_serialize,
                     std::function<result_t(std::string &)> _result_deserialize,
                     bool _deleteDb,
                     bool _sharedDb,
                     size_t _whoami,
                     size_t _frag_size,
                     uint64_t _win_len,
                     uint64_t _slide_len,
                     uint64_t _lateness,
                     Win_Type_t _winType,
                     size_t _cacheCapacity):
                     Basic_Replica(_opName, _context, _closing_func, true),
                     func(_func),
                     key_extr(_key_extr),
                     n_max_elements(_frag_size),
                     win_len(_win_len),
                     slide_len(_slide_len),
                     lateness(_lateness),
                     winType(_winType),
                     ignored_tuples(0),
                     last_time(0)
    {
        _dbpath = _sharedDb ? _dbpath + "_shared" : _dbpath;
        if constexpr (isNonIncNonRiched || isNonIncRiched) {
            mydb_wrappers = new DBHandle<tuple_t>(_tuple_serialize,
                                                  _tuple_deserialize,
                                                  _deleteDb,
                                                  _dbpath + "_frag",
                                                  tuple_t{},
                                                  _whoami);
            mydb_results = nullptr;
        }
        else {
            mydb_wrappers = nullptr;
            mydb_results = new DBHandle<result_t>(_result_serialize,
                                                  _result_deserialize,
                                                  _deleteDb,
                                                  _dbpath + "_result",
                                                  result_t{},
                                                  _whoami);
        }
        if ((_cacheCapacity != 0) && (slide_len < win_len)) { // cache creation
            cache = new LRUCache<key_t, window_buffer_t>(_cacheCapacity);
            // cache = new LFUCache<key_t, window_buffer_t>(_cacheCapacity);
        }
        else {
            cache = nullptr;
        }
    }

    // Copy Constructor
    P_Window_Replica(const P_Window_Replica &_other):
                     Basic_Replica(_other),
                     func(_other.func),
                     key_extr(_other.key_extr),
                     n_max_elements(_other.n_max_elements),
                     keyMap(_other.keyMap),          
                     compare_func(_other.compare_func),
                     geqt(_other.geqt),
                     leqt(_other.leqt),
                     compare_func_index(_other.compare_func_index),
                     win_len(_other.win_len),
                     slide_len(_other.slide_len),
                     lateness(_other.lateness),
                     winType(_other.winType),                     
                     ignored_tuples(_other.ignored_tuples),
                     last_time(_other.last_time)
    {
        if (_other.mydb_wrappers != nullptr) {
            mydb_wrappers = (_other.mydb_wrappers)->getCopy();
        }
        else {
            mydb_wrappers = nullptr;
        }
        if (_other.mydb_results != nullptr) {
            mydb_results = (_other.mydb_results)->getCopy();
        }
        else {
            mydb_results = nullptr;
        }
        auto other_cache = _other.cache; // cache creation
        if (other_cache != nullptr) {
            cache = new LRUCache<key_t, window_buffer_t>(other_cache->capacity());
            // cache = new LFUCache<key_t, window_buffer_t>(other_cache->capacity());
        }
        else {
            cache = nullptr;
        }
    }

    // Destructor
    ~P_Window_Replica()
    {
        if (mydb_wrappers != nullptr) {
            delete mydb_wrappers;
        }
        if (mydb_results != nullptr) {
            delete mydb_results;
        }
        if (cache != nullptr) {
            delete cache;
        }
    }

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
#if defined(WF_TRACING_ENABLED)
            (this->stats_record).inputs_received += batch_input->getSize();
            (this->stats_record).bytes_received += batch_input->getSize() * sizeof(tuple_t);
#endif
            for (size_t i = 0; i < batch_input->getSize(); i++) { // process all the inputs within the received batch
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
#if defined(WF_TRACING_ENABLED)
            (this->stats_record).inputs_received++;
            (this->stats_record).bytes_received += sizeof(tuple_t);
#endif
            process_input(input->tuple, 0, input->getTimestamp(), input->getWatermark((this->context).getReplicaIndex()));
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
        else { // timestamps are monotonically increasing in DETERMINISTIC and PROBABILISTIC modes
            assert(last_time <= _timestamp); // sanity check
            last_time = _timestamp;
        }
        auto key = key_extr(_tuple); // get the key attribute of the input tuple
        size_t hashcode = std::hash<key_t>()(key); // compute the hashcode of the key
        auto it = keyMap.find(key); // find the corresponding key_descriptor (or allocate it if does not exist)
        if (it == keyMap.end()) {
            auto p = keyMap.insert(std::make_pair(key, Key_Descriptor())); // create the state of the key
            it = p.first;
        }
        Key_Descriptor &key_d = (*it).second;
        _identifier = key_d.next_input_id++; // set the progressive identifier of the tuple (per key basis)
        uint64_t index = (winType == Win_Type_t::CB) ? _identifier : _timestamp; // index value is the identifier (CB) of the timestamp (TB) of the tuple
        // gwid of the first window of the key assigned to the replica
        uint64_t first_gwid_key = 0;
        // initial identifer (CB) or timestamp (TB) of the keyed sub-stream arriving at the replica
        uint64_t initial_index = 0;
        uint64_t min_boundary = (key_d.last_lwid >= 0) ? win_len + (key_d.last_lwid * slide_len) : 0; // if the tuple is related to a closed window -> IGNORED
        if (index < initial_index + min_boundary) {
            if (key_d.last_lwid >= 0) {
#if defined(WF_TRACING_ENABLED)
                stats_record.inputs_ignored++;
#endif
                ignored_tuples++;
            }
            return;
        }
        long last_w = -1; // determine the lwid of the last window containing t
        if (win_len >= slide_len) { // sliding or tumbling windows
            last_w = ceil(((double)index + 1 - initial_index) / ((double)slide_len)) - 1;
        }
        else { // hopping windows
            uint64_t n = floor((double)(index - initial_index) / slide_len);
            last_w = n;
        }
        std::deque<result_t> _win_results; // used only by incremental processing
        bool res_opened = false; // used only by incremental processing
        auto &wins = key_d.wins;
        if constexpr (isIncNonRiched || isIncRiched) {
            if ((long) key_d.next_lwid <= last_w) { // if there are new windows, and results are kept on RocksDB
                _win_results = mydb_results->get_list_result(key); // deserialize windows results associated with key
                res_opened = true;
            }
        }
        for (long lwid = key_d.next_lwid; lwid <= last_w; lwid++) { // create all the new opened windows
            uint64_t gwid = first_gwid_key + lwid; // translate lwid -> gwid
            if constexpr (isIncNonRiched || isIncRiched) {
                result_t new_res = create_win_result_t<result_t, key_t>(key, gwid);
                _win_results.push_back(new_res);
            }
            if (winType == Win_Type_t::CB) {
                wins.push_back(win_t(key, lwid, gwid, Triggerer_CB(win_len, slide_len, lwid, initial_index), Win_Type_t::CB, win_len, slide_len));
            }
            else {
                wins.push_back(win_t(key, lwid, gwid, Triggerer_TB(win_len, slide_len, lwid, initial_index), Win_Type_t::TB, win_len, slide_len));
            }
            key_d.next_lwid++;
        }
        size_t cnt_fired = 0;
        if constexpr (isNonIncRiched || isNonIncNonRiched) {
            insert(wrapper_t(_tuple, index), key_d, key); // insert the wrapped tuple in the archive of the key (non-incremental processing only)
        }
        if constexpr (isIncNonRiched || isIncRiched) {
            if (!wins.empty() && !res_opened) {
                _win_results = mydb_results->get_list_result(key); // deserialize windows results associated with key
                res_opened = true;
            }
        }
        typename std::deque<result_t>::iterator result_it_list = _win_results.begin();
        for (auto &win: wins) { // evaluate all the open windows of the key
            win_event_t event = win.onTuple(_tuple, index, _timestamp); // get the event
            if (event == win_event_t::IN) { // window is not fired
                if constexpr (isIncNonRiched) { // incremental and non-riched
                    result_t &res = *result_it_list;
                    func(_tuple, res);
                }
                if constexpr (isIncRiched) { // incremental and riched
                    result_t &res = *result_it_list;
                    (this->context).setContextParameters(_timestamp, _watermark); // set the parameter of the RuntimeContext
                    func(_tuple, res, this->context);
                }
            }
            else if (event == win_event_t::FIRED) { // window is fired
                if ((winType == Win_Type_t::CB) || (this->execution_mode != Execution_Mode_t::DEFAULT) || (win.getResultTimestamp() + lateness < _watermark)) {
                    std::optional<wrapper_t> t_s = win.getFirstTuple();
                    std::optional<wrapper_t> t_e = win.getLastTuple();
                    if constexpr (isNonIncNonRiched || isNonIncRiched) { // non-incremental
                        std::pair<input_iterator_t, input_iterator_t> its;
                        std::deque<wrapper_t> history_buffer;
                        if (!t_s) { // empty window
                            its.first = getEnd(key_d);
                            its.second = getEnd(key_d);
                        }
                        else { // non-empty window
                            history_buffer = get_history_buffer(win.getLWID(), *t_s, *t_e, false, key_d, key);
                            its.first = std::lower_bound(history_buffer.begin(), history_buffer.end(), *t_s, compare_func);
                            its.second = std::lower_bound(history_buffer.begin(), history_buffer.end(), *t_e, compare_func);
                            if (cache != nullptr) { // select only the portion really useful to the next window
                                auto min_idx = win.getLWID() * slide_len;
                                auto start = std::find_if(history_buffer.begin(),
                                                          history_buffer.end(),
                                                          [&min_idx](const wrapper_t& w) { return w.index >= min_idx; });
                                if (start != history_buffer.end() && start != its.second) {
                                    cache->put(key, window_buffer_t(start, its.second));
                                }
                            }
                        }
                        Iterable<tuple_t> iter(its.first, its.second);
                        result_t res = create_win_result_t<result_t, key_t>(key, win.getGWID());
                        if constexpr (isNonIncNonRiched) { // non-riched
                            func(iter, res);
                        }
                        if constexpr (isNonIncRiched) { // riched
                            (this->context).setContextParameters(_timestamp, _watermark); // set the parameter of the RuntimeContext
                            func(iter, res, this->context);
                        }
                        if (t_s) { // purge tuples from the archive
                            purge(*t_s, key_d, key);
                        }
                        cnt_fired++;
                        key_d.last_lwid++;
                        uint64_t used_ts = (this->execution_mode != Execution_Mode_t::DEFAULT) ? _timestamp : _watermark;
                        uint64_t used_wm = (this->execution_mode != Execution_Mode_t::DEFAULT) ? 0 : _watermark;
                        this->doEmit(this->emitter, &(res), 0, used_ts, used_wm, this);
                    }
                    else {
                        result_t &res = *result_it_list;
                        cnt_fired++;
                        key_d.last_lwid++;
                        uint64_t used_ts = (this->execution_mode != Execution_Mode_t::DEFAULT) ? _timestamp : _watermark;
                        uint64_t used_wm = (this->execution_mode != Execution_Mode_t::DEFAULT) ? 0 : _watermark;
                        this->doEmit(this->emitter, &(res), 0, used_ts, used_wm, this);
                    }
#if defined(WF_TRACING_ENABLED)
                    (this->stats_record).outputs_sent++;
                    (this->stats_record).bytes_sent += sizeof(result_t);
#endif
                }
            }
            if constexpr (isIncNonRiched || isIncRiched) {
                result_it_list++;
            }
        }
        if constexpr (isIncNonRiched || isIncRiched) {
            _win_results.erase(_win_results.begin(), _win_results.begin() + cnt_fired);
            mydb_results->put(_win_results, key);
        }
        wins.erase(wins.begin(), wins.begin() + cnt_fired); // purge the fired windows
    }

    // method to manage the EOS (utilized by the FastFlow runtime)
    void eosnotify(ssize_t id) override
    {
        for (auto &k: keyMap) { // iterate over all the keys
            key_t key = (k.first);
            Key_Descriptor &key_d = (k.second);
            std::deque<result_t> _win_results; // used only by incremental processing
            typename std::deque<result_t>::iterator result_it_list;
            if constexpr (isIncNonRiched || isIncRiched) {
                _win_results = mydb_results->get_list_result(key);
                result_it_list = _win_results.begin();
            }
            auto &wins = key_d.wins;
            for (auto &win: wins) { // iterate over all the windows of the key
                if constexpr (isNonIncNonRiched || isNonIncRiched) { // non-incremental
                    std::optional<wrapper_t> t_s = win.getFirstTuple();
                    std::optional<wrapper_t> t_e = win.getLastTuple();
                    std::pair<input_iterator_t, input_iterator_t> its;
                    std::deque<wrapper_t> history_buffer;
                    if (!t_s) { // empty window
                        its.first = getEnd(key_d);
                        its.second = getEnd(key_d);
                    }
                    else { // non-empty window
                        if (!t_e) {
                            history_buffer = get_history_buffer(win.getLWID(), *t_s, *t_s, true, key_d, key);
                            its.first = std::lower_bound(history_buffer.begin(), history_buffer.end(), *t_s, compare_func);
                            its.second = history_buffer.end();
                        }
                        else {
                            history_buffer = get_history_buffer(win.getLWID(), *t_s, *t_e, false, key_d, key);
                            its.first = std::lower_bound(history_buffer.begin(), history_buffer.end(), *t_s, compare_func);
                            its.second = std::lower_bound(history_buffer.begin(), history_buffer.end(), *t_e, compare_func);
                        }
                    }
                    Iterable<tuple_t> iter(its.first, its.second);
                    result_t res = create_win_result_t<result_t, key_t>(key, win.getGWID());
                    if constexpr (isNonIncNonRiched) { // non-riched
                        func(iter, res);
                    }
                    if constexpr (isNonIncRiched) { // riched
                        func(iter, res, this->context);
                    }
                    uint64_t used_wm = (this->execution_mode != Execution_Mode_t::DEFAULT) ? 0 : last_time;
                    this->doEmit(this->emitter, &(res), 0, last_time, used_wm, this);
                }
                else {
                    result_t &res = *result_it_list;
                    uint64_t used_wm = (this->execution_mode != Execution_Mode_t::DEFAULT) ? 0 : last_time;
                    this->doEmit(this->emitter, &(res), 0, last_time, used_wm, this);
                    result_it_list++;
                }
#if defined(WF_TRACING_ENABLED)
                (this->stats_record).outputs_sent++;
                (this->stats_record).bytes_sent += sizeof(result_t);
#endif
            }
            if constexpr (isIncNonRiched || isIncRiched) { // I don't think this part is really necessary
                mydb_results->put(_win_results, key);
            }
        }
        Basic_Replica::eosnotify(id);
    }

    // Get the number of ignored tuples
    size_t getNumIgnoredTuples() const
    {
        return ignored_tuples;
    }

    P_Window_Replica(P_Window_Replica &&) = delete; ///< Move constructor is deleted
    P_Window_Replica &operator=(const P_Window_Replica &) = delete; ///< Copy assignment operator is deleted
    P_Window_Replica &operator=(P_Window_Replica &&) = delete; ///< Move assignment operator is deleted
};

} // namespace wf

#endif
