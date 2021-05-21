/******************************************************************************
 *  This program is free software; you can redistribute it and/or modify it
 *  under the terms of the GNU Lesser General Public License version 3 as
 *  published by the Free Software Foundation.
 *  
 *  This program is distributed in the hope that it will be useful, but WITHOUT
 *  ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 *  FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public
 *  License for more details.
 *  
 *  You should have received a copy of the GNU Lesser General Public License
 *  along with this program; if not, write to the Free Software Foundation,
 *  Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 ******************************************************************************
 */

/** 
 *  @file    win_seq.hpp
 *  @author  Gabriele Mencagli
 *  @date    30/06/2017
 *  
 *  @brief Win_Seq node executing windowed queries on a multi-core CPU
 *  
 *  @section Win_Seq (Description)
 *  
 *  This file implements the Win_Seq node able to execute windowed queries on a
 *  multicore. The node executes streaming windows in a serial fashion on a CPU
 *  core and supports both a non-incremental and an incremental query definition.
 *  
 *  The template parameters tuple_t and result_t must be default constructible, with
 *  a copy constructor and a copy assignment operator, and they must provide and implement
 *  the setControlFields() and getControlFields() methods.
 */ 

#ifndef WIN_SEQ_H
#define WIN_SEQ_H

// includes
#include<vector>
#include<string>
#include<unordered_map>
#include<math.h>
#include<ff/node.hpp>
#include<ff/multinode.hpp>
#include<meta.hpp>
#include<window.hpp>
#include<context.hpp>
#include<iterable.hpp>
#if defined (TRACE_WINDFLOW)
    #include<stats_record.hpp>
#endif
#include<stream_archive.hpp>

namespace wf {

// Win_Seq class
template<typename tuple_t, typename result_t, typename input_t>
class Win_Seq: public ff::ff_minode_t<input_t, result_t>
{
public:
    // type of the non-incremental window processing function
    using win_func_t = std::function<void(uint64_t, const Iterable<tuple_t> &, result_t &)>;
    // type of the rich non-incremental window processing function
    using rich_win_func_t = std::function<void(uint64_t, const Iterable<tuple_t> &, result_t &, RuntimeContext &)>;
    // type of the incremental window processing function
    using winupdate_func_t = std::function<void(uint64_t, const tuple_t &, result_t &)>;
    // type of the rich incremental window processing function
    using rich_winupdate_func_t = std::function<void(uint64_t, const tuple_t &, result_t &, RuntimeContext &)>;
    // type of the closing function
    using closing_func_t = std::function<void(RuntimeContext &)>;

private:
    // type of the stream archive
    using archive_t = StreamArchive<tuple_t, std::deque<tuple_t>>;
    // iterator type for accessing tuples
    using input_iterator_t = typename std::deque<tuple_t>::iterator;
    // window type used by the Win_Seq node
    using win_t = Window<tuple_t, result_t>;
    // function type to compare two tuples
    using compare_func_t = std::function<bool(const tuple_t &, const tuple_t &)>;
    tuple_t tmp; // never used
    // key data type
    using key_t = typename std::remove_reference<decltype(std::get<0>(tmp.getControlFields()))>::type;
    // friendships with other classes in the library
    template<typename T1, typename T2, typename T3>
    friend class Win_Farm;
    template<typename T1, typename T2, typename T3>
    friend class Key_Farm;
    template<typename T1, typename T2, typename T3>
    friend class Pane_Farm;
    template<typename T1, typename T2, typename T3>
    friend class Win_MapReduce;
    template<typename T1, typename T2, typename T3, typename T4>
    friend class Pane_Farm_GPU;
    template<typename T1, typename T2, typename T3, typename T4>
    friend class Win_MapReduce_GPU;
    // struct of a key descriptor
    struct Key_Descriptor
    {
        archive_t archive; // archive of tuples of this key
        std::vector<win_t> wins; // open windows of this key
        uint64_t emit_counter; // progressive counter (used if role is PLQ or MAP)
        uint64_t next_ids; // progressive counter (used if isRenumbering is true)
        uint64_t next_lwid; // next window to be opened of this key (lwid)
        int64_t last_lwid; // last window closed of this key (lwid)

        // Constructor
        Key_Descriptor(compare_func_t _compare_func,
                       uint64_t _emit_counter=0):
                       archive(_compare_func),
                       emit_counter(_emit_counter),
                       next_ids(0),
                       next_lwid(0),
                       last_lwid(-1)
        {
            wins.reserve(DEFAULT_VECTOR_CAPACITY);
        }

        // move Constructor
        Key_Descriptor(Key_Descriptor &&_k):
                       archive(move(_k.archive)),
                       wins(move(_k.wins)),
                       emit_counter(_k.emit_counter),
                       next_ids(_k.next_ids),
                       next_lwid(_k.next_lwid),
                       last_lwid(_k.last_lwid) {}
    };
    win_func_t win_func; // function for the non-incremental window processing
    rich_win_func_t rich_win_func; // rich function for the non-incremental window processing
    winupdate_func_t winupdate_func; // function for the incremental window processing
    rich_winupdate_func_t rich_winupdate_func; // rich function for the incremental window processing
    closing_func_t closing_func; // closing function
    compare_func_t compare_func; // function to compare two tuples
    uint64_t win_len; // window length (no. of tuples or in time units)
    uint64_t slide_len; // slide length (no. of tuples or in time units)
    uint64_t triggering_delay; // triggering delay in time units (meaningful for TB windows only)
    win_type_t winType; // window type (CB or TB)
    std::string name; // std::string of the unique name of the node
    bool isNIC; // this flag is true if the node is instantiated with a non-incremental query function
    bool isRich; // flag stating whether the function to be used is rich
    RuntimeContext context; // RuntimeContext
    WinOperatorConfig config; // configuration structure of the Win_Seq node
    role_t role; // role of the Win_Seq node
    std::unordered_map<key_t, Key_Descriptor> keyMap; // hash table that maps a descriptor for each key
    std::pair<size_t, size_t> map_indexes = std::make_pair(0, 1); // indexes useful is the role is MAP
    size_t ignored_tuples; // number of ignored tuples
    size_t eos_received; // number of received EOS messages
    bool terminated; // true if the replica has finished its work
    bool isRenumbering; // if true, the node assigns increasing identifiers to the input tuples (useful for count-based windows in DEFAULT mode)
#if defined (TRACE_WINDFLOW)
    Stats_Record stats_record;
    double avg_td_us = 0;
    double avg_ts_us = 0;
    volatile uint64_t startTD, startTS, endTD, endTS;
#endif

    // private initialization method
    void init()
    {
        // check the validity of the windowing parameters
        if (win_len == 0 || slide_len == 0) {
            std::cerr << RED << "WindFlow Error: window length or slide in Win_Seq cannot be zero" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // define the compare function depending on the window type
        if (winType == win_type_t::CB) {
            compare_func = [](const tuple_t &t1, const tuple_t &t2) {
                return std::get<1>(t1.getControlFields()) < std::get<1>(t2.getControlFields());
            };
        }
        else {
            compare_func = [](const tuple_t &t1, const tuple_t &t2) {
                return std::get<2>(t1.getControlFields()) < std::get<2>(t2.getControlFields());
            };
        }
    }

    // method to set the indexes useful if role is MAP
    void setMapIndexes(size_t _first, size_t _second) {
        map_indexes.first = _first; // id
        map_indexes.second = _second; // pardegree
    }

public:
    // Constructor I
    Win_Seq(win_func_t _win_func,
            uint64_t _win_len,
            uint64_t _slide_len,
            uint64_t _triggering_delay,
            win_type_t _winType,
            std::string _name,
            closing_func_t _closing_func,
            RuntimeContext _context,
            WinOperatorConfig _config,
            role_t _role):
            win_func(_win_func),
            win_len(_win_len),
            slide_len(_slide_len),
            triggering_delay(_triggering_delay),
            winType(_winType),
            name(_name),
            closing_func(_closing_func),
            context(_context),
            config(_config),
            role(_role),
            isNIC(true),
            isRich(false),
            ignored_tuples(0),
            eos_received(0),
            terminated(false),
            isRenumbering(false)
    {
        init();
    }

    // Constructor II
    Win_Seq(rich_win_func_t _rich_win_func,
            uint64_t _win_len,
            uint64_t _slide_len,
            uint64_t _triggering_delay,
            win_type_t _winType,
            std::string _name,
            closing_func_t _closing_func,
            RuntimeContext _context,
            WinOperatorConfig _config,
            role_t _role):
            rich_win_func(_rich_win_func),
            win_len(_win_len),
            slide_len(_slide_len),
            triggering_delay(_triggering_delay),
            winType(_winType),
            name(_name),
            closing_func(_closing_func),
            context(_context),
            config(_config),
            role(_role),
            isNIC(true),
            isRich(true),
            ignored_tuples(0),
            eos_received(0),
            terminated(false),
            isRenumbering(false)
    {
        init();
    }

    // Constructor III
    Win_Seq(winupdate_func_t _winupdate_func,
            uint64_t _win_len,
            uint64_t _slide_len,
            uint64_t _triggering_delay,
            win_type_t _winType,
            std::string _name,
            closing_func_t _closing_func,
            RuntimeContext _context,
            WinOperatorConfig _config,
            role_t _role):
            winupdate_func(_winupdate_func),
            win_len(_win_len),
            slide_len(_slide_len),
            triggering_delay(_triggering_delay),
            winType(_winType),
            name(_name),
            closing_func(_closing_func),
            context(_context),
            config(_config),
            role(_role),
            isNIC(false),
            isRich(false),
            ignored_tuples(0),
            eos_received(0),
            terminated(false),
            isRenumbering(false)
    {
        init();
    }

    // Constructor IV
    Win_Seq(rich_winupdate_func_t _rich_winupdate_func,
            uint64_t _win_len,
            uint64_t _slide_len,
            uint64_t _triggering_delay,
            win_type_t _winType,
            std::string _name,
            closing_func_t _closing_func,
            RuntimeContext _context,
            WinOperatorConfig _config,
            role_t _role):
            rich_winupdate_func(_rich_winupdate_func),
            win_len(_win_len),
            slide_len(_slide_len),
            triggering_delay(_triggering_delay),
            winType(_winType),
            name(_name),
            closing_func(_closing_func),
            context(_context),
            config(_config),
            role(_role),
            isNIC(false),
            isRich(true),
            ignored_tuples(0),
            eos_received(0),
            terminated(false),
            isRenumbering(false)
    {
        init();
    }

    // svc_init method (utilized by the FastFlow runtime)
    int svc_init() override
    {
#if defined (TRACE_WINDFLOW)
            stats_record = Stats_Record(name, std::to_string(this->get_my_id()), true, false);
#endif
        return 0;
    }

    // svc method (utilized by the FastFlow runtime)
    result_t *svc(input_t *wt) override
    {
#if defined (TRACE_WINDFLOW)
        startTS = current_time_nsecs();
        if (stats_record.inputs_received == 0) {
            startTD = current_time_nsecs();
        }
        stats_record.inputs_received++;
        stats_record.bytes_received += sizeof(tuple_t);
#endif
        // extract the key and id/timestamp fields from the input tuple
        tuple_t *t = extractTuple<tuple_t, input_t>(wt);
        auto key = std::get<0>(t->getControlFields()); // key
        size_t hashcode = std::hash<decltype(key)>()(key); // compute the hashcode of the key
        uint64_t id = (winType == win_type_t::CB) ? std::get<1>(t->getControlFields()) : std::get<2>(t->getControlFields()); // identifier or timestamp
        // access the descriptor of the input key
        auto it = keyMap.find(key);
        if (it == keyMap.end()) {
            // create the descriptor of that key
            keyMap.insert(std::make_pair(key, Key_Descriptor(compare_func, role == role_t::MAP ? map_indexes.first : 0)));
            it = keyMap.find(key);
        }
        Key_Descriptor &key_d = (*it).second;
        // check if isRenumbering is enabled (used for count-based windows in DEFAULT mode)
        if (isRenumbering) {
            assert(winType == win_type_t::CB);
            id = key_d.next_ids++;
            t->setControlFields(std::get<0>(t->getControlFields()), id, std::get<2>(t->getControlFields()));
        }
        // gwid of the first window of that key assigned to this Win_Seq node
        uint64_t first_gwid_key = ((config.id_inner - (hashcode % config.n_inner) + config.n_inner) % config.n_inner) * config.n_outer + (config.id_outer - (hashcode % config.n_outer) + config.n_outer) % config.n_outer;
        // initial identifer/timestamp of the keyed sub-stream arriving at this Win_Seq node
        uint64_t initial_outer = ((config.id_outer - (hashcode % config.n_outer) + config.n_outer) % config.n_outer) * config.slide_outer;
        uint64_t initial_inner = ((config.id_inner - (hashcode % config.n_inner) + config.n_inner) % config.n_inner) * config.slide_inner;
        uint64_t initial_id = initial_outer + initial_inner;
        // special cases: if role is WLQ or REDUCE
        if (role == role_t::WLQ || role == role_t::REDUCE) {
            initial_id = initial_inner;
        }
        // check if the tuple must be ignored
        uint64_t min_boundary = (key_d.last_lwid >= 0) ? win_len + (key_d.last_lwid  * slide_len) : 0;
        if (id < initial_id + min_boundary) {
            if (key_d.last_lwid >= 0) {
#if defined (TRACE_WINDFLOW)
                stats_record.inputs_ignored++;
#endif
                ignored_tuples++;
            }
            deleteTuple<tuple_t, input_t>(wt);
#if defined (TRACE_WINDFLOW)
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
        // determine the local identifier of the last window containing t
        long last_w = -1;
        // sliding or tumbling windows
        if (win_len >= slide_len) {
            last_w = ceil(((double) id + 1 - initial_id)/((double) slide_len)) - 1;
        }
        // hopping windows
        else {
            uint64_t n = floor((double) (id-initial_id) / slide_len);
            last_w = n;
            // if the tuple does not belong to at least one window assigned to this Win_Seq node
            if ((id-initial_id < n*(slide_len)) || (id-initial_id >= (n*slide_len)+win_len)) {
                // if it is not an EOS marker, we delete the tuple immediately
                if (!isEOSMarker<tuple_t, input_t>(*wt)) {
                    // delete the received tuple
                    deleteTuple<tuple_t, input_t>(wt);
#if defined (TRACE_WINDFLOW)
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
            }
        }
        // copy the tuple into the archive of the corresponding key
        if (!isEOSMarker<tuple_t, input_t>(*wt) && isNIC) {
            (key_d.archive).insert(*t);
        }
        auto &wins = key_d.wins;
        // create all the new windows that need to be opened by the arrival of t
        for (long lwid = key_d.next_lwid; lwid <= last_w; lwid++) {
            // translate the lwid into the corresponding gwid
            uint64_t gwid = first_gwid_key + (lwid * config.n_outer * config.n_inner);
            if (winType == win_type_t::CB) {
                wins.push_back(win_t(key, lwid, gwid, Triggerer_CB(win_len, slide_len, lwid, initial_id), win_type_t::CB, win_len, slide_len));
            }
            else {
                wins.push_back(win_t(key, lwid, gwid, Triggerer_TB(win_len, slide_len, lwid, initial_id, triggering_delay), win_type_t::TB, win_len, slide_len));
            }
            key_d.next_lwid++;
        }
        // evaluate all the open windows
        size_t cnt_fired = 0;
        for (auto &win: wins) {
            // evaluate the status of the window given the input tuple *t
            win_event_t event = win.onTuple(*t);
            if (event == win_event_t::IN) { // *t is within the window
                if (!isNIC && !isEOSMarker<tuple_t, input_t>(*wt)) {
                    // incremental query -> call rich_/winupdate_func
                    if (!isRich) {
                        winupdate_func(win.getGWID(), *t, win.getResult());
                    }
                    else {
                        rich_winupdate_func(win.getGWID(), *t, win.getResult(), context);
                    }
                }
            }
            else if (event == win_event_t::FIRED) { // window is fired
                // acquire from the archive the optionals to the first and the last tuple of the window
                std::optional<tuple_t> t_s = win.getFirstTuple();
                std::optional<tuple_t> t_e = win.getLastTuple();
                // non-incremental query -> call win_func
                if (isNIC) {
                    std::pair<input_iterator_t, input_iterator_t> its;
                    // empty window
                    if (!t_s) {
                        its.first = (key_d.archive).end();
                        its.second = (key_d.archive).end();
                    }
                    // non-empty window
                    else {
                        its = (key_d.archive).getWinRange(*t_s, *t_e);
                    }
                    Iterable<tuple_t> iter(its.first, its.second);
                    // non-incremental query -> call rich_/win_func
                    if (!isRich) {
                        win_func(win.getGWID(), iter, win.getResult());
                    }
                    else {
                        rich_win_func(win.getGWID(), iter, win.getResult(), context);
                    }
                }
                // purge the tuples from the archive (if the window is not empty)
                if (t_s) {
                    (key_d.archive).purge(*t_s);
                }
                cnt_fired++;
                key_d.last_lwid++;
                // send the result of the fired window
                result_t *out = new result_t(win.getResult());
                // special cases: role is PLQ or MAP
                if (role == role_t::MAP) {
                    out->setControlFields(key, key_d.emit_counter, std::get<2>(out->getControlFields()));
                    key_d.emit_counter += map_indexes.second;
                }
                else if (role == role_t::PLQ) {
                    uint64_t new_id = ((config.id_inner - (hashcode % config.n_inner) + config.n_inner) % config.n_inner) + (key_d.emit_counter * config.n_inner);
                    out->setControlFields(key, new_id, std::get<2>(out->getControlFields()));
                    key_d.emit_counter++;
                }
                this->ff_send_out(out);
#if defined (TRACE_WINDFLOW)
                stats_record.outputs_sent++;
                stats_record.bytes_sent += sizeof(result_t);
#endif
            }
        }
        // purge the fired windows
        wins.erase(wins.begin(), wins.begin() + cnt_fired);
        // delete the received tuple
        deleteTuple<tuple_t, input_t>(wt);
#if defined (TRACE_WINDFLOW)
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

    // method to manage the EOS (utilized by the FastFlow runtime)
    void eosnotify(ssize_t id) override
    {
        eos_received++;
        // check the number of received EOS messages
        if ((eos_received != this->get_num_inchannels()) && (this->get_num_inchannels() != 0)) { // workaround due to FastFlow
            return;
        }
        // iterate over all the keys
        for (auto &k: keyMap) {
            auto &wins = (k.second).wins;
            // iterate over all the existing windows of the key
            for (auto &win: wins) {
                // non-incremental query
                if (isNIC) {
                    // acquire from the archive the optional to the first and the last tuples of the window
                    std::optional<tuple_t> t_s = win.getFirstTuple();
                    std::optional<tuple_t> t_e = win.getLastTuple();
                    std::pair<input_iterator_t, input_iterator_t> its;
                    // empty window
                    if (!t_s) {
                        its.first = ((k.second).archive).end();
                        its.second = ((k.second).archive).end();
                    }
                    // non-empty window
                    else {
                        if (!t_e) {
                            its = ((k.second).archive).getWinRange(*t_s);
                        }
                        else {
                            its = ((k.second).archive).getWinRange(*t_s, *t_e);
                        }
                    }
                    Iterable<tuple_t> iter(its.first, its.second);
                    // non-incremental query -> call rich_/win_func
                    if (!isRich) {
                        win_func(win.getGWID(), iter, win.getResult());
                    }
                    else {
                        rich_win_func(win.getGWID(), iter, win.getResult(), context);
                    }
                }
                // send the result of the window
                result_t *out = new result_t(win.getResult());
                // special cases: role is PLQ or MAP
                if (role == role_t::MAP) {
                    out->setControlFields(k.first, (k.second).emit_counter, std::get<2>(out->getControlFields()));
                    (k.second).emit_counter += map_indexes.second;
                }
                else if (role == role_t::PLQ) {
                    size_t hashcode = std::hash<key_t>()(k.first); // compute the hashcode of the key
                    uint64_t new_id = ((config.id_inner - (hashcode % config.n_inner) + config.n_inner) % config.n_inner) + ((k.second).emit_counter * config.n_inner);
                    out->setControlFields(k.first, new_id, std::get<2>(out->getControlFields()));
                    (k.second).emit_counter++;
                }
                this->ff_send_out(out);
#if defined (TRACE_WINDFLOW)
                stats_record.outputs_sent++;
                stats_record.bytes_sent += sizeof(result_t);
#endif
            }
        }
        terminated = true;
#if defined (TRACE_WINDFLOW)
        stats_record.set_Terminated();
#endif
    }

    // svc_end method (utilized by the FastFlow runtime)
    void svc_end() override
    {
        // call the closing function
        closing_func(context);
    }

    // method to return the number of ignored tuples by this node
    size_t getNumIgnoredTuples() const
    {
        return ignored_tuples;
    }

    // method the check the termination of the replica
    bool isTerminated() const
    {
        return terminated;
    }

#if defined (TRACE_WINDFLOW)
    // method to return a copy of the Stats_Record of this node
    Stats_Record get_StatsRecord() const
    {
        return stats_record;
    }
#endif

    // method to start the node execution asynchronously
    int run(bool) override
    {
        return ff::ff_minode::run();
    }

    // method to wait the node termination
    int wait() override
    {
        return ff::ff_minode::wait();
    }
};

} // namespace wf

#endif
