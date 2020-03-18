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
 *  @brief Win_Seq operator executing a windowed query on a multi-core CPU
 *  
 *  @section Win_Seq (Description)
 *  
 *  This file implements the Win_Seq operator able to execute windowed queries on a
 *  multicore. The operator executes streaming windows in a serial fashion on a CPU
 *  core and supports both a non-incremental and an incremental query definition.
 *  
 *  The template parameters tuple_t and result_t must be default constructible, with
 *  a copy constructor and copy assignment operator, and they must provide and implement
 *  the setControlFields() and getControlFields() methods.
 */ 

#ifndef WIN_SEQ_H
#define WIN_SEQ_H

/// includes
#include <vector>
#include <string>
#include <unordered_map>
#include <math.h>
#include <ff/node.hpp>
#include <window.hpp>
#include <context.hpp>
#include <iterable.hpp>
#include <meta.hpp>
#include <stream_archive.hpp>

namespace wf {

/** 
 *  \class Win_Seq
 *  
 *  \brief Win_Seq operator executing a windowed query on a multi-core CPU
 *  
 *  This class implements the Win_Seq operator executing windowed queries on a multicore
 *  in a serial fashion.
 */ 
template<typename tuple_t, typename result_t, typename input_t>
class Win_Seq: public ff::ff_node_t<input_t, result_t>
{
public:
    /// type of the non-incremental window processing function
    using win_func_t = std::function<void(uint64_t, const Iterable<tuple_t> &, result_t &)>;
    /// type of the rich non-incremental window processing function
    using rich_win_func_t = std::function<void(uint64_t, const Iterable<tuple_t> &, result_t &, RuntimeContext &)>;
    /// type of the incremental window processing function
    using winupdate_func_t = std::function<void(uint64_t, const tuple_t &, result_t &)>;
    /// type of the rich incremental window processing function
    using rich_winupdate_func_t = std::function<void(uint64_t, const tuple_t &, result_t &, RuntimeContext &)>;
    /// type of the closing function
    using closing_func_t = std::function<void(RuntimeContext &)>;

private:
    // iterator type for accessing tuples
    using input_iterator_t = typename std::deque<tuple_t>::iterator;
    // type of the stream archive used by the Win_Seq operator
    using archive_t = StreamArchive<tuple_t, std::deque<tuple_t>>;
    // window type used by the Win_Seq operator
    using win_t = Window<tuple_t, result_t>;
    // function type to compare two tuples
    using compare_func_t = std::function<bool(const tuple_t &, const tuple_t &)>;
    tuple_t tmp; // never used
    // key data type
    using key_t = typename std::remove_reference<decltype(std::get<0>(tmp.getControlFields()))>::type;
    // friendships with other classes in the library
    template<typename T1, typename T2, typename T3>
    friend class Win_Farm;
    template<typename T1, typename T2>
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
        uint64_t next_lwid; // next window to be opened of this key (lwid)
        int64_t last_lwid; // last window closed of this key (lwid)

        // Constructor
        Key_Descriptor(compare_func_t _compare_func,
                       uint64_t _emit_counter=0):
                       archive(_compare_func),
                       emit_counter(_emit_counter),
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
    std::string name; // std::string of the unique name of the operator
    bool isNIC; // this flag is true if the operator is instantiated with a non-incremental query function
    bool isRich; // flag stating whether the function to be used is rich
    RuntimeContext context; // RuntimeContext
    OperatorConfig config; // configuration structure of the Win_Seq operator
    role_t role; // role of the Win_Seq
    std::unordered_map<key_t, Key_Descriptor> keyMap; // hash table that maps a descriptor for each key
    std::pair<size_t, size_t> map_indexes = std::make_pair(0, 1); // indexes useful is the role is MAP
    size_t dropped_tuples; // number of dropped tuples
#if defined(TRACE_WINDFLOW)
    bool isTriggering = false;
    unsigned long rcvTuples = 0;
    unsigned long rcvTuplesTriggering = 0;
    double avg_td_us = 0;
    double avg_ts_us = 0;
    double avg_ts_triggering_us = 0;
    double avg_ts_non_triggering_us = 0;
    volatile unsigned long startTD, startTS, endTD, endTS;
    std::ofstream *logfile = nullptr;
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
        if (winType == CB) {
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
    /** 
     *  \brief Constructor I
     *  
     *  \param _win_func the non-incremental window processing function
     *  \param _win_len window length (in no. of tuples or in time units)
     *  \param _slide_len slide length (in no. of tuples or in time units)
     *  \param _triggering_delay (triggering delay in time units, meaningful for TB windows only otherwise it must be 0)
     *  \param _winType window type (count-based CB or time-based TB)
     *  \param _name string with the unique name of the operator
     *  \param _closing_func closing function
     *  \param _context RuntimeContext object to be used
     *  \param _config configuration of the operator
     *  \param _role role of the operator
     */ 
    Win_Seq(win_func_t _win_func,
            uint64_t _win_len,
            uint64_t _slide_len,
            uint64_t _triggering_delay,
            win_type_t _winType,
            std::string _name,
            closing_func_t _closing_func,
            RuntimeContext _context,
            OperatorConfig _config,
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
            dropped_tuples(0)
    {
        init();
    }

    /** 
     *  \brief Constructor II
     *  
     *  \param _rich_win_func the rich non-incremental window processing function
     *  \param _win_len window length (in no. of tuples or in time units)
     *  \param _slide_len slide length (in no. of tuples or in time units)
     *  \param _triggering_delay (triggering delay in time units, meaningful for TB windows only otherwise it must be 0)
     *  \param _winType window type (count-based CB or time-based TB)
     *  \param _name string with the unique name of the operator
     *  \param _closing_func closing function
     *  \param _context RuntimeContext object to be used
     *  \param _config configuration of the operator
     *  \param _role role of the operator
     */ 
    Win_Seq(rich_win_func_t _rich_win_func,
            uint64_t _win_len,
            uint64_t _slide_len,
            uint64_t _triggering_delay,
            win_type_t _winType,
            std::string _name,
            closing_func_t _closing_func,
            RuntimeContext _context,
            OperatorConfig _config,
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
            dropped_tuples(0)
    {
        init();
    }

    /** 
     *  \brief Constructor III
     *  
     *  \param _winupdate_func the incremental window processing function
     *  \param _win_len window length (in no. of tuples or in time units)
     *  \param _slide_len slide length (in no. of tuples or in time units)
     *  \param _triggering_delay (triggering delay in time units, meaningful for TB windows only otherwise it must be 0)
     *  \param _winType window type (count-based CB or time-based TB)
     *  \param _name string with the unique name of the operator
     *  \param _closing_func closing function
     *  \param _context RuntimeContext object to be used
     *  \param _config configuration of the operator
     *  \param _role role of the operator
     */ 
    Win_Seq(winupdate_func_t _winupdate_func,
            uint64_t _win_len,
            uint64_t _slide_len,
            uint64_t _triggering_delay,
            win_type_t _winType,
            std::string _name,
            closing_func_t _closing_func,
            RuntimeContext _context,
            OperatorConfig _config,
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
            dropped_tuples(0)
    {
        init();
    }

    /** 
     *  \brief Constructor IV
     *  
     *  \param _rich_winupdate_func the rich incremental window processing function
     *  \param _win_len window length (in no. of tuples or in time units)
     *  \param _slide_len slide length (in no. of tuples or in time units)
     *  \param _triggering_delay (triggering delay in time units, meaningful for TB windows only otherwise it must be 0)
     *  \param _winType window type (count-based CB or time-based TB)
     *  \param _name string with the unique name of the operator
     *  \param _closing_func closing function
     *  \param _context RuntimeContext object to be used
     *  \param _config configuration of the operator
     *  \param _role role of the operator
     */ 
    Win_Seq(rich_winupdate_func_t _rich_winupdate_func,
            uint64_t _win_len,
            uint64_t _slide_len,
            uint64_t _triggering_delay,
            win_type_t _winType,
            std::string _name,
            closing_func_t _closing_func,
            RuntimeContext _context,
            OperatorConfig _config,
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
            dropped_tuples(0)
    {
        init();
    }

//@cond DOXY_IGNORE

    // svc_init method (utilized by the FastFlow runtime)
    int svc_init()
    {
#if defined(TRACE_WINDFLOW)
        logfile = new std::ofstream();
        name += "_" + std::to_string(this->get_my_id()) + "_" + std::to_string(getpid()) + ".log";
#if defined(LOG_DIR)
        std::string filename = std::string(STRINGIFY(LOG_DIR)) + "/" + name;
        std::string log_dir = std::string(STRINGIFY(LOG_DIR));
#else
        std::string filename = "log/" + name;
        std::string log_dir = std::string("log");
#endif
        // create the log directory
        if (mkdir(log_dir.c_str(), 0777) != 0) {
            struct stat st;
            if((stat(log_dir.c_str(), &st) != 0) || !S_ISDIR(st.st_mode)) {
                std::cerr << RED << "WindFlow Error: directory for log files cannot be created" << DEFAULT_COLOR << std::endl;
                exit(EXIT_FAILURE);
            }
        }
        logfile->open(filename);
#endif
        return 0;
    }

    // svc method (utilized by the FastFlow runtime)
    result_t *svc(input_t *wt)
    {
#if defined(TRACE_WINDFLOW)
        startTS = current_time_nsecs();
        if (rcvTuples == 0)
            startTD = current_time_nsecs();
        rcvTuples++;
#endif
        // extract the key and id/timestamp fields from the input tuple
        tuple_t *t = extractTuple<tuple_t, input_t>(wt);
        auto key = std::get<0>(t->getControlFields()); // key
        size_t hashcode = std::hash<decltype(key)>()(key); // compute the hashcode of the key
        uint64_t id = (winType == CB) ? std::get<1>(t->getControlFields()) : std::get<2>(t->getControlFields()); // identifier or timestamp
        // access the descriptor of the input key
        auto it = keyMap.find(key);
        if (it == keyMap.end()) {
            // create the descriptor of that key
            keyMap.insert(std::make_pair(key, Key_Descriptor(compare_func, role == MAP ? map_indexes.first : 0)));
            it = keyMap.find(key);
        }
        Key_Descriptor &key_d = (*it).second;
        // gwid of the first window of that key assigned to this Win_Seq
        uint64_t first_gwid_key = ((config.id_inner - (hashcode % config.n_inner) + config.n_inner) % config.n_inner) * config.n_outer + (config.id_outer - (hashcode % config.n_outer) + config.n_outer) % config.n_outer;
        // initial identifer/timestamp of the keyed sub-stream arriving at this Win_Seq
        uint64_t initial_outer = ((config.id_outer - (hashcode % config.n_outer) + config.n_outer) % config.n_outer) * config.slide_outer;
        uint64_t initial_inner = ((config.id_inner - (hashcode % config.n_inner) + config.n_inner) % config.n_inner) * config.slide_inner;
        uint64_t initial_id = initial_outer + initial_inner;
        // special cases: if role is WLQ or REDUCE
        if (role == WLQ || role == REDUCE)
            initial_id = initial_inner;
        // check if the tuple must be dropped
        uint64_t min_boundary = (key_d.last_lwid >= 0) ? win_len + (key_d.last_lwid  * slide_len) : 0;
        if (id < initial_id + min_boundary) {
            if (key_d.last_lwid >= 0)
                dropped_tuples++;
            deleteTuple<tuple_t, input_t>(wt);
            return this->GO_ON;
        }
        // determine the local identifier of the last window containing t
        long last_w = -1;
        // sliding or tumbling windows
        if (win_len >= slide_len)
            last_w = ceil(((double) id + 1 - initial_id)/((double) slide_len)) - 1;
        // hopping windows
        else {
            uint64_t n = floor((double) (id-initial_id) / slide_len);
            last_w = n;
            // if the tuple does not belong to at least one window assigned to this Win_Seq
            if ((id-initial_id < n*(slide_len)) || (id-initial_id >= (n*slide_len)+win_len)) {
                // if it is not an EOS marker, we delete the tuple immediately
                if (!isEOSMarker<tuple_t, input_t>(*wt)) {
                    // delete the received tuple
                    deleteTuple<tuple_t, input_t>(wt);
                    return this->GO_ON;
                }
            }
        }
        // copy the tuple into the archive of the corresponding key
        if (!isEOSMarker<tuple_t, input_t>(*wt) && isNIC)
            (key_d.archive).insert(*t);
        auto &wins = key_d.wins;
        // create all the new windows that need to be opened by the arrival of t
        for (long lwid = key_d.next_lwid; lwid <= last_w; lwid++) {
            // translate the lwid into the corresponding gwid
            uint64_t gwid = first_gwid_key + (lwid * config.n_outer * config.n_inner);
            if (winType == CB)
                wins.push_back(win_t(key, lwid, gwid, Triggerer_CB(win_len, slide_len, lwid, initial_id), CB, win_len, slide_len));
            else
                wins.push_back(win_t(key, lwid, gwid, Triggerer_TB(win_len, slide_len, lwid, initial_id, triggering_delay), TB, win_len, slide_len));
            key_d.next_lwid++;
        }
        // evaluate all the open windows
        size_t cnt_fired = 0;
        for (auto &win: wins) {
            // evaluate the status of the window given the input tuple *t
            win_event_t event = win.onTuple(*t);
            if (event == IN) { // *t is within the window
                if (!isNIC && !isEOSMarker<tuple_t, input_t>(*wt)) {
                    // incremental query -> call rich_/winupdate_func
                    if (!isRich)
                        winupdate_func(win.getGWID(), *t, win.getResult());
                    else
                        rich_winupdate_func(win.getGWID(), *t, win.getResult(), context);
                }
            }
            else if (event == FIRED) { // window is fired
                // acquire from the archive the optionals to the first and the last tuple of the window
                std::optional<tuple_t> t_s = win.getFirstTuple();
                std::optional<tuple_t> t_e = win.getLastTuple();
                // non-incremental query -> call win_func
                if (isNIC) {
#if defined(TRACE_WINDFLOW)
                    if (!isTriggering) {
                        rcvTuplesTriggering++;
                    }
                    isTriggering = true;
#endif
                    std::pair<input_iterator_t, input_iterator_t> its;
                    // empty window
                    if (!t_s) {
                        its.first = (key_d.archive).end();
                        its.second = (key_d.archive).end();
                    }
                    // non-empty window
                    else
                        its = (key_d.archive).getWinRange(*t_s, *t_e);
                    Iterable<tuple_t> iter(its.first, its.second);
                    // non-incremental query -> call rich_/win_func
                    if (!isRich)
                        win_func(win.getGWID(), iter, win.getResult());
                    else
                        rich_win_func(win.getGWID(), iter, win.getResult(), context);
                }
                // purge the tuples from the archive (if the window is not empty)
                if (t_s)
                    (key_d.archive).purge(*t_s);
                cnt_fired++;
                key_d.last_lwid++;
                // send the result of the fired window
                result_t *out = new result_t(win.getResult());
                // special cases: role is PLQ or MAP
                if (role == MAP) {
                    out->setControlFields(key, key_d.emit_counter, std::get<2>(out->getControlFields()));
                    key_d.emit_counter += map_indexes.second;
                }
                else if (role == PLQ) {
                    uint64_t new_id = ((config.id_inner - (hashcode % config.n_inner) + config.n_inner) % config.n_inner) + (key_d.emit_counter * config.n_inner);
                    out->setControlFields(key, new_id, std::get<2>(out->getControlFields()));
                    key_d.emit_counter++;
                }
                this->ff_send_out(out);
            }
        }
        // purge the fired windows
        wins.erase(wins.begin(), wins.begin() + cnt_fired);
        // delete the received tuple
        deleteTuple<tuple_t, input_t>(wt);
#if defined(TRACE_WINDFLOW)
        endTS = current_time_nsecs();
        endTD = current_time_nsecs();
        double elapsedTS_us = ((double) (endTS - startTS)) / 1000;
        avg_ts_us += (1.0 / rcvTuples) * (elapsedTS_us - avg_ts_us);
        if (isNIC) {
            if (isTriggering)
                avg_ts_triggering_us += (1.0 / rcvTuplesTriggering) * (elapsedTS_us - avg_ts_triggering_us);
            else
                avg_ts_non_triggering_us += (1.0 / (rcvTuples - rcvTuplesTriggering)) * (elapsedTS_us - avg_ts_non_triggering_us);
            isTriggering = false;
        }
        double elapsedTD_us = ((double) (endTD - startTD)) / 1000;
        avg_td_us += (1.0 / rcvTuples) * (elapsedTD_us - avg_td_us);
        startTD = current_time_nsecs();
#endif
        return this->GO_ON;
    }

    // method to manage the EOS (utilized by the FastFlow runtime)
    void eosnotify(ssize_t id)
    {
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
                        if (!t_e)
                            its = ((k.second).archive).getWinRange(*t_s);
                        else
                            its = ((k.second).archive).getWinRange(*t_s, *t_e);
                    }
                    Iterable<tuple_t> iter(its.first, its.second);
                    // non-incremental query -> call rich_/win_func
                    if (!isRich)
                        win_func(win.getGWID(), iter, win.getResult());
                    else
                        rich_win_func(win.getGWID(), iter, win.getResult(), context);
                }
                // send the result of the window
                result_t *out = new result_t(win.getResult());
                // special cases: role is PLQ or MAP
                if (role == MAP) {
                    out->setControlFields(k.first, (k.second).emit_counter, std::get<2>(out->getControlFields()));
                    (k.second).emit_counter += map_indexes.second;
                }
                else if (role == PLQ) {
                    size_t hashcode = std::hash<key_t>()(k.first); // compute the hashcode of the key
                    uint64_t new_id = ((config.id_inner - (hashcode % config.n_inner) + config.n_inner) % config.n_inner) + ((k.second).emit_counter * config.n_inner);
                    out->setControlFields(k.first, new_id, std::get<2>(out->getControlFields()));
                    (k.second).emit_counter++;
                }
                this->ff_send_out(out);
            }
        }
    }

    // svc_end method (utilized by the FastFlow runtime)
    void svc_end()
    {
        // call the closing function
        closing_func(context);
#if defined(TRACE_WINDFLOW)
        std::ostringstream stream;
        if (!isNIC) {
            stream << "************************************LOG************************************\n";
            stream << "No. of received tuples: " << rcvTuples << "\n";
            stream << "Average service time: " << avg_ts_us << " usec \n";
            stream << "Average inter-departure time: " << avg_td_us << " usec \n";
            stream << "Dropped tuples: " << dropped_tuples << "\n";
            stream << "***************************************************************************\n";
        }
        else {
            stream << "************************************LOG************************************\n";
            stream << "No. of received tuples: " << rcvTuples << "\n";
            stream << "No. of received tuples (triggering): " << rcvTuplesTriggering << "\n";
            stream << "Average service time: " << avg_ts_us << " usec \n";
            stream << "Average service time (triggering): " << avg_ts_triggering_us << " usec \n";
            stream << "Average service time (non triggering): " << avg_ts_non_triggering_us << " usec \n";
            stream << "Average inter-departure time: " << avg_td_us << " usec \n";
            stream << "Dropped tuples: " << dropped_tuples << "\n";
            stream << "***************************************************************************\n";
        }
        *logfile << stream.str();
        logfile->close();
        delete logfile;
#endif
    }

//@endcond

    /** 
     *  \brief Get the window type (CB or TB) utilized by the operator
     *  \return adopted windowing semantics (count- or time-based)
     */
    win_type_t getWinType() const
    {
        return winType;
    }

    /** 
     *  \brief Get the number of dropped tuples by the Win_Seq
     *  \return number of tuples dropped during the processing by the Win_Seq
     */ 
    size_t getNumDroppedTuples() const
    {
        return dropped_tuples;
    }

    /// Method to start the operator execution asynchronously
    virtual int run(bool)
    {
        return ff::ff_node::run();
    }

    /// Method to wait the operator termination
    virtual int wait()
    {
        return ff::ff_node::wait();
    }
};

} // namespace wf

#endif
