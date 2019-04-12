/******************************************************************************
 *  This program is free software; you can redistribute it and/or modify it
 *  under the terms of the GNU Lesser General Public License version 3 as
 *  published by the Free Software Foundation.
 *  
 *  This program is distributed in the hope that it will be useful, but WITHOUT
 *  ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 *  FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public
 *  License for more details
 *  
 *  You should have received a copy of the GNU Lesser General Public License
 *  along with this program; if not, write to the Free Software Foundation,
 *  Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 ******************************************************************************
 */

/** 
 *  @file    pane_farm.hpp
 *  @author  Gabriele Mencagli
 *  @date    17/10/2017
 *  
 *  @brief Pane_Farm pattern executing a windowed transformation in parallel on multi-core CPUs
 *  
 *  @section Pane_Farm (Description)
 *  
 *  This file implements the Pane_Farm pattern able to execute windowed queries on a multicore.
 *  The pattern processes (possibly in parallel) panes of the windows in the so-called PLQ stage
 *  (Pane-Level Sub-Query) and computes (possibly in parallel) results of the windows from the
 *  pane results in the so-called WLQ stage (Window-Level Sub-Query). Panes shared by more than
 *  one window are not recomputed by saving processing time. The pattern supports both a
 *  non-incremental and an incremental query definition in the two stages.
 *  
 *  The template arguments tuple_t and result_t must be default constructible, with a copy constructor
 *  and copy assignment operator, and they must provide and implement the setInfo() and
 *  getInfo() methods.
 */ 

#ifndef PANE_FARM_H
#define PANE_FARM_H

// includes
#include <ff/combine.hpp>
#include <ff/pipeline.hpp>
#include <win_farm.hpp>
#include <orderingNode.hpp>

/** 
 *  \class Pane_Farm
 *  
 *  \brief Pane_Farm pattern executing a windowed transformation in parallel on multi-core CPUs
 *  
 *  This class implements the Pane_Farm pattern executing windowed queries in parallel on
 *  a multicore. The pattern processes (possibly in parallel) panes in the PLQ stage while
 *  window results are built out from the pane results (possibly in parallel) in the WLQ
 *  stage.
 */ 
template<typename tuple_t, typename result_t, typename input_t>
class Pane_Farm: public ff_pipeline
{
public:
    /// function type of the non-incremental pane processing
    using f_plqfunction_t = function<int(size_t, uint64_t, Iterable<tuple_t> &, result_t &)>;
    /// function type of the incremental pane processing
    using f_plqupdate_t = function<int(size_t, uint64_t, const tuple_t &, result_t &)>;
    /// function type of the non-incremental window processing
    using f_wlqfunction_t = function<int(size_t, uint64_t, Iterable<result_t> &, result_t &)>;
    /// function type of the incremental window function
    using f_wlqupdate_t = function<int(size_t, uint64_t, const result_t &, result_t &)>;
private:
    // friendships with other classes in the library
    template<typename T1, typename T2, typename T3>
    friend class Win_Farm;
    template<typename T1, typename T2, typename T3>
    friend class Key_Farm;
    template<typename T>
    friend class WinFarm_Builder;
    template<typename T>
    friend class KeyFarm_Builder;
    // compute the gcd between two numbers
    function<uint64_t(uint64_t, uint64_t)> gcd = [](uint64_t u, uint64_t v) {
        while (v != 0) {
            unsigned long r = u % v;
            u = v;
            v = r;
        }
        return u;
    };
    // configuration variables of the Pane_Farm
    f_plqfunction_t plqFunction;
    f_plqupdate_t plqUpdate;
    f_wlqfunction_t wlqFunction;
    f_wlqupdate_t wlqUpdate;
    bool isNICPLQ;
    bool isNICWLQ;
    uint64_t win_len;
    uint64_t slide_len;
    win_type_t winType;
    size_t plq_degree;
    size_t wlq_degree;
    string name;
    bool ordered;
    opt_level_t opt_level;
    PatternConfig config;

    // private constructor I (non-incremental PLQ stage and non-incremental WLQ stage)
    Pane_Farm(f_plqfunction_t _plqFunction,
              f_wlqfunction_t _wlqFunction,
              uint64_t _win_len,
              uint64_t _slide_len,
              win_type_t _winType,
              size_t _plq_degree,
              size_t _wlq_degree,
              string _name,
              bool _ordered,
              opt_level_t _opt_level,
              PatternConfig _config)
              :
              plqFunction(_plqFunction),
              wlqFunction(_wlqFunction),
              isNICPLQ(true),
              isNICWLQ(true),
              win_len(_win_len),
              slide_len(_slide_len),
              winType(_winType),
              plq_degree(_plq_degree),
              wlq_degree(_wlq_degree),
              name(_name),
              ordered(_ordered),
              opt_level(_opt_level),
              config(_config)
    {
        // check the validity of the windowing parameters
        if (_win_len == 0 || _slide_len == 0) {
            cerr << RED << "WindFlow Error: window length or slide cannot be zero" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // check the validity of the parallelism degrees
        if (_plq_degree == 0 || _wlq_degree == 0) {
            cerr << RED << "WindFlow Error: parallelism degrees cannot be zero" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // the Pane_Farm can be utilized with sliding windows only
        if (_win_len <= _slide_len) {
            cerr << RED << "WindFlow Error: Pane_Farm can be used with sliding windows only (s<w)" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // compute the pane length (no. of tuples or in time units)
        uint64_t _pane_len = gcd(_win_len, _slide_len);
        // general fastflow pointers to the PLQ and WLQ stages
        ff_node *plq_stage, *wlq_stage;
        // create the first stage PLQ (Pane Level Query)
        if (_plq_degree > 1) {
            // configuration structure of the Win_Farm instance (PLQ)
            PatternConfig configWFPLQ(_config.id_outer, _config.n_outer, _config.slide_outer, _config.id_inner, _config.n_inner, _config.slide_inner);
            auto *plq_wf = new Win_Farm<tuple_t, result_t, input_t>(_plqFunction, _pane_len, _pane_len, _winType, 1, _plq_degree, _name + "_plq", true, LEVEL0, configWFPLQ, PLQ);
            plq_stage = plq_wf;
        }
        else {
            // configuration structure of the Win_Seq instance (PLQ)
            PatternConfig configSeqPLQ(_config.id_inner, _config.n_inner, _config.slide_inner, 0, 1, _pane_len);
            auto *plq_seq = new Win_Seq<tuple_t, result_t, input_t>(_plqFunction, _pane_len, _pane_len, _winType, _name + "_plq", configSeqPLQ, PLQ);
            plq_stage = plq_seq;
        }
        // create the second stage WLQ (Window Level Query)
        if (_wlq_degree > 1) {
            // configuration structure of the Win_Farm instance (WLQ)
            PatternConfig configWFWLQ(_config.id_outer, _config.n_outer, _config.slide_outer, _config.id_inner, _config.n_inner, _config.slide_inner);
            auto *wlq_wf = new Win_Farm<result_t, result_t>(_wlqFunction, (_win_len/_pane_len), (_slide_len/_pane_len), CB, 1, _wlq_degree, _name + "_wlq", _ordered, LEVEL0, configWFWLQ, WLQ);
            wlq_stage = wlq_wf;
        }
        else {
            // configuration structure of the Win_Seq instance (WLQ)
            PatternConfig configSeqWLQ(_config.id_inner, _config.n_inner, _config.slide_inner, 0, 1, (_slide_len/_pane_len));
            auto *wlq_seq = new Win_Seq<result_t, result_t>(_wlqFunction, (_win_len/_pane_len), (_slide_len/_pane_len), CB, _name + "_wlq", configSeqWLQ, WLQ);
            wlq_stage = wlq_seq;
        }
        // add to this the pipeline optimized according to the provided optimization level
        ff_pipeline::add_stage(optimize_PaneFarm(plq_stage, wlq_stage, _opt_level));
        // when the Pane_Farm will be destroyed we need also to destroy the two stages
        ff_pipeline::cleanup_nodes();
        // flatten the pipeline
        ff_pipeline::flatten();
    }

    // private constructor II (incremental PLQ stage and incremental WLQ stage)
    Pane_Farm(f_plqupdate_t _plqUpdate,
              f_wlqupdate_t _wlqUpdate,
              uint64_t _win_len,
              uint64_t _slide_len,
              win_type_t _winType,
              size_t _plq_degree,
              size_t _wlq_degree,
              string _name,
              bool _ordered,
              opt_level_t _opt_level,
              PatternConfig _config)
              :
              plqUpdate(_plqUpdate),
              wlqUpdate(_wlqUpdate),
              isNICPLQ(false),
              isNICWLQ(false),
              win_len(_win_len),
              slide_len(_slide_len),
              winType(_winType),
              plq_degree(_plq_degree),
              wlq_degree(_wlq_degree),
              name(_name),
              ordered(_ordered),
              opt_level(_opt_level),
              config(_config)
    {
        // check the validity of the windowing parameters
        if (_win_len == 0 || _slide_len == 0) {
            cerr << RED << "WindFlow Error: window length or slide cannot be zero" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // check the validity of the parallelism degrees
        if (_plq_degree == 0 || _wlq_degree == 0) {
            cerr << RED << "WindFlow Error: parallelism degrees cannot be zero" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // the Pane_Farm can be utilized with sliding windows only
        if (_win_len <= _slide_len) {
            cerr << RED << "WindFlow Error: Pane_Farm can be used with sliding windows only (s<w)" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // compute the pane length (no. of tuples or in time units)
        uint64_t _pane_len = gcd(_win_len, _slide_len);
        // general fastflow pointers to the PLQ and WLQ stages
        ff_node *plq_stage, *wlq_stage;
        // create the first stage PLQ
        if (_plq_degree > 1) {
            // configuration structure of the Win_Farm instance (PLQ)
            PatternConfig configWFPLQ(_config.id_outer, _config.n_outer, _config.slide_outer, _config.id_inner, _config.n_inner, _config.slide_inner);
            auto *plq_wf = new Win_Farm<tuple_t, result_t, input_t>(_plqUpdate, _pane_len, _pane_len, _winType, 1, _plq_degree, _name + "_plq", true, LEVEL0, configWFPLQ, PLQ);
            plq_stage = plq_wf;
        }
        else {
            // configuration structure of the Win_Seq instance (PLQ)
            PatternConfig configSeqPLQ(_config.id_inner, _config.n_inner, _config.slide_inner, 0, 1, _pane_len);
            auto *plq_seq = new Win_Seq<tuple_t, result_t, input_t>(_plqUpdate, _pane_len, _pane_len, _winType, _name + "_plq", configSeqPLQ, PLQ);
            plq_stage = plq_seq;
        }
        // create the second stage WLQ
        if (_wlq_degree > 1) {
            // configuration structure of the Win_Farm instance (WLQ)
            PatternConfig configWFWLQ(_config.id_outer, _config.n_outer, _config.slide_outer, _config.id_inner, _config.n_inner, _config.slide_inner);
            auto *wlq_wf = new Win_Farm<result_t, result_t>(_wlqUpdate, _win_len/_pane_len, _slide_len/_pane_len, CB, 1, _wlq_degree, _name + "_wlq", _ordered, LEVEL0, configWFWLQ, WLQ);
            wlq_stage = wlq_wf;
        }
        else {
            // configuration structure of the Win_Seq instance (WLQ)
            PatternConfig configSeqWLQ(_config.id_inner, _config.n_inner, _config.slide_inner, 0, 1, (_slide_len/_pane_len));
            auto *wlq_seq = new Win_Seq<result_t, result_t>(_wlqUpdate, _win_len/_pane_len, _slide_len/_pane_len, CB, _name + "_wlq", configSeqWLQ, WLQ);
            wlq_stage = wlq_seq;
        }
        // add to this the pipeline optimized according to the provided optimization level
        ff_pipeline::add_stage(optimize_PaneFarm(plq_stage, wlq_stage, _opt_level));
        // when the Pane_Farm will be destroyed we need aslo to destroy the two stages
        ff_pipeline::cleanup_nodes();
        // flatten the pipeline
        ff_pipeline::flatten();
    }

    // private constructor III (non-incremental PLQ stage and incremental WLQ stage)
    Pane_Farm(f_plqfunction_t _plqFunction,
              f_wlqupdate_t _wlqUpdate,
              uint64_t _win_len,
              uint64_t _slide_len,
              win_type_t _winType,
              size_t _plq_degree,
              size_t _wlq_degree,
              string _name,
              bool _ordered,
              opt_level_t _opt_level,
              PatternConfig _config)
              :
              plqFunction(_plqFunction),
              wlqUpdate(_wlqUpdate),
              isNICPLQ(true),
              isNICWLQ(false),
              win_len(_win_len),
              slide_len(_slide_len),
              winType(_winType),
              plq_degree(_plq_degree),
              wlq_degree(_wlq_degree),
              name(_name),
              ordered(_ordered),
              opt_level(_opt_level),
              config(_config)
    {
        // check the validity of the windowing parameters
        if (_win_len == 0 || _slide_len == 0) {
            cerr << RED << "WindFlow Error: window length or slide cannot be zero" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // check the validity of the parallelism degrees
        if (_plq_degree == 0 || _wlq_degree == 0) {
            cerr << RED << "WindFlow Error: parallelism degrees cannot be zero" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // the Pane_Farm can be utilized with sliding windows only
        if (_win_len <= _slide_len) {
            cerr << RED << "WindFlow Error: Pane_Farm can be used with sliding windows only (s<w)" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // compute the pane length (no. of tuples or in time units)
        uint64_t _pane_len = gcd(_win_len, _slide_len);
        // general fastflow pointers to the PLQ and WLQ stages
        ff_node *plq_stage, *wlq_stage;
        // create the first stage PLQ (Pane Level Query)
        if (_plq_degree > 1) {
            // configuration structure of the Win_Farm instance (PLQ)
            PatternConfig configWFPLQ(_config.id_outer, _config.n_outer, _config.slide_outer, _config.id_inner, _config.n_inner, _config.slide_inner);
            auto *plq_wf = new Win_Farm<tuple_t, result_t, input_t>(_plqFunction, _pane_len, _pane_len, _winType, 1, _plq_degree, _name + "_plq", true, LEVEL0, configWFPLQ, PLQ);
            plq_stage = plq_wf;
        }
        else {
            // configuration structure of the Win_Seq instance (PLQ)
            PatternConfig configSeqPLQ(_config.id_inner, _config.n_inner, _config.slide_inner, 0, 1, _pane_len);
            auto *plq_seq = new Win_Seq<tuple_t, result_t, input_t>(_plqFunction, _pane_len, _pane_len, _winType, _name + "_plq", configSeqPLQ, PLQ);
            plq_stage = plq_seq;
        }
        // create the second stage WLQ (Window Level Query)
        if (_wlq_degree > 1) {
            // configuration structure of the Win_Farm instance (WLQ)
            PatternConfig configWFWLQ(_config.id_outer, _config.n_outer, _config.slide_outer, _config.id_inner, _config.n_inner, _config.slide_inner);
            auto *wlq_wf = new Win_Farm<result_t, result_t>(_wlqUpdate, _win_len/_pane_len, _slide_len/_pane_len, CB, 1, _wlq_degree, _name + "_wlq", _ordered, LEVEL0, configWFWLQ, WLQ);
            wlq_stage = wlq_wf;
        }
        else {
            // configuration structure of the Win_Seq instance (WLQ)
            PatternConfig configSeqWLQ(_config.id_inner, _config.n_inner, _config.slide_inner, 0, 1, (_slide_len/_pane_len));
            auto *wlq_seq = new Win_Seq<result_t, result_t>(_wlqUpdate, _win_len/_pane_len, _slide_len/_pane_len, CB, _name + "_wlq", configSeqWLQ, WLQ);
            wlq_stage = wlq_seq;
        }
        // add to this the pipeline optimized according to the provided optimization level
        ff_pipeline::add_stage(optimize_PaneFarm(plq_stage, wlq_stage, _opt_level));
        // when the Pane_Farm will be destroyed we need aslo to destroy the two stages
        ff_pipeline::cleanup_nodes();
        // flatten the pipeline
        ff_pipeline::flatten();
    }

    // private constructor IV (incremental PLQ stage and non-incremental WLQ stage)
    Pane_Farm(f_plqupdate_t _plqUpdate,
              f_wlqfunction_t _wlqFunction,
              uint64_t _win_len,
              uint64_t _slide_len,
              win_type_t _winType,
              size_t _plq_degree,
              size_t _wlq_degree,
              string _name,
              bool _ordered,
              opt_level_t _opt_level,
              PatternConfig _config)
              :
              plqUpdate(_plqUpdate),
              wlqFunction(_wlqFunction),
              isNICPLQ(false),
              isNICWLQ(true),
              win_len(_win_len),
              slide_len(_slide_len),
              winType(_winType),
              plq_degree(_plq_degree),
              wlq_degree(_wlq_degree),
              name(_name),
              ordered(_ordered),
              opt_level(_opt_level),
              config(_config)
    {
        // check the validity of the windowing parameters
        if (_win_len == 0 || _slide_len == 0) {
            cerr << RED << "WindFlow Error: window length or slide cannot be zero" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // check the validity of the parallelism degrees
        if (_plq_degree == 0 || _wlq_degree == 0) {
            cerr << RED << "WindFlow Error: parallelism degrees cannot be zero" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // the Pane_Farm can be utilized with sliding windows only
        if (_win_len <= _slide_len) {
            cerr << RED << "WindFlow Error: Pane_Farm can be used with sliding windows only (s<w)" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // compute the pane length (no. of tuples or in time units)
        uint64_t _pane_len = gcd(_win_len, _slide_len);
        // general fastflow pointers to the PLQ and WLQ stages
        ff_node *plq_stage, *wlq_stage;
        // create the first stage PLQ (Pane Level Query)
        if (_plq_degree > 1) {
            // configuration structure of the Win_Farm instance (PLQ)
            PatternConfig configWFPLQ(_config.id_outer, _config.n_outer, _config.slide_outer, _config.id_inner, _config.n_inner, _config.slide_inner);
            auto *plq_wf = new Win_Farm<tuple_t, result_t, input_t>(_plqUpdate, _pane_len, _pane_len, _winType, 1, _plq_degree, _name + "_plq", true, LEVEL0, configWFPLQ, PLQ);
            plq_stage = plq_wf;
        }
        else {
            // configuration structure of the Win_Seq instance (PLQ)
            PatternConfig configSeqPLQ(_config.id_inner, _config.n_inner, _config.slide_inner, 0, 1, _pane_len);
            auto *plq_seq = new Win_Seq<tuple_t, result_t, input_t>(_plqUpdate, _pane_len, _pane_len, _winType, _name + "_plq", configSeqPLQ, PLQ);
            plq_stage = plq_seq;
        }
        // create the second stage WLQ (Window Level Query)
        if (_wlq_degree > 1) {
            // configuration structure of the Win_Farm instance (WLQ)
            PatternConfig configWFWLQ(_config.id_outer, _config.n_outer, _config.slide_outer, _config.id_inner, _config.n_inner, _config.slide_inner);
            auto *wlq_wf = new Win_Farm<result_t, result_t>(_wlqFunction, _win_len/_pane_len, _slide_len/_pane_len, CB, 1, _wlq_degree, _name + "_wlq", _ordered, LEVEL0, configWFWLQ, WLQ);
            wlq_stage = wlq_wf;
        }
        else {
            // configuration structure of the Win_Seq instance (WLQ)
            PatternConfig configSeqWLQ(_config.id_inner, _config.n_inner, _config.slide_inner, 0, 1, (_slide_len/_pane_len));
            auto *wlq_seq = new Win_Seq<result_t, result_t>(_wlqFunction, _win_len/_pane_len, _slide_len/_pane_len, CB, _name + "_wlq", configSeqWLQ, WLQ);
            wlq_stage = wlq_seq;
        }
        // add to this the pipeline optimized according to the provided optimization level
        ff_pipeline::add_stage(optimize_PaneFarm(plq_stage, wlq_stage, _opt_level));
        // when the Pane_Farm will be destroyed we need aslo to destroy the two stages
        ff_pipeline::cleanup_nodes();
        // flatten the pipeline
        ff_pipeline::flatten();
    }

    // method to optimize the structure of the Pane_Farm pattern
    const ff_pipeline optimize_PaneFarm(ff_node *plq, ff_node *wlq, opt_level_t opt)
    {
        if (opt == LEVEL0) { // no optimization
            ff_pipeline pipe;
            pipe.add_stage(plq);
            pipe.add_stage(wlq);
            pipe.cleanup_nodes();
            return pipe;
        }
        else if (opt == LEVEL1) { // optimization level 1
            if (plq_degree == 1 && wlq_degree == 1) {
                ff_pipeline pipe;
                pipe.add_stage(new ff_comb(plq, wlq, true, true));
                pipe.cleanup_nodes();
                return pipe;
            }
            else return combine_nodes_in_pipeline(*plq, *wlq, true, true);
        }
        else { // optimization level 2
            if (!plq->isFarm() || !wlq->isFarm()) // like level 1
                if (plq_degree == 1 && wlq_degree == 1) {
                    ff_pipeline pipe;
                    pipe.add_stage(new ff_comb(plq, wlq, true, true));
                    pipe.cleanup_nodes();
                    return pipe;
                }
                else return combine_nodes_in_pipeline(*plq, *wlq, true, true);
            else {
                using emitter_wlq_t = WF_Emitter<result_t, result_t>;
                ff_farm *farm_plq = static_cast<ff_farm *>(plq);
                ff_farm *farm_wlq = static_cast<ff_farm *>(wlq);
                emitter_wlq_t *emitter_wlq = static_cast<emitter_wlq_t *>(farm_wlq->getEmitter());
                OrderingNode<result_t, wrapper_tuple_t<result_t>> *buf_node = new OrderingNode<result_t, wrapper_tuple_t<result_t>>();
                const ff_pipeline result = combine_farms(*farm_plq, emitter_wlq, *farm_wlq, buf_node, false);
                delete farm_plq;
                delete farm_wlq;
                delete buf_node;
                return result;
            }
        }
    }

public:
    /** 
     *  \brief Constructor I (Non-Incremental PLQ stage and Non-Incremental WLQ stage)
     *  
     *  \param _plqFunction the non-incremental pane processing function (PLQ)
     *  \param _wlqFunction the non-incremental window processing function (WLQ)
     *  \param _win_len window length (in no. of tuples or in time units)
     *  \param _slide_len slide length (in no. of tuples or in time units)
     *  \param _winType window type (count-based CB or time-based TB)
     *  \param _plq_degree parallelism degree of the PLQ stage
     *  \param _wlq_degree parallelism degree of the WLQ stage
     *  \param _name string with the unique name of the pattern
     *  \param _ordered true if the results of the same key must be emitted in order, false otherwise
     *  \param _opt_level optimization level used to build the pattern
     */ 
    Pane_Farm(f_plqfunction_t _plqFunction,
              f_wlqfunction_t _wlqFunction,
              uint64_t _win_len,
              uint64_t _slide_len,
              win_type_t _winType,
              size_t _plq_degree,
              size_t _wlq_degree,
              string _name,
              bool _ordered=true,
              opt_level_t _opt_level=LEVEL0)
              :
              Pane_Farm(_plqFunction, _wlqFunction, _win_len, _slide_len, _winType, _plq_degree, _wlq_degree, _name, _ordered, _opt_level, PatternConfig(0, 1, _slide_len, 0, 1, _slide_len)) {}

    /** 
     *  \brief Constructor II (Incremental PLQ stage and Incremental WLQ stage)
     *  
     *  \param _plqUpdate the incremental pane processing function (PLQ)
     *  \param _wlqUpdate the incremental window processing function (WLQ)
     *  \param _win_len window length (in no. of tuples or in time units)
     *  \param _slide_len slide length (in no. of tuples or in time units)
     *  \param _winType window type (count-based CB or time-based TB)
     *  \param _plq_degree parallelism degree of the PLQ stage
     *  \param _wlq_degree parallelism degree of the WLQ stage
     *  \param _name string with the unique name of the pattern
     *  \param _ordered true if the results of the same key must be emitted in order, false otherwise
     *  \param _opt_level optimization level used to build the pattern
     */ 
    Pane_Farm(f_plqupdate_t _plqUpdate,
              f_wlqupdate_t _wlqUpdate,
              uint64_t _win_len,
              uint64_t _slide_len,
              win_type_t _winType,
              size_t _plq_degree,
              size_t _wlq_degree,
              string _name,
              bool _ordered=true,
              opt_level_t _opt_level=LEVEL0)
              :
              Pane_Farm(_plqUpdate, _wlqUpdate, _win_len, _slide_len, _winType, _plq_degree, _wlq_degree, _name, _ordered, _opt_level, PatternConfig(0, 1, _slide_len, 0, 1, _slide_len)) {}

    /** 
     *  \brief Constructor III (Non-Incremental PLQ stage and Incremental WLQ stage)
     *  
     *  \param _plqFunction the non-incremental pane processing function (PLQ)
     *  \param _wlqUpdate the incremental window processing function (WLQ)
     *  \param _win_len window length (in no. of tuples or in time units)
     *  \param _slide_len slide length (in no. of tuples or in time units)
     *  \param _winType window type (count-based CB or time-based TB)
     *  \param _plq_degree parallelism degree of the PLQ stage
     *  \param _wlq_degree parallelism degree of the WLQ stage
     *  \param _name string with the unique name of the pattern
     *  \param _ordered true if the results of the same key must be emitted in order, false otherwise
     *  \param _opt_level optimization level used to build the pattern
     */ 
    Pane_Farm(f_plqfunction_t _plqFunction,
              f_wlqupdate_t _wlqUpdate,
              uint64_t _win_len,
              uint64_t _slide_len,
              win_type_t _winType,
              size_t _plq_degree,
              size_t _wlq_degree,
              string _name,
              bool _ordered=true,
              opt_level_t _opt_level=LEVEL0)
              :
              Pane_Farm(_plqFunction, _wlqUpdate, _win_len, _slide_len, _winType, _plq_degree, _wlq_degree, _name, _ordered, _opt_level, PatternConfig(0, 1, _slide_len, 0, 1, _slide_len)) {}

    /** 
     *  \brief Constructor IV (Incremental PLQ stage and Non-Incremental WLQ stage)
     *  
     *  \param _plqUpdate the incremental pane processing function (PLQ)
     *  \param _wlqFunction the non-incremental window processing function (WLQ)
     *  \param _win_len window length (in no. of tuples or in time units)
     *  \param _slide_len slide length (in no. of tuples or in time units)
     *  \param _winType window type (count-based CB or time-based TB)
     *  \param _plq_degree parallelism degree of the PLQ stage
     *  \param _wlq_degree parallelism degree of the WLQ stage
     *  \param _name string with the unique name of the pattern
     *  \param _ordered true if the results of the same key must be emitted in order, false otherwise
     *  \param _opt_level optimization level used to build the pattern
     */ 
    Pane_Farm(f_plqupdate_t _plqUpdate,
              f_wlqfunction_t _wlqFunction,
              uint64_t _win_len,
              uint64_t _slide_len,
              win_type_t _winType,
              size_t _plq_degree,
              size_t _wlq_degree,
              string _name,
              bool _ordered=true,
              opt_level_t _opt_level=LEVEL0)
              :
              Pane_Farm(_plqUpdate, _wlqFunction, _win_len, _slide_len, _winType, _plq_degree, _wlq_degree, _name, _ordered, _opt_level, PatternConfig(0, 1, _slide_len, 0, 1, _slide_len)) {}

    /** 
     *  \brief Get the optimization level used to build the pattern
     *  \return adopted utilization level by the pattern
     */
    opt_level_t getOptLevel() { return opt_level; }

    /** 
     *  \brief Get the window type (CB or TB) utilized by the pattern
     *  \return adopted windowing semantics (count- or time-based)
     */
    win_type_t getWinType() { return winType; }
};

#endif
