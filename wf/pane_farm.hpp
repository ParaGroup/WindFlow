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
 *  @brief Pane_Farm operator executing windowed queries in parallel on multi-core CPUs
 *  
 *  @section Pane_Farm (Description)
 *  
 *  This file implements the Pane_Farm operator able to execute windowed queries on a multicore.
 *  The operator processes (possibly in parallel) panes of the windows in the so-called PLQ stage
 *  (Pane-Level Sub-Query) and computes (possibly in parallel) results of the windows from the
 *  pane results in the so-called WLQ stage (Window-Level Sub-Query). Panes shared by more than
 *  one window are not recomputed by saving processing time. The operator supports both a non
 *  incremental and an incremental query definition in the two stages. The approach is based on [1].
 *  
 *  [1] Jin Li, David Maier, Kristin Tufte, Vassilis Papadimos, and Peter A. Tucker. 2005.
 *  No pane, no gain: efficient evaluation of sliding-window aggregates over data streams.
 *  SIGMOD Rec. 34, 1 (March 2005), 39–44. DOI:https://doi.org/10.1145/1058150.1058158
 *  
 *  The template parameters tuple_t and result_t must be default constructible, with a copy
 *  constructor and a copy assignment operator, and they must provide and implement the
 *  setControlFields() and getControlFields() methods.
 */ 

#ifndef PANE_FARM_H
#define PANE_FARM_H

/// includes
#include<ff/pipeline.hpp>
#include<ff/farm.hpp>
#include<meta.hpp>
#include<basic.hpp>
#include<win_farm.hpp>
#include<basic_operator.hpp>

namespace wf {

/** 
 *  \class Pane_Farm
 *  
 *  \brief Pane_Farm operator executing windowed queries in parallel on multi-core CPUs
 *  
 *  This class implements the Pane_Farm operator executing windowed queries in parallel on
 *  a multicore. The operator processes (possibly in parallel) panes in the PLQ stage while
 *  window results are built out from the pane results (possibly in parallel) in the WLQ
 *  stage.
 */ 
template<typename tuple_t, typename result_t, typename input_t>
class Pane_Farm: public ff::ff_pipeline, public Basic_Operator
{
public:
    /// type of the non-incremental pane processing function
    using plq_func_t = std::function<void(uint64_t, const Iterable<tuple_t> &, result_t &)>;
    /// type of the rich non-incremental pane processing function
    using rich_plq_func_t = std::function<void(uint64_t, const Iterable<tuple_t> &, result_t &, RuntimeContext &)>;
    /// type of the incremental pane processing function
    using plqupdate_funct_t = std::function<void(uint64_t, const tuple_t &, result_t &)>;
    /// type of the rich incremental pane processing function
    using rich_plqupdate_funct_t = std::function<void(uint64_t, const tuple_t &, result_t &, RuntimeContext &)>;
    /// type of the non-incremental window processing function
    using wlq_func_t = std::function<void(uint64_t, const Iterable<result_t> &, result_t &)>;
    /// type of the rich non-incremental window processing function
    using rich_wlq_func_t = std::function<void(uint64_t, const Iterable<result_t> &, result_t &, RuntimeContext &)>;
    /// type of the incremental window processing function
    using wlqupdate_func_t = std::function<void(uint64_t, const result_t &, result_t &)>;
    /// type of the rich incremental window processing function
    using rich_wlqupdate_func_t = std::function<void(uint64_t, const result_t &, result_t &, RuntimeContext &)>;
    /// type of the closing function
    using closing_func_t = std::function<void(RuntimeContext &)>;

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
    friend class MultiPipe;
    std::string name; // name of the Pane_Farm
    size_t parallelism; // internal parallelism of the Pane_Farm
    bool used; // true if the Pane_Farm has been added/chained in a MultiPipe
    bool used4Nesting; // true if the Pane_Farm has been used in a nested structure
    win_type_t winType; // type of windows (count-based or time-based)
    // other configuration variables of the Pane_Farm
    plq_func_t plq_func;
    rich_plq_func_t rich_plq_func;
    plqupdate_funct_t plqupdate_func;
    rich_plqupdate_funct_t rich_plqupdate_func;
    wlq_func_t wlq_func;
    rich_wlq_func_t rich_wlq_func;
    wlqupdate_func_t wlqupdate_func;
    rich_wlqupdate_func_t rich_wlqupdate_func;
    closing_func_t closing_func;
    bool isNICPLQ;
    bool isNICWLQ;
    bool isRichPLQ;
    bool isRichWLQ;
    uint64_t win_len;
    uint64_t slide_len;
    uint64_t pane_len;
    uint64_t triggering_delay;
    size_t plq_parallelism;
    size_t wlq_parallelism;
    bool ordered;
    opt_level_t opt_level;
    WinOperatorConfig config;
    std::vector<ff_node *> plq_workers; // vector of pointers to the Win_Seq instances in the PLQ stage
    std::vector<ff_node *> wlq_workers; // vector of pointers to the Win_Seq instances in the WLQ stage

    // Private Constructor
    template<typename F_t, typename G_t>
    Pane_Farm(F_t _func_PLQ,
              G_t _func_WLQ,
              uint64_t _win_len,
              uint64_t _slide_len,
              uint64_t _triggering_delay,
              win_type_t _winType,
              size_t _plq_parallelism,
              size_t _wlq_parallelism,
              std::string _name,
              closing_func_t _closing_func,
              bool _ordered,
              opt_level_t _opt_level,
              WinOperatorConfig _config):
              name(_name),
              parallelism(_plq_parallelism + _wlq_parallelism),
              used(false),
              used4Nesting(false),
              winType(_winType),
              closing_func(_closing_func),
              win_len(_win_len),
              slide_len(_slide_len),
              triggering_delay(_triggering_delay),
              plq_parallelism(_plq_parallelism),
              wlq_parallelism(_wlq_parallelism),
              ordered(_ordered),
              opt_level(_opt_level),
              config(WinOperatorConfig(0, 1, _slide_len, 0, 1, _slide_len))
    {
        // check the validity of the windowing parameters
        if (_win_len == 0 || _slide_len == 0) {
            std::cerr << RED << "WindFlow Error: window length or slide in Pane_Farm cannot be zero" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // check the validity of the parallelism values
        if (_plq_parallelism == 0 || _wlq_parallelism == 0) {
            std::cerr << RED << "WindFlow Error: Pane_Farm has parallelism (PLQ or WLQ) equal to zero" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // the Pane_Farm can be utilized with sliding windows only
        if (_win_len <= _slide_len) {
            std::cerr << RED << "WindFlow Error: Pane_Farm can be used with sliding windows only (s<w)" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // compute the pane length (no. of tuples or in time units)
        pane_len = gcd(_win_len, _slide_len);
        // general fastflow pointers to the PLQ and WLQ stages
        ff_node *plq_stage, *wlq_stage;
        // create the first stage PLQ (Pane Level Query)
        if (_plq_parallelism > 1) {
            // configuration structure of the Win_Farm (PLQ)
            WinOperatorConfig configWFPLQ(_config.id_outer, _config.n_outer, _config.slide_outer, _config.id_inner, _config.n_inner, _config.slide_inner);
            auto *plq_wf = new Win_Farm<tuple_t, result_t, input_t>(_func_PLQ, pane_len, pane_len, _triggering_delay, _winType, _plq_parallelism, _name + "_plq", _closing_func, true, opt_level_t::LEVEL0, configWFPLQ, role_t::PLQ);
            plq_stage = plq_wf;
            for (auto *w: plq_wf->getWorkers()) {
                plq_workers.push_back(w);
            }
        }
        else {
            // configuration structure of the Win_Seq (PLQ)
            WinOperatorConfig configSeqPLQ(_config.id_inner, _config.n_inner, _config.slide_inner, 0, 1, pane_len);
            auto *plq_seq = new Win_Seq<tuple_t, result_t, input_t>(_func_PLQ, pane_len, pane_len, _triggering_delay, _winType, _name + "_plq", _closing_func, RuntimeContext(1, 0), configSeqPLQ, role_t::PLQ);
            plq_stage = plq_seq;
            plq_workers.push_back(plq_seq);
        }
        // create the second stage WLQ (Window Level Query)
        if (_wlq_parallelism > 1) {
            // configuration structure of the Win_Farm (WLQ)
            WinOperatorConfig configWFWLQ(_config.id_outer, _config.n_outer, _config.slide_outer, _config.id_inner, _config.n_inner, _config.slide_inner);
            auto *wlq_wf = new Win_Farm<result_t, result_t>(_func_WLQ, (_win_len/pane_len), (_slide_len/pane_len), 0, win_type_t::CB, _wlq_parallelism, _name + "_wlq", _closing_func, _ordered, opt_level_t::LEVEL0, configWFWLQ, role_t::WLQ);
            wlq_stage = wlq_wf;
            for (auto *w: wlq_wf->getWorkers()) {
                wlq_workers.push_back(w);
            }
        }
        else {
            // configuration structure of the Win_Seq (WLQ)
            WinOperatorConfig configSeqWLQ(_config.id_inner, _config.n_inner, _config.slide_inner, 0, 1, (_slide_len/pane_len));
            auto *wlq_seq = new Win_Seq<result_t, result_t>(_func_WLQ, (_win_len/pane_len), (_slide_len/pane_len), 0, win_type_t::CB, _name + "_wlq", _closing_func, RuntimeContext(1, 0), configSeqWLQ, role_t::WLQ);
            wlq_stage = wlq_seq;
            wlq_workers.push_back(wlq_seq);
        }
        // add to this the pipeline optimized according to the provided optimization level
        ff::ff_pipeline::add_stage(optimize_PaneFarm(plq_stage, wlq_stage, _opt_level));
        // when the Pane_Farm will be destroyed we need also to destroy the two stages
        ff::ff_pipeline::cleanup_nodes();
        // flatten the pipeline
        ff::ff_pipeline::flatten();
    }

    // method to optimize the structure of the Pane_Farm operator
    const ff::ff_pipeline optimize_PaneFarm(ff_node *plq, ff_node *wlq, opt_level_t opt)
    {
        if (opt == opt_level_t::LEVEL0) { // no optimization
            ff::ff_pipeline pipe;
            pipe.add_stage(plq);
            pipe.add_stage(wlq);
            pipe.cleanup_nodes();
            return pipe;
        }
        else if (opt == opt_level_t::LEVEL1) { // optimization level 1
            if (plq_parallelism == 1 && wlq_parallelism == 1) {
                ff::ff_pipeline pipe;
                pipe.add_stage(new ff::ff_comb(plq, wlq, true, true));
                pipe.cleanup_nodes();
                return pipe;
            }
            else return combine_nodes_in_pipeline(*plq, *wlq, true, true);
        }
        else { // optimization level 2
            if (!plq->isFarm() || !wlq->isFarm()) { // like level 1
                if (plq_parallelism == 1 && wlq_parallelism == 1) {
                    ff::ff_pipeline pipe;
                    pipe.add_stage(new ff::ff_comb(plq, wlq, true, true));
                    pipe.cleanup_nodes();
                    return pipe;
                }
                else return combine_nodes_in_pipeline(*plq, *wlq, true, true);
            }
            else {
                using emitter_wlq_t = WF_Emitter<result_t, result_t>;
                ff::ff_farm *farm_plq = static_cast<ff::ff_farm *>(plq);
                ff::ff_farm *farm_wlq = static_cast<ff::ff_farm *>(wlq);
                emitter_wlq_t *emitter_wlq = static_cast<emitter_wlq_t *>(farm_wlq->getEmitter());
                farm_wlq->cleanup_emitter(false);
                Ordering_Node<result_t, wrapper_tuple_t<result_t>> *buf_node = new Ordering_Node<result_t, wrapper_tuple_t<result_t>>(ordering_mode_t::ID);   
                const ff::ff_pipeline result = combine_farms(*farm_plq, emitter_wlq, *farm_wlq, buf_node, false);
                delete farm_plq;
                delete farm_wlq;
                delete buf_node;
                delete emitter_wlq;
                return result;
            }
        }
    }

    // function to compute the gcd (std::gcd is available only in C++17)
    uint64_t gcd(uint64_t u, uint64_t v) {
        while (v != 0) {
            unsigned long r = u % v;
            u = v;
            v = r;
        }
        return u;
    };

public:
    /** 
     *  \brief Constructor I
     *  
     *  \param _plq_func the non-incremental pane processing function (PLQ)
     *  \param _wlq_func the non-incremental window processing function (WLQ)
     *  \param _win_len window length (in no. of tuples or in time units)
     *  \param _slide_len slide length (in no. of tuples or in time units)
     *  \param _triggering_delay (triggering delay in time units, meaningful for TB windows only otherwise it must be 0)
     *  \param _winType window type (count-based CB or time-based TB)
     *  \param _plq_parallelism parallelism of the PLQ stage
     *  \param _wlq_parallelism parallelism of the WLQ stage
     *  \param _name name of the operator
     *  \param _closing_func closing function
     *  \param _ordered true if the results of the same key must be emitted in order, false otherwise
     *  \param _opt_level optimization level used to build the operator
     */ 
    Pane_Farm(plq_func_t _plq_func,
              wlq_func_t _wlq_func,
              uint64_t _win_len,
              uint64_t _slide_len,
              uint64_t _triggering_delay,
              win_type_t _winType,
              size_t _plq_parallelism,
              size_t _wlq_parallelism,
              std::string _name,
              closing_func_t _closing_func,
              bool _ordered,
              opt_level_t _opt_level):
              Pane_Farm(_plq_func, _wlq_func, _win_len, _slide_len, _triggering_delay, _winType, _plq_parallelism, _wlq_parallelism, _name, _closing_func, _ordered, _opt_level, WinOperatorConfig(0, 1, _slide_len, 0, 1, _slide_len))
    {
        plq_func = _plq_func;
        wlq_func = _wlq_func;
        isNICPLQ = true;
        isNICWLQ = true;
        isRichPLQ = false;
        isRichWLQ = false;
    }

    /** 
     *  \brief Constructor II
     *  
     *  \param _rich_plq_func the rich non-incremental pane processing function (PLQ)
     *  \param _wlq_func the non-incremental window processing function (WLQ)
     *  \param _win_len window length (in no. of tuples or in time units)
     *  \param _slide_len slide length (in no. of tuples or in time units)
     *  \param _triggering_delay (triggering delay in time units, meaningful for TB windows only otherwise it must be 0)
     *  \param _winType window type (count-based CB or time-based TB)
     *  \param _plq_parallelism parallelism of the PLQ stage
     *  \param _wlq_parallelism parallelism of the WLQ stage
     *  \param _name name of the operator
     *  \param _closing_func closing function
     *  \param _ordered true if the results of the same key must be emitted in order, false otherwise
     *  \param _opt_level optimization level used to build the operator
     */ 
    Pane_Farm(rich_plq_func_t _rich_plq_func,
              wlq_func_t _wlq_func,
              uint64_t _win_len,
              uint64_t _slide_len,
              uint64_t _triggering_delay,
              win_type_t _winType,
              size_t _plq_parallelism,
              size_t _wlq_parallelism,
              std::string _name,
              closing_func_t _closing_func,
              bool _ordered,
              opt_level_t _opt_level):
              Pane_Farm(_rich_plq_func, _wlq_func, _win_len, _slide_len, _triggering_delay, _winType, _plq_parallelism, _wlq_parallelism, _name, _closing_func, _ordered, _opt_level, WinOperatorConfig(0, 1, _slide_len, 0, 1, _slide_len))
    {
        rich_plq_func = _rich_plq_func;
        wlq_func = _wlq_func;
        isNICPLQ = true;
        isNICWLQ = true;
        isRichPLQ = true;
        isRichWLQ = false;
    }

    /** 
     *  \brief Constructor III
     *  
     *  \param _plq_func the non-incremental pane processing function (PLQ)
     *  \param _rich_wlq_func the rich non-incremental window processing function (WLQ)
     *  \param _win_len window length (in no. of tuples or in time units)
     *  \param _slide_len slide length (in no. of tuples or in time units)
     *  \param _triggering_delay (triggering delay in time units, meaningful for TB windows only otherwise it must be 0)
     *  \param _winType window type (count-based CB or time-based TB)
     *  \param _plq_parallelism parallelism of the PLQ stage
     *  \param _wlq_parallelism parallelism of the WLQ stage
     *  \param _name name of the operator
     *  \param _closing_func closing function
     *  \param _ordered true if the results of the same key must be emitted in order, false otherwise
     *  \param _opt_level optimization level used to build the operator
     */ 
    Pane_Farm(plq_func_t _plq_func,
              rich_wlq_func_t _rich_wlq_func,
              uint64_t _win_len,
              uint64_t _slide_len,
              uint64_t _triggering_delay,
              win_type_t _winType,
              size_t _plq_parallelism,
              size_t _wlq_parallelism,
              std::string _name,
              closing_func_t _closing_func,
              bool _ordered,
              opt_level_t _opt_level):
              Pane_Farm(_plq_func, _rich_wlq_func, _win_len, _slide_len, _triggering_delay, _winType, _plq_parallelism, _wlq_parallelism, _name, _closing_func, _ordered, _opt_level, WinOperatorConfig(0, 1, _slide_len, 0, 1, _slide_len))
    {
        plq_func = _plq_func;
        rich_wlq_func = _rich_wlq_func;
        isNICPLQ = true;
        isNICWLQ = true;
        isRichPLQ = false;
        isRichWLQ = true;
    }

    /** 
     *  \brief Constructor IV
     *  
     *  \param _rich_plq_func the rich non-incremental pane processing function (PLQ)
     *  \param _rich_wlq_func the rich non-incremental window processing function (WLQ)
     *  \param _win_len window length (in no. of tuples or in time units)
     *  \param _slide_len slide length (in no. of tuples or in time units)
     *  \param _triggering_delay (triggering delay in time units, meaningful for TB windows only otherwise it must be 0)
     *  \param _winType window type (count-based CB or time-based TB)
     *  \param _plq_parallelism parallelism of the PLQ stage
     *  \param _wlq_parallelism parallelism of the WLQ stage
     *  \param _name name of the operator
     *  \param _closing_func closing function
     *  \param _ordered true if the results of the same key must be emitted in order, false otherwise
     *  \param _opt_level optimization level used to build the operator
     */ 
    Pane_Farm(rich_plq_func_t _rich_plq_func,
              rich_wlq_func_t _rich_wlq_func,
              uint64_t _win_len,
              uint64_t _slide_len,
              uint64_t _triggering_delay,
              win_type_t _winType,
              size_t _plq_parallelism,
              size_t _wlq_parallelism,
              std::string _name,
              closing_func_t _closing_func,
              bool _ordered,
              opt_level_t _opt_level):
              Pane_Farm(_rich_plq_func, _rich_wlq_func, _win_len, _slide_len, _triggering_delay, _winType, _plq_parallelism, _wlq_parallelism, _name, _closing_func, _ordered, _opt_level, WinOperatorConfig(0, 1, _slide_len, 0, 1, _slide_len))
    {
        rich_plq_func = _rich_plq_func;
        rich_wlq_func = _rich_wlq_func;
        isNICPLQ = true;
        isNICWLQ = true;
        isRichPLQ = true;
        isRichWLQ = true;
    }

    /** 
     *  \brief Constructor V
     *  
     *  \param _plqupdate_func the incremental pane processing function (PLQ)
     *  \param _wlqupdate_func the incremental window processing function (WLQ)
     *  \param _win_len window length (in no. of tuples or in time units)
     *  \param _slide_len slide length (in no. of tuples or in time units)
     *  \param _triggering_delay (triggering delay in time units, meaningful for TB windows only otherwise it must be 0)
     *  \param _winType window type (count-based CB or time-based TB)
     *  \param _plq_parallelism parallelism of the PLQ stage
     *  \param _wlq_parallelism parallelism of the WLQ stage
     *  \param _name name of the operator
     *  \param _closing_func closing function
     *  \param _ordered true if the results of the same key must be emitted in order, false otherwise
     *  \param _opt_level optimization level used to build the operator
     */ 
    Pane_Farm(plqupdate_funct_t _plqupdate_func,
              wlqupdate_func_t _wlqupdate_func,
              uint64_t _win_len,
              uint64_t _slide_len,
              uint64_t _triggering_delay,
              win_type_t _winType,
              size_t _plq_parallelism,
              size_t _wlq_parallelism,
              std::string _name,
              closing_func_t _closing_func,
              bool _ordered,
              opt_level_t _opt_level):
              Pane_Farm(_plqupdate_func, _wlqupdate_func, _win_len, _slide_len, _triggering_delay, _winType, _plq_parallelism, _wlq_parallelism, _name, _closing_func, _ordered, _opt_level, WinOperatorConfig(0, 1, _slide_len, 0, 1, _slide_len))
    {
        plqupdate_func = _plqupdate_func;
        wlqupdate_func = _wlqupdate_func;
        isNICPLQ = false;
        isNICWLQ = false;
        isRichPLQ = false;
        isRichWLQ = false;
    }

    /** 
     *  \brief Constructor VI
     *  
     *  \param _rich_plqupdate_func the rich incremental pane processing function (PLQ)
     *  \param _wlqupdate_func the incremental window processing function (WLQ)
     *  \param _win_len window length (in no. of tuples or in time units)
     *  \param _slide_len slide length (in no. of tuples or in time units)
     *  \param _triggering_delay (triggering delay in time units, meaningful for TB windows only otherwise it must be 0)
     *  \param _winType window type (count-based CB or time-based TB)
     *  \param _plq_parallelism parallelism of the PLQ stage
     *  \param _wlq_parallelism parallelism of the WLQ stage
     *  \param _name name of the operator
     *  \param _closing_func closing function
     *  \param _ordered true if the results of the same key must be emitted in order, false otherwise
     *  \param _opt_level optimization level used to build the operator
     */ 
    Pane_Farm(rich_plqupdate_funct_t _rich_plqupdate_func,
              wlqupdate_func_t _wlqupdate_func,
              uint64_t _win_len,
              uint64_t _slide_len,
              uint64_t _triggering_delay,
              win_type_t _winType,
              size_t _plq_parallelism,
              size_t _wlq_parallelism,
              std::string _name,
              closing_func_t _closing_func,
              bool _ordered,
              opt_level_t _opt_level):
              Pane_Farm(_rich_plqupdate_func, _wlqupdate_func, _win_len, _slide_len, _triggering_delay, _winType, _plq_parallelism, _wlq_parallelism, _name, _closing_func, _ordered, _opt_level, WinOperatorConfig(0, 1, _slide_len, 0, 1, _slide_len))
    {
        rich_plqupdate_func = _rich_plqupdate_func;
        wlqupdate_func = _wlqupdate_func;
        isNICPLQ = false;
        isNICWLQ = false;
        isRichPLQ = true;
        isRichWLQ = false;
    }

    /** 
     *  \brief Constructor VII
     *  
     *  \param _plqupdate_func the incremental pane processing function (PLQ)
     *  \param _rich_wlqupdate_func the rich incremental window processing function (WLQ)
     *  \param _win_len window length (in no. of tuples or in time units)
     *  \param _slide_len slide length (in no. of tuples or in time units)
     *  \param _triggering_delay (triggering delay in time units, meaningful for TB windows only otherwise it must be 0)
     *  \param _winType window type (count-based CB or time-based TB)
     *  \param _plq_parallelism parallelism of the PLQ stage
     *  \param _wlq_parallelism parallelism of the WLQ stage
     *  \param _name name of the operator
     *  \param _closing_func closing function
     *  \param _ordered true if the results of the same key must be emitted in order, false otherwise
     *  \param _opt_level optimization level used to build the operator
     */ 
    Pane_Farm(plqupdate_funct_t _plqupdate_func,
              rich_wlqupdate_func_t _rich_wlqupdate_func,
              uint64_t _win_len,
              uint64_t _slide_len,
              uint64_t _triggering_delay,
              win_type_t _winType,
              size_t _plq_parallelism,
              size_t _wlq_parallelism,
              std::string _name,
              closing_func_t _closing_func,
              bool _ordered,
              opt_level_t _opt_level):
              Pane_Farm(_plqupdate_func, _rich_wlqupdate_func, _win_len, _slide_len, _triggering_delay, _winType, _plq_parallelism, _wlq_parallelism, _name, _closing_func, _ordered, _opt_level, WinOperatorConfig(0, 1, _slide_len, 0, 1, _slide_len))
    {
        plqupdate_func = _plqupdate_func;
        rich_wlqupdate_func = _rich_wlqupdate_func;
        isNICPLQ = false;
        isNICWLQ = false;
        isRichPLQ = false;
        isRichWLQ = true;
    }

    /** 
     *  \brief Constructor VIII
     *  
     *  \param _rich_plqupdate_func the rich incremental pane processing function (PLQ)
     *  \param _rich_wlqupdate_func the rich incremental window processing function (WLQ)
     *  \param _win_len window length (in no. of tuples or in time units)
     *  \param _slide_len slide length (in no. of tuples or in time units)
     *  \param _triggering_delay (triggering delay in time units, meaningful for TB windows only otherwise it must be 0)
     *  \param _winType window type (count-based CB or time-based TB)
     *  \param _plq_parallelism parallelism of the PLQ stage
     *  \param _wlq_parallelism parallelism of the WLQ stage
     *  \param _name name of the operator
     *  \param _closing_func closing function
     *  \param _ordered true if the results of the same key must be emitted in order, false otherwise
     *  \param _opt_level optimization level used to build the operator
     */ 
    Pane_Farm(rich_plqupdate_funct_t _rich_plqupdate_func,
              rich_wlqupdate_func_t _rich_wlqupdate_func,
              uint64_t _win_len,
              uint64_t _slide_len,
              uint64_t _triggering_delay,
              win_type_t _winType,
              size_t _plq_parallelism,
              size_t _wlq_parallelism,
              std::string _name,
              closing_func_t _closing_func,
              bool _ordered,
              opt_level_t _opt_level):
              Pane_Farm(_rich_plqupdate_func, _rich_wlqupdate_func, _win_len, _slide_len, _triggering_delay, _winType, _plq_parallelism, _wlq_parallelism, _name, _closing_func, _ordered, _opt_level, WinOperatorConfig(0, 1, _slide_len, 0, 1, _slide_len))
    {
        rich_plqupdate_func = rich_plqupdate_func;
        rich_wlqupdate_func = _rich_wlqupdate_func;
        isNICPLQ = false;
        isNICWLQ = false;
        isRichPLQ = true;
        isRichWLQ = true;
    }

    /** 
     *  \brief Constructor IX
     *  
     *  \param _plq_func the non-incremental pane processing function (PLQ)
     *  \param _wlqupdate_func the incremental window processing function (WLQ)
     *  \param _win_len window length (in no. of tuples or in time units)
     *  \param _slide_len slide length (in no. of tuples or in time units)
     *  \param _triggering_delay (triggering delay in time units, meaningful for TB windows only otherwise it must be 0)
     *  \param _winType window type (count-based CB or time-based TB)
     *  \param _plq_parallelism parallelism of the PLQ stage
     *  \param _wlq_parallelism parallelism of the WLQ stage
     *  \param _name name of the operator
     *  \param _closing_func closing function
     *  \param _ordered true if the results of the same key must be emitted in order, false otherwise
     *  \param _opt_level optimization level used to build the operator
     */ 
    Pane_Farm(plq_func_t _plq_func,
              wlqupdate_func_t _wlqupdate_func,
              uint64_t _win_len,
              uint64_t _slide_len,
              uint64_t _triggering_delay,
              win_type_t _winType,
              size_t _plq_parallelism,
              size_t _wlq_parallelism,
              std::string _name,
              closing_func_t _closing_func,
              bool _ordered,
              opt_level_t _opt_level):
              Pane_Farm(_plq_func, _wlqupdate_func, _win_len, _slide_len, _triggering_delay, _winType, _plq_parallelism, _wlq_parallelism, _name, _closing_func, _ordered, _opt_level, WinOperatorConfig(0, 1, _slide_len, 0, 1, _slide_len))
    {
        plq_func = _plq_func;
        wlqupdate_func = _wlqupdate_func;
        isNICPLQ = true;
        isNICWLQ = false;
        isRichPLQ = false;
        isRichWLQ = false;
    }

    /** 
     *  \brief Constructor X
     *  
     *  \param _rich_plq_func the rich non-incremental pane processing function (PLQ)
     *  \param _wlqupdate_func the incremental window processing function (WLQ)
     *  \param _win_len window length (in no. of tuples or in time units)
     *  \param _slide_len slide length (in no. of tuples or in time units)
     *  \param _triggering_delay (triggering delay in time units, meaningful for TB windows only otherwise it must be 0)
     *  \param _winType window type (count-based CB or time-based TB)
     *  \param _plq_parallelism parallelism of the PLQ stage
     *  \param _wlq_parallelism parallelism of the WLQ stage
     *  \param _name name of the operator
     *  \param _closing_func closing function
     *  \param _ordered true if the results of the same key must be emitted in order, false otherwise
     *  \param _opt_level optimization level used to build the operator
     */ 
    Pane_Farm(rich_plq_func_t _rich_plq_func,
              wlqupdate_func_t _wlqupdate_func,
              uint64_t _win_len,
              uint64_t _slide_len,
              uint64_t _triggering_delay,
              win_type_t _winType,
              size_t _plq_parallelism,
              size_t _wlq_parallelism,
              std::string _name,
              closing_func_t _closing_func,
              bool _ordered,
              opt_level_t _opt_level):
              Pane_Farm(_rich_plq_func, _wlqupdate_func, _win_len, _slide_len, _triggering_delay, _winType, _plq_parallelism, _wlq_parallelism, _name, _closing_func, _ordered, _opt_level, WinOperatorConfig(0, 1, _slide_len, 0, 1, _slide_len))
    {
        rich_plq_func = _rich_plq_func;
        wlqupdate_func = _wlqupdate_func;
        isNICPLQ = true;
        isNICWLQ = false;
        isRichPLQ = true;
        isRichWLQ = false;
    }

    /** 
     *  \brief Constructor XI
     *  
     *  \param _plq_func the non-incremental pane processing function (PLQ)
     *  \param _rich_wlqupdate_func the rich incremental window processing function (WLQ)
     *  \param _win_len window length (in no. of tuples or in time units)
     *  \param _slide_len slide length (in no. of tuples or in time units)
     *  \param _triggering_delay (triggering delay in time units, meaningful for TB windows only otherwise it must be 0)
     *  \param _winType window type (count-based CB or time-based TB)
     *  \param _plq_parallelism parallelism of the PLQ stage
     *  \param _wlq_parallelism parallelism of the WLQ stage
     *  \param _name name of the operator
     *  \param _closing_func closing function
     *  \param _ordered true if the results of the same key must be emitted in order, false otherwise
     *  \param _opt_level optimization level used to build the operator
     */ 
    Pane_Farm(plq_func_t _plq_func,
              rich_wlqupdate_func_t _rich_wlqupdate_func,
              uint64_t _win_len,
              uint64_t _slide_len,
              uint64_t _triggering_delay,
              win_type_t _winType,
              size_t _plq_parallelism,
              size_t _wlq_parallelism,
              std::string _name,
              closing_func_t _closing_func,
              bool _ordered,
              opt_level_t _opt_level):
              Pane_Farm(_plq_func, _rich_wlqupdate_func, _win_len, _slide_len, _triggering_delay, _winType, _plq_parallelism, _wlq_parallelism, _name, _closing_func, _ordered, _opt_level, WinOperatorConfig(0, 1, _slide_len, 0, 1, _slide_len))
    {
        plq_func = _plq_func;
        rich_wlqupdate_func = _rich_wlqupdate_func;
        isNICPLQ = true;
        isNICWLQ = false;
        isRichPLQ = false;
        isRichWLQ = true;
    }

    /** 
     *  \brief Constructor XII
     *  
     *  \param _rich_plq_func the rich non-incremental pane processing function (PLQ)
     *  \param _rich_wlqupdate_func the rich incremental window processing function (WLQ)
     *  \param _win_len window length (in no. of tuples or in time units)
     *  \param _slide_len slide length (in no. of tuples or in time units)
     *  \param _triggering_delay (triggering delay in time units, meaningful for TB windows only otherwise it must be 0)
     *  \param _winType window type (count-based CB or time-based TB)
     *  \param _plq_parallelism parallelism of the PLQ stage
     *  \param _wlq_parallelism parallelism of the WLQ stage
     *  \param _name name of the operator
     *  \param _closing_func closing function
     *  \param _ordered true if the results of the same key must be emitted in order, false otherwise
     *  \param _opt_level optimization level used to build the operator
     */ 
    Pane_Farm(rich_plq_func_t _rich_plq_func,
              rich_wlqupdate_func_t _rich_wlqupdate_func,
              uint64_t _win_len,
              uint64_t _slide_len,
              uint64_t _triggering_delay,
              win_type_t _winType,
              size_t _plq_parallelism,
              size_t _wlq_parallelism,
              std::string _name,
              closing_func_t _closing_func,
              bool _ordered,
              opt_level_t _opt_level):
              Pane_Farm(_rich_plq_func, _rich_wlqupdate_func, _win_len, _slide_len, _triggering_delay, _winType, _plq_parallelism, _wlq_parallelism, _name, _closing_func, _ordered, _opt_level, WinOperatorConfig(0, 1, _slide_len, 0, 1, _slide_len))
    {
        rich_plq_func = _rich_plq_func;
        rich_wlqupdate_func = _rich_wlqupdate_func;
        isNICPLQ = true;
        isNICWLQ = false;
        isRichPLQ = true;
        isRichWLQ = true;
    }

    /** 
     *  \brief Constructor XIII
     *  
     *  \param _plqupdate_func the incremental pane processing function (PLQ)
     *  \param _wlq_func the non-incremental window processing function (WLQ)
     *  \param _win_len window length (in no. of tuples or in time units)
     *  \param _slide_len slide length (in no. of tuples or in time units)
     *  \param _triggering_delay (triggering delay in time units, meaningful for TB windows only otherwise it must be 0)
     *  \param _winType window type (count-based CB or time-based TB)
     *  \param _plq_parallelism parallelism of the PLQ stage
     *  \param _wlq_parallelism parallelism of the WLQ stage
     *  \param _name name of the operator
     *  \param _closing_func closing function
     *  \param _ordered true if the results of the same key must be emitted in order, false otherwise
     *  \param _opt_level optimization level used to build the operator
     */ 
    Pane_Farm(plqupdate_funct_t _plqupdate_func,
              wlq_func_t _wlq_func,
              uint64_t _win_len,
              uint64_t _slide_len,
              uint64_t _triggering_delay,
              win_type_t _winType,
              size_t _plq_parallelism,
              size_t _wlq_parallelism,
              std::string _name,
              closing_func_t _closing_func,
              bool _ordered,
              opt_level_t _opt_level):
              Pane_Farm(_plqupdate_func, _wlq_func, _win_len, _slide_len, _triggering_delay, _winType, _plq_parallelism, _wlq_parallelism, _name, _closing_func, _ordered, _opt_level, WinOperatorConfig(0, 1, _slide_len, 0, 1, _slide_len))
    {
        plqupdate_func = _plqupdate_func;
        wlq_func = _wlq_func;
        isNICPLQ = false;
        isNICWLQ = true;
        isRichPLQ = false;
        isRichWLQ = false;
    }

    /** 
     *  \brief Constructor XIV
     *  
     *  \param _rich_plqupdate_func the rich incremental pane processing function (PLQ)
     *  \param _wlq_func the non-incremental window processing function (WLQ)
     *  \param _win_len window length (in no. of tuples or in time units)
     *  \param _slide_len slide length (in no. of tuples or in time units)
     *  \param _triggering_delay (triggering delay in time units, meaningful for TB windows only otherwise it must be 0)
     *  \param _winType window type (count-based CB or time-based TB)
     *  \param _plq_parallelism parallelism of the PLQ stage
     *  \param _wlq_parallelism parallelism of the WLQ stage
     *  \param _name name of the operator
     *  \param _closing_func closing function
     *  \param _ordered true if the results of the same key must be emitted in order, false otherwise
     *  \param _opt_level optimization level used to build the operator
     */ 
    Pane_Farm(rich_plqupdate_funct_t _rich_plqupdate_func,
              wlq_func_t _wlq_func,
              uint64_t _win_len,
              uint64_t _slide_len,
              uint64_t _triggering_delay,
              win_type_t _winType,
              size_t _plq_parallelism,
              size_t _wlq_parallelism,
              std::string _name,
              closing_func_t _closing_func,
              bool _ordered,
              opt_level_t _opt_level):
              Pane_Farm(_rich_plqupdate_func, _wlq_func, _win_len, _slide_len, _triggering_delay, _winType, _plq_parallelism, _wlq_parallelism, _name, _closing_func, _ordered, _opt_level, WinOperatorConfig(0, 1, _slide_len, 0, 1, _slide_len))
    {
        rich_plqupdate_func = _rich_plqupdate_func;
        wlq_func = _wlq_func;
        isNICPLQ = false;
        isNICWLQ = true;
        isRichPLQ = true;
        isRichWLQ = false;
    }

    /** 
     *  \brief Constructor XV
     *  
     *  \param _plqupdate_func the incremental pane processing function (PLQ)
     *  \param _rich_wlq_func the rich non-incremental window processing function (WLQ)
     *  \param _win_len window length (in no. of tuples or in time units)
     *  \param _slide_len slide length (in no. of tuples or in time units)
     *  \param _triggering_delay (triggering delay in time units, meaningful for TB windows only otherwise it must be 0)
     *  \param _winType window type (count-based CB or time-based TB)
     *  \param _plq_parallelism parallelism of the PLQ stage
     *  \param _wlq_parallelism parallelism of the WLQ stage
     *  \param _name name of the operator
     *  \param _closing_func closing function
     *  \param _ordered true if the results of the same key must be emitted in order, false otherwise
     *  \param _opt_level optimization level used to build the operator
     */ 
    Pane_Farm(plqupdate_funct_t _plqupdate_func,
              rich_wlq_func_t _rich_wlq_func,
              uint64_t _win_len,
              uint64_t _slide_len,
              uint64_t _triggering_delay,
              win_type_t _winType,
              size_t _plq_parallelism,
              size_t _wlq_parallelism,
              std::string _name,
              closing_func_t _closing_func,
              bool _ordered,
              opt_level_t _opt_level):
              Pane_Farm(_plqupdate_func, _rich_wlq_func, _win_len, _slide_len, _triggering_delay, _winType, _plq_parallelism, _wlq_parallelism, _name, _closing_func, _ordered, _opt_level, WinOperatorConfig(0, 1, _slide_len, 0, 1, _slide_len))
    {
        plqupdate_func = _plqupdate_func;
        rich_wlq_func = _rich_wlq_func;
        isNICPLQ = false;
        isNICWLQ = true;
        isRichPLQ = false;
        isRichWLQ = true;
    }

    /** 
     *  \brief Constructor XVI
     *  
     *  \param _rich_plqupdate_func the rich_incremental pane processing function (PLQ)
     *  \param _rich_wlq_func the rich non-incremental window processing function (WLQ)
     *  \param _win_len window length (in no. of tuples or in time units)
     *  \param _slide_len slide length (in no. of tuples or in time units)
     *  \param _triggering_delay (triggering delay in time units, meaningful for TB windows only otherwise it must be 0)
     *  \param _winType window type (count-based CB or time-based TB)
     *  \param _plq_parallelism parallelism of the PLQ stage
     *  \param _wlq_parallelism parallelism of the WLQ stage
     *  \param _name name of the operator
     *  \param _closing_func closing function
     *  \param _ordered true if the results of the same key must be emitted in order, false otherwise
     *  \param _opt_level optimization level used to build the operator
     */ 
    Pane_Farm(rich_plqupdate_funct_t _rich_plqupdate_func,
              rich_wlq_func_t _rich_wlq_func,
              uint64_t _win_len,
              uint64_t _slide_len,
              uint64_t _triggering_delay,
              win_type_t _winType,
              size_t _plq_parallelism,
              size_t _wlq_parallelism,
              std::string _name,
              closing_func_t _closing_func,
              bool _ordered,
              opt_level_t _opt_level):
              Pane_Farm(_rich_plqupdate_func, _rich_wlq_func, _win_len, _slide_len, _triggering_delay, _winType, _plq_parallelism, _wlq_parallelism, _name, _closing_func, _ordered, _opt_level, WinOperatorConfig(0, 1, _slide_len, 0, 1, _slide_len))
    {
        rich_plqupdate_func = _rich_plqupdate_func;
        rich_wlq_func = _rich_wlq_func;
        isNICPLQ = false;
        isNICWLQ = true;
        isRichPLQ = true;
        isRichWLQ = true;
    }

    /** 
     *  \brief Check whether the Pane_Farm has been used in a nested structure
     *  \return true if the Pane_Farm has been used in a nested structure
     */
    bool isUsed4Nesting() const
    {
        return used4Nesting;
    }

    /** 
     *  \brief Get the optimization level used to build the Pane_Farm
     *  \return adopted utilization level by the Pane_Farm
     */ 
    opt_level_t getOptLevel() const
    {
      return opt_level;
    }

    /** 
     *  \brief Get the parallelism of the PLQ stage
     *  \return PLQ parallelism
     */ 
    size_t getPLQParallelism() const
    {
      return plq_parallelism;
    }

    /** 
     *  \brief Get the parallelism of the WLQ stage
     *  \return WLQ parallelism
     */ 
    size_t getWLQParallelism() const
    {
      return wlq_parallelism;
    }

    /** 
     *  \brief Get the window type (CB or TB) utilized by the Pane_Farm
     *  \return adopted windowing semantics (count-based or time-based)
     */ 
    win_type_t getWinType() const
    {
        return winType;
    }

    /** 
     *  \brief Get the number of ignored tuples by the Pane_Farm
     *  \return number of tuples ignored during the processing by the Pane_Farm
     */ 
    size_t getNumIgnoredTuples() const
    {
        size_t count = 0;
        for (auto *w: plq_workers) {
            auto *seq = static_cast<Win_Seq<tuple_t, result_t, input_t> *>(w);
            count += seq->getNumIgnoredTuples();
        }
        return count;
    }

    /** 
     *  \brief Get the name of the Pane_Farm
     *  \return name of the Pane_Farm
     */ 
    std::string getName() const override
    {
        return name;
    }

    /** 
     *  \brief Get the total parallelism within the Pane_Farm
     *  \return total parallelism within the Pane_Farm
     */ 
    size_t getParallelism() const override
    {
        return parallelism;
    }

    /** 
     *  \brief Return the routing mode of inputs to the Pane_Farm
     *  \return routing mode (always COMPLEX for the Pane_Farm)
     */ 
    routing_modes_t getRoutingMode() const override
    {
        return routing_modes_t::COMPLEX;
    }

    /** 
     *  \brief Check whether the Pane_Farm has been used in a MultiPipe
     *  \return true if the Pane_Farm has been added/chained to an existing MultiPipe
     */ 
    bool isUsed() const override
    {
        return used;
    }

    /** 
     *  \brief Check whether the operator has been terminated
     *  \return true if the operator has finished its work
     */ 
    virtual bool isTerminated() const override
    {
        bool terminated = true;
        // scan all the replicas to check their termination
        for (auto *w: plq_workers) {
            auto *seq = static_cast<Win_Seq<tuple_t, result_t, input_t> *>(w);
            terminated = terminated && seq->isTerminated();
        }
        for (auto *w: wlq_workers) {
            auto *seq = static_cast<Win_Seq<result_t, result_t> *>(w);
            terminated = terminated && seq->isTerminated();
        }
        return terminated;
    }

#if defined (TRACE_WINDFLOW)
    /// Dump the log file (JSON format) in the LOG_DIR directory
    void dump_LogFile() const override
    {
        // create and open the log file in the LOG_DIR directory
        std::ofstream logfile;
#if defined (LOG_DIR)
        std::string log_dir = std::string(STRINGIFY(LOG_DIR));
        std::string filename = std::string(STRINGIFY(LOG_DIR)) + "/" + std::to_string(getpid()) + "_" + name + ".json";
#else
        std::string log_dir = std::string("log");
        std::string filename = "log/" + std::to_string(getpid()) + "_" + name + ".json";
#endif
        // create the log directory
        if (mkdir(log_dir.c_str(), 0777) != 0) {
            struct stat st;
            if((stat(log_dir.c_str(), &st) != 0) || !S_ISDIR(st.st_mode)) {
                std::cerr << RED << "WindFlow Error: directory for log files cannot be created" << DEFAULT_COLOR << std::endl;
                exit(EXIT_FAILURE);
            }
        }
        logfile.open(filename);
        // create the rapidjson writer
        rapidjson::StringBuffer buffer;
        rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(buffer);
        // append the statistics of this operator
        this->append_Stats(writer);
        // serialize the object to file
        logfile << buffer.GetString();
        logfile.close();
    }

    // append the statistics of this operator
    void append_Stats(rapidjson::PrettyWriter<rapidjson::StringBuffer> &writer) const override
    {
        // create the header of the JSON file
        writer.StartObject();
        writer.Key("Operator_name");
        writer.String(name.c_str());
        writer.Key("Operator_type");
        writer.String("Pane_Farm");
        writer.Key("Distribution");
        writer.String("COMPLEX");
        writer.Key("isTerminated");
        writer.Bool(this->isTerminated());
        writer.Key("isGPU_1");
        writer.Bool(false);
        writer.Key("Name_Stage_1");
        writer.String("PLQ");
        writer.Key("Window_type_1");
        if (winType == win_type_t::CB) {
            writer.String("count-based");
        }
        else {
            writer.String("time-based");
            writer.Key("Window_delay");
            writer.Uint(triggering_delay);  
        }
        writer.Key("Window_length_1");
        writer.Uint(pane_len);
        writer.Key("Window_slide_1");
        writer.Uint(pane_len);
        writer.Key("Parallelism_1");
        writer.Uint(plq_parallelism);
        writer.Key("Replicas_1");
        writer.StartArray();
        // get statistics from all the replicas of the PLQ stage
        for (auto *w: plq_workers) {
            auto *seq = static_cast<Win_Seq<tuple_t, result_t, input_t> *>(w);
            Stats_Record record = seq->get_StatsRecord();
            record.append_Stats(writer);
        }
        writer.EndArray();
        writer.Key("isGPU_2");
        writer.Bool(false);
        writer.Key("Name_Stage_2");
        writer.String("WLQ");
        writer.Key("Window_type_2");
        writer.String("count-based");
        writer.Key("Window_length_2");
        writer.Uint(win_len/pane_len);
        writer.Key("Window_slide_2");
        writer.Uint(slide_len/pane_len);
        writer.Key("Parallelism_2");
        writer.Uint(wlq_parallelism);
        writer.Key("Replicas_2");
        writer.StartArray();
        // get statistics from all the replicas of the WLQ stage
        for (auto *w: wlq_workers) {
            auto *seq = static_cast<Win_Seq<result_t, result_t> *>(w);
            Stats_Record record = seq->get_StatsRecord();
            record.append_Stats(writer);
        }
        writer.EndArray();      
        writer.EndObject();     
    }
#endif

    /// deleted constructors/operators
    Pane_Farm(const Pane_Farm &) = delete; // copy constructor
    Pane_Farm(Pane_Farm &&) = delete; // move constructor
    Pane_Farm &operator=(const Pane_Farm &) = delete; // copy assignment operator
    Pane_Farm &operator=(Pane_Farm &&) = delete; // move assignment operator
};

} // namespace wf

#endif
