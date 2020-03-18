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
 *  @brief Pane_Farm operator executing a windowed query in parallel
 *         on multi-core CPUs
 *  
 *  @section Pane_Farm (Description)
 *  
 *  This file implements the Pane_Farm operator able to execute windowed queries
 *  on a multicore. The operator processes (possibly in parallel) panes of the
 *  windows in the so-called PLQ stage (Pane-Level Sub-Query) and computes
 *  (possibly in parallel) results of the windows from the pane results in the
 *  so-called WLQ stage (Window-Level Sub-Query). Panes shared by more than one window
 *  are not recomputed by saving processing time. The operator supports both a
 *  non-incremental and an incremental query definition in the two stages.
 *  
 *  The template parameters tuple_t and result_t must be default constructible, with a copy
 *  constructor and copy assignment operator, and they must provide and implement the
 *  setControlFields() and getControlFields() methods.
 */ 

#ifndef PANE_FARM_H
#define PANE_FARM_H

/// includes
#include <ff/pipeline.hpp>
#include <ff/farm.hpp>
#include <win_farm.hpp>
#include <basic.hpp>
#include <meta.hpp>

namespace wf {

/** 
 *  \class Pane_Farm
 *  
 *  \brief Pane_Farm operator executing a windowed query in parallel on multi-core CPUs
 *  
 *  This class implements the Pane_Farm operator executing windowed queries in parallel on
 *  a multicore. The operator processes (possibly in parallel) panes in the PLQ stage while
 *  window results are built out from the pane results (possibly in parallel) in the WLQ
 *  stage.
 */ 
template<typename tuple_t, typename result_t, typename input_t>
class Pane_Farm: public ff::ff_pipeline
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
    template<typename T1, typename T2>
    friend class Key_Farm;
    template<typename T>
    friend class WinFarm_Builder;
    template<typename T>
    friend class KeyFarm_Builder;
    friend class MultiPipe;
    // configuration variables of the Pane_Farm
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
    uint64_t triggering_delay;
    win_type_t winType;
    size_t plq_degree;
    size_t wlq_degree;
    std::string name;
    bool ordered;
    opt_level_t opt_level;
    OperatorConfig config;
    bool used; // true if the operator has been added/chained in a MultiPipe
    bool used4Nesting; // true if the operator has been used in a nested structure
    ff_node *plq; // pointer to the PLQ stage

    // Private Constructor
    template<typename F_t, typename G_t>
    Pane_Farm(F_t _func_PLQ,
              G_t _func_WLQ,
              uint64_t _win_len,
              uint64_t _slide_len,
              uint64_t _triggering_delay,
              win_type_t _winType,
              size_t _plq_degree,
              size_t _wlq_degree,
              std::string _name,
              closing_func_t _closing_func,
              bool _ordered,
              opt_level_t _opt_level,
              OperatorConfig _config):
              win_len(_win_len),
              slide_len(_slide_len),
              triggering_delay(_triggering_delay),
              winType(_winType),
              plq_degree(_plq_degree),
              wlq_degree(_wlq_degree),
              name(_name),
              closing_func(_closing_func),
              ordered(_ordered),
              opt_level(_opt_level),
              config(OperatorConfig(0, 1, _slide_len, 0, 1, _slide_len))
    {
        // check the validity of the windowing parameters
        if (_win_len == 0 || _slide_len == 0) {
            std::cerr << RED << "WindFlow Error: window length or slide in Pane_Farm cannot be zero" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // check the validity of the parallelism degrees
        if (_plq_degree == 0 || _wlq_degree == 0) {
            std::cerr << RED << "WindFlow Error: Pane_Farm has parallelism zero" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // the Pane_Farm can be utilized with sliding windows only
        if (_win_len <= _slide_len) {
            std::cerr << RED << "WindFlow Error: Pane_Farm can be used with sliding windows only (s<w)" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // compute the pane length (no. of tuples or in time units)
        uint64_t _pane_len = gcd(_win_len, _slide_len);
        // general fastflow pointers to the PLQ and WLQ stages
        ff_node *plq_stage, *wlq_stage;
        // create the first stage PLQ (Pane Level Query)
        if (_plq_degree > 1) {
            // configuration structure of the Win_Farm (PLQ)
            OperatorConfig configWFPLQ(_config.id_outer, _config.n_outer, _config.slide_outer, _config.id_inner, _config.n_inner, _config.slide_inner);
            auto *plq_wf = new Win_Farm<tuple_t, result_t, input_t>(_func_PLQ, _pane_len, _pane_len, _triggering_delay, _winType, _plq_degree, _name + "_plq", _closing_func, true, LEVEL0, configWFPLQ, PLQ);
            plq_stage = plq_wf;
            plq = plq_wf;
        }
        else {
            // configuration structure of the Win_Seq (PLQ)
            OperatorConfig configSeqPLQ(_config.id_inner, _config.n_inner, _config.slide_inner, 0, 1, _pane_len);
            auto *plq_seq = new Win_Seq<tuple_t, result_t, input_t>(_func_PLQ, _pane_len, _pane_len, _triggering_delay, _winType, _name + "_plq", _closing_func, RuntimeContext(1, 0), configSeqPLQ, PLQ);
            plq_stage = plq_seq;
            plq = plq_seq;
        }
        // create the second stage WLQ (Window Level Query)
        if (_wlq_degree > 1) {
            // configuration structure of the Win_Farm (WLQ)
            OperatorConfig configWFWLQ(_config.id_outer, _config.n_outer, _config.slide_outer, _config.id_inner, _config.n_inner, _config.slide_inner);
            auto *wlq_wf = new Win_Farm<result_t, result_t>(_func_WLQ, (_win_len/_pane_len), (_slide_len/_pane_len), 0, CB, _wlq_degree, _name + "_wlq", _closing_func, _ordered, LEVEL0, configWFWLQ, WLQ);
            wlq_stage = wlq_wf;
        }
        else {
            // configuration structure of the Win_Seq (WLQ)
            OperatorConfig configSeqWLQ(_config.id_inner, _config.n_inner, _config.slide_inner, 0, 1, (_slide_len/_pane_len));
            auto *wlq_seq = new Win_Seq<result_t, result_t>(_func_WLQ, (_win_len/_pane_len), (_slide_len/_pane_len), 0, CB, _name + "_wlq", _closing_func, RuntimeContext(1, 0), configSeqWLQ, WLQ);
            wlq_stage = wlq_seq;
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
        if (opt == LEVEL0) { // no optimization
            ff::ff_pipeline pipe;
            pipe.add_stage(plq);
            pipe.add_stage(wlq);
            pipe.cleanup_nodes();
            return pipe;
        }
        else if (opt == LEVEL1) { // optimization level 1
            if (plq_degree == 1 && wlq_degree == 1) {
                ff::ff_pipeline pipe;
                pipe.add_stage(new ff::ff_comb(plq, wlq, true, true));
                pipe.cleanup_nodes();
                return pipe;
            }
            else return combine_nodes_in_pipeline(*plq, *wlq, true, true);
        }
        else { // optimization level 2
            if (!plq->isFarm() || !wlq->isFarm()) { // like level 1
                if (plq_degree == 1 && wlq_degree == 1) {
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
                Ordering_Node<result_t, wrapper_tuple_t<result_t>> *buf_node = new Ordering_Node<result_t, wrapper_tuple_t<result_t>>(ID);   
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
     *  \param _plq_degree parallelism degree of the PLQ stage
     *  \param _wlq_degree parallelism degree of the WLQ stage
     *  \param _name string with the unique name of the operator
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
              size_t _plq_degree,
              size_t _wlq_degree,
              std::string _name,
              closing_func_t _closing_func,
              bool _ordered,
              opt_level_t _opt_level):
              Pane_Farm(_plq_func, _wlq_func, _win_len, _slide_len, _triggering_delay, _winType, _plq_degree, _wlq_degree, _name, _closing_func, _ordered, _opt_level, OperatorConfig(0, 1, _slide_len, 0, 1, _slide_len))
    {
        plq_func = _plq_func;
        wlq_func = _wlq_func;
        isNICPLQ = true;
        isNICWLQ = true;
        isRichPLQ = false;
        isRichWLQ = false;
        used = false;
        used4Nesting = false;
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
     *  \param _plq_degree parallelism degree of the PLQ stage
     *  \param _wlq_degree parallelism degree of the WLQ stage
     *  \param _name string with the unique name of the operator
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
              size_t _plq_degree,
              size_t _wlq_degree,
              std::string _name,
              closing_func_t _closing_func,
              bool _ordered,
              opt_level_t _opt_level):
              Pane_Farm(_rich_plq_func, _wlq_func, _win_len, _slide_len, _triggering_delay, _winType, _plq_degree, _wlq_degree, _name, _closing_func, _ordered, _opt_level, OperatorConfig(0, 1, _slide_len, 0, 1, _slide_len))
    {
        rich_plq_func = _rich_plq_func;
        wlq_func = _wlq_func;
        isNICPLQ = true;
        isNICWLQ = true;
        isRichPLQ = true;
        isRichWLQ = false;
        used = false;
        used4Nesting = false;
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
     *  \param _plq_degree parallelism degree of the PLQ stage
     *  \param _wlq_degree parallelism degree of the WLQ stage
     *  \param _name string with the unique name of the operator
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
              size_t _plq_degree,
              size_t _wlq_degree,
              std::string _name,
              closing_func_t _closing_func,
              bool _ordered,
              opt_level_t _opt_level):
              Pane_Farm(_plq_func, _rich_wlq_func, _win_len, _slide_len, _triggering_delay, _winType, _plq_degree, _wlq_degree, _name, _closing_func, _ordered, _opt_level, OperatorConfig(0, 1, _slide_len, 0, 1, _slide_len))
    {
        plq_func = _plq_func;
        rich_wlq_func = _rich_wlq_func;
        isNICPLQ = true;
        isNICWLQ = true;
        isRichPLQ = false;
        isRichWLQ = true;
        used = false;
        used4Nesting = false;
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
     *  \param _plq_degree parallelism degree of the PLQ stage
     *  \param _wlq_degree parallelism degree of the WLQ stage
     *  \param _name string with the unique name of the operator
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
              size_t _plq_degree,
              size_t _wlq_degree,
              std::string _name,
              closing_func_t _closing_func,
              bool _ordered,
              opt_level_t _opt_level):
              Pane_Farm(_rich_plq_func, _rich_wlq_func, _win_len, _slide_len, _triggering_delay, _winType, _plq_degree, _wlq_degree, _name, _closing_func, _ordered, _opt_level, OperatorConfig(0, 1, _slide_len, 0, 1, _slide_len))
    {
        rich_plq_func = _rich_plq_func;
        rich_wlq_func = _rich_wlq_func;
        isNICPLQ = true;
        isNICWLQ = true;
        isRichPLQ = true;
        isRichWLQ = true;
        used = false;
        used4Nesting = false;
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
     *  \param _plq_degree parallelism degree of the PLQ stage
     *  \param _wlq_degree parallelism degree of the WLQ stage
     *  \param _name string with the unique name of the operator
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
              size_t _plq_degree,
              size_t _wlq_degree,
              std::string _name,
              closing_func_t _closing_func,
              bool _ordered,
              opt_level_t _opt_level):
              Pane_Farm(_plqupdate_func, _wlqupdate_func, _win_len, _slide_len, _triggering_delay, _winType, _plq_degree, _wlq_degree, _name, _closing_func, _ordered, _opt_level, OperatorConfig(0, 1, _slide_len, 0, 1, _slide_len))
    {
        plqupdate_func = _plqupdate_func;
        wlqupdate_func = _wlqupdate_func;
        isNICPLQ = false;
        isNICWLQ = false;
        isRichPLQ = false;
        isRichWLQ = false;
        used = false;
        used4Nesting = false;
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
     *  \param _plq_degree parallelism degree of the PLQ stage
     *  \param _wlq_degree parallelism degree of the WLQ stage
     *  \param _name string with the unique name of the operator
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
              size_t _plq_degree,
              size_t _wlq_degree,
              std::string _name,
              closing_func_t _closing_func,
              bool _ordered,
              opt_level_t _opt_level):
              Pane_Farm(_rich_plqupdate_func, _wlqupdate_func, _win_len, _slide_len, _triggering_delay, _winType, _plq_degree, _wlq_degree, _name, _closing_func, _ordered, _opt_level, OperatorConfig(0, 1, _slide_len, 0, 1, _slide_len))
    {
        rich_plqupdate_func = _rich_plqupdate_func;
        wlqupdate_func = _wlqupdate_func;
        isNICPLQ = false;
        isNICWLQ = false;
        isRichPLQ = true;
        isRichWLQ = false;
        used = false;
        used4Nesting = false;
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
     *  \param _plq_degree parallelism degree of the PLQ stage
     *  \param _wlq_degree parallelism degree of the WLQ stage
     *  \param _name string with the unique name of the operator
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
              size_t _plq_degree,
              size_t _wlq_degree,
              std::string _name,
              closing_func_t _closing_func,
              bool _ordered,
              opt_level_t _opt_level):
              Pane_Farm(_plqupdate_func, _rich_wlqupdate_func, _win_len, _slide_len, _triggering_delay, _winType, _plq_degree, _wlq_degree, _name, _closing_func, _ordered, _opt_level, OperatorConfig(0, 1, _slide_len, 0, 1, _slide_len))
    {
        plqupdate_func = _plqupdate_func;
        rich_wlqupdate_func = _rich_wlqupdate_func;
        isNICPLQ = false;
        isNICWLQ = false;
        isRichPLQ = false;
        isRichWLQ = true;
        used = false;
        used4Nesting = false;
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
     *  \param _plq_degree parallelism degree of the PLQ stage
     *  \param _wlq_degree parallelism degree of the WLQ stage
     *  \param _name string with the unique name of the operator
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
              size_t _plq_degree,
              size_t _wlq_degree,
              std::string _name,
              closing_func_t _closing_func,
              bool _ordered,
              opt_level_t _opt_level):
              Pane_Farm(_rich_plqupdate_func, _rich_wlqupdate_func, _win_len, _slide_len, _triggering_delay, _winType, _plq_degree, _wlq_degree, _name, _closing_func, _ordered, _opt_level, OperatorConfig(0, 1, _slide_len, 0, 1, _slide_len))
    {
        rich_plqupdate_func = rich_plqupdate_func;
        rich_wlqupdate_func = _rich_wlqupdate_func;
        isNICPLQ = false;
        isNICWLQ = false;
        isRichPLQ = true;
        isRichWLQ = true;
        used = false;
        used4Nesting = false;
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
     *  \param _plq_degree parallelism degree of the PLQ stage
     *  \param _wlq_degree parallelism degree of the WLQ stage
     *  \param _name string with the unique name of the operator
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
              size_t _plq_degree,
              size_t _wlq_degree,
              std::string _name,
              closing_func_t _closing_func,
              bool _ordered,
              opt_level_t _opt_level):
              Pane_Farm(_plq_func, _wlqupdate_func, _win_len, _slide_len, _triggering_delay, _winType, _plq_degree, _wlq_degree, _name, _closing_func, _ordered, _opt_level, OperatorConfig(0, 1, _slide_len, 0, 1, _slide_len))
    {
        plq_func = _plq_func;
        wlqupdate_func = _wlqupdate_func;
        isNICPLQ = true;
        isNICWLQ = false;
        isRichPLQ = false;
        isRichWLQ = false;
        used = false;
        used4Nesting = false;
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
     *  \param _plq_degree parallelism degree of the PLQ stage
     *  \param _wlq_degree parallelism degree of the WLQ stage
     *  \param _name string with the unique name of the operator
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
              size_t _plq_degree,
              size_t _wlq_degree,
              std::string _name,
              closing_func_t _closing_func,
              bool _ordered,
              opt_level_t _opt_level):
              Pane_Farm(_rich_plq_func, _wlqupdate_func, _win_len, _slide_len, _triggering_delay, _winType, _plq_degree, _wlq_degree, _name, _closing_func, _ordered, _opt_level, OperatorConfig(0, 1, _slide_len, 0, 1, _slide_len))
    {
        rich_plq_func = _rich_plq_func;
        wlqupdate_func = _wlqupdate_func;
        isNICPLQ = true;
        isNICWLQ = false;
        isRichPLQ = true;
        isRichWLQ = false;
        used = false;
        used4Nesting = false;
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
     *  \param _plq_degree parallelism degree of the PLQ stage
     *  \param _wlq_degree parallelism degree of the WLQ stage
     *  \param _name string with the unique name of the operator
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
              size_t _plq_degree,
              size_t _wlq_degree,
              std::string _name,
              closing_func_t _closing_func,
              bool _ordered,
              opt_level_t _opt_level):
              Pane_Farm(_plq_func, _rich_wlqupdate_func, _win_len, _slide_len, _triggering_delay, _winType, _plq_degree, _wlq_degree, _name, _closing_func, _ordered, _opt_level, OperatorConfig(0, 1, _slide_len, 0, 1, _slide_len))
    {
        plq_func = _plq_func;
        rich_wlqupdate_func = _rich_wlqupdate_func;
        isNICPLQ = true;
        isNICWLQ = false;
        isRichPLQ = false;
        isRichWLQ = true;
        used = false;
        used4Nesting = false;
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
     *  \param _plq_degree parallelism degree of the PLQ stage
     *  \param _wlq_degree parallelism degree of the WLQ stage
     *  \param _name string with the unique name of the operator
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
              size_t _plq_degree,
              size_t _wlq_degree,
              std::string _name,
              closing_func_t _closing_func,
              bool _ordered,
              opt_level_t _opt_level):
              Pane_Farm(_rich_plq_func, _rich_wlqupdate_func, _win_len, _slide_len, _triggering_delay, _winType, _plq_degree, _wlq_degree, _name, _closing_func, _ordered, _opt_level, OperatorConfig(0, 1, _slide_len, 0, 1, _slide_len))
    {
        rich_plq_func = _rich_plq_func;
        rich_wlqupdate_func = _rich_wlqupdate_func;
        isNICPLQ = true;
        isNICWLQ = false;
        isRichPLQ = true;
        isRichWLQ = true;
        used = false;
        used4Nesting = false;
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
     *  \param _plq_degree parallelism degree of the PLQ stage
     *  \param _wlq_degree parallelism degree of the WLQ stage
     *  \param _name string with the unique name of the operator
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
              size_t _plq_degree,
              size_t _wlq_degree,
              std::string _name,
              closing_func_t _closing_func,
              bool _ordered,
              opt_level_t _opt_level):
              Pane_Farm(_plqupdate_func, _wlq_func, _win_len, _slide_len, _triggering_delay, _winType, _plq_degree, _wlq_degree, _name, _closing_func, _ordered, _opt_level, OperatorConfig(0, 1, _slide_len, 0, 1, _slide_len))
    {
        plqupdate_func = _plqupdate_func;
        wlq_func = _wlq_func;
        isNICPLQ = false;
        isNICWLQ = true;
        isRichPLQ = false;
        isRichWLQ = false;
        used = false;
        used4Nesting = false;
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
     *  \param _plq_degree parallelism degree of the PLQ stage
     *  \param _wlq_degree parallelism degree of the WLQ stage
     *  \param _name string with the unique name of the operator
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
              size_t _plq_degree,
              size_t _wlq_degree,
              std::string _name,
              closing_func_t _closing_func,
              bool _ordered,
              opt_level_t _opt_level):
              Pane_Farm(_rich_plqupdate_func, _wlq_func, _win_len, _slide_len, _triggering_delay, _winType, _plq_degree, _wlq_degree, _name, _closing_func, _ordered, _opt_level, OperatorConfig(0, 1, _slide_len, 0, 1, _slide_len))
    {
        rich_plqupdate_func = _rich_plqupdate_func;
        wlq_func = _wlq_func;
        isNICPLQ = false;
        isNICWLQ = true;
        isRichPLQ = true;
        isRichWLQ = false;
        used = false;
        used4Nesting = false;
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
     *  \param _plq_degree parallelism degree of the PLQ stage
     *  \param _wlq_degree parallelism degree of the WLQ stage
     *  \param _name string with the unique name of the operator
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
              size_t _plq_degree,
              size_t _wlq_degree,
              std::string _name,
              closing_func_t _closing_func,
              bool _ordered,
              opt_level_t _opt_level):
              Pane_Farm(_plqupdate_func, _rich_wlq_func, _win_len, _slide_len, _triggering_delay, _winType, _plq_degree, _wlq_degree, _name, _closing_func, _ordered, _opt_level, OperatorConfig(0, 1, _slide_len, 0, 1, _slide_len))
    {
        plqupdate_func = _plqupdate_func;
        rich_wlq_func = _rich_wlq_func;
        isNICPLQ = false;
        isNICWLQ = true;
        isRichPLQ = false;
        isRichWLQ = true;
        used = false;
        used4Nesting = false;
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
     *  \param _plq_degree parallelism degree of the PLQ stage
     *  \param _wlq_degree parallelism degree of the WLQ stage
     *  \param _name string with the unique name of the operator
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
              size_t _plq_degree,
              size_t _wlq_degree,
              std::string _name,
              closing_func_t _closing_func,
              bool _ordered,
              opt_level_t _opt_level):
              Pane_Farm(_rich_plqupdate_func, _rich_wlq_func, _win_len, _slide_len, _triggering_delay, _winType, _plq_degree, _wlq_degree, _name, _closing_func, _ordered, _opt_level, OperatorConfig(0, 1, _slide_len, 0, 1, _slide_len))
    {
        rich_plqupdate_func = _rich_plqupdate_func;
        rich_wlq_func = _rich_wlq_func;
        isNICPLQ = false;
        isNICWLQ = true;
        isRichPLQ = true;
        isRichWLQ = true;
        used = false;
        used4Nesting = false;
    }

    /** 
     *  \brief Get the optimization level used to build the operator
     *  \return adopted utilization level by the operator
     */ 
    opt_level_t getOptLevel() const
    {
      return opt_level;
    }

    /** 
     *  \brief Get the window type (CB or TB) utilized by the operator
     *  \return adopted windowing semantics (count- or time-based)
     */ 
    win_type_t getWinType() const
    {
      return winType;
    }

    /** 
     *  \brief Get the parallelism degree of the PLQ stage
     *  \return PLQ parallelism degree
     */ 
    size_t getPLQParallelism() const
    {
      return plq_degree;
    }

    /** 
     *  \brief Get the parallelism degree of the WLQ stage
     *  \return WLQ parallelism degree
     */ 
    size_t getWLQParallelism() const
    {
      return wlq_degree;
    }

    /** 
     *  \brief Check whether the Pane_Farm has been used in a MultiPipe
     *  \return true if the Pane_Farm has been added/chained to an existing MultiPipe
     */
    bool isUsed() const
    {
        return used;
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
     *  \brief Get the number of dropped tuples by the Pane_Farm
     *  \return number of tuples dropped during the processing by the Pane_Farm
     */ 
    size_t getNumDroppedTuples() const
    {
        size_t count = 0;
        if (plq_degree == 1) {
            auto *seq = static_cast<Win_Seq<tuple_t, result_t, input_t> *>(plq);
            count += seq->getNumDroppedTuples();
        }
        else {
            auto *wf = static_cast<Win_Farm<tuple_t, result_t, input_t> *>(plq);
            count += wf->getNumDroppedTuples();
        }
        return count;
    }

    /// deleted constructors/operators
    Pane_Farm(const Pane_Farm &) = delete; // copy constructor
    Pane_Farm(Pane_Farm &&) = delete; // move constructor
    Pane_Farm &operator=(const Pane_Farm &) = delete; // copy assignment operator
    Pane_Farm &operator=(Pane_Farm &&) = delete; // move assignment operator
};

} // namespace wf

#endif
