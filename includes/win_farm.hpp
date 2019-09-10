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
 *  @file    win_farm.hpp
 *  @author  Gabriele Mencagli
 *  @date    03/10/2017
 *  
 *  @brief Win_Farm pattern executing a windowed transformation in parallel on
 *         multi-core CPUs
 *  
 *  @section Win_Farm (Description)
 *  
 *  This file implements the Win_Farm pattern able to executes windowed queries on a
 *  multicore. The pattern executes streaming windows in parallel on the CPU cores
 *  and supports both a non-incremental and an incremental query definition.
 *  
 *  The template parameters tuple_t and result_t must be default constructible, with a
 *  copy Constructor and copy assignment operator, and they must provide and implement
 *  the setControlFields() and getControlFields() methods.
 */ 

#ifndef WIN_FARM_H
#define WIN_FARM_H

/// includes
#include <ff/pipeline.hpp>
#include <ff/all2all.hpp>
#include <ff/farm.hpp>
#include <ff/optimize.hpp>
#include <basic.hpp>
#include <win_seq.hpp>
#include <wf_nodes.hpp>
#include <wm_nodes.hpp>
#include <ordering_node.hpp>
#include <tree_combiner.hpp>
#include <transformations.hpp>

namespace wf {

/** 
 *  \class Win_Farm
 *  
 *  \brief Win_Farm pattern executing a windowed transformation in parallel on multi-core CPUs
 *  
 *  This class implements the Win_Farm pattern executing windowed queries in parallel on
 *  a multicore.
 */ 
template<typename tuple_t, typename result_t, typename input_t>
class Win_Farm: public ff::ff_farm
{
public:
    /// type of the non-incremental window processing function
    using win_func_t = std::function<void(uint64_t, Iterable<tuple_t> &, result_t &)>;
    /// type of the rich non-incremental window processing function
    using rich_win_func_t = std::function<void(uint64_t, Iterable<tuple_t> &, result_t &, RuntimeContext &)>;
    /// type of the incremental window processing function
    using winupdate_func_t = std::function<void(uint64_t, const tuple_t &, result_t &)>;
    /// type of the rich incremental window processing function
    using rich_winupdate_func_t = std::function<void(uint64_t, const tuple_t &, result_t &, RuntimeContext &)>;
    /// type of the closing function
    using closing_func_t = std::function<void(RuntimeContext &)>;
    /// type of the Pane_Farm passed to the proper nesting Constructor
    using pane_farm_t = Pane_Farm<tuple_t, result_t>;
    /// type of the Win_MapReduce passed to the proper nesting Constructor
    using win_mapreduce_t = Win_MapReduce<tuple_t, result_t>;

private:
    // type of the wrapper of input tuples
    using wrapper_in_t = wrapper_tuple_t<tuple_t>;
    // type of the WF_Emitter node
    using wf_emitter_t = WF_Emitter<tuple_t, input_t>;
    // type of the WF_Collector node
    using wf_collector_t = WF_Collector<result_t>;
    // type of the Win_Seq to be created within the regular Constructor
    using win_seq_t = Win_Seq<tuple_t, result_t, wrapper_in_t>;
    // friendships with other classes in the library
    template<typename T1, typename T2, typename T3>
    friend class Pane_Farm;
    template<typename T1, typename T2, typename T3>
    friend class Win_MapReduce;
    template<typename T1, typename T2, typename T3, typename T4>
    friend class Pane_Farm_GPU;
    template<typename T1, typename T2, typename T3, typename T4>
    friend class Win_MapReduce_GPU;
    template<typename T>
    friend auto get_WF_nested_type(T);
    // flag stating whether the Win_Farm has been instantiated with complex workers (Pane_Farm or Win_MapReduce)
    bool hasComplexWorkers;
    // optimization level of the Win_Farm
    opt_level_t outer_opt_level;
    // optimization level of the inner patterns
    opt_level_t inner_opt_level;
    // type of the inner patterns
    pattern_t inner_type;
    // parallelism of the Win_Farm
    size_t parallelism;
    // parallelism degrees of the inner patterns
    size_t inner_parallelism_1;
    size_t inner_parallelism_2;
    // number of emitters
    size_t num_emitters;
    // window type (CB or TB)
    win_type_t winType;

    // Private Constructor I (stub)
    Win_Farm() {}

    // Private Constructor II
    template<typename F_t>
    Win_Farm(F_t _func,
             uint64_t _win_len,
             uint64_t _slide_len,
             win_type_t _winType,
             size_t _emitter_degree,
             size_t _pardegree,
             std::string _name,
             closing_func_t _closing_func,
             bool _ordered,
             opt_level_t _opt_level,
             PatternConfig _config,
             role_t _role):
             hasComplexWorkers(false),
             outer_opt_level(_opt_level),
             inner_opt_level(LEVEL0),
             inner_type(SEQ_CPU),
             parallelism(_pardegree),
             inner_parallelism_1(1),
             inner_parallelism_2(0),
             num_emitters(_emitter_degree),
             winType(_winType)
    {
        // check the validity of the windowing parameters
        if (_win_len == 0 || _slide_len == 0) {
            std::cerr << RED << "WindFlow Error: window length or slide cannot be zero" << DEFAULT << std::endl;
            exit(EXIT_FAILURE);
        }
        // check the validity of the emitter degree
        if (_emitter_degree == 0) {
            std::cerr << RED << "WindFlow Error: at least one emitter is needed" << DEFAULT << std::endl;
            exit(EXIT_FAILURE);
        }
        // check the validity of the parallelism degree
        if (_pardegree == 0) {
            std::cerr << RED << "WindFlow Error: parallelism degree cannot be zero" << DEFAULT << std::endl;
            exit(EXIT_FAILURE);
        }
        // check the optimization level
        if (_opt_level != LEVEL0) {
            //std::cerr << YELLOW << "WindFlow Warning: optimization level has no effect" << DEFAULT << std::endl;
            outer_opt_level = LEVEL0;
        }
        // std::vector of Win_Seq
        std::vector<ff_node *> w;
        // private sliding factor of each Win_Seq
        uint64_t private_slide = _slide_len * _pardegree;
        // standard case: one Emitter node
        if (_emitter_degree == 1) {
            // create the Win_Seq
            for (size_t i = 0; i < _pardegree; i++) {
                // configuration structure of the Win_Seq
                PatternConfig configSeq(_config.id_inner, _config.n_inner, _config.slide_inner, i, _pardegree, _slide_len);
                auto *seq = new win_seq_t(_func, _win_len, private_slide, _winType, _name + "_wf", _closing_func, RuntimeContext(_pardegree, i), configSeq, _role);
                w.push_back(seq);
            }
        }
        // advanced case: multiple Emitter nodes
        else {
            ff::ff_a2a *a2a = new ff::ff_a2a();
            // create the Emitter nodes
            std::vector<ff_node *> emitters(_emitter_degree);
            for (size_t i = 0; i < _emitter_degree; i++) {
                auto *emitter = new wf_emitter_t(_winType, _win_len, _slide_len, _pardegree, _config.id_inner, _config.n_inner, _config.slide_inner, _role);
                emitters[i] = emitter;
            }
            a2a->add_firstset(emitters, 0, true);
            // create the Win_Seq nodes composed with an orderingNodes
            std::vector<ff_node *> seqs(_pardegree);
            for (size_t i = 0; i < _pardegree; i++) {
                auto *ord = new Ordering_Node<tuple_t, wrapper_in_t>(((_winType == CB) ? ID : TS));
                // configuration structure of the Win_Seq
                PatternConfig configSeq(_config.id_inner, _config.n_inner, _config.slide_inner, i, _pardegree, _slide_len);
                auto *seq = new win_seq_t(_func, _win_len, private_slide, _winType, _name + "_wf", _closing_func, RuntimeContext(_pardegree, i), configSeq, _role);
                auto *comb = new ff::ff_comb(ord, seq, true, true);
                seqs[i] = comb;
            }
            a2a->add_secondset(seqs, true);
            w.push_back(a2a);
        }
        ff::ff_farm::add_workers(w);
        // create the Emitter and Collector nodes
        if (_emitter_degree == 1)
            ff::ff_farm::add_emitter(new wf_emitter_t(_winType, _win_len, _slide_len, _pardegree, _config.id_inner, _config.n_inner, _config.slide_inner, _role));
        if (_ordered)
            ff::ff_farm::add_collector(new wf_collector_t());
        else
            ff::ff_farm::add_collector(nullptr);
        // when the Win_Farm will be destroyed we need aslo to destroy the emitter, workers and collector
        ff::ff_farm::cleanup_all();
    }

    // method to optimize the structure of the Win_Farm pattern
    template<typename inner_emitter_t>
    void optimize_WinFarm(opt_level_t opt)
    {
        if (opt == LEVEL0) // no optimization
            return;
        else if (opt == LEVEL1) // optimization level 1
            remove_internal_collectors(*this); // remove all the default collectors in the Win_Farm
        else { // optimization level 2
            if (num_emitters == 1) {
                wf_emitter_t *wf_e = static_cast<wf_emitter_t *>(this->getEmitter());
                auto &oldWorkers = this->getWorkers();
                std::vector<inner_emitter_t *> Es;
                bool tobeTransformmed = true;
                // change the workers by removing their first emitter (if any)
                for (auto *w: oldWorkers) {
                    ff::ff_pipeline *pipe = static_cast<ff::ff_pipeline *>(w);
                    ff_node *e = remove_emitter_from_pipe(*pipe);
                    if (e == nullptr)
                        tobeTransformmed = false;
                    else {
                        inner_emitter_t *my_e = static_cast<inner_emitter_t *>(e);
                        Es.push_back(my_e);
                    }
                }
                if (tobeTransformmed) {
                    // create the tree emitter
                    auto *treeEmitter = new TreeComb<wf_emitter_t, inner_emitter_t>(wf_e, Es);
                    this->cleanup_emitter(false);
                    this->change_emitter(treeEmitter, true);
                }
                remove_internal_collectors(*this);
                return;
            }
            else {
                std::cerr << YELLOW << "WindFlow Warning: Optimization LEVEL2 is incompatible with multiple emitters -> LEVEL1 used instead!" << DEFAULT << std::endl;
                remove_internal_collectors(*this);
                return;
            }
        }
    }

public:
    /** 
     *  \brief Constructor I
     *  
     *  \param _win_func the non-incremental window processing function
     *  \param _win_len window length (in no. of tuples or in time units)
     *  \param _slide_len slide length (in no. of tuples or in time units)
     *  \param _winType window type (count-based CB or time-based TB)
     *  \param _emitter_degree number of replicas of the emitter node
     *  \param _pardegree parallelism degree of the Win_Farm pattern
     *  \param _name std::string with the unique name of the pattern
     *  \param _closing_func closing function
     *  \param _ordered true if the results of the same key must be emitted in order, false otherwise
     *  \param _opt_level optimization level used to build the pattern
     */ 
    Win_Farm(win_func_t _win_func,
             uint64_t _win_len,
             uint64_t _slide_len,
             win_type_t _winType,
             size_t _emitter_degree,
             size_t _pardegree,
             std::string _name,
             closing_func_t _closing_func,
             bool _ordered,
             opt_level_t _opt_level):
             Win_Farm(_win_func, _win_len, _slide_len, _winType, _emitter_degree, _pardegree, _name, _closing_func, _ordered, _opt_level, PatternConfig(0, 1, _slide_len, 0, 1, _slide_len), SEQ)
    {}

    /** 
     *  \brief Constructor II
     *  
     *  \param _rich_win_func the rich non-incremental window processing function
     *  \param _win_len window length (in no. of tuples or in time units)
     *  \param _slide_len slide length (in no. of tuples or in time units)
     *  \param _winType window type (count-based CB or time-based TB)
     *  \param _emitter_degree number of replicas of the emitter node
     *  \param _pardegree parallelism degree of the Win_Farm pattern
     *  \param _name std::string with the unique name of the pattern
     *  \param _closing_func closing function
     *  \param _ordered true if the results of the same key must be emitted in order, false otherwise
     *  \param _opt_level optimization level used to build the pattern
     */ 
    Win_Farm(rich_win_func_t _rich_win_func,
             uint64_t _win_len,
             uint64_t _slide_len,
             win_type_t _winType,
             size_t _emitter_degree,
             size_t _pardegree,
             std::string _name,
             closing_func_t _closing_func,
             bool _ordered,
             opt_level_t _opt_level):
             Win_Farm(_rich_win_func, _win_len, _slide_len, _winType, _emitter_degree, _pardegree, _name, _closing_func, _ordered, _opt_level, PatternConfig(0, 1, _slide_len, 0, 1, _slide_len), SEQ)
    {}

    /** 
     *  \brief Constructor III
     *  
     *  \param _winupdate_func the incremental window processing function
     *  \param _win_len window length (in no. of tuples or in time units)
     *  \param _slide_len slide length (in no. of tuples or in time units)
     *  \param _winType window type (count-based CB or time-based TB)
     *  \param _emitter_degree number of replicas of the emitter node
     *  \param _pardegree parallelism degree of the Win_Farm pattern
     *  \param _name std::string with the unique name of the pattern
     *  \param _closing_func closing function
     *  \param _ordered true if the results of the same key must be emitted in order, false otherwise
     *  \param _opt_level optimization level used to build the pattern
     */ 
    Win_Farm(winupdate_func_t _winupdate_func,
             uint64_t _win_len,
             uint64_t _slide_len,
             win_type_t _winType,
             size_t _emitter_degree,
             size_t _pardegree,
             std::string _name,
             closing_func_t _closing_func,
             bool _ordered,
             opt_level_t _opt_level):
             Win_Farm(_winupdate_func, _win_len, _slide_len, _winType, _emitter_degree, _pardegree, _name, _closing_func, _ordered, _opt_level, PatternConfig(0, 1, _slide_len, 0, 1, _slide_len), SEQ)
    {}

    /** 
     *  \brief Constructor IV
     *  
     *  \param _rich_winupdate_func the rich incremental window processing function
     *  \param _win_len window length (in no. of tuples or in time units)
     *  \param _slide_len slide length (in no. of tuples or in time units)
     *  \param _winType window type (count-based CB or time-based TB)
     *  \param _emitter_degree number of replicas of the emitter node
     *  \param _pardegree parallelism degree of the Win_Farm pattern
     *  \param _name std::string with the unique name of the pattern
     *  \param _closing_func closing function
     *  \param _ordered true if the results of the same key must be emitted in order, false otherwise
     *  \param _opt_level optimization level used to build the pattern
     */ 
    Win_Farm(rich_winupdate_func_t _rich_winupdate_func,
             uint64_t _win_len,
             uint64_t _slide_len,
             win_type_t _winType,
             size_t _emitter_degree,
             size_t _pardegree,
             std::string _name,
             closing_func_t _closing_func,
             bool _ordered,
             opt_level_t _opt_level):
             Win_Farm(_rich_winupdate_func, _win_len, _slide_len, _winType, _emitter_degree, _pardegree, _name, _closing_func, _ordered, _opt_level, PatternConfig(0, 1, _slide_len, 0, 1, _slide_len), SEQ)
    {}

    /** 
     *  \brief Constructor V (Nesting with Pane_Farm)
     *  
     *  \param _pf Pane_Farm to be replicated within the Win_Farm pattern
     *  \param _win_len window length (in no. of tuples or in time units)
     *  \param _slide_len slide length (in no. of tuples or in time units)
     *  \param _winType window type (count-based CB or time-based TB)
     *  \param _emitter_degree number of replicas of the emitter node
     *  \param _pardegree parallelism degree of the Win_Farm pattern
     *  \param _name std::string with the unique name of the pattern
     *  \param _closing_func closing function
     *  \param _ordered true if the results of the same key must be emitted in order, false otherwise
     *  \param _opt_level optimization level used to build the pattern
     */ 
    Win_Farm(const pane_farm_t &_pf,
             uint64_t _win_len,
             uint64_t _slide_len,
             win_type_t _winType,
             size_t _emitter_degree,
             size_t _pardegree,
             std::string _name,
             closing_func_t _closing_func,
             bool _ordered,
             opt_level_t _opt_level):
             hasComplexWorkers(true),
             outer_opt_level(_opt_level),
             inner_type(PF_CPU),
             parallelism(_pardegree),
             num_emitters(_emitter_degree),
             winType(_winType)
    {
        // type of the Pane_Farm to be created within the Win_Farm pattern
        using panewrap_farm_t = Pane_Farm<tuple_t, result_t, wrapper_in_t>;
        // type of the PLQ emitter in the first stage of the Pane_Farm
        using plq_emitter_t = WF_Emitter<tuple_t, wrapper_in_t>;
        // check the validity of the windowing parameters
        if (_win_len == 0 || _slide_len == 0) {
            std::cerr << RED << "WindFlow Error: window length or slide cannot be zero" << DEFAULT << std::endl;
            exit(EXIT_FAILURE);
        }
        // check the validity of the emitter degree
        if (_emitter_degree == 0) {
            std::cerr << RED << "WindFlow Error: at least one emitter is needed" << DEFAULT << std::endl;
            exit(EXIT_FAILURE);
        }
        // check the validity of the parallelism degree
        if (_pardegree == 0) {
            std::cerr << RED << "WindFlow Error: parallelism degree cannot be zero" << DEFAULT << std::endl;
            exit(EXIT_FAILURE);
        }
        // check the compatibility of the windowing parameters
        if (_pf.win_len != _win_len || _pf.slide_len != _slide_len || _pf.winType != _winType) {
            std::cerr << RED << "WindFlow Error: incompatible windowing parameters" << DEFAULT << std::endl;
            exit(EXIT_FAILURE);
        }
        inner_opt_level = _pf.opt_level;
        inner_parallelism_1 = _pf.plq_degree;
        inner_parallelism_2 = _pf.wlq_degree;
        // std::vector of Pane_Farm
        std::vector<ff_node *> w;
        // standard case: one Emitter node
        if (_emitter_degree == 1) {
            // create the Pane_Farm starting from the input one
            for (size_t i = 0; i < _pardegree; i++) {
                // configuration structure of the Pane_Farm
                PatternConfig configPF(0, 1, _slide_len, i, _pardegree, _slide_len);
                // create the correct Pane_Farm
                panewrap_farm_t *pf_W = nullptr;
                if (_pf.isNICPLQ && _pf.isNICWLQ && !_pf.isRichPLQ && !_pf.isRichWLQ)
                    pf_W = new panewrap_farm_t(_pf.plq_func, _pf.wlq_func, _pf.win_len, _pf.slide_len * _pardegree, _pf.winType, _pf.plq_degree, _pf.wlq_degree, _name + "_wf_" + std::to_string(i), _pf.closing_func, false, _pf.opt_level, configPF);
                if (_pf.isNICPLQ && !_pf.isNICWLQ && !_pf.isRichPLQ && !_pf.isRichWLQ)
                    pf_W = new panewrap_farm_t(_pf.plq_func, _pf.wlqupdate_func, _pf.win_len, _pf.slide_len * _pardegree, _pf.winType, _pf.plq_degree, _pf.wlq_degree, _name + "_wf_" + std::to_string(i), _pf.closing_func, false, _pf.opt_level, configPF);
                if (!_pf.isNICPLQ && _pf.isNICWLQ && !_pf.isRichPLQ && !_pf.isRichWLQ)
                    pf_W = new panewrap_farm_t(_pf.plqupdate_func, _pf.wlq_func, _pf.win_len, _pf.slide_len * _pardegree, _pf.winType, _pf.plq_degree, _pf.wlq_degree, _name + "_wf_" + std::to_string(i), _pf.closing_func, false, _pf.opt_level, configPF);
                if (!_pf.isNICPLQ && !_pf.isNICWLQ && !_pf.isRichPLQ && !_pf.isRichWLQ)
                    pf_W = new panewrap_farm_t(_pf.plqupdate_func, _pf.wlqupdate_func, _pf.win_len, _pf.slide_len * _pardegree, _pf.winType, _pf.plq_degree, _pf.wlq_degree, _name + "_wf_" + std::to_string(i), _pf.closing_func, false, _pf.opt_level, configPF);
                if (_pf.isNICPLQ && _pf.isNICWLQ && _pf.isRichPLQ && !_pf.isRichWLQ)
                    pf_W = new panewrap_farm_t(_pf.rich_plq_func, _pf.wlq_func, _pf.win_len, _pf.slide_len * _pardegree, _pf.winType, _pf.plq_degree, _pf.wlq_degree, _name + "_wf_" + std::to_string(i), _pf.closing_func, false, _pf.opt_level, configPF);
                if (_pf.isNICPLQ && !_pf.isNICWLQ && _pf.isRichPLQ && !_pf.isRichWLQ)
                    pf_W = new panewrap_farm_t(_pf.rich_plq_func, _pf.wlqupdate_func, _pf.win_len, _pf.slide_len * _pardegree, _pf.winType, _pf.plq_degree, _pf.wlq_degree, _name + "_wf_" + std::to_string(i), _pf.closing_func, false, _pf.opt_level, configPF);
                if (!_pf.isNICPLQ && _pf.isNICWLQ && _pf.isRichPLQ && !_pf.isRichWLQ)
                    pf_W = new panewrap_farm_t(_pf.rich_plqupdate_func, _pf.wlq_func, _pf.win_len, _pf.slide_len * _pardegree, _pf.winType, _pf.plq_degree, _pf.wlq_degree, _name + "_wf_" + std::to_string(i), _pf.closing_func, false, _pf.opt_level, configPF);
                if (!_pf.isNICPLQ && !_pf.isNICWLQ && _pf.isRichPLQ && !_pf.isRichWLQ)
                    pf_W = new panewrap_farm_t(_pf.rich_plqupdate_func, _pf.wlqupdate_func, _pf.win_len, _pf.slide_len * _pardegree, _pf.winType, _pf.plq_degree, _pf.wlq_degree, _name + "_wf_" + std::to_string(i), _pf.closing_func, false, _pf.opt_level, configPF);
                if (_pf.isNICPLQ && _pf.isNICWLQ && !_pf.isRichPLQ && _pf.isRichWLQ)
                    pf_W = new panewrap_farm_t(_pf.plq_func, _pf.rich_wlq_func, _pf.win_len, _pf.slide_len * _pardegree, _pf.winType, _pf.plq_degree, _pf.wlq_degree, _name + "_wf_" + std::to_string(i), _pf.closing_func, false, _pf.opt_level, configPF);
                if (_pf.isNICPLQ && !_pf.isNICWLQ && !_pf.isRichPLQ && _pf.isRichWLQ)
                    pf_W = new panewrap_farm_t(_pf.plq_func, _pf.rich_wlqupdate_func, _pf.win_len, _pf.slide_len * _pardegree, _pf.winType, _pf.plq_degree, _pf.wlq_degree, _name + "_wf_" + std::to_string(i), _pf.closing_func, false, _pf.opt_level, configPF);
                if (!_pf.isNICPLQ && _pf.isNICWLQ && !_pf.isRichPLQ && _pf.isRichWLQ)
                    pf_W = new panewrap_farm_t(_pf.plqupdate_func, _pf.rich_wlq_func, _pf.win_len, _pf.slide_len * _pardegree, _pf.winType, _pf.plq_degree, _pf.wlq_degree, _name + "_wf_" + std::to_string(i), _pf.closing_func, false, _pf.opt_level, configPF);
                if (!_pf.isNICPLQ && !_pf.isNICWLQ && !_pf.isRichPLQ && _pf.isRichWLQ)
                    pf_W = new panewrap_farm_t(_pf.plqupdate_func, _pf.rich_wlqupdate_func, _pf.win_len, _pf.slide_len * _pardegree, _pf.winType, _pf.plq_degree, _pf.wlq_degree, _name + "_wf_" + std::to_string(i), _pf.closing_func, false, _pf.opt_level, configPF);
                if (_pf.isNICPLQ && _pf.isNICWLQ && _pf.isRichPLQ && _pf.isRichWLQ)
                    pf_W = new panewrap_farm_t(_pf.rich_plq_func, _pf.rich_wlq_func, _pf.win_len, _pf.slide_len * _pardegree, _pf.winType, _pf.plq_degree, _pf.wlq_degree, _name + "_wf_" + std::to_string(i), _pf.closing_func, false, _pf.opt_level, configPF);
                if (_pf.isNICPLQ && !_pf.isNICWLQ && _pf.isRichPLQ && _pf.isRichWLQ)
                    pf_W = new panewrap_farm_t(_pf.rich_plq_func, _pf.rich_wlqupdate_func, _pf.win_len, _pf.slide_len * _pardegree, _pf.winType, _pf.plq_degree, _pf.wlq_degree, _name + "_wf_" + std::to_string(i), _pf.closing_func, false, _pf.opt_level, configPF);
                if (!_pf.isNICPLQ && _pf.isNICWLQ && _pf.isRichPLQ && _pf.isRichWLQ)
                    pf_W = new panewrap_farm_t(_pf.rich_plqupdate_func, _pf.rich_wlq_func, _pf.win_len, _pf.slide_len * _pardegree, _pf.winType, _pf.plq_degree, _pf.wlq_degree, _name + "_wf_" + std::to_string(i), _pf.closing_func, false, _pf.opt_level, configPF);
                if (!_pf.isNICPLQ && !_pf.isNICWLQ && _pf.isRichPLQ && _pf.isRichWLQ)
                    pf_W = new panewrap_farm_t(_pf.rich_plqupdate_func, _pf.rich_wlqupdate_func, _pf.win_len, _pf.slide_len * _pardegree, _pf.winType, _pf.plq_degree, _pf.wlq_degree, _name + "_wf_" + std::to_string(i), _pf.closing_func, false, _pf.opt_level, configPF);
                w.push_back(pf_W);
            }
        }
        // advanced case: multiple Emitter nodes
        else {
            ff::ff_a2a *a2a = new ff::ff_a2a();
            // create the Emitter nodes
            std::vector<ff_node *> emitters(_emitter_degree);
            for (size_t i = 0; i < _emitter_degree; i++) {
                auto *emitter = new wf_emitter_t(_winType, _win_len, _slide_len, _pardegree, 0, 1, _slide_len, SEQ);
                emitters[i] = emitter;
            }
            a2a->add_firstset(emitters, 0, true);
            // create the correct Pane_Farm
            std::vector<ff_node *> pfs(_pardegree);
            for (size_t i = 0; i < _pardegree; i++) {
                // an ordering node must be composed before the first node of the Pane_Farm
                auto *ord = new Ordering_Node<tuple_t, wrapper_in_t>(((_winType == CB) ? ID : TS));
                // configuration structure of the Pane_Farm
                PatternConfig configPF(0, 1, _slide_len, i, _pardegree, _slide_len);
                // create the correct Pane_Farm
                panewrap_farm_t *pf_W = nullptr;
                if (_pf.isNICPLQ && _pf.isNICWLQ && !_pf.isRichPLQ && !_pf.isRichWLQ)
                    pf_W = new panewrap_farm_t(_pf.plq_func, _pf.wlq_func, _pf.win_len, _pf.slide_len * _pardegree, _pf.winType, _pf.plq_degree, _pf.wlq_degree, _name + "_wf_" + std::to_string(i), _pf.closing_func, false, _pf.opt_level, configPF);
                if (_pf.isNICPLQ && !_pf.isNICWLQ && !_pf.isRichPLQ && !_pf.isRichWLQ)
                    pf_W = new panewrap_farm_t(_pf.plq_func, _pf.wlqupdate_func, _pf.win_len, _pf.slide_len * _pardegree, _pf.winType, _pf.plq_degree, _pf.wlq_degree, _name + "_wf_" + std::to_string(i), _pf.closing_func, false, _pf.opt_level, configPF);
                if (!_pf.isNICPLQ && _pf.isNICWLQ && !_pf.isRichPLQ && !_pf.isRichWLQ)
                    pf_W = new panewrap_farm_t(_pf.plqupdate_func, _pf.wlq_func, _pf.win_len, _pf.slide_len * _pardegree, _pf.winType, _pf.plq_degree, _pf.wlq_degree, _name + "_wf_" + std::to_string(i), _pf.closing_func, false, _pf.opt_level, configPF);
                if (!_pf.isNICPLQ && !_pf.isNICWLQ && !_pf.isRichPLQ && !_pf.isRichWLQ)
                    pf_W = new panewrap_farm_t(_pf.plqupdate_func, _pf.wlqupdate_func, _pf.win_len, _pf.slide_len * _pardegree, _pf.winType, _pf.plq_degree, _pf.wlq_degree, _name + "_wf_" + std::to_string(i), _pf.closing_func, false, _pf.opt_level, configPF);
                if (_pf.isNICPLQ && _pf.isNICWLQ && _pf.isRichPLQ && !_pf.isRichWLQ)
                    pf_W = new panewrap_farm_t(_pf.rich_plq_func, _pf.wlq_func, _pf.win_len, _pf.slide_len * _pardegree, _pf.winType, _pf.plq_degree, _pf.wlq_degree, _name + "_wf_" + std::to_string(i), _pf.closing_func, false, _pf.opt_level, configPF);
                if (_pf.isNICPLQ && !_pf.isNICWLQ && _pf.isRichPLQ && !_pf.isRichWLQ)
                    pf_W = new panewrap_farm_t(_pf.rich_plq_func, _pf.wlqupdate_func, _pf.win_len, _pf.slide_len * _pardegree, _pf.winType, _pf.plq_degree, _pf.wlq_degree, _name + "_wf_" + std::to_string(i), _pf.closing_func, false, _pf.opt_level, configPF);
                if (!_pf.isNICPLQ && _pf.isNICWLQ && _pf.isRichPLQ && !_pf.isRichWLQ)
                    pf_W = new panewrap_farm_t(_pf.rich_plqupdate_func, _pf.wlq_func, _pf.win_len, _pf.slide_len * _pardegree, _pf.winType, _pf.plq_degree, _pf.wlq_degree, _name + "_wf_" + std::to_string(i), _pf.closing_func, false, _pf.opt_level, configPF);
                if (!_pf.isNICPLQ && !_pf.isNICWLQ && _pf.isRichPLQ && !_pf.isRichWLQ)
                    pf_W = new panewrap_farm_t(_pf.rich_plqupdate_func, _pf.wlqupdate_func, _pf.win_len, _pf.slide_len * _pardegree, _pf.winType, _pf.plq_degree, _pf.wlq_degree, _name + "_wf_" + std::to_string(i), _pf.closing_func, false, _pf.opt_level, configPF);
                if (_pf.isNICPLQ && _pf.isNICWLQ && !_pf.isRichPLQ && _pf.isRichWLQ)
                    pf_W = new panewrap_farm_t(_pf.plq_func, _pf.rich_wlq_func, _pf.win_len, _pf.slide_len * _pardegree, _pf.winType, _pf.plq_degree, _pf.wlq_degree, _name + "_wf_" + std::to_string(i), _pf.closing_func, false, _pf.opt_level, configPF);
                if (_pf.isNICPLQ && !_pf.isNICWLQ && !_pf.isRichPLQ && _pf.isRichWLQ)
                    pf_W = new panewrap_farm_t(_pf.plq_func, _pf.rich_wlqupdate_func, _pf.win_len, _pf.slide_len * _pardegree, _pf.winType, _pf.plq_degree, _pf.wlq_degree, _name + "_wf_" + std::to_string(i), _pf.closing_func, false, _pf.opt_level, configPF);
                if (!_pf.isNICPLQ && _pf.isNICWLQ && !_pf.isRichPLQ && _pf.isRichWLQ)
                    pf_W = new panewrap_farm_t(_pf.plqupdate_func, _pf.rich_wlq_func, _pf.win_len, _pf.slide_len * _pardegree, _pf.winType, _pf.plq_degree, _pf.wlq_degree, _name + "_wf_" + std::to_string(i), _pf.closing_func, false, _pf.opt_level, configPF);
                if (!_pf.isNICPLQ && !_pf.isNICWLQ && !_pf.isRichPLQ && _pf.isRichWLQ)
                    pf_W = new panewrap_farm_t(_pf.plqupdate_func, _pf.rich_wlqupdate_func, _pf.win_len, _pf.slide_len * _pardegree, _pf.winType, _pf.plq_degree, _pf.wlq_degree, _name + "_wf_" + std::to_string(i), _pf.closing_func, false, _pf.opt_level, configPF);
                if (_pf.isNICPLQ && _pf.isNICWLQ && _pf.isRichPLQ && _pf.isRichWLQ)
                    pf_W = new panewrap_farm_t(_pf.rich_plq_func, _pf.rich_wlq_func, _pf.win_len, _pf.slide_len * _pardegree, _pf.winType, _pf.plq_degree, _pf.wlq_degree, _name + "_wf_" + std::to_string(i), _pf.closing_func, false, _pf.opt_level, configPF);
                if (_pf.isNICPLQ && !_pf.isNICWLQ && _pf.isRichPLQ && _pf.isRichWLQ)
                    pf_W = new panewrap_farm_t(_pf.rich_plq_func, _pf.rich_wlqupdate_func, _pf.win_len, _pf.slide_len * _pardegree, _pf.winType, _pf.plq_degree, _pf.wlq_degree, _name + "_wf_" + std::to_string(i), _pf.closing_func, false, _pf.opt_level, configPF);
                if (!_pf.isNICPLQ && _pf.isNICWLQ && _pf.isRichPLQ && _pf.isRichWLQ)
                    pf_W = new panewrap_farm_t(_pf.rich_plqupdate_func, _pf.rich_wlq_func, _pf.win_len, _pf.slide_len * _pardegree, _pf.winType, _pf.plq_degree, _pf.wlq_degree, _name + "_wf_" + std::to_string(i), _pf.closing_func, false, _pf.opt_level, configPF);
                if (!_pf.isNICPLQ && !_pf.isNICWLQ && _pf.isRichPLQ && _pf.isRichWLQ)
                    pf_W = new panewrap_farm_t(_pf.rich_plqupdate_func, _pf.rich_wlqupdate_func, _pf.win_len, _pf.slide_len * _pardegree, _pf.winType, _pf.plq_degree, _pf.wlq_degree, _name + "_wf_" + std::to_string(i), _pf.closing_func, false, _pf.opt_level, configPF);
                // combine the first node of the Pane_Farm with the buffering node
                combine_with_firststage(*pf_W, ord, true);
                pfs[i] = pf_W;
            }
            a2a->add_secondset(pfs, true);
            w.push_back(a2a);
        }
        ff::ff_farm::add_workers(w);
        // create the Emitter and Collector nodes
        if (_emitter_degree == 1)
            ff::ff_farm::add_emitter(new wf_emitter_t(_winType, _win_len, _slide_len, _pardegree, 0, 1, _slide_len, SEQ));
        if (_ordered)
            ff::ff_farm::add_collector(new wf_collector_t());
        else
            ff::ff_farm::add_collector(nullptr);
        // optimization process according to the provided optimization level
        this->optimize_WinFarm<plq_emitter_t>(_opt_level);
        // when the Win_Farm will be destroyed we need aslo to destroy the emitter, workers and collector
        ff::ff_farm::cleanup_all();
    }

    /** 
     *  \brief Constructor IV (Nesting with Win_MapReduce)
     *  
     *  \param _wm Win_MapReduce to be replicated within the Win_Farm pattern
     *  \param _win_len window length (in no. of tuples or in time units)
     *  \param _slide_len slide length (in no. of tuples or in time units)
     *  \param _winType window type (count-based CB or time-based TB)
     *  \param _emitter_degree number of replicas of the emitter node
     *  \param _pardegree parallelism degree of the Win_Farm pattern
     *  \param _name std::string with the unique name of the pattern
     *  \param _closing_func closing function
     *  \param _ordered true if the results of the same key must be emitted in order, false otherwise
     *  \param _opt_level optimization level used to build the pattern
     */ 
    Win_Farm(const win_mapreduce_t &_wm,
             uint64_t _win_len,
             uint64_t _slide_len,
             win_type_t _winType,
             size_t _emitter_degree,
             size_t _pardegree,
             std::string _name,
             closing_func_t _closing_func,
             bool _ordered,
             opt_level_t _opt_level):
             hasComplexWorkers(true),
             outer_opt_level(_opt_level),
             inner_type(WMR_CPU),
             parallelism(_pardegree),
             num_emitters(_emitter_degree),
             winType(_winType)
    {
        // type of the Win_MapReduce to be created within the Win_Farm pattern
        using winwrap_map_t = Win_MapReduce<tuple_t, result_t, wrapper_in_t>;
        // type of the MAP emitter in the first stage of the Win_MapReduce
        using map_emitter_t = WinMap_Emitter<tuple_t, wrapper_in_t>;
        // check the validity of the windowing parameters
        if (_win_len == 0 || _slide_len == 0) {
            std::cerr << RED << "WindFlow Error: window length or slide cannot be zero" << DEFAULT << std::endl;
            exit(EXIT_FAILURE);
        }
        // check the validity of the emitter degree
        if (_emitter_degree == 0) {
            std::cerr << RED << "WindFlow Error: at least one emitter is needed" << DEFAULT << std::endl;
            exit(EXIT_FAILURE);
        }
        // check the validity of the parallelism degree
        if (_pardegree == 0) {
            std::cerr << RED << "WindFlow Error: parallelism degree cannot be zero" << DEFAULT << std::endl;
            exit(EXIT_FAILURE);
        }
        // check the compatibility of the windowing parameters
        if (_wm.win_len != _win_len || _wm.slide_len != _slide_len || _wm.winType != _winType) {
            std::cerr << RED << "WindFlow Error: incompatible windowing parameters" << DEFAULT << std::endl;
            exit(EXIT_FAILURE);
        }
        inner_opt_level = _wm.opt_level;
        inner_parallelism_1 = _wm.map_degree;
        inner_parallelism_2 = _wm.reduce_degree;
        // std::vector of Win_MapReduce
        std::vector<ff_node *> w;
        // standard case: one Emitter node
        if (_emitter_degree == 1) {
            // create the Win_MapReduce starting from the input one
            for (size_t i = 0; i < _pardegree; i++) {
                // configuration structure of the Win_mapReduce
                PatternConfig configWM(0, 1, _slide_len, i, _pardegree, _slide_len);
                // create the correct Win_MapReduce
                winwrap_map_t *wm_W = nullptr;
                if (_wm.isNICMAP && _wm.isNICREDUCE && !_wm.isRichMAP && !_wm.isRichREDUCE)
                    wm_W = new winwrap_map_t(_wm.map_func, _wm.reduce_func, _wm.win_len, _wm.slide_len * _pardegree, _wm.winType, _wm.map_degree, _wm.reduce_degree, _name + "_wf_" + std::to_string(i), _wm.closing_func, false, _wm.opt_level, configWM);
                if (_wm.isNICMAP && !_wm.isNICREDUCE && !_wm.isRichMAP && !_wm.isRichREDUCE)
                    wm_W = new winwrap_map_t(_wm.map_func, _wm.reduceupdate_func, _wm.win_len, _wm.slide_len * _pardegree, _wm.winType, _wm.map_degree, _wm.reduce_degree, _name + "_wf_" + std::to_string(i), _wm.closing_func, false, _wm.opt_level, configWM);
                if (!_wm.isNICMAP && _wm.isNICREDUCE && !_wm.isRichMAP && !_wm.isRichREDUCE)
                    wm_W = new winwrap_map_t(_wm.mapupdate_func, _wm.reduce_func, _wm.win_len, _wm.slide_len * _pardegree, _wm.winType, _wm.map_degree, _wm.reduce_degree, _name + "_wf_" + std::to_string(i), _wm.closing_func, false, _wm.opt_level, configWM);
                if (!_wm.isNICMAP && !_wm.isNICREDUCE && !_wm.isRichMAP && !_wm.isRichREDUCE)
                    wm_W = new winwrap_map_t(_wm.mapupdate_func, _wm.reduceupdate_func, _wm.win_len, _wm.slide_len * _pardegree, _wm.winType, _wm.map_degree, _wm.reduce_degree, _name + "_wf_" + std::to_string(i), _wm.closing_func, false, _wm.opt_level, configWM);
                if (_wm.isNICMAP && _wm.isNICREDUCE && _wm.isRichMAP && !_wm.isRichREDUCE)
                    wm_W = new winwrap_map_t(_wm.rich_map_func, _wm.reduce_func, _wm.win_len, _wm.slide_len * _pardegree, _wm.winType, _wm.map_degree, _wm.reduce_degree, _name + "_wf_" + std::to_string(i), _wm.closing_func, false, _wm.opt_level, configWM);
                if (_wm.isNICMAP && !_wm.isNICREDUCE && _wm.isRichMAP && !_wm.isRichREDUCE)
                    wm_W = new winwrap_map_t(_wm.rich_map_func, _wm.reduceupdate_func, _wm.win_len, _wm.slide_len * _pardegree, _wm.winType, _wm.map_degree, _wm.reduce_degree, _name + "_wf_" + std::to_string(i), _wm.closing_func, false, _wm.opt_level, configWM);
                if (!_wm.isNICMAP && _wm.isNICREDUCE && _wm.isRichMAP && !_wm.isRichREDUCE)
                    wm_W = new winwrap_map_t(_wm.rich_mapupdate_func, _wm.reduce_func, _wm.win_len, _wm.slide_len * _pardegree, _wm.winType, _wm.map_degree, _wm.reduce_degree, _name + "_wf_" + std::to_string(i), _wm.closing_func, false, _wm.opt_level, configWM);
                if (!_wm.isNICMAP && !_wm.isNICREDUCE && _wm.isRichMAP && !_wm.isRichREDUCE)
                    wm_W = new winwrap_map_t(_wm.rich_mapupdate_func, _wm.reduceupdate_func, _wm.win_len, _wm.slide_len * _pardegree, _wm.winType, _wm.map_degree, _wm.reduce_degree, _name + "_wf_" + std::to_string(i), _wm.closing_func, false, _wm.opt_level, configWM);
                if (_wm.isNICMAP && _wm.isNICREDUCE && !_wm.isRichMAP && _wm.isRichREDUCE)
                    wm_W = new winwrap_map_t(_wm.map_func, _wm.rich_reduce_func, _wm.win_len, _wm.slide_len * _pardegree, _wm.winType, _wm.map_degree, _wm.reduce_degree, _name + "_wf_" + std::to_string(i), _wm.closing_func, false, _wm.opt_level, configWM);
                if (_wm.isNICMAP && !_wm.isNICREDUCE && !_wm.isRichMAP && _wm.isRichREDUCE)
                    wm_W = new winwrap_map_t(_wm.map_func, _wm.rich_reduceupdate_func, _wm.win_len, _wm.slide_len * _pardegree, _wm.winType, _wm.map_degree, _wm.reduce_degree, _name + "_wf_" + std::to_string(i), _wm.closing_func, false, _wm.opt_level, configWM);
                if (!_wm.isNICMAP && _wm.isNICREDUCE && !_wm.isRichMAP && _wm.isRichREDUCE)
                    wm_W = new winwrap_map_t(_wm.mapupdate_func, _wm.rich_reduce_func, _wm.win_len, _wm.slide_len * _pardegree, _wm.winType, _wm.map_degree, _wm.reduce_degree, _name + "_wf_" + std::to_string(i), _wm.closing_func, false, _wm.opt_level, configWM);
                if (!_wm.isNICMAP && !_wm.isNICREDUCE && !_wm.isRichMAP && _wm.isRichREDUCE)
                    wm_W = new winwrap_map_t(_wm.mapupdate_func, _wm.rich_reduceupdate_func, _wm.win_len, _wm.slide_len * _pardegree, _wm.winType, _wm.map_degree, _wm.reduce_degree, _name + "_wf_" + std::to_string(i), _wm.closing_func, false, _wm.opt_level, configWM);
                if (_wm.isNICMAP && _wm.isNICREDUCE && _wm.isRichMAP && _wm.isRichREDUCE)
                    wm_W = new winwrap_map_t(_wm.rich_map_func, _wm.rich_reduce_func, _wm.win_len, _wm.slide_len * _pardegree, _wm.winType, _wm.map_degree, _wm.reduce_degree, _name + "_wf_" + std::to_string(i), _wm.closing_func, false, _wm.opt_level, configWM);
                if (_wm.isNICMAP && !_wm.isNICREDUCE && _wm.isRichMAP && _wm.isRichREDUCE)
                    wm_W = new winwrap_map_t(_wm.rich_map_func, _wm.rich_reduceupdate_func, _wm.win_len, _wm.slide_len * _pardegree, _wm.winType, _wm.map_degree, _wm.reduce_degree, _name + "_wf_" + std::to_string(i), _wm.closing_func, false, _wm.opt_level, configWM);
                if (!_wm.isNICMAP && _wm.isNICREDUCE && _wm.isRichMAP && _wm.isRichREDUCE)
                    wm_W = new winwrap_map_t(_wm.rich_mapupdate_func, _wm.rich_reduce_func, _wm.win_len, _wm.slide_len * _pardegree, _wm.winType, _wm.map_degree, _wm.reduce_degree, _name + "_wf_" + std::to_string(i), _wm.closing_func, false, _wm.opt_level, configWM);
                if (!_wm.isNICMAP && !_wm.isNICREDUCE && _wm.isRichMAP && _wm.isRichREDUCE)
                    wm_W = new winwrap_map_t(_wm.rich_mapupdate_func, _wm.rich_reduceupdate_func, _wm.win_len, _wm.slide_len * _pardegree, _wm.winType, _wm.map_degree, _wm.reduce_degree, _name + "_wf_" + std::to_string(i), _wm.closing_func, false, _wm.opt_level, configWM);
                w.push_back(wm_W);
            }
        }
        // advanced case: multiple Emitter nodes
        else {
            ff::ff_a2a *a2a = new ff::ff_a2a();
            // create the Emitter nodes
            std::vector<ff_node *> emitters(_emitter_degree);
            for (size_t i = 0; i < _emitter_degree; i++) {
                auto *emitter = new wf_emitter_t(_winType, _win_len, _slide_len, _pardegree, 0, 1, _slide_len, SEQ);
                emitters[i] = emitter;
            }
            a2a->add_firstset(emitters, 0, true);
            // create the correct Win_MapReduce
            std::vector<ff_node *> wms(_pardegree);
            for (size_t i = 0; i < _pardegree; i++) {
                // an ordering node must be composed before the first node of the Win_MapReduce
                auto *ord = new Ordering_Node<tuple_t, wrapper_in_t>(((_winType == CB) ? ID : TS));
                // configuration structure of the Win_MapReduce
                PatternConfig configWM(0, 1, _slide_len, i, _pardegree, _slide_len);
                // create the correct Win_MapReduce
                winwrap_map_t *wm_W = nullptr;
                if (_wm.isNICMAP && _wm.isNICREDUCE && !_wm.isRichMAP && !_wm.isRichREDUCE)
                    wm_W = new winwrap_map_t(_wm.map_func, _wm.reduce_func, _wm.win_len, _wm.slide_len * _pardegree, _wm.winType, _wm.map_degree, _wm.reduce_degree, _name + "_wf_" + std::to_string(i), _wm.closing_func, false, _wm.opt_level, configWM);
                if (_wm.isNICMAP && !_wm.isNICREDUCE && !_wm.isRichMAP && !_wm.isRichREDUCE)
                    wm_W = new winwrap_map_t(_wm.map_func, _wm.reduceupdate_func, _wm.win_len, _wm.slide_len * _pardegree, _wm.winType, _wm.map_degree, _wm.reduce_degree, _name + "_wf_" + std::to_string(i), _wm.closing_func, false, _wm.opt_level, configWM);
                if (!_wm.isNICMAP && _wm.isNICREDUCE && !_wm.isRichMAP && !_wm.isRichREDUCE)
                    wm_W = new winwrap_map_t(_wm.mapupdate_func, _wm.reduce_func, _wm.win_len, _wm.slide_len * _pardegree, _wm.winType, _wm.map_degree, _wm.reduce_degree, _name + "_wf_" + std::to_string(i), _wm.closing_func, false, _wm.opt_level, configWM);
                if (!_wm.isNICMAP && !_wm.isNICREDUCE && !_wm.isRichMAP && !_wm.isRichREDUCE)
                    wm_W = new winwrap_map_t(_wm.mapupdate_func, _wm.reduceupdate_func, _wm.win_len, _wm.slide_len * _pardegree, _wm.winType, _wm.map_degree, _wm.reduce_degree, _name + "_wf_" + std::to_string(i), _wm.closing_func, false, _wm.opt_level, configWM);
                if (_wm.isNICMAP && _wm.isNICREDUCE && _wm.isRichMAP && !_wm.isRichREDUCE)
                    wm_W = new winwrap_map_t(_wm.rich_map_func, _wm.reduce_func, _wm.win_len, _wm.slide_len * _pardegree, _wm.winType, _wm.map_degree, _wm.reduce_degree, _name + "_wf_" + std::to_string(i), _wm.closing_func, false, _wm.opt_level, configWM);
                if (_wm.isNICMAP && !_wm.isNICREDUCE && _wm.isRichMAP && !_wm.isRichREDUCE)
                    wm_W = new winwrap_map_t(_wm.rich_map_func, _wm.reduceupdate_func, _wm.win_len, _wm.slide_len * _pardegree, _wm.winType, _wm.map_degree, _wm.reduce_degree, _name + "_wf_" + std::to_string(i), _wm.closing_func, false, _wm.opt_level, configWM);
                if (!_wm.isNICMAP && _wm.isNICREDUCE && _wm.isRichMAP && !_wm.isRichREDUCE)
                    wm_W = new winwrap_map_t(_wm.rich_mapupdate_func, _wm.reduce_func, _wm.win_len, _wm.slide_len * _pardegree, _wm.winType, _wm.map_degree, _wm.reduce_degree, _name + "_wf_" + std::to_string(i), _wm.closing_func, false, _wm.opt_level, configWM);
                if (!_wm.isNICMAP && !_wm.isNICREDUCE && _wm.isRichMAP && !_wm.isRichREDUCE)
                    wm_W = new winwrap_map_t(_wm.rich_mapupdate_func, _wm.reduceupdate_func, _wm.win_len, _wm.slide_len * _pardegree, _wm.winType, _wm.map_degree, _wm.reduce_degree, _name + "_wf_" + std::to_string(i), _wm.closing_func, false, _wm.opt_level, configWM);
                if (_wm.isNICMAP && _wm.isNICREDUCE && !_wm.isRichMAP && _wm.isRichREDUCE)
                    wm_W = new winwrap_map_t(_wm.map_func, _wm.rich_reduce_func, _wm.win_len, _wm.slide_len * _pardegree, _wm.winType, _wm.map_degree, _wm.reduce_degree, _name + "_wf_" + std::to_string(i), _wm.closing_func, false, _wm.opt_level, configWM);
                if (_wm.isNICMAP && !_wm.isNICREDUCE && !_wm.isRichMAP && _wm.isRichREDUCE)
                    wm_W = new winwrap_map_t(_wm.map_func, _wm.rich_reduceupdate_func, _wm.win_len, _wm.slide_len * _pardegree, _wm.winType, _wm.map_degree, _wm.reduce_degree, _name + "_wf_" + std::to_string(i), _wm.closing_func, false, _wm.opt_level, configWM);
                if (!_wm.isNICMAP && _wm.isNICREDUCE && !_wm.isRichMAP && _wm.isRichREDUCE)
                    wm_W = new winwrap_map_t(_wm.mapupdate_func, _wm.rich_reduce_func, _wm.win_len, _wm.slide_len * _pardegree, _wm.winType, _wm.map_degree, _wm.reduce_degree, _name + "_wf_" + std::to_string(i), _wm.closing_func, false, _wm.opt_level, configWM);
                if (!_wm.isNICMAP && !_wm.isNICREDUCE && !_wm.isRichMAP && _wm.isRichREDUCE)
                    wm_W = new winwrap_map_t(_wm.mapupdate_func, _wm.rich_reduceupdate_func, _wm.win_len, _wm.slide_len * _pardegree, _wm.winType, _wm.map_degree, _wm.reduce_degree, _name + "_wf_" + std::to_string(i), _wm.closing_func, false, _wm.opt_level, configWM);
                if (_wm.isNICMAP && _wm.isNICREDUCE && _wm.isRichMAP && _wm.isRichREDUCE)
                    wm_W = new winwrap_map_t(_wm.rich_map_func, _wm.rich_reduce_func, _wm.win_len, _wm.slide_len * _pardegree, _wm.winType, _wm.map_degree, _wm.reduce_degree, _name + "_wf_" + std::to_string(i), _wm.closing_func, false, _wm.opt_level, configWM);
                if (_wm.isNICMAP && !_wm.isNICREDUCE && _wm.isRichMAP && _wm.isRichREDUCE)
                    wm_W = new winwrap_map_t(_wm.rich_map_func, _wm.rich_reduceupdate_func, _wm.win_len, _wm.slide_len * _pardegree, _wm.winType, _wm.map_degree, _wm.reduce_degree, _name + "_wf_" + std::to_string(i), _wm.closing_func, false, _wm.opt_level, configWM);
                if (!_wm.isNICMAP && _wm.isNICREDUCE && _wm.isRichMAP && _wm.isRichREDUCE)
                    wm_W = new winwrap_map_t(_wm.rich_mapupdate_func, _wm.rich_reduce_func, _wm.win_len, _wm.slide_len * _pardegree, _wm.winType, _wm.map_degree, _wm.reduce_degree, _name + "_wf_" + std::to_string(i), _wm.closing_func, false, _wm.opt_level, configWM);
                if (!_wm.isNICMAP && !_wm.isNICREDUCE && _wm.isRichMAP && _wm.isRichREDUCE)
                    wm_W = new winwrap_map_t(_wm.rich_mapupdate_func, _wm.rich_reduceupdate_func, _wm.win_len, _wm.slide_len * _pardegree, _wm.winType, _wm.map_degree, _wm.reduce_degree, _name + "_wf_" + std::to_string(i), _wm.closing_func, false, _wm.opt_level, configWM);
                // combine the first node of the Win_MapReduce with the buffering node
                combine_with_firststage(*wm_W, ord, true);
                wms[i] = wm_W;
            }
            a2a->add_secondset(wms, true);
            w.push_back(a2a);    
        }
        ff::ff_farm::add_workers(w);
        // create the Emitter and Collector nodes
        if (_emitter_degree == 1)
            ff::ff_farm::add_emitter(new wf_emitter_t(_winType, _win_len, _slide_len, _pardegree, 0, 1, _slide_len, SEQ));
        if (_ordered)
            ff::ff_farm::add_collector(new wf_collector_t());
        else
            ff::ff_farm::add_collector(nullptr);
        // optimization process according to the provided optimization level
        this->optimize_WinFarm<map_emitter_t>(_opt_level);
        // when the Win_Farm will be destroyed we need aslo to destroy the emitter, workers and collector
        ff::ff_farm::cleanup_all();
    }

    /** 
     *  \brief Check whether the Win_Farm has been instantiated with complex patterns inside
     *  \return true if the Win_Farm has complex patterns inside
     */ 
    bool useComplexNesting() const { return hasComplexWorkers; }

    /** 
     *  \brief Get the optimization level used to build the pattern
     *  \return adopted utilization level by the pattern
     */ 
    opt_level_t getOptLevel() const { return outer_opt_level; }

    /** 
     *  \brief Type of the inner patterns used by this Win_Farm
     *  \return type of the inner patterns
     */ 
    pattern_t getInnerType() const { return inner_type; }

    /** 
     *  \brief Get the optimization level of the inner patterns within this Win_Farm
     *  \return adopted utilization level by the inner patterns
     */ 
    opt_level_t getInnerOptLevel() const { return inner_opt_level; }

    /** 
     *  \brief Get the parallelism degree of the Win_Farm
     *  \return parallelism degree of the Win_Farm
     */ 
    size_t getParallelism() const { return parallelism; }

    /** 
     *  \brief Get the parallelism degrees of the inner patterns within this Win_Farm
     *  \return parallelism degrees of the inner patterns
     */ 
    std::pair<size_t, size_t> getInnerParallelism() const { return std::make_pair(inner_parallelism_1, inner_parallelism_2); }

    /** 
     *  \brief Get the number of emitter used by this Win_Farm
     *  \return number of emitters
     */ 
    size_t getNumEmitters() const { return num_emitters; }

    /** 
     *  \brief Get the window type (CB or TB) utilized by the pattern
     *  \return adopted windowing semantics (count- or time-based)
     */ 
    win_type_t getWinType() const { return winType; }
};

} // namespace wf

#endif
