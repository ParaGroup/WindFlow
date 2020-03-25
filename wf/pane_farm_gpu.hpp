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
 *  @file    pane_farm_gpu.hpp
 *  @author  Gabriele Mencagli
 *  @date    22/05/2018
 *  
 *  @brief Pane_Farm_GPU operator executing a windowed query in parallel
 *         on a CPU+GPU system
 *  
 *  @section Pane_Farm_GPU (Description)
 *  
 *  This file implements the Pane_Farm_GPU operator able to execute windowed queries on a
 *  heterogeneous system (CPU+GPU). The operator processes (possibly in parallel) panes of
 *  the windows in the so-called PLQ stage (Pane-Level Sub-Query) and computes (possibly
 *  in parallel) results of tne windows from the pane results in the so-called WLQ stage
 *  (Window-Level Sub-Query). Panes shared by more than one window are not recomputed by
 *  saving processing time. The operator allows the user to offload either the PLQ or the WLQ
 *  processing on the GPU while the other stage is executed on the CPU with either a non
 *  incremental or an incremental query definition.
 *  
 *  The template parameters tuple_t and result_t must be default constructible, with a
 *  copy constructor and copy assignment operator, and they must provide and implement
 *  the setControlFields() and getControlFields() methods. The third template argument F_t
 *  is the type of the callable object to be used for GPU processing (either for the PLQ
 *  or for the WLQ).
 */ 

#ifndef PANE_FARM_GPU_H
#define PANE_FARM_GPU_H

/// includes
#include <ff/pipeline.hpp>
#include <ff/farm.hpp>
#include <win_farm.hpp>
#include <win_farm_gpu.hpp>
#include <basic.hpp>
#include <meta.hpp>

namespace wf {

/** 
 *  \class Pane_Farm_GPU
 *  
 *  \brief Pane_Farm_GPU operator executing a windowed query in parallel
 *         on a CPU+GPU system
 *  
 *  This class implements the Pane_Farm_GPU operator executing windowed queries in
 *  parallel on a heterogeneous system (CPU+GPU). The operator processes (possibly
 *  in parallel) panes in the PLQ stage while window results are built out from the
 *  pane results (possibly in parallel) in the WLQ stage. Either the PLQ or the WLQ
 *  stage are executed on the GPU device while the others is executed on the CPU as
 *  in the Pane_Farm operator.
 */ 
template<typename tuple_t, typename result_t, typename F_t, typename input_t>
class Pane_Farm_GPU: public ff::ff_pipeline
{
public:
    /// function type of the non-incremental pane processing
    using plq_func_t = std::function<void(uint64_t, const Iterable<tuple_t> &, result_t &)>;
    /// Function type of the incremental pane processing
    using plqupdate_func_t = std::function<void(uint64_t, const tuple_t &, result_t &)>;
    /// function type of the non-incremental window processing
    using wlq_func_t = std::function<void(uint64_t, const Iterable<result_t> &, result_t &)>;
    /// function type of the incremental window function
    using wlqupdate_func_t = std::function<void(uint64_t, const result_t &, result_t &)>;

private:
    // friendships with other classes in the library
    template<typename T1, typename T2, typename T3, typename T4>
    friend class Win_Farm_GPU;
    template<typename T1, typename T2, typename T3>
    friend class Key_Farm_GPU;
    template<typename T>
    friend class WinFarmGPU_Builder;
    template<typename T>
    friend class KeyFarmGPU_Builder;
    friend class MultiPipe;
    // configuration variables of the Pane_Farm_GPU
    F_t gpuFunction;
    plq_func_t plq_func;
    plqupdate_func_t plqupdate_func;
    wlq_func_t wlq_func;
    wlqupdate_func_t wlqupdate_func;
    bool isGPUPLQ;
    bool isGPUWLQ;
    bool isNICPLQ;
    bool isNICWLQ;
    uint64_t win_len;
    uint64_t slide_len;
    uint64_t triggering_delay;
    win_type_t winType;
    size_t plq_degree;
    size_t wlq_degree;
    size_t batch_len;
    size_t n_thread_block;
    std::string name;
    size_t scratchpad_size;
    bool ordered;
    opt_level_t opt_level;
    OperatorConfig config;
    bool used; // true if the operator has been added/chained in a MultiPipe
    bool used4Nesting; // true if the operator has been used in a nested structure
    std::vector<ff_node *> plq_workers; // vector of pointers to the Win_Seq or Win_Seq_GPU instances in the PLQ stage

    // Private Constructor I
    Pane_Farm_GPU(F_t _gpuFunction,
                  wlq_func_t _wlq_func,
                  uint64_t _win_len,
                  uint64_t _slide_len,
                  uint64_t _triggering_delay,
                  win_type_t _winType,
                  size_t _plq_degree,
                  size_t _wlq_degree,
                  size_t _batch_len,
                  size_t _n_thread_block,
                  std::string _name,
                  size_t _scratchpad_size,
                  bool _ordered,
                  opt_level_t _opt_level,
                  OperatorConfig _config):
                  gpuFunction(_gpuFunction),
                  wlq_func(_wlq_func),
                  isGPUPLQ(true),
                  isGPUWLQ(false),
                  isNICPLQ(true),
                  isNICWLQ(true),
                  win_len(_win_len),
                  slide_len(_slide_len),
                  triggering_delay(_triggering_delay),
                  winType(_winType),
                  plq_degree(_plq_degree),
                  wlq_degree(_wlq_degree),
                  batch_len(_batch_len),
                  n_thread_block(_n_thread_block),
                  name(_name),
                  scratchpad_size(_scratchpad_size),
                  ordered(_ordered),
                  opt_level(_opt_level),
                  config(_config)
    {
        // check the validity of the windowing parameters
        if (_win_len == 0 || _slide_len == 0) {
            std::cerr << RED << "WindFlow Error: window length or slide in Pane_Farm_GPU cannot be zero" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // check the validity of the parallelism degrees
        if (_plq_degree == 0 || _wlq_degree == 0) {
            std::cerr << RED << "WindFlow Error: Pane_Farm_GPU has parallelism zero" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // check the validity of the batch length
        if (_batch_len == 0) {
            std::cerr << RED << "WindFlow Error: batch length in Pane_Farm_GPU cannot be zero" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // the Pane_Farm_GPU can be utilized with sliding windows only
        if (_win_len <= _slide_len) {
            std::cerr << RED << "WindFlow Error: Pane_Farm_GPU can be used with sliding windows only (s<w)" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // compute the pane length (no. of tuples or in time units)
        uint64_t _pane_len = gcd(_win_len, _slide_len);
        // general fastflow pointers to the PLQ and WLQ stages
        ff_node *plq_stage, *wlq_stage;
        auto closing_func = [] (RuntimeContext &) { return; };
        // create the first stage PLQ
        if (_plq_degree > 1) {
            // configuration structure of the Win_Farm_GPU (PLQ)
            OperatorConfig configWFPLQ(_config.id_outer, _config.n_outer, _config.slide_outer, _config.id_inner, _config.n_inner, _config.slide_inner);
            auto *plq_wf = new Win_Farm_GPU<tuple_t, result_t, F_t, input_t>(_gpuFunction, _pane_len, _pane_len, _triggering_delay, _winType, _plq_degree, _batch_len, _n_thread_block, _name + "_plq", _scratchpad_size, true, LEVEL0, configWFPLQ, PLQ);
            plq_stage = plq_wf;
            for (auto *w: plq_wf->getWorkers()) {
                plq_workers.push_back(w);
            }
        }
        else {
            // configuration structure of the Win_Seq_GPU (PLQ)
            OperatorConfig configSeqPLQ(_config.id_inner, _config.n_inner, _config.slide_inner, 0, 1, _pane_len);
            auto *plq_seq = new Win_Seq_GPU<tuple_t, result_t, F_t, input_t>(_gpuFunction, _pane_len, _pane_len, _triggering_delay, _winType, _batch_len, _n_thread_block, _name + "_plq", _scratchpad_size, configSeqPLQ, PLQ);
            plq_stage = plq_seq;
            plq_workers.push_back(plq_seq);
        }
        // create the second stage WLQ
        if (_wlq_degree > 1) {
            // configuration structure of the Win_Farm (WLQ)
            OperatorConfig configWFWLQ(_config.id_outer, _config.n_outer, _config.slide_outer, _config.id_inner, _config.n_inner, _config.slide_inner);
            auto *wlq_wf = new Win_Farm<result_t, result_t>(_wlq_func, (_win_len/_pane_len), (_slide_len/_pane_len), 0, CB, _wlq_degree, _name + "_wlq", closing_func, _ordered, LEVEL0, configWFWLQ, WLQ);
            wlq_stage = wlq_wf;
        }
        else {
            // configuration structure of the Win_Seq (WLQ)
            OperatorConfig configSeqWLQ(_config.id_inner, _config.n_inner, _config.slide_inner, 0, 1, (_slide_len/_pane_len));
            auto *wlq_seq = new Win_Seq<result_t, result_t>(_wlq_func, (_win_len/_pane_len), (_slide_len/_pane_len), 0, CB, _name + "_wlq", closing_func, RuntimeContext(1, 0), configSeqWLQ, WLQ);
            wlq_stage = wlq_seq;
        }
        // add to this the pipeline optimized according to the provided optimization level
        ff::ff_pipeline::add_stage(optimize_PaneFarmGPU(plq_stage, wlq_stage, _opt_level));
        // when the Pane_Farm_GPU will be destroyed we need aslo to destroy the two stages
        ff::ff_pipeline::cleanup_nodes();
        // flatten the pipeline
        ff::ff_pipeline::flatten();
    }

    // Private Constructor II
    Pane_Farm_GPU(F_t _gpuFunction,
                  wlqupdate_func_t _wlqupdate_func,
                  uint64_t _win_len,
                  uint64_t _slide_len,
                  uint64_t _triggering_delay,
                  win_type_t _winType,
                  size_t _plq_degree,
                  size_t _wlq_degree,
                  size_t _batch_len,
                  size_t _n_thread_block,
                  std::string _name,
                  size_t _scratchpad_size,
                  bool _ordered,
                  opt_level_t _opt_level,
                  OperatorConfig _config):
                  gpuFunction(_gpuFunction),
                  wlqupdate_func(_wlqupdate_func),
                  isGPUPLQ(true),
                  isGPUWLQ(false),
                  isNICPLQ(true),
                  isNICWLQ(false),
                  win_len(_win_len),
                  slide_len(_slide_len),
                  triggering_delay(_triggering_delay),
                  winType(_winType),
                  plq_degree(_plq_degree),
                  wlq_degree(_wlq_degree),
                  batch_len(_batch_len),
                  n_thread_block(_n_thread_block),
                  name(_name),
                  scratchpad_size(_scratchpad_size),
                  ordered(_ordered),
                  opt_level(_opt_level),
                  config(_config)
    {
        // check the validity of the windowing parameters
        if (_win_len == 0 || _slide_len == 0) {
            std::cerr << RED << "WindFlow Error: window length or slide in Pane_Farm_GPU cannot be zero" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // check the validity of the parallelism degrees
        if (_plq_degree == 0 || _wlq_degree == 0) {
            std::cerr << RED << "WindFlow Error: Pane_Farm_GPU has parallelism zero" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // check the validity of the batch length
        if (_batch_len == 0) {
            std::cerr << RED << "WindFlow Error: batch length in Pane_Farm_GPU cannot be zero" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // the Pane_Farm_GPU can be utilized with sliding windows only
        if (_win_len <= _slide_len) {
            std::cerr << RED << "WindFlow Error: Pane_Farm_GPU can be used with sliding windows only (s<w)" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // compute the pane length (no. of tuples or in time units)
        uint64_t _pane_len = gcd(_win_len, _slide_len);
        // general fastflow pointers to the PLQ and WLQ stages
        ff_node *plq_stage, *wlq_stage;
        auto closing_func = [] (RuntimeContext &) { return; };
        // create the first stage PLQ
        if (_plq_degree > 1) {
            // configuration structure of the Win_Farm_GPU (PLQ)
            OperatorConfig configWFPLQ(_config.id_outer, _config.n_outer, _config.slide_outer, _config.id_inner, _config.n_inner, _config.slide_inner);
            auto *plq_wf = new Win_Farm_GPU<tuple_t, result_t, F_t, input_t>(_gpuFunction, _pane_len, _pane_len, _triggering_delay, _winType, _plq_degree, _batch_len, _n_thread_block, _name + "_plq", _scratchpad_size, true, LEVEL0, configWFPLQ, PLQ);
            plq_stage = plq_wf;
            for (auto *w: plq_wf->getWorkers()) {
                plq_workers.push_back(w);
            }
        }
        else {
            // configuration structure of the Win_Seq_GPU (PLQ)
            OperatorConfig configSeqPLQ(_config.id_inner, _config.n_inner, _config.slide_inner, 0, 1, _pane_len);
            auto *plq_seq = new Win_Seq_GPU<tuple_t, result_t, F_t, input_t>(_gpuFunction, _pane_len, _pane_len, _triggering_delay, _winType, _batch_len, _n_thread_block, _name + "_plq", _scratchpad_size, configSeqPLQ, PLQ);
            plq_stage = plq_seq;
            plq_workers.push_back(plq_seq);
        }
        // create the second stage WLQ
        if (_wlq_degree > 1) {
            // configuration structure of the Win_Farm (WLQ)
            OperatorConfig configWFWLQ(_config.id_outer, _config.n_outer, _config.slide_outer, _config.id_inner, _config.n_inner, _config.slide_inner);
            auto *wlq_wf = new Win_Farm<result_t, result_t>(_wlqupdate_func, (_win_len/_pane_len), (_slide_len/_pane_len), 0, CB, _wlq_degree, _name + "_wlq", closing_func, _ordered, LEVEL0, configWFWLQ, WLQ);
            wlq_stage = wlq_wf;
        }
        else {
            // configuration structure of the Win_Seq (WLQ)
            OperatorConfig configSeqWLQ(_config.id_inner, _config.n_inner, _config.slide_inner, 0, 1, (_slide_len/_pane_len));
            auto *wlq_seq = new Win_Seq<result_t, result_t>(_wlqupdate_func, (_win_len/_pane_len), (_slide_len/_pane_len), 0, CB, _name + "_wlq", closing_func, RuntimeContext(1, 0), configSeqWLQ, WLQ);
            wlq_stage = wlq_seq;
        }
        // add to this the pipeline optimized according to the provided optimization level
        ff::ff_pipeline::add_stage(optimize_PaneFarmGPU(plq_stage, wlq_stage, _opt_level));
        // when the Pane_Farm_GPU will be destroyed we need aslo to destroy the two stages
        ff::ff_pipeline::cleanup_nodes();
        // flatten the pipeline
        ff::ff_pipeline::flatten();
    }

    // Private Constructor III
    Pane_Farm_GPU(plq_func_t _plq_func,
                  F_t _gpuFunction,
                  uint64_t _win_len,
                  uint64_t _slide_len,
                  uint64_t _triggering_delay,
                  win_type_t _winType,
                  size_t _plq_degree,
                  size_t _wlq_degree,
                  size_t _batch_len,
                  size_t _n_thread_block,
                  std::string _name,
                  size_t _scratchpad_size,
                  bool _ordered,
                  opt_level_t _opt_level,
                  OperatorConfig _config):
                  plq_func(_plq_func),
                  gpuFunction(_gpuFunction),
                  isGPUPLQ(false),
                  isGPUWLQ(true),
                  isNICPLQ(true),
                  isNICWLQ(true),
                  win_len(_win_len),
                  slide_len(_slide_len),
                  triggering_delay(_triggering_delay),
                  winType(_winType),
                  plq_degree(_plq_degree),
                  wlq_degree(_wlq_degree),
                  batch_len(_batch_len),
                  n_thread_block(_n_thread_block),
                  name(_name),
                  scratchpad_size(_scratchpad_size),
                  ordered(_ordered),
                  opt_level(_opt_level),
                  config(_config)
    {
        // check the validity of the windowing parameters
        if (_win_len == 0 || _slide_len == 0) {
            std::cerr << RED << "WindFlow Error: window length or slide in Pane_Farm_GPU cannot be zero" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // check the validity of the parallelism degrees
        if (_plq_degree == 0 || _wlq_degree == 0) {
            std::cerr << RED << "WindFlow Error: Pane_Farm_GPU has parallelism zero" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // check the validity of the batch length
        if (_batch_len == 0) {
            std::cerr << RED << "WindFlow Error: batch length in Pane_Farm_GPU cannot be zero" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // the Pane_Farm_GPU can be utilized with sliding windows only
        if (_win_len <= _slide_len) {
            std::cerr << RED << "WindFlow Error: Pane_Farm_GPU can be used with sliding windows only (s<w)" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // compute the pane length (no. of tuples or in time units)
        uint64_t _pane_len = gcd(_win_len, _slide_len);
        // general fastflow pointers to the PLQ and WLQ stages
        ff_node *plq_stage, *wlq_stage;
        auto closing_func = [] (RuntimeContext &) { return; };
        // create the first stage PLQ
        if (_plq_degree > 1) {
            // configuration structure of the Win_Farm (PLQ)
            OperatorConfig configWFPLQ(_config.id_outer, _config.n_outer, _config.slide_outer, _config.id_inner, _config.n_inner, _config.slide_inner);
            auto *plq_wf = new Win_Farm<tuple_t, result_t, input_t>(_plq_func, _pane_len, _pane_len, _triggering_delay, _winType, _plq_degree, _name + "_plq", closing_func, true, LEVEL0, configWFPLQ, PLQ);
            plq_stage = plq_wf;
            for (auto *w: plq_wf->getWorkers()) {
                plq_workers.push_back(w);
            }
        }
        else {
            // configuration structure of the Win_Seq (PLQ)
            OperatorConfig configSeqPLQ(_config.id_inner, _config.n_inner, _config.slide_inner, 0, 1, _pane_len);
            auto *plq_seq = new Win_Seq<tuple_t, result_t, input_t>(_plq_func, _pane_len, _pane_len, _triggering_delay, _winType, _name + "_plq", closing_func, RuntimeContext(1, 0), configSeqPLQ, PLQ);
            plq_stage = plq_seq;
            plq_workers.push_back(plq_seq);
        }
        // create the second stage WLQ
        if (_wlq_degree > 1) {
            // configuration structure of the Win_Farm_GPU (WLQ)
            OperatorConfig configWFWLQ(_config.id_outer, _config.n_outer, _config.slide_outer, _config.id_inner, _config.n_inner, _config.slide_inner);
            auto *wlq_wf = new Win_Farm_GPU<result_t, result_t, F_t>(_gpuFunction, (_win_len/_pane_len), (_slide_len/_pane_len), 0, CB, _wlq_degree, _batch_len, _n_thread_block, _name + "_wlq", scratchpad_size, _ordered, LEVEL0, configWFWLQ, WLQ);
            wlq_stage = wlq_wf;
        }
        else {
            // configuration structure of the Win_Seq_GPU (WLQ)
            OperatorConfig configSeqWLQ(_config.id_inner, _config.n_inner, _config.slide_inner, 0, 1, (_slide_len/_pane_len));
            auto *wlq_seq = new Win_Seq_GPU<result_t, result_t, F_t>(_gpuFunction, (_win_len/_pane_len), (_slide_len/_pane_len), 0, CB, _batch_len, _n_thread_block, _name + "_wlq", scratchpad_size, configSeqWLQ, WLQ);
            wlq_stage = wlq_seq;
        }
        // add to this the pipeline optimized according to the provided optimization level
        ff::ff_pipeline::add_stage(optimize_PaneFarmGPU(plq_stage, wlq_stage, _opt_level));
        // when the Pane_Farm_GPU will be destroyed we need aslo to destroy the two stages
        ff::ff_pipeline::cleanup_nodes();
        // flatten the pipeline
        ff::ff_pipeline::flatten();
    }

    // Private Constructor IV
    Pane_Farm_GPU(plqupdate_func_t _plqupdate_func,
                  F_t _gpuFunction,
                  uint64_t _win_len,
                  uint64_t _slide_len,
                  uint64_t _triggering_delay,
                  win_type_t _winType,
                  size_t _plq_degree,
                  size_t _wlq_degree,
                  size_t _batch_len,
                  size_t _n_thread_block,
                  std::string _name,
                  size_t _scratchpad_size,
                  bool _ordered,
                  opt_level_t _opt_level,
                  OperatorConfig _config):
                  plqupdate_func(_plqupdate_func),
                  gpuFunction(_gpuFunction),
                  isGPUPLQ(false),
                  isGPUWLQ(true),
                  isNICPLQ(false),
                  isNICWLQ(true),
                  win_len(_win_len),
                  slide_len(_slide_len),
                  triggering_delay(_triggering_delay),
                  winType(_winType),
                  plq_degree(_plq_degree),
                  wlq_degree(_wlq_degree),
                  batch_len(_batch_len),
                  n_thread_block(_n_thread_block),
                  name(_name),
                  scratchpad_size(_scratchpad_size),
                  ordered(_ordered),
                  opt_level(_opt_level),
                  config(_config)
    {
        // check the validity of the windowing parameters
        if (_win_len == 0 || _slide_len == 0) {
            std::cerr << RED << "WindFlow Error: window length or slide in Pane_Farm_GPU cannot be zero" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // check the validity of the parallelism degrees
        if (_plq_degree == 0 || _wlq_degree == 0) {
            std::cerr << RED << "WindFlow Error: Pane_Farm_GPU has parallelism zero" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // check the validity of the batch length
        if (_batch_len == 0) {
            std::cerr << RED << "WindFlow Error: batch length in Pane_Farm_GPU cannot be zero" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // the Pane_Farm_GPU can be utilized with sliding windows only
        if (_win_len <= _slide_len) {
            std::cerr << RED << "WindFlow Error: Pane_Farm_GPU can be used with sliding windows only (s<w)" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // compute the pane length (no. of tuples or in time units)
        uint64_t _pane_len = gcd(_win_len, _slide_len);
        // general fastflow pointers to the PLQ and WLQ stages
        ff_node *plq_stage, *wlq_stage;
        auto closing_func = [] (RuntimeContext &) { return; };
        // create the first stage PLQ
        if (_plq_degree > 1) {
            // configuration structure of the Win_Farm (PLQ)
            OperatorConfig configWFPLQ(_config.id_outer, _config.n_outer, _config.slide_outer, _config.id_inner, _config.n_inner, _config.slide_inner);
            auto *plq_wf = new Win_Farm<tuple_t, result_t, input_t>(_plqupdate_func, _pane_len, _pane_len, _triggering_delay, _winType, _plq_degree, _name + "_plq", closing_func, true, LEVEL0, configWFPLQ, PLQ);
            plq_stage = plq_wf;
            for (auto *w: plq_wf->getWorkers()) {
                plq_workers.push_back(w);
            }
        }
        else {
            // configuration structure of the Win_Seq (PLQ)
            OperatorConfig configSeqPLQ(_config.id_inner, _config.n_inner, _config.slide_inner, 0, 1, _pane_len);
            auto *plq_seq = new Win_Seq<tuple_t, result_t, input_t>(_plqupdate_func, _pane_len, _pane_len, _triggering_delay, _winType, _name + "_plq", closing_func, RuntimeContext(1, 0), configSeqPLQ, PLQ);
            plq_stage = plq_seq;
            plq_workers.push_back(plq_seq);
        }
        // create the second stage WLQ
        if (_wlq_degree > 1) {
            // configuration structure of the Win_Farm_GPU (WLQ)
            OperatorConfig configWFWLQ(_config.id_outer, _config.n_outer, _config.slide_outer, _config.id_inner, _config.n_inner, _config.slide_inner);
            auto *wlq_wf = new Win_Farm_GPU<result_t, result_t, F_t>(_gpuFunction, (_win_len/_pane_len), (_slide_len/_pane_len), 0, CB, _wlq_degree, _batch_len, _n_thread_block, _name + "_wlq", scratchpad_size, _ordered, LEVEL0, configWFWLQ, WLQ);
            wlq_stage = wlq_wf;
        }
        else {
            // configuration structure of the Win_Seq_GPU (WLQ)
            OperatorConfig configSeqWLQ(_config.id_inner, _config.n_inner, _config.slide_inner, 0, 1, (_slide_len/_pane_len));
            auto *wlq_seq = new Win_Seq_GPU<result_t, result_t, F_t>(_gpuFunction, (_win_len/_pane_len), (_slide_len/_pane_len), 0, CB, _batch_len, _n_thread_block, _name + "_wlq", scratchpad_size, configSeqWLQ, WLQ);
            wlq_stage = wlq_seq;
        }
        // add to this the pipeline optimized according to the provided optimization level
        ff::ff_pipeline::add_stage(optimize_PaneFarmGPU(plq_stage, wlq_stage, _opt_level));
        // when the Pane_Farm_GPU will be destroyed we need aslo to destroy the two stages
        ff::ff_pipeline::cleanup_nodes();
        // flatten the pipeline
        ff::ff_pipeline::flatten();
    }

    // method to optimize the structure of the Pane_Farm_GPU pattern
    const ff::ff_pipeline optimize_PaneFarmGPU(ff_node *plq, ff_node *wlq, opt_level_t opt)
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
            if (!plq->isFarm() || !wlq->isFarm()) // like level 1
                if (plq_degree == 1 && wlq_degree == 1) {
                    ff::ff_pipeline pipe;
                    pipe.add_stage(new ff::ff_comb(plq, wlq, true, true));
                    pipe.cleanup_nodes();
                    return pipe;
                }
                else return combine_nodes_in_pipeline(*plq, *wlq, true, true);
            else {
                using emitter_wlq_t = WF_Emitter<result_t, result_t>;
                ff::ff_farm *farm_plq = static_cast<ff::ff_farm *>(plq);
                ff::ff_farm *farm_wlq = static_cast<ff::ff_farm *>(wlq);
                emitter_wlq_t *emitter_wlq = static_cast<emitter_wlq_t *>(farm_wlq->getEmitter());
                farm_wlq->cleanup_emitter(false);
                Ordering_Node<result_t, wrapper_tuple_t<result_t>> *buf_node = new Ordering_Node<result_t, wrapper_tuple_t<result_t>>();
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
     *  \param _plq_func the non-incremental pane processing function (__host__ __device__ function)
     *  \param _wlq_func the non-incremental window processing function (__host__ function)
     *  \param _win_len window length (in no. of tuples or in time units)
     *  \param _slide_len slide length (in no. of tuples or in time units)
     *  \param _triggering_delay (triggering delay in time units, meaningful for TB windows only otherwise it must be 0)
     *  \param _winType window type (count-based CB or time-based TB)
     *  \param _plq_degree parallelism degree of the PLQ stage
     *  \param _wlq_degree parallelism degree of the WLQ stage
     *  \param _batch_len no. of panes in a batch
     *  \param _n_thread_block number of threads per block
     *  \param _name string with the unique name of the operator
     *  \param _scratchpad_size size in bytes of the scratchpad area local of a CUDA thread (pre-allocated on the global memory of the GPU)
     *  \param _ordered true if the results of the same key must be emitted in order (default)
     *  \param _opt_level optimization level used to build the operator
     */ 
    Pane_Farm_GPU(F_t _plq_func,
                  wlq_func_t _wlq_func,
                  uint64_t _win_len,
                  uint64_t _slide_len,
                  uint64_t _triggering_delay,
                  win_type_t _winType,
                  size_t _plq_degree,
                  size_t _wlq_degree,
                  size_t _batch_len,
                  size_t _n_thread_block,
                  std::string _name,
                  size_t _scratchpad_size,
                  bool _ordered,
                  opt_level_t _opt_level):
                  Pane_Farm_GPU(_plq_func, _wlq_func, _win_len, _slide_len, _triggering_delay, _winType, _plq_degree, _wlq_degree, _batch_len, _n_thread_block, _name, _scratchpad_size, _ordered, _opt_level, OperatorConfig(0, 1, _slide_len, 0, 1, _slide_len))
    {
        used = false;
        used4Nesting = false;
    }

    /** 
     *  \brief Constructor II
     *  
     *  \param _plq_func the non-incremental pane processing function (__host__ __device__ function)
     *  \param _wlqupdate_func the incremental window processing function (__host__ function)
     *  \param _win_len window length (in no. of tuples or in time units)
     *  \param _slide_len slide length (in no. of tuples or in time units)
     *  \param _triggering_delay (triggering delay in time units, meaningful for TB windows only otherwise it must be 0)
     *  \param _winType window type (count-based CB or time-based TB)
     *  \param _plq_degree parallelism degree of the PLQ stage
     *  \param _wlq_degree parallelism degree of the WLQ stage
     *  \param _batch_len no. of panes in a batch
     *  \param _n_thread_block number of threads per block
     *  \param _name string with the unique name of the operator
     *  \param _scratchpad_size size in bytes of the scratchpad area local of a CUDA thread (pre-allocated on the global memory of the GPU)
     *  \param _ordered true if the results of the same key must be emitted in order (default)
     *  \param _opt_level optimization level used to build the operator
     */ 
    Pane_Farm_GPU(F_t _plq_func,
                  wlqupdate_func_t _wlqupdate_func,
                  uint64_t _win_len,
                  uint64_t _slide_len,
                  uint64_t _triggering_delay,
                  win_type_t _winType, 
                  size_t _plq_degree,
                  size_t _wlq_degree,
                  size_t _batch_len,
                  size_t _n_thread_block,
                  std::string _name,
                  size_t _scratchpad_size,
                  bool _ordered,
                  opt_level_t _opt_level):
                  Pane_Farm_GPU(_plq_func, _wlqupdate_func, _win_len, _slide_len, _triggering_delay, _winType, _plq_degree, _wlq_degree, _batch_len, _n_thread_block, _name, _scratchpad_size, _ordered, _opt_level, OperatorConfig(0, 1, _slide_len, 0, 1, _slide_len))
    {
        used = false;
        used4Nesting = false;
    }

    /** 
     *  \brief Constructor III
     *  
     *  \param _plq_func the non-incremental pane processing function (__host__ function)
     *  \param _wlq_func the non-incremental window processing function (__host__ __device__ function)
     *  \param _win_len window length (in no. of tuples or in time units)
     *  \param _slide_len slide length (in no. of tuples or in time units)
     *  \param _triggering_delay (triggering delay in time units, meaningful for TB windows only otherwise it must be 0)
     *  \param _winType window type (count-based CB or time-based TB)
     *  \param _plq_degree parallelism degree of the PLQ stage
     *  \param _wlq_degree parallelism degree of the WLQ stage
     *  \param _batch_len no. of panes in a batch
     *  \param _n_thread_block number of threads per block
     *  \param _name string with the unique name of the operator
     *  \param _scratchpad_size size in bytes of the scratchpad area local of a CUDA thread (pre-allocated on the global memory of the GPU)
     *  \param _ordered true if the results of the same key must be emitted in order (default)
     *  \param _opt_level optimization level used to build the operator
     */ 
    Pane_Farm_GPU(plq_func_t _plq_func,
                  F_t _wlq_func,
                  uint64_t _win_len,
                  uint64_t _slide_len,
                  uint64_t _triggering_delay,
                  win_type_t _winType,
                  size_t _plq_degree,
                  size_t _wlq_degree,
                  size_t _batch_len,
                  size_t _n_thread_block,
                  std::string _name,
                  size_t _scratchpad_size,
                  bool _ordered,
                  opt_level_t _opt_level):
                  Pane_Farm_GPU(_plq_func, _wlq_func, _win_len, _slide_len, _triggering_delay, _winType, _plq_degree, _wlq_degree, _batch_len, _n_thread_block, _name, _scratchpad_size, _ordered, _opt_level, OperatorConfig(0, 1, _slide_len, 0, 1, _slide_len))
    {
        used = false;
        used4Nesting = false;
    }

    /** 
     *  \brief Constructor IV
     *  
     *  \param _plqupdate_func the incremental pane processing function (__host__ function)
     *  \param _wlq_func the non-incremental window processing function (__host__ __device__ function)
     *  \param _win_len window length (in no. of tuples or in time units)
     *  \param _slide_len slide length (in no. of tuples or in time units)
     *  \param _triggering_delay (triggering delay in time units, meaningful for TB windows only otherwise it must be 0)
     *  \param _winType window type (count-based CB or time-based TB)
     *  \param _plq_degree parallelism degree of the PLQ stage
     *  \param _wlq_degree parallelism degree of the WLQ stage
     *  \param _batch_len no. of panes in a batch
     *  \param _n_thread_block number of threads per block
     *  \param _name string with the unique name of the operator
     *  \param _scratchpad_size size in bytes of the scratchpad area local of a CUDA thread (pre-allocated on the global memory of the GPU)
     *  \param _ordered true if the results of the same key must be emitted in order (default)
     *  \param _opt_level optimization level used to build the operator
     */ 
    Pane_Farm_GPU(plqupdate_func_t _plqupdate_func,
                  F_t _wlq_func,
                  uint64_t _win_len,
                  uint64_t _slide_len,
                  uint64_t _triggering_delay,
                  win_type_t _winType,
                  size_t _plq_degree,
                  size_t _wlq_degree,
                  size_t _batch_len,
                  size_t _n_thread_block,
                  std::string _name,
                  size_t _scratchpad_size,
                  bool _ordered,
                  opt_level_t _opt_level):
                  Pane_Farm_GPU(_plqupdate_func, _wlq_func, _win_len, _slide_len, _triggering_delay, _winType, _plq_degree, _wlq_degree, _batch_len, _n_thread_block, _name, _scratchpad_size, _ordered, _opt_level, OperatorConfig(0, 1, _slide_len, 0, 1, _slide_len))
    {
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
     *  \brief Check whether the Pane_Farm_GPU has been used in a MultiPipe
     *  \return true if the Pane_Farm_GPU has been added/chained to an existing MultiPipe
     */
    bool isUsed() const
    {
        return used;
    }

    /** 
     *  \brief Check whether the Pane_Farm_GPU has been used in a nested structure
     *  \return true if the Pane_Farm_GPU has been used in a nested structure
     */
    bool isUsed4Nesting() const
    {
        return used4Nesting;
    }

    /** 
     *  \brief Get the number of dropped tuples by the Pane_Farm_GPU
     *  \return number of tuples dropped during the processing by the Pane_Farm_GPU
     */ 
    size_t getNumDroppedTuples() const
    {
        size_t count = 0;
        for (auto *w: plq_workers) {
            if (isGPUPLQ) {
                auto *seq_gpu = static_cast<Win_Seq_GPU<tuple_t, result_t, F_t, input_t> *>(w);
                count += seq_gpu->getNumDroppedTuples();
            }
            else {
                auto *seq = static_cast<Win_Seq<tuple_t, result_t, input_t> *>(w);
                count += seq->getNumDroppedTuples();
            }
        }
        return count;
    }

    /// deleted constructors/operators
    Pane_Farm_GPU(const Pane_Farm_GPU &) = delete; // copy constructor
    Pane_Farm_GPU(Pane_Farm_GPU &&) = delete; // move constructor
    Pane_Farm_GPU &operator=(const Pane_Farm_GPU &) = delete; // copy assignment operator
    Pane_Farm_GPU &operator=(Pane_Farm_GPU &&) = delete; // move assignment operator
};

} // namespace wf

#endif
