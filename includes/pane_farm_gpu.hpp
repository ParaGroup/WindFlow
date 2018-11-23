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
 *  @version 1.0
 *  
 *  @brief Pane_Farm_GPU pattern executing windowed queries on a heterogeneous system (CPU+GPU)
 *  
 *  @section DESCRIPTION
 *  
 *  This file implements the Pane_Farm_GPU pattern able to execute windowed queries on a heterogeneous
 *  system (CPU+GPU). The pattern processes (possibly in parallel) panes of the windows in the so-called
 *  PLQ stage (Pane-Level Sub-Query) and computes (possibly in parallel) results of tne windows from the
 *  pane results in the so-called WLQ stage (Window-Level Sub-Query). Panes shared by more than one window
 *  are not recomputed by saving processing time. The pattern allows the user to offload either the PLQ
 *  or the WLQ processing on the GPU while the other stage is executed on the CPU with either a non-incremental
 *  or an incremental query definition.
 */ 

#ifndef PANE_FARM_GPU_H
#define PANE_FARM_GPU_H

/// includes
#include <ff/combine.hpp>
#include <ff/pipeline.hpp>
#include <win_farm.hpp>
#include <win_farm_gpu.hpp>
#include <orderingNode.hpp>

/** 
 *  \class Pane_Farm_GPU
 *  
 *  \brief Pane_Farm_GPU pattern executing windowed queries on a heterogeneous system (CPU+GPU)
 *  
 *  This class implements the Pane_Farm_GPU pattern executing windowed queries in parallel on
 *  a heterogeneous system (CPU+GPU). The pattern processes (possibly in parallel) panes in the
 *  PLQ stage while window results are built out from the pane results (possibly in parallel)
 *  in the WLQ stage. Either the PLQ or the WLQ stage are executed on the GPU device while the
 *  others is executed on the CPU as in the Pane_Farm pattern. The pattern class has four
 *  template arguments. The first is the type of the input tuples. It must be copyable and
 *  providing the getInfo() and setInfo() methods. The second is the type of the window results.
 *  It must have a default constructor and the getInfo() and setInfo() methods. The first and the
 *  second types must be POD C++ types (Plain Old Data). The third template argument is the type of
 *  the function to process per pane/window. It must be declared in order to be executable both
 *  on the device (GPU) and on the CPU. The last template argument is used by the WindFlow run-time
 *  system and should never be utilized by the high-level programmer.
 */ 
template<typename tuple_t, typename result_t, typename F_t, typename input_t>
class Pane_Farm_GPU: public ff_pipeline
{
private:
    // function type of the non-incremental pane processing
    using f_plqfunction_t = function<int(size_t, uint64_t, Iterable<tuple_t> &, result_t &)>;
    // Function type of the incremental pane processing
    using f_plqupdate_t = function<int(size_t, uint64_t, const tuple_t &, result_t &)>;
    // function type of the non-incremental window processing
    using f_wlqfunction_t = function<int(size_t, uint64_t, Iterable<result_t> &, result_t &)>;
    // function type of the incremental window function
    using f_wlqupdate_t = function<int(size_t, uint64_t, const result_t &, result_t &)>;
    /// friendships with other classes in the library
    template<typename T1, typename T2, typename T3, typename T4>
    friend class Win_Farm_GPU;
    template<typename T1, typename T2, typename T3, typename T4>
    friend class Key_Farm_GPU;
    template<typename T>
    friend class WinFarmGPU_Builder;
    template<typename T>
    friend class KeyFarmGPU_Builder;
    // compute the gcd between two numbers
    function<uint64_t(uint64_t, uint64_t)> gcd = [](uint64_t u, uint64_t v) {
        while (v != 0) {
            unsigned long r = u % v;
            u = v;
            v = r;
        }
        return u;
    };
    // configuration variables of the Pane_Farm_GPU
    F_t gpuFunction;
    f_plqfunction_t plqFunction;
    f_plqupdate_t plqUpdate;
    f_wlqfunction_t wlqFunction;
    f_wlqupdate_t wlqUpdate;
    bool isGPUPLQ;
    bool isGPUWLQ;
    bool isNICPLQ;
    bool isNICWLQ;
    uint64_t win_len;
    uint64_t slide_len;
    win_type_t winType;
    size_t plq_degree;
    size_t wlq_degree;
    size_t batch_len;
    size_t n_thread_block;
    string name;
    size_t scratchpad_size;
    bool ordered;
    opt_level_t opt_level;
    PatternConfig config;

    // private constructor I (PLQ stage on the GPU and WLQ stage on the CPU with non-incremental query definition)
    Pane_Farm_GPU(F_t _gpuFunction,
                  f_wlqfunction_t _wlqFunction,
                  uint64_t _win_len,
                  uint64_t _slide_len,
                  win_type_t _winType,
                  size_t _plq_degree,
                  size_t _wlq_degree,
                  size_t _batch_len,
                  size_t _n_thread_block,
                  string _name,
                  size_t _scratchpad_size,
                  bool _ordered,
                  opt_level_t _opt_level,
                  PatternConfig _config)
                  :
                  gpuFunction(_gpuFunction),
                  wlqFunction(_wlqFunction),
                  isGPUPLQ(true),
                  isGPUWLQ(false),
                  isNICPLQ(true),
                  isNICWLQ(true),
                  win_len(_win_len),
                  slide_len(_slide_len),
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
            cerr << RED << "WindFlow Error: window length or slide cannot be zero" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // check the validity of the parallelism degrees
        if (_plq_degree == 0 || _wlq_degree == 0) {
            cerr << RED << "WindFlow Error: parallelism degrees cannot be zero" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // check the validity of the batch length
        if (_batch_len == 0) {
            cerr << RED << "WindFlow Error: batch length cannot be zero" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // the Pane_Farm_GPU can be utilized with sliding windows only
        if(_win_len <= _slide_len) {
            cerr << RED << "WindFlow Error: Pane_Farm_GPU can be used with sliding windows only (s<w)" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // compute the pane length (no. of tuples or in time units)
        uint64_t _pane_len = gcd(_win_len, _slide_len);
        // general fastflow pointers to the PLQ and WLQ stages
        ff_node *plq_stage, *wlq_stage;
        // create the first stage PLQ
        if(_plq_degree > 1) {
            // configuration structure of the Win_Farm_GPU instance (PLQ)
            PatternConfig configWFPLQ(_config.id_outer, _config.n_outer, _config.slide_outer, _config.id_inner, _config.n_inner, _config.slide_inner);
            auto *plq_wf = new Win_Farm_GPU<tuple_t, result_t, F_t, input_t>(_gpuFunction, _pane_len, _pane_len, _winType, 1, _plq_degree, _batch_len, _n_thread_block, _name + "_plq", _scratchpad_size, true, configWFPLQ, PLQ);
            plq_stage = plq_wf;
        }
        else {
            // configuration structure of the Win_Seq_GPU instance (PLQ)
            PatternConfig configSeqPLQ(_config.id_inner, _config.n_inner, _config.slide_inner, 0, 1, _pane_len);
            auto *plq_seq = new Win_Seq_GPU<tuple_t, result_t, F_t, input_t>(_gpuFunction, _pane_len, _pane_len, _winType, _batch_len, _n_thread_block, _name + "_plq", _scratchpad_size, configSeqPLQ, PLQ);
            plq_stage = plq_seq;
        }
        // create the second stage WLQ
        if(_wlq_degree > 1) {
            // configuration structure of the Win_Farm instance (WLQ)
            PatternConfig configWFWLQ(_config.id_outer, _config.n_outer, _config.slide_outer, _config.id_inner, _config.n_inner, _config.slide_inner);
            auto *wlq_wf = new Win_Farm<result_t, result_t>(_wlqFunction, (_win_len/_pane_len), (_slide_len/_pane_len), CB, 1, _wlq_degree, _name + "_wlq", _ordered, configWFWLQ, WLQ);
            wlq_stage = wlq_wf;
        }
        else {
            // configuration structure of the Win_Seq instance (WLQ)
            PatternConfig configSeqWLQ(_config.id_inner, _config.n_inner, _config.slide_inner, 0, 1, (_slide_len/_pane_len));
            auto *wlq_seq = new Win_Seq<result_t, result_t>(_wlqFunction, (_win_len/_pane_len), (_slide_len/_pane_len), CB, _name + "_wlq", configSeqWLQ, WLQ);
            wlq_stage = wlq_seq;
        }
        // add to this the pipeline optimized according to the provided optimization level
        ff_pipeline::add_stage(optimize_PaneFarmGPU(plq_stage, wlq_stage, _opt_level));
        // when the Pane_Farm_GPU will be destroyed we need aslo to destroy the two stages
        ff_pipeline::cleanup_nodes();
    }

    // private constructor II (PLQ stage on the GPU and WLQ stage on the CPU with incremental query definition)
    Pane_Farm_GPU(F_t _gpuFunction,
                  f_wlqupdate_t _wlqUpdate,
                  uint64_t _win_len,
                  uint64_t _slide_len,
                  win_type_t _winType,
                  size_t _plq_degree,
                  size_t _wlq_degree,
                  size_t _batch_len,
                  size_t _n_thread_block,
                  string _name,
                  size_t _scratchpad_size,
                  bool _ordered,
                  opt_level_t _opt_level,
                  PatternConfig _config)
                  :
                  gpuFunction(_gpuFunction),
                  wlqUpdate(_wlqUpdate),
                  isGPUPLQ(true),
                  isGPUWLQ(false),
                  isNICPLQ(true),
                  isNICWLQ(false),
                  win_len(_win_len),
                  slide_len(_slide_len),
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
            cerr << RED << "WindFlow Error: window length or slide cannot be zero" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // check the validity of the parallelism degrees
        if (_plq_degree == 0 || _wlq_degree == 0) {
            cerr << RED << "WindFlow Error: parallelism degrees cannot be zero" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // check the validity of the batch length
        if (_batch_len == 0) {
            cerr << RED << "WindFlow Error: batch length cannot be zero" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // the Pane_Farm_GPU can be utilized with sliding windows only
        if(_win_len <= _slide_len) {
            cerr << RED << "WindFlow Error: Pane_Farm_GPU can be used with sliding windows only (s<w)" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // compute the pane length (no. of tuples or in time units)
        uint64_t _pane_len = gcd(_win_len, _slide_len);
        // general fastflow pointers to the PLQ and WLQ stages
        ff_node *plq_stage, *wlq_stage;
        // create the first stage PLQ
        if(_plq_degree > 1) {
            // configuration structure of the Win_Farm_GPU instance (PLQ)
            PatternConfig configWFPLQ(_config.id_outer, _config.n_outer, _config.slide_outer, _config.id_inner, _config.n_inner, _config.slide_inner);
            auto *plq_wf = new Win_Farm_GPU<tuple_t, result_t, F_t, input_t>(_gpuFunction, _pane_len, _pane_len, _winType, 1, _plq_degree, _batch_len, _n_thread_block, _name + "_plq", _scratchpad_size, true, configWFPLQ, PLQ);
            plq_stage = plq_wf;
        }
        else {
            // configuration structure of the Win_Seq_GPU instance (PLQ)
            PatternConfig configSeqPLQ(_config.id_inner, _config.n_inner, _config.slide_inner, 0, 1, _pane_len);
            auto *plq_seq = new Win_Seq_GPU<tuple_t, result_t, F_t, input_t>(_gpuFunction, _pane_len, _pane_len, _winType, _batch_len, _n_thread_block, _name + "_plq", _scratchpad_size, configSeqPLQ, PLQ);
            plq_stage = plq_seq;
        }
        // create the second stage WLQ
        if(_wlq_degree > 1) {
            // configuration structure of the Win_Farm instance (WLQ)
            PatternConfig configWFWLQ(_config.id_outer, _config.n_outer, _config.slide_outer, _config.id_inner, _config.n_inner, _config.slide_inner);
            auto *wlq_wf = new Win_Farm<result_t, result_t>(_wlqUpdate, (_win_len/_pane_len), (_slide_len/_pane_len), CB, 1, _wlq_degree, _name + "_wlq", _ordered, configWFWLQ, WLQ);
            wlq_stage = wlq_wf;
        }
        else {
            // configuration structure of the Win_Seq instance (WLQ)
            PatternConfig configSeqWLQ(_config.id_inner, _config.n_inner, _config.slide_inner, 0, 1, (_slide_len/_pane_len));
            auto *wlq_seq = new Win_Seq<result_t, result_t>(_wlqUpdate, (_win_len/_pane_len), (_slide_len/_pane_len), CB, _name + "_wlq", configSeqWLQ, WLQ);
            wlq_stage = wlq_seq;
        }
        // add to this the pipeline optimized according to the provided optimization level
        ff_pipeline::add_stage(optimize_PaneFarmGPU(plq_stage, wlq_stage, _opt_level));
        // when the Pane_Farm_GPU will be destroyed we need aslo to destroy the two stages
        ff_pipeline::cleanup_nodes();
    }

    // private constructor III (PLQ stage on the CPU with non-incremental query definition and WLQ stage on the GPU)
    Pane_Farm_GPU(f_plqfunction_t _plqFunction,
                  F_t _gpuFunction,
                  uint64_t _win_len,
                  uint64_t _slide_len,
                  win_type_t _winType,
                  size_t _plq_degree,
                  size_t _wlq_degree,
                  size_t _batch_len,
                  size_t _n_thread_block,
                  string _name,
                  size_t _scratchpad_size,
                  bool _ordered,
                  opt_level_t _opt_level,
                  PatternConfig _config)
                  :
                  plqFunction(_plqFunction),
                  gpuFunction(_gpuFunction),
                  isGPUPLQ(false),
                  isGPUWLQ(true),
                  isNICPLQ(true),
                  isNICWLQ(true),
                  win_len(_win_len),
                  slide_len(_slide_len),
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
            cerr << RED << "WindFlow Error: window length or slide cannot be zero" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // check the validity of the parallelism degrees
        if (_plq_degree == 0 || _wlq_degree == 0) {
            cerr << RED << "WindFlow Error: parallelism degrees cannot be zero" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // check the validity of the batch length
        if (_batch_len == 0) {
            cerr << RED << "WindFlow Error: batch length cannot be zero" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // the Pane_Farm_GPU can be utilized with sliding windows only
        if(_win_len <= _slide_len) {
            cerr << RED << "WindFlow Error: Pane_Farm_GPU can be used with sliding windows only (s<w)" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // compute the pane length (no. of tuples or in time units)
        uint64_t _pane_len = gcd(_win_len, _slide_len);
        // general fastflow pointers to the PLQ and WLQ stages
        ff_node *plq_stage, *wlq_stage;
        // create the first stage PLQ
        if(_plq_degree > 1) {
            // configuration structure of the Win_Farm instance (PLQ)
            PatternConfig configWFPLQ(_config.id_outer, _config.n_outer, _config.slide_outer, _config.id_inner, _config.n_inner, _config.slide_inner);
            auto *plq_wf = new Win_Farm<tuple_t, result_t, input_t>(_plqFunction, _pane_len, _pane_len, _winType, 1, _plq_degree, _name + "_plq", true, configWFPLQ, PLQ);
            plq_stage = plq_wf;
        }
        else {
            // configuration structure of the Win_Seq instance (PLQ)
            PatternConfig configSeqPLQ(_config.id_inner, _config.n_inner, _config.slide_inner, 0, 1, _pane_len);
            auto *plq_seq = new Win_Seq<tuple_t, result_t, input_t>(_plqFunction, _pane_len, _pane_len, _winType, _name + "_plq", configSeqPLQ, PLQ);
            plq_stage = plq_seq;
        }
        // create the second stage WLQ
        if(_wlq_degree > 1) {
            // configuration structure of the Win_Farm_GPU instance (WLQ)
            PatternConfig configWFWLQ(_config.id_outer, _config.n_outer, _config.slide_outer, _config.id_inner, _config.n_inner, _config.slide_inner);
            auto *wlq_wf = new Win_Farm_GPU<result_t, result_t, F_t>(_gpuFunction, (_win_len/_pane_len), (_slide_len/_pane_len), CB, 1, _wlq_degree, _batch_len, _n_thread_block, _name + "_wlq", scratchpad_size, _ordered, configWFWLQ, WLQ);
            wlq_stage = wlq_wf;
        }
        else {
            // configuration structure of the Win_Seq_GPU instance (WLQ)
            PatternConfig configSeqWLQ(_config.id_inner, _config.n_inner, _config.slide_inner, 0, 1, (_slide_len/_pane_len));
            auto *wlq_seq = new Win_Seq_GPU<result_t, result_t, F_t>(_gpuFunction, (_win_len/_pane_len), (_slide_len/_pane_len), CB, _batch_len, _n_thread_block, _name + "_wlq", scratchpad_size, configSeqWLQ, WLQ);
            wlq_stage = wlq_seq;
        }
        // add to this the pipeline optimized according to the provided optimization level
        ff_pipeline::add_stage(optimize_PaneFarmGPU(plq_stage, wlq_stage, _opt_level));
        // when the Pane_Farm_GPU will be destroyed we need aslo to destroy the two stages
        ff_pipeline::cleanup_nodes();
    }

    // private constructor IV (PLQ stage on the CPU with incremental query definition and WLQ stage on the GPU)
    Pane_Farm_GPU(f_plqupdate_t _plqUpdate,
                  F_t _gpuFunction,
                  uint64_t _win_len,
                  uint64_t _slide_len,
                  win_type_t _winType,
                  size_t _plq_degree,
                  size_t _wlq_degree,
                  size_t _batch_len,
                  size_t _n_thread_block,
                  string _name,
                  size_t _scratchpad_size,
                  bool _ordered,
                  opt_level_t _opt_level,
                  PatternConfig _config)
                  :
                  plqUpdate(_plqUpdate),
                  gpuFunction(_gpuFunction),
                  isGPUPLQ(false),
                  isGPUWLQ(true),
                  isNICPLQ(false),
                  isNICWLQ(true),
                  win_len(_win_len),
                  slide_len(_slide_len),
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
            cerr << RED << "WindFlow Error: window length or slide cannot be zero" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // check the validity of the parallelism degrees
        if (_plq_degree == 0 || _wlq_degree == 0) {
            cerr << RED << "WindFlow Error: parallelism degrees cannot be zero" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // check the validity of the batch length
        if (_batch_len == 0) {
            cerr << RED << "WindFlow Error: batch length cannot be zero" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // the Pane_Farm_GPU can be utilized with sliding windows only
        if(_win_len <= _slide_len) {
            cerr << RED << "WindFlow Error: Pane_Farm_GPU can be used with sliding windows only (s<w)" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // compute the pane length (no. of tuples or in time units)
        uint64_t _pane_len = gcd(_win_len, _slide_len);
        // general fastflow pointers to the PLQ and WLQ stages
        ff_node *plq_stage, *wlq_stage;
        // create the first stage PLQ
        if(_plq_degree > 1) {
            // configuration structure of the Win_Farm instance (PLQ)
            PatternConfig configWFPLQ(_config.id_outer, _config.n_outer, _config.slide_outer, _config.id_inner, _config.n_inner, _config.slide_inner);
            auto *plq_wf = new Win_Farm<tuple_t, result_t, input_t>(_plqUpdate, _pane_len, _pane_len, _winType, 1, _plq_degree, _name + "_plq", true, configWFPLQ, PLQ);
            plq_stage = plq_wf;
        }
        else {
            // configuration structure of the Win_Seq instance (PLQ)
            PatternConfig configSeqPLQ(_config.id_inner, _config.n_inner, _config.slide_inner, 0, 1, _pane_len);
            auto *plq_seq = new Win_Seq<tuple_t, result_t, input_t>(_plqUpdate, _pane_len, _pane_len, _winType, _name + "_plq", configSeqPLQ, PLQ);
            plq_stage = plq_seq;
        }
        // create the second stage WLQ
        if(_wlq_degree > 1) {
            // configuration structure of the Win_Farm_GPU instance (WLQ)
            PatternConfig configWFWLQ(_config.id_outer, _config.n_outer, _config.slide_outer, _config.id_inner, _config.n_inner, _config.slide_inner);
            auto *wlq_wf = new Win_Farm_GPU<result_t, result_t, F_t>(_gpuFunction, (_win_len/_pane_len), (_slide_len/_pane_len), CB, 1, _wlq_degree, _batch_len, _n_thread_block, _name + "_wlq", scratchpad_size, _ordered, configWFWLQ, WLQ);
            wlq_stage = wlq_wf;
        }
        else {
            // configuration structure of the Win_Seq_GPU instance (WLQ)
            PatternConfig configSeqWLQ(_config.id_inner, _config.n_inner, _config.slide_inner, 0, 1, (_slide_len/_pane_len));
            auto *wlq_seq = new Win_Seq_GPU<result_t, result_t, F_t>(_gpuFunction, (_win_len/_pane_len), (_slide_len/_pane_len), CB, _batch_len, _n_thread_block, _name + "_wlq", scratchpad_size, configSeqWLQ, WLQ);
            wlq_stage = wlq_seq;
        }
        // add to this the pipeline optimized according to the provided optimization level
        ff_pipeline::add_stage(optimize_PaneFarmGPU(plq_stage, wlq_stage, _opt_level));
        // when the Pane_Farm_GPU will be destroyed we need aslo to destroy the two stages
        ff_pipeline::cleanup_nodes();
    }

    // method to optimize the structure of the Pane_Farm_GPU pattern
    const ff_pipeline optimize_PaneFarmGPU(ff_node *plq, ff_node *wlq, opt_level_t opt)
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
                OrderingNode<result_t> *buf_node = new OrderingNode<result_t>(farm_plq->getNWorkers());
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
     *  \brief Constructor I (PLQ stage on GPU and Non-Incremental WLQ stage on CPU)
     *  
     *  \param _plqFunction the non-incremental pane processing function (CPU/GPU function)
     *  \param _wlqFunction the non-incremental window processing function (CPU function)
     *  \param _win_len window length (in no. of tuples or in time units)
     *  \param _slide_len slide length (in no. of tuples or in time units)
     *  \param _winType window type (count-based CB or time-based TB)
     *  \param _plq_degree number of Win_Seq_GPU instances within the PLQ stage
     *  \param _wlq_degree number of Win_Seq instances within the WLQ stage
     *  \param _batch_len no. of panes in a batch (i.e. 1 pane mapped onto 1 CUDA thread)
     *  \param _n_thread_block number of threads (i.e. panes) per block
     *  \param _name string with the unique name of the pattern
     *  \param _scratchpad_size size in bytes of the scratchpad area per CUDA thread (on the GPU)
     *  \param _ordered true if the results of the same key must be emitted in order (default)
     *  \param _opt_level optimization level used to build the pattern
     */ 
    Pane_Farm_GPU(F_t _plqFunction,
                  f_wlqfunction_t _wlqFunction,
                  uint64_t _win_len,
                  uint64_t _slide_len,
                  win_type_t _winType,
                  size_t _plq_degree,
                  size_t _wlq_degree,
                  size_t _batch_len,
                  size_t _n_thread_block,
                  string _name,
                  size_t _scratchpad_size=0,
                  bool _ordered=true,
                  opt_level_t _opt_level=LEVEL0)
                  :
                  Pane_Farm_GPU(_plqFunction, _wlqFunction, _win_len, _slide_len, _winType, _plq_degree, _wlq_degree, _batch_len, _n_thread_block, _name, _scratchpad_size, _ordered, _opt_level, PatternConfig(0, 1, _slide_len, 0, 1, _slide_len)) {}

    /** 
     *  \brief Constructor II (PLQ stage on GPU and Incremental WLQ stage on CPU)
     *  
     *  \param _plqFunction the non-incremental pane processing function (CPU/GPU function)
     *  \param _wlqUpdate the incremental window processing function (CPU function)
     *  \param _win_len window length (in no. of tuples or in time units)
     *  \param _slide_len slide length (in no. of tuples or in time units)
     *  \param _winType window type (count-based CB or time-based TB)
     *  \param _plq_degree number of Win_Seq instances in the PLQ stage
     *  \param _wlq_degree number of Win_Seq instances in the WLQ stage
     *  \param _batch_len no. of panes in a batch (i.e. 1 pane mapped onto 1 CUDA thread)
     *  \param _n_thread_block number of threads (i.e. panes) per block
     *  \param _name string with the unique name of the pattern
     *  \param _scratchpad_size size in bytes of the scratchpad area per CUDA thread (on the GPU)
     *  \param _ordered true if the results of the same key must be emitted in order (default)
     *  \param _opt_level optimization level used to build the pattern
     */ 
    Pane_Farm_GPU(F_t _plqFunction,
                  f_wlqupdate_t _wlqUpdate,
                  uint64_t _win_len,
                  uint64_t _slide_len,
                  win_type_t _winType, 
                  size_t _plq_degree,
                  size_t _wlq_degree,
                  size_t _batch_len,
                  size_t _n_thread_block,
                  string _name,
                  size_t _scratchpad_size=0,
                  bool _ordered=true,
                  opt_level_t _opt_level=LEVEL0)
                  :
                  Pane_Farm_GPU(_plqFunction, _wlqUpdate, _win_len, _slide_len, _winType, _plq_degree, _wlq_degree, _batch_len, _n_thread_block, _name, _scratchpad_size, _ordered, _opt_level, PatternConfig(0, 1, _slide_len, 0, 1, _slide_len)) {}

    /** 
     *  \brief Constructor III (Non-Incremental PLQ stage on CPU and WLQ stage on GPU)
     *  
     *  \param _plqFunction the non-incremental pane processing function (CPU function)
     *  \param _wlqFunction the non-incremental window processing function (CPU/GPU function)
     *  \param _win_len window length (in no. of tuples or in time units)
     *  \param _slide_len slide length (in no. of tuples or in time units)
     *  \param _winType window type (count-based CB or time-based TB)
     *  \param _plq_degree number of Win_Seq instances in the PLQ stage
     *  \param _wlq_degree number of Win_Seq instances in the WLQ stage
     *  \param _batch_len no. of panes in a batch (i.e. 1 pane mapped onto 1 CUDA thread)
     *  \param _n_thread_block number of threads (i.e. panes) per block
     *  \param _name string with the unique name of the pattern
     *  \param _scratchpad_size size in bytes of the scratchpad area per CUDA thread (on the GPU)
     *  \param _ordered true if the results of the same key must be emitted in order (default)
     *  \param _opt_level optimization level used to build the pattern
     */ 
    Pane_Farm_GPU(f_plqfunction_t _plqFunction,
                  F_t _wlqFunction,
                  uint64_t _win_len,
                  uint64_t _slide_len,
                  win_type_t _winType,
                  size_t _plq_degree,
                  size_t _wlq_degree,
                  size_t _batch_len,
                  size_t _n_thread_block,
                  string _name,
                  size_t _scratchpad_size=0,
                  bool _ordered=true,
                  opt_level_t _opt_level=LEVEL0)
                  :
                  Pane_Farm_GPU(_plqFunction, _wlqFunction, _win_len, _slide_len, _winType, _plq_degree, _wlq_degree, _batch_len, _n_thread_block, _name, _scratchpad_size, _ordered, _opt_level, PatternConfig(0, 1, _slide_len, 0, 1, _slide_len)) {}

    /** 
     *  \brief Constructor IV (Incremental PLQ stage on CPU and WLQ stage on GPU)
     *  
     *  \param _plqUpdate the incremental pane processing function (CPU function)
     *  \param _wlqFunction the non-incremental window processing function (CPU/GPU function)
     *  \param _win_len window length (in no. of tuples or in time units)
     *  \param _slide_len slide length (in no. of tuples or in time units)
     *  \param _winType window type (count-based CB or time-based TB)
     *  \param _plq_degree number of Win_Seq instances in the PLQ stage
     *  \param _wlq_degree number of Win_Seq instances in the WLQ stage
     *  \param _batch_len no. of panes in a batch (i.e. 1 pane mapped onto 1 CUDA thread)
     *  \param _n_thread_block number of threads (i.e. panes) per block
     *  \param _name string with the unique name of the pattern
     *  \param _scratchpad_size size in bytes of the scratchpad area per CUDA thread (on the GPU)
     *  \param _ordered true if the results of the same key must be emitted in order (default)
     *  \param _opt_level optimization level used to build the pattern
     */ 
    Pane_Farm_GPU(f_plqupdate_t _plqUpdate,
                  F_t _wlqFunction,
                  uint64_t _win_len,
                  uint64_t _slide_len,
                  win_type_t _winType,
                  size_t _plq_degree,
                  size_t _wlq_degree,
                  size_t _batch_len,
                  size_t _n_thread_block,
                  string _name,
                  size_t _scratchpad_size=0,
                  bool _ordered=true,
                  opt_level_t _opt_level=LEVEL0)
                  :
                  Pane_Farm_GPU(_plqUpdate, _wlqFunction, _win_len, _slide_len, _winType, _plq_degree, _wlq_degree, _batch_len, _n_thread_block, _name, _scratchpad_size, _ordered, _opt_level, PatternConfig(0, 1, _slide_len, 0, 1, _slide_len)) {}

    /// destructor
    ~Pane_Farm_GPU() {}

    // ------------------------- deleted method ---------------------------
    int  add_stage(ff_node *s)                                    = delete;
    int  wrap_around(bool multi_input=false)                      = delete;
    void cleanup_nodes()                                          = delete;
    bool offload(void * task,
                 unsigned long retry=((unsigned long)-1),
                 unsigned long ticks=ff_node::TICKS2WAIT)         = delete;
    bool load_result(void ** task,
                     unsigned long retry=((unsigned long)-1),
                     unsigned long ticks=ff_node::TICKS2WAIT)     = delete;
    bool load_result_nb(void ** task)                             = delete;
};

#endif
