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
 *  @file    win_farm_gpu.hpp
 *  @author  Gabriele Mencagli
 *  @date    17/04/2018
 *  
 *  @brief Win_Farm_GPU pattern executing a windowed transformation in parallel on a CPU+GPU system
 *  
 *  @section Win_Farm_GPU (Description)
 *  
 *  This file implements the Win_Farm_GPU pattern able to executes windowed queries on a heterogeneous
 *  system (CPU+GPU). The pattern prepares batches of input tuples in parallel on the CPU cores and
 *  offloads on the GPU the parallel processing of the windows within each batch.
 *  
 *  The template arguments tuple_t and result_t must be default constructible, with a copy constructor
 *  and copy assignment operator, and they must provide and implement the setInfo() and
 *  getInfo() methods. The third template argument win_F_t is the type of the callable object to be used for GPU
 *  processing.
 */ 

#ifndef WIN_FARM_GPU_H
#define WIN_FARM_GPU_H

// includes
#include <ff/farm.hpp>
#include <ff/optimize.hpp>
#include <win_seq_gpu.hpp>
#include <wf_nodes.hpp>
#include <pane_farm_gpu.hpp>
#include <win_mapreduce_gpu.hpp>

/** 
 *  \class Win_Farm_GPU
 *  
 *  \brief Win_Farm_GPU pattern executing a windowed transformation in parallel on a CPU+GPU system
 *  
 *  This class implements the Win_Farm_GPU pattern. The pattern prepares in parallel distinct
 *  batches of tuples (on the CPU cores) and offloads the processing of the batches on the GPU
 *  by computing in parallel all the windows within a batch on the CUDA cores of the GPU.
 */ 
template<typename tuple_t, typename result_t, typename win_F_t, typename input_t>
class Win_Farm_GPU: public ff_farm
{
public:
    /// type of the Pane_Farm_GPU passed to the proper nesting constructor
    using pane_farm_gpu_t = Pane_Farm_GPU<tuple_t, result_t, win_F_t>;
    /// type of the Win_MapReduce_GPU passed to the proper nesting constructor
    using win_mapreduce_gpu_t = Win_MapReduce_GPU<tuple_t, result_t, win_F_t>;
private:
    // type of the wrapper of input tuples
    using wrapper_in_t = wrapper_tuple_t<tuple_t>;
    // type of the Win_Seq_GPU to be created within the regular constructor
    using win_seq_gpu_t = Win_Seq_GPU<tuple_t, result_t, win_F_t, wrapper_in_t>;
    // type of the WF_Emitter node
    using wf_emitter_t = WF_Emitter<tuple_t, input_t>;
    // type of the WF_Collector node
    using wf_collector_t = WF_Collector<result_t>;
    // friendships with other classes in the library
    template<typename T1, typename T2, typename T3, typename T4>
    friend class Pane_Farm_GPU;
    template<typename T1, typename T2, typename T3, typename T4>
    friend class Win_MapReduce_GPU;
    template<typename T>
    friend auto get_WF_GPU_nested_type(T);
    // flag stating whether the Win_Farm_GPU has been instantiated with complex workers (Pane_Farm_GPU or Win_MapReduce_GPU instances)
    bool hasComplexWorkers;
    // optimization level of the Win_Farm_GPU
    opt_level_t opt_level;
    // window type (CB or TB)
    win_type_t winType;
    // number of Win_Farm_GPU emitters
    size_t num_emitters;

    // private constructor I (stub)
    Win_Farm_GPU() {}

    // private constructor II
    Win_Farm_GPU(win_F_t _winFunction,
                 uint64_t _win_len,
                 uint64_t _slide_len,
                 win_type_t _winType,
                 size_t _emitter_degree,
                 size_t _pardegree,
                 size_t _batch_len,
                 size_t _n_thread_block,
                 string _name,
                 size_t _scratchpad_size,
                 bool _ordered,
                 opt_level_t _opt_level,
                 PatternConfig _config,
                 role_t _role): hasComplexWorkers(false), opt_level(_opt_level), winType(_winType), num_emitters(_emitter_degree)
    {
        // check the validity of the windowing parameters
        if (_win_len == 0 || _slide_len == 0) {
            cerr << RED << "WindFlow Error: window length or slide cannot be zero" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // check the validity of the emitter degree
        if (_emitter_degree == 0) {
            cerr << RED << "WindFlow Error: at least one emitter is needed" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // check the validity of the parallelism degree
        if (_pardegree == 0) {
            cerr << RED << "WindFlow Error: parallelism degree cannot be zero" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // check the validity of the batch length
        if (_batch_len == 0) {
            cerr << RED << "WindFlow Error: batch length cannot be zero" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // check the optimization level
        if (_opt_level != LEVEL0) {
            cerr << YELLOW << "WindFlow Warning: optimization level has no effect" << DEFAULT << endl;
            opt_level = LEVEL0;
        }
        // vector of Win_Seq_GPU instances
        vector<ff_node *> w;
        // private sliding factor of each Win_Seq_GPU instance
        uint64_t private_slide = _slide_len * _pardegree;
        // standard case: one Emitter node
        if (_emitter_degree == 1) {
            // create the Win_Seq_GPU instances
            for (size_t i = 0; i < _pardegree; i++) {
                // configuration structure of the Win_Seq_GPU instances
                PatternConfig configSeq(_config.id_inner, _config.n_inner, _config.slide_inner, i, _pardegree, _slide_len);
                auto *seq = new win_seq_gpu_t(_winFunction, _win_len, private_slide, _winType, _batch_len, _n_thread_block, _name + "_wf", _scratchpad_size, configSeq, _role);
                w.push_back(seq);
            }
        }
        // advanced case: multiple Emitter nodes
        else {
            ff_a2a *a2a = new ff_a2a();
            // create the Emitter nodes
            vector<ff_node *> emitters(_emitter_degree);
            for (size_t i = 0; i < _emitter_degree; i++) {
                auto *emitter = new wf_emitter_t(_winType, _win_len, _slide_len, _pardegree, _config.id_inner, _config.n_inner, _config.slide_inner, _role);
                emitters[i] = emitter;
            }
            a2a->add_firstset(emitters, 0, true);
            // create the Win_Seq_GPU nodes composed with an orderingNodes
            vector<ff_node *> seqs(_pardegree);
            for (size_t i = 0; i < _pardegree; i++) {
                auto *ord = new OrderingNode<tuple_t, wrapper_in_t>(((_winType == CB) ? ID : TS));
                // configuration structure of the Win_Seq_GPU instances
                PatternConfig configSeq(_config.id_inner, _config.n_inner, _config.slide_inner, i, _pardegree, _slide_len);
                auto *seq = new win_seq_gpu_t(_winFunction, _win_len, private_slide, _winType, _batch_len, _n_thread_block, _name + "_wf", _scratchpad_size, configSeq, _role);
                auto *comb = new ff_comb(ord, seq, true, true);
                seqs[i] = comb;
            }
            a2a->add_secondset(seqs, true);
            w.push_back(a2a);
        }
        ff_farm::add_workers(w);
        // create the Emitter and Collector nodes
        if (_emitter_degree == 1)
            ff_farm::add_emitter(new wf_emitter_t(_winType, _win_len, _slide_len, _pardegree, _config.id_inner, _config.n_inner, _config.slide_inner, _role));
        if (_ordered)
            ff_farm::add_collector(new wf_collector_t());
        else
            ff_farm::add_collector(nullptr);
        // when the Win_Farm_GPU will be destroyed we need aslo to destroy the emitter, workers and collector
        ff_farm::cleanup_all();
    }

    // method to optimize the structure of the Win_Farm_GPU pattern
    void optimize_WinFarmGPU(opt_level_t opt)
    {
        if (opt == LEVEL0) // no optimization
            return;
        else if (opt == LEVEL1 || opt == LEVEL2) // optimization level 1
            remove_internal_collectors(*this); // remove all the default collectors in the Win_Farm_GPU
        else { // optimization level 2
            cerr << YELLOW << "WindFlow Warning: optimization level not supported yet" << DEFAULT << endl;
            assert(false);
        }
    }

public:
    /** 
     *  \brief Constructor I
     *  
     *  \param _winFunction the host/device window processing function
     *  \param _win_len window length (in no. of tuples or in time units)
     *  \param _slide_len slide length (in no. of tuples or in time units)
     *  \param _winType window type (count-based CB or time-based TB)
     *  \param _emitter_degree number of replicas of the emitter node
     *  \param _pardegree parallelism degree of the Win_Farm_GPU pattern
     *  \param _batch_len no. of windows in a batch (i.e. 1 window mapped onto 1 CUDA thread)
     *  \param _n_thread_block number of threads (i.e. windows) per block
     *  \param _name string with the unique name of the pattern
     *  \param _scratchpad_size size in bytes of the scratchpad area per CUDA thread
     *  \param _ordered true if the results of the same key must be emitted in order, false otherwise
     *  \param _opt_level optimization level used to build the pattern
     */ 
    Win_Farm_GPU(win_F_t _winFunction,
                 uint64_t _win_len,
                 uint64_t _slide_len,
                 win_type_t _winType,
                 size_t _emitter_degree,
                 size_t _pardegree,
                 size_t _batch_len,
                 size_t _n_thread_block,
                 string _name,
                 size_t _scratchpad_size=0,
                 bool _ordered=true,
                 opt_level_t _opt_level=LEVEL0)
                 :
                 Win_Farm_GPU(_winFunction, _win_len, _slide_len, _winType, _emitter_degree, _pardegree, _batch_len, _n_thread_block, _name, _scratchpad_size, _ordered, _opt_level, PatternConfig(0, 1, _slide_len, 0, 1, _slide_len), SEQ) {}

    /** 
     *  \brief Constructor II (Nesting with Pane_Farm_GPU)
     *  
     *  \param _pf Pane_Farm_GPU instance to be replicated within the Win_Farm_GPU pattern
     *  \param _win_len window length (in no. of tuples or in time units)
     *  \param _slide_len slide length (in no. of tuples or in time units)
     *  \param _winType window type (count-based CB or time-based TB)
     *  \param _emitter_degree number of replicas of the emitter node
     *  \param _pardegree parallelism degree of the Win_Farm_GPU pattern
     *  \param _batch_len no. of windows in a batch (i.e. 1 window mapped onto 1 CUDA thread)
     *  \param _n_thread_block number of threads (i.e. windows) per block
     *  \param _name string with the unique name of the pattern
     *  \param _scratchpad_size size in bytes of the scratchpad area per CUDA thread
     *  \param _ordered true if the results of the same key must be emitted in order, false otherwise
     *  \param _opt_level optimization level used to build the pattern
     */ 
    Win_Farm_GPU(const pane_farm_gpu_t &_pf,
                 uint64_t _win_len,
                 uint64_t _slide_len,
                 win_type_t _winType,
                 size_t _emitter_degree,
                 size_t _pardegree,
                 size_t _batch_len,
                 size_t _n_thread_block,
                 string _name,
                 size_t _scratchpad_size=0,
                 bool _ordered=true,
                 opt_level_t _opt_level=LEVEL0): hasComplexWorkers(true), opt_level(_opt_level), winType(_winType), num_emitters(_emitter_degree)
    {
        // type of the Pane_Farm_GPU to be created within the Win_Farm_GPU pattern
        using panewrap_farm_gpu_t = Pane_Farm_GPU<tuple_t, result_t, win_F_t, wrapper_in_t>;
        // check the validity of the windowing parameters
        if (_win_len == 0 || _slide_len == 0) {
            cerr << RED << "WindFlow Error: window length or slide cannot be zero" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // check the validity of the emitter degree
        if (_emitter_degree == 0) {
            cerr << RED << "WindFlow Error: at least one emitter is needed" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // check the validity of the parallelism degree
        if (_pardegree == 0) {
            cerr << RED << "WindFlow Error: parallelism degree cannot be zero" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // check the validity of the batch length
        if (_batch_len == 0) {
            cerr << RED << "WindFlow Error: batch length cannot be zero" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // check the compatibility of the windowing/batching parameters
        if (_pf.win_len != _win_len || _pf.slide_len != _slide_len || _pf.winType != _winType || _pf.batch_len != _batch_len || _pf.n_thread_block != _n_thread_block) {
            cerr << RED << "WindFlow Error: incompatible windowing and batching parameters" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // vector of Pane_Farm_GPU instances
        vector<ff_node *> w;
        // standard case: one Emitter node
        if (_emitter_degree == 1) {
            // create the Pane_Farm_GPU instances starting from the input one
            for (size_t i = 0; i < _pardegree; i++) {
                // configuration structure of the Pane_Farm_GPU instances
                PatternConfig configPF(0, 1, _slide_len, i, _pardegree, _slide_len);
                // create the correct Pane_Farm_GPU instance
                panewrap_farm_gpu_t *pf_W = nullptr;
                if (_pf.isGPUPLQ) {
                    if (_pf.isNICWLQ)
                        pf_W = new panewrap_farm_gpu_t(_pf.gpuFunction, _pf.wlqFunction, _pf.win_len, _pf.slide_len * _pardegree, _pf.winType, _pf.plq_degree, _pf.wlq_degree, _pf.batch_len, _pf.n_thread_block, _name + "_wf_" + to_string(i), _pf.scratchpad_size, false, _pf.opt_level, configPF);
                    else
                        pf_W = new panewrap_farm_gpu_t(_pf.gpuFunction, _pf.wlqUpdate, _pf.win_len, _pf.slide_len * _pardegree, _pf.winType, _pf.plq_degree, _pf.wlq_degree, _pf.batch_len, _pf.n_thread_block, _name + "_wf_" + to_string(i), _pf.scratchpad_size, false, _pf.opt_level, configPF);
                }
                else {
                    if (_pf.isNICPLQ)
                        pf_W = new panewrap_farm_gpu_t(_pf.plqFunction, _pf.gpuFunction, _pf.win_len, _pf.slide_len * _pardegree, _pf.winType, _pf.plq_degree, _pf.wlq_degree, _pf.batch_len, _pf.n_thread_block, _name + "_wf_" + to_string(i), _pf.scratchpad_size, false, _pf.opt_level, configPF);
                    else
                        pf_W = new panewrap_farm_gpu_t(_pf.plqUpdate, _pf.gpuFunction, _pf.win_len, _pf.slide_len * _pardegree, _pf.winType, _pf.plq_degree, _pf.wlq_degree, _pf.batch_len, _pf.n_thread_block, _name + "_wf_" + to_string(i), _pf.scratchpad_size, false, _pf.opt_level, configPF);
                }
                w.push_back(pf_W);
            }
        }
        // advanced case: multiple Emitter nodes
        else {
            ff_a2a *a2a = new ff_a2a();
            // create the Emitter nodes
            vector<ff_node *> emitters(_emitter_degree);
            for (size_t i = 0; i < _emitter_degree; i++) {
                auto *emitter = new wf_emitter_t(_winType, _win_len, _slide_len, _pardegree, 0, 1, _slide_len, SEQ);
                emitters[i] = emitter;
            }
            a2a->add_firstset(emitters, 0, true);
            // create the correct Pane_Farm_GPU instances
            vector<ff_node *> pfs(_pardegree);
            for (size_t i = 0; i < _pardegree; i++) {
                // an ordering node must be composed before the first node of the Pane_Farm_GPU instance
                auto *ord = new OrderingNode<tuple_t, wrapper_in_t>(((_winType == CB) ? ID : TS));
                // configuration structure of the Pane_Farm_GPU instances
                PatternConfig configPF(0, 1, _slide_len, i, _pardegree, _slide_len);
                // create the correct Pane_Farm_GPU instance
                panewrap_farm_gpu_t *pf_W = nullptr;
                if (_pf.isGPUPLQ) {
                    if (_pf.isNICWLQ)
                        pf_W = new panewrap_farm_gpu_t(_pf.gpuFunction, _pf.wlqFunction, _pf.win_len, _pf.slide_len * _pardegree, _pf.winType, _pf.plq_degree, _pf.wlq_degree, _pf.batch_len, _pf.n_thread_block, _name + "_wf_" + to_string(i), _pf.scratchpad_size, false, _pf.opt_level, configPF);
                    else
                        pf_W = new panewrap_farm_gpu_t(_pf.gpuFunction, _pf.wlqUpdate, _pf.win_len, _pf.slide_len * _pardegree, _pf.winType, _pf.plq_degree, _pf.wlq_degree, _pf.batch_len, _pf.n_thread_block, _name + "_wf_" + to_string(i), _pf.scratchpad_size, false, _pf.opt_level, configPF);
                }
                else {
                    if (_pf.isNICPLQ)
                        pf_W = new panewrap_farm_gpu_t(_pf.plqFunction, _pf.gpuFunction, _pf.win_len, _pf.slide_len * _pardegree, _pf.winType, _pf.plq_degree, _pf.wlq_degree, _pf.batch_len, _pf.n_thread_block, _name + "_wf_" + to_string(i), _pf.scratchpad_size, false, _pf.opt_level, configPF);
                    else
                        pf_W = new panewrap_farm_gpu_t(_pf.plqUpdate, _pf.gpuFunction, _pf.win_len, _pf.slide_len * _pardegree, _pf.winType, _pf.plq_degree, _pf.wlq_degree, _pf.batch_len, _pf.n_thread_block, _name + "_wf_" + to_string(i), _pf.scratchpad_size, false, _pf.opt_level, configPF);
                }
                // combine the first node of the Pane_Farm_GPU instance with the buffering node
                combine_with_firststage(*pf_W, ord, true);
                pfs[i] = pf_W;
            }
            a2a->add_secondset(pfs, true);
            w.push_back(a2a);
        }
        ff_farm::add_workers(w);
        // create the Emitter and Collector nodes
        if (_emitter_degree == 1)
            ff_farm::add_emitter(new wf_emitter_t(_winType, _win_len, _slide_len, _pardegree, 0, 1, _slide_len, SEQ));
        if (_ordered)
            ff_farm::add_collector(new wf_collector_t());
        else
            ff_farm::add_collector(nullptr);
        // optimization process according to the provided optimization level
        this->optimize_WinFarmGPU(_opt_level);
        // when the Win_Farm_GPU will be destroyed we need aslo to destroy the emitter, workers and collector
        ff_farm::cleanup_all();
    }

    /** 
     *  \brief Constructor III (Nesting with Win_MapReduce_GPU)
     *  
     *  \param _wm Win_MapReduce_GPU instance to be replicated within the Win_Farm_GPU pattern
     *  \param _win_len window length (in no. of tuples or in time units)
     *  \param _slide_len slide length (in no. of tuples or in time units)
     *  \param _winType window type (count-based CB or time-based TB)
     *  \param _emitter_degree number of replicas of the emitter node
     *  \param _pardegree parallelism degree of the Win_Farm_GPU pattern
     *  \param _batch_len no. of windows in a batch (i.e. 1 window mapped onto 1 CUDA thread)
     *  \param _n_thread_block number of threads (i.e. windows) per block
     *  \param _name string with the unique name of the pattern
     *  \param _scratchpad_size size in bytes of the scratchpad area per CUDA thread
     *  \param _ordered true if the results of the same key must be emitted in order, false otherwise
     *  \param _opt_level optimization level used to build the pattern
     */ 
    Win_Farm_GPU(const win_mapreduce_gpu_t &_wm,
                 uint64_t _win_len,
                 uint64_t _slide_len,
                 win_type_t _winType,
                 size_t _emitter_degree,
                 size_t _pardegree,
                 size_t _batch_len,
                 size_t _n_thread_block,
                 string _name,
                 size_t _scratchpad_size=0,
                 bool _ordered=true,
                 opt_level_t _opt_level=LEVEL0): hasComplexWorkers(true), opt_level(_opt_level), winType(_winType), num_emitters(_emitter_degree)
    {
        // type of the Win_MapReduce_GPU to be created within the Win_Farm_GPU pattern
        using winwrap_mapreduce_gpu_t = Win_MapReduce_GPU<tuple_t, result_t, win_F_t, wrapper_in_t>;
        // check the validity of the windowing parameters
        if (_win_len == 0 || _slide_len == 0) {
            cerr << RED << "WindFlow Error: window length or slide cannot be zero" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // check the validity of the emitter degree
        if (_emitter_degree == 0) {
            cerr << RED << "WindFlow Error: at least one emitter is needed" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // check the validity of the parallelism degree
        if (_pardegree == 0) {
            cerr << RED << "WindFlow Error: parallelism degree cannot be zero" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // check the validity of the batch length
        if (_batch_len == 0) {
            cerr << RED << "WindFlow Error: batch length cannot be zero" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // check the compatibility of the windowing/batching parameters
        if (_wm.win_len != _win_len || _wm.slide_len != _slide_len || _wm.winType != _winType || _wm.batch_len != _batch_len || _wm.n_thread_block != _n_thread_block) {
            cerr << RED << "WindFlow Error: incompatible windowing and batching parameters" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // vector of Win_MapReduce_GPU instances
        vector<ff_node *> w;
        // standard case: one Emitter node
        if (_emitter_degree == 1) {
            // create the Win_MapReduce instances starting from the input one
            for (size_t i = 0; i < _pardegree; i++) {
                // configuration structure of the Win_mapReduce instances
                PatternConfig configWM(0, 1, _slide_len, i, _pardegree, _slide_len);
                // create the correct Win_MapReduce_GPU instance
                winwrap_mapreduce_gpu_t *wm_W = nullptr;
                if (_wm.isGPUMAP) {
                    if (_wm.isNICREDUCE)
                        wm_W = new winwrap_mapreduce_gpu_t(_wm.gpuFunction, _wm.reduceFunction, _wm.win_len, _wm.slide_len * _pardegree, _wm.winType, _wm.map_degree, _wm.reduce_degree, _wm.batch_len, _wm.n_thread_block, _name + "_wf_" + to_string(i), _wm.scratchpad_size, false, _wm.opt_level, configWM);
                    else
                        wm_W = new winwrap_mapreduce_gpu_t(_wm.gpuFunction, _wm.reduceUpdate, _wm.win_len, _wm.slide_len * _pardegree, _wm.winType, _wm.map_degree, _wm.reduce_degree, _wm.batch_len, _wm.n_thread_block, _name + "_wf_" + to_string(i), _wm.scratchpad_size, false, _wm.opt_level, configWM);
                }
                else {
                    if (_wm.isNICMAP)
                        wm_W = new winwrap_mapreduce_gpu_t(_wm.mapFunction, _wm.gpuFunction, _wm.win_len, _wm.slide_len * _pardegree, _wm.winType, _wm.map_degree, _wm.reduce_degree, _wm.batch_len, _wm.n_thread_block, _name + "_wf_" + to_string(i), _wm.scratchpad_size, false, _wm.opt_level, configWM);
                    else
                        wm_W = new winwrap_mapreduce_gpu_t(_wm.mapUpdate, _wm.gpuFunction, _wm.win_len, _wm.slide_len * _pardegree, _wm.winType, _wm.map_degree, _wm.reduce_degree, _wm.batch_len, _wm.n_thread_block, _name + "_wf_" + to_string(i), _wm.scratchpad_size, false, _wm.opt_level, configWM);
                }
                w.push_back(wm_W);
            }
        }
        // advanced case: multiple Emitter nodes
        else {
            ff_a2a *a2a = new ff_a2a();
            // create the Emitter nodes
            vector<ff_node *> emitters(_emitter_degree);
            for (size_t i = 0; i < _emitter_degree; i++) {
                auto *emitter = new wf_emitter_t(_winType, _win_len, _slide_len, _pardegree, 0, 1, _slide_len, SEQ);
                emitters[i] = emitter;
            }
            a2a->add_firstset(emitters, 0, true);
            // create the correct Win_MapReduce_GPU instances
            vector<ff_node *> wms(_pardegree);
            for (size_t i = 0; i < _pardegree; i++) {
                // an ordering node must be composed before the first node of the Win_MapReduce instance
                auto *ord = new OrderingNode<tuple_t, wrapper_in_t>(((_winType == CB) ? ID : TS));
                // configuration structure of the Win_MapReduce_GPU instances
                PatternConfig configWM(0, 1, _slide_len, i, _pardegree, _slide_len);
                // create the correct Win_MapReduce_GPU instance
                winwrap_mapreduce_gpu_t *wm_W = nullptr;
                if (_wm.isGPUMAP) {
                    if (_wm.isNICREDUCE)
                        wm_W = new winwrap_mapreduce_gpu_t(_wm.gpuFunction, _wm.reduceFunction, _wm.win_len, _wm.slide_len * _pardegree, _wm.winType, _wm.map_degree, _wm.reduce_degree, _wm.batch_len, _wm.n_thread_block, _name + "_wf_" + to_string(i), _wm.scratchpad_size, false, _wm.opt_level, configWM);
                    else
                        wm_W = new winwrap_mapreduce_gpu_t(_wm.gpuFunction, _wm.reduceUpdate, _wm.win_len, _wm.slide_len * _pardegree, _wm.winType, _wm.map_degree, _wm.reduce_degree, _wm.batch_len, _wm.n_thread_block, _name + "_wf_" + to_string(i), _wm.scratchpad_size, false, _wm.opt_level, configWM);
                }
                else {
                    if (_wm.isNICMAP)
                        wm_W = new winwrap_mapreduce_gpu_t(_wm.mapFunction, _wm.gpuFunction, _wm.win_len, _wm.slide_len * _pardegree, _wm.winType, _wm.map_degree, _wm.reduce_degree, _wm.batch_len, _wm.n_thread_block, _name + "_wf_" + to_string(i), _wm.scratchpad_size, false, _wm.opt_level, configWM);
                    else
                        wm_W = new winwrap_mapreduce_gpu_t(_wm.mapUpdate, _wm.gpuFunction, _wm.win_len, _wm.slide_len * _pardegree, _wm.winType, _wm.map_degree, _wm.reduce_degree, _wm.batch_len, _wm.n_thread_block, _name + "_wf_" + to_string(i), _wm.scratchpad_size, false, _wm.opt_level, configWM);
                }        
                // combine the first node of the Win_MapReduce instance with the buffering node
                combine_with_firststage(*wm_W, ord, true);
                wms[i] = wm_W;
            }
            a2a->add_secondset(wms, true);
            w.push_back(a2a);
        }
        ff_farm::add_workers(w);
        // create the Emitter and Collector nodes
        if (_emitter_degree == 1)
            ff_farm::add_emitter(new wf_emitter_t(_winType, _win_len, _slide_len, _pardegree, 0, 1, _slide_len, SEQ));
        if (_ordered)
            ff_farm::add_collector(new wf_collector_t());
        else
            ff_farm::add_collector(nullptr);
        // optimization process according to the provided optimization level
        this->optimize_WinFarmGPU(_opt_level);
        // when the Win_Farm_GPU will be destroyed we need aslo to destroy the emitter, workers and collector
        ff_farm::cleanup_all();
    }

    /** 
     *  \brief Check whether the Win_Farm has been instantiated with complex patterns inside
     *  \return true if the Win_Farm has complex patterns inside
     */
    bool useComplexNesting() { return hasComplexWorkers; }

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

    /** 
     *  \brief Get the number of emitters utilized by the pattern
     *  \return number of emitters
     */
    size_t getNumEmitters() { return num_emitters; }

//@cond DOXY_IGNORE

    // -------------------------------------- deleted methods ----------------------------------------
    template<typename T>
    int add_emitter(T *e)                                                                    = delete;
    template<typename T>
    int add_emitter(const T &e)                                                              = delete;
    template<typename T>
    int change_emitter(T *e, bool cleanup=false)                                             = delete;
    template<typename T>
    int change_emitter(const T &e, bool cleanup=false)                                       = delete;
    void set_ordered(const size_t MemoryElements=DEF_OFARM_ONDEMAND_MEMORY)                  = delete;
    int add_workers(std::vector<ff_node *> &w)                                               = delete;
    int add_collector(ff_node *c, bool cleanup=false)                                        = delete;
    int wrap_around(bool multi_input=false)                                                  = delete;
    int remove_collector()                                                                   = delete;
    void cleanup_workers()                                                                   = delete;
    void cleanup_all()                                                                       = delete;
    bool offload(void *task, unsigned long retry=((unsigned long)-1),
        unsigned long ticks=ff_loadbalancer::TICKS2WAIT)                                     = delete;
    bool load_result(void **task, unsigned long retry=((unsigned long)-1),
        unsigned long ticks=ff_gatherer::TICKS2WAIT)                                         = delete;
    bool load_result_nb(void **task)                                                         = delete;

private:
    using ff_farm::set_scheduling_ondemand;

//@endcond

};

#endif
