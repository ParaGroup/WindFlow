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
 *  @brief Win_Farm_GPU operator executing windowed queries in parallel on GPU
 *  
 *  @section Win_Farm_GPU (Description)
 *  
 *  This file implements the Win_Farm_GPU operator able to executes windowed queries
 *  on a GPU device. The operator prepares batches of input tuples in parallel on the
 *  CPU cores and offloads on the GPU the parallel processing of the windows within
 *  the same batch.
 *  
 *  The template parameters tuple_t and result_t must be default constructible, with a copy
 *  constructor and a copy assignment operator, and they must provide and implement the
 *  setControlFields() and getControlFields() methods. Furthermore, in order to be copyable
 *  in a GPU-accessible memory, they must be compliant with the C++ specification for standard
 *  layout types. The third template argument win_F_t is the type of the callable object to be
 *  used for GPU processing.
 */ 

#ifndef WIN_FARM_GPU_H
#define WIN_FARM_GPU_H

/// includes
#include<ff/pipeline.hpp>
#include<ff/all2all.hpp>
#include<ff/farm.hpp>
#include<ff/optimize.hpp>
#include<basic.hpp>
#include<win_seq_gpu.hpp>
#include<wf_nodes.hpp>
#include<wm_nodes.hpp>
#include<ordering_node.hpp>
#include<tree_emitter.hpp>
#include<basic_emitter.hpp>
#include<basic_operator.hpp>
#include<transformations.hpp>

namespace wf {

/** 
 *  \class Win_Farm_GPU
 *  
 *  \brief Win_Farm_GPU operator executing windowed queries in parallel on GPU
 *  
 *  This class implements the Win_Farm_GPU operator. The operator prepares in parallel
 *  distinct batches of tuples (on the CPU cores) and offloads the processing of the
 *  batches on the GPU by computing in parallel all the windows within a batch on the
 *  CUDA cores of the GPU.
 */ 
template<typename tuple_t, typename result_t, typename win_F_t, typename input_t>
class Win_Farm_GPU: public ff::ff_farm, public Basic_Operator
{
public:
    /// type of the Pane_Farm_GPU passed to the proper nesting Constructor
    using pane_farm_gpu_t = Pane_Farm_GPU<tuple_t, result_t, win_F_t>;
    /// type of the Win_MapReduce_GPU passed to the proper nesting Constructor
    using win_mapreduce_gpu_t = Win_MapReduce_GPU<tuple_t, result_t, win_F_t>;

private:
    // type of the wrapper of input tuples
    using wrapper_in_t = wrapper_tuple_t<tuple_t>;
    // type of the Win_Seq_GPU to be created within the regular Constructor
    using win_seq_gpu_t = Win_Seq_GPU<tuple_t, result_t, win_F_t, wrapper_in_t>;
    // type of the Pane_Farm_GPU to be created within the Win_Farm_GPU operator
    using panewrap_farm_gpu_t = Pane_Farm_GPU<tuple_t, result_t, win_F_t, wrapper_in_t>;   
    // type of the Win_MapReduce_GPU to be created within the Win_Farm_GPU operator
    using winwrap_mapreduce_gpu_t = Win_MapReduce_GPU<tuple_t, result_t, win_F_t, wrapper_in_t>;  
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
    friend class MultiPipe;
    std::string name; // name of the Win_Farm_GPU
    size_t parallelism; // internal parallelism of the Win_Farm_GPU
    bool used; // true if the Win_Farm_GPU has been added/chained in a MultiPipe
    bool isComplex; // true if the Win_Farm_GPU replicates Pane_Farm_GPU or Win_MapReduce_GPU instances
    opt_level_t outer_opt_level; // optimization level of the Win_Farm_GPU
    opt_level_t inner_opt_level; // optimization level of the inner operators within the Win_Farm_GPU
    pattern_t inner_type; // type of the inner operators (SEQ, PF_GPU or WMR_GPU)
    size_t outer_parallelism; // number of complex replicas within the Win_Farm_GPU
    size_t inner_parallelism_1; // first parallelism of the inner operators
    size_t inner_parallelism_2; // second parallelism of the inner operators
    uint64_t win_len; // window length (no. of tuples or in time units)
    uint64_t slide_len; // slide length (no. of tuples or in time units)
    uint64_t triggering_delay; // triggering delay in time units (meaningful for TB windows only)
    size_t batch_len; // length of the batch in terms of no. of windows
    win_type_t winType; // type of windows (count-based or time-based)
    std::vector<ff_node *> wf_workers; // vector of pointers to the Win_Farm_GPU workers (Win_Seq_GPU or Pane_Farm_GPU or Win_MapReduce_GPU instances)

    // Private Constructor
    Win_Farm_GPU(win_F_t _win_func,
                 uint64_t _win_len,
                 uint64_t _slide_len,
                 uint64_t _triggering_delay,
                 win_type_t _winType,
                 size_t _parallelism,
                 size_t _batch_len,
                 int _gpu_id,
                 size_t _n_thread_block,
                 std::string _name,
                 size_t _scratchpad_size,
                 bool _ordered,
                 opt_level_t _opt_level,
                 WinOperatorConfig _config,
                 role_t _role):
                 name(_name),
                 parallelism(_parallelism),
                 used(false),
                 isComplex(false),
                 outer_opt_level(_opt_level),
                 inner_opt_level(opt_level_t::LEVEL0), // not meaningful
                 inner_type(pattern_t::SEQ_GPU),
                 outer_parallelism(0), // not meaningful
                 inner_parallelism_1(0), // not meaningful
                 inner_parallelism_2(0), // not meaningful
                 win_len(_win_len),
                 slide_len(_slide_len),
                 triggering_delay(_triggering_delay),
                 batch_len(_batch_len),
                 winType(_winType)
    {
        // check the validity of the windowing parameters
        if (_win_len == 0 || _slide_len == 0) {
            std::cerr << RED << "WindFlow Error: window length or slide in Win_Farm_GPU cannot be zero" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // check the validity of the parallelism value
        if (_parallelism == 0) {
            std::cerr << RED << "WindFlow Error: Win_Farm_GPU has parallelism zero" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // check the validity of the batch length
        if (_batch_len == 0) {
            std::cerr << RED << "WindFlow Error: batch length in Win_Farm_GPU cannot be zero" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // check the optimization level
        if (_opt_level != opt_level_t::LEVEL0) {
            //std::cerr << YELLOW << "WindFlow Warning: optimization level has no effect" << DEFAULT_COLOR << std::endl;
            outer_opt_level = opt_level_t::LEVEL0;
        }
        // std::vector of Win_Seq_GPU
        std::vector<ff_node *> w;
        // private sliding factor of each Win_Seq_GPU
        uint64_t private_slide = _slide_len * _parallelism;
        // create the Win_Seq_GPU
        for (size_t i = 0; i < _parallelism; i++) {
            // configuration structure of the Win_Seq_GPU
            WinOperatorConfig configSeq(_config.id_inner, _config.n_inner, _config.slide_inner, i, _parallelism, _slide_len);
            auto *seq = new win_seq_gpu_t(_win_func, _win_len, private_slide, _triggering_delay, _winType, _batch_len, _gpu_id, _n_thread_block, _name, _scratchpad_size, configSeq, _role);
            w.push_back(seq);
            wf_workers.push_back(seq);
        }
        ff::ff_farm::add_workers(w);
        // create the Emitter and Collector nodes
        ff::ff_farm::add_emitter(new wf_emitter_t(_winType, _win_len, _slide_len, _parallelism, _config.id_inner, _config.n_inner, _config.slide_inner, _role));
        if (_ordered) {
            ff::ff_farm::add_collector(new wf_collector_t());
        }
        else {
            ff::ff_farm::add_collector(nullptr);
        }
        // when the Win_Farm_GPU will be destroyed we need aslo to destroy the emitter, workers and collector
        ff::ff_farm::cleanup_all();
    }

    // method to optimize the structure of the Win_Farm_GPU operator
    void optimize_WinFarmGPU(opt_level_t opt)
    {
        if (opt == opt_level_t::LEVEL0) { // no optimization
            return;
        }
        else if (opt == opt_level_t::LEVEL1) { // optimization level 1
            remove_internal_collectors(*this); // remove all the default collectors in the Win_Farm_GPU
        }
        else { // optimization level 2
            wf_emitter_t *wf_e = static_cast<wf_emitter_t *>(this->getEmitter());
            auto &oldWorkers = this->getWorkers();
            std::vector<Basic_Emitter *> Es;
            bool tobeTransformmed = true;
            // change the workers by removing their first emitter (if any)
            for (auto *w: oldWorkers) {
                ff::ff_pipeline *pipe = static_cast<ff::ff_pipeline *>(w);
                ff_node *e = remove_emitter_from_pipe(*pipe);
                if (e == nullptr) {
                    tobeTransformmed = false;
                }
                else {
                    Basic_Emitter *my_e = static_cast<Basic_Emitter *>(e);
                    Es.push_back(my_e);
                }
            }
            if (tobeTransformmed) {
                // create the tree emitter
                auto *treeEmitter = new Tree_Emitter(wf_e, Es);
                this->cleanup_emitter(false);
                this->change_emitter(treeEmitter, true);
            }
            remove_internal_collectors(*this);
            return;
        }
    }

public:
    /** 
     *  \brief Constructor I
     *  
     *  \param _win_func the non-incremental window processing function (__host__ __device__ function)
     *  \param _win_len window length (in no. of tuples or in time units)
     *  \param _slide_len slide length (in no. of tuples or in time units)
     *  \param _triggering_delay (triggering delay in time units, meaningful for TB windows only otherwise it must be 0)
     *  \param _winType window type (count-based CB or time-based TB)
     *  \param _parallelism internal parallelism of the Win_Farm_GPU operator
     *  \param _batch_len no. of windows in a batch
     *  \param _gpu_id identifier of the chosen GPU device
     *  \param _n_thread_block number of threads per block
     *  \param _name name of the operator
     *  \param _scratchpad_size size in bytes of the scratchpad area local of a CUDA thread (pre-allocated on the global memory of the GPU)
     *  \param _ordered true if the results of the same key must be emitted in order, false otherwise
     *  \param _opt_level optimization level used to build the operator
     */ 
    Win_Farm_GPU(win_F_t _win_func,
                 uint64_t _win_len,
                 uint64_t _slide_len,
                 uint64_t _triggering_delay,
                 win_type_t _winType,
                 size_t _parallelism,
                 size_t _batch_len,
                 int _gpu_id,
                 size_t _n_thread_block,
                 std::string _name,
                 size_t _scratchpad_size,
                 bool _ordered,
                 opt_level_t _opt_level):
                 Win_Farm_GPU(_win_func, _win_len, _slide_len, _triggering_delay, _winType, _parallelism, _batch_len, _gpu_id, _n_thread_block, _name, _scratchpad_size, _ordered, _opt_level, WinOperatorConfig(0, 1, _slide_len, 0, 1, _slide_len), role_t::SEQ) {}

    /** 
     *  \brief Constructor II (Nesting with Pane_Farm_GPU)
     *  
     *  \param _pf Pane_Farm_GPU to be replicated within the Win_Farm_GPU operator
     *  \param _win_len window length (in no. of tuples or in time units)
     *  \param _slide_len slide length (in no. of tuples or in time units)
     *  \param _triggering_delay (triggering delay in time units, meaningful for TB windows only otherwise it must be 0)
     *  \param _winType window type (count-based CB or time-based TB)
     *  \param _num_replicas number of replicas of the Pane_Farm_GPU within this Win_Farm_GPU operator
     *  \param _batch_len no. of windows in a batch
     *  \param _gpu_id identifier of the chosen GPU device
     *  \param _n_thread_block number of threads per block
     *  \param _name name of the operator
     *  \param _scratchpad_size size in bytes of the scratchpad area local of a CUDA thread (pre-allocated on the global memory of the GPU)
     *  \param _ordered true if the results of the same key must be emitted in order, false otherwise
     *  \param _opt_level optimization level used to build the operator
     */ 
    Win_Farm_GPU(pane_farm_gpu_t &_pf,
                 uint64_t _win_len,
                 uint64_t _slide_len,
                 uint64_t _triggering_delay,
                 win_type_t _winType,
                 size_t _num_replicas,
                 size_t _batch_len,
                 int _gpu_id,
                 size_t _n_thread_block,
                 std::string _name,
                 size_t _scratchpad_size,
                 bool _ordered,
                 opt_level_t _opt_level):
                 name(_name),
                 parallelism(_num_replicas * (_pf.plq_parallelism + _pf.wlq_parallelism)),
                 used(false),
                 isComplex(true),
                 outer_opt_level(_opt_level),
                 inner_opt_level(_pf.opt_level),
                 inner_type(pattern_t::PF_GPU),
                 outer_parallelism(_num_replicas),
                 inner_parallelism_1(_pf.plq_parallelism),
                 inner_parallelism_2(_pf.wlq_parallelism),
                 win_len(_win_len),
                 slide_len(_slide_len),
                 triggering_delay(_triggering_delay),
                 batch_len(_batch_len),
                 winType(_winType)
    {
        // check the validity of the windowing parameters
        if (_win_len == 0 || _slide_len == 0) {
            std::cerr << RED << "WindFlow Error: window length or slide in Win_Farm_GPU cannot be zero" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // check the validity of the number of replicas
        if (_num_replicas == 0) {
            std::cerr << RED << "WindFlow Error: number of replicas of the Pane_Farm_GPU within the Win_Farm_GPU is zero" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // check the validity of the batch length
        if (_batch_len == 0) {
            std::cerr << RED << "WindFlow Error: batch length in Win_Farm_GPUcannot be zero" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // check that the Pane_Farm_GPU has not already been used in a nested structure
        if (_pf.isUsed4Nesting()) {
            std::cerr << RED << "WindFlow Error: Pane_Farm_GPU has already been used in a nested structure" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);            
        }
        else {
            _pf.used4Nesting = true;
        }
        // check the compatibility of the configuration parameters between the Win_Farm_GPU and the inner Pane_Farm_GPU
        if (_pf.win_len != _win_len ||
            _pf.slide_len != _slide_len ||
            _pf.triggering_delay != _triggering_delay ||
            _pf.winType != _winType ||
            _pf.batch_len != _batch_len ||
            _pf.n_thread_block != _n_thread_block||
            _pf.gpu_id != _gpu_id)
        {
            std::cerr << RED << "WindFlow Error: incompatible configuration parameters between Win_Farm_GPU and Pane_Farm_GPU" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // std::vector of Pane_Farm_GPU
        std::vector<ff_node *> w;
        // create the Pane_Farm_GPU starting from the input one
        for (size_t i = 0; i < _num_replicas; i++) {
            // configuration structure of the Pane_Farm_GPU
            WinOperatorConfig configPF(0, 1, _slide_len, i, _num_replicas, _slide_len);
            // create the correct Pane_Farm_GPU
            panewrap_farm_gpu_t *pf_W = nullptr;
            if (_pf.isGPUPLQ) {
                if (_pf.isNICWLQ) {
                    pf_W = new panewrap_farm_gpu_t(_pf.gpuFunction, _pf.wlq_func, _pf.win_len, _pf.slide_len * _num_replicas, _pf.triggering_delay, _pf.winType, _pf.plq_parallelism, _pf.wlq_parallelism, _pf.batch_len, _pf.gpu_id, _pf.n_thread_block, _name + "_pf_" + std::to_string(i), _pf.scratchpad_size, false, _pf.opt_level, configPF);
                }
                else {
                    pf_W = new panewrap_farm_gpu_t(_pf.gpuFunction, _pf.wlqupdate_func, _pf.win_len, _pf.slide_len * _num_replicas, _pf.triggering_delay, _pf.winType, _pf.plq_parallelism, _pf.wlq_parallelism, _pf.batch_len, _pf.gpu_id, _pf.n_thread_block, _name + "_pf_" + std::to_string(i), _pf.scratchpad_size, false, _pf.opt_level, configPF);
                }
            }
            else {
                if (_pf.isNICPLQ) {
                    pf_W = new panewrap_farm_gpu_t(_pf.plq_func, _pf.gpuFunction, _pf.win_len, _pf.slide_len * _num_replicas, _pf.triggering_delay, _pf.winType, _pf.plq_parallelism, _pf.wlq_parallelism, _pf.batch_len, _pf.gpu_id, _pf.n_thread_block, _name + "_pf_" + std::to_string(i), _pf.scratchpad_size, false, _pf.opt_level, configPF);
                }
                else {
                    pf_W = new panewrap_farm_gpu_t(_pf.plqupdate_func, _pf.gpuFunction, _pf.win_len, _pf.slide_len * _num_replicas, _pf.triggering_delay, _pf.winType, _pf.plq_parallelism, _pf.wlq_parallelism, _pf.batch_len, _pf.gpu_id, _pf.n_thread_block, _name + "_pf_" + std::to_string(i), _pf.scratchpad_size, false, _pf.opt_level, configPF);
                }
            }
            w.push_back(pf_W);
            wf_workers.push_back(pf_W);
        }
        ff::ff_farm::add_workers(w);
        // create the Emitter and Collector nodes
        ff::ff_farm::add_emitter(new wf_emitter_t(_winType, _win_len, _slide_len, _num_replicas, 0, 1, _slide_len, role_t::SEQ));
        if (_ordered) {
            ff::ff_farm::add_collector(new wf_collector_t());
        }
        else {
            ff::ff_farm::add_collector(nullptr);
        }
        // optimization process according to the provided optimization level
        optimize_WinFarmGPU(_opt_level);
        // when the Win_Farm_GPU will be destroyed we need aslo to destroy the emitter, workers and collector
        ff::ff_farm::cleanup_all();
    }

    /** 
     *  \brief Constructor III (Nesting with Win_MapReduce_GPU)
     *  
     *  \param _wmr Win_MapReduce_GPU to be replicated within the Win_Farm_GPU operator
     *  \param _win_len window length (in no. of tuples or in time units)
     *  \param _slide_len slide length (in no. of tuples or in time units)
     *  \param _triggering_delay (triggering delay in time units, meaningful for TB windows only otherwise it must be 0)
     *  \param _winType window type (count-based CB or time-based TB)
     *  \param _num_replicas number of replicas of the Win_MapReduce_GPU within this Win_Farm_GPU operator
     *  \param _batch_len no. of windows in a batch
     *  \param _gpu_id identifier of the chosen GPU device
     *  \param _n_thread_block number of threads per block
     *  \param _name name of the operator
     *  \param _scratchpad_size size in bytes of the scratchpad area local of a CUDA thread (pre-allocated on the global memory of the GPU)
     *  \param _ordered true if the results of the same key must be emitted in order, false otherwise
     *  \param _opt_level optimization level used to build the operator
     */ 
    Win_Farm_GPU(win_mapreduce_gpu_t &_wmr,
                 uint64_t _win_len,
                 uint64_t _slide_len,
                 uint64_t _triggering_delay,
                 win_type_t _winType,
                 size_t _num_replicas,
                 size_t _batch_len,
                 int _gpu_id,
                 size_t _n_thread_block,
                 std::string _name,
                 size_t _scratchpad_size,
                 bool _ordered,
                 opt_level_t _opt_level):
                 name(_name),
                 parallelism(_num_replicas * (_wmr.map_parallelism + _wmr.reduce_parallelism)),
                 used(false),
                 isComplex(true),
                 outer_opt_level(_opt_level),
                 inner_opt_level(_wmr.opt_level),
                 inner_type(pattern_t::WMR_GPU),
                 outer_parallelism(_num_replicas),
                 inner_parallelism_1(_wmr.map_parallelism),
                 inner_parallelism_2(_wmr.reduce_parallelism),
                 win_len(_win_len),
                 slide_len(_slide_len),
                 triggering_delay(_triggering_delay),
                 batch_len(_batch_len),
                 winType(_winType)
    {
        // check the validity of the windowing parameters
        if (_win_len == 0 || _slide_len == 0) {
            std::cerr << RED << "WindFlow Error: window length or slide in Win_Farm_GPU cannot be zero" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // check the validity of the number of replicas
        if (_num_replicas == 0) {
            std::cerr << RED << "WindFlow Error: number of replicas of the Win_MapReduce_GPU within the Win_Farm_GPU is zero" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // check the validity of the batch length
        if (_batch_len == 0) {
            std::cerr << RED << "WindFlow Error: batch length in Win_Farm_GPU cannot be zero" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // check that the Win_MapReduce_GPU has not already been used in a nested structure
        if (_wmr.isUsed4Nesting()) {
            std::cerr << RED << "WindFlow Error: Win_MapReduce_GPU has already been used in a nested structure" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);            
        }
        else {
            _wmr.used4Nesting = true;
        }
        // check the compatibility of the configuration parameters between the Win_Farm_GPU and the inner Win_MapReduce_GPU
        if (_wmr.win_len != _win_len ||
            _wmr.slide_len != _slide_len ||
            _wmr.triggering_delay != _triggering_delay ||
            _wmr.winType != _winType ||
            _wmr.batch_len != _batch_len ||
            _wmr.n_thread_block != _n_thread_block||
            _wmr.gpu_id != _gpu_id)
        {
            std::cerr << RED << "WindFlow Error: incompatible configuration parameters between Win_Farm_GPU and Win_MapReduce_GPU" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // std::vector of Win_MapReduce_GPU
        std::vector<ff_node *> w;
        // create the Win_MapReduce_GPU starting from the input one
        for (size_t i = 0; i < _num_replicas; i++) {
            // configuration structure of the Win_MapReduce_GPU
            WinOperatorConfig configWM(0, 1, _slide_len, i, _num_replicas, _slide_len);
            // create the correct Win_MapReduce_GPU
            winwrap_mapreduce_gpu_t *wmr_W = nullptr;
            if (_wmr.isGPUMAP) {
                if (_wmr.isNICREDUCE) {
                    wmr_W = new winwrap_mapreduce_gpu_t(_wmr.gpuFunction, _wmr.reduce_func, _wmr.win_len, _wmr.slide_len * _num_replicas, _wmr.triggering_delay, _wmr.winType, _wmr.map_parallelism, _wmr.reduce_parallelism, _wmr.batch_len, _wmr.gpu_id, _wmr.n_thread_block, _name + "_wmr_" + std::to_string(i), _wmr.scratchpad_size, false, _wmr.opt_level, configWM);
                }
                else {
                    wmr_W = new winwrap_mapreduce_gpu_t(_wmr.gpuFunction, _wmr.reduceupdate_func, _wmr.win_len, _wmr.slide_len * _num_replicas, _wmr.triggering_delay, _wmr.winType, _wmr.map_parallelism, _wmr.reduce_parallelism, _wmr.batch_len, _wmr.gpu_id, _wmr.n_thread_block, _name + "_wmr_" + std::to_string(i), _wmr.scratchpad_size, false, _wmr.opt_level, configWM);
                }
            }
            else {
                if (_wmr.isNICMAP) {
                    wmr_W = new winwrap_mapreduce_gpu_t(_wmr.map_func, _wmr.gpuFunction, _wmr.win_len, _wmr.slide_len * _num_replicas, _wmr.triggering_delay, _wmr.winType, _wmr.map_parallelism, _wmr.reduce_parallelism, _wmr.batch_len, _wmr.gpu_id, _wmr.n_thread_block, _name + "_wmr_" + std::to_string(i), _wmr.scratchpad_size, false, _wmr.opt_level, configWM);
                }
                else {
                    wmr_W = new winwrap_mapreduce_gpu_t(_wmr.mapupdate_func, _wmr.gpuFunction, _wmr.win_len, _wmr.slide_len * _num_replicas, _wmr.triggering_delay, _wmr.winType, _wmr.map_parallelism, _wmr.reduce_parallelism, _wmr.batch_len, _wmr.gpu_id, _wmr.n_thread_block, _name + "_wmr_" + std::to_string(i), _wmr.scratchpad_size, false, _wmr.opt_level, configWM);
                }
            }
            w.push_back(wmr_W);
            wf_workers.push_back(wmr_W);
        }
        ff::ff_farm::add_workers(w);
        // create the Emitter and Collector nodes
        ff::ff_farm::add_emitter(new wf_emitter_t(_winType, _win_len, _slide_len, _num_replicas, 0, 1, _slide_len, role_t::SEQ));
        if (_ordered) {
            ff::ff_farm::add_collector(new wf_collector_t());
        }
        else {
            ff::ff_farm::add_collector(nullptr);
        }
        // optimization process according to the provided optimization level
        optimize_WinFarmGPU(_opt_level);
        // when the Win_Farm_GPU will be destroyed we need aslo to destroy the emitter, workers and collector
        ff::ff_farm::cleanup_all();
    }

    /** 
     *  \brief Check whether the Win_Farm_GPU has been instantiated with complex operators inside
     *  \return true if the Win_Farm_GPU has complex operators inside
     */ 
    bool isComplexNesting() const
    {
        return isComplex;
    }

    /** 
     *  \brief Get the optimization level used to build the Win_Farm_GPU
     *  \return adopted utilization level by the Win_Farm_GPU
     */ 
    opt_level_t getOptLevel() const
    {
        return outer_opt_level;
    }

    /** 
     *  \brief Type of the inner operators replicated by the Win_Farm_GPU
     *  \return type of the inner operators within the Win_Farm_GPU
     */ 
    pattern_t getInnerType() const
    {
        return inner_type;
    }

    /** 
     *  \brief Get the optimization level of the inner operators within the Win_Farm_GPU
     *  \return adopted utilization level by the inner operators within the Win_Farm_GPU
     */ 
    opt_level_t getInnerOptLevel() const
    {
        assert(isComplex);
        return inner_opt_level;
    }

    /** 
     *  \brief Get the number of complex replicas within the Win_Farm_GPU
     *  \return number of complex replicas within the Win_Farm_GPU
     */ 
    size_t getNumComplexReplicas() const
    {
        assert(isComplex);
        return outer_parallelism;
    }

    /** 
     *  \brief Get the parallelism (PLQ, WLQ or MAP, REDUCE) of the inner operators within the Win_Farm_GPU
     *  \return parallelism (PLQ, WLQ or MAP, REDUCE) of the inner operators within the Win_Farm_GPU
     */ 
    std::pair<size_t, size_t> getInnerParallelisms() const
    {
        assert(isComplex);
        return std::make_pair(inner_parallelism_1, inner_parallelism_2);
    }

    /** 
     *  \brief Get the window type (CB or TB) utilized by the Win_Farm_GPU
     *  \return adopted windowing semantics (count-based or time-based)
     */ 
    win_type_t getWinType() const
    {
        return winType;
    }

    /** 
     *  \brief Get the number of ignored tuples by the Win_Farm_GPU
     *  \return number of tuples ignored during the processing by the Win_Farm_GPU
     */ 
    size_t getNumIgnoredTuples() const
    {
        size_t count = 0;
        if (this->getInnerType() == pattern_t::SEQ_GPU) {
            for (auto *w: wf_workers) {
                auto *seq = static_cast<win_seq_gpu_t *>(w);
                count += seq->getNumIgnoredTuples();
            }
        }
        else if (this->getInnerType() == pattern_t::PF_GPU) {
            for (auto *w: wf_workers) {
                auto *pf = static_cast<panewrap_farm_gpu_t *>(w);
                count += pf->getNumIgnoredTuples();
            }
        }
        else if (this->getInnerType() == pattern_t::WMR_GPU) {
            for (auto *w: wf_workers) {
                auto *wmr = static_cast<winwrap_mapreduce_gpu_t *>(w);
                count += wmr->getNumIgnoredTuples();
            }
        }
        else {
            abort();
        }
        return count;
    }

    /** 
     *  \brief Get the name of the Win_Farm_GPU
     *  \return name of the Win_Farm_GPU
     */ 
    std::string getName() const override
    {
        return name;
    }

    /** 
     *  \brief Get the total parallelism within the Win_Farm_GPU
     *  \return total parallelism within the Win_Farm_GPU
     */ 
    size_t getParallelism() const override
    {
        return parallelism;
    }

    /** 
     *  \brief Return the routing mode of inputs to the Win_Farm_GPU
     *  \return routing mode (always COMPLEX for the Win_Farm_GPU)
     */ 
    routing_modes_t getRoutingMode() const override
    {
        return routing_modes_t::COMPLEX;
    }

    /** 
     *  \brief Check whether the Win_Farm_GPU has been used in a MultiPipe
     *  \return true if the Win_Farm_GPU has been added/chained to an existing MultiPipe
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
        if (this->getInnerType() == pattern_t::SEQ_GPU) {
            for (auto *w: wf_workers) {
                auto *seq = static_cast<win_seq_gpu_t *>(w);
                terminated = terminated && seq->isTerminated();
            }
        }
        else if (this->getInnerType() == pattern_t::PF_GPU) {
            for (auto *w: wf_workers) {
                auto *pf = static_cast<panewrap_farm_gpu_t *>(w);
                terminated = terminated && pf->isTerminated();
            }
        }
        else if (this->getInnerType() == pattern_t::WMR_GPU) {
            for (auto *w: wf_workers) {
                auto *wmr = static_cast<winwrap_mapreduce_gpu_t *>(w);
                terminated = terminated && wmr->isTerminated();
            }
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
        writer.String("Win_Farm_GPU");
        writer.Key("Distribution");
        writer.String("COMPLEX");
        writer.Key("isTerminated");
        writer.Bool(this->isTerminated());
        writer.Key("isWindowed");
        writer.Bool(true);
        writer.Key("isGPU");
        writer.Bool(true);
        writer.Key("Window_type");
        if (winType == win_type_t::CB) {
            writer.String("count-based");
        }
        else {
            writer.String("time-based");
            writer.Key("Window_delay");
            writer.Uint(triggering_delay);  
        }
        writer.Key("Window_length");
        writer.Uint(win_len);
        writer.Key("Window_slide");
        writer.Uint(slide_len);
        writer.Key("Batch_len");
        writer.Uint(batch_len);
        if (!this->isComplexNesting()) {
            writer.Key("Parallelism");
            writer.Uint(parallelism);
            writer.Key("areNestedOPs");
            writer.Bool(false);
        }
        else {
            writer.Key("Parallelism");
            writer.Uint(this->getNumComplexReplicas());
            writer.Key("areNestedOPs");
            writer.Bool(true);
        }
        writer.Key("Replicas");
        writer.StartArray();
        if (this->getInnerType() == pattern_t::SEQ_GPU) {
            for (auto *w: wf_workers) {
                auto *seq = static_cast<win_seq_gpu_t *>(w);
                Stats_Record record = seq->get_StatsRecord();
                record.append_Stats(writer);
            }
        }
        else if (this->getInnerType() == pattern_t::PF_GPU) {
            for (auto *w: wf_workers) {
                auto *pf = static_cast<panewrap_farm_gpu_t *>(w);
                pf->append_Stats(writer);
            }
        }
        else if (this->getInnerType() == pattern_t::WMR_GPU) {
            for (auto *w: wf_workers) {
                auto *wmr = static_cast<winwrap_mapreduce_gpu_t *>(w);
                wmr->append_Stats(writer);
            }
        }
        writer.EndArray();
        writer.EndObject(); 
    }
#endif

    /// deleted constructors/operators
    Win_Farm_GPU(const Win_Farm_GPU &) = delete; // copy constructor
    Win_Farm_GPU(Win_Farm_GPU &&) = delete; // move constructor
    Win_Farm_GPU &operator=(const Win_Farm_GPU &) = delete; // copy assignment operator
    Win_Farm_GPU &operator=(Win_Farm_GPU &&) = delete; // move assignment operator
};

} // namespace wf

#endif
