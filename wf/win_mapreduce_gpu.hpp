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
 *  @file    win_mapreduce_gpu.hpp
 *  @author  Gabriele Mencagli
 *  @date    22/05/2018
 *  
 *  @brief Win_MapReduce_GPU operator executing windowed queries on GPU
 *  
 *  @section Win_MapReduce_GPU (Description)
 *  
 *  This file implements the Win_MapReduce_GPU operator able to execute windowed
 *  queries on a GPU device. The operator processes (possibly in parallel) partitions
 *  of the windows in the so-called MAP stage, and computes (possibly in parallel) results
 *  of the windows out of the partition results in the REDUCE stage. The operator allows
 *  the user to offload either the MAP or the REDUCE processing on the GPU while the other
 *  stage is executed on the CPU with either a non-incremental or an incremental query
 *  definition.
 *  
 *  The template parameters tuple_t and result_t must be default constructible, with a copy
 *  constructor and a copy assignment operator, and they must provide and implement the
 *  setControlFields() and getControlFields() methods. Furthermore, in order to be copyable
 *  in a GPU-accessible memory, they must be compliant with the C++ specification for standard
 *  layout types. The third template argument F_t is the type of the callable object to be
 *  used for GPU processing.
 */ 

#ifndef WIN_MAPREDUCE_GPU_H
#define WIN_MAPREDUCE_GPU_H

/// includes
#include<ff/pipeline.hpp>
#include<ff/farm.hpp>
#include<win_farm.hpp>
#include<win_farm_gpu.hpp>
#include<meta.hpp>
#include<basic.hpp>
#include<meta_gpu.hpp>
#include<basic_operator.hpp>

namespace wf {

/** 
 *  \class Win_MapReduce_GPU
 *  
 *  \brief Win_MapReduce_GPU operator executing windowed queries in parallel on GPU
 *  
 *  This class implements the Win_MapReduce_GPU operator executing windowed queries in
 *  parallel on heterogeneous system (CPU+GPU). The operator processes (possibly in parallel)
 *  window partitions in the MAP stage and builds window results out from partition results
 *  (possibly in parallel) in the REDUCE stage. Either the MAP or the REDUCE stage are executed
 *  on the GPU device while the others is executed on the CPU as in the Win_MapReduce operator.
 */ 
template<typename tuple_t, typename result_t, typename F_t, typename input_t>
class Win_MapReduce_GPU: public ff::ff_pipeline, public Basic_Operator
{
public:
    /// function type of the non-incremental MAP processing
    using map_func_t = std::function<void(uint64_t, const Iterable<tuple_t> &, result_t &)>;
    /// function type of the incremental MAP processing
    using mapupdate_func_t = std::function<void(uint64_t, const tuple_t &, result_t &)>;
    /// function type of the non-incremental REDUCE processing
    using reduce_func_t = std::function<void(uint64_t, const Iterable<result_t> &, result_t &)>;
    /// function type of the incremental REDUCE processing
    using reduceupdate_func_t = std::function<void(uint64_t, const result_t &, result_t &)>;

private:
    // type of the wrapper of input tuples
    using wrapper_in_t = wrapper_tuple_t<tuple_t>;  
    // type of the WinMap_Emitter node
    using map_emitter_t = WinMap_Emitter<tuple_t, input_t>;
    // type of the WinMap_Collector node
    using map_collector_t = WinMap_Collector<result_t>; 
    // friendships with other classes in the library
    template<typename T1, typename T2, typename T3, typename T4>
    friend class Win_Farm_GPU;
    template<typename T1, typename T2, typename T3, typename T4>
    friend class Key_Farm_GPU;
    template<typename T>
    friend class WinFarmGPU_Builder;
    template<typename T>
    friend class KeyFarmGPU_Builder;
    friend class MultiPipe;
    std::string name; // name of the Win_MapReduce_GPU
    size_t parallelism; // internal parallelism of the Win_MapReduce_GPU
    bool used; // true if the Win_MapReduce_GPU has been added/chained in a MultiPipe
    bool used4Nesting; // true if the Win_MapReduce_GPU has been used in a nested structure
    win_type_t winType; // type of windows (count-based or time-based)
    // configuration variables of the Win_MapReduce_GPU
    F_t gpuFunction;
    map_func_t map_func;
    mapupdate_func_t mapupdate_func;
    reduce_func_t reduce_func;
    reduceupdate_func_t reduceupdate_func;
    bool isGPUMAP;
    bool isGPUREDUCE;
    bool isNICMAP;
    bool isNICREDUCE;
    uint64_t win_len;
    uint64_t slide_len;
    uint64_t triggering_delay;
    size_t map_parallelism;
    size_t reduce_parallelism;
    size_t batch_len;
    int gpu_id;
    size_t n_thread_block;
    size_t scratchpad_size;
    bool ordered;
    opt_level_t opt_level;
    WinOperatorConfig config;
    std::vector<ff_node *> map_workers; // vector of pointers to the Win_Seq or Win_Seq_GPU instances in the MAP stage
    std::vector<ff_node *> reduce_workers; // vector of pointers to the Win_Seq or Win_Seq_GPU instances in the REDUCE stage

    // Private Constructor I
    Win_MapReduce_GPU(F_t _gpuFunction,
                      reduce_func_t _reduce_func,
                      uint64_t _win_len,
                      uint64_t _slide_len,
                      uint64_t _triggering_delay,
                      win_type_t _winType,
                      size_t _map_parallelism,
                      size_t _reduce_parallelism,
                      size_t _batch_len,
                      int _gpu_id,
                      size_t _n_thread_block,
                      std::string _name,
                      size_t _scratchpad_size,
                      bool _ordered,
                      opt_level_t _opt_level,
                      WinOperatorConfig _config):
                      name(_name),
                      parallelism(_map_parallelism + _reduce_parallelism),
                      used(false),
                      used4Nesting(false),
                      winType(_winType),
                      gpuFunction(_gpuFunction),
                      reduce_func(_reduce_func),
                      isGPUMAP(true),
                      isGPUREDUCE(false),
                      isNICMAP(true),
                      isNICREDUCE(true),
                      win_len(_win_len),
                      slide_len(_slide_len),
                      triggering_delay(_triggering_delay),
                      map_parallelism(_map_parallelism),
                      reduce_parallelism(_reduce_parallelism),
                      batch_len(_batch_len),
                      gpu_id(_gpu_id),
                      n_thread_block(_n_thread_block),
                      scratchpad_size(_scratchpad_size),
                      ordered(_ordered),
                      opt_level(_opt_level),
                      config(_config)
    {
        // check the validity of the windowing parameters
        if (_win_len == 0 || _slide_len == 0) {
            std::cerr << RED << "WindFlow Error: window length or slide in Win_MapReduce_GPU cannot be zero" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // the Win_MapReduce_GPU must have a parallel MAP stage
        if (_map_parallelism < 2) {
            std::cerr << RED << "WindFlow Error: Win_MapReduce_GPU must have a parallel MAP stage" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // check the validity of the reduce parallelism value
        if (_reduce_parallelism == 0) {
            std::cerr << RED << "WindFlow Error: parallelism of the REDUCE stage cannot be zero" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // check the validity of the batch length
        if (_batch_len == 0) {
            std::cerr << RED << "WindFlow Error: batch length in Win_MapReduce_GPU cannot be zero" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // general fastflow pointers to the MAP and REDUCE stages
        ff_node *map_stage, *reduce_stage;
        auto closing_func = [] (RuntimeContext &) { return; };
        // create the MAP phase
        if (_map_parallelism > 1) {
            // std::vector of Win_Seq_GPU
            std::vector<ff_node *> w(_map_parallelism);
            // create the Win_Seq_GPU
            for (size_t i = 0; i < _map_parallelism; i++) {
                // configuration structure of the Win_Seq_GPU (MAP)
                WinOperatorConfig configSeqMAP(_config.id_inner, _config.n_inner, _config.slide_inner, 0, 1, _slide_len);
                auto *seq = new Win_Seq_GPU<tuple_t, result_t, F_t, wrapper_in_t>(_gpuFunction, _win_len, _slide_len, _triggering_delay, _winType, _batch_len, _gpu_id, _n_thread_block, _name + "_map", _scratchpad_size, configSeqMAP, role_t::MAP);
                seq->setMapIndexes(i, _map_parallelism);
                w[i] = seq;
                map_workers.push_back(seq);
            }
            ff::ff_farm *farm_map = new ff::ff_farm(w);
            farm_map->remove_collector();
            farm_map->add_collector(new map_collector_t());
            farm_map->add_emitter(new map_emitter_t(_map_parallelism, _winType));
            farm_map->cleanup_all();
            map_stage = farm_map;
        }
        else {
            // configuration structure of the Win_Seq_GPU (MAP)
            WinOperatorConfig configSeqMAP(_config.id_inner, _config.n_inner, _config.slide_inner, 0, 1, _slide_len);
            auto *seq_map = new Win_Seq_GPU<tuple_t, result_t, F_t, wrapper_in_t>(_gpuFunction, _win_len, _slide_len, _triggering_delay, _winType, _batch_len, _gpu_id, _n_thread_block, _name + "_map", _scratchpad_size, configSeqMAP, role_t::MAP);
            seq_map->setMapIndexes(0, 1);
            map_stage = seq_map;
            map_workers.push_back(seq_map);
        }
        // create the REDUCE phase
        if (_reduce_parallelism > 1) {
            // configuration structure of the Win_Farm (REDUCE)
            WinOperatorConfig configWFREDUCE(_config.id_outer, _config.n_outer, _config.slide_outer, _config.id_inner, _config.n_inner, _config.slide_inner);
            auto *farm_reduce = new Win_Farm<result_t, result_t>(_reduce_func, _map_parallelism, _map_parallelism, 0, win_type_t::CB, _reduce_parallelism, _name + "_reduce", closing_func, _ordered, opt_level_t::LEVEL0, configWFREDUCE, role_t::REDUCE);
            reduce_stage = farm_reduce;
            for (auto *w: farm_reduce->getWorkers()) {
                reduce_workers.push_back(w);
            }
        }
        else {
            // configuration structure of the Win_Seq (REDUCE)
            WinOperatorConfig configSeqREDUCE(_config.id_inner, _config.n_inner, _config.slide_inner, 0, 1, _map_parallelism);
            auto *seq_reduce = new Win_Seq<result_t, result_t>(_reduce_func, _map_parallelism, _map_parallelism, 0, win_type_t::CB, _name + "_reduce", closing_func, RuntimeContext(1, 0), configSeqREDUCE, role_t::REDUCE);
            reduce_stage = seq_reduce;
            reduce_workers.push_back(seq_reduce);
        }
        // add to this the pipeline optimized according to the provided optimization level
        ff::ff_pipeline::add_stage(optimize_WinMapReduceGPU(map_stage, reduce_stage, _opt_level));
        // when the Win_MapReduce_GPU will be destroyed we need aslo to destroy the two internal stages
        ff::ff_pipeline::cleanup_nodes();
        // flatten the pipeline
        ff::ff_pipeline::flatten();
    }

    // Private Constructor II
    Win_MapReduce_GPU(F_t _gpuFunction,
                      reduceupdate_func_t _reduceupdate_func,
                      uint64_t _win_len,
                      uint64_t _slide_len,
                      uint64_t _triggering_delay,
                      win_type_t _winType,
                      size_t _map_parallelism,
                      size_t _reduce_parallelism,
                      size_t _batch_len,
                      int _gpu_id,
                      size_t _n_thread_block,
                      std::string _name,
                      size_t _scratchpad_size,
                      bool _ordered,
                      opt_level_t _opt_level,
                      WinOperatorConfig _config):
                      name(_name),
                      parallelism(_map_parallelism + _reduce_parallelism),
                      used(false),
                      used4Nesting(false),
                      winType(_winType),
                      gpuFunction(_gpuFunction),
                      reduceupdate_func(_reduceupdate_func),
                      isGPUMAP(true),
                      isGPUREDUCE(false),
                      isNICMAP(true),
                      isNICREDUCE(false),
                      win_len(_win_len),
                      slide_len(_slide_len),
                      triggering_delay(_triggering_delay),
                      map_parallelism(_map_parallelism),
                      reduce_parallelism(_reduce_parallelism),
                      batch_len(_batch_len),
                      gpu_id(_gpu_id),
                      n_thread_block(_n_thread_block),
                      scratchpad_size(_scratchpad_size),
                      ordered(_ordered),
                      opt_level(_opt_level),
                      config(_config)
    {
        // check the validity of the windowing parameters
        if (_win_len == 0 || _slide_len == 0) {
            std::cerr << RED << "WindFlow Error: window length or slide in Win_MapReduce_GPU cannot be zero" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // the Win_MapReduce_GPU must have a parallel MAP stage
        if (_map_parallelism < 2) {
            std::cerr << RED << "WindFlow Error: Win_MapReduce_GPU must have a parallel MAP stage" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // check the validity of the reduce parallelism value
        if (_reduce_parallelism == 0) {
            std::cerr << RED << "WindFlow Error: parallelism of the REDUCE stage cannot be zero" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // check the validity of the batch length
        if (_batch_len == 0) {
            std::cerr << RED << "WindFlow Error: batch length in Win_MapReduce_GPU cannot be zero" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // general fastflow pointers to the MAP and REDUCE stages
        ff_node *map_stage, *reduce_stage;
        auto closing_func = [] (RuntimeContext &) { return; };
        // create the MAP phase
        if (_map_parallelism > 1) {
            // std::vector of Win_Seq_GPU
            std::vector<ff_node *> w(_map_parallelism);
            // create the Win_Seq_GPU
            for (size_t i = 0; i < _map_parallelism; i++) {
                // configuration structure of the Win_Seq_GPU (MAP)
                WinOperatorConfig configSeqMAP(_config.id_inner, _config.n_inner, _config.slide_inner, 0, 1, _slide_len);
                auto *seq = new Win_Seq_GPU<tuple_t, result_t, F_t, wrapper_in_t>(_gpuFunction, _win_len, _slide_len, _triggering_delay, _winType, _batch_len, _gpu_id, _n_thread_block, _name + "_map", _scratchpad_size, configSeqMAP, role_t::MAP);
                seq->setMapIndexes(i, _map_parallelism);
                w[i] = seq;
                map_workers.push_back(seq);
            }
            ff::ff_farm *farm_map = new ff::ff_farm(w);
            farm_map->remove_collector();
            farm_map->add_collector(new map_collector_t());
            farm_map->add_emitter(new map_emitter_t(_map_parallelism, _winType));
            farm_map->cleanup_all();
            map_stage = farm_map;
        }
        else {
            // configuration structure of the Win_Seq_GPU (MAP)
            WinOperatorConfig configSeqMAP(_config.id_inner, _config.n_inner, _config.slide_inner, 0, 1, _slide_len);
            auto *seq_map = new Win_Seq_GPU<tuple_t, result_t, F_t, wrapper_in_t>(_gpuFunction, _win_len, _slide_len, _triggering_delay, _winType, _batch_len, _gpu_id, _n_thread_block, _name + "_map", _scratchpad_size, configSeqMAP, role_t::MAP);
            seq_map->setMapIndexes(0, 1);
            map_stage = seq_map;
            map_workers.push_back(seq_map);
        }
        // create the REDUCE phase
        if (_reduce_parallelism > 1) {
            // configuration structure of the Win_Farm (REDUCE)
            WinOperatorConfig configWFREDUCE(_config.id_outer, _config.n_outer, _config.slide_outer, _config.id_inner, _config.n_inner, _config.slide_inner);
            auto *farm_reduce = new Win_Farm<result_t, result_t>(_reduceupdate_func, _map_parallelism, _map_parallelism, 0, win_type_t::CB, _reduce_parallelism, _name + "_reduce", closing_func, _ordered, opt_level_t::LEVEL0, configWFREDUCE, role_t::REDUCE);
            reduce_stage = farm_reduce;
            for (auto *w: farm_reduce->getWorkers()) {
                reduce_workers.push_back(w);
            }
        }
        else {
            // configuration structure of the Win_Seq (REDUCE)
            WinOperatorConfig configSeqREDUCE(_config.id_inner, _config.n_inner, _config.slide_inner, 0, 1, _map_parallelism);
            auto *seq_reduce = new Win_Seq<result_t, result_t>(_reduceupdate_func, _map_parallelism, _map_parallelism, 0, win_type_t::CB, _name + "_reduce", closing_func, RuntimeContext(1, 0), configSeqREDUCE, role_t::REDUCE);
            reduce_stage = seq_reduce;
            reduce_workers.push_back(seq_reduce);
        }
        // add to this the pipeline optimized according to the provided optimization level
        ff::ff_pipeline::add_stage(optimize_WinMapReduceGPU(map_stage, reduce_stage, _opt_level));
        // when the Win_MapReduce_GPU will be destroyed we need aslo to destroy the two internal stages
        ff::ff_pipeline::cleanup_nodes();
        // flatten the pipeline
        ff::ff_pipeline::flatten();
    }

    // Private Constructor III
    Win_MapReduce_GPU(map_func_t _map_func,
                      F_t _gpuFunction,
                      uint64_t _win_len,
                      uint64_t _slide_len,
                      uint64_t _triggering_delay,
                      win_type_t _winType,
                      size_t _map_parallelism,
                      size_t _reduce_parallelism,
                      size_t _batch_len,
                      int _gpu_id,
                      size_t _n_thread_block,
                      std::string _name,
                      size_t _scratchpad_size,
                      bool _ordered,
                      opt_level_t _opt_level,
                      WinOperatorConfig _config):
                      name(_name),
                      parallelism(_map_parallelism + _reduce_parallelism),
                      used(false),
                      used4Nesting(false),
                      winType(_winType),
                      map_func(_map_func),
                      gpuFunction(_gpuFunction),
                      isGPUMAP(false),
                      isGPUREDUCE(true),
                      isNICMAP(true),
                      isNICREDUCE(true),
                      win_len(_win_len),
                      slide_len(_slide_len),
                      triggering_delay(_triggering_delay),
                      map_parallelism(_map_parallelism),
                      reduce_parallelism(_reduce_parallelism),
                      batch_len(_batch_len),
                      gpu_id(_gpu_id),
                      n_thread_block(_n_thread_block),
                      scratchpad_size(_scratchpad_size),
                      ordered(_ordered),
                      opt_level(_opt_level),
                      config(_config)
    {
        // check the validity of the windowing parameters
        if (_win_len == 0 || _slide_len == 0) {
            std::cerr << RED << "WindFlow Error: window length or slide in Win_MapReduce_GPU cannot be zero" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // the Win_MapReduce_GPU must have a parallel MAP stage
        if (_map_parallelism < 2) {
            std::cerr << RED << "WindFlow Error: Win_MapReduce_GPU must have a parallel MAP stage" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // check the validity of the reduce parallelism value
        if (_reduce_parallelism == 0) {
            std::cerr << RED << "WindFlow Error: parallelism of the REDUCE stage cannot be zero" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // check the validity of the batch length
        if (_batch_len == 0) {
            std::cerr << RED << "WindFlow Error: batch length in Win_MapReduce_GPU cannot be zero" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // general fastflow pointers to the MAP and REDUCE stages
        ff_node *map_stage, *reduce_stage;
        auto closing_func = [] (RuntimeContext &) { return; };
        // create the MAP phase
        if (_map_parallelism > 1) {
            // std::vector of Win_Seq
            std::vector<ff_node *> w(_map_parallelism);
            // create the Win_Seq
            for (size_t i = 0; i < _map_parallelism; i++) {
                // configuration structure of the Win_Seq (MAP)
                WinOperatorConfig configSeqMAP(_config.id_inner, _config.n_inner, _config.slide_inner, 0, 1, _slide_len);
                auto *seq = new Win_Seq<tuple_t, result_t, wrapper_in_t>(_map_func, _win_len, _slide_len, _triggering_delay, _winType, _name + "_map", closing_func, RuntimeContext(1, 0), configSeqMAP, role_t::MAP);
                seq->setMapIndexes(i, _map_parallelism);
                w[i] = seq;
                map_workers.push_back(seq);
            }
            ff::ff_farm *farm_map = new ff::ff_farm(w);
            farm_map->remove_collector();
            farm_map->add_collector(new map_collector_t());
            farm_map->add_emitter(new map_emitter_t(_map_parallelism, _winType));
            farm_map->cleanup_all();
            map_stage = farm_map;
        }
        else {
            // configuration structure of the Win_Seq_GPU (MAP)
            WinOperatorConfig configSeqMAP(_config.id_inner, _config.n_inner, _config.slide_inner, 0, 1, _slide_len);
            auto *seq_map = new Win_Seq<tuple_t, result_t, wrapper_in_t>(_map_func, _win_len, _slide_len, _triggering_delay, _winType, _name + "_map", closing_func, RuntimeContext(1, 0), configSeqMAP, role_t::MAP);
            seq_map->setMapIndexes(0, 1);
            map_stage = seq_map;
            map_workers.push_back(seq_map);
        }
        // create the REDUCE phase
        if (_reduce_parallelism > 1) {
            // configuration structure of the Win_Farm_GPU (REDUCE)
            WinOperatorConfig configWFREDUCE(_config.id_outer, _config.n_outer, _config.slide_outer, _config.id_inner, _config.n_inner, _config.slide_inner);
            auto *farm_reduce = new Win_Farm_GPU<result_t, result_t, F_t>(_gpuFunction, _map_parallelism, _map_parallelism, 0, win_type_t::CB, _reduce_parallelism, _batch_len, _gpu_id, _n_thread_block, _name + "_reduce", scratchpad_size, _ordered, opt_level_t::LEVEL0, configWFREDUCE, role_t::REDUCE);
            reduce_stage = farm_reduce;
            for (auto *w: farm_reduce->getWorkers()) {
                reduce_workers.push_back(w);
            }
        }
        else {
            // configuration structure of the Win_Seq_GPU (REDUCE)
            WinOperatorConfig configSeqREDUCE(_config.id_inner, _config.n_inner, _config.slide_inner, 0, 1, _map_parallelism);
            auto *seq_reduce = new Win_Seq_GPU<result_t, result_t, F_t>(_gpuFunction, _map_parallelism, _map_parallelism, 0, win_type_t::CB, _batch_len, _gpu_id, _n_thread_block, _name + "_reduce", scratchpad_size, configSeqREDUCE, role_t::REDUCE);
            reduce_stage = seq_reduce;
            reduce_workers.push_back(seq_reduce);
        }
        // add to this the pipeline optimized according to the provided optimization level
        ff::ff_pipeline::add_stage(optimize_WinMapReduceGPU(map_stage, reduce_stage, _opt_level));
        // when the Win_MapReduce_GPU will be destroyed we need aslo to destroy the two internal stages
        ff::ff_pipeline::cleanup_nodes();
        // flatten the pipeline
        ff::ff_pipeline::flatten();
    }

    // Private Constructor IV
    Win_MapReduce_GPU(mapupdate_func_t _mapupdate_func,
                      F_t _gpuFunction,
                      uint64_t _win_len,
                      uint64_t _slide_len,
                      uint64_t _triggering_delay,
                      win_type_t _winType,
                      size_t _map_parallelism,
                      size_t _reduce_parallelism,
                      size_t _batch_len,
                      int _gpu_id,
                      size_t _n_thread_block,
                      std::string _name,
                      size_t _scratchpad_size,
                      bool _ordered,
                      opt_level_t _opt_level,
                      WinOperatorConfig _config):
                      name(_name),
                      parallelism(_map_parallelism + _reduce_parallelism),
                      used(false),
                      used4Nesting(false),
                      winType(_winType),
                      mapupdate_func(_mapupdate_func),
                      gpuFunction(_gpuFunction),
                      isGPUMAP(false),
                      isGPUREDUCE(true),
                      isNICMAP(false),
                      isNICREDUCE(true),
                      win_len(_win_len),
                      slide_len(_slide_len),
                      triggering_delay(_triggering_delay),
                      map_parallelism(_map_parallelism),
                      reduce_parallelism(_reduce_parallelism),
                      batch_len(_batch_len),
                      gpu_id(_gpu_id),
                      n_thread_block(_n_thread_block),
                      scratchpad_size(_scratchpad_size),
                      ordered(_ordered),
                      opt_level(_opt_level),
                      config(_config)
    {
        // check the validity of the windowing parameters
        if (_win_len == 0 || _slide_len == 0) {
            std::cerr << RED << "WindFlow Error: window length or slide in Win_MapReduce_GPU cannot be zero" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // the Win_MapReduce_GPU must have a parallel MAP stage
        if (_map_parallelism < 2) {
            std::cerr << RED << "WindFlow Error: Win_MapReduce_GPU must have a parallel MAP stage" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // check the validity of the reduce parallelism value
        if (_reduce_parallelism == 0) {
            std::cerr << RED << "WindFlow Error: parallelism of the REDUCE stage cannot be zero" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // check the validity of the batch length
        if (_batch_len == 0) {
            std::cerr << RED << "WindFlow Error: batch length in Win_MapReduce_GPU cannot be zero" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // general fastflow pointers to the MAP and REDUCE stages
        ff_node *map_stage, *reduce_stage;
        auto closing_func = [] (RuntimeContext &) { return; };
        // create the MAP phase
        if (_map_parallelism > 1) {
            // std::vector of Win_Seq
            std::vector<ff_node *> w(_map_parallelism);
            // create the Win_Seq
            for (size_t i = 0; i < _map_parallelism; i++) {
                // configuration structure of the Win_Seq (MAP)
                WinOperatorConfig configSeqMAP(_config.id_inner, _config.n_inner, _config.slide_inner, 0, 1, _slide_len);
                auto *seq = new Win_Seq<tuple_t, result_t, wrapper_in_t>(_mapupdate_func, _win_len, _slide_len, _triggering_delay, _winType, _name + "_map", closing_func, RuntimeContext(1, 0), configSeqMAP, role_t::MAP);
                seq->setMapIndexes(i, _map_parallelism);
                w[i] = seq;
                map_workers.push_back(seq);
            }
            ff::ff_farm *farm_map = new ff::ff_farm(w);
            farm_map->remove_collector();
            farm_map->add_collector(new map_collector_t());
            farm_map->add_emitter(new map_emitter_t(_map_parallelism, _winType));
            farm_map->cleanup_all();
            map_stage = farm_map;
        }
        else {
            // configuration structure of the Win_Seq_GPU (MAP)
            WinOperatorConfig configSeqMAP(_config.id_inner, _config.n_inner, _config.slide_inner, 0, 1, _slide_len);
            auto *seq_map = new Win_Seq<tuple_t, result_t, wrapper_in_t>(_mapupdate_func, _win_len, _slide_len, _triggering_delay, _winType, _name + "_map", closing_func, RuntimeContext(1, 0), configSeqMAP, role_t::MAP);
            seq_map->setMapIndexes(0, 1);
            map_stage = seq_map;
            map_workers.push_back(seq_map);
        }
        // create the REDUCE phase
        if (_reduce_parallelism > 1) {
            // configuration structure of the Win_Farm_GPU (REDUCE)
            WinOperatorConfig configWFREDUCE(_config.id_outer, _config.n_outer, _config.slide_outer, _config.id_inner, _config.n_inner, _config.slide_inner);
            auto *farm_reduce = new Win_Farm_GPU<result_t, result_t, F_t>(_gpuFunction, _map_parallelism, _map_parallelism, 0, win_type_t::CB, _reduce_parallelism, _batch_len, _gpu_id, _n_thread_block, _name + "_reduce", scratchpad_size, _ordered, opt_level_t::LEVEL0, configWFREDUCE, role_t::REDUCE);
            reduce_stage = farm_reduce;
            for (auto *w: farm_reduce->getWorkers()) {
                reduce_workers.push_back(w);
            }
        }
        else {
            // configuration structure of the Win_Seq_GPU (REDUCE)
            WinOperatorConfig configSeqREDUCE(_config.id_inner, _config.n_inner, _config.slide_inner, 0, 1, _map_parallelism);
            auto *seq_reduce = new Win_Seq_GPU<result_t, result_t, F_t>(_gpuFunction, _map_parallelism, _map_parallelism, 0, win_type_t::CB, _batch_len, _gpu_id, _n_thread_block, _name + "_reduce", scratchpad_size, configSeqREDUCE, role_t::REDUCE);
            reduce_stage = seq_reduce;
            reduce_workers.push_back(seq_reduce);
        }
        // add to this the pipeline optimized according to the provided optimization level
        ff::ff_pipeline::add_stage(optimize_WinMapReduceGPU(map_stage, reduce_stage, _opt_level));
        // when the Win_MapReduce_GPU will be destroyed we need aslo to destroy the two internal stages
        ff::ff_pipeline::cleanup_nodes();
        // flatten the pipeline
        ff::ff_pipeline::flatten();
    }

    // method to optimize the structure of the Win_MapReduce_GPU operator
    const ff::ff_pipeline optimize_WinMapReduceGPU(ff_node *map, ff_node *reduce, opt_level_t opt)
    {
        if (opt == opt_level_t::LEVEL0) { // no optimization
            ff::ff_pipeline pipe;
            pipe.add_stage(map);
            pipe.add_stage(reduce);
            pipe.cleanup_nodes();
            return pipe;
        }
        else if (opt == opt_level_t::LEVEL1) { // optimization level 1
            return combine_nodes_in_pipeline(*map, *reduce, true, true);
        }
        else { // optimization level 2
            if (!map->isFarm() || !reduce->isFarm()) { // like level 1
                return combine_nodes_in_pipeline(*map, *reduce, true, true);
            }
            else {
                using emitter_reduce_t = WF_Emitter<result_t, result_t>;
                ff::ff_farm *farm_map = static_cast<ff::ff_farm *>(map);
                ff::ff_farm *farm_reduce = static_cast<ff::ff_farm *>(reduce);
                emitter_reduce_t *emitter_reduce = static_cast<emitter_reduce_t *>(farm_reduce->getEmitter());
                farm_reduce->cleanup_emitter(false);
                Ordering_Node<result_t, wrapper_tuple_t<result_t>> *buf_node = new Ordering_Node<result_t, wrapper_tuple_t<result_t>>();
                const ff::ff_pipeline result = combine_farms(*farm_map, emitter_reduce, *farm_reduce, buf_node, false);
                delete farm_map;
                delete farm_reduce;
                delete buf_node;
                delete emitter_reduce;
                return result;
            }
        }
    }

public:
    /** 
     *  \brief Constructor I
     *  
     *  \param _map_func the non-incremental map processing function (__host__ __device__ function)
     *  \param _reduce_func the non-incremental window reduce processing function (__host__ function)
     *  \param _win_len window length (in no. of tuples or in time units)
     *  \param _slide_len slide length (in no. of tuples or in time units)
     *  \param _triggering_delay (triggering delay in time units, meaningful for TB windows only otherwise it must be 0)
     *  \param _winType window type (count-based CB or time-based TB)
     *  \param _map_parallelism parallelism of the MAP stage
     *  \param _reduce_parallelism parallelism of the REDUCE stage
     *  \param _batch_len no. of window partitions in a batch
     *  \param _gpu_id identifier of the chosen GPU device
     *  \param _n_thread_block number of threads per block
     *  \param _name name of the operator
     *  \param _scratchpad_size size in bytes of the scratchpad area local of a CUDA thread (pre-allocated on the global memory of the GPU)
     *  \param _ordered true if the results of the same key must be emitted in order (default)
     *  \param _opt_level optimization level used to build the operator
     */ 
    Win_MapReduce_GPU(F_t _map_func,
                      reduce_func_t _reduce_func,
                      uint64_t _win_len,
                      uint64_t _slide_len,
                      uint64_t _triggering_delay,
                      win_type_t _winType,
                      size_t _map_parallelism,
                      size_t _reduce_parallelism,
                      size_t _batch_len,
                      int _gpu_id,
                      size_t _n_thread_block,
                      std::string _name,
                      size_t _scratchpad_size,
                      bool _ordered,
                      opt_level_t _opt_level):
                      Win_MapReduce_GPU(_map_func, _reduce_func, _win_len, _slide_len, _triggering_delay, _winType, _map_parallelism, _reduce_parallelism, _batch_len, _gpu_id, _n_thread_block, _name, _scratchpad_size, _ordered, _opt_level, WinOperatorConfig(0, 1, _slide_len, 0, 1, _slide_len)) {}

    /** 
     *  \brief Constructor II
     *  
     *  \param _map_func the non-incremental map processing function (__host__ __device__ function)
     *  \param _reduceupdate_func the incremental window reduce processing function (__host__ function)
     *  \param _win_len window length (in no. of tuples or in time units)
     *  \param _slide_len slide length (in no. of tuples or in time units)
     *  \param _triggering_delay (triggering delay in time units, meaningful for TB windows only otherwise it must be 0)
     *  \param _winType window type (count-based CB or time-based TB)
     *  \param _map_parallelism parallelism of the MAP stage
     *  \param _reduce_parallelism parallelism of the REDUCE stage
     *  \param _batch_len no. of window partitions in a batch
     *  \param _gpu_id identifier of the chosen GPU device
     *  \param _n_thread_block number of threads per block
     *  \param _name name of the operator
     *  \param _scratchpad_size size in bytes of the scratchpad area local of a CUDA thread (pre-allocated on the global memory of the GPU)
     *  \param _ordered true if the results of the same key must be emitted in order (default)
     *  \param _opt_level optimization level used to build the operator
     */ 
    Win_MapReduce_GPU(F_t _map_func,
                      reduceupdate_func_t _reduceupdate_func,
                      uint64_t _win_len,
                      uint64_t _slide_len,
                      uint64_t _triggering_delay,
                      win_type_t _winType,
                      size_t _map_parallelism,
                      size_t _reduce_parallelism,
                      size_t _batch_len,
                      int _gpu_id,
                      size_t _n_thread_block,
                      std::string _name,
                      size_t _scratchpad_size,
                      bool _ordered,
                      opt_level_t _opt_level):
                      Win_MapReduce_GPU(_map_func, _reduceupdate_func, _win_len, _slide_len, _triggering_delay, _winType, _map_parallelism, _reduce_parallelism, _batch_len, _gpu_id, _n_thread_block, _name, _scratchpad_size, _ordered, _opt_level, WinOperatorConfig(0, 1, _slide_len, 0, 1, _slide_len)) {}

    /** 
     *  \brief Constructor III
     *  
     *  \param _map_func the non-incremental map processing function (__host__ function)
     *  \param _reduce_func the non-incremental window reduce processing function (__host__ __device__ function)
     *  \param _win_len window length (in no. of tuples or in time units)
     *  \param _slide_len slide length (in no. of tuples or in time units)
     *  \param _triggering_delay (triggering delay in time units, meaningful for TB windows only otherwise it must be 0)
     *  \param _winType window type (count-based CB or time-based TB)
     *  \param _map_parallelism parallelism of the MAP stage
     *  \param _reduce_parallelism parallelism of the REDUCE stage
     *  \param _batch_len no. of window partitions in a batch
     *  \param _gpu_id identifier of the chosen GPU device
     *  \param _n_thread_block number of threads per block
     *  \param _name name of the operator
     *  \param _scratchpad_size size in bytes of the scratchpad area local of a CUDA thread (pre-allocated on the global memory of the GPU)
     *  \param _ordered true if the results of the same key must be emitted in order (default)
     *  \param _opt_level optimization level used to build the operator
     */ 
    Win_MapReduce_GPU(map_func_t _map_func,
                      F_t _reduce_func,
                      uint64_t _win_len,
                      uint64_t _slide_len,
                      uint64_t _triggering_delay,
                      win_type_t _winType,
                      size_t _map_parallelism,
                      size_t _reduce_parallelism,
                      size_t _batch_len,
                      int _gpu_id,
                      size_t _n_thread_block,
                      std::string _name,
                      size_t _scratchpad_size,
                      bool _ordered,
                      opt_level_t _opt_level):
                      Win_MapReduce_GPU(_map_func, _reduce_func, _win_len, _slide_len, _triggering_delay, _winType, _map_parallelism, _reduce_parallelism, _batch_len, _gpu_id, _n_thread_block, _name, _scratchpad_size, _ordered, _opt_level, WinOperatorConfig(0, 1, _slide_len, 0, 1, _slide_len)) {}

    /** 
     *  \brief Constructor IV
     *  
     *  \param _mapupdate_func the incremental map processing function (__host__ function)
     *  \param _reduceupdate_func the incremental window reduce processing function (__host__ __device__ function)
     *  \param _win_len window length (in no. of tuples or in time units)
     *  \param _slide_len slide length (in no. of tuples or in time units)
     *  \param _triggering_delay (triggering delay in time units, meaningful for TB windows only otherwise it must be 0)
     *  \param _winType window type (count-based CB or time-based TB)
     *  \param _map_parallelism parallelism of the MAP stage
     *  \param _reduce_parallelism parallelism of the REDUCE stage
     *  \param _batch_len no. of window partitions in a batch
     *  \param _gpu_id identifier of the chosen GPU device
     *  \param _n_thread_block number of threads per block
     *  \param _name name of the operator
     *  \param _scratchpad_size size in bytes of the scratchpad area local of a CUDA thread (pre-allocated on the global memory of the GPU)
     *  \param _ordered true if the results of the same key must be emitted in order (default)
     *  \param _opt_level optimization level used to build the operator
     */ 
    Win_MapReduce_GPU(mapupdate_func_t _mapupdate_func,
                      F_t _reduceupdate_func,
                      uint64_t _win_len,
                      uint64_t _slide_len,
                      uint64_t _triggering_delay,
                      win_type_t _winType,
                      size_t _map_parallelism,
                      size_t _reduce_parallelism,
                      size_t _batch_len,
                      int _gpu_id,
                      size_t _n_thread_block,
                      std::string _name,
                      size_t _scratchpad_size,
                      bool _ordered,
                      opt_level_t _opt_level):
                      Win_MapReduce_GPU(_mapupdate_func, _reduceupdate_func, _win_len, _slide_len, _triggering_delay, _winType, _map_parallelism, _reduce_parallelism, _batch_len, _gpu_id, _n_thread_block, _name, _scratchpad_size, _ordered, _opt_level, WinOperatorConfig(0, 1, _slide_len, 0, 1, _slide_len)) {}

    /** 
     *  \brief Check whether the Win_MapReduce_GPU has been used in a nested structure
     *  \return true if the Win_MapReduce_GPU has been used in a nested structure
     */
    bool isUsed4Nesting() const
    {
        return used4Nesting;
    }

    /** 
     *  \brief Get the optimization level used to build the Win_MapReduce_GPU
     *  \return adopted utilization level by the Win_MapReduce_GPU
     */ 
    opt_level_t getOptLevel() const
    {
      return opt_level;
    }

    /** 
     *  \brief Get the parallelism of the MAP stage
     *  \return MAP parallelism
     */ 
    size_t getMAPParallelism() const
    {
      return map_parallelism;
    }

    /** 
     *  \brief Get the parallelism of the REDUCE stage
     *  \return REDUCE parallelism
     */ 
    size_t getREDUCEParallelism() const
    {
      return reduce_parallelism;
    }

    /** 
     *  \brief Get the window type (CB or TB) utilized by the Pane_Farm_GPU
     *  \return adopted windowing semantics (count-based or time-based)
     */ 
    win_type_t getWinType() const
    {
        return winType;
    }

    /** 
     *  \brief Get the number of ignored tuples by the Win_MapReduce_GPU
     *  \return number of tuples ignored during the processing by the Win_MapReduce_GPU
     */ 
    size_t getNumIgnoredTuples() const
    {
        size_t count = 0;
        for (auto *w: map_workers) {
            if (isGPUMAP) {
                auto *seq_gpu = static_cast<Win_Seq_GPU<tuple_t, result_t, F_t, wrapper_in_t> *>(w);
                count += seq_gpu->getNumIgnoredTuples();
            }
            else {
                auto *seq = static_cast<Win_Seq<tuple_t, result_t, wrapper_in_t> *>(w);
                count += seq->getNumIgnoredTuples();
            }
        }
        return count;
    }

    /** 
     *  \brief Get the name of the Win_MapReduce_GPU
     *  \return name of the Win_MapReduce_GPU
     */ 
    std::string getName() const override
    {
        return name;
    }

    /** 
     *  \brief Get the total parallelism within the Win_MapReduce_GPU
     *  \return total parallelism within the Win_MapReduce_GPU
     */ 
    size_t getParallelism() const override
    {
        return parallelism;
    }

    /** 
     *  \brief Return the routing mode of inputs to the Win_MapReduce_GPU
     *  \return routing mode (always COMPLEX for the Win_MapReduce_GPU)
     */ 
    routing_modes_t getRoutingMode() const override
    {
        return routing_modes_t::COMPLEX;
    }

    /** 
     *  \brief Check whether the Win_MapReduce_GPU has been used in a MultiPipe
     *  \return true if the Win_MapReduce_GPU has been added/chained to an existing MultiPipe
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
        for (auto *w: map_workers) {
            if (isGPUMAP) {
                auto *seq_gpu = static_cast<Win_Seq_GPU<tuple_t, result_t, F_t, wrapper_in_t> *>(w);
                terminated = terminated && seq_gpu->isTerminated();
            }
            else {
                auto *seq = static_cast<Win_Seq<tuple_t, result_t, wrapper_in_t> *>(w);
                terminated = terminated && seq->isTerminated();
            }
        }
        for (auto *w: reduce_workers) {
            if (isGPUREDUCE) {
                auto *seq_gpu = static_cast<Win_Seq_GPU<result_t, result_t, F_t> *>(w);
                terminated = terminated && seq_gpu->isTerminated();
            }
            else {
                auto *seq = static_cast<Win_Seq<result_t, result_t> *>(w);
                terminated = terminated && seq->isTerminated();
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
        writer.String("Win_MapReduce_GPU");
        writer.Key("Distribution");
        writer.String("COMPLEX");
        writer.Key("isTerminated");
        writer.Bool(this->isTerminated());
        writer.Key("isGPU_1");
        if (!isGPUMAP) {
            writer.Bool(false);
        }
        else {
            writer.Bool(true);
        }
        writer.Key("Window_type_1");
        if (winType == win_type_t::CB) {
            writer.String("count-based");
        }
        else {
            writer.String("time-based");
            writer.Key("Window_delay");
            writer.Uint(triggering_delay);  
        }
        writer.Key("Name_Stage_1");
        writer.String("MAP");
        writer.Key("Window_length_1");
        writer.Uint(win_len);
        writer.Key("Window_slide_1");
        writer.Uint(slide_len);
        if (isGPUMAP) {
            writer.Key("Batch_len");
            writer.Uint(batch_len);
        }
        writer.Key("Parallelism_1");
        writer.Uint(map_parallelism);
        writer.Key("Replicas_1");
        writer.StartArray();
        // get statistics from all the replicas of the MAP stage
        for (auto *w: map_workers) {
            if (isGPUMAP) {
                auto *seq_gpu = static_cast<Win_Seq_GPU<tuple_t, result_t, F_t, wrapper_in_t> *>(w);
                Stats_Record record = seq_gpu->get_StatsRecord();
                record.append_Stats(writer);
            }
            else {
                auto *seq = static_cast<Win_Seq<tuple_t, result_t, wrapper_in_t> *>(w);
                Stats_Record record = seq->get_StatsRecord();
                record.append_Stats(writer);
            }
        }
        writer.EndArray();
        writer.Key("isGPU_2");
        if (!isGPUREDUCE) {
            writer.Bool(false);
        }
        else {
            writer.Bool(true);
        }
        writer.Key("Name_Stage_2");
        writer.String("Reduce");
        writer.Key("Window_type_2");
        writer.String("count-based");
        writer.Key("Window_length_2");
        writer.Uint(map_parallelism);
        writer.Key("Window_slide_2");
        writer.Uint(map_parallelism);
        if (isGPUREDUCE) {
            writer.Key("Batch_len");
            writer.Uint(batch_len);
        }
        writer.Key("Parallelism_2");
        writer.Uint(reduce_parallelism);
        writer.Key("Replicas_2");
        writer.StartArray();
        // get statistics from all the replicas of the REDUCE stage
        for (auto *w: reduce_workers) {
            if (isGPUREDUCE) {
                auto *seq_gpu = static_cast<Win_Seq_GPU<result_t, result_t, F_t> *>(w);
                Stats_Record record = seq_gpu->get_StatsRecord();
                record.append_Stats(writer);
            }
            else {
                auto *seq = static_cast<Win_Seq<result_t, result_t> *>(w);
                Stats_Record record = seq->get_StatsRecord();
                record.append_Stats(writer);
            }
        }
        writer.EndArray();      
        writer.EndObject();     
    }
#endif

    /// deleted constructors/operators
    Win_MapReduce_GPU(const Win_MapReduce_GPU &) = delete; // copy constructor
    Win_MapReduce_GPU(Win_MapReduce_GPU &&) = delete; // move constructor
    Win_MapReduce_GPU &operator=(const Win_MapReduce_GPU &) = delete; // copy assignment operator
    Win_MapReduce_GPU &operator=(Win_MapReduce_GPU &&) = delete; // move assignment operator
};

} // namespace wf

#endif
