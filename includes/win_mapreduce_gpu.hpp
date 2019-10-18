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
 *  @brief Win_MapReduce_GPU pattern executing a windowed transformation in
 *         parallel on a CPU+GPU system
 *  
 *  @section Win_MapReduce_GPU (Description)
 *  
 *  This file implements the Win_MapReduce_GPU pattern able to execute windowed
 *  queries on a heterogeneous system (CPU+GPU). The pattern processes (possibly
 *  in parallel) partitions of the windows in the so-called MAP stage, and computes
 *  (possibly in parallel) results of the windows out of the partition results in the
 *  REDUCE stage. The pattern allows the user to offload either the MAP or the REDUCE
 *  processing on the GPU while the other stage is executed on the CPU with either a
 *  non-incremental or an incremental query definition.
 *  
 *  The template parameters tuple_t and result_t must be default constructible, with a
 *  copy constructor and copy assignment operator, and they must provide and implement
 *  the setControlFields() and getControlFields() methods. The third template argument
 *  F_t is the type of the callable object to be used for GPU processing (either for
 *  the MAP or for the REDUCE stage).
 */ 

#ifndef WIN_MAPREDUCE_GPU_H
#define WIN_MAPREDUCE_GPU_H

/// includes
#include <ff/pipeline.hpp>
#include <ff/farm.hpp>
#include <win_farm.hpp>
#include <win_farm_gpu.hpp>
#include <basic.hpp>
#include <meta_utils.hpp>

namespace wf {

/** 
 *  \class Win_MapReduce_GPU
 *  
 *  \brief Win_MapReduce_GPU pattern executing a windowed transformation in parallel
 *         on a CPU+GPU system
 *  
 *  This class implements the Win_MapReduce_GPU pattern executing windowed queries in
 *  parallel on heterogeneous system (CPU+GPU). The pattern processes (possibly in parallel)
 *  window partitions in the MAP stage and builds window results out from partition results
 *  (possibly in parallel) in the REDUCE stage. Either the MAP or the REDUCE stage are executed
 *  on the GPU device while the others is executed on the CPU as in the Win_MapReduce pattern.
 */ 
template<typename tuple_t, typename result_t, typename F_t, typename input_t>
class Win_MapReduce_GPU: public ff::ff_pipeline
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
    template<typename T1, typename T2, typename T3>
    friend class Key_Farm_GPU;
    template<typename T>
    friend class WinFarmGPU_Builder;
    template<typename T>
    friend class KeyFarmGPU_Builder;
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
    win_type_t winType;
    size_t map_degree;
    size_t reduce_degree;
    size_t batch_len;
    size_t n_thread_block;
    std::string name;
    size_t scratchpad_size;
    bool ordered;
    opt_level_t opt_level;
    PatternConfig config;

    // Private Constructor I
    Win_MapReduce_GPU(F_t _gpuFunction,
                      reduce_func_t _reduce_func,
                      uint64_t _win_len,
                      uint64_t _slide_len,
                      win_type_t _winType,
                      size_t _map_degree,
                      size_t _reduce_degree,
                      size_t _batch_len,
                      size_t _n_thread_block,
                      std::string _name,
                      size_t _scratchpad_size,
                      bool _ordered,
                      opt_level_t _opt_level,
                      PatternConfig _config):
                      gpuFunction(_gpuFunction),
                      reduce_func(_reduce_func),
                      isGPUMAP(true),
                      isGPUREDUCE(false),
                      isNICMAP(true),
                      isNICREDUCE(true),
                      win_len(_win_len),
                      slide_len(_slide_len),
                      winType(_winType),
                      map_degree(_map_degree),
                      reduce_degree(_reduce_degree),
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
            std::cerr << RED << "WindFlow Error: window length or slide cannot be zero" << DEFAULT << std::endl;
            exit(EXIT_FAILURE);
        }
        // the Win_MapReduce_GPU must have a parallel MAP stage
        if (_map_degree < 2) {
            std::cerr << RED << "WindFlow Error: Win_MapReduce_GPU must have a parallel MAP stage" << DEFAULT << std::endl;
            exit(EXIT_FAILURE);
        }
        // check the validity of the reduce parallelism degree
        if (_reduce_degree == 0) {
            std::cerr << RED << "WindFlow Error: parallelism degree of the REDUCE cannot be zero" << DEFAULT << std::endl;
            exit(EXIT_FAILURE);
        }
        // check the validity of the batch length
        if (_batch_len == 0) {
            std::cerr << RED << "WindFlow Error: batch length cannot be zero" << DEFAULT << std::endl;
            exit(EXIT_FAILURE);
        }
        // general fastflow pointers to the MAP and REDUCE stages
        ff_node *map_stage, *reduce_stage;
        auto closing_func = [] (RuntimeContext &) { return; };
        // create the MAP phase
        if (_map_degree > 1) {
            // std::vector of Win_Seq_GPU
            std::vector<ff_node *> w(_map_degree);
            // create the Win_Seq_GPU
            for (size_t i = 0; i < _map_degree; i++) {
                // configuration structure of the Win_Seq_GPU (MAP)
                PatternConfig configSeqMAP(_config.id_inner, _config.n_inner, _config.slide_inner, 0, 1, _slide_len);
                auto *seq = new Win_Seq_GPU<tuple_t, result_t, F_t, wrapper_in_t>(_gpuFunction, _win_len, _slide_len, _winType, _batch_len, _n_thread_block, _name + "_map_wf", _scratchpad_size, configSeqMAP, MAP);
                seq->setMapIndexes(i, _map_degree);
                w[i] = seq;
            }
            ff::ff_farm *farm_map = new ff::ff_farm(w);
            farm_map->remove_collector();
            farm_map->add_collector(new map_collector_t());
            farm_map->add_emitter(new map_emitter_t(_map_degree, _winType));
            farm_map->cleanup_all();
            map_stage = farm_map;
        }
        else {
            // configuration structure of the Win_Seq_GPU (MAP)
            PatternConfig configSeqMAP(_config.id_inner, _config.n_inner, _config.slide_inner, 0, 1, _slide_len);
            auto *seq_map = new Win_Seq_GPU<tuple_t, result_t, F_t, wrapper_in_t>(_gpuFunction, _win_len, _slide_len, _winType, _batch_len, _n_thread_block, _name + "_map", _scratchpad_size, configSeqMAP, MAP);
            seq_map->setMapIndexes(0, 1);
            map_stage = seq_map;
        }
        // create the REDUCE phase
        if (_reduce_degree > 1) {
            // configuration structure of the Win_Farm (REDUCE)
            PatternConfig configWFREDUCE(_config.id_outer, _config.n_outer, _config.slide_outer, _config.id_inner, _config.n_inner, _config.slide_inner);
            auto *farm_reduce = new Win_Farm<result_t, result_t>(_reduce_func, _map_degree, _map_degree, CB, _reduce_degree, _name + "_reduce", closing_func, _ordered, LEVEL0, configWFREDUCE, REDUCE);
            reduce_stage = farm_reduce;
        }
        else {
            // configuration structure of the Win_Seq (REDUCE)
            PatternConfig configSeqREDUCE(_config.id_inner, _config.n_inner, _config.slide_inner, 0, 1, _map_degree);
            auto *seq_reduce = new Win_Seq<result_t, result_t>(_reduce_func, _map_degree, _map_degree, CB, _name + "_reduce", closing_func, RuntimeContext(1, 0), configSeqREDUCE, REDUCE);
            reduce_stage = seq_reduce;
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
                      win_type_t _winType,
                      size_t _map_degree,
                      size_t _reduce_degree,
                      size_t _batch_len,
                      size_t _n_thread_block,
                      std::string _name,
                      size_t _scratchpad_size,
                      bool _ordered,
                      opt_level_t _opt_level,
                      PatternConfig _config):
                      gpuFunction(_gpuFunction),
                      reduceupdate_func(_reduceupdate_func),
                      isGPUMAP(true),
                      isGPUREDUCE(false),
                      isNICMAP(true),
                      isNICREDUCE(false),
                      win_len(_win_len),
                      slide_len(_slide_len),
                      winType(_winType),
                      map_degree(_map_degree),
                      reduce_degree(_reduce_degree),
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
            std::cerr << RED << "WindFlow Error: window length or slide cannot be zero" << DEFAULT << std::endl;
            exit(EXIT_FAILURE);
        }
        // the Win_MapReduce_GPU must have a parallel MAP stage
        if (_map_degree < 2) {
            std::cerr << RED << "WindFlow Error: Win_MapReduce_GPU must have a parallel MAP stage" << DEFAULT << std::endl;
            exit(EXIT_FAILURE);
        }
        // check the validity of the reduce parallelism degree
        if (_reduce_degree == 0) {
            std::cerr << RED << "WindFlow Error: parallelism degree of the REDUCE cannot be zero" << DEFAULT << std::endl;
            exit(EXIT_FAILURE);
        }
        // check the validity of the batch length
        if (_batch_len == 0) {
            std::cerr << RED << "WindFlow Error: batch length cannot be zero" << DEFAULT << std::endl;
            exit(EXIT_FAILURE);
        }
        // general fastflow pointers to the MAP and REDUCE stages
        ff_node *map_stage, *reduce_stage;
        auto closing_func = [] (RuntimeContext &) { return; };
        // create the MAP phase
        if (_map_degree > 1) {
            // std::vector of Win_Seq_GPU
            std::vector<ff_node *> w(_map_degree);
            // create the Win_Seq_GPU
            for (size_t i = 0; i < _map_degree; i++) {
                // configuration structure of the Win_Seq_GPU (MAP)
                PatternConfig configSeqMAP(_config.id_inner, _config.n_inner, _config.slide_inner, 0, 1, _slide_len);
                auto *seq = new Win_Seq_GPU<tuple_t, result_t, F_t, wrapper_in_t>(_gpuFunction, _win_len, _slide_len, _winType, _batch_len, _n_thread_block, _name + "_map_wf", _scratchpad_size, configSeqMAP, MAP);
                seq->setMapIndexes(i, _map_degree);
                w[i] = seq;
            }
            ff::ff_farm *farm_map = new ff::ff_farm(w);
            farm_map->remove_collector();
            farm_map->add_collector(new map_collector_t());
            farm_map->add_emitter(new map_emitter_t(_map_degree, _winType));
            farm_map->cleanup_all();
            map_stage = farm_map;
        }
        else {
            // configuration structure of the Win_Seq_GPU (MAP)
            PatternConfig configSeqMAP(_config.id_inner, _config.n_inner, _config.slide_inner, 0, 1, _slide_len);
            auto *seq_map = new Win_Seq_GPU<tuple_t, result_t, F_t, wrapper_in_t>(_gpuFunction, _win_len, _slide_len, _winType, _batch_len, _n_thread_block, _name + "_map", _scratchpad_size, configSeqMAP, MAP);
            seq_map->setMapIndexes(0, 1);
            map_stage = seq_map;
        }
        // create the REDUCE phase
        if (_reduce_degree > 1) {
            // configuration structure of the Win_Farm (REDUCE)
            PatternConfig configWFREDUCE(_config.id_outer, _config.n_outer, _config.slide_outer, _config.id_inner, _config.n_inner, _config.slide_inner);
            auto *farm_reduce = new Win_Farm<result_t, result_t>(_reduceupdate_func, _map_degree, _map_degree, CB, _reduce_degree, _name + "_reduce", closing_func, _ordered, LEVEL0, configWFREDUCE, REDUCE);
            reduce_stage = farm_reduce;
        }
        else {
            // configuration structure of the Win_Seq (REDUCE)
            PatternConfig configSeqREDUCE(_config.id_inner, _config.n_inner, _config.slide_inner, 0, 1, _map_degree);
            auto *seq_reduce = new Win_Seq<result_t, result_t>(_reduceupdate_func, _map_degree, _map_degree, CB, _name + "_reduce", closing_func, RuntimeContext(1, 0), configSeqREDUCE, REDUCE);
            reduce_stage = seq_reduce;
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
                      win_type_t _winType,
                      size_t _map_degree,
                      size_t _reduce_degree,
                      size_t _batch_len,
                      size_t _n_thread_block,
                      std::string _name,
                      size_t _scratchpad_size,
                      bool _ordered,
                      opt_level_t _opt_level,
                      PatternConfig _config):
                      map_func(_map_func),
                      gpuFunction(_gpuFunction),
                      isGPUMAP(false),
                      isGPUREDUCE(true),
                      isNICMAP(true),
                      isNICREDUCE(true),
                      win_len(_win_len),
                      slide_len(_slide_len),
                      winType(_winType),
                      map_degree(_map_degree),
                      reduce_degree(_reduce_degree),
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
            std::cerr << RED << "WindFlow Error: window length or slide cannot be zero" << DEFAULT << std::endl;
            exit(EXIT_FAILURE);
        }
        // the Win_MapReduce_GPU must have a parallel MAP stage
        if (_map_degree < 2) {
            std::cerr << RED << "WindFlow Error: Win_MapReduce_GPU must have a parallel MAP stage" << DEFAULT << std::endl;
            exit(EXIT_FAILURE);
        }
        // check the validity of the reduce parallelism degree
        if (_reduce_degree == 0) {
            std::cerr << RED << "WindFlow Error: parallelism degree of the REDUCE cannot be zero" << DEFAULT << std::endl;
            exit(EXIT_FAILURE);
        }
        // check the validity of the batch length
        if (_batch_len == 0) {
            std::cerr << RED << "WindFlow Error: batch length cannot be zero" << DEFAULT << std::endl;
            exit(EXIT_FAILURE);
        }
        // general fastflow pointers to the MAP and REDUCE stages
        ff_node *map_stage, *reduce_stage;
        auto closing_func = [] (RuntimeContext &) { return; };
        // create the MAP phase
        if (_map_degree > 1) {
            // std::vector of Win_Seq
            std::vector<ff_node *> w(_map_degree);
            // create the Win_Seq
            for (size_t i = 0; i < _map_degree; i++) {
                // configuration structure of the Win_Seq (MAP)
                PatternConfig configSeqMAP(_config.id_inner, _config.n_inner, _config.slide_inner, 0, 1, _slide_len);
                auto *seq = new Win_Seq<tuple_t, result_t, wrapper_in_t>(_map_func, _win_len, _slide_len, _winType, _name + "_map_wf", closing_func, RuntimeContext(1, 0), configSeqMAP, MAP);
                seq->setMapIndexes(i, _map_degree);
                w[i] = seq;
            }
            ff::ff_farm *farm_map = new ff::ff_farm(w);
            farm_map->remove_collector();
            farm_map->add_collector(new map_collector_t());
            farm_map->add_emitter(new map_emitter_t(_map_degree, _winType));
            farm_map->cleanup_all();
            map_stage = farm_map;
        }
        else {
            // configuration structure of the Win_Seq_GPU (MAP)
            PatternConfig configSeqMAP(_config.id_inner, _config.n_inner, _config.slide_inner, 0, 1, _slide_len);
            auto *seq_map = new Win_Seq<tuple_t, result_t, wrapper_in_t>(_map_func, _win_len, _slide_len, _winType, _name + "_map", closing_func, RuntimeContext(1, 0), configSeqMAP, MAP);
            seq_map->setMapIndexes(0, 1);
            map_stage = seq_map;
        }
        // create the REDUCE phase
        if (_reduce_degree > 1) {
            // configuration structure of the Win_Farm_GPU (REDUCE)
            PatternConfig configWFREDUCE(_config.id_outer, _config.n_outer, _config.slide_outer, _config.id_inner, _config.n_inner, _config.slide_inner);
            auto *farm_reduce = new Win_Farm_GPU<result_t, result_t, F_t>(_gpuFunction, _map_degree, _map_degree, CB, _reduce_degree, _batch_len, _n_thread_block, _name + "_reduce", scratchpad_size, _ordered, LEVEL0, configWFREDUCE, REDUCE);
            reduce_stage = farm_reduce;
        }
        else {
            // configuration structure of the Win_Seq_GPU (REDUCE)
            PatternConfig configSeqREDUCE(_config.id_inner, _config.n_inner, _config.slide_inner, 0, 1, _map_degree);
            auto *seq_reduce = new Win_Seq_GPU<result_t, result_t, F_t>(_gpuFunction, _map_degree, _map_degree, CB, _batch_len, _n_thread_block, _name + "_reduce", scratchpad_size, configSeqREDUCE, REDUCE);
            reduce_stage = seq_reduce;
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
                      win_type_t _winType,
                      size_t _map_degree,
                      size_t _reduce_degree,
                      size_t _batch_len,
                      size_t _n_thread_block,
                      std::string _name,
                      size_t _scratchpad_size,
                      bool _ordered,
                      opt_level_t _opt_level,
                      PatternConfig _config):
                      mapupdate_func(_mapupdate_func),
                      gpuFunction(_gpuFunction),
                      isGPUMAP(false),
                      isGPUREDUCE(true),
                      isNICMAP(false),
                      isNICREDUCE(true),
                      win_len(_win_len),
                      slide_len(_slide_len),
                      winType(_winType),
                      map_degree(_map_degree),
                      reduce_degree(_reduce_degree),
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
            std::cerr << RED << "WindFlow Error: window length or slide cannot be zero" << DEFAULT << std::endl;
            exit(EXIT_FAILURE);
        }
        // the Win_MapReduce_GPU must have a parallel MAP stage
        if (_map_degree < 2) {
            std::cerr << RED << "WindFlow Error: Win_MapReduce_GPU must have a parallel MAP stage" << DEFAULT << std::endl;
            exit(EXIT_FAILURE);
        }
        // check the validity of the reduce parallelism degree
        if (_reduce_degree == 0) {
            std::cerr << RED << "WindFlow Error: parallelism degree of the REDUCE cannot be zero" << DEFAULT << std::endl;
            exit(EXIT_FAILURE);
        }
        // check the validity of the batch length
        if (_batch_len == 0) {
            std::cerr << RED << "WindFlow Error: batch length cannot be zero" << DEFAULT << std::endl;
            exit(EXIT_FAILURE);
        }
        // general fastflow pointers to the MAP and REDUCE stages
        ff_node *map_stage, *reduce_stage;
        auto closing_func = [] (RuntimeContext &) { return; };
        // create the MAP phase
        if (_map_degree > 1) {
            // std::vector of Win_Seq
            std::vector<ff_node *> w(_map_degree);
            // create the Win_Seq
            for (size_t i = 0; i < _map_degree; i++) {
                // configuration structure of the Win_Seq (MAP)
                PatternConfig configSeqMAP(_config.id_inner, _config.n_inner, _config.slide_inner, 0, 1, _slide_len);
                auto *seq = new Win_Seq<tuple_t, result_t, wrapper_in_t>(_mapupdate_func, _win_len, _slide_len, _winType, _name + "_map_wf", closing_func, RuntimeContext(1, 0), configSeqMAP, MAP);
                seq->setMapIndexes(i, _map_degree);
                w[i] = seq;
            }
            ff::ff_farm *farm_map = new ff::ff_farm(w);
            farm_map->remove_collector();
            farm_map->add_collector(new map_collector_t());
            farm_map->add_emitter(new map_emitter_t(_map_degree, _winType));
            farm_map->cleanup_all();
            map_stage = farm_map;
        }
        else {
            // configuration structure of the Win_Seq_GPU (MAP)
            PatternConfig configSeqMAP(_config.id_inner, _config.n_inner, _config.slide_inner, 0, 1, _slide_len);
            auto *seq_map = new Win_Seq<tuple_t, result_t, wrapper_in_t>(_mapupdate_func, _win_len, _slide_len, _winType, _name + "_map", closing_func, RuntimeContext(1, 0), configSeqMAP, MAP);
            seq_map->setMapIndexes(0, 1);
            map_stage = seq_map;
        }
        // create the REDUCE phase
        if (_reduce_degree > 1) {
            // configuration structure of the Win_Farm_GPU (REDUCE)
            PatternConfig configWFREDUCE(_config.id_outer, _config.n_outer, _config.slide_outer, _config.id_inner, _config.n_inner, _config.slide_inner);
            auto *farm_reduce = new Win_Farm_GPU<result_t, result_t, F_t>(_gpuFunction, _map_degree, _map_degree, CB, _reduce_degree, _batch_len, _n_thread_block, _name + "_reduce", scratchpad_size, _ordered, LEVEL0, configWFREDUCE, REDUCE);
            reduce_stage = farm_reduce;
        }
        else {
            // configuration structure of the Win_Seq_GPU (REDUCE)
            PatternConfig configSeqREDUCE(_config.id_inner, _config.n_inner, _config.slide_inner, 0, 1, _map_degree);
            auto *seq_reduce = new Win_Seq_GPU<result_t, result_t, F_t>(_gpuFunction, _map_degree, _map_degree, CB, _batch_len, _n_thread_block, _name + "_reduce", scratchpad_size, configSeqREDUCE, REDUCE);
            reduce_stage = seq_reduce;
        }
        // add to this the pipeline optimized according to the provided optimization level
        ff::ff_pipeline::add_stage(optimize_WinMapReduceGPU(map_stage, reduce_stage, _opt_level));
        // when the Win_MapReduce_GPU will be destroyed we need aslo to destroy the two internal stages
        ff::ff_pipeline::cleanup_nodes();
        // flatten the pipeline
        ff::ff_pipeline::flatten();
    }

    // method to optimize the structure of the Win_MapReduce_GPU pattern
    const ff::ff_pipeline optimize_WinMapReduceGPU(ff_node *map, ff_node *reduce, opt_level_t opt)
    {
        if (opt == LEVEL0) { // no optimization
            ff::ff_pipeline pipe;
            pipe.add_stage(map);
            pipe.add_stage(reduce);
            pipe.cleanup_nodes();
            return pipe;
        }
        else if (opt == LEVEL1) { // optimization level 1
            return combine_nodes_in_pipeline(*map, *reduce, true, true);
        }
        else { // optimization level 2
            if (!map->isFarm() || !reduce->isFarm()) // like level 1
                return combine_nodes_in_pipeline(*map, *reduce, true, true);
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
     *  \param _map_func the non-incremental map processing function (CPU/GPU function)
     *  \param _reduce_func the non-incremental window reduce processing function (CPU function)
     *  \param _win_len window length (in no. of tuples or in time units)
     *  \param _slide_len slide length (in no. of tuples or in time units)
     *  \param _winType window type (count-based CB or time-based TB)
     *  \param _map_degree parallelism degree of the MAP stage
     *  \param _reduce_degree parallelism degree of the REDUCE stage
     *  \param _batch_len no. of window partitions in a batch (i.e. 1 window patition mapped onto 1 CUDA thread)
     *  \param _n_thread_block number of threads (i.e. window patitions) per block
     *  \param _name std::string with the unique name of the pattern
     *  \param _scratchpad_size size in bytes of the scratchpad area per CUDA thread (on the GPU) 
     *  \param _ordered true if the results of the same key must be emitted in order (default)
     *  \param _opt_level optimization level used to build the pattern
     */ 
    Win_MapReduce_GPU(F_t _map_func,
                      reduce_func_t _reduce_func,
                      uint64_t _win_len,
                      uint64_t _slide_len,
                      win_type_t _winType,
                      size_t _map_degree,
                      size_t _reduce_degree,
                      size_t _batch_len,
                      size_t _n_thread_block,
                      std::string _name,
                      size_t _scratchpad_size,
                      bool _ordered,
                      opt_level_t _opt_level):
                      Win_MapReduce_GPU(_map_func, _reduce_func, _win_len, _slide_len, _winType, _map_degree, _reduce_degree, _batch_len, _n_thread_block, _name, _scratchpad_size, _ordered, _opt_level, PatternConfig(0, 1, _slide_len, 0, 1, _slide_len))
    {}

    /** 
     *  \brief Constructor II
     *  
     *  \param _map_func the non-incremental map processing function (CPU/GPU function)
     *  \param _reduceupdate_func the incremental window reduce processing function (CPU function)
     *  \param _win_len window length (in no. of tuples or in time units)
     *  \param _slide_len slide length (in no. of tuples or in time units)
     *  \param _winType window type (count-based CB or time-based TB)
     *  \param _map_degree parallelism degree of the MAP stage
     *  \param _reduce_degree parallelism degree of the REDUCE stage
     *  \param _batch_len no. of window partitions in a batch (i.e. 1 window patition mapped onto 1 CUDA thread)
     *  \param _n_thread_block number of threads (i.e. window patitions) per block
     *  \param _name std::string with the unique name of the pattern
     *  \param _scratchpad_size size in bytes of the scratchpad area per CUDA thread (on the GPU)
     *  \param _ordered true if the results of the same key must be emitted in order (default)
     *  \param _opt_level optimization level used to build the pattern
     */ 
    Win_MapReduce_GPU(F_t _map_func,
                      reduceupdate_func_t _reduceupdate_func,
                      uint64_t _win_len,
                      uint64_t _slide_len,
                      win_type_t _winType,
                      size_t _map_degree,
                      size_t _reduce_degree,
                      size_t _batch_len,
                      size_t _n_thread_block,
                      std::string _name,
                      size_t _scratchpad_size,
                      bool _ordered,
                      opt_level_t _opt_level):
                      Win_MapReduce_GPU(_map_func, _reduceupdate_func, _win_len, _slide_len, _winType, _map_degree, _reduce_degree, _batch_len, _n_thread_block, _name, _scratchpad_size, _ordered, _opt_level, PatternConfig(0, 1, _slide_len, 0, 1, _slide_len))
    {}

    /** 
     *  \brief Constructor III
     *  
     *  \param _map_func the non-incremental map processing function (CPU function)
     *  \param _reduce_func the non-incremental window reduce processing function (CPU/GPU function)
     *  \param _win_len window length (in no. of tuples or in time units)
     *  \param _slide_len slide length (in no. of tuples or in time units)
     *  \param _winType window type (count-based CB or time-based TB)
     *  \param _map_degree parallelism degree of the MAP stage
     *  \param _reduce_degree parallelism degree of the REDUCE stage
     *  \param _batch_len no. of window partitions in a batch (i.e. 1 window patition mapped onto 1 CUDA thread)
     *  \param _n_thread_block number of threads (i.e. window patitions) per block
     *  \param _name std::string with the unique name of the pattern
     *  \param _scratchpad_size size in bytes of the scratchpad area per CUDA thread (on the GPU)
     *  \param _ordered true if the results of the same key must be emitted in order (default)
     *  \param _opt_level optimization level used to build the pattern
     */ 
    Win_MapReduce_GPU(map_func_t _map_func,
                      F_t _reduce_func,
                      uint64_t _win_len,
                      uint64_t _slide_len,
                      win_type_t _winType,
                      size_t _map_degree,
                      size_t _reduce_degree,
                      size_t _batch_len,
                      size_t _n_thread_block,
                      std::string _name,
                      size_t _scratchpad_size,
                      bool _ordered,
                      opt_level_t _opt_level):
                      Win_MapReduce_GPU(_map_func, _reduce_func, _win_len, _slide_len, _winType, _map_degree, _reduce_degree, _batch_len, _n_thread_block, _name, _scratchpad_size, _ordered, _opt_level, PatternConfig(0, 1, _slide_len, 0, 1, _slide_len))
    {}

    /** 
     *  \brief Constructor IV
     *  
     *  \param _mapupdate_func the incremental map processing function (CPU function)
     *  \param _reduceupdate_func the incremental window reduce processing function (CPU/GPU function)
     *  \param _win_len window length (in no. of tuples or in time units)
     *  \param _slide_len slide length (in no. of tuples or in time units)
     *  \param _winType window type (count-based CB or time-based TB)
     *  \param _map_degree parallelism degree of the MAP stage
     *  \param _reduce_degree parallelism degree of the REDUCE stage
     *  \param _batch_len no. of window partitions in a batch (i.e. 1 window patition mapped onto 1 CUDA thread)
     *  \param _n_thread_block number of threads (i.e. window patitions) per block
     *  \param _name std::string with the unique name of the pattern
     *  \param _scratchpad_size size in bytes of the scratchpad area per CUDA thread (on the GPU)
     *  \param _ordered true if the results of the same key must be emitted in order (default)
     *  \param _opt_level optimization level used to build the pattern
     */ 
    Win_MapReduce_GPU(mapupdate_func_t _mapupdate_func,
                      F_t _reduceupdate_func,
                      uint64_t _win_len,
                      uint64_t _slide_len,
                      win_type_t _winType,
                      size_t _map_degree,
                      size_t _reduce_degree,
                      size_t _batch_len,
                      size_t _n_thread_block,
                      std::string _name,
                      size_t _scratchpad_size,
                      bool _ordered,
                      opt_level_t _opt_level):
                      Win_MapReduce_GPU(_mapupdate_func, _reduceupdate_func, _win_len, _slide_len, _winType, _map_degree, _reduce_degree, _batch_len, _n_thread_block, _name, _scratchpad_size, _ordered, _opt_level, PatternConfig(0, 1, _slide_len, 0, 1, _slide_len))
    {}

    /** 
     *  \brief Get the optimization level used to build the pattern
     *  \return adopted utilization level by the pattern
     */ 
    opt_level_t getOptLevel() const
    {
      return opt_level;
    }

    /** 
     *  \brief Get the window type (CB or TB) utilized by the pattern
     *  \return adopted windowing semantics (count- or time-based)
     */ 
    win_type_t getWinType() const
    {
      return winType;
    }

    /** 
     *  \brief Get the parallelism degree of the MAP stage
     *  \return MAP parallelism degree
     */ 
    size_t getMAPParallelism() const
    {
      return map_degree;
    }

    /** 
     *  \brief Get the parallelism degree of the REDUCE stage
     *  \return REDUCE parallelism degree
     */ 
    size_t getREDUCEParallelism() const
    {
      return reduce_degree;
    }
};

} // namespace wf

#endif
