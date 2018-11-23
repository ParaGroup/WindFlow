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
 *  @version 1.0
 *  
 *  @brief Win_MapReduce_GPU pattern executing windowed queries on a heterogeneous system (CPU+GPU)
 *  
 *  @section DESCRIPTION
 *  
 *  This file implements the Win_MapReduce_GPU pattern able to execute windowed queries on a heterogeneous
 *  system (CPU+GPU). The pattern processes (possibly in parallel) partitions of the windows in the so-called
 *  MAP stage, and computes (possibly in parallel) results of the windows out of the partition results in the
 *  REDUCE stage. The pattern allows the user to offload either the MAP or the REDUCE processing on the GPU
 *  while the other stage is executed on the CPU with either a non-incremental or an incremental query
 *  definition.
 */ 

#ifndef WIN_MAPREDUCE_GPU_H
#define WIN_MAPREDUCE_GPU_H

// includes
#include <ff/combine.hpp>
#include <ff/pipeline.hpp>
#include <win_farm.hpp>
#include <wm_nodes.hpp>
#include <win_farm_gpu.hpp>
#include <orderingNode.hpp>

/** 
 *  \class Win_MapReduce_GPU
 *  
 *  \brief Win_MapReduce_GPU pattern executing windowed queries on a heterogeneous system (CPU+GPU)
 *  
 *  This class implements the Win_MapReduce_GPU pattern executing windowed queries in parallel on
 *  heterogeneous system (CPU+GPU). The pattern processes (possibly in parallel) window partitions
 *  in the MAP stage and builds window results out from partition results (possibly in parallel) in
 *  the REDUCE stage. Either the MAP or the REDUCE stage are executed on the GPU device while the
 *  others is executed on the CPU as in the Win_MapReduce pattern. The pattern class has four
 *  template arguments. The first is the type of the input tuples. It must be copyable and providing
 *  the getInfo() and setInfo() methods. The second is the type of the window results. It must have
 *  a default constructor and the getInfo() and setInfo() methods. The first and the second types
 *  must be POD C++ types (Plain Old Data). The third template argument is the type of the function
 *  to process per partition/window. It must be declared in order to be executable both on the device
 *  (GPU) and on the CPU. The last template argument is used by the WindFlow run-time system and should
 *  never be utilized by the high-level programmer.
 */ 
template<typename tuple_t, typename result_t, typename F_t, typename input_t>
class Win_MapReduce_GPU: public ff_pipeline
{
private:
    // type of the wrapper of input tuples
    using wrapper_in_t = wrapper_tuple_t<tuple_t>;  
    // function type of the non-incremental MAP processing
    using f_mapfunction_t = function<int(size_t, uint64_t, Iterable<tuple_t> &, result_t &)>;
    // function type of the incremental MAP processing
    using f_mapupdate_t = function<int(size_t, uint64_t, const tuple_t &, result_t &)>;
    // function type of the non-incremental REDUCE processing
    using f_reducefunction_t = function<int(size_t, uint64_t, Iterable<result_t> &, result_t &)>;
    // function type of the incremental REDUCE processing
    using f_reduceupdate_t = function<int(size_t, uint64_t, const result_t &, result_t &)>;
    // type of the MAP_Emitter node
    using map_emitter_t = MAP_Emitter<tuple_t, input_t>;
    // type of the MAP_Collector node
    using map_collector_t = MAP_Collector<result_t>; 
    /// friendships with other classes in the library
    template<typename T1, typename T2, typename T3, typename T4>
    friend class Win_Farm_GPU;
    template<typename T1, typename T2, typename T3, typename T4>
    friend class Key_Farm_GPU;
    template<typename T>
    friend class WinFarmGPU_Builder;
    template<typename T>
    friend class KeyFarmGPU_Builder;
    // configuration variables of the Win_MapReduce_GPU
    F_t gpuFunction;
    f_mapfunction_t mapFunction;
    f_mapupdate_t mapUpdate;
    f_reducefunction_t reduceFunction;
    f_reduceupdate_t reduceUpdate;
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
    string name;
    size_t scratchpad_size;
    bool ordered;
    opt_level_t opt_level;
    PatternConfig config;

    // private constructor I (MAP stage on the GPU and REDUCE stage on the CPU with non-incremental query definition)
    Win_MapReduce_GPU(F_t _gpuFunction,
                      f_reducefunction_t _reduceFunction,
                      uint64_t _win_len,
                      uint64_t _slide_len,
                      win_type_t _winType,
                      size_t _map_degree,
                      size_t _reduce_degree,
                      size_t _batch_len,
                      size_t _n_thread_block,
                      string _name,
                      size_t _scratchpad_size,
                      bool _ordered,
                      opt_level_t _opt_level,
                      PatternConfig _config)
                      :
                      gpuFunction(_gpuFunction),
                      reduceFunction(_reduceFunction),
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
            cerr << RED << "WindFlow Error: window length or slide cannot be zero" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // the Win_MapReduce_GPU must have a parallel MAP stage
        if(_map_degree < 2) {
            cerr << RED << "WindFlow Error: Win_MapReduce_GPU must have a parallel MAP stage" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // check the validity of the reduce parallelism degree
        if (_reduce_degree == 0) {
            cerr << RED << "WindFlow Error: reduce parallelism degree cannot be zero" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // check the validity of the batch length
        if (_batch_len == 0) {
            cerr << RED << "WindFlow Error: batch length cannot be zero" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // general fastflow pointers to the MAP and REDUCE stages
        ff_node *map_stage, *reduce_stage;
        // create the MAP phase
        if (_map_degree > 1) {
            // vector of Win_Seq_GPU instances
            vector<ff_node *> w(_map_degree);
            // create the Win_Seq_GPU instances
            for (size_t i = 0; i < _map_degree; i++) {
                // configuration structure of the Win_Seq_GPU instance (MAP)
                PatternConfig configSeqMAP(_config.id_inner, _config.n_inner, _config.slide_inner, 0, 1, _slide_len);
                auto *seq = new Win_Seq_GPU<tuple_t, result_t, F_t, wrapper_in_t>(_gpuFunction, _win_len, _slide_len, _winType, _batch_len, _n_thread_block, _name + "_map_wf", _scratchpad_size, configSeqMAP, MAP);
                seq->setMapIndexes(i, _map_degree);
                w[i] = seq;
            }
            ff_farm *farm_map = new ff_farm(w);
            farm_map->remove_collector();
            farm_map->add_collector(new map_collector_t());
            farm_map->add_emitter(new map_emitter_t(_map_degree));
            farm_map->cleanup_all();
            map_stage = farm_map;
        }
        else {
            // configuration structure of the Win_Seq_GPU instance (MAP)
            PatternConfig configSeqMAP(_config.id_inner, _config.n_inner, _config.slide_inner, 0, 1, _slide_len);
            auto *seq_map = new Win_Seq_GPU<tuple_t, result_t, F_t, wrapper_in_t>(_gpuFunction, _win_len, _slide_len, _winType, _batch_len, _n_thread_block, _name + "_map", _scratchpad_size, configSeqMAP, MAP);
            seq_map->setMapIndexes(0, 1);
            map_stage = seq_map;
        }
        // create the REDUCE phase
        if (_reduce_degree > 1) {
            // configuration structure of the Win_Farm instance (REDUCE)
            PatternConfig configWFREDUCE(_config.id_outer, _config.n_outer, _config.slide_outer, _config.id_inner, _config.n_inner, _config.slide_inner);
            auto *farm_reduce = new Win_Farm<result_t, result_t>(_reduceFunction, _map_degree, _map_degree, CB, 1, _reduce_degree, _name + "_reduce", _ordered, configWFREDUCE, REDUCE);
            reduce_stage = farm_reduce;
        }
        else {
            // configuration structure of the Win_Seq instance (REDUCE)
            PatternConfig configSeqREDUCE(_config.id_inner, _config.n_inner, _config.slide_inner, 0, 1, _map_degree);
            auto *seq_reduce = new Win_Seq<result_t, result_t>(_reduceFunction, _map_degree, _map_degree, CB, _name + "_reduce", configSeqREDUCE, REDUCE);
            reduce_stage = seq_reduce;
        }
        // add to this the pipeline optimized according to the provided optimization level
        ff_pipeline::add_stage(optimize_WinMapReduceGPU(map_stage, reduce_stage, _opt_level));
        // when the Win_MapReduce_GPU will be destroyed we need aslo to destroy the two internal stages
        ff_pipeline::cleanup_nodes();
    }

    // private constructor II (MAP stage on the GPU and REDUCE stage on the CPU with incremental query definition)
    Win_MapReduce_GPU(F_t _gpuFunction,
                      f_reduceupdate_t _reduceUpdate,
                      uint64_t _win_len,
                      uint64_t _slide_len,
                      win_type_t _winType,
                      size_t _map_degree,
                      size_t _reduce_degree,
                      size_t _batch_len,
                      size_t _n_thread_block,
                      string _name,
                      size_t _scratchpad_size,
                      bool _ordered,
                      opt_level_t _opt_level,
                      PatternConfig _config)
                      :
                      gpuFunction(_gpuFunction),
                      reduceUpdate(_reduceUpdate),
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
            cerr << RED << "WindFlow Error: window length or slide cannot be zero" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // the Win_MapReduce_GPU must have a parallel MAP stage
        if(_map_degree < 2) {
            cerr << RED << "WindFlow Error: Win_MapReduce_GPU must have a parallel MAP stage" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // check the validity of the reduce parallelism degree
        if (_reduce_degree == 0) {
            cerr << RED << "WindFlow Error: reduce parallelism degree cannot be zero" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // check the validity of the batch length
        if (_batch_len == 0) {
            cerr << RED << "WindFlow Error: batch length cannot be zero" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // general fastflow pointers to the MAP and REDUCE stages
        ff_node *map_stage, *reduce_stage;
        // create the MAP phase
        if (_map_degree > 1) {
            // vector of Win_Seq_GPU instances
            vector<ff_node *> w(_map_degree);
            // create the Win_Seq_GPU instances
            for (size_t i = 0; i < _map_degree; i++) {
                // configuration structure of the Win_Seq_GPU instance (MAP)
                PatternConfig configSeqMAP(_config.id_inner, _config.n_inner, _config.slide_inner, 0, 1, _slide_len);
                auto *seq = new Win_Seq_GPU<tuple_t, result_t, F_t, wrapper_in_t>(_gpuFunction, _win_len, _slide_len, _winType, _batch_len, _n_thread_block, _name + "_map_wf", _scratchpad_size, configSeqMAP, MAP);
                seq->setMapIndexes(i, _map_degree);
                w[i] = seq;
            }
            ff_farm *farm_map = new ff_farm(w);
            farm_map->remove_collector();
            farm_map->add_collector(new map_collector_t());
            farm_map->add_emitter(new map_emitter_t(_map_degree));
            farm_map->cleanup_all();
            map_stage = farm_map;
        }
        else {
            // configuration structure of the Win_Seq_GPU instance (MAP)
            PatternConfig configSeqMAP(_config.id_inner, _config.n_inner, _config.slide_inner, 0, 1, _slide_len);
            auto *seq_map = new Win_Seq_GPU<tuple_t, result_t, F_t, wrapper_in_t>(_gpuFunction, _win_len, _slide_len, _winType, _batch_len, _n_thread_block, _name + "_map", _scratchpad_size, configSeqMAP, MAP);
            seq_map->setMapIndexes(0, 1);
            map_stage = seq_map;
        }
        // create the REDUCE phase
        if (_reduce_degree > 1) {
            // configuration structure of the Win_Farm instance (REDUCE)
            PatternConfig configWFREDUCE(_config.id_outer, _config.n_outer, _config.slide_outer, _config.id_inner, _config.n_inner, _config.slide_inner);
            auto *farm_reduce = new Win_Farm<result_t, result_t>(_reduceUpdate, _map_degree, _map_degree, CB, 1, _reduce_degree, _name + "_reduce", _ordered, configWFREDUCE, REDUCE);
            reduce_stage = farm_reduce;
        }
        else {
            // configuration structure of the Win_Seq instance (REDUCE)
            PatternConfig configSeqREDUCE(_config.id_inner, _config.n_inner, _config.slide_inner, 0, 1, _map_degree);
            auto *seq_reduce = new Win_Seq<result_t, result_t>(_reduceUpdate, _map_degree, _map_degree, CB, _name + "_reduce", configSeqREDUCE, REDUCE);
            reduce_stage = seq_reduce;
        }
        // add to this the pipeline optimized according to the provided optimization level
        ff_pipeline::add_stage(optimize_WinMapReduceGPU(map_stage, reduce_stage, _opt_level));
        // when the Win_MapReduce_GPU will be destroyed we need aslo to destroy the two internal stages
        ff_pipeline::cleanup_nodes();
    }

    // private constructor III (MAP stage on the CPU with non-incremental query definition and REDUCE stage on the GPU)
    Win_MapReduce_GPU(f_mapfunction_t _mapFunction,
                      F_t _gpuFunction,
                      uint64_t _win_len,
                      uint64_t _slide_len,
                      win_type_t _winType,
                      size_t _map_degree,
                      size_t _reduce_degree,
                      size_t _batch_len,
                      size_t _n_thread_block,
                      string _name,
                      size_t _scratchpad_size,
                      bool _ordered,
                      opt_level_t _opt_level,
                      PatternConfig _config)
                      :
                      mapFunction(_mapFunction),
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
            cerr << RED << "WindFlow Error: window length or slide cannot be zero" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // the Win_MapReduce_GPU must have a parallel MAP stage
        if(_map_degree < 2) {
            cerr << RED << "WindFlow Error: Win_MapReduce_GPU must have a parallel MAP stage" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // check the validity of the reduce parallelism degree
        if (_reduce_degree == 0) {
            cerr << RED << "WindFlow Error: reduce parallelism degree cannot be zero" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // check the validity of the batch length
        if (_batch_len == 0) {
            cerr << RED << "WindFlow Error: batch length cannot be zero" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // general fastflow pointers to the MAP and REDUCE stages
        ff_node *map_stage, *reduce_stage;
        // create the MAP phase
        if (_map_degree > 1) {
            // vector of Win_Seq instances
            vector<ff_node *> w(_map_degree);
            // create the Win_Seq instances
            for (size_t i = 0; i < _map_degree; i++) {
                // configuration structure of the Win_Seq instance (MAP)
                PatternConfig configSeqMAP(_config.id_inner, _config.n_inner, _config.slide_inner, 0, 1, _slide_len);
                auto *seq = new Win_Seq<tuple_t, result_t, wrapper_in_t>(_mapFunction, _win_len, _slide_len, _winType, _name + "_map_wf", configSeqMAP, MAP);
                seq->setMapIndexes(i, _map_degree);
                w[i] = seq;
            }
            ff_farm *farm_map = new ff_farm(w);
            farm_map->remove_collector();
            farm_map->add_collector(new map_collector_t());
            farm_map->add_emitter(new map_emitter_t(_map_degree));
            farm_map->cleanup_all();
            map_stage = farm_map;
        }
        else {
            // configuration structure of the Win_Seq_GPU instance (MAP)
            PatternConfig configSeqMAP(_config.id_inner, _config.n_inner, _config.slide_inner, 0, 1, _slide_len);
            auto *seq_map = new Win_Seq<tuple_t, result_t, wrapper_in_t>(_mapFunction, _win_len, _slide_len, _winType, _name + "_map", configSeqMAP, MAP);
            seq_map->setMapIndexes(0, 1);
            map_stage = seq_map;
        }
        // create the REDUCE phase
        if (_reduce_degree > 1) {
            // configuration structure of the Win_Farm_GPU instance (REDUCE)
            PatternConfig configWFREDUCE(_config.id_outer, _config.n_outer, _config.slide_outer, _config.id_inner, _config.n_inner, _config.slide_inner);
            auto *farm_reduce = new Win_Farm_GPU<result_t, result_t, F_t>(_gpuFunction, _map_degree, _map_degree, CB, 1, _reduce_degree, _batch_len, _n_thread_block, _name + "_reduce", scratchpad_size, _ordered, configWFREDUCE, REDUCE);
            reduce_stage = farm_reduce;
        }
        else {
            // configuration structure of the Win_Seq_GPU instance (REDUCE)
            PatternConfig configSeqREDUCE(_config.id_inner, _config.n_inner, _config.slide_inner, 0, 1, _map_degree);
            auto *seq_reduce = new Win_Seq_GPU<result_t, result_t, F_t>(_gpuFunction, _map_degree, _map_degree, CB, _batch_len, _n_thread_block, _name + "_reduce", scratchpad_size, configSeqREDUCE, REDUCE);
            reduce_stage = seq_reduce;
        }
        // add to this the pipeline optimized according to the provided optimization level
        ff_pipeline::add_stage(optimize_WinMapReduceGPU(map_stage, reduce_stage, _opt_level));
        // when the Win_MapReduce_GPU will be destroyed we need aslo to destroy the two internal stages
        ff_pipeline::cleanup_nodes();
    }

    // private constructor IV (MAP stage on the CPU with incremental query definition and REDUCE stage on the GPU)
    Win_MapReduce_GPU(f_mapupdate_t _mapUpdate,
                      F_t _gpuFunction,
                      uint64_t _win_len,
                      uint64_t _slide_len,
                      win_type_t _winType,
                      size_t _map_degree,
                      size_t _reduce_degree,
                      size_t _batch_len,
                      size_t _n_thread_block,
                      string _name,
                      size_t _scratchpad_size,
                      bool _ordered,
                      opt_level_t _opt_level,
                      PatternConfig _config)
                      :
                      mapUpdate(_mapUpdate),
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
            cerr << RED << "WindFlow Error: window length or slide cannot be zero" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // the Win_MapReduce_GPU must have a parallel MAP stage
        if(_map_degree < 2) {
            cerr << RED << "WindFlow Error: Win_MapReduce_GPU must have a parallel MAP stage" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // check the validity of the reduce parallelism degree
        if (_reduce_degree == 0) {
            cerr << RED << "WindFlow Error: reduce parallelism degree cannot be zero" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // check the validity of the batch length
        if (_batch_len == 0) {
            cerr << RED << "WindFlow Error: batch length cannot be zero" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // general fastflow pointers to the MAP and REDUCE stages
        ff_node *map_stage, *reduce_stage;
        // create the MAP phase
        if (_map_degree > 1) {
            // vector of Win_Seq instances
            vector<ff_node *> w(_map_degree);
            // create the Win_Seq instances
            for (size_t i = 0; i < _map_degree; i++) {
                // configuration structure of the Win_Seq instance (MAP)
                PatternConfig configSeqMAP(_config.id_inner, _config.n_inner, _config.slide_inner, 0, 1, _slide_len);
                auto *seq = new Win_Seq<tuple_t, result_t, wrapper_in_t>(_mapUpdate, _win_len, _slide_len, _winType, _name + "_map_wf", configSeqMAP, MAP);
                seq->setMapIndexes(i, _map_degree);
                w[i] = seq;
            }
            ff_farm *farm_map = new ff_farm(w);
            farm_map->remove_collector();
            farm_map->add_collector(new map_collector_t());
            farm_map->add_emitter(new map_emitter_t(_map_degree));
            farm_map->cleanup_all();
            map_stage = farm_map;
        }
        else {
            // configuration structure of the Win_Seq_GPU instance (MAP)
            PatternConfig configSeqMAP(_config.id_inner, _config.n_inner, _config.slide_inner, 0, 1, _slide_len);
            auto *seq_map = new Win_Seq<tuple_t, result_t, wrapper_in_t>(_mapUpdate, _win_len, _slide_len, _winType, _name + "_map", configSeqMAP, MAP);
            seq_map->setMapIndexes(0, 1);
            map_stage = seq_map;
        }
        // create the REDUCE phase
        if (_reduce_degree > 1) {
            // configuration structure of the Win_Farm_GPU instance (REDUCE)
            PatternConfig configWFREDUCE(_config.id_outer, _config.n_outer, _config.slide_outer, _config.id_inner, _config.n_inner, _config.slide_inner);
            auto *farm_reduce = new Win_Farm_GPU<result_t, result_t, F_t>(_gpuFunction, _map_degree, _map_degree, CB, 1, _reduce_degree, _batch_len, _n_thread_block, _name + "_reduce", scratchpad_size, _ordered, configWFREDUCE, REDUCE);
            reduce_stage = farm_reduce;
        }
        else {
            // configuration structure of the Win_Seq_GPU instance (REDUCE)
            PatternConfig configSeqREDUCE(_config.id_inner, _config.n_inner, _config.slide_inner, 0, 1, _map_degree);
            auto *seq_reduce = new Win_Seq_GPU<result_t, result_t, F_t>(_gpuFunction, _map_degree, _map_degree, CB, _batch_len, _n_thread_block, _name + "_reduce", scratchpad_size, configSeqREDUCE, REDUCE);
            reduce_stage = seq_reduce;
        }
        // add to this the pipeline optimized according to the provided optimization level
        ff_pipeline::add_stage(optimize_WinMapReduceGPU(map_stage, reduce_stage, _opt_level));
        // when the Win_MapReduce_GPU will be destroyed we need aslo to destroy the two internal stages
        ff_pipeline::cleanup_nodes();
    }

    // method to optimize the structure of the Win_MapReduce_GPU pattern
    const ff_pipeline optimize_WinMapReduceGPU(ff_node *map, ff_node *reduce, opt_level_t opt)
    {
        if (opt == LEVEL0) { // no optimization
            ff_pipeline pipe;
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
                ff_farm *farm_map = static_cast<ff_farm *>(map);
                ff_farm *farm_reduce = static_cast<ff_farm *>(reduce);
                emitter_reduce_t *emitter_reduce = static_cast<emitter_reduce_t *>(farm_reduce->getEmitter());
                OrderingNode<result_t> *buf_node = new OrderingNode<result_t>(farm_map->getNWorkers());
                const ff_pipeline result = combine_farms(*farm_map, emitter_reduce, *farm_reduce, buf_node, false);
                delete farm_map;
                delete farm_reduce;
                delete buf_node;
                return result;
            }
        }
    }

public:
    /** 
     *  \brief Constructor I (MAP stage on GPU and Non-Incremental REDUCE stage on CPU)
     *  
     *  \param _mapFunction the non-incremental map processing function (CPU/GPU function)
     *  \param _reduceFunction the non-incremental window reduce processing function (CPU function)
     *  \param _win_len window length (in no. of tuples or in time units)
     *  \param _slide_len slide length (in no. of tuples or in time units)
     *  \param _winType window type (count-based CB or time-based TB)
     *  \param _map_degree number of Win_Seq_GPU instances in the MAP phase
     *  \param _reduce_degree number of Win_Seq instances in the REDUCE phase
     *  \param _batch_len no. of window partitions in a batch (i.e. 1 window patition mapped onto 1 CUDA thread)
     *  \param _n_thread_block number of threads (i.e. window patitions) per block
     *  \param _name string with the unique name of the pattern
     *  \param _scratchpad_size size in bytes of the scratchpad area per CUDA thread (on the GPU) 
     *  \param _ordered true if the results of the same key must be emitted in order (default)
     *  \param _opt_level optimization level used to build the pattern
     */ 
    Win_MapReduce_GPU(F_t _mapFunction,
                      f_reducefunction_t _reduceFunction,
                      uint64_t _win_len,
                      uint64_t _slide_len,
                      win_type_t _winType,
                      size_t _map_degree,
                      size_t _reduce_degree,
                      size_t _batch_len,
                      size_t _n_thread_block,
                      string _name,
                      size_t _scratchpad_size=0,
                      bool _ordered=true,
                      opt_level_t _opt_level=LEVEL0)
                      :
                      Win_MapReduce_GPU(_mapFunction, _reduceFunction, _win_len, _slide_len, _winType, _map_degree, _reduce_degree, _batch_len, _n_thread_block, _name, _scratchpad_size, _ordered, _opt_level, PatternConfig(0, 1, _slide_len, 0, 1, _slide_len)) {}

    /** 
     *  \brief Constructor II (MAP stage on GPU and Incremental REDUCE stage on CPU)
     *  
     *  \param _mapFunction the non-incremental map processing function (CPU/GPU function)
     *  \param _reduceUpdate the incremental window reduce processing function (CPU function)
     *  \param _win_len window length (in no. of tuples or in time units)
     *  \param _slide_len slide length (in no. of tuples or in time units)
     *  \param _winType window type (count-based CB or time-based TB)
     *  \param _map_degree number of Win_Seq_GPU instances in the MAP phase
     *  \param _reduce_degree number of Win_Seq instances in the REDUCE phase
     *  \param _batch_len no. of window partitions in a batch (i.e. 1 window patition mapped onto 1 CUDA thread)
     *  \param _n_thread_block number of threads (i.e. window patitions) per block
     *  \param _name string with the unique name of the pattern
     *  \param _scratchpad_size size in bytes of the scratchpad area per CUDA thread (on the GPU)
     *  \param _ordered true if the results of the same key must be emitted in order (default)
     *  \param _opt_level optimization level used to build the pattern
     */ 
    Win_MapReduce_GPU(F_t _mapFunction,
                      f_reduceupdate_t _reduceUpdate,
                      uint64_t _win_len,
                      uint64_t _slide_len,
                      win_type_t _winType,
                      size_t _map_degree,
                      size_t _reduce_degree,
                      size_t _batch_len,
                      size_t _n_thread_block,
                      string _name,
                      size_t _scratchpad_size=0,
                      bool _ordered=true,
                      opt_level_t _opt_level=LEVEL0)
                      :
                      Win_MapReduce_GPU(_mapFunction, _reduceUpdate, _win_len, _slide_len, _winType, _map_degree, _reduce_degree, _batch_len, _n_thread_block, _name, _scratchpad_size, _ordered, _opt_level, PatternConfig(0, 1, _slide_len, 0, 1, _slide_len)) {}

    /** 
     *  \brief Constructor III (Non-incremental MAP stage on CPU and REDUCE stage on GPU)
     *  
     *  \param _mapFunction the non-incremental map processing function (CPU function)
     *  \param _reduceFunction the non-incremental window reduce processing function (CPU/GPU function)
     *  \param _win_len window length (in no. of tuples or in time units)
     *  \param _slide_len slide length (in no. of tuples or in time units)
     *  \param _winType window type (count-based CB or time-based TB)
     *  \param _map_degree number of Win_Seq_GPU instances in the MAP phase
     *  \param _reduce_degree number of Win_Seq instances in the REDUCE phase
     *  \param _batch_len no. of window partitions in a batch (i.e. 1 window patition mapped onto 1 CUDA thread)
     *  \param _n_thread_block number of threads (i.e. window patitions) per block
     *  \param _name string with the unique name of the pattern
     *  \param _scratchpad_size size in bytes of the scratchpad area per CUDA thread (on the GPU)
     *  \param _ordered true if the results of the same key must be emitted in order (default)
     *  \param _opt_level optimization level used to build the pattern
     */ 
    Win_MapReduce_GPU(f_mapfunction_t _mapFunction,
                      F_t _reduceFunction,
                      uint64_t _win_len,
                      uint64_t _slide_len,
                      win_type_t _winType,
                      size_t _map_degree,
                      size_t _reduce_degree,
                      size_t _batch_len,
                      size_t _n_thread_block,
                      string _name,
                      size_t _scratchpad_size=0,
                      bool _ordered=true,
                      opt_level_t _opt_level=LEVEL0)
                      :
                      Win_MapReduce_GPU(_mapFunction, _reduceFunction, _win_len, _slide_len, _winType, _map_degree, _reduce_degree, _batch_len, _n_thread_block, _name, _scratchpad_size, _ordered, _opt_level, PatternConfig(0, 1, _slide_len, 0, 1, _slide_len)) {}

    /** 
     *  \brief Constructor IV (Incremental MAP stage on CPU and REDUCE stage on GPU)
     *  
     *  \param _mapUpdate the incremental map processing function (CPU function)
     *  \param _reduceFunction the non-incremental window reduce processing function (CPU/GPU function)
     *  \param _win_len window length (in no. of tuples or in time units)
     *  \param _slide_len slide length (in no. of tuples or in time units)
     *  \param _winType window type (count-based CB or time-based TB)
     *  \param _map_degree number of Win_Seq_GPU instances in the MAP phase
     *  \param _reduce_degree number of Win_Seq instances in the REDUCE phase
     *  \param _batch_len no. of window partitions in a batch (i.e. 1 window patition mapped onto 1 CUDA thread)
     *  \param _n_thread_block number of threads (i.e. window patitions) per block
     *  \param _name string with the unique name of the pattern
     *  \param _scratchpad_size size in bytes of the scratchpad area per CUDA thread (on the GPU)
     *  \param _ordered true if the results of the same key must be emitted in order (default)
     *  \param _opt_level optimization level used to build the pattern
     */ 
    Win_MapReduce_GPU(f_mapupdate_t _mapUpdate,
                      F_t _reduceUpdate,
                      uint64_t _win_len,
                      uint64_t _slide_len,
                      win_type_t _winType,
                      size_t _map_degree,
                      size_t _reduce_degree,
                      size_t _batch_len,
                      size_t _n_thread_block,
                      string _name,
                      size_t _scratchpad_size=0,
                      bool _ordered=true,
                      opt_level_t _opt_level=LEVEL0)
                      :
                      Win_MapReduce_GPU(_mapUpdate, _reduceUpdate, _win_len, _slide_len, _winType, _map_degree, _reduce_degree, _batch_len, _n_thread_block, _name, _scratchpad_size, _ordered, _opt_level, PatternConfig(0, 1, _slide_len, 0, 1, _slide_len)) {}

    /// Destructor
    ~Win_MapReduce_GPU() {}

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
