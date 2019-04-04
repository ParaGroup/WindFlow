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
 *  @file    win_mapreduce.hpp
 *  @author  Gabriele Mencagli
 *  @date    29/10/2017
 *  
 *  @brief Win_MapReduce pattern executing a windowed transformation in parallel on multi-core CPUs
 *  
 *  @section Win_MapReduce (Description)
 *  
 *  This file implements the Win_MapReduce pattern able to execute windowed queries on a
 *  multicore. The pattern processes (possibly in parallel) partitions of the windows in the
 *  so-called MAP stage, and computes (possibly in parallel) results of the windows out of the
 *  partition results in the REDUCE stage. The pattern supports both a non-incremental and an
 *  incremental query definition in the two stages.
 *  
 *  The template arguments tuple_t and result_t must be default constructible, with a copy constructor
 *  and copy assignment operator, and they must provide and implement the setInfo() and
 *  getInfo() methods.
 */ 

#ifndef WIN_MAPREDUCE_H
#define WIN_MAPREDUCE_H

// includes
#include <ff/combine.hpp>
#include <ff/pipeline.hpp>
#include <win_farm.hpp>
#include <wm_nodes.hpp>
#include <orderingNode.hpp>

/** 
 *  \class Win_MapReduce
 *  
 *  \brief Win_MapReduce pattern executing a windowed transformation in parallel on multi-core CPUs
 *  
 *  This class implements the Win_MapReduce pattern executing windowed queries in parallel on
 *  a multicore. The pattern processes (possibly in parallel) window partitions in the MAP
 *  stage and builds window results out from partition results (possibly in parallel) in the
 *  REDUCE stage.
 */ 
template<typename tuple_t, typename result_t, typename input_t>
class Win_MapReduce: public ff_pipeline
{
public:
    /// function type of the non-incremental MAP processing
    using f_mapfunction_t = function<int(size_t, uint64_t, Iterable<tuple_t> &, result_t &)>;
    /// function type of the incremental MAP processing
    using f_mapupdate_t = function<int(size_t, uint64_t, const tuple_t &, result_t &)>;
    /// function type of the non-incremental REDUCE processing
    using f_reducefunction_t = function<int(size_t, uint64_t, Iterable<result_t> &, result_t &)>;
    /// function type of the incremental REDUCE processing
    using f_reduceupdate_t = function<int(size_t, uint64_t, const result_t &, result_t &)>;
private:
    // type of the wrapper of input tuples
    using wrapper_in_t = wrapper_tuple_t<tuple_t>;
    // type of the WinMap_Emitter node
    using map_emitter_t = WinMap_Emitter<tuple_t, input_t>;
    // type of the WinMap_Collector node
    using map_collector_t = WinMap_Collector<result_t>;    
    // friendships with other classes in the library
    template<typename T1, typename T2, typename T3>
    friend class Win_Farm;
    template<typename T1, typename T2, typename T3>
    friend class Key_Farm;
    template<typename T>
    friend class WinFarm_Builder;
    template<typename T>
    friend class KeyFarm_Builder;
    // configuration variables of the Win_MapReduce
    f_mapfunction_t mapFunction;
    f_mapupdate_t mapUpdate;
    f_reducefunction_t reduceFunction;
    f_reduceupdate_t reduceUpdate;
    bool isNICMAP;
    bool isNICREDUCE;
    uint64_t win_len;
    uint64_t slide_len;
    win_type_t winType;
    size_t map_degree;
    size_t reduce_degree;
    string name;
    bool ordered;
    opt_level_t opt_level;
    PatternConfig config;

    // private constructor I (non-incremental MAP phase and non-incremental REDUCE phase)
    Win_MapReduce(f_mapfunction_t _mapFunction,
                  f_reducefunction_t _reduceFunction,
                  uint64_t _win_len,
                  uint64_t _slide_len,
                  win_type_t _winType,
                  size_t _map_degree,
                  size_t _reduce_degree,
                  string _name,
                  bool _ordered,
                  opt_level_t _opt_level,
                  PatternConfig _config)
                  :
                  mapFunction(_mapFunction),
                  reduceFunction(_reduceFunction),
                  isNICMAP(true),
                  isNICREDUCE(true),
                  win_len(_win_len),
                  slide_len(_slide_len),
                  winType(_winType),
                  map_degree(_map_degree),
                  reduce_degree(_reduce_degree),
                  name(_name),
                  ordered(_ordered),
                  opt_level(_opt_level),
                  config(_config)    
    {
        // check the validity of the windowing parameters
        if (_win_len == 0 || _slide_len == 0) {
            cerr << RED << "WindFlow Error: window length or slide cannot be zero" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // the Win_MapReduce must have a parallel MAP stage
        if (_map_degree < 2) {
            cerr << RED << "WindFlow Error: Win_MapReduce must have a parallel MAP stage" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // check the validity of the reduce parallelism degree
        if (_reduce_degree == 0) {
            cerr << RED << "WindFlow Error: parallelism degree of the REDUCE cannot be zero" << DEFAULT << endl;
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
            farm_map->add_emitter(new map_emitter_t(_map_degree, _winType));
            farm_map->cleanup_all();
            map_stage = farm_map;
        }
        else {
            // configuration structure of the Win_Seq instance (MAP)
            PatternConfig configSeqMAP(_config.id_inner, _config.n_inner, _config.slide_inner, 0, 1, _slide_len);
            auto *seq_map = new Win_Seq<tuple_t, result_t, wrapper_in_t>(_mapFunction, _win_len, _slide_len, _winType, _name + "_map", configSeqMAP, MAP);
            seq_map->setMapIndexes(0, 1);
            map_stage = seq_map;
        }
        // create the REDUCE phase
        if (_reduce_degree > 1) {
            // configuration structure of the Win_Farm instance (REDUCE)
            PatternConfig configWFREDUCE(_config.id_outer, _config.n_outer, _config.slide_outer, _config.id_inner, _config.n_inner, _config.slide_inner);
            auto *farm_reduce = new Win_Farm<result_t, result_t>(_reduceFunction, _map_degree, _map_degree, CB, 1, _reduce_degree, _name + "_reduce", _ordered, LEVEL0, configWFREDUCE, REDUCE);
            reduce_stage = farm_reduce;
        }
        else {
            // configuration structure of the Win_Seq instance (REDUCE)
            PatternConfig configSeqREDUCE(_config.id_inner, _config.n_inner, _config.slide_inner, 0, 1, _map_degree);
            auto *seq_reduce = new Win_Seq<result_t, result_t>(_reduceFunction, _map_degree, _map_degree, CB, _name + "_reduce", configSeqREDUCE, REDUCE);
            reduce_stage = seq_reduce;
        }
        // add to this the pipeline optimized according to the provided optimization level
        ff_pipeline::add_stage(optimize_WinMapReduce(map_stage, reduce_stage, _opt_level));
        // when the Win_MapReduce will be destroyed we need aslo to destroy the two internal stages
        ff_pipeline::cleanup_nodes();
        // flatten the pipeline
        ff_pipeline::flatten();
    }

    // private constructor II (incremental MAP phase and incremental REDUCE phase)
    Win_MapReduce(f_mapupdate_t _mapUpdate,
                  f_reduceupdate_t _reduceUpdate,
                  uint64_t _win_len,
                  uint64_t _slide_len,
                  win_type_t _winType,
                  size_t _map_degree,
                  size_t _reduce_degree,
                  string _name,
                  bool _ordered,
                  opt_level_t _opt_level,
                  PatternConfig _config)
                  :
                  mapUpdate(_mapUpdate),
                  reduceUpdate(_reduceUpdate),
                  isNICMAP(false),
                  isNICREDUCE(false),
                  win_len(_win_len),
                  slide_len(_slide_len),
                  winType(_winType),
                  map_degree(_map_degree),
                  reduce_degree(_reduce_degree),
                  name(_name),
                  ordered(_ordered),
                  opt_level(_opt_level),
                  config(_config)   
    {
        // check the validity of the windowing parameters
        if (_win_len == 0 || _slide_len == 0) {
            cerr << RED << "WindFlow Error: window length or slide cannot be zero" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // the Win_MapReduce must have a parallel MAP stage
        if (_map_degree < 2) {
            cerr << RED << "WindFlow Error: Win_MapReduce must have a parallel MAP stage" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // check the validity of the reduce parallelism degree
        if (_reduce_degree == 0) {
            cerr << RED << "WindFlow Error: parallelism degree of the REDUCE cannot be zero" << DEFAULT << endl;
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
            farm_map->add_emitter(new map_emitter_t(_map_degree, _winType));
            farm_map->cleanup_all();
            map_stage = farm_map;
        }
        else {
            // configuration structure of the Win_Seq instance (MAP)
            PatternConfig configSeqMAP(_config.id_inner, _config.n_inner, _config.slide_inner, 0, 1, _slide_len);
            auto *seq_map = new Win_Seq<tuple_t, result_t, wrapper_in_t>(_mapUpdate, _win_len, _slide_len, _winType, _name + "_map", configSeqMAP, MAP);
            seq_map->setMapIndexes(0, 1);
            map_stage = seq_map;
        }
        // create the REDUCE phase
        if (_reduce_degree > 1) {
            // configuration structure of the Win_Farm instance (REDUCE)
            PatternConfig configWFREDUCE(_config.id_outer, _config.n_outer, _config.slide_outer, _config.id_inner, _config.n_inner, _config.slide_inner);
            auto *farm_reduce = new Win_Farm<result_t, result_t>(_reduceUpdate, _map_degree, _map_degree, CB, 1, _reduce_degree, _name + "_reduce", _ordered, LEVEL0, configWFREDUCE, REDUCE);
            reduce_stage = farm_reduce;
        }
        else {
            // configuration structure of the Win_Seq instance (REDUCE)
            PatternConfig configSeqREDUCE(_config.id_inner, _config.n_inner, _config.slide_inner, 0, 1, _map_degree);
            auto *seq_reduce = new Win_Seq<result_t, result_t>(_reduceUpdate, _map_degree, _map_degree, CB, _name + "_reduce", configSeqREDUCE, REDUCE);
            reduce_stage = seq_reduce;
        }
        // add to this the pipeline optimized according to the provided optimization level
        ff_pipeline::add_stage(optimize_WinMapReduce(map_stage, reduce_stage, _opt_level));
        // when the Win_MapReduce will be destroyed we need aslo to destroy the two internal stages
        ff_pipeline::cleanup_nodes();
        // flatten the pipeline
        ff_pipeline::flatten();
    }

    // private constructor III (non-incremental MAP phase and incremental REDUCE phase)
    Win_MapReduce(f_mapfunction_t _mapFunction,
                  f_reduceupdate_t _reduceUpdate,
                  uint64_t _win_len,
                  uint64_t _slide_len,
                  win_type_t _winType,
                  size_t _map_degree,
                  size_t _reduce_degree,
                  string _name,
                  bool _ordered,
                  opt_level_t _opt_level,
                  PatternConfig _config)
                  :
                  mapFunction(_mapFunction),
                  reduceUpdate(_reduceUpdate),
                  isNICMAP(true),
                  isNICREDUCE(false),
                  win_len(_win_len),
                  slide_len(_slide_len),
                  winType(_winType),
                  map_degree(_map_degree),
                  reduce_degree(_reduce_degree),
                  name(_name),
                  ordered(_ordered),
                  opt_level(_opt_level),
                  config(_config) 
    {
        // check the validity of the windowing parameters
        if (_win_len == 0 || _slide_len == 0) {
            cerr << RED << "WindFlow Error: window length or slide cannot be zero" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // the Win_MapReduce must have a parallel MAP stage
        if (_map_degree < 2) {
            cerr << RED << "WindFlow Error: Win_MapReduce must have a parallel MAP stage" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // check the validity of the reduce parallelism degree
        if (_reduce_degree == 0) {
            cerr << RED << "WindFlow Error: parallelism degree of the REDUCE cannot be zero" << DEFAULT << endl;
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
            farm_map->add_emitter(new map_emitter_t(_map_degree, _winType));
            farm_map->cleanup_all();
            map_stage = farm_map;
        }
        else {
            // configuration structure of the Win_Seq instance (MAP)
            PatternConfig configSeqMAP(_config.id_inner, _config.n_inner, _config.slide_inner, 0, 1, _slide_len);
            auto *seq_map = new Win_Seq<tuple_t, result_t, wrapper_in_t>(_mapFunction, _win_len, _slide_len, _winType, _name + "_map", configSeqMAP, MAP);
            seq_map->setMapIndexes(0, 1);
            map_stage = seq_map;
        }
        // create the REDUCE phase
        if (_reduce_degree > 1) {
            // configuration structure of the Win_Farm instance (REDUCE)
            PatternConfig configWFREDUCE(_config.id_outer, _config.n_outer, _config.slide_outer, _config.id_inner, _config.n_inner, _config.slide_inner);
            auto *farm_reduce = new Win_Farm<result_t, result_t>(_reduceUpdate, _map_degree, _map_degree, CB, 1, _reduce_degree, _name + "_reduce", _ordered, LEVEL0, configWFREDUCE, REDUCE);
            reduce_stage = farm_reduce;
        }
        else {
            // configuration structure of the Win_Seq instance (REDUCE)
            PatternConfig configSeqREDUCE(_config.id_inner, _config.n_inner, _config.slide_inner, 0, 1, _map_degree);
            auto *seq_reduce = new Win_Seq<result_t, result_t>(_reduceUpdate, _map_degree, _map_degree, CB, _name + "_reduce", configSeqREDUCE, REDUCE);
            reduce_stage = seq_reduce;
        }
        // add to this the pipeline optimized according to the provided optimization level
        ff_pipeline::add_stage(optimize_WinMapReduce(map_stage, reduce_stage, _opt_level));
        // when the Win_MapReduce will be destroyed we need aslo to destroy the two internal stages
        ff_pipeline::cleanup_nodes();
        // flatten the pipeline
        ff_pipeline::flatten();
    }

    // private constructor IV (incremental MAP phase and non-incremental REDUCE phase)
    Win_MapReduce(f_mapupdate_t _mapUpdate,
                  f_reducefunction_t _reduceFunction,
                  uint64_t _win_len,
                  uint64_t _slide_len,
                  win_type_t _winType,
                  size_t _map_degree,
                  size_t _reduce_degree,
                  string _name,
                  bool _ordered,
                  opt_level_t _opt_level,
                  PatternConfig _config)
                  :
                  mapUpdate(_mapUpdate),
                  reduceFunction(_reduceFunction),
                  isNICMAP(false),
                  isNICREDUCE(true),
                  win_len(_win_len),
                  slide_len(_slide_len),
                  winType(_winType),
                  map_degree(_map_degree),
                  reduce_degree(_reduce_degree),
                  name(_name),
                  ordered(_ordered),
                  opt_level(_opt_level),
                  config(_config) 
    {
        // check the validity of the windowing parameters
        if (_win_len == 0 || _slide_len == 0) {
            cerr << RED << "WindFlow Error: window length or slide cannot be zero" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // the Win_MapReduce must have a parallel MAP stage
        if (_map_degree < 2) {
            cerr << RED << "WindFlow Error: Win_MapReduce must have a parallel MAP stage" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // check the validity of the reduce parallelism degree
        if (_reduce_degree == 0) {
            cerr << RED << "WindFlow Error: parallelism degree of the REDUCE cannot be zero" << DEFAULT << endl;
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
            farm_map->add_emitter(new map_emitter_t(_map_degree, _winType));
            farm_map->cleanup_all();
            map_stage = farm_map;
        }
        else {
            // configuration structure of the Win_Seq instance (MAP)
            PatternConfig configSeqMAP(_config.id_inner, _config.n_inner, _config.slide_inner, 0, 1, _slide_len);
            auto *seq_map = new Win_Seq<tuple_t, result_t, wrapper_in_t>(_mapUpdate, _win_len, _slide_len, _winType, _name + "_map", configSeqMAP, MAP);
            seq_map->setMapIndexes(0, 1);
            map_stage = seq_map;
        }
        // create the REDUCE phase
        if (_reduce_degree > 1) {
            // configuration structure of the Win_Farm instance (REDUCE)
            PatternConfig configWFREDUCE(_config.id_outer, _config.n_outer, _config.slide_outer, _config.id_inner, _config.n_inner, _config.slide_inner);
            auto *farm_reduce = new Win_Farm<result_t, result_t>(_reduceFunction, _map_degree, _map_degree, CB, 1, _reduce_degree, _name + "_reduce", _ordered, LEVEL0, configWFREDUCE, REDUCE);
            reduce_stage = farm_reduce;
        }
        else {
            // configuration structure of the Win_Seq instance (REDUCE)
            PatternConfig configSeqREDUCE(_config.id_inner, _config.n_inner, _config.slide_inner, 0, 1, _map_degree);
            auto *seq_reduce = new Win_Seq<result_t, result_t>(_reduceFunction, _map_degree, _map_degree, CB, _name + "_reduce", configSeqREDUCE, REDUCE);
            reduce_stage = seq_reduce;
        }
        // add to this the pipeline optimized according to the provided optimization level
        ff_pipeline::add_stage(optimize_WinMapReduce(map_stage, reduce_stage, _opt_level));
        // when the Win_MapReduce will be destroyed we need aslo to destroy the two internal stages
        ff_pipeline::cleanup_nodes();
        // flatten the pipeline
        ff_pipeline::flatten();
    }

    // method to optimize the structure of the Win_MapReduce pattern
    const ff_pipeline optimize_WinMapReduce(ff_node *map, ff_node *reduce, opt_level_t opt)
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
                OrderingNode<result_t, wrapper_tuple_t<result_t>> *buf_node = new OrderingNode<result_t, wrapper_tuple_t<result_t>>();
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
     *  \brief Constructor I (Non-Incremental MAP phase and Non-Incremental REDUCE phase)
     *  
     *  \param _mapFunction the non-incremental window map processing function (MAP)
     *  \param _reduceFunction the non-incremental window reduce processing function (REDUCE)
     *  \param _win_len window length (in no. of tuples or in time units)
     *  \param _slide_len slide length (in no. of tuples or in time units)
     *  \param _winType window type (count-based CB or time-based TB)
     *  \param _map_degree parallelism degree of the MAP stage
     *  \param _name string with the unique name of the pattern
     *  \param _reduce_degree parallelism degree of the REDUCE stage
     *  \param _ordered true if the results of the same key must be emitted in order, false otherwise
     *  \param _opt_level optimization level used to build the pattern
     */ 
    Win_MapReduce(f_mapfunction_t _mapFunction,
                  f_reducefunction_t _reduceFunction,
                  uint64_t _win_len,
                  uint64_t _slide_len,
                  win_type_t _winType,
                  size_t _map_degree,
                  size_t _reduce_degree,
                  string _name,
                  bool _ordered=true,
                  opt_level_t _opt_level=LEVEL0)
                  :
                  Win_MapReduce(_mapFunction, _reduceFunction, _win_len, _slide_len, _winType, _map_degree, _reduce_degree, _name, _ordered, _opt_level, PatternConfig(0, 1, _slide_len, 0, 1, _slide_len)) {}

    /** 
     *  \brief Constructor II (Incremental MAP phase and Incremental REDUCE phase)
     *  
     *  \param _mapUpdate the incremental window MAP processing function
     *  \param _reduceUpdate the incremental window REDUCE processing function
     *  \param _win_len window length (in no. of tuples or in time units)
     *  \param _slide_len slide length (in no. of tuples or in time units)
     *  \param _winType window type (count-based CB or time-based TB)
     *  \param _map_degree parallelism degree of the MAP stage
     *  \param _reduce_degree parallelism degree of the REDUCE stage
     *  \param _name string with the unique name of the pattern
     *  \param _ordered true if the results of the same key must be emitted in order, false otherwise
     *  \param _opt_level optimization level used to build the pattern
     */ 
    Win_MapReduce(f_mapupdate_t _mapUpdate,
                  f_reduceupdate_t _reduceUpdate,
                  uint64_t _win_len,
                  uint64_t _slide_len,
                  win_type_t _winType,
                  size_t _map_degree,
                  size_t _reduce_degree,
                  string _name,
                  bool _ordered=true,
                  opt_level_t _opt_level=LEVEL0)
                  :
                  Win_MapReduce(_mapUpdate, _reduceUpdate, _win_len, _slide_len, _winType, _map_degree, _reduce_degree, _name, _ordered, _opt_level, PatternConfig(0, 1, _slide_len, 0, 1, _slide_len)) {}

    /** 
     *  \brief Constructor III (Non-Incremental MAP phase and Incremental REDUCE phase)
     *  
     *  \param _mapFunction the non-incremental window map processing function
     *  \param _reduceUpdate the incremental window reduce processing function
     *  \param _win_len window length (in no. of tuples or in time units)
     *  \param _slide_len slide length (in no. of tuples or in time units)
     *  \param _winType window type (count-based CB or time-based TB)
     *  \param _map_degree parallelism degree of the MAP stage
     *  \param _reduce_degree parallelism degree of the REDUCE stage
     *  \param _name string with the unique name of the pattern
     *  \param _ordered true if the results of the same key must be emitted in order, false otherwise
     *  \param _opt_level optimization level used to build the pattern
     */ 
    Win_MapReduce(f_mapfunction_t _mapFunction,
                  f_reduceupdate_t _reduceUpdate,
                  uint64_t _win_len,
                  uint64_t _slide_len,
                  win_type_t _winType,
                  size_t _map_degree,
                  size_t _reduce_degree,
                  string _name,
                  bool _ordered=true,
                  opt_level_t _opt_level=LEVEL0)
                  :
                  Win_MapReduce(_mapFunction, _reduceUpdate, _win_len, _slide_len, _winType, _map_degree, _reduce_degree, _name, _ordered, _opt_level, PatternConfig(0, 1, _slide_len, 0, 1, _slide_len)) {}

    /** 
     *  \brief Constructor IV (Incremental MAP phase and Non-Incremental REDUCE phase)
     *  
     *  \param _mapUpdate the incremental window map processing function
     *  \param _reduceFunction the non-incremental window reduce processing function
     *  \param _win_len window length (in no. of tuples or in time units)
     *  \param _slide_len slide length (in no. of tuples or in time units)
     *  \param _winType window type (count-based CB or time-based TB)
     *  \param _map_degree parallelism degree of the MAP stage
     *  \param _reduce_degree parallelism degree of the REDUCE stage
     *  \param _name string with the unique name of the pattern
     *  \param _ordered true if the results of the same key must be emitted in order, false otherwise
     *  \param _opt_level optimization level used to build the pattern
     */ 
    Win_MapReduce(f_mapupdate_t _mapUpdate,
                  f_reducefunction_t _reduceFunction,
                  uint64_t _win_len,
                  uint64_t _slide_len,
                  win_type_t _winType,
                  size_t _map_degree,
                  size_t _reduce_degree,
                  string _name,
                  bool _ordered=true,
                  opt_level_t _opt_level=LEVEL0)
                  :
                  Win_MapReduce(_mapUpdate, _reduceFunction, _win_len, _slide_len, _winType, _map_degree, _reduce_degree, _name, _ordered, _opt_level, PatternConfig(0, 1, _slide_len, 0, 1, _slide_len)) {}

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

//@cond DOXY_IGNORE

    // ------------------------- deleted method ---------------------------
    template<typename T>
    int  add_stage(T *s, bool cleanup=false)                      = delete;
    template<typename T>
    int  add_stage(const T &s)                                    = delete;
    int  wrap_around(bool multi_input=false)                      = delete;
    void cleanup_nodes()                                          = delete;
    bool offload(void * task,
                 unsigned long retry=((unsigned long)-1),
                 unsigned long ticks=ff_node::TICKS2WAIT)         = delete;
    bool load_result(void ** task,
                     unsigned long retry=((unsigned long)-1),
                     unsigned long ticks=ff_node::TICKS2WAIT)     = delete;
    bool load_result_nb(void ** task)                             = delete;

//@endcond

};

#endif
