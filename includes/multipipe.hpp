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
 *  @file    multipipe.hpp
 *  @author  Gabriele Mencagli
 *  @date    14/01/2019
 *  
 *  @brief MultiPipe construct to compose a complex pipeline of streaming patterns
 *  
 *  @section MultiPipe Construct (Description)
 *  
 *  This file implements the MultiPipe construct used to build a complex pipeline
 *  of WindFlow patterns. A runnable MultiPipe starts with a Source, has a
 *  sequence of pattern inside, and terminates with a Sink.
 */ 

#ifndef MULTIPIPE_H
#define MULTIPIPE_H

/// includes
#include <string>
#include <vector>
#include <math.h>
#include <ff/ff.hpp>
#include <basic.hpp>
#include <standard.hpp>
#include <wm_nodes.hpp>
#include <wf_nodes.hpp>
#include <kf_nodes.hpp>
#include <ordering_node.hpp>
#include <tree_combiner.hpp>
#include <broadcast_node.hpp>
#include <transformations.hpp>

namespace wf {

/** 
 *  \class MultiPipe
 *  
 *  \brief MultiPipe construct to compose a complex pipeline of streaming patterns
 *  
 *  This class implements the MultiPipe construct used to build a complex linear composition
 *  of patterns available in the WindFlow library.
 */ 
class MultiPipe: public ff::ff_pipeline
{
private:
    // enumeration of the routing types
    enum routing_types_t { SIMPLE, COMPLEX };
	std::string name; // std::string with the unique name of the MultiPipe
	bool has_source; // true if the MultiPipe has a Source
	bool has_sink; // true if the MultiPipe ends with a Sink
	ff::ff_a2a *last; // pointer to the last matrioska
    ff::ff_a2a *secondToLast; // pointer to the second to last matrioska
    bool isUnified; // true if the MultiPipe is used in a union with other MultiPipe
    bool forceShuffling; // true if we force the use of a shuffling for adding the next operator to the MultiPipe
    size_t lastParallelism; // parallelism of the last operator added to the MultiPipe (0 if not defined)
    // class of the self-killer multi-input node (selfkiller_node)
    class selfkiller_node: public ff::ff_minode
    {
        // svc_init (utilized by the FastFlow runtime)
        int svc_init()
        {
            skipfirstpop(true);
            return 0;
        }

        // svc method (utilized by the FastFlow runtime)
        void *svc(void *)
        {
            return this->EOS;
        }
    };

    // Private Constructor (used by union only)
    MultiPipe(std::string _name, std::vector<ff_node *> _init_set):
              name(_name),
              has_source(true),
              has_sink(false),
              isUnified(false),
              forceShuffling(true),
              lastParallelism(0)
    {
        // create the initial matrioska
        ff::ff_a2a *matrioska = new ff::ff_a2a();
        matrioska->add_firstset(_init_set, 0, false);
        std::vector<ff_node *> second_set;
        ff::ff_pipeline *stage = new ff::ff_pipeline();
        stage->add_stage(new selfkiller_node(), true);
        second_set.push_back(stage);
        matrioska->add_secondset(second_set, true);
        this->add_stage(matrioska, true);
        this->last = matrioska;
        this->secondToLast = nullptr;
    }

    // generic method to add an operator to the MultiPipe
    template<typename emitter_t, typename collector_t>
    void add_operator(ff::ff_farm *_pattern, routing_types_t _type, ordering_mode_t _ordering)
    {
        // check the Source and Sink presence
        if (!this->has_source) {
            std::cerr << RED << "WindFlow Error: Source is not defined for the MultiPipe [" << name << "]" << DEFAULT << std::endl;
            exit(EXIT_FAILURE);
        }
        if (this->has_sink) {
            std::cerr << RED << "WindFlow Error: MultiPipe [" << name << "]" << " is terminated by a Sink" << DEFAULT << std::endl;
            exit(EXIT_FAILURE);
        }
        size_t n1 = (last->getFirstSet()).size();
        size_t n2 = (_pattern->getWorkers()).size();
        // Case 1: direct connection
        if ((n1 == n2) && _type == SIMPLE && !forceShuffling) {
            auto first_set = last->getFirstSet();
            auto worker_set = _pattern->getWorkers();
            // add the pattern's workers to the pipelines in the first set of the matrioska
            for (size_t i=0; i<n1; i++) {
                ff::ff_pipeline *stage = static_cast<ff::ff_pipeline *>(first_set[i]);
                stage->add_stage(worker_set[i], false);
            }
        }
        // Case 2: shuffling connection
        else {
            // prepare the nodes of the first_set of the last matrioska for the shuffling
            auto first_set_m = last->getFirstSet();
            for (size_t i=0; i<n1; i++) {
                ff::ff_pipeline *stage = static_cast<ff::ff_pipeline *>(first_set_m[i]);
                emitter_t *tmp_e = static_cast<emitter_t *>(_pattern->getEmitter());
                combine_with_laststage(*stage, new emitter_t(*tmp_e), true);
            }
            // create a new matrioska
            ff::ff_a2a *matrioska = new ff::ff_a2a();
            std::vector<ff_node *> first_set;
            auto worker_set = _pattern->getWorkers();
            for (size_t i=0; i<n2; i++) {
                ff::ff_pipeline *stage = new ff::ff_pipeline();
                stage->add_stage(worker_set[i], false);
                if (lastParallelism != 1 || forceShuffling || _ordering == ID || _ordering == TS_RENUMBERING)
                    combine_with_firststage(*stage, new collector_t(_ordering), true);
                else // we avoid the ordering node when possible
                    combine_with_firststage(*stage, new dummy_mi(), true);
                first_set.push_back(stage);
            }
            matrioska->add_firstset(first_set, 0, true);
            std::vector<ff_node *> second_set;
            ff::ff_pipeline *stage = new ff::ff_pipeline();
            stage->add_stage(new selfkiller_node(), true);
            second_set.push_back(stage);
            matrioska->add_secondset(second_set, true);
            ff::ff_pipeline *previous = static_cast<ff::ff_pipeline *>((last->getSecondSet())[0]);
            previous->remove_stage(0); // remove the self-killer node
            previous->add_stage(matrioska, true); // Chinese boxes
            secondToLast = last;
            last = matrioska;
            // reset forceShuffling flag if it was true
            if (forceShuffling)
                forceShuffling = false;
        }
        // save parallelism of the operator
        lastParallelism = n2;
    }

    // generic method to chain an operator with the previous one in the MultiPipe (if it is possible)
    template<typename worker_t>
    bool chain_operator(ff::ff_farm *_pattern)
    {
        // check the Source and Sink presence
        if (!this->has_source) {
            std::cerr << RED << "WindFlow Error: Source is not defined for the MultiPipe [" << name << "]" << DEFAULT << std::endl;
            exit(EXIT_FAILURE);
        }
        if (this->has_sink) {
            std::cerr << RED << "WindFlow Error: MultiPipe [" << name << "]" << " is terminated by a Sink" << DEFAULT << std::endl;
            exit(EXIT_FAILURE);
        }
        size_t n1 = (last->getFirstSet()).size();
        size_t n2 = (_pattern->getWorkers()).size();
        // _pattern is for sure SIMPLE: check additional conditions for chaining
        if ((n1 == n2) && (!forceShuffling)) {
            auto first_set = last->getFirstSet();
            auto worker_set = _pattern->getWorkers();
            // chaining the pattern's workers with the last node of each pipeline in the first set of the matrioska
            for (size_t i=0; i<n1; i++) {
                ff::ff_pipeline *stage = static_cast<ff::ff_pipeline *>(first_set[i]);
                worker_t *worker = static_cast<worker_t *>(worker_set[i]);
                combine_with_laststage(*stage, worker, false);
            }
            // save parallelism of the operator
            lastParallelism = n2;
            return true;
        }
        else
            return false;
    }

    // method to prepare the MultiPipe for the union with other MultiPipe
    std::vector<ff_node *> prepare4Union()
    {
        // check the Source and Sink presence
        if (!this->has_source) {
            std::cerr << RED << "WindFlow Error: Source is not defined for the MultiPipe [" << name << "]" << DEFAULT << std::endl;
            exit(EXIT_FAILURE);
        }
        if (this->has_sink) {
            std::cerr << RED << "WindFlow Error: MultiPipe [" << name << "]" << " is terminated by a Sink" << DEFAULT << std::endl;
            exit(EXIT_FAILURE);
        }
        std::vector<ff_node *> result;
        // Case 1
        if (this->secondToLast == nullptr) {
            auto first_set = last->getFirstSet();
            for (size_t i=0; i<first_set.size(); i++)
                result.push_back(first_set[i]);
        }
        // Case 2
        else {
            last->cleanup_firstset(false);
            auto first_set_last = last->getFirstSet();
            std::vector<ff_node *> second_set_secondToLast;
            for (size_t i=0; i<first_set_last.size(); i++)
                second_set_secondToLast.push_back(first_set_last[i]);
            secondToLast->change_secondset(second_set_secondToLast, true);
            delete last;
            last = secondToLast;
            secondToLast = nullptr;
            result.push_back(this);
        }
        this->isUnified = true;
        return result;
    }

    // prepareInitSet method: base case 1
    std::vector<ff_node *> prepareInitSet() { return std::vector<ff_node *>(); }

    // prepareInitSet method: base case 2
    template<typename MULTIPIPE>
    std::vector<ff_node *> prepareInitSet(MULTIPIPE &_pipe) { return _pipe.prepare4Union(); }

    // prepareInitSet method: generic case
    template<typename MULTIPIPE, typename ...MULTIPIPES>
    std::vector<ff_node *> prepareInitSet(MULTIPIPE &_first, MULTIPIPES&... _pipes)
    {
        std::vector<ff_node *> v1 = _first.prepare4Union();
        std::vector<ff_node *> v2 = prepareInitSet(_pipes...);
        v1.insert(v1.end(), v2.begin(), v2.end());
        return v1;
    }

public:
	/** 
     *  \brief Constructor
     *  
     *  \param _name std::string with the unique name of the MultiPipe
     */ 
    MultiPipe(std::string _name="anonymous_pipe"):
              name(_name),
              has_source(false),
              has_sink(false),
              last(nullptr),
              secondToLast(nullptr),
              isUnified(false),
              forceShuffling(false),
              lastParallelism(0)
    {}

	/** 
     *  \brief Add a Source to the MultiPipe
     *  \param _source Source pattern to be added
     *  \return the modified MultiPipe
     */ 
    template<typename tuple_t>
    MultiPipe &add_source(Source<tuple_t> &_source)
    {
        // check the Source presence
        if (this->has_source) {
            std::cerr << RED << "WindFlow Error: Source is already defined for the MultiPipe [" << name << "]" << DEFAULT << std::endl;
            exit(EXIT_FAILURE);
        }
        // create the initial matrioska
        ff::ff_a2a *matrioska = new ff::ff_a2a();
        std::vector<ff_node *> first_set;
        auto workers = _source.getFirstSet();
        for (size_t i=0; i<workers.size(); i++) {
            ff::ff_pipeline *stage = new ff::ff_pipeline();
            stage->add_stage(workers[i], false);
            first_set.push_back(stage);
        }
        matrioska->add_firstset(first_set, 0, true);
        std::vector<ff_node *> second_set;
        ff::ff_pipeline *stage = new ff::ff_pipeline();
        stage->add_stage(new selfkiller_node(), true);
        second_set.push_back(stage);
        matrioska->add_secondset(second_set, true);
        this->add_stage(matrioska, true);
    	this->has_source = true;
    	this->last = matrioska;
        // save parallelism of the operator
        lastParallelism = workers.size();
    	return *this;
    }

	/** 
     *  \brief Add a Filter to the MultiPipe
     *  \param _filter Filter pattern to be added
     *  \return the modified MultiPipe
     */ 
    template<typename tuple_t>
    MultiPipe &add(Filter<tuple_t> &_filter)
    {
        // call the generic method to add the operator to the MultiPipe
        add_operator<Standard_Emitter<tuple_t>, Ordering_Node<tuple_t, tuple_t>>(&_filter, _filter.isKeyed() ? COMPLEX : SIMPLE, TS);
    	return *this;
    }

    /** 
     *  \brief Chain a Filter to the MultiPipe (if possible, otherwise add it)
     *  \param _filter Source pattern to be chained
     *  \return the modified MultiPipe
     */ 
    template<typename tuple_t>
    MultiPipe &chain(Filter<tuple_t> &_filter)
    {
        // try to chain the pattern with the MultiPipe
        if (!_filter.isKeyed()) {
            bool chained = chain_operator<typename Filter<tuple_t>::Filter_Node>(&_filter);
            if (!chained)
                add(_filter);
        }
        else
            add(_filter);
        return *this;
    }

	/** 
     *  \brief Add a Map to the MultiPipe
     *  \param _map Map pattern to be added
     *  \return the modified MultiPipe
     */ 
    template<typename tuple_t, typename result_t>
    MultiPipe &add(Map<tuple_t, result_t> &_map)
    {
        // call the generic method to add the operator to the MultiPipe
        add_operator<Standard_Emitter<tuple_t>, Ordering_Node<tuple_t, tuple_t>>(&_map, _map.isKeyed() ? COMPLEX : SIMPLE, TS);
    	return *this;
    }

    /** 
     *  \brief Chain a Map to the MultiPipe (if possible, otherwise add it)
     *  \param _map Map pattern to be chained
     *  \return the modified MultiPipe
     */ 
    template<typename tuple_t, typename result_t>
    MultiPipe &chain(Map<tuple_t, result_t> &_map)
    {
        // try to chain the pattern with the MultiPipe
        if (!_map.isKeyed()) {
            bool chained = chain_operator<typename Map<tuple_t, result_t>::Map_Node>(&_map);
            if (!chained)
                add(_map);
        }
        else
            add(_map);
        return *this;
    }

	/** 
     *  \brief Add a FlatMap to the MultiPipe
     *  \param _flatmap FlatMap pattern to be added
     *  \return the modified MultiPipe
     */ 
    template<typename tuple_t, typename result_t>
    MultiPipe &add(FlatMap<tuple_t, result_t> &_flatmap)
    {
        // call the generic method to add the operator
        add_operator<Standard_Emitter<tuple_t>, Ordering_Node<tuple_t, tuple_t>>(&_flatmap, _flatmap.isKeyed() ? COMPLEX : SIMPLE, TS);
    	return *this;
    }

    /** 
     *  \brief Chain a FlatMap to the MultiPipe (if possible, otherwise add it)
     *  \param _flatmap FlatMap pattern to be chained
     *  \return the modified MultiPipe
     */ 
    template<typename tuple_t, typename result_t>
    MultiPipe &chain(FlatMap<tuple_t, result_t> &_flatmap)
    {
        if (!_flatmap.isKeyed()) {
            bool chained = chain_operator<typename FlatMap<tuple_t, result_t>::FlatMap_Node>(&_flatmap);
            if (!chained)
                add(_flatmap);
        }
        else
            add(_flatmap);
        return *this;
    }

    /** 
     *  \brief Add an Accumulator to the MultiPipe
     *  \param _acc Accumulator pattern to be added
     *  \return the modified MultiPipe
     */ 
    template<typename tuple_t, typename result_t>
    MultiPipe &add(Accumulator<tuple_t, result_t> &_acc)
    {
        // call the generic method to add the operator to the MultiPipe
        add_operator<Standard_Emitter<tuple_t>, Ordering_Node<tuple_t, tuple_t>>(&_acc, COMPLEX, TS);
        return *this;
    }

	/** 
     *  \brief Add a Win_Farm to the MultiPipe
     *  \param _wf Win_Farm pattern to be added
     *  \return the modified MultiPipe
     */ 
    template<typename tuple_t, typename result_t>
    MultiPipe &add(Win_Farm<tuple_t, result_t> &_wf)
    {
        // ordering mode depends on the window type (TB or CB)
        ordering_mode_t ordering_mode = (_wf.getWinType() == TB) ? TS : TS_RENUMBERING;
        // check whether the internal replicas of the Win_Farm are complex or not
        if (_wf.useComplexNesting()) {
            if (_wf.getOptLevel() != LEVEL2 || _wf.getInnerOptLevel() != LEVEL2) {
                std::cerr << RED << "WindFlow Error: LEVEL2 optimization needed to add a complex nesting of patterns to the multipipe" << DEFAULT << std::endl;
                exit(EXIT_FAILURE);
            }
            else {
                // inner replica is a Pane_Farm
                if(_wf.getInnerType() == PF_CPU) {
                    // check the parallelism degree of the PLQ stage
                    if ((_wf.getInnerParallelism()).first > 1) {
                        if (_wf.getWinType() == TB) {
                            // call the generic method to add the operator to the MultiPipe
                            add_operator<TreeComb<WF_Emitter<tuple_t>, WF_Emitter<tuple_t, wrapper_tuple_t<tuple_t>>>, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_wf, COMPLEX, ordering_mode);
                        }
                        else {
                            // special case count-based windows
                            _wf.cleanup_emitter(false);
                            _wf.change_emitter(new Broadcast_Node<tuple_t>(_wf.getParallelism() * (_wf.getInnerParallelism()).first), true);
                            // call the generic method to add the operator to the MultiPipe
                            add_operator<Broadcast_Node<tuple_t>, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_wf, COMPLEX, ordering_mode);
                        }
                    }
                    else {
                        if (_wf.getWinType() == TB) {
                            // call the generic method to add the operator to the MultiPipe
                            add_operator<WF_Emitter<tuple_t>, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_wf, COMPLEX, ordering_mode);
                        }
                        else {
                            // special case count-based windows
                            _wf.change_emitter(new Broadcast_Node<tuple_t>(_wf.getParallelism()), true);
                            // call the generic method to add the operator to the MultiPipe
                            add_operator<Broadcast_Node<tuple_t>, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_wf, COMPLEX, TS_RENUMBERING);
                        }
                    }
                }
                // inner replica is a Win_MapReduce
                else if(_wf.getInnerType() == WMR_CPU) {
                    if (_wf.getWinType() == TB) {
                        // call the generic method to add the operator to the MultiPipe
                        add_operator<TreeComb<WF_Emitter<tuple_t>, WinMap_Emitter<tuple_t, wrapper_tuple_t<tuple_t>>>, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_wf, COMPLEX, ordering_mode); 
                    }
                    else {
                        // special case count-based windows
                        _wf.cleanup_emitter(false);
                        _wf.change_emitter(new Broadcast_Node<tuple_t>(_wf.getParallelism() * (_wf.getInnerParallelism()).first), true);
                        size_t n_map = (_wf.getInnerParallelism()).first;
                        for (size_t i=0; i<_wf.getParallelism(); i++) {
                            ff::ff_pipeline *pipe = static_cast<ff::ff_pipeline *>((_wf.getWorkers())[i]);
                            auto &stages = pipe->getStages();
                            ff::ff_a2a *a2a = static_cast<ff::ff_a2a *>(stages[0]);
                            std::vector<ff_node *> nodes;
                            for (size_t j=0; j<n_map; j++)
                                nodes.push_back(new WinMap_Dropper<tuple_t>(j, n_map));
                            combine_a2a_withFirstNodes(a2a, nodes, true);
                        }
                        // call the generic method to add the operator to the MultiPipe
                        add_operator<Broadcast_Node<tuple_t>, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_wf, COMPLEX, ordering_mode);                    
                    }
                }
                forceShuffling = true;
            }
        }
        else {
            if (_wf.getWinType() == TB) {
                // call the generic method to add the operator to the MultiPipe
                add_operator<WF_Emitter<tuple_t>, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_wf, COMPLEX, ordering_mode);
            }
            else {
                // special case count-based windows
                _wf.change_emitter(new Broadcast_Node<tuple_t>(_wf.getParallelism()), true);
                // call the generic method to add the operator to the MultiPipe
                add_operator<Broadcast_Node<tuple_t>, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_wf, COMPLEX, TS_RENUMBERING);        
            }
        }
        return *this;
    }

    /** 
     *  \brief Add a Win_Farm_GPU to the MultiPipe
     *  \param _wf Win_Farm_GPU pattern to be added
     *  \return the modified MultiPipe
     */ 
    template<typename tuple_t, typename result_t, typename F_t>
    MultiPipe &add(Win_Farm_GPU<tuple_t, result_t, F_t> &_wf)
    {
        // ordering mode depends on the window type (TB or CB)
        ordering_mode_t ordering_mode = (_wf.getWinType() == TB) ? TS : TS_RENUMBERING;
        // check whether the internal replicas of the Win_Farm_GPU are complex or not
        if (_wf.useComplexNesting()) {
            if (_wf.getOptLevel() != LEVEL2 || _wf.getInnerOptLevel() != LEVEL2) {
                std::cerr << RED << "WindFlow Error: LEVEL2 optimization needed to add a complex nesting of patterns to the multipipe" << DEFAULT << std::endl;
                exit(EXIT_FAILURE);
            }
            else {
                // inner replica is a Pane_Farm_GPU
                if(_wf.getInnerType() == PF_GPU) {
                    // check the parallelism degree of the PLQ stage
                    if ((_wf.getInnerParallelism()).first > 1) {
                        if (_wf.getWinType() == TB) {
                            // call the generic method to add the operator to the MultiPipe
                            add_operator<TreeComb<WF_Emitter<tuple_t>, WF_Emitter<tuple_t, wrapper_tuple_t<tuple_t>>>, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_wf, COMPLEX, ordering_mode);
                        }
                        else {
                            // special case count-based windows
                            _wf.cleanup_emitter(false);
                            _wf.change_emitter(new Broadcast_Node<tuple_t>(_wf.getParallelism() * (_wf.getInnerParallelism()).first), true);
                            // call the generic method to add the operator to the MultiPipe
                            add_operator<Broadcast_Node<tuple_t>, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_wf, COMPLEX, ordering_mode);
                        }
                    }
                    else {
                        if (_wf.getWinType() == TB) {
                            // call the generic method to add the operator to the MultiPipe
                            add_operator<WF_Emitter<tuple_t>, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_wf, COMPLEX, ordering_mode);
                        }
                        else {
                            // special case count-based windows
                            _wf.change_emitter(new Broadcast_Node<tuple_t>(_wf.getParallelism()), true);
                            // call the generic method to add the operator to the MultiPipe
                            add_operator<Broadcast_Node<tuple_t>, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_wf, COMPLEX, TS_RENUMBERING);
                        }
                    }
                }
                // inner replica is a Win_MapReduce_GPU
                else if(_wf.getInnerType() == WMR_GPU) {
                    if (_wf.getWinType() == TB) {
                        // call the generic method to add the operator to the MultiPipe
                        add_operator<TreeComb<WF_Emitter<tuple_t>, WinMap_Emitter<tuple_t, wrapper_tuple_t<tuple_t>>>, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_wf, COMPLEX, ordering_mode); 
                    }
                    else {
                        // special case count-based windows
                        _wf.cleanup_emitter(false);
                        _wf.change_emitter(new Broadcast_Node<tuple_t>(_wf.getParallelism() * (_wf.getInnerParallelism()).first), true);
                        size_t n_map = (_wf.getInnerParallelism()).first;
                        for (size_t i=0; i<_wf.getParallelism(); i++) {
                            ff::ff_pipeline *pipe = static_cast<ff::ff_pipeline *>((_wf.getWorkers())[i]);
                            auto &stages = pipe->getStages();
                            ff::ff_a2a *a2a = static_cast<ff::ff_a2a *>(stages[0]);
                            std::vector<ff_node *> nodes;
                            for (size_t j=0; j<n_map; j++)
                                nodes.push_back(new WinMap_Dropper<tuple_t>(j, n_map));
                            combine_a2a_withFirstNodes(a2a, nodes, true);
                        }
                        // call the generic method to add the operator to the MultiPipe
                        add_operator<Broadcast_Node<tuple_t>, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_wf, COMPLEX, ordering_mode);                    
                    }
                }
                forceShuffling = true;
            }
        }
        else {
            if (_wf.getWinType() == TB) {
                // call the generic method to add the operator to the MultiPipe
                add_operator<WF_Emitter<tuple_t>, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_wf, COMPLEX, ordering_mode);
            }
            else {
                // special case count-based windows
                _wf.change_emitter(new Broadcast_Node<tuple_t>(_wf.getParallelism()), true);
                // call the generic method to add the operator to the MultiPipe
                add_operator<Broadcast_Node<tuple_t>, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_wf, COMPLEX, TS_RENUMBERING);        
            }
        }
        return *this;
    }

	/** 
     *  \brief Add a Key_Farm to the MultiPipe
     *  \param _kf Key_Farm pattern to be added
     *  \return the modified MultiPipe
     */ 
    template<typename tuple_t, typename result_t>
    MultiPipe &add(Key_Farm<tuple_t, result_t> &_kf)
    {
        // ordering mode depends on the window type (TB or CB)
        ordering_mode_t ordering_mode = (_kf.getWinType() == TB) ? TS : TS_RENUMBERING;
        // check whether the internal replicas of the Key_Farm are complex or not
        if (_kf.useComplexNesting()) {
            if (_kf.getOptLevel() != LEVEL2 || _kf.getInnerOptLevel() != LEVEL2) {
                std::cerr << RED << "WindFlow Error: LEVEL2 optimization needed to add a complex nesting of patterns to the multipipe" << DEFAULT << std::endl;
                exit(EXIT_FAILURE);
            }
            else {
                // inner replica is a Pane_Farm
                if(_kf.getInnerType() == PF_CPU) {
                    // check the parallelism degree of the PLQ stage
                    if ((_kf.getInnerParallelism()).first > 1) {
                        if (_kf.getWinType() == TB) {
                            // call the generic method to add the operator to the MultiPipe
                            add_operator<TreeComb<KF_Emitter<tuple_t>, WF_Emitter<tuple_t>>, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_kf, COMPLEX, ordering_mode);
                        }
                        else {
                            // special case count-based windows
                            auto *emitter = static_cast<TreeComb<KF_Emitter<tuple_t>, WF_Emitter<tuple_t>>*>(_kf.getEmitter());
                            KF_Emitter<tuple_t> *rootnode = new KF_Emitter<tuple_t>(*(emitter->getRootNode()));
                            std::vector<Broadcast_Node<tuple_t>*> children;
                            size_t n_plq = (_kf.getInnerParallelism()).first;
                            for (size_t i=0; i<_kf.getParallelism(); i++) {
                                auto *b_node = new Broadcast_Node<tuple_t>(n_plq);
                                b_node->setTreeCombMode(true);
                                children.push_back(b_node);
                            }
                            auto *new_emitter = new TreeComb<KF_Emitter<tuple_t>, Broadcast_Node<tuple_t>>(rootnode, children, true, true);
                            _kf.cleanup_emitter(false);
                            _kf.change_emitter(new_emitter, true);
                            // call the generic method to add the operator to the MultiPipe
                            add_operator<TreeComb<KF_Emitter<tuple_t>, Broadcast_Node<tuple_t>>, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_kf, COMPLEX, ordering_mode);
                        }
                    }
                    else {
                        // call the generic method to add the operator to the MultiPipe
                        add_operator<KF_Emitter<tuple_t>, Ordering_Node<tuple_t>>(&_kf, COMPLEX, ordering_mode);
                    }
                }
                // inner replica is a Win_MapReduce
                else if(_kf.getInnerType() == WMR_CPU) {
                    if (_kf.getWinType() == TB) {
                        // call the generic method to add the operator to the MultiPipe
                        add_operator<TreeComb<KF_Emitter<tuple_t>, WinMap_Emitter<tuple_t>>, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_kf, COMPLEX, ordering_mode);           
                    }
                    else {
                        // special case count-based windows
                        auto *emitter = static_cast<TreeComb<KF_Emitter<tuple_t>, WinMap_Emitter<tuple_t>>*>(_kf.getEmitter());
                        KF_Emitter<tuple_t> *rootnode = new KF_Emitter<tuple_t>(*(emitter->getRootNode()));
                        std::vector<Broadcast_Node<tuple_t>*> children;
                        size_t n_map = (_kf.getInnerParallelism()).first;
                        for (size_t i=0; i<_kf.getParallelism(); i++) {
                            auto *b_node = new Broadcast_Node<tuple_t>(n_map);
                            b_node->setTreeCombMode(true);
                            children.push_back(b_node);
                        }
                        auto *new_emitter = new TreeComb<KF_Emitter<tuple_t>, Broadcast_Node<tuple_t>>(rootnode, children, true, true);
                        _kf.cleanup_emitter(false);
                        _kf.change_emitter(new_emitter, true);
                        for (size_t i=0; i<_kf.getParallelism(); i++) {
                            ff::ff_pipeline *pipe = static_cast<ff::ff_pipeline *>((_kf.getWorkers())[i]);
                            auto &stages = pipe->getStages();
                            ff::ff_a2a *a2a = static_cast<ff::ff_a2a *>(stages[0]);
                            std::vector<ff_node *> nodes;
                            for (size_t j=0; j<n_map; j++)
                                nodes.push_back(new WinMap_Dropper<tuple_t>(j, n_map));
                            combine_a2a_withFirstNodes(a2a, nodes, true);
                        }
                        // call the generic method to add the operator to the MultiPipe
                        add_operator<TreeComb<KF_Emitter<tuple_t>, Broadcast_Node<tuple_t>>, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_kf, COMPLEX, ordering_mode);                    
                    }
                }
                forceShuffling = true;
            }
        }
        else {
            // call the generic method to add the operator to the MultiPipe
            add_operator<KF_Emitter<tuple_t>, Ordering_Node<tuple_t>>(&_kf, COMPLEX, ordering_mode);
        }
        return *this;
    }

    /** 
     *  \brief Add a Key_Farm_GPU to the MultiPipe
     *  \param _kf Key_Farm_GPU pattern to be added
     *  \return the modified MultiPipe
     */ 
    template<typename tuple_t, typename result_t, typename F_t>
    MultiPipe &add(Key_Farm_GPU<tuple_t, result_t, F_t> &_kf)
    {
        // ordering mode depends on the window type (TB or CB)
        ordering_mode_t ordering_mode = (_kf.getWinType() == TB) ? TS : TS_RENUMBERING;
        // check whether the internal replicas of the Key_Farm_GPU are complex or not
        if (_kf.useComplexNesting()) {
            if (_kf.getOptLevel() != LEVEL2 || _kf.getInnerOptLevel() != LEVEL2) {
                std::cerr << RED << "WindFlow Error: LEVEL2 optimization needed to add a complex nesting of patterns to the multipipe" << DEFAULT << std::endl;
                exit(EXIT_FAILURE);
            }
            else {
                // inner replica is a Pane_Farm_GPU
                if(_kf.getInnerType() == PF_GPU) {
                    // check the parallelism degree of the PLQ stage
                    if ((_kf.getInnerParallelism()).first > 1) {
                        if (_kf.getWinType() == TB) {
                            // call the generic method to add the operator to the MultiPipe
                            add_operator<TreeComb<KF_Emitter<tuple_t>, WF_Emitter<tuple_t>>, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_kf, COMPLEX, ordering_mode);
                        }
                        else {
                            // special case count-based windows
                            auto *emitter = static_cast<TreeComb<KF_Emitter<tuple_t>, WF_Emitter<tuple_t>>*>(_kf.getEmitter());
                            KF_Emitter<tuple_t> *rootnode = new KF_Emitter<tuple_t>(*(emitter->getRootNode()));
                            std::vector<Broadcast_Node<tuple_t>*> children;
                            size_t n_plq = (_kf.getInnerParallelism()).first;
                            for (size_t i=0; i<_kf.getParallelism(); i++) {
                                auto *b_node = new Broadcast_Node<tuple_t>(n_plq);
                                b_node->setTreeCombMode(true);
                                children.push_back(b_node);
                            }
                            auto *new_emitter = new TreeComb<KF_Emitter<tuple_t>, Broadcast_Node<tuple_t>>(rootnode, children, true, true);
                            _kf.cleanup_emitter(false);
                            _kf.change_emitter(new_emitter, true);
                            // call the generic method to add the operator to the MultiPipe
                            add_operator<TreeComb<KF_Emitter<tuple_t>, Broadcast_Node<tuple_t>>, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_kf, COMPLEX, ordering_mode);
                        }
                    }
                    else {
                        // call the generic method to add the operator to the MultiPipe
                        add_operator<KF_Emitter<tuple_t>, Ordering_Node<tuple_t>>(&_kf, COMPLEX, ordering_mode);
                    }
                }
                // inner replica is a Win_MapReduce_GPU
                else if(_kf.getInnerType() == WMR_GPU) {
                    if (_kf.getWinType() == TB) {
                        // call the generic method to add the operator to the MultiPipe
                        add_operator<TreeComb<KF_Emitter<tuple_t>, WinMap_Emitter<tuple_t>>, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_kf, COMPLEX, ordering_mode);           
                    }
                    else {
                        // special case count-based windows
                        auto *emitter = static_cast<TreeComb<KF_Emitter<tuple_t>, WinMap_Emitter<tuple_t>>*>(_kf.getEmitter());
                        KF_Emitter<tuple_t> *rootnode = new KF_Emitter<tuple_t>(*(emitter->getRootNode()));
                        std::vector<Broadcast_Node<tuple_t>*> children;
                        size_t n_map = (_kf.getInnerParallelism()).first;
                        for (size_t i=0; i<_kf.getParallelism(); i++) {
                            auto *b_node = new Broadcast_Node<tuple_t>(n_map);
                            b_node->setTreeCombMode(true);
                            children.push_back(b_node);
                        }
                        auto *new_emitter = new TreeComb<KF_Emitter<tuple_t>, Broadcast_Node<tuple_t>>(rootnode, children, true, true);
                        _kf.cleanup_emitter(false);
                        _kf.change_emitter(new_emitter, true);
                        for (size_t i=0; i<_kf.getParallelism(); i++) {
                            ff::ff_pipeline *pipe = static_cast<ff::ff_pipeline *>((_kf.getWorkers())[i]);
                            auto &stages = pipe->getStages();
                            ff::ff_a2a *a2a = static_cast<ff::ff_a2a *>(stages[0]);
                            std::vector<ff_node *> nodes;
                            for (size_t j=0; j<n_map; j++)
                                nodes.push_back(new WinMap_Dropper<tuple_t>(j, n_map));
                            combine_a2a_withFirstNodes(a2a, nodes, true);
                        }
                        // call the generic method to add the operator to the MultiPipe
                        add_operator<TreeComb<KF_Emitter<tuple_t>, Broadcast_Node<tuple_t>>, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_kf, COMPLEX, ordering_mode);                    
                    }
                }
                forceShuffling = true;
            }
        }
        else {
            // call the generic method to add the operator to the MultiPipe
            add_operator<KF_Emitter<tuple_t>, Ordering_Node<tuple_t>>(&_kf, COMPLEX, ordering_mode);
        }
        return *this;
    }

	/** 
     *  \brief Add a Pane_Farm to the MultiPipe
     *  \param _pf Pane_Farm pattern to be added
     *  \return the modified MultiPipe
     */ 
    template<typename tuple_t, typename result_t>
    MultiPipe &add(Pane_Farm<tuple_t, result_t> &_pf)
    {
        // check the optimization level of the Pane_Farm
        if (_pf.getOptLevel() != LEVEL0) {
            std::cerr << RED << "WindFlow Error: LEVEL0 optimization needed to add the Pane_Farm to the multipipe" << DEFAULT << std::endl;
            exit(EXIT_FAILURE);
        }
        ff::ff_pipeline *pipe = static_cast<ff::ff_pipeline *>(&_pf);
        const ff::svector<ff_node *> &stages = pipe->getStages();
        ff::ff_farm *plq = nullptr;
        // check if the PLQ stage is a farm, otherwise prepare it
        if (!stages[0]->isFarm()) {
            plq = new ff::ff_farm();
            std::vector<ff_node *> w;
            w.push_back(stages[0]); // there is for sure one single worker in the PLQ
            plq->add_emitter(new Standard_Emitter<tuple_t>(1));
            plq->add_workers(w);
            plq->add_collector(nullptr);
            plq->cleanup_emitter(true);
            plq->cleanup_workers(false);
            // ordering mode depends on the window type (TB or CB)
            ordering_mode_t ordering_mode = (_pf.getWinType() == TB) ? TS : TS_RENUMBERING;
            // call the generic method to add the operator (PLQ stage) to the MultiPipe
            add_operator<Standard_Emitter<tuple_t>, Ordering_Node<tuple_t, tuple_t>>(plq, COMPLEX, ordering_mode);
            delete plq;
        }
        else {
            plq = static_cast<ff::ff_farm *>(stages[0]);
            // check the type of the windows
            if (_pf.getWinType() == TB) { // time-based windows
                // call the generic method to add the operator (PLQ stage) to the MultiPipe
                add_operator<WF_Emitter<tuple_t>, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(plq, COMPLEX, TS);
            }
            else {
                // special case count-based windows
                size_t n_plq = (plq->getWorkers()).size();
                plq->change_emitter(new Broadcast_Node<tuple_t>(n_plq), true);
                // call the generic method to add the operator
                add_operator<Broadcast_Node<tuple_t>, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(plq, COMPLEX, TS_RENUMBERING);
            }
        }
        ff::ff_farm *wlq = nullptr;
        // check if the WLQ stage is a farm, otherwise prepare it
        if (!stages[1]->isFarm()) {
            wlq = new ff::ff_farm();
            std::vector<ff_node *> w;
            w.push_back(stages[1]); // there is for sure one single worker in the WLQ
            wlq->add_emitter(new Standard_Emitter<result_t>(1));
            wlq->add_workers(w);
            wlq->add_collector(nullptr);
            wlq->cleanup_emitter(true);
            wlq->cleanup_workers(false);
            // call the generic method to add the operator (WLQ stage) to the MultiPipe
            add_operator<Standard_Emitter<result_t>, Ordering_Node<result_t, result_t>>(wlq, COMPLEX, ID);
            delete wlq;
        }
        else {
            wlq = static_cast<ff::ff_farm *>(stages[1]);
            // call the generic method to add the operator (WLQ stage) to the MultiPipe
            add_operator<WF_Emitter<result_t>, Ordering_Node<result_t, wrapper_tuple_t<result_t>>>(wlq, COMPLEX, ID);
        }
        return *this;
    }

    /** 
     *  \brief Add a Pane_Farm_GPU to the MultiPipe
     *  \param _pf Pane_Farm_GPU pattern to be added
     *  \return the modified MultiPipe
     */ 
    template<typename tuple_t, typename result_t, typename F_t>
    MultiPipe &add(Pane_Farm_GPU<tuple_t, result_t, F_t> &_pf)
    {
        // check the optimization level of the Pane_Farm_GPU
        if (_pf.getOptLevel() != LEVEL0) {
            std::cerr << RED << "WindFlow Error: LEVEL0 optimization needed to add the Pane_Farm_GPU to the multipipe" << DEFAULT << std::endl;
            exit(EXIT_FAILURE);
        }
        ff::ff_pipeline *pipe = static_cast<ff::ff_pipeline *>(&_pf);
        const ff::svector<ff_node *> &stages = pipe->getStages();
        ff::ff_farm *plq = nullptr;
        // check if the PLQ stage is a farm, otherwise prepare it
        if (!stages[0]->isFarm()) {
            plq = new ff::ff_farm();
            std::vector<ff_node *> w;
            w.push_back(stages[0]); // there is for sure one single worker in the PLQ
            plq->add_emitter(new Standard_Emitter<tuple_t>(1));
            plq->add_workers(w);
            plq->add_collector(nullptr);
            plq->cleanup_emitter(true);
            plq->cleanup_workers(false);
            // ordering mode depends on the window type (TB or CB)
            ordering_mode_t ordering_mode = (_pf.getWinType() == TB) ? TS : TS_RENUMBERING;
            // call the generic method to add the operator (PLQ stage) to the MultiPipe
            add_operator<Standard_Emitter<tuple_t>, Ordering_Node<tuple_t, tuple_t>>(plq, COMPLEX, ordering_mode);
            delete plq;
        }
        else {
            plq = static_cast<ff::ff_farm *>(stages[0]);
            // check the type of the windows
            if (_pf.getWinType() == TB) { // time-based windows
                // call the generic method to add the operator (PLQ stage) to the MultiPipe
                add_operator<WF_Emitter<tuple_t>, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(plq, COMPLEX, TS);
            }
            else {
                // special case count-based windows
                size_t n_plq = (plq->getWorkers()).size();
                plq->change_emitter(new Broadcast_Node<tuple_t>(n_plq), true);
                // call the generic method to add the operator
                add_operator<Broadcast_Node<tuple_t>, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(plq, COMPLEX, TS_RENUMBERING);
            }
        }
        ff::ff_farm *wlq = nullptr;
        // check if the WLQ stage is a farm, otherwise prepare it
        if (!stages[1]->isFarm()) {
            wlq = new ff::ff_farm();
            std::vector<ff_node *> w;
            w.push_back(stages[1]); // there is for sure one single worker in the WLQ
            wlq->add_emitter(new Standard_Emitter<result_t>(1));
            wlq->add_workers(w);
            wlq->add_collector(nullptr);
            wlq->cleanup_emitter(true);
            wlq->cleanup_workers(false);
            // call the generic method to add the operator (WLQ stage) to the MultiPipe
            add_operator<Standard_Emitter<result_t>, Ordering_Node<result_t, result_t>>(wlq, COMPLEX, ID);
            delete wlq;
        }
        else {
            wlq = static_cast<ff::ff_farm *>(stages[1]);
            // call the generic method to add the operator (WLQ stage) to the MultiPipe
            add_operator<WF_Emitter<result_t>, Ordering_Node<result_t, wrapper_tuple_t<result_t>>>(wlq, COMPLEX, ID);
        }
        return *this;
    }

	/** 
     *  \brief Add a Win_MapReduce to the MultiPipe
     *  \param _wmr Win_MapReduce pattern to be added
     *  \return the modified MultiPipe
     */ 
    template<typename tuple_t, typename result_t>
    MultiPipe &add(Win_MapReduce<tuple_t, result_t> &_wmr)
    {
        // check the optimization level of the Win_MapReduce
        if (_wmr.getOptLevel() != LEVEL0) {
            std::cerr << RED << "WindFlow Error: LEVEL0 optimization needed to add the Win_MapReduce to the multipipe" << DEFAULT << std::endl;
            exit(EXIT_FAILURE);
        }
        // add the MAP stage
        ff::ff_pipeline *pipe = static_cast<ff::ff_pipeline *>(&_wmr);
        const ff::svector<ff_node *> &stages = pipe->getStages();
        ff::ff_farm *map = static_cast<ff::ff_farm *>(stages[0]);
        // check the type of the windows
        if (_wmr.getWinType() == TB) { // time-based windows
            // call the generic method to add the operator (MAP stage) to the MultiPipe
            add_operator<WinMap_Emitter<tuple_t>, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(map, COMPLEX, TS);
        }
        else {
            // special case count-based windows
            size_t n_map = (map->getWorkers()).size();
            ff::ff_farm *new_map = new ff::ff_farm();
            auto worker_set = map->getWorkers();
            std::vector<ff_node *> w;
            new_map->add_emitter(new Broadcast_Node<tuple_t>(n_map));
            for (size_t i=0; i<n_map; i++) {
                ff::ff_comb *comb = new ff::ff_comb(new WinMap_Dropper<tuple_t>(i, n_map), worker_set[i], true, false);
                w.push_back(comb);
            }
            new_map->add_workers(w);
            new_map->add_collector(nullptr);
            new_map->cleanup_emitter(true);
            new_map->cleanup_workers(false);
            // call the generic method to add the operator (MAP stage) to the MultiPipe
            add_operator<Broadcast_Node<tuple_t>, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(new_map, COMPLEX, TS_RENUMBERING);
            delete new_map;
        }
        // add the REDUCE stage
        ff::ff_farm *reduce = nullptr;
        // check if the REDUCE stage is a farm, otherwise prepare it
        if (!stages[1]->isFarm()) {
            reduce = new ff::ff_farm();
            std::vector<ff_node *> w;
            w.push_back(stages[1]);
            reduce->add_emitter(new Standard_Emitter<result_t>(1));
            reduce->add_workers(w);
            reduce->add_collector(nullptr);
            reduce->cleanup_emitter(true);
            reduce->cleanup_workers(false);
            // call the generic method to add the operator (REDUCE stage) to the MultiPipe
            add_operator<Standard_Emitter<result_t>, Ordering_Node<result_t, result_t>>(reduce, COMPLEX, ID);
            delete reduce;
        }
        else {
            reduce = static_cast<ff::ff_farm *>(stages[1]);
            // call the generic method to add the operator (REDUCE stage) to the MultiPipe
            add_operator<WF_Emitter<result_t>, Ordering_Node<result_t, wrapper_tuple_t<result_t>>>(reduce, COMPLEX, ID);
        }
        return *this;
    }

    /** 
     *  \brief Add a Win_MapReduce_GPU to the MultiPipe
     *  \param _wmr Win_MapReduce_GPU pattern to be added
     *  \return the modified MultiPipe
     */ 
    template<typename tuple_t, typename result_t, typename F_t>
    MultiPipe &add(Win_MapReduce_GPU<tuple_t, result_t, F_t> &_wmr)
    {
        // check the optimization level of the Win_MapReduce_GPU
        if (_wmr.getOptLevel() != LEVEL0) {
            std::cerr << RED << "WindFlow Error: LEVEL0 optimization needed to add the Win_MapReduce_GPU to the multipipe" << DEFAULT << std::endl;
            exit(EXIT_FAILURE);
        }
        // add the MAP stage
        ff::ff_pipeline *pipe = static_cast<ff::ff_pipeline *>(&_wmr);
        const ff::svector<ff_node *> &stages = pipe->getStages();
        ff::ff_farm *map = static_cast<ff::ff_farm *>(stages[0]);
        // check the type of the windows
        if (_wmr.getWinType() == TB) { // time-based windows
            // call the generic method to add the operator (MAP stage) to the MultiPipe
            add_operator<WinMap_Emitter<tuple_t>, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(map, COMPLEX, TS);
        }
        else {
            // special case count-based windows
            size_t n_map = (map->getWorkers()).size();
            ff::ff_farm *new_map = new ff::ff_farm();
            auto worker_set = map->getWorkers();
            std::vector<ff_node *> w;
            new_map->add_emitter(new Broadcast_Node<tuple_t>(n_map));
            for (size_t i=0; i<n_map; i++) {
                ff::ff_comb *comb = new ff::ff_comb(new WinMap_Dropper<tuple_t>(i, n_map), worker_set[i], true, false);
                w.push_back(comb);
            }
            new_map->add_workers(w);
            new_map->add_collector(nullptr);
            new_map->cleanup_emitter(true);
            new_map->cleanup_workers(false);
            // call the generic method to add the operator (MAP stage) to the MultiPipe
            add_operator<Broadcast_Node<tuple_t>, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(new_map, COMPLEX, TS_RENUMBERING);
            delete new_map;
        }
        // add the REDUCE stage
        ff::ff_farm *reduce = nullptr;
        // check if the REDUCE stage is a farm, otherwise prepare it
        if (!stages[1]->isFarm()) {
            reduce = new ff::ff_farm();
            std::vector<ff_node *> w;
            w.push_back(stages[1]);
            reduce->add_emitter(new Standard_Emitter<result_t>(1));
            reduce->add_workers(w);
            reduce->add_collector(nullptr);
            reduce->cleanup_emitter(true);
            reduce->cleanup_workers(false);
            // call the generic method to add the operator (REDUCE stage) to the MultiPipe
            add_operator<Standard_Emitter<result_t>, Ordering_Node<result_t, result_t>>(reduce, COMPLEX, ID);
            delete reduce;
        }
        else {
            reduce = static_cast<ff::ff_farm *>(stages[1]);
            // call the generic method to add the operator (REDUCE stage) to the MultiPipe
            add_operator<WF_Emitter<result_t>, Ordering_Node<result_t, wrapper_tuple_t<result_t>>>(reduce, COMPLEX, ID);
        }
        return *this;
    }

	/** 
     *  \brief Add a Sink to the MultiPipe
     *  \param _sink Sink pattern to be added
     *  \return the modified MultiPipe
     */ 
    template<typename tuple_t>
    MultiPipe &add_sink(Sink<tuple_t> &_sink)
    {
        // call the generic method to add the operator to the MultiPipe
        add_operator<Standard_Emitter<tuple_t>, Ordering_Node<tuple_t, tuple_t>>(&_sink, _sink.isKeyed() ? COMPLEX : SIMPLE, TS);
    	this->has_sink = true;
    	return *this;
    }

    /** 
     *  \brief Chain a Sink to the MultiPipe (if possible, otherwise add it)
     *  \param _sink Sink pattern to be chained
     *  \return the modified MultiPipe
     */ 
    template<typename tuple_t>
    MultiPipe &chain_sink(Sink<tuple_t> &_sink)
    {
        // try to chain the Sink with the MultiPipe
        if (!_sink.isKeyed()) {
            bool chained = chain_operator<typename Sink<tuple_t>::Sink_Node>(&_sink);
            if (!chained)
                add_sink(_sink);
        }
        else
            add_sink(_sink);
        this->has_sink = true;
        return *this;
    }

#if __cplusplus >= 201703L
    /** 
     *  \brief Union of the MultiPipe with a set of other MultiPipe (only C++17)
     *  \param _name std::string with the unique name of the new MultiPipe
     *  \param _pipes sequence of MultiPipe
     *  \return a new MultiPipe (the result of the union between this and _pipes)
     */ 
    template<typename ...MULTIPIPES>
    MultiPipe unionMultiPipes(std::string _name="anonymous_union_pipe", MULTIPIPES&... _pipes)
    {
        std::vector<ff_node *> init_set = prepareInitSet(*this, _pipes...);
        return MultiPipe(_name, init_set);
    }
#endif

    /** 
     *  \brief Union of the MultiPipe with a set of other MultiPipe
     *  \param _name std::string with the unique name of the new MultiPipe
     *  \param _pipes sequence of MultiPipe
     *  \return a pointer to a new MultiPipe (the result of the union between this and _pipes)
     */ 
    template<typename ...MULTIPIPES>
    MultiPipe *unionMultiPipes_ptr(std::string _name="anonymous_union_pipe", MULTIPIPES&... _pipes)
    {
        std::vector<ff_node *> init_set = prepareInitSet(*this, _pipes...);
        return new MultiPipe(_name, init_set);
    }

    /** 
     *  \brief Union of the MultiPipe with a set of other MultiPipe
     *  \param _name std::string with the unique name of the new MultiPipe
     *  \param _pipes sequence of MultiPipe
     *  \return a unique pointer to a new MultiPipe (the result of the union between this and _pipes)
     */ 
    template<typename ...MULTIPIPES>
    std::unique_ptr<MultiPipe> unionMultiPipes_unique(std::string _name="anonymous_union_pipe", MULTIPIPES&... _pipes)
    {
        std::vector<ff_node *> init_set = prepareInitSet(*this, _pipes...);
        return std::make_unique<MultiPipe>(_name, init_set);
    }

	/** 
     *  \brief Check whether the MultiPipe is runnable (i.e. it has a Source and a Sink)
     *  \return true if it is runnable, false otherwise
     */ 
    bool isRunnable() const
    {
    	return (has_source && has_sink);
    }

	/** 
     *  \brief Check whether the MultiPipe has a Source
     *  \return true if it has a defined Source, false otherwise
     */ 
    bool hasSource() const
    {
    	return has_source;
    }

	/** 
     *  \brief Check whether the MultiPipe has a Sink
     *  \return true if it has a defined Sink, false otherwise
     */ 
    bool hasSink() const
    {
    	return has_sink;
    }

    /** 
     *  \brief Return the number of raw threads used by this MultiPipe
     *  \return the number of raw threads used by the FastFlow run-time system to run the MultiPipe
     */ 
    size_t getNumThreads() const
    {
        return this->cardinality()-1;
    }

	/** 
     *  \brief Asynchronous run of the MultiPipe
     *  \return zero in case of success, non-zero otherwise
     */
    int run()
    {
        if (!this->isUnified)
            std::cout << BOLDGREEN << "WindFlow Status Message: MultiPipe [" << name << "] is running with " << this->getNumThreads() << " threads" << DEFAULT << std::endl;
    	int status = ff::ff_pipeline::run();
    	if (status != 0 && !this->isUnified)
    		std::cerr << RED << "WindFlow Error: MultiPipe [" << name << "] run failed" << DEFAULT << std::endl;
    	return status;
    }

	/** 
     *  \brief Wait the termination of an already running MultiPipe
     *  \return zero in case of success, not-zero otherwise
     */ 
    int wait()
    {
    	int status = ff::ff_pipeline::wait();
    	if (status == 0 && !this->isUnified)
    		std::cout << BOLDGREEN << "WindFlow Status Message: MultiPipe [" << name << "] terminated successfully" << DEFAULT << std::endl;
    	else if(!this->isUnified)
    		std::cerr << RED << "WindFlow Error: MultiPipe [" << name << "] terminated with error" << DEFAULT << std::endl;
        return status;
    }

	/** 
     *  \brief Synchronous run of the MultiPipe
     *         (calling thread is blocked until the streaming execution is complete)
     *  \return zero in case of success, non-zero otherwise
     */ 
    int run_and_wait_end()
    {
        if (!this->isUnified)
            std::cout << BOLDGREEN << "WindFlow Status Message: MultiPipe [" << name << "] is running with " << this->getNumThreads() << " threads" << DEFAULT << std::endl;
    	return ff::ff_pipeline::run_and_wait_end();
    }
};

} // namespace wf

#endif
