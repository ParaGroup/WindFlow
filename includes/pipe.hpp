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
 *  @file    pipe.hpp
 *  @author  Gabriele Mencagli
 *  @date    14/01/2019
 *  
 *  @brief Multi-Pipe construct to compose a complex pipeline of streaming patterns
 *  
 *  @section Multi-Pipe Construct (Description)
 *  
 *  This file implements the Multi-Pipe construct used to build a complex pipeline
 *  of WindFlow patterns. A runnable Multi-Pipe instance (i.e. with a Source, a
 *  sequence of pattern instances, and a Sink) can be executed by the user.
 */ 

#ifndef PIPE_H
#define PIPE_H

// includes
#include <string>
#include <vector>
#include <math.h>
#include <ff/ff.hpp>
#include <basic.hpp>
#include <dummy.hpp>
#include <wm_nodes.hpp>
#include <wf_nodes.hpp>
#include <kf_nodes.hpp>
#include <orderingNode.hpp>

//@cond DOXY_IGNORE

// class for broadcasting input tuples (broadcast_node)
template<typename tuple_t>
class broadcast_node: public ff_monode_t<tuple_t, wrapper_tuple_t<tuple_t>>
{
private:
    // number of output channels used to broadcast each input tuple
    size_t n;
    // struct of a key descriptor
    struct Key_Descriptor
    {
        uint64_t rcv_counter; // number of tuples received of this key
        tuple_t last_tuple; // copy of the last tuple received of this key

        // constructor
        Key_Descriptor(): rcv_counter(0) {}
    };
    unordered_map<size_t, Key_Descriptor> keyMap; // hash table that maps a descriptor for each key

public:
    // constructor
    broadcast_node(size_t _n): n(_n) {}

    // svc_init method (utilized by the FastFlow runtime)
    int svc_init()
    {
        return 0;
    }

    // svc method (utilized by the FastFlow runtime)
    wrapper_tuple_t<tuple_t> *svc(tuple_t *t)
    {
        size_t key = std::get<0>(t->getInfo()); // key
        // access the descriptor of the input key
        auto it = keyMap.find(key);
        if (it == keyMap.end()) {
            // create the descriptor of that key
            keyMap.insert(make_pair(key, Key_Descriptor()));
            it = keyMap.find(key);
        }
        Key_Descriptor &key_d = (*it).second;
        key_d.rcv_counter++;
        key_d.last_tuple = *t;
        wrapper_tuple_t<tuple_t> *wt = new wrapper_tuple_t<tuple_t>(t, n);
        for(size_t i=0; i<n; i++)
            this->ff_send_out_to(wt, i);
        return this->GO_ON;
    }

    // method to manage the EOS (utilized by the FastFlow runtime)
    void eosnotify(ssize_t id)
    {
        // iterate over all the keys
        for (auto &k: keyMap) {
            Key_Descriptor &key_d = k.second;
            if (key_d.rcv_counter > 0) {
                // send the last tuple as an EOS marker
                tuple_t *tuple = new tuple_t();
                *tuple = key_d.last_tuple;
                wrapper_tuple_t<tuple_t> *out = new wrapper_tuple_t<tuple_t>(tuple, n, true); // eos marker enabled
                for(size_t i=0; i<n; i++)
                    this->ff_send_out_to(out, i);
            }
        }
    }

    // svc_end method (utilized by the FastFlow runtime)
    void svc_end() {}
};

//@endcond

/** 
 *  \class Pipe
 *  
 *  \brief Multi-Pipe construct to compose a complex pipeline of streaming patterns
 *  
 *  This class implements the Pipe construct used to build a complex linear composition
 *  of instances belonging to the patterns available in the WindFlow library.
 */ 
class Pipe: public ff_pipeline
{
private:
    // enumeration of the routing types
    enum routing_types_t { SIMPLE, COMPLEX };
	string name; // string with the unique name of the Multi-Pipe instance
	bool has_source; // true if the Multi-Pipe already has a Source instance
	bool has_sink; // true if the Multi-Pipe ends with a Sink instance
	ff_a2a *last; // pointer to the last matrioska
    ff_a2a *secondToLast; // pointer to the second to last matrioska
    bool isUnified; // true if the Multi-Pipe is used in a union with other Multi-Pipe instances
    bool startUnion; // true if we are at the beginning of a unified Multi-Pipe instance
    // class of the self-killer multi-input node (selfkiller_node)
    class selfkiller_node: public ff_minode
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

    // private constructor used by union only
    Pipe(string _name, vector<ff_node *> _init_set): name(_name), has_source(true), has_sink(false), isUnified(false), startUnion(true)
    {
        // create the initial matrioska
        ff_a2a *matrioska = new ff_a2a();
        matrioska->add_firstset(_init_set, 0, false);
        vector<ff_node *> second_set;
        ff_pipeline *stage = new ff_pipeline();
        stage->add_stage(new selfkiller_node(), true);
        second_set.push_back(stage);
        matrioska->add_secondset(second_set, true);
        this->add_stage(matrioska, true);
        this->last = matrioska;
        this->secondToLast = nullptr;
    }

    // generic method to add an operator to the Multi-Pipe
    template<typename emitter_t, typename collector_t>
    void add_operator(ff_farm *_pattern, routing_types_t _type, ordering_mode_t _ordering)
    {
        // check the Source and Sink presence
        if (!this->has_source) {
            cerr << RED << "WindFlow Error: Source is not defined for the Multi-Pipe [" << name << "]" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        if (this->has_sink) {
            cerr << RED << "WindFlow Error: Multi-Pipe [" << name << "]" << " is terminated by a Sink" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        size_t n1 = (last->getFirstSet()).size();
        size_t n2 = (_pattern->getWorkers()).size();
        // Case 1: direct connection
        if ((n1 == n2) && _type == SIMPLE && !startUnion) {
            auto first_set = last->getFirstSet();
            auto worker_set = _pattern->getWorkers();
            // add the pattern's workers to the pipelines in the first set of the matrioska
            for (size_t i=0; i<n1; i++) {
                ff_pipeline *stage = static_cast<ff_pipeline *>(first_set[i]);
                stage->add_stage(worker_set[i], false);
            }
        }
        // Case 2: shuffling connection
        else {
            // prepare the nodes of the first_set of the last matrioska for the shuffling
            auto first_set_m = last->getFirstSet();
            for (size_t i=0; i<first_set_m.size(); i++) {
                ff_pipeline *stage = static_cast<ff_pipeline *>(first_set_m[i]);
                if (_type == COMPLEX) {
                    emitter_t *tmp_e = static_cast<emitter_t *>(_pattern->getEmitter());
                    combine_with_laststage(*stage, new emitter_t(*tmp_e), true);
                }
                else
                    combine_with_laststage(*stage, new dummy_emitter(), true);
            }
            // create a new matrioska
            ff_a2a *matrioska = new ff_a2a();
            vector<ff_node *> first_set;
            auto worker_set = _pattern->getWorkers();
            for (size_t i=0; i<n2; i++) {
                ff_pipeline *stage = new ff_pipeline();
                ff_node *node_c = (ff_node *) new collector_t(_ordering);
                ff_comb *comb = new ff_comb(node_c, worker_set[i], true, false);
                stage->add_stage(comb, true);
                first_set.push_back(stage);
            }
            matrioska->add_firstset(first_set, 0, true);
            vector<ff_node *> second_set;
            ff_pipeline *stage = new ff_pipeline();
            stage->add_stage(new selfkiller_node(), true);
            second_set.push_back(stage);
            matrioska->add_secondset(second_set, true);
            ff_pipeline *previous = static_cast<ff_pipeline *>((last->getSecondSet())[0]);
            previous->remove_stage(0);
            previous->add_stage(matrioska, true); // Chinese boxes
            secondToLast = last;
            last = matrioska;
            // reset startUnion flag if it was true
            if (startUnion)
                startUnion = false;
        }
    }

    // generic method to chain an operator with the previous one in the Multi-Pipe (if possible)
    template<typename worker_t>
    bool chain_operator(ff_farm *_pattern)
    {
        // check the Source and Sink presence
        if (!this->has_source) {
            cerr << RED << "WindFlow Error: Source is not defined for the Multi-Pipe [" << name << "]" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        if (this->has_sink) {
            cerr << RED << "WindFlow Error: Multi-Pipe [" << name << "]" << " is terminated by a Sink" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        size_t n1 = (last->getFirstSet()).size();
        size_t n2 = (_pattern->getWorkers()).size();
        // _pattern is for sure SIMPLE: check additional conditions for doing chaining
        if ((n1 == n2) && (!startUnion)) {
            auto first_set = last->getFirstSet();
            auto worker_set = _pattern->getWorkers();
            // chaining the pattern's workers with the last node of each pipeline in the first set of the matrioska
            for (size_t i=0; i<n1; i++) {
                ff_pipeline *stage = static_cast<ff_pipeline *>(first_set[i]);
                worker_t *worker = reinterpret_cast<worker_t *>(worker_set[i]);
                combine_with_laststage<worker_t>(*stage, worker, false);
            }
            return true;
        }
        else
            return false;
    }

    // method to prepare the Multi-Pipe for the union with other Multi-Pipe instances
    vector<ff_node *> prepare4Union()
    {
        // check the Source and Sink presence
        if (!this->has_source) {
            cerr << RED << "WindFlow Error: Source is not defined for the Multi-Pipe [" << name << "]" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        if (this->has_sink) {
            cerr << RED << "WindFlow Error: Multi-Pipe [" << name << "]" << " is terminated by a Sink" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        vector<ff_node *> result;
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
            vector<ff_node *> second_set_secondToLast;
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
    vector<ff_node *> prepareInitSet() { return vector<ff_node *>(); }

    // prepareInitSet method: base case 2
    template<typename PIPE>
    vector<ff_node *> prepareInitSet(PIPE &_pipe) { return _pipe.prepare4Union(); }

    // prepareInitSet method: generic case
    template<typename PIPE, typename ...PIPES>
    vector<ff_node *> prepareInitSet(PIPE &_first, PIPES&... _pipes)
    {
        vector<ff_node *> v1 = _first.prepare4Union();
        vector<ff_node *> v2 = prepareInitSet(_pipes...);
        v1.insert(v1.end(), v2.begin(), v2.end());
        return v1;
    }

public:
	/** 
     *  \brief Constructor
     *  
     *  \param _name string with the unique name of the Multi-Pipe instance
     */ 
    Pipe(string _name="anonymous_pipe"): name(_name), has_source(false), has_sink(false), last(nullptr), secondToLast(nullptr), isUnified(false), startUnion(false) {}

	/** 
     *  \brief Add a Source to the Multi-Pipe instance
     *  \param _source Source pattern to be added
     *  \return the modified Multi-Pipe instance
     */
    template<typename tuple_t>
    Pipe &add_source(Source<tuple_t> &_source)
    {
        // check the Source and Sink presence
        if (this->has_source) {
            cerr << RED << "WindFlow Error: Source is already defined for the Multi-Pipe [" << name << "]" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // create the initial matrioska
        ff_a2a *matrioska = new ff_a2a();
        vector<ff_node *> first_set;
        auto workers = _source.getFirstSet();
        for (size_t i=0; i<workers.size(); i++) {
            ff_pipeline *stage = new ff_pipeline();
            stage->add_stage(workers[i], false);
            first_set.push_back(stage);
        }
        matrioska->add_firstset(first_set, 0, true);
        vector<ff_node *> second_set;
        ff_pipeline *stage = new ff_pipeline();
        stage->add_stage(new selfkiller_node(), true);
        second_set.push_back(stage);
        matrioska->add_secondset(second_set, true);
        this->add_stage(matrioska, true);
    	this->has_source = true;
    	this->last = matrioska;
    	return *this;
    }

	/** 
     *  \brief Add a Filter to the Multi-Pipe instance
     *  \param _filter Filter pattern to be added
     *  \return the modified Multi-Pipe instance
     */
    template<typename tuple_t>
    Pipe &add(Filter<tuple_t> &_filter)
    {
        // call the generic method to add the operator to the Multi-Pipe
        add_operator<dummy_emitter, OrderingNode<tuple_t, tuple_t>>(&_filter, SIMPLE, TS);
    	return *this;
    }

    /** 
     *  \brief Chain a Filter to the Multi-Pipe instance (if possible, otherwise add)
     *  \param _filter Source pattern to be chained
     *  \return the modified Multi-Pipe instance
     */
    template<typename tuple_t>
    Pipe &chain(Filter<tuple_t> &_filter)
    {
        // try to chain the pattern with the Multi-Pipe
        bool chained = chain_operator<typename Filter<tuple_t>::Filter_Node>(&_filter);
        if (!chained) {
            // add the operator to the Multi-Pipe
            add(_filter);
        }
        return *this;
    }

	/** 
     *  \brief Add a Map to the Multi-Pipe instance
     *  \param _map Map pattern to be added
     *  \return the modified Multi-Pipe instance
     */
    template<typename tuple_t, typename result_t>
    Pipe &add(Map<tuple_t, result_t> &_map)
    {
        // call the generic method to add the operator to the Multi-Pipe
        add_operator<dummy_emitter, OrderingNode<tuple_t, tuple_t>>(&_map, SIMPLE, TS);
    	return *this;
    }

    /** 
     *  \brief Chain a Map to the Multi-Pipe instance (if possible, otherwise add)
     *  \param _map Map pattern to be chained
     *  \return the modified Multi-Pipe instance
     */
    template<typename tuple_t, typename result_t>
    Pipe &chain(Map<tuple_t, result_t> &_map)
    {
        // try to chain the pattern with the Multi-Pipe
        bool chained = chain_operator<typename Map<tuple_t, result_t>::Map_Node>(&_map);
        if (!chained) {
            // add the operator to the Multi-Pipe
            add(_map);
        }
        return *this;
    }

	/** 
     *  \brief Add a FlatMap to the Multi-Pipe instance
     *  \param _flatmap FlatMap pattern to be added
     *  \return the modified Multi-Pipe instance
     */
    template<typename tuple_t, typename result_t>
    Pipe &add(FlatMap<tuple_t, result_t> &_flatmap)
    {
        // call the generic method to add the operator
        add_operator<dummy_emitter, OrderingNode<tuple_t, tuple_t>>(&_flatmap, SIMPLE, TS);
    	return *this;
    }

    /** 
     *  \brief Chain a FlatMap to the Multi-Pipe instance (if possible, otherwise add)
     *  \param _flatmap FlatMap pattern to be chained
     *  \return the modified Multi-Pipe instance
     */
    template<typename tuple_t, typename result_t>
    Pipe &chain(FlatMap<tuple_t, result_t> &_flatmap)
    {
        // try to chain the pattern with the Multi-Pipe
        bool chained = chain_operator<typename FlatMap<tuple_t, result_t>::FlatMap_Node>(&_flatmap);
        if (!chained) {
            // add the operator to the Multi-Pipe
            add(_flatmap);
        }
        return *this;
    }

    /** 
     *  \brief Add an Accumulator to the Multi-Pipe instance
     *  \param _acc Accumulator pattern to be added
     *  \return the modified Multi-Pipe instance
     */
    template<typename tuple_t, typename result_t>
    Pipe &add(Accumulator<tuple_t, result_t> &_acc)
    {
        // call the generic method to add the operator to the Multi-Pipe
        add_operator<Accumulator_Emitter<tuple_t>, OrderingNode<tuple_t, tuple_t>>(&_acc, COMPLEX, TS);
        return *this;
    }

	/** 
     *  \brief Add a Win_Farm to the Multi-Pipe instance
     *  \param _wf Win_Farm pattern to be added
     *  \return the modified Multi-Pipe instance
     */
    template<typename tuple_t, typename result_t>
    Pipe &add(Win_Farm<tuple_t, result_t> &_wf)
    {
        // check whether the internal instances of the Win_Farm are complex or not
        if (_wf.useComplexNesting()) {
            cerr << RED << "WindFlow Error: Multi-Pipe does not support Complex Nested Win_Farm instances yet" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // check the number of emitters of the Win_Farm
        if (_wf.getNumEmitters() != 1) {
            cerr << RED << "WindFlow Error: a Win_Farm instance with multiple emitters cannot be added to a Multi-Pipe" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // check the type of the windows used by the Win_Farm pattern
        if (_wf.getWinType() == TB) { // time-based windows
            // call the generic method to add the operator to the Multi-Pipe
            add_operator<WF_Emitter<tuple_t>, OrderingNode<tuple_t, wrapper_tuple_t<tuple_t>>>(&_wf, COMPLEX, TS);
        }
        else { // count-based windows
            ff_farm *wf_farm = static_cast<ff_farm *>(&_wf);
            size_t n = (wf_farm->getWorkers()).size();
            wf_farm->change_emitter(new broadcast_node<tuple_t>(n));
            // call the generic method to add the operator to the Multi-Pipe
            add_operator<broadcast_node<tuple_t>, OrderingNode<tuple_t, wrapper_tuple_t<tuple_t>>>(&_wf, COMPLEX, TS_RENUMBERING);
        }
    	return *this;
    }

    /** 
     *  \brief Add a Win_Farm_GPU to the Multi-Pipe instance
     *  \param _wf Win_Farm_GPU pattern to be added
     *  \return the modified Multi-Pipe instance
     */
    template<typename tuple_t, typename result_t, typename F_t>
    Pipe &add(Win_Farm_GPU<tuple_t, result_t, F_t> &_wf)
    {
        // check whether the internal instances of the Win_Farm_GPU are complex or not
        if (_wf.useComplexNesting()) {
            cerr << RED << "WindFlow Error: Multi-Pipe does not support Complex Nested Win_Farm_GPU instances yet" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // check the number of emitters of the Win_Farm_GPU
        if (_wf.getNumEmitters() != 1) {
            cerr << RED << "WindFlow Error: a Win_Farm_GPU instance with multiple emitters cannot be added to a Multi-Pipe" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // check the type of the windows used by the Win_Farm_GPU pattern
        if (_wf.getWinType() == TB) { // time-based windows
            // call the generic method to add the operator to the Multi-Pipe
            add_operator<WF_Emitter<tuple_t>, OrderingNode<tuple_t, wrapper_tuple_t<tuple_t>>>(&_wf, COMPLEX, TS);
        }
        else { // count-based windows
            ff_farm *wf_farm = static_cast<ff_farm *>(&_wf);
            size_t n = (wf_farm->getWorkers()).size();
            wf_farm->change_emitter(new broadcast_node<tuple_t>(n));
            // call the generic method to add the operator to the Multi-Pipe
            add_operator<broadcast_node<tuple_t>, OrderingNode<tuple_t, wrapper_tuple_t<tuple_t>>>(&_wf, COMPLEX, TS_RENUMBERING);
        }
        return *this;
    }

	/** 
     *  \brief Add a Key_Farm to the Multi-Pipe instance
     *  \param _kf Key_Farm pattern to be added
     *  \return the modified Multi-Pipe instance
     */
    template<typename tuple_t, typename result_t>
    Pipe &add(Key_Farm<tuple_t, result_t> &_kf)
    {
        // check whether the internal instances of the Key_Farm are complex or not
        if (_kf.useComplexNesting()) {
            cerr << RED << "WindFlow Error: Multi-Pipe does not support Complex Nested Key_Farm instances yet" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // check the type of the windows used by the Key_Farm pattern
        if (_kf.getWinType() == TB) { // time-based windows
            // call the generic method to add the operator to the Multi-Pipe
            add_operator<KF_Emitter<tuple_t>, OrderingNode<tuple_t, wrapper_tuple_t<tuple_t>>>(&_kf, COMPLEX, TS);
        }
        else { // count-based windows
            // call the generic method to add the operator to the Multi-Pipe
            add_operator<KF_Emitter<tuple_t>, OrderingNode<tuple_t, wrapper_tuple_t<tuple_t>>>(&_kf, COMPLEX, TS_RENUMBERING);
        }
    	return *this;
    }

    /** 
     *  \brief Add a Key_Farm_GPU to the Multi-Pipe instance
     *  \param _kf Key_Farm_GPU pattern to be added
     *  \return the modified Multi-Pipe instance
     */
    template<typename tuple_t, typename result_t, typename F_t>
    Pipe &add(Key_Farm_GPU<tuple_t, result_t, F_t> &_kf)
    {
        // check whether the internal instances of the Key_Farm_GPU are complex or not
        if (_kf.useComplexNesting()) {
            cerr << RED << "WindFlow Error: Multi-Pipe does not support Complex Nested Key_Farm_GPU instances yet" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // check the type of the windows used by the Key_Farm_GPU pattern
        if (_kf.getWinType() == TB) { // time-based windows
            // call the generic method to add the operator to the Multi-Pipe
            add_operator<KF_Emitter<tuple_t>, OrderingNode<tuple_t, wrapper_tuple_t<tuple_t>>>(&_kf, COMPLEX, TS);
        }
        else { // count-based windows
            // call the generic method to add the operator to the Multi-Pipe
            add_operator<KF_Emitter<tuple_t>, OrderingNode<tuple_t, wrapper_tuple_t<tuple_t>>>(&_kf, COMPLEX, TS_RENUMBERING);
        }
        return *this;
    }

	/** 
     *  \brief Add a Pane_Farm to the Multi-Pipe instance
     *  \param _pf Pane_Farm pattern to be added
     *  \return the modified Multi-Pipe instance
     */
    template<typename tuple_t, typename result_t>
    Pipe &add(Pane_Farm<tuple_t, result_t> &_pf)
    {
        // check the optimization level of the Pane_Farm instance
        if (_pf.getOptLevel() != LEVEL0) {
            cerr << RED << "WindFlow Error: already optimized Pane_Farm instances are not accepted in a Multi-Pipe" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        ff_pipeline *pipe = static_cast<ff_pipeline *>(&_pf);
        const svector<ff_node *> &stages = pipe->getStages();
        ff_farm *plq = nullptr;
        // check if the PLQ stage is a farm, otherwise prepare it
        if (!stages[0]->isFarm()) {
            plq = new ff_farm();
            vector<ff_node *> w;
            w.push_back(stages[0]); // there is for sure one single worker in the PLQ
            plq->add_emitter(new dummy_emitter());
            plq->add_workers(w);
            plq->add_collector(nullptr);
            plq->cleanup_emitter(true);
            plq->cleanup_workers(false);
            // check the type of the windows
            if (_pf.getWinType() == TB) { // time-based windows
                // call the generic method to add the operator (PLQ stage) to the Multi-Pipe
                add_operator<dummy_emitter, OrderingNode<tuple_t, tuple_t>>(plq, COMPLEX, TS);
            }
            else { // count-based windows
                // call the generic method to add the operator (PLQ stage) to the Multi-Pipe
                add_operator<dummy_emitter, OrderingNode<tuple_t, tuple_t>>(plq, COMPLEX, TS_RENUMBERING);
            }
            delete plq;
        }
        else {
            plq = static_cast<ff_farm *>(stages[0]);
            // check the type of the windows
            if (_pf.getWinType() == TB) { // time-based windows
                // call the generic method to add the operator (PLQ stage) to the Multi-Pipe
                add_operator<WF_Emitter<tuple_t>, OrderingNode<tuple_t, wrapper_tuple_t<tuple_t>>>(plq, COMPLEX, TS);
            }
            else { // count-based windows
                size_t n = (plq->getWorkers()).size();
                plq->change_emitter(new broadcast_node<tuple_t>(n));
                // call the generic method to add the operator
                add_operator<broadcast_node<tuple_t>, OrderingNode<tuple_t, wrapper_tuple_t<tuple_t>>>(plq, COMPLEX, TS_RENUMBERING);
            }
        }
        ff_farm *wlq = nullptr;
        // check if the WLQ stage is a farm, otherwise prepare it
        if (!stages[1]->isFarm()) {
            wlq = new ff_farm();
            vector<ff_node *> w;
            w.push_back(stages[1]); // there is for sure one single worker in the WLQ
            wlq->add_emitter(new dummy_emitter());
            wlq->add_workers(w);
            wlq->add_collector(nullptr);
            wlq->cleanup_emitter(true);
            wlq->cleanup_workers(false);
            // call the generic method to add the operator (WLQ stage) to the Multi-Pipe
            add_operator<dummy_emitter, OrderingNode<result_t, result_t>>(wlq, COMPLEX, ID);
            delete wlq;
        }
        else {
            wlq = static_cast<ff_farm *>(stages[1]);
            // call the generic method to add the operator (WLQ stage) to the Multi-Pipe
            add_operator<WF_Emitter<result_t>, OrderingNode<result_t, wrapper_tuple_t<result_t>>>(wlq, COMPLEX, ID);
        }
    	return *this;
    }

    /** 
     *  \brief Add a Pane_Farm_GPU to the Multi-Pipe instance
     *  \param _pf Pane_Farm_GPU pattern to be added
     *  \return the modified Multi-Pipe instance
     */
    template<typename tuple_t, typename result_t, typename F_t>
    Pipe &add(Pane_Farm_GPU<tuple_t, result_t, F_t> &_pf)
    {
        // check the optimization level of the Pane_Farm_GPU instance
        if (_pf.getOptLevel() != LEVEL0) {
            cerr << RED << "WindFlow Error: already optimized Pane_Farm_GPU instances are not accepted in a Multi-Pipe" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        ff_pipeline *pipe = static_cast<ff_pipeline *>(&_pf);
        const svector<ff_node *> &stages = pipe->getStages();
        ff_farm *plq = nullptr;
        // check if the PLQ stage is a farm, otherwise prepare it
        if (!stages[0]->isFarm()) {
            plq = new ff_farm();
            vector<ff_node *> w;
            w.push_back(stages[0]); // there is for sure one single worker in the PLQ
            plq->add_emitter(new dummy_emitter());
            plq->add_workers(w);
            plq->add_collector(nullptr);
            plq->cleanup_emitter(true);
            plq->cleanup_workers(false);
            // check the type of the windows
            if (_pf.getWinType() == TB) { // time-based windows
                // call the generic method to add the operator (PLQ stage) to the Multi-Pipe
                add_operator<dummy_emitter, OrderingNode<tuple_t, tuple_t>>(plq, COMPLEX, TS);
            }
            else { // count-based windows
                // call the generic method to add the operator (PLQ stage) to the Multi-Pipe
                add_operator<dummy_emitter, OrderingNode<tuple_t, tuple_t>>(plq, COMPLEX, TS_RENUMBERING);
            }
            delete plq;
        }
        else {
            plq = static_cast<ff_farm *>(stages[0]);
            // check the type of the windows
            if (_pf.getWinType() == TB) { // time-based windows
                // call the generic method to add the operator (PLQ stage) to the Multi-Pipe
                add_operator<WF_Emitter<tuple_t>, OrderingNode<tuple_t, wrapper_tuple_t<tuple_t>>>(plq, COMPLEX, TS);
            }
            else { // count-based windows
                size_t n = (plq->getWorkers()).size();
                plq->change_emitter(new broadcast_node<tuple_t>(n));
                // call the generic method to add the operator
                add_operator<broadcast_node<tuple_t>, OrderingNode<tuple_t, wrapper_tuple_t<tuple_t>>>(plq, COMPLEX, TS_RENUMBERING);
            }
        }
        ff_farm *wlq = nullptr;
        // check if the WLQ stage is a farm, otherwise prepare it
        if (!stages[1]->isFarm()) {
            wlq = new ff_farm();
            vector<ff_node *> w;
            w.push_back(stages[1]); // there is for sure one single worker in the WLQ
            wlq->add_emitter(new dummy_emitter());
            wlq->add_workers(w);
            wlq->add_collector(nullptr);
            wlq->cleanup_emitter(true);
            wlq->cleanup_workers(false);
            // call the generic method to add the operator (WLQ stage) to the Multi-Pipe
            add_operator<dummy_emitter, OrderingNode<result_t, result_t>>(wlq, COMPLEX, ID);
            delete wlq;
        }
        else {
            wlq = static_cast<ff_farm *>(stages[1]);
            // call the generic method to add the operator (WLQ stage) to the Multi-Pipe
            add_operator<WF_Emitter<result_t>, OrderingNode<result_t, wrapper_tuple_t<result_t>>>(wlq, COMPLEX, ID);
        }
        return *this;
    }

	/** 
     *  \brief Add a Win_MapReduce to the Multi-Pipe instance
     *  \param _wmr Win_MapReduce pattern to be added
     *  \return the modified Multi-Pipe instance
     */
    template<typename tuple_t, typename result_t>
    Pipe &add(Win_MapReduce<tuple_t, result_t> &_wmr)
    {
        // check the optimization level of the Win_MapReduce instance
        if (_wmr.getOptLevel() != LEVEL0) {
            cerr << RED << "WindFlow Error: already optimized Win_MapReduce instances are not accepted in a Multi-Pipe" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // add the MAP stage
        ff_pipeline *pipe = static_cast<ff_pipeline *>(&_wmr);
        const svector<ff_node *> &stages = pipe->getStages();
        ff_farm *map = static_cast<ff_farm *>(stages[0]);
        // check the type of the windows
        if (_wmr.getWinType() == TB) { // time-based windows
            // call the generic method to add the operator (MAP stage) to the Multi-Pipe
            add_operator<WinMap_Emitter<tuple_t>, OrderingNode<tuple_t, wrapper_tuple_t<tuple_t>>>(map, COMPLEX, TS);
        }
        else { // count-based windows
            size_t n_map = (map->getWorkers()).size();
            ff_farm *new_map = new ff_farm();
            auto worker_set = map->getWorkers();
            vector<ff_node *> w;
            new_map->add_emitter(new broadcast_node<tuple_t>(n_map));
            for (size_t i=0; i<n_map; i++) {
                ff_comb *comb = new ff_comb(new WinMap_Dropper<tuple_t>(i, n_map), worker_set[i], true, false);
                w.push_back(comb);
            }
            new_map->add_workers(w);
            new_map->add_collector(nullptr);
            new_map->cleanup_emitter(true);
            new_map->cleanup_workers(false);
            // call the generic method to add the operator (MAP stage) to the Multi-Pipe
            add_operator<broadcast_node<tuple_t>, OrderingNode<tuple_t, wrapper_tuple_t<tuple_t>>>(new_map, COMPLEX, TS_RENUMBERING);
            delete new_map;
        }
        // add the REDUCE stage
        ff_farm *reduce = nullptr;
        // check if the REDUCE stage is a farm, otherwise prepare it
        if (!stages[1]->isFarm()) {
            reduce = new ff_farm();
            vector<ff_node *> w;
            w.push_back(stages[1]);
            reduce->add_emitter(new dummy_emitter());
            reduce->add_workers(w);
            reduce->add_collector(nullptr);
            reduce->cleanup_emitter(true);
            reduce->cleanup_workers(false);
            // call the generic method to add the operator (REDUCE stage) to the Multi-Pipe
            add_operator<dummy_emitter, OrderingNode<result_t, result_t>>(reduce, COMPLEX, ID);
            delete reduce;
        }
        else {
            reduce = static_cast<ff_farm *>(stages[1]);
            // call the generic method to add the operator (REDUCE stage) to the Multi-Pipe
            add_operator<WF_Emitter<result_t>, OrderingNode<result_t, wrapper_tuple_t<result_t>>>(reduce, COMPLEX, ID);
        }
        return *this;
    }

    /** 
     *  \brief Add a Win_MapReduce_GPU to the Multi-Pipe instance
     *  \param _wmr Win_MapReduce_GPU pattern to be added
     *  \return the modified Multi-Pipe instance
     */
    template<typename tuple_t, typename result_t, typename F_t>
    Pipe &add(Win_MapReduce_GPU<tuple_t, result_t, F_t> &_wmr)
    {
        // check the optimization level of the Win_MapReduce_GPU instance
        if (_wmr.getOptLevel() != LEVEL0) {
            cerr << RED << "WindFlow Error: already optimized Win_MapReduce_GPU instances are not accepted in a Multi-Pipe" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // add the MAP stage
        ff_pipeline *pipe = static_cast<ff_pipeline *>(&_wmr);
        const svector<ff_node *> &stages = pipe->getStages();
        ff_farm *map = static_cast<ff_farm *>(stages[0]);
        // check the type of the windows
        if (_wmr.getWinType() == TB) { // time-based windows
            // call the generic method to add the operator (MAP stage) to the Multi-Pipe
            add_operator<WinMap_Emitter<tuple_t>, OrderingNode<tuple_t, wrapper_tuple_t<tuple_t>>>(map, COMPLEX, TS);
        }
        else { // count-based windows
            size_t n_map = (map->getWorkers()).size();
            ff_farm *new_map = new ff_farm();
            auto worker_set = map->getWorkers();
            vector<ff_node *> w;
            new_map->add_emitter(new broadcast_node<tuple_t>(n_map));
            for (size_t i=0; i<n_map; i++) {
                ff_comb *comb = new ff_comb(new WinMap_Dropper<tuple_t>(i, n_map), worker_set[i], true, false);
                w.push_back(comb);
            }
            new_map->add_workers(w);
            new_map->add_collector(nullptr);
            new_map->cleanup_emitter(true);
            new_map->cleanup_workers(false);
            // call the generic method to add the operator (MAP stage) to the Multi-Pipe
            add_operator<broadcast_node<tuple_t>, OrderingNode<tuple_t, wrapper_tuple_t<tuple_t>>>(new_map, COMPLEX, TS_RENUMBERING);
            delete new_map;
        }
        // add the REDUCE stage
        ff_farm *reduce = nullptr;
        // check if the REDUCE stage is a farm, otherwise prepare it
        if (!stages[1]->isFarm()) {
            reduce = new ff_farm();
            vector<ff_node *> w;
            w.push_back(stages[1]);
            reduce->add_emitter(new dummy_emitter());
            reduce->add_workers(w);
            reduce->add_collector(nullptr);
            reduce->cleanup_emitter(true);
            reduce->cleanup_workers(false);
            // call the generic method to add the operator (REDUCE stage) to the Multi-Pipe
            add_operator<dummy_emitter, OrderingNode<result_t, result_t>>(reduce, COMPLEX, ID);
            delete reduce;
        }
        else {
            reduce = static_cast<ff_farm *>(stages[1]);
            // call the generic method to add the operator (REDUCE stage) to the Multi-Pipe
            add_operator<WF_Emitter<result_t>, OrderingNode<result_t, wrapper_tuple_t<result_t>>>(reduce, COMPLEX, ID);
        }
        return *this;
    }

	/** 
     *  \brief Add a Sink to the Multi-Pipe instance
     *  \param _sink Sink pattern to be added
     *  \return the modified Multi-Pipe instance
     */
    template<typename tuple_t>
    Pipe &add_sink(Sink<tuple_t> &_sink)
    {
        // call the generic method to add the operator to the Multi-Pipe
        add_operator<dummy_emitter, OrderingNode<tuple_t, tuple_t>>(&_sink, SIMPLE, TS);
    	this->has_sink = true;
    	return *this;
    }

    /** 
     *  \brief Chain a Sink to the Multi-Pipe instance (if possible, otherwise add)
     *  \param _sink Sink pattern to be chained
     *  \return the modified Multi-Pipe instance
     */
    template<typename tuple_t>
    Pipe &chain_sink(Sink<tuple_t> &_sink)
    {
        // try to chain the Sink with the Multi-Pipe
        bool chained = chain_operator<typename Sink<tuple_t>::Sink_Node>(&_sink);
        if (!chained) {
            // add the Sink to the Multi-Pipe
            add_sink(_sink);
        }
        else
            this->has_sink = true;
        return *this;
    }

#if __cplusplus >= 201703L
    /** 
     *  \brief Union of the Multi-Pipe with a set of other Multi-Pipe instances (only C++17)
     *  \param _name string with the unique name of the new Multi-Pipe instance
     *  \param _pipes sequence of Multi-Pipe instances
     *  \return a new Multi-Pipe instance (the result of the union between this and _pipes)
     */
    template<typename ...PIPES>
    Pipe unionPipes(string _name="anonymous_union_pipe", PIPES&... _pipes)
    {
        vector<ff_node *> init_set = prepareInitSet(*this, _pipes...);
        return Pipe(_name, init_set);
    }
#endif

    /** 
     *  \brief Union of the Multi-Pipe with a set of other Multi-Pipe instances
     *  \param _name string with the unique name of the new Multi-Pipe instance
     *  \param _pipes sequence of Multi-Pipe instances
     *  \return a pointer to a new Multi-Pipe instance (the result of the union between this and _pipes)
     */
    template<typename ...PIPES>
    Pipe *unionPipes_ptr(string _name="anonymous_union_pipe", PIPES&... _pipes)
    {
        vector<ff_node *> init_set = prepareInitSet(*this, _pipes...);
        return new Pipe(_name, init_set);
    }

    /** 
     *  \brief Union of the Multi-Pipe with a set of other Multi-Pipe instances
     *  \param _name string with the unique name of the new Multi-Pipe instance
     *  \param _pipes sequence of Multi-Pipe instances
     *  \return a unique pointer to a new Multi-Pipe instance (the result of the union between this and _pipes)
     */
    template<typename ...PIPES>
    unique_ptr<Pipe> unionPipes_unique(string _name="anonymous_union_pipe", PIPES&... _pipes)
    {
        vector<ff_node *> init_set = prepareInitSet(*this, _pipes...);
        return make_unique<Pipe>(_name, init_set);
    }

	/** 
     *  \brief Check whether the Multi-Pipe is runnable (i.e. it has a Source and a Sink instance)
     *  \return true if it is runnable, false otherwise
     */
    bool isRunnable() const
    {
    	return (has_source && has_sink);
    }

	/** 
     *  \brief Check whether the Multi-Pipe has a Source
     *  \return true if it has a defined Source, false otherwise
     */
    bool hasSource() const
    {
    	return has_source;
    }

	/** 
     *  \brief Check whether the Multi-Pipe has a Sink
     *  \return true if it has a defined Sink, false otherwise
     */
    bool hasSink() const
    {
    	return has_sink;
    }

    /** 
     *  \brief Return the number of raw threads used by this Multi-Pipe
     *  \return the number of raw threads used by the FastFlow run-time system to run the Multi-Pipe instance
     */
    size_t getNumNodes() const
    {
        return this->cardinality()-1;
    }

	/** 
     *  \brief Asynchronous run of the Multi-Pipe instance
     *  \return zero in case of success, non-zero otherwise
     */
    int run()
    {
        if (!this->isUnified)
            cout << BOLDGREEN << "WindFlow Status Message: Multi-Pipe [" << name << "] is running with " << this->getNumNodes() << " threads" << DEFAULT << endl;
    	int status = ff_pipeline::run();
    	if (status != 0 && !this->isUnified)
    		cerr << RED << "WindFlow Error: Multi-Pipe [" << name << "] run failed" << DEFAULT << endl;
    	return status;
    }

	/** 
     *  \brief Wait the termination of an already running Multi-Pipe instance
     *  \return zero in case of success, not-zero otherwise
     */
    int wait()
    {
    	int status = ff_pipeline::wait();
    	if (status == 0 && !this->isUnified)
    		cout << BOLDGREEN << "WindFlow Status Message: Multi-Pipe [" << name << "] terminated successfully" << DEFAULT << endl;
    	else if(!this->isUnified)
    		cerr << RED << "WindFlow Error: Multi-Pipe [" << name << "] terminated with error" << DEFAULT << endl;
    }

	/** 
     *  \brief Synchronous run of the Multi-Pipe instance
     *         (calling thread is blocked until the streaming execution is complete)
     *  \return zero in case of success, non-zero otherwise
     */
    int run_and_wait_end()
    {
        if (!this->isUnified)
            cout << BOLDGREEN << "WindFlow Status Message: Multi-Pipe [" << name << "] is running with " << this->getNumNodes() << " threads" << DEFAULT << endl;
    	return ff_pipeline::run_and_wait_end();
    }
};

#endif
