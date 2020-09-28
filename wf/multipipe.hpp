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
 *  @date    22/07/2020
 *  
 *  @brief MultiPipe construct
 *  
 *  @section MultiPipe (Description)
 *  
 *  This file implements the MultiPipe construct used to build a linear pipeline
 *  of streaming operators. Each operator can be composed of several replicas, and
 *  replicas of consecutive operators can communicate through direct of shuffle
 *  connections. In other words, a MultiPipe is a set of parallel pipelines where
 *  replicas can be connected to one replica (direct) or to all the replicas
 *  (shuffle) of the next operator in the sequence.
 */ 

#ifndef MULTIPIPE_H
#define MULTIPIPE_H

/// includes
#include<map>
#include<string>
#include<vector>
#include<random>
#include<typeinfo>
#include<algorithm>
#include<math.h>
#include<ff/ff.hpp>
#if defined (TRACE_WINDFLOW)
    #include<graphviz/gvc.h>
    #include<rapidjson/prettywriter.h>
#endif
#include<basic.hpp>
#include<wm_nodes.hpp>
#include<wf_nodes.hpp>
#include<kf_nodes.hpp>
#include<kslack_node.hpp>
#include<tree_emitter.hpp>
#include<basic_emitter.hpp>
#include<ordering_node.hpp>
#include<basic_operator.hpp>
#include<transformations.hpp>
#include<standard_emitter.hpp>
#include<splitting_emitter.hpp>
#include<broadcast_emitter.hpp>

namespace wf {

//@cond DOXY_IGNORE

// class selfkiller_node (placeholder in the second set of matrioskas)
class selfkiller_node: public ff::ff_minode
{
    // svc_init (utilized by the FastFlow runtime)
    int svc_init() override
    {
        skipfirstpop(true);
        return 0;
    }

    // svc method (utilized by the FastFlow runtime)
    void *svc(void *) override
    {
        return this->EOS; // terminates immediately!
    }
};

//@endcond

/** 
 *  \class MultiPipe
 *  
 *  \brief MultiPipe construct
 *  
 *  This class implements the MultiPipe construct used to build a set of pipelines
 *  of operators that might have shuffle connections jumping from a pipeline to
 *  another one.
 */ 
class MultiPipe: public ff::ff_pipeline
{
private:
    // friendship with the PipeGraph class
    friend class PipeGraph;
    PipeGraph *graph; // PipeGraph creating this MultiPipe
    Mode mode; // processing mode of the the sorrounding PipeGraph
    std::atomic<unsigned long> *atomic_num_dropped; // pointer to the atomic counter of dropped tuples by the sorrounding PipeGraph
    std::vector<std::reference_wrapper<Basic_Operator>> *listOperators; // pointer to a vector or reference_wrappers to operators of the sorrounding PipeGraph
    bool has_source; // true if the MultiPipe starts with a Source
    bool has_sink; // true if the MultiPipe ends with a Sink
    ff::ff_a2a *last; // pointer to the last matrioska
    ff::ff_a2a *secondToLast; // pointer to the second-to-last matrioska
    bool isMerged; // true if the MultiPipe has been merged with other MultiPipe instances
    bool isSplit; // true if the MultiPipe has been split into other MultiPipe instances
    bool fromSplitting; // true if the MultiPipe originates from a splitting of another MultiPipe
    size_t splittingBranches; // number of splitting branches (meaningful if isSplit is true)
    MultiPipe *splittingParent = nullptr; // pointer to the parent MultiPipe (meaningful if fromSplitting is true)
    Basic_Emitter *splittingEmitterRoot = nullptr; // splitting emitter (meaningful if isSplit is true)
    std::vector<Basic_Emitter *> splittingEmitterLeaves; // vector of emitters (meaningful if isSplit is true)
    std::vector<MultiPipe *> splittingChildren; // vector of children MultiPipe instances (meaningful if isSplit is true)
    bool forceShuffling; // true if the next operator that will be added to the MultiPipe is forced to generate a shuffle connection
    size_t lastParallelism; // parallelism of the last operator added to the MultiPipe (0 if not defined)
    std::string outputType; // string representing the type of the outputs from this MultiPipe (the empty string if not defined yet)
#if defined (TRACE_WINDFLOW)
    Agraph_t *gv_graph = nullptr; // pointer to the graphviz representation of the sorrounding PipeGraph
    std::vector<std::string> gv_last_typeOPs; // list of the last chained operator types
    std::vector<std::string> gv_last_nameOPs; // list of the last chained operator names
    std::vector<Agnode_t *> gv_last_vertices; // list of the last graphviz vertices
#endif

    // Private Constructor I (to create an empty MultiPipe)
    MultiPipe(PipeGraph *_graph,
              Mode _mode,
              std::atomic<unsigned long> *_atomic_num_dropped,
              std::vector<std::reference_wrapper<Basic_Operator>> *_listOperators):
              graph(_graph),
              mode(_mode),
              atomic_num_dropped(_atomic_num_dropped),
              listOperators(_listOperators),
              has_source(false),
              has_sink(false),
              last(nullptr),
              secondToLast(nullptr),
              isMerged(false),
              isSplit(false),
              fromSplitting(false),
              splittingBranches(0),
              forceShuffling(false),
              lastParallelism(0),
              outputType("") {}

    // Private Constructor II (to create a MultiPipe resulting from the merge of other MultiPipe instances)
    MultiPipe(PipeGraph *_graph,
              std::vector<MultiPipe *> _mps,
              Mode _mode,
              std::atomic<unsigned long> *_atomic_num_dropped,
              std::vector<std::reference_wrapper<Basic_Operator>> *_listOperators):
              graph(_graph),
              mode(_mode),
              atomic_num_dropped(_atomic_num_dropped),
              listOperators(_listOperators),
              has_source(true),
              has_sink(false),
              isMerged(false),
              isSplit(false),
              fromSplitting(false),
              splittingBranches(0),
              forceShuffling(true), // <-- we force a shuffle connection for the next operator
              lastParallelism(0),
              outputType("")
    {
        // create the initial matrioska
        ff::ff_a2a *matrioska = new ff::ff_a2a();
        // normalize all the MultiPipe instances to be merged in this
        std::vector<ff::ff_node *> normalization;
        for (auto *mp: _mps) {
            auto v = mp->normalize();
            mp->isMerged = true;
            normalization.insert(normalization.end(), v.begin(), v.end());
        }
        matrioska->add_firstset(normalization, 0, false); // this will share the nodes of the MultiPipe instances to be merged!
        std::vector<ff::ff_node *> second_set;
        ff::ff_pipeline *stage = new ff::ff_pipeline();
        stage->add_stage(new selfkiller_node(), true);
        second_set.push_back(stage);
        matrioska->add_secondset(second_set, true);
        this->add_stage(matrioska, true);
        last = matrioska;
        secondToLast = nullptr;
    }

    // method to add a source to the MultiPipe
    template<typename tuple_t>
    MultiPipe &add_source(Source<tuple_t> &_source)
    {
        // check whether the operator has already been used in a MultiPipe
        if (_source.isUsed()) {
            std::cerr << RED << "WindFlow Error: Source operator has already been used in a MultiPipe" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // check the Source presence
        if (has_source) {
            std::cerr << RED << "WindFlow Error: Source has already been defined for the MultiPipe" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // create the initial matrioska
        ff::ff_a2a *matrioska = new ff::ff_a2a();
        std::vector<ff::ff_node *> first_set;
        auto workers = _source.getFirstSet();
        for (size_t i=0; i<workers.size(); i++) {
            ff::ff_pipeline *stage = new ff::ff_pipeline();
            stage->add_stage(workers[i], false);
            first_set.push_back(stage);
        }
        matrioska->add_firstset(first_set, 0, true);
        std::vector<ff::ff_node *> second_set;
        ff::ff_pipeline *stage = new ff::ff_pipeline();
        stage->add_stage(new selfkiller_node(), true);
        second_set.push_back(stage);
        matrioska->add_secondset(second_set, true);
        this->add_stage(matrioska, true);
        has_source = true;
        last = matrioska;
        // save parallelism of the operator
        lastParallelism = workers.size();
        // save the output type from this MultiPipe
        tuple_t t;
        outputType = typeid(t).name();
        // the Source operator is now used
        _source.used = true;
#if defined (TRACE_WINDFLOW)
        // update the graphviz representation
        gv_add_vertex("Source (" + std::to_string(_source.getFirstSet().size()) + ")", _source.getName(), true, false, routing_modes_t::NONE);
#endif
        return *this;
    }

    // method to add an operator to the MultiPipe
    template<typename emitter_t, typename collector_t=dummy_mi>
    void add_operator(ff::ff_farm *_op, routing_modes_t _type, ordering_mode_t _ordering=ordering_mode_t::TS)
        {
        // check the Source presence
        if (!has_source) {
            std::cerr << RED << "WindFlow Error: MultiPipe does not have a Source, operator cannot be added" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // check the Sink presence
        if (has_sink) {
            std::cerr << RED << "WindFlow Error: MultiPipe is terminated by a Sink, operator cannot be added" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // merged MultiPipe cannot be modified
        if (isMerged) {
            std::cerr << RED << "WindFlow Error: MultiPipe has been merged, operator cannot be added" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // split MultiPipe cannot be modified
        if (isSplit) {
            std::cerr << RED << "WindFlow Error: MultiPipe has been split, operator cannot be added" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // Case 1: first operator added after splitting
        if (fromSplitting && last == nullptr) {
            // create the initial matrioska
            ff::ff_a2a *matrioska = new ff::ff_a2a();
            std::vector<ff::ff_node *> first_set;
            auto workers = _op->getWorkers();
            for (size_t i=0; i<workers.size(); i++) {
                ff::ff_pipeline *stage = new ff::ff_pipeline();
                stage->add_stage(workers[i], false);
                if (mode != Mode::DEFAULT) {
                    collector_t *collector = new collector_t(_ordering, atomic_num_dropped);
                    combine_with_firststage(*stage, collector, true); // add the ordering_node / kslack_node
                }
                first_set.push_back(stage);
            }
            matrioska->add_firstset(first_set, 0, true);
            std::vector<ff::ff_node *> second_set;
            ff::ff_pipeline *stage = new ff::ff_pipeline();
            stage->add_stage(new selfkiller_node(), true);
            second_set.push_back(stage);
            matrioska->add_secondset(second_set, true);
            this->add_stage(matrioska, true);
            last = matrioska;
            // save parallelism of the operator
            lastParallelism = workers.size();
            // save what is needed for splitting with the parent MultiPipe
            Basic_Emitter *be = static_cast<Basic_Emitter *>(_op->getEmitter());
            assert(splittingParent != nullptr); // redundant check
            splittingParent->prepareSplittingEmitters(be);
            return;
        }
        size_t n1 = (last->getFirstSet()).size();
        size_t n2 = (_op->getWorkers()).size();
        // Case 2: direct connection
        if ((n1 == n2) && (_type == routing_modes_t::FORWARD) && (!forceShuffling)) {
            auto first_set = last->getFirstSet();
            auto worker_set = _op->getWorkers();
            // add the operator's workers to the pipelines in the first set of the matrioska
            for (size_t i=0; i<n1; i++) {
                ff::ff_pipeline *stage = static_cast<ff::ff_pipeline *>(first_set[i]);
                stage->add_stage(worker_set[i], false);
            }
        }
        // Case 3: shuffle connection
        else {
            // prepare the nodes of the first_set of the last matrioska for the shuffle connection
            auto first_set_m = last->getFirstSet();
            for (size_t i=0; i<n1; i++) {
                ff::ff_pipeline *stage = static_cast<ff::ff_pipeline *>(first_set_m[i]);
                emitter_t *tmp_e = static_cast<emitter_t *>(_op->getEmitter());
                combine_with_laststage(*stage, new emitter_t(*tmp_e), true);
            }
            // create a new matrioska
            ff::ff_a2a *matrioska = new ff::ff_a2a();
            std::vector<ff::ff_node *> first_set;
            auto worker_set = _op->getWorkers();
            for (size_t i=0; i<n2; i++) {
                ff::ff_pipeline *stage = new ff::ff_pipeline();
                stage->add_stage(worker_set[i], false);
                if (mode != Mode::DEFAULT || _ordering == ordering_mode_t::ID) {
                    collector_t *collector = new collector_t(_ordering, atomic_num_dropped);
                    combine_with_firststage(*stage, collector, true); // add the ordering_node / kslack_node
                }
                first_set.push_back(stage);
            }
            matrioska->add_firstset(first_set, 0, true);
            std::vector<ff::ff_node *> second_set;
            ff::ff_pipeline *stage = new ff::ff_pipeline();
            stage->add_stage(new selfkiller_node(), true);
            second_set.push_back(stage);
            matrioska->add_secondset(second_set, true);
            ff::ff_pipeline *previous = static_cast<ff::ff_pipeline *>((last->getSecondSet())[0]);
            previous->remove_stage(0); // remove the self-killer node (it will be deleted with previous later on)
            previous->add_stage(matrioska, true); // Chinese boxes
            secondToLast = last;
            last = matrioska;
            // reset forceShuffling flag if it was true
            if (forceShuffling) {
                forceShuffling = false;
            }
        }
        // save parallelism of the operator
        lastParallelism = n2;   
    }

    // method to chain an operator with the previous one in the MultiPipe
    template<typename worker_t>
    bool chain_operator(ff::ff_farm *_op)
    {
        // check the Source presence
        if (!has_source) {
            std::cerr << RED << "WindFlow Error: MultiPipe does not have a Source, operator cannot be chained" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // check the Sink presence
        if (has_sink) {
            std::cerr << RED << "WindFlow Error: MultiPipe is terminated by a Sink, operator cannot be chained" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // merged MultiPipe cannot be modified
        if (isMerged) {
            std::cerr << RED << "WindFlow Error: MultiPipe has been merged, operator cannot be chained" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // split MultiPipe cannot be modified
        if (isSplit) {
            std::cerr << RED << "WindFlow Error: MultiPipe has been split, operator cannot be chained" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // corner case -> first operator added to a MultiPipe after splitting (chain cannot work, do add instead)
        if (fromSplitting && last == nullptr) {
            return false;
        }
        size_t n1 = (last->getFirstSet()).size();
        size_t n2 = (_op->getWorkers()).size();
        // distribution of _op is for sure FORWARD: check additional conditions for chaining
        if ((n1 == n2) && (!forceShuffling)) {
            auto first_set = (last)->getFirstSet();
            auto worker_set = _op->getWorkers();
            // chaining the operator's workers with the last node of each pipeline in the first set of the matrioska
            for (size_t i=0; i<n1; i++) {
                ff::ff_pipeline *stage = static_cast<ff::ff_pipeline *>(first_set[i]);
                worker_t *worker = static_cast<worker_t *>(worker_set[i]);
                combine_with_laststage(*stage, worker, false);
            }
            // save parallelism of the operator (not necessary: n1 is equal to n2)
            lastParallelism = n2;
            return true;
        }
        else {
            return false;
        }   
    }

    // method to normalize the MultiPipe (removing the final self-killer nodes)
    std::vector<ff::ff_node *> normalize()
    {
        // check the Source presence
        if (!has_source) {
            std::cerr << RED << "WindFlow Error: MultiPipe does not have a Source, it cannot be merged/split" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // check the Sink presence
        if (has_sink) {
            std::cerr << RED << "WindFlow Error: MultiPipe is terminated by a Sink, it cannot be merged/split" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // empty MultiPipe cannot be normalized (only for splitting in this case)
        if (last == nullptr) {
            std::cerr << RED << "WindFlow Error: MultiPipe is empty, it cannot be split" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        std::vector<ff::ff_node *> result;
        // Case 1: the MultiPipe has not been split
        if (!isSplit) {
            // Case 1.1 (only one Matrioska)
            if (secondToLast == nullptr) {
                auto first_set = last->getFirstSet();
                for (size_t i=0; i<first_set.size(); i++) {
                    result.push_back(first_set[i]);
                }
            }
            // Case 1.2 (at least two nested Matrioska)
            else {
                last->cleanup_firstset(false);
                auto first_set_last = last->getFirstSet();
                std::vector<ff::ff_node *> second_set_secondToLast;
                for (size_t i=0; i<first_set_last.size(); i++) {
                    second_set_secondToLast.push_back(first_set_last[i]);
                }
                secondToLast->change_secondset(second_set_secondToLast, true);
                delete last;
                last = secondToLast;
                secondToLast = nullptr;
                ff::ff_pipeline *p = new ff::ff_pipeline(); // <-- Memory leak!
                p->add_stage((this->getStages())[0], false);
                result.push_back(p);
            }
        }
        // Case 2: the MultiPipe has been split
        else {
            std::vector<ff::ff_node *> normalized;
            auto second_set = last->getSecondSet();
            for (auto *node: second_set) {
                MultiPipe *mp = static_cast<MultiPipe *>(node);
                auto v = mp->normalize();
                // mp has been normalized for merging
                mp->isMerged = true;
                normalized.insert(normalized.end(), v.begin(), v.end());
            }
            last->change_secondset(normalized, false);
            ff::ff_pipeline *p = new ff::ff_pipeline(); // <-- Memory leak
            p->add_stage(last, false);
            result.push_back(p);
        }
        return result;
    }

    // prepareMergeSet method: base case 1
    std::vector<MultiPipe *> prepareMergeSet()
    {
        return std::vector<MultiPipe *>();
    }

    // prepareMergeSet method: base case 2
    template<typename MULTIPIPE>
    std::vector<MultiPipe *> prepareMergeSet(MULTIPIPE &_pipe)
    {
        // check whether the MultiPipe has already been merged
        if (_pipe.isMerged) {
            std::cerr << RED << "WindFlow Error: MultiPipe has already been merged" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // check whether the MultiPipe has been split
        if (_pipe.isSplit) {
            std::cerr << RED << "WindFlow Error: MultiPipe has been split and cannot be merged" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        std::vector<MultiPipe *> output;
        output.push_back(&_pipe);
        return output;
    }

    // prepareMergeSet method: generic case
    template<typename MULTIPIPE, typename ...MULTIPIPES>
    std::vector<MultiPipe *> prepareMergeSet(MULTIPIPE &_first, MULTIPIPES&... _pipes)
    {
        // check whether the MultiPipe has already been merged
        if (_first.isMerged) {
            std::cerr << RED << "WindFlow Error: MultiPipe has already been merged" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // check whether the MultiPipe has been split
        if (_first.isSplit) {
            std::cerr << RED << "WindFlow Error: MultiPipe has been split and cannot be merged" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        std::vector<MultiPipe *> output;
        output.push_back(&_first);
        auto v = prepareMergeSet(_pipes...);
        output.insert(output.end(), v.begin(), v.end());
        return output;
    }

    // prepareSplittingEmitters method
    void prepareSplittingEmitters(Basic_Emitter *_e)
    {
        assert(isSplit); // redundant check
        splittingEmitterLeaves.push_back(_e->clone());
        // check whether all the emitters are ready
        if (splittingEmitterLeaves.size() == splittingBranches) {
            Tree_Emitter *tE = new Tree_Emitter(splittingEmitterRoot, splittingEmitterLeaves, true, true);
            // combine tE with the nodes in the first set
            auto first_set = last->getFirstSet();
            for (size_t i=0; i<first_set.size(); i++) {
                ff::ff_pipeline *stage = static_cast<ff::ff_pipeline *>(first_set[i]);
                combine_with_laststage(*stage, new Tree_Emitter(*tE), true);
            }
            delete tE;
        }
    }

#if defined (TRACE_WINDFLOW)
    // method to add of a new operator to the graphviz representation
    void gv_add_vertex(std::string typeOP, std::string nameOP, bool isCPU_OP, bool isNested, routing_modes_t routing_type)
    {
        assert(gv_graph != nullptr);
        // creating the new vertex
        Agnode_t *node = agnode(gv_graph, NULL, 1);
        // prepare the label for this vertex
        std::string label = typeOP + "<BR/><FONT POINT-SIZE=\"8\">" + nameOP + "</FONT>";
        agset(node, const_cast<char *>("label"), agstrdup_html(gv_graph, const_cast<char *>(label.c_str()))); // change the label of the operator
        if ((typeOP.compare(0, 6, "Source") == 0) || (typeOP.compare(0, 4, "Sink") == 0)) {
            agset(node, const_cast<char *>("color"), const_cast<char *>("#B8B7B8")); // style for Source and Sink operators
        }
        // representation of a CPU operator
        else if (isCPU_OP) // style of CPU operators
        {
            agset(node, const_cast<char *>("color"), const_cast<char *>("#941100"));
            agset(node, const_cast<char *>("fillcolor"), const_cast<char *>("#ff9400"));
        }
        else { // style of GPU operators
            agset(node, const_cast<char *>("color"), const_cast<char *>("#20548E"));
            agset(node, const_cast<char *>("fillcolor"), const_cast<char *>("#469EA2"));
        }
        // change shape of the node to represent a nested operator
        if (isNested) {
            agset(node, const_cast<char *>("shape"), const_cast<char *>("doubleoctagon"));
        }
        // connect the new vertex to the previous one(s)
        for (auto *vertex: this->gv_last_vertices) {
            Agedge_t *e = agedge(gv_graph, vertex, node, 0, 1);
            // set the label of the edge
            if (routing_type == routing_modes_t::FORWARD) {
                agset(e, const_cast<char *>("label"), const_cast<char *>("FW"));
            }
            else if (routing_type == routing_modes_t::KEYBY) {
                agset(e, const_cast<char *>("label"), const_cast<char *>("KB"));
            }
            else if (routing_type == routing_modes_t::COMPLEX) {
                agset(e, const_cast<char *>("label"), const_cast<char *>("CMX"));
            }
        }
        // adjust gv_last_* vectors
        (this->gv_last_vertices).clear();
        (this->gv_last_typeOPs).clear();
        (this->gv_last_nameOPs).clear();
        (this->gv_last_vertices).push_back(node);
        (this->gv_last_typeOPs).push_back(typeOP);
        (this->gv_last_nameOPs).push_back(nameOP);
    }

    // method to chain of a new operator in the graphviz representation
    void gv_chain_vertex(std::string typeOP, std::string nameOP)
    {
        assert(gv_graph != nullptr);
        assert((this->gv_last_vertices).size() == 1);
        assert((this->gv_last_typeOPs).size() == 1);
        assert((this->gv_last_nameOPs).size() == 1);
        // prepare the new label of the last existing node
        auto *node = (this->gv_last_vertices)[0];
        auto &lastTypeOPs = (this->gv_last_typeOPs)[0];
        auto &lastNameOPs = (this->gv_last_nameOPs)[0];
        lastTypeOPs = lastTypeOPs + "->" + typeOP;
        lastNameOPs = lastNameOPs + ", " + nameOP;
        std::string label = lastTypeOPs + "<BR/><FONT POINT-SIZE=\"8\">" + lastNameOPs + "</FONT>";
        // change the label to the last vertex already present
        agset(node, const_cast<char *>("label"), agstrdup_html(gv_graph, const_cast<char *>(label.c_str())));
        if (typeOP.compare(0, 4, "Sink") == 0) { // style for Sink operator
            agset(node, const_cast<char *>("color"), const_cast<char *>("#B8B7B8"));
            agset(node, const_cast<char *>("fillcolor"), const_cast<char *>("black"));
        }
    }
#endif

    // run method
    int run()
    {
        int status = 0;
        if (!isRunnable()) {
            std::cerr << RED << "WindFlow Error: some MultiPipe is not runnable, check Sources and Sinks presence" << DEFAULT_COLOR << std::endl;
            status = -1;
        }
        else {
            status = ff::ff_pipeline::run();
        }
        return status;
    }

    // wait method
    int wait()
    {
        int status = 0;
        if (!isRunnable()) {
            std::cerr << RED << "WindFlow Error: some MultiPipe is not runnable, check Sources and Sinks presence" << DEFAULT_COLOR << std::endl;
            status = -1;
        }
        else {
            status =ff::ff_pipeline::wait();
        }
        return status;
    }

    // run_and_wait_end method
    int run_and_wait_end()
    {
        int status = 0;
        if (!isRunnable()) {
            std::cerr << RED << "WindFlow Error: some MultiPipe is not runnable, check Sources and Sinks presence" << DEFAULT_COLOR << std::endl;
            status = -1;
        }
        else {
            status = ff::ff_pipeline::run_and_wait_end();
        }
        return status;
    }

    // check whether the MultiPipe is runnable
    bool isRunnable() const
    {
        return (has_source && has_sink) || (isMerged) || (isSplit);
    }

    // check whether the MultiPipe has a Source
    bool hasSource() const
    {
        return has_source;
    }

    // check whether the MultiPipe has a Sink
    bool hasSink() const
    {
        return has_sink;
    }

    // return the number of threads used to run this MultiPipe
    size_t getNumThreads() const
    {
        if (!isSplit) {
            return this->cardinality()-1;
        }
        else {
            size_t n = 0;
            auto first_set = last->getFirstSet();
            for (size_t i=0; i<first_set.size(); i++) {
                ff::ff_pipeline *pipe = static_cast<ff::ff_pipeline *>(first_set[i]);
                n += pipe->cardinality();
            }
            auto second_set = last->getSecondSet();
            for (size_t i=0; i<second_set.size(); i++) {
                MultiPipe *mp = static_cast<MultiPipe *>(second_set[i]);
                n += mp->getNumThreads();
            }
            return n;
        }
    }

public:
    /** 
     *  \brief Add a Filter to the MultiPipe
     *  \param _filter Filter operator to be added
     *  \return the modified MultiPipe
     */ 
    template<typename tuple_t, typename result_t>
    MultiPipe &add(Filter<tuple_t, result_t> &_filter)
    {
        // check whether the operator has already been used in a MultiPipe
        if (_filter.isUsed()) {
            std::cerr << RED << "WindFlow Error: Filter operator has already been used in a MultiPipe" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // check the type compatibility
        tuple_t t;
        std::string opInType = typeid(t).name();
        if (!outputType.empty() && outputType.compare(opInType) != 0) {
            std::cerr << RED << "WindFlow Error: output type from MultiPipe is not the input type of the Filter operator" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // call the generic method to add the operator to the MultiPipe
        if (mode == Mode::DETERMINISTIC) {
            add_operator<Standard_Emitter<tuple_t>, Ordering_Node<tuple_t>>(&_filter, _filter.getRoutingMode(), ordering_mode_t::TS);
        }
        else if (mode == Mode::PROBABILISTIC) {
            add_operator<Standard_Emitter<tuple_t>, KSlack_Node<tuple_t>>(&_filter, _filter.getRoutingMode(), ordering_mode_t::TS);
        }
        else {
            add_operator<Standard_Emitter<tuple_t>>(&_filter, _filter.getRoutingMode());
        }
        // save the new output type from this MultiPipe
        result_t r;
        outputType = typeid(r).name();
        // the Filter operator is now used
        _filter.used = true;
        // add this operator to listOperators
        listOperators->push_back(std::ref(static_cast<Basic_Operator &>(_filter)));
#if defined (TRACE_WINDFLOW)
        // update the graphviz representation
        gv_add_vertex("Filter (" + std::to_string(_filter.getParallelism()) + ")", _filter.getName(), true, false, _filter.getRoutingMode());
#endif
        return *this;
    }

    /** 
     *  \brief Chain a Filter to the MultiPipe (if possible, otherwise add it)
     *  \param _filter Filter operator to be chained
     *  \return the modified MultiPipe
     */ 
    template<typename tuple_t, typename result_t>
    MultiPipe &chain(Filter<tuple_t, result_t> &_filter)
    {
        // check whether the operator has already been used in a MultiPipe
        if (_filter.isUsed()) {
            std::cerr << RED << "WindFlow Error: Filter operator has already been used in a MultiPipe" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // check the type compatibility
        tuple_t t;
        std::string opInType = typeid(t).name();
        if (!outputType.empty() && outputType.compare(opInType) != 0) {
            std::cerr << RED << "WindFlow Error: output type from MultiPipe is not the input type of the Filter operator" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // try to chain the operator with the MultiPipe
        if (_filter.getRoutingMode() != routing_modes_t::KEYBY) {
            bool chained = chain_operator<typename Filter<tuple_t, result_t>::Filter_Node>(&_filter);
            if (!chained) {
                add(_filter);
            }
            else {
                // add this operator to listOperators
                listOperators->push_back(std::ref(static_cast<Basic_Operator &>(_filter)));         
#if defined (TRACE_WINDFLOW)
                // update the graphviz representation
                gv_chain_vertex("Filter (" + std::to_string(_filter.getParallelism()) + ")", _filter.getName());
#endif        
            }
        }
        else {
            add(_filter);
        }
        // save the new output type from this MultiPipe
        result_t r;
        outputType = typeid(r).name();
        // the Filter operator is now used
        _filter.used = true;
        return *this;
    }

    /** 
     *  \brief Add a Map to the MultiPipe
     *  \param _map Map operator to be added
     *  \return the modified MultiPipe
     */ 
    template<typename tuple_t, typename result_t>
    MultiPipe &add(Map<tuple_t, result_t> &_map)
    {
        // check whether the operator has already been used in a MultiPipe
        if (_map.isUsed()) {
            std::cerr << RED << "WindFlow Error: Map operator has already been used in a MultiPipe" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // check the type compatibility
        tuple_t t;
        std::string opInType = typeid(t).name();
        if (!outputType.empty() && outputType.compare(opInType) != 0) {
            std::cerr << RED << "WindFlow Error: output type from MultiPipe is not the input type of the Map operator" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // call the generic method to add the operator to the MultiPipe
        if (mode == Mode::DETERMINISTIC) {
            add_operator<Standard_Emitter<tuple_t>, Ordering_Node<tuple_t>>(&_map, _map.getRoutingMode(), ordering_mode_t::TS);
        }
        else if (mode == Mode::PROBABILISTIC) {
            add_operator<Standard_Emitter<tuple_t>, KSlack_Node<tuple_t>>(&_map, _map.getRoutingMode(), ordering_mode_t::TS);
        }
        else {
            add_operator<Standard_Emitter<tuple_t>>(&_map, _map.getRoutingMode());
        }
        // save the new output type from this MultiPipe
        result_t r;
        outputType = typeid(r).name();
        // the Map operator is now used
        _map.used = true;
        // add this operator to listOperators
        listOperators->push_back(std::ref(static_cast<Basic_Operator &>(_map)));
#if defined (TRACE_WINDFLOW)
        // update the graphviz representation
        gv_add_vertex("Map (" + std::to_string(_map.getParallelism()) + ")", _map.getName(), true, false, _map.getRoutingMode());
#endif
        return *this;
    }

    /** 
     *  \brief Chain a Map to the MultiPipe (if possible, otherwise add it)
     *  \param _map Map operator to be chained
     *  \return the modified MultiPipe
     */ 
    template<typename tuple_t, typename result_t>
    MultiPipe &chain(Map<tuple_t, result_t> &_map)
    {
        // check whether the operator has already been used in a MultiPipe
        if (_map.isUsed()) {
            std::cerr << RED << "WindFlow Error: Map operator has already been used in a MultiPipe" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // check the type compatibility
        tuple_t t;
        std::string opInType = typeid(t).name();
        if (!outputType.empty() && outputType.compare(opInType) != 0) {
            std::cerr << RED << "WindFlow Error: output type from MultiPipe is not the input type of the Map operator" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // try to chain the operator with the MultiPipe
        if (_map.getRoutingMode() != routing_modes_t::KEYBY) {
            bool chained = chain_operator<typename Map<tuple_t, result_t>::Map_Node>(&_map);
            if (!chained) {
                add(_map);
            }
            else {
                // add this operator to listOperators
                listOperators->push_back(std::ref(static_cast<Basic_Operator &>(_map)));
#if defined (TRACE_WINDFLOW)
                // update the graphviz representation
                gv_chain_vertex("Map (" + std::to_string(_map.getParallelism()) + ")", _map.getName());
#endif        
            }
        }
        else {
            add(_map);
        }
        // save the new output type from this MultiPipe
        result_t r;
        outputType = typeid(r).name();
        // the Map operator is now used
        _map.used = true;
        return *this;
    }

    /** 
     *  \brief Add a FlatMap to the MultiPipe
     *  \param _flatmap FlatMap operator to be added
     *  \return the modified MultiPipe
     */ 
    template<typename tuple_t, typename result_t>
    MultiPipe &add(FlatMap<tuple_t, result_t> &_flatmap)
    {
        // check whether the operator has already been used in a MultiPipe
        if (_flatmap.isUsed()) {
            std::cerr << RED << "WindFlow Error: FlatMap operator has already been used in a MultiPipe" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // check the type compatibility
        tuple_t t;
        std::string opInType = typeid(t).name();
        if (!outputType.empty() && outputType.compare(opInType) != 0) {
            std::cerr << RED << "WindFlow Error: output type from MultiPipe is not the input type of the FlatMap operator" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // call the generic method to add the operator
        if (mode == Mode::DETERMINISTIC) {
            add_operator<Standard_Emitter<tuple_t>, Ordering_Node<tuple_t>>(&_flatmap, _flatmap.getRoutingMode(), ordering_mode_t::TS);
        }
        else if (mode == Mode::PROBABILISTIC) {
            add_operator<Standard_Emitter<tuple_t>, KSlack_Node<tuple_t>>(&_flatmap, _flatmap.getRoutingMode(), ordering_mode_t::TS);
        }
        else {
            add_operator<Standard_Emitter<tuple_t>>(&_flatmap, _flatmap.getRoutingMode());
        }
        // save the new output type from this MultiPipe
        result_t r;
        outputType = typeid(r).name();
        // the FlatMap operator is now used
        _flatmap.used = true;
        // add this operator to listOperators
        listOperators->push_back(std::ref(static_cast<Basic_Operator &>(_flatmap)));
#if defined (TRACE_WINDFLOW)
        // update the graphviz representation
        gv_add_vertex("FlatMap (" + std::to_string(_flatmap.getParallelism()) + ")", _flatmap.getName(), true, false, _flatmap.getRoutingMode());
#endif
        return *this;
    }

    /** 
     *  \brief Chain a FlatMap to the MultiPipe (if possible, otherwise add it)
     *  \param _flatmap FlatMap operator to be chained
     *  \return the modified MultiPipe
     */ 
    template<typename tuple_t, typename result_t>
    MultiPipe &chain(FlatMap<tuple_t, result_t> &_flatmap)
    {
        // check whether the operator has already been used in a MultiPipe
        if (_flatmap.isUsed()) {
            std::cerr << RED << "WindFlow Error: FlatMap operator has already been used in a MultiPipe" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // check the type compatibility
        tuple_t t;
        std::string opInType = typeid(t).name();
        if (!outputType.empty() && outputType.compare(opInType) != 0) {
            std::cerr << RED << "WindFlow Error: output type from MultiPipe is not the input type of the FlatMap operator" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        if (_flatmap.getRoutingMode() != routing_modes_t::KEYBY) {
            bool chained = chain_operator<typename FlatMap<tuple_t, result_t>::FlatMap_Node>(&_flatmap);
            if (!chained) {
                add(_flatmap);
            }
            else {
                // add this operator to listOperators
                listOperators->push_back(std::ref(static_cast<Basic_Operator &>(_flatmap)));
#if defined (TRACE_WINDFLOW)
                // update the graphviz representation
                gv_chain_vertex("FlatMap (" + std::to_string(_flatmap.getParallelism()) + ")", _flatmap.getName());
#endif        
            }
        }
        else {
            add(_flatmap);
        }
        // save the new output type from this MultiPipe
        result_t r;
        outputType = typeid(r).name();
        // the FlatMap operator is now used
        _flatmap.used = true;
        return *this;
    }

    /** 
     *  \brief Add an Accumulator to the MultiPipe
     *  \param _acc Accumulator operator to be added
     *  \return the modified MultiPipe
     */ 
    template<typename tuple_t, typename result_t>
    MultiPipe &add(Accumulator<tuple_t, result_t> &_acc)
    {
        // check whether the operator has already been used in a MultiPipe
        if (_acc.isUsed()) {
            std::cerr << RED << "WindFlow Error: Accumulator operator has already been used in a MultiPipe" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // check the type compatibility
        tuple_t t;
        std::string opInType = typeid(t).name();
        if (!outputType.empty() && outputType.compare(opInType) != 0) {
            std::cerr << RED << "WindFlow Error: output type from MultiPipe is not the input type of the Accumulator operator" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // call the generic method to add the operator to the MultiPipe
        if (mode == Mode::DETERMINISTIC) {
            add_operator<Standard_Emitter<tuple_t>, Ordering_Node<tuple_t>>(&_acc, routing_modes_t::KEYBY, ordering_mode_t::TS);
        }
        else if (mode == Mode::PROBABILISTIC) {
            add_operator<Standard_Emitter<tuple_t>, KSlack_Node<tuple_t>>(&_acc, routing_modes_t::KEYBY, ordering_mode_t::TS);
        }
        else {
            add_operator<Standard_Emitter<tuple_t>>(&_acc, routing_modes_t::KEYBY);
        }
        // save the new output type from this MultiPipe
        result_t r;
        outputType = typeid(r).name();
        // the Accumulator operator is now used
        _acc.used = true;
        // add this operator to listOperators
        listOperators->push_back(std::ref(static_cast<Basic_Operator &>(_acc)));
#if defined (TRACE_WINDFLOW)
        // update the graphviz representation
        gv_add_vertex("Accum (" + std::to_string(_acc.getParallelism()) + ")", _acc.getName(), true, false, routing_modes_t::KEYBY);
#endif
        return *this;
    }

    /** 
     *  \brief Add a Win_Farm to the MultiPipe
     *  \param _wf Win_Farm operator to be added
     *  \return the modified MultiPipe
     */ 
    template<typename tuple_t, typename result_t>
    MultiPipe &add(Win_Farm<tuple_t, result_t> &_wf)
    {
        // check whether the operator has already been used in a MultiPipe
        if (_wf.isUsed()) {
            std::cerr << RED << "WindFlow Error: Win_Farm operator has already been used in a MultiPipe" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // count-based windows with Win_Farm cannot be used in DEFAULT mode
        if (_wf.getWinType() == win_type_t::CB && mode == Mode::DEFAULT) {
            std::cerr << RED << "WindFlow Error: Win_Farm cannot use count-based windows in DEFAULT mode" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // check the type compatibility
        tuple_t t;
        std::string opInType = typeid(t).name();
        if (!outputType.empty() && outputType.compare(opInType) != 0) {
            std::cerr << RED << "WindFlow Error: output type from MultiPipe is not the input type of the Win_Farm operator" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // check whether the Win_Farm has complex parallel replicas inside
        if (_wf.isComplexNesting()) {
            // check whether internal replicas have been prepared for nesting
            if (_wf.getOptLevel() != opt_level_t::LEVEL2 || _wf.getInnerOptLevel() != opt_level_t::LEVEL2) {
                std::cerr << RED << "WindFlow Error: tried a nesting without preparing the inner operator" << DEFAULT_COLOR << std::endl;
                exit(EXIT_FAILURE);
            }
            else {
                // inner replica is a Pane_Farm
                if(_wf.getInnerType() == pattern_t::PF_CPU) {
                    // check the parallelism degree of the PLQ stage
                    if ((_wf.getInnerParallelisms()).first > 1) {
                        if (_wf.getWinType() == win_type_t::TB) {
                            // call the generic method to add the operator to the MultiPipe
                            if (mode == Mode::DETERMINISTIC) {
                                add_operator<Tree_Emitter, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_wf, routing_modes_t::COMPLEX, ordering_mode_t::TS);
                            }
                            else if (mode == Mode::PROBABILISTIC) {
                                add_operator<Tree_Emitter, KSlack_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_wf, routing_modes_t::COMPLEX, ordering_mode_t::TS);
                            }
                            else {
                                add_operator<Tree_Emitter>(&_wf, routing_modes_t::COMPLEX);
                            }
                        }
                        else {
                            // special case count-based windows
                            _wf.change_emitter(new Broadcast_Emitter<tuple_t>(_wf.getNumComplexReplicas() * (_wf.getInnerParallelisms()).first), true);
                            // call the generic method to add the operator to the MultiPipe
                            if (mode == Mode::DETERMINISTIC) {
                                add_operator<Broadcast_Emitter<tuple_t>, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_wf, routing_modes_t::COMPLEX, ordering_mode_t::TS_RENUMBERING);
                            }
                            else if (mode == Mode::PROBABILISTIC) {
                                add_operator<Broadcast_Emitter<tuple_t>, KSlack_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_wf, routing_modes_t::COMPLEX, ordering_mode_t::TS_RENUMBERING);
                            }
                            else {
                                abort(); // not supported CB windows in this case
                            }
                        }
                    }
                    else {
                        if (_wf.getWinType() == win_type_t::TB) {
                            // call the generic method to add the operator to the MultiPipe
                            if (mode == Mode::DETERMINISTIC) {
                                add_operator<WF_Emitter<tuple_t>, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_wf, routing_modes_t::COMPLEX, ordering_mode_t::TS);
                            }
                            else if (mode == Mode::PROBABILISTIC) {
                                add_operator<WF_Emitter<tuple_t>, KSlack_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_wf, routing_modes_t::COMPLEX, ordering_mode_t::TS);
                            }
                            else {
                               add_operator<WF_Emitter<tuple_t>>(&_wf, routing_modes_t::COMPLEX);
                            }
                        }
                        else {
                            // special case count-based windows
                            _wf.change_emitter(new Broadcast_Emitter<tuple_t>(_wf.getNumComplexReplicas()), true);
                            // call the generic method to add the operator to the MultiPipe
                            if (mode == Mode::DETERMINISTIC) {
                                add_operator<Broadcast_Emitter<tuple_t>, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_wf, routing_modes_t::COMPLEX, ordering_mode_t::TS_RENUMBERING);
                            }
                            else if (mode == Mode::PROBABILISTIC) {
                                add_operator<Broadcast_Emitter<tuple_t>, KSlack_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_wf, routing_modes_t::COMPLEX, ordering_mode_t::TS_RENUMBERING);
                            }
                            else {
                                abort(); // not supported CB windows in this case
                            }
                        }
                    }
#if defined (TRACE_WINDFLOW)
                    // update the graphviz representation
                    gv_add_vertex("WF[PF(" + std::to_string(_wf.getInnerParallelisms().first) + "," + std::to_string(_wf.getInnerParallelisms().second) + "), " + std::to_string(_wf.getNumComplexReplicas()) + "]", _wf.getName(), true, true, routing_modes_t::COMPLEX);
#endif
                }
                // inner replica is a Win_MapReduce
                else if(_wf.getInnerType() == pattern_t::WMR_CPU) {
                    if (_wf.getWinType() == win_type_t::TB) {
                        // call the generic method to add the operator to the MultiPipe
                        if (mode == Mode::DETERMINISTIC) {
                            add_operator<Tree_Emitter, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_wf, routing_modes_t::COMPLEX, ordering_mode_t::TS);
                        }
                        else if (mode == Mode::PROBABILISTIC) {
                            add_operator<Tree_Emitter, KSlack_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_wf, routing_modes_t::COMPLEX, ordering_mode_t::TS);
                        }
                        else {
                            add_operator<Tree_Emitter>(&_wf, routing_modes_t::COMPLEX);
                        }
                    }
                    else {
                        // special case count-based windows
                        _wf.change_emitter(new Broadcast_Emitter<tuple_t>(_wf.getNumComplexReplicas() * (_wf.getInnerParallelisms()).first), true);
                        size_t n_map = (_wf.getInnerParallelisms()).first;
                        for (size_t i=0; i<_wf.getNumComplexReplicas(); i++) {
                            ff::ff_pipeline *pipe = static_cast<ff::ff_pipeline *>((_wf.getWorkers())[i]);
                            auto &stages = pipe->getStages();
                            ff::ff_a2a *a2a = static_cast<ff::ff_a2a *>(stages[0]);
                            std::vector<ff::ff_node *> nodes;
                            for (size_t j=0; j<n_map; j++) {
                                nodes.push_back(new WinMap_Dropper<tuple_t>(j, n_map));
                            }
                            combine_a2a_withFirstNodes(a2a, nodes, true);
                        }
                        // call the generic method to add the operator to the MultiPipe
                        if (mode == Mode::DETERMINISTIC) {
                            add_operator<Broadcast_Emitter<tuple_t>, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_wf, routing_modes_t::COMPLEX, ordering_mode_t::TS_RENUMBERING);
                        }
                        else if (mode == Mode::PROBABILISTIC) {
                            add_operator<Broadcast_Emitter<tuple_t>, KSlack_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_wf, routing_modes_t::COMPLEX, ordering_mode_t::TS_RENUMBERING);
                        }
                        else {
                            abort(); // not supported CB windows in this case
                        } 
                    }
#if defined (TRACE_WINDFLOW)
                    // update the graphviz representation
                    gv_add_vertex("WF[WMR(" + std::to_string(_wf.getInnerParallelisms().first) + "," + std::to_string(_wf.getInnerParallelisms().second) + "), " + std::to_string(_wf.getNumComplexReplicas()) + "]", _wf.getName(), true, true, routing_modes_t::COMPLEX);
#endif
                }
                forceShuffling = true;
            }
        }
        // case with Win_Seq replicas inside
        else {
            if (_wf.getWinType() == win_type_t::TB) {
                // call the generic method to add the operator to the MultiPipe
                if (mode == Mode::DETERMINISTIC) {
                    add_operator<WF_Emitter<tuple_t>, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_wf, routing_modes_t::COMPLEX, ordering_mode_t::TS);
                }
                else if (mode == Mode::PROBABILISTIC) {
                    add_operator<WF_Emitter<tuple_t>, KSlack_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_wf, routing_modes_t::COMPLEX, ordering_mode_t::TS);
                }
                else {
                    add_operator<WF_Emitter<tuple_t>>(&_wf, routing_modes_t::COMPLEX);
                }
            }
            else {
                // special case count-based windows
                _wf.change_emitter(new Broadcast_Emitter<tuple_t>(_wf.getParallelism()), true);
                // call the generic method to add the operator to the MultiPipe
                if (mode == Mode::DETERMINISTIC) {
                    add_operator<Broadcast_Emitter<tuple_t>, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_wf, routing_modes_t::COMPLEX, ordering_mode_t::TS_RENUMBERING);
                }
                else if (mode == Mode::PROBABILISTIC) {
                    add_operator<Broadcast_Emitter<tuple_t>, KSlack_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_wf, routing_modes_t::COMPLEX, ordering_mode_t::TS_RENUMBERING);
                }
                else {
                    abort(); // not supported CB windows in this case
                }
            }
#if defined (TRACE_WINDFLOW)
            // update the graphviz representation
            gv_add_vertex("WF (" + std::to_string(_wf.getParallelism()) + ")", _wf.getName(), true, false, routing_modes_t::COMPLEX);
#endif
        }
        // save the new output type from this MultiPipe
        result_t r;
        outputType = typeid(r).name();
        // the Win_Farm operator is now used
        _wf.used = true;
        // add this operator to listOperators
        listOperators->push_back(std::ref(static_cast<Basic_Operator &>(_wf)));
        return *this;
    }

    /** 
     *  \brief Add a Win_Farm_GPU to the MultiPipe
     *  \param _wf Win_Farm_GPU operator to be added
     *  \return the modified MultiPipe
     */ 
    template<typename tuple_t, typename result_t, typename F_t>
    MultiPipe &add(Win_Farm_GPU<tuple_t, result_t, F_t> &_wf)
    {
        // check whether the operator has already been used in a MultiPipe
        if (_wf.isUsed()) {
            std::cerr << RED << "WindFlow Error: Win_Farm_GPU operator has already been used in a MultiPipe" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // count-based windows with Win_Farm_GPU cannot be used in DEFAULT mode
        if (_wf.getWinType() == win_type_t::CB && mode == Mode::DEFAULT) {
            std::cerr << RED << "WindFlow Error: Win_Farm_GPU cannot use count-based windows in DEFAULT mode" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // check the type compatibility
        tuple_t t;
        std::string opInType = typeid(t).name();
        if (!outputType.empty() && outputType.compare(opInType) != 0) {
            std::cerr << RED << "WindFlow Error: output type from MultiPipe is not the input type of the Win_Farm_GPU operator" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // check whether the Win_Farm_GPU has complex parallel replicas inside
        if (_wf.isComplexNesting()) {
            // check whether internal replicas have been prepared for nesting
            if (_wf.getOptLevel() != opt_level_t::LEVEL2 || _wf.getInnerOptLevel() != opt_level_t::LEVEL2) {
                std::cerr << RED << "WindFlow Error: tried a nesting without preparing the inner operator" << DEFAULT_COLOR << std::endl;
                exit(EXIT_FAILURE);
            }
            else {
                // inner replica is a Pane_Farm_GPU
                if(_wf.getInnerType() == pattern_t::PF_GPU) {
                    // check the parallelism degree of the PLQ stage
                    if ((_wf.getInnerParallelisms()).first > 1) {
                        if (_wf.getWinType() == win_type_t::TB) {
                            // call the generic method to add the operator to the MultiPipe
                            if (mode == Mode::DETERMINISTIC) {
                                add_operator<Tree_Emitter, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_wf, routing_modes_t::COMPLEX, ordering_mode_t::TS);
                            }
                            else if (mode == Mode::PROBABILISTIC) {
                                add_operator<Tree_Emitter, KSlack_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_wf, routing_modes_t::COMPLEX, ordering_mode_t::TS);
                            }
                            else {
                                add_operator<Tree_Emitter>(&_wf, routing_modes_t::COMPLEX);
                            }
                        }
                        else {
                            // special case count-based windows
                            _wf.change_emitter(new Broadcast_Emitter<tuple_t>(_wf.getNumComplexReplicas() * (_wf.getInnerParallelisms()).first), true);
                            // call the generic method to add the operator to the MultiPipe
                            if (mode == Mode::DETERMINISTIC) {
                                add_operator<Broadcast_Emitter<tuple_t>, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_wf, routing_modes_t::COMPLEX, ordering_mode_t::TS_RENUMBERING);
                            }
                            else if (mode == Mode::PROBABILISTIC) {
                                add_operator<Broadcast_Emitter<tuple_t>, KSlack_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_wf, routing_modes_t::COMPLEX, ordering_mode_t::TS_RENUMBERING);
                            }
                            else {
                                abort(); // not supported CB windows in this case
                            }                        
                        }
                    }
                    else {
                        if (_wf.getWinType() == win_type_t::TB) {
                            // call the generic method to add the operator to the MultiPipe
                            if (mode == Mode::DETERMINISTIC) {
                                add_operator<WF_Emitter<tuple_t>, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_wf, routing_modes_t::COMPLEX, ordering_mode_t::TS);
                            }
                            else if (mode == Mode::PROBABILISTIC) {
                                 add_operator<WF_Emitter<tuple_t>, KSlack_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_wf, routing_modes_t::COMPLEX, ordering_mode_t::TS);
                            }
                            else {
                                add_operator<WF_Emitter<tuple_t>>(&_wf, routing_modes_t::COMPLEX);
                            }
                        }
                        else {
                            // special case count-based windows
                            _wf.change_emitter(new Broadcast_Emitter<tuple_t>(_wf.getNumComplexReplicas()), true);
                            // call the generic method to add the operator to the MultiPipe
                            if (mode == Mode::DETERMINISTIC) {
                                add_operator<Broadcast_Emitter<tuple_t>, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_wf, routing_modes_t::COMPLEX, ordering_mode_t::TS_RENUMBERING);
                            }
                            else if (mode == Mode::PROBABILISTIC) {
                                add_operator<Broadcast_Emitter<tuple_t>, KSlack_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_wf, routing_modes_t::COMPLEX, ordering_mode_t::TS_RENUMBERING);
                            }
                            else {
                                abort(); // not supported CB windows in this case
                            }
                        }
                    }
#if defined (TRACE_WINDFLOW)
                    // update the graphviz representation
                    gv_add_vertex("WF_GPU[PF_GPU(" + std::to_string(_wf.getInnerParallelisms().first) + "," + std::to_string(_wf.getInnerParallelisms().second) + "), " + std::to_string(_wf.getNumComplexReplicas()) + "]", _wf.getName(), false, true, routing_modes_t::COMPLEX);
#endif
                }
                // inner replica is a Win_MapReduce_GPU
                else if(_wf.getInnerType() == pattern_t::WMR_GPU) {
                    if (_wf.getWinType() == win_type_t::TB) {
                        // call the generic method to add the operator to the MultiPipe
                        if (mode == Mode::DETERMINISTIC) {
                            add_operator<Tree_Emitter, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_wf, routing_modes_t::COMPLEX, ordering_mode_t::TS);
                        }
                        else if (mode == Mode::PROBABILISTIC) {
                            add_operator<Tree_Emitter, KSlack_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_wf, routing_modes_t::COMPLEX, ordering_mode_t::TS);
                        }
                        else {
                            add_operator<Tree_Emitter>(&_wf, routing_modes_t::COMPLEX);
                        }
                    }
                    else {
                        // special case count-based windows
                        _wf.change_emitter(new Broadcast_Emitter<tuple_t>(_wf.getNumComplexReplicas() * (_wf.getInnerParallelisms()).first), true);
                        size_t n_map = (_wf.getInnerParallelisms()).first;
                        for (size_t i=0; i<_wf.getNumComplexReplicas(); i++) {
                            ff::ff_pipeline *pipe = static_cast<ff::ff_pipeline *>((_wf.getWorkers())[i]);
                            auto &stages = pipe->getStages();
                            ff::ff_a2a *a2a = static_cast<ff::ff_a2a *>(stages[0]);
                            std::vector<ff::ff_node *> nodes;
                            for (size_t j=0; j<n_map; j++) {
                                nodes.push_back(new WinMap_Dropper<tuple_t>(j, n_map));
                            }
                            combine_a2a_withFirstNodes(a2a, nodes, true);
                        }
                        // call the generic method to add the operator to the MultiPipe
                        if (mode == Mode::DETERMINISTIC) {
                            add_operator<Broadcast_Emitter<tuple_t>, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_wf, routing_modes_t::COMPLEX, ordering_mode_t::TS_RENUMBERING);
                        }
                        else if (mode == Mode::PROBABILISTIC) {
                            add_operator<Broadcast_Emitter<tuple_t>, KSlack_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_wf, routing_modes_t::COMPLEX, ordering_mode_t::TS_RENUMBERING);
                        }
                        else {
                            abort(); // not supported CB windows in this case
                        }
                    }
#if defined (TRACE_WINDFLOW)
                    // update the graphviz representation
                    gv_add_vertex("WF_GPU[WMR_GPU(" + std::to_string(_wf.getInnerParallelisms().first) + "," + std::to_string(_wf.getInnerParallelisms().second) + "), " + std::to_string(_wf.getNumComplexReplicas()) + "]", _wf.getName(), false, true, routing_modes_t::COMPLEX);
#endif
                }
                forceShuffling = true;
            }
        }
        // case with Win_Seq_GPU replicas inside
        else {
            if (_wf.getWinType() == win_type_t::TB) {
                // call the generic method to add the operator to the MultiPipe
                if (mode == Mode::DETERMINISTIC) {
                    add_operator<WF_Emitter<tuple_t>, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_wf, routing_modes_t::COMPLEX, ordering_mode_t::TS);
                }
                else if (mode == Mode::PROBABILISTIC) {
                    add_operator<WF_Emitter<tuple_t>, KSlack_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_wf, routing_modes_t::COMPLEX, ordering_mode_t::TS);
                }
                else {
                    add_operator<WF_Emitter<tuple_t>>(&_wf, routing_modes_t::COMPLEX);
                }
            }
            else {
                // special case count-based windows
                _wf.change_emitter(new Broadcast_Emitter<tuple_t>(_wf.getParallelism()), true);
                // call the generic method to add the operator to the MultiPipe
                if (mode == Mode::DETERMINISTIC) {
                    add_operator<Broadcast_Emitter<tuple_t>, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_wf, routing_modes_t::COMPLEX, ordering_mode_t::TS_RENUMBERING);
                }
                else if (mode == Mode::PROBABILISTIC) {
                    add_operator<Broadcast_Emitter<tuple_t>, KSlack_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_wf, routing_modes_t::COMPLEX, ordering_mode_t::TS_RENUMBERING);
                }
                else {
                    abort(); // not supported CB windows in this case
                }
            }
#if defined (TRACE_WINDFLOW)
            // update the graphviz representation
            gv_add_vertex("WF_GPU (" + std::to_string(_wf.getParallelism()) + ")", _wf.getName(), false, false, routing_modes_t::COMPLEX);
#endif
        }
        // save the new output type from this MultiPipe
        result_t r;
        outputType = typeid(r).name();
        // the Win_Farm_GPU operator is now used
        _wf.used = true;
        // add this operator to listOperators
        listOperators->push_back(std::ref(static_cast<Basic_Operator &>(_wf)));
        return *this;
    }

    /** 
     *  \brief Add a Key_Farm to the MultiPipe
     *  \param _kf Key_Farm operator to be added
     *  \return the modified MultiPipe
     */ 
    template<typename tuple_t, typename result_t>
    MultiPipe &add(Key_Farm<tuple_t, result_t> &_kf)
    {
        // check whether the operator has already been used in a MultiPipe
        if (_kf.isUsed()) {
            std::cerr << RED << "WindFlow Error: Key_Farm operator has already been used in a MultiPipe" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // count-based windows and DEFAULT mode possible only without complex nested structures
        if (_kf.getWinType() == win_type_t::CB && mode == Mode::DEFAULT) {
            if (!_kf.isComplexNesting()) {
                // set the isRenumbering mode of the input operator
                _kf.set_isRenumbering();
            }
            else {
                std::cerr << RED << "WindFlow Error: count-based windows cannot be used in DEFAULT mode with complex nested structures" << DEFAULT_COLOR << std::endl;
                exit(EXIT_FAILURE);
            }
        }
        // check the type compatibility
        tuple_t t;
        std::string opInType = typeid(t).name();
        if (!outputType.empty() && outputType.compare(opInType) != 0) {
            std::cerr << RED << "WindFlow Error: output type from MultiPipe is not the input type of the Key_Farm operator" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // check whether the Key_Farm has complex parallel replicas inside
        if (_kf.isComplexNesting()) {
            // check whether internal replicas have been prepared for nesting
            if (_kf.getOptLevel() != opt_level_t::LEVEL2 || _kf.getInnerOptLevel() != opt_level_t::LEVEL2) {
                std::cerr << RED << "WindFlow Error: tried a nesting without preparing the inner operator" << DEFAULT_COLOR << std::endl;
                exit(EXIT_FAILURE);
            }
            else {
                // inner replica is a Pane_Farm
                if(_kf.getInnerType() == pattern_t::PF_CPU) {
                    // check the parallelism of the PLQ stage
                    if ((_kf.getInnerParallelisms()).first > 1) {
                        if (_kf.getWinType() == win_type_t::TB) {
                            // call the generic method to add the operator to the MultiPipe
                            if (mode == Mode::DETERMINISTIC) {
                                add_operator<Tree_Emitter, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_kf, routing_modes_t::COMPLEX, ordering_mode_t::TS);
                            }
                            else if (mode == Mode::PROBABILISTIC) {
                                add_operator<Tree_Emitter, KSlack_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_kf, routing_modes_t::COMPLEX, ordering_mode_t::TS);
                            }
                            else {
                                add_operator<Tree_Emitter>(&_kf, routing_modes_t::COMPLEX);
                            }
                        }
                        else {
                            // special case count-based windows
                            auto *emitter = static_cast<Tree_Emitter *>(_kf.getEmitter());
                            KF_Emitter<tuple_t> *rootnode = new KF_Emitter<tuple_t>(*(static_cast<KF_Emitter<tuple_t> *>(emitter->getRootNode())));
                            std::vector<Basic_Emitter *> children;
                            size_t n_plq = (_kf.getInnerParallelisms()).first;
                            for (size_t i=0; i<_kf.getNumComplexReplicas(); i++) {
                                auto *b_node = new Broadcast_Emitter<tuple_t>(n_plq);
                                b_node->setTree_EmitterMode(true);
                                children.push_back(b_node);
                            }
                            auto *new_emitter = new Tree_Emitter(rootnode, children, true, true);
                            _kf.change_emitter(new_emitter, true);
                            // call the generic method to add the operator to the MultiPipe
                            if (mode == Mode::DETERMINISTIC) {
                                add_operator<Tree_Emitter, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_kf, routing_modes_t::COMPLEX, ordering_mode_t::TS_RENUMBERING);
                            }
                            else if (mode == Mode::PROBABILISTIC) {
                                add_operator<Tree_Emitter, KSlack_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_kf, routing_modes_t::COMPLEX, ordering_mode_t::TS_RENUMBERING);
                            }
                            else {
                                abort(); // not supported CB windows in this case
                            }
                        }
                    }
                    else {
                        // call the generic method to add the operator to the MultiPipe
                        if (_kf.getWinType() == win_type_t::TB) {
                            if (mode == Mode::DETERMINISTIC) {
                                add_operator<KF_Emitter<tuple_t>, Ordering_Node<tuple_t>>(&_kf, routing_modes_t::COMPLEX, ordering_mode_t::TS);
                            }
                            else if (mode == Mode::PROBABILISTIC) {
                                add_operator<KF_Emitter<tuple_t>, KSlack_Node<tuple_t>>(&_kf, routing_modes_t::COMPLEX, ordering_mode_t::TS);
                            }
                            else {
                                add_operator<KF_Emitter<tuple_t>>(&_kf, routing_modes_t::COMPLEX);
                            }
                        }
                        else {
                            if (mode == Mode::DETERMINISTIC) {
                                add_operator<KF_Emitter<tuple_t>, Ordering_Node<tuple_t>>(&_kf, routing_modes_t::COMPLEX, ordering_mode_t::TS_RENUMBERING);
                            }
                            else if (mode == Mode::PROBABILISTIC) {
                                add_operator<KF_Emitter<tuple_t>, KSlack_Node<tuple_t>>(&_kf, routing_modes_t::COMPLEX, ordering_mode_t::TS_RENUMBERING);
                            }
                            else {
                                abort(); // not supported CB windows in this case
                            }
                        }
                    }
#if defined (TRACE_WINDFLOW)
                    // update the graphviz representation
                    gv_add_vertex("KF[PF(" + std::to_string(_kf.getInnerParallelisms().first) + "," + std::to_string(_kf.getInnerParallelisms().second) + "), " + std::to_string(_kf.getNumComplexReplicas()) + "]", _kf.getName(), true, true, routing_modes_t::KEYBY);
#endif
                }
                // inner replica is a Win_MapReduce
                else if(_kf.getInnerType() == pattern_t::WMR_CPU) {
                    if (_kf.getWinType() == win_type_t::TB) {
                        // call the generic method to add the operator to the MultiPipe
                        if (mode == Mode::DETERMINISTIC) {
                            add_operator<Tree_Emitter, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_kf, routing_modes_t::COMPLEX, ordering_mode_t::TS);
                        }
                        else if (mode == Mode::PROBABILISTIC) {
                            add_operator<Tree_Emitter, KSlack_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_kf, routing_modes_t::COMPLEX, ordering_mode_t::TS);
                        }
                        else {
                            add_operator<Tree_Emitter>(&_kf, routing_modes_t::COMPLEX);
                        }
                    }
                    else {
                        // special case count-based windows
                        auto *emitter = static_cast<Tree_Emitter *>(_kf.getEmitter());
                        KF_Emitter<tuple_t> *rootnode = new KF_Emitter<tuple_t>(*(static_cast<KF_Emitter<tuple_t> *>(emitter->getRootNode())));
                        std::vector<Basic_Emitter *> children;
                        size_t n_map = (_kf.getInnerParallelisms()).first;
                        for (size_t i=0; i<_kf.getNumComplexReplicas(); i++) {
                            auto *b_node = new Broadcast_Emitter<tuple_t>(n_map);
                            b_node->setTree_EmitterMode(true);
                            children.push_back(b_node);
                        }
                        auto *new_emitter = new Tree_Emitter(rootnode, children, true, true);
                        _kf.change_emitter(new_emitter, true);
                        for (size_t i=0; i<_kf.getNumComplexReplicas(); i++) {
                            ff::ff_pipeline *pipe = static_cast<ff::ff_pipeline *>((_kf.getWorkers())[i]);
                            auto &stages = pipe->getStages();
                            ff::ff_a2a *a2a = static_cast<ff::ff_a2a *>(stages[0]);
                            std::vector<ff::ff_node *> nodes;
                            for (size_t j=0; j<n_map; j++) {
                                nodes.push_back(new WinMap_Dropper<tuple_t>(j, n_map));
                            }
                            combine_a2a_withFirstNodes(a2a, nodes, true);
                        }
                        // call the generic method to add the operator to the MultiPipe
                        if (mode == Mode::DETERMINISTIC) {
                            add_operator<Tree_Emitter, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_kf, routing_modes_t::COMPLEX, ordering_mode_t::TS_RENUMBERING);
                        }
                        else if (mode == Mode::PROBABILISTIC) {
                            add_operator<Tree_Emitter, KSlack_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_kf, routing_modes_t::COMPLEX, ordering_mode_t::TS_RENUMBERING);
                        }
                        else {
                            abort(); // not supported CB windows in this case
                        }
                    }
#if defined (TRACE_WINDFLOW)
                    // update the graphviz representation
                    gv_add_vertex("KF[WMR(" + std::to_string(_kf.getInnerParallelisms().first) + "," + std::to_string(_kf.getInnerParallelisms().second) + "), " + std::to_string(_kf.getNumComplexReplicas()) + "]", _kf.getName(), true, true, routing_modes_t::KEYBY);
#endif
                }
                forceShuffling = true;
            }
        }
        // case with Win_Seq replicas inside
        else {
            // call the generic method to add the operator to the MultiPipe
            if (_kf.getWinType() == win_type_t::TB) {
                if (mode == Mode::DETERMINISTIC) {
                    add_operator<KF_Emitter<tuple_t>, Ordering_Node<tuple_t>>(&_kf, routing_modes_t::COMPLEX, ordering_mode_t::TS);
                }
                else if (mode == Mode::PROBABILISTIC) {
                    add_operator<KF_Emitter<tuple_t>, KSlack_Node<tuple_t>>(&_kf, routing_modes_t::COMPLEX, ordering_mode_t::TS);
                }
                else {
                    add_operator<KF_Emitter<tuple_t>>(&_kf, routing_modes_t::COMPLEX);
                }
            }
            else {
                if (mode == Mode::DETERMINISTIC) {
                    add_operator<KF_Emitter<tuple_t>, Ordering_Node<tuple_t>>(&_kf, routing_modes_t::COMPLEX, ordering_mode_t::TS_RENUMBERING);
                }
                else if (mode == Mode::PROBABILISTIC) {
                    add_operator<KF_Emitter<tuple_t>, KSlack_Node<tuple_t>>(&_kf, routing_modes_t::COMPLEX, ordering_mode_t::TS_RENUMBERING);
                }
                else {
                    add_operator<KF_Emitter<tuple_t>>(&_kf, routing_modes_t::COMPLEX);
                }
            }
#if defined (TRACE_WINDFLOW)
            // update the graphviz representation
            gv_add_vertex("KF (" + std::to_string(_kf.getParallelism()) + ")", _kf.getName(), true, false, routing_modes_t::KEYBY);
#endif
        }
        // save the new output type from this MultiPipe
        result_t r;
        outputType = typeid(r).name();
        // the Key_Farm operator is now used
        _kf.used = true;
        // add this operator to listOperators
        listOperators->push_back(std::ref(static_cast<Basic_Operator &>(_kf)));
        return *this;
    }

    /** 
     *  \brief Add a Key_Farm_GPU to the MultiPipe
     *  \param _kf Key_Farm_GPU operator to be added
     *  \return the modified MultiPipe
     */ 
    template<typename tuple_t, typename result_t, typename F_t>
    MultiPipe &add(Key_Farm_GPU<tuple_t, result_t, F_t> &_kf)
    {
        // check whether the operator has already been used in a MultiPipe
        if (_kf.isUsed()) {
            std::cerr << RED << "WindFlow Error: Key_Farm_GPU operator has already been used in a MultiPipe" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // count-based windows and DEFAULT mode possible only without complex nested structures
        if (_kf.getWinType() == win_type_t::CB && mode == Mode::DEFAULT) {
            if (!_kf.isComplexNesting()) {
                // set the isRenumbering mode of the input operator
                _kf.set_isRenumbering();
            }
            else {
                std::cerr << RED << "WindFlow Error: count-based windows cannot be used in DEFAULT mode with complex nested structures" << DEFAULT_COLOR << std::endl;
                exit(EXIT_FAILURE);
            }
        }
        // check the type compatibility
        tuple_t t;
        std::string opInType = typeid(t).name();
        if (!outputType.empty() && outputType.compare(opInType) != 0) {
            std::cerr << RED << "WindFlow Error: output type from MultiPipe is not the input type of the Key_Farm_GPU operator" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // check whether the Key_Farm_GPU has complex parallel replicas inside
        if (_kf.isComplexNesting()) {
            // check whether internal replicas have been prepared for nesting
            if (_kf.getOptLevel() != opt_level_t::LEVEL2 || _kf.getInnerOptLevel() != opt_level_t::LEVEL2) {
                std::cerr << RED << "WindFlow Error: tried a nesting without preparing the inner operator" << DEFAULT_COLOR << std::endl;
                exit(EXIT_FAILURE);
            }
            else {
                // inner replica is a Pane_Farm_GPU
                if(_kf.getInnerType() == pattern_t::PF_GPU) {
                    // check the parallelism degree of the PLQ stage
                    if ((_kf.getInnerParallelisms()).first > 1) {
                        if (_kf.getWinType() == win_type_t::TB) {
                            // call the generic method to add the operator to the MultiPipe
                            if (mode == Mode::DETERMINISTIC) {
                                add_operator<Tree_Emitter, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_kf, routing_modes_t::COMPLEX, ordering_mode_t::TS);
                            }
                            else if (mode == Mode::PROBABILISTIC) {
                                add_operator<Tree_Emitter, KSlack_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_kf, routing_modes_t::COMPLEX, ordering_mode_t::TS);
                            }
                            else {
                                add_operator<Tree_Emitter>(&_kf, routing_modes_t::COMPLEX);
                            }
                        }
                        else {
                            // special case count-based windows
                            auto *emitter = static_cast<Tree_Emitter *>(_kf.getEmitter());
                            KF_Emitter<tuple_t> *rootnode = new KF_Emitter<tuple_t>(*(static_cast<KF_Emitter<tuple_t> *>(emitter->getRootNode())));
                            std::vector<Basic_Emitter *> children;
                            size_t n_plq = (_kf.getInnerParallelisms()).first;
                            for (size_t i=0; i<_kf.getNumComplexReplicas(); i++) {
                                auto *b_node = new Broadcast_Emitter<tuple_t>(n_plq);
                                b_node->setTree_EmitterMode(true);
                                children.push_back(b_node);
                            }
                            auto *new_emitter = new Tree_Emitter(rootnode, children, true, true);
                            _kf.change_emitter(new_emitter, true);
                            // call the generic method to add the operator to the MultiPipe
                            if (mode == Mode::DETERMINISTIC) {
                                add_operator<Tree_Emitter, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_kf, routing_modes_t::COMPLEX, ordering_mode_t::TS_RENUMBERING);
                            }
                            else if (mode == Mode::PROBABILISTIC) {
                                add_operator<Tree_Emitter, KSlack_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_kf, routing_modes_t::COMPLEX, ordering_mode_t::TS_RENUMBERING);
                            }
                            else {
                                abort(); // not supported CB windows in this case
                            }
                        }
                    }
                    else {
                        // call the generic method to add the operator to the MultiPipe
                        if (_kf.getWinType() == win_type_t::TB) {
                            if (mode == Mode::DETERMINISTIC) {
                                add_operator<KF_Emitter<tuple_t>, Ordering_Node<tuple_t>>(&_kf, routing_modes_t::COMPLEX, ordering_mode_t::TS);
                            }
                            else if (mode == Mode::PROBABILISTIC) {
                                add_operator<KF_Emitter<tuple_t>, KSlack_Node<tuple_t>>(&_kf, routing_modes_t::COMPLEX, ordering_mode_t::TS);
                            }
                            else {
                                add_operator<KF_Emitter<tuple_t>>(&_kf, routing_modes_t::COMPLEX);
                            }
                        }
                        else {
                            if (mode == Mode::DETERMINISTIC) {
                                add_operator<KF_Emitter<tuple_t>, Ordering_Node<tuple_t>>(&_kf, routing_modes_t::COMPLEX, ordering_mode_t::TS_RENUMBERING);
                            }
                            else if (mode == Mode::PROBABILISTIC) {
                                add_operator<KF_Emitter<tuple_t>, KSlack_Node<tuple_t>>(&_kf, routing_modes_t::COMPLEX, ordering_mode_t::TS_RENUMBERING);
                            }
                            else {
                                abort(); // not supported CB windows in this case
                            }
                        }
                    }
#if defined (TRACE_WINDFLOW)
                    // update the graphviz representation
                    gv_add_vertex("KF_GPU[PF_GPU(" + std::to_string(_kf.getInnerParallelisms().first) + "," + std::to_string(_kf.getInnerParallelisms().second) + "), " + std::to_string(_kf.getNumComplexReplicas()) + "]", _kf.getName(), false, true, routing_modes_t::KEYBY);
#endif
                }
                // inner replica is a Win_MapReduce_GPU
                else if(_kf.getInnerType() == pattern_t::WMR_GPU) {
                    if (_kf.getWinType() == win_type_t::TB) {
                        // call the generic method to add the operator to the MultiPipe
                        if (mode == Mode::DETERMINISTIC) {
                            add_operator<Tree_Emitter, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_kf, routing_modes_t::COMPLEX, ordering_mode_t::TS);
                        }
                        else if (mode == Mode::PROBABILISTIC) {
                            add_operator<Tree_Emitter, KSlack_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_kf, routing_modes_t::COMPLEX, ordering_mode_t::TS);
                        }
                        else {
                            add_operator<Tree_Emitter>(&_kf, routing_modes_t::COMPLEX);
                        }
                    }
                    else {
                        // special case count-based windows
                        auto *emitter = static_cast<Tree_Emitter *>(_kf.getEmitter());
                        KF_Emitter<tuple_t> *rootnode = new KF_Emitter<tuple_t>(*(static_cast<KF_Emitter<tuple_t> *>(emitter->getRootNode())));
                        std::vector<Basic_Emitter *> children;
                        size_t n_map = (_kf.getInnerParallelisms()).first;
                        for (size_t i=0; i<_kf.getNumComplexReplicas(); i++) {
                            auto *b_node = new Broadcast_Emitter<tuple_t>(n_map);
                            b_node->setTree_EmitterMode(true);
                            children.push_back(b_node);
                        }
                        auto *new_emitter = new Tree_Emitter(rootnode, children, true, true);
                        _kf.change_emitter(new_emitter, true);
                        for (size_t i=0; i<_kf.getNumComplexReplicas(); i++) {
                            ff::ff_pipeline *pipe = static_cast<ff::ff_pipeline *>((_kf.getWorkers())[i]);
                            auto &stages = pipe->getStages();
                            ff::ff_a2a *a2a = static_cast<ff::ff_a2a *>(stages[0]);
                            std::vector<ff::ff_node *> nodes;
                            for (size_t j=0; j<n_map; j++)
                                nodes.push_back(new WinMap_Dropper<tuple_t>(j, n_map));
                            combine_a2a_withFirstNodes(a2a, nodes, true);
                        }
                        // call the generic method to add the operator to the MultiPipe
                        if (mode == Mode::DETERMINISTIC) {
                            add_operator<Tree_Emitter, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_kf, routing_modes_t::COMPLEX, ordering_mode_t::TS_RENUMBERING);
                        }
                        else if (mode == Mode::PROBABILISTIC) {
                            add_operator<Tree_Emitter, KSlack_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_kf, routing_modes_t::COMPLEX, ordering_mode_t::TS_RENUMBERING);
                        }
                        else {
                            abort(); // not supported CB windows in this case
                        }
                    }
#if defined (TRACE_WINDFLOW)
                    // update the graphviz representation
                    gv_add_vertex("KF_GPU[WMR_GPU(" + std::to_string(_kf.getInnerParallelisms().first) + "," + std::to_string(_kf.getInnerParallelisms().second) + "), " + std::to_string(_kf.getNumComplexReplicas()) + "]", _kf.getName(), false, true, routing_modes_t::KEYBY);
#endif
                }
                forceShuffling = true;
            }
        }
        // case with Win_Seq_GPU replicas inside
        else {
            // call the generic method to add the operator to the MultiPipe
            if (_kf.getWinType() == win_type_t::TB) {
                if (mode == Mode::DETERMINISTIC) {
                    add_operator<KF_Emitter<tuple_t>, Ordering_Node<tuple_t>>(&_kf, routing_modes_t::COMPLEX, ordering_mode_t::TS);
                }
                else if (mode == Mode::PROBABILISTIC) {
                    add_operator<KF_Emitter<tuple_t>, KSlack_Node<tuple_t>>(&_kf, routing_modes_t::COMPLEX, ordering_mode_t::TS);
                } 
                else {
                    add_operator<KF_Emitter<tuple_t>>(&_kf, routing_modes_t::COMPLEX);
                }
            }
            else {
                if (mode == Mode::DETERMINISTIC) {
                    add_operator<KF_Emitter<tuple_t>, Ordering_Node<tuple_t>>(&_kf, routing_modes_t::COMPLEX, ordering_mode_t::TS_RENUMBERING);
                }
                else if (mode == Mode::PROBABILISTIC) {
                    add_operator<KF_Emitter<tuple_t>, KSlack_Node<tuple_t>>(&_kf, routing_modes_t::COMPLEX, ordering_mode_t::TS_RENUMBERING);
                }
                else {
                    add_operator<KF_Emitter<tuple_t>>(&_kf, routing_modes_t::COMPLEX);
                }
            }
#if defined (TRACE_WINDFLOW)
            // update the graphviz representation
            gv_add_vertex("KF_GPU (" + std::to_string(_kf.getParallelism()) + ")", _kf.getName(), false, false, routing_modes_t::KEYBY);
#endif
        }
        // save the new output type from this MultiPipe
        result_t r;
        outputType = typeid(r).name();
        // the Key_Farm_GPU operator is now used
        _kf.used = true;
        // add this operator to listOperators
        listOperators->push_back(std::ref(static_cast<Basic_Operator &>(_kf)));
        return *this;
    }

    /** 
     *  \brief Add a Key_FFAT to the MultiPipe
     *  \param _kff Key_FFAT operator to be added
     *  \return the modified MultiPipe
     */ 
    template<typename tuple_t, typename result_t>
    MultiPipe &add(Key_FFAT<tuple_t, result_t> &_kff)
    {
        // check whether the operator has already been used in a MultiPipe
        if (_kff.isUsed()) {
            std::cerr << RED << "WindFlow Error: Key_FFAT operator has already been used in a MultiPipe" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // prepare the operator for count-based windows
        if (_kff.getWinType() == win_type_t::CB) {
            // set the isRenumbering mode of the input operator
            _kff.set_isRenumbering();
        }
        // check the type compatibility
        tuple_t t;
        std::string opInType = typeid(t).name();
        if (!outputType.empty() && outputType.compare(opInType) != 0) {
            std::cerr << RED << "WindFlow Error: output type from MultiPipe is not the input type of the Key_FFAT operator" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // call the generic method to add the operator to the MultiPipe
        if (_kff.getWinType() == win_type_t::TB) {
            if (mode == Mode::DETERMINISTIC) {
                add_operator<KF_Emitter<tuple_t>, Ordering_Node<tuple_t>>(&_kff, routing_modes_t::COMPLEX, ordering_mode_t::TS);
            }
            else if (mode == Mode::PROBABILISTIC) {
                add_operator<KF_Emitter<tuple_t>, KSlack_Node<tuple_t>>(&_kff, routing_modes_t::COMPLEX, ordering_mode_t::TS);
            }
            else {
                add_operator<KF_Emitter<tuple_t>>(&_kff, routing_modes_t::COMPLEX);
            }
        }
        else {
            if (mode == Mode::DETERMINISTIC) {
                add_operator<KF_Emitter<tuple_t>, Ordering_Node<tuple_t>>(&_kff, routing_modes_t::COMPLEX, ordering_mode_t::TS_RENUMBERING);
            }
            else if (mode == Mode::PROBABILISTIC) {
                add_operator<KF_Emitter<tuple_t>, KSlack_Node<tuple_t>>(&_kff, routing_modes_t::COMPLEX, ordering_mode_t::TS_RENUMBERING);
            }
            else {
                add_operator<KF_Emitter<tuple_t>>(&_kff, routing_modes_t::COMPLEX);
            }
        }
        // save the new output type from this MultiPipe
        result_t r;
        outputType = typeid(r).name();
        // the Key_FFAT operator is now used
        _kff.used = true;
        // add this operator to listOperators
        listOperators->push_back(std::ref(static_cast<Basic_Operator &>(_kff)));
#if defined (TRACE_WINDFLOW)
        // update the graphviz representation
        gv_add_vertex("KFF (" + std::to_string(_kff.getParallelism()) + ")", _kff.getName(), true, false, routing_modes_t::KEYBY);
#endif
        return *this;
    }

    /** 
     *  \brief Add a Key_FFAT_GPU to the MultiPipe
     *  \param _kff Key_FFAT_GPU operator to be added
     *  \return the modified MultiPipe
     */ 
    template<typename tuple_t, typename result_t, typename F_t>
    MultiPipe &add(Key_FFAT_GPU<tuple_t, result_t, F_t> &_kff)
    {
        // check whether the operator has already been used in a MultiPipe
        if (_kff.isUsed()) {
            std::cerr << RED << "WindFlow Error: Key_FFAT_GPU operator has already been used in a MultiPipe" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // prepare the operator for count-based windows
        if (_kff.getWinType() == win_type_t::CB) {
            // set the isRenumbering mode of the input operator
            _kff.set_isRenumbering();
        }
        // check the type compatibility
        tuple_t t;
        std::string opInType = typeid(t).name();
        if (!outputType.empty() && outputType.compare(opInType) != 0) {
            std::cerr << RED << "WindFlow Error: output type from MultiPipe is not the input type of the Key_FFAT_GPU operator" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // call the generic method to add the operator to the MultiPipe
        if (_kff.getWinType() == win_type_t::TB) {
            if (mode == Mode::DETERMINISTIC) {
                add_operator<KF_Emitter<tuple_t>, Ordering_Node<tuple_t>>(&_kff, routing_modes_t::COMPLEX, ordering_mode_t::TS);
            }
            else if (mode == Mode::PROBABILISTIC) {
                add_operator<KF_Emitter<tuple_t>, KSlack_Node<tuple_t>>(&_kff, routing_modes_t::COMPLEX, ordering_mode_t::TS);
            }
            else {
                add_operator<KF_Emitter<tuple_t>>(&_kff, routing_modes_t::COMPLEX);
            }
        }
        else {
            if (mode == Mode::DETERMINISTIC) {
                add_operator<KF_Emitter<tuple_t>, Ordering_Node<tuple_t>>(&_kff, routing_modes_t::COMPLEX, ordering_mode_t::TS_RENUMBERING);
            }
            else if (mode == Mode::PROBABILISTIC) {
                add_operator<KF_Emitter<tuple_t>, KSlack_Node<tuple_t>>(&_kff, routing_modes_t::COMPLEX, ordering_mode_t::TS_RENUMBERING);
            }
            else {
                add_operator<KF_Emitter<tuple_t>>(&_kff, routing_modes_t::COMPLEX);
            } 
        }
        // save the new output type from this MultiPipe
        result_t r;
        outputType = typeid(r).name();
        // the Key_FFAT_GPU operator is now used
        _kff.used = true;
        // add this operator to listOperators
        listOperators->push_back(std::ref(static_cast<Basic_Operator &>(_kff)));
#if defined (TRACE_WINDFLOW)
        // update the graphviz representation
        gv_add_vertex("KFF_GPU (" + std::to_string(_kff.getParallelism()) + ")", _kff.getName(), false, false, routing_modes_t::KEYBY);
#endif
        return *this;
    }

    /** 
     *  \brief Add a Pane_Farm to the MultiPipe
     *  \param _pf Pane_Farm operator to be added
     *  \return the modified MultiPipe
     */ 
    template<typename tuple_t, typename result_t>
    MultiPipe &add(Pane_Farm<tuple_t, result_t> &_pf)
    {
        // check whether the operator has already been used in a MultiPipe
        if (_pf.isUsed()) {
            std::cerr << RED << "WindFlow Error: Pane_Farm operator has already been used in a MultiPipe" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // count-based windows are not possible in DEFAULT mode
        if (_pf.getWinType() == win_type_t::CB && mode == Mode::DEFAULT) {
            std::cerr << RED << "WindFlow Error: Pane_Farm cannot be used with count-based windows in DEFAULT mode" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // check the type compatibility
        tuple_t t;
        std::string opInType = typeid(t).name();
        if (!outputType.empty() && outputType.compare(opInType) != 0) {
            std::cerr << RED << "WindFlow Error: output type from MultiPipe is not the input type of the Pane_Farm operator" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // check whether the Pane_Farm has been prepared to be nested
        if (_pf.getOptLevel() != opt_level_t::LEVEL0) {
            std::cerr << RED << "WindFlow Error: Pane_Farm has been prepared for nesting, it cannot be added directly to a MultiPipe" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        ff::ff_pipeline *pipe = static_cast<ff::ff_pipeline *>(&_pf);
        const ff::svector<ff::ff_node *> &stages = pipe->getStages();
        ff::ff_farm *plq = nullptr;
        // check if the PLQ stage is a farm, otherwise prepare it
        if (!stages[0]->isFarm()) {
            plq = new ff::ff_farm();
            std::vector<ff::ff_node *> w;
            w.push_back(stages[0]); // there is for sure one single worker in the PLQ
            plq->add_emitter(new Standard_Emitter<tuple_t>(1));
            plq->add_workers(w);
            plq->add_collector(nullptr);
            plq->cleanup_emitter(true);
            plq->cleanup_workers(false);
            // call the generic method to add the operator (PLQ stage) to the MultiPipe
            if (_pf.getWinType() == win_type_t::TB) {
                if (mode == Mode::DETERMINISTIC) {
                    add_operator<Standard_Emitter<tuple_t>, Ordering_Node<tuple_t>>(plq, routing_modes_t::COMPLEX, ordering_mode_t::TS);
                }
                else if (mode == Mode::PROBABILISTIC) {
                    add_operator<Standard_Emitter<tuple_t>, KSlack_Node<tuple_t>>(plq, routing_modes_t::COMPLEX, ordering_mode_t::TS);
                }
                else {
                    add_operator<Standard_Emitter<tuple_t>>(plq, routing_modes_t::COMPLEX);
                }
            }
            else {
                if (mode == Mode::DETERMINISTIC) {
                    add_operator<Standard_Emitter<tuple_t>, Ordering_Node<tuple_t>>(plq, routing_modes_t::COMPLEX, ordering_mode_t::TS_RENUMBERING);
                }
                else if (mode == Mode::PROBABILISTIC) {
                    add_operator<Standard_Emitter<tuple_t>, KSlack_Node<tuple_t>>(plq, routing_modes_t::COMPLEX, ordering_mode_t::TS_RENUMBERING);
                }
                else {
                    abort(); // not supported CB windows in this case
                } 
            }
            delete plq;
        }
        else {
            plq = static_cast<ff::ff_farm *>(stages[0]);
            // check the type of the windows
            if (_pf.getWinType() == win_type_t::TB) { // time-based windows
                // call the generic method to add the operator (PLQ stage) to the MultiPipe
                if (mode == Mode::DETERMINISTIC) {
                    add_operator<WF_Emitter<tuple_t>, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(plq, routing_modes_t::COMPLEX, ordering_mode_t::TS);
                }
                else if (mode == Mode::PROBABILISTIC) {
                    add_operator<WF_Emitter<tuple_t>, KSlack_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(plq, routing_modes_t::COMPLEX, ordering_mode_t::TS);
                }
                else {
                    add_operator<WF_Emitter<tuple_t>>(plq, routing_modes_t::COMPLEX);
                }
            }
            else {
                // special case count-based windows
                size_t n_plq = (plq->getWorkers()).size();
                plq->change_emitter(new Broadcast_Emitter<tuple_t>(n_plq), true);
                // call the generic method to add the operator
                if (mode == Mode::DETERMINISTIC) {
                    add_operator<Broadcast_Emitter<tuple_t>, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(plq, routing_modes_t::COMPLEX, ordering_mode_t::TS_RENUMBERING);
                }
                else if (mode == Mode::PROBABILISTIC) {
                    add_operator<Broadcast_Emitter<tuple_t>, KSlack_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(plq, routing_modes_t::COMPLEX, ordering_mode_t::TS_RENUMBERING);
                }
                else {
                    abort(); // not supported CB windows in this case
                }
            }
        }
        ff::ff_farm *wlq = nullptr;
        // check if the WLQ stage is a farm, otherwise prepare it
        if (!stages[1]->isFarm()) {
            wlq = new ff::ff_farm();
            std::vector<ff::ff_node *> w;
            w.push_back(stages[1]); // there is for sure one single worker in the WLQ
            wlq->add_emitter(new Standard_Emitter<result_t>(1));
            wlq->add_workers(w);
            wlq->add_collector(nullptr);
            wlq->cleanup_emitter(true);
            wlq->cleanup_workers(false);
            // call the generic method to add the operator (WLQ stage) to the MultiPipe
            add_operator<Standard_Emitter<result_t>, Ordering_Node<result_t>>(wlq, routing_modes_t::COMPLEX, ordering_mode_t::ID);
            delete wlq;
        }
        else {
            wlq = static_cast<ff::ff_farm *>(stages[1]);
            // call the generic method to add the operator (WLQ stage) to the MultiPipe
            add_operator<WF_Emitter<result_t>, Ordering_Node<result_t, wrapper_tuple_t<result_t>>>(wlq, routing_modes_t::COMPLEX, ordering_mode_t::ID);
        }
        // save the new output type from this MultiPipe
        result_t r;
        outputType = typeid(r).name();
        // the Pane_Farm operator is now used
        _pf.used = true;
        // add this operator to listOperators
        listOperators->push_back(std::ref(static_cast<Basic_Operator &>(_pf)));
#if defined (TRACE_WINDFLOW)
        // update the graphviz representation
        gv_add_vertex("PF (" + std::to_string(_pf.getPLQParallelism()) + "," + std::to_string(_pf.getWLQParallelism()) + ")", _pf.getName(), true, false, routing_modes_t::COMPLEX);
#endif
        return *this;
    }

    /** 
     *  \brief Add a Pane_Farm_GPU to the MultiPipe
     *  \param _pf Pane_Farm_GPU operator to be added
     *  \return the modified MultiPipe
     */ 
    template<typename tuple_t, typename result_t, typename F_t>
    MultiPipe &add(Pane_Farm_GPU<tuple_t, result_t, F_t> &_pf)
    {
        // check whether the operator has already been used in a MultiPipe
        if (_pf.isUsed()) {
            std::cerr << RED << "WindFlow Error: Pane_Farm_GPU operator has already been used in a MultiPipe" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // count-based windows are not possible in DEFAULT mode
        if (_pf.getWinType() == win_type_t::CB && mode == Mode::DEFAULT) {
            std::cerr << RED << "WindFlow Error: Pane_Farm_GPU cannot use count-based windows in DEFAULT mode" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // check the type compatibility
        tuple_t t;
        std::string opInType = typeid(t).name();
        if (!outputType.empty() && outputType.compare(opInType) != 0) {
            std::cerr << RED << "WindFlow Error: output type from MultiPipe is not the input type of the Pane_Farm_GPU operator" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // check whether the Pane_Farm_GPU has been prepared to be nested
        if (_pf.getOptLevel() != opt_level_t::LEVEL0) {
            std::cerr << RED << "WindFlow Error: Pane_Farm_GPU has been prepared for nesting, it cannot be added directly to a MultiPipe" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        ff::ff_pipeline *pipe = static_cast<ff::ff_pipeline *>(&_pf);
        const ff::svector<ff::ff_node *> &stages = pipe->getStages();
        ff::ff_farm *plq = nullptr;
        // check if the PLQ stage is a farm, otherwise prepare it
        if (!stages[0]->isFarm()) {
            plq = new ff::ff_farm();
            std::vector<ff::ff_node *> w;
            w.push_back(stages[0]); // there is for sure one single worker in the PLQ
            plq->add_emitter(new Standard_Emitter<tuple_t>(1));
            plq->add_workers(w);
            plq->add_collector(nullptr);
            plq->cleanup_emitter(true);
            plq->cleanup_workers(false);
            // call the generic method to add the operator (PLQ stage) to the MultiPipe
            if (_pf.getWinType() == win_type_t::TB) {
                if (mode == Mode::DETERMINISTIC) {
                    add_operator<Standard_Emitter<tuple_t>, Ordering_Node<tuple_t>>(plq, routing_modes_t::COMPLEX, ordering_mode_t::TS);
                }
                else if (mode == Mode::PROBABILISTIC) {
                    add_operator<Standard_Emitter<tuple_t>, KSlack_Node<tuple_t>>(plq, routing_modes_t::COMPLEX, ordering_mode_t::TS);
                }
                else {
                    add_operator<Standard_Emitter<tuple_t>>(plq, routing_modes_t::COMPLEX);
                }
            }
            else {
                if (mode == Mode::DETERMINISTIC) {
                    add_operator<Standard_Emitter<tuple_t>, Ordering_Node<tuple_t>>(plq, routing_modes_t::COMPLEX, ordering_mode_t::TS_RENUMBERING);
                }
                else if (mode == Mode::PROBABILISTIC) {
                    add_operator<Standard_Emitter<tuple_t>, KSlack_Node<tuple_t>>(plq, routing_modes_t::COMPLEX, ordering_mode_t::TS_RENUMBERING);
                }
                else {
                    abort(); // not supported CB windows in this case
                }
            }
            delete plq;
        }
        else {
            plq = static_cast<ff::ff_farm *>(stages[0]);
            // check the type of the windows
            if (_pf.getWinType() == win_type_t::TB) { // time-based windows
                // call the generic method to add the operator (PLQ stage) to the MultiPipe
                if (mode == Mode::DETERMINISTIC) {
                    add_operator<WF_Emitter<tuple_t>, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(plq, routing_modes_t::COMPLEX, ordering_mode_t::TS);
                }
                else if (mode == Mode::PROBABILISTIC) {
                    add_operator<WF_Emitter<tuple_t>, KSlack_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(plq, routing_modes_t::COMPLEX, ordering_mode_t::TS);
                }
                else {
                    add_operator<WF_Emitter<tuple_t>>(plq, routing_modes_t::COMPLEX);
                }
            }
            else {
                // special case count-based windows
                size_t n_plq = (plq->getWorkers()).size();
                plq->change_emitter(new Broadcast_Emitter<tuple_t>(n_plq), true);
                // call the generic method to add the operator
                if (mode == Mode::DETERMINISTIC) {
                    add_operator<Broadcast_Emitter<tuple_t>, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(plq, routing_modes_t::COMPLEX, ordering_mode_t::TS_RENUMBERING);
                }
                else if (mode == Mode::PROBABILISTIC) {
                    add_operator<Broadcast_Emitter<tuple_t>, KSlack_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(plq, routing_modes_t::COMPLEX, ordering_mode_t::TS_RENUMBERING);
                }
                else {
                    abort(); // not supported CB windows in this case
                }
            }
        }
        ff::ff_farm *wlq = nullptr;
        // check if the WLQ stage is a farm, otherwise prepare it
        if (!stages[1]->isFarm()) {
            wlq = new ff::ff_farm();
            std::vector<ff::ff_node *> w;
            w.push_back(stages[1]); // there is for sure one single worker in the WLQ
            wlq->add_emitter(new Standard_Emitter<result_t>(1));
            wlq->add_workers(w);
            wlq->add_collector(nullptr);
            wlq->cleanup_emitter(true);
            wlq->cleanup_workers(false);
            // call the generic method to add the operator (WLQ stage) to the MultiPipe
            add_operator<Standard_Emitter<result_t>, Ordering_Node<result_t>>(wlq, routing_modes_t::COMPLEX, ordering_mode_t::ID);
            delete wlq;
        }
        else {
            wlq = static_cast<ff::ff_farm *>(stages[1]);
            // call the generic method to add the operator (WLQ stage) to the MultiPipe
            add_operator<WF_Emitter<result_t>, Ordering_Node<result_t, wrapper_tuple_t<result_t>>>(wlq, routing_modes_t::COMPLEX, ordering_mode_t::ID);
        }
        // save the new output type from this MultiPipe
        result_t r;
        outputType = typeid(r).name();
        // the Pane_Farm operator is now used
        _pf.used = true;
        // add this operator to listOperators
        listOperators->push_back(std::ref(static_cast<Basic_Operator &>(_pf)));
#if defined (TRACE_WINDFLOW)
        // update the graphviz representation
        gv_add_vertex("PF_GPU (" + std::to_string(_pf.getPLQParallelism()) + "," + std::to_string(_pf.getWLQParallelism()) + ")", _pf.getName(), false, false, routing_modes_t::COMPLEX);
#endif
        return *this;
    }

    /** 
     *  \brief Add a Win_MapReduce to the MultiPipe
     *  \param _wmr Win_MapReduce operator to be added
     *  \return the modified MultiPipe
     */ 
    template<typename tuple_t, typename result_t>
    MultiPipe &add(Win_MapReduce<tuple_t, result_t> &_wmr)
    {
        // check whether the operator has already been used in a MultiPipe
        if (_wmr.isUsed()) {
            std::cerr << RED << "WindFlow Error: Win_MapReduce operator has already been used in a MultiPipe" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // count-based windows are not possible in DEFAULT mode
        if (_wmr.getWinType() == win_type_t::CB && mode == Mode::DEFAULT) {
            std::cerr << RED << "WindFlow Error: Win_MapReduce cannot use count-based windows in DEFAULT mode" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // check the type compatibility
        tuple_t t;
        std::string opInType = typeid(t).name();
        if (!outputType.empty() && outputType.compare(opInType) != 0) {
            std::cerr << RED << "WindFlow Error: output type from MultiPipe is not the input type of the Win_MapReduce operator" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // check whether the Win_MapReduce has been prepared to be nested
        if (_wmr.getOptLevel() != opt_level_t::LEVEL0) {
            std::cerr << RED << "WindFlow Error: Win_MapReduce has been prepared for nesting, it cannot be added directly to a MultiPipe" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // add the MAP stage
        ff::ff_pipeline *pipe = static_cast<ff::ff_pipeline *>(&_wmr);
        const ff::svector<ff::ff_node *> &stages = pipe->getStages();
        ff::ff_farm *map = static_cast<ff::ff_farm *>(stages[0]);
        // check the type of the windows
        if (_wmr.getWinType() == win_type_t::TB) { // time-based windows
            // call the generic method to add the operator (MAP stage) to the MultiPipe
            if (mode == Mode::DETERMINISTIC) {
                add_operator<WinMap_Emitter<tuple_t>, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(map, routing_modes_t::COMPLEX, ordering_mode_t::TS);
            }
            else if (mode == Mode::PROBABILISTIC) {
                add_operator<WinMap_Emitter<tuple_t>, KSlack_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(map, routing_modes_t::COMPLEX, ordering_mode_t::TS);
            }
            else {
                add_operator<WinMap_Emitter<tuple_t>>(map, routing_modes_t::COMPLEX);
            }
        }
        else {
            // special case count-based windows
            size_t n_map = (map->getWorkers()).size();
            ff::ff_farm *new_map = new ff::ff_farm();
            auto worker_set = map->getWorkers();
            std::vector<ff::ff_node *> w;
            new_map->add_emitter(new Broadcast_Emitter<tuple_t>(n_map));
            for (size_t i=0; i<n_map; i++) {
                ff::ff_comb *comb = new ff::ff_comb(new WinMap_Dropper<tuple_t>(i, n_map), worker_set[i], true, false);
                w.push_back(comb);
            }
            new_map->add_workers(w);
            new_map->add_collector(nullptr);
            new_map->cleanup_emitter(true);
            new_map->cleanup_workers(false);
            // call the generic method to add the operator (MAP stage) to the MultiPipe
            if (mode == Mode::DETERMINISTIC) {
                add_operator<Broadcast_Emitter<tuple_t>, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(new_map, routing_modes_t::COMPLEX, ordering_mode_t::TS_RENUMBERING);
            }
            else if (mode == Mode::PROBABILISTIC) {
                add_operator<Broadcast_Emitter<tuple_t>, KSlack_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(new_map, routing_modes_t::COMPLEX, ordering_mode_t::TS_RENUMBERING);
            }
            else {
                abort(); // not supported CB windows in this case
            }
            delete new_map;
        }
        // add the REDUCE stage
        ff::ff_farm *reduce = nullptr;
        // check if the REDUCE stage is a farm, otherwise prepare it
        if (!stages[1]->isFarm()) {
            reduce = new ff::ff_farm();
            std::vector<ff::ff_node *> w;
            w.push_back(stages[1]);
            reduce->add_emitter(new Standard_Emitter<result_t>(1));
            reduce->add_workers(w);
            reduce->add_collector(nullptr);
            reduce->cleanup_emitter(true);
            reduce->cleanup_workers(false);
            // call the generic method to add the operator (REDUCE stage) to the MultiPipe
            add_operator<Standard_Emitter<result_t>, Ordering_Node<result_t>>(reduce, routing_modes_t::COMPLEX, ordering_mode_t::ID);
            delete reduce;
        }
        else {
            reduce = static_cast<ff::ff_farm *>(stages[1]);
            // call the generic method to add the operator (REDUCE stage) to the MultiPipe
            add_operator<WF_Emitter<result_t>, Ordering_Node<result_t, wrapper_tuple_t<result_t>>>(reduce, routing_modes_t::COMPLEX, ordering_mode_t::ID);
        }
        // save the new output type from this MultiPipe
        result_t r;
        outputType = typeid(r).name();
        // the Win_MapReduce operator is now used
        _wmr.used = true;
        // add this operator to listOperators
        listOperators->push_back(std::ref(static_cast<Basic_Operator &>(_wmr)));
#if defined (TRACE_WINDFLOW)
        // update the graphviz representation
        gv_add_vertex("WMR (" + std::to_string(_wmr.getMAPParallelism()) + "," + std::to_string(_wmr.getREDUCEParallelism()) + ")", _wmr.getName(), true, false, routing_modes_t::COMPLEX);
#endif
        return *this;
    }

    /** 
     *  \brief Add a Win_MapReduce_GPU to the MultiPipe
     *  \param _wmr Win_MapReduce_GPU operator to be added
     *  \return the modified MultiPipe
     */ 
    template<typename tuple_t, typename result_t, typename F_t>
    MultiPipe &add(Win_MapReduce_GPU<tuple_t, result_t, F_t> &_wmr)
    {
        // check whether the operator has already been used in a MultiPipe
        if (_wmr.isUsed()) {
            std::cerr << RED << "WindFlow Error: Win_MapReduce_GPU operator has already been used in a MultiPipe" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // count-based windows are not possible in DEFAULT mode
        if (_wmr.getWinType() == win_type_t::CB && mode == Mode::DEFAULT) {
            std::cerr << RED << "WindFlow Error: Win_MapReduce_GPU cannot use count-based windows in DEFAULT mode" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // check the type compatibility
        tuple_t t;
        std::string opInType = typeid(t).name();
        if (!outputType.empty() && outputType.compare(opInType) != 0) {
            std::cerr << RED << "WindFlow Error: output type from MultiPipe is not the input type of the Win_MapReduce_GPU operator" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // check whether the Win_MapReduce_GPU has been prepared to be nested
        if (_wmr.getOptLevel() != opt_level_t::LEVEL0) {
            std::cerr << RED << "WindFlow Error: Win_MapReduce_GPU has been prepared for nesting, it cannot be added directly to a MultiPipe" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // add the MAP stage
        ff::ff_pipeline *pipe = static_cast<ff::ff_pipeline *>(&_wmr);
        const ff::svector<ff::ff_node *> &stages = pipe->getStages();
        ff::ff_farm *map = static_cast<ff::ff_farm *>(stages[0]);
        // check the type of the windows
        if (_wmr.getWinType() == win_type_t::TB) { // time-based windows
            // call the generic method to add the operator (MAP stage) to the MultiPipe
            if (mode == Mode::DETERMINISTIC) {
                add_operator<WinMap_Emitter<tuple_t>, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(map, routing_modes_t::COMPLEX, ordering_mode_t::TS);
            }
            else if (mode == Mode::PROBABILISTIC) {
                add_operator<WinMap_Emitter<tuple_t>, KSlack_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(map, routing_modes_t::COMPLEX, ordering_mode_t::TS);
            }
            else {
                add_operator<WinMap_Emitter<tuple_t>>(map, routing_modes_t::COMPLEX);
            }
        }
        else {
            // special case count-based windows
            size_t n_map = (map->getWorkers()).size();
            ff::ff_farm *new_map = new ff::ff_farm();
            auto worker_set = map->getWorkers();
            std::vector<ff::ff_node *> w;
            new_map->add_emitter(new Broadcast_Emitter<tuple_t>(n_map));
            for (size_t i=0; i<n_map; i++) {
                ff::ff_comb *comb = new ff::ff_comb(new WinMap_Dropper<tuple_t>(i, n_map), worker_set[i], true, false);
                w.push_back(comb);
            }
            new_map->add_workers(w);
            new_map->add_collector(nullptr);
            new_map->cleanup_emitter(true);
            new_map->cleanup_workers(false);
            // call the generic method to add the operator (MAP stage) to the MultiPipe
            if (mode == Mode::DETERMINISTIC) {
                add_operator<Broadcast_Emitter<tuple_t>, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(new_map, routing_modes_t::COMPLEX, ordering_mode_t::TS_RENUMBERING);
            }
            else if (mode == Mode::PROBABILISTIC) {
                add_operator<Broadcast_Emitter<tuple_t>, KSlack_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(new_map, routing_modes_t::COMPLEX, ordering_mode_t::TS_RENUMBERING);
            }
            else {
                abort(); // not supported CB windows in this case
            }
            delete new_map;
        }
        // add the REDUCE stage
        ff::ff_farm *reduce = nullptr;
        // check if the REDUCE stage is a farm, otherwise prepare it
        if (!stages[1]->isFarm()) {
            reduce = new ff::ff_farm();
            std::vector<ff::ff_node *> w;
            w.push_back(stages[1]);
            reduce->add_emitter(new Standard_Emitter<result_t>(1));
            reduce->add_workers(w);
            reduce->add_collector(nullptr);
            reduce->cleanup_emitter(true);
            reduce->cleanup_workers(false);
            // call the generic method to add the operator (REDUCE stage) to the MultiPipe
            add_operator<Standard_Emitter<result_t>, Ordering_Node<result_t>>(reduce, routing_modes_t::COMPLEX, ordering_mode_t::ID);
            delete reduce;
        }
        else {
            reduce = static_cast<ff::ff_farm *>(stages[1]);
            // call the generic method to add the operator (REDUCE stage) to the MultiPipe
            add_operator<WF_Emitter<result_t>, Ordering_Node<result_t, wrapper_tuple_t<result_t>>>(reduce, routing_modes_t::COMPLEX, ordering_mode_t::ID);
        }
        // save the new output type from this MultiPipe
        result_t r;
        outputType = typeid(r).name();
        // the Win_MapReduce_GPU operator is now used
        _wmr.used = true;
        // add this operator to listOperators
        listOperators->push_back(std::ref(static_cast<Basic_Operator &>(_wmr)));
#if defined (TRACE_WINDFLOW)
        // update the graphviz representation
        gv_add_vertex("WMR_GPU (" + std::to_string(_wmr.getMAPParallelism()) + "," + std::to_string(_wmr.getREDUCEParallelism()) + ")", _wmr.getName(), false, false, routing_modes_t::COMPLEX);
#endif
        return *this;
    }

    /** 
     *  \brief Add a Sink to the MultiPipe
     *  \param _sink Sink operator to be added
     *  \return the modified MultiPipe
     */ 
    template<typename tuple_t>
    MultiPipe &add_sink(Sink<tuple_t> &_sink)
    {
        // check whether the operator has already been used in a MultiPipe
        if (_sink.isUsed()) {
            std::cerr << RED << "WindFlow Error: Sink operator has already been used in a MultiPipe" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // check the type compatibility
        tuple_t t;
        std::string opInType = typeid(t).name();
        if (!outputType.empty() && outputType.compare(opInType) != 0) {
            std::cerr << RED << "WindFlow Error: output type from MultiPipe is not the input type of the Sink operator" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // call the generic method to add the operator to the MultiPipe
        if (mode == Mode::DETERMINISTIC) {
            add_operator<Standard_Emitter<tuple_t>, Ordering_Node<tuple_t>>(&_sink, _sink.getRoutingMode(), ordering_mode_t::TS);
        }
        else if (mode == Mode::PROBABILISTIC) {
            add_operator<Standard_Emitter<tuple_t>, KSlack_Node<tuple_t>>(&_sink, _sink.getRoutingMode(), ordering_mode_t::TS);
        }
        else {
            add_operator<Standard_Emitter<tuple_t>>(&_sink, _sink.getRoutingMode());
        }
        has_sink = true;
        // save the new output type from this MultiPipe
        outputType = opInType;
        // the Sink operator is now used
        _sink.used = true;
        // add this operator to listOperators
        listOperators->push_back(std::ref(static_cast<Basic_Operator &>(_sink)));
#if defined (TRACE_WINDFLOW)
        // update the graphviz representation
        gv_add_vertex("Sink (" + std::to_string(_sink.getParallelism()) + ")", _sink.getName(), true, false, _sink.getRoutingMode());
#endif
        return *this;
    }

    /** 
     *  \brief Chain a Sink to the MultiPipe (if possible, otherwise add it)
     *  \param _sink Sink operator to be chained
     *  \return the modified MultiPipe
     */ 
    template<typename tuple_t>
    MultiPipe &chain_sink(Sink<tuple_t> &_sink)
    {
        // check whether the operator has already been used in a MultiPipe
        if (_sink.isUsed()) {
            std::cerr << RED << "WindFlow Error: Sink operator has already been used in a MultiPipe" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // check the type compatibility
        tuple_t t;
        std::string opInType = typeid(t).name();
        if (!outputType.empty() &&outputType.compare(opInType) != 0) {
            std::cerr << RED << "WindFlow Error: output type from MultiPipe is not the input type of the Sink operator" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // try to chain the Sink with the MultiPipe
        if (_sink.getRoutingMode() != routing_modes_t::KEYBY) {
            bool chained = chain_operator<typename Sink<tuple_t>::Sink_Node>(&_sink);
            if (!chained) {
                return add_sink(_sink);
            }
            else {
                // add this operator to listOperators
                listOperators->push_back(std::ref(static_cast<Basic_Operator &>(_sink)));
#if defined (TRACE_WINDFLOW)
                // update the graphviz representation
                gv_chain_vertex("Sink (" + std::to_string(_sink.getParallelism()) + ")", _sink.getName());
#endif
            }
        }
        else {
            return add_sink(_sink);
        }
        has_sink = true;
        // save the new output type from this MultiPipe
        outputType = opInType;
        // the Sink operator is now used
        _sink.used = true;
        return *this;
    }

    /** 
     *  \brief Merge of this with a set of MultiPipe instances _pipes
     *  \param _pipes set of MultiPipe instances to be merged with this
     *  \return a reference to the new MultiPipe (the result fo the merge)
     */ 
    template<typename ...MULTIPIPES>
    MultiPipe &merge(MULTIPIPES&... _pipes)
    {
        auto mergeSet = prepareMergeSet(*this, _pipes...);
        // at least two MultiPipe instances can be merged
        if (mergeSet.size() < 2) {
            std::cerr << RED << "WindFlow Error: merge must be applied to at least two MultiPipe instances" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // check duplicates
        std::map<MultiPipe *, int> counters;
        for (auto *mp: mergeSet) {
            if (counters.find(mp) == counters.end())
                counters.insert(std::make_pair(mp, 1));
            else {
                std::cerr << RED << "WindFlow Error: a MultiPipe cannot be merged with itself" << DEFAULT_COLOR << std::endl;
                exit(EXIT_FAILURE);
            }
        }
        // check that all the MultiPipe instances to be merged have the same output type
        std::string outT = mergeSet[0]->outputType;
        for (auto *mp: mergeSet) {
            if (outT.compare(mp->outputType) != 0) {
                std::cerr << RED << "WindFlow Error: MultiPipe instances to be merged must have the same output type" << DEFAULT_COLOR << std::endl;
                exit(EXIT_FAILURE);
            }
        }
        // execute the merge through the PipeGraph
        MultiPipe *mergedMP = merge_multipipes_func(graph, mergeSet);
#if defined (TRACE_WINDFLOW)
        for (auto *mp: mergeSet) {
            (mergedMP->gv_last_vertices).insert((mergedMP->gv_last_vertices).end(), (mp->gv_last_vertices).begin(), (mp->gv_last_vertices).end());
        }
#endif
        return *mergedMP;
    }

    /** 
     *  \brief Split of this into a set of MultiPipe instances
     *  \param _splitting_func splitting logic
     *  \param _cardinality number of splitting MultiPipe instances to generate from this
     *  \return the MultiPipe this after the splitting
     */ 
    template<typename F_t>
    MultiPipe &split(F_t _splitting_func, size_t _cardinality)
    {
        // check whether the MultiPipe has been merged
        if (isMerged) {
            std::cerr << RED << "WindFlow Error: MultiPipe has been merged and cannot be split" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // check whether the MultiPipe has already been split
        if (isSplit) {
            std::cerr << RED << "WindFlow Error: MultiPipe has already been split" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // prepare the splitting of this
        splittingBranches = _cardinality;
        splittingEmitterRoot = new Splitting_Emitter<F_t>(_splitting_func, _cardinality);
        // extract the tuple type from the signature of the splitting logic
        using tuple_t = decltype(get_tuple_t_Split(_splitting_func));
        // check the type compatibility
        tuple_t t;
        std::string opInType = typeid(t).name();
        if (!outputType.empty() && outputType.compare(opInType) != 0) {
            std::cerr << RED << "WindFlow Error: output type from MultiPipe is not the input type of the splitting function" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // execute the merge through the PipeGraph
        splittingChildren = split_multipipe_func(graph, this);
#if defined (TRACE_WINDFLOW)
        for (auto *mp: this->splittingChildren) {
            mp->gv_last_vertices = this->gv_last_vertices;
        }
#endif
        return *this;
    }

    /** 
     *  \brief Select a MultiPipe upon the splitting of this
     *  \param _idx index of the MultiPipe to be selected
     *  \return reference to split MultiPipe with index _idx
     */ 
    MultiPipe &select(size_t _idx) const
    {
        // check whether the MultiPipe has been merged
        if (isMerged) {
            std::cerr << RED << "WindFlow Error: MultiPipe has been merged, select() cannot be executed" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        if (!isSplit) {
            std::cerr << RED << "WindFlow Error: MultiPipe has not been split, select() cannot be executed" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        if (_idx >= splittingChildren.size()) {
            std::cerr << RED << "WindFlow Error: index of select() is out of range" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        return *(splittingChildren[_idx]);
    }

    /// deleted constructors/operators
    MultiPipe(const MultiPipe &) = delete; // copy constructor
    MultiPipe(MultiPipe &&) = delete; // move constructor
    MultiPipe &operator=(const MultiPipe &) = delete; // copy assignment operator
    MultiPipe &operator=(MultiPipe &&) = delete; // move assignment operator
};

} // namespace wf

#endif
