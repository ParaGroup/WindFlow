/**************************************************************************************
 *  Copyright (c) 2019- Gabriele Mencagli
 *  
 *  This file is part of WindFlow.
 *  
 *  WindFlow is free software dual licensed under the GNU LGPL or MIT License.
 *  You can redistribute it and/or modify it under the terms of the
 *    * GNU Lesser General Public License as published by
 *      the Free Software Foundation, either version 3 of the License, or
 *      (at your option) any later version
 *    OR
 *    * MIT License: https://github.com/ParaGroup/WindFlow/blob/vers3.x/LICENSE.MIT
 *  
 *  WindFlow is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Lesser General Public License for more details.
 *  You should have received a copy of the GNU Lesser General Public License and
 *  the MIT License along with WindFlow. If not, see <http://www.gnu.org/licenses/>
 *  and <http://opensource.org/licenses/MIT/>.
 **************************************************************************************
 */

/** 
 *  @file    multipipe.hpp
 *  @author  Gabriele Mencagli
 *  
 *  @brief MultiPipe construct
 *  
 *  @section MultiPipe (Description)
 *  
 *  This file implements the MultiPipe construct. It is essentially a "container"
 *  of interconnected operators, where each operator can be internally replicated
 *  with one or more replicas.
 */ 

#ifndef MULTIPIPE_H
#define MULTIPIPE_H

/// includes
#include<map>
#include<ff/ff.hpp>
#if defined (WF_TRACING_ENABLED)
    #include<graphviz/gvc.h>
    #include<rapidjson/prettywriter.h>
#endif
#include<basic.hpp>
#include<basic_emitter.hpp>
#include<keyby_emitter.hpp>
#include<basic_operator.hpp>
#include<forward_emitter.hpp>
#include<broadcast_emitter.hpp>
#include<splitting_emitter.hpp>
#include<kslack_collector.hpp>
#include<ordering_collector.hpp>
#include<watermark_collector.hpp>
#include<join_collector.hpp>
#if defined (__CUDACC__)
    #include<basic_gpu.hpp>
    #include<broadcast_emitter_gpu.hpp>
    #include<forward_emitter_gpu.hpp>
    #include<keyby_emitter_gpu.hpp>
    #include<splitting_emitter_gpu.hpp>
#endif

namespace wf {

//@cond DOXY_IGNORE

// class selfkiller_node (placeholder in the second set of matrioskas)
struct selfkiller_node: ff::ff_minode
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
        return this->EOS; // terminates immediately
    }
};

//@endcond

/** 
 *  \class MultiPipe
 *  
 *  \brief MultiPipe construct
 *  
 *  This class implements the MultiPipe construct.
 */ 
class MultiPipe: public ff::ff_pipeline
{
private:
    friend class PipeGraph;
    PipeGraph *graph; // pointer to the PipeGraph
    Execution_Mode_t execution_mode; // execution mode of the PipeGraph
    Time_Policy_t time_policy; // time policy of the PipeGraph
    std::atomic<unsigned long> *atomic_num_dropped; // pointer to the atomic counter of dropped tuples
    std::vector<Basic_Operator *> localOpList; // vector of pointers to the operators added/chained to the MultiPipe
    std::vector<Basic_Operator *> *globalOpList; // pointer to a vector containing pointers to all the operators in the PipeGraph
    bool has_source; // true if the MultiPipe starts with a Source
    bool has_sink; // true if the MultiPipe ends with a Sink
    ff::ff_a2a *last; // pointer to the last matrioska
    ff::ff_a2a *secondToLast; // pointer to the second-to-last matrioska
    bool isMerged; // true if the MultiPipe has been merged with other MultiPipes
    bool fromMerging; // true if the MultiPipe originates from a merge of other MultiPipes
    std::vector<MultiPipe *> mergeParents; // vector of parent MultiPipes (meaningful if fromMerging is true)
    bool isSplit; // true if the MultiPipe has been split
    Basic_Emitter *splittingEmitter = nullptr; // pointer to the splitting emitter to be used (meaningful if isSplit is true)
    std::vector<MultiPipe *> splittingChildren; // vector of pointers to the children MultiPipes (meaningful if isSplit is true)
    size_t splittingBranches; // number of splitting branches (meaningful if isSplit is true)
    bool fromSplitting; // true if the MultiPipe originates from a splitting of a MultiPipe
    MultiPipe *splittingParent = nullptr; // pointer to the parent MultiPipe (meaningful if fromSplitting is true)
    size_t lastParallelism; // parallelism of the last operator added to the MultiPipe (0 if not defined)
    std::string outputType; // string representing the type of the outputs from this MultiPipe (the empty string "" means not defined yet)
#if defined (WF_TRACING_ENABLED)
    Agraph_t *gv_graph = nullptr; // pointer to the graphviz representation of the PipeGraph
    std::vector<std::string> gv_last_typeOPs; // list of the last chained operator types
    std::vector<std::string> gv_last_nameOPs; // list of the last chained operator names
    std::vector<Agnode_t *> gv_last_vertices; // list of the last graphviz vertices
#endif

    // Private Constructor I (to create an empty MultiPipe)
    MultiPipe(PipeGraph *_graph,
              Execution_Mode_t _execution_mode,
              Time_Policy_t _time_policy,
              std::atomic<unsigned long> *_atomic_num_dropped,
              std::vector<Basic_Operator *> *_globalOpList):
              graph(_graph),
              execution_mode(_execution_mode),
              time_policy(_time_policy),
              atomic_num_dropped(_atomic_num_dropped),
              globalOpList(_globalOpList),
              has_source(false),
              has_sink(false),
              last(nullptr),
              secondToLast(nullptr),
              isMerged(false),
              fromMerging(false),
              isSplit(false),
              splittingBranches(0),
              fromSplitting(false),
              lastParallelism(0),
              outputType("") {}

    // Private Constructor II (to create a MultiPipe resulting from a merge)
    MultiPipe(PipeGraph *_graph,
              std::vector<MultiPipe *> _mps,
              Execution_Mode_t _execution_mode,
              Time_Policy_t _time_policy,
              std::atomic<unsigned long> *_atomic_num_dropped,
              std::vector<Basic_Operator *> *_globalOpList):
              graph(_graph),
              execution_mode(_execution_mode),
              time_policy(_time_policy),
              atomic_num_dropped(_atomic_num_dropped),
              globalOpList(_globalOpList),
              has_source(true),
              has_sink(false),
              isMerged(false),
              fromMerging(true),
              mergeParents(_mps),
              isSplit(false),
              splittingBranches(0),
              fromSplitting(false),
              lastParallelism(0)
    {
        ff::ff_a2a *matrioska = new ff::ff_a2a(); // create the initial matrioska
        std::vector<ff::ff_node *> normalization; // normalize all the MultiPipes to be merged
        for (auto *mp: _mps) {
            auto v = mp->normalize();
            mp->isMerged = true;
            normalization.insert(normalization.end(), v.begin(), v.end());
        }
        matrioska->add_firstset(normalization, 0, false); // this will share the nodes of the MultiPipes to be merged
        std::vector<ff::ff_node *> second_set;
        ff::ff_pipeline *stage = new ff::ff_pipeline();
        stage->add_stage(new selfkiller_node(), true);
        second_set.push_back(stage);
        matrioska->add_secondset(second_set, true);
        this->add_stage(matrioska, true);
        last = matrioska;
        secondToLast = nullptr;
    }

    // Destructor
    ~MultiPipe()
    {
        for (auto *op: localOpList) { // delete all the operators in the Multipipe
            delete op;
        }
    }

    // Combine the right collector with the replicas of the next operator
    template<typename operator_t>
    std::vector<ff::ff_node *> combine_with_collector(operator_t &_operator,
                                                      ordering_mode_t _ordering_mode,
                                                      bool _needBatching) const
    {
        size_t id=0;
        size_t separator_id=0;
        std::vector<ff::ff_node *> result;
        if ((_operator.getType() == "Interval_Join_KP" || _operator.getType() == "Interval_Join_DP")) {
            auto lastOps = this->getLastOperators();
            separator_id = lastOps.front()->getParallelism();
        }
        for (auto *r: _operator.replicas) {
            ff::ff_pipeline *stage = new ff::ff_pipeline();
            stage->add_stage(r, false);
            // We use Join_Collector if condition for Interval_Join is with DEFAULT execution mode and DP join mode
            if (_operator.getType() == "Interval_Join_DP" && execution_mode == Execution_Mode_t::DEFAULT) {
                r->receiveBatches(_needBatching);
                auto *collector = new Join_Collector<decltype(_operator.getKeyExtractor())>(_operator.getKeyExtractor(), _ordering_mode, execution_mode, Join_Mode_t::DP, id++, _needBatching, separator_id);
                combine_with_firststage(*stage, collector, true); // combine with the Join_Collector
            }
            else if (_operator.getType() == "Parallel_Windows_WLQ" || _operator.getType() == "Parallel_Windows_REDUCE") { // special cases
                auto *collector = new Ordering_Collector<decltype(_operator.getKeyExtractor())>(_operator.getKeyExtractor(), ordering_mode_t::ID, execution_mode, id++);
                combine_with_firststage(*stage, collector, true); // combine with the Ordering_Collector
            }
            else if (execution_mode == Execution_Mode_t::DETERMINISTIC) {
                auto *collector = new Ordering_Collector<decltype(_operator.getKeyExtractor())>(_operator.getKeyExtractor(), _ordering_mode, execution_mode, id++, separator_id);
                combine_with_firststage(*stage, collector, true); // combine with the Ordering_Collector
            }
            else if (execution_mode == Execution_Mode_t::PROBABILISTIC) {
                auto *collector = new KSlack_Collector<decltype(_operator.getKeyExtractor())>(_operator.getKeyExtractor(), _ordering_mode, id++, separator_id);
                combine_with_firststage(*stage, collector, true); // combine with the Kslack_Collector
            }
            else if (execution_mode == Execution_Mode_t::DEFAULT) {
                r->receiveBatches(_needBatching);
                auto *collector = new Watermark_Collector<decltype(_operator.getKeyExtractor())>(_operator.getKeyExtractor(), _ordering_mode, id++, _needBatching, separator_id);
                combine_with_firststage(*stage, collector, true); // combine with the Watermark_Collector
            }
            else {
                abort(); // case not possible
            }
            result.push_back(stage);
        }
        return result;
    }

#if !defined (__CUDACC__)
    // Create the right emitter to be used to connect to the next operator
    template<typename keyextr_func_t, bool isDestGPUType>
    Basic_Emitter *create_emitter(typename std::enable_if<!isDestGPUType, keyextr_func_t>::type _key_extr,
                                  Routing_Mode_t _routing_mode,
                                  size_t _num_dests,
                                  size_t _outputBatchSize,
                                  bool isSourceGPU=false,
                                  bool isDestGPU=false) const
    {
        assert(!isSourceGPU && !isDestGPU); // sanity check
        if (_routing_mode == Routing_Mode_t::FORWARD) { // FW
            return new Forward_Emitter<decltype(_key_extr)>(_key_extr, _num_dests, _outputBatchSize);
        }
        else if (_routing_mode == Routing_Mode_t::REBALANCING) { // RB
            return new Forward_Emitter<decltype(_key_extr)>(_key_extr, _num_dests, _outputBatchSize);
        }
        else if (_routing_mode == Routing_Mode_t::KEYBY) { // KB
            return new KeyBy_Emitter<decltype(_key_extr)>(_key_extr, _num_dests, execution_mode, _outputBatchSize);
        }
        else if (_routing_mode == Routing_Mode_t::BROADCAST) { // BD
            return new Broadcast_Emitter<decltype(_key_extr)>(_key_extr, _num_dests, _outputBatchSize);
        }
        else {
            abort();
        }
    }

#else
    // Create the right emitter to be used to connect to the next operator
    template<typename keyextr_func_t, bool isDestGPUType>
    Basic_Emitter *create_emitter(typename std::enable_if<!isDestGPUType, keyextr_func_t>::type _key_extr,
                                  Routing_Mode_t _routing_mode,
                                  size_t _num_dests,
                                  size_t _outputBatchSize,
                                  bool isSourceGPU=false,
                                  bool isDestGPU=false) const
    {
        assert(!isDestGPU); // sanity check
        if (!isSourceGPU && !isDestGPU) { // CPU -> CPU case
            if (_routing_mode == Routing_Mode_t::FORWARD) { // FW
                return new Forward_Emitter<decltype(_key_extr)>(_key_extr, _num_dests, _outputBatchSize);
            }
            else if (_routing_mode == Routing_Mode_t::REBALANCING) { // RB
                return new Forward_Emitter<decltype(_key_extr)>(_key_extr, _num_dests, _outputBatchSize);
            }
            else if (_routing_mode == Routing_Mode_t::KEYBY) { // KB
                return new KeyBy_Emitter<decltype(_key_extr)>(_key_extr, _num_dests, execution_mode, _outputBatchSize);
            }
            else if (_routing_mode == Routing_Mode_t::BROADCAST) { // BD
                return new Broadcast_Emitter<decltype(_key_extr)>(_key_extr, _num_dests, _outputBatchSize);
            }
            else {
                abort();
            }
        }
        else if (isSourceGPU && !isDestGPU) { // GPU -> CPU case
            if (_routing_mode == Routing_Mode_t::FORWARD) { // FW
                return new Forward_Emitter_GPU<decltype(_key_extr), true, false>(_key_extr, _num_dests);
            }
            else if (_routing_mode == Routing_Mode_t::REBALANCING) { // RB
                return new Forward_Emitter_GPU<decltype(_key_extr), true, false>(_key_extr, _num_dests);
            }
            else if (_routing_mode == Routing_Mode_t::KEYBY) { // KB
                return new KeyBy_Emitter_GPU<decltype(_key_extr), true, false>(_key_extr, _num_dests);
            }
            else if (_routing_mode == Routing_Mode_t::BROADCAST) { // BD
                return new Broadcast_Emitter_GPU<decltype(_key_extr), true, false>(_key_extr, _num_dests);
            }
            else {
                abort();
            }
        }
        else {
            return nullptr;
        }
    }

    // Create the right emitter to be used to connect to the next operator
    template<typename keyextr_func_t, bool isDestGPUType>
    Basic_Emitter *create_emitter(typename std::enable_if<isDestGPUType, keyextr_func_t>::type _key_extr,
                                  Routing_Mode_t _routing_mode,
                                  size_t _num_dests,
                                  size_t _outputBatchSize,
                                  bool isSourceGPU,
                                  bool isDestGPU) const
    {
        assert(isDestGPU); // sanity check
        if (isSourceGPU && isDestGPU) { // GPU -> GPU case
            if (_routing_mode == Routing_Mode_t::FORWARD) { // FW
                return new Forward_Emitter_GPU<decltype(_key_extr), true, true>(_key_extr, _num_dests);
            }
            else if (_routing_mode == Routing_Mode_t::REBALANCING) { // RB
                return new Forward_Emitter_GPU<decltype(_key_extr), true, true>(_key_extr, _num_dests);
            }
            else if (_routing_mode == Routing_Mode_t::KEYBY) { // KB
                return new KeyBy_Emitter_GPU<decltype(_key_extr), true, true>(_key_extr, _num_dests);
            }
            else {
                abort();
            }
        }
        else { // CPU -> GPU case
            if (_routing_mode == Routing_Mode_t::FORWARD) { // FW
                return new Forward_Emitter_GPU<decltype(_key_extr), false, true>(_key_extr, _num_dests, _outputBatchSize);
            }
            else if (_routing_mode == Routing_Mode_t::REBALANCING) { // RB
                return new Forward_Emitter_GPU<decltype(_key_extr), false, true>(_key_extr, _num_dests, _outputBatchSize);
            }
            else if (_routing_mode == Routing_Mode_t::KEYBY) { // KB
                return new KeyBy_Emitter_GPU<decltype(_key_extr), false, true>(_key_extr, _num_dests, _outputBatchSize);
            }
            else {
                abort();
            }
        }
    }
#endif

    // Get the list of last operators of a MultiPipe (be careful that this method must be used under specific conditions)
    std::vector<Basic_Operator *> getLastOperators() const
    {
        std::vector<Basic_Operator *> result;
        if (!this->isSplit && (localOpList.size() > 0)) { // base case
            result.push_back(localOpList.back());
            return result;
        }
        else {
            if (this->fromMerging) { // MultiPipe is from merging
                for (auto *mp: mergeParents) {
                    auto v = mp->getLastOperators();
                    result.insert(result.end(), v.begin(), v.end());
                }
            }
            else if (this->isSplit) { // MultiPipe has been split
                for (auto *mp: splittingChildren) {
                    auto v = mp->getLastOperators();
                    result.insert(result.end(), v.begin(), v.end());
                }
            }
            return result;
        }
    }

    // Add a Source to the MultiPipe
    template<typename source_t>
    MultiPipe &add_source(const source_t &_source)
    {
        auto *copied_source = new source_t(_source); // create a copy of the operator
        copied_source->setConfiguration(execution_mode, time_policy); // set the execution mode and time policy of the operator
        ff::ff_a2a *matrioska = new ff::ff_a2a(); // create the initial matrioska
        std::vector<ff::ff_node *> first_set;
        for (auto *r: copied_source->replicas) {
            ff::ff_pipeline *stage = new ff::ff_pipeline();
            stage->add_stage(r, false);
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
        lastParallelism = (copied_source->replicas).size(); // save the parallelism of the operator
        localOpList.push_back(copied_source); // add the copied operator to local list
        globalOpList->push_back(copied_source); // add the copied operator to global list
        outputType = TypeName<typename source_t::result_t>::getName(); // save the type of result_t as a string
#if defined (WF_TRACING_ENABLED)
        gv_add_vertex(copied_source->getType() + " (" + std::to_string((copied_source->replicas).size()) + ")", copied_source->getName(), true, false, Routing_Mode_t::NONE);
#endif
        return *this;
    }

    // Add an operator to the MultiPipe
    template<typename operator_t, bool isDestGPUType=false>
    void add_operator(operator_t &_operator, ordering_mode_t _ordering_mode)
    {
        if (!has_source) { // check the Source presence
            std::cerr << RED << "WindFlow Error: MultiPipe does not have a Source, operator cannot be added" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        if (has_sink) { // check the Sink presence
            std::cerr << RED << "WindFlow Error: MultiPipe is terminated by a Sink, operator cannot be added" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        if (isMerged) { // merged MultiPipe cannot be modified
            std::cerr << RED << "WindFlow Error: MultiPipe has been merged, operator cannot be added" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        if (isSplit) { // split MultiPipe cannot be modified
            std::cerr << RED << "WindFlow Error: MultiPipe has been split, operator cannot be added" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        if (auto lastOps = this->getLastOperators(); (_operator.getType() == "Interval_Join_KP" || _operator.getType() == "Interval_Join_DP") && (!fromMerging || localOpList.size() != 0 || lastOps.size() != 2) ) {
            std::cerr << RED << "WindFlow Error: Join operators must be added after merge of two MultiPipes" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        if (fromSplitting && last == nullptr) { // Case 1: first operator added after splitting
            ff::ff_a2a *matrioska = new ff::ff_a2a(); // create the initial matrioska
            assert(splittingParent != nullptr); // sanity check
            assert((splittingParent->localOpList).size() > 0); // sanity check
            size_t outputBatchSize = ((splittingParent->localOpList).back())->getOutputBatchSize();
            bool isSourceGPU = ((splittingParent->localOpList).back())->isGPUOperator();
            bool isDestGPU = _operator.isGPUOperator();
            if (isDestGPU && outputBatchSize == 0) {
                std::cerr << RED << "WindFlow Error: GPU operators must be added after a previous operator declaring an output batch size >0" << DEFAULT_COLOR << std::endl;
                exit(EXIT_FAILURE);
            }
            bool needBatching = (outputBatchSize > 0);
            std::vector<ff::ff_node *> first_set = combine_with_collector(_operator, _ordering_mode, needBatching);
            matrioska->add_firstset(first_set, 0, true);
            std::vector<ff::ff_node *> second_set;
            ff::ff_pipeline *stage = new ff::ff_pipeline();
            stage->add_stage(new selfkiller_node(), true);
            second_set.push_back(stage);
            matrioska->add_secondset(second_set, true);
            this->add_stage(matrioska, true);
            last = matrioska;
            lastParallelism = (_operator.replicas).size(); // save parallelism of the operator
            Basic_Emitter *emitter = create_emitter<decltype(_operator.getKeyExtractor()), isDestGPUType>(_operator.getKeyExtractor(), _operator.getInputRoutingMode(), (_operator.replicas).size(), outputBatchSize, isSourceGPU, isDestGPU);
            splittingParent->setEmitterLeaf(emitter); // set the emitter leaf in the parent MultiPipe
        }
        else {
            size_t n1 = 0; // important to be initialized to zero
            if (localOpList.size() > 0) {
                n1 = (localOpList.back())->getParallelism();
            }
            size_t n2 = _operator.getParallelism();
            if ((n1 == n2) && (_operator.getInputRoutingMode() == Routing_Mode_t::FORWARD)) { // Case 2: direct connection
                auto first_set = last->getFirstSet();
                size_t outputBatchSize = (localOpList.back())->getOutputBatchSize();
                bool isSourceGPU = (localOpList.back())->isGPUOperator();
                bool isDestGPU = _operator.isGPUOperator();
                if (isDestGPU && outputBatchSize == 0) {
                    std::cerr << RED << "WindFlow Error: GPU operators must be added after a previous operator declaring an output batch size >0" << DEFAULT_COLOR << std::endl;
                    exit(EXIT_FAILURE);
                }
                for (size_t i=0; i<n1; i++) { // add the operator's replicas to the pipelines in the first set of the last matrioska
                    _operator.receiveBatches(outputBatchSize > 0);
                    ff::ff_pipeline *stage = static_cast<ff::ff_pipeline *>(first_set[i]);
                    stage->add_stage((_operator.replicas)[i], false);
                }
                (localOpList.back())->setEmitter(create_emitter<decltype(_operator.getKeyExtractor()), isDestGPUType>(_operator.getKeyExtractor(), _operator.getInputRoutingMode(), 1, outputBatchSize, isSourceGPU, isDestGPU));
            }
            else { // Case 3: shuffle connection
                auto lastOps = this->getLastOperators();
                bool needBatching = false;
                bool isDestGPU = _operator.isGPUOperator();
                for (auto *op: lastOps) {
                    if (isDestGPU || op->getOutputBatchSize() > 0) {
                        needBatching = true;
                    }
                    if (isDestGPU && op->getOutputBatchSize() == 0) {
                        std::cerr << RED << "WindFlow Error: GPU operators must be added after a previous operator declaring an output bath size >0" << DEFAULT_COLOR << std::endl;
                        exit(EXIT_FAILURE);
                    }
                }
                for (auto *op: lastOps) { // set the emitter to the last operator(s) in the MultiPipe
                    bool isSourceGPU = op->isGPUOperator();
                    size_t outputBatchSize = op->getOutputBatchSize();
                    if (needBatching && op->getOutputBatchSize() == 0) {
                        outputBatchSize = 1; // force to use batching
                    }
                    op->setEmitter(create_emitter<decltype(_operator.getKeyExtractor()), isDestGPUType>(_operator.getKeyExtractor(), _operator.getInputRoutingMode(), _operator.getParallelism(), outputBatchSize, isSourceGPU, isDestGPU));
                }
                ff::ff_a2a *matrioska = new ff::ff_a2a(); // create a new matrioska
                std::vector<ff::ff_node *> first_set = combine_with_collector(_operator, _ordering_mode, needBatching);
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
            }
            lastParallelism = n2; // save the parallelism of the new operator
        }
    }

    // Try to chain an operator with the previous one in the MultiPipe (it is added otherwise)
    template<typename operator_t, bool isDestGPUType=false>
    bool chain_operator(operator_t &_operator, ordering_mode_t _ordering_mode)
    {
        if (!has_source) { // check the Source presence
            std::cerr << RED << "WindFlow Error: MultiPipe does not have a Source, operator cannot be chained" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        if (has_sink) { // check the Sink presence
            std::cerr << RED << "WindFlow Error: MultiPipe is terminated by a Sink, operator cannot be chained" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        if (isMerged) { // merged MultiPipe cannot be modified
            std::cerr << RED << "WindFlow Error: MultiPipe has been merged, operator cannot be chained" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        if (isSplit) { // split MultiPipe cannot be modified
            std::cerr << RED << "WindFlow Error: MultiPipe has been split, operator cannot be chained" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        if (fromSplitting && last == nullptr) { // corner case -> first operator after splitting can never be chained
            add_operator<operator_t, isDestGPUType>(_operator, _ordering_mode);
            return false;
        }
        if (fromMerging && localOpList.size() == 0) { // corner case -> first operator after merging can never be chained
            add_operator<operator_t, isDestGPUType>(_operator, _ordering_mode);
            return false;
        }
        assert(localOpList.size() > 0); // sanity check
        size_t n1 = (localOpList.back())->getParallelism();
        bool isSourceGPU = (localOpList.back())->isGPUOperator();
        size_t n2 = _operator.getParallelism();
        bool isDestGPU = _operator.isGPUOperator();
        if ((n1 == n2) && (_operator.getInputRoutingMode() == Routing_Mode_t::FORWARD)) { // conditions for chaining
            auto first_set = last->getFirstSet();
            size_t outputBatchSize = (localOpList.back())->getOutputBatchSize();
            if (isDestGPU && outputBatchSize == 0) {
                std::cerr << RED << "WindFlow Error: GPU operators must be added after a previous operator declaring an output bath size >0" << DEFAULT_COLOR << std::endl;
                exit(EXIT_FAILURE);
            }
            for (size_t i=0; i<n1; i++) { // combine the operator's replicas to the replicas of the previous operator
                ff::ff_pipeline *stage = static_cast<ff::ff_pipeline *>(first_set[i]);
                auto *worker = (_operator.replicas)[i];
                worker->receiveBatches(outputBatchSize > 0);
                combine_with_laststage(*stage, worker, false);
            }
            (localOpList.back())->setEmitter(create_emitter<decltype(_operator.getKeyExtractor()), isDestGPUType>(_operator.getKeyExtractor(), _operator.getInputRoutingMode(), 1, outputBatchSize, isSourceGPU, isDestGPU));
            lastParallelism = n2; // save the parallelism of the new operator
            return true;
        }
        else {
            add_operator<operator_t, isDestGPUType>(_operator, _ordering_mode);
            return false;
        }
    }

    // Normalize the MultiPipe (removing the final self-killer nodes)
    std::vector<ff::ff_node *> normalize()
    {
        if (!has_source) { // check the Source presence
            std::cerr << RED << "WindFlow Error: MultiPipe does not have a Source, it cannot be merged/split" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        if (has_sink) { // check the Sink presence
            std::cerr << RED << "WindFlow Error: MultiPipe is terminated by a Sink, it cannot be merged/split" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        if (last == nullptr) { // empty MultiPipe cannot be normalized (only for splitting in this case)
            std::cerr << RED << "WindFlow Error: MultiPipe is empty, it cannot be split" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        std::vector<ff::ff_node *> result;
        if (!isSplit) { // Case 1: the MultiPipe has not been split
            if (secondToLast == nullptr) { // Case 1.1 (only one Matrioska)
                auto first_set = last->getFirstSet();
                for (size_t i=0; i<first_set.size(); i++) {
                    result.push_back(first_set[i]);
                }
            }
            else { // Case 1.2 (at least two nested Matrioska)
                auto first_set_last = last->getFirstSet();
                last->remove_from_cleanuplist(first_set_last);
                std::vector<ff::ff_node *> second_set_secondToLast;
                for (size_t i=0; i<first_set_last.size(); i++) {
                    second_set_secondToLast.push_back(first_set_last[i]);
                }
                secondToLast->change_secondset(second_set_secondToLast, true, true);
                delete last;
                last = secondToLast;
                secondToLast = nullptr;
                ff::ff_pipeline *p = new ff::ff_pipeline(); // <-- Memory leak!
                p->add_stage((this->getStages())[0], false); // we get the pointer to the outermost block in the MultiPipe
                result.push_back(p);
            }
        }
        else { // Case 2: the MultiPipe has been split
            std::vector<ff::ff_node *> normalized;
            auto second_set = last->getSecondSet();
            for (auto *node: second_set) {
                MultiPipe *mp = static_cast<MultiPipe *>(node);
                auto v = mp->normalize();
                mp->isMerged = true; // we know that mp has been normalized for merging
                normalized.insert(normalized.end(), v.begin(), v.end());
            }
            last->change_secondset(normalized, false);
            ff::ff_pipeline *p = new ff::ff_pipeline(); // <-- Memory leak!
            p->add_stage(last, false);
            result.push_back(p);
        }
        return result;
    }

    // Prepare the set of merged MultiPipes
    std::vector<MultiPipe *> prepareMergeSet()
    {
        return std::vector<MultiPipe *>();
    }

    // Prepare the set of merged MultiPipes
    template<typename MULTIPIPE>
    std::vector<MultiPipe *> prepareMergeSet(MULTIPIPE &_pipe)
    {
        if (_pipe.isMerged) { // check if the MultiPipe has already been merged
            std::cerr << RED << "WindFlow Error: MultiPipe has already been merged" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        if (_pipe.isSplit) { // check if the MultiPipe has been split
            std::cerr << RED << "WindFlow Error: MultiPipe has been split and cannot be merged" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        if (localOpList.size() == 0) {
            std::cerr << RED << "WindFlow Error: empty MultiPipe cannot be merged" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        std::vector<MultiPipe *> output;
        output.push_back(&_pipe);
        return output;
    }

    // Prepare the set of merged MultiPipes
    template<typename MULTIPIPE, typename ...MULTIPIPES>
    std::vector<MultiPipe *> prepareMergeSet(MULTIPIPE &_first, MULTIPIPES&... _pipes)
    { 
        if (_first.isMerged) { // check if the MultiPipe has already been merged
            std::cerr << RED << "WindFlow Error: MultiPipe has already been merged" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        if (_first.isSplit) { // check if the MultiPipe has been split
            std::cerr << RED << "WindFlow Error: MultiPipe has been split and cannot be merged" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        if (localOpList.size() == 0) {
            std::cerr << RED << "WindFlow Error: empty MultiPipe cannot be merged" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        std::vector<MultiPipe *> output;
        output.push_back(&_first);
        auto v = prepareMergeSet(_pipes...);
        output.insert(output.end(), v.begin(), v.end());
        return output;
    }

    // Set an emitter acting as a leaf of the splitting emitter
    void setEmitterLeaf(Basic_Emitter *_e)
    {
        assert(isSplit); // sanity check
        assert(splittingEmitter != nullptr); // sanity check
        splittingEmitter->addInternalEmitter(_e);
        if (splittingEmitter->getNumInternalEmitters() == splittingBranches) { // check if all the emitters are ready
            assert(localOpList.size() > 0); // sanity check
            (localOpList.back())->setEmitter(splittingEmitter); // set the emitter of all the replicas of the last operator in the MultiPipe
        }
    }

#if defined (WF_TRACING_ENABLED)
    // Add a new operator to the graphviz representation
    void gv_add_vertex(std::string typeOP,
                       std::string nameOP,
                       bool isCPU_OP,
                       bool isNested,
                       Routing_Mode_t routing_type)
    {
        assert(gv_graph != nullptr);
        Agnode_t *node = agnode(gv_graph, NULL, 1); // creating the new vertex
        std::string label = typeOP + "<BR/><FONT POINT-SIZE=\"8\">" + nameOP + "</FONT>"; // prepare the label for this vertex
        agset(node, const_cast<char *>("label"), agstrdup_html(gv_graph, const_cast<char *>(label.c_str()))); // change the label of the operator
        if ((typeOP.compare(0, 6, "Source") == 0) || (typeOP.compare(0, 4, "Sink") == 0)) {
            agset(node, const_cast<char *>("color"), const_cast<char *>("#B8B7B8")); // style for Source and Sink operators
            agset(node, const_cast<char *>("fillcolor"), const_cast<char *>("black"));
        }
        else if ((typeOP.compare(0, 12, "Kafka_Source") == 0) || (typeOP.compare(0, 10, "Kafka_Sink") == 0)) {
            agset(node, const_cast<char *>("color"), const_cast<char *>("#B8B7B8")); // style for Kafka_Source and Kafka_Sink operators
            agset(node, const_cast<char *>("fillcolor"), const_cast<char *>("black"));
        }
        else if (typeOP.compare(0, 6, "P_Sink") == 0) {
            agset(node, const_cast<char *>("color"), const_cast<char *>("#B8B7B8")); // style for P_Sink operator
            agset(node, const_cast<char *>("fillcolor"), const_cast<char *>("black"));
        }
        else if (isCPU_OP) { // style of CPU operators
            if (typeOP.compare(0, 2, "P_") == 0) { // persistent operators
                agset(node, const_cast<char *>("color"), const_cast<char *>("#E13AE2"));
                agset(node, const_cast<char *>("fillcolor"), const_cast<char *>("#EA3DF7"));                
            }
            else { // non-persistent operators
                agset(node, const_cast<char *>("color"), const_cast<char *>("#941100"));
                agset(node, const_cast<char *>("fillcolor"), const_cast<char *>("#ff9400"));
            }
        }
        else { // style of GPU operators
            agset(node, const_cast<char *>("color"), const_cast<char *>("#20548E"));
            agset(node, const_cast<char *>("fillcolor"), const_cast<char *>("#469EA2"));
        }
        if (isNested) { // <-- not used anymore!
            agset(node, const_cast<char *>("shape"), const_cast<char *>("doubleoctagon"));
        }
        for (auto *vertex: this->gv_last_vertices) { // connect the new vertex to the previous one(s)
            Agedge_t *e = agedge(gv_graph, vertex, node, 0, 1);
            // set the label of the edge
            if (routing_type == Routing_Mode_t::FORWARD) {
                agset(e, const_cast<char *>("label"), const_cast<char *>("FW"));
            }
            else if (routing_type == Routing_Mode_t::REBALANCING) {
                agset(e, const_cast<char *>("label"), const_cast<char *>("RB"));
            }
            else if (routing_type == Routing_Mode_t::KEYBY) {
                agset(e, const_cast<char *>("label"), const_cast<char *>("KB"));
            }
            else if (routing_type == Routing_Mode_t::BROADCAST) {
                agset(e, const_cast<char *>("label"), const_cast<char *>("BD"));
            }
        }
        (this->gv_last_vertices).clear();
        (this->gv_last_typeOPs).clear();
        (this->gv_last_nameOPs).clear();
        (this->gv_last_vertices).push_back(node);
        (this->gv_last_typeOPs).push_back(typeOP);
        (this->gv_last_nameOPs).push_back(nameOP);
    }

    // Chain of a new operator in the graphviz representation
    void gv_chain_vertex(std::string typeOP, std::string nameOP)
    {
        assert(gv_graph != nullptr);
        assert((this->gv_last_vertices).size() == 1);
        assert((this->gv_last_typeOPs).size() == 1);
        assert((this->gv_last_nameOPs).size() == 1);
        auto *node = (this->gv_last_vertices)[0]; // prepare the new label of the last existing node
        auto &lastTypeOPs = (this->gv_last_typeOPs)[0];
        auto &lastNameOPs = (this->gv_last_nameOPs)[0];
        lastTypeOPs = lastTypeOPs + "->" + typeOP;
        lastNameOPs = lastNameOPs + ", " + nameOP;
        std::string label = lastTypeOPs + "<BR/><FONT POINT-SIZE=\"8\">" + lastNameOPs + "</FONT>";
        agset(node, const_cast<char *>("label"), agstrdup_html(gv_graph, const_cast<char *>(label.c_str())));
        if ((typeOP.compare(0, 4, "Sink") == 0) || (typeOP.compare(0, 10, "Kafka_Sink") == 0) || (typeOP.compare(0, 6, "P_Sink") == 0)) { // style for Sink, Kafka_Sink, P_Sink operators
            agset(node, const_cast<char *>("color"), const_cast<char *>("#B8B7B8"));
            agset(node, const_cast<char *>("fillcolor"), const_cast<char *>("black"));
        }
        else if (typeOP.compare(0, 2, "P_") == 0) { // style for persistent operators
            agset(node, const_cast<char *>("color"), const_cast<char *>("#E13AE2"));
            agset(node, const_cast<char *>("fillcolor"), const_cast<char *>("#EA3DF7"));        
        }
        else if (typeOP.compare(0, 7, "Map_GPU") == 0) { // style for Map_GPU operator
            agset(node, const_cast<char *>("color"), const_cast<char *>("#20548E"));
            agset(node, const_cast<char *>("fillcolor"), const_cast<char *>("#469EA2"));         
        }
        else if (typeOP.compare(0, 10, "Filter_GPU") == 0) { // style for Filter_GPU operator
            agset(node, const_cast<char *>("color"), const_cast<char *>("#20548E"));
            agset(node, const_cast<char *>("fillcolor"), const_cast<char *>("#469EA2"));          
        }
        else if (typeOP.compare(0, 10, "Reduce_GPU") == 0) { // style for Reduce_GPU operator
            agset(node, const_cast<char *>("color"), const_cast<char *>("#20548E"));
            agset(node, const_cast<char *>("fillcolor"), const_cast<char *>("#469EA2"));         
        }
        else if (typeOP.compare(0, 16, "Ffat_Windows_GPU") == 0) { // style for Ffat_Windows_GPU operator
            agset(node, const_cast<char *>("color"), const_cast<char *>("#20548E"));
            agset(node, const_cast<char *>("fillcolor"), const_cast<char *>("#469EA2"));       
        }        
    }
#endif

    // Run the MultiPipe
    int run()
    {
        int status = 0;
        if (!isRunnable()) {
            std::cerr << RED << "WindFlow Error: some MultiPipes are not runnable, check Sources and Sinks presence" << DEFAULT_COLOR << std::endl;
            status = -1;
        }
        else {
            status = ff::ff_pipeline::run();
        }
        return status;
    }

    // Wait the processing of the MultiPipe
    int wait()
    {
        int status = 0;
        if (!isRunnable()) {
            std::cerr << RED << "WindFlow Error: some MultiPipes are not runnable, check Sources and Sinks presence" << DEFAULT_COLOR << std::endl;
            status = -1;
        }
        else {
            status =ff::ff_pipeline::wait();
        }
        return status;
    }

    // Run and wait the processing of the MultiPipe
    int run_and_wait_end()
    {
        int status = 0;
        if (!isRunnable()) {
            std::cerr << RED << "WindFlow Error: some MultiPipes are not runnable, check Sources and Sinks presence" << DEFAULT_COLOR << std::endl;
            status = -1;
        }
        else {
            status = ff::ff_pipeline::run_and_wait_end();
        }
        return status;
    }

    // Check if the MultiPipe is runnable
    bool isRunnable() const
    {
        return (has_source && has_sink) || (isMerged) || (isSplit);
    }

    // Check if the MultiPipe has a Source
    bool hasSource() const
    {
        return has_source;
    }

    // Check if the MultiPipe has a Sink
    bool hasSink() const
    {
        return has_sink;
    }

    // Get the number of threads used to run this MultiPipe
    size_t getNumThreads() const
    {
        if (!isSplit) {
            auto n = this->cardinality();
            if (n == 0) {
                return 0;
            }
            else {
                if (fromMerging && localOpList.size() == 0) {
                    return n;
                }
                else {
                    return n-1;
                }
            }
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

    // Check the input type accepted by the new operator (runtime check based on string comparison)
    template<typename op_t>
    void checkInputType(op_t &_op)
    {
        std::string opInType = TypeName<typename op_t::tuple_t>::getName(); // save the type of tuple_t as a string
        if (!outputType.empty() && outputType.compare(opInType) != 0) {
            std::cerr << RED << "WindFlow Error: output type from MultiPipe is not the input type of the" + _op.getType() + " operator" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        outputType = TypeName<typename op_t::result_t>::getName(); // save the new output type from this MultiPipe
    }

#if defined (WF_TRACING_ENABLED)
    // Update the graphviz representation with a new CPU operator
    template<typename op_t>
    void update_gv(op_t &_op, bool isChaining=false)
    {
        if (!isChaining) {
            gv_add_vertex(_op.getType() + " (" + std::to_string(_op.getParallelism()) + ")", _op.getName(), true, false, _op.getInputRoutingMode());
        }
        else {
            gv_chain_vertex(_op.getType() + " (" + std::to_string(_op.getParallelism()) + ")", _op.getName());
        }
    }

#if defined (__CUDACC__)
    // Update the graphviz representation with a new GPU operator
    template<typename op_t>
    void update_gv_gpu(op_t &_op, bool isChaining=false)
    {
        if (!isChaining) {
            gv_add_vertex(_op.getType() + " (" + std::to_string(_op.getParallelism()) + ")", _op.getName(), false, false, _op.getInputRoutingMode());
        }
        else {
            gv_chain_vertex(_op.getType() + " (" + std::to_string(_op.getParallelism()) + ")", _op.getName());
        }
    }
#endif
#endif

public:
    /** 
     *  \brief Add an operator to the MultiPipe
     *  \param _op the operator to be added
     *  \return a reference to the modified MultiPipe
     */ 
    template<typename op_t>
    MultiPipe &add(const op_t &_op)
    {
        if constexpr (op_t::op_type == op_type_t::BASIC || op_t::op_type == op_type_t::WIN ||
                      op_t::op_type == op_type_t::P_BASIC || op_t::op_type == op_type_t::P_WIN) {
            if ((_op.replicas).size() == 0) {
                std::cerr << RED << "WindFlow Error: operator of type " << _op.getType() << " cannot be added to a MultiPipe!" << DEFAULT_COLOR << std::endl;
                exit(EXIT_FAILURE);             
            }
            auto *copied_op = new op_t(_op); // create a copy of the operator
            copied_op->setExecutionMode(execution_mode);
            checkInputType(*copied_op);
            add_operator(*copied_op, ordering_mode_t::TS);
            localOpList.push_back(copied_op);
            globalOpList->push_back(copied_op);
#if defined (WF_TRACING_ENABLED)
            update_gv(*copied_op);
#endif
        }
#if defined (__CUDACC__)
        else if constexpr (op_t::op_type == op_type_t::BASIC_GPU) {
            auto *copied_op = new op_t(_op ); // create a copy of the operator
            copied_op->setExecutionMode(execution_mode);
            checkInputType(*copied_op);
            add_operator<decltype(*copied_op), true>(*copied_op, ordering_mode_t::TS);
            localOpList.push_back(copied_op);
            globalOpList->push_back(copied_op);
#if defined (WF_TRACING_ENABLED)
            update_gv_gpu(*copied_op);
#endif
        }
#endif
        else if constexpr (op_t::op_type == op_type_t::WIN_PANED) {
            checkInputType(_op);
            // add the first sub-operator (PLQ)
            auto *plq = new Parallel_Windows<decltype(_op.plq_func), decltype(_op.key_extr)>(_op.plq);
            plq->setExecutionMode(execution_mode);
            add_operator(*plq, ordering_mode_t::TS);
            localOpList.push_back(plq);
            globalOpList->push_back(plq);
            // add the second sub-operator (WLQ)
            auto *wlq = new Parallel_Windows<decltype(_op.wlq_func), decltype(_op.key_extr)>(_op.wlq);
            wlq->setExecutionMode(execution_mode);
            add_operator(*wlq, ordering_mode_t::ID);
            localOpList.push_back(wlq);
            globalOpList->push_back(wlq);
#if defined (WF_TRACING_ENABLED)
            gv_add_vertex(_op.getType() + " (" + std::to_string(_op.plq_parallelism) + "," + std::to_string(_op.wlq_parallelism) + ")", _op.getName(), false, false, _op.getInputRoutingMode());
#endif
        }
        else if constexpr (op_t::op_type == op_type_t::WIN_MR) {
            checkInputType(_op);
            // add the first sub-operator (MAP)
            auto *map = new Parallel_Windows<decltype(_op.map_func), decltype(_op.key_extr)>(_op.map);
            map->setExecutionMode(execution_mode);
            add_operator(*map, ordering_mode_t::TS);
            localOpList.push_back(map);
            globalOpList->push_back(map);
            // add the second sub-operator (REDUCE)
            auto *reduce = new Parallel_Windows<decltype(_op.reduce_func), decltype(_op.key_extr)>(_op.reduce);
            reduce->setExecutionMode(execution_mode);
            add_operator(*reduce, ordering_mode_t::ID);
            localOpList.push_back(reduce);
            globalOpList->push_back(reduce);
#if defined (WF_TRACING_ENABLED)
            gv_add_vertex(_op.getType() + " (" + std::to_string(_op.map_parallelism) + "," + std::to_string(_op.reduce_parallelism) + ")", _op.getName(), false, false, _op.getInputRoutingMode());
#endif
        }
        else if constexpr (op_t::op_type == op_type_t::WIN_FFAT) {
            auto *copied_op = new op_t(_op);
            copied_op->setExecutionMode(execution_mode);
            checkInputType(*copied_op);
            add_operator(*copied_op, ordering_mode_t::TS);
            localOpList.push_back(copied_op);
            globalOpList->push_back(copied_op);
    #if defined (WF_TRACING_ENABLED)
            update_gv(*copied_op);
    #endif
        }
#if defined (__CUDACC__)
        else if constexpr (op_t::op_type == op_type_t::WIN_FFAT_GPU) {
            auto *copied_op = new op_t(_op);
            copied_op->setExecutionMode(execution_mode);
            checkInputType(*copied_op);
            add_operator<decltype(*copied_op), true>(*copied_op, ordering_mode_t::TS);
            localOpList.push_back(copied_op);
            globalOpList->push_back(copied_op);
#if defined (WF_TRACING_ENABLED)
            update_gv_gpu(*copied_op);
#endif
        }
#endif
        else {
            if constexpr (op_t::op_type == op_type_t::SOURCE) {
                std::cerr << RED << "WindFlow Error: Source cannot be added to a MultiPipe" << DEFAULT_COLOR << std::endl;
                exit(EXIT_FAILURE);
            }
            else if constexpr (op_t::op_type == op_type_t::SINK) {
                std::cerr << RED << "WindFlow Error: Use add_sink() to add a SINK to a MultiPipe" << DEFAULT_COLOR << std::endl;
                exit(EXIT_FAILURE);
            }
            else {
                std::cerr << RED << "WindFlow Error: adding an unrecognized operator to a MultiPipe" << DEFAULT_COLOR << std::endl;
                exit(EXIT_FAILURE);
            }
        }
        return *this;
    }

    /** 
     *  \brief Try to chain an operator to the MultiPipe (if not possible, the operator is added)
     *  \param _op the operator to be chained
     *  \return a reference to the modified MultiPipe
     */ 
    template<typename op_t>
    MultiPipe &chain(const op_t &_op)
    {
        if constexpr (op_t::op_type != op_type_t::BASIC && op_t::op_type != op_type_t::BASIC_GPU &&
                      op_t::op_type != op_type_t::P_BASIC) {
            std::cerr << RED << "WindFlow Error: Trying to chain an operator for which chaining is forbidden" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        else if constexpr (op_t::op_type == op_type_t::BASIC || op_t::op_type == op_type_t::P_BASIC) {
            if (_op.getType() == "Reduce") {
                std::cerr << RED << "WindFlow Error: Reduce operator cannot be chained" << DEFAULT_COLOR << std::endl;
                exit(EXIT_FAILURE);
            }
            else if (_op.getType() == "P_Reduce") {
                std::cerr << RED << "WindFlow Error: P_Reduce operator cannot be chained" << DEFAULT_COLOR << std::endl;
                exit(EXIT_FAILURE);
            }
            auto *copied_op = new op_t(_op); // create a copy of the operator
            copied_op->setExecutionMode(execution_mode);
            checkInputType(*copied_op);
            bool isChained = chain_operator(*copied_op, ordering_mode_t::TS); // try to chain the operator (otherwise, it is added)
            localOpList.push_back(copied_op);
            globalOpList->push_back(copied_op);
#if defined (WF_TRACING_ENABLED)
            update_gv(*copied_op, isChained);
#endif
        }
#if defined (__CUDACC__)
        else if constexpr (op_t::op_type == op_type_t::BASIC_GPU) {
            auto *copied_op = new op_t(_op); // create a copy of the operator
            copied_op->setExecutionMode(execution_mode);
            checkInputType(*copied_op);
            bool isChained = chain_operator<decltype(*copied_op), true>(*copied_op, ordering_mode_t::TS); // try to chain the operator (otherwise, it is added)
            localOpList.push_back(copied_op);
            globalOpList->push_back(copied_op);
#if defined (WF_TRACING_ENABLED)
            update_gv_gpu(*copied_op, isChained);
#endif
        }
#endif
        else {
            if constexpr (op_t::op_type == op_type_t::SOURCE) {
                std::cerr << RED << "WindFlow Error: Source cannot be chained to a MultiPipe" << DEFAULT_COLOR << std::endl;
                exit(EXIT_FAILURE);
            }
            else if constexpr (op_t::op_type == op_type_t::SINK) {
                std::cerr << RED << "WindFlow Error: Use chain_sink() to chain a SINK to a MultiPipe" << DEFAULT_COLOR << std::endl;
                exit(EXIT_FAILURE);
            }
            else {
                std::cerr << RED << "WindFlow Error: chaining is forbidden for the given operator or operator not recognized" << DEFAULT_COLOR << std::endl;
                exit(EXIT_FAILURE);
            }
        }
        return *this;
    }

    /** 
     *  \brief Add a Sink operator to the MultiPipe
     *  \param _sink the Sink operator to be added
     *  \return a reference to the modified MultiPipe
     */ 
    template<typename sink_t>
    MultiPipe &add_sink(const sink_t &_sink)
    {
        auto *copied_sink = new sink_t(_sink); // create a copy of the operator
        copied_sink->setExecutionMode(execution_mode);
        std::string opInType = TypeName<typename sink_t::tuple_t>::getName(); // save the type of tuple_t as a string
        if (!outputType.empty() && outputType.compare(opInType) != 0) {
            std::cerr << RED << "WindFlow Error: output type from MultiPipe is not the input type of the Sink operator" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        add_operator(*copied_sink, ordering_mode_t::TS);
#if defined (WF_TRACING_ENABLED)
        gv_add_vertex(copied_sink->getType() + " (" + std::to_string(copied_sink->getParallelism()) + ")", copied_sink->getName(), true, false, copied_sink->getInputRoutingMode());
#endif
        localOpList.push_back(copied_sink);
        globalOpList->push_back(copied_sink);
        has_sink = true;
        return *this;
    }

    /** 
     *  \brief Try to chain a Sink operator to the MultiPipe (if not possible, the operator is added)
     *  \param _sink the Sink operator to be chained
     *  \return a reference to the modified MultiPipe
     */ 
    template<typename sink_t>
    MultiPipe &chain_sink(const sink_t &_sink)
    {
        auto *copied_sink = new sink_t(_sink); // create a copy of the operator
        copied_sink->setExecutionMode(execution_mode);
        std::string opInType = TypeName<typename sink_t::tuple_t>::getName(); // save the type of tuple_t as a string
        if (!outputType.empty() && outputType.compare(opInType) != 0) {
            std::cerr << RED << "WindFlow Error: output type from MultiPipe is not the input type of the Sink operator" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        bool isChained = chain_operator(*copied_sink, ordering_mode_t::TS); // try to chain the operator (otherwise, it is added)
        if (isChained) {
#if defined (WF_TRACING_ENABLED)
            gv_chain_vertex(copied_sink->getType() + " (" + std::to_string(copied_sink->getParallelism()) + ")", copied_sink->getName());
#endif
        }
        else {
#if defined (WF_TRACING_ENABLED)
            gv_add_vertex(copied_sink->getType() + " (" + std::to_string(copied_sink->getParallelism()) + ")", copied_sink->getName(), true, false, copied_sink->getInputRoutingMode());
#endif
        }
        localOpList.push_back(copied_sink);
        globalOpList->push_back(copied_sink);
        has_sink = true;
        return *this;
    }

    /** 
     *  \brief Merge the MultiPipe with a set of MultiPipes _pipes
     *  \param _pipes set of MultiPipes to be merged with this
     *  \return a reference to the new MultiPipe (the result fo the merge)
     */ 
    template<typename ...MULTIPIPES>
    MultiPipe &merge(MULTIPIPES&... _pipes)
    {
        auto mergeSet = prepareMergeSet(*this, _pipes...);
        if (mergeSet.size() < 2) { // at least two MultiPipes must be merged
            std::cerr << RED << "WindFlow Error: merge must be applied to at least two MultiPipes" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        std::map<MultiPipe *, int> counters; // check duplicates
        for (auto *mp: mergeSet) {
            if (counters.find(mp) == counters.end()) {
                counters.insert(std::make_pair(mp, 1));
            }
            else {
                std::cerr << RED << "WindFlow Error: a MultiPipe cannot be merged with itself" << DEFAULT_COLOR << std::endl;
                exit(EXIT_FAILURE);
            }
        }
        std::string outT = mergeSet[0]->outputType; // check that all the MultiPipe instances to be merged have the same output type
        for (auto *mp: mergeSet) {
            if (outT.compare(mp->outputType) != 0) {
                std::cerr << RED << "WindFlow Error: MultiPipe instances to be merged must have the same output type" << DEFAULT_COLOR << std::endl;
                exit(EXIT_FAILURE);
            }
        }
        MultiPipe *mergedMP = merge_multipipes_func(graph, mergeSet); // execute the merge through the PipeGraph
        mergedMP->outputType = mergeSet[0]->outputType; // set the output type of the merged MultiPipe
#if defined (WF_TRACING_ENABLED)
        for (auto *mp: mergeSet) {
            (mergedMP->gv_last_vertices).insert((mergedMP->gv_last_vertices).end(), (mp->gv_last_vertices).begin(), (mp->gv_last_vertices).end());
        }
#endif
        return *mergedMP;
    }

    /** 
     *  \brief Split of this into a set of new empty MultiPipes
     *  \param _splitting_func splitting logic (a function or a callable type)
     *  \param _cardinality number of empty MultiPipes to generate from the splitting
     *  \return a reference to the MultiPipe where the splitting has been applied
     */ 
    template<typename splitting_func_t>
    MultiPipe &split(splitting_func_t _splitting_func, size_t _cardinality)
    {
        if (isMerged) { // check if the MultiPipe has been merged
            std::cerr << RED << "WindFlow Error: MultiPipe has been merged and cannot be split" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        if (isSplit) { // check if the MultiPipe has already been split
            std::cerr << RED << "WindFlow Error: MultiPipe has already been split" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        if (localOpList.size() == 0) {
            std::cerr << RED << "WindFlow Error: empty MultiPipe cannot be split" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        if (_cardinality < 2) { // the cardinality must be 2 at least
            std::cerr << RED << "WindFlow Error: at least two splitting branches are expected" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        if (localOpList.back()->isGPUOperator()) { // split cannot be used after a GPU operator
            std::cerr << RED << "WindFlow Error: last operator of the MultiPipe is a Map_GPU or a Filter_GPU, use split_gpu() instead of split()" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        splittingBranches = _cardinality;
        splittingEmitter = new Splitting_Emitter<splitting_func_t>(_splitting_func, _cardinality, execution_mode);
        using tuple_t = decltype(get_tuple_t_Split(_splitting_func)); // extracting the tuple_t type and checking the admissible signatures
        if (!outputType.empty() && outputType.compare(TypeName<tuple_t>::getName()) != 0) { // check the type compatibility
            std::cerr << RED << "WindFlow Error: output type from MultiPipe is not the input type of the splitting logic" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        splittingChildren = split_multipipe_func(graph, this); // execute the merge through the PipeGraph
#if defined (WF_TRACING_ENABLED)
        for (auto *mp: this->splittingChildren) {
            mp->gv_last_vertices = this->gv_last_vertices;
        }
#endif
        return *this;
    }

#if defined (__CUDACC__)
    /** 
     *  \brief Split of this into a set of new empty MultiPipes
     *  \param _cardinality number of empty MultiPipes to generate from the splitting
     *  \return a reference to the MultiPipe where the splitting has been applied
     */ 
    template<typename tuple_t>
    MultiPipe &split_gpu(size_t _cardinality)
    {
        if (isMerged) { // check if the MultiPipe has been merged
            std::cerr << RED << "WindFlow Error: MultiPipe has been merged and cannot be split" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        if (isSplit) { // check if the MultiPipe has already been split
            std::cerr << RED << "WindFlow Error: MultiPipe has already been split" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        if (localOpList.size() == 0) {
            std::cerr << RED << "WindFlow Error: empty MultiPipe cannot be split" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        if (_cardinality < 2) { // the cardinality must be 2 at least
            std::cerr << RED << "WindFlow Error: at least two splitting branches are expected" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        if (!localOpList.back()->isGPUOperator()) { // split_gpu cannot be used after a CPU operator
            std::cerr << RED << "WindFlow Error: last operator of the MultiPipe is not a Map_GPU or a Filter_GPU, use split() instead of split_gpu()" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        splittingBranches = _cardinality;
        splittingEmitter = new Splitting_Emitter_GPU<tuple_t>(_cardinality);
        if (!outputType.empty() && outputType.compare(TypeName<tuple_t>::getName()) != 0) { // check the type compatibility
            std::cerr << RED << "WindFlow Error: output type from MultiPipe is not the input type expected by the split_gpu()" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        splittingChildren = split_multipipe_func(graph, this); // execute the merge through the PipeGraph
#if defined (WF_TRACING_ENABLED)
        for (auto *mp: this->splittingChildren) {
            mp->gv_last_vertices = this->gv_last_vertices;
        }
#endif
        return *this;
    }
#endif

    /** 
     *  \brief Get a splitting branch of the MultiPipe
     *  \param _idx index of the MultiPipe (splitting branch) to be selected
     *  \return reference to MultiPipe representing the chosen splitting branch
     */ 
    MultiPipe &select(size_t _idx) const
    {
        if (isMerged) {
            std::cerr << RED << "WindFlow Error: MultiPipe has been merged, select() cannot be executed" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        if (!isSplit) { // check if the MultiPipe has been merged
            std::cerr << RED << "WindFlow Error: MultiPipe has not been split, select() cannot be executed" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        if (_idx >= splittingChildren.size()) {
            std::cerr << RED << "WindFlow Error: index of select() is out of range" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        return *(splittingChildren[_idx]);
    }

    MultiPipe(const MultiPipe &) = delete; ///< Copy constructor is deleted
    MultiPipe(MultiPipe &&) = delete; ///< Move constructor is deleted
    MultiPipe &operator=(const MultiPipe &) = delete; ///< Copy assignment operator is deleted
    MultiPipe &operator=(MultiPipe &&) = delete; ///< Move assignment operator is deleted
};

} // namespace wf

#endif
