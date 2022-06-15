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
#if defined (__CUDACC__)
    #include<basic_gpu.hpp>
    #include<broadcast_emitter_gpu.hpp>
    #include<splitting_emitter_gpu.hpp>
    #if !defined (WF_GPU_UNIFIED_MEMORY) && !defined(WF_GPU_PINNED_MEMORY)
        #include<keyby_emitter_gpu.hpp> // version with CUDA explicit memory transfers
        #include<forward_emitter_gpu.hpp> // version with CUDA explicit memory transfers
    #else
        #include<keyby_emitter_gpu_u.hpp> // version with CUDA unified memory support
        #include<forward_emitter_gpu_u.hpp> // version with CUDA unified memory support
    #endif
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
    friend class PipeGraph; // friendship with the PipeGraph class
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
        std::vector<ff::ff_node *> result;
        for (auto *r: _operator.replicas) {
            ff::ff_pipeline *stage = new ff::ff_pipeline();
            stage->add_stage(r, false);
            if (_operator.getType() == "Parallel_Windows_WLQ" || _operator.getType() == "Parallel_Windows_REDUCE") { // special cases
                auto *collector = new Ordering_Collector<decltype(_operator.getKeyExtractor())>(_operator.getKeyExtractor(), ordering_mode_t::ID, execution_mode, id++);
                combine_with_firststage(*stage, collector, true); // combine with the Ordering_Collector
            }
            else if (execution_mode == Execution_Mode_t::DETERMINISTIC) {
                auto *collector = new Ordering_Collector<decltype(_operator.getKeyExtractor())>(_operator.getKeyExtractor(), _ordering_mode, execution_mode, id++);
                combine_with_firststage(*stage, collector, true); // combine with the Ordering_Collector
            }
            else if (execution_mode == Execution_Mode_t::PROBABILISTIC) {
                auto *collector = new KSlack_Collector<decltype(_operator.getKeyExtractor())>(_operator.getKeyExtractor(), _ordering_mode, id++);
                combine_with_firststage(*stage, collector, true); // combine with the Kslack_Collector
            }
            else if (execution_mode == Execution_Mode_t::DEFAULT) {
                r->receiveBatches(_needBatching);
                auto *collector = new Watermark_Collector<decltype(_operator.getKeyExtractor())>(_operator.getKeyExtractor(), _ordering_mode, id++, _needBatching);
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
    template<typename key_extractor_func_t, bool isDestGPUType>
    Basic_Emitter *create_emitter(typename std::enable_if<!isDestGPUType, key_extractor_func_t>::type _key_extr,
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
        else if (_routing_mode == Routing_Mode_t::KEYBY) { // KB
            return new KeyBy_Emitter<decltype(_key_extr)>(_key_extr, _num_dests, execution_mode, _outputBatchSize);
        }
        else { // BD
            return new Broadcast_Emitter<decltype(_key_extr)>(_key_extr, _num_dests, _outputBatchSize);
        }
    }

#else
    // Create the right emitter to be used to connect to the next operator
    template<typename key_extractor_func_t, bool isDestGPUType>
    Basic_Emitter *create_emitter(typename std::enable_if<!isDestGPUType, key_extractor_func_t>::type _key_extr,
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
            else if (_routing_mode == Routing_Mode_t::KEYBY) { // KB
                return new KeyBy_Emitter<decltype(_key_extr)>(_key_extr, _num_dests, execution_mode, _outputBatchSize);
            }
            else { // BD
                return new Broadcast_Emitter<decltype(_key_extr)>(_key_extr, _num_dests, _outputBatchSize);
            }
        }
        else if (isSourceGPU && !isDestGPU) { // GPU -> CPU case
            if (_routing_mode == Routing_Mode_t::FORWARD) { // FW
                return new Forward_Emitter_GPU<decltype(_key_extr), true, false>(_key_extr, _num_dests);
            }
            else if (_routing_mode == Routing_Mode_t::KEYBY) { // KB
                return new KeyBy_Emitter_GPU<decltype(_key_extr), true, false>(_key_extr, _num_dests);
            }
            else { // BD
                return new Broadcast_Emitter_GPU<decltype(_key_extr), true, false>(_key_extr, _num_dests);
            }
        }
        else {
            return nullptr;
        }
    }

    // Create the right emitter to be used to connect to the next operator
    template<typename key_extractor_func_t, bool isDestGPUType>
    Basic_Emitter *create_emitter(typename std::enable_if<isDestGPUType, key_extractor_func_t>::type _key_extr,
                                  Routing_Mode_t _routing_mode,
                                  size_t _num_dests,
                                  size_t _outputBatchSize,
                                  bool isSourceGPU,
                                  bool isDestGPU) const
    {
        assert(isDestGPU); // sanity chekc
        if (isSourceGPU && isDestGPU) { // GPU -> GPU case
            if (_routing_mode == Routing_Mode_t::FORWARD) { // FW
                return new Forward_Emitter_GPU<decltype(_key_extr), true, true>(_key_extr, _num_dests);
            }
            else { // KB
                return new KeyBy_Emitter_GPU<decltype(_key_extr), true, true>(_key_extr, _num_dests);
            }
        }
        else { // CPU -> GPU case
            if (_routing_mode == Routing_Mode_t::FORWARD) { // FW
                return new Forward_Emitter_GPU<decltype(_key_extr), false, true>(_key_extr, _num_dests, _outputBatchSize);
            }
            else { // KB
                return new KeyBy_Emitter_GPU<decltype(_key_extr), false, true>(_key_extr, _num_dests, _outputBatchSize);
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
        if (_source.getOutputBatchSize() > 0 && execution_mode != Execution_Mode_t::DEFAULT) {
            std::cerr << RED << "WindFlow Error: Source cannot produce a batch in non DEFAULT mode" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
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
        if (copied_source->getType() == "Kafka_Source") {
            gv_add_vertex("Kafka_Source (" + std::to_string((copied_source->replicas).size()) + ")", copied_source->getName(), true, false, Routing_Mode_t::NONE);
        }
        else {
            gv_add_vertex("Source (" + std::to_string((copied_source->replicas).size()) + ")", copied_source->getName(), true, false, Routing_Mode_t::NONE);
        }
#endif
        return *this;
    }

    // Add an operator to the MultiPipe
    template<typename operator_t, bool isDestGPUType=false>
    void add_operator(operator_t &_operator,
                      ordering_mode_t _ordering_mode)
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
    bool chain_operator(operator_t &_operator,
                        ordering_mode_t _ordering_mode)
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
    std::vector<MultiPipe *> prepareMergeSet(MULTIPIPE &_first,
                                             MULTIPIPES&... _pipes)
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
        }
        else if (isCPU_OP) // style of CPU operators
        {
            agset(node, const_cast<char *>("color"), const_cast<char *>("#941100"));
            agset(node, const_cast<char *>("fillcolor"), const_cast<char *>("#ff9400"));
        }
        else { // style of GPU operators
            agset(node, const_cast<char *>("color"), const_cast<char *>("#20548E"));
            agset(node, const_cast<char *>("fillcolor"), const_cast<char *>("#469EA2"));
        }
        if (isNested) { // change shape of the node to represent a nested operator
            agset(node, const_cast<char *>("shape"), const_cast<char *>("doubleoctagon"));
        }
        for (auto *vertex: this->gv_last_vertices) { // connect the new vertex to the previous one(s)
            Agedge_t *e = agedge(gv_graph, vertex, node, 0, 1);
            // set the label of the edge
            if (routing_type == Routing_Mode_t::FORWARD) {
                agset(e, const_cast<char *>("label"), const_cast<char *>("FW"));
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
    void gv_chain_vertex(std::string typeOP,
                         std::string nameOP)
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
        if (typeOP.compare(0, 4, "Sink") == 0) { // style for Sink operator
            agset(node, const_cast<char *>("color"), const_cast<char *>("#B8B7B8"));
            agset(node, const_cast<char *>("fillcolor"), const_cast<char *>("black"));
        }
        else if (typeOP.compare(0, 7, "Map_GPU") == 0) { // style for Map_GPU operator
            agset(node, const_cast<char *>("color"), const_cast<char *>("#00FF80"));
            agset(node, const_cast<char *>("fillcolor"), const_cast<char *>("#78A446"));            
        }
        else if (typeOP.compare(0, 7, "Filter_GPU") == 0) { // style for Filter_GPU operator
            agset(node, const_cast<char *>("color"), const_cast<char *>("#00FF80"));
            agset(node, const_cast<char *>("fillcolor"), const_cast<char *>("#78A446"));            
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

public:
    /** 
     *  \brief Add a Filter operator to the MultiPipe
     *  \param _filter the Filter operator to be added
     *  \return a reference to the modified MultiPipe
     */ 
    template<typename filter_func_t, typename key_extractor_func_t>
    MultiPipe &add(const Filter<filter_func_t, key_extractor_func_t> &_filter)
    {
        if (_filter.getOutputBatchSize() > 0 && execution_mode != Execution_Mode_t::DEFAULT) {
            std::cerr << RED << "WindFlow Error: Filter cannot produce a batch in non DEFAULT mode" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);         
        }
        auto *copied_filter = new Filter(_filter); // create a copy of the operator
        copied_filter->setExecutionMode(execution_mode); // set the execution mode of the operator
        using tuple_t = decltype(get_tuple_t_Filter(copied_filter->func)); // extracting the tuple_t type and checking the admissible signatures
        using result_t = decltype(get_result_t_Filter(copied_filter->func)); // extracting the result_t type and checking the admissible signatures
        std::string opInType = TypeName<tuple_t>::getName(); // save the type of tuple_t as a string
        if (!outputType.empty() && outputType.compare(opInType) != 0) {
            std::cerr << RED << "WindFlow Error: output type from MultiPipe is not the input type of the Filter operator" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        outputType = TypeName<result_t>::getName(); // save the new output type from this MultiPipe
        add_operator(*copied_filter, ordering_mode_t::TS);
#if defined (WF_TRACING_ENABLED)
        gv_add_vertex("Filter (" + std::to_string(copied_filter->getParallelism()) + ")", copied_filter->getName(), true, false, copied_filter->getInputRoutingMode());
#endif
        localOpList.push_back(copied_filter); // add the copied operator to local list
        globalOpList->push_back(copied_filter); // add the copied operator to global list
        return *this;
    }

    /** 
     *  \brief Try to chain a Filter operator to the MultiPipe (if not possible, the operator is added)
     *  \param _filter the Filter operator to be chained
     *  \return a reference to the modified MultiPipe
     */ 
    template<typename filter_func_t, typename key_extractor_func_t>
    MultiPipe &chain(const Filter<filter_func_t, key_extractor_func_t> &_filter)
    {
        if (_filter.getOutputBatchSize() > 0 && execution_mode != Execution_Mode_t::DEFAULT) {
            std::cerr << RED << "WindFlow Error: Filter cannot produce a batch in non DEFAULT mode" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);         
        }
        auto *copied_filter = new Filter(_filter); // create a copy of the operator
        copied_filter->setExecutionMode(execution_mode); // set the execution mode of the operator
        using tuple_t = decltype(get_tuple_t_Filter(copied_filter->func)); // extracting the tuple_t type and checking the admissible signatures
        using result_t = decltype(get_result_t_Filter(copied_filter->func)); // extracting the result_t type and checking the admissible signatures
        std::string opInType = TypeName<tuple_t>::getName(); // save the type of tuple_t as a string
        if (!outputType.empty() && outputType.compare(opInType) != 0) {
            std::cerr << RED << "WindFlow Error: output type from MultiPipe is not the input type of the Filter operator" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        outputType = TypeName<result_t>::getName(); // save the new output type from this MultiPipe
        bool isChained = chain_operator(*copied_filter, ordering_mode_t::TS); // try to chain the operator (otherwise, it is added)
        if (isChained) {
#if defined (WF_TRACING_ENABLED)
            gv_chain_vertex("Filter (" + std::to_string(copied_filter->getParallelism()) + ")", copied_filter->getName());
#endif
        }
        else {
#if defined (WF_TRACING_ENABLED)
            gv_add_vertex("Filter (" + std::to_string(copied_filter->getParallelism()) + ")", copied_filter->getName(), true, false, copied_filter->getInputRoutingMode());
#endif
        }
        localOpList.push_back(copied_filter); // add the copied operator to local list
        globalOpList->push_back(copied_filter); // add the copied operator to global list
        return *this;
    }

    /** 
     *  \brief Add a Map operator to the MultiPipe
     *  \param _map the Map operator to be added
     *  \return a reference to the modified MultiPipe
     */ 
    template<typename map_func_t, typename key_extractor_func_t>
    MultiPipe &add(const Map<map_func_t, key_extractor_func_t> &_map)
    {
        if (_map.getOutputBatchSize() > 0 && execution_mode != Execution_Mode_t::DEFAULT) {
            std::cerr << RED << "WindFlow Error: Map cannot produce a batch in non DEFAULT mode" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);         
        }
        auto *copied_map = new Map(_map); // create a copy of the operator
        copied_map->setExecutionMode(execution_mode); // set the execution mode of the operator
        using tuple_t = decltype(get_tuple_t_Map(copied_map->func)); // extracting the tuple_t type and checking the admissible signatures
        using result_t = decltype(get_result_t_Map(copied_map->func)); // extracting the result_t type and checking the admissible signatures
        std::string opInType = TypeName<tuple_t>::getName(); // save the type of tuple_t as a string
        if (!outputType.empty() && outputType.compare(opInType) != 0) {
            std::cerr << RED << "WindFlow Error: output type from MultiPipe is not the input type of the Map operator" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        outputType = TypeName<result_t>::getName(); // save the new output type from this MultiPipe
        add_operator(*copied_map, ordering_mode_t::TS);
#if defined (WF_TRACING_ENABLED)
        gv_add_vertex("Map (" + std::to_string(copied_map->getParallelism()) + ")", copied_map->getName(), true, false, copied_map->getInputRoutingMode());
#endif
        localOpList.push_back(copied_map); // add the copied operator to local list
        globalOpList->push_back(copied_map); // add the copied operator to global list
        return *this;
    }

    /** 
     *  \brief Try to chain a Map operator to the MultiPipe (if not possible, the operator is added)
     *  \param _map the Map operator to be chained
     *  \return a reference to the modified MultiPipe
     */ 
    template<typename map_func_t, typename key_extractor_func_t>
    MultiPipe &chain(const Map<map_func_t, key_extractor_func_t> &_map)
    {
        if (_map.getOutputBatchSize() > 0 && execution_mode != Execution_Mode_t::DEFAULT) {
            std::cerr << RED << "WindFlow Error: Map cannot produce a batch in non DEFAULT mode" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);         
        }
        auto *copied_map = new Map(_map); // create a copy of the operator
        copied_map->setExecutionMode(execution_mode); // set the execution mode of the operator
        using tuple_t = decltype(get_tuple_t_Map(copied_map->func)); // extracting the tuple_t type and checking the admissible signatures
        using result_t = decltype(get_result_t_Map(copied_map->func)); // extracting the result_t type and checking the admissible signatures
        std::string opInType = TypeName<tuple_t>::getName(); // save the type of tuple_t as a string
        if (!outputType.empty() && outputType.compare(opInType) != 0) {
            std::cerr << RED << "WindFlow Error: output type from MultiPipe is not the input type of the Map operator" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        outputType = TypeName<result_t>::getName(); // save the new output type from this MultiPipe
        bool isChained = chain_operator(*copied_map, ordering_mode_t::TS); // try to chain the operator (otherwise, it is added)
        if (isChained) {
#if defined (WF_TRACING_ENABLED)
            gv_chain_vertex("Map (" + std::to_string(copied_map->getParallelism()) + ")", copied_map->getName());
#endif
        }
        else {
#if defined (WF_TRACING_ENABLED)
            gv_add_vertex("Map (" + std::to_string(copied_map->getParallelism()) + ")", copied_map->getName(), true, false, copied_map->getInputRoutingMode());
#endif
        }
        localOpList.push_back(copied_map); // add the copied operator to local list
        globalOpList->push_back(copied_map); // add the copied operator to global list
        return *this;
    }

    /** 
     *  \brief Add a FlatMap operator to the MultiPipe
     *  \param _flatmap the FlatMap operator to be added
     *  \return a reference to the modified MultiPipe
     */ 
    template<typename flatmap_func_t, typename key_extractor_func_t>
    MultiPipe &add(const FlatMap<flatmap_func_t, key_extractor_func_t> &_flatmap)
    {
        if (_flatmap.getOutputBatchSize() > 0 && execution_mode != Execution_Mode_t::DEFAULT) {
            std::cerr << RED << "WindFlow Error: FlatMap cannot produce a batch in non DEFAULT mode" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);         
        }
        auto *copied_flatmap = new FlatMap(_flatmap); // create a copy of the operator
        copied_flatmap->setExecutionMode(execution_mode); // set the execution mode of the operator
        using tuple_t = decltype(get_tuple_t_FlatMap(copied_flatmap->func)); // extracting the tuple_t type and checking the admissible signatures
        using result_t = decltype(get_result_t_FlatMap(copied_flatmap->func)); // extracting the result_t type and checking the admissible signatures
        std::string opInType = TypeName<tuple_t>::getName(); // save the type of tuple_t as a string
        if (!outputType.empty() && outputType.compare(opInType) != 0) {
            std::cerr << RED << "WindFlow Error: output type from MultiPipe is not the input type of the FlatMap operator" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        outputType = TypeName<result_t>::getName(); // save the new output type from this MultiPipe
        add_operator(*copied_flatmap, ordering_mode_t::TS);
#if defined (WF_TRACING_ENABLED)
        gv_add_vertex("FlatMap (" + std::to_string(copied_flatmap->getParallelism()) + ")", copied_flatmap->getName(), true, false, copied_flatmap->getInputRoutingMode());
#endif
        localOpList.push_back(copied_flatmap); // add the copied operator to local list
        globalOpList->push_back(copied_flatmap); // add the copied operator to global list
        return *this;
    }

    /** 
     *  \brief Try to chain a FlatMap operator to the MultiPipe (if not possible, the operator is added)
     *  \param _flatmap the FlatMap operator to be chained
     *  \return a reference to the modified MultiPipe
     */ 
    template<typename flatmap_func_t, typename key_extractor_func_t>
    MultiPipe &chain(const FlatMap<flatmap_func_t, key_extractor_func_t> &_flatmap)
    {
        if (_flatmap.getOutputBatchSize() > 0 && execution_mode != Execution_Mode_t::DEFAULT) {
            std::cerr << RED << "WindFlow Error: FlatMap cannot produce a batch in non DEFAULT mode" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);         
        }
        auto *copied_flatmap = new FlatMap(_flatmap); // create a copy of the operator
        copied_flatmap->setExecutionMode(execution_mode); // set the execution mode of the operator
        using tuple_t = decltype(get_tuple_t_FlatMap(copied_flatmap->func)); // extracting the tuple_t type and checking the admissible signatures
        using result_t = decltype(get_result_t_FlatMap(copied_flatmap->func)); // extracting the result_t type and checking the admissible signatures
        std::string opInType = TypeName<tuple_t>::getName(); // save the type of tuple_t as a string
        if (!outputType.empty() && outputType.compare(opInType) != 0) {
            std::cerr << RED << "WindFlow Error: output type from MultiPipe is not the input type of the FlatMap operator" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        outputType = TypeName<result_t>::getName(); // save the new output type from this MultiPipe
        bool isChained = chain_operator(*copied_flatmap, ordering_mode_t::TS); // try to chain the operator (otherwise, it is added)
        if (isChained) {
#if defined (WF_TRACING_ENABLED)
            gv_chain_vertex("FlatMap (" + std::to_string(copied_flatmap->getParallelism()) + ")", copied_flatmap->getName());
#endif
        }
        else {
#if defined (WF_TRACING_ENABLED)
            gv_add_vertex("FlatMap (" + std::to_string(copied_flatmap->getParallelism()) + ")", copied_flatmap->getName(), true, false, copied_flatmap->getInputRoutingMode());
#endif
        }
        localOpList.push_back(copied_flatmap); // add the copied operator to local list
        globalOpList->push_back(copied_flatmap); // add the copied operator to global list
        return *this;
    }

    /** 
     *  \brief Add a Reduce operator to the MultiPipe
     *  \param _reduce the Reduce operator to be added
     *  \return a reference to the modified MultiPipe
     */ 
    template<typename reduce_func_t, typename key_extractor_func_t>
    MultiPipe &add(const Reduce<reduce_func_t, key_extractor_func_t> &_reduce)
    {
        if (_reduce.getOutputBatchSize() > 0 && execution_mode != Execution_Mode_t::DEFAULT) {
            std::cerr << RED << "WindFlow Error: Reduce cannot produce a batch in non DEFAULT mode" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);         
        }
        auto *copied_reduce = new Reduce(_reduce); // create a copy of the operator
        copied_reduce->setExecutionMode(execution_mode); // set the execution mode of the operator
        using tuple_t = decltype(get_tuple_t_Reduce(copied_reduce->func)); // extracting the tuple_t type and checking the admissible signatures
        using state_t = decltype(get_state_t_Reduce(copied_reduce->func)); // extracting the state_t type and checking the admissible signatures
        std::string opInType = TypeName<tuple_t>::getName(); // save the type of tuple_t as a string
        if (!outputType.empty() && outputType.compare(opInType) != 0) {
            std::cerr << RED << "WindFlow Error: output type from MultiPipe is not the input type of the Reduce operator" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        outputType = TypeName<state_t>::getName(); // save the new output type from this MultiPipe
        add_operator(*copied_reduce, ordering_mode_t::TS);
#if defined (WF_TRACING_ENABLED)
        gv_add_vertex("Reduce (" + std::to_string(copied_reduce->getParallelism()) + ")", copied_reduce->getName(), true, false, copied_reduce->getInputRoutingMode());
#endif
        localOpList.push_back(copied_reduce); // add the copied operator to local list
        globalOpList->push_back(copied_reduce); // add the copied operator to global list
        return *this;
    }

    /** 
     *  \brief Add a Keyed_Windows operator to the MultiPipe
     *  \param _kwins the Keyed_Windows operator to be added
     *  \return a reference to the modified MultiPipe
     */ 
    template<typename win_func_t, typename key_extractor_func_t>
    MultiPipe &add(const Keyed_Windows<win_func_t, key_extractor_func_t> &_kwins)
    {
        if (_kwins.getOutputBatchSize() > 0 && execution_mode != Execution_Mode_t::DEFAULT) {
            std::cerr << RED << "WindFlow Error: Keyed_Windows cannot produce a batch in non DEFAULT mode" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);         
        }
        auto *copied_kwins = new Keyed_Windows(_kwins); // create a copy of the operator
        copied_kwins->setExecutionMode(execution_mode); // set the execution mode of the operator
        using tuple_t = decltype(get_tuple_t_Win(copied_kwins->func)); // extracting the tuple_t type and checking the admissible signatures
        using result_t = decltype(get_result_t_Win(copied_kwins->func)); // extracting the result_t type and checking the admissible signatures
        std::string opInType = TypeName<tuple_t>::getName(); // save the type of tuple_t as a string
        if (!outputType.empty() && outputType.compare(opInType) != 0) {
            std::cerr << RED << "WindFlow Error: output type from MultiPipe is not the input type of the Keyed_Windows operator" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        outputType = TypeName<result_t>::getName(); // save the new output type from this MultiPipe
        add_operator(*copied_kwins, ordering_mode_t::TS);
#if defined (WF_TRACING_ENABLED)
        gv_add_vertex("Keyed_Windows (" + std::to_string(copied_kwins->getParallelism()) + ")", copied_kwins->getName(), true, false, copied_kwins->getInputRoutingMode());
#endif
        localOpList.push_back(copied_kwins); // add the copied operator to local list
        globalOpList->push_back(copied_kwins); // add the copied operator to global list
        return *this;
    }

    /** 
     *  \brief Add a Parallel_Windows operator to the MultiPipe
     *  \param _pwins the Parallel_Windows operator to be added
     *  \return a reference to the modified MultiPipe
     */ 
    template<typename win_func_t, typename key_extractor_func_t>
    MultiPipe &add(const Parallel_Windows<win_func_t, key_extractor_func_t> &_pwins)
    {
        if (_pwins.getOutputBatchSize() > 0 && execution_mode != Execution_Mode_t::DEFAULT) {
            std::cerr << RED << "WindFlow Error: Parallel_Windows cannot produce a batch in non DEFAULT mode" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);         
        }
        // count-based windows with Parallel_Windows cannot be used in DEFAULT mode
        if (_pwins.getWinType() == Win_Type_t::CB && execution_mode == Execution_Mode_t::DEFAULT) {
            std::cerr << RED << "WindFlow Error: Parallel_Windows cannot use count-based windows in DEFAULT mode" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        auto *copied_pwins = new Parallel_Windows(_pwins); // create a copy of the operator
        copied_pwins->setExecutionMode(execution_mode); // set the execution mode of the operator
        using tuple_t = decltype(get_tuple_t_Win(copied_pwins->func)); // extracting the tuple_t type and checking the admissible signatures
        using result_t = decltype(get_result_t_Win(copied_pwins->func)); // extracting the result_t type and checking the admissible signatures
        std::string opInType = TypeName<tuple_t>::getName(); // save the type of tuple_t as a string
        if (!outputType.empty() && outputType.compare(opInType) != 0) {
            std::cerr << RED << "WindFlow Error: output type from MultiPipe is not the input type of the Parallel_Windows operator" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        outputType = TypeName<result_t>::getName(); // save the new output type from this MultiPipe
        add_operator(*copied_pwins, ordering_mode_t::TS);
#if defined (WF_TRACING_ENABLED)
        gv_add_vertex("Parallel_Windows (" + std::to_string(copied_pwins->getParallelism()) + ")", copied_pwins->getName(), true, false, copied_pwins->getInputRoutingMode());
#endif
        localOpList.push_back(copied_pwins); // add the copied operator to local list
        globalOpList->push_back(copied_pwins); // add the copied operator to global list
        return *this;
    }

    /** 
     *  \brief Add a Paned_Windows operator to the MultiPipe
     *  \param _pan_wins the Paned_Windows operator to be added
     *  \return a reference to the modified MultiPipe
     */ 
    template<typename plq_func_t, typename wlq_func_t, typename key_extractor_func_t>
    MultiPipe &add(const Paned_Windows<plq_func_t, wlq_func_t, key_extractor_func_t> &_pan_wins)
    {
        if (_pan_wins.getOutputBatchSize() > 0 && execution_mode != Execution_Mode_t::DEFAULT) {
            std::cerr << RED << "WindFlow Error: Paned_Windows cannot produce a batch in non DEFAULT mode" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);         
        }
        // count-based windows with Paned_Windows cannot be used in DEFAULT mode
        if (_pan_wins.getWinType() == Win_Type_t::CB && execution_mode == Execution_Mode_t::DEFAULT) {
            std::cerr << RED << "WindFlow Error: Paned_Windows cannot use count-based windows in DEFAULT mode" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        using tuple_t = decltype(get_tuple_t_Win(_pan_wins.plq_func)); // extracting the tuple_t type and checking the admissible signatures
        using result_t = decltype(get_result_t_Win(_pan_wins.wlq_func)); // extracting the result_t type and checking the admissible signatures
        std::string opInType = TypeName<tuple_t>::getName(); // save the type of tuple_t as a string
        if (!outputType.empty() && outputType.compare(opInType) != 0) {
            std::cerr << RED << "WindFlow Error: output type from MultiPipe is not the input type of the Paned_Windows operator" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        outputType = TypeName<result_t>::getName(); // save the new output type from this MultiPipe
        // add the first sub-operator (PLQ)
        auto *plq = new Parallel_Windows<plq_func_t, key_extractor_func_t>(_pan_wins.plq);
        plq->setExecutionMode(execution_mode); // set the execution mode of the operator
        add_operator(*plq, ordering_mode_t::TS);
        localOpList.push_back(plq); // add the PLQ sub-operator to local list
        globalOpList->push_back(plq); // add the PLQ sub-operator to global list 
        // add the second sub-operator (WLQ)
        auto *wlq = new Parallel_Windows<wlq_func_t, key_extractor_func_t>(_pan_wins.wlq);
        wlq->setExecutionMode(execution_mode); // set the execution mode of the operator
        add_operator(*wlq, ordering_mode_t::ID);
        localOpList.push_back(wlq); // add the WLQ sub-operator to local list
        globalOpList->push_back(wlq); // add the WLQ sub-operator to global list
#if defined (WF_TRACING_ENABLED)
        gv_add_vertex("Paned_Windows (" + std::to_string(_pan_wins.plq_parallelism) + "," + std::to_string(_pan_wins.wlq_parallelism) + ")", _pan_wins.getName(), false, false, _pan_wins.getInputRoutingMode());
#endif
        return *this;
    }

    /** 
     *  \brief Add a MapReduce_Windows operator to the MultiPipe
     *  \param _mr_wins the MapReduce_Windows operator to be added
     *  \return a reference to the modified MultiPipe
     */ 
    template<typename map_func_t, typename reduce_func_t, typename key_extractor_func_t>
    MultiPipe &add(const MapReduce_Windows<map_func_t, reduce_func_t, key_extractor_func_t> &_mr_wins)
    {
        if (_mr_wins.getOutputBatchSize() > 0 && execution_mode != Execution_Mode_t::DEFAULT) {
            std::cerr << RED << "WindFlow Error: MapReduce_Windows cannot produce a batch in non DEFAULT mode" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);         
        }
        // count-based windows with MapReduce_Windows cannot be used in DEFAULT mode
        if (_mr_wins.getWinType() == Win_Type_t::CB && execution_mode == Execution_Mode_t::DEFAULT) {
            std::cerr << RED << "WindFlow Error: MapReduce_Windows cannot use count-based windows in DEFAULT mode" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        using tuple_t = decltype(get_tuple_t_Win(_mr_wins.map_func)); // extracting the tuple_t type and checking the admissible signatures
        using result_t = decltype(get_result_t_Win(_mr_wins.reduce_func)); // extracting the result_t type and checking the admissible signatures
        std::string opInType = TypeName<tuple_t>::getName(); // save the type of tuple_t as a string
        if (!outputType.empty() && outputType.compare(opInType) != 0) {
            std::cerr << RED << "WindFlow Error: output type from MultiPipe is not the input type of the MapReduce_Windows operator" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        outputType = TypeName<result_t>::getName(); // save the new output type from this MultiPipe
        // add the first sub-operator (MAP)
        auto *map = new Parallel_Windows<map_func_t, key_extractor_func_t>(_mr_wins.map);
        map->setExecutionMode(execution_mode); // set the execution mode of the operator
        add_operator(*map, ordering_mode_t::TS);
        localOpList.push_back(map); // add the MAP sub-operator to local list
        globalOpList->push_back(map); // add the MAP sub-operator to global list 
        // add the second sub-operator (REDUCE)
        auto *reduce = new Parallel_Windows<reduce_func_t, key_extractor_func_t>(_mr_wins.reduce);
        reduce->setExecutionMode(execution_mode); // set the execution mode of the operator
        add_operator(*reduce, ordering_mode_t::ID);
        localOpList.push_back(reduce); // add the WLQ sub-operator to local list
        globalOpList->push_back(reduce); // add the WLQ sub-operator to global list
#if defined (WF_TRACING_ENABLED)
        gv_add_vertex("MapReduce_Windows (" + std::to_string(_mr_wins.map_parallelism) + "," + std::to_string(_mr_wins.reduce_parallelism) + ")", _mr_wins.getName(), false, false, _mr_wins.getInputRoutingMode());
#endif
        return *this;
    }

    /** 
     *  \brief Add a FFAT_Aggregator operator to the MultiPipe
     *  \param _ffatagg the FFAT_Aggregator operator to be added
     *  \return a reference to the modified MultiPipe
     */ 
    template<typename lift_func_t, typename comb_func_t, typename key_extractor_func_t>
    MultiPipe &add(const FFAT_Aggregator<lift_func_t, comb_func_t, key_extractor_func_t> &_ffatagg)
    {
        if (_ffatagg.getOutputBatchSize() > 0 && execution_mode != Execution_Mode_t::DEFAULT) {
            std::cerr << RED << "WindFlow Error: FFAT_Aggregator cannot produce a batch in non DEFAULT mode" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);         
        }
        auto *copied_ffatagg = new FFAT_Aggregator(_ffatagg); // create a copy of the operator
        copied_ffatagg->setExecutionMode(execution_mode); // set the execution mode of the operator
        using tuple_t = decltype(get_tuple_t_Lift(copied_ffatagg->lift_func)); // extracting the tuple_t type and checking the admissible signatures
        using result_t = decltype(get_result_t_Lift(copied_ffatagg->lift_func)); // extracting the result_t type and checking the admissible signatures
        std::string opInType = TypeName<tuple_t>::getName(); // save the type of tuple_t as a string
        if (!outputType.empty() && outputType.compare(opInType) != 0) {
            std::cerr << RED << "WindFlow Error: output type from MultiPipe is not the input type of the FFAT_Aggregator operator" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        outputType = TypeName<result_t>::getName(); // save the new output type from this MultiPipe
        add_operator(*copied_ffatagg, ordering_mode_t::TS);
#if defined (WF_TRACING_ENABLED)
        gv_add_vertex("FFAT_Aggregator (" + std::to_string(copied_ffatagg->getParallelism()) + ")", copied_ffatagg->getName(), true, false, copied_ffatagg->getInputRoutingMode());
#endif
        localOpList.push_back(copied_ffatagg); // add the copied operator to local list
        globalOpList->push_back(copied_ffatagg); // add the copied operator to global list
        return *this;
    }

#if defined (__CUDACC__)
    /** 
     *  \brief Add a Map_GPU operator to the MultiPipe
     *  \param _mapgpu the Map_GPU operator to be added
     *  \return a reference to the modified MultiPipe
     */ 
    template<typename mapgpu_func_t, typename key_extractor_func_t>
    MultiPipe &add(const Map_GPU<mapgpu_func_t, key_extractor_func_t> &_mapgpu)
    {
        if (execution_mode != Execution_Mode_t::DEFAULT) {
            std::cerr << RED << "WindFlow Error: Map_GPU can be used in DEFAULT mode only" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        auto *copied_mapgpu = new Map_GPU(_mapgpu); // create a copy of the operator
        using tuple_t = decltype(get_tuple_t_MapGPU(copied_mapgpu->func)); // extracting the tuple_t type and checking the admissible signatures
        std::string opInType = TypeName<tuple_t>::getName(); // save the type of tuple_t as a string
        if (!outputType.empty() && outputType.compare(opInType) != 0) {
            std::cerr << RED << "WindFlow Error: output type from MultiPipe is not the input type of the Map_GPU operator" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        add_operator<decltype(*copied_mapgpu), true>(*copied_mapgpu, ordering_mode_t::TS);
#if defined (WF_TRACING_ENABLED)
        gv_add_vertex("Map_GPU (" + std::to_string(copied_mapgpu->getParallelism()) + ")", copied_mapgpu->getName(), false, false, copied_mapgpu->getInputRoutingMode());
#endif
        localOpList.push_back(copied_mapgpu); // add the copied operator to local list
        globalOpList->push_back(copied_mapgpu); // add the copied operator to global list
        return *this;
    }

    /** 
     *  \brief Try to chain a Map_GPU operator to the MultiPipe (if not possible, the operator is added)
     *  \param _mapgpu the Map_GPU operator to be chained
     *  \return a reference to the modified MultiPipe
     */ 
    template<typename mapgpu_func_t, typename key_extractor_func_t>
    MultiPipe &chain(const Map_GPU<mapgpu_func_t, key_extractor_func_t> &_mapgpu)
    {
        if (execution_mode != Execution_Mode_t::DEFAULT) {
            std::cerr << RED << "WindFlow Error: Map_GPU can be used in DEFAULT mode only" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        auto *copied_mapgpu = new Map_GPU(_mapgpu); // create a copy of the operator
        using tuple_t = decltype(get_tuple_t_MapGPU(copied_mapgpu->func)); // extracting the tuple_t type and checking the admissible signatures
        std::string opInType = TypeName<tuple_t>::getName(); // save the type of tuple_t as a string
        if (!outputType.empty() && outputType.compare(opInType) != 0) {
            std::cerr << RED << "WindFlow Error: output type from MultiPipe is not the input type of the Map_GPU operator" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        bool isChained = chain_operator<decltype(*copied_mapgpu), true>(*copied_mapgpu, ordering_mode_t::TS); // try to chain the operator (otherwise, it is added)
        if (isChained) {
#if defined (WF_TRACING_ENABLED)
            gv_chain_vertex("Map_GPU (" + std::to_string(copied_mapgpu->getParallelism()) + ")", copied_mapgpu->getName());
#endif
        }
        else {
#if defined (WF_TRACING_ENABLED)
            gv_add_vertex("Map_GPU (" + std::to_string(copied_mapgpu->getParallelism()) + ")", copied_mapgpu->getName(), false, false, copied_mapgpu->getInputRoutingMode());
#endif
        }
        localOpList.push_back(copied_mapgpu); // add the copied operator to local list
        globalOpList->push_back(copied_mapgpu); // add the copied operator to global list
        return *this;
    }

    /** 
     *  \brief Add a Filter_GPU operator to the MultiPipe
     *  \param _filtergpu the Filter_GPU operator to be added
     *  \return a reference to the modified MultiPipe
     */ 
    template<typename filtergpu_func_t, typename key_extractor_func_t>
    MultiPipe &add(const Filter_GPU<filtergpu_func_t, key_extractor_func_t> &_filtergpu)
    {
        if (execution_mode != Execution_Mode_t::DEFAULT) {
            std::cerr << RED << "WindFlow Error: Filter_GPU can be used in DEFAULT mode only" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        auto *copied_filtergpu = new Filter_GPU(_filtergpu); // create a copy of the operator
        copied_filtergpu->setExecutionMode(execution_mode); // set the execution mode of the operator
        using tuple_t = decltype(get_tuple_t_FilterGPU(copied_filtergpu->func)); // extracting the tuple_t type and checking the admissible signatures
        std::string opInType = TypeName<tuple_t>::getName(); // save the type of tuple_t as a string
        if (!outputType.empty() && outputType.compare(opInType) != 0) {
            std::cerr << RED << "WindFlow Error: output type from MultiPipe is not the input type of the Filter_GPU operator" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        add_operator<decltype(*copied_filtergpu), true>(*copied_filtergpu, ordering_mode_t::TS);
#if defined (WF_TRACING_ENABLED)
        gv_add_vertex("Filter_GPU (" + std::to_string(copied_filtergpu->getParallelism()) + ")", copied_filtergpu->getName(), false, false, copied_filtergpu->getInputRoutingMode());
#endif
        localOpList.push_back(copied_filtergpu); // add the copied operator to local list
        globalOpList->push_back(copied_filtergpu); // add the copied operator to global list
        return *this;
    }

    /** 
     *  \brief Try to chain a Filter_GPU operator to the MultiPipe (if not possible, the operator is added)
     *  \param _filtergpu the Filter_GPU operator to be chained
     *  \return a reference to the modified MultiPipe
     */ 
    template<typename filtergpu_func_t, typename key_extractor_func_t>
    MultiPipe &chain(const Filter_GPU<filtergpu_func_t, key_extractor_func_t> &_filtergpu)
    {
        if (execution_mode != Execution_Mode_t::DEFAULT) {
            std::cerr << RED << "WindFlow Error: Filter_GPU can be used in DEFAULT mode only" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        auto *copied_filtergpu = new Filter_GPU(_filtergpu); // create a copy of the operator
        copied_filtergpu->setExecutionMode(execution_mode); // set the execution mode of the operator
        using tuple_t = decltype(get_tuple_t_FilterGPU(copied_filtergpu->func)); // extracting the tuple_t type and checking the admissible signatures
        std::string opInType = TypeName<tuple_t>::getName(); // save the type of tuple_t as a string
        if (!outputType.empty() && outputType.compare(opInType) != 0) {
            std::cerr << RED << "WindFlow Error: output type from MultiPipe is not the input type of the Filter_GPU operator" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        bool isChained = chain_operator<decltype(*copied_filtergpu), true>(*copied_filtergpu, ordering_mode_t::TS); // try to chain the operator (otherwise, it is added)
        if (isChained) {
#if defined (WF_TRACING_ENABLED)
            gv_chain_vertex("Filter_GPU (" + std::to_string(copied_filtergpu->getParallelism()) + ")", copied_filtergpu->getName());
#endif
        }
        else {
#if defined (WF_TRACING_ENABLED)
            gv_add_vertex("Filter_GPU (" + std::to_string(copied_filtergpu->getParallelism()) + ")", copied_filtergpu->getName(), false, false, copied_filtergpu->getInputRoutingMode());
#endif
        }
        localOpList.push_back(copied_filtergpu); // add the copied operator to local list
        globalOpList->push_back(copied_filtergpu); // add the copied operator to global list
        return *this;
    }

    /** 
     *  \brief Add a Reduce_GPU operator to the MultiPipe
     *  \param _reducegpu the Reduce_GPU operator to be added
     *  \return a reference to the modified MultiPipe
     */ 
    template<typename reducegpu_func_t, typename key_extractor_func_t>
    MultiPipe &add(const Reduce_GPU<reducegpu_func_t, key_extractor_func_t> &_reducegpu)
    {
        if (execution_mode != Execution_Mode_t::DEFAULT) {
            std::cerr << RED << "WindFlow Error: Reduce_GPU can be used in DEFAULT mode only" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        auto *copied_reducegpu = new Reduce_GPU(_reducegpu); // create a copy of the operator
        using tuple_t = decltype(get_tuple_t_ReduceGPU(copied_reducegpu->func)); // extracting the tuple_t type and checking the admissible signatures
        std::string opInType = TypeName<tuple_t>::getName(); // save the type of tuple_t as a string
        if (!outputType.empty() && outputType.compare(opInType) != 0) {
            std::cerr << RED << "WindFlow Error: output type from MultiPipe is not the input type of the Reduce_GPU operator" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        add_operator<decltype(*copied_reducegpu), true>(*copied_reducegpu, ordering_mode_t::TS);
#if defined (WF_TRACING_ENABLED)
        gv_add_vertex("Reduce_GPU (" + std::to_string(copied_reducegpu->getParallelism()) + ")", copied_reducegpu->getName(), false, false, copied_reducegpu->getInputRoutingMode());
#endif
        localOpList.push_back(copied_reducegpu); // add the copied operator to local list
        globalOpList->push_back(copied_reducegpu); // add the copied operator to global list
        return *this;
    }

    /** 
     *  \brief Try to chain a Reduce_GPU operator to the MultiPipe (if not possible, the operator is added)
     *  \param _reducegpu the Reduce_GPU operator to be chained
     *  \return a reference to the modified MultiPipe
     */ 
    template<typename reducegpu_func_t, typename key_extractor_func_t>
    MultiPipe &chain(const Reduce_GPU<reducegpu_func_t, key_extractor_func_t> &_reducegpu)
    {
        if (execution_mode != Execution_Mode_t::DEFAULT) {
            std::cerr << RED << "WindFlow Error: Reduce_GPU can be used in DEFAULT mode only" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        auto *copied_reducegpu = new Reduce_GPU(_reducegpu); // create a copy of the operator
        using tuple_t = decltype(get_tuple_t_ReduceGPU(copied_reducegpu->func)); // extracting the tuple_t type and checking the admissible signatures
        std::string opInType = TypeName<tuple_t>::getName(); // save the type of tuple_t as a string
        if (!outputType.empty() && outputType.compare(opInType) != 0) {
            std::cerr << RED << "WindFlow Error: output type from MultiPipe is not the input type of the Reduce_GPU operator" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        bool isChained = chain_operator<decltype(*copied_reducegpu), true>(*copied_reducegpu, ordering_mode_t::TS); // try to chain the operator (otherwise, it is added)
        if (isChained) {
#if defined (WF_TRACING_ENABLED)
            gv_chain_vertex("Reduce_GPU (" + std::to_string(copied_reducegpu->getParallelism()) + ")", copied_reducegpu->getName());
#endif
        }
        else {
#if defined (WF_TRACING_ENABLED)
            gv_add_vertex("Filter_GPU (" + std::to_string(copied_reducegpu->getParallelism()) + ")", copied_reducegpu->getName(), false, false, copied_reducegpu->getInputRoutingMode());
#endif
        }
        localOpList.push_back(copied_reducegpu); // add the copied operator to local list
        globalOpList->push_back(copied_reducegpu); // add the copied operator to global list
        return *this;
    }

    /** 
     *  \brief Add a FFAT_Aggregator_GPU operator to the MultiPipe
     *  \param _ffatagg_gpu the FFAT_Aggregator_GPU operator to be added
     *  \return a reference to the modified MultiPipe
     */ 
    template<typename liftgpu_func_t, typename combgpu_func_t, typename key_extractor_func_t>
    MultiPipe &add(const FFAT_Aggregator_GPU<liftgpu_func_t, combgpu_func_t, key_extractor_func_t> &_ffatagg_gpu)
    {
        if (execution_mode != Execution_Mode_t::DEFAULT) {
            std::cerr << RED << "WindFlow Error: FFAT_Aggregator_GPU can be used in DEFAULT mode only" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        auto *copied_ffatagg_gpu = new FFAT_Aggregator_GPU(_ffatagg_gpu); // create a copy of the operator
        copied_ffatagg_gpu->setExecutionMode(execution_mode); // set the execution mode of the operator
        using tuple_t = decltype(get_tuple_t_Lift(copied_ffatagg_gpu->lift_func)); // extracting the tuple_t type and checking the admissible signatures
        using result_t = decltype(get_tuple_t_CombGPU(copied_ffatagg_gpu->comb_func)); // extracting the result_t type and checking the admissible signatures
        std::string opInType = TypeName<tuple_t>::getName(); // save the type of tuple_t as a string
        if (!outputType.empty() && outputType.compare(opInType) != 0) {
            std::cerr << RED << "WindFlow Error: output type from MultiPipe is not the input type of the FFAT_Aggregator_GPU operator" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        outputType = TypeName<result_t>::getName(); // save the new output type from this MultiPipe
        add_operator(*copied_ffatagg_gpu, ordering_mode_t::TS);
#if defined (WF_TRACING_ENABLED)
        gv_add_vertex("FFAT_Aggregator_GPU (" + std::to_string(copied_ffatagg_gpu->getParallelism()) + ")", copied_ffatagg_gpu->getName(), true, false, copied_ffatagg_gpu->getInputRoutingMode());
#endif
        localOpList.push_back(copied_ffatagg_gpu); // add the copied operator to local list
        globalOpList->push_back(copied_ffatagg_gpu); // add the copied operator to global list
        return *this;
    }
#endif

    /** 
     *  \brief Add a Sink operator to the MultiPipe
     *  \param _sink the Sink operator to be added
     *  \return a reference to the modified MultiPipe
     */ 
    template<typename sink_t>
    MultiPipe &add_sink(const sink_t &_sink)
    {
        auto *copied_sink = new sink_t(_sink); // create a copy of the operator
        copied_sink->setExecutionMode(execution_mode); // set the execution mode of the operator
        std::string opInType = TypeName<typename sink_t::tuple_t>::getName(); // save the type of tuple_t as a string
        if (!outputType.empty() && outputType.compare(opInType) != 0) {
            std::cerr << RED << "WindFlow Error: output type from MultiPipe is not the input type of the Sink operator" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        add_operator(*copied_sink, ordering_mode_t::TS);
#if defined (WF_TRACING_ENABLED)
        if (copied_sink->getType() == "Kafka_Sink") {
            gv_add_vertex("Kafka_Sink (" + std::to_string(copied_sink->getParallelism()) + ")", copied_sink->getName(), true, false, copied_sink->getInputRoutingMode());
        }
        else {
            gv_add_vertex("Sink (" + std::to_string(copied_sink->getParallelism()) + ")", copied_sink->getName(), true, false, copied_sink->getInputRoutingMode());
        }
#endif
        localOpList.push_back(copied_sink); // add the copied operator to local list
        globalOpList->push_back(copied_sink); // add the copied operator to global list
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
        copied_sink->setExecutionMode(execution_mode); // set the execution mode of the operator
        std::string opInType = TypeName<typename sink_t::tuple_t>::getName(); // save the type of tuple_t as a string
        if (!outputType.empty() && outputType.compare(opInType) != 0) {
            std::cerr << RED << "WindFlow Error: output type from MultiPipe is not the input type of the Sink operator" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        bool isChained = chain_operator(*copied_sink, ordering_mode_t::TS); // try to chain the operator (otherwise, it is added)
        if (isChained) {
#if defined (WF_TRACING_ENABLED)
            if (copied_sink->getType() == "Kafka_Sink") {
                gv_chain_vertex("Kafka_Sink (" + std::to_string(copied_sink->getParallelism()) + ")", copied_sink->getName());
            }
            else {
                gv_chain_vertex("Sink (" + std::to_string(copied_sink->getParallelism()) + ")", copied_sink->getName());
            }
#endif
        }
        else {
#if defined (WF_TRACING_ENABLED)
            if (copied_sink->getType() == "Kafka_Sink") {
                gv_add_vertex("Kafka_Sink (" + std::to_string(copied_sink->getParallelism()) + ")", copied_sink->getName(), true, false, copied_sink->getInputRoutingMode());
            }
            else {
                gv_add_vertex("Sink (" + std::to_string(copied_sink->getParallelism()) + ")", copied_sink->getName(), true, false, copied_sink->getInputRoutingMode());
            }
#endif
        }
        localOpList.push_back(copied_sink); // add the copied operator to local list
        globalOpList->push_back(copied_sink); // add the copied operator to global list
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
    MultiPipe &split(splitting_func_t _splitting_func,
                     size_t _cardinality)
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
