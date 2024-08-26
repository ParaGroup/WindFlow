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
 *    * MIT License: https://github.com/ParaGroup/WindFlow/blob/master/LICENSE.MIT
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
 *  @file    splitting_emitter_gpu.hpp
 *  @author  Gabriele Mencagli
 *  
 *  @brief Emitter implementing the splitting logic of a MultiPipe ending with a
 *         GPU operator
 *  
 *  @section Splitting_Emitter_GPU (Description)
 *  
 *  This file implements the splitting emitter in charge of splitting a MultiPipe.
 *  This version assumes to receive Batch_GPU_t structures. Each input batch is
 *  broadcasted (by copy) to all destination MultiPipes.
 */ 

#ifndef SPLITTING_GPU_H
#define SPLITTING_GPU_H

// includes
#if !defined (WF_GPU_UNIFIED_MEMORY) && !defined (WF_GPU_PINNED_MEMORY)
    #include<batch_gpu_t.hpp>
#else
    #include<batch_gpu_t_u.hpp>
#endif
#include<basic_emitter.hpp>

namespace wf {

// class Splitting_Emitter_GPU
template<typename tuple_t>
class Splitting_Emitter_GPU: public Basic_Emitter
{
private:
    size_t num_dests; // number of destinations connected in output to the emitter
    size_t num_dest_mps; // number of destination MultiPipes connected in output to the emitter
    std::vector<Basic_Emitter *> emitters; // vector of pointers to the internal emitters (one per destination MultiPipe)
    using recycle_t = std::pair<ff::MPMC_Ptr_Queue *, std::atomic<int> *>;
    recycle_t *recyle_descrs; // pointer to an array of recycling descriptors (one per destination MultiPipe)
    std::vector<doEmit_inplace_t> doEmits_inplace; // doEmits_inplace[i] is a pointer to the right doEmits_inplace method to call for the i-th emitter

public:
    // Constructor
    Splitting_Emitter_GPU(size_t _num_dest_mps):
                          num_dests(0),
                          num_dest_mps(_num_dest_mps)
    {
        recyle_descrs = new recycle_t[num_dest_mps];
        for (int i=0; i<num_dest_mps; i++) {
            recyle_descrs[i].first = new ff::MPMC_Ptr_Queue();
            (recyle_descrs[i].first)->init(DEFAULT_BUFFER_CAPACITY);
            recyle_descrs[i].second = new std::atomic<int>(0);
        }
    }

    // Copy Constructor
    Splitting_Emitter_GPU(const Splitting_Emitter_GPU &_other):
                          Basic_Emitter(_other),
                          num_dests(_other.num_dests),
                          num_dest_mps(_other.num_dest_mps)
    {
        for (size_t i=0; i<(_other.emitters).size(); i++) { // deep copy of the internal emitters
            Basic_Emitter *e = ((_other.emitters)[i])->clone();
            emitters.push_back(e);
            doEmits_inplace.push_back(e->get_doEmit_inplace());
        }
        recyle_descrs = new recycle_t[num_dest_mps];
        for (int i=0; i<num_dest_mps; i++) {
            recyle_descrs[i].first = new ff::MPMC_Ptr_Queue();
            (recyle_descrs[i].first)->init(DEFAULT_BUFFER_CAPACITY);
            recyle_descrs[i].second = new std::atomic<int>(0);
        }
    }

    // Destructor
    ~Splitting_Emitter_GPU() override
    {
        for (auto *e: emitters) {
            delete e;
        }
        for (int i=0; i<num_dest_mps; i++) {
            if (recyle_descrs[i].first != nullptr) { // delete all the batches in the recycling queue
                Batch_t<tuple_t> *batch = nullptr;
                while (recyle_descrs[i].first->pop((void **) &batch)) {
                    delete batch;
                }
                delete recyle_descrs[i].first; // delete the recycling queue
            }
            if (recyle_descrs[i].second != nullptr) {
                delete recyle_descrs[i].second;
            }
        }
        delete recyle_descrs;
    }

    // Create a clone of the emitter
    Basic_Emitter *clone() const override
    {
        Splitting_Emitter_GPU<tuple_t> *copy = new Splitting_Emitter_GPU<tuple_t>(*this);
        return copy;
    }

    // Get the number of destinations of the emitter
    size_t getNumDestinations() const override
    {
        return num_dests;
    }

    // Set the emitter to work in tree-based mode
    void setTreeMode(bool _useTreeMode) override
    {
        abort(); // <-- this method cannot be used!
    }

    // Get a reference to the vector of output messages used by the emitter
    std::vector<std::pair<void *, size_t>> &getOutputQueue() override
    {
        abort(); // <-- this method cannot be used!
    }

    // Static doEmit to call the right emit method
    static void doEmit(Basic_Emitter *_emitter,
                       void * _tuple,
                       uint64_t _identifier,
                       uint64_t _timestamp,
                       uint64_t _watermark,
                       ff::ff_monode *_node)
    {
        auto *_casted_emitter = static_cast<Splitting_Emitter_GPU<tuple_t> *>(_emitter);
        _casted_emitter->emit(_tuple, _identifier, _timestamp, _watermark, _node);
    }

    // Get the pointer to the doEmit method
    doEmit_t get_doEmit() const override
    {
        return Splitting_Emitter_GPU<tuple_t>::doEmit;
    }

    // Emit method (non in-place version)
    void emit(void *_out,
              uint64_t _identifier,
              uint64_t _timestamp,
              uint64_t _watermark,
              ff::ff_monode *_node)
    {
        abort(); // <-- this method cannot be used!
    }

    // Static doEmit_inplace to call the right emit_inplace method
    static void emit_inplace(Basic_Emitter *_emitter,
                             void * _tuple,
                             ff::ff_monode *_node)
    {
        auto *_casted_emitter = static_cast<Splitting_Emitter_GPU<tuple_t> *>(_emitter);
        _casted_emitter->emit_inplace(_tuple, _node);
    }

    // Get the pointer to the doEmit_inplace method
    doEmit_inplace_t get_doEmit_inplace() const override
    {
        return Splitting_Emitter_GPU<tuple_t>::emit_inplace;
    }

    // Emit method (in-place version)
    void emit_inplace(void *_out, ff::ff_monode *_node)
    {
        assert(num_dests == _node->get_num_outchannels()); // sanity check
        Batch_GPU_t<tuple_t> *input = reinterpret_cast<Batch_GPU_t<tuple_t> *>(_out);
        for (size_t i=0; i<num_dest_mps-1; i++) { // iterate across all the internal emitters (except the last one)
            Batch_GPU_t<tuple_t> *copy_batch = allocateBatch_GPU_t<tuple_t>(input->original_size, recyle_descrs[i].first, recyle_descrs[i].second); // create a new batch
            copy_batch->watermarks = input->watermarks;
            copy_batch->size = input->size;
#if !defined (WF_GPU_UNIFIED_MEMORY) && !defined (WF_GPU_PINNED_MEMORY)
            gpuErrChk(cudaMemcpyAsync(copy_batch->data_gpu,
                                      input->data_gpu,
                                      sizeof(batch_item_gpu_t<tuple_t>) * input->size,
                                      cudaMemcpyDeviceToDevice,
                                      copy_batch->cudaStream));
            gpuErrChk(cudaStreamSynchronize(copy_batch->cudaStream));
#else
            memcpy(copy_batch->data_gpu,
                   input->data_gpu,
                   sizeof(batch_item_gpu_t<tuple_t>) * input->size);
            copy_batch->prefetch2GPU(false); // <-- this is not always the best choice!
#endif            
            doEmits_inplace[i](emitters[i], copy_batch, _node); // call the logic of the emitter at position i
            auto &vect = emitters[i]->getOutputQueue();
            for (auto msg: vect) { // send each message produced by emitters[i]
                size_t offset = 0;
                for (size_t j=0; j<i; j++) {
                    offset += emitters[j]->getNumDestinations();
                }
                assert(offset + msg.second < _node->get_num_outchannels()); // sanity check
                _node->ff_send_out_to(msg.first, offset + msg.second);
            }
            vect.clear(); // clear all the sent messages
        }        
        doEmits_inplace[num_dest_mps-1](emitters[num_dest_mps-1], input, _node); // call the logic of the last emitter with the original input
        auto &vect = emitters[num_dest_mps-1]->getOutputQueue();
        for (auto msg: vect) { // send each message produced by emitters[num_dest_mps-1]
            size_t offset = 0;
            for (size_t j=0; j<num_dest_mps-1; j++) {
                offset += emitters[j]->getNumDestinations();
            }
            assert(offset + msg.second < _node->get_num_outchannels()); // sanity check
            _node->ff_send_out_to(msg.first, offset + msg.second);
        }
        vect.clear(); // clear all the sent messages
    }

    // Punctuation propagation method
    void propagate_punctuation(uint64_t _watermark, ff::ff_monode * _node) override
    {
        for (size_t i=0; i<emitters.size(); i++) {
            emitters[i]->propagate_punctuation(_watermark, _node); // call the logic of the emitter at position i
            auto &vect = emitters[i]->getOutputQueue();
            for (auto msg: vect) { // send each message produced by emitters[i]
                size_t offset = 0;
                for (size_t j=0; j<i; j++) {
                    offset += emitters[j]->getNumDestinations();
                }
                assert(offset + msg.second < _node->get_num_outchannels()); // sanity check
                _node->ff_send_out_to(msg.first, offset + msg.second);
            }
            vect.clear(); // clear all the sent messages
        }
    }

    // Flushing method
    void flush(ff::ff_monode *_node) override
    {
        assert(num_dests == _node->get_num_outchannels()); // sanity check
        for (size_t i=0; i<emitters.size(); i++) {
            emitters[i]->flush(_node); // call the flush logic of emitters[i]
            auto &vect = emitters[i]->getOutputQueue();
            for (auto msg: vect) { // send each message produced by emitters[i]
                size_t offset = 0;
                for (size_t j=0; j<i; j++) {
                    offset += emitters[j]->getNumDestinations();
                }
                assert(offset + msg.second < _node->get_num_outchannels()); // sanity check
                _node->ff_send_out_to(msg.first, offset + msg.second);
            }
            vect.clear(); // clear all the sent messages
        }
    }

    // Add a new internal emitter
    void addInternalEmitter(Basic_Emitter *_e) override
    {
        _e->setTreeMode(true); // all the emitters work in tree mode
        emitters.push_back(_e);
        doEmits_inplace.push_back(_e->get_doEmit_inplace());
        num_dests += _e->getNumDestinations();
        assert(emitters.size() <= num_dest_mps); // sanity check
    }

    // Get the number of internal emitters
    size_t getNumInternalEmitters() const override
    {
        return emitters.size();
    }

    Splitting_Emitter_GPU(Splitting_Emitter_GPU &&) = delete; ///< Move constructor is deleted
    Splitting_Emitter_GPU &operator=(const Splitting_Emitter_GPU &) = delete; ///< Copy assignment operator is deleted
    Splitting_Emitter_GPU &operator=(Splitting_Emitter_GPU &&) = delete; ///< Move assignment operator is deleted
};

} // namespace wf

#endif
