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
 *  @file    broadcast_emitter_gpu.hpp
 *  @author  Gabriele Mencagli
 *  
 *  @brief Emitter implementing the broadcast (BD) distribution of GPU operators
 *  
 *  @section Broadcast_Emitter_GPU (Description)
 *  
 *  The emitter implements the broadcast (BD) distribution in one single possible scenario:
 *  1) GPU-CPU (source is GPU operator, destination is a CPU operator). The distribution
 *  is done without copies (so it is the pointer to the same batch that is broadcasted).
 */ 

#ifndef BD_EMITTER_GPU_H
#define BD_EMITTER_GPU_H

// includes
#if !defined (WF_GPU_UNIFIED_MEMORY) && !defined (WF_GPU_PINNED_MEMORY)
    #include<batch_gpu_t.hpp>
#else
    #include<batch_gpu_t_u.hpp>
#endif
#include<basic_emitter.hpp>

namespace wf {

// class Broadcast_Emitter_GPU
template<typename keyextr_func_t, bool inputGPU, bool outputGPU>
class Broadcast_Emitter_GPU: public Basic_Emitter
{
private:
    keyextr_func_t key_extr; // functional logic to extract the key attribute from the tuple_t
    using tuple_t = decltype(get_tuple_t_KeyExtrGPU(key_extr)); // extracting the tuple_t type and checking the admissible signatures
    size_t num_dests; // number of destinations connected in output to the emitter
    bool useTreeMode; // true if the emitter is used in tree-based mode
    std::vector<std::pair<void *, size_t>> output_queue; // vector of pairs (messages and destination identifiers)
    std::atomic<int> *inTransit_counter; // pointer to the counter of in-transit batches

public:
    // Constructor (GPU->CPU case)
    Broadcast_Emitter_GPU(keyextr_func_t _key_extr, size_t _num_dests):
                          key_extr(_key_extr),
                          num_dests(_num_dests),
                          useTreeMode(false)
    {
        if constexpr (!(inputGPU && !outputGPU)) { // only GPU->CPU case is supported by the emitter
            std::cerr << RED << "WindFlow Error: Broadcast_Emitter_GPU created in an invalid manner" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        inTransit_counter = new std::atomic<int>(0);
    }

    // Copy Constructor
    Broadcast_Emitter_GPU(const Broadcast_Emitter_GPU &_other):
                          Basic_Emitter(_other),
                          key_extr(_other.key_extr),
                          num_dests(_other.num_dests),
                          useTreeMode(_other.useTreeMode)
    {
        inTransit_counter = new std::atomic<int>(0);
    }

    // Destructor
    ~Broadcast_Emitter_GPU() override
    {
        assert(output_queue.size() == 0); // sanity check
        Batch_t<tuple_t> *batch = nullptr;
        while ((this->queue)->pop((void **) &batch)) {
            delete batch;
        }
        if (inTransit_counter != nullptr) {
            delete inTransit_counter;
        }
    }

    // Create a clone of the emitter
    Basic_Emitter *clone() const override
    {
        auto *copy = new Broadcast_Emitter_GPU<keyextr_func_t, inputGPU, outputGPU>(*this);
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
        useTreeMode = _useTreeMode;
    }

    // Get a reference to the vector of output messages used by the emitter
    std::vector<std::pair<void *, size_t>> &getOutputQueue() override
    {
        return output_queue;
    }

    // Static doEmit to call the right emit method
    static void doEmit(Basic_Emitter *_emitter,
                       void * _tuple,
                       uint64_t _identifier,
                       uint64_t _timestamp,
                       uint64_t _watermark,
                       ff::ff_monode *_node)
    {
        auto *_casted_emitter = static_cast<Broadcast_Emitter_GPU<keyextr_func_t, inputGPU, outputGPU> *>(_emitter);
        _casted_emitter->emit(_tuple, _identifier, _timestamp, _watermark, _node);
    }

    // Get the pointer to the doEmit method
    doEmit_t get_doEmit() const override
    {
        return Broadcast_Emitter_GPU<keyextr_func_t, inputGPU, outputGPU>::doEmit;
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
        auto *_casted_emitter = static_cast<Broadcast_Emitter_GPU<keyextr_func_t, inputGPU, outputGPU> *>(_emitter);
        _casted_emitter->emit_inplace(_tuple, _node);
    }

    // Get the pointer to the doEmit_inplace method
    doEmit_inplace_t get_doEmit_inplace() const override
    {
        return Broadcast_Emitter_GPU<keyextr_func_t, inputGPU, outputGPU>::emit_inplace;
    }

    // Emit method (in-place version)
    void emit_inplace(void *_out, ff::ff_monode *_node)
    {
        Batch_GPU_t<tuple_t> *output = reinterpret_cast<Batch_GPU_t<tuple_t> *>(_out);
        routing<inputGPU, outputGPU>(output, _node);
    }

    // Routing GPU->CPU
    template<bool b1, bool b2>
    void routing(typename std::enable_if<b1 && !b2, Batch_GPU_t<tuple_t>>::type *_output, ff::ff_monode *_node)
    {
        (_output->delete_counter).fetch_add(num_dests-1);
        assert((_output->watermarks).size() == 1); // sanity check
        (_output->watermarks).insert((_output->watermarks).end(), num_dests-1, (_output->watermarks)[0]); // copy the watermark (having one per destination)
#if !defined (WF_GPU_UNIFIED_MEMORY) && !defined (WF_GPU_PINNED_MEMORY)
        _output->transfer2CPU(); // starting the transfer of the batch items to a host pinned memory array
#else
        _output->prefetch2CPU(false); // prefetch batch items to be efficiently accessible by the host side
#endif
        for (size_t i=0; i<num_dests; i++) {
            if (!useTreeMode) { // real send
                _node->ff_send_out_to(_output, i);
            }
            else { // batch_output is buffered
                output_queue.push_back(std::make_pair(_output, i));
            }
        }
    }

    // Punctuation propagation method
    void propagate_punctuation(uint64_t _watermark, ff::ff_monode * _node) override
    {
        flush(_node); // flush the internal partially filled batch (if any)
        Batch_GPU_t<tuple_t> *punc = allocateBatch_GPU_t<tuple_t>(1, this->queue, inTransit_counter); // punctuation batch has size 1!
        punc->setWatermark(_watermark);
        (punc->delete_counter).fetch_add(num_dests-1);
        assert((punc->watermarks).size() == 1); // sanity check
        (punc->watermarks).insert((punc->watermarks).end(), num_dests-1, (punc->watermarks)[0]); // copy the watermark (having one per destination)
        punc->isPunctuation = true;
        for (size_t i=0; i<num_dests; i++) {
            if (!useTreeMode) { // real send
                _node->ff_send_out_to(punc, i);
            }
            else { // punctuation is buffered
                output_queue.push_back(std::make_pair(punc, i));
            }
        }
    }

    // Flushing method
    void flush(ff::ff_monode *_node) override
    {
        // empty method here!
    }
};

} // namespace wf

#endif
