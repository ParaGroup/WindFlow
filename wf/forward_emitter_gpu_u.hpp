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
 *  @file    forward_emitter_gpu_u.hpp
 *  @author  Gabriele Mencagli
 *  
 *  @brief Emitter implementing the forward (FW) distribution for GPU operators.
 *         Version with CUDA Unified Memory
 *  
 *  @section Forward_Emitter_GPU (Description)
 *  
 *  The emitter implements the forward (FW) distribution in three possible scenarios:
 *  1) CPU-GPU (source is CPU operator, destination is a GPU operator)
 *  2) GPU-GPU (source is GPU operator, destination is a GPU operator)
 *  3) GPU-CPU (source is GPU operator, destination is a CPU operator)
 *  This version of the emitter uses CUDA Unified Memory.
 */ 

#ifndef FW_EMITTER_GPU_U_H
#define FW_EMITTER_GPU_U_H

// includes
#include<single_t.hpp>
#include<batch_gpu_t_u.hpp>
#include<basic_emitter.hpp>

namespace wf {

// class Forward_Emitter_GPU
template<typename key_extractor_func_t, bool inputGPU, bool outputGPU>
class Forward_Emitter_GPU: public Basic_Emitter
{
private:
    key_extractor_func_t key_extr; // functional logic to extract the key attribute from the tuple_t
    using tuple_t = decltype(get_tuple_t_KeyExtrGPU(key_extr)); // extracting the tuple_t type and checking the admissible signatures
    size_t num_dests; // number of destinations connected in output to the emitter
    ssize_t size; // size of the batches to be produced by the emitter (-1 if the emitter explicitly receives batches to be forwared as they are)
    size_t idx_dest; // identifier of the next destination to be used (meaningful if useTreeMode is true)
    bool useTreeMode; // true if the emitter is used in tree-based mode
    std::vector<std::pair<void *, size_t>> output_queue; // vector of pairs (messages and destination identifiers)
    Batch_GPU_t<tuple_t> *batch_output; // pointer to the output batch
    ff::MPMC_Ptr_Queue *queue; // pointer to the recyling queue
    std::atomic<int> *inTransit_counter; // pointer to the counter of in-transit batches
    size_t next_tuple_idx; // identifier where to copy the next tuple in the batch

public:
    // Constructor I (CPU->GPU case)
    Forward_Emitter_GPU(key_extractor_func_t _key_extr,
                        size_t _num_dests,
                        size_t _size):
                        key_extr(_key_extr),
                        num_dests(_num_dests),
                        size(_size),
                        idx_dest(0),
                        useTreeMode(false),
                        batch_output(nullptr),
                        next_tuple_idx(0)
    {
        if constexpr (!(!inputGPU && outputGPU)) {
            std::cerr << RED << "WindFlow Error: Forward_Emitter_GPU created in an invalid manner" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        assert(size > 0); // sanity check
        queue = new ff::MPMC_Ptr_Queue();
        queue->init(DEFAULT_BUFFER_CAPACITY);
        inTransit_counter = new std::atomic<int>(0);
    }

    // Constructor II (GPU->ANY cases)
    Forward_Emitter_GPU(key_extractor_func_t _key_extr,
                        size_t _num_dests):
                        key_extr(_key_extr),
                        num_dests(_num_dests),
                        size(-1),
                        idx_dest(0),
                        useTreeMode(false),
                        batch_output(nullptr),
                        next_tuple_idx(0)
    {
        if constexpr (!((inputGPU && outputGPU) || (inputGPU && !outputGPU))) {
            std::cerr << RED << "WindFlow Error: Forward_Emitter_GPU created in an invalid manner" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        queue = new ff::MPMC_Ptr_Queue();
        queue->init(DEFAULT_BUFFER_CAPACITY);
        inTransit_counter = new std::atomic<int>(0);
    }

    // Copy Constructor
    Forward_Emitter_GPU(const Forward_Emitter_GPU &_other):
                        key_extr(_other.key_extr),
                        num_dests(_other.num_dests),
                        size(_other.size),
                        idx_dest(_other.idx_dest),
                        useTreeMode(_other.useTreeMode),
                        batch_output(nullptr),
                        next_tuple_idx(_other.next_tuple_idx)
    {
        queue = new ff::MPMC_Ptr_Queue();
        queue->init(DEFAULT_BUFFER_CAPACITY);
        inTransit_counter = new std::atomic<int>(0);
    }

    // Move Constructor
    Forward_Emitter_GPU(Forward_Emitter_GPU &&_other):
                        key_extr(std::move(_other.key_extr)),
                        num_dests(_other.num_dests),
                        size(_other.size),
                        idx_dest(_other.idx_dest),
                        useTreeMode(_other.useTreeMode),
                        output_queue(std::move(_other.output_queue)),
                        batch_output(std::exchange(_other.batch_output, nullptr)),
                        queue(std::exchange(_other.queue, nullptr)),
                        inTransit_counter(std::exchange(_other.inTransit_counter, nullptr)),
                        next_tuple_idx(_other.next_tuple_idx) {}

    // Destructor
    ~Forward_Emitter_GPU() override
    {
        assert(output_queue.size() == 0); // sanity check
        assert(batch_output == nullptr); // sanity check
        if (queue != nullptr) { // delete all the batches in the recycling queue
            Batch_t<decltype(get_tuple_t_KeyExtrGPU(key_extr))> *del_batch = nullptr;
            while (queue->pop((void **) &del_batch)) {
                delete del_batch;
            }
            delete queue; // delete the recycling queue
        }
        if (inTransit_counter != nullptr) {
            delete inTransit_counter;
        }
    }

    // Copy Assignment Operator
    Forward_Emitter_GPU &operator=(const Forward_Emitter_GPU &_other)
    {
        if (this != &_other) {
            key_extr = _other.key_extr;
            num_dests = _other.num_dests;
            size = _other.size;
            idx_dest = _other.idx_dest;
            useTreeMode = _other.useTreeMode;
            if (batch_output != nullptr) {
                delete batch_output;
            }
            batch_output = nullptr;
            next_tuple_idx = _other.next_tuple_idx;
        }
        return *this;
    }

    // Move Assignment Operator
    Forward_Emitter_GPU &operator=(Forward_Emitter_GPU &&_other)
    {
        key_extr = std::move(_other.key_extr);
        num_dests = _other.num_dests;
        size = _other.size;
        idx_dest = _other.idx_dest;
        useTreeMode = _other.useTreeMode;
        assert(output_queue.size() == 0); // sanity check
        output_queue = std::move(_other.output_queue);
        if (batch_output != nullptr) {
            delete batch_output;
        }
        batch_output = std::exchange(_other.batch_output, nullptr);
        if (queue != nullptr) { // delete all the batches in the recycling queue
            Batch_t<decltype(get_tuple_t_KeyExtrGPU(key_extr))> *del_batch = nullptr;
            while (queue->pop((void **) &del_batch)) {
                delete del_batch;
            }
            delete queue; // delete the recycling queue
        }
        queue = std::exchange(_other.queue, nullptr);
        if (inTransit_counter != nullptr) {
            delete inTransit_counter;
        }
        inTransit_counter = std::exchange(_other.inTransit_counter, nullptr);
        next_tuple_idx = _other.next_tuple_idx;
        return *this;
    }

    // Create a clone of the emitter
    Basic_Emitter *clone() const override
    {
        auto *copy = new Forward_Emitter_GPU<key_extractor_func_t, inputGPU, outputGPU>(*this);
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

    // Emit method (non in-place version)
    void emit(void *_out,
              uint64_t _identifier,
              uint64_t _timestamp,
              uint64_t _watermark,
              ff::ff_monode *_node) override
    {
        if constexpr (!inputGPU && outputGPU) { // CPU->GPU case
            decltype(get_tuple_t_KeyExtrGPU(key_extr)) *tuple = reinterpret_cast<decltype(get_tuple_t_KeyExtrGPU(key_extr)) *>(_out);
            routing<inputGPU, outputGPU>(*tuple, _timestamp, _watermark, _node);
        }
        else {
            abort(); // <-- this method cannot be used!
        }
    }

    // Emit method (in-place version)
    void emit_inplace(void *_out,
                      ff::ff_monode *_node) override
    {
        if constexpr (!inputGPU && outputGPU) { // CPU->GPU case
            Single_t<decltype(get_tuple_t_KeyExtrGPU(key_extr))> *output = reinterpret_cast<Single_t<decltype(get_tuple_t_KeyExtrGPU(key_extr))> *>(_out);
            routing<inputGPU, outputGPU>(output->tuple, output->getTimestamp(), output->getWatermark(), _node);
            deleteSingle_t(output); // delete the input Single_t
        }
        else if constexpr (inputGPU) { // GPU->ANY case
            Batch_GPU_t<decltype(get_tuple_t_KeyExtrGPU(key_extr))> *output = reinterpret_cast<Batch_GPU_t<decltype(get_tuple_t_KeyExtrGPU(key_extr))> *>(_out);
            routing<inputGPU, outputGPU>(output, _node);
        }
    }

    // Routing CPU->GPU
    template<bool b1, bool b2>
    void routing(typename std::enable_if<!b1 && b2, tuple_t>::type &_tuple,
                 uint64_t _timestamp,
                 uint64_t _watermark,
                 ff::ff_monode *_node)
    {
        if (batch_output == nullptr) { // allocate the batch
            batch_output = allocateBatch_GPU_t<decltype(get_tuple_t_KeyExtrGPU(key_extr))>(size, queue, inTransit_counter);
        }
        (batch_output->data_u)[next_tuple_idx].tuple = _tuple;
        (batch_output->data_u)[next_tuple_idx].timestamp = _timestamp;
        batch_output->updateWatermark(_watermark);
        next_tuple_idx++;
        if (next_tuple_idx == size) { // batch is complete
            batch_output->prefetch2GPU(false); // prefetch batch items to be efficiently accessible by the GPU side
            if (!useTreeMode) { // real send
                _node->ff_send_out(batch_output);
            }
            else { // output is buffered
                output_queue.push_back(std::make_pair(batch_output, idx_dest));
                idx_dest = (idx_dest + 1) % num_dests;
            }
            batch_output = nullptr;
            next_tuple_idx = 0;
        }
    }

    // Routing GPU->GPU
    template<bool b1, bool b2>
    void routing(typename std::enable_if<b1 && b2, Batch_GPU_t<tuple_t>>::type *_output,
                 ff::ff_monode *_node)
    {
        if (!useTreeMode) { // real send
            _node->ff_send_out(_output);
        }
        else { // output is buffered
            output_queue.push_back(std::make_pair(_output, idx_dest));
            idx_dest = (idx_dest + 1) % num_dests;
        }
    }

    // Routing GPU->CPU
    template<bool b1, bool b2>
    void routing(typename std::enable_if<b1 && !b2, Batch_GPU_t<tuple_t>>::type *_output,
                 ff::ff_monode *_node)
    {
        _output->prefetch2CPU(false); // prefetch batch items to be efficiently accessible by the host side
        if (!useTreeMode) { // real send
            _node->ff_send_out(_output);
        }
        else { // output is buffered
            output_queue.push_back(std::make_pair(_output, idx_dest));
            idx_dest = (idx_dest + 1) % num_dests;
        }
    }

    // Punctuation propagation method
    void propagate_punctuation(uint64_t _watermark,
                               ff::ff_monode * _node) override
    {
        flush(_node); // flush the internal partially filled batch (if any)
        size_t punc_size = (size == -1) ? 1 : size; // this is the size of the punctuation batch
        Batch_GPU_t<decltype(get_tuple_t_KeyExtrGPU(key_extr))> *punc = allocateBatch_GPU_t<decltype(get_tuple_t_KeyExtrGPU(key_extr))>(punc_size, queue, inTransit_counter);
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
        if constexpr (!inputGPU && outputGPU) { // case CPU->GPU (the only one meaningful here)
            if (batch_output != nullptr) {
                assert(next_tuple_idx > 0); // sanity check
                batch_output->size = next_tuple_idx; // set the right size (this is a patially filled batch)
                batch_output->prefetch2GPU(false); // prefetch batch items to be efficiently accessible by the GPU side
                if (!useTreeMode) { // real send
                    _node->ff_send_out(batch_output);
                }
                else { // output is buffered
                    output_queue.push_back(std::make_pair(batch_output, idx_dest));
                    idx_dest = (idx_dest + 1) % num_dests;
                }
                batch_output = nullptr;
            }
            next_tuple_idx = 0;
        }
    }
};

} // namespace wf

#endif
