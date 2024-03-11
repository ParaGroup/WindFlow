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
 *  @file    forward_emitter_gpu.hpp
 *  @author  Gabriele Mencagli
 *  
 *  @brief Emitter implementing the forward (FW) distribution of GPU operators
 *  
 *  @section Forward_Emitter_GPU (Description)
 *  
 *  The emitter implements the forward (FW) distribution in three possible scenarios:
 *  1) CPU-GPU (source is CPU operator, destination is a GPU operator)
 *  2) GPU-GPU (source is GPU operator, destination is a GPU operator)
 *  3) GPU-CPU (source is GPU operator, destination is a CPU operator).
 */ 

#ifndef FW_EMITTER_GPU_H
#define FW_EMITTER_GPU_H

// includes
#include<single_t.hpp>
#if defined (WF_GPU_UNIFIED_MEMORY) || defined(WF_GPU_PINNED_MEMORY)
    #include<batch_gpu_t_u.hpp>
#else
    #include<batch_gpu_t.hpp>
#endif
#include<basic_emitter.hpp>

namespace wf {

// class Forward_Emitter_GPU
template<typename keyextr_func_gpu_t, bool inputGPU, bool outputGPU>
class Forward_Emitter_GPU: public Basic_Emitter
{
private:
    keyextr_func_gpu_t key_extr; // functional logic to extract the key attribute from the tuple_t
    using tuple_t = decltype(get_tuple_t_KeyExtrGPU(key_extr)); // extracting the tuple_t type and checking the admissible signatures
    size_t num_dests; // number of destinations connected in output to the emitter
    ssize_t size; // size of the batches to be produced by the emitter (-1 if the emitter explicitly receives batches to be forwared as they are)
    size_t idx_dest; // identifier of the next destination to be used (meaningful if useTreeMode is true)
    bool useTreeMode; // true if the emitter is used in tree-based mode
    std::vector<std::pair<void *, size_t>> output_queue; // vector of pairs (messages and destination identifiers)
    std::vector<Batch_GPU_t<tuple_t> *> batches_output; // vector of pointers to the output batches (used circularly)
    std::vector<batch_item_gpu_t<tuple_t> *> pinned_buffers_cpu; // vector of pointers to host pinned arrays (used circularly)
    std::atomic<int> *inTransit_counter; // pointer to the counter of in-transit batches
    size_t next_tuple_idx; // identifier where to copy the next tuple in the batch
    size_t id_r; // identifier used for overlapping purposes
    uint64_t sent_batches; // number of batches sent by the emitter

public:
    // Constructor I (CPU->GPU case)
    Forward_Emitter_GPU(keyextr_func_gpu_t _key_extr,
                        size_t _num_dests,
                        size_t _size):
                        key_extr(_key_extr),
                        num_dests(_num_dests),
                        size(_size),
                        idx_dest(0),
                        useTreeMode(false),
                        batches_output(2, nullptr),
                        pinned_buffers_cpu(2, nullptr),
                        next_tuple_idx(0),
                        id_r(0),
                        sent_batches(0)
    {
        if constexpr (!(!inputGPU && outputGPU)) {
            std::cerr << RED << "WindFlow Error: Forward_Emitter_GPU created in an invalid manner" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        assert(size > 0); // sanity check
        inTransit_counter = new std::atomic<int>(0);
#if !defined (WF_GPU_UNIFIED_MEMORY) && !defined(WF_GPU_PINNED_MEMORY)
        gpuErrChk(cudaMallocHost(&pinned_buffers_cpu[0], sizeof(batch_item_gpu_t<tuple_t>) * size));
        gpuErrChk(cudaMallocHost(&pinned_buffers_cpu[1], sizeof(batch_item_gpu_t<tuple_t>) * size));
#endif
    }

    // Constructor II (GPU->GPU and GPU-CPU cases)
    Forward_Emitter_GPU(keyextr_func_gpu_t _key_extr,
                        size_t _num_dests):
                        key_extr(_key_extr),
                        num_dests(_num_dests),
                        size(-1),
                        idx_dest(0),
                        useTreeMode(false),
                        batches_output(2, nullptr),
                        pinned_buffers_cpu(2, nullptr),
                        next_tuple_idx(0),
                        id_r(0),
                        sent_batches(0)
    {
        if constexpr (!((inputGPU && outputGPU) || (inputGPU && !outputGPU))) {
            std::cerr << RED << "WindFlow Error: Forward_Emitter_GPU created in an invalid manner" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        inTransit_counter = new std::atomic<int>(0);
    }

    // Copy Constructor
    Forward_Emitter_GPU(const Forward_Emitter_GPU &_other):
                        Basic_Emitter(_other),
                        key_extr(_other.key_extr),
                        num_dests(_other.num_dests),
                        size(_other.size),
                        idx_dest(_other.idx_dest),
                        useTreeMode(_other.useTreeMode),
                        batches_output(2, nullptr),
                        pinned_buffers_cpu(2, nullptr),
                        next_tuple_idx(_other.next_tuple_idx),
                        id_r(_other.id_r),
                        sent_batches(_other.sent_batches)
    {
        inTransit_counter = new std::atomic<int>(0);
#if !defined (WF_GPU_UNIFIED_MEMORY) && !defined(WF_GPU_PINNED_MEMORY)
        if constexpr (!inputGPU) {
            gpuErrChk(cudaMallocHost(&pinned_buffers_cpu[0], sizeof(batch_item_gpu_t<tuple_t>) * size));
            gpuErrChk(cudaMallocHost(&pinned_buffers_cpu[1], sizeof(batch_item_gpu_t<tuple_t>) * size));
        }
#endif
    }

    // Destructor
    ~Forward_Emitter_GPU() override
    {
        assert(output_queue.size() == 0); // sanity check
        for (auto *b: batches_output) {
            assert(b == nullptr); // sanity check
        }
        for (auto *p: pinned_buffers_cpu) {
            if (p != nullptr) {
                gpuErrChk(cudaFreeHost(p));
            }
        }
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
        auto *copy = new Forward_Emitter_GPU<keyextr_func_gpu_t, inputGPU, outputGPU>(*this);
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
            tuple_t *tuple = reinterpret_cast<tuple_t *>(_out);
            routing<inputGPU, outputGPU>(*tuple, _timestamp, _watermark, _node);
        }
        else {
            abort(); // <-- this method cannot be used!
        }
    }

    // Emit method (in-place version)
    void emit_inplace(void *_out, ff::ff_monode *_node) override
    {
        if constexpr (!inputGPU && outputGPU) { // CPU->GPU case
            Single_t<tuple_t> *output = reinterpret_cast<Single_t<tuple_t> *>(_out);
            routing<inputGPU, outputGPU>(output->tuple, output->getTimestamp(), output->getWatermark(), _node);
            deleteSingle_t(output); // delete the input Single_t
        }
        else if constexpr (inputGPU) { // GPU->ANY case
            Batch_GPU_t<tuple_t> *output = reinterpret_cast<Batch_GPU_t<tuple_t> *>(_out);
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
        if (batches_output[id_r] == nullptr) { // allocate the batch
            batches_output[id_r] = allocateBatch_GPU_t<tuple_t>(size, this->queue, inTransit_counter);
        }
#if defined (WF_GPU_UNIFIED_MEMORY) || defined(WF_GPU_PINNED_MEMORY)
        (batches_output[id_r]->data_gpu)[next_tuple_idx].tuple = _tuple; // move is useless (tuple_t is trivially copyable)
        (batches_output[id_r]->data_gpu)[next_tuple_idx].timestamp = _timestamp;       
#else
        pinned_buffers_cpu[id_r][next_tuple_idx].tuple = _tuple; // move is useless (tuple_t is trivially copyable)
        pinned_buffers_cpu[id_r][next_tuple_idx].timestamp = _timestamp;
#endif
        batches_output[id_r]->updateWatermark(_watermark);
        next_tuple_idx++;
        if (next_tuple_idx == size) { // batch is complete
#if defined (WF_GPU_UNIFIED_MEMORY) || defined(WF_GPU_PINNED_MEMORY)
            batches_output[id_r]->prefetch2GPU(false); // prefetch batch items to be efficiently accessible by the GPU side
            if (!useTreeMode) { // real send
                _node->ff_send_out(batches_output[id_r]);
            }
            else { // output is buffered
                output_queue.push_back(std::make_pair(batches_output[id_r], idx_dest));
                idx_dest = (idx_dest + 1) % num_dests;
            }
            batches_output[id_r] = nullptr;
            next_tuple_idx = 0;
#else
            if (sent_batches > 0) { // wait the copy of the previous batch to be sent
                gpuErrChk(cudaStreamSynchronize(batches_output[(id_r + 1) % 2]->cudaStream));
                if (!useTreeMode) { // real send
                    _node->ff_send_out(batches_output[(id_r + 1) % 2]);
                }
                else { // output is buffered
                    output_queue.push_back(std::make_pair(batches_output[(id_r + 1) % 2], idx_dest));
                    idx_dest = (idx_dest + 1) % num_dests;
                }
                batches_output[(id_r + 1) % 2] = nullptr;
            }
            sent_batches++;
            gpuErrChk(cudaMemcpyAsync(batches_output[id_r]->data_gpu,
                                      pinned_buffers_cpu[id_r],
                                      sizeof(batch_item_gpu_t<tuple_t>) * size,
                                      cudaMemcpyHostToDevice,
                                      batches_output[id_r]->cudaStream));
            next_tuple_idx = 0;
            id_r = (id_r + 1) % 2;
#endif
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
    void routing(typename std::enable_if<b1 && !b2, Batch_GPU_t<tuple_t>>::type *_output, ff::ff_monode *_node)
    {
#if defined (WF_GPU_UNIFIED_MEMORY) || defined(WF_GPU_PINNED_MEMORY)
        _output->prefetch2CPU(false); // prefetch batch items to be efficiently accessible by the host side
#else
        _output->transfer2CPU(); // transfer of the batch items to a host pinned memory array
#endif
        if (!useTreeMode) { // real send
            _node->ff_send_out(_output);
        }
        else { // output is buffered
            output_queue.push_back(std::make_pair(_output, idx_dest));
            idx_dest = (idx_dest + 1) % num_dests;
        }
    }

    // Punctuation propagation method
    void propagate_punctuation(uint64_t _watermark, ff::ff_monode * _node) override
    {
        flush(_node); // flush the internal partially filled batch (if any)
        size_t punc_size = (size == -1) ? 1 : size; // this is the size of the punctuation batch
        Batch_GPU_t<tuple_t> *punc = allocateBatch_GPU_t<tuple_t>(punc_size, this->queue, inTransit_counter);
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
#if !defined (WF_GPU_UNIFIED_MEMORY) && !defined(WF_GPU_PINNED_MEMORY)
            if (sent_batches > 0) { // wait the copy of the previous batch to be sent
                gpuErrChk(cudaStreamSynchronize(batches_output[(id_r + 1) % 2]->cudaStream));
                if (!useTreeMode) { // real send
                    _node->ff_send_out(batches_output[(id_r + 1) % 2]);
                }
                else { // output is buffered
                    output_queue.push_back(std::make_pair(batches_output[(id_r + 1) % 2], idx_dest));
                    idx_dest = (idx_dest + 1) % num_dests;
                }
                batches_output[(id_r + 1) % 2] = nullptr;
            }
#endif
            if (batches_output[id_r] != nullptr) {
                assert(next_tuple_idx > 0); // sanity check
                batches_output[id_r]->size = next_tuple_idx; // set the right size (this is a patially filled batch)
#if !defined (WF_GPU_UNIFIED_MEMORY) && !defined(WF_GPU_PINNED_MEMORY)
                gpuErrChk(cudaMemcpyAsync(batches_output[id_r]->data_gpu,
                                          pinned_buffers_cpu[id_r],
                                          sizeof(batch_item_gpu_t<tuple_t>) * next_tuple_idx,
                                          cudaMemcpyHostToDevice,
                                          batches_output[id_r]->cudaStream));
                gpuErrChk(cudaStreamSynchronize(batches_output[id_r]->cudaStream));
#else
                batches_output[id_r]->prefetch2GPU(false); // prefetch batch items to be efficiently accessible by the GPU side
#endif
                if (!useTreeMode) { // real send
                    _node->ff_send_out(batches_output[id_r]);
                }
                else { // output is buffered
                    output_queue.push_back(std::make_pair(batches_output[id_r], idx_dest));
                    idx_dest = (idx_dest + 1) % num_dests;
                }
                batches_output[id_r] = nullptr;
            }
            sent_batches = 0;
            id_r = 0;
            next_tuple_idx = 0;
        }
    }

    Forward_Emitter_GPU(Forward_Emitter_GPU &&) = delete; ///< Move constructor is deleted
    Forward_Emitter_GPU &operator=(const Forward_Emitter_GPU &) = delete; ///< Copy assignment operator is deleted
    Forward_Emitter_GPU &operator=(Forward_Emitter_GPU &&) = delete; ///< Move assignment operator is deleted
};

} // namespace wf

#endif
