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
 *  @file    forward_emitter_gpu.hpp
 *  @author  Gabriele Mencagli
 *  
 *  @brief Emitter implementing the forward (FW) distribution for GPU operators
 *  
 *  @section Forward_Emitter_GPU (Description)
 *  
 *  The emitter is capable of receiving/sending batches from/to GPU operators.
 */ 

#ifndef FW_EMITTER_GPU_H
#define FW_EMITTER_GPU_H

// includes
#include<single_t.hpp>
#include<batch_gpu_t.hpp>
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
    ssize_t size; // size of the batches to be produced by the emitter (-1 if the emitter explicitly receives batches to be forwared at it is)
    size_t idx_dest; // identifier of the next destination to be used (meaningful if useTreeMode is true)
    bool useTreeMode; // true if the emitter is used in tree-based mode
    std::vector<std::pair<void *, size_t>> output_queue; // vector of pairs (messages and destination identifiers)
    std::vector<Batch_GPU_t<tuple_t> *> batches_output; // vector of pointers to the output batches (used circularly)
    std::vector<batch_item_gpu_t<tuple_t> *> pinned_buffers_cpu; // vector of pointers to host pinned arrays (used circularly)
    ff::MPMC_Ptr_Queue *queue; // pointer to the recyling queue
    size_t next_tuple_idx; // identifier where to copy the next tuple in the batch
    size_t id_r; // identifier used for overlapping purposes
    uint64_t sent_batches; // number of batches sent by the emitter

public:
    // Constructor I (only CPU->GPU case)
    Forward_Emitter_GPU(key_extractor_func_t _key_extr,
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
        assert(size > 0);
        queue = new ff::MPMC_Ptr_Queue();
        queue->init(DEFAULT_BUFFER_CAPACITY);
        gpuErrChk(cudaMallocHost(&pinned_buffers_cpu[0], sizeof(batch_item_gpu_t<decltype(get_tuple_t_KeyExtrGPU(key_extr))>) * size));
        gpuErrChk(cudaMallocHost(&pinned_buffers_cpu[1], sizeof(batch_item_gpu_t<decltype(get_tuple_t_KeyExtrGPU(key_extr))>) * size));
    }

    // Constructor II (only GPU->ANY cases)
    Forward_Emitter_GPU(key_extractor_func_t _key_extr,
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
        queue = new ff::MPMC_Ptr_Queue();
        queue->init(DEFAULT_BUFFER_CAPACITY);
    }

    // Copy Constructor
    Forward_Emitter_GPU(const Forward_Emitter_GPU &_other):
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
        queue = new ff::MPMC_Ptr_Queue();
        queue->init(DEFAULT_BUFFER_CAPACITY);
        if constexpr (!inputGPU) {
            gpuErrChk(cudaMallocHost(&pinned_buffers_cpu[0], sizeof(batch_item_gpu_t<decltype(get_tuple_t_KeyExtrGPU(key_extr))>) * size));
            gpuErrChk(cudaMallocHost(&pinned_buffers_cpu[1], sizeof(batch_item_gpu_t<decltype(get_tuple_t_KeyExtrGPU(key_extr))>) * size));
        }
    }

    // Move Constructor
    Forward_Emitter_GPU(Forward_Emitter_GPU &&_other):
                        key_extr(std::move(_other.key_extr)),
                        num_dests(_other.num_dests),
                        size(_other.size),
                        idx_dest(_other.idx_dest),
                        useTreeMode(_other.useTreeMode),
                        output_queue(std::move(_other.output_queue)),
                        batches_output(std::move(_other.batches_output)),
                        pinned_buffers_cpu(std::move(_other.pinned_buffers_cpu)),
                        queue(std::exchange(_other.queue, nullptr)),
                        next_tuple_idx(_other.next_tuple_idx),
                        id_r(_other.id_r),
                        sent_batches(_other.sent_batches) {}

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
        if (queue != nullptr) { // delete all the batches in the recycling queue
            Batch_t<decltype(get_tuple_t_KeyExtrGPU(key_extr))> *del_batch = nullptr;
            while (queue->pop((void **) &del_batch)) {
                delete del_batch;
            }
            delete queue; // delete the recycling queue
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
            for (auto *b: batches_output) {
                if (b != nullptr) {
                    delete b;
                }
            }
            batches_output = { nullptr, nullptr };
            for (auto *p: pinned_buffers_cpu) {
                if (p != nullptr) {
                    gpuErrChk(cudaFreeHost(p));
                }
            }
            pinned_buffers_cpu = { nullptr, nullptr };
            if constexpr (!inputGPU) {
                gpuErrChk(cudaMallocHost(&pinned_buffers_cpu[0], sizeof(batch_item_gpu_t<decltype(get_tuple_t_KeyExtrGPU(key_extr))>) * size));
                gpuErrChk(cudaMallocHost(&pinned_buffers_cpu[1], sizeof(batch_item_gpu_t<decltype(get_tuple_t_KeyExtrGPU(key_extr))>) * size));
            }
            next_tuple_idx = _other.next_tuple_idx;
            id_r =  _other.id_r;
            sent_batches = _other.sent_batches;
        }
        return *this;
    }

    // Move Assignment Operator
    Forward_Emitter_GPU &operator=(Forward_Emitter_GPU &_other)
    {
        key_extr = std::move(_other.key_extr);
        num_dests = _other.num_dests;
        size = _other.size;
        idx_dest = _other.idx_dest;
        useTreeMode = _other.useTreeMode;
        output_queue = std::move(_other.output_queue);
        for (auto *b: batches_output) {
            if (b != nullptr) {
                delete b;
            }
        }
        batches_output = std::move(_other.batches_output);
        for (auto *p: pinned_buffers_cpu) {
            if (p != nullptr) {
                gpuErrChk(cudaFreeHost(p));
            }
        }
        pinned_buffers_cpu = std::move(_other.pinned_buffers_cpu);
        if (queue != nullptr) { // delete all the batches in the recycling queue
            Batch_t<decltype(get_tuple_t_KeyExtrGPU(key_extr))> *del_batch = nullptr;
            while (queue->pop((void **) &del_batch)) {
                delete del_batch;
            }
            delete queue; // delete the recycling queue
        }
        queue = std::exchange(_other.queue, nullptr);
        next_tuple_idx = _other.next_tuple_idx;
        id_r = _other.id_r;
        sent_batches = _other.sent_batches;
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
        if (batches_output[id_r] == nullptr) { // allocate the batch
            batches_output[id_r] = allocateBatch_GPU_t<decltype(get_tuple_t_KeyExtrGPU(key_extr))>(size, queue);
        }
        pinned_buffers_cpu[id_r][next_tuple_idx].tuple = _tuple;
        pinned_buffers_cpu[id_r][next_tuple_idx].timestamp = _timestamp;
        batches_output[id_r]->updateWatermark(_watermark);
        next_tuple_idx++;
        if (next_tuple_idx == size) { // batch is complete
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
            gpuErrChk(cudaMemcpyAsync(batches_output[id_r]->data_gpu, pinned_buffers_cpu[id_r], sizeof(batch_item_gpu_t<decltype(get_tuple_t_KeyExtrGPU(key_extr))>) * size, cudaMemcpyHostToDevice, batches_output[id_r]->cudaStream));
            next_tuple_idx = 0;
            id_r = (id_r + 1) % 2;
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
        sent_batches++;
    }

    // Routing GPU->CPU
    template<bool b1, bool b2>
    void routing(typename std::enable_if<b1 && !b2, Batch_GPU_t<tuple_t>>::type *_output,
                 ff::ff_monode *_node)
    {
        _output->transfer2CPU(); // transfer of GPU data to a host memory array
        if (!useTreeMode) { // real send
            _node->ff_send_out(_output);
        }
        else { // output is buffered
            output_queue.push_back(std::make_pair(_output, idx_dest));
            idx_dest = (idx_dest + 1) % num_dests;
        }
        sent_batches++;
    }

    // Punctuation generation method
    void generate_punctuation(uint64_t _watermark,
                              ff::ff_monode * _node) override
    {
        if constexpr (!inputGPU && outputGPU) { // CPU->GPU case
            if (next_tuple_idx > 0) {
                return; // if there is a partial batch, punctuation is not propagated!
            }
            else {
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
                    sent_batches = 0; // this must be set to zero!
                }
            }
        }
        size_t punc_size = (size == -1) ? 1 : size; // this is the size of the punctuation batch
        Batch_GPU_t<decltype(get_tuple_t_KeyExtrGPU(key_extr))> *punc = allocateBatch_GPU_t<decltype(get_tuple_t_KeyExtrGPU(key_extr))>(punc_size, queue);
        punc->setWatermark(_watermark);
        (punc->delete_counter).fetch_add(num_dests-1);
        assert((punc->watermarks).size() == 1);
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

    // Flushing function of the emitter
    void flush(ff::ff_monode *_node) override
    {
        if constexpr (!inputGPU && outputGPU) { // case CPU->GPU (the only one meaningful here)
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
            if (next_tuple_idx > 0) { // partial batch to be sent
                batches_output[id_r]->size = next_tuple_idx; // set the right size (this is a patially filled batch)
                gpuErrChk(cudaMemcpyAsync(batches_output[id_r]->data_gpu, pinned_buffers_cpu[id_r], sizeof(batch_item_gpu_t<decltype(get_tuple_t_KeyExtrGPU(key_extr))>) * next_tuple_idx, cudaMemcpyHostToDevice, batches_output[id_r]->cudaStream));
                gpuErrChk(cudaStreamSynchronize(batches_output[id_r]->cudaStream));
                if (!useTreeMode) { // real send
                    _node->ff_send_out(batches_output[id_r]);
                }
                else { // output is buffered
                    output_queue.push_back(std::make_pair(batches_output[id_r], idx_dest));
                    idx_dest = (idx_dest + 1) % num_dests;
                }
                sent_batches++;
                batches_output[id_r] = nullptr;
            }
        }
    }
};

} // namespace wf

#endif
