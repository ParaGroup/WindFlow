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
 *  @file    keyby_emitter_gpu_u.hpp
 *  @author  Gabriele Mencagli
 *  
 *  @brief Emitter implementing the keyby (KB) distribution for GPU operators.
 *         Version with CUDA Unified Memory
 *  
 *  @section KeyBy_Emitter_GPU (Description)
 *  
 *  The emitter is capable of receiving/sending batches from/to GPU operators
 *  by preparing them for a keyby processing. Version with CUDA Unified Memory.
 */ 

#ifndef KB_EMITTER_GPU_U_H
#define KB_EMITTER_GPU_U_H

// Required to compile with clang and CUDA < 11
#if defined (__clang__) and (__CUDACC_VER_MAJOR__ < 11)
    #define THRUST_CUB_NS_PREFIX namespace thrust::cuda_cub {
    #define THRUST_CUB_NS_POSTFIX }
    #include<thrust/system/cuda/detail/cub/util_debug.cuh>
    using namespace thrust::cuda_cub::cub;
#endif

// includes
#include<unordered_map>
#include<single_t.hpp>
#include<batch_cpu_t.hpp>
#include<batch_gpu_t_u.hpp>
#include<basic_emitter.hpp>
#include<thrust_allocator.hpp>
#include<thrust/sort.h>
#include<thrust/unique.h>
#include<thrust/device_ptr.h>

namespace wf {

// CUDA Kernel: Extract_Dests_Kernel
template<typename key_extractor_func_t, typename tuple_t, typename key_t>
__global__ void Extract_Dests_Kernel(batch_item_gpu_t<tuple_t> *data_u,
                                     key_t *keys_gpu,
                                     int *sequence_gpu,
                                     size_t size,
                                     key_extractor_func_t key_extr)
{
    int id = threadIdx.x + blockIdx.x * blockDim.x; // id of the thread in the kernel
    int num_threads = gridDim.x * blockDim.x; // number of threads in the kernel
    for (int i=id; i<size; i+=num_threads) {
        keys_gpu[i] = key_extr(data_u[i].tuple);
        sequence_gpu[i] = i;
    }
}

// CUDA Kernel: Compute_Mapping_Kernel
template<typename key_t>
__global__ void Compute_Mapping_Kernel(key_t *keys_gpu,
                                       int *sequence_gpu,
                                       int *map_idxs_u,
                                       size_t size)
{
    int id = threadIdx.x + blockIdx.x * blockDim.x; // id of the thread in the kernel
    int num_threads = gridDim.x * blockDim.x; // number of threads in the kernel
    for (size_t i=id; i<size; i+=num_threads) {
        if ((i < size-1) && (keys_gpu[i] == keys_gpu[i+1])) { // keys must be comparable with operator==
            map_idxs_u[sequence_gpu[i]] = sequence_gpu[i+1];
        }
        else {
            map_idxs_u[sequence_gpu[i]] = -1;
        }
    }
}

// class KeyBy_Emitter_GPU
template<typename key_extractor_func_t, bool inputGPU, bool outputGPU>
class KeyBy_Emitter_GPU: public Basic_Emitter
{
private:
    key_extractor_func_t key_extr; // functional logic to extract the key attribute from the tuple_t
    using tuple_t = decltype(get_tuple_t_KeyExtrGPU(key_extr)); // extracting the tuple_t type and checking the admissible signatures
    using key_t = decltype(get_key_t_KeyExtrGPU(key_extr)); // extracting the key_t type and checking the admissible signatures
    size_t num_dests; // number of destinations connected in output to the emitter
    ssize_t size; // size of the batches to be produced by the emitter (-1 if the emitter explicitly receives batches to be forwared as they are)
    size_t idx_dest; // identifier of the next destination to be used (meaningful if useTreeMode is true)
    bool useTreeMode; // true if the emitter is used in tree-based mode
    std::vector<std::pair<void *, size_t>> output_queue; // vector of pairs (messages and destination identifiers)
    std::unordered_map<key_t, size_t> dist_map; // hash table mapping for each key its starting index in the corresponding batch
    Batch_GPU_t<tuple_t> *batch_tobe_sent; // pointer to the output batch to be sent
    std::vector<Batch_CPU_t<tuple_t> *> bouts_cpu; // vector of pointers to CPU batches to be sent
    std::vector<key_t *> keys_gpu; // vector of pointers to GPU arrays of keys (used circularly)
    std::vector<key_t *> dist_keys_gpu; // vector of pointers to GPU arrays of distinct keys (used circularly)
    std::vector<int *> sequence_gpu; // vector of pointers to GPU arrays of progressive indexes (used circularly)
    ff::MPMC_Ptr_Queue *queue; // pointer to the recyling queue
    std::vector<size_t> internal_sizes; // vector of internal size values (used circularly)
    size_t next_tuple_idx; // identifier where to copy the next tuple in the batch
    size_t id_r; // identifier used for overlapping purposes
    uint64_t sent_batches; // number of batches sent by the emitter
    int numSMs; // number of Stream Multiprocessor of the GPU
    int max_blocks_per_sm; // maximum number of blocks resident on each Stream Multiprocessor of the GPU
    Thurst_Allocator alloc; // internal memory allocator used by CUDA/Thrust

public:
    // Constructor I (CPU->GPU case)
    KeyBy_Emitter_GPU(key_extractor_func_t _key_extr,
                      size_t _num_dests,
                      size_t _size):
                      key_extr(_key_extr),
                      num_dests(_num_dests),
                      size(_size),
                      idx_dest(0),
                      useTreeMode(false),
                      batch_tobe_sent(nullptr),
                      bouts_cpu(_num_dests, nullptr),
                      keys_gpu(2, nullptr),
                      dist_keys_gpu(2, nullptr),
                      sequence_gpu(2, nullptr),
                      internal_sizes(2, 0),
                      next_tuple_idx(0),
                      id_r(0),
                      sent_batches(0),
                      numSMs(0),
                      max_blocks_per_sm(0)
    {
        if constexpr (!((!inputGPU && outputGPU))) {
            std::cerr << RED << "WindFlow Error: KeyBy_Emitter_GPU created in an invalid manner" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        assert(size > 0); // sanity check
        queue = new ff::MPMC_Ptr_Queue();
        queue->init(WF_GPU_DEFAULT_RECYCLING_QUEUE_SIZE);
    }

    // Constructor II (GPU->ANY cases)
    KeyBy_Emitter_GPU(key_extractor_func_t _key_extr,
                      size_t _num_dests):
                      key_extr(_key_extr),
                      num_dests(_num_dests),
                      size(-1),
                      idx_dest(0),
                      useTreeMode(false),
                      batch_tobe_sent(nullptr),
                      bouts_cpu(_num_dests, nullptr),
                      keys_gpu(2, nullptr),
                      dist_keys_gpu(2, nullptr),
                      sequence_gpu(2, nullptr),
                      internal_sizes(2, 0),
                      next_tuple_idx(0),
                      id_r(0),
                      sent_batches(0)
    {
        if constexpr (!((inputGPU && outputGPU) || (inputGPU && !outputGPU))) {
            std::cerr << RED << "WindFlow Error: KeyBy_Emitter_GPU created in an invalid manner" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        queue = new ff::MPMC_Ptr_Queue();
        queue->init(WF_GPU_DEFAULT_RECYCLING_QUEUE_SIZE);
        gpuErrChk(cudaDeviceGetAttribute(&numSMs, cudaDevAttrMultiProcessorCount, 0)); // device_id = 0
#if (__CUDACC_VER_MAJOR__ >= 11) // at least CUDA 11
        gpuErrChk(cudaDeviceGetAttribute(&max_blocks_per_sm, cudaDevAttrMaxBlocksPerMultiprocessor, 0)); // device_id = 0
#else
        max_blocks_per_sm = WF_GPU_MAX_BLOCKS_PER_SM;
#endif
    }

    // Copy Constructor
    KeyBy_Emitter_GPU(const KeyBy_Emitter_GPU &_other):
                      key_extr(_other.key_extr),
                      num_dests(_other.num_dests),
                      size(_other.size),
                      idx_dest(_other.idx_dest),
                      useTreeMode(_other.useTreeMode),
                      batch_tobe_sent(nullptr),
                      bouts_cpu(_other.num_dests, nullptr),
                      keys_gpu(2, nullptr),
                      dist_keys_gpu(2, nullptr),
                      sequence_gpu(2, nullptr),
                      internal_sizes(2, 0),
                      next_tuple_idx(_other.next_tuple_idx),
                      id_r(_other.id_r),
                      sent_batches(_other.sent_batches),
                      numSMs(_other.numSMs),
                      max_blocks_per_sm(_other.max_blocks_per_sm)
    {
        queue = new ff::MPMC_Ptr_Queue();
        queue->init(WF_GPU_DEFAULT_RECYCLING_QUEUE_SIZE);
    }

    // Move Constructor
    KeyBy_Emitter_GPU(KeyBy_Emitter_GPU &&_other):
                      key_extr(std::move(_other.key_extr)),
                      num_dests(_other.num_dests),
                      size(_other.size),
                      idx_dest(_other.idx_dest),
                      useTreeMode(_other.useTreeMode),
                      output_queue(std::move(_other.output_queue)),
                      dist_map(std::move(_other.dist_map)),
                      batch_tobe_sent(std::exchange(_other.batch_tobe_sent, nullptr)),
                      bouts_cpu(std::move(_other.bouts_cpu)),
                      keys_gpu(std::move(_other.keys_gpu)),
                      dist_keys_gpu(std::move(_other.dist_keys_gpu)),
                      sequence_gpu(std::move(_other.sequence_gpu)),
                      queue(std::exchange(_other.queue, nullptr)),
                      internal_sizes(std::move(_other.internal_sizes)),
                      next_tuple_idx(_other.next_tuple_idx),
                      id_r(_other.id_r),
                      sent_batches(_other.sent_batches),
                      numSMs(_other.numSMs),
                      max_blocks_per_sm(_other.max_blocks_per_sm) {}

    // Destructor
    ~KeyBy_Emitter_GPU() override
    {
        assert(output_queue.size() == 0); // sanity check
        assert(batch_tobe_sent == nullptr); // sanity check
        for (auto *b: bouts_cpu) {
            if (b != nullptr) {
                assert(b->getSize() == 0); // sanity check
                delete b;
            }
        }
        for (auto *p: keys_gpu) {
            if (p!= nullptr) {
                gpuErrChk(cudaFree(p));
            }
        }
        for (auto *p: dist_keys_gpu) {
            if (p!= nullptr) {
                gpuErrChk(cudaFree(p));
            }
        }
        for (auto *p: sequence_gpu) {
            if (p!= nullptr) {
                gpuErrChk(cudaFree(p));
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
    KeyBy_Emitter_GPU &operator=(const KeyBy_Emitter_GPU &_other)
    {
        if (this != &_other) {
            key_extr = _other.key_extr;
            num_dests = _other.num_dests;
            size = _other.size;
            idx_dest = _other.idx_dest;
            useTreeMode = _other.useTreeMode;
            if (batch_tobe_sent != nullptr) {
                delete batch_tobe_sent;
            }
            batch_tobe_sent = nullptr;
            for (auto *b: bouts_cpu) {
                if (b != nullptr) {
                    delete b;
                }
            }
            bouts_cpu.clear();
            bouts_cpu.insert(bouts_cpu.end(), num_dests, nullptr);
            for (auto *p: keys_gpu) {
                if (p!= nullptr) {
                    gpuErrChk(cudaFree(p));
                }
            }
            keys_gpu = { nullptr, nullptr };
            for (auto *p: dist_keys_gpu) {
                if (p!= nullptr) {
                    gpuErrChk(cudaFree(p));
                }
            }
            dist_keys_gpu = { nullptr, nullptr };
            for (auto *p: sequence_gpu) {
                if (p!= nullptr) {
                    gpuErrChk(cudaFree(p));
                }
            }
            sequence_gpu = { nullptr, nullptr };
            internal_sizes = {0, 0};
            next_tuple_idx = _other.next_tuple_idx;
            id_r =  _other.id_r;
            sent_batches = _other.sent_batches;
            numSMs = _other.numSMs;
            max_blocks_per_sm = _other.max_blocks_per_sm;
        }
        return *this;
    }

    // Move Assignment Operator
    KeyBy_Emitter_GPU &operator=(KeyBy_Emitter_GPU &_other)
    {
        key_extr = std::move(_other.key_extr);
        num_dests = _other.num_dests;
        size = _other.size;
        idx_dest = _other.idx_dest;
        useTreeMode = _other.useTreeMode;
        output_queue = std::move(_other.output_queue);
        dist_map = std::move(_other.dist_map);
        if (batch_tobe_sent != nullptr) {
            delete batch_tobe_sent;
        }
        batch_tobe_sent = std::exchange(_other.batch_tobe_sent, nullptr);
        for (auto *b: bouts_cpu) {
            if (b != nullptr) {
                delete b;
            }
        }
        bouts_cpu = std::move(_other.bouts_cpu);
        for (auto *p: keys_gpu) {
            if (p!= nullptr) {
                gpuErrChk(cudaFree(p));
            }
        }
        keys_gpu = std::move(_other.keys_gpu);
        for (auto *p: dist_keys_gpu) {
            if (p!= nullptr) {
                gpuErrChk(cudaFree(p));
            }
        }
        dist_keys_gpu = std::move(_other.dist_keys_gpu);
        for (auto *p: sequence_gpu) {
            if (p!= nullptr) {
                gpuErrChk(cudaFree(p));
            }
        }
        sequence_gpu = std::move(_other.sequence_gpu);
        if (queue != nullptr) { // delete all the batches in the recycling queue
            Batch_t<decltype(get_tuple_t_KeyExtrGPU(key_extr))> *del_batch = nullptr;
            while (queue->pop((void **) &del_batch)) {
                delete del_batch;
            }
            delete queue; // delete the recycling queue
        }
        queue = std::exchange(_other.queue, nullptr);
        internal_sizes = std::move(_other.internal_sizes);
        next_tuple_idx = _other.next_tuple_idx;
        id_r = _other.id_r;
        sent_batches = _other.sent_batches;
        numSMs = _other.numSMs;
        max_blocks_per_sm = _other.max_blocks_per_sm;
        return *this;
    }

    // Create a clone of the emitter
    Basic_Emitter *clone() const override
    {
        auto *copy = new KeyBy_Emitter_GPU<key_extractor_func_t, inputGPU, outputGPU>(*this);
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
    void routing(typename std::enable_if<!b1 && b2 && !std::is_same<decltype(get_key_t_KeyExtrGPU(key_extr)), empty_key_t>::value, tuple_t>::type &_tuple,
                 uint64_t _timestamp,
                 uint64_t _watermark,
                 ff::ff_monode *_node)
    {
        if (batch_tobe_sent == nullptr) {
            batch_tobe_sent = allocateBatch_GPU_t<decltype(get_tuple_t_KeyExtrGPU(key_extr))>(size, queue); // allocate the new batch
            errChkMalloc(batch_tobe_sent->dist_keys = (void *) malloc(sizeof(key_t) * size)); // allocate space for the keys
            std::fill(batch_tobe_sent->map_idxs_u, batch_tobe_sent->map_idxs_u + size, -1); // <-- important to be done here!
        }
        key_t *dist_keys = reinterpret_cast<key_t *>(batch_tobe_sent->dist_keys);
        batch_tobe_sent->data_u[next_tuple_idx].tuple = _tuple;
        batch_tobe_sent->data_u[next_tuple_idx].timestamp = _timestamp;
        batch_tobe_sent->updateWatermark(_watermark);
        auto key = key_extr(_tuple);
        auto it = dist_map.find(key);
        if (it == dist_map.end()) {
            dist_map.insert(std::make_pair(key, next_tuple_idx));
            dist_keys[batch_tobe_sent->num_dist_keys] = key;
            batch_tobe_sent->start_idxs_u[batch_tobe_sent->num_dist_keys] = next_tuple_idx;
            batch_tobe_sent->num_dist_keys++;
        }
        else {
            batch_tobe_sent->map_idxs_u[(*it).second] = next_tuple_idx;
            (*it).second = next_tuple_idx;
        }
        next_tuple_idx++;
        if (next_tuple_idx == size) { // batch is complete
            batch_tobe_sent->prefetch2GPU(true); // prefetch batch items and support arrays to be efficiently accessible by the GPU side
            if (!useTreeMode) { // real send
                _node->ff_send_out(batch_tobe_sent);
            }
            else { // output is buffered
                output_queue.push_back(std::make_pair(batch_tobe_sent, idx_dest));
                idx_dest = (idx_dest + 1) % num_dests;
            }
            next_tuple_idx = 0;
            dist_map.clear();
            batch_tobe_sent = nullptr;
        }
    }

    // Routing stub (never used)
    template<bool b1, bool b2>
    void routing(typename std::enable_if<!b1 && b2 && std::is_same<decltype(get_key_t_KeyExtrGPU(key_extr)), empty_key_t>::value, tuple_t>::type &_tuple,
                 uint64_t _timestamp,
                 uint64_t _watermark,
                 ff::ff_monode *_node)
    {
        abort(); // <-- this method cannot be used!
    }

    // Routing GPU->GPU
    template<bool b1, bool b2>
    void routing(typename std::enable_if<b1 && b2 && !std::is_same<decltype(get_key_t_KeyExtrGPU(key_extr)), empty_key_t>::value, Batch_GPU_t<tuple_t>>::type *_output,
                 ff::ff_monode *_node)
    {
        if (sent_batches > 0) { // wait the copy of the previous batch to be sent
            gpuErrChk(cudaStreamSynchronize(batch_tobe_sent->cudaStream));
            if (!useTreeMode) { // real send
                _node->ff_send_out(batch_tobe_sent);
            }
            else { // output is buffered
                output_queue.push_back(std::make_pair(batch_tobe_sent, idx_dest));
                idx_dest = (idx_dest + 1) % num_dests;
            }
        }
        if (internal_sizes[id_r] == 0) { // first batch
            internal_sizes[id_r] = _output->original_size; // <-- we consider the original size, not the actual one
            gpuErrChk(cudaMalloc(&(keys_gpu[id_r]), sizeof(key_t) * _output->original_size));
            gpuErrChk(cudaMalloc(&(dist_keys_gpu[id_r]), sizeof(key_t) * _output->original_size));
            gpuErrChk(cudaMalloc(&(sequence_gpu[id_r]), sizeof(int) * _output->original_size));
        }
        else if (internal_sizes[id_r] < _output->original_size) { // not first batch
            internal_sizes[id_r] = _output->original_size;
            gpuErrChk(cudaFree(keys_gpu[id_r]));
            gpuErrChk(cudaFree(dist_keys_gpu[id_r]));
            gpuErrChk(cudaFree(sequence_gpu[id_r]));
            gpuErrChk(cudaMalloc(&(keys_gpu[id_r]), sizeof(key_t) * _output->original_size));
            gpuErrChk(cudaMalloc(&(dist_keys_gpu[id_r]), sizeof(key_t) * _output->original_size));
            gpuErrChk(cudaMalloc(&(sequence_gpu[id_r]), sizeof(int) * _output->original_size));
        }
        int num_blocks = std::min((int) ceil(((double) _output->size) / WF_GPU_THREADS_PER_BLOCK), numSMs * max_blocks_per_sm);
        Extract_Dests_Kernel<key_extractor_func_t, decltype(get_tuple_t_KeyExtrGPU(key_extr)), decltype(get_key_t_KeyExtrGPU(key_extr))>
                            <<<num_blocks, WF_GPU_THREADS_PER_BLOCK, 0, _output->cudaStream>>>(_output->data_u,
                                                                                                       keys_gpu[id_r],
                                                                                                       sequence_gpu[id_r],
                                                                                                       _output->size,
                                                                                                       key_extr);
        gpuErrChk(cudaPeekAtLastError());
        thrust::device_ptr<decltype(get_key_t_KeyExtrGPU(key_extr))> th_keys_gpu = thrust::device_pointer_cast(keys_gpu[id_r]);
        thrust::device_ptr<int> th_sequence_gpu = thrust::device_pointer_cast(sequence_gpu[id_r]);
        thrust::sort_by_key(thrust::cuda::par(alloc).on(_output->cudaStream),
                            th_keys_gpu,
                            th_keys_gpu + _output->size,
                            th_sequence_gpu);
        Compute_Mapping_Kernel<decltype(get_key_t_KeyExtrGPU(key_extr))>
                              <<<num_blocks, WF_GPU_THREADS_PER_BLOCK, 0, _output->cudaStream>>>(keys_gpu[id_r],
                                                                                                         sequence_gpu[id_r],
                                                                                                         _output->map_idxs_u,
                                                                                                         _output->size);
        gpuErrChk(cudaPeekAtLastError());
        thrust::device_ptr<decltype(get_key_t_KeyExtrGPU(key_extr))> th_dist_keys_gpu = thrust::device_pointer_cast(dist_keys_gpu[id_r]);
        thrust::device_ptr<int> th_start_idxs_u = thrust::device_pointer_cast(_output->start_idxs_u);
        auto end = thrust::unique_by_key_copy(thrust::cuda::par(alloc).on(_output->cudaStream),
                                              th_keys_gpu, th_keys_gpu + _output->size,
                                              th_sequence_gpu,
                                              th_dist_keys_gpu,
                                              th_start_idxs_u);
        _output->num_dist_keys = end.first - th_dist_keys_gpu; // copy the unique keys on the cpu area within the batch
        if (_output->dist_keys != nullptr) {
            free(_output->dist_keys);
        }
        errChkMalloc(_output->dist_keys = (void *) malloc(sizeof(key_t) * _output->num_dist_keys));
        gpuErrChk(cudaMemcpyAsync(_output->dist_keys,
                                  dist_keys_gpu[id_r],
                                  sizeof(key_t) * _output->num_dist_keys,
                                  cudaMemcpyDeviceToHost,
                                  _output->cudaStream));
        id_r = (id_r + 1) % 2;
        batch_tobe_sent = _output;
        sent_batches++;
    }

    // Routing GPU->CPU
    template<bool b1, bool b2>
    void routing(typename std::enable_if<b1 && !b2 && !std::is_same<decltype(get_key_t_KeyExtrGPU(key_extr)), empty_key_t>::value, Batch_GPU_t<tuple_t>>::type *_output,
                 ff::ff_monode *_node)
    {
        _output->prefetch2CPU(false); // prefetch batch items to be efficiently accessible by the host side
        if (num_dests == 1) { // optimized case of one destination only -> the input batch is delivered as it is
            if (!useTreeMode) { // real send
                _node->ff_send_out(_output);
            }
            else { // output is buffered
                output_queue.push_back(std::make_pair(_output, 0));
            }
        }
        else { // general case of multiple destinations -> this is not so optimized at the moment!
            assert(bouts_cpu.size() == num_dests); // sanity check
            for (size_t i=0; i<num_dests; i++) {
                if (bouts_cpu[i] == nullptr) {
                    bouts_cpu[i] = allocateBatch_CPU_t<decltype(get_tuple_t_KeyExtrGPU(key_extr))>(_output->size, queue);
                }
            }
            for (size_t i=0; i<_output->getSize(); i++) { // scan all the tuples in the input batch
                auto &t = _output->getTupleAtPos(i);
                auto key = key_extr(t); // extract the key from the tuple
                size_t dest_id = std::hash<decltype(get_key_t_KeyExtrGPU(key_extr))>()(key) % num_dests; // compute the corresponding destination identifier associated with the key
                bouts_cpu[dest_id]->addTuple(std::move(t), _output->getTimestampAtPos(i), _output->getWatermark()); // move the tuple in the right output batch
            }
            for (size_t dest_id=0; dest_id<num_dests; dest_id++) {
                if (bouts_cpu[dest_id]->getSize() > 0) { // if they are not empty, we send them
                    if (!useTreeMode) { // real send
                        _node->ff_send_out_to(bouts_cpu[dest_id], dest_id);
                    }
                    else { // output is buffered
                        output_queue.push_back(std::make_pair(bouts_cpu[dest_id], dest_id));
                    }
                    bouts_cpu[dest_id] = nullptr;
                }
            }
            Batch_t<decltype(get_tuple_t_KeyExtrGPU(key_extr))> *output_casted = reinterpret_cast<Batch_t<decltype(get_tuple_t_KeyExtrGPU(key_extr))> *>(_output);
            deleteBatch_t(output_casted);
        }
    }

    // Routing stub (never used)
    template<bool b1, bool b2>
    void routing(typename std::enable_if<(b1 || !b1) && std::is_same<decltype(get_key_t_KeyExtrGPU(key_extr)), empty_key_t>::value, Batch_GPU_t<tuple_t>>::type *_out,
                 ff::ff_monode *_node,
                 bool _inPlace=false)
    {
        abort(); // <-- this method cannot be used!
    }

    // Punctuation propagation method
    void propagate_punctuation(uint64_t _watermark,
                               ff::ff_monode * _node) override
    {
        flush(_node); // flush the internal partially filled batch (if any)
        size_t punc_size = (size == -1) ? 1 : size; // this is the size of the punctuation batch
        if constexpr (inputGPU && !outputGPU) { // GPU->CPU case
            Batch_CPU_t<decltype(get_tuple_t_KeyExtrGPU(key_extr))> *punc = allocateBatch_CPU_t<decltype(get_tuple_t_KeyExtrGPU(key_extr))>(punc_size, queue);
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
        else { // CPU->GPU and GPU->GPU cases
            Batch_GPU_t<decltype(get_tuple_t_KeyExtrGPU(key_extr))> *punc = allocateBatch_GPU_t<decltype(get_tuple_t_KeyExtrGPU(key_extr))>(punc_size, queue);
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
    }

    // Flushing method
    void flush(ff::ff_monode *_node) override
    {
        if constexpr (!inputGPU && outputGPU) { // case CPU->GPU
            if (next_tuple_idx > 0) { // partial batch to be sent
                assert(batch_tobe_sent != nullptr); // sanity check
                batch_tobe_sent->size = next_tuple_idx; // set the real size of the last batch
                batch_tobe_sent->prefetch2GPU(true); // prefetch batch items and support arrays to be efficiently accessible by the GPU side
                if (!useTreeMode) { // real send
                    _node->ff_send_out(batch_tobe_sent);
                }
                else { // output is buffered
                    output_queue.push_back(std::make_pair(batch_tobe_sent, idx_dest));
                    idx_dest = (idx_dest + 1) % num_dests;
                }
                batch_tobe_sent = nullptr;
            }
            assert(batch_tobe_sent == nullptr); // sanity check
            dist_map.clear();
            next_tuple_idx = 0;
        }
        else if constexpr (inputGPU && outputGPU) { // case GPU->GPU
            if (sent_batches > 0) { // wait the copy of the previous batch to be sent
                gpuErrChk(cudaStreamSynchronize(batch_tobe_sent->cudaStream));
                if (!useTreeMode) { // real send
                    _node->ff_send_out(batch_tobe_sent);
                }
                else { // output is buffered
                    output_queue.push_back(std::make_pair(batch_tobe_sent, idx_dest));
                    idx_dest = (idx_dest + 1) % num_dests;
                }
                batch_tobe_sent = nullptr;
            }
            assert(batch_tobe_sent == nullptr); // sanity check
            sent_batches = 0;
            id_r = 0;
        }
    }
};

} // namespace wf

#endif
