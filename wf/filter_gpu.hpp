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
 *  @file    filter_gpu.hpp
 *  @author  Gabriele Mencagli
 *  
 *  @brief Filter operator on GPU
 *  
 *  @section Filter_GPU (Description)
 *  
 *  This file implements the Filter_GPU operator able to execute streaming transformations
 *  producing zero or one output per input. The operator offloads the processing on a GPU
 *  device.
 */ 

#ifndef FILTER_GPU_H
#define FILTER_GPU_H

// Required to compile with clang and CUDA < 11
#if defined (__clang__) and (__CUDACC_VER_MAJOR__ < 11)
    #define THRUST_CUB_NS_PREFIX namespace thrust::cuda_cub {
    #define THRUST_CUB_NS_POSTFIX }
    #include<thrust/system/cuda/detail/cub/util_debug.cuh>
    using namespace thrust::cuda_cub::cub;
#endif

/// includes
#include<string>
#include<pthread.h>
#include<thrust/copy.h>
#include<thrust/device_ptr.h>
#include<tbb/concurrent_unordered_map.h>
#if !defined (WF_GPU_UNIFIED_MEMORY) && !defined (WF_GPU_PINNED_MEMORY)
    #include<batch_gpu_t.hpp>
#else
    #include<batch_gpu_t_u.hpp>
#endif
#include<basic_emitter.hpp>
#include<basic_operator.hpp>
#include<thrust_allocator.hpp>
#if defined (WF_TRACING_ENABLED)
    #include<stats_record.hpp>
#endif

namespace wf {

//@cond DOXY_IGNORE

// CUDA Kernel: Stateless_FILTERGPU_Kernel
template<typename tuple_t, typename filter_func_gpu_t>
__global__ void Stateless_FILTERGPU_Kernel(batch_item_gpu_t<tuple_t> *data,
                                           bool *flags,
                                           size_t len,
                                           int num_active_thread_per_warp,
                                           filter_func_gpu_t func_gpu)
{
    int id = threadIdx.x + blockIdx.x * blockDim.x; // id of the thread in the kernel
    int num_threads = gridDim.x * blockDim.x; // number of threads in the kernel
    int threads_per_worker = warpSize / num_active_thread_per_warp; // number of threads composing a worker entity
    int num_workers = num_threads / threads_per_worker; // number of workers
    int id_worker = id / threads_per_worker; // id of the worker corresponding to this thread
    if (id % threads_per_worker == 0) { // only "num_active_thread_per_warp" threads per warp work, the others are idle
        for (size_t i=id_worker; i<len; i+=num_workers) {
            flags[i] = func_gpu(data[i].tuple);
        }
    }
}

// CUDA Kernel: Stateful_FILTERGPU_Kernel
template<typename tuple_t, typename state_t, typename filter_func_gpu_t>
__global__ void Stateful_FILTERGPU_Kernel(batch_item_gpu_t<tuple_t> *data,
                                          int *map_idxs,
                                          int *start_idxs,
                                          state_t **states,
                                          bool *flags,
                                          int num_dist_keys,
                                          int num_active_thread_per_warp,
                                          filter_func_gpu_t func_gpu)
{
    int id = threadIdx.x + blockIdx.x * blockDim.x; // id of the thread in the kernel
    int num_threads = gridDim.x * blockDim.x; // number of threads in the kernel
    int threads_per_worker = warpSize / num_active_thread_per_warp; // number of threads composing a worker entity
    int num_workers = num_threads / threads_per_worker; // number of workers
    int id_worker = id / threads_per_worker; // id of the worker corresponding to this thread
    if (id % threads_per_worker == 0) { // only "num_active_thread_per_warp" threads per warp work, the others are idle
        for (int id_key=id_worker; id_key<num_dist_keys; id_key+=num_workers) {
            size_t idx = start_idxs[id_key];
            while (idx != -1) { // execute all the inputs with key in the input batch
                flags[idx] = func_gpu(data[idx].tuple, *(states[id_key]));
                idx = map_idxs[idx];
            }
        }
    }
}

// class FilterGPU_Replica (stateful version)
template<typename filter_func_gpu_t, typename key_t>
class FilterGPU_Replica: public Basic_Replica
{
private:
    template<typename T1, typename T2> friend class Filter_GPU;
    filter_func_gpu_t func; // functional logic used by the Filter_GPU replica
    using tuple_t = decltype(get_tuple_t_FilterGPU(func)); // extracting the tuple_t type and checking the admissible signatures
    using state_t = decltype(get_state_t_FilterGPU(func)); // extracting the state_t type and checking the admissible signatures
    size_t id_replica; // identifier of the Filter_GPU replica
    tbb::concurrent_unordered_map<key_t, wrapper_state_t<state_t>> *keymap; // pointer to the concurrent hashtable keeping the states of the keys
    pthread_spinlock_t *spinlock; // pointer to the spinlock to serialize stateful GPU kernel calls

    struct record_t // struct record_t
    {
        size_t size; // size of the arrays in the structure
        state_t **pinned_state_ptrs_cpu=nullptr; // pinned host array of pointers to states
        state_t **state_ptrs_gpu=nullptr; // GPU array of pointers to states
        bool *flags_gpu; // pointer to a GPU array of boolean flags
        batch_item_gpu_t<tuple_t> *new_data_gpu; // pointer to a GPU array of compacted tuples

        // Constructor
        record_t(size_t _size):
                 size(_size)
        {
            gpuErrChk(cudaMallocHost(&pinned_state_ptrs_cpu, sizeof(state_t *) * size));
            gpuErrChk(cudaMalloc(&state_ptrs_gpu, sizeof(state_t *) * size));
            gpuErrChk(cudaMalloc(&flags_gpu, sizeof(bool) * size));
            gpuErrChk(cudaMalloc(&new_data_gpu, sizeof(batch_item_gpu_t<tuple_t>) * size));
        }

        // Destructor
        ~record_t()
        {
            gpuErrChk(cudaFreeHost(pinned_state_ptrs_cpu));
            gpuErrChk(cudaFree(state_ptrs_gpu));
            gpuErrChk(cudaFree(flags_gpu));
            gpuErrChk(cudaFree(new_data_gpu));
        }

        // Resize the internal arrays of the record
        void resize(size_t _newsize)
        {
            if (_newsize > size) {
                size = _newsize;
                gpuErrChk(cudaFreeHost(pinned_state_ptrs_cpu));
                gpuErrChk(cudaFree(state_ptrs_gpu));
                gpuErrChk(cudaFree(flags_gpu));
                gpuErrChk(cudaFree(new_data_gpu));
                gpuErrChk(cudaMallocHost(&pinned_state_ptrs_cpu, sizeof(state_t *) * size));
                gpuErrChk(cudaMalloc(&state_ptrs_gpu, sizeof(state_t *) * size));
                gpuErrChk(cudaMalloc(&flags_gpu, sizeof(bool) * size));
                gpuErrChk(cudaMalloc(&new_data_gpu, sizeof(batch_item_gpu_t<tuple_t>) * size));
            }
        }
    };

    Batch_GPU_t<tuple_t> *batch_tobe_sent; // pointer to the output batch to be sent
    std::vector<record_t *> records; // vector of pointers to record structures (used circularly)
    size_t id_r; // identifier used for overlapping purposes
    int numSMs; // number of Stream Multiprocessor of the GPU
    int max_threads_per_sm; // maximum number of threads resident on each Stream Multiprocessor of the GPU
    int max_blocks_per_sm; // maximum number of blocks resident on each Stream Multiprocessor of the GPU
    int threads_per_warp; // number of threads per warp of the GPU
    Thurst_Allocator alloc; // internal memory allocator used by CUDA/Thrust

public:
    // Constructor
    FilterGPU_Replica(filter_func_gpu_t _func,
                      size_t _id_replica,
                      std::string _opName,
                      tbb::concurrent_unordered_map<key_t, wrapper_state_t<state_t>> *_keymap,
                      pthread_spinlock_t *_spinlock):
                      Basic_Replica(_opName, false),
                      func(_func),
                      id_replica(_id_replica),
                      keymap(_keymap),
                      spinlock(_spinlock),
                      batch_tobe_sent(nullptr),
                      records(2, nullptr),
                      id_r(0)
    {
        int gpu_id;
        gpuErrChk(cudaGetDevice(&gpu_id));
        gpuErrChk(cudaDeviceGetAttribute(&numSMs, cudaDevAttrMultiProcessorCount, gpu_id));
        gpuErrChk(cudaDeviceGetAttribute(&max_threads_per_sm, cudaDevAttrMaxThreadsPerMultiProcessor, gpu_id));
#if (__CUDACC_VER_MAJOR__ >= 11) // at least CUDA 11
        gpuErrChk(cudaDeviceGetAttribute(&max_blocks_per_sm, cudaDevAttrMaxBlocksPerMultiprocessor, gpu_id));
#else
        max_blocks_per_sm = WF_GPU_MAX_BLOCKS_PER_SM;
#endif
        gpuErrChk(cudaDeviceGetAttribute(&threads_per_warp, cudaDevAttrWarpSize, gpu_id));
    }

    // Copy Constructor
    FilterGPU_Replica(const FilterGPU_Replica &_other):
                      Basic_Replica(_other),
                      func(_other.func),
                      id_replica(_other.id_replica),
                      keymap(_other.keymap),
                      spinlock(_other.spinlock),
                      batch_tobe_sent(nullptr),
                      records(2, nullptr),
                      id_r(_other.id_r),
                      numSMs(_other.numSMs),
                      max_threads_per_sm(_other.max_threads_per_sm),
                      max_blocks_per_sm(_other.max_blocks_per_sm),
                      threads_per_warp(_other.threads_per_warp) {}

    // Destructor
    ~FilterGPU_Replica() override
    {
        if (batch_tobe_sent != nullptr) {
            delete batch_tobe_sent;
        }
        for (auto *p: records) {
            if (p != nullptr) {
                delete p;
            }
        }
        if (id_replica == 0) { // only the first replica deletes the spinlock and keymap
            if (pthread_spin_destroy(spinlock) != 0) { // destroy the spinlock
                std::cerr << RED << "WindFlow Error: pthread_spin_destroy() failed in Filter_GPU" << DEFAULT_COLOR << std::endl;
                exit(EXIT_FAILURE);
            }
            delete keymap;
        }
    }

    // svc (utilized by the FastFlow runtime)
    void *svc(void *_in) override
    {
        this->startStatsRecording();
        Batch_GPU_t<tuple_t> *input = reinterpret_cast<Batch_GPU_t<tuple_t> *>(_in);
        if (input->isPunct()) { // if it is a punctuaton
            sendPreviousBatch(); // send the previous output batch (if any)
            (this->emitter)->propagate_punctuation(input->getWatermark(id_replica), this); // propagate the received punctuation
            deleteBatch_t(input); // delete the punctuation
            return this->GO_ON;
        }
#if defined (WF_TRACING_ENABLED)
        (this->stats_record).inputs_received += input->size;
        (this->stats_record).bytes_received += input->size * sizeof(tuple_t);
#endif
        if (records[id_r] == nullptr) {
            records[id_r] = new record_t(input->original_size);
        }
        else {
            records[id_r]->resize(input->original_size);
        }
        key_t *dist_keys_cpu = reinterpret_cast<key_t *>(input->dist_keys_cpu);
        for (size_t i=0; i<input->num_dist_keys; i++) { // prepare the states
            auto it = keymap->find(dist_keys_cpu[i]);
            if (it == keymap->end()) {
                wrapper_state_t<state_t> newstate;
                auto res = keymap->insert(std::move(std::make_pair(dist_keys_cpu[i], std::move(newstate))));
                records[id_r]->pinned_state_ptrs_cpu[i] = ((*(res.first)).second).state_gpu;
            }
            else {
                records[id_r]->pinned_state_ptrs_cpu[i] = ((*it).second).state_gpu;
            }
        }
        gpuErrChk(cudaMemcpyAsync(records[id_r]->state_ptrs_gpu,
                                  records[id_r]->pinned_state_ptrs_cpu,
                                  input->num_dist_keys * sizeof(state_t *),
                                  cudaMemcpyHostToDevice,
                                  input->cudaStream));
#if !defined(WF_GPU_TLP_ONLY) && !defined(WF_GPU_WLP_ONLY)
        // **************************** Hyrbid Version with TLP and WLP **************************** //
        int warps_per_block = ((max_threads_per_sm / max_blocks_per_sm) / threads_per_warp);
        int tot_num_warps = warps_per_block * max_blocks_per_sm * numSMs;
        int32_t x = (int32_t) std::ceil(((double) input->num_dist_keys) / tot_num_warps);
        if (x > 1) {
            x = next_power_of_two(x);
        }
        int num_active_thread_per_warp = std::min(x, threads_per_warp);
        int num_blocks = std::min((int) ceil(((double) input->num_dist_keys) / warps_per_block), numSMs * max_blocks_per_sm);
        int threads_per_block = warps_per_block*threads_per_warp;
        // ***************************************************************************************** //
#elif defined (WF_GPU_TLP_ONLY)
        // ************************ Version with Thread-Level Parallelism ************************** //
        num_blocks = std::min((int) ceil(((double) input->num_dist_keys) / WF_GPU_THREADS_PER_BLOCK), numSMs * max_blocks_per_sm);
        threads_per_block = WF_GPU_THREADS_PER_BLOCK;
        num_active_thread_per_warp = 32;
        // ***************************************************************************************** //
#elif defined (WF_GPU_WLP_ONLY)
        // ************************** Version with Warp-level Parallelism ************************** //
        warps_per_block = ((max_threads_per_sm / max_blocks_per_sm) / threads_per_warp);
        tot_num_warps = warps_per_block * max_blocks_per_sm * numSMs;
        num_active_thread_per_warp = 1;
        num_blocks = std::min((int) ceil(((double) input->num_dist_keys) / warps_per_block), numSMs * max_blocks_per_sm);
        threads_per_block = warps_per_block*threads_per_warp;
        // ***************************************************************************************** //
#endif
        sendPreviousBatch(true); // send the previous output batch (if any)
        if (pthread_spin_lock(spinlock) != 0) { // acquire the lock
            std::cerr << RED << "WindFlow Error: pthread_spin_lock() failed in Filter_GPU" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        Stateful_FILTERGPU_Kernel<tuple_t, state_t, filter_func_gpu_t>
                                 <<<num_blocks, threads_per_block, 0, input->cudaStream>>>(input->data_gpu,
                                                                                           input->map_idxs_gpu,
                                                                                           input->start_idxs_gpu,
                                                                                           records[id_r]->state_ptrs_gpu,
                                                                                           records[id_r]->flags_gpu,
                                                                                           input->num_dist_keys,
                                                                                           num_active_thread_per_warp,
                                                                                           func);
        gpuErrChk(cudaPeekAtLastError());
        gpuErrChk(cudaStreamSynchronize(input->cudaStream));
        if (pthread_spin_unlock(spinlock) != 0) { // release the lock
            std::cerr << RED << "WindFlow Error: pthread_spin_unlock() failed in Filter_GPU" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        thrust::device_ptr<bool> th_flags_gpu = thrust::device_pointer_cast(records[id_r]->flags_gpu);
        thrust::device_ptr<batch_item_gpu_t<tuple_t>> th_data_gpu = thrust::device_pointer_cast(input->data_gpu);
        thrust::device_ptr<batch_item_gpu_t<tuple_t>> th_new_data_gpu = thrust::device_pointer_cast(records[id_r]->new_data_gpu);
        auto pred = [] __device__ (bool x) { return x; };
        auto end = thrust::copy_if(thrust::cuda::par(alloc).on(input->cudaStream),
                                   th_data_gpu,
                                   th_data_gpu + input->size,
                                   th_flags_gpu,
                                   th_new_data_gpu,
                                   pred);
        input->size = end - th_new_data_gpu; // change the new size of the batch after filtering
#if defined (WF_TRACING_ENABLED)
        (this->stats_record).outputs_sent += input->size;
        (this->stats_record).bytes_sent += input->size * sizeof(tuple_t);
#endif
        gpuErrChk(cudaMemcpyAsync(input->data_gpu,
                                  records[id_r]->new_data_gpu,
                                  input->size * sizeof(batch_item_gpu_t<tuple_t>),
                                  cudaMemcpyDeviceToDevice,
                                  input->cudaStream));
        batch_tobe_sent = input;
        id_r = (id_r + 1) % 2;
        this->endStatsRecording();
        return this->GO_ON;
    }

    // EOS management (utilized by the FastFlow runtime)
    void eosnotify(ssize_t id) override
    {
        sendPreviousBatch(); // send the previous output batch (if any)
        Basic_Replica::eosnotify(id);
    }

    // Send the previous output batch (if any)
    void sendPreviousBatch(bool _autoPunc=false)
    {
        if (batch_tobe_sent != nullptr) {
            gpuErrChk(cudaStreamSynchronize(batch_tobe_sent->cudaStream));
            if (batch_tobe_sent->size == 0) { // if the batch is now empty, it is destroyed
                (this->dropped_inputs)++;
                if (_autoPunc) {
                    if ((this->execution_mode == Execution_Mode_t::DEFAULT) && (this->dropped_inputs % WF_DEFAULT_WM_AMOUNT == 0)) { // check punctuaction generation logic
                        if (current_time_usecs() - this->last_time_punct >= WF_DEFAULT_WM_INTERVAL_USEC) { // check the end of the sample
                            (this->emitter)->propagate_punctuation(batch_tobe_sent->getWatermark(id_replica), this);
                            this->last_time_punct = current_time_usecs();
                        }
                    }
                }
                deleteBatch_t(batch_tobe_sent);
            }
            else {
                (this->emitter)->emit_inplace(batch_tobe_sent, this); // send the output batch once computed
                this->dropped_inputs = 0;
            }
            batch_tobe_sent = nullptr;
        }
    }

    // svc_end (utilized by the FastFlow runtime)
    void svc_end() override {}

    // Configure the Filter_GPU replica to receive batches instead of individual inputs
    void receiveBatches(bool _input_batching) override {}

    FilterGPU_Replica(FilterGPU_Replica &&) = delete; ///< Move constructor is deleted
    FilterGPU_Replica &operator=(const FilterGPU_Replica &) = delete; ///< Copy assignment operator is deleted
    FilterGPU_Replica &operator=(FilterGPU_Replica &&) = delete; ///< Move assignment operator is deleted
};

// class FilterGPU_Replica (stateless version)
template<typename filter_func_gpu_t>
class FilterGPU_Replica<filter_func_gpu_t, empty_key_t>: public Basic_Replica
{
private:
    filter_func_gpu_t func; // functional logic used by the Filter_GPU replica
    using tuple_t = decltype(get_tuple_t_FilterGPU(func)); // extracting the tuple_t type and checking the admissible signatures
    size_t id_replica; // identifier of the Filter_GPU replica

    struct record_t // struct record_t
    {
        size_t size; // size of the arrays in the structure
        bool *flags_gpu; // pointer to a GPU array of boolean flags
        batch_item_gpu_t<tuple_t> *new_data_gpu; // pointer to a GPU array of compacted tuples

        // Constructor
        record_t(size_t _size):
                 size(_size)
        {
            gpuErrChk(cudaMalloc(&flags_gpu, sizeof(bool) * size)); 
            gpuErrChk(cudaMalloc(&new_data_gpu, sizeof(batch_item_gpu_t<tuple_t>) * size));
        }

        // Destructor
        ~record_t()
        {
            gpuErrChk(cudaFree(flags_gpu));
            gpuErrChk(cudaFree(new_data_gpu));
        }

        // Resize the internal arrays of the record
        void resize(size_t _newsize)
        {
            if (_newsize > size) {
                size = _newsize;
                gpuErrChk(cudaFree(flags_gpu));
                gpuErrChk(cudaFree(new_data_gpu));
                gpuErrChk(cudaMalloc(&flags_gpu, sizeof(bool) * size));
                gpuErrChk(cudaMalloc(&new_data_gpu, sizeof(batch_item_gpu_t<tuple_t>) * size));
            }
        }
    };

    std::vector<record_t *> records; // vector of pointers to record structures (used circularly)
    size_t id_r; // identifier used for overlapping purposes
    int numSMs; // number of Stream Multiprocessor of the GPU
    int max_threads_per_sm; // maximum number of threads resident on each Stream Multiprocessor of the GPU
    int max_blocks_per_sm; // maximum number of blocks resident on each Stream Multiprocessor of the GPU
    int threads_per_warp; // number of threads per warp of the GPU
    Thurst_Allocator alloc; // internal memory allocator used by CUDA/Thrust

public:
    // Constructor
    FilterGPU_Replica(filter_func_gpu_t _func,
                      size_t _id_replica,
                      std::string _opName):
                      Basic_Replica(_opName, false),
                      func(_func),
                      id_replica(_id_replica),
                      records(2, nullptr),
                      id_r(0)
    {
        int gpu_id;
        gpuErrChk(cudaGetDevice(&gpu_id));
        gpuErrChk(cudaDeviceGetAttribute(&numSMs, cudaDevAttrMultiProcessorCount, gpu_id));
        gpuErrChk(cudaDeviceGetAttribute(&max_threads_per_sm, cudaDevAttrMaxThreadsPerMultiProcessor, gpu_id));
#if (__CUDACC_VER_MAJOR__ >= 11) // at least CUDA 11
        gpuErrChk(cudaDeviceGetAttribute(&max_blocks_per_sm, cudaDevAttrMaxBlocksPerMultiprocessor, gpu_id));
#else
        max_blocks_per_sm = WF_GPU_MAX_BLOCKS_PER_SM;
#endif
        gpuErrChk(cudaDeviceGetAttribute(&threads_per_warp, cudaDevAttrWarpSize, gpu_id));
    }

    // Copy Constructor
    FilterGPU_Replica(const FilterGPU_Replica &_other):
                      Basic_Replica(_other),
                      func(_other.func),
                      id_replica(_other.id_replica),
                      records(2, nullptr),
                      id_r(_other.id_r),
                      numSMs(_other.numSMs),
                      max_threads_per_sm(_other.max_threads_per_sm),
                      max_blocks_per_sm(_other.max_blocks_per_sm),
                      threads_per_warp(_other.threads_per_warp) {}

    // Destructor
    ~FilterGPU_Replica()
    {
        for (auto *p: records) {
            if (p != nullptr) {
                delete p;
            }
        }
    }

    // svc (utilized by the FastFlow runtime)
    void *svc(void *_in) override
    {
        this->startStatsRecording();
        Batch_GPU_t<tuple_t> *input = reinterpret_cast<Batch_GPU_t<tuple_t> *>(_in);
        if (input->isPunct()) { // if it is a punctuaton
            (this->emitter)->propagate_punctuation(input->getWatermark(id_replica), this); // propagate the received punctuation
            deleteBatch_t(input); // delete the punctuation
            return this->GO_ON;
        }
#if defined (WF_TRACING_ENABLED)
        (this->stats_record).inputs_received += input->size;
        (this->stats_record).bytes_received += input->size * sizeof(tuple_t);
#endif
        if (records[id_r] == nullptr) {
            records[id_r] = new record_t(input->original_size);
        }
        else {
            records[id_r]->resize(input->original_size);
        }
#if !defined(WF_GPU_TLP_ONLY) && !defined(WF_GPU_WLP_ONLY)
        // **************************** Hyrbid Version with TLP and WLP **************************** //
        int warps_per_block = ((max_threads_per_sm / max_blocks_per_sm) / threads_per_warp);
        int tot_num_warps = warps_per_block * max_blocks_per_sm * numSMs;
        int32_t x = (int32_t) ceil(((double) (input->size)) / tot_num_warps);
        if (x > 1) {
            x = next_power_of_two(x);
        }
        int num_active_thread_per_warp = std::min(x, threads_per_warp);
        int num_blocks = std::min((int) ceil(((double) (input->size)) / warps_per_block), numSMs * max_blocks_per_sm);
        int threads_per_block = warps_per_block*threads_per_warp;
        // ***************************************************************************************** //
#elif defined (WF_GPU_TLP_ONLY)
        // ************************ Version with Thread-Level Parallelism ************************** //
        num_blocks = std::min((int) ceil(((double) input->size) / WF_GPU_THREADS_PER_BLOCK), numSMs * max_blocks_per_sm);
        threads_per_block = WF_GPU_THREADS_PER_BLOCK;
        num_active_thread_per_warp = 32;
        // ***************************************************************************************** //
#elif defined (WF_GPU_WLP_ONLY)
        // ************************** Version with Warp-level Parallelism ************************** //
        warps_per_block = ((max_threads_per_sm / max_blocks_per_sm) / threads_per_warp);
        tot_num_warps = warps_per_block * max_blocks_per_sm * numSMs;
        num_active_thread_per_warp = 1;
        num_blocks = std::min((int) ceil(((double) input->size) / warps_per_block), numSMs * max_blocks_per_sm);
        threads_per_block = warps_per_block*threads_per_warp;
        // ***************************************************************************************** //
#endif
        assert(records[id_r]->size >= input->size); // sanity check
        Stateless_FILTERGPU_Kernel<tuple_t, filter_func_gpu_t>
                                  <<<num_blocks, threads_per_block, 0, input->cudaStream>>>(input->data_gpu,
                                                                                            records[id_r]->flags_gpu,
                                                                                            input->size,
                                                                                            num_active_thread_per_warp,
                                                                                            func);                       
        gpuErrChk(cudaPeekAtLastError());
        thrust::device_ptr<bool> th_flags_gpu = thrust::device_pointer_cast(records[id_r]->flags_gpu);
        thrust::device_ptr<batch_item_gpu_t<tuple_t>> th_data_gpu = thrust::device_pointer_cast(input->data_gpu);
        thrust::device_ptr<batch_item_gpu_t<tuple_t>> th_new_data_gpu = thrust::device_pointer_cast(records[id_r]->new_data_gpu);
        auto pred = [] __device__ (bool x) { return x; };
        auto end = thrust::copy_if(thrust::cuda::par(alloc).on(input->cudaStream),
                                   th_data_gpu,
                                   th_data_gpu + input->size,
                                   th_flags_gpu,
                                   th_new_data_gpu,
                                   pred);
        input->size = end - th_new_data_gpu; // change the new size of the batch after filtering
#if defined (WF_TRACING_ENABLED)
        (this->stats_record).outputs_sent += input->size;
        (this->stats_record).bytes_sent += input->size * sizeof(tuple_t);
#endif
        gpuErrChk(cudaMemcpyAsync(input->data_gpu,
                                  records[id_r]->new_data_gpu,
                                  input->size * sizeof(batch_item_gpu_t<tuple_t>),
                                  cudaMemcpyDeviceToDevice,
                                  input->cudaStream));
        gpuErrChk(cudaStreamSynchronize(input->cudaStream));
        if (input->size == 0) { // if the batch is now empty, it is destroyed
            this->dropped_inputs++;
            if ((this->execution_mode == Execution_Mode_t::DEFAULT) && (this->dropped_inputs % WF_DEFAULT_WM_AMOUNT == 0)) { // check punctuaction generation logic
                if (current_time_usecs() - this->last_time_punct >= WF_DEFAULT_WM_INTERVAL_USEC) { // check the end of the sample
                    (this->emitter)->propagate_punctuation(input->getWatermark(id_replica), this);
                    this->last_time_punct = current_time_usecs();
                }
            }
            deleteBatch_t(input);
        }
        else {
            (this->emitter)->emit_inplace(input, this); // send the output batch once computed
            this->dropped_inputs = 0;
        }
        id_r = (id_r + 1) % 2;
        this->endStatsRecording();
        return this->GO_ON;
    }

    // svc_end (utilized by the FastFlow runtime)
    void svc_end() override {}

    // Configure the Filter_GPU replica to receive batches instead of individual inputs
    void receiveBatches(bool _input_batching) override {}

    FilterGPU_Replica(FilterGPU_Replica &&) = delete; ///< Move constructor is deleted
    FilterGPU_Replica &operator=(const FilterGPU_Replica &) = delete; ///< Copy assignment operator is deleted
    FilterGPU_Replica &operator=(FilterGPU_Replica &&) = delete; ///< Move assignment operator is deleted
};

//@endcond

/** 
 *  \class Filter_GPU
 *  
 *  \brief Filter_GPU operator
 *  
 *  This class implements the Filter_GPU operator able to execute streaming transformations
 *  producing zero or one output per input. The operator offloads the processing on a GPU
 *  device.
 */ 
template<typename filter_func_gpu_t, typename keyextr_func_gpu_t>
class Filter_GPU: public Basic_Operator
{
private:
    friend class MultiPipe;
    friend class PipeGraph;
    filter_func_gpu_t func; // functional logic used by the Filter_GPU
    keyextr_func_gpu_t key_extr; // logic to extract the key attribute from the tuple_t
    using key_t = decltype(get_key_t_KeyExtrGPU(key_extr)); // extracting the key_t type and checking the admissible singatures
    using state_t = decltype(get_state_t_FilterGPU(func)); // extracting the state_t type and checking the admissible signatures
    std::vector<FilterGPU_Replica<filter_func_gpu_t, key_t> *> replicas; // vector of pointers to the replicas of the Filter_GPU

    // This method exists but its does not have any effect
    void receiveBatches(bool _input_batching) override {}

    // Set the emitter used to route outputs from the Filter_GPU
    void setEmitter(Basic_Emitter *_emitter) override
    {
        replicas[0]->setEmitter(_emitter);
        for (size_t i=1; i<replicas.size(); i++) {
            replicas[i]->setEmitter(_emitter->clone());
        }
    }

    // Check whether the Filter_GPU has terminated
    bool isTerminated() const override
    {
        bool terminated = true;
        for(auto *r: replicas) { // scan all the replicas to check their termination
            terminated = terminated && r->isTerminated();
        }
        return terminated;
    }

    // Set the execution mode of the Filter_GPU (i.e., the one of its PipeGraph)
    void setExecutionMode(Execution_Mode_t _execution_mode)
    {
        for (auto *r: replicas) {
            r->setExecutionMode(_execution_mode);
        }
    }

    // Get the logic to extract the key attribute from the tuple_t
    keyextr_func_gpu_t getKeyExtractor() const
    {
        return key_extr;
    }

#if defined (WF_TRACING_ENABLED)
    // Append the statistics (JSON format) of the Filter_GPU to a PrettyWriter
    void appendStats(rapidjson::PrettyWriter<rapidjson::StringBuffer> &writer) const override
    {
        // create the header of the JSON file
        writer.StartObject();
        writer.Key("Operator_name");
        writer.String((this->name).c_str());
        writer.Key("Operator_type");
        writer.String("Filter_GPU");
        writer.Key("Distribution");
        if (this->getInputRoutingMode() == Routing_Mode_t::KEYBY) {
            writer.String("KEYBY");
        }
        else if (this->getInputRoutingMode() == Routing_Mode_t::REBALANCING) {
            writer.String("REBALANCING");
        }
        else {
            writer.String("FORWARD");
        }
        writer.Key("isTerminated");
        writer.Bool(this->isTerminated());
        writer.Key("isWindowed");
        writer.Bool(false);
        writer.Key("isGPU");
        writer.Bool(true);
        writer.Key("Parallelism");
        writer.Uint(this->parallelism);
        writer.Key("Replicas");
        writer.StartArray();
        for (auto *r: replicas) { // append the statistics from all the replicas of the Filter_GPU
            Stats_Record record = r->getStatsRecord();
            record.appendStats(writer);
        }
        writer.EndArray();
        writer.EndObject();
    }
#endif

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _func functional logic of the Filter_GPU (a __host__ __device__ callable type)
     *  \param _key_extr key extractor (a __host__ __device__ callable type)
     *  \param _parallelism internal parallelism of the Filter_GPU
     *  \param _name name of the Filter_GPU
     *  \param _input_routing_mode input routing mode of the Filter_GPU
     */ 
    Filter_GPU(filter_func_gpu_t _func,
               keyextr_func_gpu_t _key_extr,
               size_t _parallelism,
               std::string _name,
               Routing_Mode_t _input_routing_mode):
               Basic_Operator(_parallelism, _name, _input_routing_mode, 1, true /* operator targeting GPU */),
               func(_func),
               key_extr(_key_extr)
    {
        if constexpr(std::is_same<key_t, empty_key_t>::value) { // stateless case
            for (size_t i=0; i<this->parallelism; i++) { // create the internal replicas of the Filter_GPU
                replicas.push_back(new FilterGPU_Replica<filter_func_gpu_t, empty_key_t>(_func, i, this->name));
            }
        }
        else { // stateful case
            auto *keymap = new tbb::concurrent_unordered_map<key_t, wrapper_state_t<state_t>>();
            auto *spinlock = new pthread_spinlock_t();
            if (pthread_spin_init(spinlock, 0) != 0) { // spinlock initialization
                std::cerr << RED << "WindFlow Error: pthread_spin_init() failed in Filter_GPU" << DEFAULT_COLOR << std::endl;
                exit(EXIT_FAILURE);
            }
            for (size_t i=0; i<this->parallelism; i++) { // create the internal replicas of the Filter_GPU
                replicas.push_back(new FilterGPU_Replica<filter_func_gpu_t, key_t>(_func, i, this->name, keymap, spinlock));
            }
        }
    }

    /// Copy constructor
    Filter_GPU(const Filter_GPU &_other):
               Basic_Operator(_other),
               func(_other.func),
               key_extr(_other.key_extr)
    {
        for (size_t i=0; i<this->parallelism; i++) { // deep copy of the pointers to the Filter_GPU replicas
            replicas.push_back(new FilterGPU_Replica<filter_func_gpu_t, key_t>(*(_other.replicas[i])));
        }
        if constexpr(!std::is_same<key_t, empty_key_t>::value) { // stateful case
            auto *keymap = new tbb::concurrent_unordered_map<key_t, wrapper_state_t<state_t>>();
            auto *spinlock = new pthread_spinlock_t();
            if (pthread_spin_init(spinlock, 0) != 0) { // spinlock initialization
                std::cerr << RED << "WindFlow Error: pthread_spin_init() failed in Filter_GPU" << DEFAULT_COLOR << std::endl;
                exit(EXIT_FAILURE);
            }
            for (auto *r: replicas) {
                r->spinlock = spinlock;
                r->keymap = keymap;
            }
        }
    }

    // Destructor
    ~Filter_GPU() override
    {
        for (auto *r: replicas) { // delete all the replicas
            delete r;
        }
    }

    /** 
     *  \brief Get the type of the Filter_GPU as a string
     *  \return type of the Filter_GPU
     */ 
    std::string getType() const override
    {
        return std::string("Filter_GPU");
    }

    Filter_GPU(Filter_GPU &&) = delete; ///< Move constructor is deleted
    Filter_GPU &operator=(const Filter_GPU &) = delete; ///< Copy assignment operator is deleted
    Filter_GPU &operator=(Filter_GPU &&) = delete; ///< Move assignment operator is deleted
};

} // namespace wf

#endif
