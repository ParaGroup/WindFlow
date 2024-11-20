/**************************************************************************************
 *  Copyright (c) 2019- Gabriele Mencagli and Elia Ruggeri
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
 *  @file    ffat_replica_gpu.hpp
 *  @author  Gabriele Mencagli and Elia Ruggeri
 *  
 *  @brief Ffat_Replica_GPU implements the replica of the Ffat_Windows_GPU
 *  
 *  @section Ffat_Replica_GPU (Description)
 *  
 *  This file implements the Ffat_Replica_GPU representing the replica of the
 *  Ffat_Windows_GPU operator.
 */ 

#ifndef FFAT_REPLICA_GPU_H
#define FFAT_REPLICA_GPU_H

// includes
#include<cmath>
#include<string>
#include<unordered_map>
#include<batch_t.hpp>
#if !defined (WF_GPU_UNIFIED_MEMORY) && !defined (WF_GPU_PINNED_MEMORY)
    #include<batch_gpu_t.hpp>
#else
    #include<batch_gpu_t_u.hpp>
#endif
#include<flatfat_gpu.hpp>
#if defined (WF_TRACING_ENABLED)
    #include<stats_record.hpp>
#endif
#include<basic_emitter.hpp>
#include<basic_operator.hpp>
#include<thrust/device_vector.h>

namespace wf {

// Struct containing metadata associated with a window result
template<typename key_t> // if keyed
struct info_t
{
    key_t key;
    uint64_t pane_id;
};

template<>
struct info_t<empty_key_t> // if not keyed
{
    uint64_t pane_id;
};

// Struct of the reduce function to be passed to the thrust::reduce_by_key
template<typename comb_func_gpu_t, typename result_t>
struct thrust_comb_func_t
{
    comb_func_gpu_t func; // functional logic

    // Constructor
    thrust_comb_func_t(comb_func_gpu_t _func):
                       func(_func) {}

    // Binary operator used by the thrust::reduce
    __device__ result_t operator()(const result_t &lhs, const result_t &rhs)
    {
        result_t res;
        func(lhs, rhs, res);
        return res;
    }
};

// CUDA Kernel: Lifting_Kernel_CB
template<typename tuple_t, typename result_t, typename lift_func_gpu_t>
__global__ void Lifting_Kernel_CB(batch_item_gpu_t<tuple_t> *inputs,
                                  result_t *results,
                                  size_t size,
                                  lift_func_gpu_t lift_func)
{
    int id = threadIdx.x + blockIdx.x * blockDim.x; // id of the thread in the kernel
    int num_threads = gridDim.x * blockDim.x; // number of threads in the kernel
    for (size_t i=id; i<size; i+=num_threads) {
        lift_func(inputs[i].tuple, results[i]);
    }
}

// CUDA Kernel: Lifting_Kernel_CB_Keyed
template<typename tuple_t, typename result_t, typename key_t, typename lift_func_gpu_t, typename keyextr_func_gpu_t>
__global__ void Lifting_Kernel_CB_Keyed(batch_item_gpu_t<tuple_t> *inputs,
                                        result_t *results,
                                        key_t *keys,
                                        size_t size,
                                        lift_func_gpu_t lift_func,
                                        keyextr_func_gpu_t key_extr)
{
    int id = threadIdx.x + blockIdx.x * blockDim.x; // id of the thread in the kernel
    int num_threads = gridDim.x * blockDim.x; // number of threads in the kernel
    for (size_t i=id; i<size; i+=num_threads) {
        lift_func(inputs[i].tuple, results[i]);
        keys[i] = key_extr(inputs[i].tuple);
    }
}

// CUDA Kernel: Lifting_Kernel_TB
template<typename tuple_t, typename result_t, typename lift_func_gpu_t>
__global__ void Lifting_Kernel_TB(batch_item_gpu_t<tuple_t> *inputs,
                                  result_t *results,
                                  info_t<empty_key_t> *infos,
                                  size_t size,
                                  uint64_t pane_len,
                                  uint64_t first_pane_id, // previous panes are complete!
                                  int *ignored_tuples_gpu,
                                  lift_func_gpu_t lift_func)
{
    int id = threadIdx.x + blockIdx.x * blockDim.x; // id of the thread in the kernel
    int num_threads = gridDim.x * blockDim.x; // number of threads in the kernel
    for (size_t i=id; i<size; i+=num_threads) {
        lift_func(inputs[i].tuple, results[i]);
        infos[i].pane_id = inputs[i].timestamp / pane_len;
#if !defined(__CUDA_ARCH__) || (__CUDA_ARCH__ >= 600) // atomicAdd defined for CC>=6.0
        if (infos[i].pane_id < first_pane_id) {
            atomicAdd(ignored_tuples_gpu, 1);
        }
#endif
    }
}

// CUDA Kernel: Lifting_Kernel_TB_Keyed
template<typename tuple_t, typename result_t, typename key_t, typename lift_func_gpu_t, typename keyextr_func_gpu_t>
__global__ void Lifting_Kernel_TB_Keyed(batch_item_gpu_t<tuple_t> *inputs,
                                        result_t *results,
                                        info_t<key_t> *infos,
                                        size_t size,
                                        uint64_t pane_len,
                                        uint64_t first_pane_id, // previous panes are complete!
                                        int *ignored_tuples_gpu,
                                        lift_func_gpu_t lift_func,
                                        keyextr_func_gpu_t key_extr)
{
    int id = threadIdx.x + blockIdx.x * blockDim.x; // id of the thread in the kernel
    int num_threads = gridDim.x * blockDim.x; // number of threads in the kernel
    for (size_t i=id; i<size; i+=num_threads) {
        lift_func(inputs[i].tuple, results[i]);
        infos[i].key = key_extr(inputs[i].tuple);
        infos[i].pane_id = inputs[i].timestamp / pane_len;
#if !defined(__CUDA_ARCH__) || (__CUDA_ARCH__ >= 600) // atomicAdd defined for CC>=6.0
        if (infos[i].pane_id < first_pane_id) {
            atomicAdd(ignored_tuples_gpu, 1);
        }
#endif
    }
}

// struct lessThan_func_gpu_t
template<typename T, typename key_t>
struct lessThan_func_gpu_t
{
    __device__ bool operator()(const T &l, const T &r)
    {
        if constexpr (!std::is_same<key_t, empty_key_t>::value) { // if keyed
            if (l.key == r.key) {
                return (l.pane_id > r.pane_id);
            }
            else {
                return (l.key < r.key);
            }
        }
        else { // if not keyed
            return (l.pane_id > r.pane_id);
        }
    }
};

// struct equalTo_func_gpu_t
template<typename T, typename key_t>
struct equalTo_func_gpu_t
{
    __device__ bool operator()(const T &l, const T &r)
    {
        if constexpr (!std::is_same<key_t, empty_key_t>::value) { // if keyed
            return ((l.key == r.key) && (l.pane_id == r.pane_id));
        }
        else { // if not keyed
            return (l.pane_id == r.pane_id);
        }
    }
};

// struct equalTo2_func_gpu_t (only keyed case)
template<typename T>
struct equalTo2_func_gpu_t
{
    __host__ __device__ bool operator()(const T &l, const T &r)
    {
        return (l.key == r.key);
    }
};

// CUDA Kernel: Aggregate_Panes_Kernel
template<typename result_t, typename T, typename comb_func_gpu_t>
__global__ void Aggregate_Panes_Kernel(result_t *new_panes,
                                       T *new_infos,
                                       size_t num_new,
                                       result_t *panes,
                                       size_t num_panes,
                                       size_t size,
                                       size_t first_pane_id, // previous panes are complete!
                                       size_t first_pos,
                                       comb_func_gpu_t comb_func)
{
    int id = threadIdx.x + blockIdx.x * blockDim.x; // id of the thread in the kernel
    int num_threads = gridDim.x * blockDim.x; // number of threads in the kernel
    for (size_t i=id; i<num_new; i+=num_threads) {
        if (new_infos[i].pane_id < first_pane_id) { // late pane -> it must be ignored!
            continue;
        }
        size_t pos = (first_pos + new_infos[i].pane_id - first_pane_id) % size;
        if ((num_panes > 0) && (new_infos[i].pane_id < first_pane_id + num_panes)) { // pane already exists
            comb_func(panes[pos], new_panes[i], panes[pos]); // update the partial result
        }
        else { // pane does not exist
            panes[pos] = new_panes[i];
            size_t num = 0;
            if (i == num_new-1) {
                num = new_infos[i].pane_id - (first_pane_id + num_panes);
            }
            else {
                num = new_infos[i].pane_id - new_infos[i+1].pane_id -1;
            }
            size_t j=1;
            size_t ll = (pos + size - 1) % size;
            while (j <= num) { // creating all the missing panes
                if (new_infos[i].pane_id - j >= (first_pane_id + num_panes)) {
                    new (&(panes[ll])) result_t();
                    ll = (ll + size - 1) % size;
                }
                j++;
            }
        }
    }
}

// class PendingPanes_Queue
template<typename result_t, typename key_t, typename comb_func_gpu_t>
class PendingPanes_Queue
{
private:
    size_t first_id; // identifier of the first pane in the buffer
    size_t first_pos; // position of the first pane in the buffer
    size_t last_pos; // position where to add the next pane in the buffer
    size_t capacity; // total size of the buffer
    result_t *buffer; // array implementing the buffer
    int numSMs, max_blocks_per_sm; // parameters to launch CUDA kernels
    comb_func_gpu_t comb_func; // associative and commutative binary operator

public:
    // Constructor
    PendingPanes_Queue(size_t _capacity,
                       int _numSMs,
                       int _max_blocks_per_sm,
                       comb_func_gpu_t _comb_func):
                       first_id(0),
                       first_pos(0),
                       last_pos(0),
                       capacity(_capacity),
                       numSMs(_numSMs),
                       max_blocks_per_sm(_max_blocks_per_sm),
                       comb_func(_comb_func)
    {
        gpuErrChk(cudaMalloc(&buffer, sizeof(result_t) * capacity));
    }

    // Copy Constructor
    PendingPanes_Queue(const PendingPanes_Queue &_other):
                       first_id(_other.first_id),
                       first_pos(_other.first_pos),
                       last_pos(_other.last_pos),
                       capacity(_other.capacity),
                       numSMs(_other.numSMs),
                       max_blocks_per_sm(_other.max_blocks_per_sm),
                       comb_func(_other.comb_func)
    {
        gpuErrChk(cudaMalloc(&buffer, sizeof(result_t) * capacity));
    }

    // Destructor
    ~PendingPanes_Queue()
    {
        if (buffer != nullptr) {
            gpuErrChk(cudaFree(buffer));
        }
    }

    // getNumPendingPanes method
    size_t getNumPendingPanes()
    {
        size_t num_panes = 0;
        if (first_pos <= last_pos) {
            num_panes = (last_pos - first_pos);
        }
        else {
            num_panes = (capacity - first_pos) + last_pos;
        }
        assert(num_panes < capacity); // sanity check -> the buffer cannot be full!
        return num_panes;
    }

    // resize method
    void resize(size_t _new_capacity, cudaStream_t &_stream)
    {
        assert(_new_capacity > capacity); // sanity check (only go up!)
        size_t num_elements = getNumPendingPanes();
        result_t *new_buffer;
        gpuErrChk(cudaMalloc(&new_buffer, sizeof(result_t) * _new_capacity));
        if (first_pos <= last_pos) {
            gpuErrChk(cudaMemcpyAsync(new_buffer,
                                      &(buffer[first_pos]),
                                      sizeof(result_t) * (last_pos-first_pos),
                                      cudaMemcpyDeviceToDevice,
                                      _stream));
        }
        else {
            gpuErrChk(cudaMemcpyAsync(new_buffer,
                                      &(buffer[first_pos]),
                                      sizeof(result_t) * (capacity-first_pos),
                                      cudaMemcpyDeviceToDevice,
                                      _stream));
            gpuErrChk(cudaMemcpyAsync(&new_buffer[capacity-first_pos],
                                      buffer,
                                      sizeof(result_t) * (last_pos),
                                      cudaMemcpyDeviceToDevice,
                                      _stream));
        }
        gpuErrChk(cudaStreamSynchronize(_stream));
        gpuErrChk(cudaFree(buffer));
        buffer = new_buffer;
        first_pos = 0;
        last_pos = num_elements;
        capacity = _new_capacity;
    }

    // push_panes method
    void push_panes(result_t *_new_panes,
                    info_t<key_t> *_new_infos,
                    size_t _num_new,
                    size_t _newest_pane_id, // largest pane identifier to be added to the buffer
                    cudaStream_t &_stream)
    {
        if (_newest_pane_id >= first_id) { // check whether the buffer needs resizing (up!)
            size_t new_capacity = _newest_pane_id - first_id + 2; // note + 2
            if (new_capacity > capacity) { // we must resize
                resize(new_capacity, _stream);
            }
        }
        int num_blocks = std::min((int) ceil(((double) _num_new) / WF_GPU_THREADS_PER_BLOCK), numSMs * max_blocks_per_sm);
        Aggregate_Panes_Kernel<result_t, info_t<key_t>, comb_func_gpu_t>
                              <<<num_blocks, WF_GPU_THREADS_PER_BLOCK, 0, _stream>>>(_new_panes,
                                                                                     _new_infos,
                                                                                     _num_new,
                                                                                     buffer,
                                                                                     getNumPendingPanes(),
                                                                                     capacity,
                                                                                     first_id,
                                                                                     first_pos,
                                                                                     comb_func);
        gpuErrChk(cudaPeekAtLastError());
        // check if we have to update last_pos
        if (_newest_pane_id >= first_id + getNumPendingPanes()) {
            size_t num_added_panes = _newest_pane_id - (first_id + getNumPendingPanes()) + 1;
            last_pos = (last_pos + num_added_panes) % capacity;
        }
        assert(first_pos != last_pos); // sanity check -> it cannot happen by pushing new data
    }

    // pop_and_add method
    void pop_and_add(uint64_t _num_popped,
                     FlatFAT_GPU<result_t, key_t, comb_func_gpu_t> &fat,
                     cudaStream_t &_stream)
    {
        assert(_num_popped <= getNumPendingPanes()); // sanity check
        assert(first_pos != last_pos); // sanity check -> buffer cannot be empty
        if (first_pos < last_pos) {
            fat.add_tb(&(buffer[first_pos]), _num_popped, _stream);
        }
        else {
            size_t num_to_end = capacity - first_pos;
            if (_num_popped <= num_to_end) {
                fat.add_tb(&(buffer[first_pos]), _num_popped, _stream);
            }
            else {
                fat.add_tb(&(buffer[first_pos]), num_to_end, buffer, (_num_popped - num_to_end), _stream);
            }
        }
        first_pos = (first_pos + _num_popped) % capacity;
        first_id += _num_popped;
    }

    PendingPanes_Queue(PendingPanes_Queue &&) = delete; ///< Move constructor is deleted
    PendingPanes_Queue &operator=(const PendingPanes_Queue &) = delete; ///< Copy assignment operator is deleted
    PendingPanes_Queue &operator=(PendingPanes_Queue &&) = delete; ///< Move assignment operator is deleted
};

// class Ffat_Replica_GPU
template<typename lift_func_gpu_t, typename comb_func_gpu_t, typename keyextr_func_gpu_t>
class Ffat_Replica_GPU: public Basic_Replica
{
private:
    template<typename T1, typename T2, typename T3> friend class Ffat_Windows_GPU;
    lift_func_gpu_t lift_func; // functional logic of the lift
    comb_func_gpu_t comb_func; // functional logic of the combine
    keyextr_func_gpu_t key_extr; // logic to extract the key attribute from the tuple_t
    using tuple_t = decltype(get_tuple_t_LiftGPU(lift_func)); // extracting the tuple_t type and checking the admissible signatures
    using result_t = decltype(get_result_t_LiftGPU(lift_func)); // extracting the result_t type and checking the admissible signatures
    using key_t = decltype(get_key_t_KeyExtrGPU(key_extr)); // extracting the key_t type and checking the admissible singatures
    using fat_t = FlatFAT_GPU<result_t, key_t, comb_func_gpu_t>; // type of the FlatFAT_GPU
    using pending_panes_t = PendingPanes_Queue<result_t, key_t, comb_func_gpu_t>; // type of the PendingPanes_Queue
    static constexpr bool isKeyed = !std::is_same<key_t, empty_key_t>::value;

    struct Key_Descriptor // struct of a key descriptor
    {
        fat_t fatgpu; // FlatFAT_GPU structure
        pending_panes_t *pending_queue; // pointer to the queue of pending panes (time-based windows)
        cudaStream_t *cudaStream; // pointer to the CUDA stream
        uint64_t next_pane_id; // identifier of the first non-complete pane (time-based windows)
        uint64_t pane_id_triggerer; // identifier of the first pane triggering the next window when complete (time-based windows)
        uint64_t next_gwid; // identifier of the next window result to be produced
        bool firstWinDone; // true if the first window has been computed, false otherwise (time-based windows)
        uint64_t count; // counter to handle windows activation (count-based windows)
        uint64_t count_triggerer; // count of the number of tuples that triggers the next window (count-based windows)

        // Constructor
        Key_Descriptor(Win_Type_t _winType,
                       comb_func_gpu_t _comb_func,
                       size_t _batchSize, // panes per batch (TB), tuples per batch (CB)
                       size_t _numWinPerBatch,
                       size_t _win_len, // in panes (TB), in tuples (CB)
                       size_t _slide_len, // in panes (TB), in tuples (CB)
                       key_t _key,
                       size_t _numSMs,
                       size_t _max_blocks_per_sm):
                       fatgpu(_comb_func, _batchSize, _numWinPerBatch, _win_len, _slide_len, _key, _numSMs, _max_blocks_per_sm),
                       next_pane_id(0),
                       pane_id_triggerer(_batchSize-1),
                       next_gwid(0),
                       firstWinDone(false),
                       count(0),
                       count_triggerer(_batchSize)
        {
            if (_winType == Win_Type_t::TB) { // time-based windows
                pending_queue = new pending_panes_t(_batchSize /* initial capacity of the queue */, _numSMs, _max_blocks_per_sm, _comb_func);
            }
            else { // count-based windows
                pending_queue = nullptr;
            }
            cudaStream = new cudaStream_t();
        }

        // Copy Constructor
        Key_Descriptor(const Key_Descriptor &_other):
                       fatgpu(_other.fatgpu),
                       next_pane_id(_other.next_pane_id),
                       pane_id_triggerer(_other.pane_id_triggerer),
                       next_gwid(_other.next_gwid),
                       firstWinDone(_other.firstWinDone),
                       count(_other.count),
                       count_triggerer(_other.count_triggerer)
        {
            if (_other.pending_queue != nullptr) { // time-based windows
                pending_queue = new pending_panes_t(*(_other.pending_queue));
            }
            else { // count-based windows
                pending_queue = nullptr;
            }
            cudaStream = new cudaStream_t();
        }

        // Destructor
        ~Key_Descriptor()
        {
            if (pending_queue != nullptr) {
                delete pending_queue;
            }
            if (cudaStream != nullptr) {
                delete cudaStream;
            }
        }
    };

    size_t id_replica; // identifier of the Ffat_Windows_GPU replica
    uint64_t win_len; // window length
    uint64_t slide_len; // slide length
    uint64_t pane_len; // length of each pane (meaningful for time-based windows)
    uint64_t lateness; // triggering delay in time units (meaningful for time-based windows)
    Win_Type_t winType; // window type (count-based or time-based)
    std::unordered_map<key_t, Key_Descriptor> keyMap; // hash table that maps a descriptor for each key
    size_t numWinPerBatch; // number of consecutive windows results produced per batch
    size_t batchSize; // number of panes (TB) or tuples (CB) composing one batch
    key_t *keys_gpu = nullptr; // pointer to a GPU array containing keys
    info_t<key_t> *infos_gpu = nullptr; // pointer to a GPU array containing info_t structures
    result_t *results_gpu = nullptr; // pointer to a GPU array containing results
    size_t size_buffers; // size of the GPU arrays
    info_t<key_t> *new_infos_gpu = nullptr; // pointer to a GPU array of compacted info_t structures
    result_t *new_results_gpu = nullptr; // pointer to a GPU array of compacted results
    int *sequence_gpu = nullptr; // pointer to a GPU array of progressive indexes
    key_t *unique_keys_gpu = nullptr; // pointer to a GPU array of unique keys
    info_t<key_t> *unique_infos_gpu = nullptr; // pointer to a GPU array of unique info_t structures
    int *unique_sequence_gpu = nullptr; // pointer to a GPU array of unique progressive indexes
    key_t *pinned_unique_keys_cpu = nullptr; // pointer to a host pinned array of unique keys
    info_t<key_t> *pinned_unique_infos_cpu = nullptr; // pointer of a host pinned array of unique info_t structures
    int *pinned_unique_sequence_cpu = nullptr; // pointer to a host pinned array of unique progressive indexes
    Thurst_Allocator alloc; // internal memory allocator used by CUDA/Thrust
    ff::MPMC_Ptr_Queue *queue; // pointer to the recyling queue
    std::atomic<int> *inTransit_counter; // pointer to the counter of in-transit batches
    uint64_t last_time; // last received watermark
    int ignored_tuples_cpu; // number of ignored tuples accessible by CPU
    int *ignored_tuples_gpu; // pointer to the number of ignored tuples accessible by GPU
    int numSMs; // number of Stream MultiProcessors of the used GPU
    int max_blocks_per_sm; // maximum number of blocks resident on each Stream Multiprocessor of the GPU

    // Allocate internal arrays used for GPU purposes
    void allocate_arrays(size_t _check_size)
    {
        assert(_check_size != 0); // sanity check
        if (_check_size <= size_buffers) {
            return;
        }
        deallocate_arrays();
        if (winType == Win_Type_t::TB) { // time-based windows
            gpuErrChk(cudaMalloc(&results_gpu, sizeof(result_t) * _check_size));
            gpuErrChk(cudaMalloc(&infos_gpu, sizeof(info_t<key_t>) * _check_size));
            gpuErrChk(cudaMalloc(&new_infos_gpu, sizeof(info_t<key_t>) * _check_size));
            gpuErrChk(cudaMalloc(&new_results_gpu, sizeof(result_t) * _check_size));
            if constexpr (isKeyed) { // if keyed
                gpuErrChk(cudaMalloc(&sequence_gpu, sizeof(int) * _check_size));
                gpuErrChk(cudaMalloc(&unique_infos_gpu, sizeof(info_t<key_t>) * _check_size));
                gpuErrChk(cudaMalloc(&unique_sequence_gpu, sizeof(int) * _check_size));
                gpuErrChk(cudaMallocHost(&pinned_unique_infos_cpu, sizeof(info_t<key_t>) * _check_size));
                gpuErrChk(cudaMallocHost(&pinned_unique_sequence_cpu, sizeof(int) * _check_size));
            }
            else { // if not keyed
                gpuErrChk(cudaMallocHost(&pinned_unique_infos_cpu, sizeof(info_t<key_t>)));
            }
        }
        else { // count-based windows
            gpuErrChk(cudaMalloc(&results_gpu, sizeof(result_t) * _check_size));
            if constexpr (isKeyed) { // if keyed
                gpuErrChk(cudaMalloc(&keys_gpu, sizeof(key_t) * _check_size));
                gpuErrChk(cudaMalloc(&sequence_gpu, sizeof(int) * _check_size));
                gpuErrChk(cudaMalloc(&unique_keys_gpu, sizeof(key_t) * _check_size));
                gpuErrChk(cudaMalloc(&unique_sequence_gpu, sizeof(int) * _check_size));
                gpuErrChk(cudaMallocHost(&pinned_unique_keys_cpu, sizeof(key_t) * _check_size));
                gpuErrChk(cudaMallocHost(&pinned_unique_sequence_cpu, sizeof(int) * _check_size));
            }
        }
        size_buffers = _check_size;
    }

    // Deallocate all internal array used for GPU purposes
    void deallocate_arrays()
    {
        if (size_buffers > 0) {
            // destoy previous allocations
            if (winType == Win_Type_t::TB) { // time-based windows
                gpuErrChk(cudaFree(results_gpu));
                gpuErrChk(cudaFree(infos_gpu));
                gpuErrChk(cudaFree(new_infos_gpu));
                gpuErrChk(cudaFree(new_results_gpu));
                if constexpr (isKeyed) { // if keyed
                    gpuErrChk(cudaFree(sequence_gpu));
                    gpuErrChk(cudaFree(unique_infos_gpu));
                    gpuErrChk(cudaFree(unique_sequence_gpu));
                    gpuErrChk(cudaFreeHost(pinned_unique_infos_cpu));
                    gpuErrChk(cudaFreeHost(pinned_unique_sequence_cpu));
                }
                else { // if not keyed
                   gpuErrChk(cudaFreeHost(pinned_unique_infos_cpu));
                }
            }
            else { // count-based windows
                gpuErrChk(cudaFree(results_gpu));
                if constexpr (isKeyed) { // if keyed
                    gpuErrChk(cudaFree(keys_gpu));
                    gpuErrChk(cudaFree(sequence_gpu));
                    gpuErrChk(cudaFree(unique_keys_gpu));
                    gpuErrChk(cudaFree(unique_sequence_gpu));
                    gpuErrChk(cudaFreeHost(pinned_unique_keys_cpu));
                    gpuErrChk(cudaFreeHost(pinned_unique_sequence_cpu));
                }
            }
        }
    }

public:
    // Constructor
    Ffat_Replica_GPU(lift_func_gpu_t _lift_func,
                     comb_func_gpu_t _comb_func,
                     keyextr_func_gpu_t _key_extr,
                     size_t _id_replica,
                     std::string _opName,
                     uint64_t _win_len,
                     uint64_t _slide_len,
                     uint64_t _lateness,
                     Win_Type_t _winType,
                     size_t _numWinPerBatch):
                     Basic_Replica(_opName, true),
                     lift_func(_lift_func),
                     comb_func(_comb_func),
                     key_extr(_key_extr),
                     id_replica(_id_replica),
                     win_len(_win_len),
                     slide_len(_slide_len),
                     lateness(_lateness),
                     winType(_winType),
                     numWinPerBatch(_numWinPerBatch),
                     size_buffers(0),
                     last_time(0),
                     ignored_tuples_cpu(0)
    {
        if (_winType == Win_Type_t::TB) { // time-based windows
            pane_len = compute_gcd(win_len, slide_len);
            win_len = win_len / pane_len;
            slide_len = slide_len / pane_len;
        }
        else { // count-based windows
            pane_len = 0;
        }
        int gpu_id;
        gpuErrChk(cudaGetDevice(&gpu_id));
        gpuErrChk(cudaDeviceGetAttribute(&numSMs, cudaDevAttrMultiProcessorCount, gpu_id));
#if (__CUDACC_VER_MAJOR__ >= 11) // at least CUDA 11
        gpuErrChk(cudaDeviceGetAttribute(&max_blocks_per_sm, cudaDevAttrMaxBlocksPerMultiprocessor, gpu_id));
#else
        max_blocks_per_sm = WF_GPU_MAX_BLOCKS_PER_SM;
#endif
        queue = new ff::MPMC_Ptr_Queue();
        queue->init(DEFAULT_BUFFER_CAPACITY);
        inTransit_counter = new std::atomic<int>(0);
        batchSize = (numWinPerBatch - 1) * slide_len + win_len;
        gpuErrChk(cudaMalloc(&ignored_tuples_gpu, sizeof(int)));
        gpuErrChk(cudaMemset(ignored_tuples_gpu, 0, sizeof(int)));
    }

    // Copy Constructor
    Ffat_Replica_GPU(const Ffat_Replica_GPU &_other):
                     Basic_Replica(_other),
                     lift_func(_other.lift_func),
                     comb_func(_other.comb_func),
                     key_extr(_other.key_extr),
                     id_replica(_other.id_replica),
                     win_len(_other.win_len),
                     slide_len(_other.slide_len),
                     pane_len(_other.pane_len),
                     lateness(_other.lateness),
                     winType(_other.winType),
                     numWinPerBatch(_other.numWinPerBatch),
                     batchSize(_other.batchSize),
                     size_buffers(0),
                     last_time(_other.last_time),
                     ignored_tuples_cpu(_other.ignored_tuples_cpu),
                     numSMs(_other.numSMs),
                     max_blocks_per_sm(_other.max_blocks_per_sm)
    {
        queue = new ff::MPMC_Ptr_Queue();
        queue->init(DEFAULT_BUFFER_CAPACITY);
        inTransit_counter = new std::atomic<int>(0);
        gpuErrChk(cudaMalloc(&ignored_tuples_gpu, sizeof(int)));
        gpuErrChk(cudaMemset(ignored_tuples_gpu, 0, sizeof(int)));
    }

    // Destructor
    ~Ffat_Replica_GPU() override
    {
        Batch_t<tuple_t> *batch = nullptr;
        while (queue->pop((void **) &batch)) {
            delete batch;
        }
        delete queue; // delete the recycling queue
        if (inTransit_counter != nullptr) {
            delete inTransit_counter;
        }
        if (ignored_tuples_gpu != nullptr) {
            gpuErrChk(cudaFree(ignored_tuples_gpu));
        }
        deallocate_arrays();
    }

    // svc method (utilized by the FastFlow runtime)
    void *svc(void *_in) override
    {
        this->startStatsRecording();
        Batch_t<tuple_t> *batch_input = reinterpret_cast<Batch_t<tuple_t> *>(_in);
        assert(last_time <= batch_input->getWatermark(id_replica)); // sanity check
        last_time = batch_input->getWatermark(id_replica);
        if (batch_input->isPunct()) { // if it is a punctuaton
            (this->emitter)->propagate_punctuation(batch_input->getWatermark(id_replica), this); // propagate the received punctuation
            deleteBatch_t(batch_input); // delete the punctuation
            return this->GO_ON;
        }
#if defined (WF_TRACING_ENABLED)
        (this->stats_record).inputs_received += batch_input->getSize();
        (this->stats_record).bytes_received += batch_input->getSize() * sizeof(tuple_t);
#endif
        if (winType == Win_Type_t::TB) { // time-based windows
            process_batch_tb(batch_input);
        }
        else { // count-based windows
            process_batch_cb(batch_input);
        }
        deleteBatch_t(batch_input); // delete the input batch
        this->endStatsRecording();
        return this->GO_ON;
    }

    // Process an input batch (count-based windows)
    void process_batch_cb(Batch_t<tuple_t> *_batch)
    {
        Batch_GPU_t<tuple_t> *batch_input = reinterpret_cast<Batch_GPU_t<tuple_t> *>(_batch);
        uint64_t watermark = batch_input->getWatermark(id_replica);
        allocate_arrays(batch_input->size); // check allocations of internal arrays
        int num_blocks = std::min((int) ceil(((double) batch_input->size) / WF_GPU_THREADS_PER_BLOCK), numSMs * max_blocks_per_sm);
        if constexpr (isKeyed) { // if keyed
            Lifting_Kernel_CB_Keyed<tuple_t, result_t, key_t, lift_func_gpu_t, keyextr_func_gpu_t>
                                   <<<num_blocks, WF_GPU_THREADS_PER_BLOCK, 0, batch_input->cudaStream>>>(batch_input->data_gpu,
                                                                                                          results_gpu,
                                                                                                          keys_gpu,
                                                                                                          batch_input->size,
                                                                                                          lift_func,
                                                                                                          key_extr);
            gpuErrChk(cudaPeekAtLastError());
            thrust::device_ptr<result_t> th_results_gpu = thrust::device_pointer_cast(results_gpu);
            thrust::device_ptr<key_t> th_keys_gpu = thrust::device_pointer_cast(keys_gpu);
            thrust::sort_by_key(thrust::cuda::par(alloc).on(batch_input->cudaStream),
                                th_keys_gpu,
                                th_keys_gpu + batch_input->size,
                                th_results_gpu);
            thrust::device_ptr<int> th_sequence_gpu = thrust::device_pointer_cast(sequence_gpu);
            thrust::sequence(thrust::cuda::par(alloc).on(batch_input->cudaStream),
                             th_sequence_gpu,
                             th_sequence_gpu + batch_input->size,
                             0);
            thrust::device_ptr<key_t> th_unique_keys_gpu = thrust::device_pointer_cast(unique_keys_gpu);
            thrust::device_ptr<int> th_unique_sequence_gpu = thrust::device_pointer_cast(unique_sequence_gpu);
            auto end1 = thrust::unique_by_key_copy(thrust::cuda::par(alloc).on(batch_input->cudaStream),
                                                   th_keys_gpu,
                                                   th_keys_gpu + batch_input->size,
                                                   th_sequence_gpu,
                                                   th_unique_keys_gpu,
                                                   th_unique_sequence_gpu);
            size_t num_keys = end1.first - th_unique_keys_gpu;
            assert(num_keys > 0); // sanity check
            gpuErrChk(cudaMemcpyAsync(pinned_unique_keys_cpu,
                                      unique_keys_gpu,
                                      sizeof(key_t) * num_keys,
                                      cudaMemcpyDeviceToHost,
                                      batch_input->cudaStream));
            gpuErrChk(cudaMemcpyAsync(pinned_unique_sequence_cpu,
                                      unique_sequence_gpu,
                                      sizeof(int) * num_keys,
                                      cudaMemcpyDeviceToHost,
                                      batch_input->cudaStream));
            gpuErrChk(cudaStreamSynchronize(batch_input->cudaStream));
            size_t offset_key = 0;
            for (size_t i=0; i<num_keys; i++) { // for each distinct key in the batch
                auto it = keyMap.find(pinned_unique_keys_cpu[i]); // find the corresponding key_descriptor (or allocate it if does not exist)
                if (it == keyMap.end()) {
                    auto p = keyMap.insert(std::make_pair(pinned_unique_keys_cpu[i], Key_Descriptor(winType,
                                                                                                    comb_func,
                                                                                                    batchSize, // tuples per batch
                                                                                                    numWinPerBatch,
                                                                                                    win_len,
                                                                                                    slide_len,
                                                                                                    pinned_unique_keys_cpu[i],
                                                                                                    numSMs,
                                                                                                    max_blocks_per_sm))); // create the state of the key
                    it = p.first;
                }
                Key_Descriptor &key_d = (*it).second;
                size_t num_panes_key = (i<num_keys-1) ? pinned_unique_sequence_cpu[i+1] - pinned_unique_sequence_cpu[i] : batch_input->size - pinned_unique_sequence_cpu[i];
                process_wins_cb(key_d, num_panes_key, watermark, offset_key);
                offset_key += num_panes_key;
            }
        }
        else { // if not keyed
            Lifting_Kernel_CB<tuple_t, result_t, lift_func_gpu_t>
                             <<<num_blocks, WF_GPU_THREADS_PER_BLOCK, 0, batch_input->cudaStream>>>(batch_input->data_gpu,
                                                                                                    results_gpu,
                                                                                                    batch_input->size,
                                                                                                    lift_func);
            gpuErrChk(cudaPeekAtLastError());
            gpuErrChk(cudaStreamSynchronize(batch_input->cudaStream));
            key_t empty_key;
            auto it = keyMap.find(empty_key); // there will be only one fictitious key
            if (it == keyMap.end()) {
                auto p = keyMap.insert(std::make_pair(empty_key, Key_Descriptor(winType,
                                                                                comb_func,
                                                                                batchSize,
                                                                                numWinPerBatch,
                                                                                win_len,
                                                                                slide_len,
                                                                                empty_key,
                                                                                numSMs,
                                                                                max_blocks_per_sm))); // create the state of the key
                it = p.first;
            }
            Key_Descriptor &key_d = (*it).second;
            process_wins_cb(key_d, batch_input->size, watermark);
        }
    }

    // Process count-based windows
    void process_wins_cb(Key_Descriptor &key_d,
                         size_t _num_items,
                         uint64_t _watermark,
                         size_t _offset_key=0)
    {
        size_t num = _num_items;
        size_t offset = 0;
        while (key_d.count + num >= key_d.count_triggerer) {
            if (key_d.count_triggerer == batchSize) { // first window
                (key_d.fatgpu).add_cb((results_gpu + _offset_key), (batchSize - key_d.count), *(key_d.cudaStream));
                num -= (batchSize - key_d.count);
                offset = (batchSize - key_d.count);
                key_d.count = batchSize;
                (key_d.fatgpu).build(*(key_d.cudaStream));
                Batch_GPU_t<result_t> *batch_output = allocateBatch_GPU_t<result_t>(numWinPerBatch, queue, inTransit_counter); // allocate the new batch
                (key_d.fatgpu).computeResults(batch_output->data_gpu, key_d.next_gwid, _watermark, batch_output->cudaStream);                
                this->doEmit_inplace(this->emitter, batch_output, this); // send the output batch once computed
                key_d.next_gwid += numWinPerBatch;
                key_d.count_triggerer += (slide_len*numWinPerBatch);
            }
            else { // not first window
                (key_d.fatgpu).add_cb((results_gpu + _offset_key + offset), (key_d.count_triggerer - key_d.count), *(key_d.cudaStream));
                num -= (key_d.count_triggerer - key_d.count);
                offset += (key_d.count_triggerer - key_d.count);
                key_d.count += (key_d.count_triggerer - key_d.count);
                (key_d.fatgpu).update((slide_len*numWinPerBatch), *(key_d.cudaStream));
                Batch_GPU_t<result_t> *batch_output = allocateBatch_GPU_t<result_t>(numWinPerBatch, queue, inTransit_counter); // allocate the new batch
                (key_d.fatgpu).computeResults(batch_output->data_gpu, key_d.next_gwid, _watermark, batch_output->cudaStream);
                this->doEmit_inplace(this->emitter, batch_output, this); // send the output batch once computed
                key_d.next_gwid += numWinPerBatch;
                key_d.count_triggerer += (slide_len*numWinPerBatch);
            }
        }
        if (num > 0) { // if there are remaining items to add
            (key_d.fatgpu).add_cb((results_gpu + _offset_key + offset), num, *(key_d.cudaStream));
            key_d.count += num;
        }
    }

    // Process an input batch (time-based windows)
    void process_batch_tb(Batch_t<tuple_t> *_batch)
    {
        Batch_GPU_t<tuple_t> *batch_input = reinterpret_cast<Batch_GPU_t<tuple_t> *>(_batch);
        uint64_t watermark = batch_input->getWatermark(id_replica);
        allocate_arrays(batch_input->size); // check allocations of internal arrays
        uint64_t first_pane_not_complete; // identifier of the first pane that is not complete (based on the current watermark and the fixed lateness)
        if (watermark >= lateness) {
            first_pane_not_complete = (watermark - lateness) / pane_len;
        }
        else {
            first_pane_not_complete = 0;
        }
        int num_blocks = std::min((int) ceil(((double) batch_input->size) / WF_GPU_THREADS_PER_BLOCK), numSMs * max_blocks_per_sm);
        if constexpr (isKeyed) { // if keyed
            Lifting_Kernel_TB_Keyed<tuple_t, result_t, key_t, lift_func_gpu_t, keyextr_func_gpu_t>
                                   <<<num_blocks, WF_GPU_THREADS_PER_BLOCK, 0, batch_input->cudaStream>>>(batch_input->data_gpu,
                                                                                                          results_gpu,
                                                                                                          infos_gpu,
                                                                                                          batch_input->size,
                                                                                                          pane_len,
                                                                                                          first_pane_not_complete,
                                                                                                          ignored_tuples_gpu,
                                                                                                          lift_func,
                                                                                                          key_extr);
        }
        else { // if not keyed
            Lifting_Kernel_TB<tuple_t, result_t, lift_func_gpu_t>
                             <<<num_blocks, WF_GPU_THREADS_PER_BLOCK, 0, batch_input->cudaStream>>>(batch_input->data_gpu,
                                                                                                    results_gpu,
                                                                                                    infos_gpu,
                                                                                                    batch_input->size,
                                                                                                    pane_len,
                                                                                                    first_pane_not_complete,
                                                                                                    ignored_tuples_gpu,
                                                                                                    lift_func);
        }
        gpuErrChk(cudaPeekAtLastError());
#if defined (WF_TRACING_ENABLED)
        gpuErrChk(cudaMemcpyAsync((void *) &ignored_tuples_cpu,
                                  ignored_tuples_gpu,
                                  sizeof(int),
                                  cudaMemcpyDeviceToHost,
                                  batch_input->cudaStream));
#endif
        thrust::device_ptr<result_t> th_results_gpu = thrust::device_pointer_cast(results_gpu);
        thrust::device_ptr<info_t<key_t>> th_info_gpu = thrust::device_pointer_cast(infos_gpu);
        lessThan_func_gpu_t<info_t<key_t>, key_t> lessThan;
        thrust::sort_by_key(thrust::cuda::par(alloc).on(batch_input->cudaStream),
                            th_info_gpu,
                            th_info_gpu + batch_input->size,
                            th_results_gpu,
                            lessThan);
        thrust::device_ptr<result_t> th_new_results_gpu = thrust::device_pointer_cast(new_results_gpu);
        thrust::device_ptr<info_t<key_t>> th_new_infos_gpu= thrust::device_pointer_cast(new_infos_gpu);
        equalTo_func_gpu_t<info_t<key_t>, key_t> equalTo;
        thrust_comb_func_t<comb_func_gpu_t, result_t> thrust_reduce_func(comb_func);
        auto end1 = thrust::reduce_by_key(thrust::cuda::par(alloc).on(batch_input->cudaStream),
                                          th_info_gpu,
                                          th_info_gpu + batch_input->size,
                                          th_results_gpu,
                                          th_new_infos_gpu,
                                          th_new_results_gpu,
                                          equalTo,
                                          thrust_reduce_func);
        size_t size_reduced = end1.first - th_new_infos_gpu;
        assert(batch_input->size >= size_reduced); // sanity check
        if constexpr (isKeyed) { // if keyed
            thrust::device_ptr<int> th_sequence_gpu = thrust::device_pointer_cast(sequence_gpu);
            thrust::sequence(thrust::cuda::par(alloc).on(batch_input->cudaStream),
                             th_sequence_gpu,
                             th_sequence_gpu + size_reduced,
                             0);
            thrust::device_ptr<info_t<key_t>> th_unique_infos_gpu = thrust::device_pointer_cast(unique_infos_gpu);
            thrust::device_ptr<int> th_unique_sequence_gpu = thrust::device_pointer_cast(unique_sequence_gpu);
            equalTo2_func_gpu_t<info_t<key_t>> equalTo2;
            auto end2 = thrust::unique_by_key_copy(thrust::cuda::par(alloc).on(batch_input->cudaStream),
                                                   th_new_infos_gpu,
                                                   th_new_infos_gpu + size_reduced,
                                                   th_sequence_gpu,
                                                   th_unique_infos_gpu,
                                                   th_unique_sequence_gpu,
                                                   equalTo2);
            size_t num_keys = end2.first - th_unique_infos_gpu;
            assert(num_keys > 0); // sanity check
            gpuErrChk(cudaMemcpyAsync(pinned_unique_infos_cpu,
                                      unique_infos_gpu,
                                      sizeof(info_t<key_t>) * num_keys,
                                      cudaMemcpyDeviceToHost,
                                      batch_input->cudaStream));
            gpuErrChk(cudaMemcpyAsync(pinned_unique_sequence_cpu,
                                      unique_sequence_gpu,
                                      sizeof(int) * num_keys,
                                      cudaMemcpyDeviceToHost,
                                      batch_input->cudaStream));
            gpuErrChk(cudaStreamSynchronize(batch_input->cudaStream));
#if defined (WF_TRACING_ENABLED)
            stats_record.inputs_ignored = ignored_tuples_cpu;
#endif
            size_t offset = 0;
            for (size_t i=0; i<num_keys; i++) { // for each distinct key in the batch
                auto it = keyMap.find(pinned_unique_infos_cpu[i].key); // find the corresponding key_descriptor (or allocate it if does not exist)
                if (it == keyMap.end()) {
                    auto p = keyMap.insert(std::make_pair(pinned_unique_infos_cpu[i].key, Key_Descriptor(winType,
                                                                                                         comb_func,
                                                                                                         batchSize, // panes per batch
                                                                                                         numWinPerBatch,
                                                                                                         win_len,
                                                                                                         slide_len,
                                                                                                         pinned_unique_infos_cpu[i].key,
                                                                                                         numSMs,
                                                                                                         max_blocks_per_sm))); // create the state of the key
                    it = p.first;
                }
                Key_Descriptor &key_d = (*it).second;
                key_d.next_pane_id = first_pane_not_complete;
                size_t num_panes_key = (i<num_keys-1) ? pinned_unique_sequence_cpu[i+1] - pinned_unique_sequence_cpu[i] : size_reduced - pinned_unique_sequence_cpu[i];
                process_wins_tb(key_d, num_panes_key, watermark, pinned_unique_infos_cpu[i].pane_id, offset);
                offset += num_panes_key;
            }
        }
        else { // if not keyed
            gpuErrChk(cudaMemcpyAsync(pinned_unique_infos_cpu, // copy the highest pane_id in the batch here!
                                      new_infos_gpu,
                                      sizeof(info_t<key_t>),
                                      cudaMemcpyDeviceToHost,
                                      batch_input->cudaStream));
            gpuErrChk(cudaStreamSynchronize(batch_input->cudaStream));
#if defined (WF_TRACING_ENABLED)
            stats_record.inputs_ignored = ignored_tuples_cpu;
#endif
            key_t empty_key;
            size_t offset = 0;
            auto it = keyMap.find(empty_key); // there will be only one fictitious key
            if (it == keyMap.end()) {
                auto p = keyMap.insert(std::make_pair(empty_key, Key_Descriptor(winType,
                                                                                comb_func,
                                                                                batchSize, // panes per batch
                                                                                numWinPerBatch,
                                                                                win_len,
                                                                                slide_len,
                                                                                empty_key,
                                                                                numSMs,
                                                                                max_blocks_per_sm))); // create the state of the key
                it = p.first;
            }
            Key_Descriptor &key_d = (*it).second;
            key_d.next_pane_id = first_pane_not_complete;
            process_wins_tb(key_d, size_reduced, watermark, pinned_unique_infos_cpu[0].pane_id);
        }
    }

    // Process time-based windows of a given key
    void process_wins_tb(Key_Descriptor &key_d,
                         size_t _num_panes,
                         uint64_t _watermark,
                         size_t _newest_pane_id,
                         size_t _offset=0)
    {
        (key_d.pending_queue)->push_panes(new_results_gpu + _offset, new_infos_gpu + _offset, _num_panes, _newest_pane_id, *(key_d.cudaStream));
        while (key_d.pane_id_triggerer < key_d.next_pane_id) {
            if (!key_d.firstWinDone) { // first window to be done
                assert(batchSize <= (key_d.pending_queue)->getNumPendingPanes()); // sanity check
                (key_d.pending_queue)->pop_and_add(batchSize, key_d.fatgpu, *(key_d.cudaStream));
                (key_d.fatgpu).build(*(key_d.cudaStream));
                key_d.firstWinDone = true;
            }
            else {
                assert((slide_len*numWinPerBatch) <= (key_d.pending_queue)->getNumPendingPanes()); // sanity check
                (key_d.pending_queue)->pop_and_add((slide_len*numWinPerBatch), key_d.fatgpu, *(key_d.cudaStream));
                (key_d.fatgpu).update((slide_len*numWinPerBatch), *(key_d.cudaStream));
            }
            Batch_GPU_t<result_t> *batch_output = allocateBatch_GPU_t<result_t>(numWinPerBatch, queue, inTransit_counter); // allocate the new batch
            (key_d.fatgpu).computeResults(batch_output->data_gpu, key_d.next_gwid, _watermark, batch_output->cudaStream);
            this->doEmit_inplace(this->emitter, batch_output, this); // send the output batch once computed
            key_d.next_gwid += numWinPerBatch;
            key_d.pane_id_triggerer += (slide_len*numWinPerBatch);
        }
    }

    // svc_end (utilized by the FastFlow runtime)
    void svc_end() override
    {
        for (auto &p: keyMap) {
            Key_Descriptor &key_d = p.second;
            gpuErrChk(cudaStreamSynchronize(*(key_d.cudaStream))); // <- not really needed, but safer
        }
    }

    // Configure the Ffat_Windows_GPU replica to receive batches instead of individual inputs
    void receiveBatches(bool _input_batching) override {}

    // Get the number of ignored tuples
    size_t getNumIgnoredTuples() const
    {
        return (size_t) ignored_tuples_cpu;
    }

    Ffat_Replica_GPU(Ffat_Replica_GPU &&) = delete; ///< Move constructor is deleted
    Ffat_Replica_GPU &operator=(const Ffat_Replica_GPU &) = delete; ///< Copy assignment operator is deleted
    Ffat_Replica_GPU &operator=(Ffat_Replica_GPU &&) = delete; ///< Move assignment operator is deleted
};

} // namespace wf

#endif
