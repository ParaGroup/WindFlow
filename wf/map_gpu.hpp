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
 *  @file    map_gpu.hpp
 *  @author  Gabriele Mencagli
 *  
 *  @brief Map operator on GPU
 *  
 *  @section Map on GPU (Description)
 *  
 *  This file implements the Map operator able to execute streaming transformations
 *  producing one output per input. The operator offloads the processing on a GPU
 *  device.
 */ 

#ifndef MAP_GPU_H
#define MAP_GPU_H

/// includes
#include<string>
#include<functional>
#include<pthread.h>
#include<batch_gpu_t.hpp>
#if defined (WF_TRACING_ENABLED)
    #include<stats_record.hpp>
#endif
#include<basic_emitter.hpp>
#include<basic_operator.hpp>
#include<tbb/concurrent_unordered_map.h>

namespace wf {

//@cond DOXY_IGNORE

// CUDA Kernel: Stateless_MAPGPU_Kernel
template<typename tuple_t, typename mapgpu_func_t>
__global__ void Stateless_MAPGPU_Kernel(batch_item_gpu_t<tuple_t> *data_gpu,
                                        size_t len,
                                        int num_active_thread_per_warp,
                                        mapgpu_func_t func_gpu)
{
    int id = threadIdx.x + blockIdx.x * blockDim.x; // id of the thread in the kernel
    int num_threads = gridDim.x * blockDim.x; // number of threads in the kernel
    int threads_per_worker = warpSize / num_active_thread_per_warp; // number of threads composing a worker entity
    int num_workers = num_threads / threads_per_worker; // number of workers
    int id_worker = id / threads_per_worker; // id of the worker corresponding to this thread
    if (id % threads_per_worker == 0) { // only "num_active_thread_per_warp" threads per warp work, the others are idle
        for (size_t i=id_worker; i<len; i+=num_workers) {
            func_gpu(data_gpu[i].tuple);
        }
    }
}

// CUDA Kernel: Stateful_MAPGPU_Kernel
template<typename tuple_t, typename state_t, typename mapgpu_func_t>
__global__ void Stateful_MAPGPU_Kernel(batch_item_gpu_t<tuple_t> *data_gpu,
                                       int *map_idxs_gpu,
                                       int *start_idxs_gpu,
                                       state_t **states,
                                       int num_dist_keys,
                                       int num_active_thread_per_warp,
                                       mapgpu_func_t func_gpu)
{
    int id = threadIdx.x + blockIdx.x * blockDim.x; // id of the thread in the kernel
    int num_threads = gridDim.x * blockDim.x; // number of threads in the kernel
    int threads_per_worker = warpSize / num_active_thread_per_warp; // number of threads composing a worker entity
    int num_workers = num_threads / threads_per_worker; // number of workers
    int id_worker = id / threads_per_worker; // id of the worker corresponding to this thread
    if (id % threads_per_worker == 0) { // only "num_active_thread_per_warp" threads per warp work, the others are idle
        for (int id_key=id_worker; id_key<num_dist_keys; id_key+=num_workers) {
            size_t idx = start_idxs_gpu[id_key];
            while (idx != -1) { // execute all the inputs with key in the input batch
                func_gpu(data_gpu[idx].tuple, *(states[id_key]));
                idx = map_idxs_gpu[idx];
            }
        }
    }
}

// CUDA callback
void CUDART_CB unlock_callback_map(void *data)
{
    pthread_spinlock_t *lock = (pthread_spinlock_t *) data;
    pthread_spin_unlock(lock);
}

// class MapGPU_Replica (stateful version)
template<typename mapgpu_func_t, typename key_t>
class MapGPU_Replica: public ff::ff_monode
{
private:
    template<typename T1, typename T2> friend class Map_GPU; // friendship with all the instances of the Map_GPU template
    mapgpu_func_t func; // functional logic used by the Map_GPU replica
    using tuple_t = decltype(get_tuple_t_MapGPU(func)); // extracting the tuple_t type and checking the admissible signatures
    using state_t = decltype(get_state_t_MapGPU(func)); // extracting the state_t type and checking the admissible signatures
    size_t id_replica; // identifier of the Map_GPU replica
    tbb::concurrent_unordered_map<key_t, wrapper_state_t<state_t>> *keymap; // pointer to the concurrent hashtable keeping the states of the keys
    pthread_spinlock_t *spinlock; // pointer to the spinlock used by all the replicas of the Map_GPU
    std::string opName; // name of the Map_GPU containing the replica
    bool terminated; // true if the Map_GPU replica has finished its work
    Basic_Emitter *emitter; // pointer to the used emitter
    struct record_t // struct record_t
    {
        size_t size; // size of the arrays in the structure
        state_t **state_ptrs_cpu=nullptr; // pinned host array of pointers to states
        state_t **state_ptrs_gpu=nullptr; // GPU array of pointers to states

        // Constructor
        record_t(size_t _size):
                 size(_size)
        {
            gpuErrChk(cudaMallocHost(&state_ptrs_cpu, sizeof(state_t *) * size));
            gpuErrChk(cudaMalloc(&state_ptrs_gpu, sizeof(state_t *) * size));
        }

        // Destructor
        ~record_t()
        {
            gpuErrChk(cudaFreeHost(state_ptrs_cpu));
            gpuErrChk(cudaFree(state_ptrs_gpu));
        }

        // Resize the internal arrays of the record
        void resize(size_t _newsize)
        {
            if (_newsize > size) {
                size = _newsize;
                gpuErrChk(cudaFreeHost(state_ptrs_cpu));
                gpuErrChk(cudaFree(state_ptrs_gpu));
                gpuErrChk(cudaMallocHost(&state_ptrs_cpu, sizeof(state_t *) * size));
                gpuErrChk(cudaMalloc(&state_ptrs_gpu, sizeof(state_t *) * size));
            }
        }
    };
    Batch_GPU_t<tuple_t> *batch_tobe_sent; // pointer to the output batch to be sent
    std::vector<record_t *> records; // vector of pointers to record structures used circularly)
    size_t id_r; // identifier used for overlapping purposes
    int numSMs; // number of Stream Multiprocessor of the GPU
    int max_threads_per_sm; // maximum number of threads resident on each Stream Multiprocessor of the GPU
    int max_blocks_per_sm; // maximum number of blocks resident on each Stream Multiprocessor of the GPU
    int threads_per_warp; // number of threads per warp of the GPU
    Execution_Mode_t execution_mode; // execution mode of the Map_GPU replica
#if defined (WF_TRACING_ENABLED)
    Stats_Record stats_record;
    double avg_td_us = 0;
    double avg_ts_us = 0;
    volatile uint64_t startTD, startTS, endTD, endTS;
#endif

public:
    // Constructor
    MapGPU_Replica(mapgpu_func_t _func,
                   size_t _id_replica,
                   std::string _opName,
                   tbb::concurrent_unordered_map<key_t, wrapper_state_t<state_t>> *_keymap,
                   pthread_spinlock_t *_spinlock):
                   func(_func),
                   id_replica(_id_replica),
                   keymap(_keymap),
                   spinlock(_spinlock),
                   opName(_opName),
                   terminated(false),
                   emitter(nullptr),
                   batch_tobe_sent(nullptr),
                   records(2, nullptr),
                   id_r(0),
                   execution_mode(Execution_Mode_t::DEFAULT)
    {
        gpuErrChk(cudaDeviceGetAttribute(&numSMs, cudaDevAttrMultiProcessorCount, 0));
        gpuErrChk(cudaDeviceGetAttribute(&max_threads_per_sm, cudaDevAttrMaxThreadsPerMultiProcessor, 0));
#if (__CUDACC_VER_MAJOR__ >= 11)
        gpuErrChk(cudaDeviceGetAttribute(&max_blocks_per_sm, cudaDevAttrMaxBlocksPerMultiprocessor, 0));
#else
        max_blocks_per_sm = WF_GPU_MAX_BLOCKS_PER_SM;
#endif
        gpuErrChk(cudaDeviceGetAttribute(&threads_per_warp, cudaDevAttrWarpSize, 0));
    }

    // Copy Constructor
    MapGPU_Replica(const MapGPU_Replica &_other):
                   func(_other.func),
                   id_replica(_other.id_replica),
                   keymap(_other.keymap),
                   spinlock(_other.spinlock),
                   opName(_other.opName),
                   terminated(_other.terminated),
                   batch_tobe_sent(nullptr),
                   records(2, nullptr),
                   id_r(_other.id_r),
                   numSMs(_other.numSMs),
                   max_threads_per_sm(_other.max_threads_per_sm),
                   max_blocks_per_sm(_other.max_blocks_per_sm),
                   threads_per_warp(_other.threads_per_warp),
                   execution_mode(_other.execution_mode)
    {
        if (_other.emitter == nullptr) {
            emitter = nullptr;
        }
        else {
            emitter = (_other.emitter)->clone(); // clone the emitter if it exists
        }
#if defined (WF_TRACING_ENABLED)
        stats_record = _other.stats_record;
#endif
    }

    // Move Constructor
    MapGPU_Replica(MapGPU_Replica &&_other):
                   func(std::move(_other.func)),
                   id_replica(_other.id_replica),
                   keymap(std::exchange(_other.keymap, nullptr)),
                   spinlock(std::exchange(_other.spinlock, nullptr)),
                   opName(std::move(_other.opName)),
                   terminated(_other.terminated),
                   emitter(std::exchange(_other.emitter, nullptr)),
                   batch_tobe_sent(std::exchange(_other.batch_tobe_sent, nullptr)),
                   records(std::move(_other.records)),
                   id_r(_other.id_r),
                   numSMs(_other.numSMs),
                   max_threads_per_sm(_other.max_threads_per_sm),
                   max_blocks_per_sm(_other.max_blocks_per_sm),
                   threads_per_warp(_other.threads_per_warp),
                   execution_mode(_other.execution_mode)
    {
#if defined (WF_TRACING_ENABLED)
        stats_record = std::move(_other.stats_record);
#endif
    }

    // Destructor
    ~MapGPU_Replica()
    {
        if (emitter != nullptr) {
            delete emitter;
        }
        assert(batch_tobe_sent == nullptr); // sanity check
        for (auto *p: records) {
            if (p != nullptr) {
                delete p;
            }
        }
        if (id_replica == 0) { // only the first replica deletes the spinlock and keymap
            delete spinlock;
            delete keymap; // here we destroy and deallocate all the keyed states
        }
    }

    // Copy Assignment Operator
    MapGPU_Replica &operator=(const MapGPU_Replica &_other)
    {
        if (this != &_other) {
            func = _other.func;
            id_replica = _other.id_replica;
            keymap = _other.keymap;
            spinlock = _other.spinlock;
            opName = _other.opName;
            terminated = _other.terminated;
            if (emitter != nullptr) {
                delete emitter;
            }
            if (_other.emitter == nullptr) {
                emitter = nullptr;
            }
            else {
                emitter = (_other.emitter)->clone(); // clone the emitter if it exists
            }
            if (batch_tobe_sent != nullptr) {
                delete batch_tobe_sent;
            }
            batch_tobe_sent = nullptr;
            for (auto *p: records) {
                if (p != nullptr) {
                    delete p;
                }
            }
            records = { nullptr, nullptr };
            id_r =  _other.id_r;
            numSMs = _other.numSMs;
            max_threads_per_sm = _other.max_threads_per_sm;
            max_blocks_per_sm = _other.max_blocks_per_sm;
            threads_per_warp = _other.threads_per_warp;
            execution_mode = _other.execution_mode;
#if defined (WF_TRACING_ENABLED)
            stats_record = _other.stats_record;
#endif
        }
        return *this;
    }

    // Move Assignment Operator
    MapGPU_Replica &operator=(MapGPU_Replica &_other)
    {
        func = std::move(_other.func);
        id_replica = _other.id_replica;
        keymap = std::exchange(_other.keymap, nullptr);
        spinlock = std::exchange(_other.spinlock, nullptr);
        opName = std::move(_other.opName);
        terminated = _other.terminated;
        if (emitter != nullptr) {
            delete emitter;
        }
        emitter = std::exchange(_other.emitter, nullptr);
        if (batch_tobe_sent != nullptr) {
            delete batch_tobe_sent;
        }
        batch_tobe_sent = std::exchange(_other.batch_tobe_sent, nullptr);
        for (auto *p: records) {
            if (p != nullptr) {
                delete p;
            }
        }
        records = std::move(_other.records);
        id_r =  _other.id_r;
        numSMs = _other.numSMs;
        max_threads_per_sm = _other.max_threads_per_sm;
        max_blocks_per_sm = _other.max_blocks_per_sm;
        threads_per_warp = _other.threads_per_warp;
        execution_mode = _other.execution_mode;
#if defined (WF_TRACING_ENABLED)
        stats_record = std::move(_other.stats_record);
#endif
    }

    // svc_init (utilized by the FastFlow runtime)
    int svc_init() override
    {
#if defined (WF_TRACING_ENABLED)
        stats_record = Stats_Record(opName, std::to_string(id_replica), false, true);
#endif
        return 0;
    }

    // svc (utilized by the FastFlow runtime)
    void *svc(void *_in) override
    {
#if defined (WF_TRACING_ENABLED)
        startTS = current_time_nsecs();
        if (stats_record.inputs_received == 0) {
            startTD = current_time_nsecs();
        }
#endif
        Batch_GPU_t<decltype(get_tuple_t_MapGPU(func))> *input = reinterpret_cast<Batch_GPU_t<decltype(get_tuple_t_MapGPU(func))> *>(_in);
        if (input->isPunct()) { // if it is a punctuaton
            if (batch_tobe_sent != nullptr) {
                gpuErrChk(cudaStreamSynchronize(batch_tobe_sent->cudaStream));
                emitter->emit_inplace(batch_tobe_sent, this); // send the output batch once computed
                batch_tobe_sent = nullptr;
            }
            emitter->generate_punctuation(input->getWatermark(id_replica), this); // propagate the received punctuation
            deleteBatch_t(input); // delete the punctuation
            return this->GO_ON;
        }
#if defined (WF_TRACING_ENABLED)
        stats_record.inputs_received += input->size;
        stats_record.bytes_received += input->size * sizeof(tuple_t);
        stats_record.outputs_sent += input->size;
        stats_record.bytes_sent += input->size * sizeof(tuple_t);
#endif
        if (records[id_r] == nullptr) {
            records[id_r] = new record_t(input->original_size);
        }
        else {
            records[id_r]->resize(input->original_size);
        }
        key_t *dist_keys = reinterpret_cast<key_t *>(input->dist_keys_cpu);
        for (size_t i=0; i<input->num_dist_keys; i++) { // prepare the states
            auto key = dist_keys[i];
            auto it = keymap->find(key);
            if (it == keymap->end()) {
                wrapper_state_t<decltype(get_state_t_MapGPU(func))> newstate;
                auto res = keymap->insert(std::move(std::make_pair(key, std::move(newstate))));
                records[id_r]->state_ptrs_cpu[i] = ((*(res.first)).second).state_gpu;
            }
            else {
                records[id_r]->state_ptrs_cpu[i] = ((*it).second).state_gpu;
            }
        }
        gpuErrChk(cudaMemcpyAsync(records[id_r]->state_ptrs_gpu, records[id_r]->state_ptrs_cpu, input->num_dist_keys * sizeof(state_t *), cudaMemcpyHostToDevice, input->cudaStream));
        int warps_per_block = ((max_threads_per_sm / max_blocks_per_sm) / threads_per_warp); // launch the kernel to compute the results
        int tot_num_warps = warps_per_block * max_blocks_per_sm * numSMs;
        int32_t x = (int32_t) std::ceil(((double) input->num_dist_keys) / tot_num_warps); // compute how many threads should be active per warps
        if (x > 1) {
            x = next_power_of_two(x);
        }
        int num_active_thread_per_warp = std::min(x, threads_per_warp);
        int num_blocks = std::min((int) ceil(((double) input->num_dist_keys) / warps_per_block), numSMs * max_blocks_per_sm);
        if (batch_tobe_sent != nullptr) {
            gpuErrChk(cudaStreamSynchronize(batch_tobe_sent->cudaStream));
            emitter->emit_inplace(batch_tobe_sent, this); // send the output batch once computed
        }
        pthread_spin_lock(spinlock); // acquire the lock
        Stateful_MAPGPU_Kernel<decltype(get_tuple_t_MapGPU(func)), decltype(get_state_t_MapGPU(func)), mapgpu_func_t>
                              <<<num_blocks, warps_per_block*threads_per_warp, 0, input->cudaStream>>>(input->data_gpu,
                                                                                                       input->map_idxs_gpu,
                                                                                                       input->start_idxs_gpu,
                                                                                                       records[id_r]->state_ptrs_gpu,
                                                                                                       input->num_dist_keys,
                                                                                                       num_active_thread_per_warp,
                                                                                                       func);
        gpuErrChk(cudaPeekAtLastError());
        gpuErrChk(cudaLaunchHostFunc(input->cudaStream, unlock_callback_map, (void *) spinlock));
        batch_tobe_sent = input;
        id_r = (id_r + 1) % 2;
#if defined (WF_TRACING_ENABLED)
        endTS = current_time_nsecs();
        endTD = current_time_nsecs();
        double elapsedTS_us = ((double) (endTS - startTS)) / 1000;
        avg_ts_us += (1.0 / stats_record.inputs_received) * (elapsedTS_us - avg_ts_us);
        double elapsedTD_us = ((double) (endTD - startTD)) / 1000;
        avg_td_us += (1.0 / stats_record.inputs_received) * (elapsedTD_us - avg_td_us);
        stats_record.service_time = std::chrono::duration<double, std::micro>(avg_ts_us);
        stats_record.eff_service_time = std::chrono::duration<double, std::micro>(avg_td_us);
        startTD = current_time_nsecs();
#endif
        return this->GO_ON;
    }

    // EOS management (utilized by the FastFlow runtime)
    void eosnotify(ssize_t id) override
    {
        if (batch_tobe_sent != nullptr) {
            gpuErrChk(cudaStreamSynchronize(batch_tobe_sent->cudaStream));
            emitter->emit_inplace(batch_tobe_sent, this); // send the output batch once computed
            batch_tobe_sent = nullptr;
        }
        emitter->flush(this); // call the flush of the emitter
        terminated = true;
#if defined (WF_TRACING_ENABLED)
        stats_record.setTerminated();
#endif
    }

    // Configure the Map_GPU replica to receive batches (this method does nothing)
    void receiveBatches(bool _input_batching) {}

    // Set the emitter used to route outputs from the Map_GPU replica
    void setEmitter(Basic_Emitter *_emitter)
    {
        emitter = _emitter;
    }

    // Check the termination of the Map_GPU replica
    bool isTerminated() const
    {
        return terminated;
    }

    // Set the execution mode of the Map_GPU replica
    void setExecutionMode(Execution_Mode_t _execution_mode)
    {
        execution_mode = _execution_mode;
    }

#if defined (WF_TRACING_ENABLED)
    // Get a copy of the Stats_Record of the Map_GPU replica
    Stats_Record getStatsRecord() const
    {
        return stats_record;
    }
#endif
};

// class MapGPU_Replica (stateless version)
template<typename mapgpu_func_t>
class MapGPU_Replica<mapgpu_func_t, empty_key_t>: public ff::ff_monode
{
private:
    mapgpu_func_t func; // functional logic used by the Map_GPU replica
    using tuple_t = decltype(get_tuple_t_MapGPU(func)); // extracting the tuple_t type and checking the admissible signatures
    size_t id_replica; // identifier of the Map_GPU replica
    std::string opName; // name of the Map_GPU containing the replica
    bool terminated; // true if the Map_GPU replica has finished its work
    Basic_Emitter *emitter; // pointer to the used emitter   
    int numSMs; // number of Stream Multiprocessor of the GPU
    int max_threads_per_sm; // maximum number of threads resident on each Stream Multiprocessor of the GPU
    int max_blocks_per_sm; // maximum number of blocks resident on each Stream Multiprocessor of the GPU
    int threads_per_warp; // number of threads per warp of the GPU
    Execution_Mode_t execution_mode; // execution mode of the Map_GPU replica
#if defined (WF_TRACING_ENABLED)
    Stats_Record stats_record;
    double avg_td_us = 0;
    double avg_ts_us = 0;
    volatile uint64_t startTD, startTS, endTD, endTS;
#endif

public:
    // Constructor
    MapGPU_Replica(mapgpu_func_t _func,
                   size_t _id_replica,
                   std::string _opName):
                   func(_func),
                   id_replica(_id_replica),
                   opName(_opName),
                   terminated(false),
                   emitter(nullptr),
                   execution_mode(Execution_Mode_t::DEFAULT)
    {
        gpuErrChk(cudaDeviceGetAttribute(&numSMs, cudaDevAttrMultiProcessorCount, 0));
        gpuErrChk(cudaDeviceGetAttribute(&max_threads_per_sm, cudaDevAttrMaxThreadsPerMultiProcessor, 0));
#if (__CUDACC_VER_MAJOR__ >= 11)
        gpuErrChk(cudaDeviceGetAttribute(&max_blocks_per_sm, cudaDevAttrMaxBlocksPerMultiprocessor, 0));
#else
        max_blocks_per_sm = WF_GPU_MAX_BLOCKS_PER_SM;
#endif
        gpuErrChk(cudaDeviceGetAttribute(&threads_per_warp, cudaDevAttrWarpSize, 0));
    }

    // Copy Constructor
    MapGPU_Replica(const MapGPU_Replica &_other):
                   func(_other.func),
                   id_replica(_other.id_replica),
                   opName(_other.opName),
                   terminated(_other.terminated),
                   numSMs(_other.numSMs),
                   max_threads_per_sm(_other.max_threads_per_sm),
                   max_blocks_per_sm(_other.max_blocks_per_sm),
                   threads_per_warp(_other.threads_per_warp),
                   execution_mode(_other.execution_mode)
    {
        if (_other.emitter == nullptr) {
            emitter = nullptr;
        }
        else {
            emitter = (_other.emitter)->clone(); // clone the emitter if it exists
        }
#if defined (WF_TRACING_ENABLED)
        stats_record = _other.stats_record;
#endif
    }

    // Move Constructor
    MapGPU_Replica(MapGPU_Replica &&_other):
                   func(std::move(_other.func)),
                   id_replica(_other.id_replica),
                   opName(std::move(_other.opName)),
                   terminated(_other.terminated),
                   emitter(std::exchange(_other.emitter, nullptr)),
                   numSMs(_other.numSMs),
                   max_threads_per_sm(_other.max_threads_per_sm),
                   max_blocks_per_sm(_other.max_blocks_per_sm),
                   threads_per_warp(_other.threads_per_warp),
                   execution_mode(_other.execution_mode)
    {
#if defined (WF_TRACING_ENABLED)
        stats_record = std::move(_other.stats_record);
#endif
    }

    // Destructor
    ~MapGPU_Replica()
    {
        if (emitter != nullptr) {
            delete emitter;
        }
    }

    // Copy Assignment Operator
    MapGPU_Replica &operator=(const MapGPU_Replica &_other)
    {
        if (this != &_other) {
            func = _other.func;
            id_replica = _other.id_replica;
            opName = _other.opName;
            terminated = _other.terminated;
            if (emitter != nullptr) {
                delete emitter;
            }
            if (_other.emitter == nullptr) {
                emitter = nullptr;
            }
            else {
                emitter = (_other.emitter)->clone(); // clone the emitter if it exists
            }
            numSMs = _other.numSMs;
            max_threads_per_sm = _other.max_threads_per_sm;
            max_blocks_per_sm = _other.max_blocks_per_sm;
            threads_per_warp = _other.threads_per_warp;
            execution_mode = _other.execution_mode;
#if defined (WF_TRACING_ENABLED)
            stats_record = _other.stats_record;
#endif
        }
        return *this;
    }

    // Move Assignment Operator
    MapGPU_Replica &operator=(MapGPU_Replica &_other)
    {
        func = std::move(_other.func);
        id_replica = _other.id_replica;
        opName = std::move(_other.opName);
        terminated = _other.terminated;
        if (emitter != nullptr) {
            delete emitter;
        }
        emitter = std::exchange(_other.emitter, nullptr);
        numSMs = _other.numSMs;
        max_threads_per_sm = _other.max_threads_per_sm;
        max_blocks_per_sm = _other.max_blocks_per_sm;
        threads_per_warp = _other.threads_per_warp;
        execution_mode = _other.execution_mode;
#if defined (WF_TRACING_ENABLED)
        stats_record = std::move(_other.stats_record);
#endif
    }

    // svc_init (utilized by the FastFlow runtime)
    int svc_init() override
    {
#if defined (WF_TRACING_ENABLED)
        stats_record = Stats_Record(opName, std::to_string(id_replica), false, true);
#endif
        return 0;
    }

    // svc (utilized by the FastFlow runtime)
    void *svc(void *_in) override
    {
#if defined (WF_TRACING_ENABLED)
        startTS = current_time_nsecs();
        if (stats_record.inputs_received == 0) {
            startTD = current_time_nsecs();
        }
#endif
        Batch_GPU_t<decltype(get_tuple_t_MapGPU(func))> *input = reinterpret_cast<Batch_GPU_t<decltype(get_tuple_t_MapGPU(func))> *>(_in);
        if (input->isPunct()) { // if it is a punctuaton
            emitter->generate_punctuation(input->getWatermark(id_replica), this); // propagate the received punctuation
            deleteBatch_t(input); // delete the punctuation
            return this->GO_ON;
        }
#if defined (WF_TRACING_ENABLED)
        stats_record.inputs_received += input->size;
        stats_record.bytes_received += input->size * sizeof(tuple_t);
        stats_record.outputs_sent += input->size;
        stats_record.bytes_sent += input->size * sizeof(tuple_t);
#endif
        int warps_per_block = ((max_threads_per_sm / max_blocks_per_sm) / threads_per_warp); // launch the kernel to compute the results
        int tot_num_warps = warps_per_block * max_blocks_per_sm * numSMs;
        int32_t x = (int32_t) ceil(((double) (input->size)) / tot_num_warps); // compute how many threads should be active per warps
        if (x > 1) {
            x = next_power_of_two(x);
        }
        int num_active_thread_per_warp = std::min(x, threads_per_warp);
        int num_blocks = std::min((int) ceil(((double) (input->size)) / warps_per_block), numSMs * max_blocks_per_sm);
        Stateless_MAPGPU_Kernel<decltype(get_tuple_t_MapGPU(func)), mapgpu_func_t>
                               <<<num_blocks, warps_per_block*threads_per_warp, 0, input->cudaStream>>>(input->data_gpu,
                                                                                                        input->size,
                                                                                                        num_active_thread_per_warp,
                                                                                                        func);
        gpuErrChk(cudaPeekAtLastError());
        gpuErrChk(cudaStreamSynchronize(input->cudaStream));
        emitter->emit_inplace(input, this); // send the output batch once computed
#if defined (WF_TRACING_ENABLED)
        endTS = current_time_nsecs();
        endTD = current_time_nsecs();
        double elapsedTS_us = ((double) (endTS - startTS)) / 1000;
        avg_ts_us += (1.0 / stats_record.inputs_received) * (elapsedTS_us - avg_ts_us);
        double elapsedTD_us = ((double) (endTD - startTD)) / 1000;
        avg_td_us += (1.0 / stats_record.inputs_received) * (elapsedTD_us - avg_td_us);
        stats_record.service_time = std::chrono::duration<double, std::micro>(avg_ts_us);
        stats_record.eff_service_time = std::chrono::duration<double, std::micro>(avg_td_us);
        startTD = current_time_nsecs();
#endif
        return this->GO_ON;
    }

    // EOS management (utilized by the FastFlow runtime)
    void eosnotify(ssize_t id) override
    {
        emitter->flush(this); // call the flush of the emitter
        terminated = true;
#if defined (WF_TRACING_ENABLED)
        stats_record.setTerminated();
#endif
    }

    // Configure the Map_GPU replica to receive batches (this method does nothing)
    void receiveBatches(bool _input_batching) {}

    // Set the emitter used to route outputs from the Map_GPU replica
    void setEmitter(Basic_Emitter *_emitter)
    {
        emitter = _emitter;
    }

    // Check the termination of the Map_GPU replica
    bool isTerminated() const
    {
        return terminated;
    }

    // Set the execution mode of the Map_GPU replica
    void setExecutionMode(Execution_Mode_t _execution_mode)
    {
        execution_mode = _execution_mode;
    }

#if defined (WF_TRACING_ENABLED)
    // Get a copy of the Stats_Record of the Map_GPU replica
    Stats_Record getStatsRecord() const
    {
        return stats_record;
    }
#endif
};

//@endcond

/** 
 *  \class Map_GPU
 *  
 *  \brief Map_GPU operator
 *  
 *  This class implements the Map_GPU operator executing streaming transformations producing
 *  one output per input. The operator offloads the processing on a GPU device.
 */ 
template<typename mapgpu_func_t, typename key_extractor_func_t>
class Map_GPU: public Basic_Operator
{
private:
    friend class MultiPipe; // friendship with the MultiPipe class
    friend class PipeGraph; // friendship with the PipeGraph class
    mapgpu_func_t func; // functional logic used by the Map_GPU
    key_extractor_func_t key_extr; // logic to extract the key attribute from the tuple_t
    using key_t = decltype(get_key_t_KeyExtrGPU(key_extr)); // extracting the key_t type and checking the admissible singatures
    size_t parallelism; // parallelism of the Map_GPU
    std::string name; // name of the Map_GPU
    Routing_Mode_t input_routing_mode; // routing mode of inputs to the Map_GPU
    std::vector<MapGPU_Replica<mapgpu_func_t, key_t> *> replicas; // vector of pointers to the replicas of the Map_GPU

    // This method exists but its does not have any effect
    void receiveBatches(bool _input_batching) override {}

    // Set the emitter used to route outputs from the Map_GPU
    void setEmitter(Basic_Emitter *_emitter) override
    {
        replicas[0]->setEmitter(_emitter);
        for (size_t i=1; i<replicas.size(); i++) {
            replicas[i]->setEmitter(_emitter->clone());
        }
    }

    // Check whether the Map_GPU has terminated
    bool isTerminated() const override
    {
        bool terminated = true;
        for(auto *r: replicas) { // scan all the replicas to check their termination
            terminated = terminated && r->isTerminated();
        }
        return terminated;
    }

    // Set the execution mode of the Map_GPU (i.e., the one of its PipeGraph)
    void setExecutionMode(Execution_Mode_t _execution_mode)
    {
        for (auto *r: replicas) {
            r->setExecutionMode(_execution_mode);
        }
    }

    // Get the logic to extract the key attribute from the tuple_t
    key_extractor_func_t getKeyExtractor() const
    {
        return key_extr;
    }

#if defined (WF_TRACING_ENABLED)
    // Dump the log file (JSON format) of statistics of the Map_GPU
    void dumpStats() const override
    {
        std::ofstream logfile; // create and open the log file in the LOG_DIR directory
#if defined (LOG_DIR)
        std::string log_dir = std::string(STRINGIFY(LOG_DIR));
        std::string filename = std::string(STRINGIFY(LOG_DIR)) + "/" + std::to_string(getpid()) + "_" + name + ".json";
#else
        std::string log_dir = std::string("log");
        std::string filename = "log/" + std::to_string(getpid()) + "_" + name + ".json";
#endif
        if (mkdir(log_dir.c_str(), 0777) != 0) { // create the log directory
            struct stat st;
            if((stat(log_dir.c_str(), &st) != 0) || !S_ISDIR(st.st_mode)) {
                std::cerr << RED << "WindFlow Error: directory for log files cannot be created" << DEFAULT_COLOR << std::endl;
                exit(EXIT_FAILURE);
            }
        }
        logfile.open(filename);
        rapidjson::StringBuffer buffer; // create the rapidjson writer
        rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(buffer);
        this->appendStats(writer); // append the statistics of the Map_GPU
        logfile << buffer.GetString();
        logfile.close();
    }

    // Append the statistics (JSON format) of the Map_GPU to a PrettyWriter
    void appendStats(rapidjson::PrettyWriter<rapidjson::StringBuffer> &writer) const override
    {
        // create the header of the JSON file
        writer.StartObject();
        writer.Key("Operator_name");
        writer.String(name.c_str());
        writer.Key("Operator_type");
        writer.String("Map_GPU");
        writer.Key("Distribution");
        if (input_routing_mode == Routing_Mode_t::KEYBY) {
            writer.String("KEYBY");
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
        writer.Uint(parallelism);
        writer.Key("Replicas");
        writer.StartArray();
        for (auto *r: replicas) { // append the statistics from all the replicas of the Map
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
     *  \param _func functional logic of the Map_GPU (a function or a callable type)
     *  \param _key_extr key extractor (a function or a callable type)
     *  \param _parallelism internal parallelism of the Map_GPU
     *  \param _name name of the Map_GPU
     *  \param _input_routing_mode input routing mode of the Map_GPU
     */ 
    Map_GPU(mapgpu_func_t _func,
            key_extractor_func_t _key_extr,
            size_t _parallelism,
            std::string _name,
            Routing_Mode_t _input_routing_mode):
            func(_func),
            key_extr(_key_extr),
            parallelism(_parallelism),
            name(_name),
            input_routing_mode(_input_routing_mode)
    {
        if (parallelism == 0) { // check the validity of the parallelism value
            std::cerr << RED << "WindFlow Error: Map_GPU has parallelism zero" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        if constexpr(std::is_same<decltype(get_key_t_KeyExtrGPU(key_extr)), empty_key_t>::value) { // stateless case
            for (size_t i=0; i<parallelism; i++) { // create the internal replicas of the Map_GPU
                replicas.push_back(new MapGPU_Replica<mapgpu_func_t, empty_key_t>(_func, i, name));
            }
        }
        else { // stateful case
            auto *keymap = new tbb::concurrent_unordered_map<decltype(get_key_t_KeyExtrGPU(key_extr)), wrapper_state_t<decltype(get_state_t_MapGPU(func))>>();
            auto *spinlock = new pthread_spinlock_t();
            pthread_spin_init(spinlock, 0);
            for (size_t i=0; i<parallelism; i++) { // create the internal replicas of the Map_GPU
                replicas.push_back(new MapGPU_Replica<mapgpu_func_t, decltype(get_key_t_KeyExtrGPU(key_extr))>(_func, i, name, keymap, spinlock));
            }
        }
    }

    /// Copy constructor
    Map_GPU(const Map_GPU &_other):
            func(_other.func),
            key_extr(_other.key_extr),
            parallelism(_other.parallelism),
            name(_other.name),
            input_routing_mode(_other.input_routing_mode)
    {
        for (size_t i=0; i<parallelism; i++) { // deep copy of the pointers to the Map_GPU replicas
            replicas.push_back(new MapGPU_Replica<mapgpu_func_t, decltype(get_key_t_KeyExtrGPU(key_extr))>(*(_other.replicas[i])));
        }
        if constexpr(!std::is_same<decltype(get_key_t_KeyExtrGPU(key_extr)), empty_key_t>::value) { // stateful case
            auto *keymap = new tbb::concurrent_unordered_map<decltype(get_key_t_KeyExtrGPU(key_extr)), wrapper_state_t<decltype(get_state_t_MapGPU(func))>>();
            auto *spinlock = new pthread_spinlock_t();
            pthread_spin_init(spinlock, 0);
            for (auto *r: replicas) {
                r->spinlock = spinlock;
                r->keymap = keymap;
            }
        }
    }

    // Destructor
    ~Map_GPU() override
    {
        for (auto *r: replicas) { // delete all the replicas
            delete r;
        }
    }

    /// Copy Assignment Operator
    Map_GPU& operator=(const Map_GPU &_other)
    {
        if (this != &_other) {
            func = _other.func;
            key_extr = _other.key_extr;
            parallelism = _other.parallelism;
            name = _other.name;
            input_routing_mode = _other.input_routing_mode;
            for (auto *r: replicas) { // delete all the replicas
                delete r;
            }
            replicas.clear();
            for (size_t i=0; i<parallelism; i++) { // deep copy of the pointers to the Map replicas
                replicas.push_back(new MapGPU_Replica<mapgpu_func_t, decltype(get_key_t_KeyExtrGPU(key_extr))>(*(_other.replicas[i])));
            }
            if constexpr(!std::is_same<decltype(get_key_t_KeyExtrGPU(key_extr)), empty_key_t>::value) { // stateful case
                auto *keymap = new tbb::concurrent_unordered_map<decltype(get_key_t_KeyExtrGPU(key_extr)), wrapper_state_t<decltype(get_state_t_MapGPU(func))>>();
                auto *spinlock = new pthread_spinlock_t();
                pthread_spin_init(spinlock, 0);
                for (auto *r: replicas) {
                    r->spinlock = spinlock;
                    r->keymap = keymap;
                }
            }
        }
        return *this;
    }

    /// Move Assignment Operator
    Map_GPU& operator=(Map_GPU &&_other)
    {
        func = std::move(_other.func);
        key_extr = std::move(_other.key_extr);
        parallelism = _other.parallelism;
        name = std::move(_other.name);
        input_routing_mode = _other.input_routing_mode;
        for (auto *r: replicas) { // delete all the replicas
            delete r;
        }
        replicas = std::move(_other.replicas);
        return *this;
    }

    /** 
     *  \brief Get the type of the Map_GPU as a string
     *  \return type of the Map_GPU
     */ 
    std::string getType() const override
    {
        return std::string("Map_GPU");
    }

    /** 
     *  \brief Get the name of the Map_GPU as a string
     *  \return name of the Map_GPU
     */ 
    std::string getName() const override
    {
        return name;
    }

    /** 
     *  \brief Get the total parallelism of the Map_GPU
     *  \return total parallelism of the Map_GPU
     */  
    size_t getParallelism() const override
    {
        return parallelism;
    }

    /** 
     *  \brief Return the input routing mode of the Map_GPU
     *  \return routing mode used to send inputs to the Map_GPU
     */ 
    Routing_Mode_t getInputRoutingMode() const override
    {
        return input_routing_mode;
    }

    /** 
     *  \brief Return the size of the output batches that the Map_GPU should produce
     *  \return this method returns always 1 since the exact batch size is unknown
     */ 
    size_t getOutputBatchSize() const override
    {
        return 1; // here, we can return any value greater than 0!!!
    }

    /** 
     *  \brief Check whether the Map_GPU is for GPU
     *  \return this method returns true
     */ 
    bool isGPUOperator() const override
    {
        return true;
    }
};

} // namespace wf

#endif
