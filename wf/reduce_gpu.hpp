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
 *  @file    reduce_gpu.hpp
 *  @author  Gabriele Mencagli
 *  
 *  @brief Reduce operator on GPU
 *  
 *  @section Reduce_GPU (Description)
 *  
 *  This file implements the Reduce operator on GPU. Without a key extractor, it supposes
 *  to receive batches of inputs, and applies an associative and commutative function by
 *  producing one output per batch. With a key extractor, it aggregates all the inputs of
 *  the given input batch having the same key attribute, and produces an output batch having
 *  one result per distinct key (whose value is the aggregation of all the inputs of the
 *  batch having the same key attribute). The reduceBykey is applied on each batch independently.
 */ 

#ifndef REDUCE_GPU_H
#define REDUCE_GPU_H

// Required to compile with clang and CUDA < 11
#if defined (__clang__) and (__CUDACC_VER_MAJOR__ < 11)
    #define THRUST_CUB_NS_PREFIX namespace thrust::cuda_cub {
    #define THRUST_CUB_NS_POSTFIX }
    #include<thrust/system/cuda/detail/cub/util_debug.cuh>
    using namespace thrust::cuda_cub::cub;
#endif

/// includes
#include<string>
#include<thrust/sort.h>
#include<thrust/reduce.h>
#include<thrust/device_ptr.h>
#include<thrust/functional.h>
#if !defined (WF_GPU_UNIFIED_MEMORY) && !defined (WF_GPU_PINNED_MEMORY)
    #include<batch_gpu_t.hpp>
#else
    #include<batch_gpu_t_u.hpp>
#endif
#if defined (WF_TRACING_ENABLED)
    #include<stats_record.hpp>
#endif
#include<basic_emitter.hpp>
#include<basic_operator.hpp>
#include<thrust_allocator.hpp>

namespace wf {

//@cond DOXY_IGNORE

// CUDA Kernel: Extract_Keys_Kernel
template<typename key_extractor_func_t, typename tuple_t, typename key_t>
__global__ void Extract_Keys_Kernel(batch_item_gpu_t<tuple_t> *data,
                                    key_t *keys,
                                    size_t size,
                                    key_extractor_func_t key_extr)
{
    int id = threadIdx.x + blockIdx.x * blockDim.x; // id of the thread in the kernel
    int num_threads = gridDim.x * blockDim.x; // number of threads in the kernel
    for (int i=id; i<size; i+=num_threads) {
        keys[i] = key_extr(data[i].tuple);
    }
}

// Struct of the reduce function to be passed to the thrust::reduce
template<typename reducegpu_func_t, typename tuple_t>
struct thrust_reducegpu_func_t
{
    reducegpu_func_t th_func; // functional logic

    // Constructor
    thrust_reducegpu_func_t(reducegpu_func_t _func):
                            th_func(_func) {}

    // Binary operator used by the thrust::reduce
    __host__ __device__ batch_item_gpu_t<tuple_t> operator()(const batch_item_gpu_t<tuple_t> &lhs, const batch_item_gpu_t<tuple_t> &rhs)
    {
        batch_item_gpu_t<tuple_t> result;
        result.tuple = th_func(lhs.tuple, rhs.tuple);
        result.timestamp = (lhs.timestamp < rhs.timestamp) ? rhs.timestamp: lhs.timestamp;
        return result;
    }
};

// class ReduceGPU_Replica
template<typename reducegpu_func_t, typename key_extractor_func_t>
class ReduceGPU_Replica: public ff::ff_monode
{
private:
    reducegpu_func_t func; // functional logic used by the Reduce_GPU replica
    key_extractor_func_t key_extr; // functional logic to extract the key attribute from the tuple_t
    using tuple_t = decltype(get_tuple_t_ReduceGPU(func)); // extracting the tuple_t type and checking the admissible signatures
    static_assert(std::is_same<tuple_t, decltype(get_tuple_t_KeyExtrGPU(key_extr))>::value,
        "WindFlow Compilation Error - type mismatch with the tuple_t type in Reduce_GPU:\n");
    using key_t = decltype(get_key_t_KeyExtrGPU(key_extr)); // extracting the key_t type and checking the admissible signatures
    static constexpr bool isKeyed = !std::is_same<key_t, empty_key_t>::value;
    size_t id_replica; // identifier of the Reduce_GPU replica
    std::string opName; // name of the Reduce_GPU containing the replica
    bool terminated; // true if the Reduce_GPU replica has finished its work
    Basic_Emitter *emitter; // pointer to the used emitter
    Thurst_Allocator alloc; // internal memory allocator used by CUDA/Thrust
    struct record_t // record_t struct
    {
        size_t size; // size of the arrays in the record
        key_t *keys_gpu; // pointer to a GPU array of keys
        key_t *new_keys_gpu; // pointer to a GPU array of compacted keys
        batch_item_gpu_t<tuple_t> *new_data_gpu; // pointer to a GPU array of compacted tuples

        // Constructor
        record_t(size_t _size):
                 size(_size)
        {
            gpuErrChk(cudaMalloc(&keys_gpu, sizeof(key_t) * size));
            gpuErrChk(cudaMalloc(&new_keys_gpu, sizeof(key_t) * size));
            gpuErrChk(cudaMalloc(&new_data_gpu, sizeof(batch_item_gpu_t<tuple_t>) * size));
        }

        // Destructor
        ~record_t()
        {
            gpuErrChk(cudaFree(keys_gpu));
            gpuErrChk(cudaFree(new_keys_gpu));
            gpuErrChk(cudaFree(new_data_gpu));
        }

        // Resize the internal arrays of the record
        void resize(size_t _newsize)
        {
            if (_newsize > size) {
                size = _newsize;
                gpuErrChk(cudaFree(keys_gpu));
                gpuErrChk(cudaFree(new_keys_gpu));
                gpuErrChk(cudaFree(new_data_gpu));
                gpuErrChk(cudaMalloc(&keys_gpu, sizeof(key_t) * size));
                gpuErrChk(cudaMalloc(&new_keys_gpu, sizeof(key_t) * size));
                gpuErrChk(cudaMalloc(&new_data_gpu, sizeof(batch_item_gpu_t<tuple_t>) * size));
            }
        }
    };
    thrust_reducegpu_func_t<reducegpu_func_t, tuple_t> thrust_reduce_func; // reduce function to be passed to the thrust::reduce
    record_t *record; // pointer to the record structure
    int numSMs; // number of Stream Multiprocessor of the GPU
    int max_blocks_per_sm; // maximum number of blocks resident on each Stream Multiprocessor of the GPU
#if defined (WF_TRACING_ENABLED)
    Stats_Record stats_record;
    double avg_td_us = 0;
    double avg_ts_us = 0;
    volatile uint64_t startTD, startTS, endTD, endTS;
#endif

public:
    // Constructor
    ReduceGPU_Replica(reducegpu_func_t _func,
                      key_extractor_func_t _key_extr,
                      size_t _id_replica,
                      std::string _opName):
                      func(_func),
                      key_extr(_key_extr),
                      id_replica(_id_replica),
                      opName(_opName),
                      terminated(false),
                      emitter(nullptr),
                      thrust_reduce_func(_func),
                      record(nullptr)
    {
        gpuErrChk(cudaDeviceGetAttribute(&numSMs, cudaDevAttrMultiProcessorCount, 0)); // device_id = 0
#if (__CUDACC_VER_MAJOR__ >= 11) // at least CUDA 11
        gpuErrChk(cudaDeviceGetAttribute(&max_blocks_per_sm, cudaDevAttrMaxBlocksPerMultiprocessor, 0)); // device_id = 0
#else
        max_blocks_per_sm = WF_GPU_MAX_BLOCKS_PER_SM;
#endif
    }

    // Copy Constructor
    ReduceGPU_Replica(const ReduceGPU_Replica &_other):
                      func(_other.func),
                      key_extr(_other.key_extr),
                      id_replica(_other.id_replica),
                      opName(_other.opName),
                      terminated(_other.terminated),
                      thrust_reduce_func(_other.thrust_reduce_func),
                      record(nullptr),
                      numSMs(_other.numSMs),
                      max_blocks_per_sm(_other.max_blocks_per_sm)
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
    ReduceGPU_Replica(ReduceGPU_Replica &&_other):
                      func(std::move(_other.func)),
                      key_extr(std::move(_other.key_extr)),
                      id_replica(_other.id_replica),
                      opName(std::move(_other.opName)),
                      terminated(_other.terminated),
                      emitter(std::exchange(_other.emitter, nullptr)),
                      thrust_reduce_func(std::move(_other.thrust_reduce_func)),
                      record(std::exchange(_other.record, nullptr)),
                      numSMs(_other.numSMs),
                      max_blocks_per_sm(_other.max_blocks_per_sm)                      
    {
#if defined (WF_TRACING_ENABLED)
        stats_record = std::move(_other.stats_record);
#endif
    }

    // Destructor
    ~ReduceGPU_Replica()
    {
        if (emitter != nullptr) {
            delete emitter;
        }
        if (record != nullptr) {
            delete record;
        }
    }

    // Copy Assignment Operator
    ReduceGPU_Replica &operator=(const ReduceGPU_Replica &_other)
    {
        if (this != &_other) {
            func = _other.func;
            key_extr = _other.key_extr;
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
            thrust_reduce_func = _other.thrust_reduce_func;
            if (record != nullptr) {
                delete record;
            }
            record = nullptr;
            numSMs = _other.numSMs;
            max_blocks_per_sm = _other.max_blocks_per_sm;
#if defined (WF_TRACING_ENABLED)
            stats_record = _other.stats_record;
#endif
        }
        return *this;
    }

    // Move Assignment Operator
    ReduceGPU_Replica &operator=(ReduceGPU_Replica &_other)
    {
        func = std::move(_other.func);
        key_extr = std::move(_other.key_extr);
        id_replica = _other.id_replica;
        opName = std::move(_other.opName);
        terminated = _other.terminated;
        if (emitter != nullptr) {
            delete emitter;
        }
        emitter = std::exchange(_other.emitter, nullptr);
        thrust_reduce_func = std::move(_other.thrust_reduce_func);
        if (record != nullptr) {
            delete record;
        }
        record = std::exchange(_other.record, nullptr);
        numSMs = _other.numSMs;
        max_blocks_per_sm = _other.max_blocks_per_sm;
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
        Batch_GPU_t<decltype(get_tuple_t_ReduceGPU(func))> *input = reinterpret_cast<Batch_GPU_t<decltype(get_tuple_t_ReduceGPU(func))> *>(_in);
        if (input->isPunct()) { // if it is a punctuaton
            emitter->propagate_punctuation(input->getWatermark(id_replica), this); // propagate the received punctuation
            deleteBatch_t(input); // delete the punctuation
            return this->GO_ON;
        }
#if defined (WF_TRACING_ENABLED)
        stats_record.inputs_received += input->size;
        stats_record.bytes_received += input->size * sizeof(tuple_t);
#endif
#if !defined (WF_GPU_UNIFIED_MEMORY) && !defined (WF_GPU_PINNED_MEMORY)
        auto *batch_data = input->data_gpu; // version with CUDA explicit memory transfers
#else
        auto *batch_data = input->data_u; // version with CUDA unified memory support
#endif
        if constexpr (isKeyed) { // version with key extractor
            if (record == nullptr) {
                record = new record_t(input->original_size);
            }
            else {
                record->resize(input->original_size);
            }
            int num_blocks = std::min((int) ceil(((double) input->size) / WF_GPU_THREADS_PER_BLOCK), numSMs * max_blocks_per_sm);
            Extract_Keys_Kernel<key_extractor_func_t, decltype(get_tuple_t_ReduceGPU(func)), decltype(get_key_t_KeyExtrGPU(key_extr))>
                               <<<num_blocks, WF_GPU_THREADS_PER_BLOCK, 0, input->cudaStream>>>(batch_data,
                                                                                                        record->keys_gpu,
                                                                                                        input->size,
                                                                                                        key_extr);
            gpuErrChk(cudaPeekAtLastError());
            thrust::device_ptr<batch_item_gpu_t<decltype(get_tuple_t_ReduceGPU(func))>> th_data_gpu = thrust::device_pointer_cast(batch_data);
            thrust::device_ptr<decltype(get_key_t_KeyExtrGPU(key_extr))> th_keys_gpu = thrust::device_pointer_cast(record->keys_gpu);
            thrust::sort_by_key(thrust::cuda::par(alloc).on(input->cudaStream),
                                th_keys_gpu,
                                th_keys_gpu + input->size,
                                th_data_gpu);
            thrust::device_ptr<batch_item_gpu_t<decltype(get_tuple_t_ReduceGPU(func))>> th_new_data_gpu = thrust::device_pointer_cast(record->new_data_gpu);
            thrust::device_ptr<decltype(get_key_t_KeyExtrGPU(key_extr))> th_new_keys_gpu = thrust::device_pointer_cast(record->new_keys_gpu);
            auto end = thrust::reduce_by_key(thrust::cuda::par(alloc).on(input->cudaStream),
                                             th_keys_gpu,
                                             th_keys_gpu + input->size,
                                             th_data_gpu,
                                             th_new_keys_gpu,
                                             th_new_data_gpu,
                                             thrust::equal_to<decltype(get_key_t_KeyExtrGPU(key_extr))>(),
                                             thrust_reduce_func);
            assert(input->size >= end.first - th_new_keys_gpu); // sanity check
            input->size = end.first - th_new_keys_gpu;
            gpuErrChk(cudaMemcpyAsync(batch_data,
                                      record->new_data_gpu,
                                      input->size * sizeof(batch_item_gpu_t<decltype(get_tuple_t_ReduceGPU(func))>),
                                      cudaMemcpyDeviceToDevice,
                                      input->cudaStream));
            gpuErrChk(cudaStreamSynchronize(input->cudaStream));
#if defined (WF_TRACING_ENABLED)
            stats_record.outputs_sent += input->size;
            stats_record.bytes_sent += input->size * sizeof(tuple_t);
#endif
            emitter->emit_inplace(input, this); // send the output batch once computed
        }
        if constexpr (!isKeyed) { // version without key extractor
            thrust::device_ptr<batch_item_gpu_t<decltype(get_tuple_t_ReduceGPU(func))>> th_data_gpu = thrust::device_pointer_cast(batch_data);
            auto result = thrust::reduce(thrust::cuda::par(alloc).on(input->cudaStream),
                                         th_data_gpu,
                                         th_data_gpu + input->size,
                                         batch_item_gpu_t<decltype(get_tuple_t_ReduceGPU(func))>(),
                                         thrust_reduce_func);
            gpuErrChk(cudaMemcpyAsync(batch_data,
                                      &result,
                                      sizeof(batch_item_gpu_t<decltype(get_tuple_t_ReduceGPU(func))>),
                                      cudaMemcpyHostToDevice,
                                      input->cudaStream));
            input->size = 1;
            gpuErrChk(cudaStreamSynchronize(input->cudaStream));
#if defined (WF_TRACING_ENABLED)
            stats_record.outputs_sent++;
            stats_record.bytes_sent += sizeof(tuple_t);
#endif
            emitter->emit_inplace(input, this); // send the output batch once computed
        }
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

    // Configure the Reduce_GPU replica to receive batches (this method does nothing)
    void receiveBatches(bool _input_batching) {}

    // Set the emitter used to route outputs from the Reduce_GPU replica
    void setEmitter(Basic_Emitter *_emitter)
    {
        emitter = _emitter;
    }

    // Check the termination of the Reduce_GPU replica
    bool isTerminated() const
    {
        return terminated;
    }

#if defined (WF_TRACING_ENABLED)
    // Get a copy of the Stats_Record of the Reduce_GPU replica
    Stats_Record getStatsRecord() const
    {
        return stats_record;
    }
#endif
};

//@endcond

/** 
 *  \class Reduce_GPU
 *  
 *  \brief Reduce_GPU operator
 *  
 *  This class implements the Reduce_GPU operator executing an associative and commutative
 *  function over all the inputs of each received batch, by aggregating all the inputs having
 *  the same key within the same batch. Without a key extractor, the operator produces one
 *  result per batch.
 */ 
template<typename reducegpu_func_t, typename key_extractor_func_t>
class Reduce_GPU: public Basic_Operator
{
private:
    friend class MultiPipe; // friendship with the MultiPipe class
    friend class PipeGraph; // friendship with the PipeGraph class
    reducegpu_func_t func; // functional logic used by the Reduce_GPU
    key_extractor_func_t key_extr; // logic to extract the key attribute from the tuple_t
    using key_t = decltype(get_key_t_KeyExtrGPU(key_extr)); // extracting the key_t type and checking the admissible singatures
    size_t parallelism; // parallelism of the Reduce_GPU
    std::string name; // name of the Reduce_GPU
    std::vector<ReduceGPU_Replica<reducegpu_func_t, key_extractor_func_t> *> replicas; // vector of pointers to the replicas of the Reduce_GPU

    // This method exists but its does not have any effect
    void receiveBatches(bool _input_batching) override {}

    // Set the emitter used to route outputs from the Reduce_GPU
    void setEmitter(Basic_Emitter *_emitter) override
    {
        replicas[0]->setEmitter(_emitter);
        for (size_t i=1; i<replicas.size(); i++) {
            replicas[i]->setEmitter(_emitter->clone());
        }
    }

    // Check whether the Reduce_GPU has terminated
    bool isTerminated() const override
    {
        bool terminated = true;
        for(auto *r: replicas) { // scan all the replicas to check their termination
            terminated = terminated && r->isTerminated();
        }
        return terminated;
    }

    // Get the logic to extract the key attribute from the tuple_t
    key_extractor_func_t getKeyExtractor() const
    {
        return key_extr;
    }

#if defined (WF_TRACING_ENABLED)
    // Dump the log file (JSON format) of statistics of the Reduce_GPU
    void dumpStats() const override
    {
        std::ofstream logfile; // create and open the log file in the WF_LOG_DIR directory
#if defined (WF_LOG_DIR)
        std::string log_dir = std::string(STRINGIFY(WF_LOG_DIR));
        std::string filename = std::string(STRINGIFY(WF_LOG_DIR)) + "/" + std::to_string(getpid()) + "_" + name + ".json";
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
        this->appendStats(writer); // append the statistics of the Reduce_GPU
        logfile << buffer.GetString();
        logfile.close();
    }

    // Append the statistics (JSON format) of the Reduce_GPU to a PrettyWriter
    void appendStats(rapidjson::PrettyWriter<rapidjson::StringBuffer> &writer) const override
    {
        // create the header of the JSON file
        writer.StartObject();
        writer.Key("Operator_name");
        writer.String(name.c_str());
        writer.Key("Operator_type");
        writer.String("Reduce_GPU");
        writer.Key("Distribution");
        writer.String("FORWARD");
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
        for (auto *r: replicas) { // append the statistics from all the replicas of the Reduce_GPU
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
     *  \param _func functional logic of the Reduce_GPU (a __host__ __device__ lambda or a __device__ functor object)
     *  \param _key_extr key extractor (a __host__ __device__ lambda or a __host__ __device__ functor object)
     *  \param _parallelism internal parallelism of the Reduce_GPU
     *  \param _name name of the Reduce_GPU
     */ 
    Reduce_GPU(reducegpu_func_t _func,
               key_extractor_func_t _key_extr,
               size_t _parallelism,
               std::string _name):
               func(_func),
               key_extr(_key_extr),
               parallelism(_parallelism),
               name(_name)
    {
        if (parallelism == 0) { // check the validity of the parallelism value
            std::cerr << RED << "WindFlow Error: Reduce_GPU has parallelism zero" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        for (size_t i=0; i<parallelism; i++) { // create the internal replicas of the Reduce_GPU
            replicas.push_back(new ReduceGPU_Replica<reducegpu_func_t, key_extractor_func_t>(_func, _key_extr, i, name));
        }
    }

    /// Copy constructor
    Reduce_GPU(const Reduce_GPU &_other):
               func(_other.func),
               key_extr(_other.key_extr),
               parallelism(_other.parallelism),
               name(_other.name)
    {
        for (size_t i=0; i<parallelism; i++) { // deep copy of the pointers to the Reduce_GPU replicas
            replicas.push_back(new ReduceGPU_Replica<reducegpu_func_t, key_extractor_func_t>(*(_other.replicas[i])));
        }
    }

    // Destructor
    ~Reduce_GPU() override
    {
        for (auto *r: replicas) { // delete all the replicas
            delete r;
        }
    }

    /// Copy Assignment Operator
    Reduce_GPU& operator=(const Reduce_GPU &_other)
    {
        if (this != &_other) {
            func = _other.func;
            key_extr = _other.key_extr;
            parallelism = _other.parallelism;
            name = _other.name;
            for (auto *r: replicas) { // delete all the replicas
                delete r;
            }
            replicas.clear();
            for (size_t i=0; i<parallelism; i++) { // deep copy of the pointers to the Reduce_GPU replicas
                replicas.push_back(new ReduceGPU_Replica<reducegpu_func_t, key_extractor_func_t>(*(_other.replicas[i])));
            }
        }
        return *this;
    }

    /// Move Assignment Operator
    Reduce_GPU& operator=(Reduce_GPU &&_other)
    {
        func = std::move(_other.func);
        key_extr = std::move(_other.key_extr);
        parallelism = _other.parallelism;
        name = std::move(_other.name);
        for (auto *r: replicas) { // delete all the replicas
            delete r;
        }
        replicas = std::move(_other.replicas);
        return *this;
    }

    /** 
     *  \brief Get the type of the Reduce_GPU as a string
     *  \return type of the Reduce_GPU
     */ 
    std::string getType() const override
    {
        return std::string("Reduce_GPU");
    }

    /** 
     *  \brief Get the name of the Reduce_GPU as a string
     *  \return name of the Reduce_GPU
     */ 
    std::string getName() const override
    {
        return name;
    }

    /** 
     *  \brief Get the total parallelism of the Reduce_GPU
     *  \return total parallelism of the Reduce_GPU
     */  
    size_t getParallelism() const override
    {
        return parallelism;
    }

    /** 
     *  \brief Return the input routing mode of the Reduce_GPU
     *  \return routing mode used to send inputs to the Reduce_GPU
     */ 
    Routing_Mode_t getInputRoutingMode() const override
    {
        return Routing_Mode_t::FORWARD;
    }

    /** 
     *  \brief Return the size of the output batches that the Reduce_GPU should produce
     *  \return this method returns always 1 since the exact batch size is unknown
     */ 
    size_t getOutputBatchSize() const override
    {
        return 1; // here, we can return any value greater than 0!!!
    }

    /** 
     *  \brief Check whether the Reduce_GPU is for GPU
     *  \return this method returns true
     */ 
    bool isGPUOperator() const override
    {
        return true;
    }
};

} // namespace wf

#endif
