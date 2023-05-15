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
template<typename keyextr_func_gpu_t, typename tuple_t, typename key_t>
__global__ void Extract_Keys_Kernel(batch_item_gpu_t<tuple_t> *data,
                                    key_t *keys,
                                    size_t size,
                                    keyextr_func_gpu_t key_extr)
{
    int id = threadIdx.x + blockIdx.x * blockDim.x; // id of the thread in the kernel
    int num_threads = gridDim.x * blockDim.x; // number of threads in the kernel
    for (int i=id; i<size; i+=num_threads) {
        keys[i] = key_extr(data[i].tuple);
    }
}

// Struct of the reduce function to be passed to the thrust::reduce
template<typename reduce_func_gpu_t, typename tuple_t>
struct thrust_reduce_func_gpu_t
{
    reduce_func_gpu_t th_func; // functional logic

    // Constructor
    thrust_reduce_func_gpu_t(reduce_func_gpu_t _func):
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
template<typename reduce_func_gpu_t, typename keyextr_func_gpu_t>
class ReduceGPU_Replica: public Basic_Replica
{
private:
    reduce_func_gpu_t func; // functional logic used by the Reduce_GPU replica
    keyextr_func_gpu_t key_extr; // functional logic to extract the key attribute from the tuple_t
    using tuple_t = decltype(get_tuple_t_ReduceGPU(func)); // extracting the tuple_t type and checking the admissible signatures
    static_assert(std::is_same<tuple_t, decltype(get_tuple_t_KeyExtrGPU(key_extr))>::value,
        "WindFlow Compilation Error - type mismatch with the tuple_t type in Reduce_GPU:\n");
    using key_t = decltype(get_key_t_KeyExtrGPU(key_extr)); // extracting the key_t type and checking the admissible signatures
    static constexpr bool isKeyed = !std::is_same<key_t, empty_key_t>::value;
    size_t id_replica; // identifier of the Reduce_GPU replica
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

    thrust_reduce_func_gpu_t<reduce_func_gpu_t, tuple_t> thrust_reduce_func; // reduce function to be passed to the thrust::reduce
    record_t *record; // pointer to the record structure
    int numSMs; // number of Stream Multiprocessor of the GPU
    int max_blocks_per_sm; // maximum number of blocks resident on each Stream Multiprocessor of the GPU

public:
    // Constructor
    ReduceGPU_Replica(reduce_func_gpu_t _func,
                      keyextr_func_gpu_t _key_extr,
                      size_t _id_replica,
                      std::string _opName):
                      Basic_Replica(_opName, false),
                      func(_func),
                      key_extr(_key_extr),
                      id_replica(_id_replica),
                      thrust_reduce_func(_func),
                      record(nullptr)
    {
        int gpu_id;
        gpuErrChk(cudaGetDevice(&gpu_id));
        gpuErrChk(cudaDeviceGetAttribute(&numSMs, cudaDevAttrMultiProcessorCount, gpu_id));
#if (__CUDACC_VER_MAJOR__ >= 11) // at least CUDA 11
        gpuErrChk(cudaDeviceGetAttribute(&max_blocks_per_sm, cudaDevAttrMaxBlocksPerMultiprocessor, gpu_id));
#else
        max_blocks_per_sm = WF_GPU_MAX_BLOCKS_PER_SM;
#endif
    }

    // Copy Constructor
    ReduceGPU_Replica(const ReduceGPU_Replica &_other):
                      Basic_Replica(_other),
                      func(_other.func),
                      key_extr(_other.key_extr),
                      id_replica(_other.id_replica),
                      thrust_reduce_func(_other.thrust_reduce_func),
                      record(nullptr),
                      numSMs(_other.numSMs),
                      max_blocks_per_sm(_other.max_blocks_per_sm) {}

    // Destructor
    ~ReduceGPU_Replica() override
    {
        if (record != nullptr) {
            delete record;
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
        auto *batch_data = input->data_gpu; // version with CUDA explicit memory transfers
        if constexpr (isKeyed) { // version with key extractor
            if (record == nullptr) {
                record = new record_t(input->original_size);
            }
            else {
                record->resize(input->original_size);
            }
            int num_blocks = std::min((int) ceil(((double) input->size) / WF_GPU_THREADS_PER_BLOCK), numSMs * max_blocks_per_sm);
            Extract_Keys_Kernel<keyextr_func_gpu_t, tuple_t, key_t>
                               <<<num_blocks, WF_GPU_THREADS_PER_BLOCK, 0, input->cudaStream>>>(batch_data,
                                                                                                record->keys_gpu,
                                                                                                input->size,
                                                                                                key_extr);
            gpuErrChk(cudaPeekAtLastError());
            thrust::device_ptr<batch_item_gpu_t<tuple_t>> th_data_gpu = thrust::device_pointer_cast(batch_data);
            thrust::device_ptr<key_t> th_keys_gpu = thrust::device_pointer_cast(record->keys_gpu);
            thrust::sort_by_key(thrust::cuda::par(alloc).on(input->cudaStream),
                                th_keys_gpu,
                                th_keys_gpu + input->size,
                                th_data_gpu);
            thrust::device_ptr<batch_item_gpu_t<tuple_t>> th_new_data_gpu = thrust::device_pointer_cast(record->new_data_gpu);
            thrust::device_ptr<key_t> th_new_keys_gpu = thrust::device_pointer_cast(record->new_keys_gpu);
            auto end = thrust::reduce_by_key(thrust::cuda::par(alloc).on(input->cudaStream),
                                             th_keys_gpu,
                                             th_keys_gpu + input->size,
                                             th_data_gpu,
                                             th_new_keys_gpu,
                                             th_new_data_gpu,
                                             thrust::equal_to<key_t>(),
                                             thrust_reduce_func);
            assert(input->size >= end.first - th_new_keys_gpu); // sanity check
            input->size = end.first - th_new_keys_gpu;
            gpuErrChk(cudaMemcpyAsync(batch_data,
                                      record->new_data_gpu,
                                      input->size * sizeof(batch_item_gpu_t<tuple_t>),
                                      cudaMemcpyDeviceToDevice,
                                      input->cudaStream));
            gpuErrChk(cudaStreamSynchronize(input->cudaStream));
#if defined (WF_TRACING_ENABLED)
            (this->stats_record).outputs_sent += input->size;
            (this->stats_record).bytes_sent += input->size * sizeof(tuple_t);
#endif
            (this->emitter)->emit_inplace(input, this); // send the output batch once computed
        }
        if constexpr (!isKeyed) { // version without key extractor
            thrust::device_ptr<batch_item_gpu_t<tuple_t>> th_data_gpu = thrust::device_pointer_cast(batch_data);
            auto result = thrust::reduce(thrust::cuda::par(alloc).on(input->cudaStream),
                                         th_data_gpu,
                                         th_data_gpu + input->size,
                                         batch_item_gpu_t<tuple_t>(),
                                         thrust_reduce_func);
            gpuErrChk(cudaMemcpyAsync(batch_data,
                                      &result,
                                      sizeof(batch_item_gpu_t<tuple_t>),
                                      cudaMemcpyHostToDevice,
                                      input->cudaStream));
            input->size = 1;
            gpuErrChk(cudaStreamSynchronize(input->cudaStream));
#if defined (WF_TRACING_ENABLED)
            (this->stats_record).outputs_sent++;
            (this->stats_record).bytes_sent += sizeof(tuple_t);
#endif
            (this->emitter)->emit_inplace(input, this); // send the output batch once computed
        }
        this->endStatsRecording();
        return this->GO_ON;
    }

    // svc_end (utilized by the FastFlow runtime)
    void svc_end() override {}

    // Configure the Reduce_GPU replica to receive batches instead of individual inputs
    void receiveBatches(bool _input_batching) override {}

    ReduceGPU_Replica(ReduceGPU_Replica &&) = delete; ///< Move constructor is deleted
    ReduceGPU_Replica &operator=(const ReduceGPU_Replica &) = delete; ///< Copy assignment operator is deleted
    ReduceGPU_Replica &operator=(ReduceGPU_Replica &&) = delete; ///< Move assignment operator is deleted
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
template<typename reduce_func_gpu_t, typename keyextr_func_gpu_t>
class Reduce_GPU: public Basic_Operator
{
private:
    friend class MultiPipe;
    friend class PipeGraph;
    reduce_func_gpu_t func; // functional logic used by the Reduce_GPU
    keyextr_func_gpu_t key_extr; // logic to extract the key attribute from the tuple_t
    using key_t = decltype(get_key_t_KeyExtrGPU(key_extr)); // extracting the key_t type and checking the admissible singatures
    std::vector<ReduceGPU_Replica<reduce_func_gpu_t, keyextr_func_gpu_t> *> replicas; // vector of pointers to the replicas of the Reduce_GPU

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
    keyextr_func_gpu_t getKeyExtractor() const
    {
        return key_extr;
    }

#if defined (WF_TRACING_ENABLED)
    // Append the statistics (JSON format) of the Reduce_GPU to a PrettyWriter
    void appendStats(rapidjson::PrettyWriter<rapidjson::StringBuffer> &writer) const override
    {
        // create the header of the JSON file
        writer.StartObject();
        writer.Key("Operator_name");
        writer.String((this->name).c_str());
        writer.Key("Operator_type");
        writer.String("Reduce_GPU");
        writer.Key("Distribution");
        if (this->getInputRoutingMode() == Routing_Mode_t::REBALANCING) {
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
     *  \param _func functional logic of the Reduce_GPU (a __host__ __device__ callable type)
     *  \param _key_extr key extractor (a __host__ __device__ callable type)
     *  \param _parallelism internal parallelism of the Reduce_GPU
     *  \param _name name of the Reduce_GPU
     *  \param _input_routing_mode input routing mode of the Reduce_GPU
     */ 
    Reduce_GPU(reduce_func_gpu_t _func,
               keyextr_func_gpu_t _key_extr,
               size_t _parallelism,
               std::string _name,
               Routing_Mode_t _input_routing_mode):
               Basic_Operator(_parallelism, _name, _input_routing_mode, 1, true /* operator targeting GPU */),
               func(_func),
               key_extr(_key_extr)
    {
        for (size_t i=0; i<this->parallelism; i++) { // create the internal replicas of the Reduce_GPU
            replicas.push_back(new ReduceGPU_Replica<reduce_func_gpu_t, keyextr_func_gpu_t>(_func, _key_extr, i, this->name));
        }
    }

    /// Copy constructor
    Reduce_GPU(const Reduce_GPU &_other):
               Basic_Operator(_other),
               func(_other.func),
               key_extr(_other.key_extr)
    {
        for (size_t i=0; i<this->parallelism; i++) { // deep copy of the pointers to the Reduce_GPU replicas
            replicas.push_back(new ReduceGPU_Replica<reduce_func_gpu_t, keyextr_func_gpu_t>(*(_other.replicas[i])));
        }
    }

    // Destructor
    ~Reduce_GPU() override
    {
        for (auto *r: replicas) { // delete all the replicas
            delete r;
        }
    }

    /** 
     *  \brief Get the type of the Reduce_GPU as a string
     *  \return type of the Reduce_GPU
     */ 
    std::string getType() const override
    {
        return std::string("Reduce_GPU");
    }

    Reduce_GPU(Reduce_GPU &&) = delete; ///< Move constructor is deleted
    Reduce_GPU &operator=(const Reduce_GPU &) = delete; ///< Copy assignment operator is deleted
    Reduce_GPU &operator=(Reduce_GPU &&) = delete; ///< Move assignment operator is deleted
};

} // namespace wf

#endif
