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
 *  @file    basic_gpu.hpp
 *  @author  Gabriele Mencagli
 *  
 *  @brief Basic definitions and macros used by the WindFlow library for GPU operators
 *  
 *  @section Additional Basic Definitions and Macros for GPU operators (Description)
 *  
 *  Set of definitions and macros used by the WindFlow library and by its GPU
 *  operators.
 */ 

#ifndef BASIC_GPU_H
#define BASIC_GPU_H

/// includes
#include<cassert>
#include<iostream>
#include<stdint.h>
#include<basic.hpp>

namespace wf {

/// Forward declaration of the Map_GPU operator
template<typename mapgpu_func_t, typename key_extractor_func_t>
class Map_GPU;

/// Forward declaration of the Filter_GPU operator
template<typename filtergpu_func_t, typename key_extractor_func_t>
class Filter_GPU;

/// Forward declaration of the Reduce_GPU operator
template<typename reducegpu_func_t, typename key_extractor_func_t>
class Reduce_GPU;

/// Forward declaration of the FFAT_Aggregator_GPU operator
template<typename liftgpu_func_t, typename combgpu_func_t, typename key_extractor_func_t>
class FFAT_Aggregator_GPU;

//@cond DOXY_IGNORE

// Default number of blocks per Stream-Multiprocessor
#if !defined (WF_GPU_MAX_BLOCKS_PER_SM)
    #define WF_GPU_MAX_BLOCKS_PER_SM 32
#endif

// Default number of CUDA threads per block
#if !defined (WF_GPU_THREADS_PER_BLOCK)
    #define WF_GPU_THREADS_PER_BLOCK 256
#endif

// Default size of the feedback recycling queues (for GPU operators only)
#if !defined (WF_GPU_DEFAULT_RECYCLING_QUEUE_SIZE)
    #define WF_GPU_DEFAULT_RECYCLING_QUEUE_SIZE 8192
#endif

// Compute the next power of two greater than a 32-bit integer
inline int32_t next_power_of_two(int32_t n)
{
    assert(n>0);
    --n;
    n |= n >> 1;
    n |= n >> 2;
    n |= n >> 4;
    n |= n >> 8;
    n |= n >> 16;
    return n + 1;
}

// Assert function on GPU
inline void gpuAssert(cudaError_t code,
                      const char *file,
                      int line,
                      bool abort=true)
{
    if (code != cudaSuccess) {
        std::cerr << RED << "WindFlow Error: GPUassert with code => " << cudaGetErrorString(code) << ", file => " << file << ", at line => " << line << std::endl;
        if (abort) {
            exit(code);
        }
    }
}

// Macro gpuErrChk
#define gpuErrChk(ans) { gpuAssert((ans), __FILE__, __LINE__); }

// Macro errChkMalloc
#define errChkMalloc(ans) if ( (ans) == NULL ) { \
                            std::cerr << RED << "WindFlow Error: error malloc() call" << DEFAULT_COLOR << std::endl; \
                            exit(EXIT_FAILURE); \
                          }

// Struct of a data item used by GPU batches
template<typename tuple_t>
struct batch_item_gpu_t
{
    tuple_t tuple;
    uint64_t timestamp;

    // Constructor
    __host__ __device__ batch_item_gpu_t(): timestamp(0) {}
};

// CUDA Kernel: Build_State_Kernel
template<typename state_t>
__global__ void Build_State_Kernel(state_t *state_gpu)
{
    int id = threadIdx.x + blockIdx.x * blockDim.x; // id of the thread in the kernel
    if (id == 0) {
        new (state_gpu) state_t(); // placement new with default constructor
    }
}

// CUDA Kernel: Copy_State_Kernel
template<typename state_t>
__global__ void Copy_State_Kernel(state_t *state_gpu_1, state_t *state_gpu_2)
{
    int id = threadIdx.x + blockIdx.x * blockDim.x; // id of the thread in the kernel
    if (id == 0) {
        new (state_gpu_1) state_t(*state_gpu_2); // placement new with copy constructor
    }
}

// CUDA Kernel: Destroy_State_Kernel
template<typename state_t>
__global__ void Destroy_State_Kernel(state_t *state_gpu)
{
    int id = threadIdx.x + blockIdx.x * blockDim.x; // id of the thread in the kernel
    if (id == 0) {
        state_gpu->~state_t(); // call the destructor
    }
}

// Struct wrapper of a keyed state used by GPU operators
template<typename state_t>
struct wrapper_state_t
{
    state_t *state_gpu; // pointer to the keyed state on GPU

    // Constructor
    wrapper_state_t()
    {
        gpuErrChk(cudaMalloc(&state_gpu, sizeof(state_t)));
        Build_State_Kernel<<<1, WF_GPU_THREADS_PER_BLOCK>>>(state_gpu); // use the default CUDA stream
        gpuErrChk(cudaPeekAtLastError());
        gpuErrChk(cudaDeviceSynchronize());
    }

    // Copy Constructor
    wrapper_state_t(const wrapper_state_t &_other)
    {
        gpuErrChk(cudaMalloc(&state_gpu, sizeof(state_t)));
        Copy_State_Kernel<<<1, WF_GPU_THREADS_PER_BLOCK>>>(state_gpu, _other.state_gpu); // use the default CUDA stream
        gpuErrChk(cudaPeekAtLastError());
        gpuErrChk(cudaDeviceSynchronize());
    }

    // Move Constructor
    wrapper_state_t(wrapper_state_t &&_other):
                    state_gpu(std::exchange(_other.state_gpu, nullptr)) {}

    // Destructor
    ~wrapper_state_t()
    {
        if (state_gpu != nullptr) {
            Destroy_State_Kernel<<<1, WF_GPU_THREADS_PER_BLOCK, 0>>>(state_gpu); // use the default CUDA stream
            gpuErrChk(cudaPeekAtLastError());
            gpuErrChk(cudaDeviceSynchronize());
            gpuErrChk(cudaFree(state_gpu));
        }
    }

    // Copy Assignment Operator
    wrapper_state_t &operator=(const wrapper_state_t &_other)
    {
        if (state_gpu != nullptr) {
            Destroy_State_Kernel<<<1, WF_GPU_THREADS_PER_BLOCK, 0>>>(state_gpu); // use the default CUDA stream
            gpuErrChk(cudaPeekAtLastError());
            gpuErrChk(cudaDeviceSynchronize());
            gpuErrChk(cudaFree(state_gpu));
        }
        gpuErrChk(cudaMalloc(&state_gpu, sizeof(state_t)));
        Copy_State_Kernel<<<1, WF_GPU_THREADS_PER_BLOCK>>>(state_gpu, _other.state_gpu); // use the default CUDA stream
        gpuErrChk(cudaPeekAtLastError());
        gpuErrChk(cudaDeviceSynchronize());
        return *this;
    }

    // Move Assignment Operator
    wrapper_state_t &operator=(wrapper_state_t &&_other)
    {
        state_gpu = std::exchange(_other.state_gpu, nullptr);
        return *this;
    }
};

// Function to create a window result on GPU
template<typename result_t, typename key_t>
__device__ inline result_t create_win_result_t_gpu(key_t _key,
                                                   uint64_t _id=0)
{
    if constexpr (std::is_same<key_t, empty_key_t>::value) { // case without key
        result_t res(_id); // constructor with id parameter
        return res;
    }
    else { // case with key
        result_t res(_key, _id); // constructor with key and id parameters
        return res;
    }
}

//@endcond

} // namespace wf

#endif
