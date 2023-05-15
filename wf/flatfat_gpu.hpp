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
 *  @file    flatfat_gpu.hpp
 *  @author  Gabriele Mencagli and Elia Ruggeri
 *  
 *  @brief Flat Fixed-size Aggregator Tree on GPU
 *  
 *  @section FlatFAT_GPU (Description)
 *  
 *  This file implements the GPU-based Flat Fixed-size Aggregator Tree (FlatFAT_GPU).
 *  It is based on the FlatFAT data structure[1], revisited to process batch of
 *  windows efficiently on GPU devices.
 *  
 *  [1] Kanat Tangwongsan, et al. 2015. General incremental sliding-window aggregation.
 *  Proc. VLDB Endow. 8, 7 (February 2015), 702–713.
 */ 

#ifndef FLATFAT_GPU_H
#define FLATFAT_GPU_H

// includes
#include<list>
#include<cmath>
#include<vector>
#include<utility>
#include<algorithm>
#include<functional>
#include<basic_gpu.hpp>

namespace wf {

// function to compute the partent identifier of node at index pos
__host__ __device__ int Parent(int pos, int B)
{
    return (pos >> 1) | B;
}

// CUDA KERNEL: Init_TreeLevel_Kernel
template<typename result_t, typename comb_func_gpu_t>
__global__ void Init_TreeLevel_Kernel(comb_func_gpu_t comb_func,
                                      result_t *levelA,
                                      result_t *levelB,
                                      size_t levelBSize)
{
    int id = threadIdx.x + blockIdx.x * blockDim.x; // id of the thread in the kernel
    int num_threads = gridDim.x * blockDim.x; // number of threads in the kernel
    for (size_t i=id; i<levelBSize; i+=num_threads) {
        comb_func(levelA[i*2], levelA[i*2+1], levelB[i]);
    }
}

// CUDA KERNEL: Update_TreeLevel_Kernel
template<typename result_t, typename comb_func_gpu_t>
__global__ void Update_TreeLevel_Kernel(comb_func_gpu_t comb_func,
                                        result_t *levelA,
                                        result_t *levelB,
                                        size_t offset,
                                        size_t levelBSize,
                                        int sizeUpdate)
{
    int id = threadIdx.x + blockIdx.x * blockDim.x; // id of the thread in the kernel
    int num_threads = gridDim.x * blockDim.x; // number of threads in the kernel
    for (size_t i=id; i<sizeUpdate; i+=num_threads) {
        size_t my_i = (i + offset) % levelBSize;
        comb_func(levelA[my_i*2], levelA[my_i*2+1], levelB[my_i]);
    }
}

// CUDA KERNEL: Compute_Results_Kernel (thanks to Massimo Coppola, ISTI-CNR, Pisa, Italy)
template<typename key_t, typename result_t, typename comb_func_gpu_t>
__global__ void Compute_Results_Kernel(comb_func_gpu_t comb_func,
                                       key_t key,
                                       result_t *fat,
                                       batch_item_gpu_t<result_t> *results,
                                       uint64_t initial_gwid,
                                       size_t offset,
                                       int numLeaves,
                                       int B,
                                       int W,
                                       int numWinsPerBatch,
                                       int S,
                                       uint64_t time)
{
    int id = threadIdx.x + blockIdx.x * blockDim.x; // id of the thread in the kernel
    int num_threads = gridDim.x * blockDim.x; // number of threads in the kernel
    for (size_t i=id; i<numWinsPerBatch; i+=num_threads) {
        int wS = (offset + i * S) % B;
        int WIN = W;
        results[i].tuple = create_win_result_t_gpu<result_t, key_t>(key, initial_gwid + i);
        results[i].timestamp = time;
        while(WIN > 0) {
            int range;
            wS = wS >= B ? 0 : wS;
            range = wS == 0 ? B : ( wS & -wS );
            int64_t pow = WIN;
            pow |= pow >> 1;
            pow |= pow >> 2;
            pow |= pow >> 4;
            pow |= pow >> 8;
            pow |= pow >> 16;
            pow |= pow >> 32;
            pow = (pow >> 1) + 1;
            range = range < pow ? range : pow;
            int tr = range;
            int tn = wS;
            while (tr > 1) {
                tn = Parent(tn, numLeaves);
                tr >>= 1;
            }
            comb_func(results[i].tuple, fat[tn], results[i].tuple);
            int oldWS = wS;
            wS += range;
            range = wS >= B ? B - oldWS : range;
            WIN -= range;
        }
    }
}

// class FlatFAT_GPU
template<typename result_t, typename key_t, typename comb_func_gpu_t>
class FlatFAT_GPU
{
private:
    comb_func_gpu_t comb_func; // combine function
    size_t treeSize; // size of the tree (in number of results)
    size_t treeSizeBytes; // size of the tree (in bytes)
    size_t batchSize; // size of the batch (in number of results)
    size_t batchSizeBytes; // size of the batch (in bytes)
    size_t numWinsPerBatch; // number of consecutive window results per batch
    size_t numLeaves; // number of results representing the leaves of the tree
    size_t leavesSizeBytes; // size of the leaves of the tree (in bytes)
    size_t windowSize; // size of the windows (in number of results)
    size_t slide; // size of the slide (in number of results)
    key_t key; // key value used by the FlatFAT_GPU
    uint64_t offset; // offset value
    result_t *gpu_tree = nullptr; // GPU array of the whole tree
    int numSMs; // number of SMs in the GPU
    int max_blocks_per_sm; // maximum number of blocks resident on each Stream Multiprocessor of the GPU
    uint64_t incr; // increment value (used by add_cb only)

public:
    // Constructor
    FlatFAT_GPU(comb_func_gpu_t _comb_func,
                size_t _batchSize,
                size_t _numWinsPerBatch,
                size_t _windowSize,
                size_t _slide,
                key_t _key,
                size_t _numSMs,
                size_t _max_blocks_per_sm):
                comb_func(_comb_func),
                batchSize(_batchSize),
                numWinsPerBatch(_numWinsPerBatch),
                windowSize(_windowSize),
                slide(_slide),
                key(_key),
                offset(0),
                numSMs(_numSMs),
                max_blocks_per_sm(_max_blocks_per_sm),
                incr(0)
    {
        size_t noBits = (size_t) ceil(log2(batchSize));
        size_t n = 1 << noBits;
        treeSize = n*2-1;
        treeSizeBytes = treeSize * sizeof(result_t);
        numLeaves = n;
        leavesSizeBytes = numLeaves * sizeof(result_t);
        batchSizeBytes = batchSize * sizeof(result_t);
        gpuErrChk(cudaMalloc(&gpu_tree, treeSizeBytes));
    }

    // Copy Constructor
    FlatFAT_GPU(const FlatFAT_GPU &_other):
                comb_func(_other.comb_func),
                batchSize(_other.batchSize),
                numWinsPerBatch(_other.numWinsPerBatch),
                windowSize(_other.windowSize),
                slide(_other.slide),
                key(_other.key),
                offset(_other.offset),
                numSMs(_other.numSMs),
                max_blocks_per_sm(_other.max_blocks_per_sm),
                incr(_other.incr)      
    {
        size_t noBits = (size_t) ceil(log2(batchSize));
        size_t n = 1 << noBits;
        treeSize = n*2-1;
        treeSizeBytes = treeSize * sizeof(result_t);
        numLeaves = n;
        leavesSizeBytes = numLeaves * sizeof(result_t);
        batchSizeBytes = batchSize * sizeof(result_t);
        gpuErrChk(cudaMalloc(&gpu_tree, treeSizeBytes));
    }

    // Destructor
    ~FlatFAT_GPU()
    {
        if (gpu_tree != nullptr) {
            cudaFree(gpu_tree);
        }
    }

    // Add inputs to the FlatFAT_GPU tree (CB semantics)
    void add_cb(result_t *_new_data,
                size_t _size,
                cudaStream_t &_stream)
    {
        // copy _new_data
        size_t spaceLeft = batchSize - ((offset + incr) % batchSize); // compute the remaining space at the end of the tree
        if (_size <= spaceLeft) {
            gpuErrChk(cudaMemcpyAsync(gpu_tree + ((offset + incr) % batchSize),
                                      _new_data,
                                      _size * sizeof(result_t),
                                      cudaMemcpyDeviceToDevice,
                                      _stream));
        }
        else { // split the copy in two parts
            gpuErrChk(cudaMemcpyAsync(gpu_tree + ((offset + incr) % batchSize),
                                      _new_data,
                                      spaceLeft * sizeof(result_t),
                                      cudaMemcpyDeviceToDevice,
                                      _stream));
            gpuErrChk(cudaMemcpyAsync(gpu_tree,
                                      _new_data + spaceLeft,
                                      (_size - spaceLeft) * sizeof(result_t),
                                      cudaMemcpyDeviceToDevice,
                                      _stream));
        }
        incr += _size;
    }

    // Add inputs to the FlatFAT_GPU tree (TB semantics)
    void add_tb(result_t *_new_data,
                size_t _size,
                cudaStream_t &_stream)
    {
        // copy _new_data
        size_t spaceLeft = batchSize - offset; // compute the remaining space at the end of the tree
        if (_size <= spaceLeft) {
            gpuErrChk(cudaMemcpyAsync(gpu_tree + offset,
                                      _new_data,
                                      _size * sizeof(result_t),
                                      cudaMemcpyDeviceToDevice,
                                      _stream));
        }
        else { // split the copy in two parts
            gpuErrChk(cudaMemcpyAsync(gpu_tree + offset,
                                      _new_data,
                                      spaceLeft * sizeof(result_t),
                                      cudaMemcpyDeviceToDevice,
                                      _stream));
            gpuErrChk(cudaMemcpyAsync(gpu_tree,
                                      _new_data + spaceLeft,
                                      (_size - spaceLeft) * sizeof(result_t),
                                      cudaMemcpyDeviceToDevice,
                                      _stream));
        }
    }

    // Add inputs to the FlatFAT_GPU tree (TB semantics)
    void add_tb(result_t *_new_data_1,
                size_t _size_1,
                result_t *_new_data_2,
                size_t _size_2,
                cudaStream_t &_stream)
    {
        assert(_size_1 + _size_2 <= batchSize); // sanity check
        size_t offset2 = 0;
        // copy _new_data_1
        size_t spaceLeft = batchSize - offset; // compute the remaining space at the end of the tree
        if (_size_1 <= spaceLeft) {
            gpuErrChk(cudaMemcpyAsync(gpu_tree + offset,
                                      _new_data_1,
                                      _size_1 * sizeof(result_t),
                                      cudaMemcpyDeviceToDevice,
                                      _stream));
            offset2 = (offset + _size_1) % batchSize;
        }
        else { // split the copy in two parts
            gpuErrChk(cudaMemcpyAsync(gpu_tree + offset,
                                      _new_data_1,
                                      spaceLeft * sizeof(result_t),
                                      cudaMemcpyDeviceToDevice,
                                      _stream));
            gpuErrChk(cudaMemcpyAsync(gpu_tree,
                                      _new_data_1 + spaceLeft,
                                      (_size_1 - spaceLeft) * sizeof(result_t),
                                      cudaMemcpyDeviceToDevice,
                                      _stream));
            offset2 = (_size_1 - spaceLeft);
        }
        // copy _new_data_2
        size_t spaceLeft2 = batchSize - offset2; // compute the remaining space at the end  of the tree
        if (_size_2 <= spaceLeft2) {
            gpuErrChk(cudaMemcpyAsync(gpu_tree + offset2,
                                      _new_data_2,
                                      _size_2 * sizeof(result_t),
                                      cudaMemcpyDeviceToDevice,
                                      _stream));
        }
        else { // split the copy in two parts
            gpuErrChk(cudaMemcpyAsync(gpu_tree + offset2,
                                      _new_data_2,
                                      spaceLeft2 * sizeof(result_t),
                                      cudaMemcpyDeviceToDevice,
                                      _stream));
            gpuErrChk(cudaMemcpyAsync(gpu_tree,
                                      _new_data_2 + spaceLeft2,
                                      (_size_2 - spaceLeft2) * sizeof(result_t),
                                      cudaMemcpyDeviceToDevice,
                                      _stream));
        }
    }

    // Build the FlatFAT_GPU tree
    void build(cudaStream_t &_stream)
    {
        result_t *d_levelA = gpu_tree; // pointer to the first level of the tree
        int pow = 1;
        result_t *d_levelB = d_levelA + numLeaves / pow;
        int i = numLeaves / 2;
        while (d_levelB < gpu_tree + treeSize && i > 0) { // fill the levels of the tree
            int numBlocks = std::min((int) ceil(i / ((double) WF_GPU_THREADS_PER_BLOCK)), numSMs * max_blocks_per_sm);
            Init_TreeLevel_Kernel<result_t, comb_func_gpu_t>
                                 <<<numBlocks, WF_GPU_THREADS_PER_BLOCK, 0, _stream>>>(comb_func,
                                                                                       d_levelA,
                                                                                       d_levelB,
                                                                                       i);
            gpuErrChk(cudaPeekAtLastError());
            d_levelA = d_levelB; // switch the levels
            pow = pow << 1;
            d_levelB = d_levelA + numLeaves / pow;
            i /= 2;
        }
        incr = 0;
        gpuErrChk(cudaStreamSynchronize(_stream));
    }

    // Update the FlatFAT_GPU tree
    void update(size_t _num_new_inputs, cudaStream_t &_stream)
    {
        int pow = 1;
        result_t *d_levelA = gpu_tree;
        result_t *d_levelB = d_levelA + numLeaves / pow;
        size_t sizeB = ceil((double) batchSize / (pow << 1 ));
        size_t update_pos = Parent(offset, numLeaves);
        size_t numSeenElements = numLeaves;
        size_t distance = update_pos - numSeenElements;
        int sizeUpdate = ceil((double) _num_new_inputs / (pow << 1)) + 1;
        while (d_levelB < gpu_tree + treeSize) { // update the levels of the tree, each with a separate kernel
            // call the kernel to update a level of the tree
            size_t numBlocks = std::min((int) ceil(sizeUpdate / ((double) WF_GPU_THREADS_PER_BLOCK)), numSMs * max_blocks_per_sm);
            Update_TreeLevel_Kernel<result_t, comb_func_gpu_t>
                                   <<<numBlocks, WF_GPU_THREADS_PER_BLOCK, 0, _stream>>>(comb_func,
                                                                                         d_levelA,
                                                                                         d_levelB,
                                                                                         distance,
                                                                                         sizeB,
                                                                                         sizeUpdate);
            gpuErrChk(cudaPeekAtLastError());
            pow = pow << 1;
            d_levelA = d_levelB;
            d_levelB = d_levelA + numLeaves / pow;
            sizeB = ceil((double) batchSize / (pow << 1));
            update_pos = Parent(update_pos, numLeaves);
            numSeenElements += numLeaves / pow;
            distance = update_pos - numSeenElements;
            sizeUpdate = ceil((double) _num_new_inputs / (pow << 1)) + 1;
        }
        offset = (offset + _num_new_inputs) % batchSize;
        incr = 0;
        gpuErrChk(cudaStreamSynchronize(_stream));
    }

    // Compute results
    void computeResults(batch_item_gpu_t<result_t> *results_gpu,
                        uint64_t _initial_gwid,
                        uint64_t _watermark,
                        cudaStream_t &_stream)
    {
        int numBlocks = std::min((int) ceil(numWinsPerBatch / ((double) WF_GPU_THREADS_PER_BLOCK)), numSMs * max_blocks_per_sm);
        Compute_Results_Kernel<key_t, result_t, comb_func_gpu_t>
                              <<<numBlocks, WF_GPU_THREADS_PER_BLOCK, 0, _stream>>>(comb_func,
                                                                                    key,
                                                                                    gpu_tree,
                                                                                    results_gpu,
                                                                                    _initial_gwid,
                                                                                    offset,
                                                                                    numLeaves,
                                                                                    batchSize,
                                                                                    windowSize,
                                                                                    numWinsPerBatch,
                                                                                    slide,
                                                                                    _watermark);
        gpuErrChk(cudaPeekAtLastError());
        gpuErrChk(cudaStreamSynchronize(_stream));
    }

    FlatFAT_GPU(FlatFAT_GPU &&) = delete; ///< Move constructor is deleted
    FlatFAT_GPU &operator=(const FlatFAT_GPU &) = delete; ///< Copy assignment operator is deleted
    FlatFAT_GPU &operator=(FlatFAT_GPU &&) = delete; ///< Move assignment operator is deleted
};

} // namespace wf

#endif
