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
 *  @file    flatfat_gpu_u.hpp
 *  @author  Gabriele Mencagli and Elia Ruggeri
 *  
 *  @brief Flat Fixed-size Aggregator Tree on GPU (Version with CUDA Unified Memory)
 *  
 *  @section FlatFAT_GPU (Description)
 *  
 *  This file implements the GPU-based Flat Fixed-size Aggregator Tree (FlatFAT_GPU).
 *  It is based on the FlatFAT data structure [1], revisited to process batch of
 *  windows efficiently on a GPU device. Version with CUDA Unified Memory.
 *  
 *  [1] Kanat Tangwongsan, et al. 2015. General incremental sliding-window aggregation.
 *  Proc. VLDB Endow. 8, 7 (February 2015), 702–713.
 */ 

#ifndef FLATFAT_GPU_U_H
#define FLATFAT_GPU_U_H

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
template<typename result_t, typename combgpu_func_t>
__global__ void Init_TreeLevel_Kernel(combgpu_func_t comb_func,
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
template<typename result_t, typename combgpu_func_t>
__global__ void Update_TreeLevel_Kernel(combgpu_func_t comb_func,
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
template<typename key_t, typename result_t, typename combgpu_func_t>
__global__ void Compute_Results_Kernel(combgpu_func_t comb_func,
                                       key_t key,
                                       result_t *fat, 
                                       result_t *results,
                                       uint64_t initial_gwid,
                                       size_t offset,
                                       int numLeaves,
                                       int B,
                                       int W,
                                       int numWinsPerBatch,
                                       int S)
{
    int id = threadIdx.x + blockIdx.x * blockDim.x; // id of the thread in the kernel
    int num_threads = gridDim.x * blockDim.x; // number of threads in the kernel
    for (size_t i=id; i<numWinsPerBatch; i+=num_threads) {
        int wS = (offset + i * S) % B;
        int WIN = W;
        results[i] = create_win_result_t_gpu<result_t, key_t>(key, initial_gwid + i);
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
            comb_func(results[i], fat[tn], results[i]);
            int oldWS = wS;
            wS += range;
            range = wS >= B ? B - oldWS : range;
            WIN -= range;
        }
    }
}

// class FlatFAT_GPU
template<typename result_t, typename key_t, typename combgpu_func_t>
class FlatFAT_GPU
{
private:
    combgpu_func_t comb_func; // combine function
    size_t treeSize; // size of the tree (in number of results)
    size_t treeSizeBytes; // size of the tree (in bytes)
    size_t batchSize; // size of the batch (in number of results)
    size_t batchSizeBytes; // size of the batch (in bytes)
    size_t numWinsPerBatch; // number of windows within a batch
    size_t numLeaves; // number of results representing the leaves of the tree
    size_t leavesSizeBytes; // size of the leaves of the tree (in bytes)
    size_t windowSize; // size of the windows (in number of results)
    size_t slide; // size of the slide (in number of results)
    key_t key; // key value used by the FlatFAT_GPU
    size_t offset; // offset value
    result_t *tree_u = nullptr; // array of the whole tree in CUDA unified memory
    result_t *results_u = nullptr; // array of the final results in CUDA unified memory
    int numSMs = 0; // number of SMs in the GPU
    int max_blocks_per_sm; // maximum number of blocks resident on each Stream Multiprocessor of the GPU
    cudaStream_t *cudaStream; // pointer to the CUDA stream used by the FlatFAT_GPU
    cudaDeviceProp deviceProp; // object containing the properties of the used GPU device
    int isTegra; // flag equal to 1 if the GPU (device 0) is integrated (Tegra), 0 otherwise

public:
    // Constructor
    FlatFAT_GPU(combgpu_func_t _comb_func,
                size_t _batchSize,
                size_t _numWinsPerBatch,
                size_t _windowSize,
                size_t _slide,
                key_t _key,
                cudaStream_t *_cudaStream,
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
                cudaStream(_cudaStream)
    {
        size_t noBits = (size_t) ceil(log2(batchSize));
        size_t n = 1 << noBits;
        treeSize = n*2-1;
        treeSizeBytes = treeSize * sizeof(result_t);
        numLeaves = n;
        leavesSizeBytes = numLeaves * sizeof(result_t);
        batchSizeBytes = batchSize * sizeof(result_t);
        gpuErrChk(cudaGetDeviceProperties(&deviceProp, 0)); // get the properties of the GPU device with id 0
        gpuErrChk(cudaDeviceGetAttribute(&isTegra, cudaDevAttrIntegrated, 0));
#if defined (WF_GPU_PINNED_MEMORY)
        if (!isTegra) {
            std::cerr << RED << "WindFlow Error: pinned memory support can be used on Tegra devices only" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        else {
            // allocate the pinned memory arrays
            gpuErrChk(cudaMallocHost(&tree_u, treeSizeBytes));
            gpuErrChk(cudaMallocHost(&results_u, numWinsPerBatch * sizeof(result_t)));
        }
#else
        if (!isTegra && deviceProp.concurrentManagedAccess) { // new discrete GPU models
            // allocate the CUDA unified memory arrays
            gpuErrChk(cudaMallocManaged(&tree_u, treeSizeBytes));
            gpuErrChk(cudaMallocManaged(&results_u, numWinsPerBatch * sizeof(result_t)));
        }
        else { // old discrete GPU models and Tegra devices
            // allocate the CUDA unified memory arrays and attach them to the host side
            gpuErrChk(cudaMallocManaged(&tree_u, treeSizeBytes, cudaMemAttachHost));
            gpuErrChk(cudaMallocManaged(&results_u, numWinsPerBatch * sizeof(result_t), cudaMemAttachHost));
            // attach the CUDA unified memory arrays to the GPU side
            gpuErrChk(cudaStreamAttachMemAsync(*cudaStream, tree_u, treeSizeBytes, cudaMemAttachSingle));
            gpuErrChk(cudaStreamAttachMemAsync(*cudaStream, results_u, numWinsPerBatch * sizeof(result_t), cudaMemAttachSingle));
        }
#endif
    }

    // Copy Constructor
    FlatFAT_GPU(const FlatFAT_GPU &_other):
                comb_func(_other._comb_func),
                treeSize(_other.treeSize),
                treeSizeBytes(_other.treeSizeBytes),
                batchSize(_other.batchSize),
                batchSizeBytes(_other.batchSizeBytes),
                numWinsPerBatch(_other.numWinsPerBatch),
                numLeaves(_other.numLeaves),
                leavesSizeBytes(_other.leavesSizeBytes),
                windowSize(_other.windowSize),
                slide(_other.slide),
                key(_other.key),
                offset(_other.offset),
                numSMs(_other.numSMs),
                max_blocks_per_sm(_other.max_blocks_per_sm),
                cudaStream(_other.cudaStream),
                deviceProp(_other.deviceProp),
                isTegra(_other.isTegra)
    {
#if defined (WF_GPU_PINNED_MEMORY)
        if (!isTegra) {
            std::cerr << RED << "WindFlow Error: pinned memory support can be used on Tegra devices only" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        else {
            // allocate the pinned memory arrays
            gpuErrChk(cudaMallocHost(&tree_u, treeSizeBytes));
            gpuErrChk(cudaMallocHost(&results_u, numWinsPerBatch * sizeof(result_t)));
        }
#else
        if (!isTegra && deviceProp.concurrentManagedAccess) { // new discrete GPU models
            // allocate the CUDA unified memory arrays
            gpuErrChk(cudaMallocManaged(&tree_u, treeSizeBytes));
            gpuErrChk(cudaMallocManaged(&results_u, numWinsPerBatch * sizeof(result_t)));
        }
        else { // old discrete GPU models and Tegra devices
            // allocate the CUDA unified memory arrays and attach them to the host side
            gpuErrChk(cudaMallocManaged(&tree_u, treeSizeBytes, cudaMemAttachHost));
            gpuErrChk(cudaMallocManaged(&results_u, numWinsPerBatch * sizeof(result_t), cudaMemAttachHost));
            // attach the CUDA unified memory arrays to the GPU side
            gpuErrChk(cudaStreamAttachMemAsync(*cudaStream, tree_u, treeSizeBytes, cudaMemAttachSingle));
            gpuErrChk(cudaStreamAttachMemAsync(*cudaStream, results_u, numWinsPerBatch * sizeof(result_t), cudaMemAttachSingle));
        }
#endif
    }

    // Move Constructor
    FlatFAT_GPU(FlatFAT_GPU &&_other):
                comb_func(std::move(_other.comb_func)),
                treeSize(_other.treeSize),
                treeSizeBytes(_other.treeSizeBytes),
                batchSize(_other.batchSize),
                batchSizeBytes(_other.batchSizeBytes),
                numWinsPerBatch(_other.numWinsPerBatch),
                numLeaves(_other.numLeaves),
                leavesSizeBytes(_other.leavesSizeBytes),
                windowSize(_other.windowSize),
                slide(_other.slide),
                key(std::move(_other.key)),
                offset(_other.offset),
                tree_u(std::exchange(_other.tree_u, nullptr)),
                results_u(std::exchange(_other.results_u, nullptr)),
                numSMs(_other.numSMs),
                max_blocks_per_sm(_other.max_blocks_per_sm),
                cudaStream(std::exchange(_other.cudaStream, nullptr)),
                deviceProp(_other.deviceProp),
                isTegra(_other.isTegra) {}

    // Destructor
    ~FlatFAT_GPU()
    {
        if (tree_u != nullptr) {
            cudaFree(tree_u);
        }
        if (results_u != nullptr) {
            cudaFree(results_u);
        }
    }

    // Copy Assignment Operator
    FlatFAT_GPU &operator=(const FlatFAT_GPU &_other)
    {
        if (this != &_other) {
            comb_func = _other.comb_func;
            treeSize = _other.treeSize;
            treeSizeBytes = _other.treeSizeBytes;
            batchSize = _other.batchSize;
            batchSizeBytes = _other.batchSizeBytes;
            numWinsPerBatch = _other.numWinsPerBatch;
            numLeaves = _other.numLeaves;
            leavesSizeBytes = _other.leavesSizeBytes;
            windowSize = _other.windowSize;
            slide = _other.slide;
            key = _other.key;
            offset = _other.offset;
            numSMs = _other.numSMs;
            max_blocks_per_sm = _other.max_blocks_per_sm;
            cudaStream = _other.cudaStream;
            deviceProp = _other.deviceProp;
            isTegra = _other.isTegra;
#if defined (WF_GPU_PINNED_MEMORY)
            if (!isTegra) {
                std::cerr << RED << "WindFlow Error: pinned memory support can be used on Tegra devices only" << DEFAULT_COLOR << std::endl;
                exit(EXIT_FAILURE);
            }
            else {
                // allocate the pinned memory arrays
                gpuErrChk(cudaMallocHost(&tree_u, treeSizeBytes));
                gpuErrChk(cudaMallocHost(&results_u, numWinsPerBatch * sizeof(result_t)));
            }
#else
            if (!isTegra && deviceProp.concurrentManagedAccess) { // new discrete GPU models
                // allocate the CUDA unified memory arrays
                gpuErrChk(cudaMallocManaged(&tree_u, treeSizeBytes));
                gpuErrChk(cudaMallocManaged(&results_u, numWinsPerBatch * sizeof(result_t)));
            }
            else { // old discrete GPU models and Tegra devices
                // allocate the CUDA unified memory arrays and attach them to the host side
                gpuErrChk(cudaMallocManaged(&tree_u, treeSizeBytes, cudaMemAttachHost));
                gpuErrChk(cudaMallocManaged(&results_u, numWinsPerBatch * sizeof(result_t), cudaMemAttachHost));
                // attach the CUDA unified memory arrays to the GPU side
                gpuErrChk(cudaStreamAttachMemAsync(*cudaStream, tree_u, treeSizeBytes, cudaMemAttachSingle));
                gpuErrChk(cudaStreamAttachMemAsync(*cudaStream, results_u, numWinsPerBatch * sizeof(result_t), cudaMemAttachSingle));
            }
#endif
        }
        return *this;
    }

    // Move Assignment Operator
    FlatFAT_GPU &operator=(FlatFAT_GPU &&_other)
    {
            comb_func = std::move(_other.comb_func);
            treeSize = _other.treeSize;
            treeSizeBytes = _other.treeSizeBytes;
            batchSize = _other.batchSize;
            batchSizeBytes = _other.batchSizeBytes;
            numWinsPerBatch = _other.numWinsPerBatch;
            numLeaves = _other.numLeaves;
            leavesSizeBytes = _other.leavesSizeBytes;
            windowSize = _other.windowSize;
            slide = _other.slide;
            key = std::move(_other.key);
            offset = _other.offset;
            tree_u = std::exchange(_other.tree_u, nullptr); 
            results_u = std::exchange(_other.results_u, nullptr);
            numSMs = _other.numSMs;
            max_blocks_per_sm = _other.max_blocks_per_sm;
            cudaStream = std::exchange(_other.cudaStream, nullptr);
            deviceProp = _other.deviceProp;
            isTegra = _other.isTegra;
            return *this;
    }

    // Build the FlatFAT_GPU tree
    void build(const std::vector<result_t> &inputs,
               uint64_t _initial_gwid)
    {
        assert(inputs.size() == batchSize); // sanity check
        std::vector<result_t> tree(inputs.begin(), inputs.end()); // copy the input vector in the tree vector
        tree.insert(tree.end(), treeSize - batchSize, create_win_result_t<result_t, key_t>(key, 0)); // fill the remaining entries in the tree with empty results
        assert(tree.size() == treeSize); // sanity check
        memcpy(tree_u,
               tree.data(),
               treeSizeBytes); // copy the tree vector in the tree arrey in CUDA unified memory
#if !defined(WF_GPU_NO_PREFETCHING) && !defined(WF_GPU_PINNED_MEMORY)
        if (isTegra) { // Tegra devices
            // attach the CUDA unified memory array results_u to the GPU side
            gpuErrChk(cudaStreamAttachMemAsync(*cudaStream, tree_u, treeSizeBytes, cudaMemAttachSingle));
        }
        else if (deviceProp.concurrentManagedAccess) { // new discrete GPU models
            // prefetch the CUDA unified memory array tree_u to the GPU side
            gpuErrChk(cudaMemPrefetchAsync(tree_u, treeSizeBytes, 0, *cudaStream)); // device_id = 0
        }
#endif
        result_t *d_levelA = tree_u; // pointer to the first level of the tree
        int pow = 1;
        result_t *d_levelB = d_levelA + numLeaves / pow;
        int i = numLeaves / 2;
        while (d_levelB < tree_u + treeSize && i > 0) { // fill the levels of the tree
            int numBlocks = std::min((int) ceil(i / ((double) WF_GPU_THREADS_PER_BLOCK)), numSMs * max_blocks_per_sm);
            Init_TreeLevel_Kernel<result_t, combgpu_func_t>
                                 <<<numBlocks, WF_GPU_THREADS_PER_BLOCK, 0, *cudaStream>>>(comb_func,
                                                                                                   d_levelA,
                                                                                                   d_levelB,
                                                                                                   i);
            gpuErrChk(cudaPeekAtLastError());
            d_levelA = d_levelB; // switch the levels
            pow = pow << 1;
            d_levelB = d_levelA + numLeaves / pow;
            i /= 2;
        }
        int numBlocks = std::min((int) ceil(numWinsPerBatch / ((double) WF_GPU_THREADS_PER_BLOCK)), numSMs * max_blocks_per_sm); // compute the results of the first batch
        Compute_Results_Kernel<key_t, result_t, combgpu_func_t>
                              <<<numBlocks, WF_GPU_THREADS_PER_BLOCK, 0, *cudaStream>>>(comb_func,
                                                                                                key,
                                                                                                tree_u,
                                                                                                results_u,
                                                                                                _initial_gwid,
                                                                                                offset,
                                                                                                numLeaves,
                                                                                                batchSize,
                                                                                                windowSize,
                                                                                                numWinsPerBatch,
                                                                                                slide);
        gpuErrChk(cudaPeekAtLastError());
#if !defined(WF_GPU_NO_PREFETCHING) && !defined(WF_GPU_PINNED_MEMORY)
        if (isTegra) { // Tegra devices
            // attach the CUDA unified memory array results_u to the host side
            gpuErrChk(cudaStreamAttachMemAsync(*cudaStream, results_u, numWinsPerBatch * sizeof(result_t), cudaMemAttachHost));
        }
        else if (deviceProp.concurrentManagedAccess) { // new discrete GPU models
            // prefetch the CUDA unified memory array results_u to the host side
            gpuErrChk(cudaMemPrefetchAsync(results_u, numWinsPerBatch * sizeof(result_t), cudaCpuDeviceId, *cudaStream));
        }
#endif
    }

    // Update the FlatFAT_GPU tree
    void update(const std::vector<result_t> &inputs,
                uint64_t _initial_gwid)
    {
        size_t spaceLeft = batchSize - offset; // compute the remaining space at the end of the tree
        if (inputs.size() <= spaceLeft) {
            memcpy(tree_u,
                   inputs.data(),
                   inputs.size() * sizeof(result_t)); // copy the inputs in the tree array in CUDA unified memory
        }
        else { // split the copy in two parts
            memcpy(tree_u + offset,
                   inputs.data(),
                   spaceLeft * sizeof(result_t));
            memcpy(tree_u,
                   inputs.data() + spaceLeft,
                   (inputs.size() - spaceLeft) * sizeof(result_t));
        }
#if !defined(WF_GPU_NO_PREFETCHING) && !defined(WF_GPU_PINNED_MEMORY)
        if (isTegra) { // Tegra devices
            // attach the CUDA unified memory array results_u to the GPU side
            gpuErrChk(cudaStreamAttachMemAsync(*cudaStream, tree_u, treeSizeBytes, cudaMemAttachSingle));
        }
        else if (deviceProp.concurrentManagedAccess) { // new discrete GPU models
            // prefetch the CUDA unified memory array tree_u to the GPU side
            gpuErrChk(cudaMemPrefetchAsync(tree_u, treeSizeBytes, 0, *cudaStream)); // device_id = 0
        }
#endif
        int pow = 1;
        result_t *d_levelA = tree_u;
        result_t *d_levelB = d_levelA + numLeaves / pow;
        size_t sizeB = ceil((double) batchSize / (pow << 1 ));
        size_t update_pos = Parent(offset, numLeaves);
        size_t numSeenElements = numLeaves;
        size_t distance = update_pos - numSeenElements;
        int sizeUpdate = ceil((double) inputs.size() / (pow << 1)) + 1;
        while (d_levelB < tree_u + treeSize) { // update the levels of the tree, each with a separate kernel
            // call the kernel to update a level of the tree
            size_t numBlocks = std::min((int) ceil(sizeUpdate / ((double) WF_GPU_THREADS_PER_BLOCK)), numSMs * max_blocks_per_sm);
            Update_TreeLevel_Kernel<result_t, combgpu_func_t>
                                   <<<numBlocks, WF_GPU_THREADS_PER_BLOCK, 0, *cudaStream>>>(comb_func,
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
            sizeUpdate = ceil((double) inputs.size() / (pow << 1)) + 1;
        }
        offset = (offset + inputs.size()) % batchSize;
        int numBlocks = std::min((int) ceil(numWinsPerBatch / ((double) WF_GPU_THREADS_PER_BLOCK)), numSMs * max_blocks_per_sm);
        Compute_Results_Kernel<key_t, result_t, combgpu_func_t>
                              <<<numBlocks, WF_GPU_THREADS_PER_BLOCK, 0, *cudaStream>>>(comb_func,
                                                                                                key,
                                                                                                tree_u,
                                                                                                results_u,
                                                                                                _initial_gwid,
                                                                                                offset,
                                                                                                numLeaves,
                                                                                                batchSize,
                                                                                                windowSize,
                                                                                                numWinsPerBatch,
                                                                                                slide);
        gpuErrChk(cudaPeekAtLastError());
#if !defined(WF_GPU_NO_PREFETCHING) && !defined(WF_GPU_PINNED_MEMORY)
        if (isTegra) { // Tegra devices
            // attach the CUDA unified memory array results_u to the host side
            gpuErrChk(cudaStreamAttachMemAsync(*cudaStream, results_u, numWinsPerBatch * sizeof(result_t), cudaMemAttachHost));
        }
        else if (deviceProp.concurrentManagedAccess) { // new discrete GPU models
            // prefetch the CUDA unified memory array results_u to the host side
            gpuErrChk(cudaMemPrefetchAsync(results_u, numWinsPerBatch * sizeof(result_t), cudaCpuDeviceId, *cudaStream));
        }
#endif
    }

    // Get (synchronously) the array of results computed by the GPU
    result_t *getSyncResults()
    {
        gpuErrChk(cudaStreamSynchronize(*cudaStream));
        return results_u;
    }

    // Start (asynchronously) the copy of the array of results from the GPU
    void getAsyncResults() {} // it is a stub to maintain the same interface

    // Wait the completion of the results copy from the GPU to the host
    result_t *waitResults()
    {
        gpuErrChk(cudaStreamSynchronize(*cudaStream));
        return results_u;
    }

    // Get the vector of batched inputs
    std::vector<result_t> getBatchedTuples()
    {
        gpuErrChk(cudaStreamSynchronize(*cudaStream));
        result_t *tmp_results = nullptr;
        errChkMalloc(tmp_results = (result_t *) malloc(treeSizeBytes));
        memcpy(tmp_results,
               (tree_u + offset),
               (batchSize - offset) * sizeof(result_t));
        memcpy((tmp_results + batchSize - offset),
               tree_u,
               offset * sizeof(result_t));
        std::vector<result_t> output(tmp_results, tmp_results + batchSize);
        free(tmp_results);
        return output;
    }
};

} // namespace wf

#endif
