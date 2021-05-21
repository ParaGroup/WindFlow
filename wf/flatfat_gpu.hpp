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
 *  @file    flatfat_gpu.hpp
 *  @author  Elia Ruggeri and Gabriele Mencagli
 *  @date    15/03/2020
 *  
 *  @brief Flat Fixed-size Aggregator Tree on GPU
 *  
 *  @section FlatFAT_GPU (Description)
 *  
 *  This file implements the Flat Fixed-size Aggregator Tree on GPU (FlatFAT_GPU), which
 *  is a modified version of the original FlatFAT able to offload the processing on a GPU
 *  device. As for the other GPU-based sliding-window operators in the library, the approach
 *  consists in buffering tuples belonging to a batch of B>0 consecutive windows, where
 *  each window is processed in parallel by the GPU cores. To process windows within the
 *  same batch, the FLATFAT_GPU allows computing windows by using partial results shared
 *  between cosecutive windows.
 */ 

#ifndef FLATFAT_GPU_H
#define FLATFAT_GPU_H

// includes
#include<cmath>
#include<vector>
#include<utility>
#include<algorithm>
#include<functional>
#include<basic.hpp>
#if defined (TRACE_WINDFLOW)
    #include<stats_record.hpp>
#endif

namespace wf {

// CUDA KERNEL: initialize a level of the tree
template<typename result_t, typename comb_F_t>
__global__ void InitTreeLevel_Kernel(comb_F_t winComb_func,
                                     result_t *levelA,
                                     result_t *levelB,
                                     size_t levelBSize)
{
    int id = blockIdx.x * blockDim.x + threadIdx.x;
    int stride = blockDim.x * gridDim.x;
    // grid-stride loop
    for (size_t i=id; i<levelBSize; i+=stride) {
        winComb_func(levelA[i*2], levelA[i*2+1], levelB[i]);
    }
}

// CUDA KERNEL: update a level of the tree
template<typename result_t, typename comb_F_t>
__global__ void UpdateTreeLevel_Kernel(comb_F_t winComb_func,
                                       result_t *levelA,
                                       result_t *levelB,
                                       size_t offset,
                                       size_t levelBSize,
                                       int sizeUpdate)
{
    int id = blockIdx.x * blockDim.x + threadIdx.x;
    int stride = blockDim.x * gridDim.x;
    // grid-stride loop
    for (size_t i=id; i<sizeUpdate; i+=stride) {
        size_t my_i = (i + offset) % levelBSize;
        winComb_func(levelA[my_i*2], levelA[my_i*2+1], levelB[my_i]);
    }
}

// function to compute the partent identifier of node pos
__host__ __device__ int Parent(int pos, int B)
{
    return (pos >> 1) | B;
}

// CUDA KERNEL: compute the results of all the windows within the batch (thanks to Massimo Coppola, ISTI-CNR, Pisa, Italy)
template<typename result_t, typename comb_F_t>
__global__ void ComputeResults_Kernel(comb_F_t winComb_func,
                                      result_t *fat, 
                                      result_t *results,
                                      size_t offset,
                                      int numLeaves,
                                      int B,
                                      int W,
                                      int b_id,
                                      int Nb,
                                      int S)
{
    int id = blockIdx.x * blockDim.x + threadIdx.x;
    int stride = blockDim.x * gridDim.x;
    // grid-stride loop
    for (size_t i=id; i<Nb; i+=stride) {
        int wS = (offset + i * S) % B;
        int WIN = W;
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
            winComb_func(results[i], fat[tn], results[i]);
            int oldWS = wS;
            wS += range;
            range = wS >= B ? B - oldWS : range;
            WIN -= range;
        }
    }
}

// class FlatFAT_GPU
template<typename tuple_t, typename result_t, typename comb_F_t>
class FlatFAT_GPU
{
private:
    // type of the lift function
    using winLift_func_t = std::function<void(const tuple_t &, result_t &)>;
    tuple_t tmp; // never used
    // key data type
    using key_t = typename std::remove_reference<decltype(std::get<0>(tmp.getControlFields()))>::type;
    // CPU variables
    size_t treeSize; // size of the tree
    size_t treeMemSize; // size of the tree in bytes
    size_t batchSize; // size of the batch
    size_t batchMemSize; // size of the batch in bytes
    size_t noLeaves; // number of leaves
    size_t leavesMemSize; // size of the leaves in bytes
    size_t windowSize; // size of the windows (in number of tuples)
    size_t slide; // size of the slide (in number of tuples)
    key_t key; // key value used by this FlatFAT_GPU
    size_t Nb; // number of windows within a batch
    size_t offset; // offset value
    winLift_func_t winLift_func; // lift function
    comb_F_t winComb_func; // combine function
    result_t zero; // zero result
    // memory arrays allocated in a page-locked manner on the HOST (prefix "pinned" used for them)
    result_t *pinned_buffer = nullptr; // array of generale use
    result_t *pinned_results = nullptr; // array of results to be read from the tree (one per window within a batch)
    result_t *pinned_init_results = nullptr; // array of initial results
    // GPU variables
    int numSMs = 0; // number of SMs in the GPU
    size_t n_thread_block; // number of threads per block
    cudaStream_t *cudaStream; // pointer to the CUDA stream used by this FlatFAT_GPU
    // arrays allocated in the global memory of the GPU (prefix "gpu" ised for them)
    result_t *gpu_tree = nullptr; // array representing the whole flat tree
    result_t *gpu_results = nullptr; // array of results of a batch (one per window within a batch)
#if defined (TRACE_WINDFLOW)
    Stats_Record *stats_record = nullptr;
#endif

public:
    // Constructor
    FlatFAT_GPU(winLift_func_t _winLift_func,
                comb_F_t _winComb_func,
                size_t _batchSize,
                size_t _numWindows,
                size_t _windowSize,
                size_t _slide,
                key_t _key,
                cudaStream_t *_cudaStream,
                size_t _n_thread_block,
                size_t _numSMs):
                batchSize(_batchSize),
                windowSize(_windowSize),
                slide(_slide),
                key(_key),
                Nb(_numWindows),
                offset(0),
                winLift_func(_winLift_func),
                winComb_func(_winComb_func),
                numSMs(_numSMs),
                n_thread_block(_n_thread_block),
                cudaStream(_cudaStream)
                
    {
        size_t noBits = (size_t) ceil(log2(batchSize));
        size_t n = 1 << noBits;
        treeSize = n*2-1;
        treeMemSize = treeSize * sizeof(result_t);
        noLeaves = n;
        leavesMemSize = noLeaves * sizeof(result_t);
        batchMemSize = batchSize * sizeof(result_t);
        zero.setControlFields(key, 0, 0);
        // allocate the gpu_tree array on GPU
        gpuErrChk(cudaMalloc(&gpu_tree, treeMemSize));
        // allocate the gpu_results array on GPU
        gpuErrChk(cudaMalloc(&gpu_results, Nb * sizeof(result_t)));
        // allocate the pinned_buffer array on HOST
        gpuErrChk(cudaMallocHost(&pinned_buffer, treeMemSize));
        // allocate the pinned_results array on HOST
        gpuErrChk(cudaMallocHost(&pinned_results, Nb * sizeof(result_t)));
        // allocate the pinned_init_results array on HOST
        gpuErrChk(cudaMallocHost(&pinned_init_results, Nb * sizeof(result_t)));
        // initialize the content of pinned_init_results
        std::vector<result_t> tmp_vect;
        tmp_vect.resize(Nb, zero);
        memcpy(pinned_init_results, tmp_vect.data(), Nb * sizeof(result_t));
    }

    // move Constructor
    FlatFAT_GPU(FlatFAT_GPU &&_fatgpu):
                treeSize(_fatgpu.treeSize),
                treeMemSize(_fatgpu.treeMemSize),
                batchSize(_fatgpu.batchSize),
                batchMemSize(_fatgpu.batchMemSize),
                noLeaves(_fatgpu.noLeaves),
                leavesMemSize(_fatgpu.leavesMemSize),
                windowSize(_fatgpu.windowSize),
                slide(_fatgpu.slide),
                key(_fatgpu.key),
                Nb(_fatgpu.Nb),
                offset(_fatgpu.offset),
                winLift_func(_fatgpu.winLift_func),
                winComb_func(_fatgpu.winComb_func),
                zero(_fatgpu.zero),
                numSMs(_fatgpu.numSMs),
                n_thread_block(_fatgpu.n_thread_block),
                cudaStream(_fatgpu.cudaStream)
    {
        // allocate the gpu_tree array on GPU
        gpuErrChk(cudaMalloc(&gpu_tree, treeMemSize));
        // allocate the gpu_results array on GPU
        gpuErrChk(cudaMalloc(&gpu_results, Nb * sizeof(result_t)));
        // allocate the pinned_buffer array on HOST
        gpuErrChk(cudaMallocHost(&pinned_buffer, treeMemSize));
        // allocate the pinned_results array on HOST
        gpuErrChk(cudaMallocHost(&pinned_results, Nb * sizeof(result_t)));
        // allocate the pinned_init_results array on HOST
        gpuErrChk(cudaMallocHost(&pinned_init_results, Nb * sizeof(result_t)));
        // initialize the content of pinned_init_results
        std::vector<result_t> tmp_vect;
        tmp_vect.resize(Nb, zero);
        memcpy(pinned_init_results, tmp_vect.data(), Nb * sizeof(result_t));
    }

    // Destructor
    ~FlatFAT_GPU()
    {
        // deallocate the arrays on GPU
        gpuErrChk(cudaFree(gpu_tree));
        gpuErrChk(cudaFree(gpu_results));
        // deallocate the arrays on HOST
        gpuErrChk(cudaFreeHost(pinned_buffer));
        gpuErrChk(cudaFreeHost(pinned_results));
        gpuErrChk(cudaFreeHost(pinned_init_results));
    }

    // method to build the FlatFAT_GPU tree
    void build(const std::vector<result_t> &inputs, int b_id)
    {
        // check the size of the input vector
        if (inputs.size() != batchSize) {
            return;
        }
        // copy the input vector in the tree vector
        std::vector<result_t> tree(inputs.begin(), inputs.end());
        // fill the remaining entries in the tree with zeros
        tree.insert(tree.end(), treeSize - batchSize, zero);
        assert(tree.size() == treeSize);
        // copy tree in the page-locked memory
        memcpy(pinned_buffer, tree.data(), treeMemSize);
        // copy the tree data in the GPU
        gpuErrChk(cudaMemcpyAsync(gpu_tree, pinned_buffer, treeMemSize, cudaMemcpyHostToDevice, *cudaStream));
#if defined (TRACE_WINDFLOW)
        stats_record->bytes_copied_hd += treeMemSize;
#endif
        // pointer to the first level of the tree
        result_t *d_levelA = gpu_tree;
        int pow = 1;
        result_t *d_levelB = d_levelA + noLeaves / pow;
        int i = noLeaves / 2;
        cudaError_t err;
        // fill the levels of the tree, each with a separate kernel
        while (d_levelB < gpu_tree + treeSize && i > 0) {
            int noBlocks = std::min((int) ceil(i / ((double) n_thread_block)), 32 * numSMs); // at most 32 blocks per SM
            // call the kernel to initialize level i
#if defined (TRACE_WINDFLOW)
            stats_record->num_kernels++;
#endif
            InitTreeLevel_Kernel<result_t, comb_F_t><<<noBlocks, n_thread_block, 0, *cudaStream>>>(winComb_func, d_levelA, d_levelB, i);
            if (err = cudaGetLastError()) {
                std::cerr << RED << "WindFlow Error: invoking the GPU kernel (InitTreeLevel_Kernel) causes error -> " << err << DEFAULT_COLOR << std::endl;
                exit(EXIT_FAILURE);
            }
            // switch the levels
            d_levelA = d_levelB;
            pow = pow << 1;
            d_levelB = d_levelA + noLeaves / pow;
            i /= 2;
        }
        // copy the initial values in the GPU
#if defined (TRACE_WINDFLOW)
        stats_record->bytes_copied_hd += Nb * sizeof(result_t);
#endif
        gpuErrChk(cudaMemcpyAsync((void *) gpu_results, (void *) pinned_init_results, Nb * sizeof(result_t), cudaMemcpyHostToDevice, *cudaStream));
        // compute the results of the first batch
        int noBlocks = std::min((int) ceil(Nb / ((double) n_thread_block)), 32 * numSMs); // at most 32 blocks per SM
        // call the kernel
#if defined (TRACE_WINDFLOW)
        stats_record->num_kernels++;
#endif        
        ComputeResults_Kernel<result_t, comb_F_t><<<noBlocks, n_thread_block, 0, *cudaStream>>>(winComb_func, gpu_tree, gpu_results, offset, noLeaves, batchSize, windowSize, b_id, Nb, slide);
        if (err = cudaGetLastError()) {
            std::cerr << RED << "WindFlow Error: invoking the GPU kernel (ComputeResults_Kernel) causes error -> " << err << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
    }

    // method to update the FlatFAT_GPU with new inputs
    void update(const std::vector<result_t> &inputs, int b_id)
    {
        // compute the remaining space at the end of the tree
        size_t spaceLeft = batchSize - offset;
        if (inputs.size() <= spaceLeft) {
#if defined (TRACE_WINDFLOW)
            stats_record->bytes_copied_hd += inputs.size() * sizeof(result_t);
#endif
            // copy the inputs in the page-locked memory
            memcpy(pinned_buffer, inputs.data(), inputs.size() * sizeof(result_t));
            // add all the input elements to the tree
            gpuErrChk(cudaMemcpyAsync(gpu_tree + offset, pinned_buffer, inputs.size() * sizeof(result_t), cudaMemcpyHostToDevice, *cudaStream));
        }
        else {
#if defined (TRACE_WINDFLOW)
            stats_record->bytes_copied_hd += spaceLeft * sizeof(result_t) + (inputs.size() - spaceLeft) * sizeof(result_t);
#endif
            // copy the inputs in the page-locked memory
            memcpy(pinned_buffer, inputs.data(), inputs.size() * sizeof(result_t));
            // split in two parts
            gpuErrChk(cudaMemcpyAsync(gpu_tree + offset, pinned_buffer, spaceLeft * sizeof(result_t), cudaMemcpyHostToDevice, *cudaStream));
            gpuErrChk(cudaMemcpyAsync(gpu_tree, pinned_buffer + spaceLeft, (inputs.size() - spaceLeft) * sizeof(result_t), cudaMemcpyHostToDevice, *cudaStream));
        }
        int pow = 1;
        result_t *d_levelA = gpu_tree;
        result_t *d_levelB = d_levelA + noLeaves / pow;
        size_t sizeB = ceil((double) batchSize / (pow << 1 ));
        size_t update_pos = Parent(offset, noLeaves);
        size_t numSeenElements = noLeaves;
        size_t distance = update_pos - numSeenElements;
        int sizeUpdate = ceil((double) inputs.size() / (pow << 1)) + 1;
        // update the levels of the tree, each with a separate kernel
        while (d_levelB < gpu_tree + treeSize) {
            // call the kernel to update a level of the tree
            size_t numBlocks = std::min((int) ceil(sizeUpdate / ((double) n_thread_block)), 32 * numSMs); // at most 32 blocks per SM
            // call the kernel
#if defined (TRACE_WINDFLOW)
            stats_record->num_kernels++;
#endif
            cudaError_t err;
            UpdateTreeLevel_Kernel<result_t, comb_F_t><<<numBlocks, n_thread_block, 0, *cudaStream>>>(winComb_func, d_levelA, d_levelB, distance, sizeB, sizeUpdate);
            if (err = cudaGetLastError()) {
                std::cerr << RED << "WindFlow Error: invoking the GPU kernel (UpdateTreeLevel_Kernel) causes error -> " << err << DEFAULT_COLOR << std::endl;
                exit(EXIT_FAILURE);
            }
            pow = pow << 1;
            d_levelA = d_levelB;
            d_levelB = d_levelA + noLeaves / pow;
            sizeB = ceil((double) batchSize / (pow << 1));
            update_pos = Parent(update_pos, noLeaves);
            numSeenElements += noLeaves / pow;
            distance = update_pos - numSeenElements;
            sizeUpdate = ceil((double) inputs.size() / (pow << 1)) + 1;
        }
        offset = (offset + inputs.size()) % batchSize;
#if defined (TRACE_WINDFLOW)
        stats_record->bytes_copied_hd += Nb * sizeof(result_t);
#endif
        // copy the initial values in the GPU
        gpuErrChk(cudaMemcpyAsync((void *) gpu_results, (void *) pinned_init_results, Nb * sizeof(result_t), cudaMemcpyHostToDevice, *cudaStream));
        // compute the results of the batch
        int noBlocks = std::min((int) ceil(Nb / ((double) n_thread_block)), 32 * numSMs); // at most 32 blocks per SM
        // call the kernel
#if defined (TRACE_WINDFLOW)
        stats_record->num_kernels++;
#endif
        cudaError_t err;
        ComputeResults_Kernel<result_t, comb_F_t><<<noBlocks, n_thread_block, 0, *cudaStream>>>(winComb_func, gpu_tree, gpu_results, offset, noLeaves, batchSize, windowSize, b_id, Nb, slide);
        if (err = cudaGetLastError()) {
            std::cerr << RED << "WindFlow Error: invoking the GPU kernel (ComputeResults_Kernel) causes error -> " << err << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
    }

    // method to get (synchronously) the array of results computed by the GPU
    const result_t *getSyncResults()
    {
#if defined (TRACE_WINDFLOW)
        stats_record->bytes_copied_dh += Nb * sizeof(result_t);
#endif
        // synchronous copy
        gpuErrChk(cudaMemcpyAsync((void *) pinned_results, (void *) gpu_results, Nb * sizeof(result_t), cudaMemcpyDeviceToHost, *cudaStream));
        gpuErrChk(cudaStreamSynchronize(*cudaStream));
        return pinned_results;
    }

    // method to start (asynchronously) the copy of the array of results from the GPU
    void getAsyncResults()
    {
#if defined (TRACE_WINDFLOW)
        stats_record->bytes_copied_dh += Nb * sizeof(result_t);
#endif
        gpuErrChk(cudaMemcpyAsync((void *) pinned_results, (void *) gpu_results, Nb * sizeof(result_t), cudaMemcpyDeviceToHost, *cudaStream));
    }

    // method to wait the completion of the results copy from the GPU to the host
    const result_t* waitResults()
    {
        gpuErrChk(cudaStreamSynchronize(*cudaStream));
        return pinned_results;
    }

    // method to get the vector of batched inputs
    std::vector<result_t> getBatchedTuples()
    {
#if defined (TRACE_WINDFLOW)
        stats_record->bytes_copied_dh += (batchSize - offset) * sizeof(result_t) + offset * sizeof(result_t);
#endif
        gpuErrChk(cudaMemcpyAsync((void *) pinned_buffer, (void *) (gpu_tree + offset), (batchSize - offset) * sizeof(result_t), cudaMemcpyDeviceToHost, *cudaStream));
        gpuErrChk(cudaMemcpyAsync((void *) (pinned_buffer + batchSize - offset), (void *) gpu_tree, offset * sizeof(result_t), cudaMemcpyDeviceToHost, *cudaStream));
        gpuErrChk(cudaStreamSynchronize(*cudaStream));
        return std::vector<result_t>(pinned_buffer, pinned_buffer + batchSize);
    }

#if defined (TRACE_WINDFLOW)
    // method to provide the Stats_Record structure for gathering statistics
    void set_StatsRecord(Stats_Record *_stats_record)
    {
        stats_record = _stats_record;
    }
#endif
};

} // namespace wf

#endif
