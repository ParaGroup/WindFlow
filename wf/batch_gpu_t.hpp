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
 *  @file    batch_gpu_t.hpp
 *  @author  Gabriele Mencagli
 *  
 *  @brief Class implementing a batch of data tuples accessible by the GPU side
 *  
 *  @section Batch_GPU_t (Description)
 *  
 *  Class implementing a batch of data tuples accessible by the GPU side.
 */ 

#ifndef BATCH_GPU_T_H
#define BATCH_GPU_T_H

// includes
#include<atomic>
#include<vector>
#include<stddef.h>
#include<limits.h>
#include<batch_t.hpp>
#include<basic_gpu.hpp>

namespace wf {

// class Batch_GPU_t
template<typename tuple_t>
struct Batch_GPU_t: Batch_t<tuple_t>
{
    std::vector<uint64_t> watermarks; // vector of watermarks of the batch (one per destination receiving the batch)
    size_t size; // actual number of meaningful items within the batch
    size_t original_size; // original size of the batch (and of its internal arrays)
    bool isPunctuation; // flag true if the message is a punctuation, false otherwise
    std::atomic<size_t> delete_counter; // atomic counter to delete correctly the batch
    batch_item_gpu_t<tuple_t> *pinned_data_cpu; // host pinned array of batch items
    batch_item_gpu_t<tuple_t> *data_gpu; // GPU array of batch items
    size_t num_dist_keys; // number of distinct keys in the batch
    void *dist_keys_cpu; // untyped pointer to a host array containing the distinct keys in the batch
    int *start_idxs_gpu; // GPU array of starting indexes of keys in the batch
    int *map_idxs_gpu; // GPU array to find tuples with the same key within the batch
    cudaStream_t cudaStream; // CUDA stream associated with the batch

    // Constructor
    Batch_GPU_t(size_t _size,
                size_t _delete_counter=1):
                size(_size),
                original_size(_size),
                isPunctuation(false),
                delete_counter(_delete_counter),
                pinned_data_cpu(nullptr),
                num_dist_keys(0),
                dist_keys_cpu(nullptr)
    {
        watermarks.push_back(std::numeric_limits<uint64_t>::max());
        gpuErrChk(cudaStreamCreate(&cudaStream)); // create the CUDA stream associated with this batch object
        gpuErrChk(cudaMalloc(&data_gpu, sizeof(batch_item_gpu_t<tuple_t>) * size));
        gpuErrChk(cudaMalloc(&start_idxs_gpu, sizeof(int) * size));
        gpuErrChk(cudaMalloc(&map_idxs_gpu, sizeof(int) * size));
    }

    // Destructor
    ~Batch_GPU_t() override
    {
        if (pinned_data_cpu != nullptr) {
            gpuErrChk(cudaFreeHost(pinned_data_cpu));
        }
        if (data_gpu != nullptr) {
            gpuErrChk(cudaFree(data_gpu));
        }
        if (dist_keys_cpu != nullptr) {
            free(dist_keys_cpu);
        }
        if (start_idxs_gpu != nullptr) {
            gpuErrChk(cudaFree(start_idxs_gpu));
        }
        if (map_idxs_gpu != nullptr) {
            gpuErrChk(cudaFree(map_idxs_gpu));
        }
        gpuErrChk(cudaStreamDestroy(cudaStream));
    }

    // Check whether the batch can be deleted or not
    bool isDeletable() override
    {
        size_t old_cnt = delete_counter.fetch_sub(1);
        if (old_cnt == 1) {
            return true;
        }
        else {
            return false;
        }
    }

    // Check whether the batch is a punctuation or not
    bool isPunct() const override
    {
        return isPunctuation;
    }

    // Get the size of the batch in terms of tuples
    size_t getSize() const override
    {
        return size;
    }

    // Trasfering of the batch items to a host pinned memory array
    void transfer2CPU()
    {
        if (pinned_data_cpu == nullptr) { // create the host pinned array if it does not exist yet
            gpuErrChk(cudaMallocHost(&pinned_data_cpu, sizeof(batch_item_gpu_t<tuple_t>) * original_size)); // <-- using the original size is safer!
        }
        gpuErrChk(cudaMemcpyAsync(pinned_data_cpu,
                                  data_gpu,
                                  sizeof(batch_item_gpu_t<tuple_t>) * size,
                                  cudaMemcpyDeviceToHost,
                                  cudaStream));
        gpuErrChk(cudaStreamSynchronize(cudaStream));
    }

    // Get the tuple (by reference) at position pos of the batch
    tuple_t &getTupleAtPos(size_t _pos) override
    {
        assert(_pos < size && pinned_data_cpu != nullptr);
        return pinned_data_cpu[_pos].tuple;
    }

    // Get the timestamp of the element at position pos of the batch
    uint64_t getTimestampAtPos(size_t _pos) override
    {
        assert(_pos < size && pinned_data_cpu != nullptr);
        return pinned_data_cpu[_pos].timestamp;
    }

    // Get the watermark of the batch related to a specific destination _node_id
    uint64_t getWatermark(size_t _node_id=0) override
    {
        if(_node_id < watermarks.size()) {
            return watermarks[_node_id];
        }
        else {
            return watermarks[0];
        }
    }

    // Set the watermark of the batch related to a specific destination _node_id
    void setWatermark(uint64_t _wm,
                      size_t _node_id=0) override
    {
        if(_node_id < watermarks.size()) {
            watermarks[_node_id] = _wm;
        }
        else {
            watermarks[0] = _wm;
        }
    }

    // Update the watermark of the batch (if necessary)
    void updateWatermark(uint64_t _watermark)
    {
        assert(watermarks.size() == 1); // sanity check
        if (watermarks[0] > _watermark) {
            watermarks[0] = _watermark;
        }
    }

    // Reset the batch content
    void reset(size_t _delete_counter=1)
    {
        watermarks.resize(1);
        watermarks[0] = std::numeric_limits<uint64_t>::max();
        size = original_size; // restore the original size of the batch
        isPunctuation = false;
        delete_counter = _delete_counter;
        num_dist_keys = 0;
        if (dist_keys_cpu != nullptr) {
            free(dist_keys_cpu);
            dist_keys_cpu = nullptr;
        }
    }

    Batch_GPU_t(const Batch_GPU_t &) = delete; ///< Copy constructor is deleted
    Batch_GPU_t(Batch_GPU_t &&) = delete; ///< Move constructor is deleted
    Batch_GPU_t &operator=(const Batch_GPU_t &) = delete; ///< Copy assignment operator is deleted
    Batch_GPU_t &operator=(Batch_GPU_t &&) = delete; ///< Move assignment operator is deleted
};

// Allocate a Batch_GPU_t (trying to recycle an old one)
template<typename tuple_t>
inline Batch_GPU_t<tuple_t> *allocateBatch_GPU_t(size_t _requested_size,
                                                 ff::MPMC_Ptr_Queue *_queue)
{
    Batch_GPU_t<tuple_t> *batch_input = nullptr;
#if !defined (WF_NO_RECYCLING)
    if (_queue != nullptr) {
        if (!_queue->pop((void **) &batch_input)) { // create a new batch
            batch_input = new Batch_GPU_t<tuple_t>(_requested_size);
            batch_input->queue = _queue;
            return batch_input;
        }
        else { // recycling a previous batch
            if (_requested_size <= batch_input->original_size) {
                batch_input->reset();
                batch_input->size = _requested_size;
                return batch_input;
            }
            else {
                delete batch_input;
                batch_input = new Batch_GPU_t<tuple_t>(_requested_size);
                batch_input->queue = _queue;
                return batch_input;
            }
        }
    }
    else { // create a new batch
        batch_input = new Batch_GPU_t<tuple_t>(_requested_size);
        return batch_input;
    }
#else
    batch_input = new Batch_GPU_t<tuple_t>(_requested_size);
    return batch_input;
#endif
}

} // namespace wf

#endif
