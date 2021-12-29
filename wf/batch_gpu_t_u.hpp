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
 *  @file    batch_gpu_t_u.hpp
 *  @author  Gabriele Mencagli
 *  
 *  @brief Class implementing a batch of data tuples accessible by the GPU side.
 *         Version with CUDA Unified Memory
 *  
 *  @section Batch_GPU_t (Description)
 *  
 *  Class implementing a batch of data tuples accessible by the GPU side. Version
 *  with CUDA Unified Memory.
 */ 

#ifndef BATCH_GPU_T_U_H
#define BATCH_GPU_T_U_H

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
    batch_item_gpu_t<tuple_t> *data_u; // array of batch items in CUDA unified memory
    size_t num_dist_keys; // number of distinct keys in the batch
    void *dist_keys; // untyped pointer to a host array containing the distinct keys in the batch
    int *start_idxs_u; // array with the starting indexes of the keys in the batch in CUDA unified memory
    int *map_idxs_u; // array to find tuples with the same key within the batch in CUDA unified memory
    cudaStream_t cudaStream; // CUDA stream associated with the batch
    cudaDeviceProp deviceProp; // object containing the properties of the used GPU device
    int isTegra; // flag equal to 1 if the GPU (device 0) is integrated (Tegra), 0 otherwise

    // Constructor
    Batch_GPU_t(size_t _size,
                size_t _delete_counter=1):
                size(_size),
                original_size(_size),
                isPunctuation(false),
                delete_counter(_delete_counter),
                num_dist_keys(0),
                dist_keys(nullptr)
    {
        watermarks.push_back(std::numeric_limits<uint64_t>::max());
        gpuErrChk(cudaStreamCreate(&cudaStream)); // create the CUDA stream associated with this batch object
        gpuErrChk(cudaGetDeviceProperties(&deviceProp, 0)); // get the properties of the GPU device with id 0
        gpuErrChk(cudaDeviceGetAttribute(&isTegra, cudaDevAttrIntegrated, 0));
        if (!isTegra && deviceProp.concurrentManagedAccess) { // new discrete GPU models
            // allocate the CUDA unified memory arrays
            gpuErrChk(cudaMallocManaged(&data_u, sizeof(batch_item_gpu_t<tuple_t>) * size));
            gpuErrChk(cudaMallocManaged(&start_idxs_u, sizeof(int) * size));
            gpuErrChk(cudaMallocManaged(&map_idxs_u, sizeof(int) * size));
        }
        else { // old discrete GPU models and Tegra devices
            // allocate the CUDA unified memory arrays and attach them to the host side
            gpuErrChk(cudaMallocManaged(&data_u, sizeof(batch_item_gpu_t<tuple_t>) * size, cudaMemAttachHost));
            gpuErrChk(cudaMallocManaged(&start_idxs_u, sizeof(int) * size, cudaMemAttachHost));
            gpuErrChk(cudaMallocManaged(&map_idxs_u, sizeof(int) * size, cudaMemAttachHost));
            // attach the CUDA unified memory arrays to the GPU side
            gpuErrChk(cudaStreamAttachMemAsync(cudaStream, data_u, sizeof(batch_item_gpu_t<tuple_t>) * original_size, cudaMemAttachSingle));
            gpuErrChk(cudaStreamAttachMemAsync(cudaStream, start_idxs_u, sizeof(int) * size, cudaMemAttachSingle));
            gpuErrChk(cudaStreamAttachMemAsync(cudaStream, map_idxs_u, sizeof(int) * size, cudaMemAttachSingle));
        }
    }

    // Destructor
    ~Batch_GPU_t() override
    {
        if (data_u != nullptr) {
            gpuErrChk(cudaFree(data_u));
        }
        if (dist_keys != nullptr) {
            free(dist_keys);
        }
        if (start_idxs_u != nullptr) {
            gpuErrChk(cudaFree(start_idxs_u));
        }
        if (map_idxs_u != nullptr) {
            gpuErrChk(cudaFree(map_idxs_u));
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

    // prefetch the internal CUDA unified memory arrays to be efficiently accessible by the host side
    void prefetch2CPU(bool _isKB=false)
    {
        if (isTegra) { // Tegra devices
            // attach the CUDA unified memory array of batch items to the host side
            gpuErrChk(cudaStreamAttachMemAsync(cudaStream, data_u, sizeof(batch_item_gpu_t<tuple_t>) * original_size, cudaMemAttachHost));
            if (_isKB) {
                // attach the CUDA unified memory arrays used for keyby distribution to the host side
                gpuErrChk(cudaStreamAttachMemAsync(cudaStream, start_idxs_u, sizeof(int) * original_size, cudaMemAttachHost));
                gpuErrChk(cudaStreamAttachMemAsync(cudaStream, map_idxs_u, sizeof(int) * original_size, cudaMemAttachHost));
            }
        }
        else if (deviceProp.concurrentManagedAccess) { // new discrete GPU models
            // prefetch the CUDA unified memory array of batch items to the host side
            gpuErrChk(cudaMemPrefetchAsync(data_u, sizeof(batch_item_gpu_t<tuple_t>) * size, cudaCpuDeviceId, cudaStream));
            if (_isKB) {
                // prefetch the CUDA unified memory arrays used for keyby distribution to the host side
                gpuErrChk(cudaMemPrefetchAsync(start_idxs_u, sizeof(int) * size, cudaCpuDeviceId, cudaStream));
                gpuErrChk(cudaMemPrefetchAsync(map_idxs_u, sizeof(int) * size, cudaCpuDeviceId, cudaStream));
            }
        }
    }

    // prefetch the internal CUDA unified memory arrays to be efficiently accessible by the GPU side
    void prefetch2GPU(bool _isKB=false)
    {
        if (isTegra) { // Tegra devices
            // attach the CUDA unified memory array of batch items to the GPU side
            gpuErrChk(cudaStreamAttachMemAsync(cudaStream, data_u, sizeof(batch_item_gpu_t<tuple_t>) * original_size, cudaMemAttachSingle));
            if (_isKB) {
                // attach the CUDA unified memory arrays used for keyby distribution to the GPU side
                gpuErrChk(cudaStreamAttachMemAsync(cudaStream, start_idxs_u, sizeof(int) * original_size, cudaMemAttachSingle));
                gpuErrChk(cudaStreamAttachMemAsync(cudaStream, map_idxs_u, sizeof(int) * original_size, cudaMemAttachSingle));
            }
        }
        else if (deviceProp.concurrentManagedAccess) { // new discrete GPU models
            // prefetch the CUDA unified memory array of batch items to the GPU side
            gpuErrChk(cudaMemPrefetchAsync(data_u, sizeof(batch_item_gpu_t<tuple_t>) * size, 0, cudaStream)); // device_id = 0
            if (_isKB) {
                // prefetch the CUDA unified memory arrays used for keyby distribution to the GPU side
                gpuErrChk(cudaMemPrefetchAsync(start_idxs_u, sizeof(int) * size, 0, cudaStream)); // device_id = 0
                gpuErrChk(cudaMemPrefetchAsync(map_idxs_u, sizeof(int) * size, 0, cudaStream)); // device_id = 0
            }
        }
    }

    // Get the tuple (by reference) at position pos of the batch
    tuple_t &getTupleAtPos(size_t _pos) override
    {
        assert(_pos < size);
        return data_u[_pos].tuple;
    }

    // Get the timestamp of the element at position pos of the batch
    uint64_t getTimestampAtPos(size_t _pos) override
    {
        assert(_pos < size);
        return data_u[_pos].timestamp;
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
        assert(watermarks.size() == 1);
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
        if (dist_keys != nullptr) {
            free(dist_keys);
            dist_keys = nullptr;
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
}

} // namespace wf

#endif
