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
 *  @file    recycling_gpu.hpp
 *  @author  Gabriele Mencagli
 *  
 *  @brief Class implementing the recycling functions of Batch_GPU_t structures
 *  
 *  @section Recycling Functions Part II (Description)
 *  
 *  Class implementing the recycling functions of Single_t and Batch_t structures.
 */ 

#ifndef RECYCLING_GPU_H
#define RECYCLING_GPU_H

// includes
#include<basic.hpp>
#include<basic_gpu.hpp>

namespace wf {

// Ceate a new Batch_GPU_t (it blocks until the creation is complete)
template<typename tuple_t>
inline Batch_GPU_t<tuple_t> *createBatchGPU(size_t _requested_size,
                                            ff::MPMC_Ptr_Queue *_queue,
                                            std::atomic<int> *_inTransit_counter)
{
    Batch_GPU_t<tuple_t> *batch_input = nullptr;
    bool allocate = true;
    do {
        try {
            batch_input = new Batch_GPU_t<tuple_t>(_requested_size, _inTransit_counter);
            batch_input->queue = _queue;
            (*_inTransit_counter)++;
        }
        catch (wf::FullGPUMemoryException &e) {
            allocate = false;
        }
    }
    while (!allocate);
    return batch_input;
}

// Try to recycle a Batch_GPU_t (it returns nullptr otherwise)
template<typename tuple_t>
inline Batch_GPU_t<tuple_t> *recycleBatchGPU(size_t _requested_size,
                                             ff::MPMC_Ptr_Queue *_queue,
                                             std::atomic<int> *_inTransit_counter)
{
    Batch_GPU_t<tuple_t> *batch_input = nullptr;
    if (_queue->pop((void **) &batch_input)) {
        if (_requested_size <= batch_input->original_size) {
            batch_input->reset();
            batch_input->size = _requested_size;
        }
        else {
            delete batch_input;
            batch_input = createBatchGPU<tuple_t>(_requested_size, _queue, _inTransit_counter);
        }
    }
    return batch_input;
}

// Allocate a Batch_GPU_t (trying to recycle an old one if possible)
template<typename tuple_t>
inline Batch_GPU_t<tuple_t> *allocateBatch_GPU_t(size_t _requested_size,
                                                 ff::MPMC_Ptr_Queue *_queue,
                                                 std::atomic<int> *_inTransit_counter)
{
    Batch_GPU_t<tuple_t> *batch_input = nullptr;
#if !defined (WF_NO_RECYCLING)
    if (_queue != nullptr) {
        // Case 1: if too few batches are in transit, we shall allocate a new one
        if ((*_inTransit_counter) < 2) {
            batch_input = createBatchGPU<tuple_t>(_requested_size, _queue, _inTransit_counter);
            return batch_input;
        }
        // Case 2: if (we suppose) there are some in-transit batches
        else {
            // Case 2.1: we have successfully recycled a previous batch
            batch_input = recycleBatchGPU<tuple_t>(_requested_size, _queue, _inTransit_counter);
            if (batch_input != nullptr) {
                return batch_input;
            }
            // Case 2.2: a recycled batch is not immediately available
            else {
                size_t mem_free, mem_total;
                gpuErrChk(cudaMemGetInfo(&mem_free, &mem_total));
                // Case 2.2.1: memory is enough, so we create a new batch
                if (mem_free > (WF_GPU_FREE_MEMORY_LIMIT * mem_total)) {
                    batch_input = createBatchGPU<tuple_t>(_requested_size, _queue, _inTransit_counter);
                    return batch_input;
                }
                // Case 2.2.2: memory is not enough, we are forced to recycle a previous batch
                else {
                    do {
                        batch_input = recycleBatchGPU<tuple_t>(_requested_size, _queue, _inTransit_counter);
                    }
                    while((batch_input == nullptr) && ((*_inTransit_counter) > 1)); // note 1 here because we have overlapping in some emitters
                    if (batch_input != nullptr) { // we recyled a previous batch :)
                        return batch_input;
                    }
                    else { // no in-transit batch exists anymore :(
                        batch_input = createBatchGPU<tuple_t>(_requested_size, _queue, _inTransit_counter);
                        return batch_input;
                    }
                }
            }
        }
    }
    else {
        batch_input = new Batch_GPU_t<tuple_t>(_requested_size, _inTransit_counter);
        return batch_input;
    }
#else
    batch_input = new Batch_GPU_t<tuple_t>(_requested_size, _inTransit_counter);
    return batch_input;
#endif
}

} // namespace wf

#endif
