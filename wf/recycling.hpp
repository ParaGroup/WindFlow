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
 *  @file    recycling.hpp
 *  @author  Gabriele Mencagli
 *  
 *  @brief Class implementing the recycling functions of Single_t and Batch_t structures
 *  
 *  @section Recycling Functions Part I (Description)
 *  
 *  Class implementing the recycling functions of Single_t and Batch_t structures.
 */ 

#ifndef RECYCLING_H
#define RECYCLING_H

// includes
#include<basic.hpp>

namespace wf {

// Delete a Single_t message (pushing it to the recycling queue if possible)
template<typename tuple_t>
inline void deleteSingle_t(Single_t<tuple_t> *input)
{
#if !defined (WF_NO_RECYCLING)
    if (input->isDeletable()) {
        if (input->queue != nullptr) {
            if (!(input->queue)->push((void * const) input)) {
                delete input;
            }
        }
        else {
            delete input;
        }
    }
#else
    if (input->isDeletable()) {
        delete input;
    }
#endif
}

// Delete a Batch_t message (pushing it to the recycling queue if possible)
template<typename tuple_t>
inline void deleteBatch_t(Batch_t<tuple_t> *batch_input)
{
#if !defined (WF_NO_RECYCLING)
    if (batch_input->isDeletable()) {
        if (batch_input->queue != nullptr) {
            if (!(batch_input->queue)->push((void * const) batch_input)) {
                delete batch_input;
            }
        }
        else {
            delete batch_input;
        }
    }
#else
    if (batch_input->isDeletable()) {
        delete batch_input;
    }
#endif
}

// Allocate a Single_t message (trying to recycle an old one if possible)
template<typename tuple_t>
inline Single_t<tuple_t> *allocateSingle_t(tuple_t &&_tuple, // universal reference!
                                           uint64_t _identifier,
                                           uint64_t _timestamp,
                                           uint64_t _watermark,
                                           ff::MPMC_Ptr_Queue *_queue)
{
    Single_t<tuple_t> *input = nullptr;
#if !defined (WF_NO_RECYCLING)
    if (_queue != nullptr) {
        if (!_queue->pop((void **) &input)) {
            input = new Single_t<tuple_t>(std::forward<tuple_t>(_tuple), _identifier, _timestamp, _watermark);
            input->queue = _queue;
            return input;
        }
        else {
            input->reset(std::forward<tuple_t>(_tuple), _identifier, _timestamp, _watermark);
            return input;
        }
    }
    else {
        input = new Single_t<tuple_t>(std::forward<tuple_t>(_tuple), _identifier, _timestamp, _watermark);
        return input;
    }
#else
    input = new Single_t<tuple_t>(std::forward<tuple_t>(_tuple), _identifier, _timestamp, _watermark);
    return input;
#endif
}

// Allocate a Batch_CPU_t (trying to recycle an old one if possible)
template<typename tuple_t>
inline Batch_CPU_t<tuple_t> *allocateBatch_CPU_t(size_t _reserved_size,
                                                 ff::MPMC_Ptr_Queue *_queue)
{
    Batch_CPU_t<tuple_t> *batch_input = nullptr;
#if !defined (WF_NO_RECYCLING)
    if (_queue != nullptr) {
        if (!_queue->pop((void **) &batch_input)) {
            batch_input = new Batch_CPU_t<tuple_t>(_reserved_size);
            batch_input->queue = _queue;
            return batch_input;
        }
        else { // recycling a previous batch
            batch_input->reset();
            return batch_input;
        }
    }
    else { // create a new batch
        batch_input = new Batch_CPU_t<tuple_t>(_reserved_size);
        return batch_input;
    }
#else
    batch_input = new Batch_CPU_t<tuple_t>(_reserved_size);
    return batch_input;
#endif
}

} // namespace wf

#endif
