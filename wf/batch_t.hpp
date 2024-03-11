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
 *  @file    batch_t.hpp
 *  @author  Gabriele Mencagli
 *  
 *  @brief Abstract base class of a batch containing data tuples
 *  
 *  @section Batch_t (Description)
 *  
 *  Abstract base class of a batch containing data tuples.
 */ 

#ifndef BATCH_T_H
#define BATCH_T_H

// includes
#include<cstdint>
#include<stddef.h>
#include<ff/mpmc/MPMCqueues.hpp>
#include<recycling.hpp>

namespace wf {

// class Batch_t
template<typename tuple_t>
struct Batch_t
{
public:
    ff::MPMC_Ptr_Queue *queue; // pointer to the recyling queue

protected:
    // Constructor
    Batch_t(): queue(nullptr) {}

    // Copy Constructor
    Batch_t(const Batch_t &_other):
            queue(_other.queue) {}

public:
    // Destructor
    virtual ~Batch_t() = default;

    // Check whether the batch can be deleted or not
    virtual bool isDeletable() = 0;

    // Check whether the batch is a punctuation or not
    virtual bool isPunct() const = 0;

    // Get the size of the batch in terms of tuples
    virtual size_t getSize() const = 0;

    // Get the tuple (by reference) at position pos of the batch
    virtual tuple_t &getTupleAtPos(size_t _pos) = 0;

    // Get the timestamp of the element at position pos of the batch
    virtual uint64_t getTimestampAtPos(size_t _pos) = 0;

    // Get the watermark of the batch related to a specific destination _node_id
    virtual uint64_t getWatermark(size_t _node_id) = 0;

    // Set the watermark of the batch related to a specific destination _node_id
    virtual void setWatermark(uint64_t _wm, size_t _node_id) = 0;

    Batch_t(Batch_t &&) = delete; ///< Move constructor is deleted
    Batch_t &operator=(const Batch_t &) = delete; ///< Copy assignment operator is deleted
    Batch_t &operator=(Batch_t &&) = delete; ///< Move assignment operator is deleted
};

} // namespace wf

#endif
