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

namespace wf {

// class Batch_t
template<typename tuple_t>
struct Batch_t
{
    ff::MPMC_Ptr_Queue *queue = nullptr; // pointer to the recyling queue

    // Constructor
    Batch_t() {}

    // Copy Constructor
    Batch_t(const Batch_t &_other):
            queue(_other.queue) {}

    // Move Constructor
    Batch_t(Batch_t &&_other):
            queue(std::exchange(_other.queue, nullptr)) {}

    // Destructor
    virtual ~Batch_t() = default;

    // Copy Assignment Operator
    Batch_t &operator=(const Batch_t &_other)
    {
        if (this != &_other) {
            queue = _other.queue;
        }
        return *this;
    }

    // Move Assignment Operator
    Batch_t &operator=(Batch_t &&_other)
    {
        queue = std::exchange(_other.queue, nullptr);
        return *this;
    }

    // Check whether the batch can be deleted or not
    virtual bool isDeletable() = 0;

    // Check whether the batch is a punctuation or not
    virtual bool isPunct() const = 0;

    // Get the size of the batch in terms of tuples
    virtual size_t getSize() const = 0;

    // Get the tuple (by reference) at position pos of the batch
    virtual tuple_t &getTupleAtPos(size_t _pos) = 0;

    // Get the timestamp of the element at position pos of the batch
    virtual uint64_t getTimestampAtPos(size_t _pos) const = 0;

    // Get the watermark of the batch related to a specific destination _node_id
    virtual uint64_t getWatermark(size_t _node_id) const = 0;

    // Set the watermark of the batch related to a specific destination _node_id
    virtual void setWatermark(uint64_t _wm, size_t _node_id) = 0;
};

// Delete a Batch_CPU_t message (trying to recycle it)
template<typename tuple_t>
inline void deleteBatch_t(Batch_t<tuple_t> *batch_input)
{
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
}

} // namespace wf

#endif
