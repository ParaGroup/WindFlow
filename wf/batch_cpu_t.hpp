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
 *  @file    batch_cpu_t.hpp
 *  @author  Gabriele Mencagli
 *  
 *  @brief Class implementing a batch containing data tuples
 *  
 *  @section Batch_CPU_t (Description)
 *  
 *  Class implementing a batch containing data tuples.
 */ 

#ifndef BATCH_CPU_T_H
#define BATCH_CPU_T_H

// includes
#include<atomic>
#include<vector>
#include<cassert>
#include<stddef.h>
#include<limits.h>
#include<batch_t.hpp>

namespace wf {

// class Batch_CPU_t
template<typename tuple_t>
struct Batch_CPU_t: Batch_t<tuple_t>
{
    struct batch_item_t // struct implementing a batch item
    {
        tuple_t tuple;
        uint64_t timestamp;

        // Constructor I (copy semantics of the tuple)
        batch_item_t(const tuple_t &_tuple,
                     uint64_t _timestamp):
                     tuple(_tuple),
                     timestamp(_timestamp) {}

        // Constructor II (move semantics of the tuple)
        batch_item_t(tuple_t &&_tuple,
                     uint64_t _timestamp):
                     tuple(std::move(_tuple)),
                     timestamp(_timestamp) {}
    };
    std::vector<batch_item_t> batch_data; // vector of batch items
    std::vector<uint64_t> watermarks; // vector of watermarks of the batch (one per destination receiving the batch)
    std::atomic<size_t> delete_counter; // atomic counter to delete correctly the batch
    size_t size; // number of meaningful items within the batch
    bool isPunctuation; // flag true if the message is a punctuation, false otherwise

    // Constructor
    Batch_CPU_t(size_t _reserved_size,
                size_t _delete_counter=1):
                delete_counter(_delete_counter),
                size(0),
                isPunctuation(false)
    {
        batch_data.reserve(_reserved_size);
        watermarks.push_back(std::numeric_limits<uint64_t>::max());
    }

    // Copy Constructor
    Batch_CPU_t(const Batch_CPU_t &_other): // do not copy delete_counter
                Batch_t<tuple_t>(_other),
                batch_data(_other.batch_data),
                watermarks(_other.watermarks),
                delete_counter(1),
                size(_other.size),
                isPunctuation(_other.isPunctuation) {}

    // Move Constructor
    Batch_CPU_t(Batch_CPU_t &&_other): // do not move delete_counter
                Batch_t<tuple_t>(std::move(_other)),
                batch_data(std::move(_other.batch_data)),
                watermarks(std::move(watermarks)),
                delete_counter(1),
                size(std::exchange(_other.size, 0)),
                isPunctuation(_other.isPunctuation) {}

    // Destructor
    ~Batch_CPU_t() override = default;

    // Copy Assignment Operator
    Batch_CPU_t &operator=(const Batch_CPU_t &_other) // do not copy delete_counter
    {
        if (this != &_other) {
            Batch_t<tuple_t>::operator=(_other);
            batch_data = _other.batch_data;
            watermarks = _other.watermarks;
            delete_counter = 1;
            size = _other.size;
            isPunctuation = _other.isPunctuation;
        }
        return *this;
    }

    // Move Assignment Operator
    Batch_CPU_t &operator=(Batch_CPU_t &&_other) // do not move delete_counter
    {
        Batch_t<tuple_t>::operator=(std::move(_other));
        batch_data = std::move(_other.batch_data);
        watermarks = std::move(_other.watermarks);
        delete_counter = 1;
        size = std::exchange(_other.size, 0);
        isPunctuation = _other.isPunctuation;
        return *this;
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

    // Get the tuple (by reference) at position pos of the batch
    tuple_t &getTupleAtPos(size_t _pos) override
    {
        assert(_pos < size && _pos < batch_data.size());
        return batch_data[_pos].tuple;
    }

    // Get the timestamp of the element at position pos of the batch
    uint64_t getTimestampAtPos(size_t _pos) override
    {
        assert(_pos < size && _pos < batch_data.size());
        return batch_data[_pos].timestamp;
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

    // Append the tuple at the end of the batch (copy semantics of the tuple)
    void addTuple(const tuple_t &_tuple,
                  uint64_t _timestamp,
                  uint64_t _watermark)
    {
        if (size < batch_data.size()) {
            batch_data[size].tuple = _tuple;
            batch_data[size].timestamp = _timestamp;
        }
        else {
            batch_data.emplace_back(_tuple, _timestamp);
        }
        size++;
        assert(watermarks.size() == 1);
        if (watermarks[0] > _watermark) {
            watermarks[0] = _watermark;
        }
    }

    // Append the tuple at the end of the batch (move semantics of the tuple)
    void addTuple(tuple_t &&_tuple,
                  uint64_t _timestamp,
                  uint64_t _watermark)
    {
        if (size < batch_data.size()) {
            batch_data[size].tuple = std::move(_tuple);
            batch_data[size].timestamp = _timestamp;
        }
        else {
            batch_data.emplace_back(std::move(_tuple), _timestamp);
        }
        size++;
        assert(watermarks.size() == 1);
        if (watermarks[0] > _watermark) {
            watermarks[0] = _watermark;
        }
    }

    // Reset the batch content
    void reset(size_t _delete_counter=1)
    {
        size = 0;
        watermarks.resize(1);
        watermarks[0] = std::numeric_limits<uint64_t>::max();
        delete_counter = _delete_counter;
        isPunctuation = false;
    }
};

// Allocate a Batch_CPU_t (trying to recycle an old one)
template<typename tuple_t>
inline Batch_CPU_t<tuple_t> *allocateBatch_CPU_t(size_t _reserved_size,
                                                 ff::MPMC_Ptr_Queue *_queue)
{
    Batch_CPU_t<tuple_t> *batch_input = nullptr;
#if !defined (WF_NO_RECYCLING)
    if (_queue != nullptr) {
        if (!_queue->pop((void **) &batch_input)) { // create a new batch
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
