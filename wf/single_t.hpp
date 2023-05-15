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
 *  @file    single_t.hpp
 *  @author  Gabriele Mencagli
 *  
 *  @brief Class of the generic message containing one data item
 *  
 *  @section Single_t (Description)
 *  
 *  Class implementing the generic message containing one single data item.
 */ 

#ifndef SINGLE_T_H
#define SINGLE_T_H

// includes
#include<atomic>
#include<vector>
#include<cassert>
#include<stddef.h>
#include<ff/mpmc/MPMCqueues.hpp>
#include<recycling.hpp>

namespace wf {

// class Single_t
template<typename tuple_t>
struct Single_t
{
    tuple_t tuple; // data item
    std::vector<uint64_t> fields; // fields of the data item (position 0-identifier, 1-timestamp, 2-watermarks)
    std::atomic<size_t> delete_counter; // atomic counter to delete correctly the Single_t
    ff::MPMC_Ptr_Queue *queue = nullptr; // pointer to the recycling queue
    bool isPunctuation = false; // flag true if the message is a punctuation, false otherwise

    // Constructor I (copy semantics of the tuple)
    Single_t(const tuple_t &_tuple,
             uint64_t _identifier,
             uint64_t _timestamp,
             uint64_t _watermark,
             size_t _delete_counter=1):
             tuple(_tuple),
             fields(3, 0),
             delete_counter(_delete_counter)
    {
        fields[0] = _identifier;
        fields[1] = _timestamp;
        fields[2] = _watermark;
    }

    // Constructor II (move semantics of the tuple)
    Single_t(tuple_t &&_tuple,
             uint64_t _identifier,
             uint64_t _timestamp,
             uint64_t _watermark,
             size_t _delete_counter=1):
             tuple(std::move(_tuple)),
             fields(3, 0),
             delete_counter(_delete_counter)
    {
        fields[0] = _identifier;
        fields[1] = _timestamp;
        fields[2] = _watermark;
    }

    // Copy Constructor
    Single_t(const Single_t &_other): // do not copy delete_counter
             tuple(_other.tuple),
             fields(_other.fields),
             delete_counter(1),
             queue(_other.queue),
             isPunctuation(_other.isPunctuation) {}

    // Check whether the Single_t can be deleted or not
    bool isDeletable()
    {
        size_t old_cnt = delete_counter.fetch_sub(1);
        if (old_cnt == 1) {
            return true;
        }
        else {
            return false;
        }
    }

    // Check whether the Single_t is a punctuation or not
    bool isPunct() const
    {
        return isPunctuation;
    }

    // Reset the Single_t content (copy semantics of the tuple)
    void reset(const tuple_t &_tuple,
               uint64_t _identifier,
               uint64_t _timestamp,
               uint64_t _watermark,
               size_t _delete_counter=1)
    {
        tuple = _tuple;
        fields.resize(3);
        fields[0] = _identifier;
        fields[1] = _timestamp;
        fields[2] = _watermark;
        delete_counter = _delete_counter;
        isPunctuation = false;
    }

    // Reset the Single_t content (move semantics of the tuple)
    void reset(tuple_t &&_tuple,
               uint64_t _identifier,
               uint64_t _timestamp,
               uint64_t _watermark,
               size_t _delete_counter=1)
    {
        tuple = std::move(_tuple);
        fields.resize(3);
        fields[0] = _identifier;
        fields[1] = _timestamp;
        fields[2] = _watermark;
        delete_counter = _delete_counter;
        isPunctuation = false;
    }

    // Get the identifier
    uint64_t getIdentifier() const
    {
        return fields[0];
    }

    // Get the timestamp
    uint64_t getTimestamp() const
    {
        return fields[1];
    }

    // Get the watermark related to a specific destination _node_id
    uint64_t getWatermark(size_t _node_id=0) const
    {
        if (_node_id + 2 < fields.size()) {
            return fields[_node_id + 2];
        }
        else {
            return fields[2];
        }
    }

    // Set the watermark related to a specific destination _node_id
    void setWatermark(uint64_t _wm, size_t _node_id)
    {
        if (_node_id + 2 < fields.size()) {
            fields[_node_id + 2] = _wm;
        }
        else {
            fields[2] = _wm;
        }
    }

    Single_t(Single_t &&) = delete; ///< Move constructor is deleted
    Single_t &operator=(const Single_t &) = delete; ///< Copy assignment operator is deleted
    Single_t &operator=(Single_t &&) = delete; ///< Move assignment operator is deleted
};

} // namespace wf

#endif
