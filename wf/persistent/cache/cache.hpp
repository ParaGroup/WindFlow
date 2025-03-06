/**************************************************************************************
 *  Copyright (c) 2019- Gabriele Mencagli and Andrea Filippi
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
 *  @file    cache.hpp
 *  @author  Gabriele Mencagli and Andrea Filippi
 *  
 *  @brief class defining the general interface of a cache used by the 
 *         P_Keyed_Windows with non-incremental processing
 *  
 *  @section Cache (Description)
 *  
 *  Abstract class of a cache used by the P_Keyed_Windows operator with non-incremental
 *  processing functions.
 */ 

#ifndef CACHE_H
#define CACHE_H

#include<optional>

namespace wf {

// class Cache
template<typename key_t, typename value_t>
class Cache
{
public:
    // Destructor
    virtual ~Cache() = default;

    // Insert a new value associated with key in the cache
    virtual void put(const key_t &_key, const value_t &_value) = 0;

    // Read the value associated with a key (returns std::nullopt otherwise)
    virtual std::optional<value_t> get(const key_t &_key) = 0;

    // Remove a key entry from the cache
    virtual void remove(const key_t &_key) = 0;

    // Empty the cache
    virtual void clear() = 0;

    // Check if a key is present in the cache
    virtual bool exists(const key_t &_key) const = 0;

    // Get the number of elements in the cache
    virtual size_t size() const = 0;

    // Get the maximum capacity of the cache
    virtual size_t capacity() const = 0;
};

} // namespace wf

#endif
