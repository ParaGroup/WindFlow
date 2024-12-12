/**************************************************************************************
 *  Copyright (c) 2023- Gabriele Mencagli and Yuriy Rymarchuk
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
 *  @file    archive.hpp
 *  @author  Gabriele Mencagli and Yuriy Rymarchuk
 *  
 *  @brief Archive
 *  
 *  @section Archive (Description)
 *  
 *  Abstract class implementing the archive used by window-based and join-based operators.
 */ 

#ifndef ARCHIVE_H
#define ARCHIVE_H

// includes
#include<deque>
#include<functional>
#include<basic.hpp>

namespace wf {

// class Archive
template<typename tuple_t, typename compare_func_t>
class Archive
{
protected:
    using wrapper_t = wrapper_tuple_t<tuple_t>; // alias for the wrapped tuple type
    using iterator_t = typename std::deque<wrapper_t>::iterator; // iterator type
    compare_func_t lessThan; // function to compare two wrapped tuples
    std::deque<wrapper_t> archive; // container implementing the ordered archive of wrapped tuples

    // Constructor
    Archive(compare_func_t _lessThan):
            lessThan(_lessThan) {}

public:
    // Destructor
    virtual ~Archive() = default;

    // Add a wrapped tuple to the archive (copy semantics)
    virtual void insert(const wrapper_t &_wt) = 0;

    // Add a wrapped tuple to the archive (move semantics)
    virtual void insert(wrapper_t &&_wt) = 0;

    // Remove all the tuples prior to _wt in the archive
    virtual size_t purge(const wrapper_t &_wt) = 0;

    // Get the size of the archive
    size_t size() const
    {
        return archive.size();
    }

    // Get the iterator to the first wrapped tuple in the archive
    iterator_t begin()
    {
        return archive.begin();
    }

    // Get the iterator to the end of the archive
    iterator_t end()
    {
        return archive.end();
    }
};

} // namespace wf

#endif
