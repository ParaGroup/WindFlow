/**************************************************************************************
 *  Copyright (c) 2024- Gabriele Mencagli and Yuriy Rymarchuk
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
 *  @file    join_archive.hpp
 *  @author  Gabriele Mencagli and Yuriy Rymarchuk
 *  
 *  @brief Join archive
 *  
 *  @section JoinArchive (Description)
 *  
 *  Join archive of tuples received from the input streams (A/B) useful for the
 *  processing (used by join-based operators).
 */ 

#ifndef JOIN_ARCHIVE_H
#define JOIN_ARCHIVE_H

// includes
#include<deque>
#include<functional>
#include<basic.hpp>
#include<archive.hpp>

namespace wf {

// class JoinArchive
template<typename tuple_t, typename compare_func_t>
class JoinArchive: public Archive<tuple_t, compare_func_t>
{
private:
    using wrapper_t = wrapper_tuple_t<tuple_t>; // alias for the wrapped tuple type
    using iterator_t = typename std::deque<wrapper_t>::iterator; // iterator type
    using Archive<tuple_t, compare_func_t>::archive; // container implementing the ordered archive of wrapped tuples
    using Archive<tuple_t, compare_func_t>::lessThan; // function to compare two wrapped tuples
    static_assert(std::is_same<compare_func_t, std::function<bool(const wrapper_t &, const uint64_t &)>>::value,
                  "WindFlow Compilation Error - unknown compare function passed to the Join Archive:\n"
                  "  Candidate: bool(const wrapper_t &, const uint64_t &)\n");

public:
    // Constructor
    JoinArchive(compare_func_t lessThan):
                Archive<tuple_t, compare_func_t>(lessThan) {}

    // Add a wrapped tuple to the archive (copy semantics)
    void insert(const wrapper_t &_wt) override
    {
        auto it = std::lower_bound(archive.begin(), archive.end(), _wt.index, lessThan);
        if (it == archive.end()) { // add at the end of the archive
            archive.push_back(_wt);
        }
        else { // add the in the right position of the archive
            archive.insert(it, _wt);
        }
    }

    // Add a wrapped tuple to the archive (move semantics)
    void insert(wrapper_t &&_wt) override
    {
        auto it = std::lower_bound(archive.begin(), archive.end(), _wt.index, lessThan);
        if (it == archive.end()) { // add at the end of the archive
            archive.push_back(std::move(_wt));
        }
        else { // add the in the right position of the archive
            archive.insert(it, std::move(_wt));
        }
    }

    // Remove all the tuples with timestamp prior to the one of the wrapped tuple _wt in the ordering
    size_t purge(const wrapper_t &_wt) override
    {
        auto it = std::lower_bound(archive.begin(), archive.end(), _wt.index, lessThan);
        size_t n = std::distance(archive.begin(), it);
        archive.erase(archive.begin(), it);
        return n;
    }

    // Remove all the tuples with timestamp prior to watermark _wm in the ordering
    size_t purge(const uint64_t &_wm)
    {
        auto it = std::lower_bound(archive.begin(), archive.end(), _wm, lessThan);
        size_t n = std::distance(archive.begin(), it);
        archive.erase(archive.begin(), it);
        return n;
    }
    
    /*  
     *  Method to get a pair of iterators that represent the join range [first, last] given
     *  an input lower bound and upper bound for timestamps as unsigned integers. The method
     *  returns the iterator (first) to the wrapped tuple in the archive that has index
     *  (ts) >= lower bound, and the iterator (end) to the wrapped tuple in the archive that
     *  has index (ts) <= upper bound.
     */ 
    std::pair<iterator_t, iterator_t> getJoinRange(const uint64_t &_l_b, const uint64_t &_u_b)
    {
        assert(_l_b <= _u_b);
        std::pair<iterator_t, iterator_t> its;
        its.first = std::lower_bound(archive.begin(), archive.end(), _l_b, lessThan);
        its.second = std::lower_bound(its.first, archive.end(), _u_b, lessThan);
        return its;
    }
};

} // namespace wf

#endif
