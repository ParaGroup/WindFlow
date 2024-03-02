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
 *  @file    win_archive.hpp
 *  @author  Gabriele Mencagli
 *  
 *  @brief Window archive
 *  
 *  @section WinArchive (Description)
 *  
 *  Window archive of tuples received from the input streams and still useful for the
 *  processing (used by window-based operators with non-incremental queries).
 */ 

#ifndef WIN_ARCHIVE_H
#define WIN_ARCHIVE_H

// includes
#include<deque>
#include<functional>
#include<basic.hpp>
#include<archive.hpp>

namespace wf {

// class WinArchive
template<typename tuple_t, typename compare_func_t>
class WinArchive: public Archive<tuple_t, compare_func_t>
{
private:
    using wrapper_t = wrapper_tuple_t<tuple_t>; // alias for the wrapped tuple type
    using iterator_t = typename std::deque<wrapper_t>::iterator; // iterator type
    using Archive<tuple_t, compare_func_t>::archive; // container implementing the ordered archive of wrapped tuples
    using Archive<tuple_t, compare_func_t>::lessThan; // function to compare two wrapped tuples

public:

    // Constructor
    WinArchive(compare_func_t lessThan) : Archive<tuple_t, compare_func_t>(lessThan) {}

    // Add a wrapped tuple to the archive (copy semantics)
    void insert(const wrapper_t &_wt)
    {
        auto it = std::lower_bound(archive.begin(), archive.end(), _wt, lessThan);
        if (it == archive.end()) { // add at the end of the archive
            archive.push_back(_wt);
        }
        else { // add the in the right position of the archive
            archive.insert(it, _wt);
        }
    }

    // Add a wrapped tuple to the archive (move semantics)
    void insert(wrapper_t &&_wt)
    {
        auto it = std::lower_bound(archive.begin(), archive.end(), _wt, lessThan);
        if (it == archive.end()) { // add at the end of the archive
            archive.push_back(std::move(_wt));
        }
        else { // add the in the right position of the archive
            archive.insert(it, std::move(_wt));
        }
    }

    // Remove all the tuples prior to _wt in the ordering
    size_t purge(const wrapper_t &_wt)
    {
        auto it = std::lower_bound(archive.begin(), archive.end(), _wt, lessThan);
        size_t n = std::distance(archive.begin(), it);
        archive.erase(archive.begin(), it);
        return n;
    }

    /*  
     *  Method to get a pair of iterators that represent the window range [first, last) given two wrapped
     *  tuples _w1 and _w2, where _w1 must compare less than _w2. The method returns the iterator (first) to
     *  the smallest wrapped tuple in the archive that compares greater or equal than _w1, and the iterator
     *  (last) to the smallest wrapped tuple in the archive that compares greater or equal than _w2.
     */ 
    std::pair<iterator_t, iterator_t> getWinRange(const wrapper_t &_w1, const wrapper_t &_w2)
    {
        assert(lessThan(_w1, _w2));
        std::pair<iterator_t, iterator_t> its;
        its.first = std::lower_bound(archive.begin(), archive.end(), _w1, lessThan);
        its.second = std::lower_bound(archive.begin(), archive.end(), _w2, lessThan);
        return its;
    }

    /*  
     *  Method to get a pair of iterators that represent the window range [first, end) given
     *  an input wrapped tuple _wt. The method returns the iterator (first) to the smallest
     *  wrapped tuple in the archive that compares greater or equal than _wt, and the iterator
     *  (end) to the end of the archive.
     */ 
    std::pair<iterator_t, iterator_t> getWinRange(const wrapper_t &_wt)
    {
        std::pair<iterator_t, iterator_t> its;
        its.first = std::lower_bound(archive.begin(), archive.end(), _wt, lessThan);
        its.second = archive.end();
        return its;
    }

    /*  
     *  Method which, given a pair of two wrapped tuples _w1 and _w2 contained in the archive, returns
     *  the distance from _w1 to _w2.
     */ 
    size_t getDistance(const wrapper_t &_w1, const wrapper_t &_w2)
    {
        assert(lessThan(_w1, _w2));
        std::pair<iterator_t, iterator_t> its;
        its.first = std::lower_bound(archive.begin(), archive.end(), _w1, lessThan);
        its.second = std::lower_bound(archive.begin(), archive.end(), _w2, lessThan);
        return std::distance(its.first, its.second);
    }

    /*  
     *  Method which, given a wrapped tuple _wt contained in the archive, returns
     *  the distance from _wt to the end of the archive.
     */ 
    size_t getDistance(const wrapper_t &_wt)
    {
        std::pair<iterator_t, iterator_t> its;
        its.first = std::lower_bound(archive.begin(), archive.end(), _wt, lessThan);
        return std::distance(its.first, archive.end());
    }

    /*  
     *  Method used to get an iterator to a given wrapped tuple in the archive. It return an itertor
     *  to the end of the archive if the wrapped tuple is not present.
     */ 
    iterator_t getIterator(const wrapper_t &_wt)
    {
        return std::lower_bound(archive.begin(), archive.end(), _wt, lessThan);
    }
};

} // namespace wf

#endif
