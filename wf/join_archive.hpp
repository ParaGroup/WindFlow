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

// class Triggerer_Join_TB
class Triggerer_Join_TB
{
private:
    uint64_t win_len;
    uint64_t slide_len;
    long lwid;

public:
    // Constructor
    Triggerer_Join_TB(uint64_t _win_len,
                      uint64_t _slide_len,
                      uint64_t _lwid):
                      win_len(_win_len),
                      slide_len(_slide_len),
                      lwid(_lwid) {}

    // Operator to check the window event for a given timestamp
    win_event_t operator()(uint64_t _ts) const
    {
        uint64_t window_start = lwid < 0 ? 0 : lwid * slide_len;
        uint64_t window_end = window_start + win_len;
        // check if tuple is within the overall window
        if (_ts < window_start) {
            return win_event_t::OLD;
        }
        else if (_ts >= window_end) {
            return win_event_t::FIRED;
        }
        return win_event_t::IN;
    }
};

// class JoinWindow
template<typename key_t>
class JoinWindow
{
private:
    using triggerer_t = std::function<win_event_t(uint64_t)>; // triggerer type of the window
    key_t key; // key attribute of the window
    long wid; // identifier of the window (starting from zero)
    triggerer_t triggerer; // triggerer used by the window
    Win_Type_t winType; // type of the window (CB or TB)
    uint64_t num_tuples; // number of tuples raising a IN event
    uint64_t win_end_ts; // timestamp of the window result (upper bound of the window)
    uint64_t win_start_ts; // timestamp of start of the window (lower bound of the window, only for TB windows)
    size_t replica_id; // actual replica ID (could be 1, 4, 7, ...)

public:
    // Constructor 
    JoinWindow(key_t _key,
               long _wid,
               uint64_t _win_len,
               uint64_t _slide_len,
               Win_Type_t _winType,
               size_t _replica_id,
               triggerer_t _triggerer):
               key(_key),
               wid(_wid),
               triggerer(_triggerer),
               winType(_winType),
               num_tuples(0),
               replica_id(_replica_id)
    {
        if (winType == Win_Type_t::CB) { // intialize the timestamp of the window result
            win_start_ts = 0;
            win_end_ts = 0;
        }
        else {
            win_start_ts = wid < 0 ? 0 : wid * _slide_len; // TB windows have a start timestamp
            win_end_ts = win_start_ts + _win_len - 1; // set the result timestamp
        }
    }

    // Method to process a new tuple arriving to the window
    win_event_t onJoinTuple(uint64_t _ts, Join_Stream_t _tag)
    {
        assert(_tag != Join_Stream_t::NONE); // sanity check
        assert(winType == Win_Type_t::TB);
        // time-based windows (timestamps can be received disordered)
        win_event_t event = triggerer(_ts); // evaluate the triggerer
        if (event == win_event_t::IN) {
            num_tuples++;
        }
        return event;
    }

    // Get the window bounds (start timestamp, end timestamp)
    std::pair<uint64_t, uint64_t> getWinBounds() const
    {
        return {win_start_ts, win_end_ts};
    }

    // Get the key attribute of the window
    key_t getKEY() const
    {
        return key;
    }

    // Get the window identifier
    long getWID() const
    {
        return wid;
    }

    // Get the number of tuples that raised a IN event
    size_t getSize() const
    {
        return num_tuples;
    }

    // Get the timestamp of the window result
    uint64_t getResultTimestamp() const
    {
        return win_end_ts;
    }

    uint64_t getStartTimestamp() const
    {
        return win_start_ts;
    }
};

} // namespace wf

#endif
