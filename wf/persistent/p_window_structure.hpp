/**************************************************************************************
 *  Copyright (c) 2024- Gabriele Mencagli and Simone Frassinelli
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
 *  @file    p_window_structure.hpp
 *  @author  Gabriele Mencagli and Simone Frassinelli
 *  
 *  @brief Persistent streaming windows
 *  
 *  @section P_Window (Description)
 *  
 *  This file implements the classes used by the WindFlow library to support
 *  streaming windows with persistent operators.
 */ 

#ifndef P_WINDOW_H
#define P_WINDOW_H

// includes
#include<optional>
#include<functional>
#include<basic.hpp>

namespace wf {

// class P_Window
template<typename tuple_t, typename result_t, typename key_t>
class P_Window
{
private:
    using wrapper_t = wrapper_tuple_t<tuple_t>; // alias for the wrapped tuple type
    using triggerer_t = std::function<win_event_t(uint64_t)>; // triggerer type of the window
    key_t key; // key attribute of the window
    uint64_t lwid; // local identifier of the window (starting from zero)
    uint64_t gwid; // global identifier of the window (starting from zero)
    triggerer_t triggerer; // triggerer used by the window
    Win_Type_t winType; // type of the window (CB or TB)
    uint64_t num_tuples; // number of tuples raising a IN event
    std::optional<wrapper_t> firstTuple; // optional containing the first wrapped tuple
    std::optional<wrapper_t> lastTuple; // optional containing the last wrapped tuple
    uint64_t result_timestamp; // timestamp of the window result

public:
    // Constructor
    P_Window(key_t _key,
             uint64_t _lwid,
             uint64_t _gwid,
             triggerer_t _triggerer,
             Win_Type_t _winType,
             uint64_t _win_len,
             uint64_t _slide_len):
             key(_key),
             lwid(_lwid),
             gwid(_gwid),
             triggerer(_triggerer),
             winType(_winType),
             num_tuples(0)
    {
        if (winType == Win_Type_t::CB) {
            result_timestamp = 0;
        }
        else {
            result_timestamp = gwid * _slide_len + _win_len - 1;
        }
    }

    // Evaluate the status of the window given a new wrapped tuple
    win_event_t onTuple(const tuple_t &_tuple,
                        uint64_t _index,
                        uint64_t _ts)
    {
        if (winType == Win_Type_t::CB) { // count-based windows (identifiers are assumed ordered)
            win_event_t event = triggerer(_index); // evaluate the triggerer
            if (event == win_event_t::IN) {
                num_tuples++;
                if (!firstTuple) {
                    firstTuple = std::make_optional(wrapper_tuple_t(_tuple, _index)); // save the input wrapped tuple
                    result_timestamp = _ts; // set the result timestamp to be the one of the input wrapped tuple
                }
                else {
                    if (result_timestamp < _ts) {
                        result_timestamp = _ts; // update the result timestamp with a more recent one
                    }
                }
            }
            else if (event == win_event_t::FIRED) {
                if (!lastTuple) {
                    lastTuple = std::make_optional(wrapper_tuple_t(_tuple, _index)); // save the input wrapped tuple
                }
            }
            else {
                abort(); // OLD event is not possible with in-order streams
            }
            return event;
        }
        else { // time-based windows (timestamps can be received disordered)
            assert(_index == _ts); // sanity check
            win_event_t event = triggerer(_index); // evaluate the triggerer
            if (event == win_event_t::IN) {
                num_tuples++;
                if (!firstTuple) {
                    firstTuple = std::make_optional(wrapper_tuple_t(_tuple, _index)); // save the input wrapped tuple
                }
                else {
                    if (_index < firstTuple->index) {
                        firstTuple = std::make_optional(wrapper_tuple_t(_tuple, _index)); // save the oldest tuple returning IN
                    }
                }
            }
            else if (event == win_event_t::FIRED) {
                if (!lastTuple) {
                    lastTuple = std::make_optional(wrapper_tuple_t(_tuple, _index)); // save the input wrapped tuple
                }
                else {
                    if (_index < lastTuple->index) {
                        lastTuple = std::make_optional(wrapper_tuple_t(_tuple, _index)); // save the oldest tuple returning FIRED
                    }
                }
            }
            return event;
        }
    }

    // Get an optional to the first wrapped tuple
    std::optional<wrapper_t> getFirstTuple() const
    {
        return firstTuple;
    }

    // method to get an optional to the last wrapped tuple
    std::optional<wrapper_t> getLastTuple() const
    {
        return lastTuple;
    }

    // Get the key attribute of the window
    key_t getKEY() const
    {
        return key;
    }

    // Get the local window identifier
    uint64_t getLWID() const
    {
        return lwid;
    }

    // Get the global window identifier
    uint64_t getGWID() const
    {
        return gwid;
    }

    // Get the number of tuples that raised a IN event
    size_t getSize() const
    {
        return num_tuples;
    }

    // Get the timestamp of the window result
    uint64_t getResultTimestamp() const
    {
        return result_timestamp;
    }
};

} // namespace wf

#endif
