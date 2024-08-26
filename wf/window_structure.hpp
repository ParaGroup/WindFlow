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
 *  @file    window_structure.hpp
 *  @author  Gabriele Mencagli
 *  
 *  @brief Streaming windows
 *  
 *  @section Window (Description)
 *  
 *  This file implements the classes used by the WindFlow library to support
 *  streaming windows. The library supports count-based and time-based
 *  (tumbling, sliding or hopping) models. It is used by some of the window-based
 *  operators of the library.
 */ 

#ifndef WINDOW_H
#define WINDOW_H

// includes
#include<optional>
#include<functional>
#include<basic.hpp>

namespace wf {

// class Triggerer_CB
class Triggerer_CB
{
private:
    uint64_t win_len; // window length in number of tuples
    uint64_t slide_len; // slide length in number of tuples
    uint64_t lwid; // local window identifier (starting from zero)
    uint64_t initial_id; // starting identifier

public:
    // Constructor
    Triggerer_CB(uint64_t _win_len,
                 uint64_t _slide_len,
                 uint64_t _lwid,
                 uint64_t _initial_id):
                 win_len(_win_len),
                 slide_len(_slide_len),
                 lwid(_lwid),
                 initial_id(_initial_id) {}

    // Generate a new event from the input identifier
    win_event_t operator()(uint64_t _id) const
    {
        if (_id < initial_id + lwid * slide_len) {
            return win_event_t::OLD;
        }
        else if (_id <= (win_len + lwid * slide_len - 1) + initial_id) {
            return win_event_t::IN;
        }
        else {
            return win_event_t::FIRED;
        }
    }
};

// class Triggerer_TB
class Triggerer_TB
{
private:
    uint64_t win_len; // window length in time units
    uint64_t slide_len; // slide length in time units
    uint64_t lwid; // local window identifier (starting from zero)
    uint64_t initial_ts; // starting timestamp

public:
    // Constructor
    Triggerer_TB(uint64_t _win_len,
                 uint64_t _slide_len,
                 uint64_t _lwid,
                 uint64_t _initial_ts):
                 win_len(_win_len),
                 slide_len(_slide_len),
                 lwid(_lwid),
                 initial_ts(_initial_ts) {}

    // Generate a new event from the input timestamp
    win_event_t operator()(uint64_t _ts) const
    {
        if (_ts < initial_ts + lwid * slide_len) {
            return win_event_t::OLD;
        }
        else if (_ts < (win_len + lwid * slide_len) + initial_ts) {
            return win_event_t::IN;
        }
        else {
            return win_event_t::FIRED;
        }
    }
};

// class Window
template<typename tuple_t, typename result_t, typename key_t>
class Window
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
    result_t result; // result of the window
    std::optional<wrapper_t> firstTuple; // optional containing the first wrapped tuple
    std::optional<wrapper_t> lastTuple; // optional containing the last wrapped tuple
    uint64_t result_timestamp; // timestamp of the window result

public:
    // Constructor 
    Window(key_t _key,
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
           num_tuples(0),
           result(create_win_result_t<result_t, key_t>(_key, _gwid)) // <-- create the window result
    {
        if (winType == Win_Type_t::CB) { // intialize the timestamp of the window result
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

    // Get a reference to the window result
    result_t &getResult()
    {
        return result;
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
