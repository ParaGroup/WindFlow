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
 *  @file    window.hpp
 *  @author  Gabriele Mencagli
 *  @date    28/06/2017
 *  
 *  @brief Streaming windows
 *  
 *  @section Window (Description)
 *  
 *  This file implements the classes used by the WindFlow library to support
 *  streaming windows. The library natively supports count-based and time-
 *  based (tumbling, sliding or hopping) window models.
 */ 

#ifndef WINDOW_H
#define WINDOW_H

// includes
#include<tuple>
#include<functional>
#if __cplusplus < 201703L // not C++17
    #include<experimental/optional>
    namespace std { using namespace experimental; }
#else
    #include<optional>
#endif
#include<basic.hpp>

namespace wf {

// class Triggerer_CB (for in-order streams only)
class Triggerer_CB
{
private:
    uint64_t win_len; // window length in number of tuples
    uint64_t slide_len; // slide length in number of tuples
    uint64_t lwid; // local window identifier (starting from zero)
    uint64_t initial_id; // identifier of the first tuple

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

    // method to trigger a new event from the input tuple's identifier
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

// class Triggerer_TB (also for out-of-order streams)
class Triggerer_TB
{
private:
    uint64_t win_len; // window length in time units
    uint64_t slide_len; // slide length in time units
    uint64_t lwid; // local window identifier (starting from zero)
    uint64_t starting_ts; // starting timestamp
    uint64_t triggering_delay; // triggering delay in time units

public:
    // Constructor
    Triggerer_TB(uint64_t _win_len,
                 uint64_t _slide_len,
                 uint64_t _lwid,
                 uint64_t _starting_ts,
                 uint64_t _triggering_delay):
                 win_len(_win_len),
                 slide_len(_slide_len),
                 lwid(_lwid),
                 starting_ts(_starting_ts),
                 triggering_delay(_triggering_delay) {}

    // method to trigger a new event from the input tuple's timestamp
    win_event_t operator()(uint64_t _ts) const
    {
        if (_ts < starting_ts + lwid * slide_len) {
            return win_event_t::OLD;
        }
        else if (_ts < (win_len + lwid * slide_len) + starting_ts) {
            return win_event_t::IN;
        }
        else if (_ts < (win_len + lwid * slide_len) + starting_ts + triggering_delay) {
            return win_event_t::DELAYED;
        }
        else {
            return win_event_t::FIRED;
        }
    }
};

// class Window
template<typename tuple_t, typename result_t>
class Window
{
private:
    // triggerer type of the window
    using triggerer_t = std::function<win_event_t(uint64_t)>;
    tuple_t tmp; // never used
    // key data type
    using key_t = typename std::remove_reference<decltype(std::get<0>(tmp.getControlFields()))>::type;
    key_t key; // key attribute
    uint64_t lwid; // local identifier of the window (starting from zero)
    uint64_t gwid; // global identifier of the window (starting from zero)
    triggerer_t triggerer; // triggerer used by the window (it must be compliant with its type)
    win_type_t winType; // type of the window (CB or TB)
    size_t no_tuples; // number of tuples raising a IN event for the window
    bool batched; // flag stating whether the window is batched or not
    result_t result; // result of the window processing
    std::optional<tuple_t> firstTuple;
    std::optional<tuple_t> lastTuple;

public:
    // Constructor 
    Window(key_t _key,
           uint64_t _lwid,
           uint64_t _gwid,
           triggerer_t _triggerer,
           win_type_t _winType,
           uint64_t _win_len,
           uint64_t _slide_len):
           key(_key),
           lwid(_lwid),
           gwid(_gwid),
           triggerer(_triggerer),
           winType(_winType),
           no_tuples(0),
           batched(false)
    {
        // initialize the key, gwid and timestamp of the window result
        if (winType == win_type_t::CB) {
            result.setControlFields(_key, _gwid, 0);
        }
        else {
            result.setControlFields(_key, _gwid, _gwid * _slide_len + _win_len - 1);
        }
    }

    // copy Constructor
    Window(const Window &win)
    {
        key = win.key;
        lwid = win.lwid;
        gwid = win.gwid;
        triggerer = win.triggerer;
        winType = win.winType;
        no_tuples = win.no_tuples;
        batched = win.batched;
        result = win.result;
        firstTuple = win.firstTuple;
        lastTuple = win.lastTuple;
    }

    // method to evaluate the status of the window given a new input tuple _t
    win_event_t onTuple(const tuple_t &_t)
    {
        // the window has been batched, doing nothing
        if (batched) {
            return win_event_t::BATCHED;
        }
        if (winType == win_type_t::CB) { // count-based windows (the stream is assumed to be received ordered by identifiers, not necessarily by timestamps!)
            uint64_t id = std::get<1>(_t.getControlFields()); // id of the input tuple
            // evaluate the triggerer
            win_event_t event = triggerer(id);
            if (event == win_event_t::IN) {
                no_tuples++;
                if (!firstTuple) {
                    firstTuple = std::make_optional(_t); // save this tuple
                    // window result has the timestamp of the most recent tuple raising IN
                    result.setControlFields(std::get<0>(result.getControlFields()), std::get<1>(result.getControlFields()), std::get<2>(_t.getControlFields()));             
                }
                else {
                    uint64_t result_ts = std::get<2>(result.getControlFields());
                    uint64_t ts = std::get<2>(_t.getControlFields());
                    if (result_ts < ts) {
                        // window result has the timestamp of the most recent tuple raising IN
                        result.setControlFields(std::get<0>(result.getControlFields()), std::get<1>(result.getControlFields()), ts);
                    }
                }
            }
            else if (event == win_event_t::FIRED) {
                if (!lastTuple) {
                    lastTuple = std::make_optional(_t); // save the first tuple returning FIRED
                }
            }
            else {
                abort(); // OLD is not possible with in-order streams
            }
            return event;
        }
        else { // time-based windows
            uint64_t ts = std::get<2>(_t.getControlFields()); // timestamp of the input tuple
            // evaluate the triggerer
            win_event_t event = triggerer(ts);
            if (event == win_event_t::IN) {
                no_tuples++;
                if (!firstTuple) {
                    firstTuple = std::make_optional(_t); // save this tuple
                }
                else {
                    uint64_t old_ts = std::get<2>(firstTuple->getControlFields());
                    if (ts < old_ts) {
                        firstTuple = std::make_optional(_t); // save the oldest tuple returning IN
                    }
                }
            }
            else if (event == win_event_t::DELAYED || event == win_event_t::FIRED) {
                if (!lastTuple) {
                    lastTuple = std::make_optional(_t); // save this tuple
                }
                else {
                    uint64_t old_ts = std::get<2>(lastTuple->getControlFields());
                    if (ts < old_ts) {
                        lastTuple = std::make_optional(_t); // save the oldest tuple more recent that the window final boundary
                    }
                }
            }
            return event;
        }
    }

    // set the window as batched
    void setBatched()
    {
        batched = true;
    }

    // method to return a reference to the result
    result_t &getResult()
    {
        return result;
    }

    // method to get an optional object to the first tuple
    std::optional<tuple_t> getFirstTuple() const
    {
        return firstTuple;
    }

    // method to get an optional object to the last tuple
    std::optional<tuple_t> getLastTuple() const
    {
        return lastTuple;
    }

    // method to get the key identifier
    size_t getKEY() const
    {
        return key;
    }

    // method to get the local window identifier
    uint64_t getLWID() const
    {
        return lwid;
    }

    // method to get the global window identifier
    uint64_t getGWID() const
    {
        return gwid;
    }

    // method to get the number of tuples that raised a CONTINUE event on the window
    size_t getSize() const
    {
        return no_tuples;
    }

    // the method returns true if the window was batched
    bool isBatched() const
    {
        return batched;
    }
};

} // namespace wf

#endif
