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
 *  @section Windows (Description)
 *  
 *  This file implements the classes used by the WindFlow library to support
 *  streaming windows. The library natively supports count-based and time-
 *  based (tumbling, sliding or hopping) window models.
 */ 

#ifndef WINDOW_H
#define WINDOW_H

// includes
#include<tuple>
#include <functional>
#if __cplusplus < 201703L //not C++17
    #include <experimental/optional>
    using namespace std::experimental;
#else
    #include <optional>
#endif
#include <basic.hpp>

namespace wf {

// window events
enum win_event_t { CONTINUE, FIRED, BATCHED };

// class Triggerer_CB
class Triggerer_CB
{
private:
    uint64_t win_len; // window length in number of tuples
    uint64_t slide_len; // slide length in number of tuples
    uint64_t wid; // window identifier (starting from zero)
    uint64_t initial_id; // identifier of the first tuple

public:
    // Constructor
    Triggerer_CB(uint64_t _win_len,
                 uint64_t _slide_len,
                 uint64_t _wid,
                 uint64_t _initial_id=0):
                 win_len(_win_len),
                 slide_len(_slide_len),
                 wid(_wid),
                 initial_id(_initial_id)
    {}

    // method to trigger a new event from a tuple's identifier
    win_event_t operator()(uint64_t _id) const
    {
        return (_id > (win_len + wid * slide_len - 1) + initial_id) ? FIRED : CONTINUE;
    }
};

// class Triggerer_TB
class Triggerer_TB
{
private:
    uint64_t win_len; // window length in time units
    uint64_t slide_len; // slide length in time units
    uint64_t wid; // window identifier (starting from zero)
    uint64_t starting_ts; // starting timestamp

public:
    // Constructor 
    Triggerer_TB(uint64_t _win_len,
                 uint64_t _slide_len,
                 uint64_t _wid,
                 uint64_t _starting_ts=0):
                 win_len(_win_len),
                 slide_len(_slide_len),
                 wid(_wid),
                 starting_ts(_starting_ts)
    {}

    // method to trigger a new event from a tuple's identifier
    win_event_t operator()(uint64_t _ts) const
    {
        return (_ts >= (win_len + wid * slide_len) + starting_ts) ? FIRED : CONTINUE;
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
    win_type_t winType; // type of the window (CB or TB)
    triggerer_t triggerer; // triggerer used by the window (it must be compliant with its type)
    result_t *result; // pointer to the result of the window processing
    std::optional<tuple_t> firstTuple; // optional object containing the first tuple raising a CONTINUE event
    std::optional<tuple_t> firingTuple; // optional object containing the first tuple raising a FIRED event
    key_t key; // key attribute
    uint64_t lwid; // local identifier of the window (starting from zero)
    uint64_t gwid; // global identifier of the window (starting from zero)
    size_t no_tuples; // number of tuples that raised a CONTINUE event on the window
    bool batched; // flag stating whether the window is batched or not

public:
    // Constructor 
    Window(key_t _key,
           uint64_t _lwid,
           uint64_t _gwid,
           triggerer_t _triggerer,
           win_type_t _winType,
           uint64_t _win_len,
           uint64_t _slide_len):
           winType(_winType),
           triggerer(_triggerer),
           result(new result_t()),
           key(_key),
           lwid(_lwid),
           gwid(_gwid),
           no_tuples(0),
           batched(false)
    {
        // initialize the key, gwid and timestamp of the window result
        if (winType == CB)
            result->setControlFields(_key, _gwid, 0);
        else
            result->setControlFields(_key, _gwid, _gwid * _slide_len + _win_len - 1);
    }

    // copy Constructor
    Window(const Window &win)
    {
        winType = win.winType;
        triggerer = win.triggerer;
        result = new result_t(*win.result);
        firstTuple = win.firstTuple;
        firingTuple = win.firingTuple;
        key = win.key;
        lwid = win.lwid;
        gwid = win.gwid;
        no_tuples = win.no_tuples;
        batched = win.batched;
    }

    // method to evaluate the status of the window
    win_event_t onTuple(const tuple_t &_t)
    {
        // extract the tuple identifier/timestamp field
        uint64_t id = (winType == CB) ? std::get<1>(_t.getControlFields()) : std::get<2>(_t.getControlFields());
        // evaluate the triggerer
        win_event_t event = triggerer(id);
        if (event == CONTINUE) {
            no_tuples++;
            if (!firstTuple)
                firstTuple = std::make_optional(_t); // need a copy Constructor for tuple_t
            if (winType == CB)
                result->setControlFields(key, gwid, std::get<2>(_t.getControlFields()));
        }
        if (event == FIRED) {
            if (!firingTuple)
                firingTuple = std::make_optional(_t); // need a copy Constructor for tuple_t
        }
        if (batched)
            return BATCHED;
        else return event;
    }

    // set the window as batched
    void setBatched()
    {
        batched = true;
    }

    // method to return the pointer to the result
    result_t *getResult() const
    {
        return result;
    }

    // method to get an optional object to the first tuple raising a CONTINUE event on the window
    std::optional<tuple_t> getFirstTuple() const
    {
        return firstTuple;
    }

    // method to get an optional object to the first tuple raising a FIRED event on the window
    std::optional<tuple_t> getFiringTuple() const
    {
        return firingTuple;
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
