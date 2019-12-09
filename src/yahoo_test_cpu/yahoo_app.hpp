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

/*
 *  Classes of the Yahoo! Streaming Benchmark (WindFlow version)
 *
 *  This version of the Yahoo! Streaming Benchmark is the one modified in the
 *  StreamBench project available in GitHub:
 *  https://github.com/lsds/StreamBench
 */

#ifndef YAHOO_H
#define YAHOO_H

// include
#include <tuple>

using namespace std;
using namespace wf;

// event_t struct
struct event_t
{
    unsigned long ts; // timestamp
    unsigned long user_id; // user id
    unsigned long page_id; // page id
    unsigned long ad_id; // advertisement id
    unsigned int ad_type; // advertisement type (0, 1, 2, 3, 4) => ("banner", "modal", "sponsored-search", "mail", "mobile")
    unsigned int event_type; // event type (0, 1, 2) => ("view", "click", "purchase")
    unsigned int ip; // ip address
    char padding[20]; // padding

    // constructor
    event_t() {}

    // destructor
    ~event_t() {}

    // getControlFields method (needed by the WindFlow library)
    tuple<size_t, uint64_t, uint64_t> getControlFields() const
    {
        return tuple<size_t, uint64_t, uint64_t>((size_t) event_type, (uint64_t) 0, (uint64_t) ts); // be careful with this cast!
    }

    // setControlFields method (needed by the WindFlow library)
    void setControlFields(size_t _key, uint64_t _id, uint64_t _ts)
    {
        event_type = (long) _key;
        ts = (long) _ts;
    }
};

// projected_event_t struct
struct projected_event_t
{
    unsigned long ts; // timestamp
    unsigned long ad_id; // advertisement id

    // constructor
    projected_event_t() {}

    // destructor
    ~projected_event_t() {}

    // getControlFields method (needed by the WindFlow library)
    tuple<size_t, uint64_t, uint64_t> getControlFields() const
    {
        return tuple<size_t, uint64_t, uint64_t>((size_t) 0, (uint64_t) ad_id, (uint64_t) ts); // be careful with this cast!
    }

    // setControlFields method (needed by the WindFlow library)
    void setControlFields(size_t _key, uint64_t _id, uint64_t _ts)
    {
        ad_id = (long) _id;
        ts = (long) _ts;
    }
};

// joined_event_t struct
struct joined_event_t
{
    unsigned long ts; // timestamp
    unsigned long ad_id; // advertisement id
    unsigned long relational_ad_id;
    unsigned long cmp_id; // campaign id

    // constructor
    joined_event_t() {}

    // destructor
    ~joined_event_t() {}

    // getControlFields method (needed by the WindFlow library)
    tuple<size_t, uint64_t, uint64_t> getControlFields() const
    {
        return tuple<size_t, uint64_t, uint64_t>((size_t) cmp_id, (uint64_t) 0, (uint64_t) ts); // be careful with this cast!
    }

    // setControlFields method (needed by the WindFlow library)
    void setControlFields(size_t _key, uint64_t _id, uint64_t _ts)
    {
        cmp_id = (long) _key;
        ts = (long) _ts;
    }
};

// win_result struct
struct win_result
{
    unsigned long wid; // id
    unsigned long ts; // timestamp
    unsigned long cmp_id; // campaign id
    unsigned long lastUpdate; // MAX(TS)
    unsigned long count; // COUNT(*)

    // constructor
    win_result(): lastUpdate(0), count(0) {}

    // destructor
    ~win_result() {}

    // getControlFields method (needed by the WindFlow library)
    tuple<size_t, uint64_t, uint64_t> getControlFields() const
    {
        return tuple<size_t, uint64_t, uint64_t>((size_t) cmp_id, (uint64_t) wid, (uint64_t) ts); // be careful with this cast!
    }

    // setControlFields method (needed by the WindFlow library)
    void setControlFields(size_t _key, uint64_t _id, uint64_t _ts)
    {
        cmp_id = (long) _key;
        ts = (long) _ts;
        wid = _id;
    }
};

// function for computing the final aggregates on tumbling windows (INCremental version)
void aggregateFunctionINC(size_t wid, const joined_event_t &event, win_result &result)
{
    result.count++;
    if (event.ts > result.lastUpdate)
        result.lastUpdate = event.ts;
}

// function for computing the final aggregates on tumbling windows (reduce version)
void reduceFunctionINC(size_t wid, const win_result &r1, win_result &r2)
{
    r2.count += r1.count;
    if (r1.lastUpdate > r2.lastUpdate)
        r2.lastUpdate = r1.lastUpdate;
}

// function for computing the final aggregates on tumbling windows (Non InCremental version)
void aggregateFunctionNIC(size_t wid, const Iterable<joined_event_t> &win, win_result &result)
{
    result.count = win.size();
    for (auto t : win) {
        if (t.ts > result.lastUpdate)
            result.lastUpdate = t.ts;
    }
}

#endif
