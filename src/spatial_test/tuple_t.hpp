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
 *  Data type of the input item of the spatial query application.
 *  
 *  A macro named DIM is used to set the number of floating point attributes
 *  per tuple.
 */ 

#ifndef TUPLE_H
#define TUPLE_H

// include
#include <tuple>
#include <array>
#include <iostream>
#include <algorithm>
#include <string.h>
#include <basic.hpp>

using namespace std;

struct tuple_t
{
	size_t key; // not used
    uint64_t id; // unique identifier (starting from zero)
    uint64_t lid; // local id used by the skyline query (starting from zero)
    uint64_t ts; // timestamp in nanoseconds (starting from zero)
    float elems[DIM]; // array of DIM attributes

    // empty constructor
    tuple_t() {
    	key = 0;
        id = 0;
        lid = 0;
        ts = 0;
        fill_n(elems, DIM, 0);
    }

    // copy constructor
    tuple_t(const tuple_t &t)
    {
    	key = t.key;
        id = t.id;
        lid = t.lid;
        ts = t.ts;
        memcpy(elems, t.elems, sizeof(float) * DIM);
    }

    // copy operator
    tuple_t &operator= (const tuple_t &other)
    {
    	key = other.key;
        id = other.id;
        lid = other.lid;
        ts = other.ts;
        memcpy(elems, other.elems, sizeof(float) * DIM);
        return *this;
    }

    // operator == (equal to)
    bool operator== (const tuple_t &other) const
    {
        bool equal = (key == other.key) && (id == other.id) && (lid == other.lid) && (ts == other.ts);
        for (size_t i=0; i<DIM; i++)
            equal = equal && (elems[i] == other.elems[i]);
        return equal;
    }

    // operator < (less than)
    bool operator< (const tuple_t &other) const
    {
        return (id < other.id); // compare only the unique identifiers
    }

    // getInfo method
    tuple<size_t, uint64_t, uint64_t> getInfo() const
    {
        return tuple<size_t, uint64_t, uint64_t>(key, id, ts);
    }

    // setInfo method
    void setInfo(size_t _key, uint64_t _id, uint64_t _ts)
    {
        key = _key;
        id = _id;
        ts = _ts;
    }

    // convert a tuple into an array
    inline array<float, DIM> toArray() const
    {
        array<float, DIM> a;
        for (int i=0; i<DIM; i++)
            a[i] = elems[i];
        return a;
    }

    // print the tuple attributes
    void print() const
    {
        cout << "[";
        for (size_t i=0; i<DIM-1; i++)
            cout << elems[i] << ", ";
        cout << elems[DIM-1] << endl;
    }
};

#endif
