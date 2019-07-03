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
 *  @file    standard.hpp
 *  @author  Gabriele Mencagli
 *  @date    12/02/2019
 *  
 *  @brief Standard nodes used by the WindFlow library
 *  
 *  @section Standard Nodes (Description)
 *  
 *  This file implements a set of nodes used by some of the patterns in the library.
 */ 

#ifndef STANDARD_H
#define STANDARD_H

// includes
#include <vector>
#include <ff/multinode.hpp>

using namespace ff;
using namespace std;

// class Standard_Emitter
template<typename tuple_t>
class Standard_Emitter: public ff_monode_t<tuple_t>
{
private:
    // type of the function to map the key hashcode onto an identifier starting from zero to pardegree-1
    using routing_func_t = function<size_t(size_t, size_t)>;
    bool isKeyed; // flag stating whether the key-based distribution is used or not
    routing_func_t routing_func; // routing function
    bool isCombined; // true if this node is used within a treeComb node
    vector<pair<tuple_t *, int>> output_queue; // used in case of treeComb mode
    size_t dest_w; // used to select the destination
    size_t n_dest; // number of destinations

public:
    // Constructor I
    Standard_Emitter(size_t _n_dest):
                     isKeyed(false), isCombined(false), dest_w(0), n_dest(_n_dest) {}

    // Constructor II
    Standard_Emitter(routing_func_t _routing_func, size_t _n_dest):
                     isKeyed(true), routing_func(_routing_func), isCombined(false), dest_w(0), n_dest(_n_dest) {}

    // svc_init method (utilized by the FastFlow runtime)
    int svc_init()
    {
        return 0;
    }

    // svc method (utilized by the FastFlow runtime)
    tuple_t *svc(tuple_t *t)
    {
    	if (isKeyed) { // keyed-based distribution enabled
	        // extract the key from the input tuple
	        auto key = std::get<0>(t->getControlFields()); // key
            size_t hashcode = hash<decltype(key)>()(key); // compute the hashcode of the key
	        // evaluate the routing function
	        dest_w = routing_func(hashcode, this->get_num_outchannels());
	        // sent the tuple
            if (!isCombined)
	           this->ff_send_out_to(t, dest_w);
            else
                output_queue.push_back(make_pair(t, dest_w));
	        return this->GO_ON;
    	}
    	else { // default distribution
    		if (!isCombined)
                return t;
            else {
               output_queue.push_back(make_pair(t, dest_w));
               dest_w = (dest_w + 1) % n_dest;
               return this->GO_ON;
            }
        }
    }

    // svc_end method (FastFlow runtime)
    void svc_end() {}

    // get the number of destinations
    size_t getNDestinations()
    {
        return n_dest;
    }

    // set/unset the treeComb mode
    void setTreeCombMode(bool _val)
    {
        isCombined = _val;
    }

    // method to get a reference to the internal output queue (used in treeComb mode)
    vector<pair<tuple_t *, int>> &getOutputQueue()
    {
        return output_queue;
    }
};

// class Standard_Collector
class Standard_Collector: public ff_minode
{
    void *svc(void *t) { return t; }
};

#endif
