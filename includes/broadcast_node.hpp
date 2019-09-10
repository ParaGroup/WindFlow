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
 *  @file    broadcast_node.hpp
 *  @author  Gabriele Mencagli
 *  @date    27/06/2019
 *  
 *  @brief FastFlow node used for broadcasting data items to several destinations
 *  
 *  @section Broadcast_Node (Description)
 *  
 *  The node has multiple input streams and delivers each input item to all of its
 *  destination nodes.
 */ 

#ifndef BROADCASTNODE_H
#define BROADCASTNODE_H

// includes
#include <vector>
#include <ff/multinode.hpp>
#include <meta_utils.hpp>

namespace wf {

// class Broadcast_Node
template<typename tuple_t, typename input_t=tuple_t>
class Broadcast_Node: public ff::ff_monode_t<input_t, wrapper_tuple_t<tuple_t>>
{
private:
    // type of the wrapper of input tuples
    using wrapper_in_t = wrapper_tuple_t<tuple_t>;
    size_t n_dest; // number of destinations
    bool isCombined; // true if this node is used within a treeComb node
    std::vector<std::pair<wrapper_in_t *, int>> output_queue; // used in case of treeComb mode

public:
    // Constructor
    Broadcast_Node(size_t _n_dest): n_dest(_n_dest), isCombined(false) {}

    // svc_init method (utilized by the FastFlow runtime)
    int svc_init()
    {
        return 0;
    }

    // svc method (utilized by the FastFlow runtime)
    wrapper_in_t *svc(input_t *wt)
    {
        wrapper_in_t *out = prepareWrapper<input_t, wrapper_in_t>(wt, n_dest);
        for(size_t i=0; i<n_dest; i++) {
            if (!isCombined)
                this->ff_send_out_to(out, i);
            else
                output_queue.push_back(std::make_pair(out, i));
        }
        return this->GO_ON;
    }

    // svc_end method (utilized by the FastFlow runtime)
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
    std::vector<std::pair<wrapper_in_t *, int>> &getOutputQueue()
    {
        return output_queue;
    }
};

} // namespace wf

#endif
