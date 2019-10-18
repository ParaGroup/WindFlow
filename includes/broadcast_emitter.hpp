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
 *  @file    broadcast_emitter.hpp
 *  @author  Gabriele Mencagli
 *  @date    27/06/2019
 *  
 *  @brief Emitter used for broadcasting data items to several destinations
 *  
 *  @section Broadcast_Emitter (Description)
 *  
 *  The emitter delivers each input item to all of its destination nodes.
 */ 

#ifndef BROADCASTNODE_H
#define BROADCASTNODE_H

// includes
#include <vector>
#include <ff/multinode.hpp>
#include <meta_utils.hpp>
#include <basic_emitter.hpp>

namespace wf {

// class Broadcast_Emitter
template<typename tuple_t, typename input_t=tuple_t>
class Broadcast_Emitter: public Basic_Emitter
{
private:
    // type of the wrapper of input tuples
    using wrapper_in_t = wrapper_tuple_t<tuple_t>;
    size_t n_dest; // number of destinations
    bool isCombined; // true if this node is used within a Tree_Emitter node
    std::vector<std::pair<void *, int>> output_queue; // used in case of Tree_Emitter mode

public:
    // Constructor
    Broadcast_Emitter(size_t _n_dest):
                   n_dest(_n_dest),
                   isCombined(false)
    {}

    // clone method
    Basic_Emitter *clone() const
    {
        Broadcast_Emitter<tuple_t, input_t> *copy = new Broadcast_Emitter<tuple_t, input_t>(*this);
        return copy;
    }

    // svc_init method (utilized by the FastFlow runtime)
    int svc_init()
    {
        return 0;
    }

    // svc method (utilized by the FastFlow runtime)
    void *svc(void *in)
    {
        input_t *wt = reinterpret_cast<input_t *>(in);
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
    size_t getNDestinations() const
    {
        return n_dest;
    }

    // set/unset the Tree_Emitter mode
    void setTree_EmitterMode(bool _val)
    {
        isCombined = _val;
    }

    // method to get a reference to the internal output queue (used in Tree_Emitter mode)
    std::vector<std::pair<void *, int>> &getOutputQueue()
    {
        return output_queue;
    }
};

} // namespace wf

#endif
