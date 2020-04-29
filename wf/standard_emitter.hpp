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
 *  @file    standard_emitter.hpp
 *  @author  Gabriele Mencagli
 *  @date    12/02/2019
 *  
 *  @brief Standard emitter used by the WindFlow library
 *  
 *  @section Standard Emitter (Description)
 *  
 *  This file implements the standard emitter used by the library.
 */ 

#ifndef STANDARD_EMITTER_H
#define STANDARD_EMITTER_H

// includes
#include<vector>
#include<basic.hpp>
#include<ff/multinode.hpp>
#include<basic_emitter.hpp>

namespace wf {

// class Standard_Emitter
template<typename tuple_t>
class Standard_Emitter: public Basic_Emitter
{
private:
    // type of the function to map the key hashcode onto an identifier starting from zero to n_dest-1
    using routing_func_t = std::function<size_t(size_t, size_t)>;
    bool isKeyBy; // flag stating whether the key-based distribution is used or not
    routing_func_t routing_func; // routing function
    bool isCombined; // true if this node is used within a Tree_Emitter node
    std::vector<std::pair<void *, int>> output_queue; // used in case of Tree_Emitter mode
    size_t dest_w; // used to select the destination
    size_t n_dest; // number of destinations

public:
    // Constructor I
    Standard_Emitter(size_t _n_dest):
                     isKeyBy(false),
                     isCombined(false),
                     dest_w(0),
                     n_dest(_n_dest) {}

    // Constructor II
    Standard_Emitter(routing_func_t _routing_func,
                     size_t _n_dest):
                     isKeyBy(true),
                     routing_func(_routing_func),
                     isCombined(false),
                     dest_w(0),
                     n_dest(_n_dest) {}

    // clone method
    Basic_Emitter *clone() const override
    {
        Standard_Emitter<tuple_t> *copy = new Standard_Emitter<tuple_t>(*this);
        return copy;
    }

    // svc_init method (utilized by the FastFlow runtime)
    int svc_init() override
    {
        return 0;
    }

    // svc method (utilized by the FastFlow runtime)
    void *svc(void *in) override
    {
        tuple_t *t = reinterpret_cast<tuple_t *>(in);
        if (isKeyBy) { // keyed-based distribution enabled
            // extract the key from the input tuple
            auto key = std::get<0>(t->getControlFields()); // key
            size_t hashcode = std::hash<decltype(key)>()(key); // compute the hashcode of the key
            // evaluate the routing function
            dest_w = routing_func(hashcode, n_dest);
            // send the tuple
            if (!isCombined)
               this->ff_send_out_to(t, dest_w);
            else
                output_queue.push_back(std::make_pair(t, dest_w));
            return this->GO_ON;
        }
        else { // default distribution
            if (!isCombined)
                return t;
            else {
               output_queue.push_back(std::make_pair(t, dest_w));
               dest_w = (dest_w + 1) % n_dest;
               return this->GO_ON;
            }
        }
    }

    // svc_end method (FastFlow runtime)
    void svc_end() override {}

    // get the number of destinations
    size_t getNDestinations() const override
    {
        return n_dest;
    }

    // set/unset the Tree_Emitter mode
    void setTree_EmitterMode(bool _val) override
    {
        isCombined = _val;
    }

    // method to get a reference to the internal output queue (used in Tree_Emitter mode)
    std::vector<std::pair<void *, int>> &getOutputQueue() override
    {
        return output_queue;
    }
};

} // namespace wf

#endif
