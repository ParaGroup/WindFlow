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
 *  @file    splitting_emitter.hpp
 *  @author  Gabriele Mencagli
 *  @date    07/10/2019
 *
 *  @brief Splitting emitter used by the WindFlow library
 *
 *  @section Splitting_Emitter (Description)
 *
 *  This file implements the splitting emitter in charge of doing the splitting of MultiPipe.
 */

#ifndef SPLITTING_H
#define SPLITTING_H

// includes
#include <vector>
#include <ff/multinode.hpp>
#include <basic_emitter.hpp>

namespace wf {

// class Splitting_Emitter
template<typename tuple_t>
class Splitting_Emitter: public Basic_Emitter
{
private:
    // function to get the destination from an input (unicast)
    using unicast_func_t = std::function<size_t(const tuple_t &)>;
    // function to get the destinations from an input (multicast or broadcast)
    using multicast_func_t = std::function<std::vector<size_t>(const tuple_t &)>;
    unicast_func_t unicast_func; // unicast function
    multicast_func_t multicast_func; // multicast or broadcast function
    bool isUnicast; // true if the unicast function must be used (false otherwise)
    size_t n_dest; // number of destinations
    bool isCombined; // true if this node is used within a Tree_Emitter node
    std::vector<std::pair<void *, int>> output_queue; // used in case of Tree_Emitter mode

public:
    // Constructor I
    Splitting_Emitter(unicast_func_t _splitting_func,
                     size_t _n_dest):
                     unicast_func(_splitting_func),
                     isUnicast(true),
                     n_dest(_n_dest),
                     isCombined(false)
    {}

    // Constructor II
    Splitting_Emitter(multicast_func_t _splitting_func,
                     size_t _n_dest):
                     multicast_func(_splitting_func),
                     isUnicast(false),
                     n_dest(_n_dest),
                     isCombined(false)
    {}

    // clone method
    Basic_Emitter *clone() const
    {
        Splitting_Emitter<tuple_t> *copy = new Splitting_Emitter<tuple_t>(*this);
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
        tuple_t *t = reinterpret_cast<tuple_t *>(in);
        if (isUnicast) { // unicast
            size_t dest_w = unicast_func(*t);
            assert(dest_w < n_dest);
            // send tuple
            if (!isCombined)
                this->ff_send_out_to(t, dest_w);
            else
                output_queue.push_back(std::make_pair(t, dest_w));
            return this->GO_ON;
        }
        else { // multicast or broadcast
            auto dests_w = multicast_func(*t);
            assert(dests_w.size() > 0 && dests_w.size() <= n_dest);
            size_t idx = 0;
            while (idx < dests_w.size()) {
                assert(dests_w[idx] < n_dest);
                // send tuple
                if (!isCombined)
                    this->ff_send_out_to(t, dests_w[idx]);
                else
                    output_queue.push_back(std::make_pair(t, dests_w[idx]));
                idx++;
                if (idx < dests_w.size())
                    t = new tuple_t(*t); // copy of the input tuple
            }
            return this->GO_ON;
        }
    }

    // svc_end method (FastFlow runtime)
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
