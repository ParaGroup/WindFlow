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
 *  This file implements the splitting emitter in charge of doing splitting of MultiPipe.
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
    // function to get the destination from an input
    using splitting_func_t = std::function<size_t(const tuple_t &)>;
    splitting_func_t splitting_func; // splitting function
    size_t n_dest; // number of destinations
    bool isCombined; // true if this node is used within a Tree_Emitter node
    std::vector<std::pair<void *, int>> output_queue; // used in case of Tree_Emitter mode

public:
    // Constructor I
    Splitting_Emitter(splitting_func_t _splitting_func,
                     size_t _n_dest):
                     splitting_func(_splitting_func),
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
        size_t dest_w = splitting_func(*t);
        assert(dest_w<n_dest);
        // send tuple
        if (!isCombined)
            this->ff_send_out_to(t, dest_w);
        else
            output_queue.push_back(std::make_pair(t, dest_w));
        return this->GO_ON;
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
