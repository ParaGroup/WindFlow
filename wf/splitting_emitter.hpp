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
 *  This file implements the splitting emitter in charge of doing the splitting of a MultiPipe.
 */ 

#ifndef SPLITTING_H
#define SPLITTING_H

// includes
#include <vector>
#include <ff/multinode.hpp>
#include <basic_emitter.hpp>

namespace wf {

// class Splitting_Emitter
template<typename F_t>
class Splitting_Emitter: public Basic_Emitter
{
private:
    F_t splitting_func;
    // extract the tuple type from the signature of the splitting logic
    using tuple_t = decltype(get_tuple_t_Split(splitting_func));
    // extract the result type from the signature of the splitting logic
    using result_t = decltype(get_result_t_Split(splitting_func));
    // static assert to check the signature
    static_assert(!(std::is_same<tuple_t, std::false_type>::value || std::is_same<result_t, std::false_type>::value),
        "WindFlow Compilation Error - unknown signature used in a MultiPipe splitting:\n"
        "  Candidate 1 : size_t(const tuple_t &)\n"
        "  Candidate 2 : std::vector<size_t>(const tuple_t &)\n"
        "  Candidate 3 : size_t(tuple_t &)\n"
        "  Candidate 4 : std::vector<size_t>(tuple_t &)\n"
        "  You can replace size_t in the signatures above with any C++ integral type\n");
    size_t n_dest; // number of destinations
    bool isCombined; // true if this node is used within a Tree_Emitter node
    std::vector<std::pair<void *, int>> output_queue; // used in case of Tree_Emitter mode

    // method for calling the splitting function (used if it returns a single identifier)
    template<typename split_func_t=F_t>
    std::vector<size_t> callSplittingFunction(typename std::enable_if<std::is_same<split_func_t, split_func_t>::value && std::is_integral<result_t>::value, F_t>::type &func, tuple_t &t)
    {
        std::vector<size_t> dests;
        auto dest = func(t);
        dests.push_back(dest); // integral type will be converted into size_t
        return dests;
    }

    // method for calling the splitting function (used if it returns a vector of identifiers)
    template<typename split_func_t=F_t>
    auto callSplittingFunction(typename std::enable_if<std::is_same<split_func_t, split_func_t>::value && !std::is_integral<result_t>::value, F_t>::type &func, tuple_t &t)
    {
        return func(t);
    }

public:
    // Constructor
    Splitting_Emitter(F_t _splitting_func,
                      size_t _n_dest):
                      splitting_func(_splitting_func),
                      n_dest(_n_dest),
                      isCombined(false)
    {}

    // clone method
    Basic_Emitter *clone() const
    {
        Splitting_Emitter<F_t> *copy = new Splitting_Emitter<F_t>(*this);
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
        auto dests_w = callSplittingFunction(splitting_func, *t);
        assert(dests_w.size() <= n_dest);
        // the input must be dropped (like in a filter)
        if (dests_w.size() == 0) {
            delete t;
            return this->GO_ON;
        }
        size_t idx = 0;
        while (idx < dests_w.size()) {
            assert(dests_w[idx] < n_dest);
            // send tuple
            if (!isCombined) {
                this->ff_send_out_to(t, dests_w[idx]);
            }
            else {
                output_queue.push_back(std::make_pair(t, dests_w[idx]));
            }
            idx++;
            if (idx < dests_w.size()) {
                t = new tuple_t(*t); // copy of the input tuple
            }
        }
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
