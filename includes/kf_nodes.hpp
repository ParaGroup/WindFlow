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
 *  @file    kf_nodes.hpp
 *  @author  Gabriele Mencagli
 *  @date    02/10/2018
 *  
 *  @brief Emitter and Collector nodes of the Key_Farm and Key_Farm_GPU patterns
 *  
 *  @section Key_Farm_Nodes (Description)
 *  
 *  This file implements the Emitter and the Collector nodes used in the Key_Farm
 *  and Key_Farm_GPU patterns in the library.
 */ 

#ifndef KF_NODES_H
#define KF_NODES_H

// includes
#include <vector>
#include <ff/multinode.hpp>

namespace wf {

// class KF_Emitter
template<typename tuple_t>
class KF_Emitter: public ff::ff_monode_t<tuple_t, tuple_t>
{
private:
    // type of the function to map the key hashcode onto an identifier starting from zero to pardegree-1
    using routing_func_t = std::function<size_t(size_t, size_t)>;
    routing_func_t routing_func; // routing function
    size_t pardegree; // parallelism degree (number of inner patterns)
    bool isCombined; // true if this node is used within a treeComb node
    std::vector<std::pair<tuple_t *, int>> output_queue; // used in case of treeComb mode

public:
    // Constructor
    KF_Emitter(routing_func_t _routing_func,
               size_t _pardegree):
               routing_func(_routing_func),
               pardegree(_pardegree),
               isCombined(false)
    {}

    // svc_init method (utilized by the FastFlow runtime)
    int svc_init()
    {
        return 0;
    }

    // svc method (utilized by the FastFlow runtime)
    tuple_t *svc(tuple_t *t)
    {
        // extract the key from the input tuple
        auto key = std::get<0>(t->getControlFields()); // key
        size_t hashcode = std::hash<decltype(key)>()(key); // compute the hashcode of the key
        // evaluate the routing function
        size_t dest_w = routing_func(hashcode, pardegree);
        if (!isCombined)
            this->ff_send_out_to(t, dest_w);
        else
            output_queue.push_back(std::make_pair(t, dest_w));
        return this->GO_ON;
    }

    // svc_end method (FastFlow runtime)
    void svc_end() {}

    // get the number of destinations
    size_t getNDestinations()
    {
        return pardegree;
    }

    // set/unset the treeComb mode
    void setTreeCombMode(bool _val)
    {
        isCombined = _val;
    }

    // method to get a reference to the internal output queue (used in treeComb mode)
    std::vector<std::pair<tuple_t *, int>> &getOutputQueue()
    {
        return output_queue;
    }
};

// class KF_NestedCollector
template<typename result_t>
class KF_NestedCollector: public ff::ff_minode_t<result_t, result_t>
{
private:
    result_t tmp; // never used
    // key data type
    using key_t = typename std::remove_reference<decltype(std::get<0>(tmp.getControlFields()))>::type;
    // inner struct of a key descriptor
    struct Key_Descriptor
    {
        uint64_t next_win; // next window to be transmitted of that key
        std::deque<result_t *> resultsSet; // std::deque of buffered results of that key

        // Constructor
        Key_Descriptor(): next_win(0) {}

    };
    // hash table that maps key identifiers onto key descriptors
    std::unordered_map<key_t, Key_Descriptor> keyMap;

public:
    // Constructor
    KF_NestedCollector() {}

    // svc_init method (utilized by the FastFlow runtime)
    int svc_init()
    {
        return 0;
    }

    // svc method (utilized by the FastFlow runtime)
    result_t *svc(result_t *r)
    {
        // extract key and identifier from the result
        auto key = std::get<0>(r->getControlFields()); // key
        size_t hashcode = std::hash<decltype(key)>()(key); // compute the hashcode of the key
        uint64_t wid = std::get<1>(r->getControlFields()); // identifier
        // find the corresponding key descriptor
        auto it = keyMap.find(key);
        if (it == keyMap.end()) {
            // create the descriptor of that key
            keyMap.insert(std::make_pair(key, Key_Descriptor()));
            it = keyMap.find(key);
        }
        Key_Descriptor &key_d = (*it).second;
        uint64_t &next_win = key_d.next_win;
        std::deque<result_t *> &resultsSet = key_d.resultsSet;
        // add the new result at the correct place
        if ((wid - next_win) >= resultsSet.size()) {
            size_t new_size = (wid - next_win) + 1;
            resultsSet.resize(new_size, nullptr);
        }
        resultsSet[wid - next_win] = r;
        // scan all the buffered results and emit the ones in order
        auto itr = resultsSet.begin();
        for (; itr < resultsSet.end(); itr++) {
            if (*itr != nullptr) {
                this->ff_send_out(*itr);
                next_win++;
            }
            else break;
        }
        // delete the entries of the emitted results
        resultsSet.erase(resultsSet.begin(), itr);
        return this->GO_ON;
    }

    // svc_end method (utilized by the FastFlow runtime)
    void svc_end() {}
};

} // namespace wf

#endif
