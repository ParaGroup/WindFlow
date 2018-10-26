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
 *  @version 1.0
 *  
 *  @brief Emitter and Collector nodes of the Key_Farm and Key_Farm_GPU patterns
 *  
 *  @section DESCRIPTION
 *  
 *  This file implements the Emitter and the Collector nodes used in the Key_Farm
 *  and Key_Farm_GPU patterns in the library.
 */ 

#ifndef KF_NODES_H
#define KF_NODES_H

// includes
#include <ff/multinode.hpp>

// class KF_Emitter
template<typename tuple_t>
class KF_Emitter: public ff_monode_t<tuple_t, tuple_t>
{
private:
    // function type to map the key onto an identifier starting from zero to pardegree-1
    using f_routing_t = function<size_t(size_t, size_t)>;
    // friendships with other classes in the library
    template<typename T1, typename T2, typename T3>
    friend class Key_Farm;
    template<typename T1, typename T2, typename T3, typename T4>
    friend class Key_Farm_GPU;
    f_routing_t routing; // routing function
    size_t pardegree; // parallelism degree (number of inner patterns)
    vector<size_t> to_workers; // vector of identifiers used for scheduling purposes

    // private constructor
    KF_Emitter(f_routing_t _routing, size_t _pardegree):
               routing(_routing), pardegree(_pardegree), to_workers(_pardegree) {}

    // destructor
    ~KF_Emitter() {}

    // svc_init method (utilized by the FastFlow runtime)
    int svc_init()
    {
        return 0;
    }

    // svc method (utilized by the FastFlow runtime)
    tuple_t *svc(tuple_t *t)
    {
        // extract the key field from the input tuple
        size_t key = (t->getInfo()).first; // key
        // evaluate the routing function
        size_t dest_w = routing(key, pardegree);
        this->ff_send_out_to(t, dest_w);
        return this->GO_ON;
    }

    // svc_end method (FastFlow runtime)
    void svc_end() {}
};

// class KF_NestedCollector
template<typename result_t>
class KF_NestedCollector: public ff_node_t<result_t, result_t>
{
private:
    // friendships with other classes in the library
    template<typename T1, typename T2, typename T3>
    friend class Key_Farm;
    template<typename T1, typename T2, typename T3, typename T4>
    friend class Key_Farm_GPU;
    // inner struct of a key descriptor
    struct Key_Descriptor
    {
        size_t next_win; // next window to be transmitted of that key
        deque<result_t *> resultsSet; // deque of buffered results of that key

        // constructor
        Key_Descriptor(): next_win(0) {}

        // destructor
        ~Key_Descriptor() {}
    };
    // hash table that maps key identifiers onto key descriptors
    unordered_map<size_t, Key_Descriptor> keyMap;

    // private constructor
    KF_NestedCollector() {}

    // destructor
    ~KF_NestedCollector() {}

    // svc_init method (utilized by the FastFlow runtime)
    int svc_init()
    {
        return 0;
    }

    // svc method (utilized by the FastFlow runtime)
    result_t *svc(result_t *r)
    {
        // extract key and identifier from the result
        size_t key = (r->getInfo()).first; // key
        size_t wid = (r->getInfo()).second; // identifier
        // find the corresponding key descriptor
        auto it = keyMap.find(key);
        if (it == keyMap.end()) {
            // create the descriptor of that key
            keyMap.insert(make_pair(key, Key_Descriptor()));
            it = keyMap.find(key);
        }
        Key_Descriptor &key_d = (*it).second;
        size_t &next_win = key_d.next_win;
        deque<result_t *> &resultsSet = key_d.resultsSet;
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

#endif
