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
 *  @file    wm_nodes.hpp
 *  @author  Gabriele Mencagli
 *  @date    02/10/2018
 *  @version 1.0
 *  
 *  @brief Emitter and Collector nodes of the MAP stage of the Win_MapReduce and
 *         Win_MapReduce_GPU patterns
 *  
 *  @section DESCRIPTION
 *  
 *  This file implements the Emitter and the Collector nodes of the MAP stage of the
 *  Win_MapReduce and Win_MapReduce_GPU patterns in the library.
 */ 

#ifndef WM_NODES_H
#define WM_NODES_H

// includes
#include <ff/multinode.hpp>

// class MAP_Emitter
template<typename tuple_t, typename input_t=tuple_t>
class MAP_Emitter: public ff_monode_t<input_t, wrapper_tuple_t<tuple_t>>
{
private:
    // type of the wrapper of input tuples
    using wrapper_in_t = wrapper_tuple_t<tuple_t>;
    // friendships with other classes in the library
    template<typename T1, typename T2, typename T3>
    friend class Win_MapReduce;
    template<typename T1, typename T2, typename T3, typename T4>
    friend class Win_MapReduce_GPU;
    size_t map_degree; // parallelism degree (MAP phase)
    // struct of a key descriptor
    struct Key_Descriptor
    {
        size_t rcv_counter; // number of tuples received of this key
        tuple_t last_tuple; // copy of the last tuple received of this key
        size_t nextDst; // id of the Win_Seq instance receiving the next tuple of this key

        // constructor
        Key_Descriptor(size_t _nextDst): rcv_counter(0), nextDst(_nextDst) {}

        // destructor
       ~Key_Descriptor() {}
    };
    unordered_map<size_t, Key_Descriptor> keyMap; // hash table that maps a descriptor for each key

    // private constructor
    MAP_Emitter(size_t _map_degree): map_degree(_map_degree) {}

    // destructor
    ~MAP_Emitter() {}

    // svc_init method (utilized by the FastFlow runtime)
    int svc_init()
    {
        return 0;
    }

    // svc method (utilized by the FastFlow runtime)
    wrapper_in_t *svc(input_t *wt)
    {
        // extract the key and id/timestamp fields from the input tuple
        tuple_t *t = extractTuple<tuple_t, input_t>(wt);
        size_t key = (t->getInfo()).first; // key
        uint64_t id = (t->getInfo()).second; // identifier or timestamp
        // access the descriptor of the input key
        auto it = keyMap.find(key);
        if (it == keyMap.end()) {
            // create the descriptor of that key
            keyMap.insert(make_pair(key, Key_Descriptor(key % map_degree)));
            it = keyMap.find(key);
        }
        Key_Descriptor &key_d = (*it).second;
        key_d.rcv_counter++;
        key_d.last_tuple = *t;
        // prepare the wrapper to be sent
        wrapper_in_t *out = prepareWrapper<input_t, wrapper_in_t>(wt, 1);
        // send the wrapper to the next Win_Seq instance
        this->ff_send_out_to(out, key_d.nextDst);
        key_d.nextDst = (key_d.nextDst + 1) % map_degree;
        return this->GO_ON;
    }

    // method to manage the EOS (utilized by the FastFlow runtime)
    void eosnotify(ssize_t id)
    {
        // iterate over all the keys
        for (auto &k: keyMap) {
            Key_Descriptor &key_d = k.second;
            if (key_d.rcv_counter > 0) {
                // send the last tuple to all the internal patterns
                tuple_t *tuple = new tuple_t();
                *tuple = key_d.last_tuple;
                wrapper_in_t *out = new wrapper_in_t(tuple, map_degree, true); // eos marker enabled
                for (size_t i=0; i < map_degree; i++)
                    this->ff_send_out_to(out, i);
            }
        }
    }

    // svc_end method (utilized by the FastFlow runtime)
    void svc_end() {}
};

// class MAP_Collector
template<typename result_t>
class MAP_Collector: public ff_node_t<result_t, result_t>
{
private:
    // friendships with other classes in the library
    template<typename T1, typename T2, typename T3>
    friend class Win_MapReduce;
    template<typename T1, typename T2, typename T3, typename T4>
    friend class Win_MapReduce_GPU;
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
    MAP_Collector() {}

    // destructor
    ~MAP_Collector() {}

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
