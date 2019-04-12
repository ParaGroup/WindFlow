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
 *  
 *  @brief Utility nodes of the Win_MapReduce and Win_MapReduce_GPU patterns
 *  
 *  @section Win_MapReduce_Nodes (Description)
 *  
 *  This file implements the utility nodes (Emitter, Collector and DroppingNode) of the
 *  MAP stage of the Win_MapReduce and Win_MapReduce_GPU patterns in the library.
 */ 

#ifndef WM_NODES_H
#define WM_NODES_H

// includes
#include <ff/multinode.hpp>

using namespace ff;

// class WinMap_Emitter
template<typename tuple_t, typename input_t=tuple_t>
class WinMap_Emitter: public ff_monode_t<input_t, wrapper_tuple_t<tuple_t>>
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
    win_type_t winType; // type of the windows (CB or TB)
    // struct of a key descriptor
    struct Key_Descriptor
    {
        uint64_t rcv_counter; // number of tuples received of this key
        tuple_t last_tuple; // copy of the last tuple received of this key
        size_t nextDst; // id of the Win_Seq instance receiving the next tuple of this key

        // constructor
        Key_Descriptor(size_t _nextDst): rcv_counter(0), nextDst(_nextDst) {}
    };
    unordered_map<size_t, Key_Descriptor> keyMap; // hash table that maps a descriptor for each key

    // private constructor
    WinMap_Emitter(size_t _map_degree, win_type_t _winType): map_degree(_map_degree), winType(_winType) {}

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
        size_t key = std::get<0>(t->getInfo()); // key
        uint64_t id = (winType == CB) ? std::get<1>(t->getInfo()) : std::get<2>(t->getInfo()); // identifier or timestamp
        // access the descriptor of the input key
        auto it = keyMap.find(key);
        if (it == keyMap.end()) {
            // create the descriptor of that key
            keyMap.insert(make_pair(key, Key_Descriptor(key % map_degree)));
            it = keyMap.find(key);
        }
        Key_Descriptor &key_d = (*it).second;
        // check duplicate or out-of-order tuples
        if (key_d.rcv_counter == 0) {
            key_d.rcv_counter++;
            key_d.last_tuple = *t;
        }
        else {
            // tuples can be received only ordered by id/timestamp
            uint64_t last_id = (winType == CB) ? std::get<1>((key_d.last_tuple).getInfo()) : std::get<2>((key_d.last_tuple).getInfo());
            if (id < last_id) {
                // the tuple is immediately deleted
                deleteTuple<tuple_t, input_t>(wt);
                return this->GO_ON;
            }
            else {
                key_d.rcv_counter++;
                key_d.last_tuple = *t;
            }
        }
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

// class WinMap_Dropper
template<typename tuple_t>
class WinMap_Dropper: public ff_node_t<wrapper_tuple_t<tuple_t>, wrapper_tuple_t<tuple_t>>
{
private:
    // type of the wrapper of input tuples
    using wrapper_in_t = wrapper_tuple_t<tuple_t>;
    // friendships with other classes in the library
    friend class MultiPipe;
    size_t map_degree; // parallelism degree (MAP phase)
    // struct of a key descriptor
    struct Key_Descriptor
    {
        uint64_t rcv_counter; // number of tuples received of this key
        tuple_t last_tuple; // copy of the last tuple received of this key
        size_t nextDst; // id of the Win_Seq instance receiving the next tuple of this key

        // constructor
        Key_Descriptor(size_t _nextDst): rcv_counter(0), nextDst(_nextDst) {}
    };
    unordered_map<size_t, Key_Descriptor> keyMap; // hash table that maps a descriptor for each key
    size_t my_id; // identifier of the Win_Seq instance associated with this WinMap_Dropper istance

    // private constructor
    WinMap_Dropper(size_t _my_id, size_t _map_degree): my_id(_my_id), map_degree(_map_degree) {}

    // svc_init method (utilized by the FastFlow runtime)
    int svc_init()
    {
        return 0;
    }

    // svc method (utilized by the FastFlow runtime)
    wrapper_in_t *svc(wrapper_in_t *wt)
    {
        // extract the key field from the input tuple
        tuple_t *t = extractTuple<tuple_t, wrapper_in_t>(wt);
        size_t key = std::get<0>(t->getInfo()); // key
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
        // decide whether to drop or send the tuple
        if (key_d.nextDst == my_id) {
            // sent the tuple
            this->ff_send_out(wt);
        }
        else {
            // delete the tuple
            deleteTuple<tuple_t, wrapper_in_t>(wt);
        }
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
                // send the last tuple to the Win_Seq instance associated with this WinMap_Dropper instance
                tuple_t *tuple = new tuple_t();
                *tuple = key_d.last_tuple;
                wrapper_in_t *out = new wrapper_in_t(tuple, 1, true); // eos marker enabled
                this->ff_send_out(out);
            }
        }
    }

    // svc_end method (utilized by the FastFlow runtime)
    void svc_end() {}
};

// class WinMap_Collector
template<typename result_t>
class WinMap_Collector: public ff_node_t<result_t, result_t>
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
        uint64_t next_win; // next window to be transmitted of that key
        deque<result_t *> resultsSet; // deque of buffered results of that key

        // constructor
        Key_Descriptor(): next_win(0) {}
    };
    // hash table that maps key identifiers onto key descriptors
    unordered_map<size_t, Key_Descriptor> keyMap;

    // private constructor
    WinMap_Collector() {}

    // svc_init method (utilized by the FastFlow runtime)
    int svc_init()
    {
        return 0;
    }

    // svc method (utilized by the FastFlow runtime)
    result_t *svc(result_t *r)
    {
        // extract key and identifier from the result
        size_t key = std::get<0>(r->getInfo()); // key
        uint64_t wid = std::get<1>(r->getInfo()); // identifier
        // find the corresponding key descriptor
        auto it = keyMap.find(key);
        if (it == keyMap.end()) {
            // create the descriptor of that key
            keyMap.insert(make_pair(key, Key_Descriptor()));
            it = keyMap.find(key);
        }
        Key_Descriptor &key_d = (*it).second;
        uint64_t &next_win = key_d.next_win;
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
