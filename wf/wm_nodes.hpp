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
 *  @brief Emitter, dropper and collector nodes of the Win_MapReduce
 *         and Win_MapReduce_GPU operators
 *  
 *  @section Emitter, dropper and collector nodes of the Win_MapReduce
 *           and Win_MapReduce_GPU operators (Description)
 *  
 *  This file implements the emitter, dropper and collector nodes used by the
 *  MAP stage of the Win_MapReduce and Win_MapReduce_GPU operators.
 */ 

#ifndef WM_NODES_H
#define WM_NODES_H

// includes
#include<vector>
#include<ff/multinode.hpp>
#include<meta.hpp>
#include<basic_emitter.hpp>

namespace wf {

// class WinMap_Emitter
template<typename tuple_t, typename input_t=tuple_t>
class WinMap_Emitter: public Basic_Emitter
{
private:
    // type of the wrapper of input tuples
    using wrapper_in_t = wrapper_tuple_t<tuple_t>;
    tuple_t tmp; // never used
    // key data type
    using key_t = typename std::remove_reference<decltype(std::get<0>(tmp.getControlFields()))>::type;
    size_t map_degree; // parallelism degree (MAP phase)
    win_type_t winType; // type of the windows (CB or TB)
    // struct of a key descriptor
    struct Key_Descriptor
    {
        uint64_t rcv_counter; // number of tuples received of this key
        tuple_t last_tuple; // copy of the last tuple received of this key (the one with highest timestamp)
        size_t nextDst; // id of the Win_Seq receiving the next tuple of this key

        // Constructor
        Key_Descriptor(size_t _nextDst): rcv_counter(0), nextDst(_nextDst) {}
    };
    std::unordered_map<key_t, Key_Descriptor> keyMap; // hash table that maps a descriptor for each key
    bool isCombined; // true if this node is used within a Tree_Emitter node
    std::vector<std::pair<void *, int>> output_queue; // used in case of Tree_Emitter mode

public:
    // Constructor
    WinMap_Emitter(size_t _map_degree,
                   win_type_t _winType):
                   map_degree(_map_degree),
                   winType(_winType),
                   isCombined(false) {}

    // clone method
    Basic_Emitter *clone() const override
    {
        WinMap_Emitter<tuple_t, input_t> *copy = new WinMap_Emitter<tuple_t, input_t>(*this);
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
        input_t *wt = reinterpret_cast<input_t *>(in);
        // extract the key and id/timestamp fields from the input tuple
        tuple_t *t = extractTuple<tuple_t, input_t>(wt);
        auto key = std::get<0>(t->getControlFields()); // key
        size_t hashcode = std::hash<decltype(key)>()(key); // compute the hashcode of the key
        uint64_t id = (winType == win_type_t::CB) ? std::get<1>(t->getControlFields()) : std::get<2>(t->getControlFields()); // identifier or timestamp
        // access the descriptor of the input key
        auto it = keyMap.find(key);
        if (it == keyMap.end()) {
            // create the descriptor of that key
            keyMap.insert(std::make_pair(key, Key_Descriptor(hashcode % map_degree)));
            it = keyMap.find(key);
        }
        Key_Descriptor &key_d = (*it).second;
        // keep track of the last tuple (the one with highest timestamp with that key)
        if (key_d.rcv_counter == 0) {
            key_d.rcv_counter++;
            key_d.last_tuple = *t;
        }
        else {
            key_d.rcv_counter++;
            // get the id/timestamp of current last_tuple
            uint64_t last_id = (winType == win_type_t::CB) ? std::get<1>((key_d.last_tuple).getControlFields()) : std::get<2>((key_d.last_tuple).getControlFields());
            if (id > last_id) {
                key_d.last_tuple = *t;
            }
        }
        // delete the input if it is an EOS marker
        if (isEOSMarker<tuple_t, input_t>(*wt)) {
            deleteTuple<tuple_t, input_t>(wt);
            return this->GO_ON;
        }
        // prepare the wrapper to be sent
        wrapper_in_t *out = prepareWrapper<input_t, wrapper_in_t>(wt, 1);
        // send the wrapper to the next Win_Seq
        if (!isCombined) {
            this->ff_send_out_to(out, key_d.nextDst);
        }
        else {
            output_queue.push_back(std::make_pair(out, key_d.nextDst));
        }
        key_d.nextDst = (key_d.nextDst + 1) % map_degree;
        return this->GO_ON;
    }

    // method to manage the EOS (utilized by the FastFlow runtime)
    void eosnotify(ssize_t id) override
    {
        // iterate over all the keys
        for (auto &k: keyMap) {
            Key_Descriptor &key_d = k.second;
            if (key_d.rcv_counter > 0) {
                // send the last tuple to all the internal operators
                tuple_t *tuple = new tuple_t();
                *tuple = key_d.last_tuple;
                wrapper_in_t *out = new wrapper_in_t(tuple, map_degree, true); // eos marker enabled
                for (size_t i=0; i < map_degree; i++) {
                    if (!isCombined) {
                        this->ff_send_out_to(out, i);
                    }
                    else {
                        output_queue.push_back(std::make_pair(out, i));
                    }
                }
            }
        }
    }

    // svc_end method (utilized by the FastFlow runtime)
    void svc_end() override {}

    // get the number of destinations
    size_t getNDestinations() const override
    {
        return map_degree;
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

// class WinMap_Dropper
template<typename tuple_t>
class WinMap_Dropper: public ff::ff_node_t<wrapper_tuple_t<tuple_t>, wrapper_tuple_t<tuple_t>>
{
private:
    // type of the wrapper of input tuples
    using wrapper_in_t = wrapper_tuple_t<tuple_t>;
    tuple_t tmp; // never used
    // key data type
    using key_t = typename std::remove_reference<decltype(std::get<0>(tmp.getControlFields()))>::type;
    size_t map_degree; // parallelism degree (MAP phase)
    // struct of a key descriptor
    struct Key_Descriptor
    {
        size_t nextDst; // id of the Win_Seq receiving the next tuple of this key

        // Constructor
        Key_Descriptor(size_t _nextDst): nextDst(_nextDst) {}
    };
    std::unordered_map<key_t, Key_Descriptor> keyMap; // hash table that maps a descriptor for each key
    size_t my_id; // identifier of the Win_Seq associated with this WinMap_Dropper istance

public:
    // Constructor
    WinMap_Dropper(size_t _my_id,
                   size_t _map_degree):
                   my_id(_my_id),
                   map_degree(_map_degree) {}

    // svc_init method (utilized by the FastFlow runtime)
    int svc_init() override
    {
        return 0;
    }

    // svc method (utilized by the FastFlow runtime)
    wrapper_in_t *svc(wrapper_in_t *wt) override
    {
        // extract the key field from the input tuple
        tuple_t *t = extractTuple<tuple_t, wrapper_in_t>(wt);
        auto key = std::get<0>(t->getControlFields()); // key
        size_t hashcode = std::hash<decltype(key)>()(key); // compute the hashcode of the key
        // access the descriptor of the input key
        auto it = keyMap.find(key);
        if (it == keyMap.end()) {
            // create the descriptor of that key
            keyMap.insert(std::make_pair(key, Key_Descriptor(hashcode % map_degree)));
            it = keyMap.find(key);
        }
        Key_Descriptor &key_d = (*it).second;
        // pass through of EOSMarkers
        if (isEOSMarker<tuple_t, wrapper_in_t>(*wt)) {
            // sent the tuple
            this->ff_send_out(wt);
        }
        else {
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
        }
        return this->GO_ON;
    }

    // svc_end method (utilized by the FastFlow runtime)
    void svc_end() override {}
};

// class WinMap_Collector
template<typename result_t>
class WinMap_Collector: public ff::ff_minode_t<result_t, result_t>
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
    // svc_init method (utilized by the FastFlow runtime)
    int svc_init() override
    {
        return 0;
    }

    // svc method (utilized by the FastFlow runtime)
    result_t *svc(result_t *r) override
    {
        // extract key and identifier from the result
        auto key = std::get<0>(r->getControlFields()); // key
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
    void svc_end() override {}
};

} // namespace wf

#endif
