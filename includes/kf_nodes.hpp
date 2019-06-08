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
#include <ff/multinode.hpp>

using namespace ff;

// class KF_Emitter
template<typename tuple_t, typename input_t=tuple_t>
class KF_Emitter: public ff_monode_t<input_t, wrapper_tuple_t<tuple_t>>
{
private:
    // type of the wrapper of input tuples
    using wrapper_in_t = wrapper_tuple_t<tuple_t>;
    // function type to map the key hashcode onto an identifier starting from zero to pardegree-1
    using routing_func_t = function<size_t(size_t, size_t)>;
    // friendships with other classes in the library
    template<typename T1, typename T2, typename T3>
    friend class Key_Farm;
    template<typename T1, typename T2, typename T3, typename T4>
    friend class Key_Farm_GPU;
    friend class MultiPipe;
    routing_func_t routing; // routing function
    size_t pardegree; // parallelism degree (number of inner patterns)

    // private constructor
    KF_Emitter(routing_func_t _routing, size_t _pardegree):
               routing(_routing), pardegree(_pardegree) {}

    // svc_init method (utilized by the FastFlow runtime)
    int svc_init()
    {
        return 0;
    }

    // svc method (utilized by the FastFlow runtime)
    wrapper_in_t *svc(input_t *wt)
    {
        // extract the key from the input tuple
        tuple_t *t = extractTuple<tuple_t, input_t>(wt);
        auto key = std::get<0>(t->getControlFields()); // key
        size_t hashcode = hash<decltype(key)>()(key); // compute the hashcode of the key
        // evaluate the routing function
        size_t dest_w = routing(hashcode, pardegree);
        // prepare the wrapper to be sent
        wrapper_in_t *out = prepareWrapper<input_t, wrapper_in_t>(wt, 1);
        this->ff_send_out_to(out, dest_w);
        return this->GO_ON;
    }

    // svc_end method (FastFlow runtime)
    void svc_end() {}
};

// class KF_NestedEmitter
template<typename tuple_t, typename input_t=tuple_t>
class KF_NestedEmitter: public ff_monode_t<input_t, wrapper_tuple_t<tuple_t>>
{
private:
    // type of the wrapper of input tuples
    using wrapper_in_t = wrapper_tuple_t<tuple_t>;
    // function type to map the key hashcode onto an identifier starting from zero to pardegree1-1
    using routing_func_t = function<size_t(size_t, size_t)>;
    tuple_t tmp; // never used
    // key data type
    using key_t = typename remove_reference<decltype(std::get<0>(tmp.getControlFields()))>::type;
    // friendships with other classes in the library
    template<typename T1, typename T2, typename T3>
    friend class Key_Farm;
    template<typename T1, typename T2, typename T3, typename T4>
    friend class Key_Farm_GPU;
    friend class MultiPipe;
    routing_func_t routing; // routing function (used by level 1)
    win_type_t winType; // type of the windows (CB or TB, used by level 2)
    uint64_t win_len; // window length (in no. of tuples or in time units, used by level 2)
    uint64_t slide_len; // window slide (in no. of tuples or in time units, used by level 2)
    size_t pardegree1; // parallelism degree (of level 1)
    size_t pardegree2; // parallelism degree (of level 2)
    role_t role; // role of level 2
    vector<size_t> to_workers; // vector of identifiers used for scheduling purposes (used by level 2)
    // struct of a key descriptor
    struct Key_Descriptor
    {
        uint64_t rcv_counter; // number of tuples received of this key
        tuple_t last_tuple; // copy of the last tuple received of this key
        size_t nextDst; // id of the Win_Seq instance receiving the next tuple of this key (meaningful if role is MAP)

        // constructor
        Key_Descriptor(size_t _nextDst): rcv_counter(0), nextDst(_nextDst) {}
    };
    unordered_map<key_t, Key_Descriptor> keyMap; // hash table that maps a descriptor for each key

    // private constructor
    KF_NestedEmitter(routing_func_t _routing, win_type_t _winType, uint64_t _win_len, uint64_t _slide_len, size_t _pardegree1, size_t _pardegree2, role_t _role):
                     routing(_routing),
                     winType(_winType),
                     win_len(_win_len),
                     slide_len(_slide_len),
                     pardegree1(_pardegree1),
                     pardegree2(_pardegree2),
                     role(_role),
                     to_workers(pardegree2) {}

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
        auto key = std::get<0>(t->getControlFields()); // key
        size_t hashcode = hash<decltype(key)>()(key); // compute the hashcode of the key
        uint64_t id = (winType == CB) ? std::get<1>(t->getControlFields()) : std::get<2>(t->getControlFields()); // identifier or timestamp
        // access the descriptor of the input key
        auto it = keyMap.find(key);
        if (it == keyMap.end()) {
            // create the descriptor of that key
            keyMap.insert(make_pair(key, Key_Descriptor(hashcode % pardegree2)));
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
            uint64_t last_id = (winType == CB) ? std::get<1>((key_d.last_tuple).getControlFields()) : std::get<2>((key_d.last_tuple).getControlFields());
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
        // **************************************** KF_Emitter emitter logic (Level 1) **************************************** //
        // evaluate the routing function
        size_t level1_idx = routing(hashcode, pardegree1);
        if (role == PLQ) {
            // **************************************** WF_Emitter emitter logic (Level 2) **************************************** //
            // gwid of the first window of that key assigned to this Win_Farm instance
            uint64_t first_gwid_key = 0;
            // initial identifer/timestamp of the keyed sub-stream arriving at this Win_Farm instance
            uint64_t initial_id = 0;
            // if the id/timestamp of the tuple is smaller than the initial one, it must be discarded
            if (id < initial_id) {
                deleteTuple<tuple_t, input_t>(wt);
                return this->GO_ON;
            }
            // determine the range of local window identifiers that contain t
            long first_w = -1;
            long last_w = -1;
            // sliding or tumbling windows
            if (win_len >= slide_len) {
                if (id+1-initial_id < win_len)
                    first_w = 0;
                else
                    first_w = ceil(((double) (id + 1 - win_len - initial_id))/((double) slide_len));
                last_w = ceil(((double) id + 1 - initial_id)/((double) slide_len)) - 1;
            }
            // hopping windows
            else {
                uint64_t n = floor((double) (id-initial_id) / slide_len);
                // if the tuple belongs to at least one window of this Win_Farm instance
                if (id-initial_id >= n*(slide_len) && id-initial_id < (n*slide_len)+win_len) {
                    first_w = last_w = n;
                }
                else {
                    // delete the received tuple
                    deleteTuple<tuple_t, input_t>(wt);
                    return this->GO_ON;
                }
            }
            // determine the set of internal patterns that will receive the tuple
            uint64_t countRcv = 0;
            uint64_t i = first_w;
            // the first window of the key is assigned to worker startDstIdx
            size_t startDstIdx = hashcode % pardegree2;
            while ((i <= last_w) && (countRcv < pardegree2)) {
                to_workers[countRcv] = (startDstIdx + i) % pardegree2;
                countRcv++;
                i++;
            }
            // prepare the wrapper to be sent
            wrapper_in_t *out = prepareWrapper<input_t, wrapper_in_t>(wt, countRcv);
            // for each destination we send the same wrapper
            for (size_t i = 0; i < countRcv; i++)
                this->ff_send_out_to(out, (level1_idx * pardegree2) + to_workers[i]);
        }
        else {
            // **************************************** WM_Emitter basic logic (Level 2) **************************************** //
            // prepare the wrapper to be sent
            wrapper_in_t *out = prepareWrapper<input_t, wrapper_in_t>(wt, 1);
            // send the wrapper to the next Win_Seq instance
            this->ff_send_out_to(out, (level1_idx * pardegree2) + key_d.nextDst);
            key_d.nextDst = (key_d.nextDst + 1) % pardegree2;
        }
        return this->GO_ON;
    }

    // method to manage the EOS (utilized by the FastFlow runtime)
    void eosnotify(ssize_t id)
    {
        // iterate over all the keys
        for (auto &k: keyMap) {
            auto key = k.first;
            size_t hashcode = hash<decltype(key)>()(key); // compute the hashcode of the key
            Key_Descriptor &key_d = k.second;
            if (key_d.rcv_counter > 0) {
                // evaluate the routing function
                size_t level1_idx = routing(hashcode, pardegree1);
                // send the last tuple to all the internal patterns as an EOS marker
                tuple_t *t = new tuple_t();
                *t = key_d.last_tuple;
                wrapper_in_t *wt = new wrapper_in_t(t, pardegree2, true); // eos marker enabled
                for (size_t i=0; i < pardegree2; i++)
                    this->ff_send_out_to(wt, (level1_idx * pardegree2) + i);
            }
        }
    }

    // svc_end method (FastFlow runtime)
    void svc_end() {}
};

// class KF_NestedCollector
template<typename result_t>
class KF_NestedCollector: public ff_node_t<result_t, result_t>
{
private:
    result_t tmp; // never used
    // key data type
    using key_t = typename remove_reference<decltype(std::get<0>(tmp.getControlFields()))>::type;
    // friendships with other classes in the library
    template<typename T1, typename T2, typename T3>
    friend class Key_Farm;
    template<typename T1, typename T2, typename T3, typename T4>
    friend class Key_Farm_GPU;
    // inner struct of a key descriptor
    struct Key_Descriptor
    {
        uint64_t next_win; // next window to be transmitted of that key
        deque<result_t *> resultsSet; // deque of buffered results of that key

        // constructor
        Key_Descriptor(): next_win(0) {}

    };
    // hash table that maps key identifiers onto key descriptors
    unordered_map<key_t, Key_Descriptor> keyMap;

    // private constructor
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
        size_t hashcode = hash<decltype(key)>()(key); // compute the hashcode of the key
        uint64_t wid = std::get<1>(r->getControlFields()); // identifier
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
