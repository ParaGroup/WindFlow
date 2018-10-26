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
 *  @file    wf_nodes.hpp
 *  @author  Gabriele Mencagli
 *  @date    01/10/2018
 *  @version 1.0
 *  
 *  @brief Emitter and Collector nodes of the Win_Farm and Win_Farm_GPU patterns
 *  
 *  @section DESCRIPTION
 *  
 *  This file implements the Emitter and the Collector nodes used in the Win_Farm
 *  and Win_Farm_GPU patterns in the library.
 */ 

#ifndef WF_NODES_H
#define WF_NODES_H

// includes
#include <ff/multinode.hpp>

// class WF_Emitter
template<typename tuple_t, typename input_t=tuple_t>
class WF_Emitter: public ff_monode_t<input_t, wrapper_tuple_t<tuple_t>>
{
private:
    // type of the wrapper of input tuples
    using wrapper_in_t = wrapper_tuple_t<tuple_t>;
    // friendships with other classes in the library
    template<typename T1, typename T2, typename T3>
    friend class Win_Farm;
    template<typename T1, typename T2, typename T3, typename T4>
    friend class Win_Farm_GPU;
    uint64_t win_len; // window length (in no. of tuples or in time units)
    uint64_t slide_len; // window slide (in no. of tuples or in time units)
    size_t pardegree; // parallelism degree (number of inner patterns)
    size_t id_outer; // identifier in the outermost pattern
    size_t n_outer; // parallelism degree in the outermost pattern
    size_t slide_outer; // sliding factor utilized by the outermost pattern
    role_t role; // role of the innermost pattern
    vector<size_t> to_workers; // vector of identifiers used for scheduling purposes
    // struct of a key descriptor
    struct Key_Descriptor
    {
        size_t rcv_counter; // number of tuples received of this key
        tuple_t last_tuple; // copy of the last tuple received of this key

        // constructor
        Key_Descriptor(): rcv_counter(0) {}

        // destructor
       ~Key_Descriptor() {}
    };
    unordered_map<size_t, Key_Descriptor> keyMap; // hash table that maps a descriptor for each key

    // private constructor
    WF_Emitter(uint64_t _win_len, uint64_t _slide_len, size_t _pardegree, size_t _id_outer, size_t _n_outer, size_t _slide_outer, role_t _role):
               win_len(_win_len),
               slide_len(_slide_len),
               pardegree(_pardegree),
               id_outer(_id_outer),
               n_outer(_n_outer),
               slide_outer(_slide_outer),
               role(_role),
               to_workers(pardegree) {}

    // destructor
    ~WF_Emitter() {}

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
            keyMap.insert(make_pair(key, Key_Descriptor()));
            it = keyMap.find(key);
        }
        Key_Descriptor &key_d = (*it).second;
        key_d.rcv_counter++;
        key_d.last_tuple = *t;
        // gwid of the first window of that key assigned to this Win_Farm instance
        size_t first_gwid_key = (id_outer - (key % n_outer) + n_outer) % n_outer;
        // initial identifer/timestamp of the keyed sub-stream arriving at this Win_Farm instance
        size_t initial_id = first_gwid_key * slide_outer;
        // special cases: role is WLQ or REDUCE
        if (role == WLQ || role == REDUCE)
            initial_id = 0;
        // if the id/timestamp of the tuple is smaller than the initial one, it must be discarded
        if (id < initial_id) {
            deleteTuple<tuple_t, input_t>(wt);
            return this->GO_ON;
        }
        // determine the range of local window identifiers that contain t
        size_t first_w;
        size_t last_w;
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
            size_t n = floor((double) (id-initial_id) / slide_len);
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
        size_t countRcv = 0;
        size_t i = first_w;
        // the first window of the key is assigned to worker startDstIdx
        size_t startDstIdx = key % pardegree;
        while ((i <= last_w) && (countRcv < pardegree)) {
            to_workers[countRcv] = (startDstIdx + i) % pardegree;
            countRcv++;
            i++;
        }
        // prepare the wrapper to be sent
        wrapper_in_t *out = prepareWrapper<input_t, wrapper_in_t>(wt, countRcv);
        // for each destination we send the same wrapper
        for (size_t i = 0; i < countRcv; i++)
            this->ff_send_out_to(out, to_workers[i]);
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
                tuple_t *t = new tuple_t();
                *t = key_d.last_tuple;
                wrapper_in_t *wt = new wrapper_in_t(t, pardegree, true); // eos marker enabled
                for (size_t i=0; i < pardegree; i++)
                    this->ff_send_out_to(wt, i);
            }
        }
    }

    // svc_end method (utilized by the FastFlow runtime)
    void svc_end() {}
};

// class WF_Collector
template<typename result_t>
class WF_Collector: public ff_node_t<result_t, result_t>
{
private:
    // friendships with other classes in the library
    template<typename T1, typename T2, typename T3>
    friend class Win_Farm;
    template<typename T1, typename T2, typename T3, typename T4>
    friend class Win_Farm_GPU;
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
    WF_Collector() {}

    // destructor
    ~WF_Collector() {}

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
