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
 *  
 *  @brief Emitter and collector nodes of the Win_Farm and Win_Farm_GPU operators
 *  
 *  @section Emitter and collector nodes of the Win_Farm and Win_Farm_GPU
 *           operators (Description)
 *  
 *  This file implements the emitter and the collector nodes used by the Win_Farm
 *  and Win_Farm_GPU operators.
 */ 

#ifndef WF_NODES_H
#define WF_NODES_H

// includes
#include<cmath>
#include<vector>
#include<ff/multinode.hpp>
#include<meta.hpp>
#include<basic_emitter.hpp>

namespace wf {

// class WF_Emitter
template<typename tuple_t, typename input_t=tuple_t>
class WF_Emitter: public Basic_Emitter
{
private:
    // type of the wrapper of input tuples
    using wrapper_in_t = wrapper_tuple_t<tuple_t>;
    tuple_t tmp; // never used
    // key data type
    using key_t = typename std::remove_reference<decltype(std::get<0>(tmp.getControlFields()))>::type;
    win_type_t winType; // type of the windows (CB or TB)
    uint64_t win_len; // window length (in no. of tuples or in time units)
    uint64_t slide_len; // window slide (in no. of tuples or in time units)
    size_t pardegree; // parallelism degree (number of inner operators)
    size_t id_outer; // identifier in the outermost operator
    size_t n_outer; // parallelism degree in the outermost operator
    uint64_t slide_outer; // sliding factor utilized by the outermost operator
    role_t role; // role of the innermost operator
    std::vector<size_t> to_workers; // std::vector of identifiers used for scheduling purposes
    // struct of a key descriptor
    struct Key_Descriptor
    {
        uint64_t rcv_counter; // number of tuples received of this key
        tuple_t last_tuple; // copy of the last tuple received of this key (the one with highest timestamp)

        // Constructor
        Key_Descriptor(): rcv_counter(0) {}
    };
    std::unordered_map<key_t, Key_Descriptor> keyMap; // hash table that maps a descriptor for each key
    bool isCombined; // true if this node is used within a Tree_Emitter node
    std::vector<std::pair<void *, int>> output_queue; // used in case of Tree_Emitter mode

public:
    // Constructor
    WF_Emitter(win_type_t _winType,
               uint64_t _win_len,
               uint64_t _slide_len,
               size_t _pardegree,
               size_t _id_outer,
               size_t _n_outer,
               uint64_t _slide_outer,
               role_t _role):
               winType(_winType),
               win_len(_win_len),
               slide_len(_slide_len),
               pardegree(_pardegree),
               id_outer(_id_outer),
               n_outer(_n_outer),
               slide_outer(_slide_outer),
               role(_role),
               to_workers(pardegree),
               isCombined(false) {}

    // clone method
    Basic_Emitter *clone() const override
    {
        WF_Emitter<tuple_t, input_t> *copy = new WF_Emitter<tuple_t, input_t>(*this);
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
            keyMap.insert(std::make_pair(key, Key_Descriptor()));
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
        // gwid of the first window of that key assigned to this Win_Farm
        uint64_t first_gwid_key = (id_outer - (hashcode % n_outer) + n_outer) % n_outer;
        // initial identifer/timestamp of the keyed sub-stream arriving at this Win_Farm
        uint64_t initial_id = first_gwid_key * slide_outer;
        // special cases: role is WLQ or REDUCE
        if (role == role_t::WLQ || role == role_t::REDUCE)
            initial_id = 0;
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
            if (id+1-initial_id < win_len) {
                first_w = 0;
            }
            else {
                first_w = ceil(((double) (id + 1 - win_len - initial_id))/((double) slide_len));
            }
            last_w = ceil(((double) id + 1 - initial_id)/((double) slide_len)) - 1;
        }
        // hopping windows
        else {
            uint64_t n = floor((double) (id-initial_id) / slide_len);
            // if the tuple belongs to at least one window of this Win_Farm
            if (id-initial_id >= n*(slide_len) && id-initial_id < (n*slide_len)+win_len) {
                first_w = last_w = n;
            }
            else {
                // delete the received tuple
                deleteTuple<tuple_t, input_t>(wt);
                return this->GO_ON;
            }
        }
        // determine the set of internal operators that will receive the tuple
        uint64_t countRcv = 0;
        uint64_t i = first_w;
        // the first window of the key is assigned to worker startDstIdx
        size_t startDstIdx = hashcode % pardegree;
        while ((i <= last_w) && (countRcv < pardegree)) {
            to_workers[countRcv] = (startDstIdx + i) % pardegree;
            countRcv++;
            i++;
        }
        // prepare the wrapper to be sent
        wrapper_in_t *out = prepareWrapper<input_t, wrapper_in_t>(wt, countRcv);
        // for each destination we send the same wrapper
        for (size_t i = 0; i < countRcv; i++) {
            if (!isCombined) {
                this->ff_send_out_to(out, to_workers[i]);
            }
            else {
                output_queue.push_back(std::make_pair(out, to_workers[i]));
            }
        }
        return this->GO_ON;
    }

    // method to manage the EOS (utilized by the FastFlow runtime)
    void eosnotify(ssize_t id) override
    {
        // iterate over all the keys
        for (auto &k: keyMap) {
            Key_Descriptor &key_d = k.second;
            if (key_d.rcv_counter > 0) {
                // send the last tuple to all the internal operators as an EOS marker
                tuple_t *t = new tuple_t();
                *t = key_d.last_tuple;
                wrapper_in_t *wt = new wrapper_in_t(t, pardegree, true); // eos marker enabled
                for (size_t i=0; i < pardegree; i++) {
                    if (!isCombined) {
                        this->ff_send_out_to(wt, i);
                    }
                    else {
                        output_queue.push_back(std::make_pair(wt, i));
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
        return pardegree;
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

// class WF_Collector
template<typename result_t>
class WF_Collector: public ff::ff_minode_t<result_t, result_t>
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
