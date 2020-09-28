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
 *  @file    ordering_node.hpp
 *  @author  Gabriele Mencagli
 *  @date    19/08/2018
 *  
 *  @brief Node used for reordering data items received from multiple streams
 *  
 *  @section Ordering_Node (Description)
 *  
 *  The node has multiple input streams and assumes that input items are received
 *  in order from each distinct input stream. The node reorders items and emits
 *  them in increasing order. The node can be configured to order either by unique
 *  identifiers or by timestamps.
 */ 

#ifndef ORDERING_NODE_H
#define ORDERING_NODE_H

// includes
#include<deque>
#include<queue>
#include<unordered_map>
#include<ff/multinode.hpp>
#include<meta.hpp>
#include<basic.hpp>

namespace wf {

// class Ordering_Node
template<typename tuple_t, typename input_t=tuple_t>
class Ordering_Node: public ff::ff_minode_t<input_t, input_t>
{
private:
    tuple_t tmp; // never used
    // key data type
    using key_t = typename std::remove_reference<decltype(std::get<0>(tmp.getControlFields()))>::type;
    // comparator functor (returns true if A comes before B in the ordering)
    struct Comparator {
        // ordering mode
        ordering_mode_t mode;

        // Constructor
        Comparator(ordering_mode_t _mode): mode(_mode) {}

        // operator()
        bool operator() (input_t *wA, input_t *wB) {
            tuple_t *A = extractTuple<tuple_t, input_t>(wA);
            tuple_t *B = extractTuple<tuple_t, input_t>(wB);
            uint64_t id_A = (mode == ordering_mode_t::ID) ? std::get<1>(A->getControlFields()) : std::get<2>(A->getControlFields());
            uint64_t id_B = (mode == ordering_mode_t::ID) ? std::get<1>(B->getControlFields()) : std::get<2>(B->getControlFields());
            if (id_A > id_B) {
                return true;
            }
            else if (id_A < id_B) {
                return false;
            }
            else {
                assert(A != B);
                return (A > B); // compare the memory pointers to have a unique ordering!!!
            }
        }
    };
    // inner struct of a key descriptor
    struct Key_Descriptor
    {
        uint64_t emit_counter; // progressive counter (used if mode is TS_RENUMBERING)
        std::vector<uint64_t> maxs; // maxs[i] contains the greatest identifier/timestamp received from the i-th input stream
        input_t *eos_marker; // pointer to the most recent EOS marker of this key
        // ordered queue of tuples of the given key received by the node
        std::priority_queue<input_t *, std::deque<input_t *>, Comparator> queue;

        // Constructor
        Key_Descriptor(size_t _n,
                       ordering_mode_t _mode):
                       emit_counter(0),
                       maxs(_n, 0),
                       eos_marker(nullptr),
                       queue(Comparator(_mode)) {}
    };
    // hash table that maps key identifiers onto key descriptors
    std::unordered_map<key_t, Key_Descriptor> keyMap;
    size_t eos_received; // number of received EOS messages
    ordering_mode_t mode; // ordering mode
    // variables for correcting the bug (temporarily)
    std::priority_queue<input_t *, std::deque<input_t *>, Comparator> globalQueue;
    std::vector<uint64_t> globalMaxs;

public:
    // Constructor
    Ordering_Node(ordering_mode_t _mode=ordering_mode_t::ID, std::atomic<unsigned long> *_atomic_num_dropped=nullptr):
                  eos_received(0),
                  mode(_mode),
                  globalQueue(Comparator(_mode)) {}

    // svc_init method (utilized by the FastFlow runtime)
    int svc_init() override
    {
        for (size_t i=0; i<this->get_num_inchannels(); i++) {
            globalMaxs.push_back(0);
        }
        return 0;
    }

    // svc method (utilized by the FastFlow runtime)
    input_t *svc(input_t *wr) override
    {
        // extract the key and id/ts from the input tuple
        tuple_t *r = extractTuple<tuple_t, input_t>(wr);
        auto key = std::get<0>(r->getControlFields()); // key
        uint64_t wid = (mode == ordering_mode_t::ID) ? std::get<1>(r->getControlFields()) : std::get<2>(r->getControlFields()); // identifier/timestamp
        // find the corresponding key descriptor
        auto it = keyMap.find(key);
        if (it == keyMap.end()) {
            // create the descriptor of that key
            keyMap.insert(std::make_pair(key, Key_Descriptor(this->get_num_inchannels(), mode)));
            it = keyMap.find(key);
        }
        Key_Descriptor &key_d = (*it).second;
        // update the most recent EOS marker of this key
        if (key_d.eos_marker == nullptr && isEOSMarker<tuple_t, input_t>(*wr)) {
            key_d.eos_marker = wr;
            return this->GO_ON;
        }
        else if (isEOSMarker<tuple_t, input_t>(*wr)) {
            tuple_t *tmp = extractTuple<tuple_t, input_t>(key_d.eos_marker);
            uint64_t tmp_id = (mode == ordering_mode_t::ID) ? std::get<1>(tmp->getControlFields()) : std::get<2>(tmp->getControlFields());
            if (wid > tmp_id) {
                deleteTuple<tuple_t, input_t>(key_d.eos_marker);
                key_d.eos_marker = wr;
            }
            else
                deleteTuple<tuple_t, input_t>(wr);
            return this->GO_ON;
        }
        // get the index of the source's stream
        size_t source_id = this->get_channel_id();
        uint64_t min_id = 0;
        auto &queue = (mode == ordering_mode_t::ID) ? key_d.queue : globalQueue;
        if (mode == ordering_mode_t::ID) { // ordering on a key-basis
            key_d.maxs[source_id] = wid;
            min_id = *(std::min_element((key_d.maxs).begin(), (key_d.maxs).end()));
        }
        else { // ordering regardless the key
            globalMaxs[source_id] = wid;
            min_id = *(std::min_element(globalMaxs.begin(), globalMaxs.end()));
        }
        // add the new input item in the priority queue
        queue.push(wr);
        // check if buffered tuples can be emitted in order
        while (!queue.empty()) {
            // emit all the buffered tuples with identifier/timestamp lower or equal than min_i
            input_t *wnext = queue.top();
            tuple_t *next = extractTuple<tuple_t, input_t>(wnext);
            uint64_t id = (mode == ordering_mode_t::ID) ? std::get<1>(next->getControlFields()) : std::get<2>(next->getControlFields());
            if (id > min_id)
                break;
            else {
                // deque the tuple
                queue.pop();
                // emit the tuple
                if (mode == ordering_mode_t::TS_RENUMBERING) { // check if renumbering is required
                    tuple_t *copy = new tuple_t(*next); // copy of the tuple
                    deleteTuple<tuple_t, input_t>(wnext);
                    auto tmp_key = std::get<0>(copy->getControlFields());
                    auto tmp_it = keyMap.find(tmp_key);
                    Key_Descriptor &tmp_key_d = (*tmp_it).second;
                    copy->setControlFields(tmp_key, tmp_key_d.emit_counter++, std::get<2>(copy->getControlFields()));
                    auto *copy_wt = createWrapper<tuple_t, input_t, wrapper_tuple_t<tuple_t>>(copy, 1);
                    this->ff_send_out(copy_wt);
                }
                else {
                    this->ff_send_out(wnext);
                }
            }
        }
        return this->GO_ON;
    }

    // method to manage the EOS (utilized by the FastFlow runtime)
    void eosnotify(ssize_t id) override
    {
        eos_received++;
        // check the number of received EOS messages
        if (eos_received != this->get_num_inchannels()) {
            return;
        }
        if (mode != ordering_mode_t::ID) {
            while (!globalQueue.empty()) {
                // extract the next tuple
                input_t *wnext = globalQueue.top();
                tuple_t *next = extractTuple<tuple_t, input_t>(wnext);
                globalQueue.pop();
                // emit the tuple
                if (mode == ordering_mode_t::TS_RENUMBERING) { // check if renumbering is required
                    tuple_t *copy = new tuple_t(*next); // copy of the tuple
                    deleteTuple<tuple_t, input_t>(wnext);
                    auto key = std::get<0>(copy->getControlFields());
                    auto it = keyMap.find(key);
                    Key_Descriptor &key_d = (*it).second;
                    copy->setControlFields(key, key_d.emit_counter++, std::get<2>(copy->getControlFields()));
                    auto *copy_wt = createWrapper<tuple_t, input_t, wrapper_tuple_t<tuple_t>>(copy, 1);
                    this->ff_send_out(copy_wt);
                }
                else {
                    this->ff_send_out(wnext);
                }
            }
            for (auto &k: keyMap) {
                auto key = k.first;
                auto &key_d = (k.second);
                // send the most recent EOS marker of this key (if it exists)
                if(key_d.eos_marker != nullptr) {
                    if (mode == ordering_mode_t::TS_RENUMBERING) { // check if renumbering is required
                        tuple_t *next = extractTuple<tuple_t, input_t>(key_d.eos_marker);
                        tuple_t *copy = new tuple_t(*next); // copy of the tuple
                        deleteTuple<tuple_t, input_t>(key_d.eos_marker);
                        copy->setControlFields(key, key_d.emit_counter++, std::get<2>(copy->getControlFields()));
                        auto *copy_wt = createWrapper<tuple_t, input_t, wrapper_tuple_t<tuple_t>>(copy, 1, true);
                        this->ff_send_out(copy_wt);
                    }
                    else {
                        this->ff_send_out(key_d.eos_marker);
                    }
                }
            }
        }
        else {
            // send (in order) all the queued tuples of all the keys
            for (auto &k: keyMap) {
                auto key = k.first;
                auto &key_d = (k.second);
                while (!(key_d.queue).empty()) {
                    // extract the next tuple
                    input_t *wnext = (key_d.queue).top();
                    tuple_t *next = extractTuple<tuple_t, input_t>(wnext);
                    (key_d.queue).pop();
                    // emit the tuple
                    if (mode == ordering_mode_t::TS_RENUMBERING) { // check if renumbering is required
                        tuple_t *copy = new tuple_t(*next); // copy of the tuple
                        deleteTuple<tuple_t, input_t>(wnext);
                        copy->setControlFields(key, key_d.emit_counter++, std::get<2>(copy->getControlFields()));
                        auto *copy_wt = createWrapper<tuple_t, input_t, wrapper_tuple_t<tuple_t>>(copy, 1);
                        this->ff_send_out(copy_wt);
                    }
                    else {
                        this->ff_send_out(wnext);
                    }
                }
                // send the most recent EOS marker of this key (if it exists)
                if(key_d.eos_marker != nullptr) {
                    if (mode == ordering_mode_t::TS_RENUMBERING) { // check if renumbering is required
                        tuple_t *next = extractTuple<tuple_t, input_t>(key_d.eos_marker);
                        tuple_t *copy = new tuple_t(*next); // copy of the tuple
                        deleteTuple<tuple_t, input_t>(key_d.eos_marker);
                        copy->setControlFields(key, key_d.emit_counter++, std::get<2>(copy->getControlFields()));
                        auto *copy_wt = createWrapper<tuple_t, input_t, wrapper_tuple_t<tuple_t>>(copy, 1, true);
                        this->ff_send_out(copy_wt);
                    }
                    else {
                        this->ff_send_out(key_d.eos_marker);
                    }
                }
            }
        }
    }

    // svc_end method (utilized by the FastFlow runtime)
    void svc_end() override {}
};

} // namespace wf

#endif
