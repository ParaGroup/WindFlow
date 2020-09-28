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
 *  @file    kslack_node.hpp
 *  @author  Gabriele Mencagli
 *  @date    13/07/2020
 *  
 *  @brief Node used for reordering data items in a probabilistic manner
 *  
 *  @section KSlack_Node (Description)
 *  
 *  The node has multiple input streams. It receives data items that are buffered
 *  into an internal queue. Data items are dequeued from the queue and emitted in
 *  increasing ordering of timestamp. To enforce this ordering, the node can drop
 *  inputs.
 */ 

#ifndef KSLACK_NODE_H
#define KSLACK_NODE_H

// includes
#include<deque>
#include<vector>
#include<unordered_map>
#include<ff/multinode.hpp>
#include<meta.hpp>
#include<basic.hpp>

namespace wf {

// class KSlack_Node
template<typename tuple_t, typename input_t=tuple_t>
class KSlack_Node: public ff::ff_minode_t<input_t, input_t>
{
private:
    tuple_t tmp; // never used
    // key data type
    using key_t = typename std::remove_reference<decltype(std::get<0>(tmp.getControlFields()))>::type;
    uint64_t K = 0; // K parameter of the slack (of the same time unit of the timestamps)
    uint64_t tcurr = 0; // highest application timestamp of the inputs seen so far
    std::deque<input_t *> bufferedInputs; // buffer of inputs waiting to be emitted
    std::vector<uint64_t> ts_vect; // vector of the last timestamps
    bool toEmit = false; // true if some inputs must be emitted, false otherwise
    typename std::deque<input_t *>::iterator first; // iterator to the first input to emit
    typename std::deque<input_t *>::iterator last; // iterator to the last input to emit
    // comparator functor (returns true if A comes before B in the ordering)
    struct Comparator {

        // operator()
        bool operator() (input_t *wA, input_t *wB) {
            tuple_t *A = extractTuple<tuple_t, input_t>(wA);
            tuple_t *B = extractTuple<tuple_t, input_t>(wB);
            uint64_t ts_A = std::get<2>(A->getControlFields());
            uint64_t ts_B = std::get<2>(B->getControlFields());
            if (ts_A < ts_B) {
                return true;
            }
            else if (ts_A > ts_B) {
                return false;
            }
            else {
                assert(A != B);
                return (A < B); // compare the memory pointers to have a unique ordering!!!
            }
        }
    };
    Comparator comparator;
    long dropped_sample = 0; // number of dropped inputs during the last sample
    long dropped_inputs = 0; // number of dropped inputs during the whole processing
    long received_inputs = 0; // number of received inputs during the whole processing
    uint64_t last_timestamp = 0; // timestamp of the last input emitted by this node
    size_t eos_received; // number of received EOS messages
    ordering_mode_t mode; // ordering mode supported by the KSlack_Node (TS or TS_RENUMBERING)
    std::atomic<unsigned long> *atomic_num_dropped; // pointer to the atomic counter with the total number of dropped tuples
    std::unordered_map<key_t, long> keyMap; // hash table to map keys onto progressive counters
    volatile long last_update_atomic_usec; // time of the last update of the atomic counter

    // method to insert a new input into the buffer
    bool insertInput(input_t *wt)
    {
        // extract the tuple from the input
        tuple_t *t = extractTuple<tuple_t, input_t>(wt);
        // insert the timestamp of the tuple in the vector
        auto ts = std::get<2>(t->getControlFields());
        ts_vect.push_back(ts);
        // insertion of the input in the buffer
        auto it = std::lower_bound(bufferedInputs.begin(), bufferedInputs.end(), wt, comparator);
        // add the input in the proper position of the buffer
        if (it == bufferedInputs.end()) {
            bufferedInputs.push_back(wt);
        }
        else {
            bufferedInputs.insert(it, wt);
        }
        // check if we can emit some buffered inputs or not
        if (ts <= tcurr) {
            return false;
        }
        else {
            tcurr = ts; // update tcurr
            // find the maximum delay
            uint64_t max_d = 0;
            for (auto const &ts_i: ts_vect) {
                assert(tcurr >= ts_i);
                uint64_t diff = tcurr - ts_i;
                if (max_d < diff) {
                    max_d = diff;
                }
            }
            if (max_d > K) {
                K = max_d; // update K;
            }
            // determine the iterator to the last input to emit
            tuple_t *tmp = new tuple_t(*t);
            tmp->setControlFields(std::get<0>(t->getControlFields()), std::get<1>(t->getControlFields()), tcurr - K);
            input_t *tmp_wt = createWrapper<tuple_t, input_t, wrapper_tuple_t<tuple_t>>(tmp, 1);
            ts_vect.clear(); // empty the vector of timestamps
            first = bufferedInputs.begin();
            last = std::lower_bound(bufferedInputs.begin(), bufferedInputs.end(), tmp_wt, comparator);
            // delete the temporary values created
            deleteTuple<tuple_t, input_t>(tmp_wt);
            toEmit = true;
            return true;
        }
    }

    // method to get the next input from the buffer
    input_t *extractInput()
    {
        if (!toEmit) {
            return nullptr;
        }
        else if (first == last) {
            // remove the emitted inputs from the buffer
            bufferedInputs.erase(bufferedInputs.begin(), last);
            toEmit = false;
            return nullptr;
        }
        else {
            input_t *wt = *first;
            first++;
            return wt;
        }
    }

    // method to prepare the flush of the buffer
    void prepareToFlush() {
        toEmit = true;
        first = bufferedInputs.begin();
        last = bufferedInputs.end();
    }

public:
    // Constructor
    KSlack_Node(ordering_mode_t _mode=ordering_mode_t::TS, std::atomic<unsigned long> *_atomic_num_dropped=nullptr):
                eos_received(0),
                mode(_mode),
                atomic_num_dropped(_atomic_num_dropped)
    {
        assert(mode != ordering_mode_t::ID);
        last_update_atomic_usec = current_time_usecs();
    }

    // svc_init method (utilized by the FastFlow runtime)
    int svc_init() override {
        return 0;
    }

    // svc method (utilized by the FastFlow runtime)
    input_t *svc(input_t *wt) override
    {
        // add the input to the buffer
        this->insertInput(wt);
        received_inputs++;
        // extract tuples from the buffer (likely in order)
        input_t *input = this->extractInput();
        while (input != nullptr) {
            tuple_t *t = extractTuple<tuple_t, input_t>(input);
            // if the input is not emitted in order we drop it
            if (std::get<2>(t->getControlFields()) < last_timestamp) {
                dropped_inputs++;
                dropped_sample++;
                updateAtomicDroppedCounter();
                // delete the input to be dropped
                deleteTuple<tuple_t, input_t>(input);
            }
            // otherwise, we can send the input
            else {
                last_timestamp = std::get<2>(t->getControlFields());
                if (mode == ordering_mode_t::TS_RENUMBERING) {
                    auto key = std::get<0>(t->getControlFields()); // key
                    // initialize the corresponding counter
                    auto it = keyMap.find(key);
                    if (it == keyMap.end()) {
                        // create the descriptor of that key
                        keyMap.insert(std::make_pair(key, 0));
                        it = keyMap.find(key);
                    }
                    auto &counter = (*it).second;
                    // create the copy of the input
                    tuple_t *copy = new tuple_t(*t);
                    deleteTuple<tuple_t, input_t>(input);
                    copy->setControlFields(key, counter++, std::get<2>(copy->getControlFields()));
                    auto *copy_wt = createWrapper<tuple_t, input_t, wrapper_tuple_t<tuple_t>>(copy, 1);
                    this->ff_send_out(copy_wt);
                }
                else {
                    this->ff_send_out(input);
                }
            }
            input = this->extractInput();
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
        else {
            // prepare the buffer to be flushed
            this->prepareToFlush();
            input_t *input = this->extractInput();
            while (input != nullptr) {
                tuple_t *t = extractTuple<tuple_t, input_t>(input);
                // if the input is not emitted in order we discard it
                if (std::get<2>(t->getControlFields()) < last_timestamp) {
                    dropped_inputs++;
                    dropped_sample++;
                    updateAtomicDroppedCounter();
                    // delete the input to be dropped
                    deleteTuple<tuple_t, input_t>(input);
                }
                else {
                    last_timestamp = std::get<2>(t->getControlFields());
                    if (mode == ordering_mode_t::TS_RENUMBERING) {
                        auto key = std::get<0>(t->getControlFields()); // key
                        // initialize the corresponding counter
                        auto it = keyMap.find(key);
                        if (it == keyMap.end()) {
                            // create the descriptor of that key
                            keyMap.insert(std::make_pair(key, 0));
                            it = keyMap.find(key);
                        }
                        auto &counter = (*it).second;
                        // create the copy of the input
                        tuple_t *copy = new tuple_t(*t);
                        deleteTuple<tuple_t, input_t>(input);
                        copy->setControlFields(key, counter++, std::get<2>(copy->getControlFields()));
                        auto *copy_wt = createWrapper<tuple_t, input_t, wrapper_tuple_t<tuple_t>>(copy, 1);
                        this->ff_send_out(copy_wt);
                    }
                    else {
                        this->ff_send_out(input);
                    }
                }
                input = this->extractInput();
            }
        }
    }

    // svc_end method (utilized by the FastFlow runtime)
    void svc_end() override
    {
        // update the number of dropped tuples by the whole operator
        (*atomic_num_dropped) += dropped_sample;
    }

    // method to check whether the atomic counter must be updated
    void updateAtomicDroppedCounter()
    {
        // if we have to update the atomic counter
        if (current_time_usecs() - last_update_atomic_usec >= DEFAULT_UPDATE_INTERVAL_USEC) {
            (*atomic_num_dropped) += dropped_sample;
            last_update_atomic_usec = current_time_usecs();
            dropped_sample = 0;
        }
    }
};

} // namespace wf

#endif
