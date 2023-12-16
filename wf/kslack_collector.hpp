/**************************************************************************************
 *  Copyright (c) 2019- Gabriele Mencagli
 *  
 *  This file is part of WindFlow.
 *  
 *  WindFlow is free software dual licensed under the GNU LGPL or MIT License.
 *  You can redistribute it and/or modify it under the terms of the
 *    * GNU Lesser General Public License as published by
 *      the Free Software Foundation, either version 3 of the License, or
 *      (at your option) any later version
 *    OR
 *    * MIT License: https://github.com/ParaGroup/WindFlow/blob/vers3.x/LICENSE.MIT
 *  
 *  WindFlow is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Lesser General Public License for more details.
 *  You should have received a copy of the GNU Lesser General Public License and
 *  the MIT License along with WindFlow. If not, see <http://www.gnu.org/licenses/>
 *  and <http://opensource.org/licenses/MIT/>.
 **************************************************************************************
 */

/** 
 *  @file    kslack_collector.hpp
 *  @author  Gabriele Mencagli
 *  
 *  @brief Collector used for reordering inputs in a probabilistic manner
 *  
 *  @section KSlack_Collector (Description)
 *  
 *  This class implements a FastFlow multi-input node able to sort inputs based on their
 *  timestamps (TS). This is done in a probabilistic manner by taking into account the
 *  highest delay of the inputs seen so far.
 */ 

#ifndef KSLACK_COLLECTOR_H
#define KSLACK_COLLECTOR_H

// includes
#include<deque>
#include<vector>
#include<unordered_map>
#include<ff/multinode.hpp>
#include<basic.hpp>
#include<single_t.hpp>

namespace wf {

// class KSlack_Collector
template<typename keyextr_func_t>
class KSlack_Collector: public ff::ff_minode
{
private:
    template<typename T> class Comparator_t; // forward declaration of the inner struct Comparator_t
    keyextr_func_t key_extr; // key extractor
    using tuple_t = decltype(get_tuple_t_KeyExtr(key_extr)); // extracting the tuple_t type and checking the admissible singatures
    uint64_t K = 0; // K parameter of the slack
    uint64_t tcurr = 0; // highest timestamp of the inputs seen so far
    std::deque<Single_t<tuple_t> *> bufferedInputs; // buffer of inputs waiting to be emitted
    std::vector<uint64_t> ts_vect; // vector of the last timestamps
    bool toEmit = false; // true if some inputs must be emitted, false otherwise
    typename std::deque<Single_t<tuple_t> *>::iterator next_input_it; // iterator to the next input to emit
    Comparator_t<tuple_t> comparator; // comparator functor
    unsigned long dropped_sample = 0; // number of dropped inputs during the last sample
    unsigned long dropped_inputs = 0; // number of dropped inputs during the whole processing
    unsigned long received_inputs = 0; // number of received inputs during the whole processing
    uint64_t last_timestamp = 0; // timestamp of the last input emitted by the collector
    ordering_mode_t ordering_mode; // ordering mode used by the KSlack_Collector
    size_t id_collector; // identifier of the KSlack_Collector
    std::atomic<unsigned long> *atomic_num_dropped; // pointer to the atomic counter with the total number of dropped tuples
    volatile long last_update_atomic_usec; // time of the last update of the atomic counter
    size_t eos_received; // number of received EOS messages
    size_t separator_id; // streams separator meaningful to join operators

    // Comparator_t functor (it returns true if A comes before B in the ordering)
    template<typename tuple_t>
    struct Comparator_t
    {
        // operator()
        bool operator() (const Single_t<tuple_t> *A, const Single_t<tuple_t> *B)
        {
            if (A->getTimestamp() < B->getTimestamp()) {
                return true;
            }
            else if (A->getTimestamp() > B->getTimestamp()) {
                return false;
            }
            else {
                assert(A != B);
                return (A < B); // compare the memory pointers to have a unique ordering
            }
        }
    };

    // Insert a new input into the buffer
    bool insertInput(Single_t<tuple_t> *_input)
    {
        ts_vect.push_back(_input->getTimestamp()); // add the timestamp of the input to the vector
        auto it = std::lower_bound(bufferedInputs.begin(), bufferedInputs.end(), _input, comparator); // insertion of the input in the buffer
        if (it == bufferedInputs.end()) {
            bufferedInputs.push_back(_input);
        }
        else {
            bufferedInputs.insert(it, _input);
        }
        if (_input->getTimestamp() <= tcurr) { // check if we can emit some buffered inputs or not
            return false;
        }
        else {
            tcurr = _input->getTimestamp(); // update tcurr
            uint64_t max_d = 0; // find the maximum delay
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
            next_input_it = bufferedInputs.begin(); // first points to the first input in the buffer
            ts_vect.clear(); // empty the vector of timestamps
            toEmit = true;
            return true;
        }
    }

    // Get the next input from the buffer
    Single_t<tuple_t> *extractInput()
    {
        if (!toEmit) {
            return nullptr;
        }
        else if (next_input_it == bufferedInputs.end()) {
            bufferedInputs.erase(bufferedInputs.begin(), next_input_it);
            toEmit = false;
            return nullptr;
        }
        else {
            auto *next = *next_input_it;
            if (next->getTimestamp() <= tcurr-K) {
                next_input_it++;
                return next;
            }
            else {
                bufferedInputs.erase(bufferedInputs.begin(), next_input_it);
                toEmit = false;
                return nullptr;
            }
        }
    }

    // Check whether the atomic counter must be updated
    void updateAtomicDroppedCounter()
    {
        if (current_time_usecs() - last_update_atomic_usec >= WF_DEFAULT_DROP_INTERVAL_USEC) { // if we have to update the atomic counter
            (*atomic_num_dropped) += dropped_sample;
            last_update_atomic_usec = current_time_usecs();
            dropped_sample = 0;
        }
    }

public:
    // Constructor
    KSlack_Collector(keyextr_func_t _key_extr,
                     ordering_mode_t _ordering_mode,
                     size_t _id_collector,
                     size_t _separator_id=0,
                     std::atomic<unsigned long> *_atomic_num_dropped=nullptr):
                     key_extr(_key_extr),
                     ordering_mode(_ordering_mode),
                     id_collector(_id_collector),
                     atomic_num_dropped(_atomic_num_dropped),
                     separator_id(_separator_id),
                     eos_received(0)
    {
        assert(_ordering_mode != ordering_mode_t::ID);
        last_update_atomic_usec = current_time_usecs();
    }

    // svc method (utilized by the FastFlow runtime)
    void *svc(void *_in) override
    {
        Single_t<tuple_t> *input = reinterpret_cast<Single_t<tuple_t> *>(_in); // cast the input to a Single_t structure
        this->insertInput(input);  // add the input to the buffer
        received_inputs++;
        auto *next = this->extractInput(); // extract inputs from the buffer (likely in order)
        while (next != nullptr) {
            if (next->getTimestamp() < last_timestamp) { // if the next input is not emitted in order, we drop it
                dropped_inputs++;
                dropped_sample++;
                updateAtomicDroppedCounter();
                deleteSingle_t(next);
            }
            else { // otherwise, we can send the next input
                last_timestamp = next->getTimestamp();
                if (separator_id != 0) {
                    size_t source_id = this->get_channel_id(); // get the index of the source's stream
                    next->setStreamTag(source_id < separator_id ? Join_Stream_t::A : Join_Stream_t::B);
                }
                this->ff_send_out(next);
            }
            next = this->extractInput();
        }
        return this->GO_ON;
    }

    // method to manage the EOS (utilized by the FastFlow runtime)
    void eosnotify(ssize_t id) override
    {
        eos_received++;
        if (eos_received != this->get_num_inchannels()) { // check the number of received EOS messages
            return;
        }
        else {
            for (auto *next: bufferedInputs) { // iterate across all the pending inputs buffered
                if (next->getTimestamp() < last_timestamp) { // if the next input is not emitted in order, we drop it
                    dropped_inputs++;
                    dropped_sample++;
                    updateAtomicDroppedCounter();
                    deleteSingle_t(next);
                }
                else { // otherwise, we can send the next input
                    last_timestamp = next->getTimestamp();
                    if (separator_id != 0) {
                        next->setStreamTag(id < separator_id ? Join_Stream_t::A : Join_Stream_t::B);
                    }
                    this->ff_send_out(next);
                }
            }
        }
    }

    // svc_end method (utilized by the FastFlow runtime)
    void svc_end() override
    {
        assert(bufferedInputs.size() == 0); // check that the buffer is empty
        (*atomic_num_dropped) += dropped_sample; // update the number of dropped tuples by the whole operator
    }
};

} // namespace wf

#endif
