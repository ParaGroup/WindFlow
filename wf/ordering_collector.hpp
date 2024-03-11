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
 *    * MIT License: https://github.com/ParaGroup/WindFlow/blob/master/LICENSE.MIT
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
 *  @file    ordering_collector.hpp
 *  @author  Gabriele Mencagli
 *  
 *  @brief Collector used for reordering inputs received from multiple streams
 *  
 *  @section Ordering_Collector (Description)
 *  
 *  This class implements a FastFlow multi-input node able to sort inputs based on their
 *  identifier (ID) or their timestamps (TS).
 */ 

#ifndef ORDERING_COLLECTOR_H
#define ORDERING_COLLECTOR_H

// includes
#include<deque>
#include<queue>
#include<unordered_map>
#include<ff/multinode.hpp>
#include<basic.hpp>
#include<single_t.hpp>

namespace wf {

// class Ordering_Collector
template<typename keyextr_func_t>
class Ordering_Collector: public ff::ff_minode
{
private:
    template<typename tuple_t> class Comparator_t; // forward declaration of the inner struct Comparator_t
    keyextr_func_t key_extr; // key extractor
    using tuple_t = decltype(get_tuple_t_KeyExtr(key_extr)); // extracting the tuple_t type and checking the admissible singatures
    using key_t = decltype(get_key_t_KeyExtr(key_extr)); // extracting the key_t type and checking the admissible singatures

    struct Key_Descriptor // struct of the key descriptor
    {
        std::vector<uint64_t> maxs; // maximum identifiers/timestamps for each input stream (key basis)
        std::priority_queue<Single_t<tuple_t> *, std::deque<Single_t<tuple_t> *>, Comparator_t<tuple_t>> queue; // ordered queue local of the key
        size_t id_collector; // identifier of the Ordering_Collector

        // Constructor
        Key_Descriptor(size_t _num_inputs,
                       ordering_mode_t _ordering_mode):
                       maxs(_num_inputs, 0),
                       queue(Comparator_t<tuple_t>(_ordering_mode)) {}
    };

    std::unordered_map<key_t, Key_Descriptor> keyMap; // hash table mapping keys onto key descriptors
    Execution_Mode_t execution_mode; // execution mode of the PipeGraph
    ordering_mode_t ordering_mode; // ordering mode used by the Ordering_Collector
    size_t id_collector; // identifier of the Ordering_Collector
    std::priority_queue<Single_t<tuple_t> *, std::deque<Single_t<tuple_t> *>, Comparator_t<tuple_t>> globalQueue; // ordered queue (global for all keys)
    std::vector<bool> enabled; // enable[i] is true if channel i is enabled
    std::vector<uint64_t> globalMaxs; // maximum identifiers/timestamps for each input stream (global for all keys)
    size_t eos_received; // number of received EOS messages

    // Comparator_t functor (it returns true if A comes after B in the ordering)
    template<typename tuple_t>
    struct Comparator_t
    {
        ordering_mode_t ordering_mode; // ordering mode

        // Constructor
        Comparator_t(ordering_mode_t _ordering_mode):
                     ordering_mode(_ordering_mode) {}

        // operator()
        bool operator() (const Single_t<tuple_t> *A, const Single_t<tuple_t> *B)
        {
            uint64_t value_A = (ordering_mode == ordering_mode_t::ID) ? A->getIdentifier() : A->getTimestamp();
            uint64_t value_B = (ordering_mode == ordering_mode_t::ID) ? B->getIdentifier() : B->getTimestamp();
            if (value_A > value_B) {
                return true;
            }
            else if (value_A < value_B) {
                return false;
            }
            else {
                assert(A != B);
                return (A > B); // compare the memory pointers to have a unique ordering
            }
        }
    };

    // Get the minimum identifier/timestamp among the enabled channels
    uint64_t getMinimum(std::vector<uint64_t> &maxs)
    {
        uint64_t min;
        bool first = true;
        for (size_t i=0; i<this->get_num_inchannels(); i++) {
            if (enabled[i] && first) {
                min = maxs[i];
                first = false;
            }
            else if ((enabled[i]) && (maxs[i] < min)) {
                min = maxs[i];
            }
        }
        assert(first == false);
        return min;
    }

public:
    // Constructor
    Ordering_Collector(keyextr_func_t _key_extr,
                       ordering_mode_t _ordering_mode,
                       Execution_Mode_t _execution_mode,
                       size_t _id_collector):
                       key_extr(_key_extr),
                       ordering_mode(_ordering_mode),
                       execution_mode(_execution_mode),
                       id_collector(_id_collector),
                       globalQueue(Comparator_t<tuple_t>(_ordering_mode)),
                       eos_received(0)
    {
        assert(execution_mode != Execution_Mode_t::PROBABILISTIC);
        assert(execution_mode != Execution_Mode_t::DEFAULT || ordering_mode == ordering_mode_t::ID);
    }

    // svc_init method (utilized by the FastFlow runtime)
    int svc_init() override
    {
        globalMaxs.clear();
        enabled.clear();
        for (size_t i=0; i<this->get_num_inchannels(); i++) {
            globalMaxs.push_back(0);
            enabled.push_back(true);
        }
        return 0;
    }

    // svc method (utilized by the FastFlow runtime)
    void *svc(void *_in) override
    {
        Single_t<tuple_t> *input = reinterpret_cast<Single_t<tuple_t> *>(_in); // cast the input to a Single_t structure
        size_t source_id = this->get_channel_id(); // get the index of the source's stream
        if (ordering_mode == ordering_mode_t::ID) { // ordering based on identifiers
            auto key = key_extr(input->tuple); // extract the key
            auto it = keyMap.find(key); // find the corresponding key descriptor
            if (it == keyMap.end()) {
                auto p = keyMap.insert(std::make_pair(key, Key_Descriptor(this->get_num_inchannels(), ordering_mode))); // create the descriptor of that key
                it = p.first;
            }
            Key_Descriptor &key_d = (*it).second;   
            if (execution_mode != Execution_Mode_t::DEFAULT) {
                assert(globalMaxs[source_id] <= input->getTimestamp()); // sanity check
                globalMaxs[source_id] = input->getTimestamp();
            }
            else {
                assert(globalMaxs[source_id] <= input->getWatermark(id_collector)); // sanity check
                globalMaxs[source_id] = input->getWatermark(id_collector);
                if (input->isPunct()) { // special case -> the input is a punctuation, it must be propagated with the right watermark
                    input->setWatermark(getMinimum(globalMaxs), id_collector);
                    this->ff_send_out(input); // emit the punctuation
                    return this->GO_ON;
                }
            }
            uint64_t id = input->getIdentifier(); // get the private identifier       
            assert(key_d.maxs[source_id] <= id); // sanity check
            key_d.maxs[source_id] = id;
            uint64_t min_id = 0;
            min_id = getMinimum(key_d.maxs);
            (key_d.queue).push(input); // add the new input in the priority queue of the key
            while (!(key_d.queue).empty()) { // check if buffered inputs of the key can be emitted in order
                Single_t<tuple_t> *next = (key_d.queue).top(); // read the next input in the queue of the key
                if (next->getIdentifier() > min_id) {
                    break;
                }
                else {
                    (key_d.queue).pop(); // extract the next input from the queue of the key
                    next->setWatermark(getMinimum(globalMaxs), id_collector);
                    this->ff_send_out(next); // emit the next Single_t
                }
            }
        }
        else { // ordering based on timestamps
            uint64_t ts = input->getTimestamp();
            assert(globalMaxs[source_id] <= ts); // sanity check
            globalMaxs[source_id] = ts;
            uint64_t min_ts = 0;
            min_ts = getMinimum(globalMaxs);
            globalQueue.push(input); // add the new input in the global priority queue
            while (!globalQueue.empty()) { // check if buffered inputs can be emitted in order
                Single_t<tuple_t> *next = globalQueue.top(); // read the next input in the global queue
                if (next->getTimestamp() > min_ts) {
                    break;
                }
                else {
                    globalQueue.pop(); // extract the next input from the global queue
                    this->ff_send_out(next);
                }
            }
        }
        return this->GO_ON;
    }

    // method to manage the EOS (utilized by the FastFlow runtime)
    void eosnotify(ssize_t id) override
    {
        assert(id < this->get_num_inchannels());
        eos_received++;
        if (eos_received != this->get_num_inchannels()) { // check the number of received EOS messages
            enabled[id] = false; // disable the channel where we received the EOS
            return;
        }
        if (ordering_mode != ordering_mode_t::ID) { // ordering based on timestamps
            while (!globalQueue.empty()) {
                Single_t<tuple_t> *next = globalQueue.top(); // read the next input in the global queue
                globalQueue.pop(); // extract the next input from the global queue
                this->ff_send_out(next);
            }
        }
        else { // ordering based on identifiers
            for (auto &k: keyMap) {
                auto &key_d = (k.second);
                while (!(key_d.queue).empty()) {
                    Single_t<tuple_t> *next = (key_d.queue).top(); // read the next input in the queue of the key
                    (key_d.queue).pop(); // extract the next input from the queue of the key
                    next->setWatermark(getMinimum(globalMaxs), id_collector);
                    this->ff_send_out(next);               
                }
            }
        }
    }

    // svc_end method (utilized by the FastFlow runtime)
    void svc_end() override
    {
        assert(globalQueue.size() == 0); // check that the global queue is empty
        for (auto &k: keyMap) { // check that the all the key queues are empty
            auto &key_d = (k.second);
            assert((key_d.queue).size() == 0);
        }
    }
};

} // namespace wf

#endif
