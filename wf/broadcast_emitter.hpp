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
 *  @file    broadcast_emitter.hpp
 *  @author  Gabriele Mencagli
 *  
 *  @brief Emitter implementing the broadcast (BD) distribution
 *  
 *  @section Broadcast_Emitter (Description)
 *  
 *  The emitter delivers each received tuple to all its destination (without copies).
 *  The emitter can be configured to work without batching (using Single_t structures)
 *  or in batched mode (using Batch_CPU_t structures).
 */ 

#ifndef BD_EMITTER_H
#define BD_EMITTER_H

// includes
#include<single_t.hpp>
#include<batch_cpu_t.hpp>
#include<basic_emitter.hpp>

namespace wf {

// class Broadcast_Emitter
template<typename key_extractor_func_t>
class Broadcast_Emitter: public Basic_Emitter
{
private:
    key_extractor_func_t key_extr; // functional logic to extract the key attribute from the tuple_t
    using tuple_t = decltype(get_tuple_t_KeyExtr(key_extr)); // extracting the tuple_t type and checking the admissible signatures
    size_t num_dests; // number of destinations connected in output to the emitter
    size_t size; // if >0 the emitter works in batched more, otherwise in a per-tuple basis
    bool useTreeMode; // true if the emitter is used in tree-based mode
    std::vector<std::pair<void *, size_t>> output_queue; // vector of pairs (messages and destination identifiers)
    Batch_CPU_t<tuple_t> *batch_output; // pointer to the output batch (meaningful if size > 0)
    ff::MPMC_Ptr_Queue *queue; // pointer to the recyling queue

public:
    // Constructor
    Broadcast_Emitter(key_extractor_func_t _key_extr,
                      size_t _num_dests,
                      size_t _size=0):
                      key_extr(_key_extr),
                      num_dests(_num_dests),
                      size(_size),
                      useTreeMode(false),
                      batch_output(nullptr)
    {
        queue = new ff::MPMC_Ptr_Queue();
        queue->init(DEFAULT_BUFFER_CAPACITY);
    }

    // Copy Constructor
    Broadcast_Emitter(const Broadcast_Emitter &_other):
                      key_extr(_other.key_extr),
                      num_dests(_other.num_dests),
                      size(_other.size),
                      useTreeMode(_other.useTreeMode),
                      batch_output(nullptr)
    {
        queue = new ff::MPMC_Ptr_Queue();
        queue->init(DEFAULT_BUFFER_CAPACITY);
    }

    // Move Constructor
    Broadcast_Emitter(Broadcast_Emitter &&_other):
                      key_extr(std::move(_other.key_extr)),
                      num_dests(_other.num_dests),
                      size(_other.size),
                      useTreeMode(_other.useTreeMode),
                      output_queue(std::move(_other.output_queue)),
                      batch_output(std::exchange(_other.batch_output, nullptr)),
                      queue(std::exchange(_other.queue, nullptr)) {}

    // Destructor
    ~Broadcast_Emitter() override
    {
        assert(output_queue.size() == 0); // sanity check
        assert(batch_output == nullptr); // sanity check
        if (size == 0) { // delete all the Single_t items in the recycling queue
            if (queue != nullptr) {
                Single_t<decltype(get_tuple_t_KeyExtr(key_extr))> *del_single = nullptr;
                while (queue->pop((void **) &del_single)) {
                    delete del_single;
                }
                delete queue; // delete the recycling queue
            }
        }
        else { // delete all the batches in the recycling queue
            if (queue != nullptr) {
                Batch_t<decltype(get_tuple_t_KeyExtr(key_extr))> *del_batch = nullptr;
                while (queue->pop((void **) &del_batch)) {
                    delete del_batch;
                }
                delete queue; // delete the recycling queue
            }
        }
    }

    // Copy Assignment Operator
    Broadcast_Emitter &operator=(const Broadcast_Emitter &_other)
    {
        if (this != &_other) {
            key_extr = _other.key_extr;
            num_dests = _other.num_dests;
            size = _other.size;
            useTreeMode = _other.useTreeMode;
            if (batch_output != nullptr) {
                delete batch_output;
            }
            batch_output = nullptr;       
        }
        return *this;
    }

    // Move Assignment Operator
    Broadcast_Emitter &operator=(Broadcast_Emitter &_other)
    {
        key_extr = std::move(_other.key_extr);
        num_dests = _other.num_dests;
        size = _other.size;
        useTreeMode = _other.useTreeMode;
        output_queue = std::move(_other.output_queue);
        if (batch_output != nullptr) {
            delete batch_output;
        }
        batch_output = std::exchange(_other.batch_output, nullptr);
        if (size == 0) { // delete all the Single_t items in the recycling queue
            if (queue != nullptr) {
                Single_t<decltype(get_tuple_t_KeyExtr(key_extr))> *del_single = nullptr;
                while (queue->pop((void **) &del_single)) {
                    delete del_single;
                }
                delete queue; // delete the recycling queue
            }
        }
        else { // delete all the batches in the recycling queue
            if (queue != nullptr) {
                Batch_t<decltype(get_tuple_t_KeyExtr(key_extr))> *del_batch = nullptr;
                while (queue->pop((void **) &del_batch)) {
                    delete del_batch;
                }
                delete queue; // delete the recycling queue
            }
        }
        queue = std::exchange(_other.queue, nullptr);
        return *this;
    }

    // Create a clone of the emitter
    Basic_Emitter *clone() const override
    {
        Broadcast_Emitter<key_extractor_func_t> *copy = new Broadcast_Emitter<key_extractor_func_t>(*this);
        return copy;
    }

    // Get the number of destinations of the emitter
    size_t getNumDestinations() const override
    {
        return num_dests;
    }

    // Set the emitter to work in tree-based mode
    void setTreeMode(bool _useTreeMode) override
    {
        useTreeMode = _useTreeMode;
    }

    // Get a reference to the vector of output messages used by the emitter
    std::vector<std::pair<void *, size_t>> &getOutputQueue() override
    {
        return output_queue;
    }

    // Emit method (non in-place version)
    void emit(void *_out,
              uint64_t _identifier,
              uint64_t _timestamp,
              uint64_t _watermark,
              ff::ff_monode *_node) override
    {
        decltype(get_tuple_t_KeyExtr(key_extr)) *tuple = reinterpret_cast<decltype(get_tuple_t_KeyExtr(key_extr)) *>(_out);
        if (size == 0) { // no batching
            Single_t<decltype(get_tuple_t_KeyExtr(key_extr))> *output = allocateSingle_t(std::move(*tuple), _identifier, _timestamp, _watermark, queue);
            routing(output, _node);
        }
        else { // batching
            routing_batched(*tuple, _timestamp, _watermark, _node);
        }
    }

    // Emit method (in-place version)
    void emit_inplace(void *_out,
                      ff::ff_monode *_node) override
    {
        Single_t<decltype(get_tuple_t_KeyExtr(key_extr))> *output = reinterpret_cast<Single_t<decltype(get_tuple_t_KeyExtr(key_extr))> *>(_out);
        if (size == 0) { // no batching
            routing(output, _node);
        }
        else { // batching
            routing_batched(output->tuple, output->getTimestamp(), output->getWatermark(), _node);
            deleteSingle_t(output); // delete the input Single_t
        }
    }

    // Routing method
    void routing(Single_t<tuple_t> *_output,
                 ff::ff_monode *_node)
    {
        (_output->delete_counter).fetch_add(num_dests-1);
        assert((_output->fields).size() == 3); // sanity check
        (_output->fields).insert((_output->fields).end(), num_dests-1, (_output->fields)[2]); // copy the watermark (having one per destination)
        for (size_t i=0; i<num_dests; i++) {
            if (!useTreeMode) { // real send
                _node->ff_send_out_to(_output, i);
            }
            else { // output is buffered
               output_queue.push_back(std::make_pair(_output, i));
            }
        }
    }

    // Routing method to be used in batched mode
    void routing_batched(tuple_t &_tuple,
                         uint64_t _timestamp,
                         uint64_t _watermark,
                         ff::ff_monode *_node)
    {
        if (batch_output == nullptr) {
            batch_output = allocateBatch_CPU_t<decltype(get_tuple_t_KeyExtr(key_extr))>(size, queue);
        }
        batch_output->addTuple(std::move(_tuple), _timestamp, _watermark);
        if (batch_output->getSize() == size) { // batch is ready to be sent
            (batch_output->delete_counter).fetch_add(num_dests-1);
            assert((batch_output->watermarks).size() == 1); // sanity check
            (batch_output->watermarks).insert((batch_output->watermarks).end(), num_dests-1, (batch_output->watermarks)[0]); // copy the watermark (having one per destination)
            for (size_t i=0; i<num_dests; i++) {
                if (!useTreeMode) { // real send
                    _node->ff_send_out_to(batch_output, i);
                }
                else { // batch_output is buffered
                    output_queue.push_back(std::make_pair(batch_output, i));
                }
            }
            batch_output = nullptr;
        }
    }

    // Punctuation propagation method
    void propagate_punctuation(uint64_t _watermark,
                               ff::ff_monode * _node) override
    {
        flush(_node); // flush the internal partially filled batch (if any)
        if (size == 0) { // no batching
            tuple_t t; // create an empty tuple
            Single_t<decltype(get_tuple_t_KeyExtr(key_extr))> *punc = allocateSingle_t(std::move(t), 0, 0, _watermark, queue);
            (punc->delete_counter).fetch_add(num_dests-1);
            assert((punc->fields).size() == 3); // sanity check
            (punc->fields).insert((punc->fields).end(), num_dests-1, (punc->fields)[2]); // copy the watermark (having one per destination)
            punc->isPunctuation = true;
            for (size_t i=0; i<num_dests; i++) {
                if (!useTreeMode) { // real send
                    _node->ff_send_out_to(punc, i);
                }
                else { // punctuation is buffered
                    output_queue.push_back(std::make_pair(punc, i));
                }
            }
        }
        else { // batching
            tuple_t t; // create an empty tuple
            Batch_CPU_t<decltype(get_tuple_t_KeyExtr(key_extr))> *punc = allocateBatch_CPU_t<decltype(get_tuple_t_KeyExtr(key_extr))>(size, queue);
            punc->addTuple(std::move(t), 0, _watermark);
            (punc->delete_counter).fetch_add(num_dests-1);
            assert((punc->watermarks).size() == 1); // sanity check
            (punc->watermarks).insert((punc->watermarks).end(), num_dests-1, (punc->watermarks)[0]); // copy the watermark (having one per destination)
            punc->isPunctuation = true;
            for (size_t i=0; i<num_dests; i++) {
                if (!useTreeMode) { // real send
                    _node->ff_send_out_to(punc, i);
                }
                else { // punctuation is buffered
                    output_queue.push_back(std::make_pair(punc, i));
                }
            }
        }
    }

    // Flushing method
    void flush(ff::ff_monode *_node) override
    {
        if (size > 0) { // only batching
            if (batch_output != nullptr) { // send the partial batch if it exists
                assert(batch_output->getSize() > 0); // sanity check
                (batch_output->delete_counter).fetch_add(num_dests-1);
                (batch_output->watermarks).insert((batch_output->watermarks).end(), num_dests-1, (batch_output->watermarks)[0]); // copy the watermark (having one per destination)
                for (size_t i=0; i<num_dests; i++) {
                    if (!useTreeMode) { // real send
                        _node->ff_send_out_to(batch_output, i);
                    }
                    else { // output is buffered
                        output_queue.push_back(std::make_pair(batch_output, i));
                    }
                }
                batch_output = nullptr;
            }
        }
    }
};

} // namespace wf

#endif
