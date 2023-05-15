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
 *  @file    forward_emitter.hpp
 *  @author  Gabriele Mencagli
 *  
 *  @brief Emitter implementing the forward (FW) distribution
 *  
 *  @section Forward_Emitter (Description)
 *  
 *  The emitter delivers each received tuple to one of its destinations chosen
 *  randomly. The emitter can be configured to work without batching (using
 *  Single_t structures) or in batched mode (using Batch_CPU_t structures).
 */ 

#ifndef FW_EMITTER_H
#define FW_EMITTER_H

// includes
#include<single_t.hpp>
#include<batch_cpu_t.hpp>
#include<basic_emitter.hpp>

namespace wf {

// class Forward_Emitter
template<typename keyextr_func_t>
class Forward_Emitter: public Basic_Emitter
{
private:
    keyextr_func_t key_extr; // functional logic to extract the key attribute from the tuple_t
    using tuple_t = decltype(get_tuple_t_KeyExtr(key_extr)); // extracting the tuple_t type and checking the admissible signatures
    size_t num_dests; // number of destinations connected in output to the emitter
    size_t size; // if >0 the emitter works in batched more, otherwise in a per-tuple basis
    size_t idx_dest; // identifier of the next destination to be used (meaningful if useTreeMode is true)
    bool useTreeMode; // true if the emitter is used in tree-based mode
    std::vector<std::pair<void *, size_t>> output_queue; // vector of pairs (messages and destination identifiers)
    Batch_CPU_t<tuple_t> *batch_output; // pointer to the output batch (meaningful if size > 0)

public:
    // Constructor
    Forward_Emitter(keyextr_func_t _key_extr,
                    size_t _num_dests,
                    size_t _size=0):
                    key_extr(_key_extr),
                    num_dests(_num_dests),
                    size(_size),
                    idx_dest(0),
                    useTreeMode(false),
                    batch_output(nullptr) {}

    // Copy Constructor
    Forward_Emitter(const Forward_Emitter &_other):
                    Basic_Emitter(_other),
                    key_extr(_other.key_extr),
                    num_dests(_other.num_dests),
                    size(_other.size),
                    idx_dest(_other.idx_dest),
                    useTreeMode(_other.useTreeMode),
                    batch_output(nullptr) {}

    // Destructor
    ~Forward_Emitter() override
    {
        assert(output_queue.size() == 0); // sanity check
        assert(batch_output == nullptr); // sanity check
        if (size == 0) { // delete all the Single_t items in the recycling queue
            Single_t<tuple_t> *msg = nullptr;
            while ((this->queue)->pop((void **) &msg)) {
                delete msg;
            }
        }
        else { // delete all the batches in the recycling queue
            Batch_t<tuple_t> *batch = nullptr;
            while ((this->queue)->pop((void **) &batch)) {
                delete batch;
            }
        }
    }

    // Create a clone of the emitter
    Basic_Emitter *clone() const override
    {
        Forward_Emitter<keyextr_func_t> *copy = new Forward_Emitter<keyextr_func_t>(*this);
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
        tuple_t *tuple = reinterpret_cast<tuple_t *>(_out);
        if (size == 0) { // no batching
            Single_t<tuple_t> *output = allocateSingle_t(std::move(*tuple), _identifier, _timestamp, _watermark, this->queue);
            routing(output, _node);
        }
        else { // batching
            routing_batched(*tuple, _timestamp, _watermark, _node);
        }
    }

    // Emit method (in-place version)
    void emit_inplace(void *_out, ff::ff_monode *_node) override
    {
        Single_t<tuple_t> *output = reinterpret_cast<Single_t<tuple_t> *>(_out);
        if (size == 0) { // no batching
            routing(output, _node);
        }
        else { // batching
            routing_batched(output->tuple, output->getTimestamp(), output->getWatermark(), _node);
            deleteSingle_t(output); // delete the input Single_t
        }
    }

    // Routing method
    void routing(Single_t<tuple_t> *_output, ff::ff_monode *_node)
    {
        if (!useTreeMode) { // real send
            _node->ff_send_out(_output);
        }
        else { // output is buffered
            output_queue.push_back(std::make_pair(_output, idx_dest));
            idx_dest = (idx_dest + 1) % num_dests;
        }
    }

    // Routing method to be used in batched mode
    void routing_batched(tuple_t &_tuple,
                         uint64_t _timestamp,
                         uint64_t _watermark,
                         ff::ff_monode *_node)
    {
        if (batch_output == nullptr) {
            batch_output = allocateBatch_CPU_t<tuple_t>(size, this->queue);
        }
        batch_output->addTuple(std::move(_tuple), _timestamp, _watermark);
        if (batch_output->getSize() == size) { // batch is ready to be sent
            if (!useTreeMode) { // real send
                _node->ff_send_out(batch_output);
            }
            else { // output is buffered
                output_queue.push_back(std::make_pair(batch_output, idx_dest));
                idx_dest = (idx_dest + 1) % num_dests;
            }
            batch_output = nullptr;
        }
    }

    // Punctuation propagation method
    void propagate_punctuation(uint64_t _watermark, ff::ff_monode * _node) override
    {
        flush(_node); // flush the internal partially filled batch (if any)
        if (size == 0) { // no batching
            tuple_t t; // create an empty tuple (default constructor needed!)
            Single_t<tuple_t> *punc = allocateSingle_t(std::move(t), 0, 0, _watermark, this->queue);
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
            tuple_t t; // create an empty tuple (default constructor needed!)
            Batch_CPU_t<tuple_t> *punc = allocateBatch_CPU_t<tuple_t>(size, this->queue);
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
                if (!useTreeMode) { // real send
                    _node->ff_send_out(batch_output);
                }
                else { // output is buffered
                    output_queue.push_back(std::make_pair(batch_output, idx_dest));
                    idx_dest = (idx_dest + 1) % num_dests;
                }
                batch_output = nullptr;
            }
        }
    }

    Forward_Emitter(Forward_Emitter &&) = delete; ///< Move constructor is deleted
    Forward_Emitter &operator=(const Forward_Emitter &) = delete; ///< Copy assignment operator is deleted
    Forward_Emitter &operator=(Forward_Emitter &&) = delete; ///< Move assignment operator is deleted
};

} // namespace wf

#endif
