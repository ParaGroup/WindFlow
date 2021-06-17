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
 *  @file    broadcast_emitter_gpu.hpp
 *  @author  Gabriele Mencagli
 *  
 *  @brief Emitter implementing the broadcast (BD) distribution for GPU operators
 *  
 *  @section Broadcast_Emitter_GPU (Description)
 *  
 *  The emitter is capable of receiving/sending batches from/to GPU operators by
 *  implementing the broadcast distribution.
 */ 

#ifndef BD_EMITTER_GPU_H
#define BD_EMITTER_GPU_H

// includes
#include<batch_gpu_t.hpp>
#include<basic_emitter.hpp>

namespace wf {

// class Broadcast_Emitter_GPU
template<typename key_extractor_func_t, bool inputGPU, bool outputGPU>
class Broadcast_Emitter_GPU: public Basic_Emitter
{
private:
    key_extractor_func_t key_extr; // functional logic to extract the key attribute from the tuple_t
    using tuple_t = decltype(get_tuple_t_KeyExtrGPU(key_extr)); // extracting the tuple_t type and checking the admissible signatures
    size_t num_dests; // number of destinations connected in output to the emitter
    bool useTreeMode; // true if the emitter is used in tree-based mode
    std::vector<std::pair<void *, size_t>> output_queue; // vector of pairs (messages and destination identifiers)
    ff::MPMC_Ptr_Queue *queue; // pointer to the recyling queue

public:
    // Constructor
    Broadcast_Emitter_GPU(key_extractor_func_t _key_extr,
                          size_t _num_dests):
                          key_extr(_key_extr),
                          num_dests(_num_dests),
                          useTreeMode(false)
    {
        if constexpr (!(inputGPU && !outputGPU)) { // only GPU->CPU case is supported by the emitter
            std::cerr << RED << "WindFlow Error: Broadcast_Emitter_GPU created in an invalid manner" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        queue = new ff::MPMC_Ptr_Queue();
        queue->init(DEFAULT_BUFFER_CAPACITY);
    }

    // Copy Constructor
    Broadcast_Emitter_GPU(const Broadcast_Emitter_GPU &_other):
                          key_extr(_other.key_extr),
                          num_dests(_other.num_dests),
                          useTreeMode(_other.useTreeMode)
    {
        queue = new ff::MPMC_Ptr_Queue();
        queue->init(DEFAULT_BUFFER_CAPACITY);
    }

    // Move Constructor
    Broadcast_Emitter_GPU(Broadcast_Emitter_GPU &&_other):
                          key_extr(std::move(_other.key_extr)),
                          num_dests(_other.num_dests),
                          useTreeMode(_other.useTreeMode),
                          output_queue(std::move(_other.output_queue)),
                          queue(std::exchange(_other.queue, nullptr)) {}

    // Destructor
    ~Broadcast_Emitter_GPU() override
    {
        assert(output_queue.size() == 0); // sanity check
        if (queue != nullptr) {
            Batch_t<decltype(get_tuple_t_KeyExtr(key_extr))> *del_batch = nullptr;
            while (queue->pop((void **) &del_batch)) {
                delete del_batch;
            }
            delete queue; // delete the recycling queue
        }        
    }

    // Copy Assignment Operator
    Broadcast_Emitter_GPU &operator=(const Broadcast_Emitter_GPU &_other)
    {
        if (this != &_other) {
            key_extr = _other.key_extr;
            num_dests = _other.num_dests;
            useTreeMode = _other.useTreeMode;
        }
        return *this;
    }

    // Move Assignment Operator
    Broadcast_Emitter_GPU &operator=(Broadcast_Emitter_GPU &_other)
    {
        key_extr = std::move(_other.key_extr);
        num_dests = _other.num_dests;
        useTreeMode = _other.useTreeMode;
        output_queue = std::move(_other.output_queue);
        if (queue != nullptr) {
            Batch_t<decltype(get_tuple_t_KeyExtr(key_extr))> *del_batch = nullptr;
            while (queue->pop((void **) &del_batch)) {
                delete del_batch;
            }
            delete queue; // delete the recycling queue
        }
        queue = std::exchange(_other.queue, nullptr);
        return *this;
    }

    // Create a clone of the emitter
    Basic_Emitter *clone() const override
    {
        auto *copy = new Broadcast_Emitter_GPU<key_extractor_func_t, inputGPU, outputGPU>(*this);
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
        abort(); // <-- this method cannot be used!
    }

    // Emit method (in-place version)
    void emit_inplace(void *_out,
                      ff::ff_monode *_node) override
    {
        Batch_GPU_t<decltype(get_tuple_t_KeyExtrGPU(key_extr))> *output = reinterpret_cast<Batch_GPU_t<decltype(get_tuple_t_KeyExtrGPU(key_extr))> *>(_out);
        routing<inputGPU, outputGPU>(output, _node);
    }

    // Routing GPU->CPU
    template<bool b1, bool b2>
    void routing(typename std::enable_if<b1 && !b2, Batch_GPU_t<tuple_t>>::type *_output,
                 ff::ff_monode *_node)
    {
        (_output->delete_counter).fetch_add(num_dests-1);
        assert((_output->watermarks).size() == 1);
        (_output->watermarks).insert((_output->watermarks).end(), num_dests-1, (_output->watermarks)[0]); // copy the watermark (having one per destination)
        _output->transfer2CPU(); // transfer of GPU data to a host memory array
        for (size_t i=0; i<num_dests; i++) {
            if (!useTreeMode) { // real send
                _node->ff_send_out_to(_output, i);
            }
            else { // batch_output is buffered
                output_queue.push_back(std::make_pair(_output, i));
            }
        }
    }

    // Punctuation generation method
    void generate_punctuation(uint64_t _watermark,
                              ff::ff_monode * _node) override
    {
        Batch_GPU_t<decltype(get_tuple_t_KeyExtrGPU(key_extr))> *punc = allocateBatch_GPU_t<decltype(get_tuple_t_KeyExtrGPU(key_extr))>(1, queue); // punctuation batch has size 1!
        punc->setWatermark(_watermark);
        (punc->delete_counter).fetch_add(num_dests-1);
        assert((punc->watermarks).size() == 1);
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

    // Flushing function of the emitter
    void flush(ff::ff_monode *_node) override {}
};

} // namespace wf

#endif
