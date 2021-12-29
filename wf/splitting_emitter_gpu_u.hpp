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
 *  @file    splitting_emitter_gpu_u.hpp
 *  @author  Gabriele Mencagli
 *  
 *  @brief Emitter implementing the splitting logic of a MultiPipe ending with a
 *         GPU operator. Version with CUDA Unified Memory
 *  
 *  @section Splitting_Emitter_GPU (Description)
 *  
 *  This file implements the splitting emitter in charge of splitting a MultiPipe.
 *  This version assumes to receive Batch_GPU_t messages. Version with CUDA Unified Memory
 */ 

#ifndef SPLITTING_GPU_U_H
#define SPLITTING_GPU_U_H

// includes
#include<batch_gpu_t_u.hpp>
#include<basic_emitter.hpp>

namespace wf {

// class Splitting_Emitter_GPU
template<typename tuple_t>
class Splitting_Emitter_GPU: public Basic_Emitter
{
private:
    size_t num_dests; // number of destinations connected in output to the emitter
    size_t num_dest_mps; // number of destination MultiPipes connected in output to the emitter
    std::vector<Basic_Emitter *> emitters; // vector of pointers to the internal emitters (one per destination MultiPipes)
    ff::MPMC_Ptr_Queue *queue; // pointer to the recyling queue

public:
    // Constructor
    Splitting_Emitter_GPU(size_t _num_dest_mps):
                          num_dests(0),
                          num_dest_mps(_num_dest_mps)
    {
        queue = new ff::MPMC_Ptr_Queue();
        queue->init(WF_GPU_DEFAULT_RECYCLING_QUEUE_SIZE);
    }

    // Copy Constructor
    Splitting_Emitter_GPU(const Splitting_Emitter_GPU &_other):
                          num_dests(_other.num_dests),
                          num_dest_mps(_other.num_dest_mps)
    {
        for (size_t i=0; i<(_other.emitters).size(); i++) { // deep copy of the internal emitters
            emitters.push_back(((_other.emitters)[i])->clone());
        }
        queue = new ff::MPMC_Ptr_Queue();
        queue->init(WF_GPU_DEFAULT_RECYCLING_QUEUE_SIZE);
    }

    // Move Constructor
    Splitting_Emitter_GPU(Splitting_Emitter_GPU &&_other):
                          num_dests(_other.num_dests),
                          num_dest_mps(_other.num_dest_mps),
                          emitters(std::move(_other.emitters)),
                          queue(std::exchange(_other.queue, nullptr)) {}

    // Destructor
    ~Splitting_Emitter_GPU() override
    {
        for (auto *e: emitters) {
            delete e;
        }
        if (queue != nullptr) { // delete all the batches in the recycling queue
            Batch_t<tuple_t> *del_batch = nullptr;
            while (queue->pop((void **) &del_batch)) {
                delete del_batch;
            }
            delete queue; // delete the recycling queue
        }
    }

    // Copy Assignment Operator
    Splitting_Emitter_GPU &operator=(const Splitting_Emitter_GPU &_other)
    {
        if (this != &_other) {
            num_dests = _other.num_dests;
            num_dest_mps = _other.num_dest_mps;
            for (auto *e: emitters) {
                delete e;
            }
            emitters.clear();
            for (size_t i=0; i<(_other.emitters).size(); i++) { // deep copy of the emitters
                emitters.push_back(((_other.emitters)[i])->clone());
            }
        }
        return *this;
    }

    // Move Assignment Operator
    Splitting_Emitter_GPU &operator=(Splitting_Emitter_GPU &&_other)
    {
        num_dests = _other.num_dests;
        num_dest_mps = _other.num_dest_mps;
        for (auto *e: emitters) {
            delete e;
        }
        emitters = std::move(_other.emitters);
        queue = std::exchange(_other.queue, nullptr);
    }

    // Create a clone of the emitter
    Basic_Emitter *clone() const override
    {
        Splitting_Emitter_GPU<tuple_t> *copy = new Splitting_Emitter_GPU<tuple_t>(*this);
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
        abort(); // <-- this method cannot be used!
    }

    // Get a reference to the vector of output messages used by the emitter
    std::vector<std::pair<void *, size_t>> &getOutputQueue() override
    {
        abort(); // <-- this method cannot be used!
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
        assert(num_dests == _node->get_num_outchannels());
        Batch_GPU_t<tuple_t> *input = reinterpret_cast<Batch_GPU_t<tuple_t> *>(_out);
        for (size_t i=0; i<num_dest_mps-1; i++) { // iterate across all the internal emitters (except the last one)
            Batch_GPU_t<tuple_t> *copy_batch = allocateBatch_GPU_t<tuple_t>(input->original_size, queue); // create a new batch
            copy_batch->watermarks = input->watermarks;
            copy_batch->size = input->size;
            memcpy(copy_batch->data_u, input->data_u, sizeof(batch_item_gpu_t<tuple_t>) * input->size);
            copy_batch->prefetch2GPU(false); // <-- this is not always the best choice!
            emitters[i]->emit_inplace(copy_batch, _node); // call the logic of the emitter at position i
            auto &vect = emitters[i]->getOutputQueue();
            for (auto msg: vect) { // send each message produced by emitters[i]
                size_t offset = 0;
                for (size_t j=0; j<i; j++) {
                    offset += emitters[j]->getNumDestinations();
                }
                assert(offset + msg.second < _node->get_num_outchannels());
                _node->ff_send_out_to(msg.first, offset + msg.second);
            }
            vect.clear(); // clear all the sent messages
        }
        emitters[num_dest_mps-1]->emit_inplace(input, _node); // call the logic of the last emitter with the original input
        auto &vect = emitters[num_dest_mps-1]->getOutputQueue();
        for (auto msg: vect) { // send each message produced by emitters[num_dest_mps-1]
            size_t offset = 0;
            for (size_t j=0; j<num_dest_mps-1; j++) {
                offset += emitters[j]->getNumDestinations();
            }
            assert(offset + msg.second < _node->get_num_outchannels());
            _node->ff_send_out_to(msg.first, offset + msg.second);
        }
        vect.clear(); // clear all the sent messages
    }

    // Punctuation generation method
    void generate_punctuation(uint64_t _watermark,
                              ff::ff_monode * _node) override
    {
        for (size_t i=0; i<emitters.size(); i++) {
            emitters[i]->generate_punctuation(_watermark, _node); // call the logic of the emitter at position i
            auto &vect = emitters[i]->getOutputQueue();
            for (auto msg: vect) { // send each message produced by emitters[i]
                size_t offset = 0;
                for (size_t j=0; j<i; j++) {
                    offset += emitters[j]->getNumDestinations();
                }
                assert(offset + msg.second < _node->get_num_outchannels());
                _node->ff_send_out_to(msg.first, offset + msg.second);
            }
            vect.clear(); // clear all the sent messages
        }
    }

    // Flushing function of the emitter
    void flush(ff::ff_monode *_node) override
    {
        assert(num_dests == _node->get_num_outchannels());
        for (size_t i=0; i<emitters.size(); i++) {
            emitters[i]->flush(_node); // call the flush logic of emitters[i]
            auto &vect = emitters[i]->getOutputQueue();
            for (auto msg: vect) { // send each message produced by emitters[i]
                size_t offset = 0;
                for (size_t j=0; j<i; j++) {
                    offset += emitters[j]->getNumDestinations();
                }
                assert(offset + msg.second < _node->get_num_outchannels());
                _node->ff_send_out_to(msg.first, offset + msg.second);
            }
            vect.clear(); // clear all the sent messages
        }
    }

    // Add a new internal emitter
    void addInternalEmitter(Basic_Emitter *_e) override
    {
        _e->setTreeMode(true); // all the emitters work in tree mode
        emitters.push_back(_e);
        num_dests += _e->getNumDestinations();
        assert(emitters.size() <= num_dest_mps);
    }

    // Get the number of internal emitters
    size_t getNumInternalEmitters() const override
    {
        return emitters.size();
    }
};

} // namespace wf

#endif
