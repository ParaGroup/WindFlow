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
 *  @file    splitting_emitter.hpp
 *  @author  Gabriele Mencagli
 *  
 *  @brief Emitter implementing the splitting logic of a MultiPipe
 *  
 *  @section Splitting_Emitter (Description)
 *  
 *  This file implements the splitting emitter in charge of splitting a MultiPipe.
 *  This version assumes to receive single tuples.
 */ 

#ifndef SPLITTING_H
#define SPLITTING_H

// includes
#include<basic.hpp>
#include<single_t.hpp>
#include<basic_emitter.hpp>

namespace wf {

// class Splitting_Emitter
template<typename splitting_func_t>
class Splitting_Emitter: public Basic_Emitter
{
private:
    splitting_func_t splitting_func; // splitting functional logic
    using tuple_t = decltype(get_tuple_t_Split(splitting_func)); // extracting the tuple_t type and checking the admissible signatures
    using return_t = decltype(get_return_t_Split(splitting_func)); // extracting the return_t type and checking the admissible signatures
    // static assert to check the signature
    static_assert(!(std::is_same<tuple_t, std::false_type>::value || std::is_same<return_t, std::false_type>::value),
        "WindFlow Compilation Error - unknown signature used in a MultiPipe splitting:\n"
        "  Candidate 1 : size_t(const tuple_t &)\n"
        "  Candidate 2 : std::vector<size_t>(const tuple_t &)\n"
        "  Candidate 3 : size_t(tuple_t &)\n"
        "  Candidate 4 : std::vector<size_t>(tuple_t &)\n"
        "  You can replace size_t in the signatures above with any C++ integral type\n");
    size_t num_dests; // number of destinations connected in output to the emitter
    size_t num_dest_mps; // number of destination MultiPipes connected in output to the emitter
    std::vector<Basic_Emitter *> emitters; // vector of pointers to the internal emitters (one per destination MultiPipes)
    Execution_Mode_t execution_mode; // execution mode of the PipeGraph
    uint64_t last_time_punct; // last time used to send punctuations
    std::vector<int> delivered; // delivered[i] is the number of outputs delivered to the MultiPipe i during the last sample
    uint64_t received_inputs; // total number of inputs received by the emitter

    // Call the splitting function (used if it returns a single identifier)
    template<typename F_t=splitting_func_t>
    auto callSplittingFunction(typename std::enable_if<std::is_same<F_t, F_t>::value && std::is_integral<return_t>::value,
                               splitting_func_t>::type &_func,
                               tuple_t &_t)
    {
        std::vector<decltype(get_return_t_Split(splitting_func))> dests;
        auto dest = _func(_t);
        dests.push_back(dest);
        return dests;
    }

    // Call the splitting function (used if it returns a vector of identifiers)
    template<typename F_t=splitting_func_t>
    auto callSplittingFunction(typename std::enable_if<std::is_same<F_t, F_t>::value && !std::is_integral<return_t>::value,
                               splitting_func_t>::type &_func,
                               tuple_t &_t)
    {
        return _func(_t);
    }

public:
    // Constructor
    Splitting_Emitter(splitting_func_t _splitting_func,
                      size_t _num_dest_mps,
                      Execution_Mode_t _execution_mode):
                      splitting_func(_splitting_func),
                      num_dests(0),
                      num_dest_mps(_num_dest_mps),
                      execution_mode(_execution_mode),
                      last_time_punct(current_time_usecs()),
                      delivered(_num_dest_mps, 0),
                      received_inputs(0) {}

    // Copy Constructor
    Splitting_Emitter(const Splitting_Emitter &_other):
                      splitting_func(_other.splitting_func),
                      num_dests(_other.num_dests),
                      num_dest_mps(_other.num_dest_mps),
                      execution_mode(_other.execution_mode),
                      last_time_punct(_other.last_time_punct),
                      delivered(_other.delivered),
                      received_inputs(_other.received_inputs)
    {
        for (size_t i=0; i<(_other.emitters).size(); i++) { // deep copy of the internal emitters
            emitters.push_back(((_other.emitters)[i])->clone());
        }
    }

    // Move Constructor
    Splitting_Emitter(Splitting_Emitter &&_other):
                      splitting_func(std::move(_other.splitting_func)),
                      num_dests(_other.num_dests),
                      num_dest_mps(_other.num_dest_mps),
                      execution_mode(_other.execution_mode),
                      emitters(std::move(_other.emitters)),
                      last_time_punct(_other.last_time_punct),
                      delivered(std::move(_other.delivered)),
                      received_inputs(_other.received_inputs) {}

    // Destructor
    ~Splitting_Emitter() override
    {
        for (auto *e: emitters) {
            delete e;
        }
    }

    // Copy Assignment Operator
    Splitting_Emitter &operator=(const Splitting_Emitter &_other)
    {
        if (this != &_other) {
            splitting_func = _other.splitting_func;
            num_dests = _other.num_dests;
            num_dest_mps = _other.num_dest_mps;
            execution_mode = _other.execution_mode;
            for (auto *e: emitters) {
                delete e;
            }
            emitters.clear();
            for (size_t i=0; i<(_other.emitters).size(); i++) { // deep copy of the emitters
                emitters.push_back(((_other.emitters)[i])->clone());
            }
            last_time_punct = _other.last_time_punct;
            delivered = _other.delivered;
            received_inputs = _other.received_inputs;
        }
        return *this;
    }

    // Move Assignment Operator
    Splitting_Emitter &operator=(Splitting_Emitter &&_other)
    {
        splitting_func = std::move(_other.splitting_func);
        num_dests = _other.num_dests;
        num_dest_mps = _other.num_dest_mps;
        execution_mode = _other.execution_mode;
        for (auto *e: emitters) {
            delete e;
        }
        emitters = std::move(_other.emitters);
        last_time_punct = _other.last_time_punct;
        delivered = std::move(_other.delivered);
        received_inputs = _other.received_inputs;
    }

    // Create a clone of the emitter
    Basic_Emitter *clone() const override
    {
        Splitting_Emitter<splitting_func_t> *copy = new Splitting_Emitter<splitting_func_t>(*this);
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
        received_inputs++;
        decltype(get_tuple_t_Split(splitting_func)) *tuple = reinterpret_cast<decltype(get_tuple_t_Split(splitting_func)) *>(_out);
        routing(*tuple, _timestamp, _watermark, _node);
    }

    // Emit method (in-place version)
    void emit_inplace(void *_out,
                      ff::ff_monode *_node) override
    {
        received_inputs++;
        Single_t<decltype(get_tuple_t_Split(splitting_func))> *output = reinterpret_cast<Single_t<decltype(get_tuple_t_Split(splitting_func))> *>(_out);
        routing(output->tuple, output->getTimestamp(), output->getWatermark(), _node);
        deleteSingle_t(output); // delete the input Single_t
    }

    // Routing method
    void routing(tuple_t &_tuple,
                 uint64_t _timestamp,
                 uint64_t _watermark,
                 ff::ff_monode *_node)
    {
        assert(num_dests == _node->get_num_outchannels()); // sanity check
        auto dests = callSplittingFunction(splitting_func, _tuple);
        if ((execution_mode == Execution_Mode_t::DEFAULT) && (received_inputs % WF_DEFAULT_WM_AMOUNT == 0)) { // check punctuaction generation logic
            generate_punctuation(_watermark, _node);
        }
        if (dests.size() == 0) { // the input must be dropped (like in a filter)
            return;
        }
        size_t idx = 0;
        while (idx < dests.size()) {
            if (dests[idx] >= num_dest_mps) {
                std::cerr << RED << "WindFlow Error: splitting index is out of range" << DEFAULT_COLOR << std::endl;
                exit(EXIT_FAILURE);
            }
            tuple_t copy_tuple = _tuple; // copy the input tuple
            delivered[dests[idx]]++;
            emitters[dests[idx]]->emit(&copy_tuple, 0, _timestamp, _watermark, _node); // call the logic of the emitter at position dests[idx]
            auto &vect = emitters[dests[idx]]->getOutputQueue();
            for (auto msg: vect) { // send each message produced by emitters[dests[idx]]
                size_t offset = 0;
                for (size_t i=0; i<dests[idx]; i++) {
                    offset += emitters[i]->getNumDestinations();
                }
                assert(offset + msg.second < _node->get_num_outchannels()); // sanity check
                _node->ff_send_out_to(msg.first, offset + msg.second);
            }
            vect.clear(); // clear all the sent messages
            idx++;
        }
    }

    // Punctuation propagation method
    void propagate_punctuation(uint64_t _watermark,
                               ff::ff_monode * _node) override
    {
        for (size_t i=0; i<emitters.size(); i++) {
            emitters[i]->propagate_punctuation(_watermark, _node);
            auto &vect = emitters[i]->getOutputQueue();
            for (auto msg: vect) { // send each message produced by emitters[i]
                size_t offset = 0;
                for (size_t j=0; j<i; j++) {
                    offset += emitters[j]->getNumDestinations();
                }
                assert(offset + msg.second < _node->get_num_outchannels()); // sanity check
                _node->ff_send_out_to(msg.first, offset + msg.second);
            }
            vect.clear(); // clear all the sent messages
        }
    }

    // Punctuation generation method
    void generate_punctuation(uint64_t _watermark,
                              ff::ff_monode *_node)
    {
        if (current_time_usecs() - last_time_punct >= WF_DEFAULT_WM_INTERVAL_USEC) { // check the end of the sample
            std::vector<int> idxs;
            for (size_t i=0; i<num_dest_mps; i++) { // select the destinations (MultiPipes) receiving the punctuation
                if (delivered[i] == 0) {
                    idxs.push_back(i);
                }
                else {
                    delivered[i] = 0;
                }
            }
            if (idxs.size() == 0) {
                return;
            }
            for (int id: idxs) {
                emitters[id]->propagate_punctuation(_watermark, _node);
                auto &vect = emitters[id]->getOutputQueue();
                for (auto msg: vect) { // send each message produced by emitters[i]
                    size_t offset = 0;
                    for (size_t j=0; j<id; j++) {
                        offset += emitters[j]->getNumDestinations();
                    }
                    assert(offset + msg.second < _node->get_num_outchannels()); // sanity check
                    _node->ff_send_out_to(msg.first, offset + msg.second);
                }
                vect.clear(); // clear all the sent messages
            }
            last_time_punct = current_time_usecs();
        }
    }

    // Flushing method
    void flush(ff::ff_monode *_node) override
    {
        assert(num_dests == _node->get_num_outchannels()); // sanity check
        for (size_t i=0; i<emitters.size(); i++) {
            emitters[i]->flush(_node);
            auto &vect = emitters[i]->getOutputQueue();
            for (auto msg: vect) { // send each message produced by emitters[i]
                size_t offset = 0;
                for (size_t j=0; j<i; j++) {
                    offset += emitters[j]->getNumDestinations();
                }
                assert(offset + msg.second < _node->get_num_outchannels()); // sanity check
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
        assert(emitters.size() <= num_dest_mps); // sanity check
    }

    // Get the number of internal emitters
    size_t getNumInternalEmitters() const override
    {
        return emitters.size();
    }
};

} // namespace wf

#endif
