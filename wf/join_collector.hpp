/**************************************************************************************
 *  Copyright (c) 2023- Gabriele Mencagli and Yuriy Rymarchuk
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
 *  @file    join_collector.hpp
 *  @author  Gabriele Mencagli and Yuriy Rymarchuk
 *  
 *  @brief Collector used for managing inputs for join operators in data partitioning mode ( Round Robin dispatching )
 *  
 *  @section Join_Collector (Description)
 *  
 *  This class implements a FastFlow multi-input node able to receive inputs with
 *  watermarks, and to send them in output by adjusting watermarks and stream tag for join operators in a correct
 *  manner. The collector is used in data partitioning mode ( Round Robin dispatching ).
 */ 

#ifndef JOIN_COLLECTOR_H
#define JOIN_COLLECTOR_H

// includes
#include<ff/multinode.hpp>
#include<basic.hpp>
#include<batch_t.hpp>
#include<single_t.hpp>

#include<unordered_map>
#include <queue>

namespace wf {

// class Join_Collector
template<typename keyextr_func_t>
class Join_Collector: public ff::ff_minode
{
private:
    keyextr_func_t key_extr; // key extractor
    using tuple_t = decltype(get_tuple_t_KeyExtr(key_extr)); // extracting the tuple_t type and checking the admissible singatures

    std::vector<bool> enabled; // enable[i] is true if channel i is enabled
    std::vector<uint64_t> maxs; // maxs[i] constains the highest watermark received from the i-th input channel

    bool input_batching; // true if the collector expects to receive batches, false otherwise
    ordering_mode_t ordering_mode; // ordering mode used by the Join_Collector
    Execution_Mode_t execution_mode; // execution mode of the PipeGraph
    Join_Mode_t interval_join_mode; // interval join mode
    size_t id_collector; // identifier of the Join_Collector
    size_t separator_id; // streams separator meaningful to join operators
    Join_Stream_t stream; // next stream to forward from the output
    size_t A_id; // next channel id to forward the output from (A stream)
    size_t B_id; // next channel id to forward the output from (B stream)
    size_t id; // next channel id to forward the output from
    size_t eos_received; // number of received EOS messages

    std::unordered_map<size_t, std::queue<void *>> channelMap; // hash table mapping keys onto key descriptors

    // Get the minimum watermark among the enabled channels
    uint64_t getMinimumWM()
    {
        uint64_t min_wm;
        bool first = true;
        for (size_t i=0; i<this->get_num_inchannels(); i++) {
            if ((enabled[i] || !channelMap[i].empty()) && first) {
                min_wm = maxs[i];
                first = false;
            }
            else if ((enabled[i] || !channelMap[i].empty()) && (maxs[i] < min_wm)) {
                min_wm = maxs[i];
            }
        }
        assert(first == false); // sanity check
        return min_wm;
    }

    template <typename in_t>
    inline void setup_tuple(in_t _in, size_t source_id)
    {
        assert(maxs[source_id] <= _in->getWatermark(id_collector)); // sanity check
        maxs[source_id] = _in->getWatermark(id_collector); // watermarks are received ordered on the same input channel
        uint64_t min_wm = getMinimumWM();
        _in->setWatermark(min_wm, id_collector); // replace the watermark with the right one to use
        _in->setStreamTag(source_id < separator_id ? Join_Stream_t::A : Join_Stream_t::B);
    }

    inline size_t nextA_channel()
    {
        A_id = (A_id + 1) % separator_id;
        return A_id;
    }

    inline size_t nextB_channel()
    {
        B_id = (B_id + 1) % (this->get_num_inchannels());
        if(B_id==0) B_id += separator_id;
        return B_id;
    }

    inline void nextStream() {
        stream = (stream == Join_Stream_t::A) ? Join_Stream_t::B : Join_Stream_t::A;
    }

    void loop_ch()
    {
        if(stream == Join_Stream_t::A){
            for(size_t i=0; i<separator_id; i++){
                if(enabled[id] || !channelMap[id].empty()) break;
                id = nextA_channel();
            }
        } else {
            for(size_t i=separator_id; i<this->get_num_inchannels(); i++){
                if(enabled[id] || !channelMap[id].empty()) break;
                id = nextB_channel();
            }
        }
    }


public:
    // Constructor
    Join_Collector(keyextr_func_t _key_extr,
                        ordering_mode_t _ordering_mode,
                        Execution_Mode_t _execution_mode,
                        Join_Mode_t _interval_join_mode,
                        size_t _id_collector,
                        bool _input_batching=false,
                        size_t _separator_id=0):
                        key_extr(_key_extr),
                        input_batching(_input_batching),
                        ordering_mode(_ordering_mode),
                        execution_mode(_execution_mode),
                        interval_join_mode(_interval_join_mode),
                        id_collector(_id_collector),
                        separator_id(_separator_id),
                        A_id(0),
                        B_id(separator_id),
                        id(0),
                        stream(Join_Stream_t::A),
                        eos_received(0)
    {
        assert(execution_mode == Execution_Mode_t::DEFAULT && _ordering_mode == ordering_mode_t::TS && _interval_join_mode == Join_Mode_t::DP); // sanity check
    }

    // svc_init method (utilized by the FastFlow runtime)
    int svc_init() override
    {
        maxs.clear();
        enabled.clear();
        for (size_t i=0; i<this->get_num_inchannels(); i++) {
            maxs.push_back(0);
            enabled.push_back(true);
            channelMap.insert(std::make_pair(i,  std::queue<void * >() ));
        }
        return 0;
    }

    // svc method (utilized by the FastFlow runtime)
    void *svc(void *_in) override
    {
        size_t source_id = this->get_channel_id(); // get the index of the source's stream
        if (!input_batching) { // non batching mode
            Single_t<tuple_t> * input = reinterpret_cast<Single_t<tuple_t> *>(_in); // cast the input to a Single_t structure

            id = (stream == Join_Stream_t::A) ? A_id : B_id;
            if (source_id != id) {
                channelMap[source_id].push(input);
                loop_ch();
                if (channelMap[id].empty()) return this->GO_ON;
                input = reinterpret_cast<Single_t<tuple_t> *>(channelMap[id].front());
                channelMap[id].pop();
            } else if (!channelMap[id].empty()) {
                channelMap[id].push(input);
                input = reinterpret_cast<Single_t<tuple_t> *>(channelMap[id].front());
                channelMap[id].pop();
            }
            
            setup_tuple(input, id);
            (stream == Join_Stream_t::A) ? nextA_channel() : nextB_channel();
            nextStream();
            return input;
        }
        else { // batching mode
            Batch_t<tuple_t> *batch_input = reinterpret_cast<Batch_t<tuple_t> *>(_in); // cast the input to a Batch_t structure

            id = (stream == Join_Stream_t::A) ? A_id : B_id;
            if (source_id != id) {
                channelMap[source_id].push(batch_input);
                loop_ch();
                if (channelMap[id].empty()) return this->GO_ON;
                batch_input = reinterpret_cast<Batch_t<tuple_t> *>(channelMap[id].front());
                channelMap[id].pop();
            } else if (!channelMap[id].empty()) {
                channelMap[id].push(batch_input);
                batch_input = reinterpret_cast<Batch_t<tuple_t> *>(channelMap[id].front());
                channelMap[id].pop();
            }
            
            setup_tuple(batch_input, id);
            (stream == Join_Stream_t::A) ? nextA_channel() : nextB_channel();
            nextStream();
            return batch_input;
        }
    }

    // method to manage the EOS (utilized by the FastFlow runtime)
    void eosnotify(ssize_t id) override
    {
        assert(id < this->get_num_inchannels()); // sanity check
        eos_received++;
        enabled[id] = false; // disable the channel where we received the EOS
        if (eos_received != this->get_num_inchannels()) { // check the number of received EOS messages
            return;
        }
        
        size_t a_size = 0;
        size_t b_size = 0;
        for(size_t i=0; i<this->get_num_inchannels(); i++){
            if(i < separator_id) a_size += channelMap[i].size();
            else b_size += channelMap[i].size();
        }
       
        size_t idx;
        while ((a_size + b_size) != 0){
            if((stream == Join_Stream_t::A) && (a_size == 0)) stream = Join_Stream_t::B;
            if((stream == Join_Stream_t::B) && (b_size == 0)) stream = Join_Stream_t::A;
            idx = (stream == Join_Stream_t::A) ? A_id : B_id;
            if (!channelMap[idx].empty()) {
                Single_t<tuple_t> *out = reinterpret_cast<Single_t<tuple_t> *>(channelMap[idx].front());
                channelMap[idx].pop();
                setup_tuple(out, idx);
                this->ff_send_out(out);
                (stream == Join_Stream_t::A) ? a_size-- : b_size--;
            } else {
                Batch_t<tuple_t> *out = reinterpret_cast<Batch_t<tuple_t> *>(channelMap[idx].front());
                channelMap[idx].pop();
                setup_tuple(out, idx);
                this->ff_send_out(out);
                (stream == Join_Stream_t::A) ? a_size-- : b_size--;
            }
            (stream == Join_Stream_t::A) ? nextA_channel() : nextB_channel();
            nextStream();
        }
    }

    // svc_end method (utilized by the FastFlow runtime)
    void svc_end() override
    {
        for (auto &q: channelMap) { // check that the all the channel queues are empty
            auto &channel_queue = (q.second);
            assert((channel_queue).size() == 0);
        }
    }

};

} // namespace wf

#endif
