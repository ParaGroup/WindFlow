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
 *  @brief Collector used for managing watermarks and tagging stream for join operation
 *  
 *  @section Join_Collector (Description)
 *  
 *  This class implements a FastFlow multi-input node able to receive inputs with
 *  watermarks, and to send them in output by adjusting watermarks in a correct
 *  manner. It also tag the incoming input ( source stream A or B ) for proper join execution by Join operator that follows
 */ 

#ifndef JOIN_COLLECTOR_H
#define JOIN_COLLECTOR_H

// includes
#include<unordered_map>
#include<ff/multinode.hpp>
#include<basic.hpp>
#include<batch_t.hpp>
#include<single_t.hpp>

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
    size_t id_collector; // identifier of the Join_Collector
    size_t eos_received; // number of received EOS messages
    size_t separator_id;

    uint64_t getMinimumWM()
    {
        uint64_t min_wm;
        bool first = true;
        for (size_t i=0; i<this->get_num_inchannels(); i++) {
            if (enabled[i] && first) {
                min_wm = maxs[i];
                first = false;
            }
            else if ((enabled[i]) && (maxs[i] < min_wm)) {
                min_wm = maxs[i];
            }
        }
        assert(first == false); // sanity check
        return min_wm;
    }

public:
    // Constructor
    Join_Collector(keyextr_func_t _key_extr,
                        ordering_mode_t _ordering_mode,
                        size_t _id_collector,
                        bool _input_batching=false,
                        size_t _separator_id=0):
                        key_extr(_key_extr),
                        input_batching(_input_batching),
                        ordering_mode(_ordering_mode),
                        id_collector(_id_collector),
                        separator_id(_separator_id),
                        eos_received(0)
    {
        assert(_ordering_mode == ordering_mode_t::TS);
    }

    // svc_init method (utilized by the FastFlow runtime)
    int svc_init() override
    {
        maxs.clear();
        enabled.clear();
        for (size_t i=0; i<this->get_num_inchannels(); i++) {
            maxs.push_back(0);
            enabled.push_back(true);
        }
        return 0;
    }

    // svc method (utilized by the FastFlow runtime)
    void *svc(void *_in) override
    {
        size_t source_id = this->get_channel_id(); // get the index of the source's stream
        Join_Stream_t stream_tag = source_id < separator_id ? Join_Stream_t::A : Join_Stream_t::B;
        
        if (!input_batching) { // non batching mode
            Single_t<tuple_t> *input = reinterpret_cast<Single_t<tuple_t> *>(_in); // cast the input to a Single_t structure
            assert(maxs[source_id] <= input->getWatermark(id_collector)); // sanity check
            maxs[source_id] = input->getWatermark(id_collector); // watermarks are received ordered on the same input channel
            uint64_t min_wm = getMinimumWM();
            input->setWatermark(min_wm, id_collector); // replace the watermark with the right one to use
            input->setStreamTag(stream_tag);
            return input;
        }
        else { // batching mode
            Batch_t<tuple_t> *batch_input = reinterpret_cast<Batch_t<tuple_t> *>(_in); // cast the input to a Batch_t structure
            assert(maxs[source_id] <= batch_input->getWatermark(id_collector)); // sanity check
            maxs[source_id] = batch_input->getWatermark(id_collector); // watermarks are received ordered on the same input channel
            uint64_t min_wm = getMinimumWM();
            batch_input->setWatermark(min_wm, id_collector); // replace the watermark with the right one to use
            batch_input->setStreamTag(stream_tag);
            return batch_input;
        }
    }

    // method to manage the EOS (utilized by the FastFlow runtime)
    void eosnotify(ssize_t id) override
    {
        assert(id < this->get_num_inchannels()); // sanity check
        enabled[id] = false; // disable the channel where we received the EOS
    }
};

} // namespace wf

#endif
