/**************************************************************************************
 *  Copyright (c) 2019- Gabriele Mencagli and Matteo Della Bartola
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

/*  
 *  Data types and operator functors used by the kafka tests.
 */ 

// include
#include<iostream>
#include<windflow.hpp>
#include<kafka/windflow_kafka.hpp>

using namespace std;
using namespace wf;

// struct tuple_t
struct tuple_t
{
    size_t key;
    std::string value;
};

// class KafkaSource_Functor
class KafkaSource_Functor
{
public:
    // operator()
    bool operator()(std::optional<std::reference_wrapper<RdKafka::Message>> msg, Source_Shipper<tuple_t> &shipper)
    {
        if (msg) {
            tuple_t out;
            out.key = 0;
            out.value = static_cast<const char *>(msg->get().payload());
            shipper.push(std::move(out));
            return true;
        }
        else {
            return false; // if timeout, we terminate listening
        }
    }
};

// Map functor
class Map_Functor
{
public:
    // operator()
    void operator()(tuple_t &t)
    {
        t.value = t.value + " arrived";
    }
};

// class KafkaSink_Functor
class KafkaSink_Functor
{
public:
    // operator()
    wf::wf_kafka_sink_msg operator()(tuple_t &out)
    {
        wf::wf_kafka_sink_msg tmp;
        std::string msg = out.value;
        tmp.payload = msg;
        tmp.topic = "output";
        return tmp;
    }
};
