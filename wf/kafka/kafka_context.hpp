/**************************************************************************************
 *  Copyright (c) 2019- Gabriele Mencagli and Matteo della Bartola
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
 *  @file    kafka_context.hpp
 *  @author  Gabriele Mencagli and Matteo Della Bartola
 *  
 *  @brief KafkaRuntimeContext class to access the run-time system information by the
 *         Kafka operators
 *  
 *  @section KafkaRuntimeContext (Description)
 *  
 *  This file implements the KafkaRuntimeContext class used to access the run-time system
 *  information accessible with the "riched" functional logic supported by the Kafka operators.
 */ 

#ifndef KAFKA_CONTEXT_H
#define KAFKA_CONTEXT_H

/// includes
#include<functional>
#include<basic.hpp>
#include<context.hpp>
#include<local_storage.hpp>
#include<librdkafka/rdkafkacpp.h>

namespace wf {

/** 
 *  \class KafkaRuntimeContext
 *  
 *  \brief KafkaRuntimeContext class used to access to run-time system information by the
 *         Kafka operators
 *  
 *  This class implements the KafkaRuntimeContext object used to access the run-time system
 *  information accessible with the "riched" variants of the functional logic of Kafka operators.
 */ 
class KafkaRuntimeContext: public RuntimeContext
{
private:
    template<typename T> friend class KafkaSource_Replica; // friendship with KafkaSource_Replica class
    template<typename T> friend class KafkaSink_Replica; // friendship with KafkaSink_Replica class
    RdKafka::KafkaConsumer *consumer; // pointer to the consumer object
    RdKafka::Producer *producer; // pointer to the producer object

    // Method to set the producer pointer
    void setProducer(RdKafka::Producer *_producer)
    {
        producer = _producer;
    }

    // Method to set the consumer pointer
    void setConsumer(RdKafka::KafkaConsumer *_consumer)
    {
        consumer = _consumer;
    }

public:
    /// Constructor
    KafkaRuntimeContext(size_t _parallelism,
                        size_t _index):
                        RuntimeContext(_parallelism, _index) {}

    /** 
     *  \brief Get the pointer to the consumer object
     *  
     *  \return pointer to the consumer
     */ 
    RdKafka::KafkaConsumer *getConsumer()
    {
        return consumer;
    }

    /** 
     *  \brief Get the pointer to the producer object
     *  
     *  \return pointer to the producer
     */ 
    RdKafka::Producer *getProducer()
    {
        return producer;
    }
};

} // namespace wf

#endif
