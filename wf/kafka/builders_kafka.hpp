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
 *  @file    builders_kafka.hpp
 *  @author  Gabriele Mencagli and Matteo della Bartola
 *  
 *  @brief Builder classes used to create the WindFlow operators to communicate
 *         with Apache Kafka
 *  
 *  @section Builders-Kafka (Description)
 *  
 *  Builder classes used to create the WindFlow operators communicating with Apache Kafka.
 *  They are the Kafka_Source and Kafka_Sink operators.
 */ 

#ifndef BUILDERS_KAFKA_H
#define BUILDERS_KAFKA_H

/// includes
#include<chrono>
#include<string>
#include<vector>
#include<functional>
#include<basic.hpp>
#include<kafka/meta_kafka.hpp>
#include<kafka/kafka_context.hpp>

namespace wf {

//@cond DOXY_IGNORE

// Struct with methods to create a concatenated string of tokens
struct stringLabels
{
    std::string strs;

    // Method to add a string
    template<typename H>
    void add_strings(H first)
    {
        strs.append(first);
        strs.append(", ");
    }

    // Method to add more strings
    template <typename H, typename... Args>
    void add_strings(H first, Args... others)
    {
        strs.append(first);
        strs.append(", ");
        add_strings(others...);
    }
};

// Struct to create a vector of tokens
struct vectorLabels
{
    std::vector<std::string> strs;

    // Method to add a string
    template<typename G>
    void add_strings(G first)
    {
        strs.push_back(first);
    }

    // Method to add more strings
    template <typename G, typename... Args>
    void add_strings(G first, Args... others)
    {
        strs.push_back(first);
        add_strings(others...);
    }
};

// Struct to create a vector of offset values
struct vectorTopicOffsets
{
    std::vector<int> offsets;

    // Method to add an offset
    template<typename O>
    void add_ints(O first)
    {
        offsets.push_back(first);
    }

    // Method to add more offsets
    template <typename O, typename... OSets>
    void add_ints(O first, OSets... others)
    {
        offsets.push_back(first);
        add_ints(others...);
    }
};

//@endcond

/** 
 *  \class KafkaSource_Builder
 *  
 *  \brief Builder of the Kafka_Source operator
 *  
 *  Builder class to ease the creation of the Kafka_Source operator.
 */ 
template<typename kafka_deser_func_t>
class KafkaSource_Builder
{
private:
    kafka_deser_func_t func; // deserialization logic of the Kafka_Source
    using result_t = decltype(get_result_t_KafkaSource(func)); // extracting the result_t type and checking the admissible signatures
    // static assert to check the signature of the Kafka_Source functional logic
    static_assert(!(std::is_same<result_t, std::false_type>::value),
        "WindFlow Compilation Error - unknown signature passed to the KafkaSource_Builder:\n"
        "  Candidate 1 : bool(std::optional<std::reference_wrapper<RdKafka::Message>>, Source_Shipper<result_t> &)\n"
        "  Candidate 2 : bool(std::optional<std::reference_wrapper<RdKafka::Message>>, Source_Shipper<result_t> &, KafkaRuntimeContext &)\n");
    // static assert to check that the result_t type must be default constructible
    static_assert(std::is_default_constructible<result_t>::value,
        "WindFlow Compilation Error - result_t type must be default constructible (KafkaSource_Builder):\n");
    using kafka_source_t = Kafka_Source<kafka_deser_func_t>; // type of the Kafka_Source to be created by the builder
    using closing_func_t = std::function<void(KafkaRuntimeContext&)>; // type of the closing functional logic
    std::string name = "kafka_source"; // name of the Kafka_Source
    size_t parallelism = 1; // parallelism of the Kafka_Source
    size_t outputBatchSize = 0; // output batch size of the Kafka_Source
    stringLabels broker_names; // struct containing the broker names
    std::string brokers; // concatenated string with broker names
    std::string groupid; // group identifier of the Kafka_Souce
    std::string strat; // assignment strategy of partitions to replicas
    int idleTime; // idle time in milliseconds
    vectorTopicOffsets offset_values; // struct containing the offset values
    std::vector<int> offsets; // vector of offsets
    vectorLabels topic_values; // struct containing the topic names
    std::vector<std::string> topics; // vector of topic names
    closing_func_t closing_func = [](KafkaRuntimeContext &r) -> void { return; }; // closing function logic of the Kafka_Source

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _func functional logic of the Kafka_Source (a function or a callable type)
     */ 
    KafkaSource_Builder(kafka_deser_func_t _func):
                        func(_func) {}

    /** 
     *  \brief Set the name of the Kafka_Source
     *  
     *  \param _name of the Kafka_Source
     *  \return a reference to the builder object
     */ 
    KafkaSource_Builder<kafka_deser_func_t> &withName(std::string _name)
    {
        name = _name;
        return *this;
    }

    /** 
     *  \brief Set the output batch size of the Kafka_Source
     *  
     *  \param _outputBatchSize number of outputs per batch (zero means no batching)
     *  \return a reference to the builder object
     */ 
    KafkaSource_Builder<kafka_deser_func_t> &withOutputBatchSize(size_t _outputBatchSize)
    {
        outputBatchSize = _outputBatchSize;
        return *this;
    }

    /** 
     *  \brief Set the closing functional logic used by the Kafka_Source
     *  
     *  \param _closing_func closing functional logic (a function or a callable type)
     *  \return a reference to the builder object
     */ 
    template<typename closing_F_t>
    KafkaSource_Builder<kafka_deser_func_t> &withClosingFunction(closing_F_t _closing_func)
    {
        // static assert to check the signature
        static_assert(!std::is_same<decltype(check_kafka_closing_t(_closing_func)), std::false_type>::value,
            "WindFlow Compilation Error - unknown signature passed to withClosingFunction (KafkaSource_Builder):\n"
            "  Candidate : void(KafkaRuntimeContext &)\n");
        closing_func = _closing_func;
        return *this;
    }

    /** 
     *  \brief Set the parallelism of the Kafka_Source
     *  
     *  \param _parallelism of the Kafka_Source
     *  \return a reference to the builder object
     */ 
    KafkaSource_Builder<kafka_deser_func_t> &withParallelism(size_t _parallelism)
    {
        parallelism = _parallelism;
        return *this;
    }

    /** 
     *  \brief Set the topic names
     *  
     *  \param _topics list of topics
     *  \return a reference to the builder object
     */ 
    template<typename G, typename... Args>
    KafkaSource_Builder<kafka_deser_func_t> &withTopics(G first, Args... Ts)
    {
        topic_values.add_strings(first, Ts...);
        topics = topic_values.strs;
        return *this;
    }

    /** 
     *  \brief Set the topic offsets
     *  
     *  \param _offsets list of offsets used for the topics
     *  \return a reference to the builder object
     */ 
    template<typename O, typename... OSets>
    KafkaSource_Builder<kafka_deser_func_t> &withOffsets(O first, OSets... Os)
    {
        offset_values.add_ints(first, Os...);
        offsets = offset_values.offsets;
        return *this;
    }

    /** 
     *  \brief Set the Kafka Brokers
     *  
     *  \param _brokers kafka servers
     *  \return a reference to the builder object
     */ 
    template<typename H, typename... Args>
    KafkaSource_Builder<kafka_deser_func_t> &withBrokers(H first, Args... Ts)
    {
        broker_names.add_strings(first, Ts...);
        broker_names.strs.pop_back(); // delete the last char
        broker_names.strs.pop_back(); // delete the last char
        brokers = broker_names.strs;
        return *this;
    }

    /** 
     *  \brief Set the consumer groupid
     *  
     *  \param _groupid of the consumer
     *  \return a reference to the builder object
     */ 
    KafkaSource_Builder<kafka_deser_func_t> &withGroupID(std::string _groupid)
    {
        groupid = _groupid;
        return *this;
    }

    /** 
     *  \brief Set the partition assignment strategy
     *  
     *  \param _strat string defining the assignment strategy
     *  \return a reference to the builder object
     */ 
    KafkaSource_Builder<kafka_deser_func_t> &withAssignmentPolicy(std::string _strat)
    {
        strat = _strat;
        return *this;
    }

    /** 
     *  \brief Set the idle time while fetching from brokers
     *  
     *  \param _idelTime idle period (in milliseconds)
     *  \return a reference to the builder object
     */ 
    KafkaSource_Builder<kafka_deser_func_t> &withIdleness(std::chrono::milliseconds _idleTime)
    {
        idleTime = _idleTime.count();
        return *this;
    }

    /** 
     *  \brief Create the Kafka_Source
     *  
     *  \return a new Kafka_Source instance
     */ 
    kafka_source_t build()
    {
        return kafka_source_t(func,
                              name,
                              outputBatchSize,
                              brokers,
                              topics,
                              groupid,
                              strat,
                              idleTime,
                              parallelism,
                              offsets,
                              closing_func);
    }
};

/** 
 *  \class KafkaSink_Builder
 *  
 *  \brief Builder of the Kafka_Sink operator
 *  
 *  Builder class to ease the creation of the Kafka_Sink operator.
 */ 
template<typename kafka_ser_func_t, typename key_t=empty_key_t>
class KafkaSink_Builder
{
private:
    template<typename T1, typename T2> friend class KafkaSink_Builder;
    kafka_ser_func_t func; // serialization logic of the Kafka_Sink
    using tuple_t = decltype(get_tuple_t_KafkaSink(func)); // extracting the tuple_t type and checking the admissible signatures
    // static assert to check the signature of the Kafka_Sink functional logic
    static_assert(!(std::is_same<tuple_t, std::false_type>::value),
        "WindFlow Compilation Error - unknown signature passed to the KafkaSink_Builder:\n"
        "  Candidate 1 : wf_kafka_sink_msg(tuple_t &)\n"
        "  Candidate 2 : wf_kafka_sink_msg(tuple_t &, KafkaRuntimeContext &)\n");
    // static assert to check that the result_t type must be default constructible
    static_assert(std::is_default_constructible<tuple_t>::value,
        "WindFlow Compilation Error - tuple_t type must be default constructible (KafkaSink_Builder):\n");
    using key_extractor_func_t = std::function<key_t(const tuple_t&)>; // type of the key extractor
    using kafka_sink_t = Kafka_Sink<kafka_ser_func_t, key_extractor_func_t>; // type of the Kafka_Sink to be created by the builder
    using kafka_closing_func_t = std::function<void(wf::KafkaRuntimeContext&)>; // type of the closing functional logic
    std::string name = "kafka_sink"; // name of the Kafka_Sink
    size_t parallelism = 1; // parallelism of the Kafka_Sink
    Routing_Mode_t input_routing_mode = Routing_Mode_t::FORWARD; // routing mode of inputs to the Kafka_Sink
    key_extractor_func_t key_extr = [](const tuple_t &t) -> key_t { return key_t(); }; // key extractor
    kafka_closing_func_t closing_func = [](KafkaRuntimeContext &r) -> void { return; }; // closing functional logic
    stringLabels broker_names; // struct containing the topic names
    std::string brokers; // string with the topic names

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _func functional logic of the Kafka_Sink (a function or a callable type)
     */ 
    KafkaSink_Builder(kafka_ser_func_t _func):
                      func(_func) {}

    /** 
     *  \brief Set the name of the Kafka_Sink
     *  
     *  \param _name of the Kafka_Sink
     *  \return a reference to the builder object
     */ 
    KafkaSink_Builder<kafka_ser_func_t, key_t> &withName(std::string _name)
    {
        name = _name;
        return *this;
    }

    /** 
     *  \brief Set the closing functional logic used by the Kafka_Sink
     *  
     *  \param _closing_func closing functional logic (a function or a callable type)
     *  \return a reference to the builder object
     */ 
    template<typename closing_F_t>
    KafkaSink_Builder<kafka_ser_func_t, key_t> &withClosingFunction(closing_F_t _closing_func)
    {
        // static assert to check the signature
        static_assert(!std::is_same<decltype(check_kafka_closing_t(_closing_func)), std::false_type>::value,
            "WindFlow Compilation Error - unknown signature passed to withClosingFunction (KafkaSink_Builder):\n"
            "  Candidate : void(KafkaRuntimeContext &)\n");
        closing_func = _closing_func;
        return *this;
    }

    /** 
     *  \brief Set the parallelism
     *  
     *  \param _parallelism for the Kafka_Sink
     *  \return a reference to the builder object
     */ 
    KafkaSink_Builder<kafka_ser_func_t> &withParallelism(size_t _parallelism)
    {
        parallelism = _parallelism;
        return *this;
    }

    /** 
     *  \brief Set the Kafka Brokers
     *  
     *  \param _brokers kafka servers
     *  \return a reference to the builder object
     */ 
    template<typename H, typename... Args>
    KafkaSink_Builder<kafka_ser_func_t> &withBrokers(H first, Args... Ts)
    {
        broker_names.add_strings(first, Ts...);
        broker_names.strs.pop_back(); // delete the last char
        broker_names.strs.pop_back(); // delete the last char
        brokers = broker_names.strs;
        return *this;
    }

    /** 
     *  \brief Set the KEYBY routing mode of inputs to the Kafka_Sink
     *  
     *  \param _key_extr key extractor functional logic (a function or a callable type)
     *  \return a new builder object with the right key type
     */ 
    template<typename new_key_extractor_func_t>
    auto withKeyBy(new_key_extractor_func_t _key_extr)
    {
        // static assert to check the signature
        static_assert(!std::is_same<decltype(get_tuple_t_KeyExtr(_key_extr)), std::false_type>::value,
            "WindFlow Compilation Error - unknown signature passed to withKeyBy (KafkaSink_Builder):\n"
            "  Candidate : key_t(const tuple_t &)\n");
        // static assert to check that the tuple_t type of the new key extractor is the right one
        static_assert(std::is_same<decltype(get_tuple_t_KeyExtr(_key_extr)), tuple_t>::value,
            "WindFlow Compilation Error - key extractor receives a wrong input type (KafkaSink_Builder):\n");
        using new_key_t = decltype(get_key_t_KeyExtr(_key_extr)); // extract the key type
        // static assert to check the new_key_t type
        static_assert(!std::is_same<new_key_t, void>::value,
            "WindFlow Compilation Error - key type cannot be void (KafkaSink_Builder):\n");
        // static assert to check that new_key_t is default constructible
        static_assert(std::is_default_constructible<new_key_t>::value,
            "WindFlow Compilation Error - key type must be default constructible (KafkaSink_Builder):\n");
        KafkaSink_Builder<kafka_ser_func_t, new_key_t> new_builder(func);
        new_builder.name = name;
        new_builder.parallelism = parallelism;
        new_builder.input_routing_mode = Routing_Mode_t::KEYBY;
        new_builder.key_extr = _key_extr;
        new_builder.closing_func = closing_func;
        new_builder.broker_names = broker_names;
        new_builder.brokers = brokers;
        return new_builder;
    }

    /** 
     *  \brief Create the Kafka_Sink
     *  
     *  \return a new Kafka_Sink instance
     */ 
    kafka_sink_t build()
    {
        return kafka_sink_t(func,
                            key_extr,
                            parallelism,
                            brokers,
                            name,
                            input_routing_mode,
                            closing_func);
    }
};

} // namespace wf

#endif
