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

/** 
 *  @file    kafka_sink.hpp
 *  @author  Gabriele Mencagli and Matteo Della Bartola
 *  
 *  @brief Kafka_Sink operator
 *  
 *  @section Kafka_Sink (Description)
 *  
 *  This file implements the Kafka_Sink operator able to absorb input streams
 *  of messages by producing them to Apache Kafka.
 */ 

#ifndef KAFKA_SINK_H
#define KAFKA_SINK_H

/// includes
#include<string>
#include<functional>
#include<batch_t.hpp>
#include<single_t.hpp>
#if defined (WF_TRACING_ENABLED)
    #include<stats_record.hpp>
#endif
#include<basic_emitter.hpp>
#include<basic_operator.hpp>
#include<kafka/meta_kafka.hpp>
#include<kafka/kafka_context.hpp>

namespace wf {

//@cond DOXY_IGNORE

// Class ExampleDeliveryReportCb
class ExampleDeliveryReportCb: public RdKafka::DeliveryReportCb
{
public:
    void dr_cb(RdKafka::Message &message)
    {
        if (message.err()) {
            std::cerr << RED << "WindFlow Error: message delivery error in Kafka_Sink, error: " << message.errstr() << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
    }
};

// class KafkaSink_Replica
template<typename kafka_ser_func_t>
class KafkaSink_Replica: public Basic_Replica
{
private:
    kafka_ser_func_t func; // functional logic used by the Sink replica
    using tuple_t = decltype(get_tuple_t_KafkaSink(func)); // extracting the tuple_t type and checking the admissible signatures
    // static predicates to check the type of the functional logic to be invoked
    static constexpr bool isNonRiched = std::is_invocable<decltype(func), tuple_t &>::value;
    static constexpr bool isRiched = std::is_invocable<decltype(func), tuple_t &, KafkaRuntimeContext &>::value;
    // check the presence of a valid functional logic
    static_assert(isNonRiched || isRiched,
        "WindFlow Compilation Error - KafkaSink_Replica does not have a valid serialization logic:\n");
    KafkaRuntimeContext kafka_context; // KafkaRuntimeContext object
    std::function<void(KafkaRuntimeContext &)> kafka_closing_func; // closing functional logic used by the Kafka_Sink replica
    size_t parallelism; // parallelism of the Kafka_Sink
    std::string brokers; // list of brokers
    RdKafka::Producer *producer; // pointer to the producer object
    RdKafka::Conf *conf; // pointer to the configuration object
    ExampleDeliveryReportCb ex_dr_cb; // delivery reporter

public:
    // Constructor
    KafkaSink_Replica(kafka_ser_func_t _func,
                      std::string _opName,
                      KafkaRuntimeContext _kafka_context,
                      std::string _brokers,
                      size_t _parallelism,
                      std::function<void(KafkaRuntimeContext &)> _kafka_closing_func):
                      Basic_Replica(_opName, _kafka_context, [](RuntimeContext &context) -> void { return; }, false),
                      func(_func),
                      kafka_context(_kafka_context),
                      kafka_closing_func(_kafka_closing_func),
                      parallelism(_parallelism),
                      brokers(_brokers),
                      producer(nullptr),
                      conf(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL)) {}

    // Copy Constructor
    KafkaSink_Replica(const KafkaSink_Replica &_other):
                      Basic_Replica(_other),
                      func(_other.func),
                      kafka_context(_other.kafka_context),
                      kafka_closing_func(_other.kafka_closing_func),
                      parallelism(_other.parallelism),
                      brokers(_other.brokers),
                      producer(nullptr),
                      conf(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL)) {}

    // svc_init (utilized by the FastFlow runtime)
    int svc_init() override
    {
        std::string errstr;
        // set the prodcuer
        if (conf->set("bootstrap.servers", brokers, errstr) != RdKafka::Conf::CONF_OK) {
            std::cerr << RED << "WindFlow Error: bootstrap.servers failed in Kafka_Sink, error: " << errstr << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        producer = RdKafka::Producer::create(conf, errstr);
        if (!producer) {
            std::cerr << RED << "WindFlow Error: reating the producer object in Kafka_Sink failed, error: " << errstr << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        kafka_context.setProducer(producer); // set the parameter of the KafkaRuntimeContext
#if defined (WF_TRACING_ENABLED)
        stats_record = Stats_Record(this->opName, std::to_string(kafka_context.getReplicaIndex()), false, false);
#endif
        return 0;
    }

    // svc (utilized by the FastFlow runtime)
    void *svc(void *_in) override
    {
        this->startStatsRecording();
        if (this->input_batching) { // receiving a batch
            Batch_t<tuple_t> *batch_input = reinterpret_cast<Batch_t<tuple_t> *>(_in);
            if (batch_input->isPunct()) { // if it is a punctuaton
                deleteBatch_t(batch_input); // delete the punctuation
                return this->GO_ON;
            }
#if defined (WF_TRACING_ENABLED)
            (this->stats_record).inputs_received += batch_input->getSize();
            (this->stats_record).bytes_received += batch_input->getSize() * sizeof(tuple_t);
#endif
            for (size_t i=0; i<batch_input->getSize(); i++) { // process all the inputs within the received batch
                process_input(batch_input->getTupleAtPos(i), batch_input->getTimestampAtPos(i), batch_input->getWatermark(kafka_context.getReplicaIndex()));
            }
            deleteBatch_t(batch_input); // delete the input batch
        }
        else { // receiving a single input
            Single_t<tuple_t> *input = reinterpret_cast<Single_t<tuple_t> *>(_in);
            if (input->isPunct()) { // if it is a punctuaton
                deleteSingle_t(input); // delete the punctuation
                return this->GO_ON;
            }
#if defined (WF_TRACING_ENABLED)
            (this->stats_record).inputs_received++;
            (this->stats_record).bytes_received += sizeof(tuple_t);
#endif
            process_input(input->tuple, input->getTimestamp(), input->getWatermark(kafka_context.getReplicaIndex()));                                       
            deleteSingle_t(input); // delete the input Single_t
        }
        this->endStatsRecording();
        return this->GO_ON;
    }

    // Process a single input
    void process_input(tuple_t &_tuple,
                       uint64_t _timestamp,
                       uint64_t _watermark)
    {
        if constexpr (isNonRiched) { // non-riched version
            wf::wf_kafka_sink_msg ret = func(_tuple);
            RdKafka::ErrorCode err = producer->produce(ret.topic,
                                                       ret.partition,
                                                       RdKafka::Producer::RK_MSG_COPY,
                                                       const_cast<char *>(ret.payload.c_str()),
                                                       ret.payload.size(),
                                                       NULL,
                                                       0,
                                                       0,
                                                       NULL,
                                                       NULL);
            producer->poll(0);
        }
        if constexpr (isRiched)  { // riched version
            wf::wf_kafka_sink_msg ret = func(_tuple, kafka_context);
            RdKafka::ErrorCode err = producer->produce(ret.topic,
                                                       ret.partition,
                                                       RdKafka::Producer::RK_MSG_COPY,
                                                       const_cast<char *>(ret.payload.c_str()),
                                                       ret.payload.size(),
                                                       NULL,
                                                       0,
                                                       0,
                                                       NULL,
                                                       NULL);
            producer->poll(0);
        }
    }

    // svc_end (utilized by the FastFlow runtime)
    void svc_end() override
    {
        kafka_closing_func(kafka_context); // call the closing function
    }

    KafkaSink_Replica(KafkaSink_Replica &&) = delete; ///< Move constructor is deleted
    KafkaSink_Replica &operator=(const KafkaSink_Replica &) = delete; ///< Copy assignment operator is deleted
    KafkaSink_Replica &operator=(KafkaSink_Replica &&) = delete; ///< Move assignment operator is deleted
};

//@endcond

/** 
 *  \class Kafka_Sink
 *  
 *  \brief Kafka_Sink operator
 *  
 *  This class implements the Kafka_Sink operator able to absorb input streams and producing
 *  them to Apache Kafka.
 */ 
template<typename kafka_ser_func_t, typename keyextr_func_t>
class Kafka_Sink: public Basic_Operator
{
private:
    friend class MultiPipe;
    friend class PipeGraph;
    kafka_ser_func_t func; // functional logic used by the Kafka_Sink
    using tuple_t = decltype(get_tuple_t_KafkaSink(func)); // extracting the tuple_t type and checking the admissible signatures
    keyextr_func_t key_extr; // logic to extract the key attribute from the tuple_t
    std::vector<KafkaSink_Replica<kafka_ser_func_t>*> replicas; // vector of pointers to the replicas of the Kafka_Sink

    // Configure the Kafka_Sink to receive batches instead of individual inputs
    void receiveBatches(bool _input_batching) override
    {
        for (auto *r: replicas) {
            r->receiveBatches(_input_batching);
        }
    }

    // Set the emitter used to route outputs from the Kafka_Sink (cannot be called for the Sink)
    void setEmitter(Basic_Emitter *_emitter) override
    { 
        abort(); // <-- this method cannot be used!
    }

    // Check whether the Kafka_Sink has terminated
    bool isTerminated() const override
    {
        bool terminated = true;
        for(auto *r: replicas) { // scan all the replicas to check their termination
            terminated = terminated && r->isTerminated();
        }
        return terminated;
    }

    // Set the execution mode of the Kafka_Sink
    void setExecutionMode(Execution_Mode_t _execution_mode)
    {
        for (auto *r: replicas) {
            r->setExecutionMode(_execution_mode);
        }
    }

    // Get the logic to extract the key attribute from the tuple_t
    keyextr_func_t getKeyExtractor() const
    {
        return key_extr;
    }

#if defined (WF_TRACING_ENABLED)
    // Append the statistics (JSON format) of the Sink to a PrettyWriter
    void appendStats(rapidjson::PrettyWriter<rapidjson::StringBuffer> &writer) const override
    {
        writer.StartObject(); // create the header of the JSON file
        writer.Key("Operator_name");
        writer.String((this->name).c_str());
        writer.Key("Operator_type");
        writer.String("Kafka_Sink");
        writer.Key("Distribution");
        if (this->getInputRoutingMode() == Routing_Mode_t::KEYBY) {
            writer.String("KEYBY");
        }
        else if (this->getInputRoutingMode() == Routing_Mode_t::REBALANCING) {
            writer.String("REBALANCING");
        }
        else {
            writer.String("FORWARD");
        }
        writer.Key("isTerminated");
        writer.Bool(this->isTerminated());
        writer.Key("isWindowed");
        writer.Bool(false);
        writer.Key("isGPU");
        writer.Bool(false);
        writer.Key("Parallelism");
        writer.Uint(this->parallelism);
        writer.Key("Replicas");
        writer.StartArray();
        for (auto *r: replicas) { // append the statistics from all the replicas of the Sink
            Stats_Record record = r->getStatsRecord();
            record.appendStats(writer);
        }
        writer.EndArray();
        writer.EndObject();
    }
#endif

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _func functional logic of the Kafka_Sink (a function or any callable type)
     *  \param _key_extr key extractor (a function or any callable type)
     *  \param _parallelism internal parallelism of the Kafka_Sink
     *  \param _brokers ip addresses of the kafka brokers (concatenated by commas)
     *  \param _name name of the Kafka_Sink
     *  \param _input_routing_mode input routing mode of the Kafka_Sink
     *  \param _kafka_closing_func closing functional logic of the Kafka_Sink (a function or any callable type)
     */ 
    Kafka_Sink(kafka_ser_func_t _func,
               keyextr_func_t _key_extr,
               size_t _parallelism,
               std::string _brokers,
               std::string _name,
               Routing_Mode_t _input_routing_mode,
               std::function<void(KafkaRuntimeContext &)> _kafka_closing_func):
               Basic_Operator(_parallelism, _name, _input_routing_mode, 1 /* outputBatchSize fixed to 1 */),
               func(_func),
               key_extr(_key_extr)
    {
        for (size_t i=0; i<this->parallelism; i++) { // create the internal replicas of the Sink
            replicas.push_back(new KafkaSink_Replica<kafka_ser_func_t>(_func, this->name, KafkaRuntimeContext(this->parallelism, i), _brokers, this->parallelism, _kafka_closing_func));
        }
    }

    /// Copy Constructor
    Kafka_Sink(const Kafka_Sink &_other):
               Basic_Operator(_other),
               func(_other.func),
               key_extr(_other.key_extr)
    {
        for (size_t i=0; i<this->parallelism; i++) { // deep copy of the pointers to the Sink replicas
            replicas.push_back(new KafkaSink_Replica<kafka_ser_func_t>(*(_other.replicas[i])));
        }
    }

    // Destructor
    ~Kafka_Sink() override
    {
        for (auto *r: replicas) { // delete all the replicas
            delete r;
        }
    }

    /** 
     *  \brief Get the type of the Kafka_Sink as a string
     *  \return type of the Kafka_Sink
     */ 
    std::string getType() const override
    {
        return std::string("Kafka_Sink");
    }

    Kafka_Sink(Kafka_Sink &&) = delete; ///< Move constructor is deleted
    Kafka_Sink &operator=(const Kafka_Sink &) = delete; ///< Copy assignment operator is deleted
    Kafka_Sink &operator=(Kafka_Sink &&) = delete; ///< Move assignment operator is deleted
};

} // namespace wf

#endif
