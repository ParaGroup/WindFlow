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
 *  @file    kafka_source.hpp
 *  @author  Gabriele Mencagli and Matteo Della Bartola
 *  
 *  @brief Kafka_Source operator
 *  
 *  @section Kafka_Source (Description)
 *  
 *  This file implements the Kafka_Source operator able to generate output streams
 *  of messages read from Apache Kafka.
 */ 

#ifndef KAFKA_SOURCE_H
#define KAFKA_SOURCE_H

/// includes
#include<string>
#include<optional>
#include<functional>
#include<source_shipper.hpp>
#if defined (WF_TRACING_ENABLED)
    #include<stats_record.hpp>
#endif
#include<basic_emitter.hpp>
#include<basic_operator.hpp>
#include<kafka/meta_kafka.hpp>
#include<kafka/kafka_context.hpp>

namespace wf {

//@cond DOXY_IGNORE

// Class ExampleRebalanceCb
class ExampleRebalanceCb: public RdKafka::RebalanceCb
{
private:
    std::vector<int> offsets; // vector of offsets
    std::vector<std::string> topics; // vector of topic names
    int size = 0; // number of topics
    int init = 0; // init value

public:
    // Method to initialize the topics and offsets
    void initOffsetTopics(std::vector<int> _offsets,
                          std::vector<std::string> _topics)
    {
        offsets = _offsets;
        topics = _topics;
        size = topics.size();
        init = 0; // reload offset mid execution (at next rebalance callback) (need to test)
    }

    // Method rebalance_cb
    void rebalance_cb(RdKafka::KafkaConsumer *consumer,
                      RdKafka::ErrorCode err,
                      std::vector<RdKafka::TopicPartition *> &partitions)
    {
        if (init == 0) {
            if (offsets.size() != 0){
                for (int i = 0; i<size; i++) {
                    for (auto j:partitions) {
                        if (j->topic() == topics[i]) {
                            if (offsets[i] > -1) {
                                j->set_offset(offsets[i]);
                            }
                        }
                    }
                }
            }
            init++;
        }
        RdKafka::Error *error = NULL;
        RdKafka::ErrorCode ret_err = RdKafka::ERR_NO_ERROR;
        if (err == RdKafka::ERR__ASSIGN_PARTITIONS) {
            if (consumer->rebalance_protocol() == "COOPERATIVE") {
                error = consumer->incremental_assign(partitions);
            }
            else {
                ret_err = consumer->assign(partitions);
            }
        }
        else {
            if (consumer->rebalance_protocol() == "COOPERATIVE") {
                error = consumer->incremental_unassign(partitions);
            }
            else {
                ret_err = consumer->unassign();
            }
        }
        if (error) {
            std::cerr << RED << "WindFlow Error: incremental assign failed in Kafka_Source, error: " << error->str() << DEFAULT_COLOR << std::endl;
            delete error;
            exit(EXIT_FAILURE);
        }
        else if (ret_err) {
            std::cerr << RED << "WindFlow Error: assignment error in Kafka_Source, error: " << RdKafka::err2str(ret_err) << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
    }
};

// class KafkaSource_Replica
template<typename kafka_deser_func_t>
class KafkaSource_Replica: public Basic_Replica
{
private:
    template<typename T> friend class Kafka_Source;
    kafka_deser_func_t func; // logic for deserializing messages from Apache Kafka
    using result_t = decltype(get_result_t_KafkaSource(func)); // extracting the result_t type and checking the admissible signatures
    // static predicates to check the type of the deserialization logic to be invoked
    static constexpr bool isNonRiched = std::is_invocable<decltype(func), std::optional<std::reference_wrapper<RdKafka::Message>>, Source_Shipper<result_t> &>::value;
    static constexpr bool isRiched = std::is_invocable<decltype(func), std::optional<std::reference_wrapper<RdKafka::Message>>, Source_Shipper<result_t> &, KafkaRuntimeContext &>::value;
    // check the presence of a valid deserialization logic
    static_assert(isNonRiched || isRiched,
        "WindFlow Compilation Error - KafkaSource_Replica does not have a valid deserialization logic:\n");
    KafkaRuntimeContext kafka_context; // KafkaRuntimeContext object
    std::function<void(KafkaRuntimeContext &)> kafka_closing_func; // closing functional logic used by the Kafka_Source replica
    Time_Policy_t time_policy; // time policy of the Kafka_Source replica
    Source_Shipper<result_t> *shipper; // pointer to the shipper object used by the Kafka_Source replica to send outputs
    RdKafka::KafkaConsumer *consumer; // pointer to the consumer object
    RdKafka::Conf *conf; // pointer to the configuration object
    int idleTime; // idle time in milliseconds
    std::string brokers; // list of brokers
    std::string groupid; // group identifier
    std::string strat; // string identifying the partition assignment strategy
    std::vector<int> offsets; // vector of offsets
    size_t parallelism; // number of replicas of the Kafka_Source
    ExampleRebalanceCb ex_rebalance_cb; // partiotion manager
    std::vector<std::string> topics; // list of topic names
    pthread_barrier_t *bar; // pointer to a barrier object ot synchronize the replicas

public:
    // Constructor
    KafkaSource_Replica(kafka_deser_func_t _func,
                        std::string _opName,
                        KafkaRuntimeContext _kafka_context,
                        size_t _outputBatchSize,
                        std::string _brokers,
                        std::vector<std::string> _topics,
                        std::string _groupid,
                        std::string _strat,
                        size_t _parallelism,
                        int _idleTime,
                        std::vector<int> _offsets,
                        pthread_barrier_t *_bar,
                        std::function<void(KafkaRuntimeContext &)> _kafka_closing_func):
                        Basic_Replica(_opName, _kafka_context, [](RuntimeContext &context) -> void { return; }, false),
                        func(_func),
                        kafka_context(_kafka_context),
                        kafka_closing_func(_kafka_closing_func),
                        time_policy(Time_Policy_t::INGRESS_TIME),
                        shipper(nullptr),
                        consumer(nullptr),
                        conf(nullptr),
                        idleTime(_idleTime),
                        brokers(_brokers),
                        groupid(_groupid),
                        strat(_strat),
                        offsets(_offsets),
                        parallelism(_parallelism),
                        topics(_topics),
                        bar(_bar) {}

    // Copy Constructor
    KafkaSource_Replica(const KafkaSource_Replica &_other):
                        Basic_Replica(_other),
                        func(_other.func),
                        kafka_context(_other.kafka_context),
                        kafka_closing_func(_other.kafka_closing_func),
                        time_policy(_other.time_policy),
                        consumer(nullptr),
                        conf(nullptr),
                        idleTime(_other.idleTime),
                        brokers(_other.brokers),
                        groupid(_other.groupid),
                        strat(_other.strat),
                        offsets(_other.offsets),
                        parallelism(_other.parallelism),
                        topics(_other.topics),
                        bar(_other.bar)            
    {
        if (_other.shipper != nullptr) {
            shipper = new Source_Shipper<result_t>(*(_other.shipper));
            shipper->node = this; // change the node referred by the shipper
#if defined (WF_TRACING_ENABLED)
            shipper->setStatsRecord(&(this->stats_record)); // change the Stats_Record referred by the shipper
#endif
        }
        else {
            shipper = nullptr;
        }
    }

    // Destructor
    ~KafkaSource_Replica() override
    {
        if (shipper != nullptr) {
            delete shipper;
        }
        if (consumer != nullptr) {
            delete consumer;
        }
        if (conf != nullptr) {
            delete conf;
        }
    }

    // svc_init (utilized by the FastFlow runtime)
    int svc_init() override
    {
        ex_rebalance_cb.initOffsetTopics(offsets, topics);
        conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
        std::string errstr;
        if (conf->set("metadata.broker.list", brokers, errstr) != RdKafka::Conf::CONF_OK) {
            std::cerr << RED << "WindFlow Error: setting metadata.broker.list in Kafka_Source failed, error: " << errstr << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        if (conf->set("rebalance_cb", &ex_rebalance_cb, errstr) != RdKafka::Conf::CONF_OK) {
            std::cerr << RED << "WindFlow Error: in setting rebalance_cb in Kafka_Source failed, error: " << errstr << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        if (conf->set("group.id", groupid, errstr) != RdKafka::Conf::CONF_OK) {
            std::cerr << RED << "WindFlow Error: setting group.id in Kafka_Source failed, error: " << errstr << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        if (conf->set("partition.assignment.strategy", strat, errstr) != RdKafka::Conf::CONF_OK) {
            std::cerr << RED << "WindFlow Error: setting partition.assignment.strategy failed in Kafka_Source, error: " << errstr << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        consumer = RdKafka::KafkaConsumer::create(conf, errstr); // create consumer object
        if (!consumer) {
            std::cerr << RED << "WindFlow Error: creating the consumer object in Kafka_Source failed, error: " << errstr << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        RdKafka::ErrorCode err = consumer->subscribe(topics); // subscribe to the topics
        if (err) {
            std::cerr << RED << "WindFlow Error: subscribing to the topics in Kafka_Source failed, error: " << errstr << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        kafka_context.setConsumer(consumer);
        consumer->poll(0);
        shipper->setInitialTime(current_time_usecs()); // set the initial time
        pthread_barrier_wait(bar); // barrier with the other replicas
        return Basic_Replica::svc_init();
    }

    // svc (utilized by the FastFlow runtime)
    void *svc(void *) override
    {
        bool run = true; // running flag
        while (run) { // loop until run becomes false
            RdKafka::Message *msg = consumer->consume(idleTime); // consume a new message
            switch (msg->err()) {
                case RdKafka::ERR__TIMED_OUT: // timeout, no message
                    if constexpr (isNonRiched) {
                        run = func(std::nullopt, *shipper);
                    }
                    if constexpr (isRiched) {
                        run = func(std::nullopt, *shipper, kafka_context);
                    }
                    break;
                case RdKafka::ERR_NO_ERROR: // got a new message
                    if constexpr (isNonRiched) {
                        run = func(*msg, *shipper);
                    }
                    if constexpr (isRiched) {
                        run = func(*msg, *shipper, kafka_context);
                    }
                    break;
            }
            delete msg;
        }
        kafka_closing_func(kafka_context); // call the closing function
        shipper->flush(); // call the flush of the shipper
        this->terminated = true;
#if defined (WF_TRACING_ENABLED)
        (this->stats_record).setTerminated();
#endif
        return this->EOS; // end-of-stream
    }

    // eosnotify (utilized by the FastFlow runtime)
    void eosnotify(ssize_t id) override {} // no actions here (it is a Kafka_Source)

    // svc_end (utilized by the FastFlow runtime)
    void svc_end() override
    {
        consumer->close(); // close the consumer
        RdKafka::wait_destroyed(5000); // waiting for RdKafka to decommission
    }

    // Set the emitter used to route outputs generated by the Kafka_Source replica
    void setEmitter(Basic_Emitter *_emitter)
    {
        // if a shipper already exists, it is destroyed
        if (shipper != nullptr) {
            delete shipper;
        }
        shipper = new Source_Shipper<result_t>(_emitter, this, this->execution_mode, time_policy); // create the shipper
        shipper->setInitialTime(current_time_usecs()); // set the initial time
#if defined (WF_TRACING_ENABLED)
        shipper->setStatsRecord(&(this->stats_record));
#endif
    }

    // Set the execution and time mode of the Kafka_Source replica
    void setConfiguration(Execution_Mode_t _execution_mode, Time_Policy_t _time_policy)
    {
        this->setExecutionMode(_execution_mode);
        time_policy = _time_policy;
        if (shipper != nullptr) {
            shipper->setConfiguration(_execution_mode, time_policy);
        }
    }

    KafkaSource_Replica(KafkaSource_Replica &&) = delete; ///< Move constructor is deleted
    KafkaSource_Replica &operator=(const KafkaSource_Replica &) = delete; ///< Copy assignment operator is deleted
    KafkaSource_Replica &operator=(KafkaSource_Replica &&) = delete; ///< Move assignment operator is deleted
};

//@endcond

/** 
 *  \class Kafka_Source
 *  
 *  \brief Kafka_Source operator
 *  
 *  This class implements the Kafka_Source operator able to generate a stream of outputs
 *  all having the same type, by reading messages from Apache Kafka.
 */ 
template<typename kafka_deser_func_t>
class Kafka_Source: public Basic_Operator
{
private:
    friend class MultiPipe;
    friend class PipeGraph;
    kafka_deser_func_t func; // functional logic to deserialize messages from Apache Kafka
    using result_t = decltype(get_result_t_KafkaSource(func)); // extracting the result_t type and checking the admissible signatures
    std::vector<KafkaSource_Replica<kafka_deser_func_t>*> replicas; // vector of pointers to the replicas of the Kafka_Source
    pthread_barrier_t *bar; // pointer to a barrier used to synchronize the Kafka_Source replicas
    static constexpr op_type_t op_type = op_type_t::SOURCE;

    // Configure the Kafka_Source to receive batches instead of individual inputs (cannot be called for the Kafka_Source)
    void receiveBatches(bool _input_batching) override
    {
        abort(); // <-- this method cannot be used!
    }

    // Set the emitter used to route outputs from the Kafka_Source
    void setEmitter(Basic_Emitter *_emitter) override
    {
        replicas[0]->setEmitter(_emitter);
        for (size_t i=1; i<replicas.size(); i++) {
            replicas[i]->setEmitter(_emitter->clone());
        }
    }

    // Check whether the Kafka_Source has terminated
    bool isTerminated() const override
    {
        bool terminated = true;
        for(auto *r: replicas) { // scan all the replicas to check their termination
            terminated = terminated && r->isTerminated();
        }
        return terminated;
    }

    // Set the execution mode and the time policy of the Kafka_Source
    void setConfiguration(Execution_Mode_t _execution_mode, Time_Policy_t _time_policy)
    {
        if (this->getOutputBatchSize() > 0 && _execution_mode != Execution_Mode_t::DEFAULT) {
            std::cerr << RED << "WindFlow Error: Kafka_Source is trying to produce a batch in non DEFAULT mode" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        for(auto *r: replicas) {
            r->setConfiguration(_execution_mode, _time_policy);
        }
    }

#if defined (WF_TRACING_ENABLED)
    // Append the statistics (JSON format) of the Kafka_Source to a PrettyWriter
    void appendStats(rapidjson::PrettyWriter<rapidjson::StringBuffer> &writer) const override
    {
        writer.StartObject(); // create the header of the JSON file
        writer.Key("Operator_name");
        writer.String((this->name).c_str());
        writer.Key("Operator_type");
        writer.String("Kafka_Source");
        writer.Key("Distribution");
        writer.String("NONE");
        writer.Key("isTerminated");
        writer.Bool(this->isTerminated());
        writer.Key("isWindowed");
        writer.Bool(false);
        writer.Key("isGPU");
        writer.Bool(false);
        writer.Key("Parallelism");
        writer.Uint(this->parallelism);
        writer.Key("OutputBatchSize");
        writer.Uint(this->outputBatchSize);
        writer.Key("Replicas");
        writer.StartArray();
        for (auto *r: replicas) { // append the statistics from all the replicas of the Kafka_Source
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
     *  \param _func deserialization logic of the Kafak_Source (a function or any callable type)
     *  \param _name name of the Kafka Source
     *  \param _outBatchSize size (in num of tuples) of the batches produced by this operator (0 for no batching)
     *  \param _brokers ip addresses of the kafka brokers (concatenated by commas)
     *  \param _topics vector of strings representing the names of the topics to subscribe
     *  \param _groupid name of the group id (all replicas are in the same group)
     *  \param _strat strategy used to assign the partitions of the topics among the Kafka_Source replicas
     *  \param _idleTime interval in milliseconds used as a timeout
     *  \param _parallelism internal parallelism of the Kafka_Source
     *  \param _offsets vector of offsets one per topic
     *  \param _kafka_closing_func closing functional logic of the Kafka_Source (a function or any callable type)
     */ 
    Kafka_Source(kafka_deser_func_t _func,
                 std::string _name,
                 size_t _outputBatchSize,
                 std::string _brokers,
                 std::vector<std::string> _topics,
                 std::string _groupid, //merge group-id
                 std::string _strat,
                 int _idleTime,
                 int32_t _parallelism,
                 std::vector<int> _offsets,
                 std::function<void(KafkaRuntimeContext &)> _kafka_closing_func):
                 Basic_Operator(_parallelism, _name, Routing_Mode_t::NONE, _outputBatchSize),
                 func(_func)
    {
        if (_offsets.size() < _topics.size()) {
            std::cerr << RED << "WindFlow Error: number of offsets (used by Kafka_Source) are less than the number of topics" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        bar = new pthread_barrier_t();
        pthread_barrier_init(bar, NULL, this->parallelism);
        for (size_t i=0; i<this->parallelism; i++) { // create the internal replicas of the Kafka_Source
            replicas.push_back(new KafkaSource_Replica<kafka_deser_func_t>(_func, this->name, KafkaRuntimeContext(this->parallelism, i), this->outputBatchSize, _brokers, _topics, _groupid, _strat, this->parallelism, _idleTime, _offsets, bar, _kafka_closing_func));
        }
    }

    /// Copy constructor
    Kafka_Source(const Kafka_Source &_other):
                 Basic_Operator(_other),
                 func(_other.func)
    {
        bar = new pthread_barrier_t();
        pthread_barrier_init(bar, NULL, this->parallelism);       
        for (size_t i=0; i<this->parallelism; i++) { // deep copy of the pointers to the Kafka_Source replicas
            replicas.push_back(new KafkaSource_Replica<kafka_deser_func_t>(*(_other.replicas[i])));
        }
        for (auto *r: replicas) {
            r->bar = bar;
        }
    }

    // Destructor
    ~Kafka_Source() override
    {
        for (auto *r: replicas) { // delete all the replicas
            delete r;
        }
        if (bar != nullptr) {
            pthread_barrier_destroy(bar); // destroy the barrier
            delete bar; // deallocate the barrier
        }
    }

    /** 
     *  \brief Get the type of the Kafka_Source as a string
     *  \return type of the Kafka_Source
     */ 
    std::string getType() const override
    {
        return std::string("Kafka_Source");
    }

    Kafka_Source(Kafka_Source &&) = delete; ///< Move constructor is deleted
    Kafka_Source &operator=(const Kafka_Source &) = delete; ///< Copy assignment operator is deleted
    Kafka_Source &operator=(Kafka_Source &&) = delete; ///< Move assignment operator is deleted
};

} // namespace wf

#endif
