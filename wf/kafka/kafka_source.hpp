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
class KafkaSource_Replica: public ff::ff_monode
{
private:
    template<typename T> friend class Kafka_Source; // friendship with all the instances of the Kafka_Source template
    kafka_deser_func_t func; // logic for deserializing messages from Apache Kafka
    using result_t = decltype(get_result_t_KafkaSource(func)); // extracting the result_t type and checking the admissible signatures
    // static predicates to check the type of the deserialization logic to be invoked
    static constexpr bool isNonRiched = std::is_invocable<decltype(func), std::optional<std::reference_wrapper<RdKafka::Message>>, Source_Shipper<result_t> &>::value;
    static constexpr bool isRiched = std::is_invocable<decltype(func), std::optional<std::reference_wrapper<RdKafka::Message>>, Source_Shipper<result_t> &, KafkaRuntimeContext &>::value;
    // check the presence of a valid deserialization logic
    static_assert(isNonRiched || isRiched,
        "WindFlow Compilation Error - KafkaSource_Replica does not have a valid deserialization logic:\n");
    std::string opName; // name of the Kafka_Source containing the replica
    KafkaRuntimeContext context; // KafkaRuntimeContext object
    std::function<void(KafkaRuntimeContext &)> closing_func; // closing functional logic used by the Kafka_Source replica
    bool terminated; // true if the Kafka_Source replica has finished its work
    Execution_Mode_t execution_mode;// execution mode of the Kafka_Source replica
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
#if defined (WF_TRACING_ENABLED)
    Stats_Record stats_record;
#endif

public:
    // Constructor
    KafkaSource_Replica(kafka_deser_func_t _func,
                        std::string _opName,
                        KafkaRuntimeContext _context,
                        size_t _outputBatchSize,
                        std::string _brokers,
                        std::vector<std::string> _topics,
                        std::string _groupid,
                        std::string _strat,
                        size_t _parallelism,
                        int _idleTime,
                        std::vector<int> _offsets,
                        pthread_barrier_t *_bar,
                        std::function<void(KafkaRuntimeContext &)> _closing_func):
                        func(_func),
                        opName(_opName),
                        context(_context),
                        closing_func(_closing_func),
                        terminated(false),
                        execution_mode(Execution_Mode_t::DEFAULT),
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
                        func(_other.func),
                        opName(_other.opName),
                        context(_other.context),
                        closing_func(_other.closing_func),
                        terminated(_other.terminated),
                        execution_mode(_other.execution_mode),
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
            shipper = new Source_Shipper<decltype(get_result_t_KafkaSource(func))>(*(_other.shipper));
            shipper->node = this; // change the node referred by the shipper
#if defined (WF_TRACING_ENABLED)
            shipper->setStatsRecord(&stats_record); // change the Stats_Record referred by the shipper
#endif
        }
        else {
            shipper = nullptr;
        }
    }

    // Move Constructor
    KafkaSource_Replica(KafkaSource_Replica &&_other):
                        func(std::move(_other.func)),
                        opName(std::move(_other.opName)),
                        context(std::move(_other.context)),
                        closing_func(std::move(_other.closing_func)),
                        terminated(_other.terminated),
                        execution_mode(_other.execution_mod),
                        time_policy(_other.time_policy),
                        consumer(std::exchange(_other.consumer, nullptr)),
                        conf(std::exchange(_other.conf, nullptr)),
                        idleTime(_other.idleTime),
                        brokers(std::move(_other.brokers)),
                        groupid(std::move(_other.groupid)),
                        strat(std::move(_other.strat)),
                        offsets(std::move(_other.offsets)),
                        parallelism(_other.parallelism),
                        topics(std::move(_other.topics)),
                        bar(std::exchange(_other.bar, nullptr))          
    {
        shipper = std::exchange(_other.shipper, nullptr);
        if (shipper != nullptr) {
            shipper->node = this; // change the node referred by the shipper
#if defined (WF_TRACING_ENABLED)
            shipper->setStatsRecord(&stats_record); // change the Stats_Record referred by the shipper
#endif
        }
    }

    // Destructor
    ~KafkaSource_Replica()
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

    // Copy Assignment Operator
    KafkaSource_Replica &operator=(const KafkaSource_Replica &_other)
    {
        if (this != &_other) {
            func = _other.func;
            opName = _other.opName;
            context = _other.context;
            closing_func = _other.closing_func;
            terminated = _other.terminated;
            execution_mode = _other.execution_mode;
            time_policy = _other.time_policy;
            consumer = nullptr;
            conf = nullptr;
            idleTime = _other.idleTime;
            brokers = _other.brokers;
            groupid = _other.groupid;
            strat = _other.strat;
            offsets = _other.offsets;
            parallelism = _other.parallelism;
            topics = _other.topics;
            bar = _other.bar;
            if (shipper != nullptr) {
                delete shipper;
            }
            if (_other.shipper != nullptr) {
                shipper = new Source_Shipper<decltype(get_result_t_KafkaSource(func))>(*(_other.shipper));
                shipper->node = this; // change the node referred by the shipper
    #if defined (WF_TRACING_ENABLED)
                shipper->setStatsRecord(&stats_record); // change the Stats_Record referred by the shipper
    #endif
            }
            else {
                shipper = nullptr;
            }
        }
        return *this;
    }

    // Move Assignment Operator
    KafkaSource_Replica &operator=(KafkaSource_Replica &_other)
    {
        func = std::move(_other.func);
        opName = std::move(_other.opName);
        context = std::move(_other.context);
        closing_func = std::move(_other.closing_func);
        terminated = _other.terminated;
        execution_mode = _other.execution_mode;
        time_policy = _other.time_policy;
        consumer = std::exchange(_other.consumer, nullptr);
        conf = std::exchange(_other.conf, nullptr);
        idleTime = _other.idleTime;
        brokers = std::move(_other.brokers);
        groupid = std::move(_other.groupid);
        strat = std::move(_other.strat);
        offsets = std::move(_other.offsets);
        parallelism = _other.parallelism;
        topics = std::move(_other.topics);
        bar = std::exchange(_other.bar, nullptr);
        if (shipper != nullptr) {
            delete shipper;
        }
        shipper = std::exchange(_other.shipper, nullptr);
        if (shipper != nullptr) {
            shipper->node = this; // change the node referred by the shipper
#if defined (WF_TRACING_ENABLED)
            shipper->setStatsRecord(&stats_record); // change the Stats_Record referred by the shipper
#endif
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
        context.setConsumer(consumer);
        consumer->poll(0);
#if defined (WF_TRACING_ENABLED)
        stats_record = Stats_Record(opName, std::to_string(context.getReplicaIndex()), false, false);
#endif
        shipper->setInitialTime(current_time_usecs()); // set the initial time
        pthread_barrier_wait(bar); // barrier with the other replicas
        return 0;
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
                        run = func(std::nullopt, *shipper, context);
                    }
                    break;
                case RdKafka::ERR_NO_ERROR: // got a new message
                    if constexpr (isNonRiched) {
                        run = func(*msg, *shipper);
                    }
                    if constexpr (isRiched) {
                        run = func(*msg, *shipper, context);
                    }
                    break;
            }
            delete msg;
        }
        shipper->flush(); // call the flush of the shipper
#if defined (WF_TRACING_ENABLED)
        stats_record.setTerminated();
#endif
        return this->EOS; // end-of-stream
    }

    // svc_end (utilized by the FastFlow runtime)
    void svc_end() override
    {
        consumer->close(); // close the consumer
        RdKafka::wait_destroyed(5000); // waiting for RdKafka to decommission
        closing_func(context); // call the closing function
    }

    // Set the emitter used to route outputs generated by the Kafka_Source replica
    void setEmitter(Basic_Emitter *_emitter)
    {
        // if a shipper already exists, it is destroyed
        if (shipper != nullptr) {
            delete shipper;
        }
        shipper = new Source_Shipper<decltype(get_result_t_KafkaSource(func))>(_emitter, this, execution_mode, time_policy); // create the shipper
        shipper->setInitialTime(current_time_usecs()); // set the initial time
#if defined (WF_TRACING_ENABLED)
        shipper->setStatsRecord(&stats_record);
#endif
    }

    // Check the termination of the Kafka_Source replica
    bool isTerminated() const
    {
        return terminated;
    }

    // Set the execution and time mode of the Kafka_Source replica
    void setConfiguration(Execution_Mode_t _execution_mode,
                          Time_Policy_t _time_policy)
    {
        execution_mode = _execution_mode;
        time_policy = _time_policy;
        if (shipper != nullptr) {
            shipper->setConfiguration(execution_mode, time_policy);
        }
    }

#if defined (WF_TRACING_ENABLED)
    // Get a copy of the Stats_Record of the Kafka_Source replica
    Stats_Record getStatsRecord() const
    {
        return stats_record;
    }
#endif
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
    friend class MultiPipe; // friendship with the MultiPipe class
    friend class PipeGraph; // friendship with the PipeGraph class
    kafka_deser_func_t func; // functional logic to deserialize messages from Apache Kafka
    using result_t = decltype(get_result_t_KafkaSource(func)); // extracting the result_t type and checking the admissible signatures
    size_t parallelism; // parallelism of the Kafka_Source
    std::string name; // name of the Kafka_Source
    size_t outputBatchSize; // batch size of the outputs produced by the Kafka_Source
    std::vector<KafkaSource_Replica<kafka_deser_func_t>*> replicas; // vector of pointers to the replicas of the Kafka_Source
    pthread_barrier_t *bar; // pointer to a barrier used to synchronize the Kafka_Source replicas

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

    // Set the execution mode and the time policy of the Kafka_Source (i.e., the ones of its PipeGraph)
    void setConfiguration(Execution_Mode_t _execution_mode,
                          Time_Policy_t _time_policy)
    {
        for(auto *r: replicas) {
            r->setConfiguration(_execution_mode, _time_policy);
        }
    }

#if defined (WF_TRACING_ENABLED)
    // Dump the log file (JSON format) of statistics of the Kafka_Source
    void dumpStats() const override
    {
        std::ofstream logfile; // create and open the log file in the LOG_DIR directory
#if defined (LOG_DIR)
        std::string log_dir = std::string(STRINGIFY(LOG_DIR));
        std::string filename = std::string(STRINGIFY(LOG_DIR)) + "/" + std::to_string(getpid()) + "_" + name + ".json";
#else
        std::string log_dir = std::string("log");
        std::string filename = "log/" + std::to_string(getpid()) + "_" + name + ".json";
#endif
        if (mkdir(log_dir.c_str(), 0777) != 0) { // create the log directory
            struct stat st;
            if((stat(log_dir.c_str(), &st) != 0) || !S_ISDIR(st.st_mode)) {
                std::cerr << RED << "WindFlow Error: directory for log files cannot be created" << DEFAULT_COLOR << std::endl;
                exit(EXIT_FAILURE);
            }
        }
        logfile.open(filename);
        rapidjson::StringBuffer buffer; // create the rapidjson writer
        rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(buffer);
        this->appendStats(writer); // append the statistics of the Kafka_Source
        logfile << buffer.GetString();
        logfile.close();
    }

    // Append the statistics (JSON format) of the Kafka_Source to a PrettyWriter
    void appendStats(rapidjson::PrettyWriter<rapidjson::StringBuffer> &writer) const override
    {
        writer.StartObject(); // create the header of the JSON file
        writer.Key("Operator_name");
        writer.String(name.c_str());
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
        writer.Uint(parallelism);
        writer.Key("OutputBatchSize");
        writer.Uint(outputBatchSize);
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
     *  \param _func deserialization logic of the Kafak_Source (a function or a callable type)
     *  \param _name name of the Kafka Source
     *  \param _outBatchSize size (in num of tuples) of the batches produced by this operator (0 for no batching)
     *  \param _brokers ip addresses of the kafka brokers (concatenated by commas)
     *  \param _topics vector of strings representing the names of the topics to subscribe
     *  \param _groupid name of the group id (all replicas are in the same group)
     *  \param _strat strategy used to assign the partitions of the topics among the Kafka_Source replicas
     *  \param _idleTime interval in milliseconds used as a timeout
     *  \param _parallelism internal parallelism of the Kafka_Source
     *  \param _offsets vector of offsets one per topic
     *  \param _closing_func closing functional logic of the Kafka_Source (a function or callable type)
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
                 std::function<void(KafkaRuntimeContext &)> _closing_func):
                 func(_func),
                 name(_name),
                 outputBatchSize(_outputBatchSize),
                 parallelism(_parallelism)
    {
        if (parallelism == 0) { // check the validity of the parallelism value
            std::cerr << RED << "WindFlow Error: Kafka_Source has parallelism zero" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        if (_offsets.size() < _topics.size()) {
            std::cerr << RED << "WindFlow Error: number of offsets (used by Kafka_Source) are less than the number of topics" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        bar = new pthread_barrier_t();
        pthread_barrier_init(bar, NULL, parallelism);
        for (size_t i=0; i<parallelism; i++) { // create the internal replicas of the Kafka_Source
            replicas.push_back(new KafkaSource_Replica<kafka_deser_func_t>(_func, name, KafkaRuntimeContext(parallelism, i), outputBatchSize, _brokers, _topics, _groupid, _strat, parallelism, _idleTime, _offsets, bar, _closing_func));
        }
    }

    /// Copy constructor
    Kafka_Source(const Kafka_Source &_other):
                 func(_other.func),
                 parallelism(_other.parallelism),
                 name(_other.name),
                 outputBatchSize(_other.outputBatchSize)
    {
        bar = new pthread_barrier_t();
        pthread_barrier_init(bar, NULL, parallelism);       
        for (size_t i=0; i<parallelism; i++) { // deep copy of the pointers to the Kafka_Source replicas
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

    /// Copy assignment operator
    Kafka_Source& operator=(const Kafka_Source &_other)
    {
        if (this != &_other) {
            func = _other.func;
            parallelism = _other.parallelism;
            name = _other.name;
            outputBatchSize = _other.outputBatchSize;
            for (auto *r: replicas) { // delete all the replicas
                delete r;
            }
            replicas.clear();
            if (bar != nullptr) {
                pthread_barrier_destroy(bar); // destroy the barrier
                delete bar; // deallocate the barrier               
            }
            bar = new pthread_barrier_t();
            pthread_barrier_init(bar, NULL, parallelism);     
            for (size_t i=0; i<parallelism; i++) { // deep copy of the pointers to the Source replicas
                replicas.push_back(new KafkaSource_Replica<kafka_deser_func_t>(*(_other.replicas[i])));
            }
            for (auto *r: replicas) {
                r->bar = bar;
            }            
        }
        return *this;
    }

    /// Move assignment operator
    Kafka_Source& operator=(Kafka_Source &&_other)
    {
        func = std::move(_other.func);
        parallelism = _other.parallelism;
        name = std::move(_other.name);
        outputBatchSize = _other.outputBatchSize;
        for (auto *r: replicas) { // delete all the replicas
            delete r;
        }
        replicas = std::move(_other.replicas);
        if (bar != nullptr) {
            pthread_barrier_destroy(bar); // destroy the barrier
            delete bar; // deallocate the barrier               
        }
        bar = std::exchange(_other.bar, nullptr);
        return *this;
    }

    /** 
     *  \brief Get the type of the Kafka_Source as a string
     *  \return type of the Kafka_Source
     */ 
    std::string getType() const override
    {
        return std::string("Kafka_Source");
    }

    /** 
     *  \brief Get the name of the Kafka_Source as a string
     *  \return name of the Kafka_Source
     */ 
    std::string getName() const override
    {
        return name;
    }

    /** 
     *  \brief Get the total parallelism of the Kafka_Source
     *  \return total parallelism of the Kafka_Source
     */  
    size_t getParallelism() const override
    {
        return parallelism;
    }

    /** 
     *  \brief Return the input routing mode of the Kafka_Source
     *  \return routing mode used to send inputs to the Kafka_Source
     */ 
    Routing_Mode_t getInputRoutingMode() const override
    {
        return Routing_Mode_t::NONE;
    }

    /** 
     *  \brief Return the size of the output batches that the Kafka_Source should produce
     *  \return output batch size in number of tuples
     */ 
    size_t getOutputBatchSize() const override
    {
        return outputBatchSize;
    }
};

} // namespace wf

#endif
