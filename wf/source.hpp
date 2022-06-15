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
 *  @file    source.hpp
 *  @author  Gabriele Mencagli
 *  
 *  @brief Source operator
 *  
 *  @section Source (Description)
 *  
 *  This file implements the Source operator able to generate output streams
 */ 

#ifndef SOURCE_H
#define SOURCE_H

/// includes
#include<string>
#include<functional>
#include<context.hpp>
#include<source_shipper.hpp>
#if defined (WF_TRACING_ENABLED)
    #include<stats_record.hpp>
#endif
#include<basic_emitter.hpp>
#include<basic_operator.hpp>

namespace wf {

//@cond DOXY_IGNORE

// class Source_Replica
template<typename source_func_t>
class Source_Replica: public ff::ff_monode
{
private:
    source_func_t func; // functional logic used by the Source replica
    using result_t = decltype(get_result_t_Source(func)); // extracting the result_t type and checking the admissible signatures
    // static predicates to check the type of the functional logic to be invoked
    static constexpr bool isNonRiched = std::is_invocable<decltype(func), Source_Shipper<result_t> &>::value;
    static constexpr bool isRiched = std::is_invocable<decltype(func), Source_Shipper<result_t> &, RuntimeContext &>::value;
    // check the presence of a valid functional logic
    static_assert(isNonRiched || isRiched,
        "WindFlow Compilation Error - Source_Replica does not have a valid functional logic:\n");
    std::string opName; // name of the Source containing the replica
    RuntimeContext context; // RuntimeContext object
    std::function<void(RuntimeContext &)> closing_func; // closing functional logic used by the Source replica
    bool terminated; // true if the Source replica has finished its work
    Execution_Mode_t execution_mode;// execution mode of the Source replica
    Time_Policy_t time_policy; // time policy of the Source replica
    Source_Shipper<result_t> *shipper; // pointer to the shipper object used by the Source replica to send outputs
#if defined (WF_TRACING_ENABLED)
    Stats_Record stats_record;
#endif

public:
    // Constructor
    Source_Replica(source_func_t _func,
                   std::string _opName,
                   RuntimeContext _context,
                   std::function<void(RuntimeContext &)> _closing_func):
                   func(_func),
                   opName(_opName),
                   context(_context),
                   closing_func(_closing_func),
                   terminated(false),
                   execution_mode(Execution_Mode_t::DEFAULT),
                   time_policy(Time_Policy_t::INGRESS_TIME),
                   shipper(nullptr) {}

    // Copy Constructor
    Source_Replica(const Source_Replica &_other):
                   func(_other.func),
                   opName(_other.opName),
                   context(_other.context),
                   closing_func(_other.closing_func),
                   terminated(_other.terminated),
                   execution_mode(_other.execution_mode),
                   time_policy(_other.time_policy)
    {
        if (_other.shipper != nullptr) {
            shipper = new Source_Shipper<decltype(get_result_t_Source(func))>(*(_other.shipper));
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
    Source_Replica(Source_Replica &&_other):
                   func(std::move(_other.func)),
                   opName(std::move(_other.opName)),
                   context(std::move(_other.context)),
                   closing_func(std::move(_other.closing_func)),
                   terminated(_other.terminated),
                   execution_mode(_other.execution_mod),
                   time_policy(_other.time_policy)
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
    ~Source_Replica()
    {
        if (shipper != nullptr) {
            delete shipper;
        }
    }

    // Copy Assignment Operator
    Source_Replica &operator=(const Source_Replica &_other)
    {
        if (this != &_other) {
            func = _other.func;
            opName = _other.opName;
            context = _other.context;
            closing_func = _other.closing_func;
            terminated = _other.terminated;
            execution_mode = _other.execution_mode;
            time_policy = _other.time_policy;
            if (shipper != nullptr) {
                delete shipper;
            }
            if (_other.shipper != nullptr) {
                shipper = new Source_Shipper<decltype(get_result_t_Source(func))>(*(_other.shipper));
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
    Source_Replica &operator=(Source_Replica &_other)
    {
        func = std::move(_other.func);
        opName = std::move(_other.opName);
        context = std::move(_other.context);
        closing_func = std::move(_other.closing_func);
        terminated = _other.terminated;
        execution_mode = _other.execution_mode;
        time_policy = _other.time_policy;
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
#if defined (WF_TRACING_ENABLED)
        stats_record = Stats_Record(opName, std::to_string(context.getReplicaIndex()), false, false);
#endif
        shipper->setInitialTime(current_time_usecs()); // set the initial time
        return 0;
    }

    // svc (utilized by the FastFlow runtime)
    void *svc(void *) override
    {
        if constexpr (isNonRiched) { // non-riched version
            func(*shipper);
        }
        if constexpr (isRiched)  { // riched version
            func(*shipper, context);
        }
        shipper->flush(); // call the flush of the shipper
        terminated = true;
#if defined (WF_TRACING_ENABLED)
        stats_record.setTerminated();
#endif
        return this->EOS; // end-of-stream
    }

    // svc_end (utilized by the FastFlow runtime)
    void svc_end() override
    {
        closing_func(context); // call the closing function
    }

    // Set the emitter used to route outputs generated by the Source replica
    void setEmitter(Basic_Emitter *_emitter)
    {
        // if a shipper already exists, it is destroyed
        if (shipper != nullptr) {
            delete shipper;
        }
        shipper = new Source_Shipper<decltype(get_result_t_Source(func))>(_emitter, this, execution_mode, time_policy); // create the shipper
        shipper->setInitialTime(current_time_usecs()); // set the initial time
#if defined (WF_TRACING_ENABLED)
        shipper->setStatsRecord(&stats_record);
#endif
    }

    // Check the termination of the Source replica
    bool isTerminated() const
    {
        return terminated;
    }

    // Set the execution and time mode of the Source replica
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
    // Get a copy of the Stats_Record of the Source replica
    Stats_Record getStatsRecord() const
    {
        return stats_record;
    }
#endif
};

//@endcond

/** 
 *  \class Source
 *  
 *  \brief Source operator
 *  
 *  This class implements the Source operator able to generate a stream of outputs
 *  all having the same type.
 */ 
template<typename source_func_t>
class Source: public Basic_Operator
{
private:
    friend class MultiPipe; // friendship with the MultiPipe class
    friend class PipeGraph; // friendship with the PipeGraph class
    source_func_t func; // functional logic used by the Source
    using result_t = decltype(get_result_t_Source(func)); // extracting the result_t type and checking the admissible signatures
    size_t parallelism; // parallelism of the Source
    std::string name; // name of the Source
    size_t outputBatchSize; // batch size of the outputs produced by the Source
    std::vector<Source_Replica<source_func_t>*> replicas; // vector of pointers to the replicas of the Source

    // Configure the Source to receive batches instead of individual inputs (cannot be called for the Source)
    void receiveBatches(bool _input_batching) override
    {
        abort(); // <-- this method cannot be used!
    }

    // Set the emitter used to route outputs from the Source
    void setEmitter(Basic_Emitter *_emitter) override
    {
        replicas[0]->setEmitter(_emitter);
        for (size_t i=1; i<replicas.size(); i++) {
            replicas[i]->setEmitter(_emitter->clone());
        }   
    }

    // Check whether the Source has terminated 
    bool isTerminated() const override
    {
        bool terminated = true;
        for(auto *r: replicas) { // scan all the replicas to check their termination
            terminated = terminated && r->isTerminated();
        }
        return terminated;
    }

    // Set the execution mode and the time policy of the Source (i.e., the ones of its PipeGraph)
    void setConfiguration(Execution_Mode_t _execution_mode,
                          Time_Policy_t _time_policy)
    {
        for(auto *r: replicas) {
            r->setConfiguration(_execution_mode, _time_policy);
        }
    }

#if defined (WF_TRACING_ENABLED)
    // Dump the log file (JSON format) of statistics of the Source
    void dumpStats() const override
    {
        std::ofstream logfile; // create and open the log file in the WF_LOG_DIR directory
#if defined (WF_LOG_DIR)
        std::string log_dir = std::string(STRINGIFY(WF_LOG_DIR));
        std::string filename = std::string(STRINGIFY(WF_LOG_DIR)) + "/" + std::to_string(getpid()) + "_" + name + ".json";
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
        this->appendStats(writer); // append the statistics of the Source
        logfile << buffer.GetString();
        logfile.close();
    }

    // Append the statistics (JSON format) of the Source to a PrettyWriter
    void appendStats(rapidjson::PrettyWriter<rapidjson::StringBuffer> &writer) const override
    {
        writer.StartObject(); // create the header of the JSON file
        writer.Key("Operator_name");
        writer.String(name.c_str());
        writer.Key("Operator_type");
        writer.String("Source");
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
        for (auto *r: replicas) { // append the statistics from all the replicas of the Source
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
     *  \param _func functional logic of the Source (a function or a callable type)
     *  \param _parallelism internal parallelism of the Source
     *  \param _name name of the Source
     *  \param _outputBatchSize size (in num of tuples) of the batches produced by this operator (0 for no batching)
     *  \param _closing_func closing functional logic of the Source (a function or callable type)
     */ 
    Source(source_func_t _func,
           size_t _parallelism,
           std::string _name,
           size_t _outputBatchSize,
           std::function<void(RuntimeContext &)> _closing_func):
           func(_func),
           parallelism(_parallelism),
           name(_name),
           outputBatchSize(_outputBatchSize)
    {
        if (parallelism == 0) { // check the validity of the parallelism value
            std::cerr << RED << "WindFlow Error: Source has parallelism zero" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        for (size_t i=0; i<parallelism; i++) { // create the internal replicas of the Source
            replicas.push_back(new Source_Replica<source_func_t>(_func, name, RuntimeContext(parallelism, i), _closing_func));
        }
    }

    /// Copy constructor
    Source(const Source &_other):
           func(_other.func),
           parallelism(_other.parallelism),
           name(_other.name),
           outputBatchSize(_other.outputBatchSize)
    {
        for (size_t i=0; i<parallelism; i++) { // deep copy of the pointers to the Source replicas
            replicas.push_back(new Source_Replica<source_func_t>(*(_other.replicas[i])));
        }
    }

    // Destructor
    ~Source() override
    {
        for (auto *r: replicas) { // delete all the replicas
            delete r;
        }
    }

    /// Copy assignment operator
    Source& operator=(const Source &_other)
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
            for (size_t i=0; i<parallelism; i++) { // deep copy of the pointers to the Source replicas
                replicas.push_back(new Source_Replica<source_func_t>(*(_other.replicas[i])));
            }
        }
        return *this;
    }

    /// Move assignment operator
    Source& operator=(Source &&_other)
    {
        func = std::move(_other.func);
        parallelism = _other.parallelism;
        name = std::move(_other.name);
        outputBatchSize = _other.outputBatchSize;
        for (auto *r: replicas) { // delete all the replicas
            delete r;
        }
        replicas = std::move(_other.replicas);
        return *this;
    }

    /** 
     *  \brief Get the type of the Source as a string
     *  \return type of the Source
     */ 
    std::string getType() const override
    {
        return std::string("Source");
    }

    /** 
     *  \brief Get the name of the Source as a string
     *  \return name of the Source
     */ 
    std::string getName() const override
    {
        return name;
    }

    /** 
     *  \brief Get the total parallelism of the Source
     *  \return total parallelism of the Source
     */  
    size_t getParallelism() const override
    {
        return parallelism;
    }

    /** 
     *  \brief Return the input routing mode of the Source
     *  \return routing mode used to send inputs to the Source
     */ 
    Routing_Mode_t getInputRoutingMode() const override
    {
        return Routing_Mode_t::NONE;
    }

    /** 
     *  \brief Return the size of the output batches that the Source should produce
     *  \return output batch size in number of tuples
     */ 
    size_t getOutputBatchSize() const override
    {
        return outputBatchSize;
    }
};

} // namespace wf

#endif
