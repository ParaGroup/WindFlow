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
 *    * MIT License: https://github.com/ParaGroup/WindFlow/blob/master/LICENSE.MIT
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
 *  @file    basic_operator.hpp
 *  @author  Gabriele Mencagli
 *  
 *  @brief Abstract base class of the generic operator in WindFlow
 *  
 *  @section Basic_Operator (Description)
 *  
 *  Abstract base class of the generic operator in WindFlow. All operators
 *  in the library extend this abstract class.
 */ 

#ifndef BASIC_OP_H
#define BASIC_OP_H

/// includes
#include<vector>
#include<ff/multinode.hpp>
#include<basic.hpp>
#include<basic_emitter.hpp>
#if defined (WF_TRACING_ENABLED)
    #include<stats_record.hpp>
    #include<rapidjson/prettywriter.h>
#endif

namespace wf {

//@cond DOXY_IGNORE

// class Basic_Replica
class Basic_Replica: public ff::ff_monode
{
protected:
    std::string opName; // name of the operator containing the replica
    bool input_batching; // if true, the operator replica expects to receive batches instead of individual inputs
    RuntimeContext context; // RuntimeContext object
    std::function<void(RuntimeContext &)> closing_func; // closing functional logic used by the operator replica
    bool terminated; // true if the operator replica has finished its work
    Basic_Emitter *emitter; // pointer to the used emitter
    size_t dropped_inputs; // number of "consecutive" dropped inputs
    uint64_t last_time_punct; // last time used to send punctuations
    Execution_Mode_t execution_mode; // execution mode of the operator replica
    bool isWinOP; // true if the replica belongs to a window-based operator
    bool isGpuOP; // true if the replica belongs to a GPU operator
    doEmit_t doEmit = nullptr; // pointer to the doEmit method of the Emitter
    doEmit_inplace_t doEmit_inplace = nullptr; // pointer to the doEmit_inplace method of the Emitter
#if defined (WF_TRACING_ENABLED)
    Stats_Record stats_record;
    double avg_td_us = 0;
    double avg_ts_us = 0;
    volatile uint64_t startTD, startTS, endTD, endTS;
#endif

    // Constructor I (used by CPU operators)
    Basic_Replica(std::string _opName,
                  RuntimeContext _context,
                  std::function<void(RuntimeContext &)> _closing_func,
                  bool _isWinOP):
                  opName(_opName),
                  input_batching(false),
                  context(_context),
                  closing_func(_closing_func),
                  terminated(false),
                  emitter(nullptr),
                  dropped_inputs(0),
                  last_time_punct(0),
                  execution_mode(Execution_Mode_t::DEFAULT),
                  isWinOP(_isWinOP),
                  isGpuOP(false) {}

    // Constructor II (used by GPU operators)
    Basic_Replica(std::string _opName,
                  bool _isWinOP):
                  opName(_opName),
                  input_batching(true),
                  terminated(false),
                  emitter(nullptr),
                  dropped_inputs(0),
                  last_time_punct(0),
                  execution_mode(Execution_Mode_t::DEFAULT),
                  isWinOP(_isWinOP),
                  isGpuOP(true) {}

    // Copy Constructor
    Basic_Replica(const Basic_Replica &_other):
                  opName(_other.opName),
                  input_batching(_other.input_batching),
                  context(_other.context),
                  closing_func(_other.closing_func),
                  terminated(_other.terminated),
                  dropped_inputs(_other.dropped_inputs),
                  last_time_punct(_other.last_time_punct),
                  execution_mode(_other.execution_mode),
                  isWinOP(_other.isWinOP),
                  isGpuOP(_other.isGpuOP)
    {
        if (_other.emitter == nullptr) {
            emitter = nullptr;
        }
        else {
            emitter = (_other.emitter)->clone(); // clone the emitter if it exists
            doEmit = emitter->get_doEmit();
            doEmit_inplace = emitter->get_doEmit_inplace();
        }
#if defined (WF_TRACING_ENABLED)
        stats_record = _other.stats_record;
#endif
    }

    // startStatsRecording method
    void startStatsRecording()
    {
#if defined (WF_TRACING_ENABLED)
        startTS = current_time_nsecs();
        if (stats_record.inputs_received == 0) {
            startTD = current_time_nsecs();
        }
#endif
    }

    // endStatsRecording method
    void endStatsRecording()
    {
#if defined (WF_TRACING_ENABLED)
        endTS = current_time_nsecs();
        endTD = current_time_nsecs();
        double elapsedTS_us = ((double) (endTS - startTS)) / 1000;
        avg_ts_us += (1.0 / stats_record.inputs_received) * (elapsedTS_us - avg_ts_us);
        double elapsedTD_us = ((double) (endTD - startTD)) / 1000;
        avg_td_us += (1.0 / stats_record.inputs_received) * (elapsedTD_us - avg_td_us);
        stats_record.service_time = std::chrono::duration<double, std::micro>(avg_ts_us);
        stats_record.eff_service_time = std::chrono::duration<double, std::micro>(avg_td_us);
        startTD = current_time_nsecs();
#endif
    }

public:
    // Destructor
    virtual ~Basic_Replica()
    {
        if (emitter != nullptr) {
            delete emitter;
        }
    }

    // svc_init (utilized by the FastFlow runtime)
    int svc_init() override
    {
#if defined (WF_TRACING_ENABLED)
        stats_record = Stats_Record(opName, std::to_string(context.getReplicaIndex()), isWinOP, isGpuOP);
#endif
        last_time_punct = current_time_usecs();
        return 0;
    }

    // EOS management (utilized by the FastFlow runtime)
    void eosnotify(ssize_t id) override
    {
        if (emitter != nullptr) {
            emitter->flush(this); // call the flush of the emitter
        }
        terminated = true;
#if defined (WF_TRACING_ENABLED)
        stats_record.setTerminated();
#endif
    }

    // svc_end (utilized by the FastFlow runtime)
    void svc_end() override
    {
        closing_func(context); // call the closing function
    }

    // Configure the operator replica to receive batches instead of individual inputs
    virtual void receiveBatches(bool _input_batching)
    {
        input_batching = _input_batching;
    }

    // Set the emitter used to route outputs from the operator replica
    virtual void setEmitter(Basic_Emitter *_emitter)
    {
        assert(_emitter != nullptr); // sanity check
        emitter = _emitter;
        doEmit = emitter->get_doEmit();
        doEmit_inplace = emitter->get_doEmit_inplace();
    }

    // Check the termination of the operator replica
    bool isTerminated() const
    {
        return terminated;
    }

    // Set the execution mode of the operator replica
    void setExecutionMode(Execution_Mode_t _execution_mode)
    {
        execution_mode = _execution_mode;
    }

#if defined (WF_TRACING_ENABLED)
    // Get a copy of the Stats_Record of the operator replica
    Stats_Record getStatsRecord() const
    {
        return stats_record;
    }
#endif

    Basic_Replica(Basic_Replica &&) = delete; ///< Move constructor is deleted
    Basic_Replica &operator=(const Basic_Replica &) = delete; ///< Copy assignment operator is deleted
    Basic_Replica &operator=(Basic_Replica &&) = delete; ///< Move assignment operator is deleted
};

//@endcond

/** 
 *  \class Basic_Operator
 *  
 *  \brief Abstract base class of the generic operator in WindFlow
 *  
 *  Abstract base class extended by all operators in the library.
 */ 
class Basic_Operator
{
protected:
    friend class MultiPipe;
    friend class PipeGraph;
    size_t parallelism; // parallelism of the operator
    std::string name; // name of the operator
    Routing_Mode_t input_routing_mode; // routing mode used to deliver inputs to the operator
    size_t outputBatchSize; // batch size of the outputs produced by the operator
    bool isGpuOP; // true if the operator targets GPU (false otherwise)

    // Constructor
    Basic_Operator(size_t _parallelism,
                   std::string _name,
                   Routing_Mode_t _input_routing_mode,
                   size_t _outputBatchSize,
                   bool _isGpuOP=false):
                   parallelism(_parallelism),
                   name(_name),
                   input_routing_mode(_input_routing_mode),
                   outputBatchSize(_outputBatchSize),
                   isGpuOP(_isGpuOP)
    {
        if (parallelism == 0) { // check the validity of the parallelism value
            std::cerr << RED << "WindFlow Error: operator has parallelism zero" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
    }

    // Copy Constructor
    Basic_Operator(const Basic_Operator &_other):
                   parallelism(_other.parallelism),
                   name(_other.name),
                   input_routing_mode(_other.input_routing_mode),
                   outputBatchSize(_other.outputBatchSize),
                   isGpuOP(_other.isGpuOP) {}

    // Configure the operator to receive batches instead of individual inputs
    virtual void receiveBatches(bool) = 0;

    // Set the emitter used to route outputs from the operator
    virtual void setEmitter(Basic_Emitter *) = 0;

    // Check whether the operator has terminated
    virtual bool isTerminated() const = 0;

#if defined (WF_TRACING_ENABLED)
    // Dump the log file (JSON format) of statistics of the operator
    virtual void dumpStats() const
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
        this->appendStats(writer); // append the statistics of the Filter
        logfile << buffer.GetString();
        logfile.close();
    }

    // Append the statistics (JSON format) of the operator to a PrettyWriter
    virtual void appendStats(rapidjson::PrettyWriter<rapidjson::StringBuffer> &) const = 0;
#endif

public:
    /// Destructor
    virtual ~Basic_Operator() = default;

    /** 
     *  \brief Get the type of the operator as a string
     *  \return type of the operator
     */ 
    virtual std::string getType() const = 0;

    /** 
     *  \brief Get the name of the operator as a string
     *  \return name of the operator
     */ 
    std::string getName() const
    {
        return name;
    }

    /** 
     *  \brief Get the total parallelism of the operator
     *  \return total parallelism of the operator
     */  
    size_t getParallelism() const
    {
        return parallelism;
    }

    /** 
     *  \brief Return the input routing mode of the operator
     *  \return routing mode used to deliver inputs to the operator
     */ 
    Routing_Mode_t getInputRoutingMode() const
    {
        return input_routing_mode;
    }

    /** 
     *  \brief Return the size of the output batches that the operator should produce
     *  \return output batch size in number of tuples
     */ 
    size_t getOutputBatchSize() const
    {
        return outputBatchSize;
    }

    /** 
     *  \brief Check whether the operator is for GPU
     *  \return true if the operator targets a GPU device, false otherwise
     */ 
    bool isGPUOperator() const
    {
        return isGpuOP;
    }

    Basic_Operator(Basic_Operator &&) = delete; ///< Move constructor is deleted
    Basic_Operator &operator=(const Basic_Operator &) = delete; ///< Copy assignment operator is deleted
    Basic_Operator &operator=(Basic_Operator &&) = delete; ///< Move assignment operator is deleted
};

} // namespace wf

#endif
