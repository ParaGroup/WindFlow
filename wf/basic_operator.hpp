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
 *  @file    basic_operator.hpp
 *  @author  Gabriele Mencagli
 *  
 *  @brief Abstract base class of the generic operator in WindFlow
 *  
 *  @section Basic_Operator (Description)
 *  
 *  Abstract base class of the generic operator in WindFlow. All the operators
 *  in the library extend this abstract class.
 */ 

#ifndef BASIC_OP_H
#define BASIC_OP_H

/// includes
#include<vector>
#include<basic.hpp>
#include<basic_emitter.hpp>
#if defined (WF_TRACING_ENABLED)
    #include<stats_record.hpp>
    #include<rapidjson/prettywriter.h>
#endif

namespace wf {

/** 
 *  \class Basic_Operator
 *  
 *  \brief Abstract base class of the generic operator in WindFlow
 *  
 *  Abstract base class extended by all the operators in the library.
 */ 
class Basic_Operator
{
private:
    friend class MultiPipe; // friendship with the MultiPipe class
    friend class PipeGraph; // friendship with the PipeGraph class

    // Configure the operator to receive batches instead of individual inputs
    virtual void receiveBatches(bool) = 0;

    // Set the emitter used to route outputs from the operator
    virtual void setEmitter(Basic_Emitter *) = 0;

    // Check whether the operator has terminated
    virtual bool isTerminated() const = 0;

#if defined (WF_TRACING_ENABLED)
    // Dump the log file (JSON format) of statistics of the operator
    virtual void dumpStats() const = 0;

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
    virtual std::string getName() const = 0;

    /** 
     *  \brief Get the total parallelism of the operator
     *  \return total parallelism of the operator
     */ 
    virtual size_t getParallelism() const = 0;

    /** 
     *  \brief Return the input routing mode of the operator
     *  \return routing mode used to send inputs to the operator
     */ 
    virtual Routing_Mode_t getInputRoutingMode() const = 0;

    /** 
     *  \brief Return the size of the output batches that the operator should produce
     *  \return output batch size in number of tuples
     */ 
    virtual size_t getOutputBatchSize() const = 0;

    /** 
     *  \brief Check whether the operator is for GPU
     *  \return true if the operator targets a GPU device, false otherwise
     */ 
    virtual bool isGPUOperator() const { return false; }
};

} // namespace wf

#endif
