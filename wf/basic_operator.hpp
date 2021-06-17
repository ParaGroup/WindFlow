/******************************************************************************
 *  This program is free software; you can redistribute it and/or modify it
 *  under the terms of the GNU Lesser General Public License version 3 as
 *  published by the Free Software Foundation.
 *  
 *  This program is distributed in the hope that it will be useful, but WITHOUT
 *  ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 *  FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public
 *  License for more details.
 *  
 *  You should have received a copy of the GNU Lesser General Public License
 *  along with this program; if not, write to the Free Software Foundation,
 *  Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 ******************************************************************************
 */

/** 
 *  @file    basic_operator.hpp
 *  @author  Gabriele Mencagli
 *  
 *  @brief Abstract base class of the generic operator in WindFlow
 *  
 *  @section Basic Operator (Description)
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
