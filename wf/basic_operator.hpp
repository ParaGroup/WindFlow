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
 *  @date    20/04/2020
 *  
 *  @brief Abstract class of the generic operator in WindFlow
 *  
 *  @section Basic Operator (Description)
 *  
 *  Abstract class of the generic operator in WindFlow. All the operators
 *  in the library extend this abstract class.
 */ 

#ifndef BASIC_OP_H
#define BASIC_OP_H

/// includes
#include<basic.hpp>
#if defined (TRACE_WINDFLOW)
    #include<stats_record.hpp>
    #include<rapidjson/prettywriter.h>
#endif

namespace wf {

/** 
 *  \class Basic_Operator
 *  
 *  \brief Abstract base class of a generic operator in WindFlow
 *  
 *  Abstract base class extended by all the operators in the library.
 */ 
class Basic_Operator
{
public:
    /** 
     *  \brief Get the name of the operator
     *  \return name of the operator
     */ 
    virtual std::string getName() const = 0;

    /** 
     *  \brief Get the total parallelism of the operator
     *  \return total parallelism of the operator
     */ 
    virtual size_t getParallelism() const = 0;

    /** 
     *  \brief Return the routing mode of the operator
     *  \return routing mode used by the operator
     */ 
    virtual routing_modes_t getRoutingMode() const = 0;

    /** 
     *  \brief Check whether the operator has been used in a MultiPipe
     *  \return true if the operator has been added/chained to an existing MultiPipe
     */ 
    virtual bool isUsed() const = 0;

    /** 
     *  \brief Check whether the operator has been terminated
     *  \return true if the operator has finished its work
     */ 
    virtual bool isTerminated() const = 0;

#if defined (TRACE_WINDFLOW)
    /// Dump the log file (JSON format) in the LOG_DIR directory
    virtual void dump_LogFile() const = 0;

    /// append the statistics (JSON format) of this operator
    virtual void append_Stats(rapidjson::PrettyWriter<rapidjson::StringBuffer> &) const = 0;
#endif
};

} // namespace wf

#endif
