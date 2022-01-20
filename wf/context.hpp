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
 *  @file    context.hpp
 *  @author  Gabriele Mencagli
 *  
 *  @brief RuntimeContext class to access the run-time system information
 *  
 *  @section RuntimeContext (Description)
 *  
 *  This file implements the RuntimeContext class used to access the run-time system
 *  information accessible with the "riched" functional logic supported by some
 *  operators in the library.
 */ 

#ifndef CONTEXT_H
#define CONTEXT_H

/// includes
#include<local_storage.hpp>

namespace wf {

/** 
 *  \class RuntimeContext
 *  
 *  \brief RuntimeContext class used to access to run-time system information
 *  
 *  This class implements the RuntimeContext object used to access the run-time system
 *  information accessible with the "riched" variants of the functional logic of some
 *  operators.
 */ 
class RuntimeContext
{
private:
    template<typename T> friend class Source_Replica; // friendship with Source_Replica class
    template<typename T1> friend class Map_Replica; // friendship with Map_Replica class
    template<typename T1> friend class Filter_Replica; // friendship with Filter_Replica class
    template<typename T1, typename T2> friend class Reduce_Replica; // friendship with Reduce_Replica class
    template<typename T1> friend class FlatMap_Replica; // friendship with FlatMap_Replica class
    template<typename T1> friend class Sink_Replica; // friendship with Sink_Replica class
    template<typename T1, typename T2> friend class Window_Replica; // friendship with Window_Replica class
    template<typename T1, typename T2, typename T3> friend class FFAT_Replica; // friendship with FFAT_Replica class
    size_t parallelism; // parallelism of the operator
    size_t index; // index of the replica
    LocalStorage storage; // local storage object
    uint64_t timestamp; // timestamp of the current input
    uint64_t watermark; // last received watermark

    // Set the configuration parameters
    void setContextParameters(uint64_t _ts,
                              uint64_t _wm)
    {
        timestamp = _ts;
        watermark = _wm;
    }

public:
    /// Constructor
    RuntimeContext(size_t _parallelism,
                   size_t _index):
                   parallelism(_parallelism),
                   index(_index),
                   timestamp(0),
                   watermark(0) {}

    /// Copy Constructor
    RuntimeContext(const RuntimeContext &_other): // do not copy the storage
                   parallelism(_other.parallelism),
                   index(_other.index),
                   timestamp(_other.timestamp),
                   watermark(_other.watermark) {}

    /// Copy Assignment Operator
    RuntimeContext &operator=(const RuntimeContext &_other) // do not copy the storage
    {
        if (this != &_other) {
            parallelism = _other.parallelism;
            index = _other.index;
            timestamp = _other.timestamp;
            watermark = _other.watermark;
        }
        return *this;
    }

    /** 
     *  \brief Get the parallelism of the operator
     *  
     *  \return number of replicas
     */ 
    size_t getParallelism() const
    {
        return parallelism;
    }

    /** 
     *  \brief Get the index of the replica
     *  
     *  \return index of the replica (starting from zero)
     */ 
    size_t getReplicaIndex() const
    {
        return index;
    }

    /** 
     *  \brief Get a reference to the local storage (private per replica)
     *  
     *  \return reference to the local storage
     */ 
    LocalStorage &getLocalStorage()
    {
        return storage;
    }

    /** 
     *  \brief Get the timestamp of the current input (in microseconds starting from zero)
     *  \return timestamp value
     */ 
    uint64_t getCurrentTimestamp()
    {
        return timestamp;
    }

    /** 
     *  \brief Get the last watermark (in microseconds starting from zero)
     *  
     *  \return watermark value
     */ 
    uint64_t getLastWatermark()
    {
        return watermark;
    }
};

} // namespace wf

#endif
