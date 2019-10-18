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
 *  @file    context.hpp
 *  @author  Gabriele Mencagli
 *  @date    23/02/2019
 *  
 *  @brief RuntimeContext class to access the run-time system information
 *         used by a pattern's functional logic
 *  
 *  @section RuntimeContext (Description)
 *  
 *  This file implements the RuntimeContext class used to access the run-time system
 *  information used by the functional logic of a pattern (static information such
 *  as the parallelism degree of the operator and which is the current replica invoking
 *  the operator's functional logic).
 */ 

#ifndef CONTEXT_H
#define CONTEXT_H

/// includes
#include <local_storage.hpp>

namespace wf {

/** 
 *  \class RuntimeContext
 *  
 *  \brief RuntimeContext class used to access to run-time system information
 *         used by a pattern's functional logic
 *  
 *  This class implements the RuntimeContext object used to access the run-time system
 *  information used by the pattern's functiona logic (access to static information like
 *  number of replicas and identifier of the replica which is invoking the functional logic).
 */ 
class RuntimeContext
{
private:
    size_t parallelism; // parallelism degree of the pattern
    size_t index; // index of the replica
    LocalStorage storage; // local storage

public:
    /// Constructor I
    RuntimeContext(): parallelism(0),
                      index(0)
    {}

    /** 
     *  \brief Constructor II
     *  
     *  \param _parallelism number of replicas of the pattern
     *  \param _index index of the replica invoking the functional logic
     */ 
    RuntimeContext(size_t _parallelism,
                   size_t _index):
                   parallelism(_parallelism),
                   index(_index)
    {}

    /** 
     *  \brief Return the parallelism of the pattern in which the RuntimeContext is used
     *  
     *  \return parallelism degree (number of operator's replicas)
     */  
    size_t getParallelism() const
    {
        return parallelism;
    }

    /** 
     *  \brief Return the index of the replica where the RuntimeContext is used
     *  
     *  \return index of the replica (starting from zero)
     */ 
    size_t getReplicaIndex() const
    {
        return index;
    }

    /** 
     *  \brief Return a reference to the local storage used by the operator replica
     *  
     *  \return reference to the local storage
     */ 
    LocalStorage &getLocalStorage()
    {
        return storage;
    }
};

} // namespace wf

#endif
