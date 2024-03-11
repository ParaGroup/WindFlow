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
 *  @file    basic_emitter.hpp
 *  @author  Gabriele Mencagli
 *  
 *  @brief Abstract base class of the generic emitter in WindFlow
 *  
 *  @section Basic_Emitter (Description)
 *  
 *  Abstract base class of the generic emitter in WindFlow. All the emitters
 *  in the library extend this abstract class.
 */ 

#ifndef BASIC_EMITTER_H
#define BASIC_EMITTER_H

// includes
#include<vector>
#include<ff/multinode.hpp>

namespace wf {

// class Basic_Emitter
class Basic_Emitter
{
protected:
    ff::MPMC_Ptr_Queue *queue; // pointer to the recyling queue

    // Constructor
    Basic_Emitter()
    {
        queue = new ff::MPMC_Ptr_Queue();
        queue->init(DEFAULT_BUFFER_CAPACITY);
    }

    // Copy Constructor
    Basic_Emitter(const Basic_Emitter &_other)
    {
        queue = new ff::MPMC_Ptr_Queue();
        queue->init(DEFAULT_BUFFER_CAPACITY);
    }

public:
    // Destructor
    virtual ~Basic_Emitter()
    {
        delete queue;
    }

    // Create a clone of the emitter
    virtual Basic_Emitter *clone() const = 0;

    // Get the number of destinations of the emitter
    virtual size_t getNumDestinations() const = 0;

    // Set the emitter to work in tree-based mode
    virtual void setTreeMode(bool) = 0;

    // Get a reference to the vector of output messages used by the emitter (meaningful in tree-based mode only)
    virtual std::vector<std::pair<void *, size_t>> &getOutputQueue() = 0;

    // Emit method (non in-place version)
    virtual void emit(void *, uint64_t, uint64_t, uint64_t, ff::ff_monode *) = 0;

    // Emit method (in-place version)
    virtual void emit_inplace(void *, ff::ff_monode *) = 0;

    // Punctuation propagation method
    virtual void propagate_punctuation(uint64_t, ff::ff_monode *) = 0;

    // Flushing method
    virtual void flush(ff::ff_monode *) = 0;

    // Setting a new internal emitter (meaningful in tree-based mode only)
    virtual void addInternalEmitter(Basic_Emitter *_e) {}

    // Get the number of internal emitters (meaningful in tree-based mode only)
    virtual size_t getNumInternalEmitters() const { return 0; }

    Basic_Emitter(Basic_Emitter &&) = delete; ///< Move constructor is deleted
    Basic_Emitter &operator=(const Basic_Emitter &) = delete; ///< Copy assignment operator is deleted
    Basic_Emitter &operator=(Basic_Emitter &&) = delete; ///< Move assignment operator is deleted
};

} // namespace wf

#endif
