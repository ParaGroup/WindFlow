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
public:
    // Destructor
    virtual ~Basic_Emitter() = default;

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

    // Punctuation generation method
    virtual void generate_punctuation(uint64_t, ff::ff_monode *) = 0;

    // Flushing method
    virtual void flush(ff::ff_monode *) = 0;

    // Setting a new internal emitter (meaningful in tree-based mode only)
    virtual void addInternalEmitter(Basic_Emitter *_e) {}

    // Get the number of internal emitters (meaningful in tree-based mode only)
    virtual size_t getNumInternalEmitters() const { return 0; }
};

} // namespace wf

#endif
