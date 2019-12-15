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
 *  @date    03/10/2019
 *  
 *  @brief Abstract class of the generic emitter node in WindFlow
 *  
 *  @section Basic_Emitter (Description)
 *  
 *  Abstract class of the generic emitter node in WindFlow. All the emitter nodes
 *  in the library extend this abstract class.
 */ 

#ifndef BASIC_EMITTER_H
#define BASIC_EMITTER_H

// includes
#include <vector>
#include <ff/multinode.hpp>

namespace wf {

// class Basic_Emitter
class Basic_Emitter: public ff::ff_monode
{
public:
    // destructor
    virtual ~Basic_Emitter() {}

    // clone method
    virtual Basic_Emitter *clone() const = 0;

    // getNDestinations method
    virtual size_t getNDestinations() const = 0;

    // setTree_EmitterMode method
    virtual void setTree_EmitterMode(bool) = 0;

    // getOutputQueue method
    virtual std::vector<std::pair<void *, int>> &getOutputQueue() = 0;
};

} // namespace wf

#endif
