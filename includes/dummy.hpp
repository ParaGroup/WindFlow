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
 *  @file    dummy.hpp
 *  @author  Gabriele Mencagli
 *  @date    12/02/2019
 *  
 *  @brief Dummy nodes used by the WindFlow library
 *  
 *  @section Dummy Nodes (Description)
 *  
 *  This file implements a set of dummy emitter and collector nodes used by the library.
 */ 

#ifndef DUMMY_H
#define DUMMY_H

// includes
#include <ff/multinode.hpp>

using namespace ff;

// class of the dummy multi-output node (dummy_emitter)
class dummy_emitter: public ff_monode
{
    // svc method (utilized by the FastFlow runtime)
    void *svc(void *t) { return t; }
};

// class of the dummy multi-input node (dummy_collector)
class dummy_collector: public ff_minode
{
    // svc method (utilized by the FastFlow runtime)
    void *svc(void *t) { return t; }
};

#endif
