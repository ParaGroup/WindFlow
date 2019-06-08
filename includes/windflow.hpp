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
 *  @file    windflow.hpp
 *  @author  Gabriele Mencagli
 *  @date    28/01/2019
 *  
 *  @brief General header file of the WindFlow library
 *  
 *  @section General Header File
 *  
 *  General header file to be included in any WindFlow program.
 */ 

#ifndef WINDFLOW_H
#define WINDFLOW_H

/// includes
#include <ff/ff.hpp>
#include <builders.hpp>
#include <source.hpp>
#include <map.hpp>
#include <filter.hpp>
#include <flatmap.hpp>
#include <accumulator.hpp>
#include <win_seq.hpp>
#include <win_farm.hpp>
#include <key_farm.hpp>
#include <pane_farm.hpp>
#include <win_mapreduce.hpp>
#include <multipipe.hpp>
#include <sink.hpp>

/// namespace
using namespace ff;

#endif
