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
 *  @file    windflow_gpu.hpp
 *  @author  Gabriele Mencagli
 *  @date    28/01/2019
 *  
 *  @brief General header file of the WindFlow library (GPU patterns)
 *  
 *  @section General Header File (GPU patterns)
 *  
 *  General header file to be included in any WindFlow program for using the
 *  GPU version of the patterns.
 */ 

#ifndef WINDFLOW_GPU_H
#define WINDFLOW_GPU_H

/// includes
#include <ff/ff.hpp>
#include <win_seq_gpu.hpp>
#include <win_farm_gpu.hpp>
#include <key_farm_gpu.hpp>
#include <pane_farm_gpu.hpp>
#include <win_mapreduce_gpu.hpp>

/// namespace
using namespace ff;

#endif
