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
 *  @file    windflow.hpp
 *  @author  Gabriele Mencagli
 *  
 *  @brief General Header File
 *  
 *  @section First General Header File of the WindFlow Library
 *  
 *  General header file to be included by any WindFlow program.
 */ 

#ifndef WINDFLOW_H
#define WINDFLOW_H

/// includes
#include<builders.hpp>
#include<source.hpp>
#include<map.hpp>
#include<reduce.hpp>
#include<filter.hpp>
#include<flatmap.hpp>
#include<sink.hpp>
#include<keyed_windows.hpp>
#include<parallel_windows.hpp>
#include<paned_windows.hpp>
#include<mapreduce_windows.hpp>
#include<ffat_windows.hpp>
#if defined (WF_TRACING_ENABLED)
    #include<monitoring.hpp>
#endif
#include<pipegraph.hpp>

#endif
