/******************************************************************************s
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
#include<ffat_aggregator.hpp>
#if defined (WF_TRACING_ENABLED)
    #include<monitoring.hpp>
#endif
#include<pipegraph.hpp>

#endif
