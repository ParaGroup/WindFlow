/**************************************************************************************
 *  Copyright (c) 2019- Gabriele Mencagli and Simone Frassinelli
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
 *  @file    windflow_rocksdb.hpp
 *  @author  Gabriele Mencagli and Simone Frassinelli
 *  
 *  @brief General Header File-4
 *  
 *  @section Fourth General Header File of the WindFlow Library to include Rocksdb Operators
 *  
 *  General header file to be included by any WindFlow program to use operators
 *  saving their state in RocksDB.
 */ 

#ifndef WINDFLOW_ROCKSDB_H
#define WINDFLOW_ROCKSDB_H

/// includes
#include<persistent/meta_rocksdb.hpp>
#include<persistent/builders_rocksdb.hpp>
#include<persistent/db_handle.hpp>
#include<persistent/db_options.hpp>
#include<persistent/p_filter.hpp>
#include<persistent/p_flatmap.hpp>
#include<persistent/p_map.hpp>
#include<persistent/p_reduce.hpp>
#include<persistent/p_sink.hpp>
#include<persistent/p_keyed_windows.hpp>

#endif
