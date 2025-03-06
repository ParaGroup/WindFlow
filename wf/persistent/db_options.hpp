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
 *  @file    db_options.hpp
 *  @author  Gabriele Mencagli and Simone Frassinelli
 *  
 *  @brief DBOptions
 *  
 *  @section DBOptions (Description)
 *  
 *  This file implements an object representing the options to be passed to RocksDB primitives.
 */ 

#ifndef DB_OPTIONS_HPP
#define DB_OPTIONS_HPP

// includes
#include<string>
#include<functional>
#include<context.hpp>
#include<rocksdb/db.h>
#include<rocksdb/options.h>

namespace wf {

// class DBOptions
class DBOptions
{
public:
    // set_default_db_options method
    static void set_default_db_options(rocksdb::Options &_options)
    {
        _options.compaction_style = rocksdb::kCompactionStyleLevel;
        _options.write_buffer_size = 256 * 1024 * 1024; // default is 256MB for each memTable
        _options.max_write_buffer_number = 1; // default is 1 memTable
        _options.max_open_files = -1;
        _options.level0_file_num_compaction_trigger = 10;
        _options.env->SetBackgroundThreads(8, rocksdb::Env::Priority::HIGH); // 8 background threads used by RocksDB
        _options.max_background_jobs = 8;
        _options.allow_concurrent_memtable_write = true;
        // _options.unordered_write = true; // <- not supported in new RocksDB versions
        _options.use_direct_reads = true;
        _options.use_direct_io_for_flush_and_compaction = true;
        _options.create_if_missing = true;
        _options.compression_opts.enabled = true;
        // options.block_cache = rocksdb::NewLRUCache(cache_size); <-- if not specified (or nullptr), default is LRU of size 32 MiB
    }
};

}; // namespace wf

#endif
