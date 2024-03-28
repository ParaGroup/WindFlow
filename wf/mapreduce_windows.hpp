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
 *  @file    mapreduce_windows.hpp
 *  @author  Gabriele Mencagli
 *  
 *  @brief MapReduce_Windows meta operator
 *  
 *  @section MapReduce_Windows (Description)
 *  
 *  This file implements the MapReduce_Windows meta operator able to execute incremental
 *  or non-incremental queries on count- or time-based windows. Each window is split
 *  into disjoint partitions. Results of the partitions are used to compute
 *  window-wise results.
 */ 

#ifndef MAPREDUCE_WIN_H
#define MAPREDUCE_WIN_H

/// includes
#include<string>
#include<functional>
#include<context.hpp>
#include<single_t.hpp>
#if defined (WF_TRACING_ENABLED)
    #include<stats_record.hpp>
#endif
#include<basic_emitter.hpp>
#include<basic_operator.hpp>
#include<window_replica.hpp>
#include<parallel_windows.hpp>

namespace wf {

/** 
 *  \class MapReduce_Windows
 *  
 *  \brief MapReduce_Windows meta operator
 *  
 *  This class implements the MapReduce_Windows meta operator executing incremental or
 *  non-incremental queries on streaming windows. Each window is split into disjoint
 *  partitions. Results of the partitions are used to compute results of whole
 *  windows.
 */ 
template<typename map_func_t, typename reduce_func_t, typename keyextr_func_t>
class MapReduce_Windows: public Basic_Operator
{
private:
    friend class MultiPipe;
    friend class PipeGraph;
    map_func_t map_func; // functional logic of the MAP stage
    reduce_func_t reduce_func; // functional logic of the REDUCE stage
    keyextr_func_t key_extr; // logic to extract the key attribute from the tuple_t
    size_t map_parallelism; // parallelism of the MAP stage
    using tuple_t = decltype(get_tuple_t_Win(map_func)); // extracting the tuple_t type and checking the admissible signatures
    using result_t = decltype(get_result_t_Win(reduce_func)); // extracting the result_t type and checking the admissible signatures
    size_t reduce_parallelism; // parallelism of the REDUCE stage
    uint64_t win_len; // window length (in no. of tuples or in time units)
    uint64_t slide_len; // slide length (in no. of tuples or in time units)
    uint64_t lateness; // triggering delay in time units (meaningful for TB windows in DEFAULT mode)
    Win_Type_t winType; // window type (CB or TB)
    Parallel_Windows<map_func_t, keyextr_func_t> map; // MAP sub-operator
    Parallel_Windows<reduce_func_t, keyextr_func_t> reduce; // REDUCE sub-operator
    static constexpr op_type_t op_type = op_type_t::WIN_MR;

    // Configure the MapReduce_Windows to receive batches instead of individual inputs
    void receiveBatches(bool _input_batching) override { abort(); } // cannot be used

    // Set the emitter used to route outputs from the MapReduce_Windows
    void setEmitter(Basic_Emitter *_emitter) override { abort(); } // cannot be used

    // Check whether the MapReduce_Windows has terminated
    bool isTerminated() const override { abort(); } // cannot be used

#if defined (WF_TRACING_ENABLED)
    // Append the statistics (JSON format) of the MapReduce_Windows to a PrettyWriter
    void appendStats(rapidjson::PrettyWriter<rapidjson::StringBuffer> &writer) const override { abort(); } // cannot be used
#endif

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _map_func functional logic of the MAP stage (a function or any callable type)
     *  \param _reduce_func functional logic of the REDUCE stage (a function or any callable type)
     *  \param _key_extr key extractor (a function or any callable type)
     *  \param _map_parallelism internal parallelism of the MAP stage
     *  \param _reduce_parallelism internal parallelism of the REDUCE stage
     *  \param _name name of the MapReduce_Windows
     *  \param _outputBatchSize size (in num of tuples) of the batches produced by this operator (0 for no batching)
     *  \param _closing_func closing functional logic of the MapReduce_Windows (a function or any callable type)
     *  \param _win_len window length (in no. of tuples or in time units)
     *  \param _slide_len slide length (in no. of tuples or in time units)
     *  \param _lateness (lateness in time units, meaningful for TB windows in DEFAULT mode)
     *  \param _winType window type (count-based CB or time-based TB)
     */ 
    MapReduce_Windows(map_func_t _map_func,
                      reduce_func_t _reduce_func,
                      keyextr_func_t _key_extr,
                      size_t _map_parallelism,
                      size_t _reduce_parallelism,
                      std::string _name,
                      size_t _outputBatchSize,
                      std::function<void(RuntimeContext &)> _closing_func,
                      uint64_t _win_len,
                      uint64_t _slide_len,
                      uint64_t _lateness,
                      Win_Type_t _winType):
                      Basic_Operator(_map_parallelism+_reduce_parallelism, _name, Routing_Mode_t::BROADCAST, _outputBatchSize),
                      map_func(_map_func),
                      reduce_func(_reduce_func),
                      key_extr(_key_extr),
                      map_parallelism(_map_parallelism),
                      reduce_parallelism(_reduce_parallelism),
                      win_len(_win_len),
                      slide_len(_slide_len),
                      lateness(_lateness),
                      winType(_winType),
                      map(_map_func, _key_extr, _map_parallelism, _name + "_map", 0, _closing_func, _win_len, _slide_len, _lateness, _winType, role_t::MAP),
                      reduce(_reduce_func, _key_extr, _reduce_parallelism, _name + "_reduce", _outputBatchSize, _closing_func, _map_parallelism, _map_parallelism, 0, Win_Type_t::CB, role_t::REDUCE)
    {
        if (map_parallelism == 0) { // check the validity of the MAP parallelism value
            std::cerr << RED << "WindFlow Error: MapReduce_Windows has MAP parallelism zero" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        if (reduce_parallelism == 0) { // check the validity of the REDUCE parallelism value
            std::cerr << RED << "WindFlow Error: MapReduce_Windows has REDUCE parallelism zero" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        if (win_len == 0 || slide_len == 0) { // check the validity of the windowing parameters
            std::cerr << RED << "WindFlow Error: MapReduce_Windows used with window length or slide equal to zero" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // set the types of the internal operators
        map.type = "Parallel_Windows_MAP";
        reduce.type = "Parallel_Windows_REDUCE";
    }

    /// Copy constructor
    MapReduce_Windows(const MapReduce_Windows &_other):
                      Basic_Operator(_other),
                      map_func(_other.map_func),
                      reduce_func(_other.reduce_func),
                      key_extr(_other.key_extr),
                      map_parallelism(_other.map_parallelism),
                      reduce_parallelism(_other.reduce_parallelism),
                      win_len(_other.win_len),
                      slide_len(_other.slide_len),
                      lateness(_other.lateness),
                      winType(_other.winType),
                      map(_other.map),
                      reduce(_other.reduce) {}

    /** 
     *  \brief Get the type of the MapReduce_Windows as a string
     *  \return type of the MapReduce_Windows
     */ 
    std::string getType() const override
    {
        return std::string("MapReduce_Windows");
    }

    /** 
     *  \brief Get the window type (CB or TB) utilized by the MapReduce_Windows
     *  \return adopted windowing semantics (count-based or time-based)
     */ 
    Win_Type_t getWinType() const
    {
        return winType;
    }

    /** 
     *  \brief Get the number of ignored tuples by the MapReduce_Windows
     *  \return number of tuples ignored during the processing by the MapReduce_Windows
     */ 
    size_t getNumIgnoredTuples() const
    {
        return map->getNumIgnoredTuples();
    }

    MapReduce_Windows(MapReduce_Windows &&) = delete; ///< Move constructor is deleted
    MapReduce_Windows &operator=(const MapReduce_Windows &) = delete; ///< Copy assignment operator is deleted
    MapReduce_Windows &operator=(MapReduce_Windows &&) = delete; ///< Move assignment operator is deleted
};

} // namespace wf

#endif
