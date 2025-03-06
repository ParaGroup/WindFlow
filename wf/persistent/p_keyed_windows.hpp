/**************************************************************************************
 *  Copyright (c) 2019- Gabriele Mencagli, Simone Frassinelli, and Andrea Filippi
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
 *  @file    p_keyed_windows.hpp
 *  @author  Gabriele Mencagli, Simone Frassinelli and Andrea Filippi
 *  
 *  @brief P_Keyed_Windows operator
 *  
 *  @section P_Keyed_Windows (Description)
 *  
 *  This file implements the P_Keyed_Windows operator able to execute incremental
 *  or non-incremental queries over count- or time-based windows. If created with
 *  parallelism greater than one, windows belonging to different keys are processed
 *  in parallel by different operator replicas. Information about windows is kept
 *  on RocksDB.
 */ 

#ifndef P_KEYED_WIN_H
#define P_KEYED_WIN_H

/// includes
#include<string>
#include<functional>
#include<context.hpp>
#include<single_t.hpp>
#if defined(WF_TRACING_ENABLED)
    #include <stats_record.hpp>
#endif
#include<basic_emitter.hpp>
#include<basic_operator.hpp>
#include<persistent/p_window_replica.hpp>

namespace wf {

/** 
 *  \class P_Keyed_Windows
 *  
 *  \brief P_Keyed_Windows operator
 *  
 *  This class implements the P_Keyed_Windows operator executing incremental or
 *  non-incremental queries over streaming windows. The operator allows windows
 *  of different keys to be processed in parallel by different replica. Information
 *  about windows is kept on RocksDB.
 */ 
template<typename win_func_t, typename keyextr_func_t>
class P_Keyed_Windows: public Basic_Operator
{
private:
    friend class MultiPipe;
    friend class PipeGraph;
    win_func_t func; // functional logic used by the Keyed_Windows
    keyextr_func_t key_extr; // logic to extract the key attribute from the tuple_t
    std::vector<P_Window_Replica<win_func_t, keyextr_func_t> *> replicas; // vector of pointers to the replicas of the P_Keyed_Windows
    uint64_t win_len; // window length (in no. of tuples or in time units)
    uint64_t slide_len; // slide length (in no. of tuples or in time units)
    uint64_t lateness; // triggering delay in time units (meaningful for TB windows in DEFAULT mode)
    Win_Type_t winType; // window type (CB or TB)
    using tuple_t = decltype(get_tuple_t_Win(func));
    using result_t = decltype(get_result_t_Win(func));
    using wrapper_t = wrapper_tuple_t<decltype(get_tuple_t_Win(func))>;
    static constexpr op_type_t op_type = op_type_t::WIN;

    // Configure the P_Keyed_Windows to receive batches instead of individual inputs
    void receiveBatches(bool _input_batching) override
    {
        for (auto *r: replicas) {
            r->receiveBatches(_input_batching);
        }
    }

    // Set the emitter used to route outputs from the P_Keyed_Windows
    void setEmitter(Basic_Emitter *_emitter) override
    {
        replicas[0]->setEmitter(_emitter);
        for (size_t i = 1; i < replicas.size(); i++) {
            replicas[i]->setEmitter(_emitter->clone());
        }
    }

    // Check whether the P_Keyed_Windows has terminated
    bool isTerminated() const override
    {
        bool terminated = true;
        for (auto *r: replicas) { // scan all the replicas to check their termination
            terminated = terminated && r->isTerminated();
        }
        return terminated;
    }

    // Set the execution mode of the P_Keyed_Windows
    void setExecutionMode(Execution_Mode_t _execution_mode)
    {
        for (auto *r: replicas) {
            r->setExecutionMode(_execution_mode);
        }
    }

    // Get the logic to extract the key attribute from the tuple_t
    keyextr_func_t getKeyExtractor() const
    {
        return key_extr;
    }

#if defined(WF_TRACING_ENABLED)
    // Append the statistics (JSON format) of the Keyed_Windows to a PrettyWriter
    void appendStats(rapidjson::PrettyWriter<rapidjson::StringBuffer> &writer) const override
    {
        writer.StartObject(); // create the header of the JSON file
        writer.Key("Operator_name");
        writer.String((this->name).c_str());
        writer.Key("Operator_type");
        writer.String("P_Keyed_Windows");
        writer.Key("Distribution");
        writer.String("KEYBY");
        writer.Key("isTerminated");
        writer.Bool(this->isTerminated());
        writer.Key("isWindowed");
        writer.Bool(true);
        writer.Key("isGPU");
        writer.Bool(false);
        writer.Key("Window_type");
        if (winType == Win_Type_t::CB) {
            writer.String("count-based");
        }
        else {
            writer.String("time-based");
            writer.Key("Lateness");
            writer.Uint(lateness);
        }
        writer.Key("Window_length");
        writer.Uint(win_len);
        writer.Key("Window_slide");
        writer.Uint(slide_len);
        writer.Key("Parallelism");
        writer.Uint(this->parallelism);
        writer.Key("areNestedOPs"); // this for backward compatibility
        writer.Bool(false);
        writer.Key("OutputBatchSize");
        writer.Uint(this->outputBatchSize);
        writer.Key("Replicas");
        writer.StartArray();
        for (auto *r: replicas) { // append the statistics from all the replicas of the P_Keyed_Windows
            Stats_Record record = r->getStatsRecord();
            record.appendStats(writer);
        }
        writer.EndArray();
        writer.EndObject();
    }
#endif

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _func functional logic of the P_Keyed_Windows (a function or any callable type)
     *  \param _key_extr key extractor (a function or any callable type)
     *  \param _parallelism internal parallelism of the P_Keyed_Windows
     *  \param _name name of the P_Keyed_Windows
     *  \param _dbpath pathname of SST files used by RocksDB
     *  \param _outputBatchSize size (in num of tuples) of the batches produced by this operator (0 for no batching)
     *  \param _closing_func closing functional logic of the P_Keyed_Windows (a function or any callable type)
     *  \param _tuple_serialize serialize function of input tuples (a function or any callable type) 
     *  \param _tuple_deserialize deserialize function of input tuples (a function or any callable type) 
     *  \param _result_serialize serialize function of results (a function or any callable type) 
     *  \param _result_deserialize deserialize function of results (a function or any callable type) 
     *  \param _deleteDb Boolean flag stating whether SST files in _dbpath must be deleted before starting
     *  \param _sharedDb Boolean flag stating whether replicas of this operator should use the same RockDB states
     *  \param _options options to be provided to RocksDB
     *  \param _read_options read options to be provided to RocksDB
     *  \param _write_options read options to be provided to RocksDB
     *  \param _frag_size size of each archive fragment of the stream (in no. of tuples)
     *  \param _win_len window length (in no. of tuples or in time units)
     *  \param _slide_len slide length (in no. of tuples or in time units)
     *  \param _lateness (lateness in time units, meaningful for TB windows in DEFAULT mode)
     *  \param _winType window type (count-based CB or time-based TB)
     *  \param _cacheCapacity capacity of the internal cache (0 means cache is not used)
     */ 
    P_Keyed_Windows(win_func_t _func,
                    keyextr_func_t _key_extr,
                    size_t _parallelism,
                    std::string _name,
                    std::string _dbpath,
                    size_t _outputBatchSize,
                    std::function<void(RuntimeContext &)> _closing_func,
                    std::function<std::string(tuple_t &)> _tuple_serialize,
                    std::function<tuple_t(std::string &)> _tuple_deserialize,
                    std::function<std::string(result_t &)> _result_serialize,
                    std::function<result_t(std::string &)> _result_deserialize,
                    bool _deleteDb,
                    bool _sharedDb,
                    rocksdb::Options _options,
                    rocksdb::ReadOptions _read_options,
                    rocksdb::WriteOptions _write_options,
                    size_t _frag_size,
                    uint64_t _win_len,
                    uint64_t _slide_len,
                    uint64_t _lateness,
                    Win_Type_t _winType,
                    size_t _cacheCapacity):
                    Basic_Operator(_parallelism, _name, Routing_Mode_t::KEYBY, _outputBatchSize),
                    func(_func),
                    key_extr(_key_extr),
                    win_len(_win_len),
                    slide_len(_slide_len),
                    lateness(_lateness),
                    winType(_winType)
    {
        if (win_len == 0 || slide_len == 0) { // check the validity of the windowing parameters
            std::cerr << RED << "WindFlow Error: P_Keyed_Windows used with window length or slide equal to zero" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        for (size_t i = 0; i < this->parallelism; i++) { // create the internal replicas of the P_Keyed_Windows
            std::string shared_dbpath = _dbpath + this->name;
            std::string normal_dbpath = shared_dbpath + "_" + std::to_string(i);
            replicas.push_back(new P_Window_Replica<win_func_t, keyextr_func_t>(func,
                                                                                key_extr,
                                                                                this->name,
                                                                                _sharedDb ? shared_dbpath : normal_dbpath,
                                                                                RuntimeContext(this->parallelism, i),
                                                                                _closing_func,
                                                                                _tuple_serialize,
                                                                                _tuple_deserialize,
                                                                                _result_serialize,
                                                                                _result_deserialize,
                                                                                _deleteDb,
                                                                                _sharedDb,
                                                                                i,
                                                                                _frag_size,
                                                                                win_len,
                                                                                slide_len,
                                                                                lateness,
                                                                                winType,
                                                                                _cacheCapacity));
        }
        assert(this->parallelism > 0);
        // initialize the internal DB of the replicas
        if ((replicas[0]->mydb_wrappers) != nullptr) {
            (replicas[0]->mydb_wrappers)->initDB(nullptr, _options, _read_options, _write_options);
        }
        if ((replicas[0]->mydb_results) != nullptr) {
            (replicas[0]->mydb_results)->initDB(nullptr, _options, _read_options, _write_options);
        }
        for (size_t i = 1; i < this->parallelism; i++) {
            if (_sharedDb) {
                if ((replicas[0]->mydb_wrappers) != nullptr) {
                    assert((replicas[i]->mydb_wrappers) != nullptr); // sanity check
                    (replicas[i]->mydb_wrappers)->initDB((replicas[0]->mydb_wrappers)->get_internal_db(), _options, _read_options, _write_options);
                }
                if ((replicas[0]->mydb_results) != nullptr) {
                    assert((replicas[i]->mydb_results) != nullptr); // sanity check
                    (replicas[i]->mydb_results)->initDB((replicas[0]->mydb_results)->get_internal_db(), _options, _read_options, _write_options);
                }
            }
            else {
                if ((replicas[i]->mydb_wrappers) != nullptr) {
                    (replicas[i]->mydb_wrappers)->initDB(nullptr, _options, _read_options, _write_options);
                }
                if ((replicas[i]->mydb_results) != nullptr) {
                    (replicas[i]->mydb_results)->initDB(nullptr, _options, _read_options, _write_options);
                }
            }
        }
    }

    /// Copy constructor
    P_Keyed_Windows(const P_Keyed_Windows &_other):
                    Basic_Operator(_other),
                    func(_other.func),
                    key_extr(_other.key_extr),
                    win_len(_other.win_len),
                    slide_len(_other.slide_len),
                    lateness(_other.lateness),
                    winType(_other.winType)
    {
        for (size_t i = 0; i < this->parallelism; i++) { // deep copy of the pointers to the Keyed_Windows replicas
            replicas.push_back(new P_Window_Replica<win_func_t, keyextr_func_t>(*(_other.replicas[i])));
        }
    }

    // Destructor
    ~P_Keyed_Windows() override
    {
        for (auto *r: replicas) { // delete all the replicas
            delete r;
        }
    }

    /** 
     *  \brief Get the type of the P_Keyed_Windows as a string
     *  \return type of the P_Keyed_Windows
     */ 
    std::string getType() const override
    {
        return std::string("P_Keyed_Windows");
    }

    /** 
     *  \brief Get the window type (CB or TB) utilized by the P_Keyed_Windows
     *  \return adopted windowing semantics (count-based or time-based)
     */ 
    Win_Type_t getWinType() const
    {
        return winType;
    }

    /** 
     *  \brief Get the number of ignored tuples by the P_Keyed_Windows
     *  \return number of tuples ignored during the processing by the P_Keyed_Windows
     */ 
    size_t getNumIgnoredTuples() const
    {
        size_t count = 0;
        for (auto *r: replicas) {
            count += r->getNumIgnoredTuples();
        }
        return count;
    }

    P_Keyed_Windows(P_Keyed_Windows &&) = delete; ///< Move constructor is deleted
    P_Keyed_Windows &operator=(const P_Keyed_Windows &) = delete; ///< Copy assignment operator is deleted
    P_Keyed_Windows &operator=(P_Keyed_Windows &&) = delete; ///< Move assignment operator is deleted
};

} // namespace wf

#endif
