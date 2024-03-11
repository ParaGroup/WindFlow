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
 *  @file    builders_rocksdb.hpp
 *  @author  Gabriele Mencagli and Simone Frassinelli
 *  
 *  @brief Builder classes used to create the WindFlow operators that use
 *         RocksDB as the back-end for in-flight state
 *  
 *  @section Builders-RocksDB (Description)
 *  
 *  Builder classes used to create the WindFlow operators that use RocksDB
 *  as the back-end for in-flight state.
 */ 

#ifndef BUILDERS_ROCKSDB_H
#define BUILDERS_ROCKSDB_H

/// includes
#include<chrono>
#include<functional>
#include<meta.hpp>
#include<basic.hpp>
#include<persistent/db_options.hpp>
#include<persistent/meta_rocksdb.hpp>
#include<rocksdb/options.h>

namespace wf {

/** 
 *  \class P_Basic_Builder
 *  
 *  \brief Abstract class of the builder for basic operators using RocksDB support
 *  
 *  Abstract class extended by all the builders of basic operators using RocksDB support
 */ 
template<template<class, class> class p_builder_t, class p_func_t, class key_t>
class P_Basic_Builder
{
protected:
    std::string name = "N/D"; // name of the operator
    size_t parallelism = 1; // parallelism of the operator
    size_t outputBatchSize = 0; // output batch size of the operator
    using closing_func_t = std::function<void(RuntimeContext&)>; // type of the closing functional logic
    closing_func_t closing_func = [](RuntimeContext &r) -> void { return; }; // closing functional logic
    std::string dbpath = "/tmp/"; // path used for the SST files by RocksDB
    bool deleteDb = true; // deleteDB flag
    bool sharedDb = false; // sharedBD flag
    rocksdb::Options options;  // options used by RocksDB
    rocksdb::ReadOptions read_options; // read options used by RocksDB
    rocksdb::WriteOptions write_options; // write options used by RocksDB

    // Constructor
    P_Basic_Builder()
    {
        DBOptions::set_default_db_options(options);
    }

    // Copy Constructor
    P_Basic_Builder(const P_Basic_Builder &) = default;

public:
    /** 
     *  \brief Set the name of the operator
     *  
     *  \param _name of the operator
     *  \return a reference to the builder object
     */ 
    auto &withName(std::string _name)
    {
        name = _name;
        return static_cast<p_builder_t<p_func_t, key_t> &>(*this);
    }

    /** 
     *  \brief Set the parallelism of the operator
     *  
     *  \param _parallelism of the operator
     *  \return a reference to the builder object
     */ 
    auto &withParallelism(size_t _parallelism)
    {
        parallelism = _parallelism;
        return static_cast<p_builder_t<p_func_t, key_t> &>(*this);
    }

    /** 
     *  \brief Set the output batch size of the operator
     *  
     *  \param _outputBatchSize number of outputs per batch (zero means no batching)
     *  \return a reference to the builder object
     */ 
    auto &withOutputBatchSize(size_t _outputBatchSize)
    {
        outputBatchSize = _outputBatchSize;
        return static_cast<p_builder_t<p_func_t, key_t> &>(*this);
    }

    /** 
     *  \brief Set the closing functional logic used by the operator
     *  
     *  \param _closing_func closing functional logic (a function or any callable type)
     *  \return a reference to the builder object
     */ 
    template<typename closing_F_t>
    auto &withClosingFunction(closing_F_t _closing_func)
    {
        // static assert to check the signature
        static_assert(!std::is_same<decltype(check_closing_t(_closing_func)), std::false_type>::value,
            "WindFlow Compilation Error - unknown signature passed to withClosingFunction:\n"
            "  Candidate : void(RuntimeContext &)\n");
        closing_func = _closing_func;
        return static_cast<p_builder_t<p_func_t, key_t> &>(*this);
    }

    /** 
     *  \brief Set the path of SST files used by RocksDB
     *  
     *  \param _dbpath path of SST files used by RocksDB
     *  \return a reference to the builder object
     */ 
    auto &withDbPath(std::string _dbpath)
    {
        dbpath = _dbpath;
        return static_cast<p_builder_t<p_func_t, key_t> &>(*this);
    }

    /** 
     *  \brief If true, delete pre-existing SST files in the same path (if any)
     *  
     *  \param _deleteDb value of Boolean flag
     *  \return a reference to the builder object
     */ 
    auto &withDeleteDb(bool _deleteDb)
    {
        deleteDb = _deleteDb;
        return static_cast<p_builder_t<p_func_t, key_t> &>(*this);
    }

    /** 
     *  \brief If true, replicas of the operator share the RocksDB persistent data
     *  
     *  \param _sharedDb value of Boolean flag
     *  \return a reference to the builder object
     */ 
    auto &withSharedDb(bool _sharedDb)
    {
        sharedDb = _sharedDb;
        return static_cast<p_builder_t<p_func_t, key_t> &>(*this);
    }

    /** 
     *  \brief Set user-defined options to be used by RocksDB
     *  
     *  \param _options options struct
     *  \return a reference to the builder object
     */ 
    auto &withOptions(rocksdb::Options _options)
    {
        options = _options;
        return static_cast<p_builder_t<p_func_t, key_t> &>(*this);
    }

    /** 
     *  \brief Set user-defined read options to be used by RocksDB
     *  
     *  \param _read_options read options struct
     *  \return a reference to the builder object
     */ 
    auto &withReadOptions(rocksdb::ReadOptions _read_options)
    {
        read_options = _read_options;
        return static_cast<p_builder_t<p_func_t, key_t> &>(*this);
    }

    /** 
     *  \brief Set user-defined read options to be used by RocksDB
     *  
     *  \param _write_options read options struct
     *  \return a reference to the builder object
     */ 
    auto &withWriteOptions(rocksdb::WriteOptions _write_options)
    {
        write_options = _write_options;
        return static_cast<p_builder_t<p_func_t, key_t> &>(*this);
    }
};

/** 
 *  \class P_Filter_Builder
 *  
 *  \brief Builder of the P_Filter operator
 *  
 *  Builder class to ease the creation of the P_Filter operator.
 */ 
template<typename p_filter_func_t, typename key_t=empty_key_t>
class P_Filter_Builder: public P_Basic_Builder<P_Filter_Builder, p_filter_func_t, key_t>
{
private:
    template<typename T1, typename T2> friend class P_Filter_Builder;
    p_filter_func_t func; // functional logic of the P_Filter
    using tuple_t = decltype(get_tuple_t_P_Filter(func)); // extracting the tuple_t type and checking the admissible signatures
    using state_t = decltype(get_state_t_P_Filter(func)); // extracting the state_t type and checking the admissible signatures
    // static assert to check the signature of the P_Filter functional logic
    static_assert(!(std::is_same<tuple_t, std::false_type>::value || std::is_same<state_t, std::false_type>::value),
                  "WindFlow Compilation Error - unknown signature passed to the P_Filter_Builder:\n"
                  "  Candidate 1 : bool(tuple_t &, state_t &)\n"
                  "  Candidate 2 : bool(tuple_t &, state_t &, RuntimeContext &)\n");
    // static assert to check that the tuple_t type must be default constructible
    static_assert(std::is_default_constructible<tuple_t>::value,
                  "WindFlow Compilation Error - tuple_t type must be default constructible (P_Filter_Builder):\n");
    // static assert to check that the state_t type must be default constructible
    static_assert(std::is_default_constructible<state_t>::value,
                  "WindFlow Compilation Error - state_t type must be default constructible (P_Filter_Builder):\n");
    using keyextr_func_t = std::function<key_t(const tuple_t &)>; // type of the key extractor
    using serialize_func_t = std::function<std::string(state_t &)>; // type of the serialization function
    using deserialize_func_t = std::function<state_t(std::string &)>; // type of the deserialization function
    using p_filter_t = P_Filter<p_filter_func_t, keyextr_func_t>; // type of the P_Filter to be created by the builder
    serialize_func_t serialize; // serialization function of the state
    deserialize_func_t deserialize; // deserialization function of the state
    state_t initial_state; // initial state to be created one per key by RocksDB
    Routing_Mode_t input_routing_mode = Routing_Mode_t::FORWARD; // routing mode of inputs to the P_Filter
    keyextr_func_t key_extr = [](const tuple_t &t) -> key_t { return key_t(); }; // key extractor
    bool isKeyBySet = false; // true if a key extractor has been provided
    bool isSerializer = false; // flag stating if the serialized has been provided or not
    bool isDeserializer = false; // flag stating if the deserialized has been provided or not

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _func functional logic of the P_Filter (a function or any callable type)
     */ 
    P_Filter_Builder(p_filter_func_t _func): func(_func) {}

    /** 
     *  \brief Set the KEYBY routing mode of inputs to the P_Filter
     *  
     *  \param _key_extr key extractor functional logic (a function or any callable type)
     *  \return a new builder object with the right key type
     */ 
    template<typename new_keyextr_func_t>
    auto withKeyBy(new_keyextr_func_t _key_extr)
    {
        // static assert to check the signature
        static_assert(!std::is_same<decltype(get_tuple_t_KeyExtr(_key_extr)), std::false_type>::value,
                      "WindFlow Compilation Error - unknown signature passed to withKeyBy (P_Filter_Builder):\n"
                      "  Candidate : key_t(const tuple_t &)\n");
        // static assert to check that the tuple_t type of the new key extractor is the right one
        static_assert(std::is_same<decltype(get_tuple_t_KeyExtr(_key_extr)), tuple_t>::value,
                      "WindFlow Compilation Error - key extractor receives a wrong input type (P_Filter_Builder):\n");
        using new_key_t = decltype(get_key_t_KeyExtr(_key_extr)); // extract the key type
        // static assert to check the new_key_t type
        static_assert(!std::is_same<new_key_t, void>::value,
                      "WindFlow Compilation Error - key type cannot be void (P_Filter_Builder):\n");
        // static assert to check that new_key_t is default constructible
        static_assert(std::is_default_constructible<new_key_t>::value,
                      "WindFlow Compilation Error - key type must be default constructible (P_Filter_Builder):\n");
        if (input_routing_mode != Routing_Mode_t::FORWARD) {
            std::cerr << RED << "WindFlow Error: wrong use of withKeyBy() in the P_Filter_Builder" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        P_Filter_Builder<p_filter_func_t, new_key_t> new_builder(func);
        new_builder.name = this->name;
        new_builder.parallelism = this->parallelism;
        new_builder.outputBatchSize = this->outputBatchSize;
        new_builder.closing_func = this->closing_func;
        new_builder.dbpath = this->dbpath;
        new_builder.deleteDb = this->deleteDb;
        new_builder.sharedDb = this->sharedDb;
        new_builder.options = this->options;
        new_builder.read_options = this->read_options;
        new_builder.write_options = this->write_options;
        new_builder.serialize = serialize;
        new_builder.deserialize = deserialize;
        new_builder.initial_state = initial_state;
        new_builder.input_routing_mode = Routing_Mode_t::KEYBY;
        new_builder.key_extr = _key_extr;
        new_builder.isKeyBySet = true;
        new_builder.isSerializer = isSerializer;
        new_builder.isDeserializer = isDeserializer;
        return new_builder;
    }

    /** 
     *  \brief Set the BROADCAST routing mode of inputs to the P_Filter
     *  
     *  \return a reference to the builder object
     */ 
    auto &withBroadcast()
    {
        if (input_routing_mode != Routing_Mode_t::FORWARD)
        {
            std::cerr << RED << "WindFlow Error: wrong use of withBroadcast() in the P_Filter_Builder" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        input_routing_mode = Routing_Mode_t::BROADCAST;
        return *this;
    }

    /** 
     *  \brief Set the REBALANCING routing mode of inputs to the P_Filter
     *         (it forces a re-shuffling before this new operator)
     *  
     *  \return a reference to the builder object
     */ 
    auto &withRebalancing()
    {
        if (input_routing_mode != Routing_Mode_t::FORWARD)
        {
            std::cerr << RED << "WindFlow Error: wrong use of withRebalancing() in the P_Filter_Builder" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        input_routing_mode = Routing_Mode_t::REBALANCING;
        return *this;
    }

    /** 
     *  \brief Set the user logic of the serialize function
     *  
     *  \param _serialize user logic of the serialize function (a function or any callable type)
     *  \return a reference to the builder object
     */ 
    template<typename serialize_F_t>
    auto &withSerializer(serialize_F_t _serialize)
    {
        // static assert to check that the state_t type used by the serialize function
        static_assert(std::is_same<decltype(get_state_t_Serialize(_serialize)), state_t>::value,
                      "WindFlow Compilation Error - serialize function receives a wrong state type (P_Filter_Builder):\n");
        serialize = _serialize;
        isSerializer = true;
        return *this;
    }

    /** 
     *  \brief Set the user logic of the deserialize function
     *  
     *  \param _deserialize user logic of the deserialize function (a function or any callable type)
     *  \return a reference to the builder object
     */ 
    template<typename deserialize_F_t>
    auto &withDeserializer(deserialize_F_t _deserialize)
    {
        // static assert to check that the state_t type used by the deserialize function
        static_assert(std::is_same<decltype(get_state_t_Deserialize(_deserialize)), state_t>::value,
                      "WindFlow Compilation Error - deserialize function receives a wrong state type (P_Filter_Builder):\n");
        deserialize = _deserialize;
        isDeserializer = true;
        return *this;
    }

    /** 
     *  \brief Set the value of the user-provided initial state per key by RocksDB
     *  
     *  \param _initial_state value of the user-provided initial state to be created one per key by RocksDB
     *  \return a reference to the builder object
     */ 
    auto &withInitialState(state_t &_initial_state)
    {
        initial_state = _initial_state;
        return *this;
    }

    /** 
     *  \brief Create the P_Filter
     *  
     *  \return a new P_Filter instance
     */ 
    auto build()
    {
        // check presence of serializer and deserializer
        if (!isSerializer || !isDeserializer) {
            std::cerr << RED << "WindFlow Error: missing serializer or deserializer in P_Filter_Builder" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        if (!isKeyBySet && this->sharedDb && this->parallelism > 1) {
            std::cerr << RED << "WindFlow Error: P_Filter created with shared DB and without a key extractor" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);         
        }
        return p_filter_t(func,
                          key_extr,
                          this->parallelism,
                          this->name,
                          this->dbpath,
                          input_routing_mode,
                          this->outputBatchSize,
                          this->closing_func,
                          serialize,
                          deserialize,
                          this->deleteDb,
                          initial_state,
                          this->sharedDb,
                          this->options,
                          this->read_options,
                          this->write_options);
    }
};

/** 
 *  \class P_Map_Builder
 *  
 *  \brief Builder of the P_Map operator
 *  
 *  Builder class to ease the creation of the P_Map operator.
 */ 
template<typename p_map_func_t, typename key_t=empty_key_t>
class P_Map_Builder: public P_Basic_Builder<P_Map_Builder, p_map_func_t, key_t>
{
private:
    template<typename T1, typename T2> friend class P_Map_Builder;
    p_map_func_t func; // functional logic of the P_Map
    using tuple_t = decltype(get_tuple_t_P_Map(func)); // extracting the tuple_t type and checking the admissible signatures
    using state_t = decltype(get_state_t_P_Map(func)); // extracting the state_t type and checking the admissible signatures
    using result_t = decltype(get_result_t_P_Map(func)); // extracting the result_t type and checking the admissible signatures
    // static assert to check the signature of the P_Map functional logic
    static_assert(!(std::is_same<tuple_t, std::false_type>::value || std::is_same<state_t, std::false_type>::value || std::is_same<result_t, std::false_type>::value),
                  "WindFlow Compilation Error - unknown signature passed to the P_Map_Builder:\n"
                  "  Candidate 1 : void(tuple_t &, state_t &)\n"
                  "  Candidate 2 : void(tuple_t &, state_t &, RuntimeContext &)\n"
                  "  Candidate 3 : result_t(const tuple_t &, state_t &)\n"
                  "  Candidate 4 : result_t(const tuple_t &, state_t &, RuntimeContext &)\n");
    // static assert to check that the tuple_t type must be default constructible
    static_assert(std::is_default_constructible<tuple_t>::value,
                  "WindFlow Compilation Error - tuple_t type must be default constructible (P_Map_Builder):\n");
    // static assert to check that the state_t type must be default constructible
    static_assert(std::is_default_constructible<state_t>::value,
                  "WindFlow Compilation Error - state_t type must be default constructible (P_Map_Builder):\n");
    // static assert to check that the result_t type must be default constructible
    static_assert(std::is_default_constructible<result_t>::value,
        "WindFlow Compilation Error - result_t type must be default constructible (P_Map_Builder):\n");
    using keyextr_func_t = std::function<key_t(const tuple_t &)>; // type of the key extractor
    using serialize_func_t = std::function<std::string(state_t &)>; // type of the serialization function
    using deserialize_func_t = std::function<state_t(std::string &)>; // type of the deserialization function
    using p_map_t = P_Map<p_map_func_t, keyextr_func_t>; // type of the P_Map to be created by the builder
    serialize_func_t serialize; // serialization function of the state
    deserialize_func_t deserialize; // deserialization function of the state
    state_t initial_state; // initial state to be created one per key by RocksDB
    Routing_Mode_t input_routing_mode = Routing_Mode_t::FORWARD; // routing mode of inputs to the P_Map
    keyextr_func_t key_extr = [](const tuple_t &t) -> key_t { return key_t(); }; // key extractor
    bool isKeyBySet = false; // true if a key extractor has been provided      
    bool isSerializer = false; // flag stating if the serialized has been provided or not
    bool isDeserializer = false; // flag stating if the deserialized has been provided or not

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _func functional logic of the P_Map (a function or any callable type)
     */ 
    P_Map_Builder(p_map_func_t _func): func(_func) {}

    /** 
     *  \brief Set the KEYBY routing mode of inputs to the P_Map
     *  
     *  \param _key_extr key extractor functional logic (a function or any callable type)
     *  \return a new builder object with the right key type
     */ 
    template<typename new_keyextr_func_t>
    auto withKeyBy(new_keyextr_func_t _key_extr)
    {
        // static assert to check the signature
        static_assert(!std::is_same<decltype(get_tuple_t_KeyExtr(_key_extr)), std::false_type>::value,
                      "WindFlow Compilation Error - unknown signature passed to withKeyBy (P_Map_Builder):\n"
                      "  Candidate : key_t(const tuple_t &)\n");
        // static assert to check that the tuple_t type of the new key extractor is the right one
        static_assert(std::is_same<decltype(get_tuple_t_KeyExtr(_key_extr)), tuple_t>::value,
                      "WindFlow Compilation Error - key extractor receives a wrong input type (P_Map_Builder):\n");
        using new_key_t = decltype(get_key_t_KeyExtr(_key_extr)); // extract the key type
        // static assert to check the new_key_t type
        static_assert(!std::is_same<new_key_t, void>::value,
                      "WindFlow Compilation Error - key type cannot be void (P_Map_Builder):\n");
        // static assert to check that new_key_t is default constructible
        static_assert(std::is_default_constructible<new_key_t>::value,
                      "WindFlow Compilation Error - key type must be default constructible (P_Map_Builder):\n");
        if (input_routing_mode != Routing_Mode_t::FORWARD) {
            std::cerr << RED << "WindFlow Error: wrong use of withKeyBy() in the P_Map_Builder" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        P_Map_Builder<p_map_func_t, new_key_t> new_builder(func);
        new_builder.name = this->name;
        new_builder.parallelism = this->parallelism;
        new_builder.outputBatchSize = this->outputBatchSize;
        new_builder.closing_func = this->closing_func;
        new_builder.dbpath = this->dbpath;
        new_builder.deleteDb = this->deleteDb;
        new_builder.sharedDb = this->sharedDb;
        new_builder.options = this->options;
        new_builder.read_options = this->read_options;
        new_builder.write_options = this->write_options;
        new_builder.serialize = serialize;
        new_builder.deserialize = deserialize;
        new_builder.initial_state = initial_state;
        new_builder.input_routing_mode = Routing_Mode_t::KEYBY;
        new_builder.key_extr = _key_extr;
        new_builder.isKeyBySet = true;
        new_builder.isSerializer = isSerializer;
        new_builder.isDeserializer = isDeserializer;
        return new_builder;
    }

    /** 
     *  \brief Set the BROADCAST routing mode of inputs to the P_Map
     *  
     *  \return a reference to the builder object
     */ 
    auto &withBroadcast()
    {
        if (input_routing_mode != Routing_Mode_t::FORWARD)
        {
            std::cerr << RED << "WindFlow Error: wrong use of withBroadcast() in the P_Map_Builder" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        input_routing_mode = Routing_Mode_t::BROADCAST;
        return *this;
    }

    /** 
     *  \brief Set the REBALANCING routing mode of inputs to the P_Map
     *         (it forces a re-shuffling before this new operator)
     *  
     *  \return a reference to the builder object
     */ 
    auto &withRebalancing()
    {
        if (input_routing_mode != Routing_Mode_t::FORWARD)
        {
            std::cerr << RED << "WindFlow Error: wrong use of withRebalancing() in the P_Map_Builder" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        input_routing_mode = Routing_Mode_t::REBALANCING;
        return *this;
    }

    /** 
     *  \brief Set the user logic of the serialize function
     *  
     *  \param _serialize user logic of the serialize function (a function or any callable type)
     *  \return a reference to the builder object
     */ 
    template<typename serialize_F_t>
    auto &withSerializer(serialize_F_t _serialize)
    {
        // static assert to check that the state_t type used by the serialize function
        static_assert(std::is_same<decltype(get_state_t_Serialize(_serialize)), state_t>::value,
                      "WindFlow Compilation Error - serialize function receives a wrong state type (P_Map_Builder):\n");
        serialize = _serialize;
        isSerializer = true;
        return *this;
    }

    /** 
     *  \brief Set the user logic of the deserialize function
     *  
     *  \param _deserialize user logic of the deserialize function (a function or any callable type)
     *  \return a reference to the builder object
     */ 
    template<typename deserialize_F_t>
    auto &withDeserializer(deserialize_F_t _deserialize)
    {
        // static assert to check that the state_t type used by the deserialize function
        static_assert(std::is_same<decltype(get_state_t_Deserialize(_deserialize)), state_t>::value,
                      "WindFlow Compilation Error - deserialize function receives a wrong state type (P_Map_Builder):\n");
        deserialize = _deserialize;
        isDeserializer = true;
        return *this;
    }

    /** 
     *  \brief Set the value of the user-provided initial state per key by RocksDB
     *  
     *  \param _initial_state value of the user-provided initial state to be created one per key by RocksDB
     *  \return a reference to the builder object
     */ 
    auto &withInitialState(state_t &_initial_state)
    {
        initial_state = _initial_state;
        return *this;
    }

    /** 
     *  \brief Create the P_Map
     *  
     *  \return a new P_Map instance
     */ 
    auto build()
    {
        // check presence of serializer and deserializer
        if (!isSerializer || !isDeserializer) {
            std::cerr << RED << "WindFlow Error: missing serializer or deserializer in P_Map_Builder" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        if (!isKeyBySet && this->sharedDb && this->parallelism > 1) {
            std::cerr << RED << "WindFlow Error: P_Map created with shared DB and without a key extractor" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);         
        }
        return p_map_t(func,
                       key_extr,
                       this->parallelism,
                       this->name,
                       this->dbpath,
                       input_routing_mode,
                       this->outputBatchSize,
                       this->closing_func,
                       serialize,
                       deserialize,
                       this->deleteDb,
                       initial_state,
                       this->sharedDb,
                       this->options,
                       this->read_options,
                       this->write_options);
    }
};

/** 
 *  \class P_FlatMap_Builder
 *  
 *  \brief Builder of the P_FlatMap operator
 *  
 *  Builder class to ease the creation of the P_FlatMap operator.
 */ 
template<typename p_flatmap_func_t, typename key_t=empty_key_t>
class P_FlatMap_Builder: public P_Basic_Builder<P_FlatMap_Builder, p_flatmap_func_t, key_t>
{
private:
    template<typename T1, typename T2> friend class P_FlatMap_Builder;
    p_flatmap_func_t func; // functional logic of the P_FlatMap
    using tuple_t = decltype(get_tuple_t_P_FlatMap(func)); // extracting the tuple_t type and checking the admissible signatures
    using state_t = decltype(get_state_t_P_FlatMap(func)); // extracting the state_t type and checking the admissible signatures    
    using result_t = decltype(get_result_t_P_FlatMap(func)); // extracting the result_t type and checking the admissible signatures
    // static assert to check the signature of the P_FlatMap functional logic
    static_assert(!(std::is_same<tuple_t, std::false_type>::value || std::is_same<state_t, std::false_type>::value || std::is_same<result_t, std::false_type>::value),
                  "WindFlow Compilation Error - unknown signature passed to the P_FlatMap_Builder:\n"
                  "  Candidate 1 : void(const tuple_t &, state_t &, Shipper<result_t> &)\n"
                  "  Candidate 2 : void(const tuple_t &, state_t &, Shipper<result_t> &, RuntimeContext &)\n");
    // static assert to check that the tuple_t type must be default constructible
    static_assert(std::is_default_constructible<tuple_t>::value,
                  "WindFlow Compilation Error - tuple_t type must be default constructible (P_FlatMap_Builder):\n");
    // static assert to check that the state_t type must be default constructible
    static_assert(std::is_default_constructible<state_t>::value,
                  "WindFlow Compilation Error - state_t type must be default constructible (P_FlatMap_Builder):\n");
    // static assert to check that the result_t type must be default constructible
    static_assert(std::is_default_constructible<result_t>::value,
                  "WindFlow Compilation Error - result_t type must be default constructible (P_FlatMap_Builder):\n");
    using keyextr_func_t = std::function<key_t(const tuple_t &)>; // type of the key extractor
    using serialize_func_t = std::function<std::string(state_t &)>; // type of the serialization function
    using deserialize_func_t = std::function<state_t(std::string &)>; // type of the deserialization function
    using p_flatmap_t = P_FlatMap<p_flatmap_func_t, keyextr_func_t>; // type of the P_FlatMap to be created by the builder
    serialize_func_t serialize; // serialization function of the state
    deserialize_func_t deserialize; // deserialization function of the state
    state_t initial_state; // initial state to be created one per key by RocksDB
    Routing_Mode_t input_routing_mode = Routing_Mode_t::FORWARD; // routing mode of inputs to the P_FlatMap
    keyextr_func_t key_extr = [](const tuple_t &t) -> key_t { return key_t(); }; // key extractor
    bool isKeyBySet = false; // true if a key extractor has been provided      
    bool isSerializer = false; // flag stating if the serialized has been provided or not
    bool isDeserializer = false; // flag stating if the deserialized has been provided or not

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _func functional logic of the P_FlatMap (a function or any callable type)
     */ 
    P_FlatMap_Builder(p_flatmap_func_t _func): func(_func) {}

    /** 
     *  \brief Set the KEYBY routing mode of inputs to the P_FlatMap
     *  
     *  \param _key_extr key extractor functional logic (a function or any callable type)
     *  \return a new builder object with the right key type
     */ 
    template<typename new_keyextr_func_t>
    auto withKeyBy(new_keyextr_func_t _key_extr)
    {
        // static assert to check the signature
        static_assert(!std::is_same<decltype(get_tuple_t_KeyExtr(_key_extr)), std::false_type>::value,
                      "WindFlow Compilation Error - unknown signature passed to withKeyBy (P_FlatMap_Builder):\n"
                      "  Candidate : key_t(const tuple_t &)\n");
        // static assert to check that the tuple_t type of the new key extractor is the right one
        static_assert(std::is_same<decltype(get_tuple_t_KeyExtr(_key_extr)), tuple_t>::value,
                      "WindFlow Compilation Error - key extractor receives a wrong input type (P_FlatMap_Builder):\n");
        using new_key_t = decltype(get_key_t_KeyExtr(_key_extr)); // extract the key type
        // static assert to check the new_key_t type
        static_assert(!std::is_same<new_key_t, void>::value,
                      "WindFlow Compilation Error - key type cannot be void (P_FlatMap_Builder):\n");
        // static assert to check that new_key_t is default constructible
        static_assert(std::is_default_constructible<new_key_t>::value,
                      "WindFlow Compilation Error - key type must be default constructible (P_FlatMap_Builder):\n");
        if (input_routing_mode != Routing_Mode_t::FORWARD) {
            std::cerr << RED << "WindFlow Error: wrong use of withKeyBy() in the P_FlatMap_Builder" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        P_FlatMap_Builder<p_flatmap_func_t, new_key_t> new_builder(func);
        new_builder.name = this->name;
        new_builder.parallelism = this->parallelism;
        new_builder.outputBatchSize = this->outputBatchSize;
        new_builder.closing_func = this->closing_func;
        new_builder.dbpath = this->dbpath;
        new_builder.deleteDb = this->deleteDb;
        new_builder.sharedDb = this->sharedDb;
        new_builder.options = this->options;
        new_builder.read_options = this->read_options;
        new_builder.write_options = this->write_options;
        new_builder.serialize = serialize;
        new_builder.deserialize = deserialize;
        new_builder.initial_state = initial_state;
        new_builder.input_routing_mode = Routing_Mode_t::KEYBY;
        new_builder.key_extr = _key_extr;
        new_builder.isKeyBySet = true;
        new_builder.isSerializer = isSerializer;
        new_builder.isDeserializer = isDeserializer;
        return new_builder;
    }

    /** 
     *  \brief Set the BROADCAST routing mode of inputs to the P_FlatMap
     *  
     *  \return a reference to the builder object
     */ 
    auto &withBroadcast()
    {
        if (input_routing_mode != Routing_Mode_t::FORWARD)
        {
            std::cerr << RED << "WindFlow Error: wrong use of withBroadcast() in the P_FlatMap_Builder" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        input_routing_mode = Routing_Mode_t::BROADCAST;
        return *this;
    }

    /** 
     *  \brief Set the REBALANCING routing mode of inputs to the P_FlatMap
     *         (it forces a re-shuffling before this new operator)
     *  
     *  \return a reference to the builder object
     */ 
    auto &withRebalancing()
    {
        if (input_routing_mode != Routing_Mode_t::FORWARD)
        {
            std::cerr << RED << "WindFlow Error: wrong use of withRebalancing() in the P_FlatMap_Builder" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        input_routing_mode = Routing_Mode_t::REBALANCING;
        return *this;
    }

    /** 
     *  \brief Set the user logic of the serialize function
     *  
     *  \param _serialize user logic of the serialize function (a function or any callable type)
     *  \return a reference to the builder object
     */ 
    template<typename serialize_F_t>
    auto &withSerializer(serialize_F_t _serialize)
    {
        // static assert to check that the state_t type used by the serialize function
        static_assert(std::is_same<decltype(get_state_t_Serialize(_serialize)), state_t>::value,
                      "WindFlow Compilation Error - serialize function receives a wrong state type (P_FlatMap_Builder):\n");
        serialize = _serialize;
        isSerializer = true;
        return *this;
    }

    /** 
     *  \brief Set the user logic of the deserialize function
     *  
     *  \param _deserialize user logic of the deserialize function (a function or any callable type)
     *  \return a reference to the builder object
     */ 
    template<typename deserialize_F_t>
    auto &withDeserializer(deserialize_F_t _deserialize)
    {
        // static assert to check that the state_t type used by the deserialize function
        static_assert(std::is_same<decltype(get_state_t_Deserialize(_deserialize)), state_t>::value,
                      "WindFlow Compilation Error - deserialize function receives a wrong state type (P_FlatMap_Builder):\n");
        deserialize = _deserialize;
        isDeserializer = true;
        return *this;
    }

    /** 
     *  \brief Set the value of the user-provided initial state per key by RocksDB
     *  
     *  \param _initial_state value of the user-provided initial state to be created one per key by RocksDB
     *  \return a reference to the builder object
     */ 
    auto &withInitialState(state_t &_initial_state)
    {
        initial_state = _initial_state;
        return *this;
    }

    /** 
     *  \brief Create the P_FlatMap
     *  
     *  \return a new P_FlatMap instance
     */ 
    auto build()
    {
        // check presence of serializer and deserializer
        if (!isSerializer || !isDeserializer) {
            std::cerr << RED << "WindFlow Error: missing serializer or deserializer in P_FlatMap_Builder" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        if (!isKeyBySet && this->sharedDb && this->parallelism > 1) {
            std::cerr << RED << "WindFlow Error: P_FlatMap created with shared DB and without a key extractor" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);         
        }
        return p_flatmap_t(func,
                           key_extr,
                           this->parallelism,
                           this->name,
                           this->dbpath,
                           input_routing_mode,
                           this->outputBatchSize,
                           this->closing_func,
                           serialize,
                           deserialize,
                           this->deleteDb,
                           initial_state,
                           this->sharedDb,
                           this->options,
                           this->read_options,
                           this->write_options);
    }
};

/** 
 *  \class P_Reduce_Builder
 *  
 *  \brief Builder of the P_Reduce operator
 *  
 *  Builder class to ease the creation of the P_Reduce operator.
 */ 
template<typename p_reduce_func_t, typename key_t=empty_key_t>
class P_Reduce_Builder: public P_Basic_Builder<P_Reduce_Builder, p_reduce_func_t, key_t>
{
private:
    template<typename T1, typename T2> friend class P_Reduce_Builder;
    p_reduce_func_t func; // functional logic of the P_Reduce
    using tuple_t = decltype(get_tuple_t_P_Reduce(func)); // extracting the tuple_t type and checking the admissible signatures
    using state_t = decltype(get_state_t_P_Reduce(func)); // extracting the state_t type and checking the admissible signatures
    // static assert to check the signature of the P_Reduce functional logic
    static_assert(!(std::is_same<tuple_t, std::false_type>::value || std::is_same<state_t, std::false_type>::value),
                  "WindFlow Compilation Error - unknown signature passed to the P_Reduce_Builder:\n"
                  "  Candidate 1 : void(const tuple_t &, state_t &)\n"
                  "  Candidate 2 : void(const tuple_t &, state_t &, RuntimeContext &)\n");
    // static assert to check that the tuple_t type must be default constructible
    static_assert(std::is_default_constructible<tuple_t>::value,
                  "WindFlow Compilation Error - tuple_t type must be default constructible (P_Reduce_Builder):\n");
    // static assert to check that the state_t type must be default constructible
    static_assert(std::is_default_constructible<state_t>::value,
                  "WindFlow Compilation Error - state_t type must be default constructible (P_Reduce_Builder):\n");
    using keyextr_func_t = std::function<key_t(const tuple_t &)>; // type of the key extractor
    using p_reduce_t = P_Reduce<p_reduce_func_t, keyextr_func_t>; // type of the P_Reduce to be created by the builder
    using serialize_func_t = std::function<std::string(state_t &)>; // type of the serialization function
    using deserialize_func_t = std::function<state_t(std::string &)>; // type of the deserialization function
    serialize_func_t serialize; // serialization function of the state
    deserialize_func_t deserialize; // deserialization function of the state
    state_t initial_state; // initial state to be created one per key by RocksDB
    keyextr_func_t key_extr = [](const tuple_t &t) -> key_t { return key_t(); }; // key extractor
    bool isKeyBySet = false; // true if a key extractor has been provided
    bool isSerializer = false; // flag stating if the serialized has been provided or not
    bool isDeserializer = false; // flag stating if the deserialized has been provided or not

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _func functional logic of the P_Reduce (a function or any callable type)
     */ 
    P_Reduce_Builder(p_reduce_func_t _func): func(_func) {}

    /** 
     *  \brief Set the KEYBY routing mode of inputs to the P_Reduce
     *  
     *  \param _key_extr key extractor functional logic (a function or any callable type)
     *  \return a new builder object with the right key type
     */ 
    template<typename new_keyextr_func_t>
    auto withKeyBy(new_keyextr_func_t _key_extr)
    {
        // static assert to check the signature
        static_assert(!std::is_same<decltype(get_tuple_t_KeyExtr(_key_extr)), std::false_type>::value,
                      "WindFlow Compilation Error - unknown signature passed to withKeyBy (P_Reduce_Builder):\n"
                      "  Candidate : key_t(const tuple_t &)\n");
        // static assert to check that the tuple_t type of the new key extractor is the right one
        static_assert(std::is_same<decltype(get_tuple_t_KeyExtr(_key_extr)), tuple_t>::value,
                      "WindFlow Compilation Error - key extractor receives a wrong input type (P_Reduce_Builder):\n");
        using new_key_t = decltype(get_key_t_KeyExtr(_key_extr)); // extract the key type
        // static assert to check the new_key_t type
        static_assert(!std::is_same<new_key_t, void>::value,
                      "WindFlow Compilation Error - key type cannot be void (P_Reduce_Builder):\n");
        // static assert to check that new_key_t is default constructible
        static_assert(std::is_default_constructible<new_key_t>::value,
                      "WindFlow Compilation Error - key type must be default constructible (P_Reduce_Builder):\n");
        P_Reduce_Builder<p_reduce_func_t, new_key_t> new_builder(func);
        new_builder.name = this->name;
        new_builder.parallelism = this->parallelism;
        new_builder.outputBatchSize = this->outputBatchSize;
        new_builder.closing_func = this->closing_func;
        new_builder.dbpath = this->dbpath;
        new_builder.deleteDb = this->deleteDb;
        new_builder.sharedDb = this->sharedDb;
        new_builder.options = this->options;
        new_builder.read_options = this->read_options;
        new_builder.write_options = this->write_options;
        new_builder.serialize = serialize;
        new_builder.deserialize = deserialize;
        new_builder.initial_state = initial_state;
        new_builder.key_extr = _key_extr;
        new_builder.isKeyBySet = true;
        new_builder.isSerializer = isSerializer;
        new_builder.isDeserializer = isDeserializer;
        return new_builder;
    }

    /** 
     *  \brief Set the user logic of the serialize function
     *  
     *  \param _serialize user logic of the serialize function (a function or any callable type)
     *  \return a reference to the builder object
     */ 
    template<typename serialize_F_t>
    auto &withSerializer(serialize_F_t _serialize)
    {
        // static assert to check that the state_t type used by the serialize function
        static_assert(std::is_same<decltype(get_state_t_Serialize(_serialize)), state_t>::value,
                      "WindFlow Compilation Error - serialize function receives a wrong state type (P_Reduce_Builder):\n");
        serialize = _serialize;
        isSerializer = true;
        return *this;
    }

    /** 
     *  \brief Set the user logic of the deserialize function
     *  
     *  \param _deserialize user logic of the deserialize function (a function or any callable type)
     *  \return a reference to the builder object
     */ 
    template<typename deserialize_F_t>
    auto &withDeserializer(deserialize_F_t _deserialize)
    {
        // static assert to check that the state_t type used by the deserialize function
        static_assert(std::is_same<decltype(get_state_t_Deserialize(_deserialize)), state_t>::value,
                      "WindFlow Compilation Error - deserialize function receives a wrong state type (P_Reduce_Builder):\n");
        deserialize = _deserialize;
        isDeserializer = true;
        return *this;
    }

    /** 
     *  \brief Set the value of the user-provided initial state per key by RocksDB
     *  
     *  \param _initial_state value of the user-provided initial state to be created one per key by RocksDB
     *  \return a reference to the builder object
     */ 
    auto &withInitialState(state_t &_initial_state)
    {
        initial_state = _initial_state;
        return *this;
    }

    /**
     *  \brief Create the Reduce
     *
     *  \return a new Reduce instance
     */
    auto build()
    {
        // check the presence of a key extractor
        if (!isKeyBySet && this->parallelism > 1)
        {
            std::cerr << RED << "WindFlow Error: P_Reduce with paralellism > 1 requires a key extractor" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // check presence of serializer and deserializer
        if (!isSerializer || !isDeserializer) {
            std::cerr << RED << "WindFlow Error: missing serializer or deserializer in P_Reduce_Builder" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        return p_reduce_t(func,
                          key_extr,
                          this->parallelism,
                          this->name,
                          this->dbpath,
                          this->outputBatchSize,
                          this->closing_func,
                          serialize,
                          deserialize,
                          this->deleteDb,
                          initial_state,
                          this->sharedDb,
                          this->options,
                          this->read_options,
                          this->write_options);
    }
};

/** 
 *  \class P_Sink_Builder
 *  
 *  \brief Builder of the P_Sink operator
 *  
 *  Builder class to ease the creation of the P_Sink operator.
 */ 
template<typename p_sink_func_t, typename key_t=empty_key_t>
class P_Sink_Builder: public P_Basic_Builder<P_Sink_Builder, p_sink_func_t, key_t>
{
private:
    template<typename T1, typename T2> friend class P_Sink_Builder;
    p_sink_func_t func; // functional logic of the P_Sink
    using tuple_t = decltype(get_tuple_t_P_Sink(func)); // extracting the tuple_t type and checking the admissible signatures
    using state_t = decltype(get_state_t_P_Sink(func)); // extracting the state_t type and checking the admissible signatures
    // static assert to check the signature of the P_Sink functional logic
    static_assert(!(std::is_same<tuple_t, std::false_type>::value || std::is_same<state_t, std::false_type>::value),
                  "WindFlow Compilation Error - unknown signature passed to the P_Sink_Builder:\n"
                  "  Candidate 1 : void(std::optional<tuple_t> &, state_t &)\n"
                  "  Candidate 2 : void(std::optional<tuple_t> &, state_t &, RuntimeContext &)\n"
                  "  Candidate 3 : void(std::optional<std::reference_wrapper<tuple_t>>, state_t &);\n"
                  "  Candidate 4 : void(std::optional<std::reference_wrapper<tuple_t>>, state_t &, RuntimeContext &)\n");
    // static assert to check that the tuple_t type must be default constructible
    static_assert(std::is_default_constructible<tuple_t>::value,
                  "WindFlow Compilation Error - tuple_t type must be default constructible (P_Sink_Builder):\n");
    // static assert to check that the state_t type must be default constructible
    static_assert(std::is_default_constructible<state_t>::value,
                  "WindFlow Compilation Error - state_t type must be default constructible (P_Sink_Builder):\n");
    using keyextr_func_t = std::function<key_t(const tuple_t &)>; // type of the key extractor
    using p_sink_t = P_Sink<p_sink_func_t, keyextr_func_t>; // type of the P_Sink to be created by the builder
    using serialize_func_t = std::function<std::string(state_t &)>; // type of the serialization function
    using deserialize_func_t = std::function<state_t(std::string &)>; // type of the deserialization function
    serialize_func_t serialize; // serialization function of the state
    deserialize_func_t deserialize; // deserialization function of the state
    state_t initial_state; // initial state to be created one per key by RocksDB
    Routing_Mode_t input_routing_mode = Routing_Mode_t::FORWARD; // routing mode of inputs to the P_Sink
    keyextr_func_t key_extr = [](const tuple_t &t) -> key_t { return key_t(); }; // key extractor
    bool isKeyBySet = false; // true if a key extractor has been provided       
    bool isSerializer = false; // flag stating if the serialized has been provided or not
    bool isDeserializer = false; // flag stating if the deserialized has been provided or not

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _func functional logic of the P_Sink (a function or any callable type)
     */ 
    P_Sink_Builder(p_sink_func_t _func): func(_func) {}

    /** 
     *  \brief Set the KEYBY routing mode of inputs to the P_Sink
     *  
     *  \param _key_extr key extractor functional logic (a function or any callable type)
     *  \return a new builder object with the right key type
     */ 
    template<typename new_keyextr_func_t>
    auto withKeyBy(new_keyextr_func_t _key_extr)
    {
        // static assert to check the signature
        static_assert(!std::is_same<decltype(get_tuple_t_KeyExtr(_key_extr)), std::false_type>::value,
                      "WindFlow Compilation Error - unknown signature passed to withKeyBy (P_Sink_Builder):\n"
                      "  Candidate : key_t(const tuple_t &)\n");
        // static assert to check that the tuple_t type of the new key extractor is the right one
        static_assert(std::is_same<decltype(get_tuple_t_KeyExtr(_key_extr)), tuple_t>::value,
                      "WindFlow Compilation Error - key extractor receives a wrong input type (P_Sink_Builder):\n");
        using new_key_t = decltype(get_key_t_KeyExtr(_key_extr)); // extract the key type
        // static assert to check the new_key_t type
        static_assert(!std::is_same<new_key_t, void>::value,
                      "WindFlow Compilation Error - key type cannot be void (P_Sink_Builder):\n");
        // static assert to check that new_key_t is default constructible
        static_assert(std::is_default_constructible<new_key_t>::value,
                      "WindFlow Compilation Error - key type must be default constructible (P_Sink_Builder):\n");
        if (input_routing_mode != Routing_Mode_t::FORWARD) {
            std::cerr << RED << "WindFlow Error: wrong use of withKeyBy() in the P_Sink_Builder" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        P_Sink_Builder<p_sink_func_t, new_key_t> new_builder(func);
        new_builder.name = this->name;
        new_builder.parallelism = this->parallelism;
        new_builder.outputBatchSize = this->outputBatchSize;
        new_builder.closing_func = this->closing_func;
        new_builder.dbpath = this->dbpath;
        new_builder.deleteDb = this->deleteDb;
        new_builder.sharedDb = this->sharedDb;
        new_builder.options = this->options;
        new_builder.read_options = this->read_options;
        new_builder.write_options = this->write_options;
        new_builder.serialize = serialize;
        new_builder.deserialize = deserialize;
        new_builder.initial_state = initial_state;
        new_builder.input_routing_mode = Routing_Mode_t::KEYBY;
        new_builder.key_extr = _key_extr;
        new_builder.isKeyBySet = true;
        new_builder.isSerializer = isSerializer;
        new_builder.isDeserializer = isDeserializer;
        return new_builder;
    }

    /** 
     *  \brief Set the BROADCAST routing mode of inputs to the P_Sink
     *  
     *  \return a reference to the builder object
     */ 
    auto &withBroadcast()
    {
        if (input_routing_mode != Routing_Mode_t::FORWARD)
        {
            std::cerr << RED << "WindFlow Error: wrong use of withBroadcast() in the P_Sink_Builder" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        input_routing_mode = Routing_Mode_t::BROADCAST;
        return *this;
    }

    /** 
     *  \brief Set the REBALANCING routing mode of inputs to the P_Sink
     *         (it forces a re-shuffling before this new operator)
     *  
     *  \return a reference to the builder object
     */ 
    auto &withRebalancing()
    {
        if (input_routing_mode != Routing_Mode_t::FORWARD)
        {
            std::cerr << RED << "WindFlow Error: wrong use of withRebalancing() in the P_Sink_Builder" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        input_routing_mode = Routing_Mode_t::REBALANCING;
        return *this;
    }

    /** 
     *  \brief Set the user logic of the serialize function
     *  
     *  \param _serialize user logic of the serialize function (a function or any callable type)
     *  \return a reference to the builder object
     */ 
    template<typename serialize_F_t>
    auto &withSerializer(serialize_F_t _serialize)
    {
        // static assert to check that the state_t type used by the serialize function
        static_assert(std::is_same<decltype(get_state_t_Serialize(_serialize)), state_t>::value,
                      "WindFlow Compilation Error - serialize function receives a wrong state type (P_Sink_Builder):\n");
        serialize = _serialize;
        isSerializer = true;
        return *this;
    }

    /** 
     *  \brief Set the user logic of the deserialize function
     *  
     *  \param _deserialize user logic of the deserialize function (a function or any callable type)
     *  \return a reference to the builder object
     */ 
    template<typename deserialize_F_t>
    auto &withDeserializer(deserialize_F_t _deserialize)
    {
        // static assert to check that the state_t type used by the deserialize function
        static_assert(std::is_same<decltype(get_state_t_Deserialize(_deserialize)), state_t>::value,
                      "WindFlow Compilation Error - deserialize function receives a wrong state type (P_Sink_Builder):\n");
        deserialize = _deserialize;
        isDeserializer = true;
        return *this;
    }

    /// Delete withOutputBatchSize method
    P_Sink_Builder<p_sink_func_t, key_t> &withOutputBatchSize(size_t _outputBatchSize) = delete;

    /** 
     *  \brief Set the value of the user-provided initial state per key by RocksDB
     *  
     *  \param _initial_state value of the user-provided initial state to be created one per key by RocksDB
     *  \return a reference to the builder object
     */ 
    auto &withInitialState(state_t &_initial_state)
    {
        initial_state = _initial_state;
        return *this;
    }

    /** 
     *  \brief Create the P_Sink
     *  
     *  \return a new Sink instance
     */ 
    auto build()
    {
        // check presence of serializer and deserializer
        if (!isSerializer || !isDeserializer) {
            std::cerr << RED << "WindFlow Error: missing serializer or deserializer in P_Sink_Builder" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        if (!isKeyBySet && this->sharedDb && this->parallelism > 1) {
            std::cerr << RED << "WindFlow Error: P_Sink created with shared DB and without a key extractor" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);         
        }
        return p_sink_t(func,
                        key_extr,
                        this->parallelism,
                        this->name,
                        this->dbpath,
                        input_routing_mode,
                        this->closing_func,
                        serialize,
                        deserialize,
                        this->deleteDb,
                        initial_state,
                        this->sharedDb,
                        this->options,
                        this->read_options,
                        this->write_options);
    }
};

/** 
 *  \class P_Keyed_Windows_Builder
 *  
 *  \brief Builder of the P_Keyed_Windows operator
 *  
 *  Builder class to ease the creation of the P_Keyed_Windows operator.
 */ 
template<typename win_func_t, typename key_t=empty_key_t>
class P_Keyed_Windows_Builder: public P_Basic_Builder<P_Keyed_Windows_Builder, win_func_t, key_t>
{
private:
    template<typename T1, typename T3> friend class P_Keyed_Windows_Builder;
    win_func_t func; // functional logic of the P_Keyed_Windows
    using tuple_t = decltype(get_tuple_t_Win(func));   // extracting the tuple_t type and checking the admissible signatures
    using result_t = decltype(get_result_t_Win(func)); // extracting the result_t type and checking the admissible signatures
    // static assert to check the signature of the P_Keyed_Windows functional logic
    static_assert(!(std::is_same<tuple_t, std::false_type>::value || std::is_same<result_t, std::false_type>::value),
                  "WindFlow Compilation Error - unknown signature passed to the P_Keyed_Windows_Builder:\n"
                  "  Candidate 1 : void(const Iterable<tuple_t> &, result_t &)\n"
                  "  Candidate 2 : void(const Iterable<tuple_t> &, result_t &, RuntimeContext &)\n"
                  "  Candidate 3 : void(const tuple_t &, result_t &)\n"
                  "  Candidate 4 : void(const tuple_t &, result_t &, RuntimeContext &)\n");
    // static assert to check that the tuple_t type must be default constructible
    static_assert(std::is_default_constructible<tuple_t>::value,
                  "WindFlow Compilation Error - tuple_t type must be default constructible (P_Keyed_Windows_Builder):\n");
    static_assert(std::is_default_constructible<result_t>::value,
                  "WindFlow Compilation Error - result_t type must be default constructible (P_Keyed_Windows_Builder):\n");
    using keyextr_func_t = std::function<key_t(const tuple_t &)>; // type of the key extractor
    using p_keyed_wins_t = P_Keyed_Windows<win_func_t, keyextr_func_t>; // type of the P_Keyed_Windows to be created by the builder
    using serialize_tuple_func_t = std::function<std::string(tuple_t &)>; // type of the tuple serialization function
    using deserialize_tuple_func_t = std::function<tuple_t(std::string &)>; // type of the tuple deserialization function
    using serialize_result_func_t = std::function<std::string(result_t &)>; // type of the result serialization function
    using deserialize_result_func_t = std::function<result_t(std::string &)>; // type of the result deserialization function
    serialize_tuple_func_t tuple_serialize;
    deserialize_tuple_func_t tuple_deserialize;
    serialize_result_func_t result_serialize;
    deserialize_result_func_t result_deserialize;
    keyextr_func_t key_extr = [](const tuple_t &t) -> key_t { return key_t(); }; // key extractor
    bool isKeyBySet = false; // true if a key extractor has been provided
    size_t frag_bytes = sizeof(tuple_t) * 16; // size in bytes of each archive fragment of the stream
    bool results_in_memory = true; // flag stating if results must be kepts in memory or on RocksDB
    bool isTupleFunctions = false; // flag stating if the tuple serializer/deserializer have been provided
    bool isResultFunctions = false; // flag stating if the result serializer/deserializer have been provided
    uint64_t win_len=0; // window length in number of tuples or in time units
    uint64_t slide_len=0; // slide length in number of tuples or in time units
    uint64_t lateness=0; // lateness in time units
    Win_Type_t winType=Win_Type_t::CB; // window type (CB or TB)

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _func functional logic of the P_Keyed_Windows (a function or any callable type)
     */ 
    P_Keyed_Windows_Builder(win_func_t _func): func(_func) {}

    /** 
     *  \brief Set the configuration for count-based windows
     *  
     *  \param _win_len window length (in number of tuples)
     *  \param _slide_len slide length (in number of tuples)
     *  \return a reference to the builder object
     */ 
    auto &withCBWindows(uint64_t _win_len, uint64_t _slide_len)
    {
        win_len = _win_len;
        slide_len = _slide_len;
        winType = Win_Type_t::CB;
        lateness = 0;
        return *this;
    }

    /** 
     *  \brief Set the configuration for time-based windows
     *  
     *  \param _win_len window length (in microseconds)
     *  \param _slide_len slide length (in microseconds)
     *  \return a reference to the builder object
     */ 
    auto &withTBWindows(std::chrono::microseconds _win_len, std::chrono::microseconds _slide_len)
    {
        win_len = _win_len.count();
        slide_len = _slide_len.count();
        winType = Win_Type_t::TB;
        return *this;
    }

    /** 
     *  \brief Set the lateness for time-based windows
     *  
     *  \param _lateness (in microseconds)
     *  \return a reference to the builder object
     */ 
    auto &withLateness(std::chrono::microseconds _lateness)
    {
        if (winType != Win_Type_t::TB) { // check that time-based semantics is used
            std::cerr << RED << "WindFlow Error: lateness can be set only for time-based windows" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        lateness = _lateness.count();
        return *this;
    }

    /** 
     *  \brief Set the KEYBY routing mode of inputs to the P_Keyed_Windows
     *  
     *  \param _key_extr key extractor functional logic (a function or any callable type)
     *  \return a new builder object with the right key type
     */ 
    template<typename new_keyextr_func_t>
    auto withKeyBy(new_keyextr_func_t _key_extr)
    {
        // static assert to check the signature
        static_assert(!std::is_same<decltype(get_tuple_t_KeyExtr(_key_extr)), std::false_type>::value,
                      "WindFlow Compilation Error - unknown signature passed to withKeyBy (P_Keyed_Windows_Builder):\n"
                      "  Candidate : key_t(const tuple_t &)\n");
        // static assert to check that the tuple_t type of the new key extractor is the right one
        static_assert(std::is_same<decltype(get_tuple_t_KeyExtr(_key_extr)), tuple_t>::value,
                      "WindFlow Compilation Error - key extractor receives a wrong input type (P_Keyed_Windows_Builder):\n");
        using new_key_t = decltype(get_key_t_KeyExtr(_key_extr)); // extract the key type
        // static assert to check the new_key_t type
        static_assert(!std::is_same<new_key_t, void>::value,
                      "WindFlow Compilation Error - key type cannot be void (P_Keyed_Windows_Builder):\n");
        // static assert to check that new_key_t is default constructible
        static_assert(std::is_default_constructible<new_key_t>::value,
                      "WindFlow Compilation Error - key type must be default constructible (P_Keyed_Windows_Builder):\n");
        P_Keyed_Windows_Builder<win_func_t, new_key_t> new_builder(func);
        new_builder.name = this->name;
        new_builder.parallelism = this->parallelism;
        new_builder.outputBatchSize = this->outputBatchSize;
        new_builder.closing_func = this->closing_func;
        new_builder.dbpath = this->dbpath;
        new_builder.deleteDb = this->deleteDb;
        new_builder.sharedDb = this->sharedDb;
        new_builder.options = this->options;
        new_builder.read_options = this->read_options;
        new_builder.write_options = this->write_options;
        new_builder.tuple_serialize = tuple_serialize;
        new_builder.tuple_deserialize = tuple_deserialize;
        new_builder.result_serialize = result_serialize;
        new_builder.result_deserialize = result_deserialize;
        new_builder.key_extr = _key_extr;
        new_builder.isKeyBySet = true;
        new_builder.isTupleFunctions = isTupleFunctions;
        new_builder.isResultFunctions = isResultFunctions;
        new_builder.frag_bytes = frag_bytes;
        new_builder.results_in_memory = results_in_memory;
        new_builder.win_len = win_len;
        new_builder.slide_len = slide_len;
        new_builder.lateness = lateness;
        new_builder.winType = winType;
        return new_builder;
    }

    /** 
     *  \brief Set the size in bytes of each archive fragment of the stream
     *  
     *  \param _frag_bytes size in bytes
     *  \return a reference to the builder object
     */ 
    auto &setFragSizeBytes(size_t _frag_bytes)
    {
        frag_bytes = _frag_bytes;
        return *this;
    }

    /** 
     *  \brief Set the tuple serialize/deserialize functions
     *  
     *  \param _tuple_serialize tuple serialization function
     *  \param _tuple_deserialize tuple deserialization function
     *  \return a reference to the builder object
     */ 
    auto &withTupleSerializerAndDeserializer(serialize_tuple_func_t _tuple_serialize,
                                             deserialize_tuple_func_t _tuple_deserialize)
    {
        tuple_serialize = _tuple_serialize;
        tuple_deserialize = _tuple_deserialize;
        isTupleFunctions = true;
        return *this;
    }

    /** 
     *  \brief Set the result serialize/deserialize functions
     *  
     *  \param _result_serialize result serialization function
     *  \param _result_deserialize result deserialization function
     *  \return a reference to the builder object
     */ 
    auto &withResultSerializerAndDeserializer(serialize_result_func_t _result_serialize,
                                              deserialize_result_func_t _result_deserialize)
    {
        result_serialize = _result_serialize;
        result_deserialize = _result_deserialize;
        isResultFunctions = true;
        results_in_memory = false;
        return *this;
    }

    /** 
     *  \brief Create the P_Keyed_Windows
     *  
     *  \return a new P_Keyed_Windows instance
     */ 
    auto build()
    {
        // static asserts to check that result_t is properly constructible
        if constexpr (std::is_same<key_t, empty_key_t>::value) { // case without key
            static_assert(std::is_constructible<result_t, uint64_t>::value,
                          "WindFlow Compilation Error - result_t type must be constructible with a uin64_t (P_Keyed_Windows_Builder):\n");
        }
        else { // case with key
            static_assert(std::is_constructible<result_t, key_t, uint64_t>::value,
                          "WindFlow Compilation Error - result_t type must be constructible with a key_t and uint64_t (P_Keyed_Windows_Builder):\n");
        }
        // check the presence of a key extractor
        if (!isKeyBySet && this->parallelism > 1) {
            std::cerr << RED << "WindFlow Error: P_Keyed_Windows with paralellism > 1 requires a key extractor" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // check the presence of the tuple/result serializer/deserializer according to type of the window processing logic
        if constexpr (std::is_invocable<decltype(func), const Iterable<tuple_t> &, result_t &>::value ||
                      std::is_invocable<decltype(func), const Iterable<tuple_t> &, result_t &, RuntimeContext &>::value) {
            if (!isTupleFunctions) {
                std::cerr << RED << "WindFlow Error: P_Keyed_Windows instantiated with a non-incremental logic without tuple serializer/serializer" << DEFAULT_COLOR << std::endl;
                exit(EXIT_FAILURE);
            }
        }
        else if constexpr (std::is_invocable<decltype(func), const tuple_t &, result_t &>::value ||
                           std::is_invocable<decltype(func), const tuple_t &, result_t &, RuntimeContext &>::value) {
            if (isTupleFunctions) {
                std::cerr << RED << "WindFlow Error: P_Keyed_Windows receives tuple serializer/deserializer with an incremental logic" << DEFAULT_COLOR << std::endl;
                exit(EXIT_FAILURE);
            }
            if (!isResultFunctions) {
                std::cerr << RED << "WindFlow Error: P_Keyed_Windows instantiated with an incremental logic without result serializer/serializer" << DEFAULT_COLOR << std::endl;
                exit(EXIT_FAILURE);
            }
        }
        return p_keyed_wins_t(func,
                              key_extr,
                              this->parallelism,
                              this->name,
                              this->dbpath,
                              this->outputBatchSize,
                              this->closing_func,
                              tuple_serialize,
                              tuple_deserialize,
                              result_serialize,
                              result_deserialize,
                              this->deleteDb,
                              this->sharedDb,
                              this->options,
                              this->read_options,
                              this->write_options,
                              results_in_memory,
                              frag_bytes,
                              win_len,
                              slide_len,
                              lateness,
                              winType);
    }
};

} // namespace wf

#endif
