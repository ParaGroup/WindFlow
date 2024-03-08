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
 *  @file    db_handle.hpp
 *  @author  Gabriele Mencagli and Simone Frassinelli
 *
 *  @brief DB_Handle
 *
 *  @section DB_Handle (Description)
 *
 *  This file implements the DB_Handle class used to interface persistent operators with
 *  the RocksDB support.
 */

#ifndef DB_HANDLE_HPP
#define DB_HANDLE_HPP

// includes
#include<string>
#include<functional>
#include<context.hpp>
#include<persistent/db_options.hpp>
#include<rocksdb/db.h>
#include<rocksdb/options.h>

namespace wf {

#define WRAP_TOKEN "_$$_"

// class DBHandle
template<typename T>
class DBHandle
{
private:
    using serialize_func_t = std::function<std::string(T &)>;   // type of the serialize function
    using deserialize_func_t = std::function<T(std::string &)>; // type of the deserialize function
    using wrapper_t = wrapper_tuple_t<T>;
    rocksdb::DB *db; // pointer to the RocksDB database
    rocksdb::Options options; // options used by RocksDB
    rocksdb::ReadOptions read_options; // read options used by RocksDB
    rocksdb::WriteOptions write_options; // write options used by RocksDB
    serialize_func_t serialize; // serialization function
    deserialize_func_t deserialize; // deserialization function
    T initial_state; // initial value to be used for each new stream key
    bool isOwner; // true if db is owned by this DBHandle, false otherwise
    bool deleteDb; // true if db should be destroyed in the BDHandle destructor (if isOwner), false otherwise
    std::string db_key; // serialized version of the current stream key
    std::string dbpath; // pathname used by db
    size_t whoami; // index of the replica using this DB_Handle

public:
    // Constuctor
    DBHandle(serialize_func_t _serialize,
             deserialize_func_t _deserialize,
             bool _deleteDb,
             std::string _dbpath,
             T _initial_state,
             size_t _whoami):
             db(nullptr),
             serialize(_serialize),
             deserialize(_deserialize),
             initial_state(_initial_state),
             isOwner(false),
             deleteDb(_deleteDb),
             dbpath(_dbpath),
             whoami(_whoami) {}

    // Copy Constructor
    DBHandle(const DBHandle &_other):
             db(nullptr), // it does not copy db
             options(_other.options),
             read_options(_other.read_options),
             write_options(_other.write_options),
             serialize(_other.serialize),
             deserialize(_other.deserialize),
             initial_state(_other.initial_state),
             isOwner(false),
             deleteDb(_other.deleteDb),
             db_key(_other.db_key),
             dbpath(_other.dbpath),
             whoami(_other.whoami) {}

    // Destructor
    ~DBHandle()
    {
        if (isOwner) {
            assert(db != nullptr);
            db->Close();
            if (deleteDb) {
                rocksdb::DestroyDB(dbpath, options);
            }
        }
    }

    // Method to initialize the internal db
    void initDB(rocksdb::DB *_db,
                rocksdb::Options _options,
                rocksdb::ReadOptions _read_options,
                rocksdb::WriteOptions _write_options)
    {
        assert(db == nullptr);
        options = _options;
        read_options = _read_options;
        write_options = _write_options;
        if (_db == nullptr) {
            rocksdb::Status s = rocksdb::DB::Open(options, dbpath, &db);
            assert(s.ok());
            isOwner = true;
        }
        else {
            db = _db;
            assert(!isOwner);
        }
        assert(db != nullptr);
    }

    // Method to get the pointer to the internal DB
    rocksdb::DB *get_internal_db()
    {
        return this->db;
    }

    // Method to obtain a pointer to a copy of this DBHandle (it will steal the db pointer)
    DBHandle<T> *getCopy()
    {
        DBHandle<T> *copy_db = new DBHandle<T>(*this);
        copy_db->db = this->db;
        this->db = nullptr;
        copy_db->isOwner = this->isOwner;
        this->isOwner = false;
        return copy_db;
    }

    // Method to serialize a stream key
    template<typename key_t>
    std::string key_serializer(key_t &_key)
    {
        if constexpr (std::is_same<key_t, std::string>::value) {
            return _key;
        }
        else if constexpr (std::is_same<key_t, size_t>::value) {
            return std::to_string(_key);
        }
        else {
            std::string new_key(sizeof(key_t), L'\0');
            std::memcpy(&new_key[0], &_key, new_key.size());
            return new_key;
        }
    }

    // Method to serialize a fragment key
    template<typename key_t>
    std::string key_serializer(key_t &_key, size_t _idx)
    {
        if constexpr (std::is_same<key_t, std::string>::value) {
            return _key + "_" + std::to_string(_idx);
        }
        else if constexpr (std::is_same<key_t, size_t>::value) {
            return std::to_string(_key) + "_" + std::to_string(_idx);
        }
        else {
            std::string new_key(sizeof(key_t) + sizeof(size_t), L'\0');
            std::memcpy(&new_key[0], &_key, sizeof(key_t));
            std::memcpy(&new_key[sizeof(key_t)], &_idx, sizeof(size_t));
            return new_key;
        }
    }

    // Method to serialize a deque of objects of type T
    std::string T_list_serializer(std::deque<T> &_in)
    {
        std::string out;
        for (T &val: _in) {
            out = out + serialize(val) + WRAP_TOKEN;
        }
        return out;
    }

    // Method to deserialize a deque of objects of type T
    std::deque<T> T_list_deserializer(std::string &_state)
    {
        std::deque<T> out;
        size_t pos = 0;
        std::string token;
        size_t wrap_len = std::string(WRAP_TOKEN).length();
        while ((pos = _state.find(WRAP_TOKEN)) != std::string::npos) {
            token = _state.substr(0, pos);
            T new_wrap = deserialize(token);
            out.push_back(new_wrap);
            _state.erase(0, pos + wrap_len);
        }
        return out;
    }

    // Method to serialize a deque of objects of type wrapper_t
    std::string wrapper_list_serializer(std::deque<wrapper_t> &_list)
    {
        std::string out;
        for (wrapper_t &wrap: _list) {
            out = out + serialize(wrap.tuple) + " " + std::to_string(wrap.index) + "\n";
        }
        return out;
    }

    // Method to deserialize a deque of objects of type wrapper_t
    std::deque<wrapper_t> wrapper_list_deserializer(std::string &_state)
    {
        std::deque<wrapper_t> out;
        size_t pos = 0, innerpos = 0;
        std::string token, innertoken;
        size_t wrap_len = std::string("\n").length();
        size_t inner_wrap_len = std::string(" ").length();
        while ((pos = _state.find("\n")) != std::string::npos) {
            wrapper_t new_wrap;
            token = _state.substr(0, pos);
            innerpos = token.find(" ");
            innertoken = token.substr(0, innerpos);
            new_wrap.tuple = deserialize(innertoken);
            token.erase(0, innerpos + inner_wrap_len);
            new_wrap.index = std::stoull(token, NULL, 10);
            _state.erase(0, pos + wrap_len);
            out.push_back(new_wrap);
        }
        out.shrink_to_fit();
        return out;
    }

    // Method to get an object of type T from a stream key
    template<typename key_t>
    T get(key_t &_key)
    {
        std::string db_val;
        std::string *memory_db_val = nullptr;
        rocksdb::PinnableSlice pinnable_db_val(&db_val);
        T real_val(initial_state);
        rocksdb::Status key_status;
        db_key = key_serializer(_key);
        key_status = db->Get(read_options, db->DefaultColumnFamily(), db_key, &pinnable_db_val);
        if (key_status.ok()) {
            if (pinnable_db_val.IsPinned()) {
                memory_db_val = pinnable_db_val.GetSelf();
            }
            real_val = memory_db_val ? deserialize(*memory_db_val) : deserialize(db_val);
        }
        return real_val;
    }

    // Method to get a deque of objects of type T for a stream key
    template<typename key_t>
    std::deque<T> get_list_result(key_t &_key)
    {
        std::string db_val;
        std::string *memory_db_val = nullptr;
        rocksdb::PinnableSlice pinnable_db_val(&db_val);
        std::deque<T> real_val;
        rocksdb::Status key_status;
        db_key = key_serializer(_key);
        key_status = db->Get(read_options, db->DefaultColumnFamily(), db_key, &pinnable_db_val);
        if (key_status.ok()) {
            if (pinnable_db_val.IsPinned()) {
                memory_db_val = pinnable_db_val.GetSelf();
            }
            real_val = memory_db_val ? T_list_deserializer(*memory_db_val) : T_list_deserializer(db_val);
        }
        return real_val;
    }

    // Method to get a deque of objects of type wrapper_t for a given fragment key
    template<typename key_t>
    std::deque<wrapper_t> get_list_frag(key_t &_key, size_t _idx)
    {
        std::string db_val;
        std::string *memory_db_val = nullptr;
        rocksdb::PinnableSlice pinnable_db_val(&db_val);
        std::deque<wrapper_t> real_val;
        rocksdb::Status key_status;
        db_key = key_serializer(_key, _idx);
        key_status = db->Get(read_options, db->DefaultColumnFamily(), db_key, &pinnable_db_val);
        if (key_status.ok()) {
            if (pinnable_db_val.IsPinned()) {
                memory_db_val = pinnable_db_val.GetSelf();
            }
            real_val = memory_db_val ? wrapper_list_deserializer(*memory_db_val) : wrapper_list_deserializer(db_val);
        }
        return real_val;
    }

    // Method to put a value of type T associated with db_key
    void put(T &_val)
    {
        db->Put(write_options, db_key, serialize(_val));
    }

    // Method to put a value represented by a deque of objects of type wrapper_t associated with a fragment key
    template<typename key_t>
    void put(std::deque<wrapper_t> &_val, key_t &_key, size_t _idx)
    {
        db->Put(write_options, key_serializer(_key, _idx), wrapper_list_serializer(_val));
    }

    // Method to put a value represented by a deque of objects of type T associated with a stream key
    template<typename key_t>
    void put(std::deque<T> &_val, key_t &_key)
    {
        db->Put(write_options, key_serializer(_key), T_list_serializer(_val));
    }

    // Method to delete a fragment key
    template<typename key_t>
    void delete_key(key_t &_key, size_t _idx)
    {
        db->Delete(write_options, key_serializer(_key, _idx));
    }

    // Method to serialize the internal state of type T
    std::string get_internal_serialize(const T &_state)
    {
        T temp_state = _state;
        return this->serialize(temp_state);
    }
};

}; // namespace wf

#endif
