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
 *  @file    local_storage.hpp
 *  @author  Gabriele Mencagli
 *  
 *  @brief Class implementing the local storage maintained per operator replica
 *  
 *  @section LocalStorage (Description)
 *  
 *  This file implements the LocalStorage class. It is a private repository per replica,
 *  which can be used both in the processing logic (if based on a riched variant) and in
 *  the closing function of some operators.
 */ 

#ifndef LOCALSTORAGE_H
#define LOCALSTORAGE_H

/// includes
#include<string>
#include<unordered_map>
#include<assert.h>

namespace wf {

/** 
 *  \class LocalStorage
 *  
 *  \brief Class used to maintain a local storage per operator replica
 *  
 *  This file implements the LocalStorage class. It is a private repository per replica,
 *  which can be used both in the processing logic (if based on a riched variant) and in
 *  the closing function of some operators.
 */ 
class LocalStorage
{
private:
    size_t num_fields; // number of dat fields present in the storage
    std::unordered_map<std::string, void*> storage; // storage implemented by a hashtable

public:
    /// Constructor
    LocalStorage(): num_fields(0) {}

    /// Move Constructor
    LocalStorage(LocalStorage &&_other):
                 num_fields(std::exchange(_other.num_fields, 0)),
                 storage(std::move(_other.storage)) {}

    /** 
     *  \brief Get a reference to the data field with type type_t and name _name
     *         in the storage. If the data field is not in the storage, it creates
     *         a new object of type type_t with the default constructor, and the
     *         data field is added to the storage with the given name
     *  
     *  \param _name name of the data field
     *  \return a reference to the data field
     */ 
    template<typename type_t>
    type_t &get(std::string _name)
    {
        auto it = storage.find(_name);
        if (it == storage.end()) { // the field _name does not exist
            type_t *field = new type_t(); // create the field with _name
            storage.insert(std::make_pair(_name, field));
            num_fields++;
            return *field;
        }
        else { // the field already exists
            type_t *field = reinterpret_cast<type_t *>((*it).second);
            return *field;
        }
    }

    /** 
     *  \brief Delete the data field with type type_t and name _name from the storage.
     *         The method does not have any effect if the data field does not exist
     *  
     *  \param _name name of the data field to be deleted from the storage
     */ 
    template<typename type_t>
    void remove(std::string _name)
    {
        auto it = storage.find(_name);
        type_t *field = reinterpret_cast<type_t *>((*it).second);
        delete field;
        storage.erase(_name);
        num_fields--;
    }

    /** 
     *  \brief Check whether a data field with name _name is present in the storage
     *  
     *  \return true if the data field is present, false otherwise
     */ 
    bool isContained(std::string _name) const
    {
        auto it = storage.find(_name);
        if (it == storage.end()) {
            return false;
        }
        else {
            return true;
        }
    }

    /** 
     *  \brief Get the number of data fields present in the storage
     *  
     *  \return number of data fields in the storage
     */ 
    size_t getSize() const
    {
        return num_fields;
    }

    LocalStorage(const LocalStorage &) = delete; ///< Copy constructor is deleted
    LocalStorage &operator=(const LocalStorage &) = delete; ///< Copy assignment operator is deleted
    LocalStorage &operator=(LocalStorage &&_other) = delete; ///< Move assignment operator is deleted
};

} // namespace wf

#endif
