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
 *  @file    local_storage.hpp
 *  @author  Gabriele Mencagli
 *  @date    31/05/2019
 *  
 *  @brief LocalStorage class to maintain a local storage per operator replica
 *         to be used by several user-defined functions of the same operator.
 *  
 *  @section LocalStorage (Description)
 *  
 *  This file implements the LocalStorage class used to access some data fields that the user
 *  would like to made available by different functions of the same operator.
 */ 

#ifndef LOCALSTORAGE_H
#define LOCALSTORAGE_H

/// includes
#include <string>
#include <unordered_map>

namespace wf {

/** 
 *  \class LocalStorage
 *  
 *  \brief LocalStorage class used to maintain a local storage per operator replica
 *         to be used by several user-defined functions of the same operator.
 *  
 *  This file implements the LocalStorage class used to access some data fields that should
 *  be made available by different functions of the same operator (i.e. the user-defined
 *  business logic code and the closing function, if any).
 */ 
class LocalStorage
{
private:
    size_t n_fields; // number of fields present in the storage
    std::unordered_map<std::string, void *> storage; // storage implemented by a hashmap

public:
    /// Constructor
    LocalStorage(): n_fields(0) {}

    /** 
     *  \brief Return a copy of the value of the data field with type type_t and
     *         name _name in the storage. If the data field is not in the storage,
     *         it returns a value obtained by constructing an object of type type_t
     *         with its default Constructor and the field is added to the storage
     *  
     *  \param _name name of the data field to be retrieved from the storage
     *  \return a copy of the data field
     */ 
    template<typename type_t>
    type_t get(std::string _name)
    {
        auto it = storage.find(_name);
        if (it == storage.end()) { // the field _name does not exist
            // create the field with _name
            type_t *field = new type_t();
            storage.insert(std::make_pair(_name, field));
            n_fields++;
            return *field;
        }
        else { // the field already exists
            type_t *field = reinterpret_cast<type_t *>((*it).second);
            return *field;
        }
    }

    /** 
     *  \brief Put a data field of type type_t and name _name into the storage. If the field
     *         with name _name does not exist, it is created with value equal to _val
     *  
     *  \param _name name of the data field
     *  \param _val value of the data field to be written into the storage
     */ 
    template<typename type_t>
    void put(std::string _name, const type_t &_val)
    {
        auto it = storage.find(_name);
        if (it == storage.end()) { // the field _name does not exist
            // create the field with key _name
            type_t *field = new type_t(_val);
            storage.insert(std::make_pair(_name, field));
            n_fields++;
            return;
        }
        else { // the field already exists
            type_t *field = reinterpret_cast<type_t *>((*it).second);
            *field = _val;
            return;
        }
    }

    /** 
     *  \brief Delete a data field with type type_t and name _name from the storage.
     *         The method does not have effect if the data field does not exist
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
        n_fields--;
    }

    /** 
     *  \brief Return the number of data fields present in the storage
     *  
     *  \return number of data fields
     */ 
    size_t getSize() const
    {
        return n_fields;
    }
};

} // namespace wf

#endif
