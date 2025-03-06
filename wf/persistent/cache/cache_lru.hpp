/**************************************************************************************
 *  Copyright (c) 2019- Gabriele Mencagli and Andrea Filippi
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
 *  @file    cache_lru.hpp
 *  @author  Gabriele Mencagli and Andrea Filippi
 *  
 *  @brief class implementing a cache with LRU (Least Recently Used) replacement policy
 *  
 *  @section Cache_LRU (Description)
 *  
 *  Concrete class of a cache implementing the LRU replacement policy.
 */ 

#ifndef LRU_CACHE_H
#define LRU_CACHE_H

#include<list>
#include<unordered_map>
#include<persistent/cache/cache.hpp>

namespace wf {

// class LRUCache
template<typename key_t, typename value_t>
class LRUCache: public Cache<key_t, value_t>
{
private:
    using EntryPair = std::pair<key_t, value_t>;
    using CacheList = std::list<EntryPair>;
    using CacheListIt = typename CacheList::iterator;
    using CacheMap = std::unordered_map<key_t, CacheListIt>;
    size_t maxCapacity; // max cache capacity
    CacheList cacheList; // cache list
    CacheMap cacheMap; // cache map

public:
    // Constructor
    explicit LRUCache(size_t _capacity):
                      maxCapacity(_capacity) {}

    // Put method to add an entry in cache
    void put(const key_t &_key,
             const value_t &_value) override
    {
        auto it = cacheMap.find(_key);
        cacheList.push_front(EntryPair(_key, _value));
        if (it != cacheMap.end()) {
            cacheList.erase(it->second);
            cacheMap.erase(it);
        } 
        cacheMap[_key] = cacheList.begin();
        if (cacheMap.size() > maxCapacity) {
           auto last = cacheList.end();
           last--;
           cacheMap.erase(last->first);
           cacheList.pop_back();
        }
    }

    // Get method to read an entry from the cache
    std::optional<value_t> get(const key_t &_key) override
    {
        auto it = cacheMap.find(_key);
        if (it == cacheMap.end()) {
            return std::nullopt;
        }
        cacheList.splice(cacheList.begin(), cacheList, it->second); // move the key at the beginning
        return it->second->second;
    }

    // Method to check if a key exists in the cache
    bool exists(const key_t& _key) const override
    {
        return cacheMap.find(_key) != cacheMap.end();
    }

    // Method to remove a key entry in the cache
    void remove(const key_t &_key) override
    {
        auto it = cacheMap.find(_key);
        if (it != cacheMap.end()) {
            cacheList.erase(it->second);
            cacheMap.erase(it);
        }
    }

    // Method to empty the cache
    void clear() override
    {
        cacheList.clear();
        cacheMap.clear();
    }

    // Get the current size of the cache
    size_t size() const override
    {
        return cacheMap.size();
    }

    // Get the capacity of the cache
    size_t capacity() const override
    {
        return maxCapacity;
    }
};

} // namespace wf

#endif
