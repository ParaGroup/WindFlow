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
 *  @file    cache_lfu.hpp
 *  @author  Gabriele Mencagli and Andrea Filippi
 *  
 *  @brief class implementing a cache with LFU (Least Frequently Used) replacement policy
 *  
 *  @section Cache_LFU (Description)
 *  
 *  Concrete class of a cache implementing the LFU replacement policy.
 */ 

#ifndef LFU_CACHE_H
#define LFU_CACHE_H

#include<list>
#include<unordered_map>
#include<persistent/cache/cache.hpp>

namespace wf {

// class LFUCache
template<typename key_t, typename value_t>
class LFUCache: public Cache<key_t, value_t> {
private:
    using Frequency = size_t;
    using FrequencyValuePair = std::pair<Frequency, value_t>;
    using KeyList = std::list<key_t>;
    using KeyListIt = typename KeyList::iterator;
    using FrequencyListMap = std::unordered_map<Frequency, KeyList>; // map frequency -> list of keys
    using KeyNodeMap = std::unordered_map<key_t, KeyListIt>; // map key -> list iterator
    using EntryMap = std::unordered_map<key_t, FrequencyValuePair>; // map key -> (frequency, value)
    size_t maxCapacity; // max cache capacity
    size_t minFrequency; // minimum frequency
    size_t currentSize; // current size
    FrequencyListMap frequencyList; // map frequency -> keys list
    KeyNodeMap keyNode; // map key -> iterator
    EntryMap frequency; // map key -> (frequency, value)

public:
    // Constructor
    explicit LFUCache(size_t _capacity):
                      maxCapacity(_capacity),
                      minFrequency(0),
                      currentSize(0) {}

    // Put method to add an entry in cache
    void put(const key_t &_key,
             const value_t &_value) override
    {
        if (maxCapacity <= 0) {
            return; // capacity is zero or negative, do nothing
        }
        if (keyNode.find(_key) != keyNode.end()) {
            frequency[_key].second = _value; // key already exists, update its value and frequency
            get(_key);
            return;
        }
        if (currentSize == maxCapacity) {
            key_t minFreqBack = frequencyList[minFrequency].back(); // cache is full, evict the least frequently used key
            keyNode.erase(minFreqBack);
            frequency.erase(minFreqBack);
            frequencyList[minFrequency].pop_back();
            currentSize--;
        }
        currentSize++; // add the new key to the cache
        minFrequency = 1;
        frequencyList[minFrequency].push_front(_key);
        keyNode[_key] = frequencyList[minFrequency].begin();
        frequency[_key].first = 1, frequency[_key].second = _value;
    }

    // Get method to read an entry from the cache
    std::optional<value_t> get(const key_t &_key) override
    {
        if (keyNode.find(_key) == keyNode.end()) {
            return std::nullopt; // key not found
        }
        Frequency keyFreq = frequency[_key].first;
        frequencyList[keyFreq].erase(keyNode[_key]);
        frequency[_key].first++;
        frequencyList[frequency[_key].first].push_front(_key);
        keyNode[_key] = frequencyList[frequency[_key].first].begin();
        if (frequencyList[minFrequency].size() == 0) {
            minFrequency++; // update minFrequency if the list is empty
        }
        return frequency[_key].second; // return the value of the key
    }

    // Method to check if a key exists in the cache
    bool exists(const key_t &_key) const override
    {
        return keyNode.find(_key) != keyNode.end();
    }

    // Method to remove a key entry in the cache
    void remove(const key_t &_key) override
    {
        if (keyNode.find(_key) == keyNode.end()) {
            return; // La chiave non esiste
        }
        Frequency keyFreq = frequency[_key].first;
        frequencyList[keyFreq].erase(keyNode[_key]);
        frequency.erase(_key);
        keyNode.erase(_key);
        if (frequencyList[minFrequency].empty()) {
            minFrequency++; // update minFrequency if the list is empty
        }
        currentSize--; // decrease the size
    }

    // Method to empty the cache
    void clear() override
    {
        frequencyList.clear();
        keyNode.clear();
        frequency.clear();
        minFrequency = 0;
        currentSize = 0;
    }

    // Get the current size of the cache
    size_t size() const override
    {
        return currentSize;
    }

    // Get the capacity of the cache
    size_t capacity() const override
    {
        return maxCapacity;
    }
};

} // namespace wf

#endif
