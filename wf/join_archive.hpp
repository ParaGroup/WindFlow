/**************************************************************************************
 *  Copyright (c) 2023- Gabriele Mencagli and Yuriy Rymarchuk
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
 *  @file    join_archive.hpp
 *  @author  Gabriele Mencagli and Yuriy Rymarchuk
 *  
 *  @brief Join archive
 *  
 *  @section JoinArchive (Description)
 *  
 *  Join archive of tuples received from the input stream ( A or B ) and still useful for the
 *  processing (used by join-based operators).
 */ 

#ifndef JOIN_ARCHIVE_H
#define JOIN_ARCHIVE_H

// includes
#include<deque>
#include<functional>
#include<basic.hpp>

namespace wf {

// class StreamArchive
template<typename tuple_t, typename Container = std::deque<join_tuple_t<tuple_t>>>
class JoinArchive
{
private:
    using wrapper_t = join_tuple_t<tuple_t>; // alias for the wrapped tuple type
    using compare_func_t = std::function<bool(const wrapper_t &, const uint64_t &)>; // function type to compare wrapped tuple to an uint64
    using iterator_t = typename Container::iterator; // iterator type
    compare_func_t lessThan; // function to compare wrapped to an uint64 that rapresent an timestamp (index) or watermark
    Container archive; // container implementing the ordered archive of wrapped tuples

public:
    // Constructor
    JoinArchive(compare_func_t _lessThan):
                  lessThan(_lessThan) {}

    // Add a wrapped tuple to the archive (copy semantics)
    void insert(const wrapper_t &_wt)
    {
        auto it = std::lower_bound(archive.begin(), archive.end(), _wt.index, lessThan);
        if (it == archive.end()) { // add at the end of the archive
            archive.push_back(_wt);
        }
        else { // add the in the right position of the archive
            archive.insert(it, _wt);
        }
    }

    // Add a wrapped tuple to the archive (move semantics)
    void insert(wrapper_t &&_wt)
    {
        auto it = std::lower_bound(archive.begin(), archive.end(), _wt.index, lessThan);
        if (it == archive.end()) { // add at the end of the archive
            archive.push_back(std::move(_wt));
        }
        else { // add the in the right position of the archive
            archive.insert(it, std::move(_wt));
        }
    }

    // Remove all the tuples with timestamp prior to watermark _wm in the ordering
    size_t purge(const uint64_t &_wm)
    {
        auto it = std::lower_bound(archive.begin(), archive.end(), _wm, lessThan);
        size_t n = std::distance(archive.begin(), it);
        archive.erase(archive.begin(), it);
        return n;
    }

    // Get the size of the archive
    size_t size() const
    {
        return archive.size();
    }

    // Get the iterator to the first wrapped tuple in the archive
    iterator_t begin()
    {
        return archive.begin();
    }

    // Get the iterator to the end of the archive
    iterator_t end()
    {
        return archive.end();
    }

    /*  
     *  Method to get a pair of iterators that represent the join range [first, last) given
     *  an input lower bound and upper bound for timestamps as unsigned integers. The method returns the iterator (first) to the
     *  wrapped tuple in the archive that has index (ts) >= lower bound, and the iterator
     *  (end) to the wrapped tuple in the archive that has index (ts) < upper bound.
     */ 
    std::pair<iterator_t, iterator_t> getJoinRange(const uint64_t &_l_b, const uint64_t &_u_b)
    {
        assert(_l_b <= _u_b);
        std::pair<iterator_t, iterator_t> its;
        its.first = std::lower_bound(archive.begin(), archive.end(), _l_b, lessThan);
        its.second = std::lower_bound(archive.begin(), archive.end(), _u_b, lessThan);
        return its;
    }
};

template<typename wrapper_t, typename Container = std::deque<wrapper_t>>
class Iterable_Interval
{
private:
    using iterator_t = typename Container::iterator; // non-const iterator type
    iterator_t first; // iterator to the first wrapped tuple
    iterator_t last; // iterator to the last wrapped tuple (excluded)
    size_t num_tuples; // number of tuples that can be accessed through the iterable

public:
    /// class Iterator
    template<typename T>
    class Iterator
    {
    public:
//@cond DOXY_IGNORE
        typedef Iterator self_type;
        typedef T value_type;
        typedef T &reference;
        typedef T *pointer;
        typedef std::forward_iterator_tag iterator_category;
        typedef int difference_type;
        using iterator_t = typename Container::iterator;
        iterator_t it;

        /// Constructor
        Iterator(iterator_t _it): it(_it) {}

        // ++ operator (postfix)
        self_type operator++() { self_type i = *this; it++; return i; }

        // ++ operator (prefix)
        self_type operator++(int junk) { it++; return *this; }

        // * operator
        reference operator*() { return (*it).tuple; }

        // -> operator
        pointer operator->() { return &((*it).tuple); }

        // == operator
        bool operator==(const self_type &rhs) const { return it == rhs.it; }

        // != operator
        bool operator!=(const self_type &rhs) const { return it != rhs.it; }

        // <= operator
        bool operator<=(const self_type &rhs) const { return it <= rhs.it; }

        // < operator
        bool operator<(const self_type &rhs) const { return it < rhs.it; }

        // >= operator
        bool operator>=(const self_type &rhs) const { return it >= rhs.it; }

        // > operator
        bool operator>(const self_type &rhs) const { return it > rhs.it; }
//@endcond
    };
    
    /** 
     *  \brief Constructor
     *  
     *  \param _first first iterator
     *  \param _last last iterator
     */ 
    Iterable_Interval(iterator_t _first,
             iterator_t _last):
             first(_first),
             last(_last),
             num_tuples(std::distance(_first, _last)) {}

    /** 
     *  \brief Get an iterator to the begin of the iterable object
     *  
     *  \return iterator to the begin of the iterable object
     */ 
    Iterator<wrapper_t> begin()
    {
        return Iterator<wrapper_t>(first);
    }

    /** 
     *  \brief Get an iterator to the end of the iterable object
     *  
     *  \return iterator to the end of the iterable object
     */ 
    Iterator<wrapper_t> end()
    {
        return Iterator<wrapper_t>(last);
    }

    /** 
     *  \brief Get the size of the iterable object
     *  
     *  \return number of tuples in the iterable object
     */ 
    size_t size() const
    {
        return num_tuples;
    }

    /** 
     *  \brief Get a reference to the tuple at a given position
     *  
     *  \param i index of the tuple to be accessed
     *  \return reference to the tuple at position i
     */ 
    wrapper_t &operator[](size_t i)
    {
        if (i >= num_tuples) {
            std::cerr << RED << "WindFlow Error: index of the Iterable out-of-range" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        return (*(std::next(first, i)));
    }

    /** 
     *  \brief Get a const reference to the tuple at a given position
     *  
     *  \param i index of the tuple to be accessed
     *  \return const reference to the tuple at position i
     */ 
    const wrapper_t &operator[](size_t i) const
    {
        if (i >= num_tuples) {
            std::cerr << RED << "WindFlow Error: index of the Iterable out-of-range" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        return (*(std::next(first, i)));
    }

    /** 
     *  \brief Get a reference to the tuple at a given position
     *  
     *  \param i index of the tuple to be accessed
     *  \return reference to the tuple at position i
     */ 
    wrapper_t &at(size_t i)
    {
        if (i >= num_tuples) {
            std::cerr << RED << "WindFlow Error: index of the Iterable out-of-range" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        return (*(std::next(first, i)));
    }

    /** 
     *  \brief Get a const reference to the tuple at a given position
     *  
     *  \param i index of the tuple to be accessed
     *  \return const reference to the tuple at position i
     */ 
    const wrapper_t &at(size_t i) const
    {
        if (i >= num_tuples) {
            std::cerr << RED << "WindFlow Error: index of the Iterable out-of-range" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        return (*(std::next(first, i)));
    }

    /** 
     *  \brief Get a reference to the first tuple of the iterable object
     *  
     *  \return reference to the first tuple
     */ 
    wrapper_t &front()
    {
        if (num_tuples == 0) {
            std::cerr << RED << "WindFlow Error: index of the Iterable out-of-range" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        return (*(first));
    }

    /** 
     *  \brief Get a const reference to the first tuple of the iterable object
     *  
     *  \return const reference to the first tuple
     */ 
    const wrapper_t &front() const
    {
        if (num_tuples == 0) {
            std::cerr << RED << "WindFlow Error: index of the Iterable out-of-range" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        return (*(first));
    }

    /** 
     *  \brief Get a reference to the last tuple of the iterable object
     *  
     *  \return reference to the last tuple
     */ 
    wrapper_t &back()
    {
        if (num_tuples == 0) {
            std::cerr << RED << "WindFlow Error: index of the Iterable out-of-range" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        return (*(last));
    }

    /** 
     *  \brief Get a const reference to the last tuple of the iterable object
     *  
     *  \return const reference to the last tuple
     */ 
    const wrapper_t &back() const
    {
        if (num_tuples == 0) {
            std::cerr << RED << "WindFlow Error: index of the Iterable out-of-range" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        return (*(last));
    }
};

} // namespace wf

#endif
