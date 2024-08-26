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
 *  @file    iterable.hpp
 *  @author  Gabriele Mencagli
 *  
 *  @brief Iterable class providing access to the tuples in a window
 *  
 *  @section Iterable (Description)
 *  
 *  An Iterable object gives to the user a view of the tuples belonging to a window
 *  to be processed. This is used for some of the window-based operators instantiated
 *  with non-incremental processing logic.
 */ 

#ifndef ITERABLE_H
#define ITERABLE_H

/// includes
#include<deque>
#include<basic.hpp>

namespace wf {

/** 
 *  \class Iterable
 *  
 *  \brief Iterable class providing access to the tuples in a window
 *  
 *  An Iterable object gives to the user a view of the tuples belonging to a given window
 *  to be processed.
 */ 
template<typename tuple_t>
class Iterable
{
private:
    using wrapper_t = wrapper_tuple_t<tuple_t>; // alias for the wrapped tuple type
    using iterator_t = typename std::deque<wrapper_t>::iterator; // non-const iterator type
    using const_iterator_t = typename std::deque<wrapper_t>::const_iterator; // const iterator type
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
        using iterator_t = typename std::deque<wrapper_tuple_t<T>>::iterator;
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

    /// class Const_Iterator
    template<typename T>
    class Const_Iterator
    {
    public:
//@cond DOXY_IGNORE
        typedef Const_Iterator self_type;
        typedef T value_type;
        typedef T &reference;
        typedef const T &const_reference;
        typedef T *pointer;
        typedef const T *const_pointer;
        typedef int difference_type;
        typedef std::forward_iterator_tag iterator_category;
        using const_iterator_t = typename std::deque<wrapper_tuple_t<T>>::const_iterator;
        const_iterator_t it;

        /// Constructor
        Const_Iterator(const_iterator_t _it): it(_it) {}

        // ++ operator (postfix)
        self_type operator++() { self_type i = *this; it++; return i; }

        // ++ operator (prefix)
        self_type operator++(int junk) { it++; return *this; }

        // * operator
        const_reference operator*() { return (*it).tuple; }

        // -> operator
        const_pointer operator->() { return &((*it).tuple); }

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
    Iterable(iterator_t _first,
             iterator_t _last):
             first(_first),
             last(_last),
             num_tuples(std::distance(_first, _last)) {}

    /** 
     *  \brief Get an iterator to the begin of the iterable object
     *  
     *  \return iterator to the begin of the iterable object
     */ 
    Iterator<tuple_t> begin()
    {
        return Iterator<tuple_t>(first);
    }

    /** 
     *  \brief Get a const iterator to the begin of the iterable object
     *  
     *  \return const iterator to the begin of the iterable object
     */ 
    Const_Iterator<tuple_t> begin() const
    {
        return Const_Iterator<tuple_t>(first);
    }

    /** 
     *  \brief Get an iterator to the end of the iterable object
     *  
     *  \return iterator to the end of the iterable object
     */ 
    Iterator<tuple_t> end()
    {
        return Iterator<tuple_t>(last);
    }

    /** 
     *  \brief Get a const iterator to the end of the iterable object
     *  
     *  \return const iterator to the end of the iterable object
     */ 
    Const_Iterator<tuple_t> end() const
    {
        return Const_Iterator<tuple_t>(last);
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
    tuple_t &operator[](size_t i)
    {
        if (i >= num_tuples) {
            std::cerr << RED << "WindFlow Error: index of the Iterable out-of-range" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        return (*(first+i)).tuple;
    }

    /** 
     *  \brief Get a const reference to the tuple at a given position
     *  
     *  \param i index of the tuple to be accessed
     *  \return const reference to the tuple at position i
     */ 
    const tuple_t &operator[](size_t i) const
    {
        if (i >= num_tuples) {
            std::cerr << RED << "WindFlow Error: index of the Iterable out-of-range" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        return (*(first+i)).tuple;
    }

    /**
     * Returns the index value at the specified position in the Iterable.
     * If the position is out of range, an error message is printed and the program exits.
     *
     * @param i The position of the index value to retrieve.
     * @return The index value at the specified position.
     */
    uint64_t index_at(size_t i)
    {
        if (i >= num_tuples) {
            std::cerr << RED << "WindFlow Error: index of the Iterable out-of-range" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        return (*(first+i)).index;
    }

    /** 
     *  \brief Get a reference to the tuple at a given position
     *  
     *  \param i index of the tuple to be accessed
     *  \return reference to the tuple at position i
     */ 
    tuple_t &at(size_t i)
    {
        if (i >= num_tuples) {
            std::cerr << RED << "WindFlow Error: index of the Iterable out-of-range" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        return (*(first+i)).tuple;
    }

    /** 
     *  \brief Get a const reference to the tuple at a given position
     *  
     *  \param i index of the tuple to be accessed
     *  \return const reference to the tuple at position i
     */ 
    const tuple_t &at(size_t i) const
    {
        if (i >= num_tuples) {
            std::cerr << RED << "WindFlow Error: index of the Iterable out-of-range" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        return (*(first+i)).tuple;
    }

    /** 
     *  \brief Get a reference to the first tuple of the iterable object
     *  
     *  \return reference to the first tuple
     */ 
    tuple_t &front()
    {
        if (num_tuples == 0) {
            std::cerr << RED << "WindFlow Error: index of the Iterable out-of-range" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        return (*(first)).tuple;
    }

    /** 
     *  \brief Get a const reference to the first tuple of the iterable object
     *  
     *  \return const reference to the first tuple
     */ 
    const tuple_t &front() const
    {
        if (num_tuples == 0) {
            std::cerr << RED << "WindFlow Error: index of the Iterable out-of-range" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        return (*(first)).tuple;
    }

    /** 
     *  \brief Get a reference to the last tuple of the iterable object
     *  
     *  \return reference to the last tuple
     */ 
    tuple_t &back()
    {
        if (num_tuples == 0) {
            std::cerr << RED << "WindFlow Error: index of the Iterable out-of-range" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        return (*(last-1)).tuple;
    }

    /** 
     *  \brief Get a const reference to the last tuple of the iterable object
     *  
     *  \return const reference to the last tuple
     */ 
    const tuple_t &back() const
    {
        if (num_tuples == 0) {
            std::cerr << RED << "WindFlow Error: index of the Iterable out-of-range" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        return (*(last-1)).tuple;
    }

    /** 
     *  \brief Get the index of the tuple at a given position
     *  
     *  \param i position of the tuple to be accessed
     *  \return index of the selected tuple (the timestamp for TB windows,
     *          otherwise it is a progressive identifier)
     */ 
    uint64_t getTupleIndex(size_t i) const
    {
        if (i >= num_tuples) {
            std::cerr << RED << "WindFlow Error: index of the Iterable out-of-range" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        return (*(first+i)).index;
    }
};

} // namespace wf

#endif
