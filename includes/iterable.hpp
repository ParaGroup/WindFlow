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
 *  @file    iterable.hpp
 *  @author  Gabriele Mencagli
 *  @date    14/01/2018
 *  
 *  @brief Iterable class providing access to the tuples within a streaming window
 *  
 *  @section Iterable (Description)
 *  
 *  An Iterable object gives to the user a view of the tuples belonging to a window
 *  to be processed. This is used by queries instantiated with the non-incremental
 *  interface for operators implemented on the CPU.
 *  
 *  The template parameter of the data items that can be used with the Iterable must be default
 *  constructible, with a copy constructor and copy assignment operator, and they
 *  must provide and implement the setControlFields() and getControlFields() methods.
 */ 

#ifndef ITERABLE_H
#define ITERABLE_H

/// includes
#include <deque>
#include <stdexcept>

namespace wf {

/** 
 *  \class Iterable
 *  
 *  \brief Iterable class providing access to the tuples within a streaming window
 *  
 *  An Iterable object gives to the user a view of the tuples belonging to a given window
 *  to be processed. The template parameter is the type of the items used by the Iterable.
 */ 
template<typename tuple_t>
class Iterable
{
private:
	// iterator types
    using iterator_t = typename std::deque<tuple_t>::iterator;
    using const_iterator_t = typename std::deque<tuple_t>::const_iterator;
    iterator_t first; // iterator to the first tuple
    iterator_t last; // iterator to the last tuple (excluded)
    size_t n_size; // number of tuples that can be accessed through the iterable object

public:
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
             n_size(distance(_first, _last))
    {}

    /** 
     *  \brief Return an iterator to the begin of the iterable object
     *  
     *  \return iterator to the begin of the iterable object
     */ 
    iterator_t begin()
    {
    	return first;
    }

    /** 
     *  \brief Return a const iterator to the begin of the iterable object
     *  
     *  \return const iterator to the begin of the iterable object
     */ 
    const_iterator_t begin() const
    {
        return first;
    }

    /** 
     *  \brief Return an iterator to the end of the iterable object
     *  
     *  \return iterator to the end of the iterable object
     */ 
    iterator_t end()
    {
    	return last;
    }

    /** 
     *  \brief Return a const iterator to the end of the iterable object
     *  
     *  \return const iterator to the end of the iterable object
     */ 
    const_iterator_t end() const
    {
        return last;
    }

    /** 
     *  \brief Return the size of the iterable object
     *  
     *  \return number of tuples in the iterable object
     */ 
    size_t size() const
    {
    	return n_size;
    }

    /** 
     *  \brief Return a reference to the tuple at a given position
     *  
     *  \param i index of the tuple to be accessed
     *  \return reference to the tuple at position i. Calling this method with
     *          an invalid argument i causes an out_of_range exception to be thrown.
     */ 
    tuple_t &operator[](size_t i)
    {
    	if (i >= n_size)
    		throw std::out_of_range ("Invalid index of the Iterable");
    	return *(first+i);
    }

    /** 
     *  \brief Return a const reference to the tuple at a given position
     *  
     *  \param i index of the tuple to be accessed
     *  \return const reference to the tuple at position i. Calling this method with
     *          an invalid argument i causes an out_of_range exception to be thrown.
     */ 
    const tuple_t &operator[](size_t i) const
    {
        if (i >= n_size)
            throw std::out_of_range ("Invalid index of the Iterable");
        return *(first+i);
    }

    /** 
     *  \brief Return a reference to the tuple at a given position
     *  
     *  \param i index of the tuple to be accessed
     *  \return reference to the tuple at position i. Calling this method with
     *          an invalid argument i causes an out_of_range exception to be thrown.
     */ 
    tuple_t &at(size_t i)
    {
    	if (i >= n_size)
    		throw std::out_of_range ("Invalid index of the Iterable");
    	return *(first+i);
    }

    /** 
     *  \brief Return a const reference to the tuple at a given position
     *  
     *  \param i index of the tuple to be accessed
     *  \return const reference to the tuple at position i. Calling this method with
     *          an invalid argument i causes an out_of_range exception to be thrown.
     */ 
    const tuple_t &at(size_t i) const
    {
        if (i >= n_size)
            throw std::out_of_range ("Invalid index of the Iterable");
        return *(first+i);
    }

    /** 
     *  \brief Return a reference to the first tuple of the iterable object
     *  
     *  \return reference to the first tuple. Calling this method on an empty iterable
     *          object causes an out_of_range exception to be thrown.
     */ 
    tuple_t &front()
    {
    	if (n_size == 0)
    		throw std::out_of_range ("Invalid index of the Iterable");
    	return *(first);
    }

    /** 
     *  \brief Return a const reference to the first tuple of the iterable object
     *  
     *  \return const reference to the first tuple. Calling this method on an empty iterable
     *          object causes an out_of_range exception to be thrown.
     */ 
    const tuple_t &front() const
    {
        if (n_size == 0)
            throw std::out_of_range ("Invalid index of the Iterable");
        return *(first);
    }

    /** 
     *  \brief Return a reference to the last tuple of the iterable object
     *  
     *  \return reference to the last tuple. Calling this method on an empty iterable
     *          object causes an out_of_range exception to be thrown.
     */ 
    tuple_t &back()
    {
    	if (n_size == 0)
    		throw std::out_of_range ("Invalid index of the Iterable");
    	return *(last-1);
    }

    /** 
     *  \brief Return a const reference to the last tuple of the iterable object
     *  
     *  \return const reference to the last tuple. Calling this method on an empty iterable
     *          object causes an out_of_range exception to be thrown.
     */ 
    const tuple_t &back() const
    {
        if (n_size == 0)
            throw std::out_of_range ("Invalid index of the Iterable");
        return *(last-1);
    }
};

} // namespace wf

#endif
