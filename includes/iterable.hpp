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
 *  @version 1.0
 *  
 *  @brief Iterable
 *  
 *  @section DESCRIPTION
 *  
 *  An iterable object gives to the user a read-only view of the tuples belonging
 *  to a window to be processed. This is used by queries instantiated with the
 *  non-incremental interface for patterns implemented on the CPU.
 */ 

#ifndef ITERABLE_H
#define ITERABLE_H

// includes
#include <deque>
#include <stdexcept>

using namespace std;

/** 
 *  \class Iterable
 *  
 *  \brief Iterable
 *  
 *  An iterable object gives to the user a read-only view of the tuples belonging to a
 *  given window to be processed. The template argument is the type of the tuples. This
 *  must be copyable and providing the getInfo() and setInfo() methods.
 */ 
template<typename tuple_t>
class Iterable
{
private:
	// const iterator type
    using const_iterator_t = typename deque<tuple_t>::const_iterator;
    const_iterator_t first; // const iterator to the first tuple
    const_iterator_t last; // const iterator to the last tuple (excluded)
    size_t n_size; // number of tuples that can be accessed through the iterable object

public:
    /** 
     *  \class Iterator
     *  
     *  \brief Iterator object to access the tuples in an Iterable
     *  
     *  An Iterator object to access (read-only) the tuples in an Iterable.
     */ 
	class Iterator {
	private:
		const_iterator_t pos; // internal iterator

	public:
    	/** 
     	 *  \brief Constructor
     	 *  
     	 *  \param _pos initial const iterator
     	 */ 
		Iterator(const_iterator_t _pos): pos(_pos) {}

		/// Destructor
		~Iterator() {}

    	/** 
     	 *  \brief Comparison operator
     	 *  
     	 *  \param _it Iterator to be compared with this
     	 *  \return true if the comparison returns true, false otherwise
     	 */ 
    	bool operator!= (const Iterator &_it) const
    	{
        	return pos != _it.pos;
    	}

    	/** 
     	 *  \brief Dereference operator
     	 *  
     	 *  \return a const reference to the tuple referred to this
     	 */ 
    	const tuple_t &operator* () const
    	{
        	return *pos;
    	}

     	/** 
     	 *  \brief Increment operator
     	 *  
     	 *  \return move this to the next tuple in the Iterable
     	 */ 
    	const Iterator &operator++ ()
    	{
        	pos++;
        	return *this;
    	}

        /** 
         *  \brief Decrement operator
         *  
         *  \return move this to the previous tuple in the Iterable
         */ 
        const Iterator &operator-- ()
        {
            pos--;
            return *this;
        }
	};

    /** 
     *  \brief Constructor
     *  
     *  \param _first first const iterator
     *  \param _last last const iterator
     */ 
    Iterable(const_iterator_t _first, const_iterator_t _last):
             first(_first),
             last(_last),
             n_size(distance(_first, _last)) {}

    /// Destructor
    ~Iterable() {}

    /** 
     *  \brief Return an Iterator to the begin of the iterable object
     *  
     *  \return Iterator to the begin of the iterable object
     */ 
    Iterator begin() const
    {
    	return Iterator(first);
    }

    /** 
     *  \brief Return an Iterator to the end of the iterable object
     *  
     *  \return Iterator to the end of the iterable object
     */ 
    Iterator end() const
    {
    	return Iterator(last);
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
     *  \brief Return a const reference to the tuple at a given position
     *  
     *  \param i index of the tuple to be accessed
     *  \return const reference to the tuple at position i. Calling this method with
     *          an invalid argument i causes an out_of_range exception to be thrown.
     */ 
    const tuple_t &operator[](size_t i) const
    {
    	if (i >= n_size)
    		throw out_of_range ("Invalid index of the Iterable");
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
    		throw out_of_range ("Invalid index of the Iterable");
    	return *(first+i);
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
    		throw out_of_range ("Invalid index of the Iterable");
    	return *(first);
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
    		throw out_of_range ("Invalid index of the Iterable");
    	return *(last-1);
    }
};

#endif
