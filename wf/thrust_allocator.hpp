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
 *  @file    thrust_allocator.hpp
 *  @author  Gabriele Mencagli (work of Nathan Bell and Jared Hoberock)
 *  
 *  @brief Custom allocator for recycling internal buffers for Thrust transforms
 *  
 *  @section Thurst_Allocator (Description)
 *  
 *  This allocator intercept calls to get_temporary_buffer and return_temporary_buffer
 *  to control how Thrust allocates temporary storage during its transform functions.
 *  The idea is to create a cache of allocations to search when temporary storage
 *  is requested. If a hit is found in the cache, we quickly return the cached allocation
 *  instead of resorting to the more expensive thrust::cuda::malloc.
 */ 

#ifndef TH_ALLOC_H
#define TH_ALLOC_H

// includes
#include<map>
#include<iostream>
#include<thrust/pair.h>
#include<thrust/generate.h>
#include<thrust/host_vector.h>
#include<thrust/system/cuda/vector.h>
#include<thrust/system/cuda/execution_policy.h>

namespace wf {

// class Thurst_Allocator
class Thurst_Allocator
{
public:
    typedef char value_type; // value_type alias

    // Constructor
    Thurst_Allocator() {}

    // Destructor
    ~Thurst_Allocator()
    {
        free_all(); // free all allocations when Thurst_Allocator goes out of scope
    }

    // Allocate num_bytes in a buffer accessible by the GPU side
    char *allocate(std::ptrdiff_t num_bytes)
    {
        char *result = 0;
        free_blocks_type::iterator free_block = free_blocks.find(num_bytes); // search the cache for a free block
        if (free_block != free_blocks.end()) { // if present, we recycle the memory aray 
            result = free_block->second; // get the pointer
            free_blocks.erase(free_block); // erase from the free_blocks map
        }
        else { // not found, create a new one with cuda::malloc and throw exception if it does not work
            try {
                result = thrust::cuda::malloc<char>(num_bytes).get(); // allocate memory and convert cuda::pointer to raw pointer
            }
            catch(std::runtime_error &e)
            {
                throw;
            }
        }
        allocated_blocks.insert(std::make_pair(result, num_bytes)); // insert the allocated pointer into the allocated_blocks map
        return result;
    }

    // Deallocate a memory buffer of n bytes
    void deallocate(char* ptr, size_t n)
    {
        allocated_blocks_type::iterator iter = allocated_blocks.find(ptr); // erase the allocated block from the allocated blocks map
        std::ptrdiff_t num_bytes = iter->second;
        allocated_blocks.erase(iter);
        free_blocks.insert(std::make_pair(num_bytes, ptr)); // insert the block into the free blocks map
    }

private:
    typedef std::multimap<std::ptrdiff_t, char*> free_blocks_type; // multimap of the free blocks
    typedef std::map<char*, std::ptrdiff_t> allocated_blocks_type; // hashmap of the allocated blocks
    free_blocks_type free_blocks;
    allocated_blocks_type allocated_blocks;

    // Free all the blocks of the allocator (both free and used ones)
    void free_all()
    { 
        for (free_blocks_type::iterator i = free_blocks.begin(); i != free_blocks.end(); i++) {
            thrust::cuda::free(thrust::cuda::pointer<char>(i->second)); // transform the pointer to cuda::pointer before calling cuda::free
        }
        for (allocated_blocks_type::iterator i = allocated_blocks.begin(); i != allocated_blocks.end(); i++) {
            thrust::cuda::free(thrust::cuda::pointer<char>(i->first)); // transform the pointer to cuda::pointer before calling cuda::free
        }
    }
};

} // namespace wf

#endif
