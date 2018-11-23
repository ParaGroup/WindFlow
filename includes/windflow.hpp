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
 *  @file    windflow.hpp
 *  @author  Gabriele Mencagli
 *  @date    28/06/2017
 *  @version 1.0
 *  
 *  @brief General definitions and macros used by the WindFlow library
 *  
 *  @section DESCRIPTION
 *  
 *  Set of definitions and macros used by the WindFlow library and by its
 *  patterns.
 */ 

#ifndef WINDFLOW_H
#define WINDFLOW_H

// includes
#include <deque>
#include <mutex>
#include <sstream>
#include <iostream>
#include <sys/time.h>

// defines
#define STRINGIFY(x) XSTRINGIFY(x)
#define XSTRINGIFY(x) #x

using namespace std;

/** 
 *  \brief Function to return the number of microseconds from the epoch
 *  
 *  This function returns the number of microseconds from the epoch using
 *  the clock_gettime() call.
 */ 
inline unsigned long current_time_usecs() __attribute__((always_inline));
inline unsigned long current_time_usecs()
{
    struct timespec t;
    clock_gettime(CLOCK_REALTIME, &t);
    return (t.tv_sec)*1000000L + (t.tv_nsec / 1000);
}

/** 
 *  \brief Function to return the number of nanoseconds from the epoch
 *  
 *  This function returns the number of nanoseconds from the epoch using
 *  the clock_gettime() call.
 */ 
inline unsigned long current_time_nsecs() __attribute__((always_inline));
inline unsigned long current_time_nsecs()
{
    struct timespec t;
    clock_gettime(CLOCK_REALTIME, &t);
    return (t.tv_sec)*1000000000L + t.tv_nsec;
}

/** 
 *  \brief Window Type
 *  
 *  Enumeration of supported window types:
 *  -CB for count-based windows;
 *  -TB for time-based windows.
 */ 
enum win_type_t { CB, TB };

// supported roles of the Win_Seq pattern
enum role_t { SEQ, PLQ, WLQ, MAP, REDUCE };

/** 
 *  \brief Supported optimization levels
 *  
 *  Enumeration of supported optimization levels:
 *  -LEVEL0: no optimization used;
 *  -LEVEL1: first optimization level;
 *  -LEVEL2: second optimization level.
 */ 
enum opt_level_t { LEVEL0, LEVEL1, LEVEL2 };

/// utility macros
#define DEFAULT_VECTOR_CAPACITY 500 ///< default capacity of vectors used internally by the library

/// macros used by the GPU patterns
#define DEFAULT_BATCH_SIZE_TB 100 ///< inital batch size (in no. of tuples) used by GPU patterns with time-based windows
#define DEFAULT_CUDA_NUM_THREAD_BLOCK 384 ///< default number of threads per block
#define gpuErrChk(ans) { gpuAssert((ans), __FILE__, __LINE__); }

// macros for the linux terminal colors
#define DEFAULT 	"\033[0m"
#define BLACK 		"\033[30m"
#define RED   		"\033[31m"
#define GREEN   	"\033[32m"
#define YELLOW  	"\033[33m"
#define BLUE    	"\033[34m"
#define MAGENTA 	"\033[35m"
#define CYAN    	"\033[36m"
#define WHITE   	"\033[37m"
#define BOLDBLACK   "\033[1m\033[30m"
#define BOLDRED     "\033[1m\033[31m"
#define BOLDGREEN   "\033[1m\033[32m"
#define BOLDYELLOW  "\033[1m\033[33m"
#define BOLDBLUE    "\033[1m\033[34m"
#define BOLDMAGENTA "\033[1m\033[35m"
#define BOLDCYAN    "\033[1m\033[36m"
#define BOLDWHITE   "\033[1m\033[37m"

// global variable used by LOCKED_PRINT
mutex mutex_screen;

/// macro to avoid mixing of concurrent couts/prints
#define LOCKED_PRINT(...) { \
 	ostringstream stream; \
 	stream << __VA_ARGS__; \
 	mutex_screen.lock(); \
	cout << stream.str(); \
	mutex_screen.unlock(); \
}

// struct of the pattern's configuration parameters
struct PatternConfig {
    size_t id_outer; // identifier in the outermost pattern
    size_t n_outer; // parallelism degree in the outermost pattern
    uint64_t slide_outer; // sliding factor of the outermost pattern
    size_t id_inner; // identifier in the innermost pattern
    size_t n_inner; // parallelism degree in the innermost pattern
    uint64_t slide_inner; // sliding factor of the innermost pattern

    // constructor
    PatternConfig(size_t _id_outer, size_t _n_outer, uint64_t _slide_outer, size_t _id_inner, size_t _n_inner, uint64_t _slide_inner):
                  id_outer(_id_outer),
                  n_outer(_n_outer),
                  slide_outer(_slide_outer),
                  id_inner(_id_inner),
                  n_inner(_n_inner),
                  slide_inner(_slide_inner) {}

    // destructor
    ~PatternConfig() {}
};

/// forward declaration of the Win_Seq pattern
template<typename tuple_t, typename result_t, typename input_t=tuple_t>
class Win_Seq;

/// forward declaration of the Win_Farm pattern
template<typename tuple_t, typename result_t, typename input_t=tuple_t>
class Win_Farm;

/// forward declaration of the Key_Farm pattern
template<typename tuple_t, typename result_t, typename input_t=tuple_t>
class Key_Farm;

/// forward declaration of the Pane_Farm pattern
template<typename tuple_t, typename result_t, typename input_t=tuple_t>
class Pane_Farm;

/// forward declaration of the Win_MapReduce pattern
template<typename tuple_t, typename result_t, typename input_t=tuple_t>
class Win_MapReduce;

/// forward declaration of the Win_Seq_GPU pattern
template<typename tuple_t, typename result_t, typename fun_t, typename input_t=tuple_t>
class Win_Seq_GPU;

/// forward declaration of the Win_Farm_GPU pattern
template<typename tuple_t, typename result_t, typename fun_, typename input_t=tuple_t>
class Win_Farm_GPU;

/// forward declaration of the Key_Farm_GPU pattern
template<typename tuple_t, typename result_t, typename fun_t, typename input_t=tuple_t>
class Key_Farm_GPU;

/// forward declaration of the Pane_Farm_GPU pattern
template<typename tuple_t, typename result_t, typename fun_t, typename input_t=tuple_t>
class Pane_Farm_GPU;

/// forward declaration of the Win_MapReduce_GPU pattern
template<typename tuple_t, typename result_t, typename fun_t, typename input_t=tuple_t>
class Win_MapReduce_GPU;

#endif
