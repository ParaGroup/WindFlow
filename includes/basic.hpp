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
 *  @file    basic.hpp
 *  @author  Gabriele Mencagli
 *  @date    28/06/2017
 *  
 *  @brief Basic definitions and macros used by the WindFlow library
 *  
 *  @section Basic Definitions and Macros (Description)
 *  
 *  Set of definitions and macros used by the WindFlow library and by its
 *  operators.
 */ 

#ifndef BASIC_H
#define BASIC_H

/// includes
#include <deque>
#include <mutex>
#include <sstream>
#include <iostream>
#include <sys/time.h>

namespace wf {

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

/// utility macros
#define DEFAULT_VECTOR_CAPACITY 500 //< default capacity of vectors used internally by the library
#define DEFAULT_BATCH_SIZE_TB 1000 //< inital batch size (in no. of tuples) used by GPU operators with time-based windows
#define DEFAULT_CUDA_NUM_THREAD_BLOCK 256 //< default number of threads per block used by GPU operators
#define gpuErrChk(ans) { gpuAssert((ans), __FILE__, __LINE__); }

//@cond DOXY_IGNORE

// defines
#define STRINGIFY(x) XSTRINGIFY(x)
#define XSTRINGIFY(x) #x

// window types
enum win_type_t { CB, TB };

// supported roles of the Win_Seq operator
enum role_t { SEQ, PLQ, WLQ, MAP, REDUCE };

// window-based operators of the library
enum pattern_t { SEQ_CPU, SEQ_GPU, KF_CPU, KF_GPU, WF_CPU, WF_GPU, PF_CPU, PF_GPU, WMR_CPU, WMR_GPU };

// optimization levels
enum opt_level_t { LEVEL0, LEVEL1, LEVEL2 };

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
std::mutex mutex_screen;

// macros to avoid mixing of concurrent couts/prints
#define LOCKED_PRINT(...) { \
 	std::ostringstream stream; \
 	stream << __VA_ARGS__; \
 	mutex_screen.lock(); \
	std::cout << stream.str(); \
	mutex_screen.unlock(); \
}

// struct of the window-based operator's configuration parameters
struct PatternConfig {
    size_t id_outer; // identifier in the outermost operator
    size_t n_outer; // parallelism degree in the outermost operator
    uint64_t slide_outer; // sliding factor of the outermost operator
    size_t id_inner; // identifier in the innermost operator
    size_t n_inner; // parallelism degree in the innermost operator
    uint64_t slide_inner; // sliding factor of the innermost operator

    // Constructor I
    PatternConfig(): id_outer(0),
                     n_outer(0),
                     slide_outer(0),
                     id_inner(0),
                     n_inner(0),
                     slide_inner(0)
    {}

    // Constructor II
    PatternConfig(size_t _id_outer,
                  size_t _n_outer,
                  uint64_t _slide_outer,
                  size_t _id_inner,
                  size_t _n_inner,
                  uint64_t _slide_inner):
                  id_outer(_id_outer),
                  n_outer(_n_outer),
                  slide_outer(_slide_outer),
                  id_inner(_id_inner),
                  n_inner(_n_inner),
                  slide_inner(_slide_inner)
    {}
};

//@endcond

/// forward declaration of the Source operator
template<typename tuple_t>
class Source;

/// forward declaration of the Map operator
template<typename tuple_t, typename result_t>
class Map;

/// forward declaration of the Filter operator
template<typename tuple_t>
class Filter;

/// forward declaration of the FlatMap operator
template<typename tuple_t, typename result_t>
class FlatMap;

/// forward declaration of the Accumulator operator
template<typename tuple_t, typename result_t>
class Accumulator;

/// forward declaration of the Win_Seq operator
template<typename tuple_t, typename result_t, typename input_t=tuple_t>
class Win_Seq;

/// forward declaration of the Win_Farm operator
template<typename tuple_t, typename result_t, typename input_t=tuple_t>
class Win_Farm;

/// forward declaration of the Key_Farm operator
template<typename tuple_t, typename result_t>
class Key_Farm;

/// forward declaration of the Pane_Farm operator
template<typename tuple_t, typename result_t, typename input_t=tuple_t>
class Pane_Farm;

/// forward declaration of the Win_MapReduce operator
template<typename tuple_t, typename result_t, typename input_t=tuple_t>
class Win_MapReduce;

/// forward declaration of the Win_Seq_GPU operator
template<typename tuple_t, typename result_t, typename fun_t, typename input_t=tuple_t>
class Win_Seq_GPU;

/// forward declaration of the Win_Farm_GPU operator
template<typename tuple_t, typename result_t, typename fun_t, typename input_t=tuple_t>
class Win_Farm_GPU;

/// forward declaration of the Key_Farm_GPU operator
template<typename tuple_t, typename result_t, typename fun_t>
class Key_Farm_GPU;

/// forward declaration of the Pane_Farm_GPU operator
template<typename tuple_t, typename result_t, typename fun_t, typename input_t=tuple_t>
class Pane_Farm_GPU;

/// forward declaration of the Win_MapReduce_GPU operator
template<typename tuple_t, typename result_t, typename fun_t, typename input_t=tuple_t>
class Win_MapReduce_GPU;

/// forward declaration of the Sink operator
template<typename tuple_t>
class Sink;

/// forward declaration of the MultiPipe construct
class MultiPipe;

/// forward declaration of the PipeGraph construct
class PipeGraph;

} // namespace wf

#endif
