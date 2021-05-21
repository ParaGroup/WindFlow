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
#include<deque>
#include<mutex>
#include<numeric>
#include<sstream>
#include<iostream>
#include<errno.h>
#include<sys/time.h>
#include<sys/stat.h>

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

/// default capacity of vectors used internally by the library
#define DEFAULT_VECTOR_CAPACITY 500

/// inital batch size (in no. of tuples) used by GPU window-based operators with time-based windows
#define DEFAULT_BATCH_SIZE_TB 1000

/// default number of threads per block used by GPU window-based operators
#define DEFAULT_CUDA_NUM_THREAD_BLOCK 256

/// default interval time to update the atomic counter of dropped tuples
#define DEFAULT_UPDATE_INTERVAL_USEC 100000

/// supported processing modes of the PipeGraph
enum class Mode { DEFAULT, DETERMINISTIC, PROBABILISTIC };

/// supported window types of window-based operators
enum class win_type_t { CB, TB };

/// supported optimization levels of window-based operators
enum class opt_level_t { LEVEL0, LEVEL1, LEVEL2 };

/// enumeration of the routing modes of inputs to operator replicas
enum class routing_modes_t { NONE, FORWARD, KEYBY, COMPLEX };

/// existing types of window-based operators in the library
enum class pattern_t { SEQ_CPU, SEQ_GPU, KF_CPU, KFF_CPU, KF_GPU, KFF_GPU, WF_CPU, WF_GPU, PF_CPU, PF_GPU, WMR_CPU, WMR_GPU };

//@cond DOXY_IGNORE

#if __CUDACC__
// assert function on GPU
inline void gpuAssert(cudaError_t code,
                      const char *file,
                      int line,
                      bool abort=false)
{
    if (code != cudaSuccess) {
        fprintf(stderr, "GPUassert: %s %s %d\n", cudaGetErrorString(code), file, line);
        if (abort) {
            exit(code);
        }
    }
}

// gpuErrChk macro
#define gpuErrChk(ans) { gpuAssert((ans), __FILE__, __LINE__); }
#endif

// defines useful for strings
#define STRINGIFY(x) XSTRINGIFY(x)
#define XSTRINGIFY(x) #x

// supported window events
enum class win_event_t { OLD, IN, DELAYED, FIRED, BATCHED };

// supported ordering modes
enum class ordering_mode_t { ID, TS, TS_RENUMBERING };

// supported roles of the Win_Seq/Win_Seq_GPU operators
enum class role_t { SEQ, PLQ, WLQ, MAP, REDUCE };

// macros for the linux terminal colors
#define DEFAULT_COLOR   "\033[0m"
#define BLACK             "\033[30m"
#define RED               "\033[31m"
#define GREEN             "\033[32m"
#define YELLOW            "\033[33m"
#define BLUE              "\033[34m"
#define MAGENTA           "\033[35m"
#define CYAN              "\033[36m"
#define WHITE             "\033[37m"
#define BOLDBLACK       "\033[1m\033[30m"
#define BOLDRED         "\033[1m\033[31m"
#define BOLDGREEN       "\033[1m\033[32m"
#define BOLDYELLOW      "\033[1m\033[33m"
#define BOLDBLUE        "\033[1m\033[34m"
#define BOLDMAGENTA     "\033[1m\033[35m"
#define BOLDCYAN        "\033[1m\033[36m"
#define BOLDWHITE       "\033[1m\033[37m"

// struct of the window-based operator's configuration parameters
struct WinOperatorConfig {
    size_t id_outer; // identifier in the outermost operator
    size_t n_outer; // parallelism degree in the outermost operator
    uint64_t slide_outer; // sliding factor of the outermost operator
    size_t id_inner; // identifier in the innermost operator
    size_t n_inner; // parallelism degree in the innermost operator
    uint64_t slide_inner; // sliding factor of the innermost operator

    // Constructor I
    WinOperatorConfig():
                     id_outer(0),
                     n_outer(0),
                     slide_outer(0),
                     id_inner(0),
                     n_inner(0),
                     slide_inner(0) {}

    // Constructor II
    WinOperatorConfig(size_t _id_outer,
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
                      slide_inner(_slide_inner) {}
};

//@endcond

/// forward declaration of the Source operator
template<typename tuple_t>
class Source;

/// forward declaration of the Filter operator
template<typename tuple_t, typename result_t>
class Filter;

/// forward declaration of the Map operator
template<typename tuple_t, typename result_t>
class Map;

/// forward declaration of the FlatMap operator
template<typename tuple_t, typename result_t>
class FlatMap;

/// forward declaration of the Accumulator operator
template<typename tuple_t, typename result_t>
class Accumulator;

/// forward declaration of the Win_Seq operator
template<typename tuple_t, typename result_t, typename input_t=tuple_t>
class Win_Seq;

/// forward declaration of the Win_SeqFFAT operator
template<typename tuple_t, typename result_t>
class Win_SeqFFAT;

/// forward declaration of the Win_Farm operator
template<typename tuple_t, typename result_t, typename input_t=tuple_t>
class Win_Farm;

/// forward declaration of the Key_Farm operator
template<typename tuple_t, typename result_t, typename input_t=tuple_t>
class Key_Farm;

/// forward declaration of the Key_FFAT operator
template<typename tuple_t, typename result_t>
class Key_FFAT;

/// forward declaration of the Pane_Farm operator
template<typename tuple_t, typename result_t, typename input_t=tuple_t>
class Pane_Farm;

/// forward declaration of the Win_MapReduce operator
template<typename tuple_t, typename result_t, typename input_t=tuple_t>
class Win_MapReduce;

/// forward declaration of the Win_Seq_GPU operator
template<typename tuple_t, typename result_t, typename win_F_t, typename input_t=tuple_t>
class Win_Seq_GPU;

/// forward declaration of the Win_SeqFFAT_GPU operator
template<typename tuple_t, typename result_t, typename comb_F_t>
class Win_SeqFFAT_GPU;

/// forward declaration of the Win_Farm_GPU operator
template<typename tuple_t, typename result_t, typename win_F_t, typename input_t=tuple_t>
class Win_Farm_GPU;

/// forward declaration of the Key_Farm_GPU operator
template<typename tuple_t, typename result_t, typename win_F_t, typename input_t=tuple_t>
class Key_Farm_GPU;

/// forward declaration of the Key_FFAT_GPU operator
template<typename tuple_t, typename result_t, typename comb_F_t>
class Key_FFAT_GPU;

/// forward declaration of the Pane_Farm_GPU operator
template<typename tuple_t, typename result_t, typename F_t, typename input_t=tuple_t>
class Pane_Farm_GPU;

/// forward declaration of the Win_MapReduce_GPU operator
template<typename tuple_t, typename result_t, typename F_t, typename input_t=tuple_t>
class Win_MapReduce_GPU;

/// forward declaration of the Sink operator
template<typename tuple_t>
class Sink;

/// forward declaration of the MultiPipe construct
class MultiPipe;

/// forward declaration of the PipeGraph construct
class PipeGraph;

//@cond DOXY_IGNORE

// forward declaration of the merge_multipipes_func function
inline MultiPipe *merge_multipipes_func(PipeGraph *, std::vector<MultiPipe *>);

// forward declaration of the split_multipipe_func function
inline std::vector<MultiPipe *> split_multipipe_func(PipeGraph *, MultiPipe *);

#if defined (TRACE_WINDFLOW)
    /// forward declaration of the MonitoringThread class
    class MonitoringThread;

    // forward declaration of the is_ended_func function
    inline bool is_ended_func(PipeGraph *graph);

    // forward declaration of the get_diagram function
    inline std::string get_diagram(PipeGraph *graph);

    // forward declaration of the get_stats_report function
    inline std::string get_stats_report(PipeGraph *graph);
#endif

//@cond DOXY_IGNORE

} // namespace wf

#endif
