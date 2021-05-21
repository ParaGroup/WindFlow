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
#include<vector>
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
inline uint64_t current_time_usecs() __attribute__((always_inline));
inline uint64_t current_time_usecs()
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
inline uint64_t current_time_nsecs() __attribute__((always_inline));
inline uint64_t current_time_nsecs()
{
    struct timespec t;
    clock_gettime(CLOCK_REALTIME, &t);
    return (t.tv_sec)*1000000000L + t.tv_nsec;
}

/// Default capacity of vectors used internally by the library
#define DEFAULT_VECTOR_CAPACITY 500

/// Default interval time to update the atomic counter of dropped tuples
#define DEFAULT_DROP_INTERVAL_USEC 100000

/// Default interval time to generate punctuations conveying watermarks
#define DEFAULT_WM_INTERVAL_USEC 1000

/// Default number of inputs to generate punctuations coveying watermarks
#define DEFAULT_WM_AMOUNT 100

/// Supported execution modes of the PipeGraph
enum class Execution_Mode_t { DEFAULT, DETERMINISTIC, PROBABILISTIC };

/// Supported time policies of the PipeGraph
enum class Time_Policy_t { INGRESS_TIME, EVENT_TIME };

/// Supported window types of window-based operators
enum class Win_Type_t { CB, TB };

/// Enumeration of the routing modes of inputs to the operator replicas
enum class Routing_Mode_t { NONE, FORWARD, KEYBY, BROADCAST };

/// Forward declaration of the Source operator
template<typename source_func_t>
class Source;

/// Forward declaration of the Filter operator
template<typename filter_func_t, typename key_extractor_func_t>
class Filter;

/// Forward declaration of the Map operator
template<typename map_func_t, typename key_extractor_func_t>
class Map;

/// Forward declaration of the FlatMap operator
template<typename flatmap_func_t, typename key_extractor_func_t>
class FlatMap;

/// Forward declaration of the Reduce operator
template<typename reduce_func_t, typename key_extractor_func_t>
class Reduce;

/// Forward declaration of the Sink operator
template<typename sink_func_t, typename key_extractor_func_t>
class Sink;

/// Forward declaration of the Keyed_Windows operator
template<typename win_func_t, typename key_extractor_func_t>
class Keyed_Windows;

/// Forward declaration of the Parallel_Windows operator
template<typename win_func_t, typename key_extractor_func_t>
class Parallel_Windows;

/// Forward declaration of the Paned_Windows operator
template<typename plq_func_t, typename wlq_func_t, typename key_extractor_func_t>
class Paned_Windows;

/// Forward declaration of the MapReduce_Windows operator
template<typename map_func_t, typename reduce_func_t, typename key_extractor_func_t>
class MapReduce_Windows;

/// Forward declaration of the FFAT_Aggregator operator
template<typename lift_func_t, typename comb_func_t, typename key_extractor_func_t>
class FFAT_Aggregator;

/// Forward declaration of the MultiPipe construct
class MultiPipe;

/// Forward declaration of the PipeGraph construct
class PipeGraph;

/// Forward declaration of the RuntimeContext class
class RuntimeContext;

//@cond DOXY_IGNORE

// Defines useful for strings
#define STRINGIFY(x) XSTRINGIFY(x)
#define XSTRINGIFY(x) #x

// Supported window events
enum class win_event_t { OLD, IN, FIRED };

// Supported ordering modes
enum class ordering_mode_t { ID, TS };

// Supported roles of the Win_Seq/Win_Seq_GPU replicas
enum class role_t { SEQ, PLQ, WLQ, MAP, REDUCE };

// Macros for the linux terminal colors
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

// Forward declaration of the merge_multipipes_func function
inline MultiPipe *merge_multipipes_func(PipeGraph *, std::vector<MultiPipe *>);

// Forward declaration of the split_multipipe_func function
inline std::vector<MultiPipe *> split_multipipe_func(PipeGraph *, MultiPipe *);

#if defined (TRACE_WINDFLOW)
    // Forward declaration of the MonitoringThread class
    class MonitoringThread;

    // Forward declaration of the is_ended_func function
    inline bool is_ended_func(PipeGraph *graph);

    // Forward declaration of the get_diagram function
    inline std::string get_diagram(PipeGraph *graph);

    // Forward declaration of the get_stats_report function
    inline std::string get_stats_report(PipeGraph *graph);
#endif

// Struct the get the type name as a string
template<typename T>
struct TypeName
{
    static const std::string getName()
    {
        return std::string(typeid(T).name());
    }
};

// Struct implementing the empty_key_t
struct empty_key_t
{
    int key_value;

    // Constructor
    empty_key_t(): key_value(0) {}
};

// Operator< for the empty_key_t type
inline bool operator<(const empty_key_t &l,
                      const empty_key_t &r)
{
    return (l.key_value < r.key_value);
}

// Equality operator of empty_key_t
inline bool operator==(const empty_key_t &k1,
                       const empty_key_t &k2)
{
    return k1.key_value == k2.key_value;
}

// Function to compute the compute_gcd
inline uint64_t compute_gcd(uint64_t u,
                            uint64_t v)
{
    while (v != 0) {
        unsigned long r = u % v;
        u = v;
        v = r;
    }
    return u;
};

// Struct wrapping a tuple for window-based operators
template<typename tuple_t>
struct wrapper_tuple_t
{
    tuple_t tuple; // tuple
    uint64_t index; // identifier or timestamp depending on the window semantics

    // Constructor
    wrapper_tuple_t(const tuple_t &_tuple,
                    uint64_t _index):
                    tuple(_tuple),
                    index(_index) {}
};

// Function to create a window result
template<typename result_t, typename key_t>
inline result_t create_win_result_t(key_t _key,
                                    uint64_t _id=0)
{
    if constexpr (std::is_same<key_t, empty_key_t>::value) { // case without key
        result_t res(_id); // constructor with id parameter
        return res;
    }
    else { // case with key
        result_t res(_key, _id); // constructor with key and id parameters
        return res;
    }
}

} // namespace wf

// Hash function of the empty_key_t
namespace std
{
    template<>
    struct hash<wf::empty_key_t>
    {
        size_t operator()(const wf::empty_key_t &_key) const
        {
            return std::hash<int>()(_key.key_value);
        }
    };
}

//@endcond

#endif
