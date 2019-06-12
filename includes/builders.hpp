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
 *  @file    builders.hpp
 *  @author  Gabriele Mencagli
 *  @date    06/09/2018
 *  
 *  @brief Builders used to simplify the creation of the WindFlow patterns
 *  
 *  @section Builders (Description)
 *  
 *  Set of builders to facilitate the creation of the WindFlow patterns.
 */ 

#ifndef BUILDERS_H
#define BUILDERS_H

// includes
#include <chrono>
#include <memory>
#include <functional>
#include <basic.hpp>
#include <meta_utils.hpp>

using namespace chrono;

/** 
 *  \class Source_Builder
 *  
 *  \brief Builder of the Source pattern
 *  
 *  Builder class to ease the creation of the Source pattern.
 */ 
template<typename F_t>
class Source_Builder
{
private:
    F_t func;
    // type of the pattern to be created by this builder
    using source_t = Source<decltype(get_tuple_t(func))>;
    // type of the closing function
    using closing_func_t = function<void(RuntimeContext&)>;
    uint64_t pardegree = 1;
    string name = "anonymous_source";
    closing_func_t closing_func = [](RuntimeContext &r) -> void { return; };

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _func function to generate the stream elements
     */ 
    Source_Builder(F_t _func): func(_func) {}

    /** 
     *  \brief Method to specify the name of the Source pattern
     *  
     *  \param _name string with the name to be given
     *  \return the object itself
     */ 
    Source_Builder<F_t>& withName(string _name)
    {
        name = _name;
        return *this;
    }

    /** 
     *  \brief Method to specify the number of parallel instances within the Source pattern
     *  
     *  \param _pardegree number of parallel instances
     *  \return the object itself
     */ 
    Source_Builder<F_t>& withParallelism(size_t _pardegree)
    {
        pardegree = _pardegree;
        return *this;
    }

    /** 
     *  \brief Method to specify the closing function used by the pattern
     *  
     *  \param _closing_func closing function to be used by the pattern
     *  \return the object itself
     */ 
    Source_Builder<F_t>& withClosingFunction(closing_func_t _closing_func)
    {
        closing_func = _closing_func;
        return *this;
    }

#if __cplusplus >= 201703L
    /** 
     *  \brief Method to create the Source pattern (only C++17)
     *  
     *  \return a copy of the created Source pattern
     */ 
    source_t build()
    {
        return source_t(func, pardegree, name, closing_func); // copy elision in C++17
    }
#endif

    /** 
     *  \brief Method to create the Source pattern
     *  
     *  \return a pointer to the created Source pattern (to be explicitly deallocated/destroyed)
     */ 
    source_t *build_ptr()
    {
        return new source_t(func, pardegree, name, closing_func);
    }

    /** 
     *  \brief Method to create the Source pattern
     *  
     *  \return a unique_ptr to the created Source pattern
     */ 
    unique_ptr<source_t> build_unique()
    {
        return make_unique<source_t>(func, pardegree, name, closing_func);
    }
};

/** 
 *  \class Filter_Builder
 *  
 *  \brief Builder of the Filter pattern
 *  
 *  Builder class to ease the creation of the Filter pattern.
 */ 
template<typename F_t>
class Filter_Builder
{
private:
    F_t func;
    // type of the pattern to be created by this builder
    using filter_t = Filter<decltype(get_tuple_t(func))>;
    // type of the closing function
    using closing_func_t = function<void(RuntimeContext&)>;
    // type of the function to map the key hashcode onto an identifier starting from zero to pardegree-1
    using routing_func_t = function<size_t(size_t, size_t)>;
    uint64_t pardegree = 1;
    string name = "anonymous_filter";
    bool isKeyed = false;
    closing_func_t closing_func = [](RuntimeContext &r) -> void { return; };
    routing_func_t routing_func;

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _func function implementing the boolean predicate
     */ 
    Filter_Builder(F_t _func): func(_func) {}

    /** 
     *  \brief Method to specify the name of the Filter pattern
     *  
     *  \param _name string with the name to be given
     *  \return the object itself
     */ 
    Filter_Builder<F_t>& withName(string _name)
    {
        name = _name;
        return *this;
    }

    /** 
     *  \brief Method to specify the number of parallel instances within the Filter pattern
     *  
     *  \param _pardegree number of parallel instances
     *  \return the object itself
     */ 
    Filter_Builder<F_t>& withParallelism(size_t _pardegree)
    {
        pardegree = _pardegree;
        return *this;
    }

    /** 
     *  \brief Method to enable the key-based routing
     *  
     *  \param _routing_func function to map the key hashcode onto an identifier starting from zero to pardegree-1
     *  \return the object itself
     */ 
    Filter_Builder<F_t>& enable_KeyBy(routing_func_t _routing_func=[](size_t k, size_t n) { return k%n; })
    {
        isKeyed = true;
        routing_func = _routing_func;
        return *this;
    }

    /** 
     *  \brief Method to specify the closing function used by the pattern
     *  
     *  \param _closing_func closing function to be used by the pattern
     *  \return the object itself
     */ 
    Filter_Builder<F_t>& withClosingFunction(closing_func_t _closing_func)
    {
        closing_func = _closing_func;
        return *this;
    }

#if __cplusplus >= 201703L
    /** 
     *  \brief Method to create the Filter pattern (only C++17)
     *  
     *  \return a copy of the created Map pattern
     */ 
    filter_t build()
    {
        if (!isKeyed)
            return filter_t(func, pardegree, name, closing_func); // copy elision in C++17
        else
            return filter_t(func, pardegree, name, closing_func, routing_func); // copy elision in C++17
    }
#endif

    /** 
     *  \brief Method to create the Filter pattern
     *  
     *  \return a pointer to the created Filter pattern (to be explicitly deallocated/destroyed)
     */ 
    filter_t *build_ptr()
    {
        if (!isKeyed)
            return new filter_t(func, pardegree, name, closing_func);
        else
            return new filter_t(func, pardegree, name, closing_func, routing_func);
    }

    /** 
     *  \brief Method to create the Filter pattern
     *  
     *  \return a unique_ptr to the created Filter pattern
     */ 
    unique_ptr<filter_t> build_unique()
    {
        if (!isKeyed)
            return make_unique<filter_t>(func, pardegree, name, closing_func);
        else
            return make_unique<filter_t>(func, pardegree, name, closing_func, routing_func);
    }
};

/** 
 *  \class Map_Builder
 *  
 *  \brief Builder of the Map pattern
 *  
 *  Builder class to ease the creation of the Map pattern.
 */ 
template<typename F_t>
class Map_Builder
{
private:
    F_t func;
    // type of the pattern to be created by this builder
    using map_t = Map<decltype(get_tuple_t(func)),
                             decltype(get_result_t(func))>;
    // type of the closing function
    using closing_func_t = function<void(RuntimeContext&)>;
    // type of the function to map the key hashcode onto an identifier starting from zero to pardegree-1
    using routing_func_t = function<size_t(size_t, size_t)>;
    uint64_t pardegree = 1;
    string name = "anonymous_map";
    bool isKeyed = false;
    closing_func_t closing_func = [](RuntimeContext &r) -> void { return; };
    routing_func_t routing_func;

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _func function of the one-to-one transformation
     */ 
    Map_Builder(F_t _func): func(_func) {}

    /** 
     *  \brief Method to specify the name of the Map pattern
     *  
     *  \param _name string with the name to be given
     *  \return the object itself
     */ 
    Map_Builder<F_t>& withName(string _name)
    {
        name = _name;
        return *this;
    }

    /** 
     *  \brief Method to specify the number of parallel instances within the Map pattern
     *  
     *  \param _pardegree number of parallel instances
     *  \return the object itself
     */ 
    Map_Builder<F_t>& withParallelism(size_t _pardegree)
    {
        pardegree = _pardegree;
        return *this;
    }

    /** 
     *  \brief Method to enable the key-based routing
     *  
     *  \param _routing_func function to map the key hashcode onto an identifier starting from zero to pardegree-1
     *  \return the object itself
     */ 
    Map_Builder<F_t>& enable_KeyBy(routing_func_t _routing_func=[](size_t k, size_t n) { return k%n; })
    {
        isKeyed = true;
        routing_func = _routing_func;
        return *this;
    }

    /** 
     *  \brief Method to specify the closing function used by the pattern
     *  
     *  \param _closing_func closing function to be used by the pattern
     *  \return the object itself
     */ 
    Map_Builder<F_t>& withClosingFunction(closing_func_t _closing_func)
    {
        closing_func = _closing_func;
        return *this;
    }

#if __cplusplus >= 201703L
    /** 
     *  \brief Method to create the Map pattern (only C++17)
     *  
     *  \return a copy of the created Map pattern
     */ 
    map_t build()
    {
        if (!isKeyed)
            return map_t(func, pardegree, name, closing_func); // copy elision in C++17
        else
            return map_t(func, pardegree, name, closing_func, routing_func); // copy elision in C++17
    }
#endif

    /** 
     *  \brief Method to create the Map pattern
     *  
     *  \return a pointer to the created Map pattern (to be explicitly deallocated/destroyed)
     */ 
    map_t *build_ptr()
    {
        if (!isKeyed)
            return new map_t(func, pardegree, name, closing_func);
        else
            return new map_t(func, pardegree, name, closing_func, routing_func);
    }

    /** 
     *  \brief Method to create the Map pattern
     *  
     *  \return a unique_ptr to the created Map pattern
     */ 
    unique_ptr<map_t> build_unique()
    {
        if (!isKeyed)
            return make_unique<map_t>(func, pardegree, name, closing_func);
        else
            return make_unique<map_t>(func, pardegree, name, closing_func, routing_func);
    }
};

/** 
 *  \class FlatMap_Builder
 *  
 *  \brief Builder of the FlatMap pattern
 *  
 *  Builder class to ease the creation of the FlatMap pattern.
 */ 
template<typename F_t>
class FlatMap_Builder
{
private:
    F_t func;
    // type of the pattern to be created by this builder
    using flatmap_t = FlatMap<decltype(get_tuple_t(func)),
                             decltype(get_result_t(func))>;
    // type of the closing function
    using closing_func_t = function<void(RuntimeContext&)>;
    // type of the function to map the key hashcode onto an identifier starting from zero to pardegree-1
    using routing_func_t = function<size_t(size_t, size_t)>;
    uint64_t pardegree = 1;
    string name = "anonymous_flatmap";
    bool isKeyed = false;
    closing_func_t closing_func = [](RuntimeContext &r) -> void { return; };
    routing_func_t routing_func;

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _func function of the one-to-any transformation
     */ 
    FlatMap_Builder(F_t _func): func(_func) {}

    /** 
     *  \brief Method to specify the name of the FlatMap pattern
     *  
     *  \param _name string with the name to be given
     *  \return the object itself
     */ 
    FlatMap_Builder<F_t>& withName(string _name)
    {
        name = _name;
        return *this;
    }

    /** 
     *  \brief Method to specify the number of parallel instances within the FlatMap pattern
     *  
     *  \param _pardegree number of parallel instances
     *  \return the object itself
     */ 
    FlatMap_Builder<F_t>& withParallelism(size_t _pardegree)
    {
        pardegree = _pardegree;
        return *this;
    }

    /** 
     *  \brief Method to enable the key-based routing
     *  
     *  \param _routing_func function to map the key hashcode onto an identifier starting from zero to pardegree-1
     *  \return the object itself
     */ 
    FlatMap_Builder<F_t>& enable_KeyBy(routing_func_t _routing_func=[](size_t k, size_t n) { return k%n; })
    {
        isKeyed = true;
        routing_func = _routing_func;
        return *this;
    }

    /** 
     *  \brief Method to specify the closing function used by the pattern
     *  
     *  \param _closing_func closing function to be used by the pattern
     *  \return the object itself
     */ 
    FlatMap_Builder<F_t>& withClosingFunction(closing_func_t _closing_func)
    {
        closing_func = _closing_func;
        return *this;
    }

#if __cplusplus >= 201703L
    /** 
     *  \brief Method to create the FlatMap pattern (only C++17)
     *  
     *  \return a copy of the created FlatMap pattern
     */ 
    flatmap_t build()
    {
        if (!isKeyed)
            return flatmap_t(func, pardegree, name, closing_func); // copy elision in C++17
        else
            return flatmap_t(func, pardegree, name, closing_func, routing_func); // copy elision in C++17
    }
#endif

    /** 
     *  \brief Method to create the FlatMap pattern
     *  
     *  \return a pointer to the created FlatMap pattern (to be explicitly deallocated/destroyed)
     */ 
    flatmap_t *build_ptr()
    {
        if (!isKeyed)
            return new flatmap_t(func, pardegree, name, closing_func);
        else
            return new flatmap_t(func, pardegree, name, closing_func, routing_func);
    }

    /** 
     *  \brief Method to create the FlatMap pattern
     *  
     *  \return a unique_ptr to the created FlatMap pattern
     */ 
    unique_ptr<flatmap_t> build_unique()
    {
        if (!isKeyed)
            return make_unique<flatmap_t>(func, pardegree, name, closing_func);
        else
            return make_unique<flatmap_t>(func, pardegree, name, closing_func, routing_func);
    }
};

/** 
 *  \class Accumulator_Builder
 *  
 *  \brief Builder of the Accumulator pattern
 *  
 *  Builder class to ease the creation of the Accumulator pattern.
 */ 
template<typename F_t>
class Accumulator_Builder
{
private:
    F_t func;
    // type of the pattern to be created by this builder
    using accumulator_t = Accumulator<decltype(get_tuple_t(func)), decltype(get_result_t(func))>;
    // type of the closing function
    using closing_func_t = function<void(RuntimeContext&)>;
    // type of the function to map the key hashcode onto an identifier starting from zero to pardegree-1
    using routing_func_t = function<size_t(size_t, size_t)>;
    // type of the result produced by the Accumulator instance
    using result_t = decltype(get_result_t(func));
    uint64_t pardegree = 1;
    string name = "anonymous_accumulator";
    result_t init_value;
    closing_func_t closing_func = [](RuntimeContext &r) -> void { return; };
    routing_func_t routing_func = [](size_t k, size_t n) { return k%n; };

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _func function implementing the reduce/fold
     */ 
    Accumulator_Builder(F_t _func): func(_func) {}

    /** 
     *  \brief Method to specify the name of the Accumulator pattern
     *  
     *  \param _name string with the name to be given
     *  \return the object itself
     */ 
    Accumulator_Builder<F_t>& withName(string _name)
    {
        name = _name;
        return *this;
    }

    /** 
     *  \brief Method to specify the initial value for fold functions
     *         (for reduce the initial value is the one obtained by the
     *          default Constructor of result_t)
     *  
     *  \param _init_value initial value
     *  \return the object itself
     */ 
    Accumulator_Builder<F_t>& withInitialValue(result_t _init_value)
    {
        init_value = _init_value;
        return *this;
    }

    /** 
     *  \brief Method to specify the number of parallel instances within the Accumulator pattern
     *  
     *  \param _pardegree number of parallel instances
     *  \return the object itself
     */ 
    Accumulator_Builder<F_t>& withParallelism(size_t _pardegree)
    {
        pardegree = _pardegree;
        return *this;
    }

    /** 
     *  \brief Method to specify the routing function of input tuples to the internal patterns
     *  
     *  \param _routing_func function to map the key hashcode onto an identifier starting from zero to pardegree-1
     *  \return the object itself
     */ 
    Accumulator_Builder<F_t>& set_KeyBy(routing_func_t _routing_func)
    {
        routing_func = _routing_func;
        return *this;
    }

    /** 
     *  \brief Method to specify the closing function used by the pattern
     *  
     *  \param _closing_func closing function to be used by the pattern
     *  \return the object itself
     */ 
    Accumulator_Builder<F_t>& withClosingFunction(closing_func_t _closing_func)
    {
        closing_func = _closing_func;
        return *this;
    }

#if __cplusplus >= 201703L
    /** 
     *  \brief Method to create the Accumulator pattern (only C++17)
     *  
     *  \return a copy of the created Accumulator pattern
     */ 
    accumulator_t build()
    {
        return accumulator_t(func, init_value, pardegree, name, closing_func, routing_func); // copy elision in C++17
    }
#endif

    /** 
     *  \brief Method to create the Accumulator pattern
     *  
     *  \return a pointer to the created Accumulator pattern (to be explicitly deallocated/destroyed)
     */ 
    accumulator_t *build_ptr()
    {
        return new accumulator_t(func, init_value, pardegree, name, closing_func, routing_func);
    }

    /** 
     *  \brief Method to create the Accumulator pattern
     *  
     *  \return a unique_ptr to the created Accumulator pattern
     */ 
    unique_ptr<accumulator_t> build_unique()
    {
        return make_unique<accumulator_t>(func, init_value, pardegree, name, closing_func, routing_func);
    }
};

/** 
 *  \class WinSeq_Builder
 *  
 *  \brief Builder of the Win_Seq pattern
 *  
 *  Builder class to ease the creation of the Win_Seq pattern.
 */ 
template<typename F_t>
class WinSeq_Builder
{
private:
    F_t func;
    // type of the pattern to be created by this builder
    using winseq_t = Win_Seq<decltype(get_tuple_t(func)),
                             decltype(get_result_t(func))>;
    // type of the closing function
    using closing_func_t = function<void(RuntimeContext&)>;
    uint64_t win_len = 1;
    uint64_t slide_len = 1;
    win_type_t winType = CB;
    string name = "anonymous_seq";
    closing_func_t closing_func = [](RuntimeContext &r) -> void { return; };

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _func non-incremental/incremental function
     */ 
    WinSeq_Builder(F_t _func): func(_func) {}

    /** 
     *  \brief Method to specify the configuration for count-based windows
     *  
     *  \param _win_len window length (in no. of tuples)
     *  \param _slide_len slide length (in no. of tuples)
     *  \return the object itself
     */ 
    WinSeq_Builder<F_t>& withCBWindow(uint64_t _win_len, uint64_t _slide_len)
    {
        win_len = _win_len;
        slide_len = _slide_len;
        winType = CB;
        return *this;
    }

    /** 
     *  \brief Method to specify the configuration for time-based windows
     *  
     *  \param _win_len window length (in microseconds)
     *  \param _slide_len slide length (in microseconds)
     *  \return the object itself
     */ 
    WinSeq_Builder<F_t>& withTBWindow(microseconds _win_len, microseconds _slide_len)
    {
        win_len = _win_len.count();
        slide_len = _slide_len.count();
        winType = TB;
        return *this;
    }

    /** 
     *  \brief Method to specify the name of the Win_Seq pattern
     *  
     *  \param _name string with the name to be given
     *  \return the object itself
     */ 
    WinSeq_Builder<F_t>& withName(string _name)
    {
        name = _name;
        return *this;
    }

    /** 
     *  \brief Method to specify the closing function used by the pattern
     *  
     *  \param _closing_func closing function to be used by the pattern
     *  \return the object itself
     */ 
    WinSeq_Builder<F_t>& withClosingFunction(closing_func_t _closing_func)
    {
        closing_func = _closing_func;
        return *this;
    }

#if __cplusplus >= 201703L
    /** 
     *  \brief Method to create the Win_Seq pattern (only C++17)
     *  
     *  \return a copy of the created Win_Seq pattern
     */ 
    winseq_t build()
    {
        return winseq_t(func, win_len, slide_len, winType, name, closing_func, RuntimeContext(1, 0), PatternConfig(0, 1, slide_len, 0, 1, slide_len), SEQ); // copy elision in C++17
    }
#endif

    /** 
     *  \brief Method to create the Win_Seq pattern
     *  
     *  \return a pointer to the created Win_Seq pattern (to be explicitly deallocated/destroyed)
     */ 
    winseq_t *build_ptr()
    {
        return new winseq_t(func, win_len, slide_len, winType, name, closing_func, RuntimeContext(1, 0), PatternConfig(0, 1, slide_len, 0, 1, slide_len), SEQ);
    }

    /** 
     *  \brief Method to create the Win_Seq pattern
     *  
     *  \return a unique_ptr to the created Win_Seq pattern
     */ 
    unique_ptr<winseq_t> build_unique()
    {
        return make_unique<winseq_t>(func, win_len, slide_len, winType, name, closing_func, RuntimeContext(1, 0), PatternConfig(0, 1, slide_len, 0, 1, slide_len), SEQ);
    }
};

/** 
 *  \class WinSeqGPU_Builder
 *  
 *  \brief Builder of the Win_Seq_GPU pattern
 *  
 *  Builder class to ease the creation of the Win_Seq_GPU pattern.
 */ 
template<typename F_t>
class WinSeqGPU_Builder
{
private:
    F_t func;
    // type of the pattern to be created by this builder
    using winseq_gpu_t = Win_Seq_GPU<decltype(get_tuple_t(func)),
                                     decltype(get_result_t(func)),
                                     decltype(func)>;
    uint64_t win_len = 1;
    uint64_t slide_len = 1;
    win_type_t winType = CB;
    size_t batch_len = 1;
    size_t n_thread_block = DEFAULT_CUDA_NUM_THREAD_BLOCK;
    string name = "anonymous_seq_gpu";
    size_t scratchpad_size = 0;

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _func host/device function
     */ 
    WinSeqGPU_Builder(F_t _func): func(_func) {}

    /** 
     *  \brief Method to specify the configuration for count-based windows
     *  
     *  \param _win_len window length (in no. of tuples)
     *  \param _slide_len slide length (in no. of tuples)
     *  \return the object itself
     */ 
    WinSeqGPU_Builder<F_t>& withCBWindow(uint64_t _win_len, uint64_t _slide_len)
    {
        win_len = _win_len;
        slide_len = _slide_len;
        winType = CB;
        return *this;
    }

    /** 
     *  \brief Method to specify the configuration for time-based windows
     *  
     *  \param _win_len window length (in microseconds)
     *  \param _slide_len slide length (in microseconds)
     *  \return the object itself
     */ 
    WinSeqGPU_Builder<F_t>& withTBWindow(microseconds _win_len, microseconds _slide_len)
    {
        win_len = _win_len.count();
        slide_len = _slide_len.count();
        winType = TB;
        return *this;
    }

    /** 
     *  \brief Method to specify the batch configuration
     *  
     *  \param _batch_len number of windows in a batch (1 window executed by 1 CUDA thread)
     *  \param _n_thread_block number of threads per block
     *  \return the object itself
     */ 
    WinSeqGPU_Builder<F_t>& withBatch(size_t _batch_len, size_t _n_thread_block=DEFAULT_CUDA_NUM_THREAD_BLOCK)
    {
        batch_len = _batch_len;
        n_thread_block = _n_thread_block;
        return *this;
    }

    /** 
     *  \brief Method to specify the name of the Win_Seq_GPU pattern
     *  
     *  \param _name string with the name to be given
     *  \return the object itself
     */ 
    WinSeqGPU_Builder<F_t>& withName(string _name)
    {
        name = _name;
        return *this;
    }

    /** 
     *  \brief Method to specify the size in bytes of the scratchpad memory per CUDA thread
     *  
     *  \param _scratchpad_size size in bytes of the scratchpad memory per CUDA thread
     *  \return the object itself
     */ 
    WinSeqGPU_Builder<F_t>& withScratchpad(size_t _scratchpad_size)
    {
        scratchpad_size = _scratchpad_size;
        return *this;
    }

    /** 
     *  \brief Method to create the Win_Seq_GPU pattern
     *  
     *  \return a pointer to the created Win_Seq_GPU pattern (to be explicitly deallocated/destroyed)
     */ 
    winseq_gpu_t *build_ptr()
    {
        return new winseq_gpu_t(func, win_len, slide_len, winType, batch_len, n_thread_block, name, scratchpad_size);
    }

    /** 
     *  \brief Method to create the Win_Seq_GPU pattern
     *  
     *  \return a unique_ptr to the created Win_Seq_GPU pattern
     */ 
    unique_ptr<winseq_gpu_t> build_unique()
    {
        return make_unique<winseq_gpu_t>(func, win_len, slide_len, winType, batch_len, n_thread_block, name, scratchpad_size);
    }
};

/** 
 *  \class WinFarm_Builder
 *  
 *  \brief Builder of the Win_Farm pattern
 *  
 *  Builder class to ease the creation of the Win_Farm pattern.
 */ 
template<typename T>
class WinFarm_Builder
{
private:
    T input;
    // type of the pattern to be created by this builder
    using winfarm_t = decltype(get_WF_nested_type(input));
    // type of the closing function
    using closing_func_t = function<void(RuntimeContext&)>;
    uint64_t win_len = 1;
    uint64_t slide_len = 1;
    win_type_t winType = CB;
    size_t emitter_degree = 1;
    size_t pardegree = 1;
    string name = "anonymous_wf";
    bool ordered = true;
    opt_level_t opt_level = LEVEL0;
    closing_func_t closing_func = [](RuntimeContext &r) -> void { return; };

    // window parameters initialization (input is a Pane_Farm instance)
    template<typename ...Args>
    void initWindowConf(Pane_Farm<Args...> _pf)
    {
        win_len = _pf.win_len;
        slide_len = _pf.slide_len;
        winType = _pf.winType;
    }

    // window parameters initialization (input is a Win_MapReduce instance)
    template<typename ...Args>
    void initWindowConf(Win_MapReduce<Args...> _wm)
    {
        win_len = _wm.win_len;
        slide_len = _wm.slide_len;
        winType = _wm.winType;
    }

    // window parameters initialization (input is a function)
    template<typename T2>
    void initWindowConf(T2 f)
    {
        win_len = 1;
        slide_len = 1;
        winType = CB;
    }

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _input can be either a function or an already instantiated Pane_Farm or Win_MapReduce pattern.
     */ 
    WinFarm_Builder(T _input): input(_input)
    {
        initWindowConf(input);
    }

    /** 
     *  \brief Method to specify the configuration for count-based windows
     *  
     *  \param _win_len window length (in no. of tuples)
     *  \param _slide_len slide length (in no. of tuples)
     *  \return the object itself
     */ 
    WinFarm_Builder<T>& withCBWindow(uint64_t _win_len, uint64_t _slide_len)
    {
        win_len = _win_len;
        slide_len = _slide_len;
        winType = CB;
        return *this;
    }

    /** 
     *  \brief Method to specify the configuration for time-based windows
     *  
     *  \param _win_len window length (in microseconds)
     *  \param _slide_len slide length (in microseconds)
     *  \return the object itself
     */ 
    WinFarm_Builder<T>& withTBWindow(microseconds _win_len, microseconds _slide_len)
    {
        win_len = _win_len.count();
        slide_len = _slide_len.count();
        winType = TB;
        return *this;
    }

    /** 
     *  \brief Method to specify the number of parallel emitters within the Win_Farm pattern
     *  
     *  \param _emitter_degree number of emitters
     *  \return the object itself
     */ 
    WinFarm_Builder<T>& withEmitters(size_t _emitter_degree)
    {
        emitter_degree = _emitter_degree;
        return *this;
    }

    /** 
     *  \brief Method to specify the number of parallel instances within the Win_Farm pattern
     *  
     *  \param _pardegree number of parallel instances
     *  \return the object itself
     */ 
    WinFarm_Builder<T>& withParallelism(size_t _pardegree)
    {
        pardegree = _pardegree;
        return *this;
    }

    /** 
     *  \brief Method to specify the name of the Win_Farm pattern
     *  
     *  \param _name string with the name to be given
     *  \return the object itself
     */ 
    WinFarm_Builder<T>& withName(string _name)
    {
        name = _name;
        return *this;
    }

    /** 
     *  \brief Method to specify the ordering behavior of the Win_Farm pattern
     *  
     *  \param _ordered boolean flag (true for total key-based ordering, false no ordering is provided)
     *  \return the object itself
     */ 
    WinFarm_Builder<T>& withOrdering(bool _ordered)
    {
        ordered = _ordered;
        return *this;
    }

    /** 
     *  \brief Method to specify the optimization level to build the Win_Farm pattern
     *  
     *  \param _opt_level (optimization level)
     *  \return the object itself
     */ 
    WinFarm_Builder<T>& withOptLevel(opt_level_t _opt_level)
    {
        opt_level = _opt_level;
        return *this;
    }

    /** 
     *  \brief Method to specify the closing function used by the pattern
     *         This method does not have any effect in case the Win_Farm
     *         replicates complex patterns (i.e. Pane_Farm and Win_MapReduce instances).
     *  
     *  \param _closing_func closing function to be used by the pattern
     *  \return the object itself
     */ 
    WinFarm_Builder<T>& withClosingFunction(closing_func_t _closing_func)
    {
        closing_func = _closing_func;
        return *this;
    }

#if __cplusplus >= 201703L
    /** 
     *  \brief Method to create the Win_Farm pattern (only C++17)
     *  
     *  \return a copy of the created Win_Farm pattern
     */ 
    winfarm_t build()
    {
        return winfarm_t(input, win_len, slide_len, winType, emitter_degree, pardegree, name, closing_func, ordered, opt_level); // copy elision in C++17
    }
#endif

    /** 
     *  \brief Method to create the Win_Farm pattern
     *  
     *  \return a pointer to the created Win_Farm pattern (to be explicitly deallocated/destroyed)
     */ 
    winfarm_t *build_ptr()
    {
        return new winfarm_t(input, win_len, slide_len, winType, emitter_degree, pardegree, name, closing_func, ordered, opt_level);
    }

    /** 
     *  \brief Method to create the Win_Farm pattern
     *  
     *  \return a unique_ptr to the created Win_Farm pattern
     */ 
    unique_ptr<winfarm_t> build_unique()
    {
        return make_unique<winfarm_t>(input, win_len, slide_len, winType, emitter_degree, pardegree, name, closing_func, ordered, opt_level);
    }
};

/** 
 *  \class WinFarmGPU_Builder
 *  
 *  \brief Builder of the Win_Farm_GPU pattern
 *  
 *  Builder class to ease the creation of the Win_Farm_GPU pattern.
 */ 
template<typename T>
class WinFarmGPU_Builder
{
private:
    T input;
    // type of the pattern to be created by this builder
    using winfarm_gpu_t = decltype(get_WF_GPU_nested_type(input));
    uint64_t win_len = 1;
    uint64_t slide_len = 1;
    win_type_t winType = CB;
    size_t emitter_degree = 1;
    size_t pardegree = 1;
    size_t batch_len = 1;
    size_t n_thread_block = DEFAULT_CUDA_NUM_THREAD_BLOCK;
    string name = "anonymous_wf_gpu";
    size_t scratchpad_size = 0;
    bool ordered = true;
    opt_level_t opt_level = LEVEL0;

    // window parameters initialization (input is a Pane_Farm_GPU instance)
    template<typename ...Args>
    void initWindowConf(Pane_Farm_GPU<Args...> _pf)
    {
        win_len = _pf.win_len;
        slide_len = _pf.slide_len;
        winType = _pf.winType;
        batch_len = _pf.batch_len;
        n_thread_block = _pf.n_thread_block;
    }

    // window parameters initialization (input is a Win_MapReduce_GPU instance)
    template<typename ...Args>
    void initWindowConf(Win_MapReduce_GPU<Args...> _wm)
    {
        win_len = _wm.win_len;
        slide_len = _wm.slide_len;
        winType = _wm.winType;
        batch_len = _wm.batch_len;
        n_thread_block = _wm.n_thread_block;
    }

    // window parameters initialization (input is a function)
    template<typename T2>
    void initWindowConf(T2 f)
    {
        win_len = 1;
        slide_len = 1;
        winType = CB;
        batch_len = 1;
        n_thread_block = DEFAULT_CUDA_NUM_THREAD_BLOCK;
    }

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _input can be either a host/device function or an already instantiated Pane_Farm_GPU or Win_MapReduce_GPU pattern.
     */ 
    WinFarmGPU_Builder(T _input): input(_input) {
        initWindowConf(input);
    }

    /** 
     *  \brief Method to specify the configuration for count-based windows
     *  
     *  \param _win_len window length (in no. of tuples)
     *  \param _slide_len slide length (in no. of tuples)
     *  \return the object itself
     */ 
    WinFarmGPU_Builder<T>& withCBWindow(uint64_t _win_len, uint64_t _slide_len)
    {
        win_len = _win_len;
        slide_len = _slide_len;
        winType = CB;
        return *this;
    }

    /** 
     *  \brief Method to specify the configuration for time-based windows
     *  
     *  \param _win_len window length (in microseconds)
     *  \param _slide_len slide length (in microseconds)
     *  \return the object itself
     */ 
    WinFarmGPU_Builder<T>& withTBWindow(microseconds _win_len, microseconds _slide_len)
    {
        win_len = _win_len.count();
        slide_len = _slide_len.count();
        winType = TB;
        return *this;
    }

    /** 
     *  \brief Method to specify the number of parallel emitters within the Win_Farm_GPU pattern
     *  
     *  \param _emitter_degree number of emitters
     *  \return the object itself
     */ 
    WinFarmGPU_Builder<T>& withEmitters(size_t _emitter_degree)
    {
        emitter_degree = _emitter_degree;
        return *this;
    }

    /** 
     *  \brief Method to specify the number of parallel instances within the Win_Farm_GPU pattern
     *  
     *  \param _pardegree number of parallel instances
     *  \return the object itself
     */ 
    WinFarmGPU_Builder<T>& withParallelism(size_t _pardegree)
    {
        pardegree = _pardegree;
        return *this;
    }

    /** 
     *  \brief Method to specify the batch configuration
     *  
     *  \param _batch_len number of windows in a batch (1 window executed by 1 CUDA thread)
     *  \param _n_thread_block number of threads per block
     *  \return the object itself
     */ 
    WinFarmGPU_Builder<T>& withBatch(size_t _batch_len, size_t _n_thread_block=DEFAULT_CUDA_NUM_THREAD_BLOCK)
    {
        batch_len = _batch_len;
        n_thread_block = _n_thread_block;
        return *this;
    }

    /** 
     *  \brief Method to specify the name of the Win_Farm_GPU pattern
     *  
     *  \param _name string with the name to be given
     *  \return the object itself
     */ 
    WinFarmGPU_Builder<T>& withName(string _name)
    {
        name = _name;
        return *this;
    }

    /** 
     *  \brief Method to specify the size in bytes of the scratchpad memory per CUDA thread
     *  
     *  \param _scratchpad_size size in bytes of the scratchpad memory per CUDA thread
     *  \return the object itself
     */ 
    WinFarmGPU_Builder<T>& withScratchpad(size_t _scratchpad_size)
    {
        scratchpad_size = _scratchpad_size;
        return *this;
    }

    /** 
     *  \brief Method to specify the ordering behavior of the Win_Farm_GPU pattern
     *  
     *  \param _ordered boolean flag (true for total key-based ordering, false no ordering is provided)
     *  \return the object itself
     */ 
    WinFarmGPU_Builder<T>& withOrdering(bool _ordered)
    {
        ordered = _ordered;
        return *this;
    }

    /** 
     *  \brief Method to specify the optimization level to build the Win_Farm_GPU pattern
     *  
     *  \param _opt_level (optimization level)
     *  \return the object itself
     */ 
    WinFarmGPU_Builder<T>& withOptLevel(opt_level_t _opt_level)
    {
        opt_level = _opt_level;
        return *this;
    }

    /** 
     *  \brief Method to create the Win_Farm_GPU pattern
     *  
     *  \return a pointer to the created Win_Farm_GPU pattern (to be explicitly deallocated/destroyed)
     */ 
    winfarm_gpu_t *build_ptr()
    {
        return new winfarm_gpu_t(input, win_len, slide_len, winType, emitter_degree, pardegree, batch_len, n_thread_block, name, scratchpad_size, ordered, opt_level);
    }

    /** 
     *  \brief Method to create the Win_Farm_GPU pattern
     *  
     *  \return a unique_ptr to the created Win_Farm_GPU pattern
     */ 
    unique_ptr<winfarm_gpu_t> build_unique()
    {
        return make_unique<winfarm_gpu_t>(input, win_len, slide_len, winType, emitter_degree, pardegree, batch_len, n_thread_block, name, scratchpad_size, ordered, opt_level);
    }
};

/** 
 *  \class KeyFarm_Builder
 *  
 *  \brief Builder of the Key_Farm pattern
 *  
 *  Builder class to ease the creation of the Key_Farm pattern.
 */ 
template<typename T>
class KeyFarm_Builder
{
private:
    T input;
    // type of the pattern to be created by this builder
    using keyfarm_t = decltype(get_KF_nested_type(input));
    // type of the closing function
    using closing_func_t = function<void(RuntimeContext&)>;
    // type of the function to map the key hashcode onto an identifier starting from zero to pardegree-1
    using routing_func_t = function<size_t(size_t, size_t)>;
    uint64_t win_len = 1;
    uint64_t slide_len = 1;
    win_type_t winType = CB;
    size_t pardegree = 1;
    string name = "anonymous_kf";
    routing_func_t routing_func = [](size_t k, size_t n) { return k%n; };
    opt_level_t opt_level = LEVEL0;
    closing_func_t closing_func = [](RuntimeContext &r) -> void { return; };

    // window parameters initialization (input is a Pane_Farm instance)
    template<typename ...Args>
    void initWindowConf(Pane_Farm<Args...> _pf)
    {
        win_len = _pf.win_len;
        slide_len = _pf.slide_len;
        winType = _pf.winType;
    }

    // window parameters initialization (input is a Win_MapReduce instance)
    template<typename ...Args>
    void initWindowConf(Win_MapReduce<Args...> _wm)
    {
        win_len = _wm.win_len;
        slide_len = _wm.slide_len;
        winType = _wm.winType;
    }

    // window parameters initialization (input is a function)
    template<typename T2>
    void initWindowConf(T2 f)
    {
        win_len = 1;
        slide_len = 1;
        winType = CB;
    }

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _input can be either a function or an already instantiated Pane_Farm or Win_MapReduce pattern.
     */ 
    KeyFarm_Builder(T _input): input(_input)
    {
        initWindowConf(input);
    }

    /** 
     *  \brief Method to specify the configuration for count-based windows
     *  
     *  \param _win_len window length (in no. of tuples)
     *  \param _slide_len slide length (in no. of tuples)
     *  \return the object itself
     */ 
    KeyFarm_Builder<T>& withCBWindow(uint64_t _win_len, uint64_t _slide_len)
    {
        win_len = _win_len;
        slide_len = _slide_len;
        winType = CB;
        return *this;
    }

    /** 
     *  \brief Method to specify the configuration for time-based windows
     *  
     *  \param _win_len window length (in microseconds)
     *  \param _slide_len slide length (in microseconds)
     *  \return the object itself
     */ 
    KeyFarm_Builder<T>& withTBWindow(microseconds _win_len, microseconds _slide_len)
    {
        win_len = _win_len.count();
        slide_len = _slide_len.count();
        winType = TB;
        return *this;
    }

    /** 
     *  \brief Method to specify the number of parallel instances within the Key_Farm pattern
     *  
     *  \param _pardegree number of parallel instances
     *  \return the object itself
     */ 
    KeyFarm_Builder<T>& withParallelism(size_t _pardegree)
    {
        pardegree = _pardegree;
        return *this;
    }

    /** 
     *  \brief Method to specify the name of the Key_Farm pattern
     *  
     *  \param _name string with the name to be given
     *  \return the object itself
     */ 
    KeyFarm_Builder<T>& withName(string _name)
    {
        name = _name;
        return *this;
    }

    /** 
     *  \brief Method to specify the routing function of input tuples to the internal patterns
     *  
     *  \param _routing_func function to map the key hashcode onto an identifier starting from zero to pardegree-1
     *  \return the object itself
     */ 
    KeyFarm_Builder<T>& set_KeyBy(routing_func_t _routing_func)
    {
        routing_func = _routing_func;
        return *this;
    }

    /** 
     *  \brief Method to specify the optimization level to build the Key_Farm pattern
     *  
     *  \param _opt_level (optimization level)
     *  \return the object itself
     */ 
    KeyFarm_Builder<T>& withOptLevel(opt_level_t _opt_level)
    {
        opt_level = _opt_level;
        return *this;
    }

    /** 
     *  \brief Method to specify the closing function used by the pattern
     *  
     *  \param _closing_func closing function to be used by the pattern
     *  \return the object itself
     */ 
    KeyFarm_Builder<T>& withClosingFunction(closing_func_t _closing_func)
    {
        closing_func = _closing_func;
        return *this;
    }

#if __cplusplus >= 201703L
    /** 
     *  \brief Method to create the Key_Farm pattern (only C++17)
     *  
     *  \return a copy of the created Key_Farm pattern
     */ 
    keyfarm_t build()
    {
        return keyfarm_t(input, win_len, slide_len, winType, pardegree, name, closing_func, routing_func, opt_level); // copy elision in C++17
    }
#endif

    /** 
     *  \brief Method to create the Key_Farm pattern
     *  
     *  \return a pointer to the created Key_Farm pattern (to be explicitly deallocated/destroyed)
     */ 
    keyfarm_t *build_ptr()
    {
        return new keyfarm_t(input, win_len, slide_len, winType, pardegree, name, closing_func, routing_func, opt_level);
    }

    /** 
     *  \brief Method to create the Key_Farm pattern
     *  
     *  \return a unique_ptr to the created Key_Farm pattern
     */ 
    unique_ptr<keyfarm_t> build_unique()
    {
        return make_unique<keyfarm_t>(input, win_len, slide_len, winType, pardegree, name, closing_func, routing_func, opt_level);
    }
};

/** 
 *  \class KeyFarmGPU_Builder
 *  
 *  \brief Builder of the Key_Farm_GPU pattern
 *  
 *  Builder class to ease the creation of the Key_Farm_GPU pattern.
 */ 
template<typename T>
class KeyFarmGPU_Builder
{
private:
    T input;
    // type of the function to map the key hashcode onto an identifier starting from zero to pardegree-1
    using routing_func_t = function<size_t(size_t, size_t)>;
    // type of the pattern to be created by this builder
    using keyfarm_gpu_t = decltype(get_KF_GPU_nested_type(input));
    uint64_t win_len = 1;
    uint64_t slide_len = 1;
    win_type_t winType = CB;
    size_t pardegree = 1;
    size_t batch_len = 1;
    size_t n_thread_block = DEFAULT_CUDA_NUM_THREAD_BLOCK;
    string name = "anonymous_wf_gpu";
    size_t scratchpad_size = 0;
    routing_func_t routing_func = [](size_t k, size_t n) { return k%n; };
    opt_level_t opt_level = LEVEL0;

    // window parameters initialization (input is a Pane_Farm_GPU instance)
    template<typename ...Args>
    void initWindowConf(Pane_Farm_GPU<Args...> _pf)
    {
        win_len = _pf.win_len;
        slide_len = _pf.slide_len;
        winType = _pf.winType;
        batch_len = _pf.batch_len;
        n_thread_block = _pf.n_thread_block;
    }

    // window parameters initialization (input is a Win_MapReduce_GPU instance)
    template<typename ...Args>
    void initWindowConf(Win_MapReduce_GPU<Args...> _wm)
    {
        win_len = _wm.win_len;
        slide_len = _wm.slide_len;
        winType = _wm.winType;
        batch_len = _wm.batch_len;
        n_thread_block = _wm.n_thread_block;
    }

    // window parameters initialization (input is a function)
    template<typename T2>
    void initWindowConf(T2 f)
    {
        win_len = 1;
        slide_len = 1;
        winType = CB;
        batch_len = 1;
        n_thread_block = DEFAULT_CUDA_NUM_THREAD_BLOCK;
    }

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _input can be either a host/device function or an already instantiated Pane_Farm_GPU or Win_MapReduce_GPU pattern.
     */ 
    KeyFarmGPU_Builder(T _input): input(_input) {
        initWindowConf(input);
    }

    /** 
     *  \brief Method to specify the configuration for count-based windows
     *  
     *  \param _win_len window length (in no. of tuples)
     *  \param _slide_len slide length (in no. of tuples)
     *  \return the object itself
     */ 
    KeyFarmGPU_Builder<T>& withCBWindow(uint64_t _win_len, uint64_t _slide_len)
    {
        win_len = _win_len;
        slide_len = _slide_len;
        winType = CB;
        return *this;
    }

    /** 
     *  \brief Method to specify the configuration for time-based windows
     *  
     *  \param _win_len window length (in microseconds)
     *  \param _slide_len slide length (in microseconds)
     *  \return the object itself
     */ 
    KeyFarmGPU_Builder<T>& withTBWindow(microseconds _win_len, microseconds _slide_len)
    {
        win_len = _win_len.count();
        slide_len = _slide_len.count();
        winType = TB;
        return *this;
    }

    /** 
     *  \brief Method to specify the number of parallel instances within the Key_Farm_GPU pattern
     *  
     *  \param _pardegree number of parallel instances
     *  \return the object itself
     */ 
    KeyFarmGPU_Builder<T>& withParallelism(size_t _pardegree)
    {
        pardegree = _pardegree;
        return *this;
    }

    /** 
     *  \brief Method to specify the batch configuration
     *  
     *  \param _batch_len number of windows in a batch (1 window executed by 1 CUDA thread)
     *  \param _n_thread_block number of threads per block
     *  \return the object itself
     */ 
    KeyFarmGPU_Builder<T>& withBatch(size_t _batch_len, size_t _n_thread_block=DEFAULT_CUDA_NUM_THREAD_BLOCK)
    {
        batch_len = _batch_len;
        n_thread_block = _n_thread_block;
        return *this;
    }

    /** 
     *  \brief Method to specify the name of the Key_Farm_GPU pattern
     *  
     *  \param _name string with the name to be given
     *  \return the object itself
     */ 
    KeyFarmGPU_Builder<T>& withName(string _name)
    {
        name = _name;
        return *this;
    }

    /** 
     *  \brief Method to specify the size in bytes of the scratchpad memory per CUDA thread
     *  
     *  \param _scratchpad_size size in bytes of the scratchpad memory per CUDA thread
     *  \return the object itself
     */ 
    KeyFarmGPU_Builder<T>& withScratchpad(size_t _scratchpad_size)
    {
        scratchpad_size = _scratchpad_size;
        return *this;
    }

    /** 
     *  \brief Method to specify the routing function of input tuples to the internal patterns
     *  
     *  \param _routing_func function to map the key hashcode onto an identifier starting from zero to pardegree-1
     *  \return the object itself
     */ 
    KeyFarmGPU_Builder<T>& set_KeyBy(routing_func_t _routing_func)
    {
        routing_func = _routing_func;
        return *this;
    }

    /** 
     *  \brief Method to specify the optimization level to build the Key_Farm_GPU pattern
     *  
     *  \param _opt_level (optimization level)
     *  \return the object itself
     */ 
    KeyFarmGPU_Builder<T>& withOptLevel(opt_level_t _opt_level)
    {
        opt_level = _opt_level;
        return *this;
    }

    /** 
     *  \brief Method to create the Key_Farm_GPU pattern
     *  
     *  \return a pointer to the created Key_Farm_GPU pattern (to be explicitly deallocated/destroyed)
     */ 
    keyfarm_gpu_t *build_ptr()
    {
        return new keyfarm_gpu_t(input, win_len, slide_len, winType, pardegree, batch_len, n_thread_block, name, scratchpad_size, routing_func, opt_level);
    }

    /** 
     *  \brief Method to create the Key_Farm_GPU pattern
     *  
     *  \return a unique_ptr to the created Key_Farm_GPU pattern
     */ 
    unique_ptr<keyfarm_gpu_t> build_unique()
    {
        return make_unique<keyfarm_gpu_t>(input, win_len, slide_len, winType, pardegree, batch_len, n_thread_block, name, scratchpad_size, routing_func, opt_level);
    }
};

/** 
 *  \class PaneFarm_Builder
 *  
 *  \brief Builder of the Pane_Farm pattern
 *  
 *  Builder class to ease the creation of the Pane_Farm pattern.
 */ 
template<typename F_t, typename G_t>
class PaneFarm_Builder
{
private:
    F_t func_F;
    G_t func_G;
    using panefarm_t = Pane_Farm<decltype(get_tuple_t(func_F)),
                                 decltype(get_result_t(func_F))>;
    // type of the closing function
    using closing_func_t = function<void(RuntimeContext&)>;
    uint64_t win_len = 1;
    uint64_t slide_len = 1;
    win_type_t winType = CB;
    size_t plq_degree = 1;
    size_t wlq_degree = 1;
    string name = "anonymous_pf";
    bool ordered = true;
    opt_level_t opt_level = LEVEL0;
    closing_func_t closing_func = [](RuntimeContext &r) -> void { return; };

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _func_F non-incremental/incremental function for the PLQ stage
     *  \param _func_G non-incremental/incremental function for the WLQ stage
     */ 
    PaneFarm_Builder(F_t _func_F, G_t _func_G): func_F(_func_F), func_G(_func_G) {}

    /** 
     *  \brief Method to specify the configuration for count-based windows
     *  
     *  \param _win_len window length (in no. of tuples)
     *  \param _slide_len slide length (in no. of tuples)
     *  \return the object itself
     */ 
    PaneFarm_Builder<F_t, G_t>& withCBWindow(uint64_t _win_len, uint64_t _slide_len)
    {
        win_len = _win_len;
        slide_len = _slide_len;
        winType = CB;
        return *this;
    }

    /** 
     *  \brief Method to specify the configuration for time-based windows
     *  
     *  \param _win_len window length (in microseconds)
     *  \param _slide_len slide length (in microseconds)
     *  \return the object itself
     */ 
    PaneFarm_Builder<F_t, G_t>& withTBWindow(microseconds _win_len, microseconds _slide_len)
    {
        win_len = _win_len.count();
        slide_len = _slide_len.count();
        winType = TB;
        return *this;
    }

    /** 
     *  \brief Method to specify parallel configuration within the Pane_Farm pattern
     *  
     *  \param _plq_degree number of Win_Seq instances in the PLQ stage
     *  \param _wlq_degree number of Win_Seq instances in the WLQ stage
     *  \return the object itself
     */ 
    PaneFarm_Builder<F_t, G_t>& withParallelism(size_t _plq_degree, size_t _wlq_degree)
    {
        plq_degree = _plq_degree;
        wlq_degree = _wlq_degree;
        return *this;
    }

    /** 
     *  \brief Method to specify the name of the Pane_Farm pattern
     *  
     *  \param _name string with the name to be given
     *  \return the object itself
     */ 
    PaneFarm_Builder<F_t, G_t>& withName(string _name)
    {
        name = _name;
        return *this;
    }

    /** 
     *  \brief Method to specify the ordering behavior of the Pane_Farm pattern
     *  
     *  \param _ordered boolean flag (true for total key-based ordering, false no ordering is provided)
     *  \return the object itself
     */ 
    PaneFarm_Builder<F_t, G_t>& withOrdering(bool _ordered)
    {
        ordered = _ordered;
        return *this;
    }

    /** 
     *  \brief Method to specify the optimization level to build the Pane_Farm pattern
     *  
     *  \param _opt_level (optimization level)
     *  \return the object itself
     */ 
    PaneFarm_Builder<F_t, G_t>& withOptLevel(opt_level_t _opt_level)
    {
        opt_level = _opt_level;
        return *this;
    }

    /** 
     *  \brief Method to specify the closing function used by the pattern
     *  
     *  \param _closing_func closing function to be used by the pattern
     *  \return the object itself
     */ 
    PaneFarm_Builder<F_t, G_t>& withClosingFunction(closing_func_t _closing_func)
    {
        closing_func = _closing_func;
        return *this;
    }

#if __cplusplus >= 201703L
    /** 
     *  \brief Method to create the Pane_Farm pattern (only C++17)
     *  
     *  \return a copy of the created Pane_Farm pattern
     */ 
    panefarm_t build()
    {
        return panefarm_t(func_F, func_G, win_len, slide_len, winType, plq_degree, wlq_degree, name, closing_func, ordered, opt_level); // copy elision in C++17
    }
#endif

    /** 
     *  \brief Method to create the Pane_Farm pattern
     *  
     *  \return a pointer to the created Pane_Farm pattern (to be explicitly deallocated/destroyed)
     */ 
    panefarm_t *build_ptr()
    {
        return new panefarm_t(func_F, func_G, win_len, slide_len, winType, plq_degree, wlq_degree, name, closing_func, ordered, opt_level);
    }

    /** 
     *  \brief Method to create the Pane_Farm pattern
     *  
     *  \return a unique_ptr to the created Pane_Farm pattern
     */ 
    unique_ptr<panefarm_t> build_unique()
    {
        return make_unique<panefarm_t>(func_F, func_G, win_len, slide_len, winType, plq_degree, wlq_degree, name, closing_func, ordered, opt_level);
    }
};

/** 
 *  \class PaneFarmGPU_Builder
 *  
 *  \brief Builder of the Pane_Farm_GPU pattern
 *  
 *  Builder class to ease the creation of the Pane_Farm_GPU pattern.
 */ 
template<typename F_t, typename G_t>
class PaneFarmGPU_Builder
{
private:
    F_t func_F;
    G_t func_G;
    using panefarm_gpu_t = Pane_Farm_GPU<decltype(get_tuple_t(func_F)),
                                         decltype(get_result_t(func_F)),
                                         decltype(get_GPU_F(&F_t::operator(), &G_t::operator()))>;
    uint64_t win_len = 1;
    uint64_t slide_len = 1;
    win_type_t winType = CB;
    size_t plq_degree = 1;
    size_t wlq_degree = 1;
    size_t batch_len = 1;
    size_t n_thread_block = DEFAULT_CUDA_NUM_THREAD_BLOCK;
    string name = "anonymous_pf_gpu";
    size_t scratchpad_size = 0;
    bool ordered = true;
    opt_level_t opt_level = LEVEL0;

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _func_F host or host/device function for the PLQ stage
     *  \param _func_G host or host/device function for the WLQ stage
     *  \note
     *  The GPU function must be passed through a callable object (e.g., lambda, functor)
     */ 
    PaneFarmGPU_Builder(F_t _func_F, G_t _func_G): func_F(_func_F), func_G(_func_G) {}

    /** 
     *  \brief Method to specify the configuration for count-based windows
     *  
     *  \param _win_len window length (in no. of tuples)
     *  \param _slide_len slide length (in no. of tuples)
     *  \return the object itself
     */ 
    PaneFarmGPU_Builder<F_t, G_t>& withCBWindow(uint64_t _win_len, uint64_t _slide_len)
    {
        win_len = _win_len;
        slide_len = _slide_len;
        winType = CB;
        return *this;
    }

    /** 
     *  \brief Method to specify the configuration for time-based windows
     *  
     *  \param _win_len window length (in microseconds)
     *  \param _slide_len slide length (in microseconds)
     *  \return the object itself
     */ 
    PaneFarmGPU_Builder<F_t, G_t>& withTBWindow(microseconds _win_len, microseconds _slide_len)
    {
        win_len = _win_len.count();
        slide_len = _slide_len.count();
        winType = TB;
        return *this;
    }

    /** 
     *  \brief Method to specify parallel configuration within the Pane_Farm_GPU pattern
     *  
     *  \param _plq_degree number of Win_Seq_GPU instances in the PLQ stage
     *  \param _wlq_degree number of Win_Seq_GPU instances in the WLQ stage
     *  \return the object itself
     */ 
    PaneFarmGPU_Builder<F_t, G_t>& withParallelism(size_t _plq_degree, size_t _wlq_degree)
    {
        plq_degree = _plq_degree;
        wlq_degree = _wlq_degree;
        return *this;
    }

    /** 
     *  \brief Method to specify the batch configuration
     *  
     *  \param _batch_len number of windows in a batch (1 window executed by 1 CUDA thread)
     *  \param _n_thread_block number of threads per block
     *  \return the object itself
     */ 
    PaneFarmGPU_Builder<F_t, G_t>& withBatch(size_t _batch_len, size_t _n_thread_block=DEFAULT_CUDA_NUM_THREAD_BLOCK)
    {
        batch_len = _batch_len;
        n_thread_block = _n_thread_block;
        return *this;
    }

    /** 
     *  \brief Method to specify the name of the Pane_Farm_GPU pattern
     *  
     *  \param _name string with the name to be given
     *  \return the object itself
     */ 
    PaneFarmGPU_Builder<F_t, G_t>& withName(string _name)
    {
        name = _name;
        return *this;
    }

    /** 
     *  \brief Method to specify the size in bytes of the scratchpad memory per CUDA thread
     *  
     *  \param _scratchpad_size size in bytes of the scratchpad memory per CUDA thread
     *  \return the object itself
     */ 
    PaneFarmGPU_Builder<F_t, G_t>& withScratchpad(size_t _scratchpad_size)
    {
        scratchpad_size = _scratchpad_size;
        return *this;
    }

    /** 
     *  \brief Method to specify the ordering behavior of the Pane_Farm_GPU pattern
     *  
     *  \param _ordered boolean flag (true for total key-based ordering, false no ordering is provided)
     *  \return the object itself
     */ 
    PaneFarmGPU_Builder<F_t, G_t>& withOrdering(bool _ordered)
    {
        ordered = _ordered;
        return *this;
    }

    /** 
     *  \brief Method to specify the optimization level to build the Pane_Farm_GPU pattern
     *  
     *  \param _opt_level (optimization level)
     *  \return the object itself
     */ 
    PaneFarmGPU_Builder<F_t, G_t>& withOptLevel(opt_level_t _opt_level)
    {
        opt_level = _opt_level;
        return *this;
    }

    /** 
     *  \brief Method to create the Pane_Farm_GPU pattern
     *  
     *  \return a pointer to the created Pane_Farm_GPU pattern (to be explicitly deallocated/destroyed)
     */ 
    panefarm_gpu_t *build_ptr()
    {
        return new panefarm_gpu_t(func_F, func_G, win_len, slide_len, winType, plq_degree, wlq_degree, batch_len, n_thread_block, name, scratchpad_size, ordered, opt_level);
    }

    /** 
     *  \brief Method to create the Pane_Farm_GPU pattern
     *  
     *  \return a unique_ptr to the created Pane_Farm_GPU pattern
     */ 
    unique_ptr<panefarm_gpu_t> build_unique()
    {
        return make_unique<panefarm_gpu_t>(func_F, func_G, win_len, slide_len, winType, plq_degree, wlq_degree, batch_len, n_thread_block, name, scratchpad_size, ordered, opt_level);
    }
};

/** 
 *  \class WinMapReduce_Builder
 *  
 *  \brief Builder of the Win_MapReduce pattern
 *  
 *  Builder class to ease the creation of the Win_MapReduce pattern.
 */ 
template<typename F_t, typename G_t>
class WinMapReduce_Builder
{
private:
    F_t func_F;
    G_t func_G;
    // type of the pattern to be created by this builder
    using winmapreduce_t = Win_MapReduce<decltype(get_tuple_t(func_F)),
                                         decltype(get_result_t(func_F))>;
    // type of the closing function
    using closing_func_t = function<void(RuntimeContext&)>;
    uint64_t win_len = 1;
    uint64_t slide_len = 1;
    win_type_t winType = CB;
    size_t map_degree = 2;
    size_t reduce_degree = 1;
    string name = "anonymous_wmr";
    bool ordered = true;
    opt_level_t opt_level = LEVEL0;
    closing_func_t closing_func = [](RuntimeContext &r) -> void { return; };

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _func_F non-incremental/incremental function for the MAP stage
     *  \param _func_G non-incremental/incremental function for the REDUCE stage
     */ 
    WinMapReduce_Builder(F_t _func_F, G_t _func_G): func_F(_func_F), func_G(_func_G) {}

    /** 
     *  \brief Method to specify the configuration for count-based windows
     *  
     *  \param _win_len window length (in no. of tuples)
     *  \param _slide_len slide length (in no. of tuples)
     *  \return the object itself
     */ 
    WinMapReduce_Builder<F_t, G_t>& withCBWindow(uint64_t _win_len, uint64_t _slide_len)
    {
        win_len = _win_len;
        slide_len = _slide_len;
        winType = CB;
        return *this;
    }

    /** 
     *  \brief Method to specify the configuration for time-based windows
     *  
     *  \param _win_len window length (in microseconds)
     *  \param _slide_len slide length (in microseconds)
     *  \return the object itself
     */ 
    WinMapReduce_Builder<F_t, G_t>& withTBWindow(microseconds _win_len, microseconds _slide_len)
    {
        win_len = _win_len.count();
        slide_len = _slide_len.count();
        winType = TB;
        return *this;
    }

    /** 
     *  \brief Method to specify parallel configuration within the Win_MapReduce pattern
     *  
     *  \param _map_degree number of Win_Seq instances in the MAP stage
     *  \param _reduce_degree number of Win_Seq instances in the REDUCE stage
     *  \return the object itself
     */ 
    WinMapReduce_Builder<F_t, G_t>& withParallelism(size_t _map_degree, size_t _reduce_degree)
    {
        map_degree = _map_degree;
        reduce_degree = _reduce_degree;
        return *this;
    }

    /** 
     *  \brief Method to specify the name of the Win_MapReduce pattern
     *  
     *  \param _name string with the name to be given
     *  \return the object itself
     */ 
    WinMapReduce_Builder<F_t, G_t>& withName(string _name)
    {
        name = _name;
        return *this;
    }

    /** 
     *  \brief Method to specify the ordering feature of the Win_MapReduce pattern
     *  
     *  \param _ordered boolean flag (true for total key-based ordering, false no ordering is provided)
     *  \return the object itself
     */ 
    WinMapReduce_Builder<F_t, G_t>& withOrdering(bool _ordered)
    {
        ordered = _ordered;
        return *this;
    }

    /** 
     *  \brief Method to specify the optimization level to build the Win_MapReduce pattern
     *  
     *  \param _opt_level (optimization level)
     *  \return the object itself
     */ 
    WinMapReduce_Builder<F_t, G_t>& withOptLevel(opt_level_t _opt_level)
    {
        opt_level = _opt_level;
        return *this;
    }

    /** 
     *  \brief Method to specify the closing function used by the pattern
     *  
     *  \param _closing_func closing function to be used by the pattern
     *  \return the object itself
     */ 
    WinMapReduce_Builder<F_t, G_t>& withClosingFunction(closing_func_t _closing_func)
    {
        closing_func = _closing_func;
        return *this;
    }

#if __cplusplus >= 201703L
    /** 
     *  \brief Method to create the Win_MapReduce pattern (only C++17)
     *  
     *  \return a copy of the created Win_MapReduce pattern
     */ 
    winmapreduce_t build()
    {
        return winmapreduce_t(func_F, func_G, win_len, slide_len, winType, map_degree, reduce_degree, name, closing_func, ordered, opt_level); // copy elision in C++17
    }
#endif

    /** 
     *  \brief Method to create the Win_MapReduce pattern
     *  
     *  \return a pointer to the created Win_MapReduce pattern (to be explicitly deallocated/destroyed)
     */ 
    winmapreduce_t *build_ptr()
    {
        return new winmapreduce_t(func_F, func_G, win_len, slide_len, winType, map_degree, reduce_degree, name, closing_func, ordered, opt_level);
    }

    /** 
     *  \brief Method to create the Win_MapReduce pattern
     *  
     *  \return a unique_ptr to the created Win_MapReduce pattern
     */ 
    unique_ptr<winmapreduce_t> build_unique()
    {
        return make_unique<winmapreduce_t>(func_F, func_G, win_len, slide_len, winType, map_degree, reduce_degree, name, closing_func, ordered, opt_level);
    }
};

/** 
 *  \class WinMapReduceGPU_Builder
 *  
 *  \brief Builder of the Win_MapReduce_GPU pattern
 *  
 *  Builder class to ease the creation of the Win_MapReduce_GPU pattern.
 */ 
template<typename F_t, typename G_t>
class WinMapReduceGPU_Builder
{
private:
    F_t func_F;
    G_t func_G;
    using winmapreduce_gpu_t = Win_MapReduce_GPU<decltype(get_tuple_t(func_F)),
                                                 decltype(get_result_t(func_F)),
                                                 decltype(get_GPU_F(&F_t::operator(), &G_t::operator()))>;
    uint64_t win_len = 1;
    uint64_t slide_len = 1;
    win_type_t winType = CB;
    size_t map_degree = 2;
    size_t reduce_degree = 1;
    size_t batch_len = 1;
    size_t n_thread_block = DEFAULT_CUDA_NUM_THREAD_BLOCK;
    string name = "anonymous_wmw_gpu";
    size_t scratchpad_size = 0;
    bool ordered = true;
    opt_level_t opt_level = LEVEL0;

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _func_F host or host/device function for the MAP stage
     *  \param _func_G host or host/device function for the REDUCE stage
     *  \note
     *  The GPU function must be passed through a callable object (e.g., lambda, functor)
     */ 
    WinMapReduceGPU_Builder(F_t _func_F, G_t _func_G): func_F(_func_F), func_G(_func_G) {}

    /** 
     *  \brief Method to specify the configuration for count-based windows
     *  
     *  \param _win_len window length (in no. of tuples)
     *  \param _slide_len slide length (in no. of tuples)
     *  \return the object itself
     */ 
    WinMapReduceGPU_Builder<F_t, G_t>& withCBWindow(uint64_t _win_len, uint64_t _slide_len)
    {
        win_len = _win_len;
        slide_len = _slide_len;
        winType = CB;
        return *this;
    }

    /** 
     *  \brief Method to specify the configuration for time-based windows
     *  
     *  \param _win_len window length (in microseconds)
     *  \param _slide_len slide length (in microseconds)
     *  \return the object itself
     */ 
    WinMapReduceGPU_Builder<F_t, G_t>& withTBWindow(microseconds _win_len, microseconds _slide_len)
    {
        win_len = _win_len.count();
        slide_len = _slide_len.count();
        winType = TB;
        return *this;
    }

    /** 
     *  \brief Method to specify parallel configuration within the Win_MapReduce_GPU pattern
     *  
     *  \param _map_degree number of Win_Seq_GPU instances in the MAP stage
     *  \param _reduce_degree number of Win_Seq_GPU instances in the REDUCE stage
     *  \return the object itself
     */ 
    WinMapReduceGPU_Builder<F_t, G_t>& withParallelism(size_t _map_degree, size_t _reduce_degree)
    {
        map_degree = _map_degree;
        reduce_degree = _reduce_degree;
        return *this;
    }

    /** 
     *  \brief Method to specify the batch configuration
     *  
     *  \param _batch_len number of windows in a batch (1 window executed by 1 CUDA thread)
     *  \param _n_thread_block number of threads per block
     *  \return the object itself
     */ 
    WinMapReduceGPU_Builder<F_t, G_t>& withBatch(size_t _batch_len, size_t _n_thread_block=DEFAULT_CUDA_NUM_THREAD_BLOCK)
    {
        batch_len = _batch_len;
        n_thread_block = _n_thread_block;
        return *this;
    }

    /** 
     *  \brief Method to specify the name of the Win_MapReduce_GPU pattern
     *  
     *  \param _name string with the name to be given
     *  \return the object itself
     */ 
    WinMapReduceGPU_Builder<F_t, G_t>& withName(string _name)
    {
        name = _name;
        return *this;
    }

    /** 
     *  \brief Method to specify the size in bytes of the scratchpad memory per CUDA thread
     *  
     *  \param _scratchpad_size size in bytes of the scratchpad memory per CUDA thread
     *  \return the object itself
     */ 
    WinMapReduceGPU_Builder<F_t, G_t>& withScratchpad(size_t _scratchpad_size)
    {
        scratchpad_size = _scratchpad_size;
        return *this;
    }

    /** 
     *  \brief Method to specify the ordering behavior of the Win_MapReduce_GPU pattern
     *  
     *  \param _ordered boolean flag (true for total key-based ordering, false no ordering is provided)
     *  \return the object itself
     */ 
    WinMapReduceGPU_Builder<F_t, G_t>& withOrdering(bool _ordered)
    {
        ordered = _ordered;
        return *this;
    }

    /** 
     *  \brief Method to specify the optimization level to build the Win_MapReduce_GPU pattern
     *  
     *  \param _opt_level (optimization level)
     *  \return the object itself
     */ 
    WinMapReduceGPU_Builder<F_t, G_t>& withOptLevel(opt_level_t _opt_level)
    {
        opt_level = _opt_level;
        return *this;
    }

    /** 
     *  \brief Method to create the Pane_Farm_GPU pattern
     *  
     *  \return a pointer to the created Pane_Farm_GPU pattern (to be explicitly deallocated/destroyed)
     */ 
    winmapreduce_gpu_t *build_ptr()
    {
        return new winmapreduce_gpu_t(func_F, func_G, win_len, slide_len, winType, map_degree, reduce_degree, batch_len, n_thread_block, name, scratchpad_size, ordered, opt_level);
    }

    /** 
     *  \brief Method to create the Pane_Farm_GPU pattern
     *  
     *  \return a unique_ptr to the created Pane_Farm_GPU pattern
     */ 
    unique_ptr<winmapreduce_gpu_t> build_unique()
    {
        return make_unique<winmapreduce_gpu_t>(func_F, func_G, win_len, slide_len, winType, map_degree, reduce_degree, batch_len, n_thread_block, name, scratchpad_size, ordered, opt_level);
    }
};

/** 
 *  \class Sink_Builder
 *  
 *  \brief Builder of the Sink pattern
 *  
 *  Builder class to ease the creation of the Sink pattern.
 */ 
template<typename F_t>
class Sink_Builder
{
private:
    F_t func;
    // type of the pattern to be created by this builder
    using sink_t = Sink<decltype(get_tuple_t(func))>;
    // type of the closing function
    using closing_func_t = function<void(RuntimeContext&)>;
    // type of the function to map the key hashcode onto an identifier starting from zero to pardegree-1
    using routing_func_t = function<size_t(size_t, size_t)>;
    uint64_t pardegree = 1;
    string name = "anonymous_sink";
    bool isKeyed = false;
    closing_func_t closing_func = [](RuntimeContext &r) -> void { return; };
    routing_func_t routing_func;

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _func function to absorb the stream elements
     */ 
    Sink_Builder(F_t _func): func(_func) {}

    /** 
     *  \brief Method to specify the name of the Sink pattern
     *  
     *  \param _name string with the name to be given
     *  \return the object itself
     */ 
    Sink_Builder<F_t>& withName(string _name)
    {
        name = _name;
        return *this;
    }

    /** 
     *  \brief Method to specify the number of parallel instances within the Sink pattern
     *  
     *  \param _pardegree number of parallel instances
     *  \return the object itself
     */ 
    Sink_Builder<F_t>& withParallelism(size_t _pardegree)
    {
        pardegree = _pardegree;
        return *this;
    }

    /** 
     *  \brief Method to enable the key-based routing
     *  
     *  \param _routing_func function to map the key hashcode onto an identifier starting from zero to pardegree-1
     *  \return the object itself
     */ 
    Sink_Builder<F_t>& set_KeyBy(routing_func_t _routing_func=[](size_t k, size_t n) { return k%n; })
    {
        isKeyed = true;
        routing_func = _routing_func;
        return *this;
    }

    /** 
     *  \brief Method to specify the closing function used by the pattern
     *  
     *  \param _closing_func closing function to be used by the pattern
     *  \return the object itself
     */ 
    Sink_Builder<F_t>& withClosingFunction(closing_func_t _closing_func)
    {
        closing_func = _closing_func;
        return *this;
    }

#if __cplusplus >= 201703L
    /** 
     *  \brief Method to create the Sink pattern (only C++17)
     *  
     *  \return a copy of the created Sink pattern
     */ 
    sink_t build()
    {
        if (!isKeyed)
            return sink_t(func, pardegree, name, closing_func); // copy elision in C++17
        else
            return sink_t(func, pardegree, name, closing_func, routing_func); // copy elision in C++17
    }
#endif

    /** 
     *  \brief Method to create the Sink pattern
     *  
     *  \return a pointer to the created Sink pattern (to be explicitly deallocated/destroyed)
     */ 
    sink_t *build_ptr()
    {
        if (!isKeyed)
            return new sink_t(func, pardegree, name, closing_func);
        else
            return new sink_t(func, pardegree, name, closing_func, routing_func);
    }

    /** 
     *  \brief Method to create the Sink pattern
     *  
     *  \return a unique_ptr to the created Sink pattern
     */ 
    unique_ptr<sink_t> build_unique()
    {
        if (!isKeyed)
            return make_unique<sink_t>(func, pardegree, name, closing_func);
        else
            return make_unique<sink_t>(func, pardegree, name, closing_func, routing_func);
    }
};

#endif
