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
 *  @version 1.0
 *  
 *  @brief Builders used to simplify the creation of the WindFlow patterns
 *  
 *  @section DESCRIPTION
 *  
 *  Set of builders based on method chaining to facilitate the creation of
 *  the WindFlow patterns.
 */ 

#ifndef BUILDERS_H
#define BUILDERS_H

// includes
#include <tuple>
#include <chrono>
#include <memory>
#include <functional>
#include <windflow.hpp>
#include <meta_utils.hpp>

using namespace chrono;

/** 
 *  \class Builder of the Win_Seq pattern
 *  
 *  \brief WinSeq_Builder
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
    uint64_t win_len = 1;
    uint64_t slide_len = 1;
    win_type_t winType = CB;
    string name = "noname";

public:
	/** 
     *  \brief Constructor
     *  
     *  \param _func non-incremental/incremental function for query processing
     */ 
    WinSeq_Builder(F_t _func): func(_func) {}

    /// Destructor
    ~WinSeq_Builder() {}

    /** 
     *  \brief Method to specify the configuration for count-based windows
     *  
     *  \param _win_len window length (in no. of tuples)
     *  \param _slide_len slide length (in no. of tuples)
     *  \return the object itself (method chaining)
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
     *  \return the object itself (method chaining)
     */ 
    WinSeq_Builder<F_t>& withTBWindow(microseconds _win_len, microseconds _slide_len)
    {
        win_len = _win_len.count();
        slide_len = _slide_len.count();
        winType = TB;
        return *this;
    }

    /** 
     *  \brief Method to specify the name of the Win_Seq instance
     *  
     *  \param _name string with the name to be given
     *  \return the object itself (method chaining)
     */ 
    WinSeq_Builder<F_t>& withName(string _name)
    {
        name = _name;
        return *this;
    }

#if __cplusplus >= 201703L
    /** 
     *  \brief Method to create the Win_Seq instance (only C++17)
     *  
     *  \return a copy of the created Win_Seq instance
     */ 
    winseq_t build()
    {
        return winseq_t(func, win_len, slide_len, winType, name); // copy elision in C++17
    }
#endif

    /** 
     *  \brief Method to create the Win_Seq instance
     *  
     *  \return a pointer to the created Win_Seq instance (to be explicitly deallocated/destroyed)
     */ 
    winseq_t *build_ptr()
    {
        return new winseq_t(func, win_len, slide_len, winType, name);
    }

    /** 
     *  \brief Method to create the Win_Seq instance
     *  
     *  \return a unique_ptr to the created Win_Seq instance
     */ 
    unique_ptr<winseq_t> build_unique()
    {
        return make_unique<winseq_t>(func, win_len, slide_len, winType, name);
    }
};

/** 
 *  \class Builder of the Win_Seq_GPU pattern
 *  
 *  \brief WinSeqGPU_Builder
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
    string name = "noname";
    size_t scratchpad_size = 0;

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _func host/device function for query processing
     */ 
    WinSeqGPU_Builder(F_t _func): func(_func) {}

    /// Destructor
    ~WinSeqGPU_Builder() {}

    /** 
     *  \brief Method to specify the configuration for count-based windows
     *  
     *  \param _win_len window length (in no. of tuples)
     *  \param _slide_len slide length (in no. of tuples)
     *  \return the object itself (method chaining)
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
     *  \return the object itself (method chaining)
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
     *  \return the object itself (method chaining)
     */ 
    WinSeqGPU_Builder<F_t>& withBatch(size_t _batch_len, size_t _n_thread_block=DEFAULT_CUDA_NUM_THREAD_BLOCK)
    {
        batch_len = _batch_len;
        n_thread_block = _n_thread_block;
        return *this;
    }

    /** 
     *  \brief Method to specify the name of the Win_Seq_GPU instance
     *  
     *  \param _name string with the name to be given
     *  \return the object itself (method chaining)
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
     *  \return the object itself (method chaining)
     */ 
    WinSeqGPU_Builder<F_t>& withScratchpad(size_t _scratchpad_size)
    {
        scratchpad_size = _scratchpad_size;
        return *this;
    }

    /** 
     *  \brief Method to create the Win_Seq_GPU instance
     *  
     *  \return a pointer to the created Win_Seq_GPU instance (to be explicitly deallocated/destroyed)
     */ 
    winseq_gpu_t *build_ptr()
    {
        return new winseq_gpu_t(func, win_len, slide_len, winType, batch_len, n_thread_block, name, scratchpad_size);
    }

    /** 
     *  \brief Method to create the Win_Seq_GPU instance
     *  
     *  \return a unique_ptr to the created Win_Seq_GPU instance
     */ 
    unique_ptr<winseq_gpu_t> build_unique()
    {
        return make_unique<winseq_gpu_t>(func, win_len, slide_len, winType, batch_len, n_thread_block, name, scratchpad_size);
    }
};

/** 
 *  \class Builder of the Win_Farm pattern
 *  
 *  \brief WinFarm_Builder
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
    uint64_t win_len = 1;
    uint64_t slide_len = 1;
    win_type_t winType = CB;
    size_t emitter_degree = 1;
    size_t pardegree = 1;
    string name = "noname";
    bool ordered = true;
    opt_level_t opt_level = LEVEL0;

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
     *  \param _input can be either a function or an already instantiated Pane_Farm or Win_MapReduce instance.
     */ 
    WinFarm_Builder(T _input): input(_input)
    {
        initWindowConf(input);
    }

    /// Destructor
    ~WinFarm_Builder() {}

    /** 
     *  \brief Method to specify the configuration for count-based windows
     *  
     *  \param _win_len window length (in no. of tuples)
     *  \param _slide_len slide length (in no. of tuples)
     *  \return the object itself (method chaining)
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
     *  \return the object itself (method chaining)
     */ 
    WinFarm_Builder<T>& withTBWindow(microseconds _win_len, microseconds _slide_len)
    {
        win_len = _win_len.count();
        slide_len = _slide_len.count();
        winType = TB;
        return *this;
    }

    /** 
     *  \brief Method to specify the number of parallel emitters within the Win_Farm instance
     *  
     *  \param _emitter_degree number of emitters
     *  \return the object itself (method chaining)
     */ 
    WinFarm_Builder<T>& withEmitters(size_t _emitter_degree)
    {
        emitter_degree = _emitter_degree;
        return *this;
    }

    /** 
     *  \brief Method to specify the number of parallel instances within the Win_Farm instance
     *  
     *  \param _pardegree number of parallel instances
     *  \return the object itself (method chaining)
     */ 
    WinFarm_Builder<T>& withParallelism(size_t _pardegree)
    {
        pardegree = _pardegree;
        return *this;
    }

    /** 
     *  \brief Method to specify the name of the Win_Farm instance
     *  
     *  \param _name string with the name to be given
     *  \return the object itself (method chaining)
     */ 
    WinFarm_Builder<T>& withName(string _name)
    {
        name = _name;
        return *this;
    }

    /** 
     *  \brief Method to specify the ordering behavior of the Win_Farm instance
     *  
     *  \param _ordered boolean flag (true for total key-based ordering, false no ordering is provided)
     *  \return the object itself (method chaining)
     */ 
    WinFarm_Builder<T>& withOrdered(bool _ordered)
    {
        ordered = _ordered;
        return *this;
    }

    /** 
     *  \brief Method to specify the optimization level to build the Win_Farm instance
     *  
     *  \param _opt_level (optimization level)
     *  \return the object itself (method chaining)
     */ 
    WinFarm_Builder<T>& withOpt(opt_level_t _opt_level)
    {
        opt_level = _opt_level;
        return *this;
    }

#if __cplusplus >= 201703L
    /** 
     *  \brief Method to create the Win_Farm instance (only C++17)
     *  
     *  \return a copy of the created Win_Farm instance
     */ 
    winfarm_t build()
    {
        return winfarm_t(input, win_len, slide_len, winType, emitter_degree, pardegree, name, ordered, opt_level); // copy elision in C++17
    }
#endif

    /** 
     *  \brief Method to create the Win_Farm instance
     *  
     *  \return a pointer to the created Win_Farm instance (to be explicitly deallocated/destroyed)
     */ 
    winfarm_t *build_ptr()
    {
        return new winfarm_t(input, win_len, slide_len, winType, emitter_degree, pardegree, name, ordered, opt_level);
    }

    /** 
     *  \brief Method to create the Win_Farm instance
     *  
     *  \return a unique_ptr to the created Win_Farm instance
     */ 
    unique_ptr<winfarm_t> build_unique()
    {
        return make_unique<winfarm_t>(input, win_len, slide_len, winType, emitter_degree, pardegree, name, ordered, opt_level);
    }
};

/** 
 *  \class Builder of the Win_Farm_GPU pattern
 *  
 *  \brief WinFarmGPU_Builder
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
    string name = "noname";
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
     *  \param _input can be either a function or an already instantiated Pane_Farm_GPU or Win_MapReduce_GPU instance.
     */ 
    WinFarmGPU_Builder(T _input): input(_input) {
        initWindowConf(input);
    }

    /// Destructor
    ~WinFarmGPU_Builder() {}

    /** 
     *  \brief Method to specify the configuration for count-based windows
     *  
     *  \param _win_len window length (in no. of tuples)
     *  \param _slide_len slide length (in no. of tuples)
     *  \return the object itself (method chaining)
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
     *  \return the object itself (method chaining)
     */ 
    WinFarmGPU_Builder<T>& withTBWindow(microseconds _win_len, microseconds _slide_len)
    {
        win_len = _win_len.count();
        slide_len = _slide_len.count();
        winType = TB;
        return *this;
    }

    /** 
     *  \brief Method to specify the number of parallel emitters within the Win_Farm_GPU instance
     *  
     *  \param _emitter_degree number of emitters
     *  \return the object itself (method chaining)
     */ 
    WinFarmGPU_Builder<T>& withEmitters(size_t _emitter_degree)
    {
        emitter_degree = _emitter_degree;
        return *this;
    }

    /** 
     *  \brief Method to specify the number of parallel instances within the Win_Farm_GPU instance
     *  
     *  \param _pardegree number of parallel instances
     *  \return the object itself (method chaining)
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
     *  \return the object itself (method chaining)
     */ 
    WinFarmGPU_Builder<T>& withBatch(size_t _batch_len, size_t _n_thread_block=DEFAULT_CUDA_NUM_THREAD_BLOCK)
    {
        batch_len = _batch_len;
        n_thread_block = _n_thread_block;
        return *this;
    }

    /** 
     *  \brief Method to specify the name of the Win_Farm_GPU instance
     *  
     *  \param _name string with the name to be given
     *  \return the object itself (method chaining)
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
     *  \return the object itself (method chaining)
     */ 
    WinFarmGPU_Builder<T>& withScratchpad(size_t _scratchpad_size)
    {
        scratchpad_size = _scratchpad_size;
        return *this;
    }

    /** 
     *  \brief Method to specify the ordering behavior of the Win_Farm_GPU instance
     *  
     *  \param _ordered boolean flag (true for total key-based ordering, false no ordering is provided)
     *  \return the object itself (method chaining)
     */ 
    WinFarmGPU_Builder<T>& withOrdered(bool _ordered)
    {
        ordered = _ordered;
        return *this;
    }

    /** 
     *  \brief Method to specify the optimization level to build the Win_Farm_GPU instance
     *  
     *  \param _opt_level (optimization level)
     *  \return the object itself (method chaining)
     */ 
    WinFarmGPU_Builder<T>& withOpt(opt_level_t _opt_level)
    {
        opt_level = _opt_level;
        return *this;
    }

    /** 
     *  \brief Method to create the Win_Farm_GPU instance
     *  
     *  \return a pointer to the created Win_Farm_GPU instance (to be explicitly deallocated/destroyed)
     */ 
    winfarm_gpu_t *build_ptr()
    {
        return new winfarm_gpu_t(input, win_len, slide_len, winType, emitter_degree, pardegree, batch_len, n_thread_block, name, scratchpad_size, ordered, opt_level);
    }

    /** 
     *  \brief Method to create the Win_Farm_GPU instance
     *  
     *  \return a unique_ptr to the created Win_Farm_GPU instance
     */ 
    unique_ptr<winfarm_gpu_t> build_unique()
    {
        return make_unique<winfarm_gpu_t>(input, win_len, slide_len, winType, emitter_degree, pardegree, batch_len, n_thread_block, name, scratchpad_size, ordered, opt_level);
    }
};

/** 
 *  \class Builder of the Key_Farm pattern
 *  
 *  \brief KeyFarm_Builder
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
    // type of the routing function
    using routing_F_t = function<size_t(size_t, size_t)>;
    uint64_t win_len = 1;
    uint64_t slide_len = 1;
    win_type_t winType = CB;
    size_t pardegree = 1;
    string name = "noname";
    routing_F_t routing_F = [](size_t k, size_t n) { return k%n; };
    opt_level_t opt_level = LEVEL0;

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
     *  \param _input can be either a function or an already instantiated Pane_Farm or Win_MapReduce instance.
     */ 
    KeyFarm_Builder(T _input): input(_input)
    {
        initWindowConf(input);
    }

    /// Destructor
    ~KeyFarm_Builder() {}

    /** 
     *  \brief Method to specify the configuration for count-based windows
     *  
     *  \param _win_len window length (in no. of tuples)
     *  \param _slide_len slide length (in no. of tuples)
     *  \return the object itself (method chaining)
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
     *  \return the object itself (method chaining)
     */ 
    KeyFarm_Builder<T>& withTBWindow(microseconds _win_len, microseconds _slide_len)
    {
        win_len = _win_len.count();
        slide_len = _slide_len.count();
        winType = TB;
        return *this;
    }

    /** 
     *  \brief Method to specify the number of parallel instances within the Key_Farm instance
     *  
     *  \param _pardegree number of parallel instances
     *  \return the object itself (method chaining)
     */ 
    KeyFarm_Builder<T>& withParallelism(size_t _pardegree)
    {
        pardegree = _pardegree;
        return *this;
    }

    /** 
     *  \brief Method to specify the name of the Key_Farm instance
     *  
     *  \param _name string with the name to be given
     *  \return the object itself (method chaining)
     */ 
    KeyFarm_Builder<T>& withName(string _name)
    {
        name = _name;
        return *this;
    }

    /** 
     *  \brief Method to specify the routing function of input tuples to the internal instances
     *  
     *  \param _routing_F routing function to be used
     *  \return the object itself (method chaining)
     */ 
    KeyFarm_Builder<T>& withRouting(routing_F_t _routing_F)
    {
        routing_F = _routing_F;
        return *this;
    }

    /** 
     *  \brief Method to specify the optimization level to build the Key_Farm instance
     *  
     *  \param _opt_level (optimization level)
     *  \return the object itself (method chaining)
     */ 
    KeyFarm_Builder<T>& withOpt(opt_level_t _opt_level)
    {
        opt_level = _opt_level;
        return *this;
    }

#if __cplusplus >= 201703L
    /** 
     *  \brief Method to create the Key_Farm instance (only C++17)
     *  
     *  \return a copy of the created Key_Farm instance
     */ 
    keyfarm_t build()
    {
        return keyfarm_t(input, win_len, slide_len, winType, pardegree, name, routing_F, opt_level); // copy elision in C++17
    }
#endif

    /** 
     *  \brief Method to create the Key_Farm instance
     *  
     *  \return a pointer to the created Key_Farm instance (to be explicitly deallocated/destroyed)
     */ 
    keyfarm_t *build_ptr()
    {
        return new keyfarm_t(input, win_len, slide_len, winType, pardegree, name, routing_F, opt_level);
    }

    /** 
     *  \brief Method to create the Key_Farm instance
     *  
     *  \return a unique_ptr to the created Key_Farm instance
     */ 
    unique_ptr<keyfarm_t> build_unique()
    {
        return make_unique<keyfarm_t>(input, win_len, slide_len, winType, pardegree, name, routing_F, opt_level);
    }
};

/** 
 *  \class Builder of the Key_Farm_GPU pattern
 *  
 *  \brief KeyFarmGPU_Builder
 *  
 *  Builder class to ease the creation of the Key_Farm_GPU pattern.
 */ 
template<typename T>
class KeyFarmGPU_Builder
{
private:
    T input;
    // type of the routing function
    using routing_F_t = function<size_t(size_t, size_t)>;
    // type of the pattern to be created by this builder
    using keyfarm_gpu_t = decltype(get_KF_GPU_nested_type(input));
    uint64_t win_len = 1;
    uint64_t slide_len = 1;
    win_type_t winType = CB;
    size_t pardegree = 1;
    size_t batch_len = 1;
    size_t n_thread_block = DEFAULT_CUDA_NUM_THREAD_BLOCK;
    string name = "noname";
    size_t scratchpad_size = 0;
    routing_F_t routing_F = [](size_t k, size_t n) { return k%n; };
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
     *  \param _func host/device function for query processing
     */ 
    KeyFarmGPU_Builder(T _input): input(_input) {
        initWindowConf(input);
    }

    /// Destructor
    ~KeyFarmGPU_Builder() {}

    /** 
     *  \brief Method to specify the configuration for count-based windows
     *  
     *  \param _win_len window length (in no. of tuples)
     *  \param _slide_len slide length (in no. of tuples)
     *  \return the object itself (method chaining)
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
     *  \return the object itself (method chaining)
     */ 
    KeyFarmGPU_Builder<T>& withTBWindow(microseconds _win_len, microseconds _slide_len)
    {
        win_len = _win_len.count();
        slide_len = _slide_len.count();
        winType = TB;
        return *this;
    }

    /** 
     *  \brief Method to specify the number of parallel instances within the Key_Farm_GPU instance
     *  
     *  \param _pardegree number of parallel instances
     *  \return the object itself (method chaining)
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
     *  \return the object itself (method chaining)
     */ 
    KeyFarmGPU_Builder<T>& withBatch(size_t _batch_len, size_t _n_thread_block=DEFAULT_CUDA_NUM_THREAD_BLOCK)
    {
        batch_len = _batch_len;
        n_thread_block = _n_thread_block;
        return *this;
    }

    /** 
     *  \brief Method to specify the name of the Key_Farm_GPU instance
     *  
     *  \param _name string with the name to be given
     *  \return the object itself (method chaining)
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
     *  \return the object itself (method chaining)
     */ 
    KeyFarmGPU_Builder<T>& withScratchpad(size_t _scratchpad_size)
    {
        scratchpad_size = _scratchpad_size;
        return *this;
    }

    /** 
     *  \brief Method to specify the routing function of input tuples to the internal instances
     *  
     *  \param _routing_F routing function to be used
     *  \return the object itself (method chaining)
     */ 
    KeyFarmGPU_Builder<T>& withRouting(routing_F_t _routing_F)
    {
        routing_F = _routing_F;
        return *this;
    }

    /** 
     *  \brief Method to specify the optimization level to build the Key_Farm_GPU instance
     *  
     *  \param _opt_level (optimization level)
     *  \return the object itself (method chaining)
     */ 
    KeyFarmGPU_Builder<T>& withOpt(opt_level_t _opt_level)
    {
        opt_level = _opt_level;
        return *this;
    }

    /** 
     *  \brief Method to create the Key_Farm_GPU instance
     *  
     *  \return a pointer to the created Key_Farm_GPU instance (to be explicitly deallocated/destroyed)
     */ 
    keyfarm_gpu_t *build_ptr()
    {
        return new keyfarm_gpu_t(input, win_len, slide_len, winType, pardegree, batch_len, n_thread_block, name, scratchpad_size, routing_F, opt_level);
    }

    /** 
     *  \brief Method to create the Key_Farm_GPU instance
     *  
     *  \return a unique_ptr to the created Key_Farm_GPU instance
     */ 
    unique_ptr<keyfarm_gpu_t> build_unique()
    {
        return make_unique<keyfarm_gpu_t>(input, win_len, slide_len, winType, pardegree, batch_len, n_thread_block, name, scratchpad_size, routing_F, opt_level);
    }
};

/** 
 *  \class Builder of the Pane_Farm pattern
 *  
 *  \brief PaneFarm_Builder
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
    uint64_t win_len = 1;
    uint64_t slide_len = 1;
    win_type_t winType = CB;
    size_t plq_degree = 1;
    size_t wlq_degree = 1;
    string name = "noname";
    bool ordered = true;
    opt_level_t opt_level = LEVEL0;

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _func_F non-incremental/incremental function for the PLQ stage
     *  \param _func_G non-incremental/incremental function for the WLQ stage
     */ 
    PaneFarm_Builder(F_t _func_F, G_t _func_G): func_F(_func_F), func_G(_func_G) {}

    /// Destructor
    ~PaneFarm_Builder() {}

    /** 
     *  \brief Method to specify the configuration for count-based windows
     *  
     *  \param _win_len window length (in no. of tuples)
     *  \param _slide_len slide length (in no. of tuples)
     *  \return the object itself (method chaining)
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
     *  \return the object itself (method chaining)
     */ 
    PaneFarm_Builder<F_t, G_t>& withTBWindow(microseconds _win_len, microseconds _slide_len)
    {
        win_len = _win_len.count();
        slide_len = _slide_len.count();
        winType = TB;
        return *this;
    }

    /** 
     *  \brief Method to specify parallel configuration within the Pane_Farm instance
     *  
     *  \param _plq_degree number of Win_Seq instances in the PLQ stage
     *  \param _wlq_degree number of Win_Seq instances in the WLQ stage
     *  \return the object itself (method chaining)
     */ 
    PaneFarm_Builder<F_t, G_t>& withParallelism(size_t _plq_degree, size_t _wlq_degree)
    {
        plq_degree = _plq_degree;
        wlq_degree = _wlq_degree;
        return *this;
    }

    /** 
     *  \brief Method to specify the name of the Pane_Farm instance
     *  
     *  \param _name string with the name to be given
     *  \return the object itself (method chaining)
     */ 
    PaneFarm_Builder<F_t, G_t>& withName(string _name)
    {
        name = _name;
        return *this;
    }

    /** 
     *  \brief Method to specify the ordering behavior of the Pane_Farm instance
     *  
     *  \param _ordered boolean flag (true for total key-based ordering, false no ordering is provided)
     *  \return the object itself (method chaining)
     */ 
    PaneFarm_Builder<F_t, G_t>& withOrdered(bool _ordered)
    {
        ordered = _ordered;
        return *this;
    }

    /** 
     *  \brief Method to specify the optimization level to build the Pane_Farm instance
     *  
     *  \param _opt_level (optimization level)
     *  \return the object itself (method chaining)
     */ 
    PaneFarm_Builder<F_t, G_t>& withOpt(opt_level_t _opt_level)
    {
        opt_level = _opt_level;
        return *this;
    }

#if __cplusplus >= 201703L
    /** 
     *  \brief Method to create the Pane_Farm instance (only C++17)
     *  
     *  \return a copy of the created Pane_Farm instance
     */ 
    panefarm_t build()
    {
        return panefarm_t(func_F, func_G, win_len, slide_len, winType, plq_degree, wlq_degree, name, ordered, opt_level); // copy elision in C++17
    }
#endif

    /** 
     *  \brief Method to create the Pane_Farm instance
     *  
     *  \return a pointer to the created Pane_Farm instance (to be explicitly deallocated/destroyed)
     */ 
    panefarm_t *build_ptr()
    {
        return new panefarm_t(func_F, func_G, win_len, slide_len, winType, plq_degree, wlq_degree, name, ordered, opt_level);
    }

    /** 
     *  \brief Method to create the Pane_Farm instance
     *  
     *  \return a unique_ptr to the created Pane_Farm instance
     */ 
    unique_ptr<panefarm_t> build_unique()
    {
        return make_unique<panefarm_t>(func_F, func_G, win_len, slide_len, winType, plq_degree, wlq_degree, name, ordered, opt_level);
    }
};

/** 
 *  \class Builder of the Pane_Farm_GPU pattern
 *  
 *  \brief PaneFarmGPU_Builder
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
    string name = "noname";
    size_t scratchpad_size = 0;
    bool ordered = true;
    opt_level_t opt_level = LEVEL0;

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _func_F function for the PLQ stage
     *  \param _func_G function for the WLQ stage
     *  \note
     *  The GPU function must be passed through a callable object (e.g., lambda, functor)
     */ 
    PaneFarmGPU_Builder(F_t _func_F, G_t _func_G): func_F(_func_F), func_G(_func_G) {}

    /// Destructor
    ~PaneFarmGPU_Builder() {}

    /** 
     *  \brief Method to specify the configuration for count-based windows
     *  
     *  \param _win_len window length (in no. of tuples)
     *  \param _slide_len slide length (in no. of tuples)
     *  \return the object itself (method chaining)
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
     *  \return the object itself (method chaining)
     */ 
    PaneFarmGPU_Builder<F_t, G_t>& withTBWindow(microseconds _win_len, microseconds _slide_len)
    {
        win_len = _win_len.count();
        slide_len = _slide_len.count();
        winType = TB;
        return *this;
    }

    /** 
     *  \brief Method to specify parallel configuration within the Pane_Farm_GPU instance
     *  
     *  \param _plq_degree number of Win_Seq_GPU instances in the PLQ stage
     *  \param _wlq_degree number of Win_Seq_GPU instances in the WLQ stage
     *  \return the object itself (method chaining)
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
     *  \return the object itself (method chaining)
     */ 
    PaneFarmGPU_Builder<F_t, G_t>& withBatch(size_t _batch_len, size_t _n_thread_block=DEFAULT_CUDA_NUM_THREAD_BLOCK)
    {
        batch_len = _batch_len;
        n_thread_block = _n_thread_block;
        return *this;
    }

    /** 
     *  \brief Method to specify the name of the Pane_Farm_GPU instance
     *  
     *  \param _name string with the name to be given
     *  \return the object itself (method chaining)
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
     *  \return the object itself (method chaining)
     */ 
    PaneFarmGPU_Builder<F_t, G_t>& withScratchpad(size_t _scratchpad_size)
    {
        scratchpad_size = _scratchpad_size;
        return *this;
    }

    /** 
     *  \brief Method to specify the ordering behavior of the Pane_Farm_GPU instance
     *  
     *  \param _ordered boolean flag (true for total key-based ordering, false no ordering is provided)
     *  \return the object itself (method chaining)
     */ 
    PaneFarmGPU_Builder<F_t, G_t>& withOrdered(bool _ordered)
    {
        ordered = _ordered;
        return *this;
    }

    /** 
     *  \brief Method to specify the optimization level to build the Pane_Farm_GPU instance
     *  
     *  \param _opt_level (optimization level)
     *  \return the object itself (method chaining)
     */ 
    PaneFarmGPU_Builder<F_t, G_t>& withOpt(opt_level_t _opt_level)
    {
        opt_level = _opt_level;
        return *this;
    }

    /** 
     *  \brief Method to create the Pane_Farm_GPU instance
     *  
     *  \return a pointer to the created Pane_Farm_GPU instance (to be explicitly deallocated/destroyed)
     */ 
    panefarm_gpu_t *build_ptr()
    {
        return new panefarm_gpu_t(func_F, func_G, win_len, slide_len, winType, plq_degree, wlq_degree, batch_len, n_thread_block, name, scratchpad_size, ordered, opt_level);
    }

    /** 
     *  \brief Method to create the Pane_Farm_GPU instance
     *  
     *  \return a unique_ptr to the created Pane_Farm_GPU instance
     */ 
    unique_ptr<panefarm_gpu_t> build_unique()
    {
        return make_unique<panefarm_gpu_t>(func_F, func_G, win_len, slide_len, winType, plq_degree, wlq_degree, batch_len, n_thread_block, name, scratchpad_size, ordered, opt_level);
    }
};

/** 
 *  \class Builder of the Win_MapReduce pattern
 *  
 *  \brief WinMapReduce_Builder
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
    uint64_t win_len = 1;
    uint64_t slide_len = 1;
    win_type_t winType = CB;
    size_t map_degree = 2;
    size_t reduce_degree = 1;
    string name = "noname";
    bool ordered = true;
    opt_level_t opt_level = LEVEL0;

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _func_F non-incremental/incremental function for the MAP stage
     *  \param _func_G non-incremental/incremental function for the REDUCE stage
     */ 
    WinMapReduce_Builder(F_t _func_F, G_t _func_G): func_F(_func_F), func_G(_func_G) {}

    /// Destructor
    ~WinMapReduce_Builder() {}

    /** 
     *  \brief Method to specify the configuration for count-based windows
     *  
     *  \param _win_len window length (in no. of tuples)
     *  \param _slide_len slide length (in no. of tuples)
     *  \return the object itself (method chaining)
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
     *  \return the object itself (method chaining)
     */ 
    WinMapReduce_Builder<F_t, G_t>& withTBWindow(microseconds _win_len, microseconds _slide_len)
    {
        win_len = _win_len.count();
        slide_len = _slide_len.count();
        winType = TB;
        return *this;
    }

    /** 
     *  \brief Method to specify parallel configuration within the Win_MapReduce instance
     *  
     *  \param _map_degree number of Win_Seq instances in the MAP stage
     *  \param _reduce_degree number of Win_Seq instances in the REDUCE stage
     *  \return the object itself (method chaining)
     */ 
    WinMapReduce_Builder<F_t, G_t>& withParallelism(size_t _map_degree, size_t _reduce_degree)
    {
        map_degree = _map_degree;
        reduce_degree = _reduce_degree;
        return *this;
    }

    /** 
     *  \brief Method to specify the name of the Win_MapReduce instance
     *  
     *  \param _name string with the name to be given
     *  \return the object itself (method chaining)
     */ 
    WinMapReduce_Builder<F_t, G_t>& withName(string _name)
    {
        name = _name;
        return *this;
    }

    /** 
     *  \brief Method to specify the ordering feature of the Win_MapReduce instance
     *  
     *  \param _ordered boolean flag (true for total key-based ordering, false no ordering is provided)
     *  \return the object itself (method chaining)
     */ 
    WinMapReduce_Builder<F_t, G_t>& withOrdered(bool _ordered)
    {
        ordered = _ordered;
        return *this;
    }

    /** 
     *  \brief Method to specify the optimization level to build the Win_MapReduce instance
     *  
     *  \param _opt_level (optimization level)
     *  \return the object itself (method chaining)
     */ 
    WinMapReduce_Builder<F_t, G_t>& withOpt(opt_level_t _opt_level)
    {
        opt_level = _opt_level;
        return *this;
    }

#if __cplusplus >= 201703L
    /** 
     *  \brief Method to create the Win_MapReduce instance (only C++17)
     *  
     *  \return a copy of the created Win_MapReduce instance
     */ 
    winmapreduce_t build()
    {
        return winmapreduce_t(func_F, func_G, win_len, slide_len, winType, map_degree, reduce_degree, name, ordered, opt_level); // copy elision in C++17
    }
#endif

    /** 
     *  \brief Method to create the Win_MapReduce instance
     *  
     *  \return a pointer to the created Win_MapReduce instance (to be explicitly deallocated/destroyed)
     */ 
    winmapreduce_t *build_ptr()
    {
        return new winmapreduce_t(func_F, func_G, win_len, slide_len, winType, map_degree, reduce_degree, name, ordered, opt_level);
    }

    /** 
     *  \brief Method to create the Win_MapReduce instance
     *  
     *  \return a unique_ptr to the created Win_MapReduce instance
     */ 
    unique_ptr<winmapreduce_t> build_unique()
    {
        return make_unique<winmapreduce_t>(func_F, func_G, win_len, slide_len, winType, map_degree, reduce_degree, name, ordered, opt_level);
    }
};

/** 
 *  \class Builder of the Win_MapReduce_GPU pattern
 *  
 *  \brief WinMapReduceGPU_Builder
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
    string name = "noname";
    size_t scratchpad_size = 0;
    bool ordered = true;
    opt_level_t opt_level = LEVEL0;

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _func_F function for the MAP stage
     *  \param _func_G function for the REDUCE stage
     *  \note
     *  The GPU function must be passed through a callable object (e.g., lambda, functor)
     */ 
    WinMapReduceGPU_Builder(F_t _func_F, G_t _func_G): func_F(_func_F), func_G(_func_G) {}

    /// Destructor
    ~WinMapReduceGPU_Builder() {}

    /** 
     *  \brief Method to specify the configuration for count-based windows
     *  
     *  \param _win_len window length (in no. of tuples)
     *  \param _slide_len slide length (in no. of tuples)
     *  \return the object itself (method chaining)
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
     *  \return the object itself (method chaining)
     */ 
    WinMapReduceGPU_Builder<F_t, G_t>& withTBWindow(microseconds _win_len, microseconds _slide_len)
    {
        win_len = _win_len.count();
        slide_len = _slide_len.count();
        winType = TB;
        return *this;
    }

    /** 
     *  \brief Method to specify parallel configuration within the Win_MapReduce_GPU instance
     *  
     *  \param _plq_degree number of Win_Seq_GPU instances in the MAP stage
     *  \param _wlq_degree number of Win_Seq_GPU instances in the REDUCE stage
     *  \return the object itself (method chaining)
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
     *  \return the object itself (method chaining)
     */ 
    WinMapReduceGPU_Builder<F_t, G_t>& withBatch(size_t _batch_len, size_t _n_thread_block=DEFAULT_CUDA_NUM_THREAD_BLOCK)
    {
        batch_len = _batch_len;
        n_thread_block = _n_thread_block;
        return *this;
    }

    /** 
     *  \brief Method to specify the name of the Win_MapReduce_GPU instance
     *  
     *  \param _name string with the name to be given
     *  \return the object itself (method chaining)
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
     *  \return the object itself (method chaining)
     */ 
    WinMapReduceGPU_Builder<F_t, G_t>& withScratchpad(size_t _scratchpad_size)
    {
        scratchpad_size = _scratchpad_size;
        return *this;
    }

    /** 
     *  \brief Method to specify the ordering behavior of the Win_MapReduce_GPU instance
     *  
     *  \param _ordered boolean flag (true for total key-based ordering, false no ordering is provided)
     *  \return the object itself (method chaining)
     */ 
    WinMapReduceGPU_Builder<F_t, G_t>& withOrdered(bool _ordered)
    {
        ordered = _ordered;
        return *this;
    }

    /** 
     *  \brief Method to specify the optimization level to build the Win_MapReduce_GPU instance
     *  
     *  \param _opt_level (optimization level)
     *  \return the object itself (method chaining)
     */ 
    WinMapReduceGPU_Builder<F_t, G_t>& withOpt(opt_level_t _opt_level)
    {
        opt_level = _opt_level;
        return *this;
    }

    /** 
     *  \brief Method to create the Pane_Farm_GPU instance
     *  
     *  \return a pointer to the created Pane_Farm_GPU instance (to be explicitly deallocated/destroyed)
     */ 
    winmapreduce_gpu_t *build_ptr()
    {
        return new winmapreduce_gpu_t(func_F, func_G, win_len, slide_len, winType, map_degree, reduce_degree, batch_len, n_thread_block, name, scratchpad_size, ordered, opt_level);
    }

    /** 
     *  \brief Method to create the Pane_Farm_GPU instance
     *  
     *  \return a unique_ptr to the created Pane_Farm_GPU instance
     */ 
    unique_ptr<winmapreduce_gpu_t> build_unique()
    {
        return make_unique<winmapreduce_gpu_t>(func_F, func_G, win_len, slide_len, winType, map_degree, reduce_degree, batch_len, n_thread_block, name, scratchpad_size, ordered, opt_level);
    }
};

#endif
