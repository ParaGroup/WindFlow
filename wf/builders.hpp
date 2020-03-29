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
 *  @brief Builders used to simplify the creation of the WindFlow operators
 *  
 *  @section Builders (Description)
 *  
 *  Set of builders to facilitate the creation of the WindFlow operators.
 */ 

#ifndef BUILDERS_H
#define BUILDERS_H

/// includes
#include <chrono>
#include <memory>
#include <functional>
#include <basic.hpp>
#include <meta.hpp>

namespace wf {

/** 
 *  \class Source_Builder
 *  
 *  \brief Builder of the Source operator
 *  
 *  Builder class to ease the creation of the Source operator.
 */ 
template<typename F_t>
class Source_Builder
{
private:
    F_t func;
    // type of the operator to be created by this builder
    using source_t = Source<decltype(get_tuple_t(func))>;
    // type of the closing function
    using closing_func_t = std::function<void(RuntimeContext&)>;
    uint64_t pardegree = 1;
    std::string name = "anonymous_source";
    closing_func_t closing_func = [](RuntimeContext &r) -> void { return; };

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _func function to generate the stream elements
     */ 
    Source_Builder(F_t _func): func(_func) {}

    /** 
     *  \brief Method to specify the name of the Source operator
     *  
     *  \param _name string with the name to be given
     *  \return the object itself
     */ 
    Source_Builder<F_t>& withName(std::string _name)
    {
        name = _name;
        return *this;
    }

    /** 
     *  \brief Method to specify the parallelism of the Source operator
     *  
     *  \param _pardegree number of source replicas
     *  \return the object itself
     */ 
    Source_Builder<F_t>& withParallelism(size_t _pardegree)
    {
        pardegree = _pardegree;
        return *this;
    }

    /** 
     *  \brief Method to specify the closing function used by the operator
     *  
     *  \param _closing_func closing function to be used by the operator
     *  \return the object itself
     */ 
    Source_Builder<F_t>& withClosingFunction(closing_func_t _closing_func)
    {
        closing_func = _closing_func;
        return *this;
    }

#if __cplusplus >= 201703L
    /** 
     *  \brief Method to create the Source operator (only C++17)
     *  
     *  \return a copy of the created Source operator
     */ 
    source_t build()
    {
        return source_t(func, pardegree, name, closing_func); // copy elision in C++17
    }
#endif

    /** 
     *  \brief Method to create the Source operator
     *  
     *  \return a pointer to the created Source operator (to be explicitly deallocated/destroyed)
     */ 
    source_t *build_ptr()
    {
        return new source_t(func, pardegree, name, closing_func);
    }

    /** 
     *  \brief Method to create the Source operator
     *  
     *  \return a unique_ptr to the created Source operator
     */ 
    std::unique_ptr<source_t> build_unique()
    {
        return std::make_unique<source_t>(func, pardegree, name, closing_func);
    }
};

/** 
 *  \class Filter_Builder
 *  
 *  \brief Builder of the Filter operator
 *  
 *  Builder class to ease the creation of the Filter operator.
 */ 
template<typename F_t>
class Filter_Builder
{
private:
    F_t func;
    // type of the operator to be created by this builder
    using filter_t = Filter<decltype(get_tuple_t(func)), decltype(get_result_t(func))>;
    // type of the closing function
    using closing_func_t = std::function<void(RuntimeContext&)>;
    // type of the function to map the key hashcode onto an identifier starting from zero to pardegree-1
    using routing_func_t = std::function<size_t(size_t, size_t)>;
    uint64_t pardegree = 1;
    std::string name = "anonymous_filter";
    bool isKeyed = false;
    closing_func_t closing_func = [](RuntimeContext &r) -> void { return; };
    routing_func_t routing_func = [](size_t k, size_t n) { return k%n; };

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _func function implementing the predicate
     */ 
    Filter_Builder(F_t _func): func(_func) {}

    /** 
     *  \brief Method to specify the name of the Filter operator
     *  
     *  \param _name string with the name to be given
     *  \return the object itself
     */ 
    Filter_Builder<F_t>& withName(std::string _name)
    {
        name = _name;
        return *this;
    }

    /** 
     *  \brief Method to specify the parallelism of the Filter operator
     *  
     *  \param _pardegree number of filter replicas
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
     *  \return the object itself
     */ 
    Filter_Builder<F_t>& enable_KeyBy()
    {
        isKeyed = true;
        return *this;
    }

    /** 
     *  \brief Method to specify the closing function used by the operator
     *  
     *  \param _closing_func closing function to be used by the operator
     *  \return the object itself
     */ 
    Filter_Builder<F_t>& withClosingFunction(closing_func_t _closing_func)
    {
        closing_func = _closing_func;
        return *this;
    }

#if __cplusplus >= 201703L
    /** 
     *  \brief Method to create the Filter operator (only C++17)
     *  
     *  \return a copy of the created Map operator
     */ 
    filter_t build()
    {
        if (!isKeyed) {
            return filter_t(func, pardegree, name, closing_func); // copy elision in C++17
        }
        else {
            return filter_t(func, pardegree, name, closing_func, routing_func); // copy elision in C++17
        }
    }
#endif

    /** 
     *  \brief Method to create the Filter operator
     *  
     *  \return a pointer to the created Filter operator (to be explicitly deallocated/destroyed)
     */ 
    filter_t *build_ptr()
    {
        if (!isKeyed) {
            return new filter_t(func, pardegree, name, closing_func);
        }
        else {
            return new filter_t(func, pardegree, name, closing_func, routing_func);
        }
    }

    /** 
     *  \brief Method to create the Filter operator
     *  
     *  \return a unique_ptr to the created Filter operator
     */ 
    std::unique_ptr<filter_t> build_unique()
    {
        if (!isKeyed) {
            return std::make_unique<filter_t>(func, pardegree, name, closing_func);
        }
        else {
            return std::make_unique<filter_t>(func, pardegree, name, closing_func, routing_func);
        }
    }
};

/** 
 *  \class Map_Builder
 *  
 *  \brief Builder of the Map operator
 *  
 *  Builder class to ease the creation of the Map operator.
 */ 
template<typename F_t>
class Map_Builder
{
private:
    F_t func;
    // type of the operator to be created by this builder
    using map_t = Map<decltype(get_tuple_t(func)),
                             decltype(get_result_t(func))>;
    // type of the closing function
    using closing_func_t = std::function<void(RuntimeContext&)>;
    // type of the function to map the key hashcode onto an identifier starting from zero to pardegree-1
    using routing_func_t = std::function<size_t(size_t, size_t)>;
    uint64_t pardegree = 1;
    std::string name = "anonymous_map";
    bool isKeyed = false;
    closing_func_t closing_func = [](RuntimeContext &r) -> void { return; };
    routing_func_t routing_func = [](size_t k, size_t n) { return k%n; };

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _func function implementing the one-to-one transformation
     */ 
    Map_Builder(F_t _func): func(_func) {}

    /** 
     *  \brief Method to specify the name of the Map operator
     *  
     *  \param _name string with the name to be given
     *  \return the object itself
     */ 
    Map_Builder<F_t>& withName(std::string _name)
    {
        name = _name;
        return *this;
    }

    /** 
     *  \brief Method to specify the parallelism of the Map operator
     *  
     *  \param _pardegree number of map replicas
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
     *  \return the object itself
     */ 
    Map_Builder<F_t>& enable_KeyBy()
    {
        isKeyed = true;
        return *this;
    }

    /** 
     *  \brief Method to specify the closing function used by the operator
     *  
     *  \param _closing_func closing function to be used by the operator
     *  \return the object itself
     */ 
    Map_Builder<F_t>& withClosingFunction(closing_func_t _closing_func)
    {
        closing_func = _closing_func;
        return *this;
    }

#if __cplusplus >= 201703L
    /** 
     *  \brief Method to create the Map operator (only C++17)
     *  
     *  \return a copy of the created Map operator
     */ 
    map_t build()
    {
        if (!isKeyed) {
            return map_t(func, pardegree, name, closing_func); // copy elision in C++17
        }
        else {
            return map_t(func, pardegree, name, closing_func, routing_func); // copy elision in C++17
        }
    }
#endif

    /** 
     *  \brief Method to create the Map operator
     *  
     *  \return a pointer to the created Map operator (to be explicitly deallocated/destroyed)
     */ 
    map_t *build_ptr()
    {
        if (!isKeyed) {
            return new map_t(func, pardegree, name, closing_func);
        }
        else {
            return new map_t(func, pardegree, name, closing_func, routing_func);
        }
    }

    /** 
     *  \brief Method to create the Map operator
     *  
     *  \return a unique_ptr to the created Map operator
     */ 
    std::unique_ptr<map_t> build_unique()
    {
        if (!isKeyed) {
            return std::make_unique<map_t>(func, pardegree, name, closing_func);
        }
        else {
            return std::make_unique<map_t>(func, pardegree, name, closing_func, routing_func);
        }
    }
};

/** 
 *  \class FlatMap_Builder
 *  
 *  \brief Builder of the FlatMap operator
 *  
 *  Builder class to ease the creation of the FlatMap operator.
 */ 
template<typename F_t>
class FlatMap_Builder
{
private:
    F_t func;
    // type of the operator to be created by this builder
    using flatmap_t = FlatMap<decltype(get_tuple_t(func)),
                             decltype(get_result_t(func))>;
    // type of the closing function
    using closing_func_t = std::function<void(RuntimeContext&)>;
    // type of the function to map the key hashcode onto an identifier starting from zero to pardegree-1
    using routing_func_t = std::function<size_t(size_t, size_t)>;
    uint64_t pardegree = 1;
    std::string name = "anonymous_flatmap";
    bool isKeyed = false;
    closing_func_t closing_func = [](RuntimeContext &r) -> void { return; };
    routing_func_t routing_func = [](size_t k, size_t n) { return k%n; };

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _func function implementing the one-to-any transformation
     */ 
    FlatMap_Builder(F_t _func): func(_func) {}

    /** 
     *  \brief Method to specify the name of the FlatMap operator
     *  
     *  \param _name string with the name to be given
     *  \return the object itself
     */ 
    FlatMap_Builder<F_t>& withName(std::string _name)
    {
        name = _name;
        return *this;
    }

    /** 
     *  \brief Method to specify the parallelism of the FlatMap operator
     *  
     *  \param _pardegree number of flatmap replicas
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
     *  \return the object itself
     */ 
    FlatMap_Builder<F_t>& enable_KeyBy()
    {
        isKeyed = true;
        return *this;
    }

    /** 
     *  \brief Method to specify the closing function used by the operator
     *  
     *  \param _closing_func closing function to be used by the operator
     *  \return the object itself
     */ 
    FlatMap_Builder<F_t>& withClosingFunction(closing_func_t _closing_func)
    {
        closing_func = _closing_func;
        return *this;
    }

#if __cplusplus >= 201703L
    /** 
     *  \brief Method to create the FlatMap operator (only C++17)
     *  
     *  \return a copy of the created FlatMap operator
     */ 
    flatmap_t build()
    {
        if (!isKeyed) {
            return flatmap_t(func, pardegree, name, closing_func); // copy elision in C++17
        }
        else {
            return flatmap_t(func, pardegree, name, closing_func, routing_func); // copy elision in C++17
        }
    }
#endif

    /** 
     *  \brief Method to create the FlatMap operator
     *  
     *  \return a pointer to the created FlatMap operator (to be explicitly deallocated/destroyed)
     */ 
    flatmap_t *build_ptr()
    {
        if (!isKeyed) {
            return new flatmap_t(func, pardegree, name, closing_func);
        }
        else {
            return new flatmap_t(func, pardegree, name, closing_func, routing_func);
        }
    }

    /** 
     *  \brief Method to create the FlatMap operator
     *  
     *  \return a unique_ptr to the created FlatMap operator
     */ 
    std::unique_ptr<flatmap_t> build_unique()
    {
        if (!isKeyed) {
            return std::make_unique<flatmap_t>(func, pardegree, name, closing_func);
        }
        else {
            return std::make_unique<flatmap_t>(func, pardegree, name, closing_func, routing_func);
        }
    }
};

/** 
 *  \class Accumulator_Builder
 *  
 *  \brief Builder of the Accumulator operator
 *  
 *  Builder class to ease the creation of the Accumulator operator.
 */ 
template<typename F_t>
class Accumulator_Builder
{
private:
    F_t func;
    // type of the operator to be created by this builder
    using accumulator_t = Accumulator<decltype(get_tuple_t(func)), decltype(get_result_t(func))>;
    // type of the closing function
    using closing_func_t = std::function<void(RuntimeContext&)>;
    // type of the function to map the key hashcode onto an identifier starting from zero to pardegree-1
    using routing_func_t = std::function<size_t(size_t, size_t)>;
    // type of the result produced by the Accumulator
    using result_t = decltype(get_result_t(func));
    uint64_t pardegree = 1;
    std::string name = "anonymous_accumulator";
    result_t init_value;
    closing_func_t closing_func = [](RuntimeContext &r) -> void { return; };
    routing_func_t routing_func = [](size_t k, size_t n) { return k%n; };

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _func function implementing the reduce/fold logic
     */ 
    Accumulator_Builder(F_t _func): func(_func) {}

    /** 
     *  \brief Method to specify the name of the Accumulator operator
     *  
     *  \param _name string with the name to be given
     *  \return the object itself
     */ 
    Accumulator_Builder<F_t>& withName(std::string _name)
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
     *  \brief Method to specify the parallelism of the Accumulator operator
     *  
     *  \param _pardegree number of accumulator replicas
     *  \return the object itself
     */ 
    Accumulator_Builder<F_t>& withParallelism(size_t _pardegree)
    {
        pardegree = _pardegree;
        return *this;
    }

    /** 
     *  \brief Method to specify the closing function used by the operator
     *  
     *  \param _closing_func closing function to be used by the operator
     *  \return the object itself
     */ 
    Accumulator_Builder<F_t>& withClosingFunction(closing_func_t _closing_func)
    {
        closing_func = _closing_func;
        return *this;
    }

#if __cplusplus >= 201703L
    /** 
     *  \brief Method to create the Accumulator operator (only C++17)
     *  
     *  \return a copy of the created Accumulator operator
     */ 
    accumulator_t build()
    {
        return accumulator_t(func, init_value, pardegree, name, closing_func, routing_func); // copy elision in C++17
    }
#endif

    /** 
     *  \brief Method to create the Accumulator operator
     *  
     *  \return a pointer to the created Accumulator operator (to be explicitly deallocated/destroyed)
     */ 
    accumulator_t *build_ptr()
    {
        return new accumulator_t(func, init_value, pardegree, name, closing_func, routing_func);
    }

    /** 
     *  \brief Method to create the Accumulator operator
     *  
     *  \return a unique_ptr to the created Accumulator operator
     */ 
    std::unique_ptr<accumulator_t> build_unique()
    {
        return std::make_unique<accumulator_t>(func, init_value, pardegree, name, closing_func, routing_func);
    }
};

/** 
 *  \class WinSeq_Builder
 *  
 *  \brief Builder of the Win_Seq operator
 *  
 *  Builder class to ease the creation of the Win_Seq operator.
 */ 
template<typename F_t>
class WinSeq_Builder
{
private:
    F_t func;
    // type of the operator to be created by this builder
    using winseq_t = Win_Seq<decltype(get_tuple_t(func)),
                             decltype(get_result_t(func))>;
    // type of the closing function
    using closing_func_t = std::function<void(RuntimeContext&)>;
    uint64_t win_len = 1;
    uint64_t slide_len = 1;
    uint64_t triggering_delay = 0;
    win_type_t winType = CB;
    std::string name = "anonymous_seq";
    closing_func_t closing_func = [](RuntimeContext &r) -> void { return; };

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _func function implementing the non-incremental/incremental window processing function
     */ 
    WinSeq_Builder(F_t _func): func(_func) {}

    /** 
     *  \brief Method to specify the configuration for count-based windows
     *  
     *  \param _win_len window length (in no. of tuples)
     *  \param _slide_len slide length (in no. of tuples)
     *  \return the object itself
     */ 
    WinSeq_Builder<F_t>& withCBWindows(uint64_t _win_len, uint64_t _slide_len)
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
     *  \param _triggering_delay (in microseconds)
     *  \return the object itself
     */ 
    WinSeq_Builder<F_t>& withTBWindows(std::chrono::microseconds _win_len, std::chrono::microseconds _slide_len, std::chrono::microseconds _triggering_delay=std::chrono::microseconds::zero())
    {
        win_len = _win_len.count();
        slide_len = _slide_len.count();
        triggering_delay = _triggering_delay.count();
        winType = TB;
        return *this;
    }

    /** 
     *  \brief Method to specify the name of the Win_Seq operator
     *  
     *  \param _name string with the name to be given
     *  \return the object itself
     */ 
    WinSeq_Builder<F_t>& withName(std::string _name)
    {
        name = _name;
        return *this;
    }

    /** 
     *  \brief Method to specify the closing function used by the operator
     *  
     *  \param _closing_func closing function to be used by the operator
     *  \return the object itself
     */ 
    WinSeq_Builder<F_t>& withClosingFunction(closing_func_t _closing_func)
    {
        closing_func = _closing_func;
        return *this;
    }

#if __cplusplus >= 201703L
    /** 
     *  \brief Method to create the Win_Seq operator (only C++17)
     *  
     *  \return a copy of the created Win_Seq operator
     */ 
    winseq_t build()
    {
        return winseq_t(func, win_len, slide_len, triggering_delay, winType, name, closing_func, RuntimeContext(1, 0), OperatorConfig(0, 1, slide_len, 0, 1, slide_len), SEQ); // copy elision in C++17
    }
#endif

    /** 
     *  \brief Method to create the Win_Seq operator
     *  
     *  \return a pointer to the created Win_Seq operator (to be explicitly deallocated/destroyed)
     */ 
    winseq_t *build_ptr()
    {
        return new winseq_t(func, win_len, slide_len, triggering_delay, winType, name, closing_func, RuntimeContext(1, 0), OperatorConfig(0, 1, slide_len, 0, 1, slide_len), SEQ);
    }

    /** 
     *  \brief Method to create the Win_Seq operator
     *  
     *  \return a unique_ptr to the created Win_Seq operator
     */ 
    std::unique_ptr<winseq_t> build_unique()
    {
        return std::make_unique<winseq_t>(func, win_len, slide_len, triggering_delay, winType, name, closing_func, RuntimeContext(1, 0), OperatorConfig(0, 1, slide_len, 0, 1, slide_len), SEQ);
    }
};

/** 
 *  \class WinSeqFFAT_Builder
 *  
 *  \brief Builder of the Win_SeqFFAT operator
 *  
 *  Builder class to ease the creation of the Win_SeqFFAT operator.
 */ 
template<typename F_t, typename G_t>
class WinSeqFFAT_Builder
{
private:
    F_t lift_func;
    G_t comb_func;
    // type of the operator to be created by this builder
    using winffat_t = Win_SeqFFAT<decltype(get_tuple_t(lift_func)),
                             decltype(get_result_t(lift_func))>;
    // type of the closing function
    using closing_func_t = std::function<void(RuntimeContext&)>;
    uint64_t win_len = 1;
    uint64_t slide_len = 1;
    uint64_t triggering_delay = 0;
    win_type_t winType = CB;
    std::string name = "anonymous_seqffat";
    closing_func_t closing_func = [](RuntimeContext &r) -> void { return; };

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _lift_func the lift function to translate a tuple into a result
     *  \param _comb_func the combine function to combine two results into a result
     */ 
    WinSeqFFAT_Builder(F_t _lift_func, G_t _comb_func): lift_func(_lift_func), comb_func(_comb_func) {}

    /** 
     *  \brief Method to specify the configuration for count-based windows
     *  
     *  \param _win_len window length (in no. of tuples)
     *  \param _slide_len slide length (in no. of tuples)
     *  \return the object itself
     */ 
    WinSeqFFAT_Builder<F_t, G_t>& withCBWindows(uint64_t _win_len, uint64_t _slide_len)
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
     *  \param _triggering_delay (in microseconds)
     *  \return the object itself
     */ 
    WinSeqFFAT_Builder<F_t, G_t>& withTBWindows(std::chrono::microseconds _win_len, std::chrono::microseconds _slide_len, std::chrono::microseconds _triggering_delay=std::chrono::microseconds::zero())
    {
        win_len = _win_len.count();
        slide_len = _slide_len.count();
        triggering_delay = _triggering_delay.count();
        winType = TB;
        return *this;
    }

    /** 
     *  \brief Method to specify the name of the Win_SeqFFAT operator
     *  
     *  \param _name string with the name to be given
     *  \return the object itself
     */ 
    WinSeqFFAT_Builder<F_t, G_t>& withName(std::string _name)
    {
        name = _name;
        return *this;
    }

    /** 
     *  \brief Method to specify the closing function used by the operator
     *  
     *  \param _closing_func closing function to be used by the operator
     *  \return the object itself
     */ 
    WinSeqFFAT_Builder<F_t, G_t>& withClosingFunction(closing_func_t _closing_func)
    {
        closing_func = _closing_func;
        return *this;
    }

#if __cplusplus >= 201703L
    /** 
     *  \brief Method to create the Win_SeqFFAT operator (only C++17)
     *  
     *  \return a copy of the created Win_SeqFFAT operator
     */ 
    winffat_t build()
    {
        return winffat_t(lift_func, comb_func, win_len, slide_len, triggering_delay, winType, name, closing_func, RuntimeContext(1, 0), OperatorConfig(0, 1, slide_len, 0, 1, slide_len));
    }
#endif

    /** 
     *  \brief Method to create the Win_SeqFFAT operator
     *  
     *  \return a pointer to the created Win_SeqFFAT operator (to be explicitly deallocated/destroyed)
     */ 
    winffat_t *build_ptr()
    {
        return new winffat_t(lift_func, comb_func, win_len, slide_len, triggering_delay, winType, name, closing_func, RuntimeContext(1, 0), OperatorConfig(0, 1, slide_len, 0, 1, slide_len));
    }

    /** 
     *  \brief Method to create the Win_SeqFFAT operator
     *  
     *  \return a unique_ptr to the created Win_SeqFFAT operator
     */ 
    std::unique_ptr<winffat_t> build_unique()
    {
        return std::make_unique<winffat_t>(lift_func, comb_func, win_len, slide_len, triggering_delay, winType, name, closing_func, RuntimeContext(1, 0), OperatorConfig(0, 1, slide_len, 0, 1, slide_len));
    }
};

/** 
 *  \class WinSeqGPU_Builder
 *  
 *  \brief Builder of the Win_Seq_GPU operator
 *  
 *  Builder class to ease the creation of the Win_Seq_GPU operator.
 */ 
template<typename F_t>
class WinSeqGPU_Builder
{
private:
    F_t func;
    // type of the operator to be created by this builder
    using winseq_gpu_t = Win_Seq_GPU<decltype(get_tuple_t(func)),
                                     decltype(get_result_t(func)),
                                     decltype(func)>;
    uint64_t win_len = 1;
    uint64_t slide_len = 1;
    uint64_t triggering_delay = 0;
    win_type_t winType = CB;
    size_t batch_len = 1;
    size_t n_thread_block = DEFAULT_CUDA_NUM_THREAD_BLOCK;
    std::string name = "anonymous_seq_gpu";
    size_t scratchpad_size = 0;

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _func the non-incremental window processing function (__host__ __device__ function)
     */ 
    WinSeqGPU_Builder(F_t _func): func(_func) {}

    /** 
     *  \brief Method to specify the configuration for count-based windows
     *  
     *  \param _win_len window length (in no. of tuples)
     *  \param _slide_len slide length (in no. of tuples)
     *  \return the object itself
     */ 
    WinSeqGPU_Builder<F_t>& withCBWindows(uint64_t _win_len, uint64_t _slide_len)
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
     *  \param _triggering_delay (in microseconds)
     *  \return the object itself
     */ 
    WinSeqGPU_Builder<F_t>& withTBWindows(std::chrono::microseconds _win_len, std::chrono::microseconds _slide_len, std::chrono::microseconds _triggering_delay=std::chrono::microseconds::zero())
    {
        win_len = _win_len.count();
        slide_len = _slide_len.count();
        triggering_delay = _triggering_delay.count();
        winType = TB;
        return *this;
    }

    /** 
     *  \brief Method to specify the batch configuration
     *  
     *  \param _batch_len number of windows in a batch
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
     *  \brief Method to specify the name of the Win_Seq_GPU operator
     *  
     *  \param _name string with the name to be given
     *  \return the object itself
     */ 
    WinSeqGPU_Builder<F_t>& withName(std::string _name)
    {
        name = _name;
        return *this;
    }

    /** 
     *  \brief Method to specify the size in bytes of the scratchpad memory per CUDA thread
     *  
     *  \param _scratchpad_size size in bytes of the scratchpad area local of a CUDA thread (pre-allocated on the global memory of the GPU)
     *  \return the object itself
     */ 
    WinSeqGPU_Builder<F_t>& withScratchpad(size_t _scratchpad_size)
    {
        scratchpad_size = _scratchpad_size;
        return *this;
    }

    /** 
     *  \brief Method to create the Win_Seq_GPU operator
     *  
     *  \return a pointer to the created Win_Seq_GPU operator (to be explicitly deallocated/destroyed)
     */ 
    winseq_gpu_t *build_ptr()
    {
        return new winseq_gpu_t(func, win_len, slide_len, triggering_delay, winType, batch_len, n_thread_block, name, scratchpad_size);
    }

    /** 
     *  \brief Method to create the Win_Seq_GPU operator
     *  
     *  \return a unique_ptr to the created Win_Seq_GPU operator
     */ 
    std::unique_ptr<winseq_gpu_t> build_unique()
    {
        return std::make_unique<winseq_gpu_t>(func, win_len, slide_len, triggering_delay, winType, batch_len, n_thread_block, name, scratchpad_size);
    }
};

/** 
 *  \class WinSeqFFATGPU_Builder
 *  
 *  \brief Builder of the WinSeqFFAT_GPU operator
 *  
 *  Builder class to ease the creation of the WinSeqFFAT_GPU operator.
 */ 
template<typename F_t, typename G_t>
class WinSeqFFATGPU_Builder
{
private:
    F_t lift_func;
    G_t comb_func;
    // type of the operator to be created by this builder
    using winffat_gpu_t = Win_SeqFFAT_GPU<decltype(get_tuple_t(lift_func)),
                                         decltype(get_result_t(lift_func)),
                                         decltype(comb_func)>;
    uint64_t win_len = 1;
    uint64_t slide_len = 1;
    uint64_t triggering_delay = 0;
    win_type_t winType = CB;
    size_t batch_len = 1;
    size_t n_thread_block = DEFAULT_CUDA_NUM_THREAD_BLOCK;
    bool rebuild = false;
    std::string name = "anonymous_seqffat_gpu";

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _lift_func the lift function to translate a tuple into a result
     *  \param _comb_func the combine function to combine two results into a result (__host__ __device__ function)
     */ 
    WinSeqFFATGPU_Builder(F_t _lift_func, G_t _comb_func): lift_func(_lift_func), comb_func(_comb_func) {}

    /** 
     *  \brief Method to specify the configuration for count-based windows
     *  
     *  \param _win_len window length (in no. of tuples)
     *  \param _slide_len slide length (in no. of tuples)
     *  \return the object itself
     */ 
    WinSeqFFATGPU_Builder<F_t, G_t>& withCBWindows(uint64_t _win_len, uint64_t _slide_len)
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
     *  \param _triggering_delay (in microseconds)
     *  \return the object itself
     */ 
    WinSeqFFATGPU_Builder<F_t, G_t>& withTBWindows(std::chrono::microseconds _win_len, std::chrono::microseconds _slide_len, std::chrono::microseconds _triggering_delay=std::chrono::microseconds::zero())
    {
        win_len = _win_len.count();
        slide_len = _slide_len.count();
        triggering_delay = _triggering_delay.count();
        winType = TB;
        return *this;
    }

    /** 
     *  \brief Method to specify the batch configuration
     *  
     *  \param _batch_len number of windows in a batch
     *  \param _n_thread_block number of threads per block
     *  \return the object itself
     */ 
    WinSeqFFATGPU_Builder<F_t, G_t>& withBatch(size_t _batch_len, size_t _n_thread_block=DEFAULT_CUDA_NUM_THREAD_BLOCK)
    {
        batch_len = _batch_len;
        n_thread_block = _n_thread_block;
        return *this;
    }

    /** 
     *  \brief Method to specify the name of the Win_SeqFFAT_GPU operator
     *  
     *  \param _name string with the name to be given
     *  \return the object itself
     */ 
    WinSeqFFATGPU_Builder<F_t, G_t>& withName(std::string _name)
    {
        name = _name;
        return *this;
    }

    /** 
     *  \brief Method to specify if the FlatFAT_GPU must recomputed from scratch for each batch
     *  
     *  \param _rebuild if true the FlatFAT_GPU structure is rebuilt for each batch (it is updated otherwise)
     *  \return the object itself
     */ 
    WinSeqFFATGPU_Builder<F_t, G_t>& withRebuild(bool _rebuild)
    {
        rebuild = _rebuild;
        return *this;
    }

    /** 
     *  \brief Method to create the Win_SeqFFAT_GPU operator
     *  
     *  \return a pointer to the created Win_SeqFFAT_GPU operator (to be explicitly deallocated/destroyed)
     */ 
    winffat_gpu_t *build_ptr()
    {
        return new winffat_gpu_t(lift_func, comb_func, win_len, slide_len, triggering_delay, winType, batch_len, n_thread_block, rebuild, name);
    }

    /** 
     *  \brief Method to create the Win_SeqFFAT_GPU operator
     *  
     *  \return a unique_ptr to the created Win_SeqFFAT_GPU operator
     */ 
    std::unique_ptr<winffat_gpu_t> build_unique()
    {
        return std::make_unique<winffat_gpu_t>(lift_func, comb_func, win_len, slide_len, triggering_delay, winType, batch_len, n_thread_block, rebuild, name);
    }
};

/** 
 *  \class WinFarm_Builder
 *  
 *  \brief Builder of the Win_Farm operator
 *  
 *  Builder class to ease the creation of the Win_Farm operator.
 */ 
template<typename T>
class WinFarm_Builder
{
private:
    T &input;
    // type of the operator to be created by this builder
    using winfarm_t = std::remove_reference_t<decltype(*get_WF_nested_type(input))>;
    // type of the closing function
    using closing_func_t = std::function<void(RuntimeContext&)>;
    uint64_t win_len = 1;
    uint64_t slide_len = 1;
    uint64_t triggering_delay = 0;
    win_type_t winType = CB;
    size_t pardegree = 1;
    std::string name = "anonymous_wf";
    opt_level_t opt_level = LEVEL2;
    closing_func_t closing_func = [](RuntimeContext &r) -> void { return; };

    // window parameters initialization (input is a Pane_Farm)
    template<typename ...Args>
    void initWindowConf(Pane_Farm<Args...> &_pf)
    {
        win_len = _pf.win_len;
        slide_len = _pf.slide_len;
        triggering_delay = _pf.triggering_delay;
        winType = _pf.winType;
    }

    // window parameters initialization (input is a Win_MapReduce)
    template<typename ...Args>
    void initWindowConf(Win_MapReduce<Args...> &_wm)
    {
        win_len = _wm.win_len;
        slide_len = _wm.slide_len;
        triggering_delay = _wm.triggering_delay;
        winType = _wm.winType;
    }

    // window parameters initialization (input is a function)
    template<typename T2>
    void initWindowConf(T2 &f)
    {
        win_len = 1;
        slide_len = 1;
        triggering_delay = 0;
        winType = CB;
    }

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _input can be either a non-incremental/incremental window processing function or an
     *                already instantiated Pane_Farm or Win_MapReduce operator.
     */ 
    WinFarm_Builder(T &_input): input(_input)
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
    WinFarm_Builder<T>& withCBWindows(uint64_t _win_len, uint64_t _slide_len)
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
     *  \param _triggering_delay (in microseconds)
     *  \return the object itself
     */ 
    WinFarm_Builder<T>& withTBWindows(std::chrono::microseconds _win_len, std::chrono::microseconds _slide_len, std::chrono::microseconds _triggering_delay=std::chrono::microseconds::zero())
    {
        win_len = _win_len.count();
        slide_len = _slide_len.count();
        triggering_delay = _triggering_delay.count();
        winType = TB;
        return *this;
    }

    /** 
     *  \brief Method to specify the parallelism of the Win_Farm operator
     *  
     *  \param _pardegree number of replicas
     *  \return the object itself
     */ 
    WinFarm_Builder<T>& withParallelism(size_t _pardegree)
    {
        pardegree = _pardegree;
        return *this;
    }

    /** 
     *  \brief Method to specify the name of the Win_Farm operator
     *  
     *  \param _name string with the name to be given
     *  \return the object itself
     */ 
    WinFarm_Builder<T>& withName(std::string _name)
    {
        name = _name;
        return *this;
    }

    /** 
     *  \brief Method to specify the optimization level to build the Win_Farm operator
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
     *  \brief Method to specify the closing function used by the operator
     *         This method does not have any effect in case the Win_Farm
     *         replicates complex operators (i.e. Pane_Farm and Win_MapReduce).
     *  
     *  \param _closing_func closing function to be used by the operator
     *  \return the object itself
     */ 
    WinFarm_Builder<T>& withClosingFunction(closing_func_t _closing_func)
    {
        closing_func = _closing_func;
        return *this;
    }

#if __cplusplus >= 201703L
    /** 
     *  \brief Method to create the Win_Farm operator (only C++17)
     *  
     *  \return a copy of the created Win_Farm operator
     */ 
    winfarm_t build()
    {
        return winfarm_t(input, win_len, slide_len, triggering_delay, winType, pardegree, name, closing_func, true, opt_level); // copy elision in C++17
    }
#endif

    /** 
     *  \brief Method to create the Win_Farm operator
     *  
     *  \return a pointer to the created Win_Farm operator (to be explicitly deallocated/destroyed)
     */ 
    winfarm_t *build_ptr()
    {
        return new winfarm_t(input, win_len, slide_len, triggering_delay, winType, pardegree, name, closing_func, true, opt_level);
    }

    /** 
     *  \brief Method to create the Win_Farm operator
     *  
     *  \return a unique_ptr to the created Win_Farm operator
     */ 
    std::unique_ptr<winfarm_t> build_unique()
    {
        return std::make_unique<winfarm_t>(input, win_len, slide_len, triggering_delay, winType, pardegree, name, closing_func, true, opt_level);
    }
};

/** 
 *  \class WinFarmGPU_Builder
 *  
 *  \brief Builder of the Win_Farm_GPU operator
 *  
 *  Builder class to ease the creation of the Win_Farm_GPU operator.
 */ 
template<typename T>
class WinFarmGPU_Builder
{
private:
    T &input;
    // type of the operator to be created by this builder
    using winfarm_gpu_t = std::remove_reference_t<decltype(*get_WF_GPU_nested_type(input))>;
    uint64_t win_len = 1;
    uint64_t slide_len = 1;
    uint64_t triggering_delay = 0;
    win_type_t winType = CB;
    size_t pardegree = 1;
    size_t batch_len = 1;
    size_t n_thread_block = DEFAULT_CUDA_NUM_THREAD_BLOCK;
    std::string name = "anonymous_wf_gpu";
    size_t scratchpad_size = 0;
    opt_level_t opt_level = LEVEL2;

    // window parameters initialization (input is a Pane_Farm_GPU)
    template<typename ...Args>
    void initWindowConf(Pane_Farm_GPU<Args...> &_pf)
    {
        win_len = _pf.win_len;
        slide_len = _pf.slide_len;
        triggering_delay = _pf.triggering_delay;
        winType = _pf.winType;
        batch_len = _pf.batch_len;
        n_thread_block = _pf.n_thread_block;
    }

    // window parameters initialization (input is a Win_MapReduce_GPU)
    template<typename ...Args>
    void initWindowConf(Win_MapReduce_GPU<Args...> &_wm)
    {
        win_len = _wm.win_len;
        slide_len = _wm.slide_len;
        triggering_delay = _wm.triggering_delay;
        winType = _wm.winType;
        batch_len = _wm.batch_len;
        n_thread_block = _wm.n_thread_block;
    }

    // window parameters initialization (input is a function)
    template<typename T2>
    void initWindowConf(T2 &f)
    {
        win_len = 1;
        slide_len = 1;
        triggering_delay = 0;
        winType = CB;
        batch_len = 1;
        n_thread_block = DEFAULT_CUDA_NUM_THREAD_BLOCK;
    }

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _input can be either a non-incremental window processing function (__host__ __device__ function) or an
     *                already instantiated Pane_Farm_GPU or Win_MapReduce_GPU operator.
     */ 
    WinFarmGPU_Builder(T &_input): input(_input) {
        initWindowConf(input);
    }

    /** 
     *  \brief Method to specify the configuration for count-based windows
     *  
     *  \param _win_len window length (in no. of tuples)
     *  \param _slide_len slide length (in no. of tuples)
     *  \return the object itself
     */ 
    WinFarmGPU_Builder<T>& withCBWindows(uint64_t _win_len, uint64_t _slide_len)
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
     *  \param _triggering_delay (in microseconds)
     *  \return the object itself
     */ 
    WinFarmGPU_Builder<T>& withTBWindows(std::chrono::microseconds _win_len, std::chrono::microseconds _slide_len, std::chrono::microseconds _triggering_delay=std::chrono::microseconds::zero())
    {
        win_len = _win_len.count();
        slide_len = _slide_len.count();
        triggering_delay = _triggering_delay.count();
        winType = TB;
        return *this;
    }

    /** 
     *  \brief Method to specify the parallelism of the Win_Farm_GPU operator
     *  
     *  \param _pardegree number of replicas
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
     *  \param _batch_len number of windows in a batch
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
     *  \brief Method to specify the name of the Win_Farm_GPU operator
     *  
     *  \param _name string with the name to be given
     *  \return the object itself
     */ 
    WinFarmGPU_Builder<T>& withName(std::string _name)
    {
        name = _name;
        return *this;
    }

    /** 
     *  \brief Method to specify the size in bytes of the scratchpad memory per CUDA thread
     *  
     *  \param _scratchpad_size size in bytes of the scratchpad area local of a CUDA thread (pre-allocated on the global memory of the GPU)
     *  \return the object itself
     */ 
    WinFarmGPU_Builder<T>& withScratchpad(size_t _scratchpad_size)
    {
        scratchpad_size = _scratchpad_size;
        return *this;
    }

    /** 
     *  \brief Method to specify the optimization level to build the Win_Farm_GPU operator
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
     *  \brief Method to create the Win_Farm_GPU operator
     *  
     *  \return a pointer to the created Win_Farm_GPU operator (to be explicitly deallocated/destroyed)
     */ 
    winfarm_gpu_t *build_ptr()
    {
        return new winfarm_gpu_t(input, win_len, slide_len, triggering_delay, winType, pardegree, batch_len, n_thread_block, name, scratchpad_size, true, opt_level);
    }

    /** 
     *  \brief Method to create the Win_Farm_GPU operator
     *  
     *  \return a unique_ptr to the created Win_Farm_GPU operator
     */ 
    std::unique_ptr<winfarm_gpu_t> build_unique()
    {
        return std::make_unique<winfarm_gpu_t>(input, win_len, slide_len, triggering_delay, winType, pardegree, batch_len, n_thread_block, name, scratchpad_size, true, opt_level);
    }
};

/** 
 *  \class KeyFarm_Builder
 *  
 *  \brief Builder of the Key_Farm operator
 *  
 *  Builder class to ease the creation of the Key_Farm operator.
 */ 
template<typename T>
class KeyFarm_Builder
{
private:
    T &input;
    // type of the operator to be created by this builder
    using keyfarm_t = std::remove_reference_t<decltype(*get_KF_nested_type(input))>;
    // type of the closing function
    using closing_func_t = std::function<void(RuntimeContext&)>;
    // type of the function to map the key hashcode onto an identifier starting from zero to pardegree-1
    using routing_func_t = std::function<size_t(size_t, size_t)>;
    uint64_t win_len = 1;
    uint64_t slide_len = 1;
    uint64_t triggering_delay = 0;
    win_type_t winType = CB;
    size_t pardegree = 1;
    std::string name = "anonymous_kf";
    routing_func_t routing_func = [](size_t k, size_t n) { return k%n; };
    opt_level_t opt_level = LEVEL2;
    closing_func_t closing_func = [](RuntimeContext &r) -> void { return; };

    // window parameters initialization (input is a Pane_Farm)
    template<typename ...Args>
    void initWindowConf(Pane_Farm<Args...> &_pf)
    {
        win_len = _pf.win_len;
        slide_len = _pf.slide_len;
        triggering_delay = _pf.triggering_delay;
        winType = _pf.winType;
    }

    // window parameters initialization (input is a Win_MapReduce)
    template<typename ...Args>
    void initWindowConf(Win_MapReduce<Args...> &_wm)
    {
        win_len = _wm.win_len;
        slide_len = _wm.slide_len;
        triggering_delay = _wm.triggering_delay;
        winType = _wm.winType;
    }

    // window parameters initialization (input is a function)
    template<typename T2>
    void initWindowConf(T2 &f)
    {
        win_len = 1;
        slide_len = 1;
        triggering_delay = 0;
        winType = CB;
    }

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _input can be either a non-incremental/incremental window processing function or an
     *                already instantiated Pane_Farm or Win_MapReduce operator.
     */ 
    KeyFarm_Builder(T &_input): input(_input)
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
    KeyFarm_Builder<T>& withCBWindows(uint64_t _win_len, uint64_t _slide_len)
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
     *  \param _triggering_delay (in microseconds)
     *  \return the object itself
     */ 
    KeyFarm_Builder<T>& withTBWindows(std::chrono::microseconds _win_len, std::chrono::microseconds _slide_len, std::chrono::microseconds _triggering_delay=std::chrono::microseconds::zero())
    {
        win_len = _win_len.count();
        slide_len = _slide_len.count();
        triggering_delay = _triggering_delay.count();
        winType = TB;
        return *this;
    }

    /** 
     *  \brief Method to specify the parallelism of the Key_Farm operator
     *  
     *  \param _pardegree number of replicas
     *  \return the object itself
     */ 
    KeyFarm_Builder<T>& withParallelism(size_t _pardegree)
    {
        pardegree = _pardegree;
        return *this;
    }

    /** 
     *  \brief Method to specify the name of the Key_Farm operator
     *  
     *  \param _name string with the name to be given
     *  \return the object itself
     */ 
    KeyFarm_Builder<T>& withName(std::string _name)
    {
        name = _name;
        return *this;
    }

    /** 
     *  \brief Method to specify the optimization level to build the Key_Farm operator
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
     *  \brief Method to specify the closing function used by the operator
     *  
     *  \param _closing_func closing function to be used by the operator
     *  \return the object itself
     */ 
    KeyFarm_Builder<T>& withClosingFunction(closing_func_t _closing_func)
    {
        closing_func = _closing_func;
        return *this;
    }

#if __cplusplus >= 201703L
    /** 
     *  \brief Method to create the Key_Farm operator (only C++17)
     *  
     *  \return a copy of the created Key_Farm operator
     */ 
    keyfarm_t build()
    {
        return keyfarm_t(input, win_len, slide_len, triggering_delay, winType, pardegree, name, closing_func, routing_func, opt_level); // copy elision in C++17
    }
#endif

    /** 
     *  \brief Method to create the Key_Farm operator
     *  
     *  \return a pointer to the created Key_Farm operator (to be explicitly deallocated/destroyed)
     */ 
    keyfarm_t *build_ptr()
    {
        return new keyfarm_t(input, win_len, slide_len, triggering_delay, winType, pardegree, name, closing_func, routing_func, opt_level);
    }

    /** 
     *  \brief Method to create the Key_Farm operator
     *  
     *  \return a unique_ptr to the created Key_Farm operator
     */ 
    std::unique_ptr<keyfarm_t> build_unique()
    {
        return std::make_unique<keyfarm_t>(input, win_len, slide_len, triggering_delay, winType, pardegree, name, closing_func, routing_func, opt_level);
    }
};

/** 
 *  \class KeyFFAT_Builder
 *  
 *  \brief Builder of the Key_FFAT operator
 *  
 *  Builder class to ease the creation of the Key_FFAT operator.
 */ 
template<typename F_t, typename G_t>
class KeyFFAT_Builder
{
private:
    F_t lift_func;
    G_t comb_func;
    // type of the operator to be created by this builder
    using keyffat_t = Key_FFAT<decltype(get_tuple_t(lift_func)),
                             decltype(get_result_t(lift_func))>;
    // type of the closing function
    using closing_func_t = std::function<void(RuntimeContext&)>;
    // type of the function to map the key hashcode onto an identifier starting from zero to pardegree-1
    using routing_func_t = std::function<size_t(size_t, size_t)>;
    uint64_t win_len = 1;
    uint64_t slide_len = 1;
    uint64_t triggering_delay = 0;
    win_type_t winType = CB;
    size_t pardegree = 1;
    std::string name = "anonymous_kff";
    routing_func_t routing_func = [](size_t k, size_t n) { return k%n; };
    closing_func_t closing_func = [](RuntimeContext &r) -> void { return; };

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _lift_func the lift function to translate a tuple into a result
     *  \param _comb_func the combine function to combine two results into a result
     */ 
    KeyFFAT_Builder(F_t _lift_func, G_t _comb_func): lift_func(_lift_func), comb_func(_comb_func) {}

    /** 
     *  \brief Method to specify the configuration for count-based windows
     *  
     *  \param _win_len window length (in no. of tuples)
     *  \param _slide_len slide length (in no. of tuples)
     *  \return the object itself
     */ 
    KeyFFAT_Builder<F_t, G_t>& withCBWindows(uint64_t _win_len, uint64_t _slide_len)
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
     *  \param _triggering_delay (in microseconds)
     *  \return the object itself
     */ 
    KeyFFAT_Builder<F_t, G_t>& withTBWindows(std::chrono::microseconds _win_len, std::chrono::microseconds _slide_len, std::chrono::microseconds _triggering_delay=std::chrono::microseconds::zero())
    {
        win_len = _win_len.count();
        slide_len = _slide_len.count();
        triggering_delay = _triggering_delay.count();
        winType = TB;
        return *this;
    }

    /** 
     *  \brief Method to specify the parallelism of the Key_FFAT operator
     *  
     *  \param _pardegree number of replicas
     *  \return the object itself
     */ 
    KeyFFAT_Builder<F_t, G_t>& withParallelism(size_t _pardegree)
    {
        pardegree = _pardegree;
        return *this;
    }

    /** 
     *  \brief Method to specify the name of the Key_FFAT operator
     *  
     *  \param _name string with the name to be given
     *  \return the object itself
     */ 
    KeyFFAT_Builder<F_t, G_t>& withName(std::string _name)
    {
        name = _name;
        return *this;
    }

    /** 
     *  \brief Method to specify the closing function used by the operator
     *  
     *  \param _closing_func closing function to be used by the operator
     *  \return the object itself
     */ 
    KeyFFAT_Builder<F_t, G_t>& withClosingFunction(closing_func_t _closing_func)
    {
        closing_func = _closing_func;
        return *this;
    }

#if __cplusplus >= 201703L
    /** 
     *  \brief Method to create the Key_FFAT operator (only C++17)
     *  
     *  \return a copy of the created Key_Farm operator
     */ 
    keyffat_t build()
    {
        return keyffat_t(lift_func, comb_func, win_len, slide_len, triggering_delay, winType, pardegree, name, closing_func, routing_func); // copy elision in C++17
    }
#endif

    /** 
     *  \brief Method to create the Key_FFAT operator
     *  
     *  \return a pointer to the created Key_FFAT operator (to be explicitly deallocated/destroyed)
     */ 
    keyffat_t *build_ptr()
    {
        return new keyffat_t(lift_func, comb_func, win_len, slide_len, triggering_delay, winType, pardegree, name, closing_func, routing_func);
    }

    /** 
     *  \brief Method to create the Key_FFAT operator
     *  
     *  \return a unique_ptr to the created Key_FFAT operator
     */ 
    std::unique_ptr<keyffat_t> build_unique()
    {
        return std::make_unique<keyffat_t>(lift_func, comb_func, win_len, slide_len, triggering_delay, winType, pardegree, name, closing_func, routing_func);
    }
};

/** 
 *  \class KeyFarmGPU_Builder
 *  
 *  \brief Builder of the Key_Farm_GPU operator
 *  
 *  Builder class to ease the creation of the Key_Farm_GPU operator.
 */ 
template<typename T>
class KeyFarmGPU_Builder
{
private:
    T &input;
    // type of the function to map the key hashcode onto an identifier starting from zero to pardegree-1
    using routing_func_t = std::function<size_t(size_t, size_t)>;
    // type of the operator to be created by this builder
    using keyfarm_gpu_t = std::remove_reference_t<decltype(*get_KF_GPU_nested_type(input))>;
    uint64_t win_len = 1;
    uint64_t slide_len = 1;
    uint64_t triggering_delay = 0;
    win_type_t winType = CB;
    size_t pardegree = 1;
    size_t batch_len = 1;
    size_t n_thread_block = DEFAULT_CUDA_NUM_THREAD_BLOCK;
    std::string name = "anonymous_wf_gpu";
    size_t scratchpad_size = 0;
    routing_func_t routing_func = [](size_t k, size_t n) { return k%n; };
    opt_level_t opt_level = LEVEL2;

    // window parameters initialization (input is a Pane_Farm_GPU)
    template<typename ...Args>
    void initWindowConf(Pane_Farm_GPU<Args...> &_pf)
    {
        win_len = _pf.win_len;
        slide_len = _pf.slide_len;
        triggering_delay = _pf.triggering_delay;
        winType = _pf.winType;
        batch_len = _pf.batch_len;
        n_thread_block = _pf.n_thread_block;
    }

    // window parameters initialization (input is a Win_MapReduce_GPU)
    template<typename ...Args>
    void initWindowConf(Win_MapReduce_GPU<Args...> &_wm)
    {
        win_len = _wm.win_len;
        slide_len = _wm.slide_len;
        triggering_delay = _wm.triggering_delay;
        winType = _wm.winType;
        batch_len = _wm.batch_len;
        n_thread_block = _wm.n_thread_block;
    }

    // window parameters initialization (input is a function)
    template<typename T2>
    void initWindowConf(T2 &f)
    {
        win_len = 1;
        slide_len = 1;
        triggering_delay = 0;
        winType = CB;
        batch_len = 1;
        n_thread_block = DEFAULT_CUDA_NUM_THREAD_BLOCK;
    }

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _input can be either a non-incremental window processing function (__host__ __device__ function) or an
     *                already instantiated Pane_Farm_GPU or Win_MapReduce_GPU operator.
     */ 
    KeyFarmGPU_Builder(T &_input): input(_input) {
        initWindowConf(input);
    }

    /** 
     *  \brief Method to specify the configuration for count-based windows
     *  
     *  \param _win_len window length (in no. of tuples)
     *  \param _slide_len slide length (in no. of tuples)
     *  \return the object itself
     */ 
    KeyFarmGPU_Builder<T>& withCBWindows(uint64_t _win_len, uint64_t _slide_len)
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
     *  \param _triggering_delay (in microseconds)
     *  \return the object itself
     */ 
    KeyFarmGPU_Builder<T>& withTBWindows(std::chrono::microseconds _win_len, std::chrono::microseconds _slide_len, std::chrono::microseconds _triggering_delay=std::chrono::microseconds::zero())
    {
        win_len = _win_len.count();
        slide_len = _slide_len.count();
        triggering_delay = _triggering_delay.count();
        winType = TB;
        return *this;
    }

    /** 
     *  \brief Method to specify the parallelism of the Key_Farm_GPU operator
     *  
     *  \param _pardegree number of replicas
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
     *  \param _batch_len number of windows in a batch
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
     *  \brief Method to specify the name of the Key_Farm_GPU operator
     *  
     *  \param _name string with the name to be given
     *  \return the object itself
     */ 
    KeyFarmGPU_Builder<T>& withName(std::string _name)
    {
        name = _name;
        return *this;
    }

    /** 
     *  \brief Method to specify the size in bytes of the scratchpad memory per CUDA thread
     *  
     *  \param _scratchpad_size size in bytes of the scratchpad area local of a CUDA thread (pre-allocated on the global memory of the GPU)
     *  \return the object itself
     */ 
    KeyFarmGPU_Builder<T>& withScratchpad(size_t _scratchpad_size)
    {
        scratchpad_size = _scratchpad_size;
        return *this;
    }

    /** 
     *  \brief Method to specify the optimization level to build the Key_Farm_GPU operator
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
     *  \brief Method to create the Key_Farm_GPU operator
     *  
     *  \return a pointer to the created Key_Farm_GPU operator (to be explicitly deallocated/destroyed)
     */ 
    keyfarm_gpu_t *build_ptr()
    {
        return new keyfarm_gpu_t(input, win_len, slide_len, triggering_delay, winType, pardegree, batch_len, n_thread_block, name, scratchpad_size, routing_func, opt_level);
    }

    /** 
     *  \brief Method to create the Key_Farm_GPU operator
     *  
     *  \return a unique_ptr to the created Key_Farm_GPU operator
     */ 
    std::unique_ptr<keyfarm_gpu_t> build_unique()
    {
        return std::make_unique<keyfarm_gpu_t>(input, win_len, slide_len, triggering_delay, winType, pardegree, batch_len, n_thread_block, name, scratchpad_size, routing_func, opt_level);
    }
};

/** 
 *  \class KeyFFATGPU_Builder
 *  
 *  \brief Builder of the Key_FFAT_GPU operator
 *  
 *  Builder class to ease the creation of the Key_FFFAT_GPU operator.
 */ 
template<typename F_t, typename G_t>
class KeyFFATGPU_Builder
{
private:
    F_t lift_func;
    G_t comb_func;
    // type of the function to map the key hashcode onto an identifier starting from zero to pardegree-1
    using routing_func_t = std::function<size_t(size_t, size_t)>;
    // type of the operator to be created by this builder
    using keyffat_gpu_t = Key_FFAT_GPU<decltype(get_tuple_t(lift_func)),
                                       decltype(get_result_t(lift_func)),
                                       decltype(comb_func)>;
    uint64_t win_len = 1;
    uint64_t slide_len = 1;
    uint64_t triggering_delay = 0;
    win_type_t winType = CB;
    size_t pardegree = 1;
    size_t batch_len = 1;
    size_t n_thread_block = DEFAULT_CUDA_NUM_THREAD_BLOCK;
    bool rebuild = false;
    std::string name = "anonymous_kff_gpu";
    routing_func_t routing_func = [](size_t k, size_t n) { return k%n; };

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _lift_func the lift function to translate a tuple into a result
     *  \param _comb_func the combine function to combine two results into a result (__host__ __device__ function)
     */ 
    KeyFFATGPU_Builder(F_t _lift_func, G_t _comb_func): lift_func(_lift_func), comb_func(_comb_func) {}

    /** 
     *  \brief Method to specify the configuration for count-based windows
     *  
     *  \param _win_len window length (in no. of tuples)
     *  \param _slide_len slide length (in no. of tuples)
     *  \return the object itself
     */ 
    KeyFFATGPU_Builder<F_t, G_t>& withCBWindows(uint64_t _win_len, uint64_t _slide_len)
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
     *  \param _triggering_delay (in microseconds)
     *  \return the object itself
     */ 
    KeyFFATGPU_Builder<F_t, G_t>& withTBWindows(std::chrono::microseconds _win_len, std::chrono::microseconds _slide_len, std::chrono::microseconds _triggering_delay=std::chrono::microseconds::zero())
    {
        win_len = _win_len.count();
        slide_len = _slide_len.count();
        triggering_delay = _triggering_delay.count();
        winType = TB;
        return *this;
    }

    /** 
     *  \brief Method to specify the parallelism of the Key_FFAT_GPU operator
     *  
     *  \param _pardegree number of replicas
     *  \return the object itself
     */ 
    KeyFFATGPU_Builder<F_t, G_t>& withParallelism(size_t _pardegree)
    {
        pardegree = _pardegree;
        return *this;
    }

    /** 
     *  \brief Method to specify the batch configuration
     *  
     *  \param _batch_len number of windows in a batch
     *  \param _n_thread_block number of threads per block
     *  \return the object itself
     */ 
    KeyFFATGPU_Builder<F_t, G_t>& withBatch(size_t _batch_len, size_t _n_thread_block=DEFAULT_CUDA_NUM_THREAD_BLOCK)
    {
        batch_len = _batch_len;
        n_thread_block = _n_thread_block;
        return *this;
    }

    /** 
     *  \brief Method to specify the name of the Key_FFAT_GPU operator
     *  
     *  \param _name string with the name to be given
     *  \return the object itself
     */ 
    KeyFFATGPU_Builder<F_t, G_t>& withName(std::string _name)
    {
        name = _name;
        return *this;
    }

    /** 
     *  \brief Method to specify if the FlatFAT_GPU must recomputed from scratch for each batch
     *  
     *  \param _rebuild if true the FlatFAT_GPU structure is rebuilt for each batch (it is updated otherwise)
     *  \return the object itself
     */ 
    KeyFFATGPU_Builder<F_t, G_t>& withRebuild(bool _rebuild)
    {
        rebuild = _rebuild;
        return *this;
    }

    /** 
     *  \brief Method to create the Key_FFAT_GPU operator
     *  
     *  \return a pointer to the created Key_FFAT_GPU operator (to be explicitly deallocated/destroyed)
     */ 
    keyffat_gpu_t *build_ptr()
    {
        return new keyffat_gpu_t(lift_func, comb_func, win_len, slide_len, triggering_delay, winType, pardegree, batch_len, n_thread_block, rebuild, name, routing_func);
    }

    /** 
     *  \brief Method to create the Key_FFAT_GPU operator
     *  
     *  \return a unique_ptr to the created Key_FFAT_GPU operator
     */ 
    std::unique_ptr<keyffat_gpu_t> build_unique()
    {
        return std::make_unique<keyffat_gpu_t>(lift_func, comb_func, win_len, slide_len, triggering_delay, winType, pardegree, batch_len, n_thread_block, rebuild, name, routing_func);
    }
};


/** 
 *  \class PaneFarm_Builder
 *  
 *  \brief Builder of the Pane_Farm operator
 *  
 *  Builder class to ease the creation of the Pane_Farm operator.
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
    using closing_func_t = std::function<void(RuntimeContext&)>;
    uint64_t win_len = 1;
    uint64_t slide_len = 1;
    uint64_t triggering_delay = 0;
    win_type_t winType = CB;
    size_t plq_degree = 1;
    size_t wlq_degree = 1;
    std::string name = "anonymous_pf";
    opt_level_t opt_level = LEVEL0;
    closing_func_t closing_func = [](RuntimeContext &r) -> void { return; };

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _func_F the non-incremental/incremental pane procesing function (PLQ)
     *  \param _func_G the non-incremental/incremental window processing function (PLQ)
     */ 
    PaneFarm_Builder(F_t _func_F, G_t _func_G): func_F(_func_F), func_G(_func_G) {}

    /** 
     *  \brief Method to specify the configuration for count-based windows
     *  
     *  \param _win_len window length (in no. of tuples)
     *  \param _slide_len slide length (in no. of tuples)
     *  \return the object itself
     */ 
    PaneFarm_Builder<F_t, G_t>& withCBWindows(uint64_t _win_len, uint64_t _slide_len)
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
     *  \param _triggering_delay (in microseconds)
     *  \return the object itself
     */ 
    PaneFarm_Builder<F_t, G_t>& withTBWindows(std::chrono::microseconds _win_len, std::chrono::microseconds _slide_len, std::chrono::microseconds _triggering_delay=std::chrono::microseconds::zero())
    {
        win_len = _win_len.count();
        slide_len = _slide_len.count();
        triggering_delay = _triggering_delay.count();
        winType = TB;
        return *this;
    }

    /** 
     *  \brief Method to specify the parallelism configuration within the Pane_Farm operator
     *  
     *  \param _plq_degree number replicas in the PLQ stage
     *  \param _wlq_degree number replicas in the WLQ stage
     *  \return the object itself
     */ 
    PaneFarm_Builder<F_t, G_t>& withParallelism(size_t _plq_degree, size_t _wlq_degree)
    {
        plq_degree = _plq_degree;
        wlq_degree = _wlq_degree;
        return *this;
    }

    /** 
     *  \brief Method to specify the name of the Pane_Farm operator
     *  
     *  \param _name string with the name to be given
     *  \return the object itself
     */ 
    PaneFarm_Builder<F_t, G_t>& withName(std::string _name)
    {
        name = _name;
        return *this;
    }

    /** 
     *  \brief Method to specify the optimization level to build the Pane_Farm operator
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
     *  \brief Method to prepare the operator for Nesting with Key_Farm or Win_Farm
     *  
     *  \return the object itself
     */ 
    PaneFarm_Builder<F_t, G_t>& prepare4Nesting()
    {
        opt_level = LEVEL2;
        return *this;
    }

    /** 
     *  \brief Method to specify the closing function used by the operator
     *  
     *  \param _closing_func closing function to be used by the operator
     *  \return the object itself
     */ 
    PaneFarm_Builder<F_t, G_t>& withClosingFunction(closing_func_t _closing_func)
    {
        closing_func = _closing_func;
        return *this;
    }

#if __cplusplus >= 201703L
    /** 
     *  \brief Method to create the Pane_Farm operator (only C++17)
     *  
     *  \return a copy of the created Pane_Farm operator
     */ 
    panefarm_t build()
    {
        return panefarm_t(func_F, func_G, win_len, slide_len, triggering_delay, winType, plq_degree, wlq_degree, name, closing_func, true, opt_level); // copy elision in C++17
    }
#endif

    /** 
     *  \brief Method to create the Pane_Farm operator
     *  
     *  \return a pointer to the created Pane_Farm operator (to be explicitly deallocated/destroyed)
     */ 
    panefarm_t *build_ptr()
    {
        return new panefarm_t(func_F, func_G, win_len, slide_len, triggering_delay, winType, plq_degree, wlq_degree, name, closing_func, true, opt_level);
    }

    /** 
     *  \brief Method to create the Pane_Farm operator
     *  
     *  \return a unique_ptr to the created Pane_Farm operator
     */ 
    std::unique_ptr<panefarm_t> build_unique()
    {
        return std::make_unique<panefarm_t>(func_F, func_G, win_len, slide_len, triggering_delay, winType, plq_degree, wlq_degree, name, closing_func, true, opt_level);
    }
};

/** 
 *  \class PaneFarmGPU_Builder
 *  
 *  \brief Builder of the Pane_Farm_GPU operator
 *  
 *  Builder class to ease the creation of the Pane_Farm_GPU operator.
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
    uint64_t triggering_delay = 0;
    win_type_t winType = CB;
    size_t plq_degree = 1;
    size_t wlq_degree = 1;
    size_t batch_len = 1;
    size_t n_thread_block = DEFAULT_CUDA_NUM_THREAD_BLOCK;
    std::string name = "anonymous_pf_gpu";
    size_t scratchpad_size = 0;
    opt_level_t opt_level = LEVEL0;

public:
    /** 
     *  \brief Constructor (only one of the two functions can be __host__ __device__)
     *  
     *  \param _func_F the pane procesing function (PLQ)
     *  \param _func_G the window processing function (PLQ)
     *  \note
     *  The __host__ __device__ function must be passed through a callable object (e.g., lambda, functor)
     */ 
    PaneFarmGPU_Builder(F_t _func_F, G_t _func_G): func_F(_func_F), func_G(_func_G) {}

    /** 
     *  \brief Method to specify the configuration for count-based windows
     *  
     *  \param _win_len window length (in no. of tuples)
     *  \param _slide_len slide length (in no. of tuples)
     *  \return the object itself
     */ 
    PaneFarmGPU_Builder<F_t, G_t>& withCBWindows(uint64_t _win_len, uint64_t _slide_len)
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
     *  \param _triggering_delay (in microseconds)
     *  \return the object itself
     */ 
    PaneFarmGPU_Builder<F_t, G_t>& withTBWindows(std::chrono::microseconds _win_len, std::chrono::microseconds _slide_len, std::chrono::microseconds _triggering_delay=std::chrono::microseconds::zero())
    {
        win_len = _win_len.count();
        slide_len = _slide_len.count();
        triggering_delay = _triggering_delay.count();
        winType = TB;
        return *this;
    }

    /** 
     *  \brief Method to specify the parallelism configuration within the Pane_Farm_GPU operator
     *  
     *  \param _plq_degree number of replicas in the PLQ stage
     *  \param _wlq_degree number of replicas in the WLQ stage
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
     *  \param _batch_len number of panes/windows in a batch
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
     *  \brief Method to specify the name of the Pane_Farm_GPU operator
     *  
     *  \param _name string with the name to be given
     *  \return the object itself
     */ 
    PaneFarmGPU_Builder<F_t, G_t>& withName(std::string _name)
    {
        name = _name;
        return *this;
    }

    /** 
     *  \brief Method to specify the size in bytes of the scratchpad memory per CUDA thread
     *  
     *  \param _scratchpad_size size in bytes of the scratchpad area local of a CUDA thread (pre-allocated on the global memory of the GPU)
     *  \return the object itself
     */ 
    PaneFarmGPU_Builder<F_t, G_t>& withScratchpad(size_t _scratchpad_size)
    {
        scratchpad_size = _scratchpad_size;
        return *this;
    }

    /** 
     *  \brief Method to specify the optimization level to build the Pane_Farm_GPU operator
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
     *  \brief Method to prepare the operator for Nesting with Key_Farm_GPU or Win_Farm_GPU
     *  
     *  \return the object itself
     */ 
    PaneFarmGPU_Builder<F_t, G_t>& prepare4Nesting()
    {
        opt_level = LEVEL2;
        return *this;
    }

    /** 
     *  \brief Method to create the Pane_Farm_GPU operator
     *  
     *  \return a pointer to the created Pane_Farm_GPU operator (to be explicitly deallocated/destroyed)
     */ 
    panefarm_gpu_t *build_ptr()
    {
        return new panefarm_gpu_t(func_F, func_G, win_len, slide_len, triggering_delay, winType, plq_degree, wlq_degree, batch_len, n_thread_block, name, scratchpad_size, true, opt_level);
    }

    /** 
     *  \brief Method to create the Pane_Farm_GPU operator
     *  
     *  \return a unique_ptr to the created Pane_Farm_GPU operator
     */ 
    std::unique_ptr<panefarm_gpu_t> build_unique()
    {
        return std::make_unique<panefarm_gpu_t>(func_F, func_G, win_len, slide_len, triggering_delay, winType, plq_degree, wlq_degree, batch_len, n_thread_block, name, scratchpad_size, true, opt_level);
    }
};

/** 
 *  \class WinMapReduce_Builder
 *  
 *  \brief Builder of the Win_MapReduce operator
 *  
 *  Builder class to ease the creation of the Win_MapReduce operator.
 */ 
template<typename F_t, typename G_t>
class WinMapReduce_Builder
{
private:
    F_t func_F;
    G_t func_G;
    // type of the operator to be created by this builder
    using winmapreduce_t = Win_MapReduce<decltype(get_tuple_t(func_F)),
                                         decltype(get_result_t(func_F))>;
    // type of the closing function
    using closing_func_t = std::function<void(RuntimeContext&)>;
    uint64_t win_len = 1;
    uint64_t slide_len = 1;
    uint64_t triggering_delay = 0;
    win_type_t winType = CB;
    size_t map_degree = 2;
    size_t reduce_degree = 1;
    std::string name = "anonymous_wmr";
    opt_level_t opt_level = LEVEL0;
    closing_func_t closing_func = [](RuntimeContext &r) -> void { return; };

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _func_F the non-incremental/incremental window map function (MAP)
     *  \param _func_G the non-incremental/incremental window reduce function (REDUCE)
     */ 
    WinMapReduce_Builder(F_t _func_F, G_t _func_G): func_F(_func_F), func_G(_func_G) {}

    /** 
     *  \brief Method to specify the configuration for count-based windows
     *  
     *  \param _win_len window length (in no. of tuples)
     *  \param _slide_len slide length (in no. of tuples)
     *  \return the object itself
     */ 
    WinMapReduce_Builder<F_t, G_t>& withCBWindows(uint64_t _win_len, uint64_t _slide_len)
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
     *  \param _triggering_delay (in microseconds)
     *  \return the object itself
     */ 
    WinMapReduce_Builder<F_t, G_t>& withTBWindows(std::chrono::microseconds _win_len, std::chrono::microseconds _slide_len, std::chrono::microseconds _triggering_delay=std::chrono::microseconds::zero())
    {
        win_len = _win_len.count();
        slide_len = _slide_len.count();
        triggering_delay = _triggering_delay.count();
        winType = TB;
        return *this;
    }

    /** 
     *  \brief Method to specify the parallelism configuration within the Win_MapReduce operator
     *  
     *  \param _map_degree number of replicas in the MAP stage
     *  \param _reduce_degree number of replicas in the REDUCE stage
     *  \return the object itself
     */ 
    WinMapReduce_Builder<F_t, G_t>& withParallelism(size_t _map_degree, size_t _reduce_degree)
    {
        map_degree = _map_degree;
        reduce_degree = _reduce_degree;
        return *this;
    }

    /** 
     *  \brief Method to specify the name of the Win_MapReduce operator
     *  
     *  \param _name string with the name to be given
     *  \return the object itself
     */ 
    WinMapReduce_Builder<F_t, G_t>& withName(std::string _name)
    {
        name = _name;
        return *this;
    }

    /** 
     *  \brief Method to specify the optimization level to build the Win_MapReduce operator
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
     *  \brief Method to prepare the operator for Nesting with Key_Farm or Win_Farm
     *  
     *  \return the object itself
     */ 
    WinMapReduce_Builder<F_t, G_t>& prepare4Nesting()
    {
        opt_level = LEVEL2;
        return *this;
    }

    /** 
     *  \brief Method to specify the closing function used by the operator
     *  
     *  \param _closing_func closing function to be used by the operator
     *  \return the object itself
     */ 
    WinMapReduce_Builder<F_t, G_t>& withClosingFunction(closing_func_t _closing_func)
    {
        closing_func = _closing_func;
        return *this;
    }

#if __cplusplus >= 201703L
    /** 
     *  \brief Method to create the Win_MapReduce operator (only C++17)
     *  
     *  \return a copy of the created Win_MapReduce operator
     */ 
    winmapreduce_t build()
    {
        return winmapreduce_t(func_F, func_G, win_len, slide_len, triggering_delay, winType, map_degree, reduce_degree, name, closing_func, true, opt_level); // copy elision in C++17
    }
#endif

    /** 
     *  \brief Method to create the Win_MapReduce operator
     *  
     *  \return a pointer to the created Win_MapReduce operator (to be explicitly deallocated/destroyed)
     */ 
    winmapreduce_t *build_ptr()
    {
        return new winmapreduce_t(func_F, func_G, win_len, slide_len, triggering_delay, winType, map_degree, reduce_degree, name, closing_func, true, opt_level);
    }

    /** 
     *  \brief Method to create the Win_MapReduce operator
     *  
     *  \return a unique_ptr to the created Win_MapReduce operator
     */ 
    std::unique_ptr<winmapreduce_t> build_unique()
    {
        return std::make_unique<winmapreduce_t>(func_F, func_G, win_len, slide_len, triggering_delay, winType, map_degree, reduce_degree, name, closing_func, true, opt_level);
    }
};

/** 
 *  \class WinMapReduceGPU_Builder
 *  
 *  \brief Builder of the Win_MapReduce_GPU operator
 *  
 *  Builder class to ease the creation of the Win_MapReduce_GPU operator.
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
    uint64_t triggering_delay = 0;
    win_type_t winType = CB;
    size_t map_degree = 2;
    size_t reduce_degree = 1;
    size_t batch_len = 1;
    size_t n_thread_block = DEFAULT_CUDA_NUM_THREAD_BLOCK;
    std::string name = "anonymous_wmw_gpu";
    size_t scratchpad_size = 0;
    opt_level_t opt_level = LEVEL0;

public:
    /** 
     *  \brief Constructor (only one of the two functions can be __host__ __device__)
     *  
     *  \param _func_F the window map function (MAP)
     *  \param _func_G the window reduce function (REDUCE)
     *  \note
     *  The __host__ __device__ function must be passed through a callable object (e.g., lambda, functor)
     */ 
    WinMapReduceGPU_Builder(F_t _func_F, G_t _func_G): func_F(_func_F), func_G(_func_G) {}

    /** 
     *  \brief Method to specify the configuration for count-based windows
     *  
     *  \param _win_len window length (in no. of tuples)
     *  \param _slide_len slide length (in no. of tuples)
     *  \return the object itself
     */ 
    WinMapReduceGPU_Builder<F_t, G_t>& withCBWindows(uint64_t _win_len, uint64_t _slide_len)
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
     *  \param _triggering_delay (in microseconds)
     *  \return the object itself
     */ 
    WinMapReduceGPU_Builder<F_t, G_t>& withTBWindows(std::chrono::microseconds _win_len, std::chrono::microseconds _slide_len, std::chrono::microseconds _triggering_delay=std::chrono::microseconds::zero())
    {
        win_len = _win_len.count();
        slide_len = _slide_len.count();
        triggering_delay = _triggering_delay.count();
        winType = TB;
        return *this;
    }

    /** 
     *  \brief Method to specify the parallelism configuration within the Win_MapReduce_GPU operator
     *  
     *  \param _map_degree number of replicas in the MAP stage
     *  \param _reduce_degree number of replicas in the REDUCE stage
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
     *  \param _batch_len number of partitions/windows in a batch
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
     *  \brief Method to specify the name of the Win_MapReduce_GPU operator
     *  
     *  \param _name string with the name to be given
     *  \return the object itself
     */ 
    WinMapReduceGPU_Builder<F_t, G_t>& withName(std::string _name)
    {
        name = _name;
        return *this;
    }

    /** 
     *  \brief Method to specify the size in bytes of the scratchpad memory per CUDA thread
     *  
     *  \param _scratchpad_size size in bytes of the scratchpad area local of a CUDA thread (pre-allocated on the global memory of the GPU)
     *  \return the object itself
     */ 
    WinMapReduceGPU_Builder<F_t, G_t>& withScratchpad(size_t _scratchpad_size)
    {
        scratchpad_size = _scratchpad_size;
        return *this;
    }

    /** 
     *  \brief Method to specify the optimization level to build the Win_MapReduce_GPU operator
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
     *  \brief Method to prepare the operator for Nesting with Key_Farm_GPU or Win_Farm_GPU
     *  
     *  \return the object itself
     */ 
    WinMapReduceGPU_Builder<F_t, G_t>& prepare4Nesting()
    {
        opt_level = LEVEL2;
        return *this;
    }

    /** 
     *  \brief Method to create the Pane_Farm_GPU operator
     *  
     *  \return a pointer to the created Pane_Farm_GPU operator (to be explicitly deallocated/destroyed)
     */ 
    winmapreduce_gpu_t *build_ptr()
    {
        return new winmapreduce_gpu_t(func_F, func_G, win_len, slide_len, triggering_delay, winType, map_degree, reduce_degree, batch_len, n_thread_block, name, scratchpad_size, true, opt_level);
    }

    /** 
     *  \brief Method to create the Pane_Farm_GPU operator
     *  
     *  \return a unique_ptr to the created Pane_Farm_GPU operator
     */ 
    std::unique_ptr<winmapreduce_gpu_t> build_unique()
    {
        return std::make_unique<winmapreduce_gpu_t>(func_F, func_G, win_len, slide_len, triggering_delay, winType, map_degree, reduce_degree, batch_len, n_thread_block, name, scratchpad_size, true, opt_level);
    }
};

/** 
 *  \class Sink_Builder
 *  
 *  \brief Builder of the Sink operator
 *  
 *  Builder class to ease the creation of the Sink operator.
 */ 
template<typename F_t>
class Sink_Builder
{
private:
    F_t func;
    // type of the operator to be created by this builder
    using sink_t = Sink<decltype(get_tuple_t(func))>;
    // type of the closing function
    using closing_func_t = std::function<void(RuntimeContext&)>;
    // type of the function to map the key hashcode onto an identifier starting from zero to pardegree-1
    using routing_func_t = std::function<size_t(size_t, size_t)>;
    uint64_t pardegree = 1;
    std::string name = "anonymous_sink";
    bool isKeyed = false;
    closing_func_t closing_func = [](RuntimeContext &r) -> void { return; };
    routing_func_t routing_func = [](size_t k, size_t n) { return k%n; };

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _func function to absorb the stream elements
     */ 
    Sink_Builder(F_t _func): func(_func) {}

    /** 
     *  \brief Method to specify the name of the Sink operator
     *  
     *  \param _name string with the name to be given
     *  \return the object itself
     */ 
    Sink_Builder<F_t>& withName(std::string _name)
    {
        name = _name;
        return *this;
    }

    /** 
     *  \brief Method to specify the parallelism of the Sink operator
     *  
     *  \param _pardegree number of sink replicas
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
     *  \return the object itself
     */ 
    Sink_Builder<F_t>& enable_KeyBy()
    {
        isKeyed = true;
        return *this;
    }

    /** 
     *  \brief Method to specify the closing function used by the operator
     *  
     *  \param _closing_func closing function to be used by the operator
     *  \return the object itself
     */ 
    Sink_Builder<F_t>& withClosingFunction(closing_func_t _closing_func)
    {
        closing_func = _closing_func;
        return *this;
    }

#if __cplusplus >= 201703L
    /** 
     *  \brief Method to create the Sink operator (only C++17)
     *  
     *  \return a copy of the created Sink operator
     */ 
    sink_t build()
    {
        if (!isKeyed) {
            return sink_t(func, pardegree, name, closing_func); // copy elision in C++17
        }
        else {
            return sink_t(func, pardegree, name, closing_func, routing_func); // copy elision in C++17
        }
    }
#endif

    /** 
     *  \brief Method to create the Sink operator
     *  
     *  \return a pointer to the created Sink operator (to be explicitly deallocated/destroyed)
     */ 
    sink_t *build_ptr()
    {
        if (!isKeyed) {
            return new sink_t(func, pardegree, name, closing_func);
        }
        else {
            return new sink_t(func, pardegree, name, closing_func, routing_func);
        }
    }

    /** 
     *  \brief Method to create the Sink operator
     *  
     *  \return a unique_ptr to the created Sink operator
     */ 
    std::unique_ptr<sink_t> build_unique()
    {
        if (!isKeyed) {
            return std::make_unique<sink_t>(func, pardegree, name, closing_func);
        }
        else {
            return std::make_unique<sink_t>(func, pardegree, name, closing_func, routing_func);
        }
    }
};

} // namespace wf

#endif
