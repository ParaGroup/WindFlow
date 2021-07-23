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
 *  @file    builders_gpu.hpp
 *  @author  Gabriele Mencagli
 *  
 *  @brief Builder classes used to create the WindFlow operators on GPU
 *  
 *  @section Builders-2 (Description)
 *  
 *  Builder classes used to create the WindFlow operators on GPU.
 */ 

#ifndef BUILDERS_GPU_H
#define BUILDERS_GPU_H

/// includes
#include<meta.hpp>
#include<meta_gpu.hpp>
#include<basic_gpu.hpp>

namespace wf {

/** 
 *  \class MapGPU_Builder
 *  
 *  \brief Builder of the Map_GPU operator
 *  
 *  Builder class to ease the creation of the Map_GPU operator.
 */ 
template<typename mapgpu_func_t, typename key_extractor_func_t=std::false_type, typename key_t=empty_key_t>
class MapGPU_Builder
{
private:
    template<typename T1, typename T2, typename T3> friend class MapGPU_Builder; // friendship with all the instances of the MapGPU_Builder template
    mapgpu_func_t func; // functional logic of the Map_GPU
    using tuple_t = decltype(get_tuple_t_MapGPU(func)); // extracting the tuple_t type and checking the admissible signatures
    using state_t = decltype(get_state_t_MapGPU(func)); // extracting the state_t type and checking the admissible signatures
    // static assert to check the signature of the Map_GPU functional logic
    static_assert(!(std::is_same<tuple_t, std::false_type>::value || std::is_same<state_t, std::false_type>::value),
        "WindFlow Compilation Error - unknown signature passed to the MapGPU_Builder:\n"
        "  Candidate 1 : __device__ void(tuple_t &)\n"
        "  Candidate 2 : __device__ void(tuple_t &, state_t &)\n");
    // static assert to check that the state_t type must be default constructible
    static_assert(std::is_default_constructible<tuple_t>::value,
        "WindFlow Compilation Error - tuple_t type must be default constructible (MapGPU_Builder):\n");
    // static assert to check that the state_t type must be default constructible
    static_assert(std::is_default_constructible<state_t>::value,
        "WindFlow Compilation Error - state_t type must be default constructible (MapGPU_Builder):\n");
    // static assert to check that the tuple_t type must be trivially copyable
    static_assert(std::is_trivially_copyable<tuple_t>::value,
        "WindFlow Compilation Error - tuple_t type must be trivially copyable (MapGPU_Builder):\n");
    std::string name = "map_gpu"; // name of the Map_GPU
    size_t parallelism = 1; // parallelism of the Map_GPU
    Routing_Mode_t input_routing_mode = Routing_Mode_t::FORWARD; // routing mode of inputs to the Map_GPU
    key_extractor_func_t key_extr; // key extractor

    // Private Constructor (keyby only)
    MapGPU_Builder(mapgpu_func_t _func,
                   key_extractor_func_t _key_extr):
                   func(_func),
                   key_extr(_key_extr) {}

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _func functional logic of the Map_GPU (a __device__ callable type)
     */ 
    MapGPU_Builder(mapgpu_func_t _func):
                   func(_func) {}

    /** 
     *  \brief Set the name of the Map_GPU
     *  
     *  \param _name of the Map_GPU
     *  \return a reference to the builder object
     */ 
    MapGPU_Builder<mapgpu_func_t, key_extractor_func_t, key_t> &withName(std::string _name)
    {
        name = _name;
        return *this;
    }

    /** 
     *  \brief Set the parallelism of the Map_GPU
     *  
     *  \param _parallelism of the Map_GPU
     *  \return a reference to the builder object
     */ 
    MapGPU_Builder<mapgpu_func_t, key_extractor_func_t, key_t> &withParallelism(size_t _parallelism)
    {
        parallelism = _parallelism;
        return *this;
    }

    /** 
     *  \brief Set the KEYBY routing mode of inputs to the Map_GPU
     *  
     *  \param _key_extr key extractor functional logic (a __host__ __device__ callable type)
     *  \return a new builder object with the right key type
     */ 
    template<typename new_key_extractor_func_t>
    auto withKeyBy(new_key_extractor_func_t _key_extr)
    {
        // static assert to check the signature
        static_assert(!std::is_same<decltype(get_tuple_t_KeyExtrGPU(_key_extr)), std::false_type>::value,
            "WindFlow Compilation Error - unknown signature passed to withKeyBy (MapGPU_Builder):\n"
            "  Candidate : __host__ __device__ key_t(const tuple_t &)\n");
        // static assert to check that the tuple_t type of the new key extractor is the right one
        static_assert(std::is_same<decltype(get_tuple_t_KeyExtrGPU(_key_extr)), tuple_t>::value,
            "WindFlow Compilation Error - key extractor receives a wrong input type (MapGPU_Builder):\n");
        using new_key_t = decltype(get_key_t_KeyExtrGPU(_key_extr)); // extract the key type
        // static assert to check the new_key_t type
        static_assert(!std::is_same<new_key_t, void>::value,
            "WindFlow Compilation Error - key type cannot be void (MapGPU_Builder):\n");
        // static assert to check that new_key_t is default constructible
        static_assert(std::is_default_constructible<new_key_t>::value,
            "WindFlow Compilation Error - key type must be default constructible (MapGPU_Builder):\n");
        // static assert to check that the tuple_t type must be trivially copyable
        static_assert(std::is_trivially_copyable<new_key_t>::value,
            "WindFlow Compilation Error - key_t type must be trivially copyable (MapGPU_Builder):\n");
        MapGPU_Builder<mapgpu_func_t, new_key_extractor_func_t, new_key_t> new_builder(func, _key_extr);
        new_builder.name = name;
        new_builder.parallelism = parallelism;
        new_builder.input_routing_mode = Routing_Mode_t::KEYBY;
        return new_builder;
    }

    /** 
     *  \brief Create the Map_GPU
     *  
     *  \return a new Map_GPU instance
     */ 
    auto build()
    {
        // static assert to check the use of stateless/stateful logic without/with keyby modifier
        static_assert((std::is_same<key_t, empty_key_t>::value && std::is_same<state_t, std::true_type>::value) ||
                     ((!std::is_same<key_t, empty_key_t>::value && !std::is_same<state_t, std::true_type>::value)),
            "WindFlow Compilation Error - stateless/stateful logic used with/without keyby modifier (MapGPU_Builder):\n"); 
        if constexpr (std::is_same<key_t, empty_key_t>::value) {
            auto k_t = [] (const tuple_t &t) -> empty_key_t {
                return empty_key_t();
            };
            return Map_GPU<mapgpu_func_t, decltype(k_t)>(func,
                                                         k_t,
                                                         parallelism,
                                                         name,
                                                         input_routing_mode);
        }
        else {
            return Map_GPU<mapgpu_func_t, key_extractor_func_t>(func,
                                                                key_extr,
                                                                parallelism,
                                                                name,
                                                                input_routing_mode);
        }
    }
};

/** 
 *  \class FilterGPU_Builder
 *  
 *  \brief Builder of the Filter_GPU operator
 *  
 *  Builder class to ease the creation of the Filter_GPU operator.
 */ 
template<typename filtergpu_func_t, typename key_extractor_func_t=std::false_type, typename key_t=empty_key_t>
class FilterGPU_Builder
{
private:
    template<typename T1, typename T2, typename T3> friend class FilterGPU_Builder; // friendship with all the instances of the FilterGPU_Builder template
    filtergpu_func_t func; // functional logic of the Filter_GPU
    using tuple_t = decltype(get_tuple_t_FilterGPU(func)); // extracting the tuple_t type and checking the admissible signatures
    using state_t = decltype(get_state_t_FilterGPU(func)); // extracting the state_t type and checking the admissible signatures
    // static assert to check the signature of the Filter_GPU functional logic
    static_assert(!(std::is_same<tuple_t, std::false_type>::value || std::is_same<state_t, std::false_type>::value),
        "WindFlow Compilation Error - unknown signature passed to the FilterGPU_Builder:\n"
        "  Candidate 1 : __device__ void(tuple_t &)\n"
        "  Candidate 2 : __device__ void(tuple_t &, state_t &)\n");
    // static assert to check that the tuple_t type must be default constructible
    static_assert(std::is_default_constructible<tuple_t>::value,
        "WindFlow Compilation Error - tuple_t type must be default constructible (FilterGPU_Builder):\n");
    // static assert to check that the state_t type must be default constructible
    static_assert(std::is_default_constructible<state_t>::value,
        "WindFlow Compilation Error - state_t type must be default constructible (FilterGPU_Builder):\n");
    // static assert to check that the tuple_t type must be trivially copyable
    static_assert(std::is_trivially_copyable<tuple_t>::value,
        "WindFlow Compilation Error - tuple_t type must be trivially copyable (FilterGPU_Builder):\n");
    std::string name = "filter_gpu"; // name of the Filter_GPU
    size_t parallelism = 1; // parallelism of the Filter_GPU
    Routing_Mode_t input_routing_mode = Routing_Mode_t::FORWARD; // routing mode of inputs to the Filter_GPU
    key_extractor_func_t key_extr; // key extractor

    // Private Constructor (keyby only)
    FilterGPU_Builder(filtergpu_func_t _func,
                      key_extractor_func_t _key_extr):
                      func(_func),
                      key_extr(_key_extr) {}

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _func functional logic of the Filter_GPU (a __device__ callable type)
     */ 
    FilterGPU_Builder(filtergpu_func_t _func):
                      func(_func) {}

    /** 
     *  \brief Set the name of the Filter_GPU
     *  
     *  \param _name of the Filter_GPU
     *  \return a reference to the builder object
     */ 
    FilterGPU_Builder<filtergpu_func_t, key_extractor_func_t, key_t> &withName(std::string _name)
    {
        name = _name;
        return *this;
    }

    /** 
     *  \brief Set the parallelism of the Filter_GPU
     *  
     *  \param _parallelism of the Filter_GPU
     *  \return a reference to the builder object
     */ 
    FilterGPU_Builder<filtergpu_func_t, key_extractor_func_t, key_t> &withParallelism(size_t _parallelism)
    {
        parallelism = _parallelism;
        return *this;
    }

    /** 
     *  \brief Set the KEYBY routing mode of inputs to the Filter_GPU
     *  
     *  \param _key_extr key extractor functional logic (a __host__ __device__ callable type)
     *  \return a new builder object with the right key type
     */ 
    template<typename new_key_extractor_func_t>
    auto withKeyBy(new_key_extractor_func_t _key_extr)
    {
        // static assert to check the signature
        static_assert(!std::is_same<decltype(get_tuple_t_KeyExtrGPU(_key_extr)), std::false_type>::value,
            "WindFlow Compilation Error - unknown signature passed to withKeyBy (FilterGPU_Builder):\n"
            "  Candidate : __host__ __device__ key_t(const tuple_t &)\n");
        // static assert to check that the tuple_t type of the new key extractor is the right one
        static_assert(std::is_same<decltype(get_tuple_t_KeyExtrGPU(_key_extr)), tuple_t>::value,
            "WindFlow Compilation Error - key extractor receives a wrong input type (FilterGPU_Builder):\n");
        using new_key_t = decltype(get_key_t_KeyExtrGPU(_key_extr)); // extract the key type
        // static assert to check the new_key_t type
        static_assert(!std::is_same<new_key_t, void>::value,
            "WindFlow Compilation Error - key type cannot be void (FilterGPU_Builder):\n");
        // static assert to check that new_key_t is default constructible
        static_assert(std::is_default_constructible<new_key_t>::value,
            "WindFlow Compilation Error - key type must be default constructible (FilterGPU_Builder):\n");
        // static assert to check that the tuple_t type must be trivially copyable
        static_assert(std::is_trivially_copyable<new_key_t>::value,
            "WindFlow Compilation Error - key_t type must be trivially copyable (FilterGPU_Builder):\n");
        FilterGPU_Builder<filtergpu_func_t, new_key_extractor_func_t, new_key_t> new_builder(func, _key_extr);
        new_builder.name = name;
        new_builder.parallelism = parallelism;
        new_builder.input_routing_mode = Routing_Mode_t::KEYBY;
        return new_builder;
    }

    /** 
     *  \brief Create the Filter_GPU
     *  
     *  \return a new Filter_GPU instance
     */ 
    auto build()
    {
        // static assert to check the use of stateless/stateful logic without/with keyby modifier
        static_assert((std::is_same<key_t, empty_key_t>::value && std::is_same<state_t, std::true_type>::value) ||
                     ((!std::is_same<key_t, empty_key_t>::value && !std::is_same<state_t, std::true_type>::value)),
            "WindFlow Compilation Error - stateless/stateful logic used with/without keyby modifier (FilterGPU_Builder):\n"); 
        if constexpr (std::is_same<key_t, empty_key_t>::value) {
            auto k_t = [] (const tuple_t &t) -> empty_key_t {
                return empty_key_t();
            };
            return Filter_GPU<filtergpu_func_t, decltype(k_t)>(func,
                                                               k_t,
                                                               parallelism,
                                                               name,
                                                               input_routing_mode);
        }
        else {
            return Filter_GPU<filtergpu_func_t, key_extractor_func_t>(func,
                                                                      key_extr,
                                                                      parallelism,
                                                                      name,
                                                                      input_routing_mode);
        }
    }
};

/** 
 *  \class FFAT_AggregatorGPU_Builder
 *  
 *  \brief Builder of the FFAT_Aggregator_GPU operator
 *  
 *  Builder class to ease the creation of the FFAT_Aggregator_GPU operator.
 */ 
template<typename liftgpu_func_t, typename combgpu_func_t, typename key_extractor_func_t=std::false_type, typename key_t=empty_key_t>
class FFAT_AggregatorGPU_Builder
{
private:
    template<typename T1, typename T2, typename T3, typename T4> friend class FFAT_AggregatorGPU_Builder; // friendship with all the instances of the FFAT_AggregatorGPU_Builder template
    liftgpu_func_t lift_func; // lift functional logic of the FFAT_Aggregator_GPU
    combgpu_func_t comb_func; // combine functional logic of the FFAT_Aggregator_GPU
    using tuple_t = decltype(get_tuple_t_Lift(lift_func)); // extracting the tuple_t type and checking the admissible signatures
    using result_t = decltype(get_result_t_Lift(lift_func)); // extracting the result_t type and checking the admissible signatures
    // static assert to check the signature of the FFAT_AggregatorGPU_Builder functional logic
    static_assert(!(std::is_same<tuple_t, std::false_type>::value || std::is_same<result_t, std::false_type>::value),
        "WindFlow Compilation Error - unknown signature passed to the FFAT_AggregatorGPU_Builder (first argument, lift logic):\n"
        "  Candidate 1 : void(const tuple_t &, result_t &)\n");
    using result_t2 = decltype(get_tuple_t_CombGPU(comb_func));
    static_assert(!(std::is_same<std::false_type, result_t2>::value),
        "WindFlow Compilation Error - unknown signature passed to the FFAT_AggregatorGPU_Builder (second argument, combine logic):\n"
        "  Candidate 1 : __host__ __device__ void(const result_t &, const result_t &, result_t &)\n");
    static_assert(std::is_same<result_t, result_t2>::value &&
                  std::is_same<result_t2, decltype(get_tuple_t_CombGPU(comb_func))>::value,
        "WindFlow Compilation Error - type mismatch in the FFAT_AggregatorGPU_Builder\n");
    // static assert to check that the tuple_t type must be default constructible
    static_assert(std::is_default_constructible<tuple_t>::value,
        "WindFlow Compilation Error - tuple_t type must be default constructible (FFAT_AggregatorGPU_Builder):\n");
    // static assert to check that the tuple_t type must be trivially copyable
    static_assert(std::is_trivially_copyable<tuple_t>::value,
        "WindFlow Compilation Error - tuple_t type must be trivially copyable (FFAT_AggregatorGPU_Builder):\n");
    // static assert to check that the result_t type must be trivially copyable
    static_assert(std::is_trivially_copyable<result_t>::value,
        "WindFlow Compilation Error - result_t type must be trivially copyable (FFAT_AggregatorGPU_Builder):\n");
    using ffat_agg_gpu_t = FFAT_Aggregator_GPU<liftgpu_func_t, combgpu_func_t, key_extractor_func_t>; // type of the FFAT_Aggregator_GPU to be created by the builder
    std::string name = "ffat_aggregator_gpu"; // name of the FFAT_Aggregator_GPU
    size_t parallelism = 1; // parallelism of the FFAT_Aggregator_GPU
    key_extractor_func_t key_extr; // key extractor
    bool isKeyBySet = false; // true if a key extractor has been provided
    size_t outputBatchSize = 0; // output batch size of the FFAT_Aggregator_GPU
    uint64_t win_len=0; // window length in number of tuples or in time units
    uint64_t slide_len=0; // slide length in number of tuples or in time units
    uint64_t quantum=0; // quantum value (for time-based windows only)
    uint64_t lateness=0; // lateness in time units
    Win_Type_t winType=Win_Type_t::CB; // window type (CB or TB)

    // Private Constructor (keyby only)
    FFAT_AggregatorGPU_Builder(liftgpu_func_t _lift_func,
                               combgpu_func_t _comb_func,
                               key_extractor_func_t _key_extr):
                               lift_func(_lift_func),
                               comb_func(_comb_func),
                               key_extr(_key_extr) {}

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _lift_func lift functional logic of the FFAT_Aggregator_GPU (a function or a callable type)
     *  \param _comb_func combine functional logic of the FFAT_Aggregator_GPU (a __host__ __device__ callable type)
     */ 
    FFAT_AggregatorGPU_Builder(liftgpu_func_t _lift_func,
                               combgpu_func_t _comb_func):
                               lift_func(_lift_func),
                               comb_func(_comb_func) {}

    /** 
     *  \brief Set the name of the FFAT_Aggregator_GPU
     *  
     *  \param _name of the FFAT_Aggregator_GPU
     *  \return a reference to the builder object
     */ 
    FFAT_AggregatorGPU_Builder<liftgpu_func_t, combgpu_func_t, key_extractor_func_t, key_t> &withName(std::string _name)
    {
        name = _name;
        return *this;
    }

    /** 
     *  \brief Set the parallelism of the FFAT_Aggregator_GPU
     *  
     *  \param _parallelism of the FFAT_Aggregator_GPU
     *  \return a reference to the builder object
     */ 
    FFAT_AggregatorGPU_Builder<liftgpu_func_t, combgpu_func_t, key_extractor_func_t, key_t> &withParallelism(size_t _parallelism)
    {
        parallelism = _parallelism;
        return *this;
    }

    /** 
     *  \brief Set the KEYBY routing mode of inputs to the FFAT_Aggregator_GPU
     *  
     *  \param _key_extr key extractor functional logic (a __host__ __device__ callable type)
     *  \return a new builder object with the right key type
     */ 
    template<typename new_key_extractor_func_t>
    auto withKeyBy(new_key_extractor_func_t _key_extr)
    {
        // static assert to check the signature
        static_assert(!std::is_same<decltype(get_tuple_t_KeyExtrGPU(_key_extr)), std::false_type>::value,
            "WindFlow Compilation Error - unknown signature passed to withKeyBy (FFAT_AggregatorGPU_Builder):\n"
            "  Candidate : __host__ __device__ key_t(const tuple_t &)\n");
        // static assert to check that the tuple_t type of the new key extractor is the right one
        static_assert(std::is_same<decltype(get_tuple_t_KeyExtrGPU(_key_extr)), tuple_t>::value,
            "WindFlow Compilation Error - key extractor receives a wrong input type (FFAT_AggregatorGPU_Builder):\n");
        using new_key_t = decltype(get_key_t_KeyExtrGPU(_key_extr)); // extract the key type
        // static assert to check the new_key_t type
        static_assert(!std::is_same<new_key_t, void>::value,
            "WindFlow Compilation Error - key type cannot be void (FFAT_AggregatorGPU_Builder):\n");
        // static assert to check that new_key_t is default constructible
        static_assert(std::is_default_constructible<new_key_t>::value,
            "WindFlow Compilation Error - key type must be default constructible (FFAT_AggregatorGPU_Builder):\n");
        // static assert to check that the tuple_t type must be trivially copyable
        static_assert(std::is_trivially_copyable<new_key_t>::value,
            "WindFlow Compilation Error - key_t type must be trivially copyable (FFAT_AggregatorGPU_Builder):\n");
        FFAT_AggregatorGPU_Builder<liftgpu_func_t, combgpu_func_t, new_key_extractor_func_t, new_key_t> new_builder(lift_func, comb_func, _key_extr);
        new_builder.name = name;
        new_builder.parallelism = parallelism;
        new_builder.isKeyBySet = true;
        new_builder.outputBatchSize = outputBatchSize;
        new_builder.win_len = win_len;
        new_builder.slide_len = slide_len;
        new_builder.quantum = quantum;
        new_builder.lateness = lateness;
        new_builder.winType = winType;
        return new_builder;
    }

    /** 
     *  \brief Set the output batch size of the FFAT_Aggregator_GPU
     *  
     *  \param _outputBatchSize number of outputs per batch (zero means no batching)
     *  \return a reference to the builder object
     */ 
    FFAT_AggregatorGPU_Builder<liftgpu_func_t, combgpu_func_t, key_extractor_func_t, key_t> &withOutputBatchSize(size_t _outputBatchSize)
    {
        outputBatchSize = _outputBatchSize;
        return *this;
    }

    /** 
     *  \brief Set the configuration for count-based windows
     *  
     *  \param _win_len window length (in number of tuples)
     *  \param _slide_len slide length (in number of tuples)
     *  \return a reference to the builder object
     */ 
    FFAT_AggregatorGPU_Builder<liftgpu_func_t, combgpu_func_t, key_extractor_func_t, key_t> &withCBWindows(uint64_t _win_len,
                                                                                                           uint64_t _slide_len)
    {
        win_len = _win_len;
        slide_len = _slide_len;
        winType = Win_Type_t::CB;
        lateness = 0;
        return *this;
    }

    /** 
     *  \brief Set the configuration for time-based windows
     *  
     *  \param _win_len window length (in microseconds)
     *  \param _slide_len slide length (in microseconds)
     *  \param _quantum quantum value (in microseconds)
     *  \return a reference to the builder object
     */ 
    FFAT_AggregatorGPU_Builder<liftgpu_func_t, combgpu_func_t, key_extractor_func_t, key_t> &withTBWindows(std::chrono::microseconds _win_len,
                                                                                                           std::chrono::microseconds _slide_len,
                                                                                                           std::chrono::microseconds _quantum)
    {
        win_len = _win_len.count();
        slide_len = _slide_len.count();
        quantum = _quantum.count();
        if ((win_len % quantum != 0) || (slide_len % quantum != 0)) {
            std::cerr << RED << "WindFlow Error: window length and slide must be divisible by the quantum parameter" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);         
        }
        winType = Win_Type_t::TB;
        return *this;
    }

    /** 
     *  \brief Set the lateness for time-based windows
     *  
     *  \param _lateness (in microseconds)
     *  \return a reference to the builder object
     */ 
    FFAT_AggregatorGPU_Builder<liftgpu_func_t, combgpu_func_t, key_extractor_func_t, key_t> &withLateness(std::chrono::microseconds _lateness)
    {
        if (winType != Win_Type_t::TB) { // check that time-based semantics is used
            std::cerr << RED << "WindFlow Error: lateness can be set only for time-based windows" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        lateness = _lateness.count();
        return *this;
    }

    /** 
     *  \brief Create the FFAT_Aggregator_GPU
     *  
     *  \return a new FFAT_Aggregator_GPU instance
     */ 
    ffat_agg_gpu_t build()
    {
        // static asserts to check that result_t is properly constructible
        if constexpr (std::is_same<key_t, empty_key_t>::value) { // case without key
            static_assert(std::is_constructible<result_t, uint64_t>::value,
                "WindFlow Compilation Error - result type must be constructible with a uint64_t (FFAT_AggregatorGPU_Builder):\n");
        }
        else { // case with key
            static_assert(std::is_constructible<result_t, key_t, uint64_t>::value,
                "WindFlow Compilation Error - result type must be constructible with a key_t and uint64_t (FFAT_AggregatorGPU_Builder):\n");
        }
        // check the presence of a key extractor
        if (!isKeyBySet && parallelism > 1) {
            std::cerr << RED << "WindFlow Error: FFAT_Aggregator_GPU with parallelism > 1 requires a key extractor" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        return ffat_agg_gpu_t(lift_func,
                              comb_func,
                              key_extr,
                              parallelism,
                              name,
                              outputBatchSize,
                              win_len,
                              slide_len,
                              quantum,
                              lateness,
                              winType);
    }
};

} // namespace wf

#endif
