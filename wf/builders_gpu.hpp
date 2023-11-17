/**************************************************************************************
 *  Copyright (c) 2019- Gabriele Mencagli
 *  
 *  This file is part of WindFlow.
 *  
 *  WindFlow is free software dual licensed under the GNU LGPL or MIT License.
 *  You can redistribute it and/or modify it under the terms of the
 *    * GNU Lesser General Public License as published by
 *      the Free Software Foundation, either version 3 of the License, or
 *      (at your option) any later version
 *    OR
 *    * MIT License: https://github.com/ParaGroup/WindFlow/blob/vers3.x/LICENSE.MIT
 *  
 *  WindFlow is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Lesser General Public License for more details.
 *  You should have received a copy of the GNU Lesser General Public License and
 *  the MIT License along with WindFlow. If not, see <http://www.gnu.org/licenses/>
 *  and <http://opensource.org/licenses/MIT/>.
 **************************************************************************************
 */

/** 
 *  @file    builders_gpu.hpp
 *  @author  Gabriele Mencagli
 *  
 *  @brief Builder classes used to create WindFlow operators on GPU
 *  
 *  @section Builders-2 (Description)
 *  
 *  Builder classes used to create WindFlow operators on GPU.
 */ 

#ifndef BUILDERS_GPU_H
#define BUILDERS_GPU_H

/// includes
#include<chrono>
#include<meta.hpp>
#include<meta_gpu.hpp>
#include<basic_gpu.hpp>

namespace wf {

/** 
 *  \class BasicGPU_Builder
 *  
 *  \brief Abstract class of a builder for GPU operators
 *  
 *  Abstract class extended by all the builders of GPU operators.
 */ 
template<template<class, class, class> class builder_gpu_t, class func_gpu_t, class keyextr_func_gpu_t, class key_t>
class BasicGPU_Builder
{
protected:
    std::string name = "N/D"; // name of the operator
    size_t parallelism = 1; // parallelism of the operator

    // Constructor
    BasicGPU_Builder() = default;

    // Copy Constructor
    BasicGPU_Builder(const BasicGPU_Builder &) = default;

public:
    /** 
     *  \brief Set the name of the GPU operator
     *  
     *  \param _name of the GPU operator
     *  \return a reference to the builder object
     */ 
    auto &withName(std::string _name)
    {
        name = _name;
        return static_cast<builder_gpu_t<func_gpu_t, keyextr_func_gpu_t, key_t> &>(*this);
    }

    /** 
     *  \brief Set the parallelism of the GPU operator
     *  
     *  \param _parallelism of the GPU operator
     *  \return a reference to the builder object
     */ 
    auto &withParallelism(size_t _parallelism)
    {
        parallelism = _parallelism;
        return static_cast<builder_gpu_t<func_gpu_t, keyextr_func_gpu_t, key_t> &>(*this);
    }
};

/** 
 *  \class FilterGPU_Builder
 *  
 *  \brief Builder of the Filter_GPU operator
 *  
 *  Builder class to ease the creation of the Filter_GPU operator.
 */ 
template<typename filter_func_gpu_t, typename keyextr_func_gpu_t=std::false_type, typename key_t=empty_key_t>
class FilterGPU_Builder: public BasicGPU_Builder<FilterGPU_Builder, filter_func_gpu_t, keyextr_func_gpu_t, key_t>
{
private:
    template<typename T1, typename T2, typename T3> friend class FilterGPU_Builder;
    filter_func_gpu_t func; // functional logic of the Filter_GPU
    using tuple_t = decltype(get_tuple_t_FilterGPU(func)); // extracting the tuple_t type and checking the admissible signatures
    using state_t = decltype(get_state_t_FilterGPU(func)); // extracting the state_t type and checking the admissible signatures
    // static assert to check the signature of the Filter_GPU functional logic
    static_assert(!(std::is_same<tuple_t, std::false_type>::value || std::is_same<state_t, std::false_type>::value),
        "WindFlow Compilation Error - unknown signature passed to the FilterGPU_Builder:\n"
        "  Candidate 1 : __host__ __device__ bool(tuple_t &)\n"
        "  Candidate 2 : __host__ __device__ bool(tuple_t &, state_t &)\n");
    // static assert to check that the tuple_t type must be default constructible
    static_assert(std::is_default_constructible<tuple_t>::value,
        "WindFlow Compilation Error - tuple_t type must be default constructible (FilterGPU_Builder):\n");
    // static assert to check that the state_t type must be default constructible
    static_assert(std::is_default_constructible<state_t>::value,
        "WindFlow Compilation Error - state_t type must be default constructible (FilterGPU_Builder):\n");
    // static assert to check that the tuple_t type must be trivially copyable
    static_assert(std::is_trivially_copyable<tuple_t>::value,
        "WindFlow Compilation Error - tuple_t type must be trivially copyable (FilterGPU_Builder):\n");
    Routing_Mode_t input_routing_mode = Routing_Mode_t::FORWARD; // routing mode of inputs to the Filter_GPU
    keyextr_func_gpu_t key_extr; // key extractor

    // Private Constructor (keyby only)
    FilterGPU_Builder(filter_func_gpu_t _func, keyextr_func_gpu_t _key_extr):
                      func(_func), key_extr(_key_extr) {}

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _func functional logic of the Filter_GPU (a __host__ __device__ callable type)
     */ 
    FilterGPU_Builder(filter_func_gpu_t _func):
                      func(_func) {}

    /** 
     *  \brief Set the KEYBY routing mode of inputs to the Filter_GPU
     *  
     *  \param _key_extr key extractor functional logic (a __host__ __device__ callable type)
     *  \return a new builder object with the right key type
     */ 
    template<typename new_keyextr_func_gpu_t>
    auto withKeyBy(new_keyextr_func_gpu_t _key_extr)
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
        // static assert to check that the new_key_t type must be trivially copyable
        static_assert(std::is_trivially_copyable<new_key_t>::value,
            "WindFlow Compilation Error - key_t type must be trivially copyable (FilterGPU_Builder):\n");
        FilterGPU_Builder<filter_func_gpu_t, new_keyextr_func_gpu_t, new_key_t> new_builder(func, _key_extr);
        new_builder.name = this->name;
        new_builder.parallelism = this->parallelism;
        new_builder.input_routing_mode = Routing_Mode_t::KEYBY;
        return new_builder;
    }

    /** 
     *  \brief Set the REBALANCING routing mode of inputs to the Filter_GPU
     *         (it forces a re-shuffling before this new operator)
     *  
     *  \return a reference to the builder object
     */ 
    auto &withRebalancing()
    {
        if (input_routing_mode == Routing_Mode_t::KEYBY) {
            std::cerr << RED << "WindFlow Error: wrong use of withRebalancing() in the FilterGPU_Builder" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        input_routing_mode = Routing_Mode_t::REBALANCING;
        return *this;
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
            return Filter_GPU<filter_func_gpu_t, decltype(k_t)>(func,
                                                                k_t,
                                                                this->parallelism,
                                                                this->name,
                                                                input_routing_mode);
        }
        else {
            return Filter_GPU<filter_func_gpu_t, keyextr_func_gpu_t>(func,
                                                                     key_extr,
                                                                     this->parallelism,
                                                                     this->name,
                                                                     input_routing_mode);
        }
    }
};

/** 
 *  \class MapGPU_Builder
 *  
 *  \brief Builder of the Map_GPU operator
 *  
 *  Builder class to ease the creation of the Map_GPU operator.
 */ 
template<typename map_func_gpu_t, typename keyextr_func_gpu_t=std::false_type, typename key_t=empty_key_t>
class MapGPU_Builder: public BasicGPU_Builder<MapGPU_Builder, map_func_gpu_t, keyextr_func_gpu_t, key_t>
{
private:
    template<typename T1, typename T2, typename T3> friend class MapGPU_Builder;
    map_func_gpu_t func; // functional logic of the Map_GPU
    using tuple_t = decltype(get_tuple_t_MapGPU(func)); // extracting the tuple_t type and checking the admissible signatures
    using state_t = decltype(get_state_t_MapGPU(func)); // extracting the state_t type and checking the admissible signatures
    // static assert to check the signature of the Map_GPU functional logic
    static_assert(!(std::is_same<tuple_t, std::false_type>::value || std::is_same<state_t, std::false_type>::value),
        "WindFlow Compilation Error - unknown signature passed to the MapGPU_Builder:\n"
        "  Candidate 1 : __host__ __device__ void(tuple_t &)\n"
        "  Candidate 2 : __host__ __device__ void(tuple_t &, state_t &)\n");
    // static assert to check that the tuple_t type must be default constructible
    static_assert(std::is_default_constructible<tuple_t>::value,
        "WindFlow Compilation Error - tuple_t type must be default constructible (MapGPU_Builder):\n");
    // static assert to check that the state_t type must be default constructible
    static_assert(std::is_default_constructible<state_t>::value,
        "WindFlow Compilation Error - state_t type must be default constructible (MapGPU_Builder):\n");
    // static assert to check that the tuple_t type must be trivially copyable
    static_assert(std::is_trivially_copyable<tuple_t>::value,
        "WindFlow Compilation Error - tuple_t type must be trivially copyable (MapGPU_Builder):\n");
    Routing_Mode_t input_routing_mode = Routing_Mode_t::FORWARD; // routing mode of inputs to the Map_GPU
    keyextr_func_gpu_t key_extr; // key extractor

    // Private Constructor (keyby only)
    MapGPU_Builder(map_func_gpu_t _func, keyextr_func_gpu_t _key_extr):
                   func(_func), key_extr(_key_extr) {}

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _func functional logic of the Map_GPU (a __host__ __device__ callable type)
     */ 
    MapGPU_Builder(map_func_gpu_t _func):
                   func(_func) {}

    /** 
     *  \brief Set the KEYBY routing mode of inputs to the Map_GPU
     *  
     *  \param _key_extr key extractor functional logic (a __host__ __device__ callable type)
     *  \return a new builder object with the right key type
     */ 
    template<typename new_keyextr_func_gpu_t>
    auto withKeyBy(new_keyextr_func_gpu_t _key_extr)
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
        // static assert to check that the new_key_t type must be trivially copyable
        static_assert(std::is_trivially_copyable<new_key_t>::value,
            "WindFlow Compilation Error - key_t type must be trivially copyable (MapGPU_Builder):\n");
        MapGPU_Builder<map_func_gpu_t, new_keyextr_func_gpu_t, new_key_t> new_builder(func, _key_extr);
        new_builder.name = this->name;
        new_builder.parallelism = this->parallelism;
        new_builder.input_routing_mode = Routing_Mode_t::KEYBY;
        return new_builder;
    }

    /** 
     *  \brief Set the REBALANCING routing mode of inputs to the Map_GPU
     *         (it forces a re-shuffling before this new operator)
     *  
     *  \return a reference to the builder object
     */ 
    auto &withRebalancing()
    {
        if (input_routing_mode == Routing_Mode_t::KEYBY) {
            std::cerr << RED << "WindFlow Error: wrong use of withRebalancing() in the MapGPU_Builder" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        input_routing_mode = Routing_Mode_t::REBALANCING;
        return *this;
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
            return Map_GPU<map_func_gpu_t, decltype(k_t)>(func,
                                                          k_t,
                                                          this->parallelism,
                                                          this->name,
                                                          input_routing_mode);
        }
        else {
            return Map_GPU<map_func_gpu_t, keyextr_func_gpu_t>(func,
                                                               key_extr,
                                                               this->parallelism,
                                                               this->name,
                                                               input_routing_mode);
        }
    }
};

/** 
 *  \class ReduceGPU_Builder
 *  
 *  \brief Builder of the Reduce_GPU operator
 *  
 *  Builder class to ease the creation of the Reduce_GPU operator.
 */ 
template<typename reduce_func_gpu_t, typename keyextr_func_gpu_t=std::false_type, typename key_t=empty_key_t>
class ReduceGPU_Builder: public BasicGPU_Builder<ReduceGPU_Builder, reduce_func_gpu_t, keyextr_func_gpu_t, key_t>
{
private:
    template<typename T1, typename T2, typename T3> friend class ReduceGPU_Builder;
    reduce_func_gpu_t func; // functional logic of the Reduce_GPU
    using tuple_t = decltype(get_tuple_t_ReduceGPU(func)); // extracting the tuple_t type and checking the admissible signatures
    // static assert to check the signature of the Reduce_GPU functional logic
    static_assert(!(std::is_same<tuple_t, std::false_type>::value),
        "WindFlow Compilation Error - unknown signature passed to the ReduceGPU_Builder:\n"
        "  Candidate 1 : __host__ __device__ tuple_t(const tuple_t &, const tuple_t &)\n");
    // static assert to check that the tuple_t type must be default constructible
    static_assert(std::is_default_constructible<tuple_t>::value,
        "WindFlow Compilation Error - tuple_t type must be default constructible (ReduceGPU_Builder):\n");
    // static assert to check that the tuple_t type must be trivially copyable
    static_assert(std::is_trivially_copyable<tuple_t>::value,
        "WindFlow Compilation Error - tuple_t type must be trivially copyable (ReduceGPU_Builder):\n");
    Routing_Mode_t input_routing_mode = Routing_Mode_t::FORWARD; // routing mode of inputs to the Reduce_GPU
    keyextr_func_gpu_t key_extr; // key extractor

    // Private Constructor (keyby only)
    ReduceGPU_Builder(reduce_func_gpu_t _func, keyextr_func_gpu_t _key_extr):
                      func(_func), key_extr(_key_extr) {}

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _func functional logic of the Reduce_GPU (a __host__ __device__ callable type)
     */ 
    ReduceGPU_Builder(reduce_func_gpu_t _func):
                      func(_func) {}

    /** 
     *  \brief Set the KEYBY routing mode of inputs to the Reduce_GPU
     *  
     *  \param _key_extr key extractor functional logic (a __host__ __device__ callable type)
     *  \return a new builder object with the right key type
     */ 
    template<typename new_keyextr_func_gpu_t>
    auto withKeyBy(new_keyextr_func_gpu_t _key_extr)
    {
        // static assert to check the signature
        static_assert(!std::is_same<decltype(get_tuple_t_KeyExtrGPU(_key_extr)), std::false_type>::value,
            "WindFlow Compilation Error - unknown signature passed to withKeyBy (ReduceGPU_Builder):\n"
            "  Candidate : __host__ __device__ key_t(const tuple_t &)\n");
        // static assert to check that the tuple_t type of the new key extractor is the right one
        static_assert(std::is_same<decltype(get_tuple_t_KeyExtrGPU(_key_extr)), tuple_t>::value,
            "WindFlow Compilation Error - key extractor receives a wrong input type (ReduceGPU_Builder):\n");
        using new_key_t = decltype(get_key_t_KeyExtrGPU(_key_extr)); // extract the key type
        // static assert to check the new_key_t type
        static_assert(!std::is_same<new_key_t, void>::value,
            "WindFlow Compilation Error - key type cannot be void (ReduceGPU_Builder):\n");
        // static assert to check that new_key_t is default constructible
        static_assert(std::is_default_constructible<new_key_t>::value,
            "WindFlow Compilation Error - key type must be default constructible (ReduceGPU_Builder):\n");
        // static assert to check that the new_key_t type must be trivially copyable
        static_assert(std::is_trivially_copyable<new_key_t>::value,
            "WindFlow Compilation Error - key_t type must be trivially copyable (ReduceGPU_Builder):\n");
        ReduceGPU_Builder<reduce_func_gpu_t, new_keyextr_func_gpu_t, new_key_t> new_builder(func, _key_extr);
        new_builder.name = this->name;
        new_builder.parallelism = this->parallelism;
        // Please note that we do not set the KEYBY distribution here
        return new_builder;
    }

    /** 
     *  \brief Set the REBALANCING routing mode of inputs to the Reduce_GPU
     *         (it forces a re-shuffling before this new operator)
     *  
     *  \return a reference to the builder object
     */ 
    auto &withRebalancing()
    {
        input_routing_mode = Routing_Mode_t::REBALANCING;
        return *this;
    }

    /** 
     *  \brief Create the Reduce_GPU
     *  
     *  \return a new Reduce_GPU instance
     */ 
    auto build()
    {
        if constexpr (std::is_same<key_t, empty_key_t>::value) {
            auto k_t = [] (const tuple_t &t) -> empty_key_t {
                return empty_key_t();
            };
            return Reduce_GPU<reduce_func_gpu_t, decltype(k_t)>(func,
                                                                k_t,
                                                                this->parallelism,
                                                                this->name,
                                                                input_routing_mode);
        }
        else {
            return Reduce_GPU<reduce_func_gpu_t, keyextr_func_gpu_t>(func,
                                                                     key_extr,
                                                                     this->parallelism,
                                                                     this->name,
                                                                     input_routing_mode);
        }
    }
};

/** 
 *  \class Ffat_WindowsGPU_Builder
 *  
 *  \brief Builder of the Ffat_Windows_GPU operator, also called (informally) "Springald"
 *  
 *  Builder class to ease the creation of the Ffat_Windows_GPU operator.
 */ 
template<typename lift_func_gpu_t, typename comb_func_gpu_t, typename keyextr_func_gpu_t=std::false_type, typename key_t=empty_key_t>
class Ffat_WindowsGPU_Builder
{
private:
    template<typename T1, typename T2, typename T3, typename T4> friend class Ffat_WindowsGPU_Builder;
    lift_func_gpu_t lift_func; // lift functional logic of the Ffat_Windows_GPU
    comb_func_gpu_t comb_func; // combine functional logic of the Ffat_Windows_GPU
    using tuple_t = decltype(get_tuple_t_LiftGPU(lift_func)); // extracting the tuple_t type and checking the admissible signatures
    using result_t = decltype(get_result_t_LiftGPU(lift_func)); // extracting the result_t type and checking the admissible signatures
    // static assert to check the signature of the Ffat_WindowsGPU_Builder functional logic
    static_assert(!(std::is_same<tuple_t, std::false_type>::value || std::is_same<result_t, std::false_type>::value),
        "WindFlow Compilation Error - unknown signature passed to the Ffat_WindowsGPU_Builder (first argument, lift logic):\n"
        "  Candidate 1 : __host__ __device__ void(const tuple_t &, result_t &)\n");
    using result_t2 = decltype(get_tuple_t_CombGPU(comb_func));
    static_assert(!(std::is_same<std::false_type, result_t2>::value),
        "WindFlow Compilation Error - unknown signature passed to the Ffat_WindowsGPU_Builder (second argument, combine logic):\n"
        "  Candidate 1 : __host__ __device__ void(const result_t &, const result_t &, result_t &)\n");
    static_assert(std::is_same<result_t, result_t2>::value,
        "WindFlow Compilation Error - type mismatch between lift and combine logics in the Ffat_WindowsGPU_Builder\n");
    // static assert to check that the tuple_t type must be default constructible
    static_assert(std::is_default_constructible<tuple_t>::value,
        "WindFlow Compilation Error - tuple_t type must be default constructible (Ffat_WindowsGPU_Builder):\n");
    // static assert to check that the tuple_t type must be trivially copyable
    static_assert(std::is_trivially_copyable<tuple_t>::value,
        "WindFlow Compilation Error - tuple_t type must be trivially copyable (Ffat_WindowsGPU_Builder):\n");
    // static assert to check that the result_t type must be trivially copyable
    static_assert(std::is_trivially_copyable<result_t>::value,
        "WindFlow Compilation Error - result_t type must be trivially copyable (Ffat_WindowsGPU_Builder):\n");
    std::string name = "ffat_windows_gpu"; // name of the Ffat_Windows_GPU
    keyextr_func_gpu_t key_extr; // key extractor
    size_t numWinPerBatch = 0; // number of consecutive window results produced per batch
    uint64_t win_len=0; // window length in number of tuples or in time units
    uint64_t slide_len=0; // slide length in number of tuples or in time units
    uint64_t lateness=0; // lateness in time units
    Win_Type_t winType=Win_Type_t::CB; // window type (CB or TB)

    // Private Constructor (keyby only)
    Ffat_WindowsGPU_Builder(lift_func_gpu_t _lift_func,
                            comb_func_gpu_t _comb_func,
                            keyextr_func_gpu_t _key_extr):
                            lift_func(_lift_func),
                            comb_func(_comb_func),
                            key_extr(_key_extr) {}

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _lift_func lift functional logic of the Ffat_Windows_GPU
     *         (a __host__ __device__ callable type)
     *  \param _comb_func combine functional logic of the Ffat_Windows_GPU
     *         (a __host__ __device__ callable type)
     */ 
    Ffat_WindowsGPU_Builder(lift_func_gpu_t _lift_func, comb_func_gpu_t _comb_func):
                            lift_func(_lift_func), comb_func(_comb_func) {}

    /** 
     *  \brief Set the name of the Ffat_Windows_GPU
     *  
     *  \param _name of the Ffat_Windows_GPU
     *  \return a reference to the builder object
     */ 
    auto &withName(std::string _name)
    {
        name = _name;
        return *this;
    }

    /** 
     *  \brief Set the KEYBY routing mode of inputs to the Ffat_Windows_GPU
     *  
     *  \param _key_extr key extractor functional logic (a __host__ __device__ callable type)
     *  \return a new builder object with the right key type
     */ 
    template<typename new_keyextr_func_gpu_t>
    auto withKeyBy(new_keyextr_func_gpu_t _key_extr)
    {
        // static assert to check the signature
        static_assert(!std::is_same<decltype(get_tuple_t_KeyExtrGPU(_key_extr)), std::false_type>::value,
            "WindFlow Compilation Error - unknown signature passed to withKeyBy (Ffat_WindowsGPU_Builder):\n"
            "  Candidate : __host__ __device__ key_t(const tuple_t &)\n");
        // static assert to check that the tuple_t type of the new key extractor is the right one
        static_assert(std::is_same<decltype(get_tuple_t_KeyExtrGPU(_key_extr)), tuple_t>::value,
            "WindFlow Compilation Error - key extractor receives a wrong input type (Ffat_WindowsGPU_Builder):\n");
        using new_key_t = decltype(get_key_t_KeyExtrGPU(_key_extr)); // extract the key type
        // static assert to check the new_key_t type
        static_assert(!std::is_same<new_key_t, void>::value,
            "WindFlow Compilation Error - key type cannot be void (Ffat_WindowsGPU_Builder):\n");
        // static assert to check that new_key_t is default constructible
        static_assert(std::is_default_constructible<new_key_t>::value,
            "WindFlow Compilation Error - key type must be default constructible (Ffat_WindowsGPU_Builder):\n");
        // static assert to check that the new_key_t type must be trivially copyable
        static_assert(std::is_trivially_copyable<new_key_t>::value,
            "WindFlow Compilation Error - key_t type must be trivially copyable (Ffat_WindowsGPU_Builder):\n");
        Ffat_WindowsGPU_Builder<lift_func_gpu_t, comb_func_gpu_t, new_keyextr_func_gpu_t, new_key_t> new_builder(lift_func, comb_func, _key_extr);
        new_builder.name = name;
        new_builder.numWinPerBatch = numWinPerBatch;
        new_builder.win_len = win_len;
        new_builder.slide_len = slide_len;
        new_builder.lateness = lateness;
        new_builder.winType = winType;
        return new_builder;
    }

    /** 
     *  \brief Set the number of consecutive windows results produced
     *         per batch by the Ffat_Windows_GPU
     *  
     *  \param _numWinPerBatch number of consecutive windows results produced per batch
     *  \return a reference to the builder object
     */ 
    auto &withNumWinPerBatch(size_t _numWinPerBatch)
    {
        numWinPerBatch = _numWinPerBatch;
        return *this;
    }

    /** 
     *  \brief Set the configuration for count-based windows
     *  
     *  \param _win_len window length (in number of tuples)
     *  \param _slide_len slide length (in number of tuples)
     *  \return a reference to the builder object
     */ 
    auto &withCBWindows(uint64_t _win_len, uint64_t _slide_len)
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
     *  \return a reference to the builder object
     */ 
    auto &withTBWindows(std::chrono::microseconds _win_len,
                        std::chrono::microseconds _slide_len)
    {
        win_len = _win_len.count();
        slide_len = _slide_len.count();
        winType = Win_Type_t::TB;
        return *this;
    }

    /** 
     *  \brief Set the lateness for time-based windows
     *  
     *  \param _lateness (in microseconds)
     *  \return a reference to the builder object
     */ 
    auto &withLateness(std::chrono::microseconds _lateness)
    {
        if (winType != Win_Type_t::TB) { // check that time-based semantics is used
            std::cerr << RED << "WindFlow Error: lateness can be set only for time-based windows" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        lateness = _lateness.count();
        return *this;
    }

    /** 
     *  \brief Create the Ffat_Windows_GPU
     *  
     *  \return a new Ffat_Windows_GPU instance
     */ 
    auto build()
    {
        if constexpr (std::is_same<key_t, empty_key_t>::value) { // case without key
            static_assert(std::is_constructible<result_t, uint64_t>::value,
                "WindFlow Compilation Error - result type must be constructible with a uint64_t (Ffat_WindowsGPU_Builder):\n");
            auto k_t = [] (const tuple_t &t) -> empty_key_t {
                return empty_key_t();
            };
            return Ffat_Windows_GPU<lift_func_gpu_t, comb_func_gpu_t, decltype(k_t)>(lift_func,
                                                                                     comb_func,
                                                                                     k_t,
                                                                                     1, // parallelism fixed to 1
                                                                                     name,
                                                                                     numWinPerBatch,
                                                                                     win_len,
                                                                                     slide_len,
                                                                                     lateness,
                                                                                     winType);
        }
        else { // case with key
            static_assert(std::is_constructible<result_t, key_t, uint64_t>::value,
                "WindFlow Compilation Error - result type must be constructible with a key_t and uint64_t (Ffat_WindowsGPU_Builder):\n");
            return Ffat_Windows_GPU<lift_func_gpu_t, comb_func_gpu_t, keyextr_func_gpu_t>(lift_func,
                                                                                          comb_func,
                                                                                          key_extr,
                                                                                          1, // parallelism fixed to 1
                                                                                          name,
                                                                                          numWinPerBatch,
                                                                                          win_len,
                                                                                          slide_len,
                                                                                          lateness,
                                                                                          winType);
        }
    }
};

} // namespace wf

#endif
