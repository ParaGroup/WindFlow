/**************************************************************************************
 * 
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
 *  @file    builders.hpp
 *  @author  Gabriele Mencagli
 *  
 *  @brief Builder classes used to create WindFlow operators
 *  
 *  @section Builders-1 (Description)
 *  
 *  Builder classes used to create WindFlow operators.
 */ 

#ifndef BUILDERS_H
#define BUILDERS_H

/// includes
#include<chrono>
#include<functional>
#include<meta.hpp>
#if defined (__CUDACC__)
    #include<meta_gpu.hpp>
#endif
#include<basic.hpp>

namespace wf {

/** 
 *  \class Basic_Builder
 *  
 *  \brief Abstract class of a builder for basic operators
 *  
 *  Abstract class extended by all the builders of basic operators.
 */ 
template<template<class, class> class builder_t, class func_t, class key_t>
class Basic_Builder
{
protected:
    std::string name = "N/D"; // name of the operator
    size_t parallelism = 1; // parallelism of the operator
    size_t outputBatchSize = 0; // output batch size of the operator
    using closing_func_t = std::function<void(RuntimeContext&)>; // type of the closing functional logic
    closing_func_t closing_func = [](RuntimeContext &r) -> void { return; }; // closing functional logic

    // Constructor
    Basic_Builder() = default;

    // Copy Constructor
    Basic_Builder(const Basic_Builder &) = default;

public:
    /** 
     *  \brief Set the name of the operator
     *  
     *  \param _name of the operator
     *  \return a reference to the builder object
     */ 
    auto &withName(std::string _name)
    {
        name = _name;
        return static_cast<builder_t<func_t, key_t> &>(*this);
    }

    /** 
     *  \brief Set the parallelism of the operator
     *  
     *  \param _parallelism of the operator
     *  \return a reference to the builder object
     */ 
    auto &withParallelism(size_t _parallelism)
    {
        parallelism = _parallelism;
        return static_cast<builder_t<func_t, key_t> &>(*this);
    }

    /** 
     *  \brief Set the output batch size of the operator
     *  
     *  \param _outputBatchSize number of outputs per batch (zero means no batching)
     *  \return a reference to the builder object
     */ 
    auto &withOutputBatchSize(size_t _outputBatchSize)
    {
        outputBatchSize = _outputBatchSize;
        return static_cast<builder_t<func_t, key_t> &>(*this);
    }

    /** 
     *  \brief Set the closing functional logic used by the operator
     *  
     *  \param _closing_func closing functional logic (a function or any callable type)
     *  \return a reference to the builder object
     */ 
    template<typename closing_F_t>
    auto &withClosingFunction(closing_F_t _closing_func)
    {
        // static assert to check the signature
        static_assert(!std::is_same<decltype(check_closing_t(_closing_func)), std::false_type>::value,
            "WindFlow Compilation Error - unknown signature passed to withClosingFunction:\n"
            "  Candidate : void(RuntimeContext &)\n");
        closing_func = _closing_func;
        return static_cast<builder_t<func_t, key_t> &>(*this);
    }
};

/** 
 *  \class Source_Builder
 *  
 *  \brief Builder of the Source operator
 *  
 *  Builder class to ease the creation of the Source operator.
 */ 
template<typename source_func_t, typename key_t=empty_key_t>
class Source_Builder: public Basic_Builder<Source_Builder, source_func_t, key_t>
{
private:
    source_func_t func; // functional logic of the Source
    using result_t = decltype(get_result_t_Source(func)); // extracting the result_t type and checking the admissible signatures
    // static assert to check the signature of the Source functional logic
    static_assert(!(std::is_same<result_t, std::false_type>::value),
        "WindFlow Compilation Error - unknown signature passed to the Source_Builder:\n"
        "  Candidate 1 : void(Source_Shipper<result_t> &)\n"
        "  Candidate 2 : void(Source_Shipper<result_t> &, RuntimeContext &)\n");
    // static assert to check that the result_t type must be default constructible
    static_assert(std::is_default_constructible<result_t>::value,
        "WindFlow Compilation Error - result_t type must be default constructible (Source_Builder):\n");
    using source_t = Source<source_func_t>; // type of the Source to be created by the builder

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _func functional logic of the Source (a function or any callable type)
     */ 
    Source_Builder(source_func_t _func):
                   func(_func) {}

    /** 
     *  \brief Create the Source
     *  
     *  \return a new Source instance
     */ 
    auto build()
    {
        return source_t(func,
                        this->parallelism,
                        this->name,
                        this->outputBatchSize,
                        this->closing_func);
    }
};

/** 
 *  \class Filter_Builder
 *  
 *  \brief Builder of the Filter operator
 *  
 *  Builder class to ease the creation of the Filter operator.
 */ 
template<typename filter_func_t, typename key_t=empty_key_t>
class Filter_Builder: public Basic_Builder<Filter_Builder, filter_func_t, key_t>
{
private:
    template<typename T1, typename T2> friend class Filter_Builder;
    filter_func_t func; // functional logic of the Filter
    using tuple_t = decltype(get_tuple_t_Filter(func)); // extracting the tuple_t type and checking the admissible signatures
    // static assert to check the signature of the Filter functional logic
    static_assert(!std::is_same<tuple_t, std::false_type>::value,
        "WindFlow Compilation Error - unknown signature passed to the Filter_Builder:\n"
        "  Candidate 1 : bool(tuple_t &)\n"
        "  Candidate 2 : bool(tuple_t &, RuntimeContext &)\n");
    // static assert to check that the tuple_t type must be default constructible
    static_assert(std::is_default_constructible<tuple_t>::value,
        "WindFlow Compilation Error - tuple_t type must be default constructible (Filter_Builder):\n");
    using keyextr_func_t = std::function<key_t(const tuple_t&)>; // type of the key extractor
    using filter_t = Filter<filter_func_t, keyextr_func_t>; // type of the Filter to be created by the builder
    Routing_Mode_t input_routing_mode = Routing_Mode_t::FORWARD; // routing mode of inputs to the Filter
    keyextr_func_t key_extr = [](const tuple_t &t) -> key_t { return key_t(); }; // key extractor

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _func functional logic of the Filter (a function or any callable type)
     */ 
    Filter_Builder(filter_func_t _func):
                   func(_func) {}

    /** 
     *  \brief Set the KEYBY routing mode of inputs to the Filter
     *  
     *  \param _key_extr key extractor functional logic (a function or any callable type)
     *  \return a new builder object with the right key type
     */ 
    template<typename new_keyextr_func_t>
    auto withKeyBy(new_keyextr_func_t _key_extr)
    {
        // static assert to check the signature
        static_assert(!std::is_same<decltype(get_tuple_t_KeyExtr(_key_extr)), std::false_type>::value,
            "WindFlow Compilation Error - unknown signature passed to withKeyBy (Filter_Builder):\n"
            "  Candidate : key_t(const tuple_t &)\n");
        // static assert to check that the tuple_t type of the new key extractor is the right one
        static_assert(std::is_same<decltype(get_tuple_t_KeyExtr(_key_extr)), tuple_t>::value,
            "WindFlow Compilation Error - key extractor receives a wrong input type (Filter_Builder):\n");
        using new_key_t = decltype(get_key_t_KeyExtr(_key_extr)); // extract the key type
        // static assert to check the new_key_t type
        static_assert(!std::is_same<new_key_t, void>::value,
            "WindFlow Compilation Error - key type cannot be void (Filter_Builder):\n");
        // static assert to check that new_key_t is default constructible
        static_assert(std::is_default_constructible<new_key_t>::value,
            "WindFlow Compilation Error - key type must be default constructible (Filter_Builder):\n");
        if (input_routing_mode != Routing_Mode_t::FORWARD) {
            std::cerr << RED << "WindFlow Error: wrong use of withKeyBy() in the Filter_Builder" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        Filter_Builder<filter_func_t, new_key_t> new_builder(func);
        new_builder.name = this->name;
        new_builder.parallelism = this->parallelism;
        new_builder.input_routing_mode = Routing_Mode_t::KEYBY;
        new_builder.key_extr = _key_extr;
        new_builder.outputBatchSize = this->outputBatchSize;
        new_builder.closing_func = this->closing_func;
        return new_builder;
    }

    /** 
     *  \brief Set the BROADCAST routing mode of inputs to the Filter
     *  
     *  \return a reference to the builder object
     */ 
    auto &withBroadcast()
    {
        if (input_routing_mode != Routing_Mode_t::FORWARD) {
            std::cerr << RED << "WindFlow Error: wrong use of withBroadcast() in the Filter_Builder" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        input_routing_mode = Routing_Mode_t::BROADCAST;
        return *this;
    }

    /** 
     *  \brief Set the REBALANCING routing mode of inputs to the Filter
     *         (it forces a re-shuffling before this new operator)
     *  
     *  \return a reference to the builder object
     */ 
    auto &withRebalancing()
    {
        if (input_routing_mode == Routing_Mode_t::FORWARD) {
            std::cerr << RED << "WindFlow Error: wrong use of withRebalancing() in the Filter_Builder" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        input_routing_mode = Routing_Mode_t::REBALANCING;
        return *this;
    }

    /** 
     *  \brief Create the Filter
     *  
     *  \return a new Filter instance
     */ 
    auto build()
    {
        return filter_t(func,
                        key_extr,
                        this->parallelism,
                        this->name,
                        input_routing_mode,
                        this->outputBatchSize,
                        this->closing_func);
    }
};

/** 
 *  \class Map_Builder
 *  
 *  \brief Builder of the Map operator
 *  
 *  Builder class to ease the creation of the Map operator.
 */ 
template<typename map_func_t, typename key_t=empty_key_t>
class Map_Builder: public Basic_Builder<Map_Builder, map_func_t, key_t>
{
private:
    template<typename T1, typename T2> friend class Map_Builder;
    map_func_t func; // functional logic of the Map
    using tuple_t = decltype(get_tuple_t_Map(func)); // extracting the tuple_t type and checking the admissible signatures
    using result_t = decltype(get_result_t_Map(func)); // extracting the result_t type and checking the admissible signatures
    // static assert to check the signature of the Map functional logic
    static_assert(!(std::is_same<tuple_t, std::false_type>::value || std::is_same<result_t, std::false_type>::value),
        "WindFlow Compilation Error - unknown signature passed to the Map_Builder:\n"
        "  Candidate 1 : void(tuple_t &)\n"
        "  Candidate 2 : void(tuple_t &, RuntimeContext &)\n"
        "  Candidate 3 : result_t(const tuple_t &)\n"
        "  Candidate 4 : result_t(const tuple_t &, RuntimeContext &)\n");
    // static assert to check that the tuple_t type must be default constructible
    static_assert(std::is_default_constructible<tuple_t>::value,
        "WindFlow Compilation Error - tuple_t type must be default constructible (Map_Builder):\n");
    // static assert to check that the result_t type must be default constructible
    static_assert(std::is_default_constructible<result_t>::value,
        "WindFlow Compilation Error - result_t type must be default constructible (Map_Builder):\n");
    using keyextr_func_t = std::function<key_t(const tuple_t&)>; // type of the key extractor
    using map_t = Map<map_func_t, keyextr_func_t>; // type of the Map to be created by the builder
    Routing_Mode_t input_routing_mode = Routing_Mode_t::FORWARD; // routing mode of inputs to the Map
    keyextr_func_t key_extr = [](const tuple_t &t) -> key_t { return key_t(); }; // key extractor

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _func functional logic of the Map (a function or any callable type)
     */ 
    Map_Builder(map_func_t _func):
                func(_func) {}

    /** 
     *  \brief Set the KEYBY routing mode of inputs to the Map
     *  
     *  \param _key_extr key extractor functional logic (a function or any callable type)
     *  \return a new builder object with the right key type
     */ 
    template<typename new_keyextr_func_t>
    auto withKeyBy(new_keyextr_func_t _key_extr)
    {
        // static assert to check the signature
        static_assert(!std::is_same<decltype(get_tuple_t_KeyExtr(_key_extr)), std::false_type>::value,
            "WindFlow Compilation Error - unknown signature passed to withKeyBy (Map_Builder):\n"
            "  Candidate : key_t(const tuple_t &)\n");
        // static assert to check that the tuple_t type of the new key extractor is the right one
        static_assert(std::is_same<decltype(get_tuple_t_KeyExtr(_key_extr)), tuple_t>::value,
            "WindFlow Compilation Error - key extractor receives a wrong input type (Map_Builder):\n");
        using new_key_t = decltype(get_key_t_KeyExtr(_key_extr)); // extract the key type
        // static assert to check the new_key_t type
        static_assert(!std::is_same<new_key_t, void>::value,
            "WindFlow Compilation Error - key type cannot be void (Map_Builder):\n");
        // static assert to check that new_key_t is default constructible
        static_assert(std::is_default_constructible<new_key_t>::value,
            "WindFlow Compilation Error - key type must be default constructible (Map_Builder):\n");
        if (input_routing_mode != Routing_Mode_t::FORWARD) {
            std::cerr << RED << "WindFlow Error: wrong use of withKeyBy() in the Map_Builder" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        Map_Builder<map_func_t, new_key_t> new_builder(func);
        new_builder.name = this->name;
        new_builder.parallelism = this->parallelism;
        new_builder.input_routing_mode = Routing_Mode_t::KEYBY;
        new_builder.key_extr = _key_extr;
        new_builder.outputBatchSize = this->outputBatchSize;
        new_builder.closing_func = this->closing_func;
        return new_builder;
    }

    /** 
     *  \brief Set the BROADCAST routing mode of inputs to the Map
     *  
     *  \return a reference to the builder object
     */ 
    auto &withBroadcast()
    {
        if (input_routing_mode != Routing_Mode_t::FORWARD) {
            std::cerr << RED << "WindFlow Error: wrong use of withBroadcast() in the Map_Builder" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        input_routing_mode = Routing_Mode_t::BROADCAST;
        return *this;
    }

    /** 
     *  \brief Set the REBALANCING routing mode of inputs to the Map
     *         (it forces a re-shuffling before this new operator)
     *  
     *  \return a reference to the builder object
     */ 
    auto &withRebalancing()
    {
        if (input_routing_mode != Routing_Mode_t::FORWARD) {
            std::cerr << RED << "WindFlow Error: wrong use of withRebalancing() in the Map_Builder" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        input_routing_mode = Routing_Mode_t::REBALANCING;
        return *this;
    }

    /** 
     *  \brief Create the Map
     *  
     *  \return a new Map instance
     */ 
    auto build()
    {
        return map_t(func,
                     key_extr,
                     this->parallelism,
                     this->name,
                     input_routing_mode,
                     this->outputBatchSize,
                     this->closing_func);
    }
};

/** 
 *  \class FlatMap_Builder
 *  
 *  \brief Builder of the FlatMap operator
 *  
 *  Builder class to ease the creation of the FlatMap operator.
 */ 
template<typename flatmap_func_t, typename key_t=empty_key_t>
class FlatMap_Builder: public Basic_Builder<FlatMap_Builder, flatmap_func_t, key_t>
{
private:
    template<typename T1, typename T2> friend class FlatMap_Builder;
    flatmap_func_t func; // functional logic of the FlatMap
    using tuple_t = decltype(get_tuple_t_FlatMap(func)); // extracting the tuple_t type and checking the admissible signatures
    using result_t = decltype(get_result_t_FlatMap(func)); // extracting the result_t type and checking the admissible signatures
    // static assert to check the signature of the FlatMap functional logic
    static_assert(!(std::is_same<tuple_t, std::false_type>::value || std::is_same<result_t, std::false_type>::value),
        "WindFlow Compilation Error - unknown signature passed to the FlatMap_Builder:\n"
        "  Candidate 1 : void(const tuple_t &, Shipper<result_t> &)\n"
        "  Candidate 2 : void(const tuple_t &, Shipper<result_t> &, RuntimeContext &)\n");
    // static assert to check that the tuple_t type must be default constructible
    static_assert(std::is_default_constructible<tuple_t>::value,
        "WindFlow Compilation Error - tuple_t type must be default constructible (FlatMap_Builder):\n");
    // static assert to check that the result_t type must be default constructible
    static_assert(std::is_default_constructible<result_t>::value,
        "WindFlow Compilation Error - result_t type must be default constructible (FlatMap_Builder):\n");
    using keyextr_func_t = std::function<key_t(const tuple_t&)>; // type of the key extractor
    using flatmap_t = FlatMap<flatmap_func_t, keyextr_func_t>; // type of the FlatMap to be created by the builder
    Routing_Mode_t input_routing_mode = Routing_Mode_t::FORWARD; // routing mode of inputs to the FlatMap
    keyextr_func_t key_extr = [](const tuple_t &t) -> key_t { return key_t(); }; // key extractor

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _func functional logic of the FlatMap (a function or any callable type)
     */ 
    FlatMap_Builder(flatmap_func_t _func):
                    func(_func) {}

    /** 
     *  \brief Set the KEYBY routing mode of inputs to the FlatMap
     *  
     *  \param _key_extr key extractor functional logic (a function or any callable type)
     *  \return a new builder object with the right key type
     */ 
    template<typename new_keyextr_func_t>
    auto withKeyBy(new_keyextr_func_t _key_extr)
    {
        // static assert to check the signature
        static_assert(!std::is_same<decltype(get_tuple_t_KeyExtr(_key_extr)), std::false_type>::value,
            "WindFlow Compilation Error - unknown signature passed to withKeyBy (FlatMap_Builder):\n"
            "  Candidate : key_t(const tuple_t &)\n");
        // static assert to check that the tuple_t type of the new key extractor is the right one
        static_assert(std::is_same<decltype(get_tuple_t_KeyExtr(_key_extr)), tuple_t>::value,
            "WindFlow Compilation Error - key extractor receives a wrong input type (FlatMap_Builder):\n");
        using new_key_t = decltype(get_key_t_KeyExtr(_key_extr)); // extract the key type
        // static assert to check the new_key_t type
        static_assert(!std::is_same<new_key_t, void>::value,
            "WindFlow Compilation Error - key type cannot be void (FlatMap_Builder):\n");
        // static assert to check that new_key_t is default constructible
        static_assert(std::is_default_constructible<new_key_t>::value,
            "WindFlow Compilation Error - key type must be default constructible (FlatMap_Builder):\n");
        if (input_routing_mode != Routing_Mode_t::FORWARD) {
            std::cerr << RED << "WindFlow Error: wrong use of withKeyBy() in the FlatMap_Builder" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        FlatMap_Builder<flatmap_func_t, new_key_t> new_builder(func);
        new_builder.name = this->name;
        new_builder.parallelism = this->parallelism;
        new_builder.input_routing_mode = Routing_Mode_t::KEYBY;
        new_builder.key_extr = _key_extr;
        new_builder.outputBatchSize = this->outputBatchSize;
        new_builder.closing_func = this->closing_func;
        return new_builder;
    }

    /** 
     *  \brief Set the BROADCAST routing mode of inputs to the FlatMap
     *  
     *  \return a reference to the builder object
     */ 
    auto &withBroadcast()
    {
        if (input_routing_mode != Routing_Mode_t::FORWARD) {
            std::cerr << RED << "WindFlow Error: wrong use of withBroadcast() in the FlatMap_Builder" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        input_routing_mode = Routing_Mode_t::BROADCAST;
        return *this;
    }

    /** 
     *  \brief Set the REBALANCING routing mode of inputs to the FlatMap
     *         (it forces a re-shuffling before this new operator)
     *  
     *  \return a reference to the builder object
     */ 
    auto &withRebalancing()
    {
        if (input_routing_mode != Routing_Mode_t::FORWARD) {
            std::cerr << RED << "WindFlow Error: wrong use of withRebalancing() in the FlatMap_Builder" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        input_routing_mode = Routing_Mode_t::REBALANCING;
        return *this;
    }

    /** 
     *  \brief Create the FlatMap
     *  
     *  \return a new FlatMap instance
     */ 
    auto build()
    {
        return flatmap_t(func,
                         key_extr,
                         this->parallelism,
                         this->name,
                         input_routing_mode,
                         this->outputBatchSize,
                         this->closing_func);
    }
};

/** 
 *  \class Reduce_Builder
 *  
 *  \brief Builder of the Reduce operator
 *  
 *  Builder class to ease the creation of the Reduce operator.
 */ 
template<typename reduce_func_t, typename key_t=empty_key_t>
class Reduce_Builder: public Basic_Builder<Reduce_Builder, reduce_func_t, key_t>
{
private:
    template<typename T1, typename T2> friend class Reduce_Builder;
    reduce_func_t func; // functional logic of the Reduce
    using tuple_t = decltype(get_tuple_t_Reduce(func)); // extracting the tuple_t type and checking the admissible signatures
    using state_t = decltype(get_state_t_Reduce(func)); // extracting the state_t type and checking the admissible signatures
    // static assert to check the signature of the Reduce functional logic
    static_assert(!(std::is_same<tuple_t, std::false_type>::value || std::is_same<state_t, std::false_type>::value),
        "WindFlow Compilation Error - unknown signature passed to the Reduce_Builder:\n"
        "  Candidate 1 : void(const tuple_t &, state_t &)\n"
        "  Candidate 2 : void(const tuple_t &, state_t &, RuntimeContext &)\n");
    // static assert to check that the tuple_t type must be default constructible
    static_assert(std::is_default_constructible<tuple_t>::value,
        "WindFlow Compilation Error - tuple_t type must be default constructible (Reduce_Builder):\n");
    // static assert to check that the state_t type must be default constructible
    static_assert(std::is_default_constructible<state_t>::value,
        "WindFlow Compilation Error - state_t type must be default constructible (Reduce_Builder):\n");
    using keyextr_func_t = std::function<key_t(const tuple_t&)>; // type of the key extractor
    using reduce_t = Reduce<reduce_func_t, keyextr_func_t>; // type of the Reduce to be created by the builder
    keyextr_func_t key_extr = [](const tuple_t &t) -> key_t { return key_t(); }; // key extractor
    bool isKeyBySet = false; // true if a key extractor has been provided
    state_t initial_state; // initial state to be created one per key

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _func functional logic of the Reduce (a function or any callable type)
     */ 
    Reduce_Builder(reduce_func_t _func):
                   func(_func) {}

    /** 
     *  \brief Set the KEYBY routing mode of inputs to the Reduce
     *  
     *  \param _key_extr key extractor functional logic (a function or any callable type)
     *  \return a new builder object with the right key type
     */ 
    template<typename new_keyextr_func_t>
    auto withKeyBy(new_keyextr_func_t _key_extr)
    {
        // static assert to check the signature
        static_assert(!std::is_same<decltype(get_tuple_t_KeyExtr(_key_extr)), std::false_type>::value,
            "WindFlow Compilation Error - unknown signature passed to withKeyBy (Reduce_Builder):\n"
            "  Candidate : key_t(const tuple_t &)\n");
        // static assert to check that the tuple_t type of the new key extractor is the right one
        static_assert(std::is_same<decltype(get_tuple_t_KeyExtr(_key_extr)), tuple_t>::value,
            "WindFlow Compilation Error - key extractor receives a wrong input type (Reduce_Builder):\n");
        using new_key_t = decltype(get_key_t_KeyExtr(_key_extr)); // extract the key type
        // static assert to check the new_key_t type
        static_assert(!std::is_same<new_key_t, void>::value,
            "WindFlow Compilation Error - key type cannot be void (Reduce_Builder):\n");
        // static assert to check that new_key_t is default constructible
        static_assert(std::is_default_constructible<new_key_t>::value,
            "WindFlow Compilation Error - key type must be default constructible (Reduce_Builder):\n");
        Reduce_Builder<reduce_func_t, new_key_t> new_builder(func);
        new_builder.name = this->name;
        new_builder.parallelism = this->parallelism;
        new_builder.key_extr = _key_extr;
        new_builder.outputBatchSize = this->outputBatchSize;
        new_builder.closing_func = this->closing_func;
        new_builder.isKeyBySet = true;
        return new_builder;
    }

    /** 
     *  \brief Set the value of the initial state per key
     *  
     *  \param _initial_state value of the initial state to be created one per key
     *  \return a reference to the builder object
     */ 
    auto &withInitialState(state_t _initial_state)
    {
        initial_state = _initial_state;
        return *this;
    }

    /** 
     *  \brief Create the Reduce
     *  
     *  \return a new Reduce instance
     */ 
    auto build()
    {
        // check the presence of a key extractor
        if (!isKeyBySet && this->parallelism > 1) {
            std::cerr << RED << "WindFlow Error: Reduce with paralellism > 1 requires a key extractor" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        return reduce_t(func,
                        key_extr,
                        this->parallelism,
                        this->name,
                        this->outputBatchSize,
                        this->closing_func,
                        initial_state);
    }
};

/** 
 *  \class Basic_Win_Builder
 *  
 *  \brief Abstract class of a builder for window-based operators
 *  
 *  Abstract class extended by all the builders of window-based operators.
 */ 
template<template<class, class, class> class builder_t, class func1_t, class func2_t, class key_t>
class Basic_Win_Builder
{
protected:
    std::string name = "N/D"; // name of the operator
    size_t parallelism = 1; // parallelism of the operator
    size_t outputBatchSize = 0; // output batch size of the window-based operator
    using closing_func_t = std::function<void(RuntimeContext&)>; // type of the closing functional logic
    closing_func_t closing_func = [](RuntimeContext &r) -> void { return; }; // closing functional logic
    uint64_t win_len=0; // window length in number of tuples or in time units
    uint64_t slide_len=0; // slide length in number of tuples or in time units
    uint64_t lateness=0; // lateness in time units
    Win_Type_t winType=Win_Type_t::CB; // window type (CB or TB)

    // Constructor
    Basic_Win_Builder() = default;

    // Copy Constructor
    Basic_Win_Builder(const Basic_Win_Builder &) = default;

public:
    /** 
     *  \brief Set the name of the window-based operator
     *  
     *  \param _name of the window-based operator
     *  \return a reference to the builder object
     */ 
    auto &withName(std::string _name)
    {
        name = _name;
        return static_cast<builder_t<func1_t, func2_t, key_t> &>(*this);
    }

    /** 
     *  \brief Set the parallelism of the operator
     *  
     *  \param _parallelism of the operator
     *  \return a reference to the builder object
     */ 
    auto &withParallelism(size_t _parallelism)
    {
        parallelism = _parallelism;
        return static_cast<builder_t<func1_t, func2_t, key_t> &>(*this);
    }

    /** 
     *  \brief Set the output batch size of the window-based operator
     *  
     *  \param _outputBatchSize number of outputs per batch (zero means no batching)
     *  \return a reference to the builder object
     */ 
    auto &withOutputBatchSize(size_t _outputBatchSize)
    {
        outputBatchSize = _outputBatchSize;
        return static_cast<builder_t<func1_t, func2_t, key_t> &>(*this);
    }

    /** 
     *  \brief Set the closing functional logic used by the window-based operator
     *  
     *  \param _closing_func closing functional logic (a function or any callable type)
     *  \return a reference to the builder object
     */ 
    template<typename closing_F_t>
    auto &withClosingFunction(closing_F_t _closing_func)
    {
        // static assert to check the signature
        static_assert(!std::is_same<decltype(check_closing_t(_closing_func)), std::false_type>::value,
            "WindFlow Compilation Error - unknown signature passed to withClosingFunction:\n"
            "  Candidate : void(RuntimeContext &)\n");
        closing_func = _closing_func;
        return static_cast<builder_t<func1_t, func2_t, key_t> &>(*this);
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
        return static_cast<builder_t<func1_t, func2_t, key_t> &>(*this);
    }

    /** 
     *  \brief Set the configuration for time-based windows
     *  
     *  \param _win_len window length (in microseconds)
     *  \param _slide_len slide length (in microseconds)
     *  \return a reference to the builder object
     */ 
    auto &withTBWindows(std::chrono::microseconds _win_len, std::chrono::microseconds _slide_len)
    {
        win_len = _win_len.count();
        slide_len = _slide_len.count();
        winType = Win_Type_t::TB;
        return static_cast<builder_t<func1_t, func2_t, key_t> &>(*this);
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
        return static_cast<builder_t<func1_t, func2_t, key_t> &>(*this);
    }
};

/** 
 *  \class Keyed_Windows_Builder
 *  
 *  \brief Builder of the Keyed_Windows operator
 *  
 *  Builder class to ease the creation of the Keyed_Windows operator.
 */ 
template<typename win_func_t, typename stub_type_t=std::true_type, typename key_t=empty_key_t>
class Keyed_Windows_Builder: public Basic_Win_Builder<Keyed_Windows_Builder, win_func_t, stub_type_t, key_t>
{
private:
    template<typename T1, typename T2, typename T3> friend class Keyed_Windows_Builder;
    win_func_t func; // functional logic of the Keyed_Windows
    using tuple_t = decltype(get_tuple_t_Win(func)); // extracting the tuple_t type and checking the admissible signatures
    using result_t = decltype(get_result_t_Win(func)); // extracting the result_t type and checking the admissible signatures
    // static assert to check the signature of the Keyed_Windows functional logic
    static_assert(!(std::is_same<tuple_t, std::false_type>::value || std::is_same<result_t, std::false_type>::value),
        "WindFlow Compilation Error - unknown signature passed to the Keyed_Windows_Builder:\n"
        "  Candidate 1 : void(const Iterable<tuple_t> &, result_t &)\n"
        "  Candidate 2 : void(const Iterable<tuple_t> &, result_t &, RuntimeContext &)\n"
        "  Candidate 3 : void(const tuple_t &, result_t &)\n"
        "  Candidate 4 : void(const tuple_t &, result_t &, RuntimeContext &)\n");
    // static assert to check that the tuple_t type must be default constructible
    static_assert(std::is_default_constructible<tuple_t>::value,
        "WindFlow Compilation Error - tuple_t type must be default constructible (Keyed_Windows_Builder):\n");
    using keyextr_func_t = std::function<key_t(const tuple_t&)>; // type of the key extractor
    using keyed_wins_t = Keyed_Windows<win_func_t, keyextr_func_t>; // type of the Keyed_Windows to be created by the builder
    keyextr_func_t key_extr = [](const tuple_t &t) -> key_t { return key_t(); }; // key extractor
    bool isKeyBySet = false; // true if a key extractor has been provided

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _func functional logic of the Keyed_Windows (a function or any callable type)
     */ 
    Keyed_Windows_Builder(win_func_t _func):
                          func(_func) {}

    /** 
     *  \brief Set the KEYBY routing mode of inputs to the Keyed_Windows
     *  
     *  \param _key_extr key extractor functional logic (a function or any callable type)
     *  \return a new builder object with the right key type
     */ 
    template<typename new_keyextr_func_t>
    auto withKeyBy(new_keyextr_func_t _key_extr)
    {
        // static assert to check the signature
        static_assert(!std::is_same<decltype(get_tuple_t_KeyExtr(_key_extr)), std::false_type>::value,
            "WindFlow Compilation Error - unknown signature passed to withKeyBy (Keyed_Windows_Builder):\n"
            "  Candidate : key_t(const tuple_t &)\n");
        // static assert to check that the tuple_t type of the new key extractor is the right one
        static_assert(std::is_same<decltype(get_tuple_t_KeyExtr(_key_extr)), tuple_t>::value,
            "WindFlow Compilation Error - key extractor receives a wrong input type (Keyed_Windows_Builder):\n");
        using new_key_t = decltype(get_key_t_KeyExtr(_key_extr)); // extract the key type
        // static assert to check the new_key_t type
        static_assert(!std::is_same<new_key_t, void>::value,
            "WindFlow Compilation Error - key type cannot be void (Keyed_Windows_Builder):\n");
        // static assert to check that new_key_t is default constructible
        static_assert(std::is_default_constructible<new_key_t>::value,
            "WindFlow Compilation Error - key type must be default constructible (Keyed_Windows_Builder):\n");
        Keyed_Windows_Builder<win_func_t, stub_type_t, new_key_t> new_builder(func);
        new_builder.name = this->name;
        new_builder.parallelism = this->parallelism;
        new_builder.key_extr = _key_extr;
        new_builder.outputBatchSize = this->outputBatchSize;
        new_builder.closing_func = this->closing_func;
        new_builder.isKeyBySet = true;
        new_builder.win_len = this->win_len;
        new_builder.slide_len = this->slide_len;
        new_builder.lateness = this->lateness;
        new_builder.winType = this->winType;
        return new_builder;
    }

    /** 
     *  \brief Create the Keyed_Windows
     *  
     *  \return a new Keyed_Windows instance
     */ 
    auto build()
    {
        // static asserts to check that result_t is properly constructible
        if constexpr (std::is_same<key_t, empty_key_t>::value) { // case without key
            static_assert(std::is_constructible<result_t, uint64_t>::value,
                "WindFlow Compilation Error - result_t type must be constructible with a uin64_t (Keyed_Windows_Builder):\n");
        }
        else { // case with key
            static_assert(std::is_constructible<result_t, key_t, uint64_t>::value,
                "WindFlow Compilation Error - result_t type must be constructible with a key_t and uint64_t (Keyed_Windows_Builder):\n");
        }
        // check the presence of a key extractor
        if (!isKeyBySet && this->parallelism > 1) {
            std::cerr << RED << "WindFlow Error: Keyed_Windows with paralellism > 1 requires a key extractor" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        return keyed_wins_t(func,
                            key_extr,
                            this->parallelism,
                            this->name,
                            this->outputBatchSize,
                            this->closing_func,
                            this->win_len,
                            this->slide_len,
                            this->lateness,
                            this->winType);
    }
};

/** 
 *  \class Parallel_Windows_Builder
 *  
 *  \brief Builder of the Parallel_Windows operator
 *  
 *  Builder class to ease the creation of the Parallel_Windows operator.
 */ 
template<typename win_func_t, typename stub_type_t=std::true_type, typename key_t=empty_key_t>
class Parallel_Windows_Builder: public Basic_Win_Builder<Parallel_Windows_Builder, win_func_t, stub_type_t, key_t>
{
private:
    template<typename T1, typename T2, typename T3> friend class Parallel_Windows_Builder;
    win_func_t func; // functional logic of the Parallel_Windows
    using tuple_t = decltype(get_tuple_t_Win(func)); // extracting the tuple_t type and checking the admissible signatures
    using result_t = decltype(get_result_t_Win(func)); // extracting the result_t type and checking the admissible signatures
    // static assert to check the signature of the Parallel_Windows functional logic
    static_assert(!(std::is_same<tuple_t, std::false_type>::value || std::is_same<result_t, std::false_type>::value),
        "WindFlow Compilation Error - unknown signature passed to the Parallel_Windows_Builder:\n"
        "  Candidate 1 : void(const Iterable<tuple_t> &, result_t &)\n"
        "  Candidate 2 : void(const Iterable<tuple_t> &, result_t &, RuntimeContext &)\n"
        "  Candidate 3 : void(const tuple_t &, result_t &)\n"
        "  Candidate 4 : void(const tuple_t &, result_t &, RuntimeContext &)\n");
    // static assert to check that the tuple_t type must be default constructible
    static_assert(std::is_default_constructible<tuple_t>::value,
        "WindFlow Compilation Error - tuple_t type must be default constructible (Parallel_Windows_Builder):\n");
    using keyextr_func_t = std::function<key_t(const tuple_t&)>; // type of the key extractor
    using par_wins_t = Parallel_Windows<win_func_t, keyextr_func_t>; // type of the Parallel_Windows to be created by the builder
    keyextr_func_t key_extr = [](const tuple_t &t) -> key_t { return key_t(); }; // key extractor

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _func functional logic of the Parallel_Windows (a function or any callable type)
     */ 
    Parallel_Windows_Builder(win_func_t _func):
                             func(_func) {}

    /** 
     *  \brief Set the KEYBY routing mode of inputs to the Parallel_Windows
     *  
     *  \param _key_extr key extractor functional logic (a function or any callable type)
     *  \return a new builder object with the right key type
     */ 
    template<typename new_keyextr_func_t>
    auto withKeyBy(new_keyextr_func_t _key_extr)
    {
        // static assert to check the signature
        static_assert(!std::is_same<decltype(get_tuple_t_KeyExtr(_key_extr)), std::false_type>::value,
            "WindFlow Compilation Error - unknown signature passed to withKeyBy (Parallel_Windows_Builder):\n"
            "  Candidate : key_t(const tuple_t &)\n");
        // static assert to check that the tuple_t type of the new key extractor is the right one
        static_assert(std::is_same<decltype(get_tuple_t_KeyExtr(_key_extr)), tuple_t>::value,
            "WindFlow Compilation Error - key extractor receives a wrong input type (Parallel_Windows_Builder):\n");
        using new_key_t = decltype(get_key_t_KeyExtr(_key_extr)); // extract the key type
        // static assert to check the new_key_t type
        static_assert(!std::is_same<new_key_t, void>::value,
            "WindFlow Compilation Error - key type cannot be void (Parallel_Windows_Builder):\n");
        // static assert to check that new_key_t is default constructible
        static_assert(std::is_default_constructible<new_key_t>::value,
            "WindFlow Compilation Error - key type must be default constructible (Parallel_Windows_Builder):\n");
        Parallel_Windows_Builder<win_func_t, stub_type_t, new_key_t> new_builder(func);
        new_builder.name = this->name;
        new_builder.parallelism = this->parallelism;
        new_builder.key_extr = _key_extr;
        new_builder.outputBatchSize = this->outputBatchSize;
        new_builder.closing_func = this->closing_func;
        new_builder.win_len = this->win_len;
        new_builder.slide_len = this->slide_len;
        new_builder.lateness = this->lateness;
        new_builder.winType = this->winType;
        return new_builder;
    }

    /** 
     *  \brief Create the Parallel_Windows
     *  
     *  \return a new Parallel_Windows instance
     */ 
    auto build()
    {
        // static asserts to check that result_t is properly constructible
        if constexpr (std::is_same<key_t, empty_key_t>::value) { // case without key
            static_assert(std::is_constructible<result_t, uint64_t>::value,
                "WindFlow Compilation Error - result_t type must be constructible with a uint64_t (Parallel_Windows_Builder):\n");
        }
        else { // case with key
            static_assert(std::is_constructible<result_t, key_t, uint64_t>::value,
                "WindFlow Compilation Error - result_t type must be constructible with a key_t and uint64_t (Parallel_Windows_Builder):\n");            
        }
        return par_wins_t(func,
                          key_extr,
                          this->parallelism,
                          this->name,
                          this->outputBatchSize,
                          this->closing_func,
                          this->win_len,
                          this->slide_len,
                          this->lateness,
                          this->winType);
    }
};

/** 
 *  \class Paned_Windows_Builder
 *  
 *  \brief Builder of the Paned_Windows operator
 *  
 *  Builder class to ease the creation of the Paned_Windows operator.
 */ 
template<typename plq_func_t, typename wlq_func_t, typename key_t=empty_key_t>
class Paned_Windows_Builder: public Basic_Win_Builder<Paned_Windows_Builder, plq_func_t, wlq_func_t, key_t>
{
private:
    template<typename T1, typename T3, typename T2> friend class Paned_Windows_Builder;
    plq_func_t plq_func; // functional logic of the PLQ stage
    wlq_func_t wlq_func; // functional logic of the WLQ stage
    using tuple_t = decltype(get_tuple_t_Win(plq_func)); // extracting the tuple_t type and checking the admissible signatures
    using result_t = decltype(get_result_t_Win(wlq_func)); // extracting the result_t type and checking the admissible signatures
    // static asserts to check the signature
    static_assert(!std::is_same<tuple_t, std::false_type>::value,
        "WindFlow Compilation Error - unknown signature passed to the Paned_Windows_Builder (first argument, PLQ logic):\n"
        "  Candidate 1 : void(const Iterable<tuple_t> &, tuple_t &)\n"
        "  Candidate 2 : void(const Iterable<tuple_t> &, tuple_t &, RuntimeContext &)\n"
        "  Candidate 3 : void(const tuple_t &, tuple_t &)\n"
        "  Candidate 4 : void(const tuple_t &, tuple_t &, RuntimeContext &)\n");
    static_assert(!std::is_same<result_t, std::false_type>::value,
        "WindFlow Compilation Error - unknown signature passed to the Paned_Windows_Builder (second argument, WLQ logic):\n"
        "  Candidate 1 : void(const Iterable<tuple_t> &, result_t &)\n"
        "  Candidate 2 : void(const Iterable<tuple_t> &, result_t &, RuntimeContext &)\n"
        "  Candidate 3 : void(const tuple_t &, result_t &)\n"
        "  Candidate 4 : void(const tuple_t &, result_t &, RuntimeContext &)\n");
    static_assert(std::is_same<decltype(get_tuple_t_Win(plq_func)), decltype(get_result_t_Win(plq_func))>::value &&
                  std::is_same<decltype(get_result_t_Win(plq_func)), decltype(get_tuple_t_Win(wlq_func))>::value,
        "WindFlow Compilation Error - type mismatch in the Paned_Windows_Builder\n");
    using keyextr_func_t = std::function<key_t(const tuple_t&)>; // type of the key extractor
    using paned_wins_t = Paned_Windows<plq_func_t, wlq_func_t, keyextr_func_t>; // type of the Paned_Windows to be created by the builder
    size_t plq_parallelism = 1; // parallelism of the PLQ stage
    size_t wlq_parallelism = 1; // parallelism of the WLQ stage
    keyextr_func_t key_extr = [](const tuple_t &t) -> key_t { return key_t(); }; // key extractor

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _plq_func functional logic of the PLQ stage (a function or any callable type)
     *  \param _wlq_func functional logic of the WLQ stage (a function or any callable type)
     */ 
    Paned_Windows_Builder(plq_func_t _plq_func, wlq_func_t _wlq_func):
                          plq_func(_plq_func), wlq_func(_wlq_func) {}

    /// Delete withParallelism method
    Paned_Windows_Builder<plq_func_t, wlq_func_t, key_t> &withParallelism(size_t _parallelism) = delete;

    /** 
     *  \brief Set the parallelisms of the Paned_Windows
     *  
     *  \param _plq_parallelism of the PLQ stage
     *  \param _wlq_parallelism of the WLQ stage
     *  \return a reference to the builder object
     */ 
    auto &withParallelism(size_t _plq_parallelism, size_t _wlq_parallelism)
    {
        plq_parallelism = _plq_parallelism;
        wlq_parallelism = _wlq_parallelism;
        return *this;
    }

    /** 
     *  \brief Set the KEYBY routing mode of inputs to the Paned_Windows
     *  
     *  \param _key_extr key extractor functional logic (a function or any callable type)
     *  \return a new builder object with the right key type
     */ 
    template<typename new_keyextr_func_t>
    auto withKeyBy(new_keyextr_func_t _key_extr)
    {
        // static assert to check the signature
        static_assert(!std::is_same<decltype(get_tuple_t_KeyExtr(_key_extr)), std::false_type>::value,
            "WindFlow Compilation Error - unknown signature passed to withKeyBy (Paned_Windows_Builder):\n"
            "  Candidate : key_t(const tuple_t &)\n");
        // static assert to check that the tuple_t type of the new key extractor is the right one
        static_assert(std::is_same<decltype(get_tuple_t_KeyExtr(_key_extr)), tuple_t>::value,
            "WindFlow Compilation Error - key extractor receives a wrong input type (Paned_Windows_Builder):\n");
        using new_key_t = decltype(get_key_t_KeyExtr(_key_extr)); // extract the key type
        // static assert to check the new_key_t type
        static_assert(!std::is_same<new_key_t, void>::value,
            "WindFlow Compilation Error - key type cannot be void (Paned_Windows_Builder):\n");
        // static assert to check that new_key_t is default constructible
        static_assert(std::is_default_constructible<new_key_t>::value,
            "WindFlow Compilation Error - key type must be default constructible (Paned_Windows_Builder):\n");
        Paned_Windows_Builder<plq_func_t, wlq_func_t, new_key_t> new_builder(plq_func, wlq_func);
        new_builder.name = this->name;
        new_builder.plq_parallelism = plq_parallelism;
        new_builder.wlq_parallelism = wlq_parallelism;
        new_builder.key_extr = _key_extr;
        new_builder.outputBatchSize = this->outputBatchSize;
        new_builder.closing_func = this->closing_func;
        new_builder.win_len = this->win_len;
        new_builder.slide_len = this->slide_len;
        new_builder.lateness = this->lateness;
        new_builder.winType = this->winType;
        return new_builder;
    }

    /** 
     *  \brief Create the Paned_Windows
     *  
     *  \return a new Paned_Windows instance
     */ 
    auto build()
    {
        // static asserts to check that tuple_t and result_t are properly constructible
        if constexpr (std::is_same<key_t, empty_key_t>::value) { // case without key
            static_assert(std::is_constructible<tuple_t, uint64_t>::value,
                "WindFlow Compilation Error - tuple_t type must be constructible with a uint64_t (Paned_Windows_Builder):\n");
            static_assert(std::is_constructible<result_t, uint64_t>::value,
                "WindFlow Compilation Error - result_t type must be constructible with a uint64_t (Paned_Windows_Builder):\n");
        }
        else { // case with key
            static_assert(std::is_constructible<tuple_t, key_t, uint64_t>::value,
                "WindFlow Compilation Error - tuple_t type must be constructible with a key_t and uint64_t (Paned_Windows_Builder):\n");
            static_assert(std::is_constructible<result_t, key_t, uint64_t>::value,
                "WindFlow Compilation Error - result_t type must be constructible with a key_t and uint64_t (Paned_Windows_Builder):\n");
        }
        return paned_wins_t(plq_func,
                            wlq_func,
                            key_extr,
                            plq_parallelism,
                            wlq_parallelism,
                            this->name,
                            this->outputBatchSize,
                            this->closing_func,
                            this->win_len,
                            this->slide_len,
                            this->lateness,
                            this->winType);
    }
};

/** 
 *  \class MapReduce_Windows_Builder
 *  
 *  \brief Builder of the MapReduce_Windows operator
 *  
 *  Builder class to ease the creation of the MapReduce_Windows operator.
 */ 
template<typename map_func_t, typename reduce_func_t, typename key_t=empty_key_t>
class MapReduce_Windows_Builder: public Basic_Win_Builder<MapReduce_Windows_Builder, map_func_t, reduce_func_t, key_t>
{
private:
    template<typename T1, typename T3, typename T2> friend class MapReduce_Windows_Builder;
    map_func_t map_func; // functional logic of the MAP stage
    reduce_func_t reduce_func; // functional logic of the REDUCE stage
    using tuple_t = decltype(get_tuple_t_Win(map_func)); // extracting the tuple_t type and checking the admissible signatures
    using result_t = decltype(get_result_t_Win(reduce_func)); // extracting the result_t type and checking the admissible signatures
    // static asserts to check the signature
    static_assert(!std::is_same<tuple_t, std::false_type>::value,
        "WindFlow Compilation Error - unknown signature passed to the MapReduce_Windows_Builder (first argument, MAP logic):\n"
        "  Candidate 1 : void(const Iterable<tuple_t> &, tuple_t &)\n"
        "  Candidate 2 : void(const Iterable<tuple_t> &, tuple_t &, RuntimeContext &)\n"
        "  Candidate 3 : void(const tuple_t &, tuple_t &)\n"
        "  Candidate 4 : void(const tuple_t &, tuple_t &, RuntimeContext &)\n");
    static_assert(!std::is_same<result_t, std::false_type>::value,
        "WindFlow Compilation Error - unknown signature passed to the MapReduce_Windows_Builder (second argument, REDUCE logic):\n"
        "  Candidate 1 : void(const Iterable<tuple_t> &, result_t &)\n"
        "  Candidate 2 : void(const Iterable<tuple_t> &, result_t &, RuntimeContext &)\n"
        "  Candidate 3 : void(const tuple_t &, result_t &)\n"
        "  Candidate 4 : void(const tuple_t &, result_t &, RuntimeContext &)\n");
    static_assert(std::is_same<decltype(get_tuple_t_Win(map_func)), decltype(get_result_t_Win(map_func))>::value &&
                  std::is_same<decltype(get_result_t_Win(map_func)), decltype(get_tuple_t_Win(reduce_func))>::value,
        "WindFlow Compilation Error - type mismatch in the MapReduce_Windows_Builder\n");
    using keyextr_func_t = std::function<key_t(const tuple_t&)>; // type of the key extractor
    using mr_wins_t = MapReduce_Windows<map_func_t, reduce_func_t, keyextr_func_t>; // type of the MapReduce_Windows to be created by the builder
    size_t map_parallelism = 1; // parallelism of the MAP stage
    size_t reduce_parallelism = 1; // parallelism of the REDUCE stage
    keyextr_func_t key_extr = [](const tuple_t &t) -> key_t { return key_t(); }; // key extractor

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _map_func functional logic of the MAP stage (a function or any callable type)
     *  \param _reduce_func functional logic of the REDUCE stage (a function or any callable type)
     */ 
    MapReduce_Windows_Builder(map_func_t _map_func, reduce_func_t _reduce_func):
                              map_func(_map_func), reduce_func(_reduce_func) {}

    /// Delete withParallelism method
    MapReduce_Windows_Builder<map_func_t, reduce_func_t, key_t> &withParallelism(size_t _parallelism) = delete;

    /** 
     *  \brief Set the parallelisms of the MapReduce_Windows
     *  
     *  \param _map_parallelism of the MAP stage
     *  \param _reduce_parallelism of the REDUCE stage
     *  \return a reference to the builder object
     */ 
    auto &withParallelism(size_t _map_parallelism, size_t _reduce_parallelism)
    {
        map_parallelism = _map_parallelism;
        reduce_parallelism = _reduce_parallelism;
        return *this;
    }

    /** 
     *  \brief Set the KEYBY routing mode of inputs to the MapReduce_Windows
     *  
     *  \param _key_extr key extractor functional logic (a function or any callable type)
     *  \return a new builder object with the right key type
     */ 
    template<typename new_keyextr_func_t>
    auto withKeyBy(new_keyextr_func_t _key_extr)
    {
        // static assert to check the signature
        static_assert(!std::is_same<decltype(get_tuple_t_KeyExtr(_key_extr)), std::false_type>::value,
            "WindFlow Compilation Error - unknown signature passed to withKeyBy (MapReduce_Windows_Builder):\n"
            "  Candidate : key_t(const tuple_t &)\n");
        // static assert to check that the tuple_t type of the new key extractor is the right one
        static_assert(std::is_same<decltype(get_tuple_t_KeyExtr(_key_extr)), tuple_t>::value,
            "WindFlow Compilation Error - key extractor receives a wrong input type (MapReduce_Windows_Builder):\n");
        using new_key_t = decltype(get_key_t_KeyExtr(_key_extr)); // extract the key type
        // static assert to check the new_key_t type
        static_assert(!std::is_same<new_key_t, void>::value,
            "WindFlow Compilation Error - key type cannot be void (MapReduce_Windows_Builder):\n");
        // static assert to check that new_key_t is default constructible
        static_assert(std::is_default_constructible<new_key_t>::value,
            "WindFlow Compilation Error - key type must be default constructible (MapReduce_Windows_Builder):\n");
        MapReduce_Windows_Builder<map_func_t, reduce_func_t, new_key_t> new_builder(map_func, reduce_func);
        new_builder.name = this->name;
        new_builder.map_parallelism = map_parallelism;
        new_builder.reduce_parallelism = reduce_parallelism;
        new_builder.key_extr = _key_extr;
        new_builder.outputBatchSize = this->outputBatchSize;
        new_builder.closing_func = this->closing_func;
        new_builder.win_len = this->win_len;
        new_builder.slide_len = this->slide_len;
        new_builder.lateness = this->lateness;
        new_builder.winType = this->winType;
        return new_builder;
    }

    /** 
     *  \brief Create the MapReduce_Windows
     *  
     *  \return a new MapReduce_Windows instance
     */ 
    auto build()
    {
        // static asserts to check that tuple_t and result_t are properly constructible
        if constexpr (std::is_same<key_t, empty_key_t>::value) { // case without key
            static_assert(std::is_constructible<tuple_t, uint64_t>::value,
                "WindFlow Compilation Error - tuple_t type must be constructible with a uint64_t (Paned_Windows_Builder):\n");
            static_assert(std::is_constructible<result_t, uint64_t>::value,
                "WindFlow Compilation Error - result_t type must be constructible with a uint64_t (Paned_Windows_Builder):\n");
        }
        else { // case with key
            static_assert(std::is_constructible<tuple_t, key_t, uint64_t>::value,
                "WindFlow Compilation Error - tuple_t type must be constructible with a key_t and uint64_t (Paned_Windows_Builder):\n");
            static_assert(std::is_constructible<result_t, key_t, uint64_t>::value,
                "WindFlow Compilation Error - result_t type must must be constructible with a key_t and uint64_t (Paned_Windows_Builder):\n");          
        }
        return mr_wins_t(map_func,
                         reduce_func,
                         key_extr,
                         map_parallelism,
                         reduce_parallelism,
                         this->name,
                         this->outputBatchSize,
                         this->closing_func,
                         this->win_len,
                         this->slide_len,
                         this->lateness,
                         this->winType);
    }
};

/** 
 *  \class Ffat_Windows_Builder
 *  
 *  \brief Builder of the Ffat_Windows operator
 *  
 *  Builder class to ease the creation of the Ffat_Windows operator.
 */ 
template<typename lift_func_t, typename comb_func_t, typename key_t=empty_key_t>
class Ffat_Windows_Builder: public Basic_Win_Builder<Ffat_Windows_Builder, lift_func_t, comb_func_t, key_t>
{
private:
    template<typename T1, typename T2, typename T3> friend class Ffat_Windows_Builder;
    lift_func_t lift_func; // lift functional logic of the Ffat_Windows
    comb_func_t comb_func; // combine functional logic of the Ffat_Windows
    using tuple_t = decltype(get_tuple_t_Lift(lift_func)); // extracting the tuple_t type and checking the admissible signatures
    using result_t = decltype(get_result_t_Lift(lift_func)); // extracting the result_t type and checking the admissible signatures
    // static assert to check the signature of the Ffat_Windows_Builder functional logic
    static_assert(!(std::is_same<tuple_t, std::false_type>::value || std::is_same<result_t, std::false_type>::value),
        "WindFlow Compilation Error - unknown signature passed to the Ffat_Windows_Builder (first argument, lift logic):\n"
        "  Candidate 1 : void(const tuple_t &, result_t &)\n"
        "  Candidate 2 : void(const tuple_t &, result_t &, RuntimeContext &)\n");
    using result_t2 = decltype(get_tuple_t_Comb(comb_func));
    static_assert(!(std::is_same<std::false_type, result_t2>::value),
        "WindFlow Compilation Error - unknown signature passed to the Ffat_Windows_Builder (second argument, combine logic):\n"
        "  Candidate 1 : void(const result_t &, const result_t &, result_t &)\n"
        "  Candidate 2 : void(const result_t &, const result_t &, result_t &, RuntimeContext &)\n");
    static_assert(std::is_same<result_t, result_t2>::value,
        "WindFlow Compilation Error - type mismatch in the Ffat_Windows_Builder\n");
    // static assert to check that the tuple_t type must be default constructible
    static_assert(std::is_default_constructible<tuple_t>::value,
        "WindFlow Compilation Error - tuple_t type must be default constructible (Ffat_Windows_Builder):\n");
    using keyextr_func_t = std::function<key_t(const tuple_t&)>; // type of the key extractor
    using ffat_agg_t = Ffat_Windows<lift_func_t, comb_func_t, keyextr_func_t>; // type of the Ffat_Windows to be created by the builder
    keyextr_func_t key_extr = [](const tuple_t &t) -> key_t { return key_t(); }; // key extractor
    bool isKeyBySet = false; // true if a key extractor has been provided

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _lift_func lift functional logic of the Ffat_Windows (a function or any callable type)
     *  \param _comb_func combine functional logic of the Ffat_Windows (a function or any callable type)
     */ 
    Ffat_Windows_Builder(lift_func_t _lift_func, comb_func_t _comb_func):
                         lift_func(_lift_func), comb_func(_comb_func) {}

    /** 
     *  \brief Set the KEYBY routing mode of inputs to the Ffat_Windows
     *  
     *  \param _key_extr key extractor functional logic (a function or any callable type)
     *  \return a new builder object with the right key type
     */ 
    template<typename new_keyextr_func_t>
    auto withKeyBy(new_keyextr_func_t _key_extr)
    {
        // static assert to check the signature
        static_assert(!std::is_same<decltype(get_tuple_t_KeyExtr(_key_extr)), std::false_type>::value,
            "WindFlow Compilation Error - unknown signature passed to withKeyBy (Ffat_Windows_Builder):\n"
            "  Candidate : key_t(const tuple_t &)\n");
        // static assert to check that the tuple_t type of the new key extractor is the right one
        static_assert(std::is_same<decltype(get_tuple_t_KeyExtr(_key_extr)), tuple_t>::value,
            "WindFlow Compilation Error - key extractor receives a wrong input type (Ffat_Windows_Builder):\n");
        using new_key_t = decltype(get_key_t_KeyExtr(_key_extr)); // extract the key type
        // static assert to check the new_key_t type
        static_assert(!std::is_same<new_key_t, void>::value,
            "WindFlow Compilation Error - key type cannot be void (Ffat_Windows_Builder):\n");
        // static assert to check that new_key_t is default constructible
        static_assert(std::is_default_constructible<new_key_t>::value,
            "WindFlow Compilation Error - key type must be default constructible (Ffat_Windows_Builder):\n");
        Ffat_Windows_Builder<lift_func_t, comb_func_t, new_key_t> new_builder(lift_func, comb_func);
        new_builder.name = this->name;
        new_builder.parallelism = this->parallelism;
        new_builder.key_extr = _key_extr;
        new_builder.outputBatchSize = this->outputBatchSize;
        new_builder.closing_func = this->closing_func;
        new_builder.isKeyBySet = true;
        new_builder.win_len = this->win_len;
        new_builder.slide_len = this->slide_len;
        new_builder.lateness = this->lateness;
        new_builder.winType = this->winType;
        return new_builder;
    }

    /** 
     *  \brief Create the Ffat_Windows
     *  
     *  \return a new Ffat_Windows instance
     */ 
    auto build()
    {
        // static asserts to check that result_t is properly constructible
        if constexpr (std::is_same<key_t, empty_key_t>::value) { // case without key
            static_assert(std::is_constructible<result_t, uint64_t>::value,
                "WindFlow Compilation Error - result type must be constructible with a uint64_t (Ffat_Windows_Builder):\n");
        }
        else { // case with key
            static_assert(std::is_constructible<result_t, key_t, uint64_t>::value,
                "WindFlow Compilation Error - result type must be constructible with a key_t and uint64_t (Ffat_Windows_Builder):\n");            
        }
        // check the presence of a key extractor
        if (!isKeyBySet && this->parallelism > 1) {
            std::cerr << RED << "WindFlow Error: Ffat_Windows with parallelism > 1 requires a key extractor" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        return ffat_agg_t(lift_func,
                          comb_func,
                          key_extr,
                          this->parallelism,
                          this->name,
                          this->outputBatchSize,
                          this->closing_func,
                          this->win_len,
                          this->slide_len,
                          this->lateness,
                          this->winType);
    }
};

/** 
 *  \class Interval_Join_Builder
 *  
 *  \brief Builder of the Interval Join operator
 *  
 *  Builder class to ease the creation of the Interval Join operator.
 */ 
template<typename join_func_t, typename key_t=empty_key_t>
class Interval_Join_Builder: public Basic_Builder<Interval_Join_Builder, join_func_t, key_t>
{
private:
    template<typename T1, typename T2> friend class Interval_Join_Builder;
    join_func_t func; // functional logic of the Interval Join
    using tuple_t = decltype(get_tuple_t_Join(func)); // extracting the tuple_t type and checking the admissible signatures
    using result_t = decltype(get_result_t_Join(func)); // extracting the result_t type and checking the admissible signatures
    // static assert to check the signature of the Interval Join functional logic
    static_assert(!std::is_same<tuple_t, std::false_type>::value || std::is_same<result_t, std::false_type>::value,
        "WindFlow Compilation Error - unknown signature passed to the Interval_Join_Builder:\n"
        "  Candidate 1 : std::optional<result_t> (const tuple_t &, const tuple_t &)\n"
        "  Candidate 2 : std::optional<result_t> (const tuple_t &, const tuple_t &, RuntimeContext &)\n");
    // static assert to check that the tuple_t type must be default constructible
    static_assert(std::is_default_constructible<tuple_t>::value,
        "WindFlow Compilation Error - tuple_t type must be default constructible (Interval_Join_Builder):\n");
    // static assert to check that the result_t type must be default constructible
    static_assert(std::is_default_constructible<result_t>::value,
        "WindFlow Compilation Error - result_t type must be default constructible (Interval_Join_Builder):\n");
    using keyextr_func_t = std::function<key_t(const tuple_t&)>; // type of the key extractor
    using join_t = Interval_Join<join_func_t, keyextr_func_t>; // type of the Interval Join to be created by the builder
    Routing_Mode_t input_routing_mode = Routing_Mode_t::FORWARD; // routing mode of inputs to the Interval_Join
    keyextr_func_t key_extr = [](const tuple_t &t) -> key_t { return key_t(); }; // key extractor
    bool isKeyBySet = false; // true if a key extractor has been provided
    int64_t lower_bound=0; // lower bound of the interval
    int64_t upper_bound=0; // upper bound of the interval
    Interval_Join_Mode_t join_mode = Interval_Join_Mode_t::NONE;


public:
    /** 
     *  \brief Constructor
     *  
     *  \param _func functional logic of the Interval Join (a function or any callable type)
     */ 
    Interval_Join_Builder(join_func_t _func):
                func(_func) {}

    /** 
     *  \brief Set the KEYBY routing mode of inputs to the Interval Join
     *  
     *  \param _key_extr key extractor functional logic (a function or any callable type)
     *  \return a new builder object with the right key type
     */ 
    template<typename new_keyextr_func_t>
    auto withKeyBy(new_keyextr_func_t _key_extr)
    {
        // static assert to check the signature
        static_assert(!std::is_same<decltype(get_tuple_t_KeyExtr(_key_extr)), std::false_type>::value,
            "WindFlow Compilation Error - unknown signature passed to withKeyBy (Interval_Join_Builder):\n"
            "  Candidate : key_t(const tuple_t &)\n");
        // static assert to check that the tuple_t type of the new key extractor is the right one
        static_assert(std::is_same<decltype(get_tuple_t_KeyExtr(_key_extr)), tuple_t>::value,
            "WindFlow Compilation Error - key extractor receives a wrong input type (Interval_Join_Builder):\n");
        using new_key_t = decltype(get_key_t_KeyExtr(_key_extr)); // extract the key type
        // static assert to check the new_key_t type
        static_assert(!std::is_same<new_key_t, void>::value,
            "WindFlow Compilation Error - key type cannot be void (Interval_Join_Builder):\n");
        // static assert to check that new_key_t is default constructible
        static_assert(std::is_default_constructible<new_key_t>::value,
            "WindFlow Compilation Error - key type must be default constructible (Interval_Join_Builder):\n");
        if (input_routing_mode != Routing_Mode_t::FORWARD) {
            std::cerr << RED << "WindFlow Error: wrong use of withKeyBy() in the Interval_Join_Builder" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        Interval_Join_Builder<join_func_t, new_key_t> new_builder(func);
        new_builder.name = this->name;
        new_builder.parallelism = this->parallelism;
        //new_builder.input_routing_mode = Routing_Mode_t::KEYBY;
        new_builder.key_extr = _key_extr;
        new_builder.outputBatchSize = this->outputBatchSize;
        new_builder.closing_func = this->closing_func;
        new_builder.isKeyBySet = true;
        new_builder.upper_bound = this->upper_bound;
        new_builder.lower_bound = this->lower_bound;
        new_builder.join_mode = this->join_mode;
        return new_builder;
    }

    /**
     * @brief Sets the lower and upper bounds for the interval join.
     * 
     * @param _lower_bound The lower bound of the interval join range.
     * @param _upper_bound The upper bound of the interval join range.
     * @return A reference to the current object.
     *
     * If the lower bound is greater than the upper bound, an error message is printed and the program exits.
     */
    auto &withBoundaries(std::chrono::microseconds _lower_bound, std::chrono::microseconds _upper_bound)
    {
        //The lower and upper bounds are inclusive in the interval join range.
        //The +-1 ensures that the bounds are inclusive when retrieving the join range using lower_bound algorithm.
        lower_bound = _lower_bound.count()-1;
        upper_bound = _upper_bound.count()+1;
        if (lower_bound > upper_bound) {
            std::cerr << RED << "WindFlow Error: Interval_Join must have lower_bound <= upper_bound" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        return *this;
    }

    /** 
     *  \brief Set Key Parallelism mode for join operator
     *  
     *  \return a reference to the builder object
     */ 
    auto &withKPMode()
    {
        if (!isKeyBySet) {
            std::cerr << RED << "WindFlow Error: Interval_Join with key parallelism mode requires a key extractor" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        if (join_mode != Interval_Join_Mode_t::NONE) {
            std::cerr << RED << "WindFlow Error: wrong use of withKPMode() in the Interval_Join_Builder, you can specify only one mode per join operator " << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        input_routing_mode = Routing_Mode_t::KEYBY;
        join_mode = Interval_Join_Mode_t::KP;
        return *this;
    }

    /** 
     *  \brief Set Key Parallelism mode for join operator
     *  
     *  \return a reference to the builder object
     */ 
    auto &withDPSMode()
    {
        if (!isKeyBySet) {
            std::cerr << RED << "WindFlow Error: Interval_Join with data parallelism mode requires a key extractor" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        if (join_mode != Interval_Join_Mode_t::NONE) {
            std::cerr << RED << "WindFlow Error: wrong use of withKPMode() in the Interval_Join_Builder, you can specify only one mode per join operator " << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        input_routing_mode = Routing_Mode_t::BROADCAST;
        join_mode = Interval_Join_Mode_t::DPS;
        return *this;
    }

    /** 
     *  \brief Create the Interval Join
     *  
     *  \return a new Interval Join instance
     */ 
    auto build()
    {
        // check if the mode is selected
        if (join_mode == Interval_Join_Mode_t::NONE) {
            std::cerr << RED << "WindFlow Error: at least one mode per join operator is need to be selected in the builder" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // check the presence of a key extractor
        if (!isKeyBySet && this->parallelism > 1) {
            std::cerr << RED << "WindFlow Error: Interval_Join with parallelism > 1 requires a key extractor" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        return join_t(func,
                     key_extr,
                     this->parallelism,
                     this->name,
                     input_routing_mode,
                     this->outputBatchSize,
                     this->closing_func,
                     lower_bound,
                     upper_bound,
                     join_mode);
    }
};

/** 
 *  \class Sink_Builder
 *  
 *  \brief Builder of the Sink operator
 *  
 *  Builder class to ease the creation of the Sink operator.
 */ 
template<typename sink_func_t, typename key_t=empty_key_t>
class Sink_Builder: public Basic_Builder<Sink_Builder, sink_func_t, key_t>
{
private:
    template<typename T1, typename T2> friend class Sink_Builder;
    sink_func_t func; // functional logic of the Sink
    using tuple_t = decltype(get_tuple_t_Sink(func)); // extracting the tuple_t type and checking the admissible signatures
    // static assert to check the signature of the Sink functional logic
    static_assert(!(std::is_same<tuple_t, std::false_type>::value),
        "WindFlow Compilation Error - unknown signature passed to the Sink_Builder:\n"
        "  Candidate 1 : void(std::optional<tuple_t> &)\n"
        "  Candidate 2 : void(std::optional<tuple_t> &, RuntimeContext &)\n"
        "  Candidate 3 : void(std::optional<std::reference_wrapper<tuple_t>>);\n"
        "  Candidate 4 : void(std::optional<std::reference_wrapper<tuple_t>>, RuntimeContext &)\n");
    // static assert to check that the tuple_t type must be default constructible
    static_assert(std::is_default_constructible<tuple_t>::value,
        "WindFlow Compilation Error - tuple_t type must be default constructible (Sink_Builder):\n");
    using keyextr_func_t = std::function<key_t(const tuple_t&)>; // type of the key extractor
    using sink_t = Sink<sink_func_t, keyextr_func_t>; // type of the Sink to be created by the builder
    Routing_Mode_t input_routing_mode = Routing_Mode_t::FORWARD; // routing mode of inputs to the Sink
    keyextr_func_t key_extr = [](const tuple_t &t) -> key_t { return key_t(); }; // key extractor

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _func functional logic of the Sink (a function or any callable type)
     */ 
    Sink_Builder(sink_func_t _func):
                 func(_func) {}

    /** 
     *  \brief Set the KEYBY routing mode of inputs to the Sink
     *  
     *  \param _key_extr key extractor functional logic (a function or any callable type)
     *  \return a new builder object with the right key type
     */ 
    template<typename new_keyextr_func_t>
    auto withKeyBy(new_keyextr_func_t _key_extr)
    {
        // static assert to check the signature
        static_assert(!std::is_same<decltype(get_tuple_t_KeyExtr(_key_extr)), std::false_type>::value,
            "WindFlow Compilation Error - unknown signature passed to withKeyBy (Sink_Builder):\n"
            "  Candidate : key_t(const tuple_t &)\n");
        // static assert to check that the tuple_t type of the new key extractor is the right one
        static_assert(std::is_same<decltype(get_tuple_t_KeyExtr(_key_extr)), tuple_t>::value,
            "WindFlow Compilation Error - key extractor receives a wrong input type (Sink_Builder):\n");
        using new_key_t = decltype(get_key_t_KeyExtr(_key_extr)); // extract the key type
        // static assert to check the new_key_t type
        static_assert(!std::is_same<new_key_t, void>::value,
            "WindFlow Compilation Error - key type cannot be void (Sink_Builder):\n");
        // static assert to check that new_key_t is default constructible
        static_assert(std::is_default_constructible<new_key_t>::value,
            "WindFlow Compilation Error - key type must be default constructible (Sink_Builder):\n");
        if (input_routing_mode != Routing_Mode_t::FORWARD) {
            std::cerr << RED << "WindFlow Error: wrong use of withKeyBy() in the Sink_Builder" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);         
        }
        Sink_Builder<sink_func_t, new_key_t> new_builder(func);
        new_builder.name = this->name;
        new_builder.parallelism = this->parallelism;
        new_builder.input_routing_mode = Routing_Mode_t::KEYBY;
        new_builder.key_extr = _key_extr;
        new_builder.closing_func = this->closing_func;
        return new_builder;
    }

    /** 
     *  \brief Set the BROADCAST routing mode of inputs to the Sink
     *  
     *  \return a reference to the builder object
     */ 
    auto &withBroadcast()
    {
        if (input_routing_mode != Routing_Mode_t::FORWARD) {
            std::cerr << RED << "WindFlow Error: wrong use of withBroadcast() in the Sink_Builder" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        input_routing_mode = Routing_Mode_t::BROADCAST;
        return *this;
    }

    /** 
     *  \brief Set the REBALANCING routing mode of inputs to the Sink
     *         (it forces a re-shuffling before this new operator)
     *  
     *  \return a reference to the builder object
     */ 
    auto &withRebalancing()
    {
        if (input_routing_mode != Routing_Mode_t::FORWARD) {
            std::cerr << RED << "WindFlow Error: wrong use of withRebalancing() in the Sink_Builder" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        input_routing_mode = Routing_Mode_t::REBALANCING;
        return *this;
    }

    /// Delete withOutputBatchSize method
    Sink_Builder<sink_func_t, key_t> &withOutputBatchSize(size_t _outputBatchSize) = delete;

    /** 
     *  \brief Create the Sink
     *  
     *  \return a new Sink instance
     */ 
    auto build()
    {
        return sink_t(func,
                      key_extr,
                      this->parallelism,
                      this->name,
                      input_routing_mode,
                      this->closing_func);
    }
};

} // namespace wf

#endif
