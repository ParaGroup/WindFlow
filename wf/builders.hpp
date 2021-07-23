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
 *  
 *  @brief Builder classes used to create the WindFlow operators
 *  
 *  @section Builders-1 (Description)
 *  
 *  Builder classes used to create the WindFlow operators.
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
 *  \class Source_Builder
 *  
 *  \brief Builder of the Source operator
 *  
 *  Builder class to ease the creation of the Source operator.
 */ 
template<typename source_func_t>
class Source_Builder
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
    using closing_func_t = std::function<void(RuntimeContext&)>; // type of the closing functional logic
    std::string name = "source"; // name of the Source
    size_t parallelism = 1; // parallelism of the Source
    size_t outputBatchSize = 0; // output batch size of the Source
    closing_func_t closing_func = [](RuntimeContext &r) -> void { return; }; // closing functional logic

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _func functional logic of the Source (a function or a callable type)
     */ 
    Source_Builder(source_func_t _func):
                   func(_func) {}

    /** 
     *  \brief Set the name of the Source
     *  
     *  \param _name of the Source
     *  \return a reference to the builder object
     */ 
    Source_Builder<source_func_t> &withName(std::string _name)
    {
        name = _name;
        return *this;
    }

    /** 
     *  \brief Set the parallelism of the Source
     *  
     *  \param _parallelism of the Source
     *  \return a reference to the builder object
     */ 
    Source_Builder<source_func_t> &withParallelism(size_t _parallelism)
    {
        parallelism = _parallelism;
        return *this;
    }

    /** 
     *  \brief Set the output batch size of the Source
     *  
     *  \param _outputBatchSize number of outputs per batch (zero means no batching)
     *  \return a reference to the builder object
     */ 
    Source_Builder<source_func_t> &withOutputBatchSize(size_t _outputBatchSize)
    {
        outputBatchSize = _outputBatchSize;
        return *this;
    }

    /** 
     *  \brief Set the closing functional logic used by the Source
     *  
     *  \param _closing_func closing functional logic (a function or a callable type)
     *  \return a reference to the builder object
     */ 
    template<typename closing_F_t>
    Source_Builder<source_func_t> &withClosingFunction(closing_F_t _closing_func)
    {
        // static assert to check the signature
        static_assert(!std::is_same<decltype(check_closing_t(_closing_func)), std::false_type>::value,
            "WindFlow Compilation Error - unknown signature passed to withClosingFunction (Source_Builder):\n"
            "  Candidate : void(RuntimeContext &)\n");
        closing_func = _closing_func;
        return *this;
    }

    /** 
     *  \brief Create the Source
     *  
     *  \return a new Source instance
     */ 
    source_t build()
    {
        return source_t(func,
                        parallelism,
                        name,
                        outputBatchSize,
                        closing_func);
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
class Filter_Builder
{
private:
    template<typename T1, typename T2> friend class Filter_Builder; // friendship with all the instances of the Filter_Builder template
    filter_func_t func; // functional logic of the Filter
    using tuple_t = decltype(get_tuple_t_Filter(func)); // extracting the tuple_t type and checking the admissible signatures
    using result_t = decltype(get_result_t_Filter(func)); // extracting the result_t type and checking the admissible signatures
    // static assert to check the signature of the Filter functional logic
    static_assert(!(std::is_same<tuple_t, std::false_type>::value || std::is_same<result_t, std::false_type>::value),
        "WindFlow Compilation Error - unknown signature passed to the Filter_Builder:\n"
        "  Candidate 1 : bool(tuple_t &)\n"
        "  Candidate 2 : bool(tuple_t &, RuntimeContext &)\n");
    // static assert to check that the tuple_t type must be default constructible
    static_assert(std::is_default_constructible<tuple_t>::value,
        "WindFlow Compilation Error - tuple_t type must be default constructible (Filter_Builder):\n");
    // static assert to check that the result_t type must be default constructible
    static_assert(std::is_default_constructible<result_t>::value,
        "WindFlow Compilation Error - result_t type must be default constructible (Filter_Builder):\n");
    using key_extractor_func_t = std::function<key_t(const tuple_t&)>; // type of the key extractor
    using filter_t = Filter<filter_func_t, key_extractor_func_t>; // type of the Filter to be created by the builder
    using closing_func_t = std::function<void(RuntimeContext&)>; // type of the closing functional logic
    std::string name = "filter"; // name of the Filter
    size_t parallelism = 1; // parallelism of the Filter
    Routing_Mode_t input_routing_mode = Routing_Mode_t::FORWARD; // routing mode of inputs to the Filter
    key_extractor_func_t key_extr = [](const tuple_t &t) -> key_t { return key_t(); }; // key extractor
    size_t outputBatchSize = 0; // output batch size of the Filter
    closing_func_t closing_func = [](RuntimeContext &r) -> void { return; }; // closing functional logic

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _func functional logic of the Filter (a function or a callable type)
     */ 
    Filter_Builder(filter_func_t _func):
                   func(_func) {}

    /** 
     *  \brief Set the name of the Filter
     *  
     *  \param _name of the Filter
     *  \return a reference to the builder object
     */ 
    Filter_Builder<filter_func_t, key_t> &withName(std::string _name)
    {
        name = _name;
        return *this;
    }

    /** 
     *  \brief Set the parallelism of the Filter
     *  
     *  \param _parallelism of the Filter
     *  \return a reference to the builder object
     */ 
    Filter_Builder<filter_func_t, key_t> &withParallelism(size_t _parallelism)
    {
        parallelism = _parallelism;
        return *this;
    }

    /** 
     *  \brief Set the KEYBY routing mode of inputs to the Filter
     *  
     *  \param _key_extr key extractor functional logic (a function or a callable type)
     *  \return a new builder object with the right key type
     */ 
    template<typename new_key_extractor_func_t>
    auto withKeyBy(new_key_extractor_func_t _key_extr)
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
        Filter_Builder<filter_func_t, new_key_t> new_builder(func);
        new_builder.name = name;
        new_builder.parallelism = parallelism;
        new_builder.input_routing_mode = Routing_Mode_t::KEYBY;
        new_builder.key_extr = _key_extr;
        new_builder.outputBatchSize = outputBatchSize;
        new_builder.closing_func = closing_func;
        return new_builder;
    }

    /** 
     *  \brief Set the output batch size of the Filter
     *  
     *  \param _outputBatchSize number of outputs per batch (zero means no batching)
     *  \return a reference to the builder object
     */ 
    Filter_Builder<filter_func_t, key_t> &withOutputBatchSize(size_t _outputBatchSize)
    {
        outputBatchSize = _outputBatchSize;
        return *this;
    }

    /** 
     *  \brief Set the closing functional logic used by the Filter
     *  
     *  \param _closing_func closing functional logic (a function or a callable type)
     *  \return a reference to the builder object
     */ 
    template<typename closing_F_t>
    Filter_Builder<filter_func_t, key_t> &withClosingFunction(closing_F_t _closing_func)
    {
        // static assert to check the signature
        static_assert(!std::is_same<decltype(check_closing_t(_closing_func)), std::false_type>::value,
            "WindFlow Compilation Error - unknown signature passed to withClosingFunction (Filter_Builder):\n"
            "  Candidate : void(RuntimeContext &)\n");
        closing_func = _closing_func;
        return *this;
    }

    /** 
     *  \brief Create the Filter
     *  
     *  \return a new Filter instance
     */ 
    filter_t build()
    {
        return filter_t(func,
                        key_extr,
                        parallelism,
                        name,
                        input_routing_mode,
                        outputBatchSize,
                        closing_func);
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
class Map_Builder
{
private:
    template<typename T1, typename T2> friend class Map_Builder; // friendship with all the instances of the Map_Builder template
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
    using key_extractor_func_t = std::function<key_t(const tuple_t&)>; // type of the key extractor
    using map_t = Map<map_func_t, key_extractor_func_t>; // type of the Map to be created by the builder
    using closing_func_t = std::function<void(RuntimeContext&)>; // type of the closing functional logic
    std::string name = "map"; // name of the Map
    size_t parallelism = 1; // parallelism of the Map
    Routing_Mode_t input_routing_mode = Routing_Mode_t::FORWARD; // routing mode of inputs to the Map
    key_extractor_func_t key_extr = [](const tuple_t &t) -> key_t { return key_t(); }; // key extractor
    size_t outputBatchSize = 0; // output batch size of the Map
    closing_func_t closing_func = [](RuntimeContext &r) -> void { return; }; // closing functional logic

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _func functional logic of the Map (a function or a callable type)
     */ 
    Map_Builder(map_func_t _func):
                func(_func) {}

    /** 
     *  \brief Set the name of the Map
     *  
     *  \param _name of the Map
     *  \return a reference to the builder object
     */ 
    Map_Builder<map_func_t, key_t> &withName(std::string _name)
    {
        name = _name;
        return *this;
    }

    /** 
     *  \brief Set the parallelism of the Map
     *  
     *  \param _parallelism of the Map
     *  \return a reference to the builder object
     */ 
    Map_Builder<map_func_t, key_t> &withParallelism(size_t _parallelism)
    {
        parallelism = _parallelism;
        return *this;
    }

    /** 
     *  \brief Set the KEYBY routing mode of inputs to the Map
     *  
     *  \param _key_extr key extractor functional logic (a function or a callable type)
     *  \return a new builder object with the right key type
     */ 
    template<typename new_key_extractor_func_t>
    auto withKeyBy(new_key_extractor_func_t _key_extr)
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
        Map_Builder<map_func_t, new_key_t> new_builder(func);
        new_builder.name = name;
        new_builder.parallelism = parallelism;
        new_builder.input_routing_mode = Routing_Mode_t::KEYBY;
        new_builder.key_extr = _key_extr;
        new_builder.outputBatchSize = outputBatchSize;
        new_builder.closing_func = closing_func;
        return new_builder;
    }

    /** 
     *  \brief Set the output batch size of the Map
     *  
     *  \param _outputBatchSize number of outputs per batch (zero means no batching)
     *  \return a reference to the builder object
     */ 
    Map_Builder<map_func_t, key_t> &withOutputBatchSize(size_t _outputBatchSize)
    {
        outputBatchSize = _outputBatchSize;
        return *this;
    }

    /** 
     *  \brief Set the closing functional logic used by the Map
     *  
     *  \param _closing_func closing functional logic (a function or a callable type)
     *  \return a reference to the builder object
     */ 
    template<typename closing_F_t>
    Map_Builder<map_func_t, key_t> &withClosingFunction(closing_F_t _closing_func)
    {
        // static assert to check the signature
        static_assert(!std::is_same<decltype(check_closing_t(_closing_func)), std::false_type>::value,
            "WindFlow Compilation Error - unknown signature passed to withClosingFunction (Map_Builder):\n"
            "  Candidate : void(RuntimeContext &)\n");
        closing_func = _closing_func;
        return *this;
    }

    /** 
     *  \brief Create the Map
     *  
     *  \return a new Map instance
     */ 
    map_t build()
    {
        return map_t(func,
                     key_extr,
                     parallelism,
                     name,
                     input_routing_mode,
                     outputBatchSize,
                     closing_func);
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
class FlatMap_Builder
{
private:
    template<typename T1, typename T2> friend class FlatMap_Builder; // friendship with all the instances of the FlatMap_Builder template
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
    using key_extractor_func_t = std::function<key_t(const tuple_t&)>; // type of the key extractor
    using flatmap_t = FlatMap<flatmap_func_t, key_extractor_func_t>; // type of the FlatMap to be created by the builder
    using closing_func_t = std::function<void(RuntimeContext&)>; // type of the closing functional logic
    std::string name = "flatmap"; // name of the FlatMap
    size_t parallelism = 1; // parallelism of the FlatMap
    Routing_Mode_t input_routing_mode = Routing_Mode_t::FORWARD; // routing mode of inputs to the FlatMap
    key_extractor_func_t key_extr = [](const tuple_t &t) -> key_t { return key_t(); }; // key extractor
    size_t outputBatchSize = 0; // output batch size of the Filter
    closing_func_t closing_func = [](RuntimeContext &r) -> void { return; }; // closing functional logic

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _func functional logic of the FlatMap (a function or a callable type)
     */ 
    FlatMap_Builder(flatmap_func_t _func):
                    func(_func) {}

    /** 
     *  \brief Set the name of the FlatMap
     *  
     *  \param _name of the FlatMap
     *  \return a reference to the builder object
     */ 
    FlatMap_Builder<flatmap_func_t, key_t> &withName(std::string _name)
    {
        name = _name;
        return *this;
    }

    /** 
     *  \brief Set the parallelism of the FlatMap
     *  
     *  \param _parallelism of the FlatMap
     *  \return a reference to the builder object
     */ 
    FlatMap_Builder<flatmap_func_t, key_t> &withParallelism(size_t _parallelism)
    {
        parallelism = _parallelism;
        return *this;
    }

    /** 
     *  \brief Set the KEYBY routing mode of inputs to the FlatMap
     *  
     *  \param _key_extr key extractor functional logic (a function or a callable type)
     *  \return a new builder object with the right key type
     */ 
    template<typename new_key_extractor_func_t>
    auto withKeyBy(new_key_extractor_func_t _key_extr)
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
        FlatMap_Builder<flatmap_func_t, new_key_t> new_builder(func);
        new_builder.name = name;
        new_builder.parallelism = parallelism;
        new_builder.input_routing_mode = Routing_Mode_t::KEYBY;
        new_builder.key_extr = _key_extr;
        new_builder.outputBatchSize = outputBatchSize;
        new_builder.closing_func = closing_func;
        return new_builder;
    }

    /** 
     *  \brief Set the output batch size of the FlatMap
     *  
     *  \param _outputBatchSize number of outputs per batch (zero means no batching)
     *  \return a reference to the builder object
     */ 
    FlatMap_Builder<flatmap_func_t, key_t> &withOutputBatchSize(size_t _outputBatchSize)
    {
        outputBatchSize = _outputBatchSize;
        return *this;
    }

    /** 
     *  \brief Set the closing functional logic used by the FlatMap
     *  
     *  \param _closing_func closing functional logic (a function or a callable type)
     *  \return a reference to the builder object
     */ 
    template<typename closing_F_t>
    FlatMap_Builder<flatmap_func_t, key_t> &withClosingFunction(closing_F_t _closing_func)
    {
        // static assert to check the signature
        static_assert(!std::is_same<decltype(check_closing_t(_closing_func)), std::false_type>::value,
            "WindFlow Compilation Error - unknown signature passed to withClosingFunction (FlatMap_Builder):\n"
            "  Candidate : void(RuntimeContext &)\n");
        closing_func = _closing_func;
        return *this;
    }

    /** 
     *  \brief Create the FlatMap
     *  
     *  \return a new FlatMap instance
     */ 
    flatmap_t build()
    {
        return flatmap_t(func,
                         key_extr,
                         parallelism,
                         name,
                         input_routing_mode,
                         outputBatchSize,
                         closing_func);
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
class Reduce_Builder
{
private:
    template<typename T1, typename T2> friend class Reduce_Builder; // friendship with all the instances of the Reduce_Builder template
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
    using key_extractor_func_t = std::function<key_t(const tuple_t&)>; // type of the key extractor
    using reduce_t = Reduce<reduce_func_t, key_extractor_func_t>; // type of the Reduce to be created by the builder
    using closing_func_t = std::function<void(RuntimeContext&)>; // type of the closing functional logic
    std::string name = "Reduce"; // name of the Reduce
    size_t parallelism = 1; // parallelism of the Reduce
    key_extractor_func_t key_extr = [](const tuple_t &t) -> key_t { return key_t(); }; // key extractor
    bool isKeyBySet = false; // true if a key extractor has been provided
    size_t outputBatchSize = 0; // output batch size of the Reduce
    closing_func_t closing_func = [](RuntimeContext &r) -> void { return; }; // closing functional logic
    state_t initial_state; // initial state to be created one per key

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _func functional logic of the Reduce (a function or a callable type)
     */ 
    Reduce_Builder(reduce_func_t _func):
                   func(_func) {}

    /** 
     *  \brief Set the name of the Reduce
     *  
     *  \param _name of the Reduce
     *  \return a reference to the builder object
     */ 
    Reduce_Builder<reduce_func_t, key_t> &withName(std::string _name)
    {
        name = _name;
        return *this;
    }

    /** 
     *  \brief Set the parallelism of the Reduce
     *  
     *  \param _parallelism of the Reduce
     *  \return a reference to the builder object
     */ 
    Reduce_Builder<reduce_func_t, key_t> &withParallelism(size_t _parallelism)
    {
        parallelism = _parallelism;
        return *this;
    }

    /** 
     *  \brief Set the KEYBY routing mode of inputs to the Reduce
     *  
     *  \param _key_extr key extractor functional logic (a function or a callable type)
     *  \return a new builder object with the right key type
     */ 
    template<typename new_key_extractor_func_t>
    auto withKeyBy(new_key_extractor_func_t _key_extr)
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
        new_builder.name = name;
        new_builder.parallelism = parallelism;
        new_builder.key_extr = _key_extr;
        new_builder.outputBatchSize = outputBatchSize;
        new_builder.closing_func = closing_func;
        new_builder.isKeyBySet = true;
        return new_builder;
    }

    /** 
     *  \brief Set the output batch size of the Reduce
     *  
     *  \param _outputBatchSize number of outputs per batch (zero means no batching)
     *  \return a reference to the builder object
     */ 
    Reduce_Builder<reduce_func_t, key_t> &withOutputBatchSize(size_t _outputBatchSize)
    {
        outputBatchSize = _outputBatchSize;
        return *this;
    }

    /** 
     *  \brief Set the closing functional logic used by the Reduce
     *  
     *  \param _closing_func closing functional logic (a function or a callable type)
     *  \return a reference to the builder object
     */ 
    template<typename closing_F_t>
    Reduce_Builder<reduce_func_t, key_t> &withClosingFunction(closing_F_t _closing_func)
    {
        // static assert to check the signature
        static_assert(!std::is_same<decltype(check_closing_t(_closing_func)), std::false_type>::value,
            "WindFlow Compilation Error - unknown signature passed to withClosingFunction (Reduce_Builder):\n"
            "  Candidate : void(RuntimeContext &)\n");
        closing_func = _closing_func;
        return *this;
    }

    /** 
     *  \brief Set the value of the initial state per key
     *  
     *  \param _initial_state value of the initial state to be created one per key
     *  \return a reference to the builder object
     */ 
    Reduce_Builder<reduce_func_t, key_t> &withInitialState(state_t _initial_state)
    {
        initial_state = _initial_state;
        return *this;
    }

    /** 
     *  \brief Create the Reduce
     *  
     *  \return a new Reduce instance
     */ 
    reduce_t build()
    {
        // check the presence of a key extractor
        if (!isKeyBySet && parallelism > 1) {
            std::cerr << RED << "WindFlow Error: Reduce with paralellism > 1 requires a key extractor" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        return reduce_t(func,
                        key_extr,
                        parallelism,
                        name,
                        outputBatchSize,
                        closing_func,
                        initial_state);
    }
};

/** 
 *  \class Keyed_Windows_Builder
 *  
 *  \brief Builder of the Keyed_Windows operator
 *  
 *  Builder class to ease the creation of the Keyed_Windows operator.
 */ 
template<typename win_func_t, typename key_t=empty_key_t>
class Keyed_Windows_Builder
{
private:
    template<typename T1, typename T2> friend class Keyed_Windows_Builder; // friendship with all the instances of the Keyed_Windows_Builder template
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
    using key_extractor_func_t = std::function<key_t(const tuple_t&)>; // type of the key extractor
    using keyed_wins_t = Keyed_Windows<win_func_t, key_extractor_func_t>; // type of the Keyed_Windows to be created by the builder
    using closing_func_t = std::function<void(RuntimeContext&)>; // type of the closing functional logic
    std::string name = "keyed_windows"; // name of the Keyed_Windows
    size_t parallelism = 1; // parallelism of the Keyed_Windows
    key_extractor_func_t key_extr = [](const tuple_t &t) -> key_t { return key_t(); }; // key extractor
    bool isKeyBySet = false; // true if a key extractor has been provided
    size_t outputBatchSize = 0; // output batch size of the Keyed_Windows
    closing_func_t closing_func = [](RuntimeContext &r) -> void { return; }; // closing functional logic
    uint64_t win_len=0; // window length in number of tuples or in time units
    uint64_t slide_len=0; // slide length in number of tuples or in time units
    uint64_t lateness=0; // lateness in time units
    Win_Type_t winType=Win_Type_t::CB; // window type (CB or TB)

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _func functional logic of the Keyed_Windows (a function or a callable type)
     */ 
    Keyed_Windows_Builder(win_func_t _func):
                          func(_func) {}

    /** 
     *  \brief Set the name of the Keyed_Windows
     *  
     *  \param _name of the Keyed_Windows
     *  \return a reference to the builder object
     */ 
    Keyed_Windows_Builder<win_func_t, key_t> &withName(std::string _name)
    {
        name = _name;
        return *this;
    }

    /** 
     *  \brief Set the parallelism of the Keyed_Windows
     *  
     *  \param _parallelism of the Keyed_Windows
     *  \return a reference to the builder object
     */ 
    Keyed_Windows_Builder<win_func_t, key_t> &withParallelism(size_t _parallelism)
    {
        parallelism = _parallelism;
        return *this;
    }

    /** 
     *  \brief Set the KEYBY routing mode of inputs to the Keyed_Windows
     *  
     *  \param _key_extr key extractor functional logic (a function or a callable type)
     *  \return a new builder object with the right key type
     */ 
    template<typename new_key_extractor_func_t>
    auto withKeyBy(new_key_extractor_func_t _key_extr)
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
        Keyed_Windows_Builder<win_func_t, new_key_t> new_builder(func);
        new_builder.name = name;
        new_builder.parallelism = parallelism;
        new_builder.key_extr = _key_extr;
        new_builder.outputBatchSize = outputBatchSize;
        new_builder.closing_func = closing_func;
        new_builder.isKeyBySet = true;
        new_builder.win_len = win_len;
        new_builder.slide_len = slide_len;
        new_builder.lateness = lateness;
        new_builder.winType = winType;
        return new_builder;
    }

    /** 
     *  \brief Set the output batch size of the Keyed_Windows
     *  
     *  \param _outputBatchSize number of outputs per batch (zero means no batching)
     *  \return a reference to the builder object
     */ 
    Keyed_Windows_Builder<win_func_t, key_t> &withOutputBatchSize(size_t _outputBatchSize)
    {
        outputBatchSize = _outputBatchSize;
        return *this;
    }

    /** 
     *  \brief Set the closing functional logic used by the Keyed_Windows
     *  
     *  \param _closing_func closing functional logic (a function or a callable type)
     *  \return a reference to the builder object
     */ 
    template<typename closing_F_t>
    Keyed_Windows_Builder<win_func_t, key_t> &withClosingFunction(closing_F_t _closing_func)
    {
        // static assert to check the signature
        static_assert(!std::is_same<decltype(check_closing_t(_closing_func)), std::false_type>::value,
            "WindFlow Compilation Error - unknown signature passed to withClosingFunction (Keyed_Windows_Builder):\n"
            "  Candidate : void(RuntimeContext &)\n");
        closing_func = _closing_func;
        return *this;
    }

    /** 
     *  \brief Set the configuration for count-based windows
     *  
     *  \param _win_len window length (in number of tuples)
     *  \param _slide_len slide length (in number of tuples)
     *  \return a reference to the builder object
     */ 
    Keyed_Windows_Builder<win_func_t, key_t> &withCBWindows(uint64_t _win_len,
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
     *  \return a reference to the builder object
     */ 
    Keyed_Windows_Builder<win_func_t, key_t> &withTBWindows(std::chrono::microseconds _win_len,
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
    Keyed_Windows_Builder<win_func_t, key_t> &withLateness(std::chrono::microseconds _lateness)
    {
        if (winType != Win_Type_t::TB) { // check that time-based semantics is used
            std::cerr << RED << "WindFlow Error: lateness can be set only for time-based windows" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        lateness = _lateness.count();
        return *this;
    }

    /** 
     *  \brief Create the Keyed_Windows
     *  
     *  \return a new Keyed_Windows instance
     */ 
    keyed_wins_t build()
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
        if (!isKeyBySet && parallelism > 1) {
            std::cerr << RED << "WindFlow Error: Keyed_Windows with paralellism > 1 requires a key extractor" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        return keyed_wins_t(func,
                            key_extr,
                            parallelism,
                            name,
                            outputBatchSize,
                            closing_func,
                            win_len,
                            slide_len,
                            lateness,
                            winType);
    }
};

/** 
 *  \class Parallel_Windows_Builder
 *  
 *  \brief Builder of the Parallel_Windows operator
 *  
 *  Builder class to ease the creation of the Parallel_Windows operator.
 */ 
template<typename win_func_t, typename key_t=empty_key_t>
class Parallel_Windows_Builder
{
private:
    template<typename T1, typename T2> friend class Parallel_Windows_Builder; // friendship with all the instances of the Parallel_Windows_Builder template
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
    using key_extractor_func_t = std::function<key_t(const tuple_t&)>; // type of the key extractor
    using par_wins_t = Parallel_Windows<win_func_t, key_extractor_func_t>; // type of the Parallel_Windows to be created by the builder
    using closing_func_t = std::function<void(RuntimeContext&)>; // type of the closing functional logic
    std::string name = "parallel_windows"; // name of the Parallel_Windows
    size_t parallelism = 1; // parallelism of the Parallel_Windows
    key_extractor_func_t key_extr = [](const tuple_t &t) -> key_t { return key_t(); }; // key extractor
    size_t outputBatchSize = 0; // output batch size of the Parallel_Windows
    closing_func_t closing_func = [](RuntimeContext &r) -> void { return; }; // closing functional logic
    uint64_t win_len=0; // window length in number of tuples or in time units
    uint64_t slide_len=0; // slide length in number of tuples or in time units
    uint64_t lateness=0; // lateness in time units
    Win_Type_t winType=Win_Type_t::CB; // window type (CB or TB)

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _func functional logic of the Parallel_Windows (a function or a callable type)
     */ 
    Parallel_Windows_Builder(win_func_t _func):
                             func(_func) {}

    /** 
     *  \brief Set the name of the Parallel_Windows
     *  
     *  \param _name of the Parallel_Windows
     *  \return a reference to the builder object
     */ 
    Parallel_Windows_Builder<win_func_t, key_t> &withName(std::string _name)
    {
        name = _name;
        return *this;
    }

    /** 
     *  \brief Set the parallelism of the Parallel_Windows
     *  
     *  \param _parallelism of the Parallel_Windows
     *  \return a reference to the builder object
     */ 
    Parallel_Windows_Builder<win_func_t, key_t> &withParallelism(size_t _parallelism)
    {
        parallelism = _parallelism;
        return *this;
    }

    /** 
     *  \brief Set the KEYBY routing mode of inputs to the Parallel_Windows
     *  
     *  \param _key_extr key extractor functional logic (a function or a callable type)
     *  \return a new builder object with the right key type
     */ 
    template<typename new_key_extractor_func_t>
    auto withKeyBy(new_key_extractor_func_t _key_extr)
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
        Parallel_Windows_Builder<win_func_t, new_key_t> new_builder(func);
        new_builder.name = name;
        new_builder.parallelism = parallelism;
        new_builder.key_extr = _key_extr;
        new_builder.outputBatchSize = outputBatchSize;
        new_builder.closing_func = closing_func;
        new_builder.win_len = win_len;
        new_builder.slide_len = slide_len;
        new_builder.lateness = lateness;
        new_builder.winType = winType;
        return new_builder;
    }

    /** 
     *  \brief Set the output batch size of the Parallel_Windows
     *  
     *  \param _outputBatchSize number of outputs per batch (zero means no batching)
     *  \return a reference to the builder object
     */ 
    Parallel_Windows_Builder<win_func_t, key_t> &withOutputBatchSize(size_t _outputBatchSize)
    {
        outputBatchSize = _outputBatchSize;
        return *this;
    }

    /** 
     *  \brief Set the closing functional logic used by the Parallel_Windows
     *  
     *  \param _closing_func closing functional logic (a function or a callable type)
     *  \return a reference to the builder object
     */ 
    template<typename closing_F_t>
    Parallel_Windows_Builder<win_func_t, key_t> &withClosingFunction(closing_F_t _closing_func)
    {
        // static assert to check the signature
        static_assert(!std::is_same<decltype(check_closing_t(_closing_func)), std::false_type>::value,
            "WindFlow Compilation Error - unknown signature passed to withClosingFunction (Parallel_Windows_Builder):\n"
            "  Candidate : void(RuntimeContext &)\n");
        closing_func = _closing_func;
        return *this;
    }

    /** 
     *  \brief Set the configuration for count-based windows
     *  
     *  \param _win_len window length (in number of tuples)
     *  \param _slide_len slide length (in number of tuples)
     *  \return a reference to the builder object
     */ 
    Parallel_Windows_Builder<win_func_t, key_t> &withCBWindows(uint64_t _win_len,
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
     *  \return a reference to the builder object
     */ 
    Parallel_Windows_Builder<win_func_t, key_t> &withTBWindows(std::chrono::microseconds _win_len,
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
    Parallel_Windows_Builder<win_func_t, key_t> &withLateness(std::chrono::microseconds _lateness)
    {
        if (winType != Win_Type_t::TB) { // check that time-based semantics is used
            std::cerr << RED << "WindFlow Error: lateness can be set only for time-based windows" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        lateness = _lateness.count();
        return *this;
    }

    /** 
     *  \brief Create the Parallel_Windows
     *  
     *  \return a new Parallel_Windows instance
     */ 
    par_wins_t build()
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
                          parallelism,
                          name,
                          outputBatchSize,
                          closing_func,
                          win_len,
                          slide_len,
                          lateness,
                          winType);
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
class Paned_Windows_Builder
{
private:
    template<typename T1, typename T3, typename T2> friend class Paned_Windows_Builder; // friendship with all the instances of the Paned_Windows_Builder template
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
    using key_extractor_func_t = std::function<key_t(const tuple_t&)>; // type of the key extractor
    using paned_wins_t = Paned_Windows<plq_func_t, wlq_func_t, key_extractor_func_t>; // type of the Paned_Windows to be created by the builder
    using closing_func_t = std::function<void(RuntimeContext&)>; // type of the closing functional logic
    std::string name = "paned_windows"; // name of the Paned_Windows
    size_t plq_parallelism = 1; // parallelism of the PLQ stage
    size_t wlq_parallelism = 1; // parallelism of the WLQ stage
    key_extractor_func_t key_extr = [](const tuple_t &t) -> key_t { return key_t(); }; // key extractor
    size_t outputBatchSize = 0; // output batch size of the Paned_Windows
    closing_func_t closing_func = [](RuntimeContext &r) -> void { return; }; // closing functional logic
    uint64_t win_len=0; // window length in number of tuples or in time units
    uint64_t slide_len=0; // slide length in number of tuples or in time units
    uint64_t lateness=0; // lateness in time units
    Win_Type_t winType=Win_Type_t::CB; // window type (CB or TB)

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _plq_func functional logic of the PLQ stage (a function or a callable type)
     *  \param _wlq_func functional logic of the WLQ stage (a function or a callable type)
     */ 
    Paned_Windows_Builder(plq_func_t _plq_func,
                          wlq_func_t _wlq_func):
                          plq_func(_plq_func),
                          wlq_func(_wlq_func) {}

    /** 
     *  \brief Set the name of the Paned_Windows
     *  
     *  \param _name of the Paned_Windows
     *  \return a reference to the builder object
     */ 
    Paned_Windows_Builder<plq_func_t, wlq_func_t, key_t> &withName(std::string _name)
    {
        name = _name;
        return *this;
    }

    /** 
     *  \brief Set the parallelism of the Paned_Windows
     *  
     *  \param _plq_parallelism of the PLQ stage
     *  \param _wlq_parallelism of the WLQ stage
     *  \return a reference to the builder object
     */ 
    Paned_Windows_Builder<plq_func_t, wlq_func_t, key_t> &withParallelism(size_t _plq_parallelism,
                                                                          size_t _wlq_parallelism)
    {
        plq_parallelism = _plq_parallelism;
        wlq_parallelism = _wlq_parallelism;
        return *this;
    }

    /** 
     *  \brief Set the KEYBY routing mode of inputs to the Paned_Windows
     *  
     *  \param _key_extr key extractor functional logic (a function or a callable type)
     *  \return a new builder object with the right key type
     */ 
    template<typename new_key_extractor_func_t>
    auto withKeyBy(new_key_extractor_func_t _key_extr)
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
        new_builder.name = name;
        new_builder.plq_parallelism = plq_parallelism;
        new_builder.wlq_parallelism = wlq_parallelism;
        new_builder.key_extr = _key_extr;
        new_builder.outputBatchSize = outputBatchSize;
        new_builder.closing_func = closing_func;
        new_builder.win_len = win_len;
        new_builder.slide_len = slide_len;
        new_builder.lateness = lateness;
        new_builder.winType = winType;
        return new_builder;
    }

    /** 
     *  \brief Set the output batch size of the Paned_Windows
     *  
     *  \param _outputBatchSize number of outputs per batch (zero means no batching)
     *  \return a reference to the builder object
     */ 
    Paned_Windows_Builder<plq_func_t, wlq_func_t, key_t> &withOutputBatchSize(size_t _outputBatchSize)
    {
        outputBatchSize = _outputBatchSize;
        return *this;
    }

    /** 
     *  \brief Set the closing functional logic used by the Paned_Windows
     *  
     *  \param _closing_func closing functional logic (a function or a callable type)
     *  \return a reference to the builder object
     */ 
    template<typename closing_F_t>
    Paned_Windows_Builder<plq_func_t, wlq_func_t, key_t> &withClosingFunction(closing_F_t _closing_func)
    {
        // static assert to check the signature
        static_assert(!std::is_same<decltype(check_closing_t(_closing_func)), std::false_type>::value,
            "WindFlow Compilation Error - unknown signature passed to withClosingFunction (Paned_Windows_Builder):\n"
            "  Candidate : void(RuntimeContext &)\n");
        closing_func = _closing_func;
        return *this;
    }

    /** 
     *  \brief Set the configuration for count-based windows
     *  
     *  \param _win_len window length (in number of tuples)
     *  \param _slide_len slide length (in number of tuples)
     *  \return a reference to the builder object
     */ 
    Paned_Windows_Builder<plq_func_t, wlq_func_t, key_t> &withCBWindows(uint64_t _win_len,
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
     *  \return a reference to the builder object
     */ 
    Paned_Windows_Builder<plq_func_t, wlq_func_t, key_t> &withTBWindows(std::chrono::microseconds _win_len,
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
    Paned_Windows_Builder<plq_func_t, wlq_func_t, key_t> &withLateness(std::chrono::microseconds _lateness)
    {
        if (winType != Win_Type_t::TB) { // check that time-based semantics is used
            std::cerr << RED << "WindFlow Error: lateness can be set only for time-based windows" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        lateness = _lateness.count();
        return *this;
    }

    /** 
     *  \brief Create the Paned_Windows
     *  
     *  \return a new Paned_Windows instance
     */ 
    paned_wins_t build()
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
                            name,
                            outputBatchSize,
                            closing_func,
                            win_len,
                            slide_len,
                            lateness,
                            winType);
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
class MapReduce_Windows_Builder
{
private:
    template<typename T1, typename T3, typename T2> friend class MapReduce_Windows_Builder; // friendship with all the instances of the MapReduce_Windows_Builder template
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
    using key_extractor_func_t = std::function<key_t(const tuple_t&)>; // type of the key extractor
    using mr_wins_t = MapReduce_Windows<map_func_t, reduce_func_t, key_extractor_func_t>; // type of the MapReduce_Windows to be created by the builder
    using closing_func_t = std::function<void(RuntimeContext&)>; // type of the closing functional logic
    std::string name = "mapreduce_windows"; // name of the MapReduce_Windows
    size_t map_parallelism = 1; // parallelism of the MAP stage
    size_t reduce_parallelism = 1; // parallelism of the REDUCE stage
    key_extractor_func_t key_extr = [](const tuple_t &t) -> key_t { return key_t(); }; // key extractor
    size_t outputBatchSize = 0; // output batch size of the MapReduce_Windows
    closing_func_t closing_func = [](RuntimeContext &r) -> void { return; }; // closing functional logic
    uint64_t win_len=0; // window length in number of tuples or in time units
    uint64_t slide_len=0; // slide length in number of tuples or in time units
    uint64_t lateness=0; // lateness in time units
    Win_Type_t winType=Win_Type_t::CB; // window type (CB or TB)

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _map_func functional logic of the MAP stage (a function or a callable type)
     *  \param _reduce_func functional logic of the REDUCE stage (a function or a callable type)
     */ 
    MapReduce_Windows_Builder(map_func_t _map_func,
                              reduce_func_t _reduce_func):
                              map_func(_map_func),
                              reduce_func(_reduce_func) {}

    /** 
     *  \brief Set the name of the MapReduce_Windows
     *  
     *  \param _name of the MapReduce_Windows
     *  \return a reference to the builder object
     */ 
    MapReduce_Windows_Builder<map_func_t, reduce_func_t, key_t> &withName(std::string _name)
    {
        name = _name;
        return *this;
    }

    /** 
     *  \brief Set the parallelism of the MapReduce_Windows
     *  
     *  \param _map_parallelism of the MAP stage
     *  \param _reduce_parallelism of the REDUCE stage
     *  \return a reference to the builder object
     */ 
    MapReduce_Windows_Builder<map_func_t, reduce_func_t, key_t> &withParallelism(size_t _map_parallelism,
                                                                                 size_t _reduce_parallelism)
    {
        map_parallelism = _map_parallelism;
        reduce_parallelism = _reduce_parallelism;
        return *this;
    }

    /** 
     *  \brief Set the KEYBY routing mode of inputs to the MapReduce_Windows
     *  
     *  \param _key_extr key extractor functional logic (a function or a callable type)
     *  \return a new builder object with the right key type
     */ 
    template<typename new_key_extractor_func_t>
    auto withKeyBy(new_key_extractor_func_t _key_extr)
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
        new_builder.name = name;
        new_builder.map_parallelism = map_parallelism;
        new_builder.reduce_parallelism = reduce_parallelism;
        new_builder.key_extr = _key_extr;
        new_builder.outputBatchSize = outputBatchSize;
        new_builder.closing_func = closing_func;
        new_builder.win_len = win_len;
        new_builder.slide_len = slide_len;
        new_builder.lateness = lateness;
        new_builder.winType = winType;
        return new_builder;
    }

    /** 
     *  \brief Set the output batch size of the MapReduce_Windows
     *  
     *  \param _outputBatchSize number of outputs per batch (zero means no batching)
     *  \return a reference to the builder object
     */ 
    MapReduce_Windows_Builder<map_func_t, reduce_func_t, key_t> &withOutputBatchSize(size_t _outputBatchSize)
    {
        outputBatchSize = _outputBatchSize;
        return *this;
    }

    /** 
     *  \brief Set the closing functional logic used by the MapReduce_Windows
     *  
     *  \param _closing_func closing functional logic (a function or a callable type)
     *  \return a reference to the builder object
     */ 
    template<typename closing_F_t>
    MapReduce_Windows_Builder<map_func_t, reduce_func_t, key_t> &withClosingFunction(closing_F_t _closing_func)
    {
        // static assert to check the signature
        static_assert(!std::is_same<decltype(check_closing_t(_closing_func)), std::false_type>::value,
            "WindFlow Compilation Error - unknown signature passed to withClosingFunction (MapReduce_Windows_Builder):\n"
            "  Candidate : void(RuntimeContext &)\n");
        closing_func = _closing_func;
        return *this;
    }

    /** 
     *  \brief Set the configuration for count-based windows
     *  
     *  \param _win_len window length (in number of tuples)
     *  \param _slide_len slide length (in number of tuples)
     *  \return a reference to the builder object
     */ 
    MapReduce_Windows_Builder<map_func_t, reduce_func_t, key_t> &withCBWindows(uint64_t _win_len,
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
     *  \return a reference to the builder object
     */ 
    MapReduce_Windows_Builder<map_func_t, reduce_func_t, key_t> &withTBWindows(std::chrono::microseconds _win_len,
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
    MapReduce_Windows_Builder<map_func_t, reduce_func_t, key_t> &withLateness(std::chrono::microseconds _lateness)
    {
        if (winType != Win_Type_t::TB) { // check that time-based semantics is used
            std::cerr << RED << "WindFlow Error: lateness can be set only for time-based windows" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        lateness = _lateness.count();
        return *this;
    }

    /** 
     *  \brief Create the MapReduce_Windows
     *  
     *  \return a new MapReduce_Windows instance
     */ 
    mr_wins_t build()
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
                         name,
                         outputBatchSize,
                         closing_func,
                         win_len,
                         slide_len,
                         lateness,
                         winType);
    }
};

/** 
 *  \class FFAT_Aggregator_Builder
 *  
 *  \brief Builder of the FFAT_Aggregator operator
 *  
 *  Builder class to ease the creation of the FFAT_Aggregator operator.
 */ 
template<typename lift_func_t, typename comb_func_t, typename key_t=empty_key_t>
class FFAT_Aggregator_Builder
{
private:
    template<typename T1, typename T2, typename T3> friend class FFAT_Aggregator_Builder; // friendship with all the instances of the FFAT_Aggregator_Builder template
    lift_func_t lift_func; // lift functional logic of the FFAT_Aggregator
    comb_func_t comb_func; // combine functional logic of the FFAT_Aggregator
    using tuple_t = decltype(get_tuple_t_Lift(lift_func)); // extracting the tuple_t type and checking the admissible signatures
    using result_t = decltype(get_result_t_Lift(lift_func)); // extracting the result_t type and checking the admissible signatures
    // static assert to check the signature of the FFAT_Aggregator_Builder functional logic
    static_assert(!(std::is_same<tuple_t, std::false_type>::value || std::is_same<result_t, std::false_type>::value),
        "WindFlow Compilation Error - unknown signature passed to the FFAT_Aggregator_Builder (first argument, lift logic):\n"
        "  Candidate 1 : void(const tuple_t &, result_t &)\n"
        "  Candidate 2 : void(const tuple_t &, result_t &, RuntimeContext &)\n");
    using result_t2 = decltype(get_tuple_t_Comb(comb_func));
    static_assert(!(std::is_same<std::false_type, result_t2>::value),
        "WindFlow Compilation Error - unknown signature passed to the FFAT_Aggregator_Builder (second argument, combine logic):\n"
        "  Candidate 1 : void(const result_t &, const result_t &, result_t &)\n"
        "  Candidate 2 : void(const result_t &, const result_t &, result_t &, RuntimeContext &)\n");
    static_assert(std::is_same<result_t, result_t2>::value &&
                  std::is_same<result_t2, decltype(get_result_t_Comb(comb_func))>::value,
        "WindFlow Compilation Error - type mismatch in the FFAT_Aggregator_Builder\n");
    // static assert to check that the tuple_t type must be default constructible
    static_assert(std::is_default_constructible<tuple_t>::value,
        "WindFlow Compilation Error - tuple_t type must be default constructible (FFAT_Aggregator_Builder):\n");
    using key_extractor_func_t = std::function<key_t(const tuple_t&)>; // type of the key extractor
    using ffat_agg_t = FFAT_Aggregator<lift_func_t, comb_func_t, key_extractor_func_t>; // type of the FFAT_Aggregator to be created by the builder
    using closing_func_t = std::function<void(RuntimeContext&)>; // type of the closing functional logic
    std::string name = "ffat_aggregator"; // name of the FFAT_Aggregator
    size_t parallelism = 1; // parallelism of the FFAT_Aggregator
    key_extractor_func_t key_extr = [](const tuple_t &t) -> key_t { return key_t(); }; // key extractor
    bool isKeyBySet = false; // true if a key extractor has been provided
    size_t outputBatchSize = 0; // output batch size of the FFAT_Aggregator
    closing_func_t closing_func = [](RuntimeContext &r) -> void { return; }; // closing functional logic
    uint64_t win_len=0; // window length in number of tuples or in time units
    uint64_t slide_len=0; // slide length in number of tuples or in time units
    uint64_t lateness=0; // lateness in time units
    Win_Type_t winType=Win_Type_t::CB; // window type (CB or TB)

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _lift_func lift functional logic of the FFAT_Aggregator (a function or a callable type)
     *  \param _comb_func combine functional logic of the FFAT_Aggregator (a function or a callable type)
     */ 
    FFAT_Aggregator_Builder(lift_func_t _lift_func,
                            comb_func_t _comb_func):
                            lift_func(_lift_func),
                            comb_func(_comb_func) {}

    /** 
     *  \brief Set the name of the FFAT_Aggregator
     *  
     *  \param _name of the FFAT_Aggregator
     *  \return a reference to the builder object
     */ 
    FFAT_Aggregator_Builder<lift_func_t, comb_func_t, key_t> &withName(std::string _name)
    {
        name = _name;
        return *this;
    }

    /** 
     *  \brief Set the parallelism of the FFAT_Aggregator
     *  
     *  \param _parallelism of the FFAT_Aggregator
     *  \return a reference to the builder object
     */ 
    FFAT_Aggregator_Builder<lift_func_t, comb_func_t, key_t> &withParallelism(size_t _parallelism)
    {
        parallelism = _parallelism;
        return *this;
    }

    /** 
     *  \brief Set the KEYBY routing mode of inputs to the FFAT_Aggregator
     *  
     *  \param _key_extr key extractor functional logic (a function or a callable type)
     *  \return a new builder object with the right key type
     */ 
    template<typename new_key_extractor_func_t>
    auto withKeyBy(new_key_extractor_func_t _key_extr)
    {
        // static assert to check the signature
        static_assert(!std::is_same<decltype(get_tuple_t_KeyExtr(_key_extr)), std::false_type>::value,
            "WindFlow Compilation Error - unknown signature passed to withKeyBy (FFAT_Aggregator_Builder):\n"
            "  Candidate : key_t(const tuple_t &)\n");
        // static assert to check that the tuple_t type of the new key extractor is the right one
        static_assert(std::is_same<decltype(get_tuple_t_KeyExtr(_key_extr)), tuple_t>::value,
            "WindFlow Compilation Error - key extractor receives a wrong input type (FFAT_Aggregator_Builder):\n");
        using new_key_t = decltype(get_key_t_KeyExtr(_key_extr)); // extract the key type
        // static assert to check the new_key_t type
        static_assert(!std::is_same<new_key_t, void>::value,
            "WindFlow Compilation Error - key type cannot be void (FFAT_Aggregator_Builder):\n");
        // static assert to check that new_key_t is default constructible
        static_assert(std::is_default_constructible<new_key_t>::value,
            "WindFlow Compilation Error - key type must be default constructible (FFAT_Aggregator_Builder):\n");
        FFAT_Aggregator_Builder<lift_func_t, comb_func_t, new_key_t> new_builder(lift_func, comb_func);
        new_builder.name = name;
        new_builder.parallelism = parallelism;
        new_builder.key_extr = _key_extr;
        new_builder.outputBatchSize = outputBatchSize;
        new_builder.closing_func = closing_func;
        new_builder.isKeyBySet = true;
        new_builder.win_len = win_len;
        new_builder.slide_len = slide_len;
        new_builder.lateness = lateness;
        new_builder.winType = winType;
        return new_builder;
    }

    /** 
     *  \brief Set the output batch size of the FFAT_Aggregator
     *  
     *  \param _outputBatchSize number of outputs per batch (zero means no batching)
     *  \return a reference to the builder object
     */ 
    FFAT_Aggregator_Builder<lift_func_t, comb_func_t, key_t> &withOutputBatchSize(size_t _outputBatchSize)
    {
        outputBatchSize = _outputBatchSize;
        return *this;
    }

    /** 
     *  \brief Set the closing functional logic used by the FFAT_Aggregator
     *  
     *  \param _closing_func closing functional logic (a function or a callable type)
     *  \return a reference to the builder object
     */ 
    template<typename closing_F_t>
    FFAT_Aggregator_Builder<lift_func_t, comb_func_t, key_t> &withClosingFunction(closing_F_t _closing_func)
    {
        // static assert to check the signature
        static_assert(!std::is_same<decltype(check_closing_t(_closing_func)), std::false_type>::value,
            "WindFlow Compilation Error - unknown signature passed to withClosingFunction (FFAT_Aggregator_Builder):\n"
            "  Candidate : void(RuntimeContext &)\n");
        closing_func = _closing_func;
        return *this;
    }

    /** 
     *  \brief Set the configuration for count-based windows
     *  
     *  \param _win_len window length (in number of tuples)
     *  \param _slide_len slide length (in number of tuples)
     *  \return a reference to the builder object
     */ 
    FFAT_Aggregator_Builder<lift_func_t, comb_func_t, key_t> &withCBWindows(uint64_t _win_len,
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
     *  \return a reference to the builder object
     */ 
    FFAT_Aggregator_Builder<lift_func_t, comb_func_t, key_t> &withTBWindows(std::chrono::microseconds _win_len,
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
    FFAT_Aggregator_Builder<lift_func_t, comb_func_t, key_t> &withLateness(std::chrono::microseconds _lateness)
    {
        if (winType != Win_Type_t::TB) { // check that time-based semantics is used
            std::cerr << RED << "WindFlow Error: lateness can be set only for time-based windows" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        lateness = _lateness.count();
        return *this;
    }

    /** 
     *  \brief Create the FFAT_Aggregator
     *  
     *  \return a new FFAT_Aggregator instance
     */ 
    ffat_agg_t build()
    {
        // static asserts to check that result_t is properly constructible
        if constexpr (std::is_same<key_t, empty_key_t>::value) { // case without key
            static_assert(std::is_constructible<result_t, uint64_t>::value,
                "WindFlow Compilation Error - result type must be constructible with a uint64_t (FFAT_Aggregator_Builder):\n");
        }
        else { // case with key
            static_assert(std::is_constructible<result_t, key_t, uint64_t>::value,
                "WindFlow Compilation Error - result type must be constructible with a key_t and uint64_t (FFAT_Aggregator_Builder):\n");            
        }
        // check the presence of a key extractor
        if (!isKeyBySet && parallelism > 1) {
            std::cerr << RED << "WindFlow Error: FFAT_Aggregator with parallelism > 1 requires a key extractor" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        return ffat_agg_t(lift_func,
                          comb_func,
                          key_extr,
                          parallelism,
                          name,
                          outputBatchSize,
                          closing_func,
                          win_len,
                          slide_len,
                          lateness,
                          winType);
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
class Sink_Builder
{
private:
    template<typename T1, typename T2> friend class Sink_Builder; // friendship with all the instances of the Sink_Builder template
    sink_func_t func; // functional logic of the Sink
    using tuple_t = decltype(get_tuple_t_Sink(func)); // extracting the tuple_t type and checking the admissible signatures
    // static assert to check the signature of the Sink functional logic
    static_assert(!(std::is_same<tuple_t, std::false_type>::value),
        "WindFlow Compilation Error - unknown signature passed to the Sink_Builder:\n"
        "  Candidate 1 : void(std::optional<tuple_t> &)\n"
        "  Candidate 2 : void(std::optional<tuple_t> &, RuntimeContext &)\n");
    // static assert to check that the tuple_t type must be default constructible
    static_assert(std::is_default_constructible<tuple_t>::value,
        "WindFlow Compilation Error - tuple_t type must be default constructible (Sink_Builder):\n");
    using key_extractor_func_t = std::function<key_t(const tuple_t&)>; // type of the key extractor
    using sink_t = Sink<sink_func_t, key_extractor_func_t>; // type of the Sink to be created by the builder
    using closing_func_t = std::function<void(RuntimeContext&)>; // type of the closing functional logic
    std::string name = "sink"; // name of the Sink
    size_t parallelism = 1; // parallelism of the Sink
    Routing_Mode_t input_routing_mode = Routing_Mode_t::FORWARD; // routing mode of inputs to the Sink
    key_extractor_func_t key_extr = [](const tuple_t &t) -> key_t { return key_t(); }; // key extractor
    closing_func_t closing_func = [](RuntimeContext &r) -> void { return; }; // closing functional logic

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _func functional logic of the Sink (a function or a callable type)
     */ 
    Sink_Builder(sink_func_t _func):
                 func(_func) {}

    /** 
     *  \brief Set the name of the Sink
     *  
     *  \param _name of the Sink
     *  \return a reference to the builder object
     */ 
    Sink_Builder<sink_func_t, key_t> &withName(std::string _name)
    {
        name = _name;
        return *this;
    }

    /** 
     *  \brief Set the parallelism of the Sink
     *  
     *  \param _parallelism of the Sink
     *  \return a reference to the builder object
     */ 
    Sink_Builder<sink_func_t, key_t> &withParallelism(size_t _parallelism)
    {
        parallelism = _parallelism;
        return *this;
    }

    /** 
     *  \brief Set the KEYBY routing mode of inputs to the Sink
     *  
     *  \param _key_extr key extractor functional logic (a function or a callable type)
     *  \return a new builder object with the right key type
     */ 
    template<typename new_key_extractor_func_t>
    auto withKeyBy(new_key_extractor_func_t _key_extr)
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
        Sink_Builder<sink_func_t, new_key_t> new_builder(func);
        new_builder.name = name;
        new_builder.parallelism = parallelism;
        new_builder.input_routing_mode = Routing_Mode_t::KEYBY;
        new_builder.key_extr = _key_extr;
        new_builder.closing_func = closing_func;
        return new_builder;
    }

    /** 
     *  \brief Set the closing functional logic used by the Sink
     *  
     *  \param _closing_func closing functional logic (a function or a callable type)
     *  \return a reference to the builder object
     */ 
    template<typename closing_F_t>
    Sink_Builder<sink_func_t, key_t> &withClosingFunction(closing_F_t _closing_func)
    {
        // static assert to check the signature
        static_assert(!std::is_same<decltype(check_closing_t(_closing_func)), std::false_type>::value,
            "WindFlow Compilation Error - unknown signature passed to withClosingFunction (Sink_Builder):\n"
            "  Candidate : void(RuntimeContext &)\n");
        closing_func = _closing_func;
        return *this;
    }

    /** 
     *  \brief Create the Sink
     *  
     *  \return a new Sink instance
     */ 
    sink_t build()
    {
        return sink_t(func,
                      key_extr,
                      parallelism,
                      name,
                      input_routing_mode,
                      closing_func);
    }
};

} // namespace wf

#endif
