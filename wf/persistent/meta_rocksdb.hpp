/**************************************************************************************
 *  Copyright (c) 2019- Gabriele Mencagli and Simone Frassinelli
 *
 *  This file is part of WindFlow.
 *
 *  WindFlow is free software dual licensed under the GNU LGPL or MIT License.
 *  You can redistribute it and/or modify it under the terms of the
 *    * GNU Lesser General Public License as published by
 *      the Free Software Foundation, either version 3 of the License, or
 *      (at your option) any later version
 *    OR
 *    * MIT License: https://github.com/ParaGroup/WindFlow/blob/master/LICENSE.MIT
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
 *  @file    meta_rocksdb.hpp
 *  @author  Gabriele Mencagli and Simone Frassinelli
 *  
 *  @brief Metafunctions used by the WindFlow library for operators using
 *         RocksDB as the back-end for in-flight state
 *  
 *  @section Metafunctions-RocksDB (Description)
 *  
 *  Set of metafunctions used by the WindFlow library for operators using
 *  RocksDB as the back-end for in-flight state.
 */ 

#ifndef META_ROCKSDB_H
#define META_ROCKSDB_H

// includes
#include<atomic>
#include<optional>
#include<basic.hpp>
#include<context.hpp>
#include<shipper.hpp>

namespace wf {

/******************************************************* SERIALIZATION  ******************************************************/
// declaration of functions to extract the type of the state attribute form the serialization logic
template<typename F_t, typename Arg>
Arg get_state_t_Serialize(std::string (F_t::*)(Arg&) const);

template<typename F_t, typename Arg>
Arg get_state_t_Serialize(std::string (F_t::*)(Arg&));

template<typename Arg>
Arg get_state_t_Serialize(std::string (*)(Arg&));

template<typename F_t>
decltype(get_state_t_Serialize(&F_t::operator())) get_state_t_Serialize(F_t);

std::false_type get_state_t_Serialize(...); // black hole
/*****************************************************************************************************************************/

/****************************************************** DESERIALIZATION  *****************************************************/
// declaration of functions to extract the type of the state attribute form the deserialization logic
template<typename F_t, typename Arg>
Arg get_state_t_Deserialize(Arg (F_t::*)(std::string &) const);

template<typename F_t, typename Arg>
Arg get_state_t_Deserialize(Arg (F_t::*)(std::string &));

template<typename Arg>
Arg get_state_t_Deserialize(Arg (*)(std::string &));

template<typename F_t>
decltype(get_state_t_Deserialize(&F_t::operator())) get_state_t_Deserialize(F_t);

std::false_type get_state_t_Deserialize(...); // black hole
/*****************************************************************************************************************************/

/***************************************************** P_FILTER OPERATOR *****************************************************/
// declaration of functions to extract the input type of the P_Filter operator
template<typename F_t, typename Arg1, typename Arg2>
Arg1 get_tuple_t_P_Filter(bool (F_t::*)(Arg1 &, Arg2 &) const); // inplace version

template<typename F_t, typename Arg1, typename Arg2>
Arg1 get_tuple_t_P_Filter(bool (F_t::*)(Arg1 &, Arg2 &)); // inplace version

template<typename Arg1, typename Arg2>
Arg1 get_tuple_t_P_Filter(bool (*)(Arg1 &, Arg2 &)); // inplace version

template<typename F_t, typename Arg1, typename Arg2>
Arg1 get_tuple_t_P_Filter(bool (F_t::*)(Arg1 &, Arg2 &, RuntimeContext &) const); // inplace riched version

template<typename F_t, typename Arg1, typename Arg2>
Arg1 get_tuple_t_P_Filter(bool (F_t::*)(Arg1 &, Arg2 &, RuntimeContext &)); // inplace riched version

template<typename Arg1, typename Arg2>
Arg1 get_tuple_t_P_Filter(bool (*)(Arg1 &, Arg2 &, RuntimeContext &)); // inplace riched version

template<typename F_t>
decltype(get_tuple_t_P_Filter(&F_t::operator())) get_tuple_t_P_Filter(F_t);

std::false_type get_tuple_t_P_Filter(...); // black hole

// declaration of functions to extract the result type of the P_Filter operator
template<typename F_t, typename Arg1, typename Arg2>
Arg2 get_state_t_P_Filter(bool (F_t::*)(Arg1 &, Arg2 &) const); // inplace version

template<typename F_t, typename Arg1, typename Arg2>
Arg2 get_state_t_P_Filter(bool (F_t::*)(Arg1 &, Arg2 &)); // inplace version

template<typename Arg1, typename Arg2>
Arg2 get_state_t_P_Filter(bool (*)(Arg1 &, Arg2 &)); // inplace version

template<typename F_t, typename Arg1, typename Arg2>
Arg2 get_state_t_P_Filter(bool (F_t::*)(Arg1 &, Arg2 &, RuntimeContext &) const); // inplace riched version

template<typename F_t, typename Arg1, typename Arg2>
Arg2 get_state_t_P_Filter(bool (F_t::*)(Arg1 &, Arg2 &, RuntimeContext &)); // inplace riched version

template<typename Arg1, typename Arg2>
Arg2 get_state_t_P_Filter(bool (*)(Arg1 &, Arg2 &, RuntimeContext &)); // inplace riched version

template<typename F_t>
decltype(get_state_t_P_Filter(&F_t::operator())) get_state_t_P_Filter(F_t);

std::false_type get_state_t_P_Filter(...); // black hole
/*****************************************************************************************************************************/

/****************************************************** P_MAP OPERATOR *******************************************************/
// declaration of functions to extract the input type of the P_Map operator
template<typename F_t, typename Arg1, typename Arg2, typename Arg3> // non-inplace version
Arg1 get_tuple_t_P_Map(Arg2 (F_t::*)(const Arg1 &, Arg3 &) const);

template<typename F_t, typename Arg1, typename Arg2, typename Arg3> // non-inplace version
Arg1 get_tuple_t_P_Map(Arg2 (F_t::*)(const Arg1 &, Arg3 &));

template<typename Arg1, typename Arg2, typename Arg3> // non-inplace version
Arg1 get_tuple_t_P_Map(Arg2 (*)(const Arg1 &, Arg3 &));

template<typename F_t, typename Arg1, typename Arg2, typename Arg3> // non-inplace riched version
Arg1 get_tuple_t_P_Map(Arg2 (F_t::*)(const Arg1 &, Arg3 &, RuntimeContext &) const);

template<typename F_t, typename Arg1, typename Arg2, typename Arg3> // non-inplace riched version
Arg1 get_tuple_t_P_Map(Arg2 (F_t::*)(const Arg1 &, Arg3 &, RuntimeContext &));

template<typename Arg1, typename Arg2, typename Arg3> // non-inplace riched version
Arg1 get_tuple_t_P_Map(Arg2 (*)(const Arg1 &, Arg3 &, RuntimeContext &));

template<typename F_t, typename Arg1, typename Arg2> // inplace version
Arg1 get_tuple_t_P_Map(void (F_t::*)(Arg1 &, Arg2 &) const);

template<typename F_t, typename Arg1, typename Arg2> // inplace version
Arg1 get_tuple_t_P_Map(void (F_t::*)(Arg1 &, Arg2 &));

template<typename Arg1, typename Arg2> // inplace version
Arg1 get_tuple_t_P_Map(void (*)(Arg1 &, Arg2 &));

template<typename F_t, typename Arg1, typename Arg2> // inplace riched version
Arg1 get_tuple_t_P_Map(void (F_t::*)(Arg1 &, Arg2 &, RuntimeContext &) const);

template<typename F_t, typename Arg1, typename Arg2> // inplace riched version
Arg1 get_tuple_t_P_Map(void (F_t::*)(Arg1 &, Arg2 &, RuntimeContext &));

template<typename Arg1, typename Arg2> // inplace riched version
Arg1 get_tuple_t_P_Map(void (*)(Arg1 &, Arg2 &, RuntimeContext &));

template<typename F_t>
decltype(get_tuple_t_P_Map(&F_t::operator())) get_tuple_t_P_Map(F_t);

std::false_type get_tuple_t_P_Map(...); // black hole

// declaration of functions to extract the state type of the P_Map operator
template<typename F_t, typename Arg1, typename Arg2, typename Arg3> // non-inplace version
Arg2 get_state_t_P_Map(Arg3 (F_t::*)(const Arg1 &, Arg2 &) const);

template<typename F_t, typename Arg1, typename Arg2, typename Arg3> // non-inplace version
Arg2 get_state_t_P_Map(Arg3 (F_t::*)(const Arg1 &, Arg2 &));

template<typename Arg1, typename Arg2, typename Arg3> // non-inplace version
Arg2 get_state_t_P_Map(Arg3 (*)(const Arg1 &, Arg2 &));

template<typename F_t, typename Arg1, typename Arg2, typename Arg3> // non-inplace riched version
Arg2 get_state_t_P_Map(Arg3 (F_t::*)(const Arg1 &, Arg2 &, RuntimeContext &) const);

template<typename F_t, typename Arg1, typename Arg2, typename Arg3> // non-inplace riched version
Arg2 get_state_t_P_Map(Arg3 (F_t::*)(const Arg1 &, Arg2 &, RuntimeContext &));

template<typename Arg1, typename Arg2, typename Arg3> // non-inplace riched version
Arg2 get_state_t_P_Map(Arg3 (*)(const Arg1 &, Arg2 &, RuntimeContext &));

template<typename F_t, typename Arg1, typename Arg2> // inplace version
Arg2 get_state_t_P_Map(void (F_t::*)(Arg1 &, Arg2 &) const);

template<typename F_t, typename Arg1, typename Arg2> // inplace version
Arg2 get_state_t_P_Map(void (F_t::*)(Arg1 &, Arg2 &));

template<typename Arg1, typename Arg2> // inplace version
Arg2 get_state_t_P_Map(void (*)(Arg1 &, Arg2 &));

template<typename F_t, typename Arg1, typename Arg2> // inplace riched version
Arg2 get_state_t_P_Map(void (F_t::*)(Arg1 &, Arg2 &, RuntimeContext &) const);

template<typename F_t, typename Arg1, typename Arg2> // inplace riched version
Arg2 get_state_t_P_Map(void (F_t::*)(Arg1 &, Arg2 &, RuntimeContext &));

template<typename Arg1, typename Arg2> // inplace riched version
Arg2 get_state_t_P_Map(void (*)(Arg1 &, Arg2 &, RuntimeContext &));

template<typename F_t>
decltype(get_state_t_P_Map(&F_t::operator())) get_state_t_P_Map(F_t);

std::false_type get_state_t_P_Map(...); // black hole

// declaration of functions to extract the result type of the P_Map operator
template<typename F_t, typename Arg1, typename Arg2, typename Arg3> // non-inplace version
Arg3 get_result_t_P_Map(Arg3 (F_t::*)(const Arg1 &, Arg2 &) const);

template<typename F_t, typename Arg1, typename Arg2, typename Arg3> // non-inplace version
Arg3 get_result_t_P_Map(Arg3 (F_t::*)(const Arg1 &, Arg2 &));

template<typename Arg1, typename Arg2, typename Arg3> // non-inplace version
Arg3 get_result_t_P_Map(Arg3 (*)(const Arg1 &, Arg2 &));

template<typename F_t, typename Arg1, typename Arg2, typename Arg3> // non-inplace riched version
Arg3 get_result_t_P_Map(Arg3 (F_t::*)(const Arg1 &, Arg2 &, RuntimeContext &) const);

template<typename F_t, typename Arg1, typename Arg2, typename Arg3> // non-inplace riched version
Arg3 get_result_t_P_Map(Arg3 (F_t::*)(const Arg1 &, Arg2 &, RuntimeContext &));

template<typename Arg1, typename Arg2, typename Arg3> // non-inplace riched version
Arg3 get_result_t_P_Map(Arg3 (*)(const Arg1 &, Arg2 &, RuntimeContext &));

template<typename F_t, typename Arg1, typename Arg2> // inplace version
Arg1 get_result_t_P_Map(void (F_t::*)(Arg1 &, Arg2 &) const);

template<typename F_t, typename Arg1, typename Arg2> // inplace version
Arg1 get_result_t_P_Map(void (F_t::*)(Arg1 &, Arg2 &));

template<typename Arg1, typename Arg2> // inplace version
Arg1 get_result_t_P_Map(void (*)(Arg1 &, Arg2 &));

template<typename F_t, typename Arg1, typename Arg2> // inplace riched version
Arg1 get_result_t_P_Map(void (F_t::*)(Arg1 &, Arg2 &, RuntimeContext &) const);

template<typename F_t, typename Arg1, typename Arg2> // inplace riched version
Arg1 get_result_t_P_Map(void (F_t::*)(Arg1 &, Arg2 &, RuntimeContext &));

template<typename Arg1, typename Arg2> // inplace riched version
Arg1 get_result_t_P_Map(void (*)(Arg1 &, Arg2 &, RuntimeContext &));

template<typename F_t>
decltype(get_result_t_P_Map(&F_t::operator())) get_result_t_P_Map(F_t);

std::false_type get_result_t_P_Map(...); // black hole

// declaration of functions to extract the return type of the P_Map operator
template<typename F_t, typename Arg1, typename Arg2, typename Arg3> // non-inplace version
Arg2 get_return_t_P_Map(Arg2 (F_t::*)(const Arg1 &, Arg3 &) const);

template<typename F_t, typename Arg1, typename Arg2, typename Arg3> // non-inplace version
Arg2 get_return_t_P_Map(Arg2 (F_t::*)(const Arg1 &, Arg3 &));

template<typename Arg1, typename Arg2, typename Arg3> // non-inplace version
Arg2 get_return_t_P_Map(Arg2 (*)(const Arg1 &, Arg3 &));

template<typename F_t, typename Arg1, typename Arg2, typename Arg3> // non-inplace riched version
Arg2 get_return_t_P_Map(Arg2 (F_t::*)(const Arg1 &, Arg3 &, RuntimeContext &) const);

template<typename F_t, typename Arg1, typename Arg2, typename Arg3> // non-inplace riched version
Arg2 get_return_t_P_Map(Arg2 (F_t::*)(const Arg1 &, Arg3 &, RuntimeContext &));

template<typename Arg1, typename Arg2, typename Arg3> // non-inplace riched version
Arg2 get_return_t_P_Map(Arg2 (*)(const Arg1 &, Arg3 &, RuntimeContext &));

template<typename F_t, typename Arg1, typename Arg2> // inplace version
void get_return_t_P_Map(void (F_t::*)(Arg1 &, Arg2 &) const);

template<typename F_t, typename Arg1, typename Arg2> // inplace version
void get_return_t_P_Map(void (F_t::*)(Arg1 &, Arg2 &));

template<typename Arg1, typename Arg2> // inplace version
void get_return_t_P_Map(void (*)(Arg1 &, Arg2 &));

template<typename F_t, typename Arg1, typename Arg2> // inplace riched version
void get_return_t_P_Map(void (F_t::*)(Arg1 &, Arg2 &, RuntimeContext &) const);

template<typename F_t, typename Arg1, typename Arg2> // inplace riched version
void get_return_t_P_Map(void (F_t::*)(Arg1 &, Arg2 &, RuntimeContext &));

template<typename Arg1, typename Arg2> // inplace riched version
void get_return_t_P_Map(void (*)(Arg1 &, Arg2 &, RuntimeContext &));

template<typename F_t>
decltype(get_return_t_P_Map(&F_t::operator())) get_return_t_P_Map(F_t);

std::false_type get_return_t_P_Map(...); // black hole
/*****************************************************************************************************************************/

/*************************************************** P_FLATMAP OPERATOR ******************************************************/
// declaration of functions to extract the input type of the P_FlatMap operator
template<typename F_t, typename Arg1, typename Arg2, typename Arg3>
Arg1 get_tuple_t_P_FlatMap(void (F_t::*)(const Arg1 &, Arg3 &, Shipper<Arg2> &) const);

template<typename F_t, typename Arg1, typename Arg2, typename Arg3>
Arg1 get_tuple_t_P_FlatMap(void (F_t::*)(const Arg1 &, Arg3 &, Shipper<Arg2> &));

template<typename Arg1, typename Arg2, typename Arg3>
Arg1 get_tuple_t_P_FlatMap(void (*)(const Arg1 &, Arg3 &, Shipper<Arg2> &));

template<typename F_t, typename Arg1, typename Arg2, typename Arg3> // riched version
Arg1 get_tuple_t_P_FlatMap(void (F_t::*)(const Arg1 &, Arg3 &, Shipper<Arg2> &, RuntimeContext &) const);

template<typename F_t, typename Arg1, typename Arg2, typename Arg3> // riched version
Arg1 get_tuple_t_P_FlatMap(void (F_t::*)(const Arg1 &, Arg3 &, Shipper<Arg2> &, RuntimeContext &));

template<typename Arg1, typename Arg2, typename Arg3> // riched version
Arg1 get_tuple_t_P_FlatMap(void (*)(const Arg1 &, Arg3 &, Shipper<Arg2> &, RuntimeContext &));

template<typename F_t>
decltype(get_tuple_t_P_FlatMap(&F_t::operator())) get_tuple_t_P_FlatMap(F_t);

std::false_type get_tuple_t_P_FlatMap(...); // black hole

// declaration of functions to extract the result type of the P_FlatMap operator
template<typename F_t, typename Arg1, typename Arg2, typename Arg3>
Arg2 get_result_t_P_FlatMap(void (F_t::*)(const Arg1 &, Arg3 &, Shipper<Arg2> &) const);

template<typename F_t, typename Arg1, typename Arg2, typename Arg3>
Arg2 get_result_t_P_FlatMap(void (F_t::*)(const Arg1 &, Arg3 &, Shipper<Arg2> &));

template<typename Arg1, typename Arg2, typename Arg3>
Arg2 get_result_t_P_FlatMap(void (*)(const Arg1 &, Arg3 &, Shipper<Arg2> &));

template<typename F_t, typename Arg1, typename Arg2, typename Arg3> // riched version
Arg2 get_result_t_P_FlatMap(void (F_t::*)(const Arg1 &, Arg3 &, Shipper<Arg2> &, RuntimeContext &) const);

template<typename F_t, typename Arg1, typename Arg2, typename Arg3> // riched version
Arg2 get_result_t_P_FlatMap(void (F_t::*)(const Arg1 &, Arg3 &, Shipper<Arg2> &, RuntimeContext &));

template<typename Arg1, typename Arg2, typename Arg3> // riched version
Arg2 get_result_t_P_FlatMap(void (*)(const Arg1 &, Arg3 &, Shipper<Arg2> &, RuntimeContext &));

template<typename F_t>
decltype(get_result_t_P_FlatMap(&F_t::operator())) get_result_t_P_FlatMap(F_t);

std::false_type get_result_t_P_FlatMap(...); // black hole

// declaration of functions to extract the state type of the P_FlatMap operator
template<typename F_t, typename Arg1, typename Arg2, typename Arg3>
Arg3 get_state_t_P_FlatMap(void (F_t::*)(const Arg1 &, Arg3 &, Shipper<Arg2> &) const);

template<typename F_t, typename Arg1, typename Arg2, typename Arg3>
Arg3 get_state_t_P_FlatMap(void (F_t::*)(const Arg1 &, Arg3 &, Shipper<Arg2> &));

template<typename Arg1, typename Arg2, typename Arg3>
Arg3 get_state_t_P_FlatMap(void (*)(const Arg1 &, Arg3 &, Shipper<Arg2> &));

template<typename F_t, typename Arg1, typename Arg2, typename Arg3> // riched version
Arg3 get_state_t_P_FlatMap(void (F_t::*)(const Arg1 &, Arg3 &, Shipper<Arg2> &, RuntimeContext &) const);

template<typename F_t, typename Arg1, typename Arg2, typename Arg3> // riched version
Arg3 get_state_t_P_FlatMap(void (F_t::*)(const Arg1 &, Arg3 &, Shipper<Arg2> &, RuntimeContext &));

template<typename Arg1, typename Arg2, typename Arg3> // riched version
Arg3 get_state_t_P_FlatMap(void (*)(const Arg1 &, Arg3 &, Shipper<Arg2> &, RuntimeContext &));

template<typename F_t>
decltype(get_state_t_P_FlatMap(&F_t::operator())) get_state_t_P_FlatMap(F_t);

std::false_type get_state_t_P_FlatMap(...); // black hole
/*****************************************************************************************************************************/

/*************************************************** P_REDUCE OPERATOR *******************************************************/
// declaration of functions to extract the input type of the P_Reduce operator
template<typename F_t, typename Arg1, typename Arg2>
Arg1 get_tuple_t_P_Reduce(void (F_t::*)(const Arg1 &, Arg2 &) const);

template<typename F_t, typename Arg1, typename Arg2>
Arg1 get_tuple_t_P_Reduce(void (F_t::*)(const Arg1 &, Arg2 &));

template<typename Arg1, typename Arg2>
Arg1 get_tuple_t_P_Reduce(void (*)(const Arg1 &, Arg2 &));

template<typename F_t, typename Arg1, typename Arg2> // riched version
Arg1 get_tuple_t_P_Reduce(void (F_t::*)(const Arg1 &, Arg2 &, RuntimeContext &) const);

template<typename F_t, typename Arg1, typename Arg2> // riched version
Arg1 get_tuple_t_P_Reduce(void (F_t::*)(const Arg1 &, Arg2 &, RuntimeContext &));

template<typename Arg1, typename Arg2> // riched version
Arg1 get_tuple_t_P_Reduce(void (*)(const Arg1 &, Arg2 &, RuntimeContext &));

template<typename F_t>
decltype(get_tuple_t_P_Reduce(&F_t::operator())) get_tuple_t_P_Reduce(F_t);

std::false_type get_tuple_t_P_Reduce(...); // black hole

// declaration of functions to extract the state type of the P_Reduce operator
template<typename F_t, typename Arg1, typename Arg2>
Arg2 get_state_t_P_Reduce(void (F_t::*)(const Arg1 &, Arg2 &) const);

template<typename F_t, typename Arg1, typename Arg2>
Arg2 get_state_t_P_Reduce(void (F_t::*)(const Arg1 &, Arg2 &));

template<typename Arg1, typename Arg2>
Arg2 get_state_t_P_Reduce(void (*)(const Arg1 &, Arg2 &));

template<typename F_t, typename Arg1, typename Arg2> // riched version
Arg2 get_state_t_P_Reduce(void (F_t::*)(const Arg1 &, Arg2 &, RuntimeContext &) const);

template<typename F_t, typename Arg1, typename Arg2> // riched version
Arg2 get_state_t_P_Reduce(void (F_t::*)(const Arg1 &, Arg2 &, RuntimeContext &));

template<typename Arg1, typename Arg2> // riched version
Arg2 get_state_t_P_Reduce(void (*)(const Arg1 &, Arg2 &, RuntimeContext &));

template<typename F_t>
decltype(get_state_t_P_Reduce(&F_t::operator())) get_state_t_P_Reduce(F_t);

std::false_type get_state_t_P_Reduce(...); // black hole
/*****************************************************************************************************************************/

/***************************************************** P_SINK OPERATOR *******************************************************/
// declaration of functions to extract the input type of the P_Sink operator
template<typename F_t, typename Arg1, typename Arg2> // optional version
Arg1 get_tuple_t_P_Sink(void (F_t::*)(std::optional<Arg1> &, Arg2 &) const);

template<typename F_t, typename Arg1, typename Arg2> // optional version
Arg1 get_tuple_t_P_Sink(void (F_t::*)(std::optional<Arg1> &, Arg2 &));

template<typename Arg1, typename Arg2> // optional version
Arg1 get_tuple_t_P_Sink(void (*)(std::optional<Arg1> &, Arg2 &));

template<typename F_t, typename Arg1, typename Arg2> // optional riched version
Arg1 get_tuple_t_P_Sink(void (F_t::*)(std::optional<Arg1> &, Arg2 &, RuntimeContext &) const);

template<typename F_t, typename Arg1, typename Arg2> // optional riched version
Arg1 get_tuple_t_P_Sink(void (F_t::*)(std::optional<Arg1> &, Arg2 &, RuntimeContext &));

template<typename Arg1, typename Arg2> // optional riched version
Arg1 get_tuple_t_P_Sink(void (*)(std::optional<Arg1> &, Arg2 &, RuntimeContext &));

template<typename F_t, typename Arg1, typename Arg2> // optional (reference wrapper) version
Arg1 get_tuple_t_P_Sink(void (F_t::*)(std::optional<std::reference_wrapper<Arg1>>, Arg2 &) const);

template<typename F_t, typename Arg1, typename Arg2> // optional (reference wrapper) version
Arg1 get_tuple_t_P_Sink(void (F_t::*)(std::optional<std::reference_wrapper<Arg1>>, Arg2 &));

template<typename Arg1, typename Arg2> // optional (reference wrapper) version
Arg1 get_tuple_t_P_Sink(void (*)(std::optional<std::reference_wrapper<Arg1>>, Arg2 &));

template<typename F_t, typename Arg1, typename Arg2> // optional (reference wrapper) riched version
Arg1 get_tuple_t_P_Sink(void (F_t::*)(std::optional<std::reference_wrapper<Arg1>>, Arg2 &, RuntimeContext &) const);

template<typename F_t, typename Arg1, typename Arg2> // optional (reference wrapper) riched version
Arg1 get_tuple_t_P_Sink(void (F_t::*)(std::optional<std::reference_wrapper<Arg1>>, Arg2 &, RuntimeContext &));

template<typename Arg1, typename Arg2> // optional (reference wrapper) riched version
Arg1 get_tuple_t_P_Sink(void (*)(std::optional<std::reference_wrapper<Arg1>>, Arg2 &, RuntimeContext &));

template<typename F_t>
decltype(get_tuple_t_P_Sink(&F_t::operator())) get_tuple_t_P_Sink(F_t);

std::false_type get_tuple_t_P_Sink(...); // black hole

// declaration of functions to extract the state type of the P_Sink operator
template<typename F_t, typename Arg1, typename Arg2> // optional version
Arg2 get_state_t_P_Sink(void (F_t::*)(std::optional<Arg1> &, Arg2 &) const);

template<typename F_t, typename Arg1, typename Arg2> // optional version
Arg2 get_state_t_P_Sink(void (F_t::*)(std::optional<Arg1> &, Arg2 &));

template<typename Arg1, typename Arg2> // optional version
Arg2 get_state_t_P_Sink(void (*)(std::optional<Arg1> &, Arg2 &));

template<typename F_t, typename Arg1, typename Arg2> // optional riched version
Arg2 get_state_t_P_Sink(void (F_t::*)(std::optional<Arg1> &, Arg2 &, RuntimeContext &) const);

template<typename F_t, typename Arg1, typename Arg2> // optional riched version
Arg2 get_state_t_P_Sink(void (F_t::*)(std::optional<Arg1> &, Arg2 &, RuntimeContext &));

template<typename Arg1, typename Arg2> // optional riched version
Arg2 get_state_t_P_Sink(void (*)(std::optional<Arg1> &, Arg2 &, RuntimeContext &));

template<typename F_t, typename Arg1, typename Arg2> // optional (reference wrapper) version
Arg2 get_state_t_P_Sink(void (F_t::*)(std::optional<std::reference_wrapper<Arg1>>, Arg2 &) const);

template<typename F_t, typename Arg1, typename Arg2> // optional (reference wrapper) version
Arg2 get_state_t_P_Sink(void (F_t::*)(std::optional<std::reference_wrapper<Arg1>>, Arg2 &));

template<typename Arg1, typename Arg2> // optional (reference wrapper) version
Arg2 get_state_t_P_Sink(void (*)(std::optional<std::reference_wrapper<Arg1>>, Arg2 &));

template<typename F_t, typename Arg1, typename Arg2> // optional (reference wrapper) riched version
Arg2 get_state_t_P_Sink(void (F_t::*)(std::optional<std::reference_wrapper<Arg1>>, Arg2 &, RuntimeContext &) const);

template<typename F_t, typename Arg1, typename Arg2> // optional (reference wrapper) riched version
Arg2 get_state_t_P_Sink(void (F_t::*)(std::optional<std::reference_wrapper<Arg1>>, Arg2 &, RuntimeContext &));

template<typename Arg1, typename Arg2> // optional (reference wrapper) riched version
Arg2 get_state_t_P_Sink(void (*)(std::optional<std::reference_wrapper<Arg1>>, Arg2 &, RuntimeContext &));

template<typename F_t>
decltype(get_state_t_P_Sink(&F_t::operator())) get_state_t_P_Sink(F_t);

std::false_type get_state_t_P_Sink(...); // black hole
/*****************************************************************************************************************************/

} // namespace wf

#endif
