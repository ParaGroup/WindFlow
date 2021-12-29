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
 *  @file    meta.hpp
 *  @author  Gabriele Mencagli
 *  
 *  @brief Metafunctions used by the WindFlow library
 *  
 *  @section Metafunctions-1 (Description)
 *  
 *  Set of metafunctions used by the WindFlow library.
 */ 

#ifndef META_H
#define META_H

// includes
#include<atomic>
#include<optional>
#include<basic.hpp>
#include<context.hpp>
#include<shipper.hpp>
#include<iterable.hpp>
#include<source_shipper.hpp>

namespace wf {

/******************************************************* KEY EXTRACTOR *******************************************************/
// declaration of functions to extract the type of the key attribute form the key extractor
template<typename F_t, typename Arg1, typename Arg2>
Arg2 get_key_t_KeyExtr(Arg2 (F_t::*)(const Arg1&) const);

template<typename F_t, typename Arg1, typename Arg2>
Arg2 get_key_t_KeyExtr(Arg2 (F_t::*)(const Arg1&));

template<typename Arg1, typename Arg2>
Arg2 get_key_t_KeyExtr(Arg2 (*)(const Arg1&));

template<typename F_t>
decltype(get_key_t_KeyExtr(&F_t::operator())) get_key_t_KeyExtr(F_t);

std::false_type get_key_t_KeyExtr(...); // black hole

// declaration of functions to extract the type of the tuple attribute form the key extractor
template<typename F_t, typename Arg1, typename Arg2>
Arg1 get_tuple_t_KeyExtr(Arg2 (F_t::*)(const Arg1&) const);

template<typename F_t, typename Arg1, typename Arg2>
Arg1 get_tuple_t_KeyExtr(Arg2 (F_t::*)(const Arg1&));

template<typename Arg1, typename Arg2>
Arg1 get_tuple_t_KeyExtr(Arg2 (*)(const Arg1&));

template<typename F_t>
decltype(get_tuple_t_KeyExtr(&F_t::operator())) get_tuple_t_KeyExtr(F_t);

std::false_type get_tuple_t_KeyExtr(...); // black hole
/*****************************************************************************************************************************/

/****************************************************** SOURCE OPERATOR ******************************************************/
// declaration of functions to extract the output type form the Source operator
template<typename F_t, typename Arg> // single-loop version
Arg get_result_t_Source(void (F_t::*)(Source_Shipper<Arg>&) const);

template<typename F_t, typename Arg> // single-loop version
Arg get_result_t_Source(void (F_t::*)(Source_Shipper<Arg>&));

template<typename Arg> // single-loop version
Arg get_result_t_Source(void (*)(Source_Shipper<Arg>&));

template<typename F_t, typename Arg> // single-loop riched version
Arg get_result_t_Source(void (F_t::*)(Source_Shipper<Arg>&, RuntimeContext&) const);

template<typename F_t, typename Arg> // single-loop riched version
Arg get_result_t_Source(void (F_t::*)(Source_Shipper<Arg>&, RuntimeContext&));

template<typename Arg> // single-loop riched version
Arg get_result_t_Source(void (*)(Source_Shipper<Arg>&, RuntimeContext&));

template<typename F_t>
decltype(get_result_t_Source(&F_t::operator())) get_result_t_Source(F_t);

std::false_type get_result_t_Source(...); // black hole
/*****************************************************************************************************************************/

/****************************************************** FILTER OPERATOR ******************************************************/
// declaration of functions to extract the input type of the Filter operator
template<typename F_t, typename Arg>
Arg get_tuple_t_Filter(bool (F_t::*)(Arg&) const); // inplace version

template<typename F_t, typename Arg>
Arg get_tuple_t_Filter(bool (F_t::*)(Arg&)); // inplace version

template<typename Arg>
Arg get_tuple_t_Filter(bool (*)(Arg&)); // inplace version

template<typename F_t, typename Arg>
Arg get_tuple_t_Filter(bool (F_t::*)(Arg&, RuntimeContext&) const); // inplace riched version

template<typename F_t, typename Arg>
Arg get_tuple_t_Filter(bool (F_t::*)(Arg&, RuntimeContext&)); // inplace riched version

template<typename Arg>
Arg get_tuple_t_Filter(bool (*)(Arg&, RuntimeContext&)); // inplace riched version

template<typename F_t>
decltype(get_tuple_t_Filter(&F_t::operator())) get_tuple_t_Filter(F_t);

std::false_type get_tuple_t_Filter(...); // black hole

// declaration of functions to extract the output type of the Filter operator
template<typename F_t, typename Arg>
Arg get_result_t_Filter(bool (F_t::*)(Arg&) const); // inplace version

template<typename F_t, typename Arg>
Arg get_result_t_Filter(bool (F_t::*)(Arg&)); // inplace version

template<typename Arg>
Arg get_result_t_Filter(bool (*)(Arg&)); // inplace version

template<typename F_t, typename Arg>
Arg get_result_t_Filter(bool (F_t::*)(Arg&, RuntimeContext&) const); // inplace riched version

template<typename F_t, typename Arg>
Arg get_result_t_Filter(bool (F_t::*)(Arg&, RuntimeContext&)); // inplace riched version

template<typename Arg>
Arg get_result_t_Filter(bool (*)(Arg&, RuntimeContext&)); // inplace riched version

template<typename F_t>
decltype(get_result_t_Filter(&F_t::operator())) get_result_t_Filter(F_t);

std::false_type get_result_t_Filter(...); // black hole
/*****************************************************************************************************************************/

/****************************************************** MAP OPERATOR *********************************************************/
// declaration of functions to extract the input type of the Map operator
template<typename F_t, typename Arg1, typename Arg2> // non-inplace version
Arg1 get_tuple_t_Map(Arg2 (F_t::*)(const Arg1&) const);

template<typename F_t, typename Arg1, typename Arg2> // non-inplace version
Arg1 get_tuple_t_Map(Arg2 (F_t::*)(const Arg1&));

template<typename Arg1, typename Arg2> // non-inplace version
Arg1 get_tuple_t_Map(Arg2 (*)(const Arg1&));

template<typename F_t, typename Arg1, typename Arg2> // non-inplace riched version
Arg1 get_tuple_t_Map(Arg2 (F_t::*)(const Arg1&, RuntimeContext&) const);

template<typename F_t, typename Arg1, typename Arg2> // non-inplace riched version
Arg1 get_tuple_t_Map(Arg2 (F_t::*)(const Arg1&, RuntimeContext&));

template<typename Arg1, typename Arg2> // non-inplace riched version
Arg1 get_tuple_t_Map(Arg2 (*)(const Arg1&, RuntimeContext&));

template<typename F_t, typename Arg> // inplace version
Arg get_tuple_t_Map(void (F_t::*)(Arg&) const);

template<typename F_t, typename Arg> // inplace version
Arg get_tuple_t_Map(void (F_t::*)(Arg&));

template<typename Arg> // inplace version
Arg get_tuple_t_Map(void (*)(Arg&));

template<typename F_t, typename Arg> // inplace riched version
Arg get_tuple_t_Map(void (F_t::*)(Arg&, RuntimeContext&) const);

template<typename F_t, typename Arg> // inplace riched version
Arg get_tuple_t_Map(void (F_t::*)(Arg&, RuntimeContext&));

template<typename Arg> // inplace riched version
Arg get_tuple_t_Map(void (*)(Arg&, RuntimeContext&));

template<typename F_t>
decltype(get_tuple_t_Map(&F_t::operator())) get_tuple_t_Map(F_t);

std::false_type get_tuple_t_Map(...); // black hole

// declaration of functions to extract the result type of the Map operator
template<typename F_t, typename Arg1, typename Arg2> // non-inplace version
Arg2 get_result_t_Map(Arg2 (F_t::*)(const Arg1&) const);

template<typename F_t, typename Arg1, typename Arg2> // non-inplace version
Arg2 get_result_t_Map(Arg2 (F_t::*)(const Arg1&));

template<typename Arg1, typename Arg2> // non-inplace version
Arg2 get_result_t_Map(Arg2 (*)(const Arg1&));

template<typename F_t, typename Arg1, typename Arg2> // non-inplace riched version
Arg2 get_result_t_Map(Arg2 (F_t::*)(const Arg1&, RuntimeContext&) const);

template<typename F_t, typename Arg1, typename Arg2> // non-inplace riched version
Arg2 get_result_t_Map(Arg2 (F_t::*)(const Arg1&, RuntimeContext&));

template<typename Arg1, typename Arg2> // non-inplace riched version
Arg2 get_result_t_Map(Arg2 (*)(const Arg1&, RuntimeContext&));

template<typename F_t, typename Arg> // inplace version
Arg get_result_t_Map(void (F_t::*)(Arg&) const);

template<typename F_t, typename Arg> // inplace version
Arg get_result_t_Map(void (F_t::*)(Arg&));

template<typename Arg> // inplace version
Arg get_result_t_Map(void (*)(Arg&));

template<typename F_t, typename Arg> // inplace riched version
Arg get_result_t_Map(void (F_t::*)(Arg&, RuntimeContext&) const);

template<typename F_t, typename Arg> // inplace riched version
Arg get_result_t_Map(void (F_t::*)(Arg&, RuntimeContext&));

template<typename Arg> // inplace riched version
Arg get_result_t_Map(void (*)(Arg&, RuntimeContext&));

template<typename F_t>
decltype(get_result_t_Map(&F_t::operator())) get_result_t_Map(F_t);

std::false_type get_result_t_Map(...); // black hole

// declaration of functions to extract the return type of the Map operator
template<typename F_t, typename Arg1, typename Arg2> // non-inplace version
Arg2 get_return_t_Map(Arg2 (F_t::*)(const Arg1&) const);

template<typename F_t, typename Arg1, typename Arg2> // non-inplace version
Arg2 get_return_t_Map(Arg2 (F_t::*)(const Arg1&));

template<typename Arg1, typename Arg2> // non-inplace version
Arg2 get_return_t_Map(Arg2 (*)(const Arg1&));

template<typename F_t, typename Arg1, typename Arg2> // non-inplace riched version
Arg2 get_return_t_Map(Arg2 (F_t::*)(const Arg1&, RuntimeContext&) const);

template<typename F_t, typename Arg1, typename Arg2> // non-inplace riched version
Arg2 get_return_t_Map(Arg2 (F_t::*)(const Arg1&, RuntimeContext&));

template<typename Arg1, typename Arg2> // non-inplace riched version
Arg2 get_return_t_Map(Arg2 (*)(const Arg1&, RuntimeContext&));

template<typename F_t, typename Arg> // inplace version
void get_return_t_Map(void (F_t::*)(Arg&) const);

template<typename F_t, typename Arg> // inplace version
void get_return_t_Map(void (F_t::*)(Arg&));

template<typename Arg> // inplace version
void get_return_t_Map(void (*)(Arg&));

template<typename F_t, typename Arg> // inplace riched version
void get_return_t_Map(void (F_t::*)(Arg&, RuntimeContext&) const);

template<typename F_t, typename Arg> // inplace riched version
void get_return_t_Map(void (F_t::*)(Arg&, RuntimeContext&));

template<typename Arg> // inplace riched version
void get_return_t_Map(void (*)(Arg&, RuntimeContext&));

template<typename F_t>
decltype(get_return_t_Map(&F_t::operator())) get_return_t_Map(F_t);

std::false_type get_return_t_Map(...); // black hole
/*****************************************************************************************************************************/

/**************************************************** FLATMAP OPERATOR *******************************************************/
// declaration of functions to extract the input type of the FlatMap operator
template<typename F_t, typename Arg1, typename Arg2>
Arg1 get_tuple_t_FlatMap(void (F_t::*)(const Arg1&, Shipper<Arg2>&) const);

template<typename F_t, typename Arg1, typename Arg2>
Arg1 get_tuple_t_FlatMap(void (F_t::*)(const Arg1&, Shipper<Arg2>&));

template<typename Arg1, typename Arg2>
Arg1 get_tuple_t_FlatMap(void (*)(const Arg1&, Shipper<Arg2>&));

template<typename F_t, typename Arg1, typename Arg2> // riched version
Arg1 get_tuple_t_FlatMap(void (F_t::*)(const Arg1&, Shipper<Arg2>&, RuntimeContext&) const);

template<typename F_t, typename Arg1, typename Arg2> // riched version
Arg1 get_tuple_t_FlatMap(void (F_t::*)(const Arg1&, Shipper<Arg2>&, RuntimeContext&));

template<typename Arg1, typename Arg2> // riched version
Arg1 get_tuple_t_FlatMap(void (*)(const Arg1&, Shipper<Arg2>&, RuntimeContext&));

template<typename F_t>
decltype(get_tuple_t_FlatMap(&F_t::operator())) get_tuple_t_FlatMap(F_t);

std::false_type get_tuple_t_FlatMap(...); // black hole

// declaration of functions to extract the result type of the FlatMap operator
template<typename F_t, typename Arg1, typename Arg2>
Arg2 get_result_t_FlatMap(void (F_t::*)(const Arg1&, Shipper<Arg2>&) const);

template<typename F_t, typename Arg1, typename Arg2>
Arg2 get_result_t_FlatMap(void (F_t::*)(const Arg1&, Shipper<Arg2>&));

template<typename Arg1, typename Arg2>
Arg2 get_result_t_FlatMap(void (*)(const Arg1&, Shipper<Arg2>&));

template<typename F_t, typename Arg1, typename Arg2> // riched version
Arg2 get_result_t_FlatMap(void (F_t::*)(const Arg1&, Shipper<Arg2>&, RuntimeContext&) const);

template<typename F_t, typename Arg1, typename Arg2> // riched version
Arg2 get_result_t_FlatMap(void (F_t::*)(const Arg1&, Shipper<Arg2>&, RuntimeContext&));

template<typename Arg1, typename Arg2> // riched version
Arg2 get_result_t_FlatMap(void (*)(const Arg1&, Shipper<Arg2>&, RuntimeContext&));

template<typename F_t>
decltype(get_result_t_FlatMap(&F_t::operator())) get_result_t_FlatMap(F_t);

std::false_type get_result_t_FlatMap(...); // black hole
/*****************************************************************************************************************************/

/**************************************************** REDUCE OPERATOR ********************************************************/
// declaration of functions to extract the input type of the Accumulator operator
template<typename F_t, typename Arg1, typename Arg2>
Arg1 get_tuple_t_Reduce(void (F_t::*)(const Arg1&, Arg2&) const);

template<typename F_t, typename Arg1, typename Arg2>
Arg1 get_tuple_t_Reduce(void (F_t::*)(const Arg1&, Arg2&));

template<typename Arg1, typename Arg2>
Arg1 get_tuple_t_Reduce(void (*)(const Arg1&, Arg2&));

template<typename F_t, typename Arg1, typename Arg2> // riched version
Arg1 get_tuple_t_Reduce(void (F_t::*)(const Arg1&, Arg2&, RuntimeContext&) const);

template<typename F_t, typename Arg1, typename Arg2> // riched version
Arg1 get_tuple_t_Reduce(void (F_t::*)(const Arg1&, Arg2&, RuntimeContext&));

template<typename Arg1, typename Arg2> // riched version
Arg1 get_tuple_t_Reduce(void (*)(const Arg1&, Arg2&, RuntimeContext&));

template<typename F_t>
decltype(get_tuple_t_Reduce(&F_t::operator())) get_tuple_t_Reduce(F_t);

std::false_type get_tuple_t_Reduce(...); // black hole

// declaration of functions to extract the state type of the Accumulator operator
template<typename F_t, typename Arg1, typename Arg2>
Arg2 get_state_t_Reduce(void (F_t::*)(const Arg1&, Arg2&) const);

template<typename F_t, typename Arg1, typename Arg2>
Arg2 get_state_t_Reduce(void (F_t::*)(const Arg1&, Arg2&));

template<typename Arg1, typename Arg2>
Arg2 get_state_t_Reduce(void (*)(const Arg1&, Arg2&));

template<typename F_t, typename Arg1, typename Arg2> // riched version
Arg2 get_state_t_Reduce(void (F_t::*)(const Arg1&, Arg2&, RuntimeContext&) const);

template<typename F_t, typename Arg1, typename Arg2> // riched version
Arg2 get_state_t_Reduce(void (F_t::*)(const Arg1&, Arg2&, RuntimeContext&));

template<typename Arg1, typename Arg2> // riched version
Arg2 get_state_t_Reduce(void (*)(const Arg1&, Arg2&, RuntimeContext&));

template<typename F_t>
decltype(get_state_t_Reduce(&F_t::operator())) get_state_t_Reduce(F_t);

std::false_type get_state_t_Reduce(...); // black hole
/*****************************************************************************************************************************/

/****************************************************** SINK OPERATOR ********************************************************/
// declaration of functions to extract the input type of the Sink operator
template<typename F_t, typename Arg> // optional version
Arg get_tuple_t_Sink(void (F_t::*)(std::optional<Arg>&) const);

template<typename F_t, typename Arg> // optional version
Arg get_tuple_t_Sink(void (F_t::*)(std::optional<Arg>&));

template<typename Arg> // optional version
Arg get_tuple_t_Sink(void (*)(std::optional<Arg>&));

template<typename F_t, typename Arg> // optional riched version
Arg get_tuple_t_Sink(void (F_t::*)(std::optional<Arg>&, RuntimeContext&) const);

template<typename F_t, typename Arg> // optional riched version
Arg get_tuple_t_Sink(void (F_t::*)(std::optional<Arg>&, RuntimeContext&));

template<typename Arg> // optional riched version
Arg get_tuple_t_Sink(void (*)(std::optional<Arg>&, RuntimeContext&));

template<typename F_t, typename Arg> // optional (reference wrapper) version
Arg get_tuple_t_Sink(void (F_t::*)(std::optional<std::reference_wrapper<Arg>>) const);

template<typename F_t, typename Arg> // optional (reference wrapper) version
Arg get_tuple_t_Sink(void (F_t::*)(std::optional<std::reference_wrapper<Arg>>));

template<typename Arg> // optional (reference wrapper) version
Arg get_tuple_t_Sink(void (*)(std::optional<std::reference_wrapper<Arg>>));

template<typename F_t, typename Arg> // optional (reference wrapper) riched version
Arg get_tuple_t_Sink(void (F_t::*)(std::optional<std::reference_wrapper<Arg>>, RuntimeContext&) const);

template<typename F_t, typename Arg> // optional (reference wrapper) riched version
Arg get_tuple_t_Sink(void (F_t::*)(std::optional<std::reference_wrapper<Arg>>, RuntimeContext&));

template<typename Arg> // optional (reference wrapper) riched version
Arg get_tuple_t_Sink(void (*)(std::optional<std::reference_wrapper<Arg>>, RuntimeContext&));

template<typename F_t>
decltype(get_tuple_t_Sink(&F_t::operator())) get_tuple_t_Sink(F_t);

std::false_type get_tuple_t_Sink(...); // black hole
/*****************************************************************************************************************************/

/**************************************************** WINDOWED OPERATORS *****************************************************/
// declaration of functions to extract the tuple type from the signature of the window function
template<typename F_t, typename Arg1, typename Arg2> // non-incremenatal version
Arg1 get_tuple_t_Win(void (F_t::*)(const Iterable<Arg1>&, Arg2&) const);

template<typename F_t, typename Arg1, typename Arg2> // non-incremenatal version
Arg1 get_tuple_t_Win(void (F_t::*)(const Iterable<Arg1>&, Arg2&));

template<typename Arg1, typename Arg2> // non-incremenatal version
Arg1 get_tuple_t_Win(void (*)(const Iterable<Arg1>&, Arg2&));

template<typename F_t, typename Arg1, typename Arg2> // non-incremenatal riched version
Arg1 get_tuple_t_Win(void (F_t::*)(const Iterable<Arg1>&, Arg2&, RuntimeContext&) const);

template<typename F_t, typename Arg1, typename Arg2> // non-incremenatal riched version
Arg1 get_tuple_t_Win(void (F_t::*)(const Iterable<Arg1>&, Arg2&, RuntimeContext&));

template<typename Arg1, typename Arg2> // non-incremenatal riched version
Arg1 get_tuple_t_Win(void (*)(const Iterable<Arg1>&, Arg2&, RuntimeContext&));

template<typename F_t, typename Arg1, typename Arg2> // incremental version
Arg1 get_tuple_t_Win(void (F_t::*)(const Arg1&, Arg2&) const);

template<typename F_t, typename Arg1, typename Arg2> // incremental version
Arg1 get_tuple_t_Win(void (F_t::*)(const Arg1&, Arg2&));

template<typename Arg1, typename Arg2> // incremental version
Arg1 get_tuple_t_Win(void (*)(const Arg1&, Arg2&));

template<typename F_t, typename Arg1, typename Arg2> // incremental riched version
Arg1 get_tuple_t_Win(void (F_t::*)(const Arg1&, Arg2&, RuntimeContext&) const);

template<typename F_t, typename Arg1, typename Arg2> // incremental riched version
Arg1 get_tuple_t_Win(void (F_t::*)(const Arg1&, Arg2&, RuntimeContext&));

template<typename Arg1, typename Arg2> // incremental riched version
Arg1 get_tuple_t_Win(void (*)(const Arg1&, Arg2&, RuntimeContext&));

template<typename F_t>
decltype(get_tuple_t_Win(&F_t::operator())) get_tuple_t_Win(F_t);

std::false_type get_tuple_t_Win(...); // black hole

// declaration of functions to extract the tuple type from the signature of the lift function
template<typename F_t, typename Arg1, typename Arg2> // lift function
Arg1 get_tuple_t_Lift(void (F_t::*)(const Arg1&, Arg2&) const);

template<typename F_t, typename Arg1, typename Arg2> // lift function
Arg1 get_tuple_t_Lift(void (F_t::*)(const Arg1&, Arg2&));

template<typename Arg1, typename Arg2> // lift function
Arg1 get_tuple_t_Lift(void (*)(const Arg1&, Arg2&));

template<typename F_t, typename Arg1, typename Arg2> // lift rich function
Arg1 get_tuple_t_Lift(void (F_t::*)(const Arg1&, Arg2&, RuntimeContext&) const);

template<typename F_t, typename Arg1, typename Arg2> // lift rich function
Arg1 get_tuple_t_Lift(void (F_t::*)(const Arg1&, Arg2&, RuntimeContext&));

template<typename Arg1, typename Arg2> // lift rich function
Arg1 get_tuple_t_Lift(void (*)(const Arg1&, Arg2&, RuntimeContext&));

template<typename F_t>
decltype(get_tuple_t_Lift(&F_t::operator())) get_tuple_t_Lift(F_t);

std::false_type get_tuple_t_Lift(...); // black hole

// declaration of functions to extract the tuple type from the signature of the combine function
template<typename F_t, typename Arg> // combine function
Arg get_tuple_t_Comb(void (F_t::*)(const Arg&, const Arg&, Arg&) const);

template<typename F_t, typename Arg> // combine function
Arg get_tuple_t_Comb(void (F_t::*)(const Arg&, const Arg&, Arg&));

template<typename Arg> // combine function
Arg get_tuple_t_Comb(void (*)(const Arg&, const Arg&, Arg&));

template<typename F_t, typename Arg> // combine riched function
Arg get_tuple_t_Comb(void (F_t::*)(const Arg&, const Arg&, Arg&, RuntimeContext&) const);

template<typename F_t, typename Arg> // combine riched function
Arg get_tuple_t_Comb(void (F_t::*)(const Arg&, const Arg&, Arg&, RuntimeContext&));

template<typename Arg> // combine riched function
Arg get_tuple_t_Comb(void (*)(const Arg&, const Arg&, Arg&, RuntimeContext&));

template<typename F_t>
decltype(get_tuple_t_Comb(&F_t::operator())) get_tuple_t_Comb(F_t);

std::false_type get_tuple_t_Comb(...); // black hole

// declaration of functions to extract the result type from the signature of the window function
template<typename F_t, typename Arg1, typename Arg2> // non-incremenatal version
Arg2 get_result_t_Win(void (F_t::*)(const Iterable<Arg1>&, Arg2&) const);

template<typename F_t, typename Arg1, typename Arg2> // non-incremenatal version
Arg2 get_result_t_Win(void (F_t::*)(const Iterable<Arg1>&, Arg2&));

template<typename Arg1, typename Arg2> // non-incremenatal version
Arg2 get_result_t_Win(void (*)(const Iterable<Arg1>&, Arg2&));

template<typename F_t, typename Arg1, typename Arg2> // non-incremenatal riched version
Arg2 get_result_t_Win(void (F_t::*)(const Iterable<Arg1>&, Arg2&, RuntimeContext&) const);

template<typename F_t, typename Arg1, typename Arg2> // non-incremenatal riched version
Arg2 get_result_t_Win(void (F_t::*)(const Iterable<Arg1>&, Arg2&, RuntimeContext&));

template<typename Arg1, typename Arg2> // non-incremenatal riched version
Arg2 get_result_t_Win(void (*)(const Iterable<Arg1>&, Arg2&, RuntimeContext&));

template<typename F_t, typename Arg1, typename Arg2> // incremental version
Arg2 get_result_t_Win(void (F_t::*)(const Arg1&, Arg2&) const);

template<typename F_t, typename Arg1, typename Arg2> // incremental version
Arg2 get_result_t_Win(void (F_t::*)(const Arg1&, Arg2&));

template<typename Arg1, typename Arg2> // incremental version
Arg2 get_result_t_Win(void (*)(const Arg1&, Arg2&));

template<typename F_t, typename Arg1, typename Arg2> // incremental riched version
Arg2 get_result_t_Win(void (F_t::*)(const Arg1&, Arg2&, RuntimeContext&) const);

template<typename F_t, typename Arg1, typename Arg2> // incremental riched version
Arg2 get_result_t_Win(void (F_t::*)(const Arg1&, Arg2&, RuntimeContext&));

template<typename Arg1, typename Arg2> // incremental riched version
Arg2 get_result_t_Win(void (*)(const Arg1&, Arg2&, RuntimeContext&));

template<typename F_t>
decltype(get_result_t_Win(&F_t::operator())) get_result_t_Win(F_t);

std::false_type get_result_t_Win(...); // black hole

// declaration of functions to extract the result type from the signature of the lift function
template<typename F_t, typename Arg1, typename Arg2> // lift function
Arg2 get_result_t_Lift(void (F_t::*)(const Arg1&, Arg2&) const);

template<typename F_t, typename Arg1, typename Arg2> // lift function
Arg2 get_result_t_Lift(void (F_t::*)(const Arg1&, Arg2&));

template<typename Arg1, typename Arg2> // lift function
Arg2 get_result_t_Lift(void (*)(const Arg1&, Arg2&));

template<typename F_t, typename Arg1, typename Arg2> // lift rich function
Arg2 get_result_t_Lift(void (F_t::*)(const Arg1&, Arg2&, RuntimeContext&) const);

template<typename F_t, typename Arg1, typename Arg2> // lift rich function
Arg2 get_result_t_Lift(void (F_t::*)(const Arg1&, Arg2&, RuntimeContext&));

template<typename Arg1, typename Arg2> // lift rich function
Arg2 get_result_t_Lift(void (*)(const Arg1&, Arg2&, RuntimeContext&));

template<typename F_t>
decltype(get_result_t_Lift(&F_t::operator())) get_result_t_Lift(F_t);

std::false_type get_result_t_Lift(...); // black holes

// declaration of functions to extract the result type from the signature of the combine function
template<typename F_t, typename Arg> // combine function
Arg get_result_t_Comb(void (F_t::*)(const Arg&, const Arg&, Arg&) const);

template<typename F_t, typename Arg> // combine function
Arg get_result_t_Comb(void (F_t::*)(const Arg&, const Arg&, Arg&));

template<typename Arg> // combine function
Arg get_result_t_Comb(void (*)(const Arg&, const Arg&, Arg&));

template<typename F_t, typename Arg> // combine riched function
Arg get_result_t_Comb(void (F_t::*)(const Arg&, const Arg&, Arg&, RuntimeContext&) const);

template<typename F_t, typename Arg> // combine riched function
Arg get_result_t_Comb(void (F_t::*)(const Arg&, const Arg&, Arg&, RuntimeContext&));

template<typename Arg> // combine riched function
Arg get_result_t_Comb(void (*)(const Arg&, const Arg&, Arg&, RuntimeContext&));

template<typename F_t>
decltype(get_result_t_Comb(&F_t::operator())) get_result_t_Comb(F_t);

std::false_type get_result_t_Comb(...); // black hole
/*****************************************************************************************************************************/

/******************************************************** CLOSING ************************************************************/
// declaration of functions to check the signature of the closing logic
template<typename F_t>
std::true_type check_closing_t(void (F_t::*)(RuntimeContext&) const);

template<typename F_t>
std::true_type check_closing_t(void (F_t::*)(RuntimeContext&));

std::true_type check_closing_t(void (*)(RuntimeContext&));

template<typename F_t>
decltype(check_closing_t(&F_t::operator())) check_closing_t(F_t);

std::false_type check_closing_t(...); // black hole
/*****************************************************************************************************************************/

/******************************************************** SPLITTING **********************************************************/
// declaration of functions to extract the tuple type from the signature of the splitting logic
template<typename F_t, typename Ret, typename Arg> // const version
typename std::enable_if<std::is_integral<Ret>::value, Arg>::type get_tuple_t_Split(Ret (F_t::*)(const Arg&) const);

template<typename F_t, typename Ret, typename Arg> // const version
typename std::enable_if<std::is_integral<Ret>::value, Arg>::type get_tuple_t_Split(Ret (F_t::*)(const Arg&));

template<typename Ret, typename Arg> // const version
typename std::enable_if<std::is_integral<Ret>::value, Arg>::type get_tuple_t_Split(Ret (*)(const Arg&));

template<typename F_t, typename Ret, typename Arg> // non-const version
typename std::enable_if<std::is_integral<Ret>::value, Arg>::type get_tuple_t_Split(Ret (F_t::*)(Arg&) const);

template<typename F_t, typename Ret, typename Arg> // non-const version
typename std::enable_if<std::is_integral<Ret>::value, Arg>::type get_tuple_t_Split(Ret (F_t::*)(Arg&));

template<typename Ret, typename Arg> // non-const version
typename std::enable_if<std::is_integral<Ret>::value, Arg>::type get_tuple_t_Split(Ret (*)(Arg&));

template<typename F_t, typename Ret, typename Arg> // const version
typename std::enable_if<std::is_integral<Ret>::value, Arg>::type get_tuple_t_Split(std::vector<Ret> (F_t::*)(const Arg&) const);

template<typename F_t, typename Ret, typename Arg> // const version
typename std::enable_if<std::is_integral<Ret>::value, Arg>::type get_tuple_t_Split(std::vector<Ret> (F_t::*)(const Arg&));

template<typename Ret, typename Arg> // const version
typename std::enable_if<std::is_integral<Ret>::value, Arg>::type get_tuple_t_Split(std::vector<Ret> (*)(const Arg&));

template<typename F_t, typename Ret, typename Arg> // non-const version
typename std::enable_if<std::is_integral<Ret>::value, Arg>::type get_tuple_t_Split(std::vector<Ret> (F_t::*)(Arg&) const);

template<typename F_t, typename Ret, typename Arg> // non-const version
typename std::enable_if<std::is_integral<Ret>::value, Arg>::type get_tuple_t_Split(std::vector<Ret> (F_t::*)(Arg&));

template<typename Ret, typename Arg> // non-const version
typename std::enable_if<std::is_integral<Ret>::value, Arg>::type get_tuple_t_Split(std::vector<Ret> (*)(Arg&));

template<typename F_t>
decltype(get_tuple_t_Split(&F_t::operator())) get_tuple_t_Split(F_t);

std::false_type get_tuple_t_Split(...); // black hole

// declaration of functions to extract the return type from the signature of the splitting logic
template<typename F_t, typename Ret, typename Arg> // const version
typename std::enable_if<std::is_integral<Ret>::value, Ret>::type get_return_t_Split(Ret (F_t::*)(const Arg&) const);

template<typename F_t, typename Ret, typename Arg> // const version
typename std::enable_if<std::is_integral<Ret>::value, Ret>::type get_return_t_Split(Ret (F_t::*)(const Arg&));

template<typename Ret, typename Arg> // const version
typename std::enable_if<std::is_integral<Ret>::value, Ret>::type get_return_t_Split(Ret (*)(const Arg&));

template<typename F_t, typename Ret, typename Arg> // non-const version
typename std::enable_if<std::is_integral<Ret>::value, Ret>::type get_return_t_Split(Ret (F_t::*)(Arg&) const);

template<typename F_t, typename Ret, typename Arg> // non-const version
typename std::enable_if<std::is_integral<Ret>::value, Ret>::type get_return_t_Split(Ret (F_t::*)(Arg&));

template<typename Ret, typename Arg> // non-const version
typename std::enable_if<std::is_integral<Ret>::value, Ret>::type get_return_t_Split(Ret (*)(Arg&));

template<typename F_t, typename Ret, typename Arg> // const version
typename std::enable_if<std::is_integral<Ret>::value, std::vector<Ret>>::type get_return_t_Split(std::vector<Ret> (F_t::*)(const Arg&) const);

template<typename F_t, typename Ret, typename Arg> // const version
typename std::enable_if<std::is_integral<Ret>::value, std::vector<Ret>>::type get_return_t_Split(std::vector<Ret> (F_t::*)(const Arg&));

template<typename Ret, typename Arg> // const version
typename std::enable_if<std::is_integral<Ret>::value, std::vector<Ret>>::type get_return_t_Split(std::vector<Ret> (*)(const Arg&));

template<typename F_t, typename Ret, typename Arg> // non-const version
typename std::enable_if<std::is_integral<Ret>::value, std::vector<Ret>>::type get_return_t_Split(std::vector<Ret> (F_t::*)(Arg&) const);

template<typename F_t, typename Ret, typename Arg> // non-const version
typename std::enable_if<std::is_integral<Ret>::value, std::vector<Ret>>::type get_return_t_Split(std::vector<Ret> (F_t::*)(Arg&));

template<typename Ret, typename Arg> // non-const version
typename std::enable_if<std::is_integral<Ret>::value, std::vector<Ret>>::type get_return_t_Split(std::vector<Ret> (*)(Arg&));

template<typename F_t>
decltype(get_return_t_Split(&F_t::operator())) get_return_t_Split(F_t);

std::false_type get_return_t_Split(...); // black hole
/*****************************************************************************************************************************/

} // namespace wf

#endif
