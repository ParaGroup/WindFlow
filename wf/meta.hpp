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
 *  @date    16/08/2018
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
#if __cplusplus < 201703L // not C++17
    #include<experimental/optional>
    namespace std { using namespace experimental; } // ugly but necessary until CUDA will support C++17!
#else
    #include<optional>
#endif
#include<basic.hpp>
#include<context.hpp>
#include<shipper.hpp>
#include<iterable.hpp>

namespace wf {

/****************************************************** SOURCE OPERATOR ******************************************************/
// declaration of functions to extract the output type form the Source operator
template<typename F_t, typename Arg>
Arg get_result_t_Source(bool (F_t::*)(Arg&) const); // item-by-item version

template<typename F_t, typename Arg>
Arg get_result_t_Source(bool (F_t::*)(Arg&)); // item-by-item version

template<typename Arg>
Arg get_result_t_Source(bool (*)(Arg&)); // item-by-item version

template<typename F_t, typename Arg>
Arg get_result_t_Source(bool (F_t::*)(Arg&, RuntimeContext&) const); // item-by-item riched version

template<typename F_t, typename Arg>
Arg get_result_t_Source(bool (F_t::*)(Arg&, RuntimeContext&)); // item-by-item riched version

template<typename Arg>
Arg get_result_t_Source(bool (*)(Arg&, RuntimeContext&)); // item-by-item riched version

template<typename F_t, typename Arg> // single-loop version
Arg get_result_t_Source(bool (F_t::*)(Shipper<Arg>&) const);

template<typename F_t, typename Arg> // single-loop version
Arg get_result_t_Source(bool (F_t::*)(Shipper<Arg>&));

template<typename Arg> // single-loop version
Arg get_result_t_Source(bool (*)(Shipper<Arg>&));

template<typename F_t, typename Arg> // single-loop riched version
Arg get_result_t_Source(bool (F_t::*)(Shipper<Arg>&, RuntimeContext&) const);

template<typename F_t, typename Arg> // single-loop riched version
Arg get_result_t_Source(bool (F_t::*)(Shipper<Arg>&, RuntimeContext&));

template<typename Arg> // single-loop riched version
Arg get_result_t_Source(bool (*)(Shipper<Arg>&, RuntimeContext&));

template<typename F_t>
decltype(get_result_t_Source(&F_t::operator())) get_result_t_Source(F_t);

std::false_type get_result_t_Source(...); // black hole
/*****************************************************************************************************************************/

/****************************************************** FILTER OPERATOR ******************************************************/
// declaration of functions to extract the input type to the Filter operator
template<typename F_t, typename Arg>
Arg get_tuple_t_Filter(bool (F_t::*)(Arg&) const); // in-place version

template<typename F_t, typename Arg>
Arg get_tuple_t_Filter(bool (F_t::*)(Arg&)); // in-place version

template<typename Arg>
Arg get_tuple_t_Filter(bool (*)(Arg&)); // in-place version

template<typename F_t, typename Arg>
Arg get_tuple_t_Filter(bool (F_t::*)(Arg&, RuntimeContext&) const); // in-place riched version

template<typename F_t, typename Arg>
Arg get_tuple_t_Filter(bool (F_t::*)(Arg&, RuntimeContext&)); // in-place riched version

template<typename Arg>
Arg get_tuple_t_Filter(bool (*)(Arg&, RuntimeContext&)); // in-place riched version

template<typename F_t, typename Arg1, typename Arg2>
Arg1 get_tuple_t_Filter(std::optional<Arg2> (F_t::*)(const Arg1&) const); // not in-place version

template<typename F_t, typename Arg1, typename Arg2>
Arg1 get_tuple_t_Filter(std::optional<Arg2> (F_t::*)(const Arg1&)); // not in-place version

template<typename Arg1, typename Arg2>
Arg1 get_tuple_t_Filter(std::optional<Arg2> (*)(const Arg1&)); // not in-place version

template<typename F_t, typename Arg1, typename Arg2>
Arg1 get_tuple_t_Filter(std::optional<Arg2> (F_t::*)(const Arg1&, RuntimeContext&) const); // not in-place riched version

template<typename F_t, typename Arg1, typename Arg2>
Arg1 get_tuple_t_Filter(std::optional<Arg2> (F_t::*)(const Arg1&, RuntimeContext&)); // not in-place riched version

template<typename Arg1, typename Arg2>
Arg1 get_tuple_t_Filter(std::optional<Arg2> (*)(const Arg1&, RuntimeContext&)); // not in-place riched version

template<typename F_t, typename Arg1, typename Arg2>
Arg1 get_tuple_t_Filter(std::optional<Arg2*> (F_t::*)(const Arg1&) const); // not in-place version (with pointer)

template<typename F_t, typename Arg1, typename Arg2>
Arg1 get_tuple_t_Filter(std::optional<Arg2*> (F_t::*)(const Arg1&)); // not in-place version (with pointer)

template<typename Arg1, typename Arg2>
Arg1 get_tuple_t_Filter(std::optional<Arg2*> (*)(const Arg1&)); // not in-place version (with pointer)

template<typename F_t, typename Arg1, typename Arg2>
Arg1 get_tuple_t_Filter(std::optional<Arg2*> (F_t::*)(const Arg1&, RuntimeContext&) const); // not in-place riched version (with pointer)

template<typename F_t, typename Arg1, typename Arg2>
Arg1 get_tuple_t_Filter(std::optional<Arg2*> (F_t::*)(const Arg1&, RuntimeContext&)); // not in-place riched version (with pointer)

template<typename Arg1, typename Arg2>
Arg1 get_tuple_t_Filter(std::optional<Arg2*> (*)(const Arg1&, RuntimeContext&)); // not in-place riched version (with pointer)

template<typename F_t>
decltype(get_tuple_t_Filter(&F_t::operator())) get_tuple_t_Filter(F_t);

std::false_type get_tuple_t_Filter(...); // black hole

// declaration of functions to extract the output type from the Filter operator
template<typename F_t, typename Arg>
Arg get_result_t_Filter(bool (F_t::*)(Arg&) const); // in-place version

template<typename F_t, typename Arg>
Arg get_result_t_Filter(bool (F_t::*)(Arg&)); // in-place version

template<typename Arg>
Arg get_result_t_Filter(bool (*)(Arg&)); // in-place version

template<typename F_t, typename Arg>
Arg get_result_t_Filter(bool (F_t::*)(Arg&, RuntimeContext&) const); // in-place riched version

template<typename F_t, typename Arg>
Arg get_result_t_Filter(bool (F_t::*)(Arg&, RuntimeContext&)); // in-place riched version

template<typename Arg>
Arg get_result_t_Filter(bool (*)(Arg&, RuntimeContext&)); // in-place riched version

template<typename F_t, typename Arg1, typename Arg2>
Arg2 get_result_t_Filter(std::optional<Arg2> (F_t::*)(const Arg1&) const); // not in-place version

template<typename F_t, typename Arg1, typename Arg2>
Arg2 get_result_t_Filter(std::optional<Arg2> (F_t::*)(const Arg1&)); // not in-place version

template<typename Arg1, typename Arg2>
Arg2 get_result_t_Filter(std::optional<Arg2> (*)(const Arg1&)); // not in-place version

template<typename F_t, typename Arg1, typename Arg2>
Arg2 get_result_t_Filter(std::optional<Arg2> (F_t::*)(const Arg1&, RuntimeContext&) const); // not in-place riched version

template<typename F_t, typename Arg1, typename Arg2>
Arg2 get_result_t_Filter(std::optional<Arg2> (F_t::*)(const Arg1&, RuntimeContext&)); // not in-place riched version

template<typename Arg1, typename Arg2>
Arg2 get_result_t_Filter(std::optional<Arg2> (*)(const Arg1&, RuntimeContext&)); // not in-place riched version

template<typename F_t, typename Arg1, typename Arg2>
Arg2 get_result_t_Filter(std::optional<Arg2*> (F_t::*)(const Arg1&) const); // not in-place version (with pointer)

template<typename F_t, typename Arg1, typename Arg2>
Arg2 get_result_t_Filter(std::optional<Arg2*> (F_t::*)(const Arg1&)); // not in-place version (with pointer)

template<typename Arg1, typename Arg2>
Arg2 get_result_t_Filter(std::optional<Arg2*> (*)(const Arg1&)); // not in-place version (with pointer)

template<typename F_t, typename Arg1, typename Arg2>
Arg2 get_result_t_Filter(std::optional<Arg2*> (F_t::*)(const Arg1&, RuntimeContext&) const); // not in-place riched version (with pointer)

template<typename F_t, typename Arg1, typename Arg2>
Arg2 get_result_t_Filter(std::optional<Arg2*> (F_t::*)(const Arg1&, RuntimeContext&)); // not in-place riched version (with pointer)

template<typename Arg1, typename Arg2>
Arg2 get_result_t_Filter(std::optional<Arg2*> (*)(const Arg1&, RuntimeContext&)); // not in-place riched version (with pointer)

template<typename F_t>
decltype(get_result_t_Filter(&F_t::operator())) get_result_t_Filter(F_t);

std::false_type get_result_t_Filter(...); // black hole
/*****************************************************************************************************************************/

/****************************************************** MAP OPERATOR *********************************************************/
// declaration of functions to extract the input type to the Map operator
template<typename F_t, typename Arg1, typename Arg2> // not in-place version
Arg1 get_tuple_t_Map(void (F_t::*)(const Arg1&, Arg2&) const);

template<typename F_t, typename Arg1, typename Arg2> // not in-place version
Arg1 get_tuple_t_Map(void (F_t::*)(const Arg1&, Arg2&));

template<typename Arg1, typename Arg2> // not in-place version
Arg1 get_tuple_t_Map(void (*)(const Arg1&, Arg2&));

template<typename F_t, typename Arg1, typename Arg2> // not in-place riched version
Arg1 get_tuple_t_Map(void (F_t::*)(const Arg1&, Arg2&, RuntimeContext&) const);

template<typename F_t, typename Arg1, typename Arg2> // not in-place riched version
Arg1 get_tuple_t_Map(void (F_t::*)(const Arg1&, Arg2&, RuntimeContext&));

template<typename Arg1, typename Arg2> // not in-place riched version
Arg1 get_tuple_t_Map(void (*)(const Arg1&, Arg2&, RuntimeContext&));

template<typename F_t, typename Arg> // in-place version
Arg get_tuple_t_Map(void (F_t::*)(Arg&) const);

template<typename F_t, typename Arg> // in-place version
Arg get_tuple_t_Map(void (F_t::*)(Arg&));

template<typename Arg> // in-place version
Arg get_tuple_t_Map(void (*)(Arg&));

template<typename F_t, typename Arg> // in-place riched version
Arg get_tuple_t_Map(void (F_t::*)(Arg&, RuntimeContext&) const);

template<typename F_t, typename Arg> // in-place riched version
Arg get_tuple_t_Map(void (F_t::*)(Arg&, RuntimeContext&));

template<typename Arg> // in-place riched version
Arg get_tuple_t_Map(void (*)(Arg&, RuntimeContext&));

template<typename F_t>
decltype(get_tuple_t_Map(&F_t::operator())) get_tuple_t_Map(F_t);

std::false_type get_tuple_t_Map(...); // black hole

// declaration of functions to extract the result type from the Map operator
template<typename F_t, typename Arg1, typename Arg2> // not in-place version
Arg2 get_result_t_Map(void (F_t::*)(const Arg1&, Arg2&) const);

template<typename F_t, typename Arg1, typename Arg2> // not in-place version
Arg2 get_result_t_Map(void (F_t::*)(const Arg1&, Arg2&));

template<typename Arg1, typename Arg2> // not in-place version
Arg2 get_result_t_Map(void (*)(const Arg1&, Arg2&));

template<typename F_t, typename Arg1, typename Arg2> // not in-place riched version
Arg2 get_result_t_Map(void (F_t::*)(const Arg1&, Arg2&, RuntimeContext&) const);

template<typename F_t, typename Arg1, typename Arg2> // not in-place riched version
Arg2 get_result_t_Map(void (F_t::*)(const Arg1&, Arg2&, RuntimeContext&));

template<typename Arg1, typename Arg2> // not in-place riched version
Arg2 get_result_t_Map(void (*)(const Arg1&, Arg2&, RuntimeContext&));

template<typename F_t, typename Arg> // in-place version
Arg get_result_t_Map(void (F_t::*)(Arg&) const);

template<typename F_t, typename Arg> // in-place version
Arg get_result_t_Map(void (F_t::*)(Arg&));

template<typename Arg> // in-place version
Arg get_result_t_Map(void (*)(Arg&));

template<typename F_t, typename Arg> // in-place riched version
Arg get_result_t_Map(void (F_t::*)(Arg&, RuntimeContext&) const);

template<typename F_t, typename Arg> // in-place riched version
Arg get_result_t_Map(void (F_t::*)(Arg&, RuntimeContext&));

template<typename Arg> // in-place riched version
Arg get_result_t_Map(void (*)(Arg&, RuntimeContext&));

template<typename F_t>
decltype(get_result_t_Map(&F_t::operator())) get_result_t_Map(F_t);

std::false_type get_result_t_Map(...); // black hole
/*****************************************************************************************************************************/

/**************************************************** FLATMAP OPERATOR *******************************************************/
// declaration of functions to extract the input type to the FlatMap operator
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

// declaration of functions to extract the result type from the FlatMap operator
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

/************************************************** ACCUMULATOR OPERATOR *****************************************************/
// declaration of functions to extract the input type to the Accumulator operator
template<typename F_t, typename Arg1, typename Arg2>
Arg1 get_tuple_t_Acc(void (F_t::*)(const Arg1&, Arg2&) const);

template<typename F_t, typename Arg1, typename Arg2>
Arg1 get_tuple_t_Acc(void (F_t::*)(const Arg1&, Arg2&));

template<typename Arg1, typename Arg2>
Arg1 get_tuple_t_Acc(void (*)(const Arg1&, Arg2&));

template<typename F_t, typename Arg1, typename Arg2> // riched version
Arg1 get_tuple_t_Acc(void (F_t::*)(const Arg1&, Arg2&, RuntimeContext&) const);

template<typename F_t, typename Arg1, typename Arg2> // riched version
Arg1 get_tuple_t_Acc(void (F_t::*)(const Arg1&, Arg2&, RuntimeContext&));

template<typename Arg1, typename Arg2> // riched version
Arg1 get_tuple_t_Acc(void (*)(const Arg1&, Arg2&, RuntimeContext&));

template<typename F_t>
decltype(get_tuple_t_Acc(&F_t::operator())) get_tuple_t_Acc(F_t);

std::false_type get_tuple_t_Acc(...); // black hole

// declaration of functions to extract the result type from the Accumulator operator
template<typename F_t, typename Arg1, typename Arg2>
Arg2 get_result_t_Acc(void (F_t::*)(const Arg1&, Arg2&) const);

template<typename F_t, typename Arg1, typename Arg2>
Arg2 get_result_t_Acc(void (F_t::*)(const Arg1&, Arg2&));

template<typename Arg1, typename Arg2>
Arg2 get_result_t_Acc(void (*)(const Arg1&, Arg2&));

template<typename F_t, typename Arg1, typename Arg2> // riched version
Arg2 get_result_t_Acc(void (F_t::*)(const Arg1&, Arg2&, RuntimeContext&) const);

template<typename F_t, typename Arg1, typename Arg2> // riched version
Arg2 get_result_t_Acc(void (F_t::*)(const Arg1&, Arg2&, RuntimeContext&));

template<typename Arg1, typename Arg2> // riched version
Arg2 get_result_t_Acc(void (*)(const Arg1&, Arg2&, RuntimeContext&));

template<typename F_t>
decltype(get_result_t_Acc(&F_t::operator())) get_result_t_Acc(F_t);

std::false_type get_result_t_Acc(...); // black hole
/*****************************************************************************************************************************/

/****************************************************** SINK OPERATOR ********************************************************/
// declaration of functions to extract the input type to the Sink operator
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

// declaration of functions to extract the result type from the signature of the splitting logic
template<typename F_t, typename Ret, typename Arg> // const version
typename std::enable_if<std::is_integral<Ret>::value, Ret>::type get_result_t_Split(Ret (F_t::*)(const Arg&) const);

template<typename F_t, typename Ret, typename Arg> // const version
typename std::enable_if<std::is_integral<Ret>::value, Ret>::type get_result_t_Split(Ret (F_t::*)(const Arg&));

template<typename Ret, typename Arg> // const version
typename std::enable_if<std::is_integral<Ret>::value, Ret>::type get_result_t_Split(Ret (*)(const Arg&));

template<typename F_t, typename Ret, typename Arg> // non-const version
typename std::enable_if<std::is_integral<Ret>::value, Ret>::type get_result_t_Split(Ret (F_t::*)(Arg&) const);

template<typename F_t, typename Ret, typename Arg> // non-const version
typename std::enable_if<std::is_integral<Ret>::value, Ret>::type get_result_t_Split(Ret (F_t::*)(Arg&));

template<typename Ret, typename Arg> // non-const version
typename std::enable_if<std::is_integral<Ret>::value, Ret>::type get_result_t_Split(Ret (*)(Arg&));

template<typename F_t, typename Ret, typename Arg> // const version
typename std::enable_if<std::is_integral<Ret>::value, std::vector<Ret>>::type get_result_t_Split(std::vector<Ret> (F_t::*)(const Arg&) const);

template<typename F_t, typename Ret, typename Arg> // const version
typename std::enable_if<std::is_integral<Ret>::value, std::vector<Ret>>::type get_result_t_Split(std::vector<Ret> (F_t::*)(const Arg&));

template<typename Ret, typename Arg> // const version
typename std::enable_if<std::is_integral<Ret>::value, std::vector<Ret>>::type get_result_t_Split(std::vector<Ret> (*)(const Arg&));

template<typename F_t, typename Ret, typename Arg> // non-const version
typename std::enable_if<std::is_integral<Ret>::value, std::vector<Ret>>::type get_result_t_Split(std::vector<Ret> (F_t::*)(Arg&) const);

template<typename F_t, typename Ret, typename Arg> // non-const version
typename std::enable_if<std::is_integral<Ret>::value, std::vector<Ret>>::type get_result_t_Split(std::vector<Ret> (F_t::*)(Arg&));

template<typename Ret, typename Arg> // non-const version
typename std::enable_if<std::is_integral<Ret>::value, std::vector<Ret>>::type get_result_t_Split(std::vector<Ret> (*)(Arg&));

template<typename F_t>
decltype(get_result_t_Split(&F_t::operator())) get_result_t_Split(F_t);

std::false_type get_result_t_Split(...); // black hole
/*****************************************************************************************************************************/

/**************************************************** WINDOWED OPERATORS *****************************************************/
// declaration of functions to extract the tuple type from the signature of the window function
template<typename F_t, typename Arg1, typename Arg2> // non-incremenatal version
Arg1 get_tuple_t_Win(void (F_t::*)(uint64_t, const Iterable<Arg1>&, Arg2&) const);

template<typename F_t, typename Arg1, typename Arg2> // non-incremenatal version
Arg1 get_tuple_t_Win(void (F_t::*)(uint64_t, const Iterable<Arg1>&, Arg2&));

template<typename Arg1, typename Arg2> // non-incremenatal version
Arg1 get_tuple_t_Win(void (*)(uint64_t, const Iterable<Arg1>&, Arg2&));

template<typename F_t, typename Arg1, typename Arg2> // non-incremenatal riched version
Arg1 get_tuple_t_Win(void (F_t::*)(uint64_t, const Iterable<Arg1>&, Arg2&, RuntimeContext&) const);

template<typename F_t, typename Arg1, typename Arg2> // non-incremenatal riched version
Arg1 get_tuple_t_Win(void (F_t::*)(uint64_t, const Iterable<Arg1>&, Arg2&, RuntimeContext&));

template<typename Arg1, typename Arg2> // non-incremenatal riched version
Arg1 get_tuple_t_Win(void (*)(uint64_t, const Iterable<Arg1>&, Arg2&, RuntimeContext&));

template<typename F_t, typename Arg1, typename Arg2> // incremental version
Arg1 get_tuple_t_Win(void (F_t::*)(uint64_t, const Arg1&, Arg2&) const);

template<typename F_t, typename Arg1, typename Arg2> // incremental version
Arg1 get_tuple_t_Win(void (F_t::*)(uint64_t, const Arg1&, Arg2&));

template<typename Arg1, typename Arg2> // incremental version
Arg1 get_tuple_t_Win(void (*)(uint64_t, const Arg1&, Arg2&));

template<typename F_t, typename Arg1, typename Arg2> // incremental riched version
Arg1 get_tuple_t_Win(void (F_t::*)(uint64_t, const Arg1&, Arg2&, RuntimeContext&) const);

template<typename F_t, typename Arg1, typename Arg2> // incremental riched version
Arg1 get_tuple_t_Win(void (F_t::*)(uint64_t, const Arg1&, Arg2&, RuntimeContext&));

template<typename Arg1, typename Arg2> // incremental riched version
Arg1 get_tuple_t_Win(void (*)(uint64_t, const Arg1&, Arg2&, RuntimeContext&));

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
Arg2 get_result_t_Win(void (F_t::*)(uint64_t, const Iterable<Arg1>&, Arg2&) const);

template<typename F_t, typename Arg1, typename Arg2> // non-incremenatal version
Arg2 get_result_t_Win(void (F_t::*)(uint64_t, const Iterable<Arg1>&, Arg2&));

template<typename Arg1, typename Arg2> // non-incremenatal version
Arg2 get_result_t_Win(void (*)(uint64_t, const Iterable<Arg1>&, Arg2&));

template<typename F_t, typename Arg1, typename Arg2> // non-incremenatal riched version
Arg2 get_result_t_Win(void (F_t::*)(uint64_t, const Iterable<Arg1>&, Arg2&, RuntimeContext&) const);

template<typename F_t, typename Arg1, typename Arg2> // non-incremenatal riched version
Arg2 get_result_t_Win(void (F_t::*)(uint64_t, const Iterable<Arg1>&, Arg2&, RuntimeContext&));

template<typename Arg1, typename Arg2> // non-incremenatal riched version
Arg2 get_result_t_Win(void (*)(uint64_t, const Iterable<Arg1>&, Arg2&, RuntimeContext&));

template<typename F_t, typename Arg1, typename Arg2> // incremental version
Arg2 get_result_t_Win(void (F_t::*)(uint64_t, const Arg1&, Arg2&) const);

template<typename F_t, typename Arg1, typename Arg2> // incremental version
Arg2 get_result_t_Win(void (F_t::*)(uint64_t, const Arg1&, Arg2&));

template<typename Arg1, typename Arg2> // incremental version
Arg2 get_result_t_Win(void (*)(uint64_t, const Arg1&, Arg2&));

template<typename F_t, typename Arg1, typename Arg2> // incremental riched version
Arg2 get_result_t_Win(void (F_t::*)(uint64_t, const Arg1&, Arg2&, RuntimeContext&) const);

template<typename F_t, typename Arg1, typename Arg2> // incremental riched version
Arg2 get_result_t_Win(void (F_t::*)(uint64_t, const Arg1&, Arg2&, RuntimeContext&));

template<typename Arg1, typename Arg2> // incremental riched version
Arg2 get_result_t_Win(void (*)(uint64_t, const Arg1&, Arg2&, RuntimeContext&));

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

std::false_type get_result_t_Lift(...); // black hole

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

/***************************************** SPECIAL FUNCTIONS FOR NESTING OF OPERATORS ****************************************/
// declaration of functions to extract the type of Win_Farm from the Pane_Farm operator provided to the builder
template<typename ...Args>
Win_Farm<Args...> *get_WF_nested_type(Pane_Farm<Args...> const&);

// declaration of functions to extract the type of Win_Farm from the Win_MapReduce operator provided to the builder
template<typename ...Args>
Win_Farm<Args...> *get_WF_nested_type(Win_MapReduce<Args...> const&);

// declaration of functions to extract the type of Win_Farm from the function/functor/lambda provided to the builder
template<typename F_t>
auto *get_WF_nested_type(F_t _f)
{
    Win_Farm<decltype(get_tuple_t_Win(_f)),
             decltype(get_result_t_Win(_f))>*ptr = nullptr;
    return ptr;
}

std::false_type *get_WF_nested_type(...); // black hole

// declaration of functions to extract the type of Key_Farm from the Pane_Farm operator provided to the builder
template<typename arg1, typename arg2, typename arg3>
Key_Farm<arg1, arg2> *get_KF_nested_type(Pane_Farm<arg1, arg2, arg3> const&);

// declaration of functions to extract the type of Key_Farm from the Win_MapReduce operator provided to the builder
template<typename arg1, typename arg2, typename arg3>
Key_Farm<arg1, arg2> *get_KF_nested_type(Win_MapReduce<arg1, arg2, arg3> const&);

// declaration of functions to extract the type of Key_Farm from the function/functor/lambda provided to the builder
template<typename F_t>
auto *get_KF_nested_type(F_t _f)
{
    Key_Farm<decltype(get_tuple_t_Win(_f)),
             decltype(get_result_t_Win(_f))>*ptr = nullptr;
    return ptr;
}

std::false_type *get_KF_nested_type(...); // black hole
/*****************************************************************************************************************************/

/************************************************** WRAPPER OF INPUT TUPLES **************************************************/
// wrapper struct of input tuples
template<typename tuple_t>
struct wrapper_tuple_t
{
    tuple_t *tuple; // pointer to a tuple
    std::atomic<size_t> counter; // atomic reference counter
    bool eos; // if true, the tuple is a EOS marker

    // Constructor
    wrapper_tuple_t(tuple_t *_t,
                    size_t _counter=1,
                    bool _eos=false):
                    tuple(_t),
                    counter(_counter),
                    eos(_eos)
    {}
};
/*****************************************************************************************************************************/

/***************************************************** UTILITY FUNCTIONS *****************************************************/
// function extractTuple: definition valid if T1 != T2
template <typename T1, typename T2>
T1 *extractTuple(typename std::enable_if<!std::is_same<T1,T2>::value, T2>::type *wt)
{
    return wt->tuple;
}

// function extractTuple: definition valid if T1 == T2
template <typename T1, typename T2>
T1 *extractTuple(typename std::enable_if<std::is_same<T1,T2>::value, T2>::type *t)
{
    return t;
}

// function deleteTuple: definition valid if T1 != T2
template <typename T1, typename T2>
void deleteTuple(typename std::enable_if<!std::is_same<T1,T2>::value, T2>::type *wt)
{
    T1 *t = wt->tuple;
    // check if the tuple and the wrapper must be destroyed/deallocated
    size_t old_cnt = (wt->counter).fetch_sub(1);
    if (old_cnt == 1) {
        delete t;
        delete wt;
    }
}

// function deleteTuple: definition valid if T1 == T2
template <typename T1, typename T2>
void deleteTuple(typename std::enable_if<std::is_same<T1,T2>::value, T2>::type *t)
{
    delete t;
}

// function createWrapper: definition valid if T2 != T3
template<typename T1, typename T2, typename T3>
T1 *createWrapper(typename std::enable_if<!std::is_same<T2,T3>::value, T1>::type *t,
				  size_t val,
				  bool isEOS=false)
{
    // only return the tuple
    return t;
}

// function createWrapper: definition valid if T2 == T3
template<typename T1, typename T2, typename T3>
T2 *createWrapper(typename std::enable_if<std::is_same<T2,T3>::value, T1>::type *t,
				  size_t val,
				  bool isEOS=false)
{
    // create and return a wrapper to the tuple
    T2 *wt = new T2(t, val, isEOS);
    return wt;
}

// function prepareWrapper: definition valid if T1 != T2
template<typename T1, typename T2>
T2 *prepareWrapper(typename std::enable_if<!std::is_same<T1,T2>::value, T1>::type *t,
				   size_t val)
{
    // create wrapper
    return new T2(t, val);
}

// function prepareWrapper: definition valid if T1 == T2
template<typename T1, typename T2>
T2 *prepareWrapper(typename std::enable_if<std::is_same<T1,T2>::value, T1>::type *wt,
			       size_t val)
{
    (wt->counter).fetch_add(val-1);
    return wt;
}

// function isEOSMarker: definition valid if T1 != T2
template<typename T1, typename T2>
bool isEOSMarker(const typename std::enable_if<!std::is_same<T1,T2>::value, T2>::type &wt)
{
    return wt.eos;
}

// function isEOSMarker: definition valid if T1 == T2
template<typename T1, typename T2>
bool isEOSMarker(const typename std::enable_if<std::is_same<T1,T2>::value, T1>::type &t)
{
    return false;
}
/*****************************************************************************************************************************/

} // namespace wf

#endif
