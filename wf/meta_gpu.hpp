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
 *  @file    meta_gpu.hpp
 *  @author  Gabriele Mencagli
 *  
 *  @brief Metafunctions used by the WindFlow library for GPU operators
 *  
 *  @section Metafunctions-2 (Description)
 *  
 *  Set of metafunctions used by the WindFlow library for GPU operators.
 */ 

#ifndef META_GPU_H
#define META_GPU_H

namespace wf {

/******************************************************* KEY EXTRACTOR *******************************************************/
// declaration of functions to extract the type of the key attribute form the key extractor of GPU operators
template<typename F_t, typename Arg1, typename Arg2>
Arg2 get_key_t_KeyExtrGPU(Arg2 (F_t::*)(const Arg1&) const);

template<typename F_t, typename Arg1, typename Arg2>
Arg2 get_key_t_KeyExtrGPU(Arg2 (F_t::*)(const Arg1&));

// template<typename Arg1, typename Arg2>
// Arg2 get_key_t_KeyExtrGPU(Arg2 (*)(const Arg1&));

template<typename F_t>
decltype(get_key_t_KeyExtrGPU(&F_t::operator())) get_key_t_KeyExtrGPU(F_t);

std::false_type get_key_t_KeyExtrGPU(...); // black hole

// declaration of functions to extract the type of the tuple attribute form the key extractor
template<typename F_t, typename Arg1, typename Arg2>
Arg1 get_tuple_t_KeyExtrGPU(Arg2 (F_t::*)(const Arg1&) const);

template<typename F_t, typename Arg1, typename Arg2>
Arg1 get_tuple_t_KeyExtrGPU(Arg2 (F_t::*)(const Arg1&));

// template<typename Arg1, typename Arg2>
// Arg1 get_tuple_t_KeyExtrGPU(Arg2 (*)(const Arg1&));

template<typename F_t>
decltype(get_tuple_t_KeyExtrGPU(&F_t::operator())) get_tuple_t_KeyExtrGPU(F_t);

std::false_type get_tuple_t_KeyExtrGPU(...); // black hole
/*****************************************************************************************************************************/

/**************************************************** MAP_GPU OPERATOR *******************************************************/
// declaration of functions to extract the input type of the Map_GPU operator
template<typename F_t, typename Arg> // stateless version
Arg get_tuple_t_MapGPU(void (F_t::*)(Arg&) const);

template<typename F_t, typename Arg> // stateless version
Arg get_tuple_t_MapGPU(void (F_t::*)(Arg&));

// template<typename Arg> // stateless version
// Arg get_tuple_t_MapGPU(void (*)(Arg&));

template<typename F_t, typename Arg1, typename Arg2> // stateful version
Arg1 get_tuple_t_MapGPU(void (F_t::*)(Arg1&, Arg2&) const);

template<typename F_t, typename Arg1, typename Arg2> // stateful version
Arg1 get_tuple_t_MapGPU(void (F_t::*)(Arg1&, Arg2&));

// template<typename Arg1, typename Arg2> // stateful version
// Arg1 get_tuple_t_MapGPU(void (*)(Arg1&, Arg2&));

template<typename F_t>
decltype(get_tuple_t_MapGPU(&F_t::operator())) get_tuple_t_MapGPU(F_t);

std::false_type get_tuple_t_MapGPU(...); // black hole

// declaration of functions to extract the state type of the Map_GPU operator
template<typename F_t, typename Arg> // stateless version
std::true_type get_state_t_MapGPU(void (F_t::*)(Arg&) const);

template<typename F_t, typename Arg> // stateless version
std::true_type get_state_t_MapGPU(void (F_t::*)(Arg&));

// template<typename Arg> // stateless version
// std::true_type get_state_t_MapGPU(void (*)(Arg&));

template<typename F_t, typename Arg1, typename Arg2> // stateful version
Arg2 get_state_t_MapGPU(void (F_t::*)(Arg1&, Arg2&) const);

template<typename F_t, typename Arg1, typename Arg2> // stateful version
Arg2 get_state_t_MapGPU(void (F_t::*)(Arg1&, Arg2&));

// template<typename Arg1, typename Arg2> // stateful version
// Arg2 get_state_t_MapGPU(void (*)(Arg1&, Arg2&));

template<typename F_t>
decltype(get_state_t_MapGPU(&F_t::operator())) get_state_t_MapGPU(F_t);

std::false_type get_state_t_MapGPU(...); // black hole
/*****************************************************************************************************************************/

/*************************************************** FILTER_GPU OPERATOR *****************************************************/
// declaration of functions to extract the input type of the Filter_GPU operator
template<typename F_t, typename Arg> // stateless version
Arg get_tuple_t_FilterGPU(bool (F_t::*)(Arg&) const);

template<typename F_t, typename Arg> // stateless version
Arg get_tuple_t_FilterGPU(bool (F_t::*)(Arg&));

// template<typename Arg> // stateless version
// Arg get_tuple_t_FilterGPU(bool (*)(Arg&));

template<typename F_t, typename Arg1, typename Arg2> // stateful version
Arg1 get_tuple_t_FilterGPU(bool (F_t::*)(Arg1&, Arg2&) const);

template<typename F_t, typename Arg1, typename Arg2> // stateful version
Arg1 get_tuple_t_FilterGPU(bool (F_t::*)(Arg1&, Arg2&));

// template<typename Arg1, typename Arg2> // stateful version
// Arg1 get_tuple_t_FilterGPU(bool (*)(Arg1&, Arg2&));

template<typename F_t>
decltype(get_tuple_t_FilterGPU(&F_t::operator())) get_tuple_t_FilterGPU(F_t);

std::false_type get_tuple_t_FilterGPU(...); // black hole

// declaration of functions to extract the state type of the Filter_GPU operator
template<typename F_t, typename Arg> // stateless version
std::true_type get_state_t_FilterGPU(bool (F_t::*)(Arg&) const);

template<typename F_t, typename Arg> // stateless version
std::true_type get_state_t_FilterGPU(bool (F_t::*)(Arg&));

// template<typename Arg> // stateless version
// std::true_type get_state_t_FilterGPU(bool (*)(Arg&));

template<typename F_t, typename Arg1, typename Arg2> // stateful version
Arg2 get_state_t_FilterGPU(bool (F_t::*)(Arg1&, Arg2&) const);

template<typename F_t, typename Arg1, typename Arg2> // stateful version
Arg2 get_state_t_FilterGPU(bool (F_t::*)(Arg1&, Arg2&));

// template<typename Arg1, typename Arg2> // stateful version
// Arg2 get_state_t_FilterGPU(bool (*)(Arg1&, Arg2&));

template<typename F_t>
decltype(get_state_t_FilterGPU(&F_t::operator())) get_state_t_FilterGPU(F_t);

std::false_type get_state_t_FilterGPU(...); // black hole
/*****************************************************************************************************************************/

/************************************************** REDUCE_GPU OPERATOR ******************************************************/
// declaration of functions to extract the input type of the Reduce_GPU operator
template<typename F_t, typename Arg>
Arg get_tuple_t_ReduceGPU(Arg (F_t::*)(const Arg&, const Arg&) const);

template<typename F_t, typename Arg>
Arg get_tuple_t_ReduceGPU(Arg (F_t::*)(const Arg&, const Arg&));

// template<typename Arg>
// Arg get_tuple_t_ReduceGPU(Arg (*)(Arg&, Arg&));

template<typename F_t>
decltype(get_tuple_t_ReduceGPU(&F_t::operator())) get_tuple_t_ReduceGPU(F_t);

std::false_type get_tuple_t_ReduceGPU(...); // black hole
/*****************************************************************************************************************************/

/**************************************************** WINDOWED OPERATORS *****************************************************/

// declaration of functions to extract the tuple type from the signature of the lift function
template<typename F_t, typename Arg1, typename Arg2> // lift function
Arg1 get_tuple_t_LiftGPU(void (F_t::*)(const Arg1&, Arg2&) const);

template<typename F_t, typename Arg1, typename Arg2> // lift function
Arg1 get_tuple_t_LiftGPU(void (F_t::*)(const Arg1&, Arg2&));

// template<typename Arg1, typename Arg2> // lift function
// Arg1 get_tuple_t_LiftGPU(void (*)(const Arg1&, Arg2&));

template<typename F_t>
decltype(get_tuple_t_LiftGPU(&F_t::operator())) get_tuple_t_LiftGPU(F_t);

std::false_type get_tuple_t_LiftGPU(...); // black hole

// declaration of functions to extract the tuple type from the signature of the combine function
template<typename F_t, typename Arg> // combine function
Arg get_tuple_t_CombGPU(void (F_t::*)(const Arg&, const Arg&, Arg&) const);

template<typename F_t, typename Arg> // combine function
Arg get_tuple_t_CombGPU(void (F_t::*)(const Arg&, const Arg&, Arg&));

// template<typename Arg> // combine function
// Arg get_tuple_t_CombGPU(vid (*)(const Arg&, const Arg&, Arg));

template<typename F_t>
decltype(get_tuple_t_CombGPU(&F_t::operator())) get_tuple_t_CombGPU(F_t);

std::false_type get_tuple_t_CombGPU(...); // black hole

// declaration of functions to extract the result type from the signature of the lift function
template<typename F_t, typename Arg1, typename Arg2> // lift function
Arg2 get_result_t_LiftGPU(void (F_t::*)(const Arg1&, Arg2&) const);

template<typename F_t, typename Arg1, typename Arg2> // lift function
Arg2 get_result_t_LiftGPU(void (F_t::*)(const Arg1&, Arg2&));

// template<typename Arg1, typename Arg2> // lift function
// Arg2 get_result_t_LiftGPU(void (*)(const Arg1&, Arg2&));

template<typename F_t>
decltype(get_result_t_LiftGPU(&F_t::operator())) get_result_t_LiftGPU(F_t);

std::false_type get_result_t_LiftGPU(...); // black holes

// declaration of functions to extract the result type from the signature of the combine function
template<typename F_t, typename Arg> // combine function
Arg get_result_t_CombGPU(void (F_t::*)(const Arg&, const Arg&, Arg &) const);

template<typename F_t, typename Arg> // combine function
Arg get_result_t_CombGPU(void (F_t::*)(const Arg&, const Arg&, Arg&));

// template<typename Arg> // combine function
// Arg get_result_t_CombGPU(void (*)(const Arg&, const Arg&, Arg&));

template<typename F_t>
decltype(get_result_t_CombGPU(&F_t::operator())) get_result_t_CombGPU(F_t);

std::false_type get_result_t_CombGPU(...); // black hole
/*****************************************************************************************************************************/

} // namespace wf

#endif
