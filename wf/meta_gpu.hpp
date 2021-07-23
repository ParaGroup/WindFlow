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

/**************************************************** WINDOWED OPERATORS *****************************************************/
// declaration of functions to extract the tuple type from the signature of the combine function
template<typename F_t, typename Arg> // combine function
Arg get_tuple_t_CombGPU(void (F_t::*)(const Arg&, const Arg&, Arg&) const);

template<typename F_t, typename Arg> // combine function
Arg get_tuple_t_CombGPU(void (F_t::*)(const Arg&, const Arg&, Arg&));

// template<typename Arg> // combine function
// Arg get_tuple_t_CombGPU(void (*)(const Arg&, const Arg&, Arg&));

template<typename F_t, typename Arg> // combine riched function
Arg get_tuple_t_CombGPU(void (F_t::*)(const Arg&, const Arg&, Arg&, RuntimeContext&) const);

template<typename F_t, typename Arg> // combine riched function
Arg get_tuple_t_CombGPU(void (F_t::*)(const Arg&, const Arg&, Arg&, RuntimeContext&));

// template<typename Arg> // combine riched function
// Arg get_tuple_t_CombGPU(void (*)(const Arg&, const Arg&, Arg&, RuntimeContext&));

template<typename F_t>
decltype(get_tuple_t_Comb(&F_t::operator())) get_tuple_t_CombGPU(F_t);

std::false_type get_tuple_t_CombGPU(...); // black hole
/*****************************************************************************************************************************/

} // namespace wf

#endif
