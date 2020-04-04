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
 *  @date    02/04/2020
 *  
 *  @brief Metafunctions used by the WindFlow library for GPU operators
 *  
 *  @section Metafunctions-2 (Description)
 *  
 *  Set of metafunctions used by the WindFlow library for GPU operators.
 */ 

#ifndef META_GPU_H
#define META_GPU_H

// includes
#include <atomic>
#if __cplusplus < 201703L // not C++17
    #include <experimental/optional>
    namespace std { using namespace experimental; } // ugly but necessary until CUDA will support C++17!
#else
    #include <optional>
#endif
#include <basic.hpp>
#include <context.hpp>

namespace wf {

/**************************************************** WINDOWED OPERATORS *****************************************************/
// declaration of functions to extract the tuple type from the signature of the GPU window function
template<typename F_t, typename Arg1, typename Arg2>
Arg1 get_tuple_t_WinGPU(void (F_t::*)(uint64_t, const Arg1*, size_t, Arg2*, char*, size_t) const);

template<typename F_t, typename Arg1, typename Arg2>
Arg1 get_tuple_t_WinGPU(void (F_t::*)(uint64_t, const Arg1*, size_t, Arg2*, char*, size_t));

template<typename Arg1, typename Arg2>
Arg1 get_tuple_t_WinGPU(void (*)(uint64_t, const Arg1*, size_t, Arg2*, char*, size_t));

template<typename F_t>
decltype(get_tuple_t_WinGPU(&F_t::operator())) get_tuple_t_WinGPU(F_t);

std::false_type get_tuple_t_WinGPU(...); // black hole

// declaration of functions to extract the result type from the signature of the GPU window function
template<typename F_t, typename Arg1, typename Arg2>
Arg2 get_result_t_WinGPU(void (F_t::*)(uint64_t, const Arg1*, size_t, Arg2*, char*, size_t) const);

template<typename F_t, typename Arg1, typename Arg2>
Arg2 get_result_t_WinGPU(void (F_t::*)(uint64_t, const Arg1*, size_t, Arg2*, char*, size_t));

template<typename Arg1, typename Arg2>
Arg2 get_result_t_WinGPU(void (*)(uint64_t, const Arg1*, size_t, Arg2*, char*, size_t));

template<typename F_t>
decltype(get_result_t_WinGPU(&F_t::operator())) get_result_t_WinGPU(F_t);

std::false_type get_result_t_WinGPU(...); // black hole
/*****************************************************************************************************************************/

/***************************************** SPECIAL FUNCTIONS FOR NESTING OF OPERATORS ****************************************/
// declaration of functions to extract the type of Win_Farm_GPU from the Pane_Farm_GPU operator provided to the builder
template<typename ...Args>
Win_Farm_GPU<Args...> *get_WF_GPU_nested_type(Pane_Farm_GPU<Args...> const&);

// declaration of functions to extract the type of Win_Farm_GPU from the Win_MapReduce_GPU operator provided to the builder
template<typename ...Args>
Win_Farm_GPU<Args...> *get_WF_GPU_nested_type(Win_MapReduce_GPU<Args...> const&);

// declaration of functions to extract the type of Win_Farm_GPU from the function/functor/lambda provided to the builder
template<typename F_t>
auto *get_WF_GPU_nested_type(F_t _f)
{
    Win_Farm_GPU<decltype(get_tuple_t_WinGPU(_f)),
                 decltype(get_result_t_WinGPU(_f)),
                 decltype(_f)>*ptr = nullptr;
    return ptr;
}

std::false_type *get_WF_GPU_nested_type(...); // black hole

// declaration of functions to extract the type of Key_Farm_GPU from the Pane_Farm_GPU operator provided to the builder
template<typename arg1, typename arg2, typename arg3, typename arg4>
Key_Farm_GPU<arg1, arg2, arg3> *get_KF_GPU_nested_type(Pane_Farm_GPU<arg1, arg2, arg3, arg4> const&);

// declaration of functions to extract the type of Key_Farm_GPU from the Win_MapReduce_GPU operator provided to the builder
template<typename arg1, typename arg2, typename arg3, typename arg4>
Key_Farm_GPU<arg1, arg2, arg3> *get_KF_GPU_nested_type(Win_MapReduce_GPU<arg1, arg2, arg3, arg4> const&);

// declaration of functions to extract the type of Key_Farm_GPU from the function/functor/lambda provided to the builder
template<typename F_t>
auto *get_KF_GPU_nested_type(F_t _f)
{
    Key_Farm_GPU<decltype(get_tuple_t_WinGPU(_f)),
                 decltype(get_result_t_WinGPU(_f)),
                 decltype(_f)>*ptr = nullptr;
    return ptr;
}

std::false_type *get_KF_GPU_nested_type(...); // black hole
/*****************************************************************************************************************************/

/***************************************************** UTILITY FUNCTIONS *****************************************************/
// declaration of functions to extract the type of the function to be executed on the GPU
template<typename F_t, typename G_t, typename Ret, typename Arg1, typename Arg2>
F_t get_GPU_F(Ret (F_t::*)(uint64_t, const Arg1*, size_t, Arg2*, char*, size_t) const, Ret (G_t::*)(uint64_t, const Iterable<Arg2>&, Arg2&) const);

template<typename F_t, typename Ret, typename Arg1, typename Arg2>
F_t get_GPU_F(Ret (F_t::*)(uint64_t, const Arg1*, size_t, Arg2*, char*, size_t) const, Ret (*)(uint64_t, const Iterable<Arg2>&, Arg2&));

template<typename F_t, typename G_t, typename Ret, typename Arg1, typename Arg2>
F_t get_GPU_F(Ret (F_t::*)(uint64_t, const Arg1*, size_t, Arg2*, char*, size_t) const, Ret (G_t::*)(uint64_t, const Arg2&, Arg2&) const);

template<typename F_t, typename Ret, typename Arg1, typename Arg2>
F_t get_GPU_F(Ret (F_t::*)(uint64_t, const Arg1*, size_t, Arg2*, char*, size_t) const, Ret (*)(uint64_t, const Arg2&, Arg2&));

template<typename F_t, typename G_t, typename Ret, typename Arg1, typename Arg2>
G_t get_GPU_F(Ret (F_t::*)(uint64_t, const Iterable<Arg1>&, Arg2&) const, Ret(G_t::*)(uint64_t, const Arg2*, size_t, Arg2*, char*, size_t) const);

template<typename G_t, typename Ret, typename Arg1, typename Arg2>
G_t get_GPU_F(Ret (*)(uint64_t, const Iterable<Arg1>&, Arg2&), Ret(G_t::*)(uint64_t, const Arg2*, size_t, Arg2*, char*, size_t) const);

template<typename F_t, typename G_t, typename Ret, typename Arg1, typename Arg2>
G_t get_GPU_F(Ret (F_t::*)(uint64_t, const Arg1&, Arg2&) const, Ret(G_t::*)(uint64_t, const Arg2*, size_t, Arg2*, char*, size_t) const);

template<typename G_t, typename Ret, typename Arg1, typename Arg2>
G_t get_GPU_F(Ret (*)(uint64_t, const Arg1&, Arg2&), Ret(G_t::*)(uint64_t, const Arg2*, size_t, Arg2*, char*, size_t) const);

template<typename F_t, typename G_t> // -> this implementation prevents plain functions to be used for the Pane_Farm_GPU and Win_MapReduce_GPU operators
decltype(get_GPU_F(&F_t::operator(), &G_t::operator())) get_GPU_F(F_t, G_t);

std::false_type get_GPU_F(...); // black hole
/*****************************************************************************************************************************/

} // namespace wf

#endif
