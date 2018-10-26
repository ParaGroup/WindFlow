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
 *  @file    meta_utils.hpp
 *  @author  Gabriele Mencagli
 *  @date    16/08/2018
 *  @version 1.0
 *  
 *  @brief Metafunctions used by the WindFlow library
 *  
 *  @section DESCRIPTION
 *  
 *  Set of metafunctions used by the WindFlow library
 */ 

#ifndef META_H
#define META_H

// includes
#include <atomic>
#include <iterable.hpp>

using namespace std;

// metafunctions to get the tuple type from a callable type (e.g., function, lambda, functor)
template<typename F_t, typename Ret, typename Arg1, typename Arg2>
Arg1 get_tuple_t(Ret (F_t::*)(size_t, size_t, Iterable<Arg1>&, Arg2&) const);

template<typename Ret, typename Arg1, typename Arg2>
Arg1 get_tuple_t(Ret (*)(size_t, size_t, Iterable<Arg1>&, Arg2&));

template<typename F_t, typename Ret, typename Arg1, typename Arg2>
Arg1 get_tuple_t(Ret (F_t::*)(size_t, size_t, const Arg1&, Arg2&) const);

template<typename Ret, typename Arg1, typename Arg2>
Arg1 get_tuple_t(Ret (*)(size_t, size_t, const Arg1&, Arg2&));

template<typename F_t, typename Ret, typename Arg1, typename Arg2>
Arg1 get_tuple_t(Ret (F_t::*)(size_t, size_t, const Arg1*, Arg2*, size_t, char*) const);

template<typename Ret, typename Arg1, typename Arg2>
Arg1 get_tuple_t(Ret (*)(size_t, size_t, const Arg1*, Arg2*, size_t, char*));

template<typename F_t>
decltype(get_tuple_t(&F_t::operator())) get_tuple_t(F_t);

// metafunctions to get the result type from a callable type (e.g., function, lambda, functor)
template<typename F_t, typename Ret, typename Arg1, typename Arg2>
Arg2 get_result_t(Ret (F_t::*)(size_t, size_t, Arg1&, Arg2&) const);

template<typename Ret, typename Arg1, typename Arg2>
Arg2 get_result_t(Ret (*)(size_t, size_t, Arg1&, Arg2&));

template<typename F_t, typename Ret, typename Arg1, typename Arg2>
Arg2 get_result_t(Ret (F_t::*)(size_t, size_t, const Arg1*, Arg2*, size_t, char*) const);

template<typename Ret, typename Arg1, typename Arg2>
Arg2 get_result_t(Ret (*)(size_t, size_t, const Arg1*, Arg2*, size_t, char*));

template<typename F_t>
decltype(get_result_t(&F_t::operator())) get_result_t(F_t);

// metafunction to extract the type of Win_Farm from the inner pattern of type Pane_Farm
template<typename ...Args>
Win_Farm<Args...> get_WF_nested_type(Pane_Farm<Args...> const);

// metafunction to extract the type of Win_Farm from the inner pattern of type Win_MapReduce
template<typename ...Args>
Win_Farm<Args...> get_WF_nested_type(Win_MapReduce<Args...> const);

// metafunction to extract the type of Win_Farm from a callable type (e.g., function, lambda, functor)
template<typename F_t>
auto get_WF_nested_type(F_t _f)
{
    return Win_Farm<decltype(get_tuple_t(_f)),
                    decltype(get_result_t(_f))>(); // stub constructor
}

// metafunction to extract the type of Win_Farm_GPU from the inner pattern of type Pane_Farm_GPU
template<typename ...Args>
Win_Farm_GPU<Args...> get_WF_GPU_nested_type(Pane_Farm_GPU<Args...> const);

// metafunction to extract the type of Win_Farm_GPU from the inner pattern of type Win_MapReduce_GPU
template<typename ...Args>
Win_Farm_GPU<Args...> get_WF_GPU_nested_type(Win_MapReduce_GPU<Args...> const);

// metafunction to extract the type of Win_Farm_GPU from a callable type (e.g., function, lambda, functor)
template<typename F_t>
auto get_WF_GPU_nested_type(F_t _f)
{
    return Win_Farm_GPU<decltype(get_tuple_t(_f)),
                        decltype(get_result_t(_f)),
                        decltype(_f)>(); // stub constructor
}

// metafunction to extract the type of Key_Farm from the inner pattern of type Pane_Farm
template<typename ...Args>
Key_Farm<Args...> get_KF_nested_type(Pane_Farm<Args...> const);

// metafunction to extract the type of Key_Farm from the inner pattern of type Win_MapReduce
template<typename ...Args>
Key_Farm<Args...> get_KF_nested_type(Win_MapReduce<Args...> const);

// metafunction to extract the type of Key_Farm from a callable type (e.g., function, lambda, functor)
template<typename F_t>
auto get_KF_nested_type(F_t _f)
{
    return Key_Farm<decltype(get_tuple_t(_f)),
                    decltype(get_result_t(_f))>(); // stub constructor
}

// metafunction to extract the type of Key_Farm_GPU from the inner pattern of type Pane_Farm_GPU
template<typename ...Args>
Key_Farm_GPU<Args...> get_KF_GPU_nested_type(Pane_Farm_GPU<Args...> const);

// metafunction to extract the type of Key_Farm_GPU from the inner pattern of type Win_MapReduce_GPU
template<typename ...Args>
Key_Farm_GPU<Args...> get_KF_GPU_nested_type(Win_MapReduce_GPU<Args...> const);

// metafunction to extract the type of Key_Farm_GPU from a callable type (e.g., function, lambda, functor)
template<typename F_t>
auto get_KF_GPU_nested_type(F_t _f)
{
    return Key_Farm_GPU<decltype(get_tuple_t(_f)),
                        decltype(get_result_t(_f)),
                        decltype(_f)>(); // stub constructor
}

// metafunctions to return the callable type to be executed on the GPU (only lambda or functor!)
template<typename F_t, typename G_t, typename Ret, typename Arg1, typename Arg2>
F_t get_GPU_F(Ret (F_t::*)(size_t, size_t, const Arg1*, Arg2*, size_t, char*) const, Ret (G_t::*)(size_t, size_t, Iterable<Arg2>&, Arg2&) const);

template<typename F_t, typename Ret, typename Arg1, typename Arg2>
F_t get_GPU_F(Ret (F_t::*)(size_t, size_t, const Arg1*, Arg2*, size_t, char*) const, Ret (*)(size_t, size_t, Iterable<Arg2>&, Arg2&));

template<typename F_t, typename G_t, typename Ret, typename Arg1, typename Arg2>
F_t get_GPU_F(Ret (F_t::*)(size_t, size_t, const Arg1*, Arg2*, size_t, char*) const, Ret (G_t::*)(size_t, size_t, const Arg2&, Arg2&) const);

template<typename F_t, typename Ret, typename Arg1, typename Arg2>
F_t get_GPU_F(Ret (F_t::*)(size_t, size_t, const Arg1*, Arg2*, size_t, char*) const, Ret (*)(size_t, size_t, const Arg2&, Arg2&));

template<typename F_t, typename G_t, typename Ret, typename Arg1, typename Arg2>
G_t get_GPU_F(Ret (F_t::*)(size_t, size_t, Iterable<Arg1>&, Arg2&) const, Ret(G_t::*)(size_t, size_t, const Arg2*, Arg2*, size_t, char*) const);

template<typename G_t, typename Ret, typename Arg1, typename Arg2>
G_t get_GPU_F(Ret (*)(size_t, size_t, Iterable<Arg1>&, Arg2&), Ret(G_t::*)(size_t, size_t, const Arg2*, Arg2*, size_t, char*) const);

template<typename F_t, typename G_t, typename Ret, typename Arg1, typename Arg2>
G_t get_GPU_F(Ret (F_t::*)(size_t, size_t, const Arg1&, Arg2&) const, Ret(G_t::*)(size_t, size_t, const Arg2*, Arg2*, size_t, char*) const);

template<typename G_t, typename Ret, typename Arg1, typename Arg2>
G_t get_GPU_F(Ret (*)(size_t, size_t, const Arg1&, Arg2&), Ret(G_t::*)(size_t, size_t, const Arg2*, Arg2*, size_t, char*) const);

// wrapper struct of input tuples
template<typename tuple_t>
struct wrapper_tuple_t
{
    tuple_t *tuple; // pointer to a tuple
    atomic<size_t> counter; // atomic reference counter
    bool eos; // if true, the tuple is a EOS marker

    // constructor
    wrapper_tuple_t(tuple_t *_t, size_t _counter=1, bool _eos=false): tuple(_t), counter(_counter), eos(_eos) {}

    // destructor
    ~wrapper_tuple_t() {}
};

// function extractTuple: definition valid if T1 != T2
template <typename T1, typename T2>
T1 *extractTuple(typename enable_if<!is_same<T1,T2>::value, T2>::type *wt) // T1 is the type of the tuple and T2 is its wrapper type
{
    return wt->tuple;
}

// function extractTuple: definition valid if T1 == T2
template <typename T1, typename T2>
T1 *extractTuple(typename enable_if<is_same<T1,T2>::value, T2>::type *t) // T1 and T2 are the same type: the tuple's type
{
    return t;
}

// function deleteTuple: definition valid if T1 != T2
template <typename T1, typename T2>
void deleteTuple(typename enable_if<!is_same<T1,T2>::value, T2>::type *wt) // T1 is the type of the tuple and T2 is its wrapper type
{
    T1 *t = wt->tuple;
    // check if the tuple and the wrapper must be destroyed/deallocated
    int old_cnt = (wt->counter).fetch_sub(1);
    if (old_cnt == 1) {
        delete t;
        delete wt;
    }
}

// function deleteTuple: definition valid if T1 == T2
template <typename T1, typename T2>
void deleteTuple(typename enable_if<is_same<T1,T2>::value, T2>::type *t) // T1 and T2 are the same type: the tuple's type
{
    delete t;
}

// function prepareWrapper: definition valid if T1 != T2
template<typename T1, typename T2>
T2 *prepareWrapper(typename enable_if<!is_same<T1,T2>::value, T1>::type *t, size_t val) // T1 is the type of the tuple and T2 is its wrapper type
{
    // create wrapper
    return new T2(t, val);
}

// function prepareWrapper: definition valid if T1 == T2
template<typename T1, typename T2>
T2 *prepareWrapper(typename enable_if<is_same<T1,T2>::value, T1>::type *wt, size_t val) // T1 and T2 are the same type: the wrapper's type
{
    (wt->counter).fetch_add(val-1);
    return wt;
}

// function isEOSMarker: definition valid if T1 != T2
template<typename T1, typename T2>
bool isEOSMarker(const typename enable_if<!is_same<T1,T2>::value, T2>::type &wt) // T1 is the type of the tuple and T2 is its wrapper type
{
    return wt.eos;
}

// function isEOSMarker: definition valid if T1 == T2
template<typename T1, typename T2>
bool isEOSMarker(const typename enable_if<is_same<T1,T2>::value, T1>::type &t) // T1 and T2 are the same type: the wrapper's type
{
    return false;
}

#endif
