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
 *  
 *  @brief Metafunctions used by the WindFlow library
 *  
 *  @section Metafunctions (Descriptions)
 *  
 *  Set of metafunctions used by the WindFlow library
 */ 

#ifndef META_H
#define META_H

// includes
#include <atomic>
#if __cplusplus < 201703L //not C++17
    #include <experimental/optional>
    using namespace std::experimental;
#else
    #include <optional>
#endif
#include <context.hpp>
#include <shipper.hpp>
#include <iterable.hpp>

using namespace std;

// metafunctions to get the tuple type from a callable type (e.g., function, lambda, functor)
template<typename F_t, typename Arg1, typename Arg2> // Map not in-place, Accumulator
Arg1 get_tuple_t(void (F_t::*)(const Arg1 &, Arg2 &) const);

template<typename F_t, typename Arg1, typename Arg2> // Map not in-place, Accumulator
Arg1 get_tuple_t(void (F_t::*)(const Arg1 &, Arg2 &));

template<typename Arg1, typename Arg2> // Map not in-place, Accumulator
Arg1 get_tuple_t(void (*)(const Arg1 &, Arg2 &));

template<typename F_t, typename Arg1, typename Arg2> // Map not in-place, Accumulator
Arg1 get_tuple_t(void (F_t::*)(const Arg1 &, Arg2 &, RuntimeContext&) const);

template<typename F_t, typename Arg1, typename Arg2> // Map not in-place, Accumulator
Arg1 get_tuple_t(void (F_t::*)(const Arg1 &, Arg2 &, RuntimeContext&));

template<typename Arg1, typename Arg2> // Map not in-place, Accumulator
Arg1 get_tuple_t(void (*)(const Arg1 &, Arg2 &, RuntimeContext&));

template<typename F_t, typename Arg1> // Map in-place
Arg1 get_tuple_t(void (F_t::*)(Arg1 &) const);

template<typename F_t, typename Arg1> // Map in-place
Arg1 get_tuple_t(void (F_t::*)(Arg1 &));

template<typename Arg1> // Map in-place
Arg1 get_tuple_t(void (*)(Arg1 &));

template<typename F_t, typename Arg1> // Map in-place
Arg1 get_tuple_t(void (F_t::*)(Arg1 &, RuntimeContext&) const);

template<typename F_t, typename Arg1> // Map in-place
Arg1 get_tuple_t(void (F_t::*)(Arg1 &, RuntimeContext&));

template<typename Arg1> // Map in-place
Arg1 get_tuple_t(void (*)(Arg1 &, RuntimeContext&));

template<typename F_t, typename Arg> // Source item-by-item, Filter
Arg get_tuple_t(bool (F_t::*)(Arg &) const);

template<typename F_t, typename Arg> // Source item-by-item, Filter
Arg get_tuple_t(bool (F_t::*)(Arg &));

template<typename Arg> // Source item-by-item, Filter
Arg get_tuple_t(bool (*)(Arg &));

template<typename F_t, typename Arg> // Source item-by-item, Filter
Arg get_tuple_t(bool (F_t::*)(Arg &, RuntimeContext&) const);

template<typename F_t, typename Arg> // Source item-by-item, Filter
Arg get_tuple_t(bool (F_t::*)(Arg &, RuntimeContext&));

template<typename Arg>
Arg get_tuple_t(bool (*)(Arg &, RuntimeContext&)); // Source item-by-item, Filter

template<typename F_t, typename Arg1, typename Arg2> // FlatMap
Arg1 get_tuple_t(void (F_t::*)(const Arg1 &, Shipper<Arg2>&) const);

template<typename F_t, typename Arg1, typename Arg2> // FlatMap
Arg1 get_tuple_t(void (F_t::*)(const Arg1 &, Shipper<Arg2>&));

template<typename Arg1, typename Arg2> // FlatMap
Arg1 get_tuple_t(void (*)(const Arg1 &, Shipper<Arg2>&));

template<typename F_t, typename Arg1, typename Arg2> // FlatMap
Arg1 get_tuple_t(void (F_t::*)(const Arg1 &, Shipper<Arg2>&, RuntimeContext&) const);

template<typename F_t, typename Arg1, typename Arg2> // FlatMap
Arg1 get_tuple_t(void (F_t::*)(const Arg1 &, Shipper<Arg2>&, RuntimeContext&));

template<typename Arg1, typename Arg2> // FlatMap
Arg1 get_tuple_t(void (*)(const Arg1 &, Shipper<Arg2>&, RuntimeContext&));

template<typename F_t, typename Arg> // Source single-loop
Arg get_tuple_t(void (F_t::*)(Shipper<Arg>&) const);

template<typename F_t, typename Arg> // Source single-loop
Arg get_tuple_t(void (F_t::*)(Shipper<Arg>&));

template<typename Arg> // Source single-loop
Arg get_tuple_t(void (*)(Shipper<Arg>&));

template<typename F_t, typename Arg> // Source single-loop
Arg get_tuple_t(void (F_t::*)(Shipper<Arg>&, RuntimeContext&) const);

template<typename F_t, typename Arg> // Source single-loop
Arg get_tuple_t(void (F_t::*)(Shipper<Arg>&, RuntimeContext&));

template<typename Arg> // Source single-loop
Arg get_tuple_t(void (*)(Shipper<Arg>&, RuntimeContext&));

template<typename F_t, typename Arg> // Sink
Arg get_tuple_t(void (F_t::*)(optional<Arg> &) const);

template<typename F_t, typename Arg> // Sink
Arg get_tuple_t(void (F_t::*)(optional<Arg> &));

template<typename Arg> // Sink
Arg get_tuple_t(void (*)(optional<Arg> &));

template<typename F_t, typename Arg> // Sink
Arg get_tuple_t(void (F_t::*)(optional<Arg> &, RuntimeContext&) const);

template<typename F_t, typename Arg> // Sink
Arg get_tuple_t(void (F_t::*)(optional<Arg> &, RuntimeContext&));

template<typename Arg> // Sink
Arg get_tuple_t(void (*)(optional<Arg> &, RuntimeContext&));

template<typename F_t, typename Ret, typename Arg1, typename Arg2> // Window-based Patterns (non-incremental)
Arg1 get_tuple_t(Ret (F_t::*)(uint64_t, Iterable<Arg1>&, Arg2&) const);

template<typename F_t, typename Ret, typename Arg1, typename Arg2> // Window-based Patterns (non-incremental)
Arg1 get_tuple_t(Ret (F_t::*)(uint64_t, Iterable<Arg1>&, Arg2&, RuntimeContext&) const);

template<typename F_t, typename Ret, typename Arg1, typename Arg2> // Window-based Patterns (non-incremental)
Arg1 get_tuple_t(Ret (F_t::*)(uint64_t, Iterable<Arg1>&, Arg2&));

template<typename F_t, typename Ret, typename Arg1, typename Arg2> // Window-based Patterns (non-incremental)
Arg1 get_tuple_t(Ret (F_t::*)(uint64_t, Iterable<Arg1>&, Arg2&, RuntimeContext&));

template<typename Ret, typename Arg1, typename Arg2> // Window-based Patterns (non-incremental)
Arg1 get_tuple_t(Ret (*)(uint64_t, Iterable<Arg1>&, Arg2&));

template<typename Ret, typename Arg1, typename Arg2> // Window-based Patterns (non-incremental)
Arg1 get_tuple_t(Ret (*)(uint64_t, Iterable<Arg1>&, Arg2&, RuntimeContext&));

template<typename F_t, typename Ret, typename Arg1, typename Arg2> // Window-based Patterns (incremental)
Arg1 get_tuple_t(Ret (F_t::*)(uint64_t, const Arg1&, Arg2&) const);

template<typename F_t, typename Ret, typename Arg1, typename Arg2> // Window-based Patterns (incremental)
Arg1 get_tuple_t(Ret (F_t::*)(uint64_t, const Arg1&, Arg2&, RuntimeContext&) const);

template<typename F_t, typename Ret, typename Arg1, typename Arg2> // Window-based Patterns (incremental)
Arg1 get_tuple_t(Ret (F_t::*)(uint64_t, const Arg1&, Arg2&));

template<typename F_t, typename Ret, typename Arg1, typename Arg2> // Window-based Patterns (incremental)
Arg1 get_tuple_t(Ret (F_t::*)(uint64_t, const Arg1&, Arg2&, RuntimeContext&));

template<typename Ret, typename Arg1, typename Arg2> // Window-based Patterns (incremental)
Arg1 get_tuple_t(Ret (*)(uint64_t, const Arg1&, Arg2&));

template<typename Ret, typename Arg1, typename Arg2> // Window-based Patterns (incremental)
Arg1 get_tuple_t(Ret (*)(uint64_t, const Arg1&, Arg2&, RuntimeContext&));

template<typename F_t, typename Ret, typename Arg1, typename Arg2> // Window-based Patterns (non-incremental on GPU)
Arg1 get_tuple_t(Ret (F_t::*)(uint64_t, const Arg1*, Arg2*, size_t, char*) const);

template<typename F_t, typename Ret, typename Arg1, typename Arg2> // Window-based Patterns (non-incremental on GPU)
Arg1 get_tuple_t(Ret (F_t::*)(uint64_t, const Arg1*, Arg2*, size_t, char*));

template<typename Ret, typename Arg1, typename Arg2> // Window-based Patterns (non-incremental on GPU)
Arg1 get_tuple_t(Ret (*)(uint64_t, const Arg1*, Arg2*, size_t, char*));

template<typename F_t>
decltype(get_tuple_t(&F_t::operator())) get_tuple_t(F_t);

// metafunctions to get the result type from a callable type (e.g., function, lambda, functor)
template<typename F_t, typename Arg1, typename Arg2> // Map not in-place, Accumulator
Arg2 get_result_t(void (F_t::*)(const Arg1 &, Arg2 &) const);

template<typename F_t, typename Arg1, typename Arg2> // Map not in-place, Accumulator
Arg2 get_result_t(void (F_t::*)(const Arg1 &, Arg2 &));

template<typename Arg1, typename Arg2> // Map not in-place, Accumulator
Arg2 get_result_t(void (*)(const Arg1 &, Arg2 &));

template<typename F_t, typename Arg1, typename Arg2> // Map not in-place, Accumulator
Arg2 get_result_t(void (F_t::*)(const Arg1 &, Arg2 &, RuntimeContext&) const);

template<typename F_t, typename Arg1, typename Arg2> // Map not in-place, Accumulator
Arg2 get_result_t(void (F_t::*)(const Arg1 &, Arg2 &, RuntimeContext&));

template<typename Arg1, typename Arg2> // Map not in-place, Accumulator
Arg2 get_result_t(void (*)(const Arg1 &, Arg2 &, RuntimeContext&));

template<typename F_t, typename Arg1> // Map in-place
Arg1 get_result_t(void (F_t::*)(Arg1 &) const);

template<typename F_t, typename Arg1> // Map in-place
Arg1 get_result_t(void (F_t::*)(Arg1 &));

template<typename Arg1> // Map in-place
Arg1 get_result_t(void (*)(Arg1 &));

template<typename F_t, typename Arg1> // Map in-place
Arg1 get_result_t(void (F_t::*)(Arg1 &, RuntimeContext&) const);

template<typename F_t, typename Arg1> // Map in-place
Arg1 get_result_t(void (F_t::*)(Arg1 &, RuntimeContext&));

template<typename Arg1> // Map in-place
Arg1 get_result_t(void (*)(Arg1 &, RuntimeContext&));

template<typename F_t, typename Arg1, typename Arg2> // FlatMap
Arg2 get_result_t(void (F_t::*)(const Arg1 &, Shipper<Arg2>&) const);

template<typename F_t, typename Arg1, typename Arg2> // FlatMap
Arg2 get_result_t(void (F_t::*)(const Arg1 &, Shipper<Arg2>&));

template<typename Arg1, typename Arg2> // FlatMap
Arg2 get_result_t(void (*)(const Arg1 &, Shipper<Arg2>&));

template<typename F_t, typename Arg1, typename Arg2> // FlatMap
Arg2 get_result_t(void (F_t::*)(const Arg1 &, Shipper<Arg2>&, RuntimeContext&) const);

template<typename F_t, typename Arg1, typename Arg2> // FlatMap
Arg2 get_result_t(void (F_t::*)(const Arg1 &, Shipper<Arg2>&, RuntimeContext&));

template<typename Arg1, typename Arg2> // FlatMap
Arg2 get_result_t(void (*)(const Arg1 &, Shipper<Arg2>&, RuntimeContext&));

template<typename F_t, typename Ret, typename Arg1, typename Arg2> // Window-based Patterns
Arg2 get_result_t(Ret (F_t::*)(uint64_t, Arg1&, Arg2&) const);

template<typename F_t, typename Ret, typename Arg1, typename Arg2> // Window-based Patterns
Arg2 get_result_t(Ret (F_t::*)(uint64_t, Arg1&, Arg2&, RuntimeContext&) const);

template<typename F_t, typename Ret, typename Arg1, typename Arg2> // Window-based Patterns
Arg2 get_result_t(Ret (F_t::*)(uint64_t, Arg1&, Arg2&));

template<typename F_t, typename Ret, typename Arg1, typename Arg2> // Window-based Patterns
Arg2 get_result_t(Ret (F_t::*)(uint64_t, Arg1&, Arg2&, RuntimeContext&));

template<typename Ret, typename Arg1, typename Arg2> // Window-based Patterns
Arg2 get_result_t(Ret (*)(uint64_t, Arg1&, Arg2&));

template<typename Ret, typename Arg1, typename Arg2> // Window-based Patterns
Arg2 get_result_t(Ret (*)(uint64_t, Arg1&, Arg2&, RuntimeContext&));

template<typename F_t, typename Ret, typename Arg1, typename Arg2> // Window-based Patterns (on GPU)
Arg2 get_result_t(Ret (F_t::*)(uint64_t, const Arg1*, Arg2*, size_t, char*) const);

template<typename F_t, typename Ret, typename Arg1, typename Arg2> // Window-based Patterns (on GPU)
Arg2 get_result_t(Ret (F_t::*)(uint64_t, const Arg1*, Arg2*, size_t, char*));

template<typename Ret, typename Arg1, typename Arg2> // Window-based Patterns (on GPU)
Arg2 get_result_t(Ret (*)(uint64_t, const Arg1*, Arg2*, size_t, char*));

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
F_t get_GPU_F(Ret (F_t::*)(uint64_t, const Arg1*, Arg2*, size_t, char*) const, Ret (G_t::*)(uint64_t, Iterable<Arg2>&, Arg2&) const);

template<typename F_t, typename Ret, typename Arg1, typename Arg2>
F_t get_GPU_F(Ret (F_t::*)(uint64_t, const Arg1*, Arg2*, size_t, char*) const, Ret (*)(uint64_t, Iterable<Arg2>&, Arg2&));

template<typename F_t, typename G_t, typename Ret, typename Arg1, typename Arg2>
F_t get_GPU_F(Ret (F_t::*)(uint64_t, const Arg1*, Arg2*, size_t, char*) const, Ret (G_t::*)(uint64_t, const Arg2&, Arg2&) const);

template<typename F_t, typename Ret, typename Arg1, typename Arg2>
F_t get_GPU_F(Ret (F_t::*)(uint64_t, const Arg1*, Arg2*, size_t, char*) const, Ret (*)(uint64_t, const Arg2&, Arg2&));

template<typename F_t, typename G_t, typename Ret, typename Arg1, typename Arg2>
G_t get_GPU_F(Ret (F_t::*)(uint64_t, Iterable<Arg1>&, Arg2&) const, Ret(G_t::*)(uint64_t, const Arg2*, Arg2*, size_t, char*) const);

template<typename G_t, typename Ret, typename Arg1, typename Arg2>
G_t get_GPU_F(Ret (*)(uint64_t, Iterable<Arg1>&, Arg2&), Ret(G_t::*)(uint64_t, const Arg2*, Arg2*, size_t, char*) const);

template<typename F_t, typename G_t, typename Ret, typename Arg1, typename Arg2>
G_t get_GPU_F(Ret (F_t::*)(uint64_t, const Arg1&, Arg2&) const, Ret(G_t::*)(uint64_t, const Arg2*, Arg2*, size_t, char*) const);

template<typename G_t, typename Ret, typename Arg1, typename Arg2>
G_t get_GPU_F(Ret (*)(uint64_t, const Arg1&, Arg2&), Ret(G_t::*)(uint64_t, const Arg2*, Arg2*, size_t, char*) const);

// wrapper struct of input tuples
template<typename tuple_t>
struct wrapper_tuple_t
{
    tuple_t *tuple; // pointer to a tuple
    atomic<size_t> counter; // atomic reference counter
    bool eos; // if true, the tuple is a EOS marker

    // constructor
    wrapper_tuple_t(tuple_t *_t, size_t _counter=1, bool _eos=false): tuple(_t), counter(_counter), eos(_eos) {}

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
    size_t old_cnt = (wt->counter).fetch_sub(1);
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

// function createWrapper: definition valid if T2 != T3
template<typename T1, typename T2, typename T3>
T1 *createWrapper(typename enable_if<!is_same<T2,T3>::value, T1>::type *t, size_t val, bool isEOS=false) // T1 is the tuple type, T2 the output type and T3 is the wrapper type
{
    // only return the tuple
    return t;
}

// function createWrapper: definition valid if T2 == T3
template<typename T1, typename T2, typename T3>
T2 *createWrapper(typename enable_if<is_same<T2,T3>::value, T1>::type *t, size_t val, bool isEOS=false) // T1 is the tuple type, T2 the output type and T3 is the wrapper type
{
    // create and return a wrapper to the tuple
    T2 *wt = new T2(t, val, isEOS);
    return wt;
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
