/**************************************************************************************
 *  Copyright (c) 2019- Gabriele Mencagli and Elia Ruggeri
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
 *  @file    flatfat.hpp
 *  @author  Gabriele Mencagli and Elia Ruggeri
 *  
 *  @brief Flat Fixed-size Aggregator Tree
 *  
 *  @section FlatFAT (Description)
 *  
 *  This file implements the Flat Fixed-size Aggregator Tree (FlatFAT) as it was described
 *  in Kanat Tangwongsan, et al. 2015. General incremental sliding-window aggregation.
 *  Proc. VLDB Endow. 8, 7 (February 2015), 702–713.
 */ 

#ifndef FLATFAT_H
#define FLATFAT_H

// includes
#include<list>
#include<cmath>
#include<vector>
#include<utility>
#include<algorithm>
#include<functional>
#include<basic.hpp>
#include<context.hpp>

namespace wf {

// class FlatFAT
template<typename comb_func_t, typename key_t>
class FlatFAT
{
private:
    comb_func_t *comb_func; // pointer to the combine function
    using result_t = decltype(get_tuple_t_Comb(*comb_func)); // extracting the result_t type and checking the admissible signatures
    // static predicates to check the type of the functional logic to be invoked
    static constexpr bool isNonRiched = std::is_invocable<decltype(*comb_func), const result_t &, const result_t &, result_t &>::value;
    static constexpr bool isRiched = std::is_invocable<decltype(*comb_func), const result_t &, const result_t &, result_t &, RuntimeContext &>::value;
    std::vector<result_t> tree; // vector representing the tree as a flat array
    bool isCommutative; // flat stating whether the combine function is commutative or not
    size_t n; // number of leaves of the tree
    size_t front; // index of the most recent inserted element
    size_t back; // index of the next element to be removed
    size_t root; // position of the root in the flat array
    bool isEmpty; // flag stating whether the tree is empty or not
    RuntimeContext *context; // pointer to the RuntimeContext
    key_t key; // key attribute used by this FlatFAT

    // Get the index of the left child of pos
    size_t left_child(size_t pos) const { return pos << 1; }

    // Get the index of the right child of pos
    size_t right_child(size_t pos) const { return ( pos << 1 ) + 1; }

    // Get the index of the leaf of pos 
    size_t leaf(size_t pos) const { return n + pos - 1; }

    // Get the parent of pos
    size_t parent(size_t pos) const { return static_cast<size_t>( floor( pos / 2.0 ) ); }

    // Compute the i-th prefix
    result_t prefix(size_t pos) const
    {
        size_t i = pos;
        result_t acc = tree[pos];
        while (i != root) {
            size_t p = parent(i);
            /* if i is the right child of p then both its left child
               and right child are in the prefix. Otherwise only the
               left child is so we pass acc unmodified. */
            if (i == right_child(p)) {
                result_t tmp = acc;
                acc = create_win_result_t<decltype(get_tuple_t_Comb(*comb_func)), key_t>(key);
                if constexpr (isNonRiched) {
                    (*comb_func)(tree[left_child(p)], tmp, acc);
                }
                if constexpr (isRiched) {
                    (*comb_func)(tree[left_child(p)], tmp, acc, *context);
                }
            }
            i = p;
        }
        return acc;
    }

    // Compute the i-th suffix
    result_t suffix(size_t pos) const
    {
        size_t i = pos;
        result_t acc = tree[pos];
        while (i != root) {
            /* if i is the left child of p then both its left child
               and right child are in the suffix. Otherwise only the
               right child is so we pass acc unmodified. */
            size_t p = parent(i);
            if (i == left_child(p)) {
                result_t tmp = acc;
                acc = create_win_result_t<decltype(get_tuple_t_Comb(*comb_func)), key_t>(key);
                if constexpr (isNonRiched) {
                    (*comb_func)(tmp, tree[right_child(p)], acc);
                }
                if constexpr (isRiched) {
                    (*comb_func)(tmp, tree[right_child(p)], acc, *context);
                }
            }
            i = p;
        }
        return acc;
    }

    // Update a single element in the three (at position pos)
    void update(size_t pos)
    {
        size_t nextNode = parent(pos);
        /* The method traverses the tree updating each node it
           encounters until it updates the root. */
        while (nextNode != 0) {
            size_t lc = left_child(nextNode);
            size_t rc = right_child(nextNode);
            tree[nextNode] = create_win_result_t<decltype(get_tuple_t_Comb(*comb_func)), key_t>(key); // empty result with the right key
            if constexpr (isNonRiched) {
                (*comb_func)(tree[lc], tree[rc], tree[nextNode]);
            }
            if constexpr (isRiched) {
                (*comb_func)(tree[lc], tree[rc], tree[nextNode], *context);
            }
            nextNode = parent(nextNode);
        }
    }

public:
    // Constructor
    FlatFAT(comb_func_t *_comb_func,
            key_t _key,
            bool _isCommutative,
            size_t _n,
            RuntimeContext *_context):
            comb_func(_comb_func),
            key(_key),
            isCommutative(_isCommutative),
            root(1),
            isEmpty(true),
            context(_context)
    {
        int noBits = (int) ceil(log2(_n)); // a complete binary tree so n must be rounded to the next power of two
        n = 1 << noBits;
        front = n-1;
        back = n-1;
        tree.resize(n*2, create_win_result_t<decltype(get_tuple_t_Comb(*comb_func)), key_t>(key)); // all the elements are empty with the right key
    }

    // Add a new element to the FlatFAT
    void insert(const result_t &input)
    {
        if ((front == back) && (front == n-1)) { // check if the tree is empty
            front++;
            back++;
            isEmpty = false;
        }
        else if (back == 2*n-1) { // check if front is the last leaf so it must wrap around
            if (front != n) { // check if the first leaf is empty
                back = n;
            }
            else {
                abort(); // <-- this case is not possible!
            }
        }
        else if (front != back+1) { // check if front < back and the tree is full
            back++;
        }
        else {
            abort(); // <-- this case is not possible!
        }
        tree[back] = input; // insert the element in the next empty position
        update(back); // update all the required internal nodes of the tree
    }

    // Add a set of new elements to the FlatFAT
    void insert(const std::vector<result_t> &inputs)
    {
        std::list<size_t> nodesToUpdate;
        for (size_t i=0; i<inputs.size(); i++) {
            if ((front == back) && (front == n-1)) { // check if the tree is empty
                front++;
                back++;
                isEmpty = false;
            }
            else if (back == 2*n-1) { // check if front is the last leaf so it must wrap around
                if (front != n) { // check if the first leaf is empty
                    back = n;
                }
                else {
                    abort(); // <-- this case is not possible!
                }
            }
            else if (front != back+1) { // check if front < back and the tree is full
                back++;
            }
            else {
                abort(); // <-- this case is not possible!
            }
            tree[back] = inputs[i];
            size_t p = parent(back);
            if ((back != root) && (nodesToUpdate.empty() || nodesToUpdate.back() != p)) {
                nodesToUpdate.push_back(p);
            }
        }
        while (!nodesToUpdate.empty()) {
            size_t nextNode = nodesToUpdate.front();
            nodesToUpdate.pop_front();
            size_t lc = left_child(nextNode);
            size_t rc = right_child(nextNode);
            tree[nextNode] = create_win_result_t<decltype(get_tuple_t_Comb(*comb_func)), key_t>(key);; // empty result with the right key
            if constexpr (isNonRiched) {
                (*comb_func)(tree[lc], tree[rc], tree[nextNode]);
            }
            if constexpr (isRiched) {
                (*comb_func)(tree[lc], tree[rc], tree[nextNode], *context);
            }
            size_t p = parent(nextNode);
            if ((nextNode != root) && (nodesToUpdate.empty() || nodesToUpdate.back() != p)) {
                nodesToUpdate.push_back(p);
            }
        }
    }

    // Remove the oldest result from the tree
    void remove()
    {
        /* it removes the element by inserting in its place
           a default constructed element. */
        tree[front] = create_win_result_t<decltype(get_tuple_t_Comb(*comb_func)), key_t>(key); // empty result with the right key
        update(front); // update all the required internal nodes of the tree
        if (front == back) {
            front = back = n - 1;
            isEmpty = true;
        }
        else if (front == 2*n-1) { // if it must wrap around
            front = n;
        }
        else {
            front++;
        }
    }

    // Remove the oldest count results from the tree
    void remove(size_t count)
    {
        std::list<size_t> nodesToUpdate;
        for (size_t i=0; i<count; i++) {
            tree[front] = create_win_result_t<decltype(get_tuple_t_Comb(*comb_func)), key_t>(key);; // empty result with the right key
            size_t p = parent(front);
            if ((front != root) && (nodesToUpdate.empty() || nodesToUpdate.back() != p)) {
                nodesToUpdate.push_back(p);
            }
            if (front == back) {
                front = back = n-1;
                isEmpty = true;
                break;
            }
            else if (front == 2*n-1) {
                front = n;
            }
            else {
                front++;
            }
        }
        while (!nodesToUpdate.empty()) {
            size_t nextNode = nodesToUpdate.front();
            nodesToUpdate.pop_front();
            size_t lc = left_child(nextNode);
            size_t rc = right_child(nextNode);
            tree[nextNode] = create_win_result_t<decltype(get_tuple_t_Comb(*comb_func)), key_t>(key); // empty result with the right key
            if constexpr (isNonRiched) {
                (*comb_func)(tree[lc], tree[rc], tree[nextNode]);
            }
            if constexpr (isRiched) {
                (*comb_func)(tree[lc], tree[rc], tree[nextNode], *context);
            }
            size_t p = parent(nextNode);
            if ((nextNode != root) && (nodesToUpdate.empty() || nodesToUpdate.back() != p)) {
                nodesToUpdate.push_back(p);
            }
        }
    }

    // Get the result of the whole window
    result_t getResult(uint64_t _gwid) const
    {
        result_t res = create_win_result_t<decltype(get_tuple_t_Comb(*comb_func)), key_t>(key, _gwid); // empty result with the right key
        if (isCommutative || front <= back) {
            /* the elements are in the correct order so the result
               in the root is valid. */
            // res = tree[root];
            if constexpr (isNonRiched) {
                (*comb_func)(tree[root], res, res);
            }
            if constexpr (isRiched) {
                (*comb_func)(tree[root], res, res, *context);
            }
        }
        else {
            /* In case comb_func is not commutative we need to
               compute the value of the combination of the elements
               at positions [n, back], the prefix, and the ones at
               positions [front, 2*n-1], the suffix, and combine
               them accordingly. */
            result_t prefixRes = prefix(back);
            result_t suffixRes = suffix(front);
            if constexpr (isNonRiched) {
                (*comb_func)(suffixRes, prefixRes, res);
            }
            if constexpr (isRiched) {
                (*comb_func)(suffixRes, prefixRes, res, *context);
            }
        }
        return res;
    }

    // Check whether the FlatFAT is empty or not
    bool is_Empty() const
    {
        return isEmpty;
    }
};

} // namespace wf

#endif
