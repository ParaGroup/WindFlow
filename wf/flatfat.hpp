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
 *  @file    flatfat.hpp
 *  @author  Elia Ruggeri and Gabriele Mencagli
 *  @date    09/03/2020
 *  
 *  @brief Flat Fixed-size Aggregator Tree
 *  
 *  @section FlatFAT (Description)
 *  
 *  This file implements the Flat Fixed-size Aggregator Tree (FlatFAT) as it was described
 *  in [1]. The data structure allows executing sliding-window queries with any associative
 *  binary operator computed over all the tuples in the window. The approach permits to avoid
 *  recomputing windows from scratch. For further details see reference [1] below.
 *  
 *  [1] Kanat Tangwongsan, et al. 2015. General incremental sliding-window aggregation.
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
template<typename tuple_t, typename result_t>
class FlatFAT
{
private:
    // type of the combine function
    using winComb_func_t = std::function<void(const result_t &, const result_t &, result_t &)>;
    /// type of the rich combine function
    using rich_winComb_func_t = std::function<void(const result_t &, const result_t &, result_t &, RuntimeContext &)>;
    tuple_t tmp; // never used
    // key data type
    using key_t = typename std::remove_reference<decltype(std::get<0>(tmp.getControlFields()))>::type;
    winComb_func_t *winComb_func; // pointer to the combine function
    rich_winComb_func_t *rich_winComb_func; // pointer to the rich combine function
    std::vector<result_t> tree; // vector representing the tree as a flat array
    bool isCommutative; // flat stating whether the combine function is commutative or not
    size_t n; // number of leaves of the tree
    key_t key; // key value used by this FlatFAT
    size_t front; // index of the most recent inserted element
    size_t back; // index of the next element to be removed
    size_t root; // position of the root in the flat array
    bool isEmpty; // flag stating whether the tree is empty or not
    bool isRichCombine; // flag stating whether the combine function is riched
    RuntimeContext *context; // pointer to the RuntimeContext
    // support methods for traversing the FlatFAT
    size_t left_child(size_t pos) const { return pos << 1; }
    size_t right_child(size_t pos) const { return ( pos << 1 ) + 1; }
    size_t leaf(size_t pos) const { return n + pos - 1; }
    size_t parent(size_t pos) const { return static_cast<size_t>( floor( pos / 2.0 ) ); }

    // method to compute the i-th prefix
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
                acc = result_t(); // re-initialize the result
                uint64_t ts = std::max(std::get<2>(tree[left_child(p)].getControlFields()), std::get<2>(tmp.getControlFields()));
                acc.setControlFields(key, 0, ts);
                if (!isRichCombine) {
                    (*winComb_func)(tree[left_child(p)], tmp, acc);
                }
                else {
                    (*rich_winComb_func)(tree[left_child(p)], tmp, acc, *context);
                }
            }
            i = p;
        }
        return acc;
    }

    // method to compute the i-th suffix
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
                acc = result_t(); // re-initialize the result
                uint64_t ts = std::max(std::get<2>(tree[right_child(p)].getControlFields()), std::get<2>(tmp.getControlFields()));
                acc.setControlFields(key, 0, ts);
                if (!isRichCombine) {
                    (*winComb_func)(tmp, tree[right_child(p)], acc);
                }
                else {
                    (*rich_winComb_func)(tmp, tree[right_child(p)], acc, *context);
                }
            }
            i = p;
        }
        return acc;
    }

    // method to update a single element in the three (at position pos)
    void update(size_t pos)
    {
        size_t nextNode = parent(pos);
        /* The method traverses the tree updating each node it
           encounters until it updates the root. */
        while (nextNode != 0) {
            size_t lc = left_child(nextNode);
            size_t rc = right_child(nextNode);
            tree[nextNode] = result_t(); // re-initialize the result
            uint64_t ts = std::max(std::get<2>(tree[lc].getControlFields()), std::get<2>(tree[rc].getControlFields()));
            tree[nextNode].setControlFields(key, 0, ts);
            if (!isRichCombine) {
                (*winComb_func)(tree[lc], tree[rc], tree[nextNode]);
            }
            else {
                (*rich_winComb_func)(tree[lc], tree[rc], tree[nextNode], *context);
            }
            nextNode = parent(nextNode);
        }
    }

public:
    // Constructor I
    FlatFAT(winComb_func_t *_winComb_func,
            bool _isCommutative,
            size_t _n,
            key_t _key,
            RuntimeContext *_context):
            winComb_func(_winComb_func),
            isCommutative(_isCommutative),
            key(_key),
            root(1),
            isEmpty(true),
            isRichCombine(false),
            context(_context)
    {
        // a complete binary tree so n must be rounded to the next power of two
        int noBits = (int) ceil(log2(_n));
        n = 1 << noBits;
        front = n-1;
        back = n-1;
        tree.resize(n*2);
        // initialization of the whole FlatFAT
        for (size_t i=0; i<n*2; i++) {
            tree[i].setControlFields(key, 0, 0);
        }
    }

    // Constructor II
    FlatFAT(rich_winComb_func_t *_rich_winComb_func,
            bool _isCommutative,
            size_t _n,
            key_t _key,
            RuntimeContext *_context):
            rich_winComb_func(_rich_winComb_func),
            isCommutative(_isCommutative),
            key(_key),
            root(1),
            isEmpty(true),
            isRichCombine(true),
            context(_context)
    {
        // a complete binary tree so n must be rounded to the next power of two
        int noBits = (int) ceil(log2(_n));
        n = 1 << noBits;
        front = n-1;
        back = n-1;
        tree.resize(n*2);
        // initialization of the whole FlatFAT
        for (size_t i=0; i<n*2; i++) {
            tree[i].setControlFields(key, 0, 0);
        }
    }

    // method to add a new element to the FlatFAT
    void insert(const result_t &input)
    {
        // check if the tree is empty
        if ((front == back) && (front == n-1)) {
            front++;
            back++;
            isEmpty = false;
        }
        // check if front is the last leaf so it must wrap around
        else if (back == 2*n-1) {
            //  check if the first leaf is empty
            if (front != n) {
                back = n;
            }
            else {
                abort();
            }
        }
        // check if front < back and the tree is full
        else if (front != back+1) {
            back++;
        }
        else {
            abort();
        }
        // insert the element in the next empty position
        tree[back] = input;
        // update all the required internal nodes of the tree
        update(back);
    }

    // method to add a set of new elements to the FlatFAT
    void insert(const std::vector<result_t> &inputs)
    {
        std::list<size_t> nodesToUpdate;
        for (size_t i=0; i<inputs.size(); i++) {
            // check if the tree is empty
            if ((front == back) && (front == n-1)) {
                front++;
                back++;
                isEmpty = false;
            }
            // check if front is the last leaf so it must wrap around
            else if (back == 2*n-1) {
                // check if the first leaf is empty
                if (front != n) {
                    back = n;
                }
                else {
                    abort();
                }
            }
            // check if front < back and the tree is full
            else if (front != back+1) {
                back++;
            }
            else {
                abort();
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
            tree[nextNode] = result_t(); // re-initialize the result
            uint64_t ts = std::max(std::get<2>(tree[lc].getControlFields()), std::get<2>(tree[rc].getControlFields()));
            tree[nextNode].setControlFields(key, 0, ts);
            if (!isRichCombine) {
                (*winComb_func)(tree[lc], tree[rc], tree[nextNode]);
            }
            else {
                (*rich_winComb_func)(tree[lc], tree[rc], tree[nextNode], *context);
            }
            size_t p = parent(nextNode);
            if ((nextNode != root) && (nodesToUpdate.empty() || nodesToUpdate.back() != p)) {
                nodesToUpdate.push_back(p);
            }
        }
    }

    // method to remove the oldest result from the tree
    void remove()
    {
        /* it removes the element by inserting in its place
           a default constructed element. */
        tree[front] = result_t(); // re-initialize the result
        tree[front].setControlFields(key, 0, 0);
        // update all the required internal nodes of the tree
        update(front);
        // then the front pointer is updated. First checks if this was the last element of the tree
        if (front == back) {
            front = back = n - 1;
            isEmpty = true;
        }
        // if it must wrap around
        else if (front == 2*n-1) {
            front = n;
        }
        else {
            front++;
        }
    }

    // method to remove the oldest count results from the tree
    void remove(size_t count)
    {
        std::list<size_t> nodesToUpdate;
        for (size_t i=0; i<count; i++) {
            tree[front] = result_t(); // re-initialize the result
            tree[front].setControlFields(key, 0, 0);
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
            tree[nextNode] = result_t(); // re-initialize the result
            uint64_t ts = std::max(std::get<2>(tree[lc].getControlFields()), std::get<2>(tree[rc].getControlFields()));
            tree[nextNode].setControlFields(key, 0, ts);
            if (!isRichCombine) {
                (*winComb_func)(tree[lc], tree[rc], tree[nextNode]);
            }
            else {
                (*rich_winComb_func)(tree[lc], tree[rc], tree[nextNode], *context);
            }
            size_t p = parent(nextNode);
            if ((nextNode != root) && (nodesToUpdate.empty() || nodesToUpdate.back() != p)) {
                nodesToUpdate.push_back(p);
            }
        }
    }

    // method to get the result of the whole window
    result_t *getResult() const
    {
        result_t *res = new result_t();
        if (isCommutative || front <= back) {
            /* the elements are in the correct order so the result
               in the root is valid. */
            *res = tree[root];
        }
        else {
            /* In case winComb_func is not commutative we need to
               compute the value of the combination of the elements
               at positions [n, back], the prefix, and the ones at
               positions [front, 2*n-1], the suffix, and combine
               them accordingly. */
            result_t prefixRes = prefix(back);
            result_t suffixRes = suffix(front);
            uint64_t ts = std::max(std::get<2>(suffixRes.getControlFields()), std::get<2>(prefixRes.getControlFields()));
            res->setControlFields(key, 0, ts);
            if (!isRichCombine) {
                (*winComb_func)(suffixRes, prefixRes, *res);
            }
            else {
                (*rich_winComb_func)(suffixRes, prefixRes, *res, *context);
            }
        }
        return res;
    }

    // method to check whether the FlatFAT is empty or not
    bool is_Empty() const
    {
        return isEmpty;
    }
};

} // namespace wf

#endif
