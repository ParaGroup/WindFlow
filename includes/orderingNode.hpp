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
 *  @file    orderingNode.hpp
 *  @author  Gabriele Mencagli
 *  @date    19/08/2018
 *  @version 1.0
 *  
 *  @brief FastFlow node used for reordering data items received from multiple input streams
 *  
 *  @section DESCRIPTION
 *  
 *  The node has multiple input streams and assumes that input items are received in
 *  increasing order from each distinct input stream. The node reorders items and
 *  emits them in increasing order.
 */ 

#ifndef ORDERINGNODE_H
#define ORDERINGNODE_H

// includes
#include <deque>
#include <queue>
#include <unordered_map>
#include <ff/multinode.hpp>
#include <meta_utils.hpp>

using namespace ff;
using namespace std;

// class OrderingNode
template<typename tuple_t>
class OrderingNode: public ff_minode_t<wrapper_tuple_t<tuple_t>, wrapper_tuple_t<tuple_t>>
{
private:
    // type of the wrapper of input tuples
    using wrapper_in_t = wrapper_tuple_t<tuple_t>;
    // friendships with other classes in the library
    template<typename T1, typename T2, typename T3>
    friend class Pane_Farm;
    template<typename T1, typename T2, typename T3, typename T4>
    friend class Pane_Farm_GPU;
    template<typename T1, typename T2, typename T3>
    friend class Win_MapReduce;
    template<typename T1, typename T2, typename T3, typename T4>
    friend class Win_MapReduce_GPU;
	size_t n_inputs; // number of input streams
    // inner struct of a key descriptor
    struct Key_Descriptor
    {
    	// maxs[i] contains the greatest identifier received from the i-th input stream
    	vector<size_t> maxs;
        wrapper_in_t *eos_marker; // pointer to the wrapper to the most recent EOS marker of this key
    	// comparator functor (returns true if A comes before B in the ordering)
        struct Comparator {
            bool operator() (wrapper_in_t *wA, wrapper_in_t *wB) {
                tuple_t *A = extractTuple<tuple_t, wrapper_in_t>(wA);
                tuple_t *B = extractTuple<tuple_t, wrapper_in_t>(wB);
                auto infoA = A->getInfo();
                auto infoB = B->getInfo();
                return infoA.second > infoB.second;
            }
        };
    	// ordered queue of tuples of the given key received by the node
    	priority_queue<wrapper_in_t *, deque<wrapper_in_t *>, Comparator> queue;

        // constructor
        Key_Descriptor(size_t _n): maxs(_n, 0), eos_marker(nullptr) {}

        // destructor
        ~Key_Descriptor() {}
    };
    // hash table that maps key identifiers onto key descriptors
    unordered_map<size_t, Key_Descriptor> keyMap;
    size_t eos_rcv; // number of EOS received

	// private constructor
	OrderingNode(size_t _n): n_inputs(_n), eos_rcv(0) {}

	// destructor
	~OrderingNode() {}

    // svc_init method (utilized by the FastFlow runtime)
    int svc_init()
    {
    	return 0;
    }

    // svc method (utilized by the FastFlow runtime)
    wrapper_in_t *svc(wrapper_in_t *wr)
   	{
        // extract the key and id from the input tuple
        tuple_t *r = extractTuple<tuple_t, wrapper_in_t>(wr);
        size_t key = (r->getInfo()).first; // key
        size_t wid = (r->getInfo()).second; // identifier
        // find the corresponding key descriptor
        auto it = keyMap.find(key);
        if (it == keyMap.end()) {
            // create the descriptor of that key
            keyMap.insert(make_pair(key, Key_Descriptor(n_inputs)));
            it = keyMap.find(key);
        }
        Key_Descriptor &key_d = (*it).second;
        // update the most recent EOS marker of this key
        if (key_d.eos_marker == nullptr && wr->eos) {
            key_d.eos_marker = wr;
            return this->GO_ON;
        }
        else if (wr->eos) {
            if (wid > (((key_d.eos_marker)->tuple)->getInfo()).second)
                key_d.eos_marker = wr;
            else
                deleteTuple<tuple_t, wrapper_in_t>(wr);
            return this->GO_ON;
        }
        // get the index of the source's stream
        size_t source_id = this->get_channel_id();
        // update the parameters of the key descriptor
        key_d.maxs[source_id] = wid;
        size_t min_id = *(min_element((key_d.maxs).begin(), (key_d.maxs).end()));
        (key_d.queue).push(wr);
        // check if buffered tuples can be emitted in order
        while (!(key_d.queue).empty()) {
        	// emit all the buffered tuples with identifier lower or equal than min_i
            wrapper_in_t *wnext = (key_d.queue).top();
            tuple_t *next = extractTuple<tuple_t, wrapper_in_t>(wnext);
        	size_t id = (next->getInfo()).second;
        	if (id > min_id)
        		break;
        	else {
        		// deque the tuple
        		(key_d.queue).pop();
        		// emit the tuple
        		this->ff_send_out(wnext);
        	}
        }
        return this->GO_ON;
   	}

    // method to manage the EOS (utilized by the FastFlow runtime)
    void eosnotify(ssize_t id)
    {
        eos_rcv++;
        if (eos_rcv != n_inputs)
            return;
        // send (in order) all the queued tuples of all the keys
        for (auto &k: keyMap) {
            auto &key_d = (k.second);
            while (!(key_d.queue).empty()) {
                // extract the new tuple
                wrapper_in_t *wnext = (key_d.queue).top();
                (key_d.queue).pop();
                // emit the tuple
                this->ff_send_out(wnext);
            }
            // send the most recent EOS marker of this key
            this->ff_send_out(key_d.eos_marker);
        }
    }

    // svc_end method (utilized by the FastFlow runtime)
    void svc_end() {}
};

#endif
