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

/*  
 *  This file implements the Skyline sequential algorithm called BSkyTree and
 *  published in the paper "Scalable skyline computation using a balanced pivot
 *  selection technique." Information Systems 39: 1--21, 2014. The implementation
 *  is ispired by the code available in GitHub (SkyBench project).
 */ 

#ifndef SKYTREE_H
#define SKYTREE_H

// includes
#include <map>
#include <stack>
#include <basic.hpp>

// defines
#define DOM_LEFT   0
#define DOM_RIGHT  1
#define DOM_INCOMP 2

using namespace std;

// useful shifts
static const uint32_t SHIFTS[] = { 1 << 0, 1 << 1, 1 << 2, 1 << 3, 1 << 4, 1
    << 5, 1 << 6, 1 << 7, 1 << 8, 1 << 9, 1 << 10, 1 << 11, 1 << 12, 1 << 13, 1
    << 14, 1 << 15, 1 << 16, 1 << 17, 1 << 18, 1 << 19, 1 << 20, 1 << 21, 1
    << 22, 1 << 23, 1 << 24, 1 << 25, 1 << 26, 1 << 27, 1 << 28, 1 << 29, 1
<< 30 };

// Node struct
struct Node
{
    uint32_t lattice;
	tuple_t point;
	vector<Node> children;

	// constructor
	Node(uint32_t _lattice=0): lattice(_lattice) {}

    // destructor
    ~Node() {}
};

// class implementing the Skyline
class Skyline
{
private:
	size_t key; // not used
    uint64_t wid; // unique identifier (starting from zero)
    uint64_t ts;
    vector<tuple_t> set; // set of tuples in the skyline
    using const_iterator_t = vector<tuple_t>::const_iterator;

public:
    // constructor
    Skyline(): key(0), wid(0), ts(0) {}

    // destructor
    ~Skyline() {}

    // getInfo method
    tuple<size_t, uint64_t, uint64_t> getInfo() const
    {
        return tuple<size_t, uint64_t, uint64_t>(key, wid, ts);
    }

    // setInfo method
    void setInfo(size_t _key, uint64_t _wid, uint64_t _ts)
    {
        key = _key;
        wid = _wid;
        ts = _ts;
    }

    // get the size of the skyline
    size_t size() const
    {
        return set.size();
    }

    // method to get a copy of the skyline content
    vector<tuple_t> getContent() const
    {
        return set;
    }

    // method to initialize the skyline content by moving an existing vector of tuples
    void setContent(vector<tuple_t> &&_set)
    {
        set = move(_set);
    }

    // method operator[] to access an element in the skyline
    const tuple_t &operator[](size_t i) const
    {
        assert(i<set.size());
        return set[i];
    }

    // method to return a const iterator to the first tuple in the skyline
    const_iterator_t begin() const
    {
        return set.cbegin();
    }

    // method to return a const iterator to the end of the skyline
    const_iterator_t end() const
    {
        return set.cend();
    }
};

// function PushStack
void PushStack(stack<Node> &tree_stack, Node &skytree)
{
    if (skytree.children.size() > 0) {
        tree_stack.push(skytree);
        const uint32_t num_child = skytree.children.size();
        for (unsigned i=0; i<num_child; i++)
            PushStack(tree_stack, skytree.children[i]);
    }
}

// function to clear the SkyTree
void ClearSkyTree(Node &skytree)
{
    stack<Node> tree_stack;
    PushStack(tree_stack, skytree);
    while (!tree_stack.empty()) {
        tree_stack.top().children.clear();
        tree_stack.pop();
    }
}

// function CountSkyTree
uint32_t CountSkyTree(Node &skytree)
{
    uint32_t count = 0;
    uint32_t num_child = skytree.children.size();
    for (uint32_t c=0; c<num_child; c++)
        count += CountSkyTree(skytree.children[c]);
    return count + 1;
}

// function to test the dominance between two tuples
inline int DominanceTest(const tuple_t &t1, const tuple_t &t2)
{
    bool t1_better = false, t2_better = false;
    for (uint32_t i=0; i<DIM; i++) {
        if (t1.elems[i] < t2.elems[i])
            t1_better = true;
        else if (t1.elems[i] > t2.elems[i])
            t2_better = true;
        if (t1_better && t2_better)
            return DOM_INCOMP;
    }
    if (!t1_better && t2_better)
        return DOM_RIGHT;
    if (!t2_better && t1_better)
        return DOM_LEFT;
  return DOM_INCOMP;
}

// function returning true if the left tuple is dominated by the right one
inline bool DominatedLeft(const tuple_t &t1, const tuple_t &t2)
{
    for (uint32_t d = 0; d<DIM; d++) {
        if (t1.elems[d] < t2.elems[d])
            return false;
    }
    return true;
}

// function for the equality test between two tuples
inline bool EqualityTest(const tuple_t &t1, const tuple_t &t2)
{
    bool eq = true;
    for (uint32_t d=0; d<DIM; d++) {
        if (t1.elems[d] != t2.elems[d]) {
            eq = false;
            break;
        }
    }
    return eq;
}

// function DT_bitmap_dvc_NOAVX
inline uint32_t DT_bitmap_dvc(const tuple_t &cur_value, const tuple_t &sky_value)
{
    uint32_t lattice = 0;
    for (uint32_t dim=0; dim<DIM; dim++)
        if (sky_value.elems[dim] <= cur_value.elems[dim])
            lattice |= SHIFTS[dim];
    return lattice;
}

// class PivotSelection
class PivotSelection {
private:
    const vector<float> &min_list_;
    const vector<float> &max_list_;

    // method to set the range list
    vector<float> SetRangeList(const vector<float> &min_list, const vector<float> &max_list)
    {
        vector<float> range_list(DIM, 0);
        for (uint32_t d=0; d<DIM; d++)
            range_list[d] = max_list[d] - min_list[d];
        return range_list;
    }

    // method to compute the distance
    float ComputeDistance(const float *value, const vector<float> &min_list, const vector<float> &range_list)
    {
        float max_d, min_d;
        max_d = min_d = (value[0] - min_list[0]) / range_list[0];
        for (uint32_t d=1; d<DIM; d++) {
            float norm_value = (value[d] - min_list[d]) / range_list[d];
            if (min_d > norm_value)
                min_d = norm_value;
            else if (max_d < norm_value)
                max_d = norm_value;
        }
        return max_d - min_d;
    }

    // method to evaluate a point
    bool EvaluatePoint(const unsigned pos, vector<tuple_t> &dataset)
    {
        const tuple_t &cur_tuple = dataset[pos];
        for (uint32_t i=0; i<pos; ++i) {
            const tuple_t &prev_value = dataset[i];
            if (DominatedLeft(cur_tuple, prev_value))
                return false;
        }
        return true;
    }

public:
    // constructor
    PivotSelection(const vector<float> &min_list, const vector<float> &max_list): min_list_(min_list), max_list_(max_list) {}

    // destructor
    ~PivotSelection(void) {}

    // method Execute
    void Execute(vector<tuple_t> &dataset)
    {
        const uint32_t head = 0;
        uint32_t tail = dataset.size() - 1, cur_pos = 1;
        float *hvalue = dataset[head].elems;
        vector<float> range_list = SetRangeList(min_list_, max_list_);
        float min_dist = ComputeDistance(hvalue, min_list_, range_list);
        while (cur_pos <= tail) {
            float *cvalue = dataset[cur_pos].elems;
            const uint32_t dtest = DominanceTest(dataset[head], dataset[cur_pos]);
            if (dtest == DOM_LEFT) {
                dataset[cur_pos] = dataset[tail];
                dataset.pop_back();
                tail--;
            }
            else if (dtest == DOM_RIGHT) {
                dataset[head] = dataset[cur_pos];
                dataset[cur_pos] = dataset[tail];
                dataset.pop_back();
                tail--;
                hvalue = dataset[head].elems;
                min_dist = ComputeDistance( hvalue, min_list_, range_list );
                cur_pos = 1; // THIS IS THE SAME BUG AS IN QSkyCube: cur_pos is not reseted
            }
            else {
                assert(dtest == DOM_INCOMP);
                float cur_dist = ComputeDistance(cvalue, min_list_, range_list);
                if (cur_dist < min_dist) {
                    if (EvaluatePoint( cur_pos, dataset)) {
                        swap(dataset[head], dataset[cur_pos]);
                        hvalue = dataset[head].elems;
                        min_dist = cur_dist;
                        cur_pos++;
                    }
                    else {
                        dataset[cur_pos] = dataset[tail];
                        dataset.pop_back();
                        tail--;
                    }
                }
                else
                    cur_pos++;
            }
        }
    }
};

// class SkyTree
class SkyTree
{
private:
    const uint32_t n_;
    const uint32_t d_;
    vector<tuple_t> data_;
    vector<float> min_list_;
    vector<float> max_list_;
    Node skytree_;
    vector<int> skyline_;
    vector<int> eqm_; // equivalence matrix
    // runtime parameters
    bool useTree_; // using SkyTree data structure in FilterPoints()
    bool useDnC_; // divide-and-conquer
    bool *dominated_; // for DnC variant
#ifndef NVERBOSE
  	map<int, int> skytree_levels_;
#endif

public:
    // constructor
    SkyTree(const uint32_t n, const uint32_t d, vector<tuple_t> data, const bool useTree, const bool useDnC):
            n_(n), d_(d), data_(data), useTree_(useTree), useDnC_(useDnC)
    {
        skytree_.lattice = 0;
		skyline_.reserve(1024);
		eqm_.reserve(1024);
  		dominated_ = NULL;
    }

    // destructor
    ~SkyTree()
    {
        min_list_.clear();
  		max_list_.clear();
  		skyline_.clear();
        ClearSkyTree(skytree_);
  		data_.clear();
  		if (useDnC_)
            delete[] dominated_;
    }

    // initialization method
    void Init()
    {
        if (useDnC_) {
    		const uint32_t n = n_;
    		if (n > 0) {
      		    dominated_ = new bool[n];
      			for (uint32_t i=0; i<n; i++)
                    dominated_[i] = false;
            }
            else
                useDnC_ = false; // so we don't try to delete[] dominated
		}
    }

	// Execute method
	vector<int> Execute()
	{
		const vector<float> min_list(DIM, 0.0);
  		const vector<float> max_list(DIM, 1.0);
		ComputeSkyTree(min_list, max_list, data_, skytree_);
  		TraverseSkyTree(skytree_);
#ifndef NVERBOSE
  		if (useDnC_) {
    		const uint32_t skytree_size = CountSkyTree(skytree_);
    		printf("Skytree size=%u, Skyline size=%lu\n", skytree_size,skyline_.size());
  		}
#endif
  		// add missing points from "equivalence matrix"
  		skyline_.insert(skyline_.end(), eqm_.begin(), eqm_.end());
		return skyline_;
	}

private:
	// method to compute the Skyline
	void ComputeSkyTree(const vector<float> min_list, const vector<float> max_list, vector<tuple_t> &dataset, Node &skytree)
	{
		// pivot selection in the dataset
  		PivotSelection selection(min_list, max_list);
  		selection.Execute(dataset);
  		// mapping points to binary vectors representing subregions
  		skytree.point = dataset[0];
  		map<uint32_t, vector<tuple_t>> point_map = MapPointToRegion(dataset);
  		for (map<uint32_t, vector<tuple_t>>::const_iterator it = point_map.begin(); it != point_map.end(); it++) {
    		uint32_t cur_lattice = (*it).first;
    		vector<tuple_t> cur_dataset = (*it).second;
    		if (!useDnC_ && skytree.children.size() > 0)
      			PartialDominance(cur_lattice, cur_dataset, skytree); // checking partial dominance relations
    		if (cur_dataset.size() > 0) {
      			vector<float> min_list2(DIM), max_list2(DIM);
      			for (uint32_t d=0; d<DIM; d++) {
        			const uint32_t bit = SHIFTS[d];
        			if ((cur_lattice & bit) == bit)
          				min_list2[d] = dataset[0].elems[d], max_list2[d] = max_list[d];
        			else
          				min_list2[d] = min_list[d], max_list2[d] = dataset[0].elems[d];
      			}
      			Node child_node(cur_lattice);
      			ComputeSkyTree(min_list2, max_list2, cur_dataset, child_node); // recursive call
      			if (useDnC_ && skytree.children.size() > 0)
        			PartialDominance_with_trees(cur_lattice, skytree, child_node); // pdom
      			skytree.children.push_back(child_node);
    		}
  		}
		//  delete point_map;
	}

	// method to map points to region
	map<uint32_t,vector<tuple_t>> MapPointToRegion(vector<tuple_t> &dataset)
	{
		const uint32_t pruned = SHIFTS[DIM] - 1;
		map<uint32_t, vector<tuple_t>> data_map;
		const tuple_t &pivot = dataset[0];
  		for (vector<tuple_t>::const_iterator it = dataset.begin() + 1; it != dataset.end(); it++) {
  			if (EqualityTest(pivot, *it)) {
      			eqm_.push_back( it->lid );
      			continue;
    		}
	    	const uint32_t lattice = DT_bitmap_dvc(*it, pivot);
    		if (lattice < pruned) { // <-- Same fix as below (same if condition). Also doubles dt's.
      			//assert(!DominateLeft(pivot, *it));
      			data_map[lattice].push_back( *it );
    		}
  		}
		return data_map;
	}

	// method PartialDominance
  	void PartialDominance(const uint32_t lattice, vector<tuple_t> &dataset, Node &skytree)
	{
		const uint32_t num_child = skytree.children.size();
  		for (uint32_t c=0; c<num_child; c++) {
    		uint32_t cur_lattice = skytree.children[c].lattice;
   			if (cur_lattice <= lattice) {
      			if ((cur_lattice & lattice) == cur_lattice) {
        			// for each point, check whether the point is dominated by the existing skyline points
        			vector<tuple_t>::iterator it = dataset.begin();
       				while (it != dataset.end()) {
          				if (useTree_) {
            				if (FilterPoint( *it, skytree.children[c])) {
              					*it = dataset.back();
              					dataset.pop_back();
            				}
                            else
              					++it;
          				}
                        else {
            				if (FilterPoint_without_skytree( *it, skytree.children[c])) {
              					*it = dataset.back();
              					dataset.pop_back();
            				}
                            else
              					++it;
          				}
        			}
        			if (dataset.empty())
          				break;
      			}
    		}
            else
      			break;
  		}
	}

	// method partial dominance with the use of trees
	bool PartialDominance_with_trees(const uint32_t lattice, Node &left_tree, Node &right_tree)
	{
  		uint32_t num_child = right_tree.children.size();
  		for (uint32_t c=0; c<num_child; ++c) {
    		if (PartialDominance_with_trees(lattice, left_tree, right_tree.children[c])) {
      			if (right_tree.children[c].children.size() == 0 ) {
        			right_tree.children.erase(right_tree.children.begin() + c-- );
        			--num_child;
      			}
    		}
  		}
  		num_child = left_tree.children.size();
  		for (uint32_t c=0; c<num_child; c++) {
    		uint32_t cur_lattice = left_tree.children[c].lattice;
    		if (cur_lattice <= lattice) {
      			if ((cur_lattice & lattice) == cur_lattice) {
        			if (useTree_) {
          				if (FilterPoint( right_tree.point, left_tree.children[c])) {
            				dominated_[right_tree.point.lid] = true;
            				return true;
          				}
        			}
                    else {
          				if (FilterPoint_without_skytree( right_tree.point, left_tree.children[c])) {
            				dominated_[right_tree.point.lid] = true;
            				return true;
          				}
        			}
      			}
    		}
            else
      			break;
  		}
  		return false;
	}

	// method FilterPoint
	bool FilterPoint(const tuple_t &cur_value, Node &skytree)
	{
		const uint32_t lattice = DT_bitmap_dvc(cur_value, skytree.point);
  		const uint32_t pruned = SHIFTS[DIM] - 1;
		if (lattice < pruned) {
    		//assert(!DominateLeft(skytree.point, cur_value));
    		if (skytree.children.size() > 0) {
      			const uint32_t num_child = skytree.children.size();
      			for (uint32_t c=0; c<num_child; c++) {
        			uint32_t cur_lattice = skytree.children[c].lattice;
        			if (cur_lattice <= lattice) {
          				if ((cur_lattice & lattice) == cur_lattice) {
            				if (FilterPoint( cur_value, skytree.children[c]))
              					return true;
          				}
        			}
                    else
          				break;
      			}
    		}
    		//assert(!DominateLeft(skytree.point, cur_value));
    		return false;
  		}
  		//assert(DominateLeft(skytree.point, cur_value));
  		return true;
	}

    // method FilterPoint without SkyTree
    bool FilterPoint_without_skytree(const tuple_t &cur_value, Node &skytree)
    {
        const uint32_t lattice = DT_bitmap_dvc(cur_value, skytree.point);
        const uint32_t pruned = SHIFTS[DIM] - 1;
        if (lattice < pruned) {
            //assert(!DominateLeft(skytree.point, cur_value));
            if (skytree.children.size() > 0) {
                const uint32_t num_child = skytree.children.size();
                for (uint32_t c = 0; c<num_child; c++) {
                    if (FilterPoint( cur_value, skytree.children[c]))
                        return true;
                }
            }
            //assert(!DominateLeft(skytree.point, cur_value));
            return false;
        }
        //assert( DominateLeft(skytree.point, cur_value));
        return true;
    }

    // method to traverse the SkyTree
	void TraverseSkyTree(const Node &skytree)
    {
        if (!useDnC_ || !dominated_[skytree.point.lid])
            skyline_.push_back(skytree.point.lid);
        uint32_t num_child = skytree.children.size();
        for (uint32_t c=0; c<num_child; c++)
            TraverseSkyTree(skytree.children[c]);
    }

#ifndef NVERBOSE
    // method to compute the max depth
	int MaxDepth(const Node &skytree, int d)
    {
        skytree_levels_[d]++;
        if (skytree.children.size() == 0)
            return 1;
        else {
            int depth = MaxDepth(skytree.children[0], d + 1);
            for (uint32_t c=1; c<skytree.children.size(); ++c) {
                int h = MaxDepth(skytree.children[c], d + 1);
                if (h > depth)
                    depth = h;
            }
            return depth + 1;
        }
    }
#endif
};

// user-defined function to compute the skyline of a set of tuples
size_t SkyLineFunction(size_t key, size_t wid, Iterable<tuple_t> &win, Skyline &skyline) {
    // prepare the input set of tuples
    size_t i=0;
    vector<tuple_t> inputSet;
    inputSet.reserve(win.size());
    for (auto &_t: win) {
        tuple_t t(_t);
        t.lid = i++;
        inputSet.push_back(t);
    }
    // compute the skyline
    SkyTree skytree(inputSet.size(), DIM, inputSet, true, false);
    skytree.Init();
    vector<int> skyIdx = skytree.Execute();
    // prepare the output set of skyline tuples
    vector<tuple_t> outputSet;
    for (auto &_t: inputSet) {
        if (find(skyIdx.begin(), skyIdx.end(), _t.lid) != skyIdx.end())
            outputSet.push_back(_t);
    }
    skyline.setContent(move(outputSet));
    return 0;
};

// user-defined function to merge different skylines non-incrementally
size_t SkyLineMergeNIC(size_t key, size_t wid, Iterable<Skyline> &win, Skyline &skyline) {
    // prepare the input set of tuples
    size_t i=0;
    vector<tuple_t> inputSet;
    inputSet.reserve(win[0].size() * win.size());
    for (auto &s: win) {
        for (auto &_t: s) {
            tuple_t t(_t);
            t.lid = i++;
            inputSet.push_back(t);
        }
    }
    // compute the merged skyline
    SkyTree skytree(inputSet.size(), DIM, inputSet, true, false);
    skytree.Init();
    vector<int> skyIdx = skytree.Execute();
    // prepare the output set of skyline tuples
    vector<tuple_t> outputSet;
    for (auto &_t: inputSet) {
        if (find(skyIdx.begin(), skyIdx.end(), _t.lid) != skyIdx.end())
            outputSet.push_back(_t);
    }
    skyline.setContent(move(outputSet));
    return 0;
};

// user-defined function to merge different skylines incrementally
size_t SkyLineMergeINC(size_t key, size_t wid, const Skyline &newS, Skyline &skyline) {
    // prepare the input set of tuples
    size_t i=0;
    vector<tuple_t> inputSet;
    inputSet.reserve(newS.size() + skyline.size());
    for (auto &_t: newS) {
        tuple_t t(_t);
        t.lid = i++;
        inputSet.push_back(t);
    }
    for (auto &_t: skyline) {
        tuple_t t(_t);
        t.lid = i++;
        inputSet.push_back(t);
    }
    // compute the merged skyline
    SkyTree skytree(inputSet.size(), DIM, inputSet, true, false);
    skytree.Init();
    vector<int> skyIdx = skytree.Execute();
    // prepare the output set of skyline tuples
    vector<tuple_t> outputSet;
    for (auto &_t: inputSet) {
        if (find(skyIdx.begin(), skyIdx.end(), _t.lid) != skyIdx.end())
            outputSet.push_back(_t);
    }
    skyline.setContent(move(outputSet));
    return 0;
};

#endif
