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
 *  @file    pipegraph.hpp
 *  @author  Gabriele Mencagli
 *  @date    18/10/2019
 *
 *  @brief PipeGraph and MultiPipe constructs
 *
 *  @section PipeGraph and MultiPipe (Description)
 *
 *  This file implements the PipeGraph and the MultiPipe constructs used to build
 *  a parallel streaming application in WindFlow. The MultiPipe construct allows
 *  building a set of parallel pipelines of operators that might have shuffle
 *  connections jumping from one pipeline to another one. The PipeGraph is the
 *  "streaming environment" to be used for obtaining MultiPipe instances with
 *  different sources. To run the application the users have to run the PipeGraph
 *  object.
 */

#ifndef PIPEGRAPH_H
#define PIPEGRAPH_H

/// includes
#include <map>
#include <string>
#include <vector>
#include <random>
#include <typeinfo>
#include <algorithm>
#include <math.h>
#include <ff/ff.hpp>
#include <basic.hpp>
#include <wm_nodes.hpp>
#include <wf_nodes.hpp>
#include <kf_nodes.hpp>
#include <tree_emitter.hpp>
#include <basic_emitter.hpp>
#include <ordering_node.hpp>
#include <standard_nodes.hpp>
#include <transformations.hpp>
#include <splitting_emitter.hpp>
#include <broadcast_emitter.hpp>

namespace wf {

//@cond DOXY_IGNORE

// AppNode struct (node of the Application Tree)
struct AppNode
{
	MultiPipe *mp;
    AppNode *parent;
	std::vector<AppNode *> children;

	// Constructor
	AppNode(MultiPipe *_mp=nullptr,
            AppNode *_parent=nullptr):
            mp(_mp),
            parent(_parent)
    {}
};

// class selfkiller_node (placeholder in the second set of matrioskas)
class selfkiller_node: public ff::ff_minode
{
    // svc_init (utilized by the FastFlow runtime)
    int svc_init()
    {
        skipfirstpop(true);
        return 0;
    }

    // svc method (utilized by the FastFlow runtime)
    void *svc(void *)
    {
        return this->EOS; // terminates immediately!
    }
};

//@endcond

/**
 *  \class PipeGraph
 *
 *  \brief PipeGraph construct to build a streaming application in WindFlow
 *
 *  This class implements the PipeGraph construct used to build a WindFlow
 *  streaming application.
 */
class PipeGraph
{
private:
	std::string name; // name of the PipeGraph
	AppNode *root; // pointer to the root of the Application Tree
	std::vector<MultiPipe *> toBeDeteled; // vector of MultiPipe instances to be deleted
    // friendship with the MultiPipe class
    friend class MultiPipe;

	// method to find the AppNode containing the MultiPipe _mp in the tree rooted at _node
	AppNode *find_AppNode(AppNode *_node, MultiPipe *_mp);

    // method to find the list of AppNode instances that are leaves of the tree rooted at _node
    std::vector<AppNode *> get_LeavesList(AppNode *_node);

    // method to find the LCA of a set of _leaves starting from _node
    AppNode *get_LCA(AppNode *_node, std::vector<AppNode *> _leaves);

	// method to delete all the AppNode instances in the tree rooted at _node
	void delete_AppNodes(AppNode *_node);

    // method to prepare the right list of AppNode instances to be merged (case merge-ind and merge-full)
    bool get_MergedNodes1(std::vector<MultiPipe *> _toBeMerged, std::vector<AppNode *> &_rightList);

    // method to prepare the right list of AppNode instances to be merged (case merge-partial)
    AppNode *get_MergedNodes2(std::vector<MultiPipe *> _toBeMerged, std::vector<AppNode *> &_rightList);

	// method to execute the split of the MultiPipe _mp
	std::vector<MultiPipe *> execute_Split(MultiPipe *_mp);

	// method to execute the merge of a set of MultiPipe instances _toBeMerged
	MultiPipe *execute_Merge(std::vector<MultiPipe *> _toBeMerged);

public:
    /**
     *  \brief Constructor
     *
     *  \param _name name of the PipeGraph
     */
	PipeGraph(std::string _name):
              name(_name),
              root(new AppNode())
    {}

	/// Destructor
	~PipeGraph();

	/**
     *  \brief Add a Source to the PipeGraph
     *  \param _source Source operator to be added
     *  \return reference to a MultiPipe object to be filled with operators fed by this Source
     */
    template<typename tuple_t>
    MultiPipe &add_source(Source<tuple_t> &_source);

	/**
     *  \brief Run the PipeGraph
     *  \return zero in case of success, non-zero otherwise
     */
    int run();

    /**
     *  \brief Return the number of threads used to run this PipeGraph
     *  \return number of threads
     */
    size_t getNumThreads() const;

    /// deleted constructors/operators
    PipeGraph(const PipeGraph &) = delete; // copy constructor
    PipeGraph(PipeGraph &&) = delete; // move constructor
    PipeGraph &operator=(const PipeGraph &) = delete; // copy assignment operator
    PipeGraph &operator=(PipeGraph &&) = delete; // move assignment operator
};

/**
 *  \class MultiPipe
 *
 *  \brief MultiPipe construct
 *
 *  This class implements the MultiPipe construct used to build a set of pipelines
 *  of operators that might have shuffle connections jumping from a pipeline
 *  to another one.
 */
class MultiPipe: public ff::ff_pipeline
{
private:
    // enumeration of the routing types
    enum routing_types_t { SIMPLE, COMPLEX };
    PipeGraph *graph; // PipeGraph creating this MultiPipe
	bool has_source; // true if the MultiPipe starts with a Source
	bool has_sink; // true if the MultiPipe ends with a Sink
	ff::ff_a2a *last; // pointer to the last matrioska
    ff::ff_a2a *secondToLast; // pointer to the second-to-last matrioska
    bool isMerged; // true if the MultiPipe has been merged with other MultiPipe instances
    bool isSplit; // true if the MultiPipe has been split into other MultiPipe instances
    bool fromSplitting; // true if the MultiPipe originates from a splitting of another MultiPipe
    size_t splittingBranches; // number of splitting branches (meaningful if isSplit is true)
    MultiPipe *splittingParent = nullptr; // pointer to the parent MultiPipe (meaningful if fromSplitting is true)
    Basic_Emitter *splittingEmitterRoot = nullptr; // splitting emitter (meaningful if isSplit is true)
    std::vector<Basic_Emitter *> splittingEmitterLeaves; // vector of emitters (meaningful if isSplit is true)
    std::vector<MultiPipe *> splittingChildren; // vector of children MultiPipe instances (meaningful if isSplit is true)
    bool forceShuffling; // true if the next operator that will be added to the MultiPipe is forced to generate a shuffling
    size_t lastParallelism; // parallelism of the last operator added to the MultiPipe (0 if not defined)
    std::string outputType; // string representing the type of the outputs from this MultiPipe (the empty string if not defined yet)
    // friendship with the PipeGraph class
    friend class PipeGraph;

    // Private Constructor I (to create an empty MultiPipe)
    MultiPipe(PipeGraph *_graph):
              graph(_graph),
              has_source(false),
              has_sink(false),
              last(nullptr),
              secondToLast(nullptr),
              isMerged(false),
              isSplit(false),
              fromSplitting(false),
              splittingBranches(0),
              forceShuffling(false),
              lastParallelism(0),
              outputType("")
    {}

    // Private Constructor II (to create a MultiPipe from the merge of other MultiPipe instances)
    MultiPipe(PipeGraph *_graph,
              std::vector<ff::ff_node *> _normalization):
              graph(_graph),
              has_source(true),
              has_sink(false),
              isMerged(false),
              isSplit(false),
              fromSplitting(false),
              splittingBranches(0),
              forceShuffling(true), // <-- we force a shuffling for the next operator
              lastParallelism(0),
              outputType("")
    {
        // create the initial matrioska
        ff::ff_a2a *matrioska = new ff::ff_a2a();
        matrioska->add_firstset(_normalization, 0, false); // this will share the nodes of the MultiPipe instances to be merged!
        std::vector<ff::ff_node *> second_set;
        ff::ff_pipeline *stage = new ff::ff_pipeline();
        stage->add_stage(new selfkiller_node(), true);
        second_set.push_back(stage);
        matrioska->add_secondset(second_set, true);
        this->add_stage(matrioska, true);
        last = matrioska;
        secondToLast = nullptr;
    }

    // method to add a source to the MultiPipe
    template<typename tuple_t>
    MultiPipe &add_source(Source<tuple_t> &_source);

    // method to add an operator to the MultiPipe
    template<typename emitter_t, typename collector_t>
    void add_operator(ff::ff_farm *_op, routing_types_t _type, ordering_mode_t _ordering);

    // method to chain an operator with the previous one in the MultiPipe
    template<typename worker_t>
    bool chain_operator(ff::ff_farm *_op);

    // method to normalize the MultiPipe (removing the final self-killer nodes)
    std::vector<ff::ff_node *> normalize();

    // prepareMergeSet method: base case 1
    std::vector<MultiPipe *> prepareMergeSet();

    // prepareMergeSet method: base case 2
    template<typename MULTIPIPE>
    std::vector<MultiPipe *> prepareMergeSet(MULTIPIPE &_pipe);

    // prepareMergeSet method: generic case
    template<typename MULTIPIPE, typename ...MULTIPIPES>
    std::vector<MultiPipe *> prepareMergeSet(MULTIPIPE &_first, MULTIPIPES&... _pipes);

    // prepareSplittingEmitters method
    void prepareSplittingEmitters(Basic_Emitter *_e);

    // run method
    int run();

    // wait method
    int wait();

    // run_and_wait_end method
    int run_and_wait_end();

    // check whether the MultiPipe is runnable
    bool isRunnable() const;

    // check whether the MultiPipe has a Source
    bool hasSource() const;

    // check whether the MultiPipe has a Sink
    bool hasSink() const;

    // return the number of threads used to run this MultiPipe
    size_t getNumThreads() const;

public:
	/**
     *  \brief Add a Filter to the MultiPipe
     *  \param _filter Filter operator to be added
     *  \return the modified MultiPipe
     */
    template<typename tuple_t>
    MultiPipe &add(Filter<tuple_t> &_filter);

    /**
     *  \brief Chain a Filter to the MultiPipe (if possible, otherwise add it)
     *  \param _filter Filter operator to be chained
     *  \return the modified MultiPipe
     */
    template<typename tuple_t>
    MultiPipe &chain(Filter<tuple_t> &_filter);

	/**
     *  \brief Add a Map to the MultiPipe
     *  \param _map Map operator to be added
     *  \return the modified MultiPipe
     */
    template<typename tuple_t, typename result_t>
    MultiPipe &add(Map<tuple_t, result_t> &_map);

    /**
     *  \brief Chain a Map to the MultiPipe (if possible, otherwise add it)
     *  \param _map Map operator to be chained
     *  \return the modified MultiPipe
     */
    template<typename tuple_t, typename result_t>
    MultiPipe &chain(Map<tuple_t, result_t> &_map);

	/**
     *  \brief Add a FlatMap to the MultiPipe
     *  \param _flatmap FlatMap operator to be added
     *  \return the modified MultiPipe
     */
    template<typename tuple_t, typename result_t>
    MultiPipe &add(FlatMap<tuple_t, result_t> &_flatmap);

    /**
     *  \brief Chain a FlatMap to the MultiPipe (if possible, otherwise add it)
     *  \param _flatmap FlatMap operator to be chained
     *  \return the modified MultiPipe
     */
    template<typename tuple_t, typename result_t>
    MultiPipe &chain(FlatMap<tuple_t, result_t> &_flatmap);

    /**
     *  \brief Add an Accumulator to the MultiPipe
     *  \param _acc Accumulator operator to be added
     *  \return the modified MultiPipe
     */
    template<typename tuple_t, typename result_t>
    MultiPipe &add(Accumulator<tuple_t, result_t> &_acc);

	/**
     *  \brief Add a Win_Farm to the MultiPipe
     *  \param _wf Win_Farm operator to be added
     *  \return the modified MultiPipe
     */
    template<typename tuple_t, typename result_t>
    MultiPipe &add(Win_Farm<tuple_t, result_t> &_wf);

    /**
     *  \brief Add a Win_Farm_GPU to the MultiPipe
     *  \param _wf Win_Farm_GPU operator to be added
     *  \return the modified MultiPipe
     */
    template<typename tuple_t, typename result_t, typename F_t>
    MultiPipe &add(Win_Farm_GPU<tuple_t, result_t, F_t> &_wf);

	/**
     *  \brief Add a Key_Farm to the MultiPipe
     *  \param _kf Key_Farm operator to be added
     *  \return the modified MultiPipe
     */
    template<typename tuple_t, typename result_t>
    MultiPipe &add(Key_Farm<tuple_t, result_t> &_kf);

    /**
     *  \brief Add a Key_Farm_GPU to the MultiPipe
     *  \param _kf Key_Farm_GPU operator to be added
     *  \return the modified MultiPipe
     */
    template<typename tuple_t, typename result_t, typename F_t>
    MultiPipe &add(Key_Farm_GPU<tuple_t, result_t, F_t> &_kf);

	/**
     *  \brief Add a Pane_Farm to the MultiPipe
     *  \param _pf Pane_Farm operator to be added
     *  \return the modified MultiPipe
     */
    template<typename tuple_t, typename result_t>
    MultiPipe &add(Pane_Farm<tuple_t, result_t> &_pf);

    /**
     *  \brief Add a Pane_Farm_GPU to the MultiPipe
     *  \param _pf Pane_Farm_GPU operator to be added
     *  \return the modified MultiPipe
     */
    template<typename tuple_t, typename result_t, typename F_t>
    MultiPipe &add(Pane_Farm_GPU<tuple_t, result_t, F_t> &_pf);

	/**
     *  \brief Add a Win_MapReduce to the MultiPipe
     *  \param _wmr Win_MapReduce operator to be added
     *  \return the modified MultiPipe
     */
    template<typename tuple_t, typename result_t>
    MultiPipe &add(Win_MapReduce<tuple_t, result_t> &_wmr);

    /**
     *  \brief Add a Win_MapReduce_GPU to the MultiPipe
     *  \param _wmr Win_MapReduce_GPU operator to be added
     *  \return the modified MultiPipe
     */
    template<typename tuple_t, typename result_t, typename F_t>
    MultiPipe &add(Win_MapReduce_GPU<tuple_t, result_t, F_t> &_wmr);

	/**
     *  \brief Add a Sink to the MultiPipe
     *  \param _sink Sink operator to be added
     *  \return the modified MultiPipe
     */
    template<typename tuple_t>
    MultiPipe &add_sink(Sink<tuple_t> &_sink);

    /**
     *  \brief Chain a Sink to the MultiPipe (if possible, otherwise add it)
     *  \param _sink Sink operator to be chained
     *  \return the modified MultiPipe
     */
    template<typename tuple_t>
    MultiPipe &chain_sink(Sink<tuple_t> &_sink);

    /**
     *  \brief Merge of this with a set of MultiPipe instances _pipes
     *  \param _pipes set of MultiPipe instances to be merged with this
     *  \return a reference to the new MultiPipe (the result fo the merge)
     */
    template<typename ...MULTIPIPES>
    MultiPipe &merge(MULTIPIPES&... _pipes);

    /**
     *  \brief Split of this into a set of MultiPipe instances
     *  \param _splitting_func splitting function
     *  \param _cardinality number of splitting MultiPipe instances to generate from this
     *  \return the MultiPipe this after the splitting
     */
    template<typename F_t>
    MultiPipe &split(F_t _splitting_func, size_t _cardinality);

    /**
     *  \brief Select a MultiPipe upon the splitting of this
     *  \param _idx index of the MultiPipe to be selected
     *  \return reference to split MultiPipe with index _idx
     */
    MultiPipe &select(size_t _idx) const;

    /// deleted constructors/operators
    MultiPipe(const MultiPipe &) = delete; // copy constructor
    MultiPipe(MultiPipe &&) = delete; // move constructor
    MultiPipe &operator=(const MultiPipe &) = delete; // copy assignment operator
    MultiPipe &operator=(MultiPipe &&) = delete; // move assignment operator
};

//@cond DOXY_IGNORE

// ######################### IMPLEMENTATION OF THE METHODS OF THE CLASS PIPEGRAPH ######################### //

// implementation of the destructor
inline PipeGraph::~PipeGraph()
{
    // delete all the MultiPipe instances in toBeDeteled
    for (auto *mp: toBeDeteled)
        delete mp;
    // delete the Application Tree
    delete_AppNodes(root);
}

// implementation of the method to find the AppNode containing the MultiPipe _mp in the tree rooted at _node
inline AppNode *PipeGraph::find_AppNode(AppNode *_node, MultiPipe *_mp)
{
    // base case
	if (_node->mp == _mp)
		return _node;
    // recursive case
	else {
		AppNode *found = nullptr;
		for (auto *child: _node->children) {
			found = find_AppNode(child, _mp);
			if (found != nullptr)
				return found;
		}
		return nullptr;
	}
}

// implementation of the method to find the list of AppNode instances that are leaves of the tree rooted at _node
inline std::vector<AppNode *> PipeGraph::get_LeavesList(AppNode *_node)
{
    std::vector<AppNode *> leaves;
    // base case
    if ((_node->children).size() == 0) {
        leaves.push_back(_node);
        return leaves;
    }
    // recursive case
    else {
        for (auto *child: _node->children) {
            auto v = get_LeavesList(child);
            leaves.insert(leaves.end(), v.begin(), v.end());
        }
        return leaves;
    }
}

// implementation of the method to find the LCA of a set of _leaves rooted at _node
inline AppNode *PipeGraph::get_LCA(AppNode *_node, std::vector<AppNode *> _leaves)
{
    // compute the leaves rooted at each child of _node
    for (auto *child: _node->children) {
        auto child_leaves = get_LeavesList(child);
        bool foundAll = true;
        for (auto *leaf: _leaves) {
            if (std::find(child_leaves.begin(), child_leaves.end(), leaf) == child_leaves.end()) { // if not present
                foundAll = false;
            }
        }
        if (foundAll)
            return get_LCA(child, _leaves);
    }
    return _node;
}

// implementation of the method to delete all the AppNode instances in the tree rooted at _node
inline void PipeGraph::delete_AppNodes(AppNode *_node)
{
    // base case
	if ((_node->children).size() == 0)
		delete _node;
    // recursive case
	else {
		for (auto *child: _node->children)
			delete_AppNodes(child);
		delete _node;
	}
}

// implementation of the method to prepare the right list of AppNode instances to be merged (case merge-ind and merge-full)
inline bool PipeGraph::get_MergedNodes1(std::vector<MultiPipe *> _toBeMerged, std::vector<AppNode *> &_rightList) {
    // all the input MultiPipe instances must be leaves of the Application Tree
    std::vector<AppNode *> inputNodes;
    assert(_toBeMerged.size() > 1); // redundant check
    for (auto *mp: _toBeMerged) {
        AppNode *node = find_AppNode(root, mp);
        if (node == nullptr) {
            std::cerr << RED << "WindFlow Error: MultiPipe to be merged does not belong to this PipeGraph" << DEFAULT << std::endl;
            exit(EXIT_FAILURE);
        }
        assert((node->children).size() == 0); // redundant check
        inputNodes.push_back(node);
    }
    // loop until inputNodes is empty
    while (inputNodes.size() > 0) {
        // pick one random node in inputNodes
        std::random_device random_device;
        std::mt19937 engine { random_device() };
        std::uniform_int_distribution<int> dist(0, inputNodes.size() - 1);
        AppNode *curr_node = inputNodes[dist(engine)];
        // remove curr_node from inputNodes
        inputNodes.erase(std::remove(inputNodes.begin(), inputNodes.end(), curr_node), inputNodes.end());
        AppNode *lca = curr_node;
        // loop in the sub-tree rooted at curr_node
        while (curr_node->parent != root && inputNodes.size() > 0) {
            // get all the others leaves of the sub-tree rooted at the parent
            std::vector<AppNode *> leaves;
            for (auto *brother: ((curr_node->parent)->children)) {
                if (brother != curr_node) { // I am not my brother :)
                    auto v = get_LeavesList(brother);
                    leaves.insert(leaves.end(), v.begin(), v.end());
                }
            }
            // check that every leaf is in inputNodes
            for (auto *leaf: leaves) {
                if (std::find(inputNodes.begin(), inputNodes.end(), leaf) == inputNodes.end()) { // if not present
                    return false;
                }
            }
            // remove all the leaves from inputNodes
            for (auto *leaf: leaves) {
                inputNodes.erase(std::remove(inputNodes.begin(), inputNodes.end(), leaf), inputNodes.end());
            }
            curr_node = curr_node->parent;
            lca = curr_node;
        }
        // add the found lca to the _rightList
        _rightList.push_back(lca);
        // important check
        if (lca->parent != root && ((inputNodes.size() > 0) || (_rightList.size() > 1))) {
            return false;
        }
    }
    return true;
}

// implementation of the method to prepare the right list of AppNode instances to be merged (case merge-partial)
inline AppNode *PipeGraph::get_MergedNodes2(std::vector<MultiPipe *> _toBeMerged, std::vector<AppNode *> &_rightList)
{
    // all the input MultiPipe instances must be leaves of the Application Tree
    std::vector<AppNode *> inputNodes;
    assert(_toBeMerged.size() > 1); // redundant check
    for (auto *mp: _toBeMerged) {
        AppNode *node = find_AppNode(root, mp);
        if (node == nullptr) {
            std::cerr << RED << "WindFlow Error: MultiPipe to be merged does not belong to this PipeGraph" << DEFAULT << std::endl;
            exit(EXIT_FAILURE);
        }
        assert((node->children).size() == 0); // redundant check
        inputNodes.push_back(node);
    }
    // we have to find the LCA
    AppNode *parent_node = get_LCA(root, inputNodes);
    if (parent_node != root) {
        for (auto *child: parent_node->children) {
            auto child_leaves = get_LeavesList(child);
            bool foundAll = true;
            size_t count = 0;
            for (auto *leaf: child_leaves) {
                if (std::find(inputNodes.begin(), inputNodes.end(), leaf) == inputNodes.end())
                    foundAll = false;
                else
                    count++;
            }
            if (foundAll)
                _rightList.push_back(child);
            else if (!foundAll && count > 0)
                return nullptr;
        }
        assert(_rightList.size() > 1);
        return parent_node;
    }
    else
        return nullptr;
}

// implementation of the method to execute the split of the MultiPipe _mp
inline std::vector<MultiPipe *> PipeGraph::execute_Split(MultiPipe *_mp)
{
    // find _mp in the Application Tree
    AppNode *found = find_AppNode(root, _mp);
    if (found == nullptr) {
        std::cerr << RED << "WindFlow Error: MultiPipe to be split does not belong to this PipeGraph" << DEFAULT << std::endl;
        exit(EXIT_FAILURE);
    }
    assert((found->children).size() == 0); // redundant check
    // prepare the MultiPipe _mp
    std::vector<MultiPipe *> splitMPs;
    std::vector<ff::ff_node *> normalization = _mp->normalize();
    _mp->isSplit = true; // be careful --> this statement after normalize() not before!
    ff::ff_a2a *container = new ff::ff_a2a();
    container->add_firstset(normalization, 0, false);
    std::vector<ff::ff_node *> second_set;
    for (size_t i=0; i<_mp->splittingBranches; i++) {
        MultiPipe *split_mp = new MultiPipe(this);
        split_mp->outputType = _mp->outputType;
        split_mp->has_source = true;
        split_mp->fromSplitting = true;
        split_mp->splittingParent = _mp;
        second_set.push_back(split_mp);
        splitMPs.push_back(split_mp);
    }
    container->add_secondset(second_set, false);
    _mp->remove_stage(0);
    _mp->add_stage(container, true);
    _mp->last = container;
    _mp->secondToLast = nullptr;
    // prepare the Application Tree
    for (auto *split_mp: splitMPs) {
        (found->children).push_back(new AppNode(split_mp, found));
        toBeDeteled.push_back(split_mp);
    }
    return splitMPs;
}

// implementation of the method to execute the merge of a set of MultiPipe instances _toBeMerged
inline MultiPipe *PipeGraph::execute_Merge(std::vector<MultiPipe *> _toBeMerged)
{
    // get the right list of AppNode instances to be merged
    std::vector<AppNode *> rightList;
    if (get_MergedNodes1(_toBeMerged, rightList)) {
        if (rightList.size() == 1) { // Case 2.1: merge-full -> we merge a whole sub-tree
            assert(rightList[0] != root); // redundant check
            MultiPipe *mp = rightList[0]->mp;
            // create the new MultiPipe, the result of the self-merge
            auto normalization = mp->normalize();
            mp->isMerged = true;
            MultiPipe *mergedMP = new MultiPipe(this, normalization);
            mergedMP->outputType = _toBeMerged[0]->outputType;
            toBeDeteled.push_back(mergedMP);
            // adjust the parent MultiPipe if it exists
            if (mp->fromSplitting && mp->splittingParent != nullptr) {
                MultiPipe *parentMP = mp->splittingParent;
                for (size_t i=0; i<(parentMP->splittingChildren).size(); i++) {
                    if ((parentMP->splittingChildren)[i] == mp) {
                        (parentMP->splittingChildren)[i] = mergedMP;
                        break;
                    }
                }
                auto second_set = (parentMP->last)->getSecondSet();
                std::vector<ff::ff_node *> new_second_set;
                for (size_t i=0; i<second_set.size(); i++) {
                    MultiPipe *mp2 = static_cast<MultiPipe *>(second_set[i]);
                    if (mp2 == mp)
                        new_second_set.push_back(mergedMP);
                    else
                        new_second_set.push_back(second_set[i]);
                }
                (parentMP->last)->change_secondset(new_second_set, false);
                mergedMP->fromSplitting = true;
                mergedMP->splittingParent = parentMP;
            }
            // adjust the Application Tree
            std::vector<AppNode *> children_new;
            for (auto *brother: (rightList[0]->parent)->children) {
                if (brother != rightList[0]) // I am not my brother :)
                    children_new.push_back(brother);
                else
                    children_new.push_back(new AppNode(mergedMP, rightList[0]->parent));
            }
            (rightList[0]->parent)->children = children_new;
            delete_AppNodes(rightList[0]);
            return mergedMP;
        }
        else { // Case 1: merge-ind -> merge independent trees
            std::vector<MultiPipe *> rightMergedMPs;
            // check that merged MultiPipe instances are sons of the root
            for (auto *an: rightList) { // redundant check
                if (std::find((root->children).begin(), (root->children).end(), an) == (root->children).end()) {
                    std::cerr << RED << "WindFlow Error: the requested merge operation is not supported" << DEFAULT << std::endl;
                    exit(EXIT_FAILURE);
                }
                rightMergedMPs.push_back(an->mp);
            }
            // create the new MultiPipe, the result of the merge
            std::vector<ff::ff_node *> normalization;
            for (auto *mp: rightMergedMPs) {
                auto v = mp->normalize();
                mp->isMerged = true;
                normalization.insert(normalization.end(), v.begin(), v.end());
            }
            MultiPipe *mergedMP = new MultiPipe(this, normalization);
            mergedMP->outputType = _toBeMerged[0]->outputType;
            toBeDeteled.push_back(mergedMP);
            // adjust the Application Tree
            std::vector<AppNode *> children_new;
            // maintaining the previous ordering of the children is not important in this case
            for (auto *brother: root->children) {
                if (std::find(rightList.begin(), rightList.end(), brother) == rightList.end())
                    children_new.push_back(brother);
            }
            children_new.push_back(new AppNode(mergedMP, root));
            root->children = children_new;
            // delete the nodes
            for (auto *an: rightList)
                delete_AppNodes(an);
            return mergedMP;
        }
    }
    else {
        rightList.clear();
        AppNode *parent_node = get_MergedNodes2(_toBeMerged, rightList);
        if (parent_node != nullptr) { // Case 1.2: merge-partial -> merge two or more sub-trees at the same level
            std::vector<int> indexes;
            for (auto *node: rightList) {
                assert(node->parent == parent_node);
                // get the index of this child
                indexes.push_back(std::distance((parent_node->children).begin(), std::find((parent_node->children).begin(), (parent_node->children).end(), node)));
            }
            // check whether the nodes in the rightList are adjacent
            std::sort(indexes.begin(), indexes.end());
            for (size_t i=0; i<indexes.size()-1; i++) {
                if (indexes[i]+1 != indexes[i+1]) {
                    std::cerr << RED << "WindFlow Error: sibling MultiPipes to be merged must be contiguous branches of the same MultiPipe" << DEFAULT << std::endl;
                    exit(EXIT_FAILURE);
                }
            }
            // everything must be put in the right order
            rightList.clear();
            std::vector<MultiPipe *> rightMergedMPs;
            for (int idx: indexes) {
                rightList.push_back((parent_node->children)[idx]);
                rightMergedMPs.push_back((parent_node->children)[idx]->mp);
            }
            // if we reach this point the merge is possible -> we create the new MultiPipe
            std::vector<ff::ff_node *> normalization;
            for (auto *mp: rightMergedMPs) {
                auto v = mp->normalize();
                mp->isMerged = true;
                normalization.insert(normalization.end(), v.begin(), v.end());
            }
            MultiPipe *mergedMP = new MultiPipe(this, normalization);
            mergedMP->outputType = _toBeMerged[0]->outputType;
            toBeDeteled.push_back(mergedMP);
            // adjust the parent MultiPipe
            MultiPipe *parentMP = parent_node->mp;
            assert(parentMP->isSplit); // redundant check
            size_t new_branches = (parentMP->splittingChildren).size() - indexes.size() + 1;
            std::vector<MultiPipe *> new_splittingChildren;
            std::vector<ff::ff_node *> new_second_set;
            auto second_set = (parentMP->last)->getSecondSet();
            // the code below is to respect the original indexes
            for (size_t i=0; i<(parentMP->splittingChildren).size(); i++) {
                if (i < indexes[0]) {
                    new_splittingChildren.push_back((parentMP->splittingChildren)[i]);
                    new_second_set.push_back(second_set[i]);
                }
                else if (i == indexes[0]) {
                    new_splittingChildren.push_back(mergedMP);
                    new_second_set.push_back(mergedMP);
                }
                else if (i > indexes[indexes.size()-1]) {
                    new_splittingChildren.push_back((parentMP->splittingChildren)[i]);
                    new_second_set.push_back(second_set[i]);
                }
            }
            assert(new_splittingChildren.size () == new_branches); // redundant check
            parentMP->splittingBranches = new_branches;
            parentMP->splittingChildren = new_splittingChildren;
            (parentMP->last)->change_secondset(new_second_set, false);
            mergedMP->fromSplitting = true;
            mergedMP->splittingParent = parentMP;
            // adjust the Application Tree
            std::vector<AppNode *> children_new;
            bool done = false;
            for (auto *brother: parent_node->children) {
                if (std::find(rightList.begin(), rightList.end(), brother) == rightList.end())
                    children_new.push_back(brother);
                else if (!done) {
                   children_new.push_back(new AppNode(mergedMP, parent_node));
                   done = true;
                }
            }
            parent_node->children = children_new;
            for (auto *an: rightList) {
                delete_AppNodes(an);
            }
            return mergedMP;
        }
        else {
            std::cerr << RED << "WindFlow Error: the requested merge operation is not supported" << DEFAULT << std::endl;
            exit(EXIT_FAILURE);
        }
    }
}

// implementation of the add_source method
template<typename tuple_t>
MultiPipe &PipeGraph::add_source(Source<tuple_t> &_source)
{
	MultiPipe *mp = new MultiPipe(this);
	mp->add_source<tuple_t>(_source);
	// update the Application Tree
	(root->children).push_back(new AppNode(mp, root));
	// this MultiPipe must be deleted at the end
	toBeDeteled.push_back(mp);
	return *mp;
}

// implementation of the run method
inline int PipeGraph::run()
{
	if ((root->children).size() == 0) {
		std::cerr << RED << "WindFlow Error: PipeGraph [" << name << "] is empty, nothing to run" << DEFAULT << std::endl;
		exit(EXIT_FAILURE);
        return EXIT_FAILURE; // useless
	}
	else {
		// count number of threads
		size_t count_threads = this->getNumThreads();
		std::cout << GREEN << "WindFlow Status Message: PipeGraph [" << name << "] is running with " << count_threads << " threads" << DEFAULT << std::endl;
		int status = 0;
		// running phase
		for (auto *an: root->children)
			status |= (an->mp)->run();
		// waiting phase
 		for (auto *an: root->children)
			status |= (an->mp)->wait();
		if (status == 0)
			std::cout << GREEN << "WindFlow Status Message: PipeGraph [" << name << "] executed successfully" << DEFAULT << std::endl;
		//else
			//std::cerr << RED << "WindFlow Error: PipeGraph [" << name << "] execution problems found" << DEFAULT << std::endl;
        return 0;
	}
}

// implementation of the method to return the number of threads used to run this PipeGraph
inline size_t PipeGraph::getNumThreads() const
{
    size_t count = 0;
    for (auto *an: root->children)
        count += (an->mp)->getNumThreads();
    return count;
}

// ######################### IMPLEMENTATION OF THE METHODS OF THE CLASS MULTIPIPE ######################### //

// implementation of the method to add a source to the MultiPipe
template<typename tuple_t>
MultiPipe &MultiPipe::add_source(Source<tuple_t> &_source)
{
    // check the Source presence
    if (has_source) {
        std::cerr << RED << "WindFlow Error: Source has been already defined for the MultiPipe" << DEFAULT << std::endl;
        exit(EXIT_FAILURE);
    }
    // create the initial matrioska
    ff::ff_a2a *matrioska = new ff::ff_a2a();
    std::vector<ff::ff_node *> first_set;
    auto workers = _source.getFirstSet();
    for (size_t i=0; i<workers.size(); i++) {
        ff::ff_pipeline *stage = new ff::ff_pipeline();
        stage->add_stage(workers[i], false);
        first_set.push_back(stage);
    }
    matrioska->add_firstset(first_set, 0, true);
    std::vector<ff::ff_node *> second_set;
    ff::ff_pipeline *stage = new ff::ff_pipeline();
    stage->add_stage(new selfkiller_node(), true);
    second_set.push_back(stage);
    matrioska->add_secondset(second_set, true);
    this->add_stage(matrioska, true);
    has_source = true;
    last = matrioska;
    // save parallelism of the operator
    lastParallelism = workers.size();
    // save the output type from this MultiPipe
    tuple_t t;
    outputType = typeid(t).name();
    return *this;
}

// implementation of the method to add an operator to the MultiPipe
template<typename emitter_t, typename collector_t>
void MultiPipe::add_operator(ff::ff_farm *_op, routing_types_t _type, ordering_mode_t _ordering)
{
    // check the Source presence
    if (!has_source) {
        std::cerr << RED << "WindFlow Error: MultiPipe does not have a Source, operator cannot be added" << DEFAULT << std::endl;
        exit(EXIT_FAILURE);
    }
    // check the Sink presence
    if (has_sink) {
        std::cerr << RED << "WindFlow Error: MultiPipe is terminated by a Sink, operator cannot be added" << DEFAULT << std::endl;
        exit(EXIT_FAILURE);
    }
    // merged MultiPipe cannot be modified
    if (isMerged) {
        std::cerr << RED << "WindFlow Error: MultiPipe has been merged, operator cannot be added" << DEFAULT << std::endl;
        exit(EXIT_FAILURE);
    }
    // split MultiPipe cannot be modified
    if (isSplit) {
        std::cerr << RED << "WindFlow Error: MultiPipe has been split, operator cannot be added" << DEFAULT << std::endl;
        exit(EXIT_FAILURE);
    }
    // Case 1: first operator added after splitting
    if (fromSplitting && last == nullptr) {
        // create the initial matrioska
        ff::ff_a2a *matrioska = new ff::ff_a2a();
        std::vector<ff::ff_node *> first_set;
        auto workers = _op->getWorkers();
        for (size_t i=0; i<workers.size(); i++) {
            ff::ff_pipeline *stage = new ff::ff_pipeline();
            stage->add_stage(workers[i], false);
            combine_with_firststage(*stage, new collector_t(_ordering), true);
            first_set.push_back(stage);
        }
        matrioska->add_firstset(first_set, 0, true);
        std::vector<ff::ff_node *> second_set;
        ff::ff_pipeline *stage = new ff::ff_pipeline();
        stage->add_stage(new selfkiller_node(), true);
        second_set.push_back(stage);
        matrioska->add_secondset(second_set, true);
        this->add_stage(matrioska, true);
        last = matrioska;
        // save parallelism of the operator
        lastParallelism = workers.size();
        // save what is needed for splitting with the parent MultiPipe
        Basic_Emitter *be = static_cast<Basic_Emitter *>(_op->getEmitter());
        assert(splittingParent != nullptr); // redundant check
        splittingParent->prepareSplittingEmitters(be);
        return;
    }
    size_t n1 = (last->getFirstSet()).size();
    size_t n2 = (_op->getWorkers()).size();
    // Case 2: direct connection
    if ((n1 == n2) && _type == SIMPLE && !forceShuffling) {
        auto first_set = last->getFirstSet();
        auto worker_set = _op->getWorkers();
        // add the operator's workers to the pipelines in the first set of the matrioska
        for (size_t i=0; i<n1; i++) {
            ff::ff_pipeline *stage = static_cast<ff::ff_pipeline *>(first_set[i]);
            stage->add_stage(worker_set[i], false);
        }
    }
    // Case 3: shuffle connection
    else {
        // prepare the nodes of the first_set of the last matrioska for the shuffling
        auto first_set_m = last->getFirstSet();
        for (size_t i=0; i<n1; i++) {
            ff::ff_pipeline *stage = static_cast<ff::ff_pipeline *>(first_set_m[i]);
            emitter_t *tmp_e = static_cast<emitter_t *>(_op->getEmitter());
            combine_with_laststage(*stage, new emitter_t(*tmp_e), true);
        }
        // create a new matrioska
        ff::ff_a2a *matrioska = new ff::ff_a2a();
        std::vector<ff::ff_node *> first_set;
        auto worker_set = _op->getWorkers();
        for (size_t i=0; i<n2; i++) {
            ff::ff_pipeline *stage = new ff::ff_pipeline();
            stage->add_stage(worker_set[i], false);
            if (lastParallelism != 1 || forceShuffling || _ordering == ID || _ordering == TS_RENUMBERING) {
                combine_with_firststage(*stage, new collector_t(_ordering), true);
            }
            else { // we avoid the ordering node when possible
                combine_with_firststage(*stage, new dummy_mi(), true);
            }
            first_set.push_back(stage);
        }
        matrioska->add_firstset(first_set, 0, true);
        std::vector<ff::ff_node *> second_set;
        ff::ff_pipeline *stage = new ff::ff_pipeline();
        stage->add_stage(new selfkiller_node(), true);
        second_set.push_back(stage);
        matrioska->add_secondset(second_set, true);
        ff::ff_pipeline *previous = static_cast<ff::ff_pipeline *>((last->getSecondSet())[0]);
        previous->remove_stage(0); // remove the self-killer node (it will be deleted with previous later on)
        previous->add_stage(matrioska, true); // Chinese boxes
        secondToLast = last;
        last = matrioska;
        // reset forceShuffling flag if it was true
        if (forceShuffling)
            forceShuffling = false;
    }
    // save parallelism of the operator
    lastParallelism = n2;
}

// implementation of the method to chain an operator with the previous one in the MultiPipe
template<typename worker_t>
bool MultiPipe::chain_operator(ff::ff_farm *_op)
{
    // check the Source presence
    if (!has_source) {
        std::cerr << RED << "WindFlow Error: MultiPipe does not have a Source, operator cannot be chained" << DEFAULT << std::endl;
        exit(EXIT_FAILURE);
    }
    // check the Sink presence
    if (has_sink) {
        std::cerr << RED << "WindFlow Error: MultiPipe is terminated by a Sink, operator cannot be chained" << DEFAULT << std::endl;
        exit(EXIT_FAILURE);
    }
    // merged MultiPipe cannot be modified
    if (isMerged) {
        std::cerr << RED << "WindFlow Error: MultiPipe has been merged, operator cannot be chained" << DEFAULT << std::endl;
        exit(EXIT_FAILURE);
    }
    // split MultiPipe cannot be modified
    if (isSplit) {
        std::cerr << RED << "WindFlow Error: MultiPipe has been split, operator cannot be chained" << DEFAULT << std::endl;
        exit(EXIT_FAILURE);
    }
    // corner case -> first operator added to a MultiPipe after splitting (chain cannot work, add instead)
    if (fromSplitting && last == nullptr)
        return false;
    size_t n1 = (last->getFirstSet()).size();
    size_t n2 = (_op->getWorkers()).size();
    // _op is for sure SIMPLE: check additional conditions for chaining
    if ((n1 == n2) && (!forceShuffling)) {
        auto first_set = (last)->getFirstSet();
        auto worker_set = _op->getWorkers();
        // chaining the operator's workers with the last node of each pipeline in the first set of the matrioska
        for (size_t i=0; i<n1; i++) {
            ff::ff_pipeline *stage = static_cast<ff::ff_pipeline *>(first_set[i]);
            worker_t *worker = static_cast<worker_t *>(worker_set[i]);
            combine_with_laststage(*stage, worker, false);
        }
        // save parallelism of the operator (not necessary: n1 is equal to n2)
        lastParallelism = n2;
        return true;
    }
    else
        return false;
}

// implementation of the method to normalize the MultiPipe (removing the final self-killer nodes)
inline std::vector<ff::ff_node *> MultiPipe::normalize()
{
    // check the Source presence
    if (!has_source) {
        std::cerr << RED << "WindFlow Error: MultiPipe does not have a Source, it cannot be merged/split" << DEFAULT << std::endl;
        exit(EXIT_FAILURE);
    }
    // check the Sink presence
    if (has_sink) {
        std::cerr << RED << "WindFlow Error: MultiPipe is terminated by a Sink, it cannot be merged/split" << DEFAULT << std::endl;
        exit(EXIT_FAILURE);
    }
    // empty MultiPipe cannot be normalized (only for splitting in this case)
    if (last == nullptr) {
        std::cerr << RED << "WindFlow Error: MultiPipe is empty, it cannot be split" << DEFAULT << std::endl;
        exit(EXIT_FAILURE);
    }
    std::vector<ff::ff_node *> result;
    // Case 1: the MultiPipe has not been split
    if (!isSplit) {
        // Case 1.1 (only one Matrioska)
        if (secondToLast == nullptr) {
            auto first_set = last->getFirstSet();
            for (size_t i=0; i<first_set.size(); i++)
                result.push_back(first_set[i]);
        }
        // Case 1.2 (at least two nested Matrioska)
        else {
            last->cleanup_firstset(false);
            auto first_set_last = last->getFirstSet();
            std::vector<ff::ff_node *> second_set_secondToLast;
            for (size_t i=0; i<first_set_last.size(); i++)
                second_set_secondToLast.push_back(first_set_last[i]);
            secondToLast->change_secondset(second_set_secondToLast, true);
            delete last;
            last = secondToLast;
            secondToLast = nullptr;
            ff::ff_pipeline *p = new ff::ff_pipeline(); // <-- Memory leak!
            p->add_stage((this->getStages())[0], false);
            result.push_back(p);
        }
    }
    // Case 2: the MultiPipe has been split
    else {
        std::vector<ff::ff_node *> normalized;
        auto second_set = last->getSecondSet();
        for (auto *node: second_set) {
            MultiPipe *mp = static_cast<MultiPipe *>(node);
            auto v = mp->normalize();
            // mp has been normalized for merging
            mp->isMerged = true;
            normalized.insert(normalized.end(), v.begin(), v.end());
        }
        last->change_secondset(normalized, false);
        ff::ff_pipeline *p = new ff::ff_pipeline(); // <-- Memory leak
        p->add_stage(last, false);
        result.push_back(p);
    }
    return result;
}

// implementation of the prepareMergeSet method: base case 1
inline std::vector<MultiPipe *> MultiPipe::prepareMergeSet()
{
    return std::vector<MultiPipe *>();
}

// implementation of the prepareMergeSet method: base case 2
template<typename MULTIPIPE>
std::vector<MultiPipe *> MultiPipe::prepareMergeSet(MULTIPIPE &_pipe)
{
    // check whether the MultiPipe has been already merged
    if (_pipe.isMerged) {
        std::cerr << RED << "WindFlow Error: MultiPipe has been already merged" << DEFAULT << std::endl;
        exit(EXIT_FAILURE);
    }
    // check whether the MultiPipe has been split
    if (_pipe.isSplit) {
        std::cerr << RED << "WindFlow Error: MultiPipe has been split and cannot be merged" << DEFAULT << std::endl;
        exit(EXIT_FAILURE);
    }
    std::vector<MultiPipe *> output;
    output.push_back(&_pipe);
    return output;
}

// implementation of the prepareMergeSet method: generic case
template<typename MULTIPIPE, typename ...MULTIPIPES>
std::vector<MultiPipe *> MultiPipe::prepareMergeSet(MULTIPIPE &_first, MULTIPIPES&... _pipes)
{
    // check whether the MultiPipe has been already merged
    if (_first.isMerged) {
        std::cerr << RED << "WindFlow Error: MultiPipe has been already merged" << DEFAULT << std::endl;
        exit(EXIT_FAILURE);
    }
    // check whether the MultiPipe has been split
    if (_first.isSplit) {
        std::cerr << RED << "WindFlow Error: MultiPipe has been split and cannot be merged" << DEFAULT << std::endl;
        exit(EXIT_FAILURE);
    }
    std::vector<MultiPipe *> output;
    output.push_back(&_first);
    auto v = prepareMergeSet(_pipes...);
    output.insert(output.end(), v.begin(), v.end());
    return output;
}

// implementation of the prepareSplittingEmitters method
inline void MultiPipe::prepareSplittingEmitters(Basic_Emitter *_e)
{
    assert(isSplit); // redundant check
    splittingEmitterLeaves.push_back(_e->clone());
    // check whether all the emitters are ready
    if (splittingEmitterLeaves.size() == splittingBranches) {
        Tree_Emitter *tE = new Tree_Emitter(splittingEmitterRoot, splittingEmitterLeaves, true, true);
        // combine tE with the nodes in the first set
        auto first_set = last->getFirstSet();
        for (size_t i=0; i<first_set.size(); i++) {
            ff::ff_pipeline *stage = static_cast<ff::ff_pipeline *>(first_set[i]);
            combine_with_laststage(*stage, new Tree_Emitter(*tE), true);
        }
        delete tE;
    }
}

// implementation of the run method
inline int MultiPipe::run()
{
    if (!isRunnable()) {
        std::cerr << RED << "WindFlow Error: MultiPipe is not runnable" << DEFAULT << std::endl;
        exit(EXIT_FAILURE);
        return EXIT_FAILURE; // useless
    }
    else
        return ff::ff_pipeline::run();
}

// implementation of the wait method
inline int MultiPipe::wait()
{
    if (!isRunnable()) {
        std::cerr << RED << "WindFlow Error: MultiPipe is not runnable" << DEFAULT << std::endl;
        exit(EXIT_FAILURE);
        return EXIT_FAILURE; // useless
    }
    else
        return ff::ff_pipeline::wait();
}

// implementation of the run_and_wait_end method
inline int MultiPipe::run_and_wait_end()
{
    if (!isRunnable()) {
        std::cerr << RED << "WindFlow Error: MultiPipe is not runnable" << DEFAULT << std::endl;
        exit(EXIT_FAILURE);
        return EXIT_FAILURE; // useless
    }
    else
        return ff::ff_pipeline::run_and_wait_end();
}

// implementation of the method to add a Filter to the MultiPipe
template<typename tuple_t>
MultiPipe &MultiPipe::add(Filter<tuple_t> &_filter)
{
    // check the type compatibility
    tuple_t t;
    std::string opInType = typeid(t).name();
    if (!outputType.empty() && outputType.compare(opInType) != 0) {
        std::cerr << RED << "WindFlow Error: output type from MultiPipe is not the input type of the Filter operator" << DEFAULT << std::endl;
        exit(EXIT_FAILURE);
    }
    // call the generic method to add the operator to the MultiPipe
    add_operator<Standard_Emitter<tuple_t>, Ordering_Node<tuple_t, tuple_t>>(&_filter, _filter.isKeyed() ? COMPLEX : SIMPLE, TS);
    // save the new output type from this MultiPipe
    outputType = opInType;
	return *this;
}

// implementation of the method to chain a Filter to the MultiPipe
template<typename tuple_t>
MultiPipe &MultiPipe::chain(Filter<tuple_t> &_filter)
{
    // check the type compatibility
    tuple_t t;
    std::string opInType = typeid(t).name();
    if (!outputType.empty() && outputType.compare(opInType) != 0) {
        std::cerr << RED << "WindFlow Error: output type from MultiPipe is not the input type of the Filter operator" << DEFAULT << std::endl;
        exit(EXIT_FAILURE);
    }
    // try to chain the operator with the MultiPipe
    if (!_filter.isKeyed()) {
        bool chained = chain_operator<typename Filter<tuple_t>::Filter_Node>(&_filter);
        if (!chained)
            add(_filter);
    }
    else
        add(_filter);
    // save the new output type from this MultiPipe
    outputType = opInType;
    return *this;
}

// implementation of the method to add a Map to the MultiPipe
template<typename tuple_t, typename result_t>
MultiPipe &MultiPipe::add(Map<tuple_t, result_t> &_map)
{
    // check the type compatibility
    tuple_t t;
    std::string opInType = typeid(t).name();
    if (!outputType.empty() && outputType.compare(opInType) != 0) {
        std::cerr << RED << "WindFlow Error: output type from MultiPipe is not the input type of the Map operator" << DEFAULT << std::endl;
        exit(EXIT_FAILURE);
    }
    // call the generic method to add the operator to the MultiPipe
    add_operator<Standard_Emitter<tuple_t>, Ordering_Node<tuple_t, tuple_t>>(&_map, _map.isKeyed() ? COMPLEX : SIMPLE, TS);
    // save the new output type from this MultiPipe
    result_t r;
    outputType = typeid(r).name();
	return *this;
}

// implementation of the method to chain a Map to the MultiPipe
template<typename tuple_t, typename result_t>
MultiPipe &MultiPipe::chain(Map<tuple_t, result_t> &_map)
{
    // check the type compatibility
    tuple_t t;
    std::string opInType = typeid(t).name();
    if (!outputType.empty() && outputType.compare(opInType) != 0) {
        std::cerr << RED << "WindFlow Error: output type from MultiPipe is not the input type of the Map operator" << DEFAULT << std::endl;
        exit(EXIT_FAILURE);
    }
    // try to chain the operator with the MultiPipe
    if (!_map.isKeyed()) {
        bool chained = chain_operator<typename Map<tuple_t, result_t>::Map_Node>(&_map);
        if (!chained)
            add(_map);
    }
    else
        add(_map);
    // save the new output type from this MultiPipe
    result_t r;
    outputType = typeid(r).name();
    return *this;
}

// implementation of the method to add a FlatMap to the MultiPipe
template<typename tuple_t, typename result_t>
MultiPipe &MultiPipe::add(FlatMap<tuple_t, result_t> &_flatmap)
{
    // check the type compatibility
    tuple_t t;
    std::string opInType = typeid(t).name();
    if (!outputType.empty() && outputType.compare(opInType) != 0) {
        std::cerr << RED << "WindFlow Error: output type from MultiPipe is not the input type of the FlatMap operator" << DEFAULT << std::endl;
        exit(EXIT_FAILURE);
    }
    // call the generic method to add the operator
    add_operator<Standard_Emitter<tuple_t>, Ordering_Node<tuple_t, tuple_t>>(&_flatmap, _flatmap.isKeyed() ? COMPLEX : SIMPLE, TS);
    // save the new output type from this MultiPipe
    result_t r;
    outputType = typeid(r).name();
	return *this;
}

// implementation of the method to chain a FlatMap to the MultiPipe
template<typename tuple_t, typename result_t>
MultiPipe &MultiPipe::chain(FlatMap<tuple_t, result_t> &_flatmap)
{
    // check the type compatibility
    tuple_t t;
    std::string opInType = typeid(t).name();
    if (!outputType.empty() && outputType.compare(opInType) != 0) {
        std::cerr << RED << "WindFlow Error: output type from MultiPipe is not the input type of the FlatMap operator" << DEFAULT << std::endl;
        exit(EXIT_FAILURE);
    }
    if (!_flatmap.isKeyed()) {
        bool chained = chain_operator<typename FlatMap<tuple_t, result_t>::FlatMap_Node>(&_flatmap);
        if (!chained)
            add(_flatmap);
    }
    else
        add(_flatmap);
    // save the new output type from this MultiPipe
    result_t r;
    outputType = typeid(r).name();
    return *this;
}

// implementation of the method to add an Accumulator to the MultiPipe
template<typename tuple_t, typename result_t>
MultiPipe &MultiPipe::add(Accumulator<tuple_t, result_t> &_acc)
{
    // check the type compatibility
    tuple_t t;
    std::string opInType = typeid(t).name();
    if (!outputType.empty() && outputType.compare(opInType) != 0) {
        std::cerr << RED << "WindFlow Error: output type from MultiPipe is not the input type of the Accumulator operator" << DEFAULT << std::endl;
        exit(EXIT_FAILURE);
    }
    // call the generic method to add the operator to the MultiPipe
    add_operator<Standard_Emitter<tuple_t>, Ordering_Node<tuple_t, tuple_t>>(&_acc, COMPLEX, TS);
    // save the new output type from this MultiPipe
    result_t r;
    outputType = typeid(r).name();
    return *this;
}

// implementation of the method to add a Win_Farm to the MultiPipe
template<typename tuple_t, typename result_t>
MultiPipe &MultiPipe::add(Win_Farm<tuple_t, result_t> &_wf)
{
    // check the type compatibility
    tuple_t t;
    std::string opInType = typeid(t).name();
    if (!outputType.empty() && outputType.compare(opInType) != 0) {
        std::cerr << RED << "WindFlow Error: output type from MultiPipe is not the input type of the Win_Farm operator" << DEFAULT << std::endl;
        exit(EXIT_FAILURE);
    }
    // check whether the Win_Farm has complex parallel replicas inside
    if (_wf.useComplexNesting()) {
        // check whether internal replicas have been prepared for nesting
        if (_wf.getOptLevel() != LEVEL2 || _wf.getInnerOptLevel() != LEVEL2) {
            std::cerr << RED << "WindFlow Error: tried a nesting without preparing the inner operator" << DEFAULT << std::endl;
            exit(EXIT_FAILURE);
        }
        else {
            // inner replica is a Pane_Farm
            if(_wf.getInnerType() == PF_CPU) {
                // check the parallelism degree of the PLQ stage
                if ((_wf.getInnerParallelism()).first > 1) {
                    if (_wf.getWinType() == TB) {
                        // call the generic method to add the operator to the MultiPipe
                        add_operator<Tree_Emitter, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_wf, COMPLEX, TS);
                    }
                    else {
                        // special case count-based windows
                        _wf.change_emitter(new Broadcast_Emitter<tuple_t>(_wf.getParallelism() * (_wf.getInnerParallelism()).first), true);
                        // call the generic method to add the operator to the MultiPipe
                        add_operator<Broadcast_Emitter<tuple_t>, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_wf, COMPLEX, TS_RENUMBERING);
                    }
                }
                else {
                    if (_wf.getWinType() == TB) {
                        // call the generic method to add the operator to the MultiPipe
                        add_operator<WF_Emitter<tuple_t>, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_wf, COMPLEX, TS);
                    }
                    else {
                        // special case count-based windows
                        _wf.change_emitter(new Broadcast_Emitter<tuple_t>(_wf.getParallelism()), true);
                        // call the generic method to add the operator to the MultiPipe
                        add_operator<Broadcast_Emitter<tuple_t>, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_wf, COMPLEX, TS_RENUMBERING);
                    }
                }
            }
            // inner replica is a Win_MapReduce
            else if(_wf.getInnerType() == WMR_CPU) {
                if (_wf.getWinType() == TB) {
                    // call the generic method to add the operator to the MultiPipe
                    add_operator<Tree_Emitter, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_wf, COMPLEX, TS);
                }
                else {
                    // special case count-based windows
                    _wf.change_emitter(new Broadcast_Emitter<tuple_t>(_wf.getParallelism() * (_wf.getInnerParallelism()).first), true);
                    size_t n_map = (_wf.getInnerParallelism()).first;
                    for (size_t i=0; i<_wf.getParallelism(); i++) {
                        ff::ff_pipeline *pipe = static_cast<ff::ff_pipeline *>((_wf.getWorkers())[i]);
                        auto &stages = pipe->getStages();
                        ff::ff_a2a *a2a = static_cast<ff::ff_a2a *>(stages[0]);
                        std::vector<ff::ff_node *> nodes;
                        for (size_t j=0; j<n_map; j++)
                            nodes.push_back(new WinMap_Dropper<tuple_t>(j, n_map));
                        combine_a2a_withFirstNodes(a2a, nodes, true);
                    }
                    // call the generic method to add the operator to the MultiPipe
                    add_operator<Broadcast_Emitter<tuple_t>, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_wf, COMPLEX, TS_RENUMBERING);
                }
            }
            forceShuffling = true;
        }
    }
    // case with Win_Seq replicas inside
    else {
        if (_wf.getWinType() == TB) {
            // call the generic method to add the operator to the MultiPipe
            add_operator<WF_Emitter<tuple_t>, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_wf, COMPLEX, TS);
        }
        else {
            // special case count-based windows
            _wf.change_emitter(new Broadcast_Emitter<tuple_t>(_wf.getParallelism()), true);
            // call the generic method to add the operator to the MultiPipe
            add_operator<Broadcast_Emitter<tuple_t>, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_wf, COMPLEX, TS_RENUMBERING);
        }
    }
    // save the new output type from this MultiPipe
    result_t r;
    outputType = typeid(r).name();
    return *this;
}

// implementation of the method to add a Win_Farm_GPU to the MultiPipe
template<typename tuple_t, typename result_t, typename F_t>
MultiPipe &MultiPipe::add(Win_Farm_GPU<tuple_t, result_t, F_t> &_wf)
{
    // check the type compatibility
    tuple_t t;
    std::string opInType = typeid(t).name();
    if (!outputType.empty() && outputType.compare(opInType) != 0) {
        std::cerr << RED << "WindFlow Error: output type from MultiPipe is not the input type of the Win_Farm_GPU operator" << DEFAULT << std::endl;
        exit(EXIT_FAILURE);
    }
    // check whether the Win_Farm_GPU has complex parallel replicas inside
    if (_wf.useComplexNesting()) {
        // check whether internal replicas have been prepared for nesting
        if (_wf.getOptLevel() != LEVEL2 || _wf.getInnerOptLevel() != LEVEL2) {
            std::cerr << RED << "WindFlow Error: tried a nesting without preparing the inner operator" << DEFAULT << std::endl;
            exit(EXIT_FAILURE);
        }
        else {
            // inner replica is a Pane_Farm_GPU
            if(_wf.getInnerType() == PF_GPU) {
                // check the parallelism degree of the PLQ stage
                if ((_wf.getInnerParallelism()).first > 1) {
                    if (_wf.getWinType() == TB) {
                        // call the generic method to add the operator to the MultiPipe
                        add_operator<Tree_Emitter, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_wf, COMPLEX, TS);
                    }
                    else {
                        // special case count-based windows
                        _wf.change_emitter(new Broadcast_Emitter<tuple_t>(_wf.getParallelism() * (_wf.getInnerParallelism()).first), true);
                        // call the generic method to add the operator to the MultiPipe
                        add_operator<Broadcast_Emitter<tuple_t>, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_wf, COMPLEX, TS_RENUMBERING);
                    }
                }
                else {
                    if (_wf.getWinType() == TB) {
                        // call the generic method to add the operator to the MultiPipe
                        add_operator<WF_Emitter<tuple_t>, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_wf, COMPLEX, TS);
                    }
                    else {
                        // special case count-based windows
                        _wf.change_emitter(new Broadcast_Emitter<tuple_t>(_wf.getParallelism()), true);
                        // call the generic method to add the operator to the MultiPipe
                        add_operator<Broadcast_Emitter<tuple_t>, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_wf, COMPLEX, TS_RENUMBERING);
                    }
                }
            }
            // inner replica is a Win_MapReduce_GPU
            else if(_wf.getInnerType() == WMR_GPU) {
                if (_wf.getWinType() == TB) {
                    // call the generic method to add the operator to the MultiPipe
                    add_operator<Tree_Emitter, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_wf, COMPLEX, TS);
                }
                else {
                    // special case count-based windows
                    _wf.change_emitter(new Broadcast_Emitter<tuple_t>(_wf.getParallelism() * (_wf.getInnerParallelism()).first), true);
                    size_t n_map = (_wf.getInnerParallelism()).first;
                    for (size_t i=0; i<_wf.getParallelism(); i++) {
                        ff::ff_pipeline *pipe = static_cast<ff::ff_pipeline *>((_wf.getWorkers())[i]);
                        auto &stages = pipe->getStages();
                        ff::ff_a2a *a2a = static_cast<ff::ff_a2a *>(stages[0]);
                        std::vector<ff::ff_node *> nodes;
                        for (size_t j=0; j<n_map; j++)
                            nodes.push_back(new WinMap_Dropper<tuple_t>(j, n_map));
                        combine_a2a_withFirstNodes(a2a, nodes, true);
                    }
                    // call the generic method to add the operator to the MultiPipe
                    add_operator<Broadcast_Emitter<tuple_t>, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_wf, COMPLEX, TS_RENUMBERING);
                }
            }
            forceShuffling = true;
        }
    }
    // case with Win_Seq_GPU replicas inside
    else {
        if (_wf.getWinType() == TB) {
            // call the generic method to add the operator to the MultiPipe
            add_operator<WF_Emitter<tuple_t>, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_wf, COMPLEX, TS);
        }
        else {
            // special case count-based windows
            _wf.change_emitter(new Broadcast_Emitter<tuple_t>(_wf.getParallelism()), true);
            // call the generic method to add the operator to the MultiPipe
            add_operator<Broadcast_Emitter<tuple_t>, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_wf, COMPLEX, TS_RENUMBERING);
        }
    }
    // save the new output type from this MultiPipe
    result_t r;
    outputType = typeid(r).name();
    return *this;
}

// implementation of the method to add a Key_Farm to the MultiPipe
template<typename tuple_t, typename result_t>
MultiPipe &MultiPipe::add(Key_Farm<tuple_t, result_t> &_kf)
{
    // check the type compatibility
    tuple_t t;
    std::string opInType = typeid(t).name();
    if (!outputType.empty() && outputType.compare(opInType) != 0) {
        std::cerr << RED << "WindFlow Error: output type from MultiPipe is not the input type of the Key_Farm operator" << DEFAULT << std::endl;
        exit(EXIT_FAILURE);
    }
    // check whether the Key_Farm has complex parallel replicas inside
    if (_kf.useComplexNesting()) {
        // check whether internal replicas have been prepared for nesting
        if (_kf.getOptLevel() != LEVEL2 || _kf.getInnerOptLevel() != LEVEL2) {
            std::cerr << RED << "WindFlow Error: tried a nesting without preparing the inner operator" << DEFAULT << std::endl;
            exit(EXIT_FAILURE);
        }
        else {
            // inner replica is a Pane_Farm
            if(_kf.getInnerType() == PF_CPU) {
                // check the parallelism degree of the PLQ stage
                if ((_kf.getInnerParallelism()).first > 1) {
                    if (_kf.getWinType() == TB) {
                        // call the generic method to add the operator to the MultiPipe
                        add_operator<Tree_Emitter, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_kf, COMPLEX, TS);
                    }
                    else {
                        // special case count-based windows
                        auto *emitter = static_cast<Tree_Emitter *>(_kf.getEmitter());
                        KF_Emitter<tuple_t> *rootnode = new KF_Emitter<tuple_t>(*(static_cast<KF_Emitter<tuple_t> *>(emitter->getRootNode())));
                        std::vector<Basic_Emitter *> children;
                        size_t n_plq = (_kf.getInnerParallelism()).first;
                        for (size_t i=0; i<_kf.getParallelism(); i++) {
                            auto *b_node = new Broadcast_Emitter<tuple_t>(n_plq);
                            b_node->setTree_EmitterMode(true);
                            children.push_back(b_node);
                        }
                        auto *new_emitter = new Tree_Emitter(rootnode, children, true, true);
                        _kf.change_emitter(new_emitter, true);
                        // call the generic method to add the operator to the MultiPipe
                        add_operator<Tree_Emitter, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_kf, COMPLEX, TS_RENUMBERING);
                    }
                }
                else {
                    // call the generic method to add the operator to the MultiPipe
                    add_operator<KF_Emitter<tuple_t>, Ordering_Node<tuple_t>>(&_kf, COMPLEX, (_kf.getWinType() == TB) ? TS : TS_RENUMBERING);
                }
            }
            // inner replica is a Win_MapReduce
            else if(_kf.getInnerType() == WMR_CPU) {
                if (_kf.getWinType() == TB) {
                    // call the generic method to add the operator to the MultiPipe
                    add_operator<Tree_Emitter, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_kf, COMPLEX, TS);
                }
                else {
                    // special case count-based windows
                    auto *emitter = static_cast<Tree_Emitter *>(_kf.getEmitter());
                    KF_Emitter<tuple_t> *rootnode = new KF_Emitter<tuple_t>(*(static_cast<KF_Emitter<tuple_t> *>(emitter->getRootNode())));
                    std::vector<Basic_Emitter *> children;
                    size_t n_map = (_kf.getInnerParallelism()).first;
                    for (size_t i=0; i<_kf.getParallelism(); i++) {
                        auto *b_node = new Broadcast_Emitter<tuple_t>(n_map);
                        b_node->setTree_EmitterMode(true);
                        children.push_back(b_node);
                    }
                    auto *new_emitter = new Tree_Emitter(rootnode, children, true, true);
                    _kf.change_emitter(new_emitter, true);
                    for (size_t i=0; i<_kf.getParallelism(); i++) {
                        ff::ff_pipeline *pipe = static_cast<ff::ff_pipeline *>((_kf.getWorkers())[i]);
                        auto &stages = pipe->getStages();
                        ff::ff_a2a *a2a = static_cast<ff::ff_a2a *>(stages[0]);
                        std::vector<ff::ff_node *> nodes;
                        for (size_t j=0; j<n_map; j++)
                            nodes.push_back(new WinMap_Dropper<tuple_t>(j, n_map));
                        combine_a2a_withFirstNodes(a2a, nodes, true);
                    }
                    // call the generic method to add the operator to the MultiPipe
                    add_operator<Tree_Emitter, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_kf, COMPLEX, TS_RENUMBERING);
                }
            }
            forceShuffling = true;
        }
    }
    // case with Win_Seq replicas inside
    else {
        // call the generic method to add the operator to the MultiPipe
        add_operator<KF_Emitter<tuple_t>, Ordering_Node<tuple_t>>(&_kf, COMPLEX, (_kf.getWinType() == TB) ? TS : TS_RENUMBERING);
    }
    // save the new output type from this MultiPipe
    result_t r;
    outputType = typeid(r).name();
    return *this;
}

// implementation of the method to add a Key_Farm_GPU to the MultiPipe
template<typename tuple_t, typename result_t, typename F_t>
MultiPipe &MultiPipe::add(Key_Farm_GPU<tuple_t, result_t, F_t> &_kf)
{
    // check the type compatibility
    tuple_t t;
    std::string opInType = typeid(t).name();
    if (!outputType.empty() && outputType.compare(opInType) != 0) {
        std::cerr << RED << "WindFlow Error: output type from MultiPipe is not the input type of the Key_Farm_GPU operator" << DEFAULT << std::endl;
        exit(EXIT_FAILURE);
    }
    // check whether the Key_Farm_GPU has complex parallel replicas inside
    if (_kf.useComplexNesting()) {
        // check whether internal replicas have been prepared for nesting
        if (_kf.getOptLevel() != LEVEL2 || _kf.getInnerOptLevel() != LEVEL2) {
            std::cerr << RED << "WindFlow Error: tried a nesting without preparing the inner operator" << DEFAULT << std::endl;
            exit(EXIT_FAILURE);
        }
        else {
            // inner replica is a Pane_Farm_GPU
            if(_kf.getInnerType() == PF_GPU) {
                // check the parallelism degree of the PLQ stage
                if ((_kf.getInnerParallelism()).first > 1) {
                    if (_kf.getWinType() == TB) {
                        // call the generic method to add the operator to the MultiPipe
                        add_operator<Tree_Emitter, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_kf, COMPLEX, TS);
                    }
                    else {
                        // special case count-based windows
                        auto *emitter = static_cast<Tree_Emitter *>(_kf.getEmitter());
                        KF_Emitter<tuple_t> *rootnode = new KF_Emitter<tuple_t>(*(static_cast<KF_Emitter<tuple_t> *>(emitter->getRootNode())));
                        std::vector<Basic_Emitter *> children;
                        size_t n_plq = (_kf.getInnerParallelism()).first;
                        for (size_t i=0; i<_kf.getParallelism(); i++) {
                            auto *b_node = new Broadcast_Emitter<tuple_t>(n_plq);
                            b_node->setTree_EmitterMode(true);
                            children.push_back(b_node);
                        }
                        auto *new_emitter = new Tree_Emitter(rootnode, children, true, true);
                        _kf.change_emitter(new_emitter, true);
                        // call the generic method to add the operator to the MultiPipe
                        add_operator<Tree_Emitter, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_kf, COMPLEX, TS_RENUMBERING);
                    }
                }
                else {
                    // call the generic method to add the operator to the MultiPipe
                    add_operator<KF_Emitter<tuple_t>, Ordering_Node<tuple_t>>(&_kf, COMPLEX, (_kf.getWinType() == TB) ? TS : TS_RENUMBERING);
                }
            }
            // inner replica is a Win_MapReduce_GPU
            else if(_kf.getInnerType() == WMR_GPU) {
                if (_kf.getWinType() == TB) {
                    // call the generic method to add the operator to the MultiPipe
                    add_operator<Tree_Emitter, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_kf, COMPLEX, TS);
                }
                else {
                    // special case count-based windows
                    auto *emitter = static_cast<Tree_Emitter *>(_kf.getEmitter());
                    KF_Emitter<tuple_t> *rootnode = new KF_Emitter<tuple_t>(*(static_cast<KF_Emitter<tuple_t> *>(emitter->getRootNode())));
                    std::vector<Basic_Emitter *> children;
                    size_t n_map = (_kf.getInnerParallelism()).first;
                    for (size_t i=0; i<_kf.getParallelism(); i++) {
                        auto *b_node = new Broadcast_Emitter<tuple_t>(n_map);
                        b_node->setTree_EmitterMode(true);
                        children.push_back(b_node);
                    }
                    auto *new_emitter = new Tree_Emitter(rootnode, children, true, true);
                    _kf.change_emitter(new_emitter, true);
                    for (size_t i=0; i<_kf.getParallelism(); i++) {
                        ff::ff_pipeline *pipe = static_cast<ff::ff_pipeline *>((_kf.getWorkers())[i]);
                        auto &stages = pipe->getStages();
                        ff::ff_a2a *a2a = static_cast<ff::ff_a2a *>(stages[0]);
                        std::vector<ff::ff_node *> nodes;
                        for (size_t j=0; j<n_map; j++)
                            nodes.push_back(new WinMap_Dropper<tuple_t>(j, n_map));
                        combine_a2a_withFirstNodes(a2a, nodes, true);
                    }
                    // call the generic method to add the operator to the MultiPipe
                    add_operator<Tree_Emitter, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(&_kf, COMPLEX, TS_RENUMBERING);
                }
            }
            forceShuffling = true;
        }
    }
    // case with Win_Seq_GPU replicas inside
    else {
        // call the generic method to add the operator to the MultiPipe
        add_operator<KF_Emitter<tuple_t>, Ordering_Node<tuple_t>>(&_kf, COMPLEX, (_kf.getWinType() == TB) ? TS : TS_RENUMBERING);
    }
    // save the new output type from this MultiPipe
    result_t r;
    outputType = typeid(r).name();
    return *this;
}

// implementation of the method to add a Pane_Farm to the MultiPipe
template<typename tuple_t, typename result_t>
MultiPipe &MultiPipe::add(Pane_Farm<tuple_t, result_t> &_pf)
{
    // check the type compatibility
    tuple_t t;
    std::string opInType = typeid(t).name();
    if (!outputType.empty() && outputType.compare(opInType) != 0) {
        std::cerr << RED << "WindFlow Error: output type from MultiPipe is not the input type of the Pane_Farm operator" << DEFAULT << std::endl;
        exit(EXIT_FAILURE);
    }
    // check whether the Pane_Farm has been prepared to be nested
    if (_pf.getOptLevel() != LEVEL0) {
        std::cerr << RED << "WindFlow Error: Pane_Farm has been prepared for nesting, it cannot be added directly to a MultiPipe" << DEFAULT << std::endl;
        exit(EXIT_FAILURE);
    }
    ff::ff_pipeline *pipe = static_cast<ff::ff_pipeline *>(&_pf);
    const ff::svector<ff::ff_node *> &stages = pipe->getStages();
    ff::ff_farm *plq = nullptr;
    // check if the PLQ stage is a farm, otherwise prepare it
    if (!stages[0]->isFarm()) {
        plq = new ff::ff_farm();
        std::vector<ff::ff_node *> w;
        w.push_back(stages[0]); // there is for sure one single worker in the PLQ
        plq->add_emitter(new Standard_Emitter<tuple_t>(1));
        plq->add_workers(w);
        plq->add_collector(nullptr);
        plq->cleanup_emitter(true);
        plq->cleanup_workers(false);
        // call the generic method to add the operator (PLQ stage) to the MultiPipe
        add_operator<Standard_Emitter<tuple_t>, Ordering_Node<tuple_t, tuple_t>>(plq, COMPLEX, (_pf.getWinType() == TB) ? TS : TS_RENUMBERING);
        delete plq;
    }
    else {
        plq = static_cast<ff::ff_farm *>(stages[0]);
        // check the type of the windows
        if (_pf.getWinType() == TB) { // time-based windows
            // call the generic method to add the operator (PLQ stage) to the MultiPipe
            add_operator<WF_Emitter<tuple_t>, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(plq, COMPLEX, TS);
        }
        else {
            // special case count-based windows
            size_t n_plq = (plq->getWorkers()).size();
            plq->change_emitter(new Broadcast_Emitter<tuple_t>(n_plq), true);
            // call the generic method to add the operator
            add_operator<Broadcast_Emitter<tuple_t>, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(plq, COMPLEX, TS_RENUMBERING);
        }
    }
    ff::ff_farm *wlq = nullptr;
    // check if the WLQ stage is a farm, otherwise prepare it
    if (!stages[1]->isFarm()) {
        wlq = new ff::ff_farm();
        std::vector<ff::ff_node *> w;
        w.push_back(stages[1]); // there is for sure one single worker in the WLQ
        wlq->add_emitter(new Standard_Emitter<result_t>(1));
        wlq->add_workers(w);
        wlq->add_collector(nullptr);
        wlq->cleanup_emitter(true);
        wlq->cleanup_workers(false);
        // call the generic method to add the operator (WLQ stage) to the MultiPipe
        add_operator<Standard_Emitter<result_t>, Ordering_Node<result_t, result_t>>(wlq, COMPLEX, ID);
        delete wlq;
    }
    else {
        wlq = static_cast<ff::ff_farm *>(stages[1]);
        // call the generic method to add the operator (WLQ stage) to the MultiPipe
        add_operator<WF_Emitter<result_t>, Ordering_Node<result_t, wrapper_tuple_t<result_t>>>(wlq, COMPLEX, ID);
    }
    // save the new output type from this MultiPipe
    result_t r;
    outputType = typeid(r).name();
    return *this;
}

// implementation of the method to add a Pane_Farm_GPU to the MultiPipe
template<typename tuple_t, typename result_t, typename F_t>
MultiPipe &MultiPipe::add(Pane_Farm_GPU<tuple_t, result_t, F_t> &_pf)
{
    // check the type compatibility
    tuple_t t;
    std::string opInType = typeid(t).name();
    if (!outputType.empty() && outputType.compare(opInType) != 0) {
        std::cerr << RED << "WindFlow Error: output type from MultiPipe is not the input type of the Pane_Farm_GPU operator" << DEFAULT << std::endl;
        exit(EXIT_FAILURE);
    }
    // check whether the Pane_Farm_GPU has been prepared to be nested
    if (_pf.getOptLevel() != LEVEL0) {
        std::cerr << RED << "WindFlow Error: Pane_Farm_GPU has been prepared for nesting, it cannot be added directly to a MultiPipe" << DEFAULT << std::endl;
        exit(EXIT_FAILURE);
    }
    ff::ff_pipeline *pipe = static_cast<ff::ff_pipeline *>(&_pf);
    const ff::svector<ff::ff_node *> &stages = pipe->getStages();
    ff::ff_farm *plq = nullptr;
    // check if the PLQ stage is a farm, otherwise prepare it
    if (!stages[0]->isFarm()) {
        plq = new ff::ff_farm();
        std::vector<ff::ff_node *> w;
        w.push_back(stages[0]); // there is for sure one single worker in the PLQ
        plq->add_emitter(new Standard_Emitter<tuple_t>(1));
        plq->add_workers(w);
        plq->add_collector(nullptr);
        plq->cleanup_emitter(true);
        plq->cleanup_workers(false);
        // call the generic method to add the operator (PLQ stage) to the MultiPipe
        add_operator<Standard_Emitter<tuple_t>, Ordering_Node<tuple_t, tuple_t>>(plq, COMPLEX, (_pf.getWinType() == TB) ? TS : TS_RENUMBERING);
        delete plq;
    }
    else {
        plq = static_cast<ff::ff_farm *>(stages[0]);
        // check the type of the windows
        if (_pf.getWinType() == TB) { // time-based windows
            // call the generic method to add the operator (PLQ stage) to the MultiPipe
            add_operator<WF_Emitter<tuple_t>, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(plq, COMPLEX, TS);
        }
        else {
            // special case count-based windows
            size_t n_plq = (plq->getWorkers()).size();
            plq->change_emitter(new Broadcast_Emitter<tuple_t>(n_plq), true);
            // call the generic method to add the operator
            add_operator<Broadcast_Emitter<tuple_t>, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(plq, COMPLEX, TS_RENUMBERING);
        }
    }
    ff::ff_farm *wlq = nullptr;
    // check if the WLQ stage is a farm, otherwise prepare it
    if (!stages[1]->isFarm()) {
        wlq = new ff::ff_farm();
        std::vector<ff::ff_node *> w;
        w.push_back(stages[1]); // there is for sure one single worker in the WLQ
        wlq->add_emitter(new Standard_Emitter<result_t>(1));
        wlq->add_workers(w);
        wlq->add_collector(nullptr);
        wlq->cleanup_emitter(true);
        wlq->cleanup_workers(false);
        // call the generic method to add the operator (WLQ stage) to the MultiPipe
        add_operator<Standard_Emitter<result_t>, Ordering_Node<result_t, result_t>>(wlq, COMPLEX, ID);
        delete wlq;
    }
    else {
        wlq = static_cast<ff::ff_farm *>(stages[1]);
        // call the generic method to add the operator (WLQ stage) to the MultiPipe
        add_operator<WF_Emitter<result_t>, Ordering_Node<result_t, wrapper_tuple_t<result_t>>>(wlq, COMPLEX, ID);
    }
    // save the new output type from this MultiPipe
    result_t r;
    outputType = typeid(r).name();
    return *this;
}

// implementation of the method to add a Win_MapReduce to the MultiPipe
template<typename tuple_t, typename result_t>
MultiPipe &MultiPipe::add(Win_MapReduce<tuple_t, result_t> &_wmr)
{
    // check the type compatibility
    tuple_t t;
    std::string opInType = typeid(t).name();
    if (!outputType.empty() && outputType.compare(opInType) != 0) {
        std::cerr << RED << "WindFlow Error: output type from MultiPipe is not the input type of the Win_MapReduce operator" << DEFAULT << std::endl;
        exit(EXIT_FAILURE);
    }
    // check whether the Win_MapReduce has been prepared to be nested
    if (_wmr.getOptLevel() != LEVEL0) {
        std::cerr << RED << "WindFlow Error: Win_MapReduce has been prepared for nesting, it cannot be added directly to a MultiPipe" << DEFAULT << std::endl;
        exit(EXIT_FAILURE);
    }
    // add the MAP stage
    ff::ff_pipeline *pipe = static_cast<ff::ff_pipeline *>(&_wmr);
    const ff::svector<ff::ff_node *> &stages = pipe->getStages();
    ff::ff_farm *map = static_cast<ff::ff_farm *>(stages[0]);
    // check the type of the windows
    if (_wmr.getWinType() == TB) { // time-based windows
        // call the generic method to add the operator (MAP stage) to the MultiPipe
        add_operator<WinMap_Emitter<tuple_t>, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(map, COMPLEX, TS);
    }
    else {
        // special case count-based windows
        size_t n_map = (map->getWorkers()).size();
        ff::ff_farm *new_map = new ff::ff_farm();
        auto worker_set = map->getWorkers();
        std::vector<ff::ff_node *> w;
        new_map->add_emitter(new Broadcast_Emitter<tuple_t>(n_map));
        for (size_t i=0; i<n_map; i++) {
            ff::ff_comb *comb = new ff::ff_comb(new WinMap_Dropper<tuple_t>(i, n_map), worker_set[i], true, false);
            w.push_back(comb);
        }
        new_map->add_workers(w);
        new_map->add_collector(nullptr);
        new_map->cleanup_emitter(true);
        new_map->cleanup_workers(false);
        // call the generic method to add the operator (MAP stage) to the MultiPipe
        add_operator<Broadcast_Emitter<tuple_t>, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(new_map, COMPLEX, TS_RENUMBERING);
        delete new_map;
    }
    // add the REDUCE stage
    ff::ff_farm *reduce = nullptr;
    // check if the REDUCE stage is a farm, otherwise prepare it
    if (!stages[1]->isFarm()) {
        reduce = new ff::ff_farm();
        std::vector<ff::ff_node *> w;
        w.push_back(stages[1]);
        reduce->add_emitter(new Standard_Emitter<result_t>(1));
        reduce->add_workers(w);
        reduce->add_collector(nullptr);
        reduce->cleanup_emitter(true);
        reduce->cleanup_workers(false);
        // call the generic method to add the operator (REDUCE stage) to the MultiPipe
        add_operator<Standard_Emitter<result_t>, Ordering_Node<result_t, result_t>>(reduce, COMPLEX, ID);
        delete reduce;
    }
    else {
        reduce = static_cast<ff::ff_farm *>(stages[1]);
        // call the generic method to add the operator (REDUCE stage) to the MultiPipe
        add_operator<WF_Emitter<result_t>, Ordering_Node<result_t, wrapper_tuple_t<result_t>>>(reduce, COMPLEX, ID);
    }
    // save the new output type from this MultiPipe
    result_t r;
    outputType = typeid(r).name();
    return *this;
}

// implementation of the method to add a Win_MapReduce_GPU to the MultiPipe
template<typename tuple_t, typename result_t, typename F_t>
MultiPipe &MultiPipe::add(Win_MapReduce_GPU<tuple_t, result_t, F_t> &_wmr)
{
    // check the type compatibility
    tuple_t t;
    std::string opInType = typeid(t).name();
    if (!outputType.empty() && outputType.compare(opInType) != 0) {
        std::cerr << RED << "WindFlow Error: output type from MultiPipe is not the input type of the Win_MapReduce_GPU operator" << DEFAULT << std::endl;
        exit(EXIT_FAILURE);
    }
    // check whether the Win_MapReduce_GPU has been prepared to be nested
    if (_wmr.getOptLevel() != LEVEL0) {
        std::cerr << RED << "WindFlow Error: Win_MapReduce_GPU has been prepared for nesting, it cannot be added directly to a MultiPipe" << DEFAULT << std::endl;
        exit(EXIT_FAILURE);
    }
    // add the MAP stage
    ff::ff_pipeline *pipe = static_cast<ff::ff_pipeline *>(&_wmr);
    const ff::svector<ff::ff_node *> &stages = pipe->getStages();
    ff::ff_farm *map = static_cast<ff::ff_farm *>(stages[0]);
    // check the type of the windows
    if (_wmr.getWinType() == TB) { // time-based windows
        // call the generic method to add the operator (MAP stage) to the MultiPipe
        add_operator<WinMap_Emitter<tuple_t>, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(map, COMPLEX, TS);
    }
    else {
        // special case count-based windows
        size_t n_map = (map->getWorkers()).size();
        ff::ff_farm *new_map = new ff::ff_farm();
        auto worker_set = map->getWorkers();
        std::vector<ff::ff_node *> w;
        new_map->add_emitter(new Broadcast_Emitter<tuple_t>(n_map));
        for (size_t i=0; i<n_map; i++) {
            ff::ff_comb *comb = new ff::ff_comb(new WinMap_Dropper<tuple_t>(i, n_map), worker_set[i], true, false);
            w.push_back(comb);
        }
        new_map->add_workers(w);
        new_map->add_collector(nullptr);
        new_map->cleanup_emitter(true);
        new_map->cleanup_workers(false);
        // call the generic method to add the operator (MAP stage) to the MultiPipe
        add_operator<Broadcast_Emitter<tuple_t>, Ordering_Node<tuple_t, wrapper_tuple_t<tuple_t>>>(new_map, COMPLEX, TS_RENUMBERING);
        delete new_map;
    }
    // add the REDUCE stage
    ff::ff_farm *reduce = nullptr;
    // check if the REDUCE stage is a farm, otherwise prepare it
    if (!stages[1]->isFarm()) {
        reduce = new ff::ff_farm();
        std::vector<ff::ff_node *> w;
        w.push_back(stages[1]);
        reduce->add_emitter(new Standard_Emitter<result_t>(1));
        reduce->add_workers(w);
        reduce->add_collector(nullptr);
        reduce->cleanup_emitter(true);
        reduce->cleanup_workers(false);
        // call the generic method to add the operator (REDUCE stage) to the MultiPipe
        add_operator<Standard_Emitter<result_t>, Ordering_Node<result_t, result_t>>(reduce, COMPLEX, ID);
        delete reduce;
    }
    else {
        reduce = static_cast<ff::ff_farm *>(stages[1]);
        // call the generic method to add the operator (REDUCE stage) to the MultiPipe
        add_operator<WF_Emitter<result_t>, Ordering_Node<result_t, wrapper_tuple_t<result_t>>>(reduce, COMPLEX, ID);
    }
    // save the new output type from this MultiPipe
    result_t r;
    outputType = typeid(r).name();
    return *this;
}

// implementation of the method to add a Sink to the MultiPipe
template<typename tuple_t>
MultiPipe &MultiPipe::add_sink(Sink<tuple_t> &_sink)
{
    // check the type compatibility
    tuple_t t;
    std::string opInType = typeid(t).name();
    if (!outputType.empty() && outputType.compare(opInType) != 0) {
        std::cerr << RED << "WindFlow Error: output type from MultiPipe is not the input type of the Sink operator" << DEFAULT << std::endl;
        exit(EXIT_FAILURE);
    }
    // call the generic method to add the operator to the MultiPipe
    add_operator<Standard_Emitter<tuple_t>, Ordering_Node<tuple_t, tuple_t>>(&_sink, _sink.isKeyed() ? COMPLEX : SIMPLE, TS);
	has_sink = true;
    // save the new output type from this MultiPipe
    outputType = opInType;
	return *this;
}

// implementation of the method to chain a Sink to the MultiPipe
template<typename tuple_t>
MultiPipe &MultiPipe::chain_sink(Sink<tuple_t> &_sink)
{
    // check the type compatibility
    tuple_t t;
    std::string opInType = typeid(t).name();
    if (!outputType.empty() &&outputType.compare(opInType) != 0) {
        std::cerr << RED << "WindFlow Error: output type from MultiPipe is not the input type of the Sink operator" << DEFAULT << std::endl;
        exit(EXIT_FAILURE);
    }
    // try to chain the Sink with the MultiPipe
    if (!_sink.isKeyed()) {
        bool chained = chain_operator<typename Sink<tuple_t>::Sink_Node>(&_sink);
        if (!chained)
            return add_sink(_sink);
    }
    else
        return add_sink(_sink);
    has_sink = true;
    // save the new output type from this MultiPipe
    outputType = opInType;
    return *this;
}

// implementation of the method to merge this with a set of MultiPipe instances _pipes
template<typename ...MULTIPIPES>
MultiPipe &MultiPipe::merge(MULTIPIPES&... _pipes)
{
    auto mergeSet = prepareMergeSet(*this, _pipes...);
    // at least two MultiPipe instances can be merged
    if (mergeSet.size() < 2) {
        std::cerr << RED << "WindFlow Error: merge must be applied to at least two MultiPipe instances" << DEFAULT << std::endl;
        exit(EXIT_FAILURE);
    }
    // check duplicates
    std::map<MultiPipe *, int> counters;
    for (auto *mp: mergeSet) {
        if (counters.find(mp) == counters.end())
            counters.insert(std::make_pair(mp, 1));
        else {
            std::cerr << RED << "WindFlow Error: a MultiPipe cannot be merged with itself" << DEFAULT << std::endl;
            exit(EXIT_FAILURE);
        }
    }
    // check that all the MultiPipe instances to be merged have the same output type
    std::string outT = mergeSet[0]->outputType;
    for (auto *mp: mergeSet) {
        if (outT.compare(mp->outputType) != 0) {
            std::cerr << RED << "WindFlow Error: MultiPipe instances to be merged must have the same output type" << DEFAULT << std::endl;
            exit(EXIT_FAILURE);
        }
    }
    // execute the merge through the PipeGraph
    MultiPipe *mergedMP = graph->execute_Merge(mergeSet);
    return *mergedMP;
}

// implementation of the method to split this into several MultiPipe instances
template<typename F_t>
MultiPipe &MultiPipe::split(F_t _splitting_func, size_t _cardinality)
{
    // check whether the MultiPipe has been merged
    if (isMerged) {
        std::cerr << RED << "WindFlow Error: MultiPipe has been merged and cannot be split" << DEFAULT << std::endl;
        exit(EXIT_FAILURE);
    }
    // check whether the MultiPipe has been already split
    if (isSplit) {
        std::cerr << RED << "WindFlow Error: MultiPipe has been already split" << DEFAULT << std::endl;
        exit(EXIT_FAILURE);
    }
    // prepare the splitting of this
    splittingBranches = _cardinality;
    splittingEmitterRoot = new Splitting_Emitter<decltype(get_tuple_split_t(_splitting_func))>(_splitting_func, _cardinality);
    // execute the merge through the PipeGraph
    splittingChildren = graph->execute_Split(this);
    return *this;
}

// implementation of the method to get the MultiPipe with index _idx from this (this must have been split before)
inline MultiPipe &MultiPipe::select(size_t _idx) const
{
    // check whether the MultiPipe has been merged
    if (isMerged) {
        std::cerr << RED << "WindFlow Error: MultiPipe has been merged, select() cannot be executed" << DEFAULT << std::endl;
        exit(EXIT_FAILURE);
    }
    if (!isSplit) {
        std::cerr << RED << "WindFlow Error: MultiPipe has not been split, select() cannot be executed" << DEFAULT << std::endl;
        exit(EXIT_FAILURE);
    }
    if (_idx >= splittingChildren.size()) {
        std::cerr << RED << "WindFlow Error: index of select() is out of range" << DEFAULT << std::endl;
        exit(EXIT_FAILURE);
    }
    return *(splittingChildren[_idx]);
}

// implementation of the method to check whether a MultiPipe is runnable
inline bool MultiPipe::isRunnable() const
{
	return (has_source && has_sink) || (isMerged) || (isSplit);
}

// implementation of the method to check whether a MultiPipe has a Source
inline bool MultiPipe::hasSource() const
{
	return has_source;
}

// implementation of the method to check whether a MultiPipe has a Sink
inline bool MultiPipe::hasSink() const
{
	return has_sink;
}

// implementation of the method to return the number of threads used to run this MultiPipe
inline size_t MultiPipe::getNumThreads() const
{
    if (!isSplit)
        return this->cardinality()-1;
    else {
        size_t n = 0;
        auto first_set = last->getFirstSet();
        for (size_t i=0; i<first_set.size(); i++) {
            ff::ff_pipeline *pipe = static_cast<ff::ff_pipeline *>(first_set[i]);
            n += pipe->cardinality();
        }
        auto second_set = last->getSecondSet();
        for (size_t i=0; i<second_set.size(); i++) {
            MultiPipe *mp = static_cast<MultiPipe *>(second_set[i]);
            n += mp->getNumThreads();
        }
        return n;
    }
}

//@endcond

} // namespace wf

#endif
