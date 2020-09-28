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
 *  @brief PipeGraph construct
 *  
 *  @section PipeGraph (Description)
 *  
 *  This file implements the PipeGraph, the "streaming environment" used to create
 *  several MultiPipe instances with different sources. To run the application the
 *  users have to run the PipeGraph object.
 */ 

#ifndef PIPEGRAPH_H
#define PIPEGRAPH_H

/// includes
#include<map>
#include<string>
#include<vector>
#include<random>
#include<thread>
#include<typeinfo>
#include<algorithm>
#include<math.h>
#include<ff/ff.hpp>
#if defined (TRACE_WINDFLOW)
    #include<graphviz/gvc.h>
    #include<rapidjson/prettywriter.h>
#endif
#include<basic.hpp>
#include<wm_nodes.hpp>
#include<wf_nodes.hpp>
#include<kf_nodes.hpp>
#include<kslack_node.hpp>
#include<tree_emitter.hpp>
#include<basic_emitter.hpp>
#include<ordering_node.hpp>
#include<basic_operator.hpp>
#include<transformations.hpp>
#include<standard_emitter.hpp>
#include<splitting_emitter.hpp>
#include<broadcast_emitter.hpp>

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
            parent(_parent) {}
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
    // friendship with the MultiPipe class and with some functions
    friend class MultiPipe;
    friend inline MultiPipe *merge_multipipes_func(PipeGraph *, std::vector<MultiPipe *>);
    friend inline std::vector<MultiPipe *> split_multipipe_func(PipeGraph *, MultiPipe *);
    std::string name; // name of the PipeGraph
    AppNode *root; // pointer to the root of the Application Tree
    std::vector<MultiPipe *> toBeDeteled; // vector of MultiPipe instances to be deleted
    Mode mode; // processing mode of the PipeGraph
    bool started; // flag stating whether the PipeGraph has already been started
    bool ended; // flag stating whether the PipeGraph has completed its processing
    std::vector<std::reference_wrapper<Basic_Operator>> listOperators;// sequence of operators that have been added/chained within this PipeGraph
    std::atomic<unsigned long> atomic_num_dropped;
#if defined (TRACE_WINDFLOW)
    GVC_t *gvc; // pointer to the GVC environment
    Agraph_t *gv_graph; // pointer to the graphviz representation of the PipeGraph
    std::thread mt_thread; // object representing the monitoring thread
#endif

    // method to find the AppNode containing the MultiPipe _mp in the tree rooted at _node
    AppNode *find_AppNode(AppNode *_node, MultiPipe *_mp)
    {
        // base case
        if (_node->mp == _mp) {
            return _node;
        }
        // recursive case
        else {
            AppNode *found = nullptr;
            for (auto *child: _node->children) {
                found = find_AppNode(child, _mp);
                if (found != nullptr) {
                    return found;
                }
            }
            return nullptr;
        }
    }

    // method to find the list of AppNode instances that are leaves of the tree rooted at _node
    std::vector<AppNode *> get_LeavesList(AppNode *_node)
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

    // method to find the LCA of a set of _leaves starting from _node
    AppNode *get_LCA(AppNode *_node, std::vector<AppNode *> _leaves)
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
            if (foundAll) {
                return get_LCA(child, _leaves);
            }
        }
        return _node;
    }

    // method to delete all the AppNode instances in the tree rooted at _node
    void delete_AppNodes(AppNode *_node)
    {
        // base case
        if ((_node->children).size() == 0) {
            delete _node;
        }
        // recursive case
        else {
            for (auto *child: _node->children) {
                delete_AppNodes(child);
            }
            delete _node;
        }
    }

    // method to prepare the right list of AppNode instances to be merged (case merge-ind and merge-full)
    bool get_MergedNodes1(std::vector<MultiPipe *> _toBeMerged, std::vector<AppNode *> &_rightList)
    {
        // all the input MultiPipe instances must be leaves of the Application Tree
        std::vector<AppNode *> inputNodes;
        assert(_toBeMerged.size() > 1); // redundant check
        for (auto *mp: _toBeMerged) {
            AppNode *node = find_AppNode(root, mp);
            if (node == nullptr) {
                std::cerr << RED << "WindFlow Error: MultiPipe to be merged does not belong to this PipeGraph" << DEFAULT_COLOR << std::endl;
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

    // method to prepare the right list of AppNode instances to be merged (case merge-partial)
    AppNode *get_MergedNodes2(std::vector<MultiPipe *> _toBeMerged, std::vector<AppNode *> &_rightList)
    {
        // all the input MultiPipe instances must be leaves of the Application Tree
        std::vector<AppNode *> inputNodes;
        assert(_toBeMerged.size() > 1); // redundant check
        for (auto *mp: _toBeMerged) {
            AppNode *node = find_AppNode(root, mp);
            if (node == nullptr) {
                std::cerr << RED << "WindFlow Error: MultiPipe to be merged does not belong to this PipeGraph" << DEFAULT_COLOR << std::endl;
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
                    if (std::find(inputNodes.begin(), inputNodes.end(), leaf) == inputNodes.end()) {
                        foundAll = false;
                    }
                    else {
                        count++;
                    }
                }
                if (foundAll) {
                    _rightList.push_back(child);
                }
                else if (!foundAll && count > 0) {
                    return nullptr;
                }
            }
            assert(_rightList.size() > 1);
            return parent_node;
        }
        else {
            return nullptr;
        }
    }

    // method to execute the split of the MultiPipe _mp
    std::vector<MultiPipe *> execute_Split(MultiPipe *_mp)
    {
        // find _mp in the Application Tree
        AppNode *found = find_AppNode(root, _mp);
        if (found == nullptr) {
            std::cerr << RED << "WindFlow Error: MultiPipe to be split does not belong to this PipeGraph" << DEFAULT_COLOR << std::endl;
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
            MultiPipe *split_mp = new MultiPipe(this, mode, &atomic_num_dropped, &listOperators);
#if defined (TRACE_WINDFLOW)
            split_mp->gv_graph = gv_graph;
#endif
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

    // method to execute the merge of a set of MultiPipe instances _toBeMerged
    MultiPipe *execute_Merge(std::vector<MultiPipe *> _toBeMerged)
    {
        // get the right list of AppNode instances to be merged
        std::vector<AppNode *> rightList;
        if (get_MergedNodes1(_toBeMerged, rightList)) {
            if (rightList.size() == 1) { // Case 2.1: merge-full -> we merge a whole sub-tree
                assert(rightList[0] != root); // redundant check
                MultiPipe *mp = rightList[0]->mp;
                // create the new MultiPipe, the result of the self-merge
                std::vector<MultiPipe *> mp_v;
                mp_v.push_back(mp);
                MultiPipe *mergedMP = new MultiPipe(this, mp_v, mode, &atomic_num_dropped, &listOperators);
#if defined (TRACE_WINDFLOW)
                mergedMP->gv_graph = gv_graph;
#endif
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
                        if (mp2 == mp) {
                            new_second_set.push_back(mergedMP);
                        }
                        else {
                            new_second_set.push_back(second_set[i]);
                        }
                    }
                    (parentMP->last)->change_secondset(new_second_set, false);
                    mergedMP->fromSplitting = true;
                    mergedMP->splittingParent = parentMP;
                }
                // adjust the Application Tree
                std::vector<AppNode *> children_new;
                for (auto *brother: (rightList[0]->parent)->children) {
                    if (brother != rightList[0]) { // I am not my brother :)
                        children_new.push_back(brother);
                    }
                    else {
                        children_new.push_back(new AppNode(mergedMP, rightList[0]->parent));
                    }
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
                        std::cerr << RED << "WindFlow Error: the requested merge operation is not supported" << DEFAULT_COLOR << std::endl;
                        exit(EXIT_FAILURE);
                    }
                    rightMergedMPs.push_back(an->mp);
                }
                // create the new MultiPipe, the result of the merge
                MultiPipe *mergedMP = new MultiPipe(this, rightMergedMPs, mode, &atomic_num_dropped, &listOperators);
#if defined (TRACE_WINDFLOW)
                mergedMP->gv_graph = gv_graph;
#endif
                mergedMP->outputType = _toBeMerged[0]->outputType;
                toBeDeteled.push_back(mergedMP);
                // adjust the Application Tree
                std::vector<AppNode *> children_new;
                // maintaining the previous ordering of the children is not important in this case
                for (auto *brother: root->children) {
                    if (std::find(rightList.begin(), rightList.end(), brother) == rightList.end()) {
                        children_new.push_back(brother);
                    }
                }
                children_new.push_back(new AppNode(mergedMP, root));
                root->children = children_new;
                // delete the nodes
                for (auto *an: rightList) {
                    delete_AppNodes(an);
                }
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
                        std::cerr << RED << "WindFlow Error: sibling MultiPipes to be merged must be contiguous branches of the same MultiPipe" << DEFAULT_COLOR << std::endl;
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
                MultiPipe *mergedMP = new MultiPipe(this, rightMergedMPs, mode, &atomic_num_dropped, &listOperators);
#if defined (TRACE_WINDFLOW)
                mergedMP->gv_graph = gv_graph;
#endif
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
                    if (std::find(rightList.begin(), rightList.end(), brother) == rightList.end()) {
                        children_new.push_back(brother);
                    }
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
                std::cerr << RED << "WindFlow Error: the requested merge operation is not supported" << DEFAULT_COLOR << std::endl;
                exit(EXIT_FAILURE);
            }
        }
    }

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _name name of the PipeGraph
     *  \param _mode processing mode of the PipeGraph
     */ 
    PipeGraph(std::string _name, Mode _mode=Mode::DEFAULT):
              name(_name),
              mode(_mode),
              started(false),
              ended(false),
              root(new AppNode()),
              atomic_num_dropped(0)
    {
#if defined (TRACE_WINDFLOW)
        gvc = gvContext(); // set up a graphviz context
        gv_graph = agopen(const_cast<char *>(name.c_str()), Agdirected, 0); // create the graphviz representation
        agattr(gv_graph, AGRAPH, const_cast<char *>("rankdir"), const_cast<char *>("LR")); // direction left to right
        agattr(gv_graph, AGNODE, const_cast<char *>("label"), const_cast<char *>("none")); // default vertex labels
        agattr(gv_graph, AGNODE, const_cast<char *>("penwidth"), const_cast<char *>("2")); // size of the vertex borders
        agattr(gv_graph, AGNODE, const_cast<char *>("shape"), const_cast<char *>("box")); // shape of the vertices
        agattr(gv_graph, AGNODE, const_cast<char *>("style"), const_cast<char *>("filled")); // vertices are filled with color
        agattr(gv_graph, AGNODE, const_cast<char *>("fontname"), const_cast<char *>("helvetica bold")); // font of the vertex labels
        agattr(gv_graph, AGNODE, const_cast<char *>("fontsize"), const_cast<char *>("12")); // font size of the vertex labels
        agattr(gv_graph, AGNODE, const_cast<char *>("fontcolor"), const_cast<char *>("white")); // font color of vertex labels
        agattr(gv_graph, AGNODE, const_cast<char *>("color"), const_cast<char *>("white")); // default color of vertex boders
        agattr(gv_graph, AGNODE, const_cast<char *>("fillcolor"), const_cast<char *>("black")); // default color of vertex area
        agattr(gv_graph, AGEDGE, const_cast<char *>("label"), const_cast<char *>("")); // default edge labels
        agattr(gv_graph, AGEDGE, const_cast<char *>("fontname"), const_cast<char *>("helvetica bold")); // font of the edge labels
        agattr(gv_graph, AGEDGE, const_cast<char *>("fontsize"), const_cast<char *>("10")); // font size of the edge labels
#endif
    }

    /// Destructor
    ~PipeGraph()
    {
        // delete all the MultiPipe instances in toBeDeteled
        for (auto *mp: toBeDeteled) {
            delete mp;
        }
        // delete the Application Tree
        delete_AppNodes(root);
#if defined (TRACE_WINDFLOW)
        agclose(this->gv_graph); // free the graph structures
        gvFreeLayout(this->gvc, this->gv_graph); // free the layout
#endif
    }

    /** 
     *  \brief Add a Source to the PipeGraph
     *  \param _source Source operator to be added
     *  \return reference to a MultiPipe object to be filled with operators fed by this Source
     */ 
    template<typename tuple_t>
    MultiPipe &add_source(Source<tuple_t> &_source)
    {
        MultiPipe *mp = new MultiPipe(this, mode, &atomic_num_dropped, &listOperators);
#if defined (TRACE_WINDFLOW)
        mp->gv_graph = gv_graph;
#endif
        mp->add_source<tuple_t>(_source);
        // update the Application Tree
        (root->children).push_back(new AppNode(mp, root));
        // this MultiPipe must be deleted at the end
        toBeDeteled.push_back(mp);
        // add this operator to listOperators
        listOperators.push_back(std::ref(static_cast<Basic_Operator &>(_source)));
        return *mp;
    }

    /** 
     *  \brief Run the PipeGraph and wait for the completion of the processing
     *  \return zero in case of success, non-zero otherwise
     */ 
    int run()
    {
        // start the PipeGraph
        int status = this->start();
        if (status == 0) {
            // wait the termination of the processing
            status = this->wait_end();
        }
        return status;
    }

    /** 
     *  \brief Start the PipeGraph (without waiting from the completion of the processing)
     *  \return zero in case of success, non-zero otherwise
     */ 
    int start()
    {
        // check if there is something to run
        if ((root->children).size() == 0) {
            std::cerr << RED << "WindFlow Error: PipeGraph [" << name << "] is empty, nothing to run" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // check if the PipeGraph has already been started
        if (this->started) {
            std::cerr << RED << "WindFlow Error: PipeGraph [" << name << "] has already been started and cannot be run again" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        else {
            this->started = true;
        }
        // get the number of threads
        size_t count_threads = this->getNumThreads();
        std::cout << GREEN << "WindFlow Status Message: PipeGraph [" << name << "] is running with " << count_threads << " threads" << DEFAULT_COLOR << std::endl;
        // useful prints of the PipeGraph configuration
        if (mode == Mode::DEFAULT) {
            std::cout << "--> DEFAULT (out-of-order) mode " << GREEN << "enabled" << DEFAULT_COLOR << std::endl;
        }
        else if (mode == Mode::DETERMINISTIC) {
            std::cout << "--> DETERMINISTIC (in-order) mode " << GREEN << "enabled" << DEFAULT_COLOR << std::endl;
        }
        else {
            std::cout << "--> PROBABILISTIC (in-order) mode " << GREEN << "enabled" << DEFAULT_COLOR << std::endl;
        }
#if defined(FF_BOUNDED_BUFFER)
        std::cout << "--> Backpressure " << GREEN << "enabled" << DEFAULT_COLOR << std::endl;
#else
        std::cout << "--> Backpressure " << RED << "disabled" << DEFAULT_COLOR << std::endl;
#endif
#if !defined(BLOCKING_MODE)
        std::cout << "--> Non-blocking queues " << GREEN << "enabled" << DEFAULT_COLOR << std::endl;
#else
        std::cout << "--> Blocking queues " << GREEN << "enabled" << DEFAULT_COLOR << std::endl;
#endif
#if !defined(NO_DEFAULT_MAPPING)
        std::cout << "--> Pinning of threads " << GREEN << "enabled" << DEFAULT_COLOR << std::endl;
#else
        std::cout << "--> Pinning of threads " << RED << "disabled" << DEFAULT_COLOR << std::endl;
#endif
#if defined (TRACE_FASTFLOW)
        std::cout << "--> FastFlow tracing " << GREEN << "enabled" << DEFAULT_COLOR << std::endl;
#endif
#if defined (TRACE_WINDFLOW)
        std::cout << "--> WindFlow tracing " << GREEN << "enabled" << DEFAULT_COLOR << std::endl;
        // start the monitoring thread connecting with the Web DashBoard
        MonitoringThread mt(this);
        mt_thread = std::thread(mt);
#endif
        // run all the topmost MultiPipe instances
        for (auto *an: root->children) {
            int status = (an->mp)->run();
            if (status == -1) {
                return status;
            }
        }
        return 0;
    }

    /** 
     *  \brief Wait the end of the processing of the PipeGraph, launched before with start()
     *  \return zero in case of success, non-zero otherwise
     */ 
    int wait_end()
    {
        // check if the PipeGraph has already been started
        if (!this->started) {
            std::cerr << RED << "WindFlow Error: PipeGraph [" << name << "] is not started yet" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // check if the PipeGraph processing is over
        if (this->ended) {
            std::cerr << RED << "WindFlow Error: PipeGraph [" << name << "] processing already complete" << DEFAULT_COLOR << std::endl;
            return 0;
        }
        // waiting the completion of all the topmost MultiPipe instances
        int status = 0;
        for (auto *an: root->children) {
            status = (an->mp)->wait();
            if (status == -1) {
                return status;
            }
        }
        this->ended = true;
        // handling windflow statistics (if enabled)
#if defined (TRACE_WINDFLOW)
#if defined (LOG_DIR)
        std::string ff_trace_file = std::string(STRINGIFY(LOG_DIR)) + "/" + this->name;
        std::string ff_trace_dir = std::string(STRINGIFY(LOG_DIR));
#else
        std::string ff_trace_file = "log/" + this->name;
        std::string ff_trace_dir = "log";
#endif
        // create the log directory
        if (mkdir(ff_trace_dir.c_str(), 0777) != 0) {
            struct stat st;
            if((stat(ff_trace_dir.c_str(), &st) != 0) || !S_ISDIR(st.st_mode)) {
                std::cerr << RED << "WindFlow Error: directory for dumping FastFlow log files cannot be created" << DEFAULT_COLOR << std::endl;
                exit(EXIT_FAILURE);
            }
        }
        // dump the diagram representing the PipeGraph (in a pdf file)
        gvLayout(this->gvc, this->gv_graph, const_cast<char *>("dot")); // set the layout to DOT
        std::string name_pdf = ff_trace_file + ".pdf";
        gvRenderFilename(this->gvc, this->gv_graph, const_cast<char *>("pdf"), const_cast<char *>(name_pdf.c_str())); // generate the pdf file
        // wait for the monitoring thread
        mt_thread.join();
        // print log files of all the operators
        for (auto &op: listOperators) {
            (op.get()).dump_LogFile();
        }
#endif
        // handling fastflow statistics (if enabled)
#if defined (TRACE_FASTFLOW)
#if defined (LOG_DIR)
        std::string ff_trace_file = std::string(STRINGIFY(LOG_DIR)) + "/ff_trace_" + this->name + "_" + std::to_string(getpid()) + ".log";
        std::string ff_trace_dir = std::string(STRINGIFY(LOG_DIR));
#else
        std::string ff_trace_file = "log/ff_trace_" + this->name + "_" + std::to_string(getpid()) + ".log";
        std::string ff_trace_dir = "log";
#endif
        // create the log directory
        if (mkdir(ff_trace_dir.c_str(), 0777) != 0) {
            struct stat st;
            if((stat(ff_trace_dir.c_str(), &st) != 0) || !S_ISDIR(st.st_mode)) {
                std::cerr << RED << "WindFlow Error: directory for dumping FastFlow log files cannot be created" << DEFAULT_COLOR << std::endl;
                exit(EXIT_FAILURE);
            }
        }
        std::ofstream tracefile;
        tracefile.open(ff_trace_file);
        for (auto *an: root->children) {
            (an->mp)->ffStats(tracefile);
        }
        tracefile.close();
#endif
        std::cout << GREEN << "WindFlow Status Message: PipeGraph [" << name << "] executed successfully" << DEFAULT_COLOR << std::endl;
        return 0;
    }

    /** 
     *  \brief Return the number of threads used to run this PipeGraph
     *  \return number of threads
     */ 
    size_t getNumThreads() const
    {
        size_t count = 0;
        for (auto *an: root->children)
            count += (an->mp)->getNumThreads();
        return count;
    }

    /** 
     *  \brief Return the list of operators added/chained within the PipeGraph
     *  \return vector containing reference wrapper to the operators
     */ 
    std::vector<std::reference_wrapper<Basic_Operator>> get_ListOperators() const
    {
        return listOperators;
    }

    /** 
     *  \brief Method to get the total number of dropped tuples during the PipeGraph processing
     *  \return number of dropped tuples
     */ 
    unsigned long get_NumDroppedTuples() const
    {
        return atomic_num_dropped.load();
    }

    /** 
     *  \brief Check whether the PipeGraph has been started
     *  \return true if the PipeGraph has been started, false otherwise
     */ 
    bool isStarted()
    {
        return started;
    }

    /** 
     *  \brief Check whether the PipeGraph execution has finished
     *  \return true if the PipeGraph execution has finished, false otherwise
     */ 
    bool isEnded()
    {
        return ended;
    }

#if defined (TRACE_WINDFLOW)
    /** 
     *  \brief Method returning a string with the statistics of the whole PipeGraph (in JSON format)
     *  \return string with the statistics
     */ 
    std::string generate_JSONStats() const
    {
        // create the rapidjson writer.
        rapidjson::StringBuffer buffer;
        rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(buffer);
        // create the header of the JSON file
        writer.StartObject();
        writer.Key("PipeGraph_name");
        writer.String(name.c_str());
        writer.Key("Mode");
        std::string mode_string;
        if (mode == Mode::DETERMINISTIC) {
            mode_string = "DETERMINISTIC";
        }
        else if (mode == Mode::PROBABILISTIC) {
            mode_string = "PROBABILISTIC";
        }
        else {
            mode_string = "DEFAULT";
        }
        writer.String(mode_string.c_str());
        writer.Key("Backpressure");
#if defined FF_BOUNDED_BUFFER
        writer.String("ON");
#else
        writer.String("OFF");
#endif
        writer.Key("Non_blocking");
#if defined BLOCKING_MODE
        writer.String("OFF");
#else
        writer.String("ON");
#endif
        writer.Key("Thread_pinning");
#if defined NO_DEFAULT_MAPPING
        writer.String("OFF");
#else
        writer.String("ON");
#endif
        writer.Key("Dropped_tuples");
        writer.Uint64(this->get_NumDroppedTuples());
        writer.Key("Operator_number");
        writer.Uint(listOperators.size());
        writer.Key("Thread_number");
        writer.Uint(this->getNumThreads());
        double vss, rss;
        get_MemUsage(vss, rss);
        writer.Key("rss_size_kb");
        writer.Double(rss);
        writer.Key("Operators");
        writer.StartArray();
        // get statistics from all the replicas of the operator
        for(auto op: listOperators) {
            (op.get()).append_Stats(writer);
        }
        writer.EndArray();
        writer.EndObject();
        // serialize the object to file
        std::string json_stats(buffer.GetString());
        return json_stats;
    }

    /** 
     *  \brief Method returning a string representing the PipeGraph diagram (in SVG format)
     *  \return string representing the PipeGraph diagram
     */ 
    std::string generate_SVGDiagram()
    {
        gvLayout(this->gvc, this->gv_graph, const_cast<char *>("dot")); // set the layout to DOT
        char *result;
        unsigned int length;
        // create the SVG representation of the PipeGraph
        gvRenderData(this->gvc, this->gv_graph, const_cast<char *>("svg"), &result, &length);
        std::string svg_str(result);
        gvFreeRenderData(result);
        return svg_str;
    }
#endif

    /// deleted constructors/operators
    PipeGraph(const PipeGraph &) = delete; // copy constructor
    PipeGraph(PipeGraph &&) = delete; // move constructor
    PipeGraph &operator=(const PipeGraph &) = delete; // copy assignment operator
    PipeGraph &operator=(PipeGraph &&) = delete; // move assignment operator
};

//@cond DOXY_IGNORE

// implementation of the merge_multipipes_func function
inline MultiPipe *merge_multipipes_func(PipeGraph *graph, std::vector<MultiPipe *> _toBeMerged)
{
    return graph->execute_Merge(_toBeMerged);
}

// implementation of the split_multipipe_func function
inline std::vector<MultiPipe *> split_multipipe_func(PipeGraph *graph, MultiPipe *_mp)
{
    return graph->execute_Split(_mp);
}

#if defined (TRACE_WINDFLOW)
    // implementation of the is_ended_func function
    inline bool is_ended_func(PipeGraph *graph)
    {
        return graph->isEnded();
    }

    // implementation of the get_diagram function
    inline std::string get_diagram(PipeGraph *graph)
    {
        return graph->generate_SVGDiagram();
    }

    // implementation of the get_stats_report function
    inline std::string get_stats_report(PipeGraph *graph)
    {
        return graph->generate_JSONStats();
    }
#endif

//@endcond

} // namespace wf

#endif
