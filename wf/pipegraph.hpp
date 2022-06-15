/**************************************************************************************
 *  Copyright (c) 2019- Gabriele Mencagli
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
 *  @file    pipegraph.hpp
 *  @author  Gabriele Mencagli
 *  
 *  @brief PipeGraph construct
 *  
 *  @section PipeGraph (Description)
 *  
 *  This file implements the PipeGraph, the WindFlow "streaming environment".
 */ 

#ifndef PIPEGRAPH_H
#define PIPEGRAPH_H

/// includes
#include<random>
#include<thread>
#include<multipipe.hpp>
#if defined (WF_TRACING_ENABLED)
    #include<monitoring.hpp>
#endif

namespace wf {

//@cond DOXY_IGNORE

// AppNode struct (Struct representing a node of the Application Tree)
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
 *  \brief PipeGraph construct used to build a streaming application in WindFlow
 *  
 *  This class implements the PipeGraph construct used to build a WindFlow
 *  streaming application.
 */ 
class PipeGraph
{
private:
    friend class MultiPipe; // friendship with the MultiPipe class
    friend class MonitoringThread; // friendship with the MonitoringThread class
    friend inline MultiPipe *merge_multipipes_func(PipeGraph *, std::vector<MultiPipe *>); // friendship with the merge_multipipes_func function
    friend inline std::vector<MultiPipe *> split_multipipe_func(PipeGraph *, MultiPipe *); // friendship with the split_multipipe_func function
    friend inline bool is_ended_func(PipeGraph *); // friendship with the is_ended_func function
    friend inline std::string get_diagram(PipeGraph *); // friendship with the get_diagram function
    friend inline std::string get_stats_report(PipeGraph *); // friendship with the get_stats_report function
    std::string name; // name of the PipeGraph
    AppNode *root; // pointer to the root of the Application Tree
    std::vector<MultiPipe *> toBeDeteled; // vector of MultiPipe instances to be deleted
    Execution_Mode_t execution_mode; // execution mode used by the PipeGraph
    Time_Policy_t time_policy; // time policy used by the PipeGraph
    bool started; // flag stating if the PipeGraph has already been started
    bool ended; // flag stating if the PipeGraph has completed its processing
    std::vector<Basic_Operator *> globalOpList; // vector containing pointers to of all the operators in the PipeGraph
    std::atomic<unsigned long> atomic_num_dropped; // atomic counters of the number of dropped tuples
#if defined (WF_TRACING_ENABLED)
    GVC_t *gvc; // pointer to the GVC environment
    Agraph_t *gv_graph; // pointer to the graphviz representation of the PipeGraph
    std::thread mt_thread; // object representing the monitoring thread
#endif

    // Find the AppNode containing the MultiPipe _mp in the tree rooted at _node
    AppNode *find_AppNode(AppNode *_node,
                          MultiPipe *_mp)
    {
        if (_node->mp == _mp) { // base case
            return _node;
        }
        else { // recursive case
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

    // Find the list of AppNodes that are leaves of the tree rooted at _node
    std::vector<AppNode *> get_LeavesList(AppNode *_node)
    {
        std::vector<AppNode *> leaves;
        if ((_node->children).size() == 0) { // base case
            leaves.push_back(_node);
            return leaves;
        }
        else { // recursive case
            for (auto *child: _node->children) {
                auto v = get_LeavesList(child);
                leaves.insert(leaves.end(), v.begin(), v.end());
            }
            return leaves;
        }
    }

    // Find the LCA of a set of _leaves starting from _node
    AppNode *get_LCA(AppNode *_node,
                     std::vector<AppNode *> _leaves)
    {
        for (auto *child: _node->children) { // compute the leaves rooted at each child of _node
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

    // Delete all the AppNodes in the tree rooted at _node
    void delete_AppNodes(AppNode *_node)
    {
        if ((_node->children).size() == 0) { // base case
            delete _node;
        }
        else { // recursive case
            for (auto *child: _node->children) {
                delete_AppNodes(child);
            }
            delete _node;
        }
    }

    // Prepare the right list of AppNodes to be merged (case merge-ind and merge-full)
    bool get_MergedNodes1(std::vector<MultiPipe *> _toBeMerged,
                          std::vector<AppNode *> &_rightList)
    {
        assert(_toBeMerged.size() > 1); // sanity check
        std::vector<AppNode *> inputNodes;
        for (auto *mp: _toBeMerged) { // check that all the input MultiPipes must be leaves of the Application Tree
            AppNode *node = find_AppNode(root, mp);
            if (node == nullptr) {
                std::cerr << RED << "WindFlow Error: MultiPipe to be merged does not belong to this PipeGraph" << DEFAULT_COLOR << std::endl;
                exit(EXIT_FAILURE);
            }
            assert((node->children).size() == 0); // sanity check
            inputNodes.push_back(node);
        }
        while (inputNodes.size() > 0) { // loop until inputNodes is empty
            std::random_device random_device; 
            std::mt19937 engine { random_device() };
            std::uniform_int_distribution<int> dist(0, inputNodes.size() - 1);
            AppNode *curr_node = inputNodes[dist(engine)]; // pick one random node in inputNodes
            inputNodes.erase(std::remove(inputNodes.begin(), inputNodes.end(), curr_node), inputNodes.end()); // remove curr_node from inputNodes
            AppNode *lca = curr_node;
            while (curr_node->parent != root && inputNodes.size() > 0) { // loop in the sub-tree rooted at curr_node
                std::vector<AppNode *> leaves;
                for (auto *brother: ((curr_node->parent)->children)) { // get all the others leaves of the sub-tree rooted at the parent
                    if (brother != curr_node) { // I am not my brother :)
                        auto v = get_LeavesList(brother);
                        leaves.insert(leaves.end(), v.begin(), v.end());
                    }
                }
                for (auto *leaf: leaves) { // check that every leaf is in inputNodes
                    if (std::find(inputNodes.begin(), inputNodes.end(), leaf) == inputNodes.end()) { // if not present
                        return false;
                    }
                }
                for (auto *leaf: leaves) { // remove all the leaves from inputNodes
                    inputNodes.erase(std::remove(inputNodes.begin(), inputNodes.end(), leaf), inputNodes.end());
                }
                curr_node = curr_node->parent;
                lca = curr_node;
            }
            _rightList.push_back(lca); // add the found lca to the _rightList
            if (lca->parent != root && ((inputNodes.size() > 0) || (_rightList.size() > 1))) { // important check
                return false;
            }
        }
        return true;
    }

    // Prepare the right list of AppNode instances to be merged (case merge-partial)
    AppNode *get_MergedNodes2(std::vector<MultiPipe *> _toBeMerged,
                              std::vector<AppNode *> &_rightList)
    {
        assert(_toBeMerged.size() > 1); // sanity check
        std::vector<AppNode *> inputNodes; 
        for (auto *mp: _toBeMerged) { // check that all the input MultiPipes must be leaves of the Application Tree
            AppNode *node = find_AppNode(root, mp);
            if (node == nullptr) {
                std::cerr << RED << "WindFlow Error: MultiPipe to be merged does not belong to this PipeGraph" << DEFAULT_COLOR << std::endl;
                exit(EXIT_FAILURE);
            }
            assert((node->children).size() == 0); // sanity check
            inputNodes.push_back(node);
        }
        AppNode *parent_node = get_LCA(root, inputNodes); // we have to find the LCA
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

    // Execute the split of the MultiPipe _mp
    std::vector<MultiPipe *> execute_Split(MultiPipe *_mp)
    {
        AppNode *found = find_AppNode(root, _mp); // find _mp in the Application Tree
        if (found == nullptr) {
            std::cerr << RED << "WindFlow Error: MultiPipe to be split does not belong to this PipeGraph" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        assert((found->children).size() == 0); // sanity check
        std::vector<MultiPipe *> splitMPs; // prepare the MultiPipe _mp
        std::vector<ff::ff_node *> normalization = _mp->normalize();
        _mp->isSplit = true; // be careful --> this statement after normalize() not before!
        ff::ff_a2a *container = new ff::ff_a2a();
        container->add_firstset(normalization, 0, false);
        std::vector<ff::ff_node *> second_set;
        for (size_t i=0; i<_mp->splittingBranches; i++) {
            MultiPipe *split_mp = new MultiPipe(this, execution_mode, time_policy, &atomic_num_dropped, &globalOpList);
#if defined (WF_TRACING_ENABLED)
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
        for (auto *split_mp: splitMPs) { // prepare the Application Tree
            (found->children).push_back(new AppNode(split_mp, found));
            toBeDeteled.push_back(split_mp);
        }
        return splitMPs;
    }

    // Execute the merge of a set of MultiPipe instances _toBeMerged
    MultiPipe *execute_Merge(std::vector<MultiPipe *> _toBeMerged)
    {
        std::vector<AppNode *> rightList; // get the right list of AppNode instances to be merged
        if (get_MergedNodes1(_toBeMerged, rightList)) {
            if (rightList.size() == 1) { // Case 2.1: merge-full -> we merge a whole sub-tree
                assert(rightList[0] != root); // sanity check
                MultiPipe *mp = rightList[0]->mp;
                std::vector<MultiPipe *> mp_v;
                mp_v.push_back(mp);
                MultiPipe *mergedMP = new MultiPipe(this, mp_v, execution_mode, time_policy, &atomic_num_dropped, &globalOpList); // create the new MultiPipe, the result of the self-merge
#if defined (WF_TRACING_ENABLED)
                mergedMP->gv_graph = gv_graph;
#endif
                mergedMP->outputType = _toBeMerged[0]->outputType;
                toBeDeteled.push_back(mergedMP);
                if (mp->fromSplitting && mp->splittingParent != nullptr) { // adjust the parent MultiPipe if it exists
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
                std::vector<AppNode *> children_new; // adjust the Application Tree
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
                for (auto *an: rightList) { // check that merged MultiPipe instances are sons of the root
                    if (std::find((root->children).begin(), (root->children).end(), an) == (root->children).end()) {
                        std::cerr << RED << "WindFlow Error: the requested merge operation is not supported" << DEFAULT_COLOR << std::endl;
                        exit(EXIT_FAILURE);
                    }
                    rightMergedMPs.push_back(an->mp);
                }
                MultiPipe *mergedMP = new MultiPipe(this, rightMergedMPs, execution_mode, time_policy, &atomic_num_dropped, &globalOpList); // create the new MultiPipe, the result of the merge
#if defined (WF_TRACING_ENABLED)
                mergedMP->gv_graph = gv_graph;
#endif
                mergedMP->outputType = _toBeMerged[0]->outputType;
                toBeDeteled.push_back(mergedMP);
                std::vector<AppNode *> children_new;
                for (auto *brother: root->children) { // adjust the Application Tree
                    if (std::find(rightList.begin(), rightList.end(), brother) == rightList.end()) {
                        children_new.push_back(brother);
                    }
                }
                children_new.push_back(new AppNode(mergedMP, root));
                root->children = children_new;
                for (auto *an: rightList) { // delete the nodes
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
                    indexes.push_back(std::distance((parent_node->children).begin(), std::find((parent_node->children).begin(), (parent_node->children).end(), node)));
                }
                std::sort(indexes.begin(), indexes.end());
                for (size_t i=0; i<indexes.size()-1; i++) { // check whether the nodes in the rightList are adjacent
                    if (indexes[i]+1 != indexes[i+1]) {
                        std::cerr << RED << "WindFlow Error: sibling MultiPipes to be merged must be contiguous branches of the same MultiPipe" << DEFAULT_COLOR << std::endl;
                        exit(EXIT_FAILURE);
                    }
                }
                rightList.clear();
                std::vector<MultiPipe *> rightMergedMPs;
                for (int idx: indexes) { // everything must be put in the right order
                    rightList.push_back((parent_node->children)[idx]);
                    rightMergedMPs.push_back((parent_node->children)[idx]->mp);
                }
                MultiPipe *mergedMP = new MultiPipe(this, rightMergedMPs, execution_mode, time_policy, &atomic_num_dropped, &globalOpList); // if we reach this point the merge is possible -> we create the new MultiPipe
#if defined (WF_TRACING_ENABLED)
                mergedMP->gv_graph = gv_graph;
#endif
                mergedMP->outputType = _toBeMerged[0]->outputType;
                toBeDeteled.push_back(mergedMP);
                MultiPipe *parentMP = parent_node->mp; // adjust the parent MultiPipe
                assert(parentMP->isSplit); // sanity check
                size_t new_branches = (parentMP->splittingChildren).size() - indexes.size() + 1;
                std::vector<MultiPipe *> new_splittingChildren;
                std::vector<ff::ff_node *> new_second_set;
                auto second_set = (parentMP->last)->getSecondSet();
                for (size_t i=0; i<(parentMP->splittingChildren).size(); i++) { // the code below is to respect the original indexes
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
                assert(new_splittingChildren.size () == new_branches); // sanity check
                parentMP->splittingBranches = new_branches;
                parentMP->splittingChildren = new_splittingChildren;
                (parentMP->last)->change_secondset(new_second_set, false);
                mergedMP->fromSplitting = true;
                mergedMP->splittingParent = parentMP;
                std::vector<AppNode *> children_new; // adjust the Application Tree
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

#if defined (WF_TRACING_ENABLED)
    // Get a string with the statistics of the whole PipeGraph (in JSON format)
    std::string generateJSONStats() const
    {
        rapidjson::StringBuffer buffer; // create the rapidjson writer.
        rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(buffer);
        writer.StartObject(); // create the header of the JSON file
        writer.Key("PipeGraph_name");
        writer.String(name.c_str());
        writer.Key("Mode");
        std::string mode_string;
        if (execution_mode == Execution_Mode_t::DETERMINISTIC) {
            mode_string = "DETERMINISTIC";
        }
        else if (execution_mode == Execution_Mode_t::PROBABILISTIC) {
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
        writer.Uint64(this->getNumDroppedTuples());
        writer.Key("Operator_number");
        writer.Uint(globalOpList.size());
        writer.Key("Thread_number");
        writer.Uint(this->getNumThreads());
        double vss, rss;
        get_MemUsage(vss, rss);
        writer.Key("rss_size_kb");
        writer.Double(rss);
        writer.Key("Operators");
        writer.StartArray();
        // get statistics from all the replicas of the operator
        for(auto *op: globalOpList) {
            op->appendStats(writer);
        }
        writer.EndArray();
        writer.EndObject();
        // serialize the object to file
        std::string json_stats(buffer.GetString());
        return json_stats;
    }

    // Get a string representing the PipeGraph diagram (in SVG format)
    std::string generateSVGDiagram()
    {
        gvLayout(this->gvc, this->gv_graph, const_cast<char *>("dot")); // set the layout to DOT
        char *result;
        unsigned int length;
        gvRenderData(this->gvc, this->gv_graph, const_cast<char *>("svg"), &result, &length); // create the SVG representation of the PipeGraph
        std::string svg_str(result);
        gvFreeRenderData(result);
        return svg_str;
    }
#endif

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _name name of the PipeGraph
     *  \param _execution_mode execution mode of the PipeGraph
     *  \param _time_policy time policy of the PipeGraph
     */ 
    PipeGraph(std::string _name,
              Execution_Mode_t _execution_mode=Execution_Mode_t::DEFAULT,
              Time_Policy_t _time_policy=Time_Policy_t::INGRESS_TIME):
              name(_name),
              execution_mode(_execution_mode),
              time_policy(_time_policy),
              started(false),
              ended(false),
              root(new AppNode()),
              atomic_num_dropped(0)
    {
#if defined (WF_TRACING_ENABLED)
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
        for (auto *mp: toBeDeteled) { // delete all the MultiPipes in toBeDeteled
            delete mp;
        }
        delete_AppNodes(root); // delete the Application Tree
#if defined (WF_TRACING_ENABLED)
        agclose(this->gv_graph); // free the graph structures
        gvFreeLayout(this->gvc, this->gv_graph); // free the layout
#endif
    }

    /** 
     *  \brief Add a new Source operator to the PipeGraph
     *  \param _source Source operator to be added
     *  \return reference to a MultiPipe object to be filled with operators fed by this Source
     */ 
    template<typename source_t>
    MultiPipe &add_source(const source_t &_source)
    {
        MultiPipe *mp = new MultiPipe(this, execution_mode, time_policy, &atomic_num_dropped, &globalOpList); // create an empty MultiPipe
#if defined (WF_TRACING_ENABLED)
        mp->gv_graph = gv_graph;
#endif
        mp->add_source(_source); // add the Source to the MultiPipe
        (root->children).push_back(new AppNode(mp, root)); // update the Application Tree
        toBeDeteled.push_back(mp); // this MultiPipe must be deleted at the end
        return *mp;
    }

    /** 
     *  \brief Run the PipeGraph and wait for the completion of the processing
     *  \return zero in case of success, non-zero otherwise
     */ 
    int run()
    {
        int status = this->start(); // start the PipeGraph
        if (status == 0) {
            status = this->wait_end(); // wait the termination of the processing
        }
        return status;
    }

    /** 
     *  \brief Start the PipeGraph asynchronously
     *  \return zero in case of success, non-zero otherwise
     */ 
    int start()
    {
        if ((root->children).size() == 0) { // check if there is something to run
            std::cerr << RED << "WindFlow Error: PipeGraph [" << name << "] is empty, nothing to run" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        if (started) { // check if the PipeGraph has already been started
            std::cerr << RED << "WindFlow Error: PipeGraph [" << name << "] has already been started and cannot be run again" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        else {
            started = true;
        }
        size_t count_threads = this->getNumThreads(); // get the number of threads
        std::cout << GREEN << "WindFlow Status Message: PipeGraph [" << name << "] is running with " << count_threads << " threads" << DEFAULT_COLOR << std::endl;
        if (execution_mode == Execution_Mode_t::DEFAULT) {
            std::cout << "--> DEFAULT (out-of-order) mode " << GREEN << "enabled" << DEFAULT_COLOR << std::endl;
        }
        else if (execution_mode == Execution_Mode_t::DETERMINISTIC) {
            std::cout << "--> DETERMINISTIC (in-order) mode " << GREEN << "enabled" << DEFAULT_COLOR << std::endl;
        }
        else {
            std::cout << "--> PROBABILISTIC (in-order) mode " << GREEN << "enabled" << DEFAULT_COLOR << std::endl;
        }
        if (time_policy == Time_Policy_t::INGRESS_TIME) {
            std::cout << "--> INGRESS_TIME policy " << GREEN << "enabled" << DEFAULT_COLOR << std::endl;
        }
        else {
            std::cout << "--> EVENT_TIME policy " << GREEN << "enabled" << DEFAULT_COLOR << std::endl;
        }
#if defined (FF_BOUNDED_BUFFER)
        std::cout << "--> Backpressure " << GREEN << "enabled" << DEFAULT_COLOR << std::endl;
#else
        std::cout << "--> Backpressure " << RED << "disabled" << DEFAULT_COLOR << std::endl;
#endif
#if !defined (BLOCKING_MODE)
        std::cout << "--> Non-blocking queues " << GREEN << "enabled" << DEFAULT_COLOR << std::endl;
#else
        std::cout << "--> Blocking queues " << GREEN << "enabled" << DEFAULT_COLOR << std::endl;
#endif
#if !defined (NO_DEFAULT_MAPPING)
        std::cout << "--> Pinning of threads " << GREEN << "enabled" << DEFAULT_COLOR << std::endl;
#else
        std::cout << "--> Pinning of threads " << RED << "disabled" << DEFAULT_COLOR << std::endl;
#endif
#if defined (TRACE_FASTFLOW)
        std::cout << "--> FastFlow tracing " << GREEN << "enabled" << DEFAULT_COLOR << std::endl;
#endif
#if defined (WF_TRACING_ENABLED)
        std::cout << "--> WindFlow tracing " << GREEN << "enabled" << DEFAULT_COLOR << std::endl;
        MonitoringThread mt(this); // start the monitoring thread connecting with the Web DashBoard
        mt_thread = std::thread(mt);
#endif
#if defined (__CUDACC__)
        int max_threads_per_block = 0;
        gpuErrChk(cudaDeviceGetAttribute(&max_threads_per_block, cudaDevAttrMaxThreadsPerBlock, 0)); // device_id = 0
        if (WF_GPU_THREADS_PER_BLOCK > max_threads_per_block) {
                std::cerr << RED << "WindFlow Error: block size (" << WF_GPU_THREADS_PER_BLOCK << ") exceeds the maximum supported by the GPU (" << max_threads_per_block << ")" << DEFAULT_COLOR << std::endl;
                exit(EXIT_FAILURE);
        }
#endif
        for (auto *an: root->children) { // run all the topmost MultiPipe instances
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
        if (!started) { // check if the PipeGraph has already been started
            std::cerr << RED << "WindFlow Error: PipeGraph [" << name << "] is not started yet" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        if (ended) { // check if the PipeGraph processing is over
            std::cerr << RED << "WindFlow Error: PipeGraph [" << name << "] processing already complete" << DEFAULT_COLOR << std::endl;
            return 0;
        }
        int status = 0; // waiting the completion of all the topmost MultiPipe instances
        for (auto *an: root->children) {
            status = (an->mp)->wait();
            if (status == -1) {
                return status;
            }
        }
        ended = true;
#if defined (WF_TRACING_ENABLED) // handling windflow statistics (if enabled)
#if defined (WF_LOG_DIR)
        std::string ff_trace_file = std::string(STRINGIFY(WF_LOG_DIR)) + "/" + this->name;
        std::string ff_trace_dir = std::string(STRINGIFY(WF_LOG_DIR));
#else
        std::string ff_trace_file = "log/" + this->name;
        std::string ff_trace_dir = "log";
#endif
        if (mkdir(ff_trace_dir.c_str(), 0777) != 0) { // create the log directory
            struct stat st;
            if((stat(ff_trace_dir.c_str(), &st) != 0) || !S_ISDIR(st.st_mode)) {
                std::cerr << RED << "WindFlow Error: directory for dumping FastFlow log files cannot be created" << DEFAULT_COLOR << std::endl;
                exit(EXIT_FAILURE);
            }
        }
        gvLayout(this->gvc, this->gv_graph, const_cast<char *>("dot")); // set the layout to DOT
        std::string name_pdf = ff_trace_file + ".pdf";
        gvRenderFilename(this->gvc, this->gv_graph, const_cast<char *>("pdf"), const_cast<char *>(name_pdf.c_str())); // generate the pdf file
        mt_thread.join(); // wait for the monitoring thread
        for (auto *op: globalOpList) { // print log files of all the operators
            op->dumpStats();
        }
#endif
#if defined (TRACE_FASTFLOW) // handling fastflow statistics (if enabled)
#if defined (WF_LOG_DIR)
        std::string ff_trace_file = std::string(STRINGIFY(WF_LOG_DIR)) + "/ff_trace_" + this->name + "_" + std::to_string(getpid()) + ".log";
        std::string ff_trace_dir = std::string(STRINGIFY(WF_LOG_DIR));
#else
        std::string ff_trace_file = "log/ff_trace_" + this->name + "_" + std::to_string(getpid()) + ".log";
        std::string ff_trace_dir = "log";
#endif
        if (mkdir(ff_trace_dir.c_str(), 0777) != 0) { // create the log directory
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
     *  \brief Method to get the total number of dropped tuples during the PipeGraph processing
     *  \return number of dropped tuples
     */ 
    unsigned long getNumDroppedTuples() const
    {
        return atomic_num_dropped.load();
    }

    /** 
     *  \brief Check if the PipeGraph has been started
     *  \return true if the PipeGraph has been started, false otherwise
     */ 
    bool isStarted() const
    {
        return started;
    }

    /** 
     *  \brief Check if the PipeGraph execution has finished
     *  \return true if the PipeGraph execution has finished, false otherwise
     */ 
    bool isEnded() const
    {
        return ended;
    }

    PipeGraph(const PipeGraph &) = delete; // Copy constructor is deleted
    PipeGraph(PipeGraph &&) = delete; // Move constructor is deleted
    PipeGraph &operator=(const PipeGraph &) = delete; // Copy assignment operator is deleted
    PipeGraph &operator=(PipeGraph &&) = delete; // Move assignment operator is deleted
};

//@cond DOXY_IGNORE

// Implementation of the merge_multipipes_func function
inline MultiPipe *merge_multipipes_func(PipeGraph *graph,
                                        std::vector<MultiPipe *> _toBeMerged)
{
    return graph->execute_Merge(_toBeMerged);
}

// Implementation of the split_multipipe_func function
inline std::vector<MultiPipe *> split_multipipe_func(PipeGraph *graph,
                                                     MultiPipe *_mp)
{
    return graph->execute_Split(_mp);
}

#if defined (WF_TRACING_ENABLED)
    // Implementation of the is_ended_func function
    inline bool is_ended_func(PipeGraph *graph)
    {
        return graph->isEnded();
    }

    // Implementation of the get_diagram function
    inline std::string get_diagram(PipeGraph *graph)
    {
        return graph->generateSVGDiagram();
    }

    // Implementation of the get_stats_report function
    inline std::string get_stats_report(PipeGraph *graph)
    {
        return graph->generateJSONStats();
    }
#endif

//@endcond

} // namespace wf

#endif
