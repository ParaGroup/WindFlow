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
 *  building a set of parallel pipelines of operators that might have cross
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

	// method to delete all the AppNode instances in the tree rooted at _node
	void delete_AppNodes(AppNode *_node);

	// method to execute the splitting of the MultiPipe _mp
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
 *  of operators that might have cross-connections jumping from a pipeline
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
    bool isSplitted; // true if the MultiPipe has been splitted into other MultiPipe instances
    bool fromSplitting; // true if the MultiPipe originates from a splitting of another MultiPipe
    size_t splittingBranches; // number of splitting branches (meaningful if isSplitted is true)
    MultiPipe *splittingParent = nullptr; // pointer to the parent MultiPipe (meaningful if fromSplitting is true)
    Basic_Emitter *splittingEmitterRoot = nullptr; // splitting emitter (meaningful if isSplitted is true)
    std::vector<Basic_Emitter *> splittingEmitterLeaves; // vector of emitters (meaningful if isSplitted is true)
    std::vector<MultiPipe *> splittingChildren; // vector of children MultiPipe instances (meaningful if isSplitted is true)
    bool forceShuffling; // true if the next operator that will be added to the MultiPipe is forced to generate a shuffling
    size_t lastParallelism; // parallelism of the last operator added to the MultiPipe (0 if not defined)
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
              isSplitted(false),
              fromSplitting(false),
              splittingBranches(0),
              forceShuffling(false),
              lastParallelism(0)
    {}

    // Private Constructor II (to create a MultiPipe from the merge of other MultiPipe instances)
    MultiPipe(PipeGraph *_graph,
              std::vector<ff::ff_node *> _normalization):
              graph(_graph),
              has_source(true),
              has_sink(false),
              isMerged(false),
              isSplitted(false),
              fromSplitting(false),
              splittingBranches(0),
              forceShuffling(true), // <-- we force a shuffling for the next operator
              lastParallelism(0)
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
    void add_operator(ff::ff_farm *_pattern, routing_types_t _type, ordering_mode_t _ordering);

    // method to chain an operator with the previous one in the MultiPipe
    template<typename worker_t>
    bool chain_operator(ff::ff_farm *_pattern);

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
     *  \brief Merge all the internal splitted MultiPipe instances of this
     *  \return a reference to the new MultiPipe
     */ 
    MultiPipe &self_merge();

    /** 
     *  \brief Merge of this with a set of independent MultiPipe instances _pipes
     *  \param _pipes set of independent MultiPipe instances to be merged with this
     *  \return a reference to the new MultiPipe
     */ 
    template<typename ...MULTIPIPES>
    MultiPipe &merge(MULTIPIPES&... _pipes);

    /** 
     *  \brief Split of this into a set of MultiPipe instances
     *  \param _splitting_func splitting function
     *  \param _cardinality number of splitting MultiPipe instances to generate from this
     */ 
    template<typename F_t>
    void split(F_t _splitting_func, size_t _cardinality);

    /** 
     *  \brief Select a MultiPipe upon the splitting of this
     *  \param _idx index of the MultiPipe to be selected
     *  \return reference to splitted MultiPipe with index _idx
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
PipeGraph::~PipeGraph()
{
    // delete all the MultiPipe instances in toBeDeteled
    for (auto *mp: toBeDeteled)
        delete mp;
    // delete the Application Tree
    delete_AppNodes(root);
}

// implementation of the method to find the AppNode containing the MultiPipe _mp in the tree rooted at _node
AppNode *PipeGraph::find_AppNode(AppNode *_node, MultiPipe *_mp)
{
	if (_node->mp == _mp)
		return _node;
	else {
		AppNode *found = nullptr;
		for (auto *an: _node->children) {
			found = find_AppNode(an, _mp);
			if (found != nullptr)
				return found;
		}
		return nullptr;
	}
}

// implementation of the method to delete all the AppNode instances in the tree rooted at _node
void PipeGraph::delete_AppNodes(AppNode *_node)
{
	if ((_node->children).size() == 0)
		delete _node;
	else {
		for (auto *an: _node->children)
			delete_AppNodes(an);
		delete _node;
	}
}

// implementation of the method to execute the splitting of the MultiPipe _mp
std::vector<MultiPipe *> PipeGraph::execute_Split(MultiPipe *_mp)
{
    // find _mp in the Application Tree
    AppNode *found = find_AppNode(root, _mp);
    if (found == nullptr) {
        std::cerr << RED << "WindFlow Error: MultiPipe to be splitted does not belong to this PipeGraph" << DEFAULT << std::endl;
        exit(EXIT_FAILURE);
    }
    assert((found->children).size() == 0); // the node must be a leaf
    // prepare the MultiPipe _mp
    std::vector<MultiPipe *> splitMPs;
    std::vector<ff::ff_node *> normalization = _mp->normalize();
    _mp->isSplitted = true; // be careful --> this statement after normalize() not before!
    ff::ff_a2a *container = new ff::ff_a2a();
    container->add_firstset(normalization, 0, false);
    std::vector<ff::ff_node *> second_set;
    for (size_t i=0; i<_mp->splittingBranches; i++) {
        MultiPipe *split_mp = new MultiPipe(this);
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
MultiPipe *PipeGraph::execute_Merge(std::vector<MultiPipe *> _toBeMerged)
{
    // Case 1: self-merge
    if (_toBeMerged.size() == 1) {
        MultiPipe *mp = _toBeMerged[0];
        // find the MultiPipe instance to be self-merged in the Application Tree
        AppNode *found = find_AppNode(root, mp);
        if (found == nullptr) {
            std::cerr << RED << "WindFlow Error: MultiPipe to be self-merged does not belong to this PipeGraph" << DEFAULT << std::endl;
            exit(EXIT_FAILURE);
        }
        // create the new MultiPipe, the result of the self-merge
        auto normalization = mp->normalize();
        mp->isMerged = true;
        MultiPipe *mergedMP = new MultiPipe(this, normalization);
        toBeDeteled.push_back(mergedMP);
        // adjust the parent MultiPipe if it exists
        if (mp->fromSplitting && mp->splittingParent != nullptr) {
            MultiPipe *parent = mp->splittingParent;
            for (size_t i=0; i<(parent->splittingChildren).size(); i++) {
                if ((parent->splittingChildren)[i] == mp) {
                    (parent->splittingChildren)[i] = mergedMP;
                    break;
                }
            }
            auto second_set = (parent->last)->getSecondSet();
            std::vector<ff::ff_node *> new_second_set;
            for (size_t i=0; i<second_set.size(); i++) {
                MultiPipe *mp2 = static_cast<MultiPipe *>(second_set[i]);
                if (mp2 == mp)
                    new_second_set.push_back(mergedMP);
                else
                    new_second_set.push_back(second_set[i]);
            }
            (parent->last)->change_secondset(new_second_set, false);
            mergedMP->fromSplitting = true;
            mergedMP->splittingParent = mp->splittingParent;
        }
        // adjust the Application Tree
        std::vector<AppNode *> children_new;
        for (auto *an: (found->parent)->children) {
            if (an != found)
                children_new.push_back(an);
        }
        children_new.push_back(new AppNode(mergedMP, found->parent));
        (found->parent)->children = children_new;
        delete_AppNodes(found);
        return mergedMP;
    }
    // Case 2: merge
    else {
        // find the merged MultiPipe instances in the Application Tree
        std::vector<AppNode *> appnodes;
        for (auto *mp: _toBeMerged) {
            // the corresponding AppNode instance must be a child of the root
            AppNode *found = nullptr;
            for (auto *an: root->children) {
                if (an->mp == mp)
                    found = an;
            }
            if (found == nullptr) {
                std::cerr << RED << "WindFlow Error: MultiPipe to be merged does not belong to this PipeGraph" << DEFAULT << std::endl;
                exit(EXIT_FAILURE);
            }
            appnodes.push_back(found);
        }
        // create the new MultiPipe, the result of the merge
        std::vector<ff::ff_node *> normalization;
        for (auto *mp: _toBeMerged) {
            auto v = mp->normalize();
            mp->isMerged = true;
            normalization.insert(normalization.end(), v.begin(), v.end());
        }
        MultiPipe *mergedMP = new MultiPipe(this, normalization);
        toBeDeteled.push_back(mergedMP);
        // adjust the Application Tree
        std::vector<AppNode *> children_new;
        for (auto *an: root->children) {
            if (std::find(appnodes.begin(), appnodes.end(), an) == appnodes.end())
                children_new.push_back(an);
        }
        children_new.push_back(new AppNode(mergedMP, root));
        root->children = children_new;
        // delete the nodes
        for (auto *an: appnodes)
            delete_AppNodes(an);
        return mergedMP;
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
int PipeGraph::run()
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
size_t PipeGraph::getNumThreads() const
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
    return *this;
}

// implementation of the method to add an operator to the MultiPipe
template<typename emitter_t, typename collector_t>
void MultiPipe::add_operator(ff::ff_farm *_pattern, routing_types_t _type, ordering_mode_t _ordering)
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
    // splitted MultiPipe cannot be modified
    if (isSplitted) {
        std::cerr << RED << "WindFlow Error: MultiPipe has been splitted, operator cannot be added" << DEFAULT << std::endl;
        exit(EXIT_FAILURE);
    }
    // Case 1: first operator added after splitting
    if (fromSplitting && last == nullptr) {
        // create the initial matrioska
        ff::ff_a2a *matrioska = new ff::ff_a2a();
        std::vector<ff::ff_node *> first_set;
        auto workers = _pattern->getWorkers();
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
        Basic_Emitter *be = static_cast<Basic_Emitter *>(_pattern->getEmitter());
        assert(splittingParent != nullptr);
        splittingParent->prepareSplittingEmitters(be);
        return;
    }
    size_t n1 = (last->getFirstSet()).size();
    size_t n2 = (_pattern->getWorkers()).size();
    // Case 2: direct connection
    if ((n1 == n2) && _type == SIMPLE && !forceShuffling) {
        auto first_set = last->getFirstSet();
        auto worker_set = _pattern->getWorkers();
        // add the pattern's workers to the pipelines in the first set of the matrioska
        for (size_t i=0; i<n1; i++) {
            ff::ff_pipeline *stage = static_cast<ff::ff_pipeline *>(first_set[i]);
            stage->add_stage(worker_set[i], false);
        }
    }
    // Case 3: shuffling connection
    else {
        // prepare the nodes of the first_set of the last matrioska for the shuffling
        auto first_set_m = last->getFirstSet();
        for (size_t i=0; i<n1; i++) {
            ff::ff_pipeline *stage = static_cast<ff::ff_pipeline *>(first_set_m[i]);
            emitter_t *tmp_e = static_cast<emitter_t *>(_pattern->getEmitter());
            combine_with_laststage(*stage, new emitter_t(*tmp_e), true);
        }
        // create a new matrioska
        ff::ff_a2a *matrioska = new ff::ff_a2a();
        std::vector<ff::ff_node *> first_set;
        auto worker_set = _pattern->getWorkers();
        for (size_t i=0; i<n2; i++) {
            ff::ff_pipeline *stage = new ff::ff_pipeline();
            stage->add_stage(worker_set[i], false);
            if (lastParallelism != 1 || forceShuffling || _ordering == ID || _ordering == TS_RENUMBERING)
                combine_with_firststage(*stage, new collector_t(_ordering), true);
            else // we avoid the ordering node when possible
                combine_with_firststage(*stage, new dummy_mi(), true);
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
bool MultiPipe::chain_operator(ff::ff_farm *_pattern)
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
    // splitted MultiPipe cannot be modified
    if (isSplitted) {
        std::cerr << RED << "WindFlow Error: MultiPipe has been splitted, operator cannot be chained" << DEFAULT << std::endl;
        exit(EXIT_FAILURE);
    }
    // corner case -> first operator added to a MultiPipe after splitting (chain cannot work, add instead)
    if (fromSplitting && last == nullptr)
        return false;
    size_t n1 = (last->getFirstSet()).size();
    size_t n2 = (_pattern->getWorkers()).size();
    // _pattern is for sure SIMPLE: check additional conditions for chaining
    if ((n1 == n2) && (!forceShuffling)) {
        auto first_set = (last)->getFirstSet();
        auto worker_set = _pattern->getWorkers();
        // chaining the pattern's workers with the last node of each pipeline in the first set of the matrioska
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
std::vector<ff::ff_node *> MultiPipe::normalize()
{
    // check the Source presence
    if (!has_source) {
        std::cerr << RED << "WindFlow Error: MultiPipe does not have a Source, it cannot be merged/splitted" << DEFAULT << std::endl;
        exit(EXIT_FAILURE);
    }
    // check the Sink presence
    if (has_sink) {
        std::cerr << RED << "WindFlow Error: MultiPipe is terminated by a Sink, it cannot be merged/splitted" << DEFAULT << std::endl;
        exit(EXIT_FAILURE);
    }
    // empty MultiPipe cannot be normalized (only for splitting in this case)
    if (last == nullptr) {
        std::cerr << RED << "WindFlow Error: MultiPipe is empty, it cannot be splitted" << DEFAULT << std::endl;
        exit(EXIT_FAILURE);
    }
    std::vector<ff::ff_node *> result;
    // Case 1: the MultiPipe has not been splitted
    if (!isSplitted) {
        // Case 1.1 (only case in which the MultiPipe this is not modified)
        if (secondToLast == nullptr) {
            auto first_set = last->getFirstSet();
            for (size_t i=0; i<first_set.size(); i++)
                result.push_back(first_set[i]);
        }
        // Case 1.2
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
            ff::ff_pipeline *p = new ff::ff_pipeline(); // <-- Memory leak?
            p->add_stage((this->getStages())[0], false);
            result.push_back(p);
        }
    }
    // Case 2: the MultiPipe has been splitted
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
        ff::ff_pipeline *p = new ff::ff_pipeline(); // <-- Memory leak?
        p->add_stage(last, false);
        result.push_back(p);
    }
    return result;
}

// implementation of the prepareMergeSet method: base case 1
std::vector<MultiPipe *> MultiPipe::prepareMergeSet()
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
    // check whether the MultiPipe originates from a split (the parent must be self-merged before)
    if (_pipe.fromSplitting) {
        std::cerr << RED << "WindFlow Error: MultiPipe cannot be merged, its parent must be self-merged before" << DEFAULT << std::endl;
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
    // check whether the MultiPipe originates from a split (the parent must be self-merged before)
    if (_first.fromSplitting) {
        std::cerr << RED << "WindFlow Error: MultiPipe cannot be merged, its parent must be self-merged before" << DEFAULT << std::endl;
        exit(EXIT_FAILURE);
    }
    std::vector<MultiPipe *> output;
    output.push_back(&_first);
    auto v = prepareMergeSet(_pipes...);
    output.insert(output.end(), v.begin(), v.end());
    return output;
}

// implementation of the prepareSplittingEmitters method
void MultiPipe::prepareSplittingEmitters(Basic_Emitter *_e)
{
    assert(isSplitted);
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
int MultiPipe::run()
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
int MultiPipe::wait()
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
int MultiPipe::run_and_wait_end()
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
    // call the generic method to add the operator to the MultiPipe
    add_operator<Standard_Emitter<tuple_t>, Ordering_Node<tuple_t, tuple_t>>(&_filter, _filter.isKeyed() ? COMPLEX : SIMPLE, TS);
	return *this;
}

// implementation of the method to chain a Filter to the MultiPipe
template<typename tuple_t>
MultiPipe &MultiPipe::chain(Filter<tuple_t> &_filter)
{
    // try to chain the operator with the MultiPipe
    if (!_filter.isKeyed()) {
        bool chained = chain_operator<typename Filter<tuple_t>::Filter_Node>(&_filter);
        if (!chained)
            add(_filter);
    }
    else
        add(_filter);
    return *this;
}

// implementation of the method to add a Map to the MultiPipe
template<typename tuple_t, typename result_t>
MultiPipe &MultiPipe::add(Map<tuple_t, result_t> &_map)
{
    // call the generic method to add the operator to the MultiPipe
    add_operator<Standard_Emitter<tuple_t>, Ordering_Node<tuple_t, tuple_t>>(&_map, _map.isKeyed() ? COMPLEX : SIMPLE, TS);
	return *this;
}

// implementation of the method to chain a Map to the MultiPipe
template<typename tuple_t, typename result_t>
MultiPipe &MultiPipe::chain(Map<tuple_t, result_t> &_map)
{
    // try to chain the operator with the MultiPipe
    if (!_map.isKeyed()) {
        bool chained = chain_operator<typename Map<tuple_t, result_t>::Map_Node>(&_map);
        if (!chained)
            add(_map);
    }
    else
        add(_map);
    return *this;
}

// implementation of the method to add a FlatMap to the MultiPipe
template<typename tuple_t, typename result_t>
MultiPipe &MultiPipe::add(FlatMap<tuple_t, result_t> &_flatmap)
{
    // call the generic method to add the operator
    add_operator<Standard_Emitter<tuple_t>, Ordering_Node<tuple_t, tuple_t>>(&_flatmap, _flatmap.isKeyed() ? COMPLEX : SIMPLE, TS);
	return *this;
}

// implementation of the method to chain a FlatMap to the MultiPipe
template<typename tuple_t, typename result_t>
MultiPipe &MultiPipe::chain(FlatMap<tuple_t, result_t> &_flatmap)
{
    if (!_flatmap.isKeyed()) {
        bool chained = chain_operator<typename FlatMap<tuple_t, result_t>::FlatMap_Node>(&_flatmap);
        if (!chained)
            add(_flatmap);
    }
    else
        add(_flatmap);
    return *this;
}

// implementation of the method to add an Accumulator to the MultiPipe
template<typename tuple_t, typename result_t>
MultiPipe &MultiPipe::add(Accumulator<tuple_t, result_t> &_acc)
{
    // call the generic method to add the operator to the MultiPipe
    add_operator<Standard_Emitter<tuple_t>, Ordering_Node<tuple_t, tuple_t>>(&_acc, COMPLEX, TS);
    return *this;
}

// implementation of the method to add a Win_Farm to the MultiPipe
template<typename tuple_t, typename result_t>
MultiPipe &MultiPipe::add(Win_Farm<tuple_t, result_t> &_wf)
{
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
    return *this;
}

// implementation of the method to add a Win_Farm_GPU to the MultiPipe
template<typename tuple_t, typename result_t, typename F_t>
MultiPipe &MultiPipe::add(Win_Farm_GPU<tuple_t, result_t, F_t> &_wf)
{
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
    return *this;
}

// implementation of the method to add a Key_Farm to the MultiPipe
template<typename tuple_t, typename result_t>
MultiPipe &MultiPipe::add(Key_Farm<tuple_t, result_t> &_kf)
{
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
    return *this;
}

// implementation of the method to add a Key_Farm_GPU to the MultiPipe
template<typename tuple_t, typename result_t, typename F_t>
MultiPipe &MultiPipe::add(Key_Farm_GPU<tuple_t, result_t, F_t> &_kf)
{
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
    return *this;
}

// implementation of the method to add a Pane_Farm to the MultiPipe
template<typename tuple_t, typename result_t>
MultiPipe &MultiPipe::add(Pane_Farm<tuple_t, result_t> &_pf)
{
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
    return *this;
}

// implementation of the method to add a Pane_Farm_GPU to the MultiPipe
template<typename tuple_t, typename result_t, typename F_t>
MultiPipe &MultiPipe::add(Pane_Farm_GPU<tuple_t, result_t, F_t> &_pf)
{
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
    return *this;
}

// implementation of the method to add a Win_MapReduce to the MultiPipe
template<typename tuple_t, typename result_t>
MultiPipe &MultiPipe::add(Win_MapReduce<tuple_t, result_t> &_wmr)
{
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
    return *this;
}

// implementation of the method to add a Win_MapReduce_GPU to the MultiPipe
template<typename tuple_t, typename result_t, typename F_t>
MultiPipe &MultiPipe::add(Win_MapReduce_GPU<tuple_t, result_t, F_t> &_wmr)
{
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
    return *this;
}

// implementation of the method to add a Sink to the MultiPipe
template<typename tuple_t>
MultiPipe &MultiPipe::add_sink(Sink<tuple_t> &_sink)
{
    // call the generic method to add the operator to the MultiPipe
    add_operator<Standard_Emitter<tuple_t>, Ordering_Node<tuple_t, tuple_t>>(&_sink, _sink.isKeyed() ? COMPLEX : SIMPLE, TS);
	has_sink = true;
	return *this;
}

// implementation of the method to chain a Sink to the MultiPipe
template<typename tuple_t>
MultiPipe &MultiPipe::chain_sink(Sink<tuple_t> &_sink)
{
    // try to chain the Sink with the MultiPipe
    if (!_sink.isKeyed()) {
        bool chained = chain_operator<typename Sink<tuple_t>::Sink_Node>(&_sink);
        if (!chained)
            return add_sink(_sink);
    }
    else
        return add_sink(_sink);
    has_sink = true;
    return *this;
}

// implementation of the method to merge all the internal splitted MultiPipe instances of this
MultiPipe &MultiPipe::self_merge()
{
    // check whether the MultiPipe has been already merged
    if (isMerged) {
        std::cerr << RED << "WindFlow Error: MultiPipe has been already merged" << DEFAULT << std::endl;
        exit(EXIT_FAILURE);
    }
    // check whether the MultiPipe has been splitted
    if (!isSplitted) {
        std::cerr << RED << "WindFlow Error: to be self-merged a MultiPipe must have been splitted before" << DEFAULT << std::endl;
        exit(EXIT_FAILURE);
    }
    std::vector<MultiPipe *> mergeSet;
    mergeSet.push_back(this);
    // execute the merge through the PipeGraph
    MultiPipe *mergedMP = graph->execute_Merge(mergeSet);
    return *mergedMP;
}

// implementation of the method to merge this with a set of independent MultiPipe instances _pipes
template<typename ...MULTIPIPES>
MultiPipe &MultiPipe::merge(MULTIPIPES&... _pipes)
{
    auto mergeSet = prepareMergeSet(*this, _pipes...);
    assert(mergeSet.size() > 1);
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
    // execute the merge through the PipeGraph
    MultiPipe *mergedMP = graph->execute_Merge(mergeSet);
    return *mergedMP;
}

// implementation of the method to split this into several MultiPipe instances
template<typename F_t>
void MultiPipe::split(F_t _splitting_func, size_t _cardinality)
{
    // check whether the MultiPipe has been merged
    if (isMerged) {
        std::cerr << RED << "WindFlow Error: MultiPipe has been merged and cannot be splitted" << DEFAULT << std::endl;
        exit(EXIT_FAILURE);
    }
    // check whether the MultiPipe has been already splitted
    if (isSplitted) {
        std::cerr << RED << "WindFlow Error: MultiPipe has been already splitted" << DEFAULT << std::endl;
        exit(EXIT_FAILURE);
    }
    // prepare the splitting of this
    splittingBranches = _cardinality;
    splittingEmitterRoot = new Splitting_Emitter<decltype(get_tuple_split_t(_splitting_func))>(_splitting_func, _cardinality);
    // execute the merge through the PipeGraph
    splittingChildren = graph->execute_Split(this);
}

// implementation of the method to get the MultiPipe with index _idx from this (this must have been splitted before)
MultiPipe &MultiPipe::select(size_t _idx) const
{
    // check whether the MultiPipe has been merged
    if (isMerged) {
        std::cerr << RED << "WindFlow Error: MultiPipe has been merged, select() cannot be executed" << DEFAULT << std::endl;
        exit(EXIT_FAILURE);
    }
    if (!isSplitted) {
        std::cerr << RED << "WindFlow Error: MultiPipe has not been splitted, select() cannot be executed" << DEFAULT << std::endl;
        exit(EXIT_FAILURE);
    }
    if (_idx >= splittingChildren.size()) {
        std::cerr << RED << "WindFlow Error: index of select() is out of range" << DEFAULT << std::endl;
        exit(EXIT_FAILURE);
    }
    return *(splittingChildren[_idx]);
}

// implementation of the method to check whether a MultiPipe is runnable
bool MultiPipe::isRunnable() const
{
	return (has_source && has_sink) || (isMerged) || (isSplitted);
}

// implementation of the method to check whether a MultiPipe has a Source
bool MultiPipe::hasSource() const
{
	return has_source;
}

// implementation of the method to check whether a MultiPipe has a Sink
bool MultiPipe::hasSink() const
{
	return has_sink;
}

// implementation of the method to return the number of threads used to run this MultiPipe
size_t MultiPipe::getNumThreads() const
{
    if (!isSplitted)
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
