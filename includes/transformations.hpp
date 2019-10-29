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
 *  @file    transformations.hpp
 *  @author  Gabriele Mencagli
 *  @date    15/06/2019
 *  
 *  @brief Set of transformations useful for operators optimization
 *  
 *  @section Transformations (Description)
 *  
 *  This file implements a set of transformations useful to optimize
 *  the structure of the WindFlow operators.
 */ 

#ifndef TRANSF_H
#define TRANSF_H

// includes
#include <ff/ff.hpp>

namespace wf {

// struct of the dummy multi-input node
struct dummy_mi: ff::ff_minode
{
    void *svc(void *in)
    {
        return in;
    }
};

// struct of the dummy multi-ouput node
struct dummy_mo: ff::ff_monode
{
    void *svc(void *in)
    {
        return in;
    }
};

// method to transform a farm into a all-to-all (with collector)
ff::ff_a2a *farm2A2A_collector(ff::ff_farm *farm)
{
    // create the a2a
    ff::ff_a2a *a2a = new ff::ff_a2a();
    auto &ws = farm->getWorkers();
    std::vector<ff::ff_node *> first_set;
    for (auto *w: ws) {
        ff::ff_comb *comb = new ff::ff_comb(w, new dummy_mo(), false, true);
        first_set.push_back(comb);
    }
    a2a->add_firstset(first_set, 0, true);
    std::vector<ff::ff_node *> second_set;
    second_set.push_back(farm->getCollector());
    a2a->add_secondset(second_set, false);
    return a2a;
}

// method to transform a farm into a all-to-all (with emitter)
ff::ff_a2a *farm2A2A_emitter(ff::ff_farm *farm)
{
    // create the a2a
    ff::ff_a2a *a2a = new ff::ff_a2a();
    std::vector<ff::ff_node *> first_set;
    first_set.push_back(farm->getEmitter());
    a2a->add_firstset(first_set, 0, false);
    auto &ws = farm->getWorkers();
    std::vector<ff::ff_node *> second_set;
    for (auto *w: ws) {
        ff::ff_comb *comb = new ff::ff_comb(new dummy_mi(), w, true, false);
        second_set.push_back(comb);
    }
    a2a->add_secondset(second_set, true);
    return a2a;
}

// method to combine a set of nodes with the first set of an a2a
void combine_a2a_withFirstNodes(ff::ff_a2a *a2a, const std::vector<ff::ff_node*> nodes, bool cleanup=false)
{
    auto firstset = a2a->getFirstSet();
    assert(firstset.size() == nodes.size());
    std::vector<ff::ff_node *> new_firstset;
    size_t i=0;
    for (auto *w: firstset) {
        ff::ff_comb *comb = new ff::ff_comb(nodes[i++], w, cleanup, false);
        new_firstset.push_back(comb);
    }
    a2a->change_firstset(new_firstset, 0, true);
}

// method to remove the first emitter in a pipeline (there might be memory leaks here! to be fixed)
ff::ff_node *remove_emitter_from_pipe(ff::ff_pipeline &pipe_in)
{
    auto &stages = pipe_in.getStages();
    if (stages.size() == 1 && stages[0]->isFarm()) {
        ff::ff_farm *farm = static_cast<ff::ff_farm *>(stages[0]);
        farm->cleanup_emitter(false);
        farm->cleanup_workers(false);
        farm->cleanup_collector(false);
        ff::ff_node *emitter = farm->getEmitter();
        if (!emitter->isComp() && (farm->getWorkers()).size() > 1) {
            // remove the farm from the pipeline
            pipe_in.remove_stage(0);
            // create the a2a
            ff::ff_a2a *a2a = farm2A2A_collector(farm);
            pipe_in.insert_stage(0, a2a, false); // should be true the cleanup here!
            return emitter;
        }
        else if (!emitter->isComp() && (farm->getWorkers()).size() == 1) {
            // remove the farm from the pipeline
            pipe_in.remove_stage(0);
            const ff::svector<ff::ff_node*> &ws = farm->getWorkers();
            pipe_in.insert_stage(0, ws[0], false);
            return emitter;
        }
        else {
            // remove the farm from the pipeline
            pipe_in.remove_stage(0);
            ff::ff_a2a *a2a = farm2A2A_emitter(farm);
            pipe_in.insert_stage(0, a2a, false); // should be true the cleanup here!
            return nullptr;
        }
    }
    else if (stages.size() == 2 && stages[0]->isFarm()) {
        ff::ff_farm *farm = static_cast<ff::ff_farm *>(stages[0]);
        farm->cleanup_emitter(false);
        farm->cleanup_workers(false);
        farm->cleanup_collector(false);
        ff::ff_node *emitter = farm->getEmitter();
        if (farm->getCollector() != nullptr) {
            // remove the farm from the pipeline
            pipe_in.remove_stage(0);
            // create the a2a
            ff::ff_a2a *a2a = farm2A2A_collector(farm);
            pipe_in.insert_stage(0, a2a, false); // should be true the cleanup here!
            return emitter;
        }
        else {
            // remove the farm from the pipeline
            ff::ff_farm *farm2 = static_cast<ff::ff_farm *>(stages[1]);
            pipe_in.remove_stage(0);
            pipe_in.remove_stage(0);
            // create the a2a
            ff::ff_a2a *a2a = new ff::ff_a2a();
            auto &ws = farm->getWorkers();
            std::vector<ff::ff_node *> first_set;
            for (auto *w: ws) {
                ff::ff_comb *comb = new ff::ff_comb(w, new dummy_mo(), false, true);
                first_set.push_back(comb);
            }
            a2a->add_firstset(first_set, 0, true);
            std::vector<ff::ff_node *> second_set;
            second_set.push_back(farm2);
            a2a->add_secondset(second_set, false);
            pipe_in.insert_stage(0, a2a, false); // should be true the cleanup here!
            return emitter;
        }
    }
    else
        return nullptr;
}

} // namespace wf

#endif
