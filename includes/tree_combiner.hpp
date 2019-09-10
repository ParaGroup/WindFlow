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
 *  @file    tree_combiner.hpp
 *  @author  Gabriele Mencagli
 *  @date    12/06/2019
 *  
 *  @brief Building block to combine a single-level tree into a FastFlow node
 *  
 *  @section Tree Combiner (Description)
 *  
 *  This file implements the building block that combines, into a single FastFlow node
 *  and thus one thread, a single-level tree composed of a root node and N children.
 *  All the nodes must be multioutput.
 */ 

#ifndef TREECOMB_H
#define TREECOMB_H

// includes
#include <vector>
#include <ff/multinode.hpp>

namespace wf {

// class TreeComb
template<typename root_t, typename child_t>
class TreeComb: public ff::ff_monode_t<typename root_t::in_type, typename child_t::out_type>
{
private:
    using input_t = typename root_t::in_type;
    using output_t = typename child_t::out_type;
    root_t *root; // root node
    std::vector<child_t *> child; // std::vector of children nodes
    bool cleanUpRoot; // flag to control the cleanup of the root node
    bool cleanUpChildren; // flag to control the cleanup of the children nodes

public:
    // Constructor
    TreeComb(root_t *_root,
             std::vector<child_t *> _child,
             bool _cleanUpRoot=true,
             bool _cleanUpChildren=true):
             root(_root),
             child(_child),
             cleanUpRoot(_cleanUpRoot),
             cleanUpChildren(_cleanUpChildren)
    {
        // configure all the nodes to work with the TreeComb
        root->setTreeCombMode(true);
        for (size_t i=0; i<child.size(); i++)
            child[i]->setTreeCombMode(true);
    }

    // Copy Constructor
    TreeComb(const TreeComb &_e)
    {
        // deep copy of the root emitter node
        root = new root_t(*(_e.root));
        // deep copy of each child emitter node
        for (size_t i=0; i<_e.child.size(); i++)
            child.push_back(new child_t(*(_e.child[i])));
        cleanUpRoot = true;
        cleanUpChildren = true;
    }

    // Destructor
    ~TreeComb()
    {
        if (cleanUpRoot)
            delete root;
        if (cleanUpChildren) {
            for (size_t i=0; i<child.size(); i++)
                delete child[i];
        }
    }

    // svc_init method (utilized by the FastFlow runtime)
    int svc_init()
    {
        return 0;
    }

    // svc method (utilized by the FastFlow runtime)
    output_t *svc(input_t *t)
    {
        // call the root svc
        root->svc(t);
        auto &v1 = root->getOutputQueue();
        for (auto msg1: v1) {
            // call the child svc
            child[msg1.second]->svc(msg1.first);
            auto &v2 = child[msg1.second]->getOutputQueue();
            for (auto msg2: v2) {
                size_t offset = 0;
                for (size_t i=0; i<msg1.second; i++)
                    offset += child[i]->getNDestinations();
                this->ff_send_out_to(msg2.first, offset + msg2.second);
            }
            v2.clear();
        }
        v1.clear();
        return this->GO_ON;
    }

    // method to manage the EOS (utilized by the FastFlow runtime)
    void eosnotify(ssize_t id)
    {
        // call the root eosnotify method
        root->eosnotify(-1);
        auto &v1 = root->getOutputQueue();
        for (auto msg1: v1) {
            // call the child svc
            child[msg1.second]->svc(msg1.first);
            auto &v2 = child[msg1.second]->getOutputQueue();
            for (auto msg2: v2) {
                size_t offset = 0;
                for (size_t i=0; i<msg1.second; i++)
                    offset += child[i]->getNDestinations();
                this->ff_send_out_to(msg2.first, offset + msg2.second);
            }
            v2.clear();
        }
        v1.clear();
        // call the eosnotify for the children
        size_t offset = 0;
        for (size_t i=0; i<child.size(); i++) {
            child[i]->eosnotify(-1);
            auto &v = child[i]->getOutputQueue();
            for (auto msg: v) {
                this->ff_send_out_to(msg.first, offset + msg.second);
            }
            v.clear();
            offset += child[i]->getNDestinations();
        }
    }

    // svc_end method (FastFlow runtime)
    void svc_end() {}

    // method to return the root node
    root_t *getRootNode() const { return root; }

    // method to return the children nodes
    std::vector<child_t> getChildrenNodes() const { return child; }
};

} // namespace wf

#endif
