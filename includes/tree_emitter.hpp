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
 *  @file    tree_emitter.hpp
 *  @author  Gabriele Mencagli
 *  @date    12/06/2019
 *  
 *  @brief Tree_Emitter executing a tree-based emitter logic
 *  
 *  @section Tree_Emitter (Description)
 *  
 *  This file implements the Tree_Emitter executing a tree-based emitter logic,
 *  with one emitter acting as the root and a set of N>1 emitter nodes working
 *  as leaves of a single-level tree.
 */ 

#ifndef TREEEMITTER_H
#define TREEEMITTER_H

// includes
#include <vector>
#include <ff/multinode.hpp>
#include <basic_emitter.hpp>

namespace wf {

// class Tree_Emitter
class Tree_Emitter: public Basic_Emitter
{
private:
    Basic_Emitter *root; // root node
    std::vector<Basic_Emitter *> children; // vector of children nodes
    bool cleanUpRoot; // flag to control the cleanup of the root node
    bool cleanUpChildren; // flag to control the cleanup of the children nodes
    bool isCombined; // true if this node is used within a Tree_Emitter node
    std::vector<std::pair<void *, int>> output_queue; // used in case of Tree_Emitter mode

public:
    // Constructor
    Tree_Emitter(Basic_Emitter *_root,
             std::vector<Basic_Emitter *> _children,
             bool _cleanUpRoot=true,
             bool _cleanUpChildren=true):
             root(_root),
             children(_children),
             cleanUpRoot(_cleanUpRoot),
             cleanUpChildren(_cleanUpChildren),
             isCombined(false)
    {
        // configure all the nodes to work with the Tree_Emitter
        root->setTree_EmitterMode(true);
        for (size_t i=0; i<children.size(); i++)
            children[i]->setTree_EmitterMode(true);
    }

    // Copy Constructor
    Tree_Emitter(const Tree_Emitter &_e)
    {
        // deep copy of the root emitter node
        root = (_e.root)->clone();
        // deep copy of each child emitter node
        for (size_t i=0; i<(_e.children).size(); i++)
            children.push_back((_e.children[i])->clone());
        cleanUpRoot = true;
        cleanUpChildren = true;
        isCombined = _e.isCombined;
        output_queue = _e.output_queue;
    }

    // Destructor
    ~Tree_Emitter()
    {
        if (cleanUpRoot)
            delete root;
        if (cleanUpChildren) {
            for (size_t i=0; i<children.size(); i++)
                delete children[i];
        }
    }

    // clone method
    Basic_Emitter *clone() const
    {
        Basic_Emitter *copy_root = root->clone();
        std::vector<Basic_Emitter *> copy_children;
        for (size_t i=0; i<children.size(); i++)
            copy_children.push_back((children[i])->clone());
        auto *t = new Tree_Emitter(copy_root, copy_children, true, true);
        t->isCombined = this->isCombined;
        return t;
    }

    // svc_init method (utilized by the FastFlow runtime)
    int svc_init()
    {
        return 0;
    }

    // svc method (utilized by the FastFlow runtime)
    void *svc(void *t)
    {
        // call the root svc
        root->svc(t);
        auto &v1 = root->getOutputQueue();
        for (auto msg1: v1) {
            // call the child svc
            children[msg1.second]->svc(msg1.first);
            auto &v2 = children[msg1.second]->getOutputQueue();
            for (auto msg2: v2) {
                size_t offset = 0;
                for (size_t i=0; i<msg1.second; i++)
                    offset += children[i]->getNDestinations();
                if (!isCombined)
                    this->ff_send_out_to(msg2.first, offset + msg2.second);
                else
                    output_queue.push_back(std::make_pair(msg2.first, offset + msg2.second));
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
            children[msg1.second]->svc(msg1.first);
            auto &v2 = children[msg1.second]->getOutputQueue();
            for (auto msg2: v2) {
                size_t offset = 0;
                for (size_t i=0; i<msg1.second; i++)
                    offset += children[i]->getNDestinations();
                if (!isCombined)
                    this->ff_send_out_to(msg2.first, offset + msg2.second);
                else
                    output_queue.push_back(std::make_pair(msg2.first, offset + msg2.second));
            }
            v2.clear();
        }
        v1.clear();
        // call the eosnotify for the children
        size_t offset = 0;
        for (size_t i=0; i<children.size(); i++) {
            children[i]->eosnotify(-1);
            auto &v = children[i]->getOutputQueue();
            for (auto msg: v) {
                if (!isCombined)
                    this->ff_send_out_to(msg.first, offset + msg.second);
                else
                    output_queue.push_back(std::make_pair(msg.first, offset + msg.second));
            }
            v.clear();
            offset += children[i]->getNDestinations();
        }
    }

    // svc_end method (FastFlow runtime)
    void svc_end() {}

    // get the number of destinations
    size_t getNDestinations() const
    {
        size_t sum = 0;
        for (size_t i=0; i<children.size(); i++)
            sum += children[i]->getNDestinations();
        return sum;
    }

    // set/unset the Tree_Emitter mode
    void setTree_EmitterMode(bool _val)
    {
        isCombined = _val;
    }

    // method to get a reference to the internal output queue (used in Tree_Emitter mode)
    std::vector<std::pair<void *, int>> &getOutputQueue()
    {
        return output_queue;
    }

    // method to return the root node
    Basic_Emitter *getRootNode()
    {
        return root;
    }

    // method to return the children nodes
    std::vector<Basic_Emitter *> getChildrenNodes()
    {
        return children;
    }
};

} // namespace wf

#endif
