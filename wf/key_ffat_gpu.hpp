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
 *  @file    key_ffat_gpu.hpp
 *  @author  Gabriele Mencagli
 *  @date    17/03/2020
 *  
 *  @brief Key_FFAT_GPU operator executing a windowed query in parallel
 *         on a CPU+GPU system using the FlatFAT algorithm for GPU
 *  
 *  @section Key_FFAT_GPU (Description)
 *  
 *  This file implements the Key_FFAT_GPU operator able to executes windowed queries
 *  on a heterogeneous system (CPU+GPU). The operator prepares batches of input tuples
 *  in parallel on the CPU cores and offloads on the GPU the parallel processing of the
 *  windows within each batch. Batches of different sub-streams can be executed in
 *  parallel while consecutive batches of the same sub-stream are prepared on the CPU and
 *  offloaded on the GPU sequentially. However, windows are efficiently processed using
 *  a variant for GPU of the FlatFAT algorithm.
 *  
 *  The template parameters tuple_t and result_t must be default constructible, with a copy
 *  constructor and copy assignment operator, and they must provide and implement the
 *  setControlFields() and getControlFields() methods. The third template argument win_F_t
 *  is the type of the callable object to be used for GPU processing.
 */ 

#ifndef KEY_FFFAT_GPU_H
#define KEY_FFFAT_GPU_H

/// includes
#include <ff/pipeline.hpp>
#include <ff/all2all.hpp>
#include <ff/farm.hpp>
#include <ff/optimize.hpp>
#include <basic.hpp>
#include <win_seqffat_gpu.hpp>
#include <kf_nodes.hpp>

namespace wf {

/** 
 *  \class Key_FFAT_GPU
 *  
 *  \brief Key_FFAT_GPU operator executing a windowed query in parallel on a CPU+GPU system
 *  
 *  This class implements the Key_FFAT_GPU operator. The operator prepares in parallel distinct
 *  batches of tuples (on the CPU cores) and offloads the processing of the batches on the GPU
 *  by computing in parallel all the windows within a batch on the CUDA cores of the GPU. Batches
 *  with tuples of same sub-stream are prepared/offloaded sequentially on the CPU. However,
 *  windows of the same substream are executed efficiently by using a variant for GPU of the FlatFAT
 *  algorithm.
 */ 
template<typename tuple_t, typename result_t, typename comb_F_t>
class Key_FFAT_GPU: public ff::ff_farm
{
public:
    /// type of the lift function
    using winLift_func_t = std::function<void(const tuple_t &, result_t &)>;
    /// function type to map the key hashcode onto an identifier starting from zero to pardegree-1
    using routing_func_t = std::function<size_t(size_t, size_t)>;

private:
    // type of the Win_SeqFFAT to be created
    using win_seqffat_gpu_t = Win_SeqFFAT_GPU<tuple_t, result_t, comb_F_t>;
    // type of the KF_Emitter node
    using kf_emitter_t = KF_Emitter<tuple_t>;
    // type of the KF_Collector node
    using kf_collector_t = KF_Collector<result_t>;
    // friendships with other classes in the library
    friend class MultiPipe;
    // parallelism of the Key_FFAT_GPU
    size_t parallelism;
    // window type (CB or TB)
    win_type_t winType;
    bool used; // true if the operator has been added/chained in a MultiPipe
    std::vector<ff_node *> kf_workers; // vector of pointers to the Key_FFAT_GPU workers

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _winLift_func the lift function to translate a tuple into a result (__host__ function)
     *  \param _winComb_func the combine function to combine two results into a result (__host__ __device__ function)
     *  \param _win_len window length (in no. of tuples or in time units)
     *  \param _slide_len slide length (in no. of tuples or in time units)
     *  \param _triggering_delay (triggering delay in time units, meaningful for TB windows only otherwise it must be 0)
     *  \param _winType window type (count-based CB or time-based TB)
     *  \param _pardegree parallelism degree of the Key_FFAT_GPU operator
     *  \param _batch_len no. of windows in a batch
     *  \param _n_thread_block number of threads per block
     *  \param _rebuild flag stating whether the FlatFAT_GPU must be rebuilt from scratch for each new batch
     *  \param _name string with the unique name of the operator
     *  \param _routing_func function to map the key hashcode onto an identifier starting from zero to pardegree-1
     */ 
    Key_FFAT_GPU(winLift_func_t _winLift_func,
                 comb_F_t _winComb_func,
                 uint64_t _win_len,
                 uint64_t _slide_len,
                 uint64_t _triggering_delay,
                 win_type_t _winType,
                 size_t _pardegree,
                 size_t _batch_len,
                 size_t _n_thread_block,
                 bool _rebuild,
                 std::string _name,
                 routing_func_t _routing_func):
                 parallelism(_pardegree),
                 winType(_winType),
                 used(false)
    {
        // check the validity of the windowing parameters
        if (_win_len == 0 || _slide_len == 0) {
            std::cerr << RED << "WindFlow Error: window length or slide in Key_FFAT_GPU cannot be zero" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // check the use of sliding windows
        if (_slide_len >= _win_len) {
            std::cerr << RED << "WindFlow Error: Key_FFAT_GPU can be used with sliding windows only (s<w)" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // check the validity of the parallelism degree
        if (_pardegree == 0) {
            std::cerr << RED << "WindFlow Error: Key_FFAT_GPU has parallelism zero" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // check the validity of the batch length
        if (_batch_len == 0) {
            std::cerr << RED << "WindFlow Error: batch length in Key_FFAT_GPU cannot be zero" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // std::vector of Win_SeqFFAT_GPU
        std::vector<ff::ff_node *> w(_pardegree);
        // create the Win_SeqFFAT_GPU
        for (size_t i = 0; i < _pardegree; i++) {
            auto *ffat_gpu = new win_seqffat_gpu_t(_winLift_func, _winComb_func, _win_len, _slide_len, _triggering_delay, _winType, _batch_len, _n_thread_block, _rebuild, _name + "_kff");
            w[i] = ffat_gpu;
            kf_workers.push_back(ffat_gpu);
        }
        ff::ff_farm::add_workers(w);
        ff::ff_farm::add_collector(nullptr);
        // create the Emitter node
        ff::ff_farm::add_emitter(new kf_emitter_t(_routing_func, _pardegree));
        // when the Key_Farm_GPU will be destroyed we need aslo to destroy the emitter, workers and collector
        ff::ff_farm::cleanup_all();
    }

    /** 
     *  \brief Get the parallelism degree of the Key_FFAT_GPU
     *  \return parallelism degree of the Key_FFAT_GPU
     */ 
    size_t getParallelism() const
    {
        return parallelism;
    }

    /** 
     *  \brief Get the window type (CB or TB) utilized by the operator
     *  \return adopted windowing semantics (count- or time-based)
     */ 
    win_type_t getWinType() const
    {
        return winType;
    }

    /** 
     *  \brief Check whether the Key_FFAT_GPU has been used in a MultiPipe
     *  \return true if the Key_FFAT_GPU has been added/chained to an existing MultiPipe
     */
    bool isUsed() const
    {
        return used;
    }

    /** 
     *  \brief Get the number of dropped tuples by the Key_FFAT_GPU
     *  \return number of tuples dropped during the processing by the Key_FFAT_GPU
     */ 
    size_t getNumDroppedTuples() const
    {
        size_t count = 0;
        for (auto *w: kf_workers) {
            auto *seq = static_cast<win_seqffat_gpu_t *>(w);
            count += seq->getNumDroppedTuples();
        }
        return count;
    }

    /// deleted constructors/operators
    Key_FFAT_GPU(const Key_FFAT_GPU &) = delete; // copy constructor
    Key_FFAT_GPU(Key_FFAT_GPU &&) = delete; // move constructor
    Key_FFAT_GPU &operator=(const Key_FFAT_GPU &) = delete; // copy assignment operator
    Key_FFAT_GPU &operator=(Key_FFAT_GPU &&) = delete; // move assignment operator
};

} // namespace wf

#endif
