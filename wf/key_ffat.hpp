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
 *  @file    key_ffat.hpp
 *  @author  Gabriele Mencagli
 *  @date    12/03/2020
 *  
 *  @brief Key_FFAT operator executing a windowed query in parallel
 *         on multi-core CPUs using the FlatFAT algorithm
 *  
 *  @section Key_FFAT (Description)
 *  
 *  This file implements the Key_FFAT operator able to execute windowed queries on a
 *  multicore. The operator executes streaming windows in parallel on the CPU cores.
 *  Only windows belonging to different sub-streams can be executed in parallel, while
 *  windows of the same sub-stream are executed rigorously in order. However, windows
 *  are efficiently processed using the FlatFAT algorithm.
 *  
 *  The template parameters tuple_t and result_t must be default constructible, with
 *  a copy constructor and copy assignment operator, and they must provide and implement
 *  the setControlFields() and getControlFields() methods.
 */ 

#ifndef KEY_FFAT_H
#define KEY_FFAT_H

/// includes
#include<ff/pipeline.hpp>
#include<ff/all2all.hpp>
#include<ff/farm.hpp>
#include<ff/optimize.hpp>
#include<basic.hpp>
#include<win_seqffat.hpp>
#include<kf_nodes.hpp>
#include<basic_operator.hpp>

namespace wf {

/** 
 *  \class Key_FFAT
 *  
 *  \brief Key_FFAT operator executing a windowed query in parallel on multi-core CPUs
 *         leveraging the FlatFAT algorithm
 *  
 *  This class implements the Key_FFAT operator executing windowed queries in parallel on
 *  a multicore. In the operator, only windows belonging to different sub-streams can be
 *  executed in parallel. However, windows of the same substream are executed efficiently
 *  by using the FlatFAT algorithm.
 */ 
template<typename tuple_t, typename result_t>
class Key_FFAT: public ff::ff_farm, public Basic_Operator
{
public:
    /// type of the lift function
    using winLift_func_t = std::function<void(const tuple_t &, result_t &)>;
    /// type of the rich lift function
    using rich_winLift_func_t = std::function<void(const tuple_t &, result_t &, RuntimeContext &)>;
    /// type of the combine function
    using winComb_func_t = std::function<void(const result_t &, const result_t &, result_t &)>;
    /// type of the rich combine function
    using rich_winComb_func_t = std::function<void(const result_t &, const result_t &, result_t &, RuntimeContext &)>;
    /// type of the closing function
    using closing_func_t = std::function<void(RuntimeContext &)>;
    /// type of the functionto map the key hashcode onto an identifier starting from zero to parallelism-1
    using routing_func_t = std::function<size_t(size_t, size_t)>;

private:
    // type of the Win_SeqFFAT to be created
    using win_seqffat_t = Win_SeqFFAT<tuple_t, result_t>;
    // type of the KF_Emitter node
    using kf_emitter_t = KF_Emitter<tuple_t>;
    // type of the KF_Collector node
    using kf_collector_t = KF_Collector<result_t>;
    // friendships with other classes in the library
    friend class MultiPipe;
    std::string name; // name of the Key_FFAT
    size_t parallelism; // internal parallelism of the Key_FFAT
    bool used; // true if the Key_FFAT has been added/chained in a MultiPipe
    win_type_t winType; // type of windows (count-based or time-based)

    // method to set the isRenumbering mode of the internal nodes
    void set_isRenumbering()
    {
    	assert(winType == CB); // only count-based windows
    	for (auto *node: this->getWorkers()) {
    		win_seqffat_t *seq = static_cast<win_seqffat_t *>(node);
    		seq->isRenumbering = true;
    	}
    }

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _winLift_func the (riched or not) lift function to translate a tuple into a result (with a signature accepted by the Key_FFAT operator)
     *  \param _winComb_func the (riched or not) combine function to combine two results into a result (with a signature accepted by the Key_FFAT operator)
     *  \param _win_len window length (in no. of tuples or in time units)
     *  \param _slide_len slide length (in no. of tuples or in time units)
     *  \param _triggering_delay (triggering delay in time units, meaningful for TB windows only otherwise it must be 0)
     *  \param _winType window type (count-based CB or time-based TB)
     *  \param _parallelism internal parallelism of the Key_Farm operator
     *  \param _name string with the unique name of the operator
     *  \param _closing_func closing function
     *  \param _routing_func function to map the key hashcode onto an identifier starting from zero to parallelism-1
     */ 
    template<typename lift_F_t, typename comb_F_t>
    Key_FFAT(lift_F_t _winLift_func,
             comb_F_t _winComb_func,
             uint64_t _win_len,
             uint64_t _slide_len,
             uint64_t _triggering_delay,
             win_type_t _winType,
             size_t _parallelism,
             std::string _name,
             closing_func_t _closing_func,
             routing_func_t _routing_func):
             name(_name),
             parallelism(_parallelism),
             used(false),
             winType(_winType)     
    {
        // check the validity of the windowing parameters
        if (_win_len == 0 || _slide_len == 0) {
            std::cerr << RED << "WindFlow Error window length or slide in Key_Farm cannot be zero" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // check the use of sliding windows
        if (_slide_len >= _win_len) {
            std::cerr << RED << "WindFlow Error: Key_FFAT can be used with sliding windows only (s<w)" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // check the validity of the parallelism value
        if (_parallelism == 0) {
            std::cerr << RED << "WindFlow Error: Key_FFAT has parallelism zero" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // std::vector of Win_SeqFFAT
        std::vector<ff_node *> w(_parallelism);
        // create the Win_SeqFFAT
        for (size_t i = 0; i < _parallelism; i++) {
            WinOperatorConfig configSeq(0, 1, _slide_len, 0, 1, _slide_len);
            auto *ffat = new win_seqffat_t(_winLift_func, _winComb_func, _win_len, _slide_len, _triggering_delay, _winType, _name, _closing_func, RuntimeContext(_parallelism, i), configSeq);
            w[i] = ffat;
        }
        ff::ff_farm::add_workers(w);
        ff::ff_farm::add_collector(nullptr);
        // create the Emitter node
        ff::ff_farm::add_emitter(new kf_emitter_t(_routing_func, _parallelism));
        // when the Key_FFAT will be destroyed we need aslo to destroy the emitter, workers and collector
        ff::ff_farm::cleanup_all();
    }

    /** 
     *  \brief Get the name of the Key_FFAT
     *  \return string representing the name of the Key_FFAT
     */ 
    std::string getName() const override
    {
        return name;
    }

    /** 
     *  \brief Get the total parallelism within the Key_FFAT
     *  \return total parallelism within the Key_FFAT
     */ 
    size_t getParallelism() const override
    {
        return parallelism;
    }

    /** 
     *  \brief Return the routing mode of inputs to the Key_FFAT
     *  \return routing mode (always KEYBY for the Key_FFAT)
     */ 
    routing_modes_t getRoutingMode() const override
    {
        return KEYBY;
    }

    /** 
     *  \brief Check whether the Key_FFAT has been used in a MultiPipe
     *  \return true if the Key_FFAT has been added/chained to an existing MultiPipe
     */ 
    bool isUsed() const override
    {
        return used;
    }

    /** 
     *  \brief Get the window type (CB or TB) utilized by the Key_FFAT
     *  \return adopted windowing semantics (count-based or time-based)
     */ 
    win_type_t getWinType() const
    {
        return winType;
    }

    /** 
     *  \brief Get the number of dropped tuples by the Key_FFAT
     *  \return number of tuples dropped during the processing by the Key_FFAT
     */ 
    size_t getNumDroppedTuples() const
    {
        size_t count = 0;
        auto workers = this->getWorkers();
        for (auto *w: workers) {
            auto *seq = static_cast<win_seqffat_t *>(w);
            count += seq->getNumDroppedTuples();
        }
        return count;
    }

    /** 
     *  \brief Get the Stats_Record of each replica within the Key_FFAT
     *  \return vector of Stats_Record objects
     */ 
    std::vector<Stats_Record> get_StatsRecords() const override
    {
#if !defined(TRACE_WINDFLOW)
        std::cerr << YELLOW << "WindFlow Warning: statistics are not enabled, compile with -DTRACE_WINDFLOW" << DEFAULT_COLOR << std::endl;
        return {};
#else
        std::vector<Stats_Record> records;
        auto workers = this->getWorkers();
        for (auto *w: workers) {
            auto *seq = static_cast<win_seqffat_t *>(w);
            records.push_back(seq->get_StatsRecord());
        }
        return records;
#endif      
    }

    /// deleted constructors/operators
    Key_FFAT(const Key_FFAT &) = delete; // copy constructor
    Key_FFAT(Key_FFAT &&) = delete; // move constructor
    Key_FFAT &operator=(const Key_FFAT &) = delete; // copy assignment operator
    Key_FFAT &operator=(Key_FFAT &&) = delete; // move assignment operator
};

} // namespace wf

#endif
