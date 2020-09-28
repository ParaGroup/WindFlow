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
 *  @brief Key_FFAT_GPU operator executing a windowed query in parallel on GPU
 *         using the FlatFAT algorithm for GPU
 *  
 *  @section Key_FFAT_GPU (Description)
 *  
 *  This file implements the Key_FFAT_GPU operator able to executes windowed queries
 *  on a GPU device. The operator prepares batches of input tuples in parallel on the
 *  CPU cores and offloads on the GPU the parallel processing of the windows within the
 *  same batch. Batches of different sub-streams can be executed in parallel while
 *  consecutive batches of the same sub-stream are prepared on the CPU and offloaded on
 *  the GPU sequentially. However, windows are efficiently processed using a variant for
 *  GPU of the FlatFAT algorithm.
 *  
 *  The template parameters tuple_t and result_t must be default constructible, with a copy
 *  constructor and a copy assignment operator, and they must provide and implement the
 *  setControlFields() and getControlFields() methods. Furthermore, in order to be copyable
 *  in a GPU-accessible memory, they must be compliant with the C++ specification for standard
 *  layout types. The third template argument comb_F_t is the type of the callable object to be
 *  used for GPU processing.
 */ 

#ifndef KEY_FFFAT_GPU_H
#define KEY_FFFAT_GPU_H

/// includes
#include<ff/pipeline.hpp>
#include<ff/all2all.hpp>
#include<ff/farm.hpp>
#include<ff/optimize.hpp>
#include<basic.hpp>
#include<win_seqffat_gpu.hpp>
#include<kf_nodes.hpp>
#include<basic_operator.hpp>

namespace wf {

/** 
 *  \class Key_FFAT_GPU
 *  
 *  \brief Key_FFAT_GPU operator executing a windowed query in parallel on GPU
 *  
 *  This class implements the Key_FFAT_GPU operator. The operator prepares in parallel distinct
 *  batches of tuples (on the CPU cores) and offloads the processing of the batches on the GPU
 *  by computing in parallel all the windows within a batch on the CUDA cores of the GPU. Batches
 *  with tuples of same sub-stream are prepared/offloaded sequentially on the CPU. However,
 *  windows of the same substream are executed efficiently by using a variant for GPU of the FlatFAT
 *  algorithm.
 */ 
template<typename tuple_t, typename result_t, typename comb_F_t>
class Key_FFAT_GPU: public ff::ff_farm, public Basic_Operator
{
public:
    /// type of the lift function
    using winLift_func_t = std::function<void(const tuple_t &, result_t &)>;
    /// function type to map the key hashcode onto an identifier starting from zero to parallelism-1
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
    std::string name; // name of the Key_FFAT_GPU
    size_t parallelism; // internal parallelism of the Key_FFAT_GPU
    bool used; // true if the Key_FFAT_GPU has been added/chained in a MultiPipe
    uint64_t win_len; // window length (no. of tuples or in time units)
    uint64_t slide_len; // slide length (no. of tuples or in time units)
    uint64_t triggering_delay; // triggering delay in time units (meaningful for TB windows only)
    size_t batch_len; // length of the batch in terms of no. of windows
    win_type_t winType; // type of windows (count-based or time-based)

    // method to set the isRenumbering mode of the internal nodes
    void set_isRenumbering()
    {
        assert(winType == win_type_t::CB); // only count-based windows
        for (auto *node: this->getWorkers()) {
            win_seqffat_gpu_t *seq = static_cast<win_seqffat_gpu_t *>(node);
            seq->isRenumbering = true;
        }
    }

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
     *  \param _parallelism internal parallelism of the Key_FFAT_GPU operator
     *  \param _batch_len no. of windows in a batch
     *  \param _gpu_id identifier of the chosen GPU device
     *  \param _n_thread_block number of threads per block
     *  \param _rebuild flag stating whether the FlatFAT_GPU must be rebuilt from scratch for each new batch
     *  \param _name string with the unique name of the operator
     *  \param _routing_func function to map the key hashcode onto an identifier starting from zero to parallelism-1
     */ 
    Key_FFAT_GPU(winLift_func_t _winLift_func,
                 comb_F_t _winComb_func,
                 uint64_t _win_len,
                 uint64_t _slide_len,
                 uint64_t _triggering_delay,
                 win_type_t _winType,
                 size_t _parallelism,
                 size_t _batch_len,
                 int _gpu_id,
                 size_t _n_thread_block,
                 bool _rebuild,
                 std::string _name,
                 routing_func_t _routing_func):
                 name(_name),
                 parallelism(_parallelism),
                 used(false),
                 win_len(_win_len),
                 slide_len(_slide_len),
                 triggering_delay(_triggering_delay),
                 batch_len(_batch_len),
                 winType(_winType)
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
        // check the validity of the parallelism value
        if (_parallelism == 0) {
            std::cerr << RED << "WindFlow Error: Key_FFAT_GPU has parallelism zero" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // check the validity of the batch length
        if (_batch_len == 0) {
            std::cerr << RED << "WindFlow Error: batch length in Key_FFAT_GPU cannot be zero" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // std::vector of Win_SeqFFAT_GPU
        std::vector<ff::ff_node *> w(_parallelism);
        // create the Win_SeqFFAT_GPU
        for (size_t i = 0; i < _parallelism; i++) {
            auto *ffat_gpu = new win_seqffat_gpu_t(_winLift_func, _winComb_func, _win_len, _slide_len, _triggering_delay, _winType, _batch_len, _gpu_id, _n_thread_block, _rebuild, _name);
            w[i] = ffat_gpu;
        }
        ff::ff_farm::add_workers(w);
        ff::ff_farm::add_collector(nullptr);
        // create the Emitter node
        ff::ff_farm::add_emitter(new kf_emitter_t(_routing_func, _parallelism));
        // when the Key_Farm_GPU will be destroyed we need aslo to destroy the emitter, workers and collector
        ff::ff_farm::cleanup_all();
    }

    /** 
     *  \brief Get the window type (CB or TB) utilized by the Key_FFAT_GPU
     *  \return adopted windowing semantics (count-based or time-based)
     */ 
    win_type_t getWinType() const
    {
        return winType;
    }

    /** 
     *  \brief Get the number of ignored tuples by the Key_FFAT_GPU
     *  \return number of tuples ignored during the processing by the Key_FFAT_GPU
     */ 
    size_t getNumIgnoredTuples() const
    {
        size_t count = 0;
        auto workers = this->getWorkers();
        for (auto *w: workers) {
            auto *seq = static_cast<win_seqffat_gpu_t *>(w);
            count += seq->getNumIgnoredTuples();
        }
        return count;
    }

    /** 
     *  \brief Get the name of the Key_FFAT_GPU
     *  \return string representing the name of the Key_FFAT_GPU
     */ 
    std::string getName() const override
    {
        return name;
    }

    /** 
     *  \brief Get the total parallelism within the Key_FFAT_GPU
     *  \return total parallelism within the Key_FFAT_GPU
     */ 
    size_t getParallelism() const override
    {
        return parallelism;
    }

    /** 
     *  \brief Return the routing mode of inputs to the Key_FFAT_GPU
     *  \return routing mode (always KEYBY for the Key_FFAT_GPU)
     */ 
    routing_modes_t getRoutingMode() const override
    {
        return routing_modes_t::KEYBY;
    }

    /** 
     *  \brief Check whether the Key_FFAT_GPU has been used in a MultiPipe
     *  \return true if the Key_FFAT_GPU has been added/chained to an existing MultiPipe
     */ 
    bool isUsed() const override
    {
        return used;
    }

    /** 
     *  \brief Check whether the operator has been terminated
     *  \return true if the operator has finished its work
     */ 
    virtual bool isTerminated() const override
    {
        bool terminated = true;
        // scan all the replicas to check their termination
        for(auto *w: this->getWorkers()) {
            auto *seq = static_cast<win_seqffat_gpu_t *>(w);
            terminated = terminated && seq->isTerminated();
        }
        return terminated;
    }

#if defined (TRACE_WINDFLOW)
    /// Dump the log file (JSON format) in the LOG_DIR directory
    void dump_LogFile() const override
    {
        // create and open the log file in the LOG_DIR directory
        std::ofstream logfile;
#if defined (LOG_DIR)
        std::string log_dir = std::string(STRINGIFY(LOG_DIR));
        std::string filename = std::string(STRINGIFY(LOG_DIR)) + "/" + std::to_string(getpid()) + "_" + name + ".json";
#else
        std::string log_dir = std::string("log");
        std::string filename = "log/" + std::to_string(getpid()) + "_" + name + ".json";
#endif
        // create the log directory
        if (mkdir(log_dir.c_str(), 0777) != 0) {
            struct stat st;
            if((stat(log_dir.c_str(), &st) != 0) || !S_ISDIR(st.st_mode)) {
                std::cerr << RED << "WindFlow Error: directory for log files cannot be created" << DEFAULT_COLOR << std::endl;
                exit(EXIT_FAILURE);
            }
        }
        logfile.open(filename);
        // create the rapidjson writer
        rapidjson::StringBuffer buffer;
        rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(buffer);
        // append the statistics of this operator
        this->append_Stats(writer);
        // serialize the object to file
        logfile << buffer.GetString();
        logfile.close();
    }

    /// append the statistics (JSON format) of this operator
    void append_Stats(rapidjson::PrettyWriter<rapidjson::StringBuffer> &writer) const override
    {
        // create the header of the JSON file
        writer.StartObject();
        writer.Key("Operator_name");
        writer.String(name.c_str());
        writer.Key("Operator_type");
        writer.String("Key_FFAT_GPU");
        writer.Key("Distribution");
        writer.String("KEYBY");
        writer.Key("isTerminated");
        writer.Bool(this->isTerminated());
        writer.Key("isWindowed");
        writer.Bool(true);
        writer.Key("isGPU");
        writer.Bool(true);
        writer.Key("Window_type");
        if (winType == win_type_t::CB) {
            writer.String("count-based");
        }
        else {
            writer.String("time-based");
            writer.Key("Window_delay");
            writer.Uint(triggering_delay);  
        }
        writer.Key("Window_length");
        writer.Uint(win_len);
        writer.Key("Window_slide");
        writer.Uint(slide_len);
        writer.Key("Batch_len");
        writer.Uint(batch_len);
        writer.Key("Parallelism");
        writer.Uint(parallelism);
        writer.Key("Replicas");
        writer.StartArray();
        // get statistics from all the replicas of the operator
        for(auto *w: this->getWorkers()) {
            auto *seq = static_cast<win_seqffat_gpu_t *>(w);
            Stats_Record record = seq->get_StatsRecord();
            record.append_Stats(writer);
        }
        writer.EndArray();
        writer.EndObject();
    }
#endif

    /// deleted constructors/operators
    Key_FFAT_GPU(const Key_FFAT_GPU &) = delete; // copy constructor
    Key_FFAT_GPU(Key_FFAT_GPU &&) = delete; // move constructor
    Key_FFAT_GPU &operator=(const Key_FFAT_GPU &) = delete; // copy assignment operator
    Key_FFAT_GPU &operator=(Key_FFAT_GPU &&) = delete; // move assignment operator
};

} // namespace wf

#endif
