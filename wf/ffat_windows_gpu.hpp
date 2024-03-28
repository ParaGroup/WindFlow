/**************************************************************************************
 *  Copyright (c) 2019- Gabriele Mencagli and Elia Ruggeri
 *  
 *  This file is part of WindFlow.
 *  
 *  WindFlow is free software dual licensed under the GNU LGPL or MIT License.
 *  You can redistribute it and/or modify it under the terms of the
 *    * GNU Lesser General Public License as published by
 *      the Free Software Foundation, either version 3 of the License, or
 *      (at your option) any later version
 *    OR
 *    * MIT License: https://github.com/ParaGroup/WindFlow/blob/master/LICENSE.MIT
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
 *  @file    ffat_windows_gpu.hpp
 *  @author  Gabriele Mencagli and Elia Ruggeri
 *  
 *  @brief Ffat_Windows_GPU operator "Springald"
 *  
 *  @section Ffat_Windows_GPU "Springald" (Description)
 *  
 *  This file implements the Ffat_Windows_GPU operator able to execute associative and
 *  commutative window-based aggregates on GPU using a variant of the FlatFAT algorithm.
 *  This operator is also called (informally) "Springald".
 */ 

#ifndef FFAT_WINDOWS_GPU_H
#define FFAT_WINDOWS_GPU_H

/// includes
#include<string>
#if defined (WF_TRACING_ENABLED)
    #include<stats_record.hpp>
#endif
#include<basic_emitter.hpp>
#include<basic_operator.hpp>
#include<ffat_replica_gpu.hpp>

namespace wf {

/** 
 *  \class Ffat_Windows_GPU
 *  
 *  \brief Ffat_Windows_GPU executing associative and commutative window-based aggregates on GPU
 *  
 *  This class implements the Ffat_Windows_GPU operator able to execute associative and
 *  commutative window-based aggregates on GPU using a variant of the FlatFAT algorithm.
 */ 
template<typename lift_func_gpu_t, typename comb_func_gpu_t, typename keyextr_func_gpu_t>
class Ffat_Windows_GPU: public Basic_Operator
{
private:
    friend class MultiPipe;
    friend class PipeGraph;
    lift_func_gpu_t lift_func; // functional logic of the lift
    comb_func_gpu_t comb_func; // functional logic of the combine
    keyextr_func_gpu_t key_extr; // logic to extract the key attribute from the tuple_t
    using tuple_t = decltype(get_tuple_t_LiftGPU(lift_func)); // extracting the tuple_t type and checking the admissible signatures
    using result_t = decltype(get_result_t_LiftGPU(lift_func)); // extracting the result_t type and checking the admissible signatures
    std::vector<Ffat_Replica_GPU<lift_func_gpu_t, comb_func_gpu_t, keyextr_func_gpu_t>*> replicas; // vector of pointers to the replicas of the Ffat_Windows_GPU
    uint64_t win_len; // window length (in no. of tuples or in time units)
    uint64_t slide_len; // slide length (in no. of tuples or in time units)
    uint64_t lateness; // triggering delay in time units (meaningful for TB windows in DEFAULT mode)
    Win_Type_t winType; // window type (CB or TB)
    static constexpr op_type_t op_type = op_type_t::WIN_GPU;

    // This method exists but its does not have any effect
    void receiveBatches(bool _input_batching) override {}

    // Set the emitter used to route outputs from the Ffat_Windows_GPU
    void setEmitter(Basic_Emitter *_emitter) override
    {
        replicas[0]->setEmitter(_emitter);
        for (size_t i=1; i<replicas.size(); i++) {
            replicas[i]->setEmitter(_emitter->clone());
        }
    }

    // Check whether the Ffat_Windows_GPU has terminated
    bool isTerminated() const override
    {
        bool terminated = true;
        for(auto *r: replicas) { // scan all the replicas to check their termination
            terminated = terminated && r->isTerminated();
        }
        return terminated;
    }

    // Set the execution mode of the Ffat_Windows_GPU (i.e., the one of its PipeGraph)
    void setExecutionMode(Execution_Mode_t _execution_mode)
    {
        if (_execution_mode != Execution_Mode_t::DEFAULT) {
            std::cerr << RED << "WindFlow Error: Ffat_Windows_GPU can be used in DEFAULT mode only" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        for (auto *r: replicas) {
            r->setExecutionMode(_execution_mode);
        }
    }

    // Get the logic to extract the key attribute from the tuple_t
    keyextr_func_gpu_t getKeyExtractor() const
    {
        return key_extr;
    }

#if defined (WF_TRACING_ENABLED)
    // Append the statistics (JSON format) of the Ffat to a PrettyWriter
    void appendStats(rapidjson::PrettyWriter<rapidjson::StringBuffer> &writer) const override
    {
        // create the header of the JSON file
        writer.StartObject();
        writer.Key("Operator_name");
        writer.String((this->name).c_str());
        writer.Key("Operator_type");
        writer.String("Ffat_Windows_GPU");
        writer.Key("Distribution");
        writer.String("FORWARD");
        writer.Key("isTerminated");
        writer.Bool(this->isTerminated());
        writer.Key("isWindowed");
        writer.Bool(true);
        writer.Key("isGPU");
        writer.Bool(true);
        writer.Key("Window_type");
        if (winType == Win_Type_t::CB) {
            writer.String("count-based");
        }
        else {
            writer.String("time-based");
            writer.Key("Lateness");
            writer.Uint(lateness);  
        }
        writer.Key("Window_length");
        writer.Uint(win_len);
        writer.Key("Window_slide");
        writer.Uint(slide_len);
        writer.Key("Parallelism");
        writer.Uint(this->parallelism);
        writer.Key("OutputBatchSize");
        writer.Uint(this->outputBatchSize);
        writer.Key("Replicas");
        writer.StartArray();
        for (auto *r: replicas) { // append the statistics from all the replicas of the Ffat
            Stats_Record record = r->getStatsRecord();
            record.appendStats(writer);
        }
        writer.EndArray();
        writer.EndObject();
    }
#endif

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _lift_func the lift functional logic of the Ffat (a __host__ __device__ callable type)
     *  \param _comb_func the combine functional logic of the Ffat (a __host__ __device__ callable type)
     *  \param _key_extr key extractor (a __host__ __device__ callable type)
     *  \param _parallelism internal parallelism of the Ffat_Windows_GPU
     *  \param _name name of the Ffat_Windows_GPU
     *  \param _numWinPerBatch number of consecutive windows results produced per batch
     *  \param _win_len window length (in no. of tuples or in time units)
     *  \param _slide_len slide length (in no. of tuples or in time units)
     *  \param _lateness (lateness in time units, meaningful for TB windows in DEFAULT mode)
     *  \param _winType window type (count-based CB or time-based TB)
     */ 
    Ffat_Windows_GPU(lift_func_gpu_t _lift_func,
                     comb_func_gpu_t _comb_func,
                     keyextr_func_gpu_t _key_extr,
                     size_t _parallelism,
                     std::string _name,
                     size_t _numWinPerBatch,
                     uint64_t _win_len,
                     uint64_t _slide_len,
                     uint64_t _lateness,
                     Win_Type_t _winType):
                     Basic_Operator(_parallelism, _name, Routing_Mode_t::FORWARD, _numWinPerBatch, true /* operator targeting GPU */),
                     lift_func(_lift_func),
                     comb_func(_comb_func),
                     key_extr(_key_extr),
                     win_len(_win_len),
                     slide_len(_slide_len),
                     lateness(_lateness),
                     winType(_winType)
    {
        assert(_parallelism == 1); // sanity check
        if (win_len == 0 || slide_len == 0) { // check the validity of the windowing parameters
            std::cerr << RED << "WindFlow Error: Ffat_Windows_GPU used with window length or slide equal to zero" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        for (size_t i=0; i<this->parallelism; i++) { // create the internal replicas of the Ffat_Windows_GPU
            replicas.push_back(new Ffat_Replica_GPU<lift_func_gpu_t, comb_func_gpu_t, keyextr_func_gpu_t>(lift_func,
                                                                                                          comb_func,
                                                                                                          key_extr,
                                                                                                          i,
                                                                                                          this->name,
                                                                                                          win_len,
                                                                                                          slide_len,
                                                                                                          lateness,
                                                                                                          winType,
                                                                                                          this->outputBatchSize));
        }
    }

    /// Copy constructor
    Ffat_Windows_GPU(const Ffat_Windows_GPU &_other):
                     Basic_Operator(_other),
                     lift_func(_other.lift_func),
                     comb_func(_other.comb_func),
                     key_extr(_other.key_extr),
                     win_len(_other.win_len),
                     slide_len(_other.slide_len),
                     lateness(_other.lateness),
                     winType(_other.winType)
    {
        for (size_t i=0; i<this->parallelism; i++) { // deep copy of the pointers to the Ffat_Windows_GPU replicas
            replicas.push_back(new Ffat_Replica_GPU<lift_func_gpu_t, comb_func_gpu_t, keyextr_func_gpu_t>(*(_other.replicas[i])));
        }
    }

    // Destructor
    ~Ffat_Windows_GPU() override
    {
        for (auto *r: replicas) { // delete all the replicas
            delete r;
        }
    }

    /** 
     *  \brief Get the type of the Ffat_Windows_GPU as a string
     *  \return type of the Ffat_Windows_GPU
     */ 
    std::string getType() const override
    {
        return std::string("Ffat_Windows_GPU");
    }

    /** 
     *  \brief Get the window type (CB or TB) utilized by the Ffat_Windows_GPU
     *  \return adopted windowing semantics (count-based or time-based)
     */ 
    Win_Type_t getWinType() const
    {
        return winType;
    }

    /** 
     *  \brief Get the number of ignored tuples by the Ffat_Windows_GPU
     *  \return number of tuples ignored during the processing by the Ffat_Windows_GPU
     */ 
    size_t getNumIgnoredTuples() const
    {
        size_t count = 0;
        for (auto *r: replicas) {
            count += r->getNumIgnoredTuples();
        }
        return count;
    }

    Ffat_Windows_GPU(Ffat_Windows_GPU &&) = delete; ///< Move constructor is deleted
    Ffat_Windows_GPU &operator=(const Ffat_Windows_GPU &) = delete; ///< Copy assignment operator is deleted
    Ffat_Windows_GPU &operator=(Ffat_Windows_GPU &&) = delete; ///< Move assignment operator is deleted
};

} // namespace wf

#endif
