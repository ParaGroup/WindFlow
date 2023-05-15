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
 *  @file    paned_windows.hpp
 *  @author  Gabriele Mencagli
 *  
 *  @brief Paned_Windows operator
 *  
 *  @section Paned_Windows (Description)
 *  
 *  This file implements the Paned_Windows operator able to execute incremental
 *  or non-incremental queries on count- or time-based windows. It allows
 *  windows to be split into disjoint panes, whose results are shared between
 *  consectuve windows.
 */ 

#ifndef PANED_WIN_H
#define PANED_WIN_H

/// includes
#include<string>
#include<functional>
#include<context.hpp>
#include<single_t.hpp>
#if defined (WF_TRACING_ENABLED)
    #include<stats_record.hpp>
#endif
#include<basic_emitter.hpp>
#include<basic_operator.hpp>
#include<window_replica.hpp>
#include<parallel_windows.hpp>

namespace wf {

/** 
 *  \class Paned_Windows
 *  
 *  \brief Paned_Windows operator
 *  
 *  This class implements the Paned_Windows operator executing incremental or
 *  non-incremental queries on streaming windows. The operator allows windows
 *  to be split into disjoint panes, whose processing result is shared between
 *  consecutive windows.
 */ 
template<typename plq_func_t, typename wlq_func_t, typename keyextr_func_t>
class Paned_Windows: public Basic_Operator
{
private:
    friend class MultiPipe;
    friend class PipeGraph;
    plq_func_t plq_func; // functional logic of the PLQ stage
    wlq_func_t wlq_func; // functional logic of the WLQ stage
    keyextr_func_t key_extr; // logic to extract the key attribute from the tuple_t
    size_t plq_parallelism; // parallelism of the PLQ stage
    size_t wlq_parallelism; // parallelism of the WLQ stage
    uint64_t win_len; // window length (in no. of tuples or in time units)
    uint64_t slide_len; // slide length (in no. of tuples or in time units)
    uint64_t lateness; // triggering delay in time units (meaningful for TB windows in DEFAULT mode)
    Win_Type_t winType; // window type (CB or TB)
    Parallel_Windows<plq_func_t, keyextr_func_t> plq; // PLQ sub-operator
    Parallel_Windows<wlq_func_t, keyextr_func_t> wlq; // WLQ sub-operator

    // Configure the Paned_Windows to receive batches instead of individual inputs
    void receiveBatches(bool _input_batching) override
    {
        for (auto *r: plq.replicas) {
            r->receiveBatches(_input_batching);
        }
    }

    // Set the emitter used to route outputs from the Paned_Windows
    void setEmitter(Basic_Emitter *_emitter) override
    {
        wlq.replicas[0]->setEmitter(_emitter);
        for (size_t i=1; i<wlq.replicas.size(); i++) {
            wlq.replicas[i]->setEmitter(_emitter->clone());
        }
    }

    // Check whether the Paned_Windows has terminated
    bool isTerminated() const override
    {
        bool terminated = true;
        for(auto *r: plq.replicas) { // scan all the PLQ replicas to check their termination
            terminated = terminated && r->isTerminated();
        }
        for(auto *r: wlq.replicas) { // scan all the WLQ replicas to check their termination
            terminated = terminated && r->isTerminated();
        }
        return terminated;
    }

    // Set the execution mode of the Paned_Windows
    void setExecutionMode(Execution_Mode_t _execution_mode)
    {
        plq.setExecutionMode(_execution_mode);
        wlq.setExecutionMode(_execution_mode);
    }

    // Get the logic to extract the key attribute from the tuple_t
    keyextr_func_t getKeyExtractor() const
    {
        return key_extr;
    }

#if defined (WF_TRACING_ENABLED)
    // Append the statistics (JSON format) of the Paned_Windows to a PrettyWriter
    void appendStats(rapidjson::PrettyWriter<rapidjson::StringBuffer> &writer) const override
    {
        writer.StartObject(); // create the header of the JSON file
        writer.Key("Operator_name");
        writer.String((this->name).c_str());
        writer.Key("Operator_type");
        writer.String("Paned_Windows");
        writer.Key("Distribution");
        writer.String("BROADCAST");
        writer.Key("isTerminated");
        writer.Bool(this->isTerminated());
        writer.Key("isGPU_1");
        writer.Bool(false);
        writer.Key("Name_Stage_1");
        writer.String("PLQ");
        writer.Key("Window_type_1");
        if (winType == Win_Type_t::CB) {
            writer.String("count-based");
        }
        else {
            writer.String("time-based");
            writer.Key("lateness");
            writer.Uint(lateness);
        }
        writer.Key("Window_length_1");
        writer.Uint(compute_gcd(win_len, slide_len));
        writer.Key("Window_slide_1");
        writer.Uint(compute_gcd(win_len, slide_len));
        writer.Key("Parallelism_1");
        writer.Uint(plq_parallelism);
        writer.Key("Replicas_1");
        writer.StartArray();
        for (auto *r: plq.replicas) { // append the statistics from all the PLQ replicas of the Paned_Windows
            Stats_Record record = r->getStatsRecord();
            record.appendStats(writer);
        }
        writer.EndArray();
        writer.Key("isGPU_2");
        writer.Bool(false);
        writer.Key("Name_Stage_2");
        writer.String("WLQ");
        writer.Key("Window_type_2");
        writer.String("count-based");
        writer.Key("Window_length_2");
        writer.Uint(win_len/compute_gcd(win_len, slide_len));
        writer.Key("Window_slide_2");
        writer.Uint(slide_len/compute_gcd(win_len, slide_len));
        writer.Key("Parallelism_2");
        writer.Uint(wlq_parallelism);
        writer.Key("OutputBatchSize");
        writer.Uint(this->outputBatchSize);
        writer.Key("Replicas_2");
        writer.StartArray();
        for (auto *r: wlq.replicas) { // append the statistics from all the WLQ replicas of the Paned_Windows
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
     *  \param _plq_func functional logic of the PLQ stage (a function or any callable type)
     *  \param _wlq_func functional logic of the WLQ stage (a function or any callable type)
     *  \param _key_extr key extractor (a function or any callable type)
     *  \param _plq_parallelism internal parallelism of the PLQ stage
     *  \param _wlq_parallelism internal parallelism of the WLQ stage
     *  \param _name name of the Paned_Windows
     *  \param _outputBatchSize size (in num of tuples) of the batches produced by this operator (0 for no batching)
     *  \param _closing_func closing functional logic of the Paned_Windows (a function or any callable type)
     *  \param _win_len window length (in no. of tuples or in time units)
     *  \param _slide_len slide length (in no. of tuples or in time units)
     *  \param _lateness (lateness in time units, meaningful for TB windows in DEFAULT mode)
     *  \param _winType window type (count-based CB or time-based TB)
     */ 
    Paned_Windows(plq_func_t _plq_func,
                  wlq_func_t _wlq_func,
                  keyextr_func_t _key_extr,
                  size_t _plq_parallelism,
                  size_t _wlq_parallelism,
                  std::string _name,
                  size_t _outputBatchSize,
                  std::function<void(RuntimeContext &)> _closing_func,
                  uint64_t _win_len,
                  uint64_t _slide_len,
                  uint64_t _lateness,
                  Win_Type_t _winType):
                  Basic_Operator(_plq_parallelism+_wlq_parallelism, _name, Routing_Mode_t::BROADCAST, _outputBatchSize),
                  plq_func(_plq_func),
                  wlq_func(_wlq_func),
                  key_extr(_key_extr),
                  plq_parallelism(_plq_parallelism),
                  wlq_parallelism(_wlq_parallelism),
                  win_len(_win_len),
                  slide_len(_slide_len),
                  lateness(_lateness),
                  winType(_winType),
                  plq(_plq_func, _key_extr, _plq_parallelism, _name + "_plq", 0, _closing_func, compute_gcd(_win_len, _slide_len), compute_gcd(_win_len, _slide_len), _lateness, _winType, role_t::PLQ),
                  wlq(_wlq_func, _key_extr, _wlq_parallelism, _name + "_wlq", _outputBatchSize, _closing_func, _win_len/compute_gcd(_win_len, _slide_len), _slide_len/compute_gcd(_win_len, _slide_len), 0, Win_Type_t::CB, role_t::WLQ)
    {
        if (plq_parallelism == 0) { // check the validity of the PLQ parallelism value
            std::cerr << RED << "WindFlow Error: Paned_Windows has PLQ parallelism zero" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        if (wlq_parallelism == 0) { // check the validity of the WLQ parallelism value
            std::cerr << RED << "WindFlow Error: Paned_Windows has WLQ parallelism zero" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        if (win_len == 0 || slide_len == 0) { // check the validity of the windowing parameters
            std::cerr << RED << "WindFlow Error: Paned_Windows used with window length or slide equal to zero" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        if (win_len <= slide_len) { // Paned_Windows can be utilized with sliding windows only
            std::cerr << RED << "WindFlow Error: Paned_Windows requires sliding windows only (s<w)" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // set the types of the internal operators
        plq.type = "Parallel_Windows_PLQ";
        wlq.type = "Parallel_Windows_WLQ";
    }

    /// Copy constructor
    Paned_Windows(const Paned_Windows &_other):
                  Basic_Operator(_other),
                  plq_func(_other.plq_func),
                  wlq_func(_other.wlq_func),
                  key_extr(_other.key_extr),
                  plq_parallelism(_other.plq_parallelism),
                  wlq_parallelism(_other.wlq_parallelism),
                  win_len(_other.win_len),
                  slide_len(_other.slide_len),
                  lateness(_other.lateness),
                  winType(_other.winType),
                  plq(_other.plq),
                  wlq(_other.wlq) {}

    /** 
     *  \brief Get the type of the Paned_Windows as a string
     *  \return type of the Paned_Windows
     */ 
    std::string getType() const override
    {
        return std::string("Paned_Windows");
    }

    /** 
     *  \brief Get the window type (CB or TB) utilized by the Paned_Windows
     *  \return adopted windowing semantics (count-based or time-based)
     */ 
    Win_Type_t getWinType() const
    {
        return winType;
    }

    /** 
     *  \brief Get the number of ignored tuples by the Paned_Windows
     *  \return number of tuples ignored during the processing by the Paned_Windows
     */ 
    size_t getNumIgnoredTuples() const
    {
        return plq->getNumIgnoredTuples();
    }

    Paned_Windows(Paned_Windows &&) = delete; ///< Move constructor is deleted
    Paned_Windows &operator=(const Paned_Windows &) = delete; ///< Copy assignment operator is deleted
    Paned_Windows &operator=(Paned_Windows &&) = delete; ///< Move assignment operator is deleted
};

} // namespace wf

#endif
