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
 *  @file    ffat_windows.hpp
 *  @author  Gabriele Mencagli and Elia Ruggeri
 *  
 *  @brief Ffat_Windows operator
 *  
 *  @section Ffat_Windows (Description)
 *  
 *  This file implements the Ffat_Windows operator able to execute associative and
 *  commutative window-based aggregates using the FlatFAT algorithm.
 */ 

#ifndef FFAT_AGGREGATOR_H
#define FFAT_AGGREGATOR_H

/// includes
#include<string>
#include<functional>
#include<context.hpp>
#include<single_t.hpp>
#if defined (WF_TRACING_ENABLED)
    #include<stats_record.hpp>
#endif
#include<ffat_replica.hpp>
#include<basic_emitter.hpp>
#include<basic_operator.hpp>

namespace wf {

/** 
 *  \class Ffat_Windows
 *  
 *  \brief Ffat_Windows executing associative and commutative window-based aggregates
 *         using the FlatFAT algorithm
 *  
 *  This class implements the Ffat_Windows operator able to execute associative and
 *  commutative window-based aggregates using the FlatFAT algorithm.
 */ 
template<typename lift_func_t, typename comb_func_t, typename keyextr_func_t>
class Ffat_Windows: public Basic_Operator
{
private:
    friend class MultiPipe;
    friend class PipeGraph;
    lift_func_t lift_func; // functional logic of the lift
    comb_func_t comb_func; // functional logic of the combine
    keyextr_func_t key_extr; // logic to extract the key attribute from the tuple_t
    std::vector<FFAT_Replica<lift_func_t, comb_func_t, keyextr_func_t>*> replicas; // vector of pointers to the replicas of the Ffat_Windows
    uint64_t win_len; // window length (in no. of tuples or in time units)
    uint64_t slide_len; // slide length (in no. of tuples or in time units)
    uint64_t lateness; // triggering delay in time units (meaningful for TB windows in DEFAULT mode)
    Win_Type_t winType; // window type (CB or TB)

    // Configure the Ffat_Windows to receive batches instead of individual inputs
    void receiveBatches(bool _input_batching) override
    {
        for (auto *r: replicas) {
            r->receiveBatches(_input_batching);
        }
    }

    // Set the emitter used to route outputs from the Ffat_Windows
    void setEmitter(Basic_Emitter *_emitter) override
    {
        replicas[0]->setEmitter(_emitter);
        for (size_t i=1; i<replicas.size(); i++) {
            replicas[i]->setEmitter(_emitter->clone());
        }
    }

    // Check whether the Ffat_Windows has terminated
    bool isTerminated() const override
    {
        bool terminated = true;
        for(auto *r: replicas) { // scan all the replicas to check their termination
            terminated = terminated && r->isTerminated();
        }
        return terminated;
    }

    // Set the execution mode of the Ffat_Windows
    void setExecutionMode(Execution_Mode_t _execution_mode)
    {
        for (auto *r: replicas) {
            r->setExecutionMode(_execution_mode);
        }
    }

    // Get the logic to extract the key attribute from the tuple_t
    keyextr_func_t getKeyExtractor() const
    {
        return key_extr;
    }

#if defined (WF_TRACING_ENABLED)
    // Append the statistics (JSON format) of the Ffat_Windows to a PrettyWriter
    void appendStats(rapidjson::PrettyWriter<rapidjson::StringBuffer> &writer) const override
    {
        // create the header of the JSON file
        writer.StartObject();
        writer.Key("Operator_name");
        writer.String((this->name).c_str());
        writer.Key("Operator_type");
        writer.String("Ffat_Windows");
        writer.Key("Distribution");
        writer.String("KEYBY");
        writer.Key("isTerminated");
        writer.Bool(this->isTerminated());
        writer.Key("isWindowed");
        writer.Bool(true);
        writer.Key("isGPU");
        writer.Bool(false);
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
        for (auto *r: replicas) { // append the statistics from all the replicas of the Ffat_Windows
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
     *  \param _lift_func the lift functional logic of the Ffat_Windows (a function or any callable type)
     *  \param _comb_func the combine functional logic of the Ffat_Windows (a function or any callable type)
     *  \param _key_extr key extractor (a function or any callable type)
     *  \param _parallelism internal parallelism of the Ffat_Windows
     *  \param _name name of the Ffat_Windows
     *  \param _outputBatchSize size (in num of tuples) of the batches produced by this operator (0 for no batching)
     *  \param _closing_func closing functional logic of the Ffat_Windows (a function or any callable type)
     *  \param _win_len window length (in no. of tuples or in time units)
     *  \param _slide_len slide length (in no. of tuples or in time units)
     *  \param _lateness (lateness in time units, meaningful for TB windows in DEFAULT mode)
     *  \param _winType window type (count-based CB or time-based TB)
     */ 
    Ffat_Windows(lift_func_t _lift_func,
                 comb_func_t _comb_func,
                 keyextr_func_t _key_extr,
                 size_t _parallelism,
                 std::string _name,
                 size_t _outputBatchSize,
                 std::function<void(RuntimeContext &)> _closing_func,
                 uint64_t _win_len,
                 uint64_t _slide_len,
                 uint64_t _lateness,
                 Win_Type_t _winType):
                 Basic_Operator(_parallelism, _name, Routing_Mode_t::KEYBY, _outputBatchSize),
                 lift_func(_lift_func),
                 comb_func(_comb_func),
                 key_extr(_key_extr),
                 win_len(_win_len),
                 slide_len(_slide_len),
                 lateness(_lateness),
                 winType(_winType)
    {
        if (win_len == 0 || slide_len == 0) { // check the validity of the windowing parameters
            std::cerr << RED << "WindFlow Error: Ffat_Windows used with window length or slide equal to zero" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        for (size_t i=0; i<this->parallelism; i++) { // create the internal replicas of the Ffat_Windows
            replicas.push_back(new FFAT_Replica<lift_func_t, comb_func_t, keyextr_func_t>(lift_func,
                                                                                                comb_func,
                                                                                                key_extr,
                                                                                                this->name,
                                                                                                RuntimeContext(this->parallelism, i),
                                                                                                _closing_func,
                                                                                                win_len,
                                                                                                slide_len,
                                                                                                lateness,
                                                                                                winType));
        }
    }

    /// Copy constructor
    Ffat_Windows(const Ffat_Windows &_other):
                 Basic_Operator(_other),
                 lift_func(_other.lift_func),
                 comb_func(_other.comb_func),
                 key_extr(_other.key_extr),
                 win_len(_other.win_len),
                 slide_len(_other.slide_len),
                 lateness(_other.lateness),
                 winType(_other.winType)
    {
        for (size_t i=0; i<this->parallelism; i++) { // deep copy of the pointers to the Ffat_Windows replicas
            replicas.push_back(new FFAT_Replica<lift_func_t, comb_func_t, keyextr_func_t>(*(_other.replicas[i])));
        }
    }

    // Destructor
    ~Ffat_Windows() override
    {
        for (auto *r: replicas) { // delete all the replicas
            delete r;
        }
    }

    /** 
     *  \brief Get the type of the Ffat_Windows as a string
     *  \return type of the Ffat_Windows
     */ 
    std::string getType() const override
    {
        return std::string("Ffat_Windows");
    }

    /** 
     *  \brief Get the window type (CB or TB) utilized by the Ffat_Windows
     *  \return adopted windowing semantics (count-based or time-based)
     */ 
    Win_Type_t getWinType() const
    {
        return winType;
    }

    /** 
     *  \brief Get the number of ignored tuples by the Ffat_Windows
     *  \return number of tuples ignored during the processing by the Ffat_Windows
     */ 
    size_t getNumIgnoredTuples() const
    {
        size_t count = 0;
        for (auto *r: replicas) {
            count += r->getNumIgnoredTuples();
        }
        return count;
    }

    Ffat_Windows(Ffat_Windows &&) = delete; ///< Move constructor is deleted
    Ffat_Windows &operator=(const Ffat_Windows &) = delete; ///< Copy assignment operator is deleted
    Ffat_Windows &operator=(Ffat_Windows &&) = delete; ///< Move assignment operator is deleted
};

} // namespace wf

#endif
