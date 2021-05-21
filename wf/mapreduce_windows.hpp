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
 *  @file    mapreduce_windows.hpp
 *  @author  Gabriele Mencagli
 *  
 *  @brief MapReduce_Windows operator
 *  
 *  @section MapReduce_Windows (Description)
 *  
 *  This file implements the MapReduce_Windows operator able to execute incremental
 *  or non-incremental processing logic on count- or time-based windows. Each window
 *  is split into disjoint partitions. Results of the partitions are used to compute
 *  window-wise results.
 */ 

#ifndef MAPREDUCE_WIN_H
#define MAPREDUCE_WIN_H

/// includes
#include<string>
#include<functional>
#include<context.hpp>
#include<single_t.hpp>
#if defined (TRACE_WINDFLOW)
    #include<stats_record.hpp>
#endif
#include<basic_emitter.hpp>
#include<basic_operator.hpp>
#include<window_replica.hpp>
#include<parallel_windows.hpp>

namespace wf {

/** 
 *  \class MapReduce_Windows
 *  
 *  \brief MapReduce_Windows operator
 *  
 *  This class implements the MapReduce_Windows operator executing incremental or
 *  non-incremental processing logic on streaming windows. Each window is split into
 *  disjoint partitions. Results of the partitions are used to compute results of
 *  whole windows.
 */ 
template<typename map_func_t, typename reduce_func_t, typename key_extractor_func_t>
class MapReduce_Windows: public Basic_Operator
{
private:
    friend class MultiPipe; // friendship with the MultiPipe class
    friend class PipeGraph; // friendship with the PipeGraph class
    map_func_t map_func; // functional logic of the MAP stage
    reduce_func_t reduce_func; // functional logic of the REDUCE stage
    key_extractor_func_t key_extr; // logic to extract the key attribute from the tuple_t
    size_t map_parallelism; // parallelism of the MAP stage
    size_t reduce_parallelism; // parallelism of the REDUCE stage
    std::string name; // name of the MapReduce_Windows
    bool input_batching; // if true, the MapReduce_Windows expects to receive batches instead of individual inputs
    size_t outputBatchSize; // batch size of the outputs produced by the MapReduce_Windows
    uint64_t win_len; // window length (in no. of tuples or in time units)
    uint64_t slide_len; // slide length (in no. of tuples or in time units)
    uint64_t lateness; // triggering delay in time units (meaningful for TB windows in DEFAULT mode)
    Win_Type_t winType; // window type (CB or TB)
    Parallel_Windows<map_func_t, key_extractor_func_t> map; // MAP sub-operator
    Parallel_Windows<reduce_func_t, key_extractor_func_t> reduce; // REDUCE sub-operator

    // Configure the MapReduce_Windows to receive batches instead of individual inputs
    void receiveBatches(bool _input_batching) override
    {
        for (auto *r: map.replicas) {
            r->receiveBatches(_input_batching);
        }
    }

    // Set the emitter used to route outputs from the MapReduce_Windows
    void setEmitter(Basic_Emitter *_emitter) override
    {
        reduce.replicas[0]->setEmitter(_emitter);
        for (size_t i=1; i<reduce.replicas.size(); i++) {
            reduce.replicas[i]->setEmitter(_emitter->clone());
        }
    }

    // Check whether the MapReduce_Windows has terminated
    bool isTerminated() const override
    {
        bool terminated = true;
        for(auto *r: map.replicas) { // scan all the MAP replicas to check their termination
            terminated = terminated && r->isTerminated();
        }
        for(auto *r: reduce.replicas) { // scan all the REDUCE replicas to check their termination
            terminated = terminated && r->isTerminated();
        }
        return terminated;
    }

    // Set the execution mode of the MapReduce_Windows (i.e., the one of its PipeGraph)
    void setExecutionMode(Execution_Mode_t _execution_mode)
    {
        map.setExecutionMode(_execution_mode);
        reduce.setExecutionMode(_execution_mode);
    }

    // Get the logic to extract the key attribute from the tuple_t
    key_extractor_func_t getKeyExtractor() const
    {
        return key_extr;
    }

#if defined (TRACE_WINDFLOW)
    // Dump the log file (JSON format) of statistics of the MapReduce_Windows
    void dumpStats() const override
    {
        std::ofstream logfile; // create and open the log file in the LOG_DIR directory
#if defined (LOG_DIR)
        std::string log_dir = std::string(STRINGIFY(LOG_DIR));
        std::string filename = std::string(STRINGIFY(LOG_DIR)) + "/" + std::to_string(getpid()) + "_" + name + ".json";
#else
        std::string log_dir = std::string("log");
        std::string filename = "log/" + std::to_string(getpid()) + "_" + name + ".json";
#endif
        if (mkdir(log_dir.c_str(), 0777) != 0) { // create the log directory
            struct stat st;
            if((stat(log_dir.c_str(), &st) != 0) || !S_ISDIR(st.st_mode)) {
                std::cerr << RED << "WindFlow Error: directory for log files cannot be created" << DEFAULT_COLOR << std::endl;
                exit(EXIT_FAILURE);
            }
        }
        logfile.open(filename);
        rapidjson::StringBuffer buffer; // create the rapidjson writer
        rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(buffer);
        this->appendStats(writer); // append the statistics of the Map
        logfile << buffer.GetString();
        logfile.close();
    }

    // Append the statistics (JSON format) of the MapReduce_Windows to a PrettyWriter
    void appendStats(rapidjson::PrettyWriter<rapidjson::StringBuffer> &writer) const override
    {
        writer.StartObject(); // create the header of the JSON file
        writer.Key("Operator_name");
        writer.String(name.c_str());
        writer.Key("Operator_type");
        writer.String("MapReduce_Windows");
        writer.Key("Distribution");
        writer.String("BROADCAST");
        writer.Key("isTerminated");
        writer.Bool(this->isTerminated());
        writer.Key("isGPU_1");
        writer.Bool(false);
        writer.Key("Name_Stage_1");
        writer.String("MAP");
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
        writer.Uint(win_len);
        writer.Key("Window_slide_1");
        writer.Uint(slide_len);
        writer.Key("Parallelism_1");
        writer.Uint(map_parallelism);
        writer.Key("Replicas_1");
        writer.StartArray();
        for (auto *r: map.replicas) { // append the statistics from all the MAP replicas of the MapReduce_Windows
            Stats_Record record = r->getStatsRecord();
            record.appendStats(writer);
        }
        writer.EndArray();
        writer.Key("isGPU_2");
        writer.Bool(false);
        writer.Key("Name_Stage_2");
        writer.String("REDUCE");
        writer.Key("Window_type_2");
        writer.String("count-based");
        writer.Key("Window_length_2");
        writer.Uint(map_parallelism);
        writer.Key("Window_slide_2");
        writer.Uint(map_parallelism);
        writer.Key("Parallelism_2");
        writer.Uint(reduce_parallelism);
        writer.Key("OutputBatchSize");
        writer.Uint(outputBatchSize);
        writer.Key("Replicas_2");
        writer.StartArray();
        for (auto *r: reduce.replicas) { // append the statistics from all the REDUCE replicas of the MapReduce_Windows
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
     *  \param _map_func functional logic of the MAP stage (a function or a callable type)
     *  \param _reduce_func functional logic of the REDUCE stage (a function or a callable type)
     *  \param _key_extr key extractor (a function or a callable type)
     *  \param _map_parallelism internal parallelism of the MAP stage
     *  \param _reduce_parallelism internal parallelism of the REDUCE stage
     *  \param _name name of the MapReduce_Windows
     *  \param _outputBatchSize size (in num of tuples) of the batches produced by this operator (0 for no batching)
     *  \param _closing_func closing functional logic of the MapReduce_Windows (a function or callable type)
     *  \param _win_len window length (in no. of tuples or in time units)
     *  \param _slide_len slide length (in no. of tuples or in time units)
     *  \param _lateness (lateness in time units, meaningful for TB windows in DEFAULT mode)
     *  \param _winType window type (count-based CB or time-based TB)
     */ 
    MapReduce_Windows(map_func_t _map_func,
                      reduce_func_t _reduce_func,
                      key_extractor_func_t _key_extr,
                      size_t _map_parallelism,
                      size_t _reduce_parallelism,
                      std::string _name,
                      size_t _outputBatchSize,
                      std::function<void(RuntimeContext &)> _closing_func,
                      uint64_t _win_len,
                      uint64_t _slide_len,
                      uint64_t _lateness,
                      Win_Type_t _winType):
                      map_func(_map_func),
                      reduce_func(_reduce_func),
                      key_extr(_key_extr),
                      map_parallelism(_map_parallelism),
                      reduce_parallelism(_reduce_parallelism),
                      name(_name),
                      input_batching(false),
                      outputBatchSize(_outputBatchSize),
                      win_len(_win_len),
                      slide_len(_slide_len),
                      lateness(_lateness),
                      winType(_winType),
                      map(_map_func, _key_extr, _map_parallelism, _name + "_map", 0, _closing_func, win_len, slide_len, _lateness, winType, role_t::MAP),
                      reduce(_reduce_func, _key_extr, _reduce_parallelism, _name + "_reduce", outputBatchSize, _closing_func, _map_parallelism, _map_parallelism, 0, Win_Type_t::CB, role_t::REDUCE)
    {
        if (map_parallelism == 0) { // check the validity of the MAP parallelism value
            std::cerr << RED << "WindFlow Error: MapReduce_Windows has MAP parallelism zero" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        if (reduce_parallelism == 0) { // check the validity of the REDUCE parallelism value
            std::cerr << RED << "WindFlow Error: MapReduce_Windows has REDUCE parallelism zero" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        if (win_len == 0 || slide_len == 0) { // check the validity of the windowing parameters
            std::cerr << RED << "WindFlow Error: MapReduce_Windows used with window length or slide equal to zero" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // set the types of the internal operators
        map.type = "Parallel_Windows_MAP";
        reduce.type = "Parallel_Windows_REDUCE";
    }

    /** 
     *  \brief Get the type of the MapReduce_Windows as a string
     *  \return type of the MapReduce_Windows
     */ 
    std::string getType() const override
    {
        return std::string("MapReduce_Windows");
    }

    /** 
     *  \brief Get the name of the MapReduce_Windows as a string
     *  \return name of the MapReduce_Windows
     */ 
    std::string getName() const override
    {
        return name;
    }

    /** 
     *  \brief Get the total parallelism of the MapReduce_Windows
     *  \return total parallelism of the MapReduce_Windows
     */  
    size_t getParallelism() const override
    {
        return map_parallelism + reduce_parallelism;
    }

    /** 
     *  \brief Return the input routing mode of the MapReduce_Windows
     *  \return routing mode used to send inputs to the MapReduce_Windows
     */ 
    Routing_Mode_t getInputRoutingMode() const override
    {
        return Routing_Mode_t::BROADCAST;
    }

    /** 
     *  \brief Return the size of the output batches that the MapReduce_Windows should produce
     *  \return output batch size in number of tuples
     */ 
    size_t getOutputBatchSize() const override
    {
        return outputBatchSize;
    }

    /** 
     *  \brief Get the window type (CB or TB) utilized by the MapReduce_Windows
     *  \return adopted windowing semantics (count-based or time-based)
     */ 
    Win_Type_t getWinType() const
    {
        return winType;
    }

    /** 
     *  \brief Get the number of ignored tuples by the MapReduce_Windows
     *  \return number of tuples ignored during the processing by the MapReduce_Windows
     */ 
    size_t getNumIgnoredTuples() const
    {
        return map->getNumIgnoredTuples();
    }
};

} // namespace wf

#endif
