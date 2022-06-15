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
 *  @file    ffat_replica_gpu.hpp
 *  @author  Gabriele Mencagli and Elia Ruggeri
 *  
 *  @brief FFAT_Replica_GPU implements the replica of the FFAT_Aggregator_GPU
 *  
 *  @section FFAT_Replica_GPU (Description)
 *  
 *  This file implements the FFAT_Replica_GPU representing the replica of the
 *  FFAT_Aggregator_GPU operator.
 */ 

#ifndef FFAT_REPLICA_GPU_H
#define FFAT_REPLICA_GPU_H

// includes
#include<cmath>
#include<string>
#include<unordered_map>
#include<ff/multinode.hpp>
#if !defined (WF_GPU_UNIFIED_MEMORY) && !defined (WF_GPU_PINNED_MEMORY)
    #include<flatfat_gpu.hpp> // version with CUDA explicit memory transfers
#else
    #include<flatfat_gpu_u.hpp> // version with CUDA unified memory support
#endif
#include<batch_t.hpp>
#include<single_t.hpp>
#if defined (WF_TRACING_ENABLED)
    #include<stats_record.hpp>
#endif
#include<basic_emitter.hpp>
#include<basic_operator.hpp>

namespace wf {

// class FFAT_Replica_GPU
template<typename liftgpu_func_t, typename combgpu_func_t, typename key_extractor_func_t>
class FFAT_Replica_GPU: public ff::ff_monode
{
private:
    template<typename T1, typename T2, typename T3> friend class FFAT_Aggregator_GPU; // friendship with the FFAT_Aggregator_GPU class
    liftgpu_func_t lift_func; // functional logic of the lift
    combgpu_func_t comb_func; // functional logic of the combine
    key_extractor_func_t key_extr; // logic to extract the key attribute from the tuple_t
    using tuple_t = decltype(get_tuple_t_Lift(lift_func)); // extracting the tuple_t type and checking the admissible signatures
    using result_t = decltype(get_result_t_Lift(lift_func)); // extracting the result_t type and checking the admissible signatures
    using key_t = decltype(get_key_t_KeyExtr(key_extr)); // extracting the key_t type and checking the admissible singatures
    using fat_t = FlatFAT_GPU<result_t, key_t, combgpu_func_t>; // type of the FlatFAT_GPU
    struct Key_Descriptor // struct of a key descriptor
    {
        fat_t fatgpu; // FlatFAT_GPU of the key
        std::vector<result_t> pending_tuples; // vector of pending tuples of the key
        std::deque<result_t> acc_results; // deque of acculumated results
        uint64_t last_quantum; // identifier of the last quantum
        uint64_t rcv_counter; // number of tuples received of the key
        uint64_t slide_counter; // counter of the tuples in the last slide
        uint64_t ts_rcv_counter; // counter of received tuples (count-based translation)
        uint64_t next_lwid;// next window to be opened of the key (lwid)
        size_t batchedWin; // number of batched windows of the key
        size_t num_processed_batches; // number of processed batches of the key
        uint64_t initial_gwid; // gwid of the first window in the next batch

        // Constructor
        Key_Descriptor(combgpu_func_t _comb_func,
                       size_t _batchSize,
                       size_t _numWindows,
                       size_t _win_len,
                       size_t _slide_len,
                       key_t _key,
                       cudaStream_t *_cudaStream,
                       size_t _numSMs,
                       size_t _max_blocks_per_sm):
                       fatgpu(_comb_func, _batchSize, _numWindows, _win_len, _slide_len, _key, _cudaStream, _numSMs, _max_blocks_per_sm),
                       last_quantum(0),
                       rcv_counter(0),
                       slide_counter(0),
                       ts_rcv_counter(0),
                       next_lwid(0),
                       batchedWin(0),
                       num_processed_batches(0),
                       initial_gwid(0)
        {
            pending_tuples.reserve(_batchSize);
        }

        // Move Constructor
        Key_Descriptor(Key_Descriptor &&_k):
                       fatgpu(std::move(_k.fatgpu)),
                       pending_tuples(std::move(_k.pending_tuples)),
                       acc_results(std::move(_k.acc_results)),
                       last_quantum(_k.last_quantum),
                       rcv_counter(_k.rcv_counter),
                       slide_counter(_k.slide_counter),
                       ts_rcv_counter(_k.ts_rcv_counter),
                       next_lwid(_k.next_lwid),
                       batchedWin(_k.batchedWin),
                       num_processed_batches(_k.num_processed_batches),
                       initial_gwid(_k.initial_gwid) {}
    };
    std::string opName; // name of the FFAT_Aggregator_GPU containing the replica
    size_t id_replica; // identifier of the FFAT_Aggregator_GPU replica
    bool input_batching; // if true, the FFAT_Aggregator_GPU expects to receive batches instead of individual inputs
    bool terminated; // true if the FFAT_Aggregator_GPU has finished its work
    Basic_Emitter *emitter; // pointer to the used emitter
    uint64_t win_len; // window length (no. of tuples or in time units)
    uint64_t slide_len; // slide length (no. of tuples or in time units)
    uint64_t lateness; // triggering delay in time units (meaningful for TB windows in DEFAULT mode)
    Win_Type_t winType; // window type (CB or TB)
    uint64_t quantum; // quantum value (for time-based windows only)
    std::unordered_map<key_t, Key_Descriptor> keyMap; // hash table that maps a descriptor for each key
    size_t batch_len; // length of the micro-batch in terms of no. of windows
    size_t tuples_per_batch; // number of tuples per batch
    bool rebuild; // flag stating whether the FlatFAT_GPU structure must be built every batch or only updated
    bool isRunningKernel; // true if the kernel is running on the GPU, false otherwise
    Key_Descriptor *lastKeyD; // pointer to the key descriptor of the running kernel on the GPU
    size_t ignored_tuples; // number of ignored tuples
    Execution_Mode_t execution_mode; // execution mode of the FFAT_Aggregator_GPU
    uint64_t last_time; // last received timestamp or watermark
    int numSMs; // number of Stream MultiProcessors of the used GPU
    int max_blocks_per_sm; // maximum number of blocks resident on each Stream Multiprocessor of the GPU
    cudaStream_t cudaStream; // CUDA stream used by the FFAT_Aggregator_GPU
#if defined (WF_TRACING_ENABLED)
    Stats_Record stats_record;
    double avg_td_us = 0;
    double avg_ts_us = 0;
    volatile uint64_t startTD, startTS, endTD, endTS;
#endif

public:
    // Constructor
    FFAT_Replica_GPU(liftgpu_func_t _lift_func,
                     combgpu_func_t _comb_func,
                     key_extractor_func_t _key_extr,
                     size_t _id_replica,
                     std::string _opName,
                     uint64_t _win_len,
                     uint64_t _slide_len,
                     uint64_t _quantum,
                     uint64_t _lateness,
                     Win_Type_t _winType,
                     size_t _batch_len,
                     bool _rebuild):
                     lift_func(_lift_func),
                     comb_func(_comb_func),
                     key_extr(_key_extr),
                     id_replica(_id_replica),
                     opName(_opName),
                     input_batching(false),
                     terminated(false),
                     emitter(nullptr),
                     win_len(_win_len),
                     slide_len(_slide_len),
                     quantum(_quantum),
                     lateness(_lateness),
                     winType(_winType),
                     batch_len(_batch_len),
                     tuples_per_batch(0),
                     rebuild(_rebuild),
                     isRunningKernel(false),
                     lastKeyD(nullptr),
                     ignored_tuples(0),
                     execution_mode(Execution_Mode_t::DEFAULT),
                     last_time(0)
    {
        if (winType == Win_Type_t::CB) {
            assert(quantum == 0); // sanity check
        }
        else {
            win_len = win_len / quantum;
            slide_len = slide_len / quantum;            
        }
        gpuErrChk(cudaStreamCreate(&cudaStream));
        gpuErrChk(cudaDeviceGetAttribute(&numSMs, cudaDevAttrMultiProcessorCount, 0)); // device_id = 0
#if (__CUDACC_VER_MAJOR__ >= 11) // at least CUDA 11
        gpuErrChk(cudaDeviceGetAttribute(&max_blocks_per_sm, cudaDevAttrMaxBlocksPerMultiprocessor, 0)); // device_id = 0
#else
        max_blocks_per_sm = WF_GPU_MAX_BLOCKS_PER_SM;
#endif
    }

    // Copy Constructor
    FFAT_Replica_GPU(const FFAT_Replica_GPU &_other):
                     lift_func(_other.lift_func),
                     comb_func(_other.comb_func),
                     key_extr(_other.key_extr),
                     id_replica(_other.id_replica),
                     opName(_other.opName),
                     input_batching(_other.input_batching),
                     terminated(_other.terminated),
                     win_len(_other.win_len),
                     slide_len(_other.slide_len),
                     quantum(_other.quantum),
                     lateness(_other.lateness),
                     winType(_other.winType),
                     batch_len(_other.batch_len),
                     tuples_per_batch(_other.tuples_per_batch),
                     rebuild(_other.rebuild),
                     isRunningKernel(_other.isRunningKernel),
                     lastKeyD(nullptr),
                     ignored_tuples(_other.ignored_tuples),
                     execution_mode(_other.execution_mode),
                     last_time(_other.last_time),
                     numSMs(_other.numSMs),
                     max_blocks_per_sm(_other.max_blocks_per_sm)
    {
        if (_other.emitter == nullptr) {
            emitter = nullptr;
        }
        else {
            emitter = (_other.emitter)->clone(); // clone the emitter if it exists
        }
        gpuErrChk(cudaStreamCreate(&cudaStream));
#if defined (WF_TRACING_ENABLED)
        stats_record = _other.stats_record;
#endif
    }                

    // Move Constructor
    FFAT_Replica_GPU(FFAT_Replica_GPU &&_other):
                     lift_func(std::move(_other.lift_func)),
                     comb_func(std::move(_other.comb_func)),
                     key_extr(std::move(_other.key_extr)),
                     id_replica(_other.id_replica),
                     opName(std::move(_other.opName)),
                     input_batching(_other.input_batching),
                     terminated(_other.terminated),
                     emitter(std::exchange(_other.emitter, nullptr)),
                     win_len(_other.win_len),
                     slide_len(_other.slide_len),
                     quantum(_other.quantum),
                     lateness(_other.lateness),
                     winType(_other.winType),
                     keyMap(std::move(_other.keyMap)),
                     batch_len(_other.batch_len),
                     tuples_per_batch(_other.tuples_per_batch),
                     rebuild(_other.rebuild),
                     isRunningKernel(_other.isRunningKernel),
                     lastKeyD(std::exchange(_other.lastKeyD, nullptr)),
                     ignored_tuples(_other.ignored_tuples),
                     execution_mode(_other.execution_mode),
                     last_time(_other.last_time),
                     numSMs(_other.numSMs),
                     max_blocks_per_sm(_other.max_blocks_per_sm)
    {
        gpuErrChk(cudaStreamCreate(&cudaStream));
#if defined (WF_TRACING_ENABLED)
        stats_record = std::move(_other.stats_record);
#endif
    }

    // Destructor
    ~FFAT_Replica_GPU()
    {
        if (emitter != nullptr) {
            delete emitter;
        }
        gpuErrChk(cudaStreamDestroy(cudaStream));
    }

    // Copy Assignment Operator
    FFAT_Replica_GPU &operator=(const FFAT_Replica_GPU &_other)
    {
        if (this != &_other) {
            lift_func = _other.lift_func;
            comb_func = _other.comb_func;
            key_extr = _other.key_extr;
            id_replica = _other.id_replica;
            opName = _other.opName;
            input_batching = _other.input_batching;
            terminated = _other.terminated;
            if (emitter != nullptr) {
                delete emitter;
            }
            if (_other.emitter == nullptr) {
                emitter = nullptr;
            }
            else {
                emitter = (_other.emitter)->clone(); // clone the emitter if it exists
            }
            win_len = _other.win_len;
            slide_len = _other.slide_len;
            quantum = _other.quantum;
            lateness = _other.lateness;
            winType = _other.winType;
            batch_len = _other.batch_len;
            tuples_per_batch = _other.tuples_per_batch;
            rebuild = _other.rebuild;
            isRunningKernel = _other.isRunningKernel;
            lastKeyD = nullptr;
            ignored_tuples = _other.ignored_tuples;
            execution_mode = _other.execution_mode;
            last_time = _other.last_time;
            numSMs = _other.numSMs;
            max_blocks_per_sm = _other.max_blocks_per_sm;
#if defined (WF_TRACING_ENABLED)
            stats_record = _other.stats_record;
#endif
        }
        return *this;
    }

    // Move Assignment Operator
    FFAT_Replica_GPU &operator=(FFAT_Replica_GPU &&_other)
    {
        lift_func = std::move(_other.lift_func);
        comb_func = std::move(_other.comb_func);
        key_extr = std::move(_other.key_extr);
        id_replica = _other.id_replica;
        opName = std::move(_other.opName);
        input_batching = _other.input_batching;
        terminated = _other.terminated;
        if (emitter != nullptr) {
            delete emitter;
        }
        emitter = std::exchange(_other.emitter, nullptr);
        win_len = _other.win_len;
        slide_len = _other.slide_len;
        quantum = _other.quantum;
        lateness = _other.lateness;
        winType = _other.winType;
        keyMap = std::move(_other.keyMap);
        batch_len = _other.batch_len;
        tuples_per_batch = _other.tuples_per_batch;
        rebuild = _other.rebuild;
        isRunningKernel = _other.isRunningKernel;
        lastKeyD = std::exchange(_other.lastKeyD, nullptr);
        ignored_tuples = _other.ignored_tuples;
        execution_mode = _other.execution_mode;
        last_time = _other.last_time;
        numSMs = _other.numSMs;
        max_blocks_per_sm = _other.max_blocks_per_sm;
 #if defined (WF_TRACING_ENABLED)
        stats_record = _other.stats_record;
#endif
        return *this;
    }

    // Wait for the completion of the previous kernel (if any) and flush the completed results
    void waitAndFlush()
    {
        if (isRunningKernel) {
            assert(lastKeyD != nullptr); // sanity check
            auto *results = (lastKeyD->fatgpu).waitResults();
            for (size_t i=0; i<batch_len; i++) {
                emitter->emit(&(results[i]), 0, last_time, last_time, this);
#if defined (WF_TRACING_ENABLED)
                stats_record.outputs_sent++;
                stats_record.bytes_sent += sizeof(result_t);
#endif
            }
            isRunningKernel = false;
            lastKeyD = nullptr;
        }
    }

    // svc_init method (utilized by the FastFlow runtime)
    int svc_init() override
    {
#if defined (WF_TRACING_ENABLED)
        stats_record = Stats_Record(opName, std::to_string(this->get_my_id()), true, true);
#endif
        tuples_per_batch = (batch_len - 1) * slide_len + win_len; // compute the size of the batch in number of tuples
        return 0;
    }

    // svc method (utilized by the FastFlow runtime)
    void *svc(void *_in) override
    {
#if defined (WF_TRACING_ENABLED)
        startTS = current_time_nsecs();
        if (stats_record.inputs_received == 0) {
            startTD = current_time_nsecs();
        }
#endif
        if (input_batching) { // receiving a batch
            Batch_t<decltype(get_tuple_t_Lift(lift_func))> *batch_input = reinterpret_cast<Batch_t<decltype(get_tuple_t_Lift(lift_func))> *>(_in);
            if (batch_input->isPunct()) { // if it is a punctuaton
                emitter->propagate_punctuation(batch_input->getWatermark(id_replica), this); // propagate the received punctuation
                assert(last_time <= batch_input->getWatermark(id_replica)); // sanity check
                last_time = batch_input->getWatermark(id_replica);
                deleteBatch_t(batch_input); // delete the punctuation
                return this->GO_ON;
            }
#if defined (WF_TRACING_ENABLED)
            stats_record.inputs_received += batch_input->getSize();
            stats_record.bytes_received += batch_input->getSize() * sizeof(tuple_t);
#endif
            for (size_t i=0; i<batch_input->getSize(); i++) { // process all the inputs within the received batch
                if (winType == Win_Type_t::CB) { // count-based windows
                    process_input_cb(batch_input->getTupleAtPos(i), batch_input->getTimestampAtPos(i), batch_input->getWatermark(id_replica));
                }
                else { // time-based windows
                    process_input_tb(batch_input->getTupleAtPos(i), batch_input->getTimestampAtPos(i), batch_input->getWatermark(id_replica));
                }
            }
            deleteBatch_t(batch_input); // delete the input batch
        }
        else { // receiving a single input
            Single_t<decltype(get_tuple_t_Lift(lift_func))> *input = reinterpret_cast<Single_t<decltype(get_tuple_t_Lift(lift_func))> *>(_in);
            if (input->isPunct()) { // if it is a punctuaton
                emitter->propagate_punctuation(input->getWatermark(id_replica), this); // propagate the received punctuation
                assert(last_time <= input->getWatermark(id_replica)); // sanity check
                last_time = input->getWatermark(id_replica);
                deleteSingle_t(input); // delete the punctuation
                return this->GO_ON;
            }
#if defined (WF_TRACING_ENABLED)
            stats_record.inputs_received++;
            stats_record.bytes_received += sizeof(tuple_t);
#endif
            if (winType == Win_Type_t::CB) { // count-based windows
                process_input_cb(input->tuple, input->getTimestamp(), input->getWatermark(id_replica));
            }
            else { // time-based windows
                process_input_tb(input->tuple, input->getTimestamp(), input->getWatermark(id_replica));
            }
            deleteSingle_t(input); // delete the input Single_t
        }
#if defined (WF_TRACING_ENABLED)
        endTS = current_time_nsecs();
        endTD = current_time_nsecs();
        double elapsedTS_us = ((double) (endTS - startTS)) / 1000;
        avg_ts_us += (1.0 / stats_record.inputs_received) * (elapsedTS_us - avg_ts_us);
        double elapsedTD_us = ((double) (endTD - startTD)) / 1000;
        avg_td_us += (1.0 / stats_record.inputs_received) * (elapsedTD_us - avg_td_us);
        stats_record.service_time = std::chrono::duration<double, std::micro>(avg_ts_us);
        stats_record.eff_service_time = std::chrono::duration<double, std::micro>(avg_td_us);
        startTD = current_time_nsecs();
#endif
        return this->GO_ON;
    }

    // Process a single input (count-based windows)
    void process_input_cb(tuple_t &_tuple,
                          uint64_t _timestamp,
                          uint64_t _watermark)
    {
        if (execution_mode == Execution_Mode_t::DEFAULT) {
            assert(last_time <= _watermark); // sanity check
            last_time = _watermark;
        }
        else {
            assert(last_time <= _timestamp); // sanity check
            last_time = _timestamp;
        }
        auto key = key_extr(_tuple); // get the key attribute of the input tuple
        auto it = keyMap.find(key); // find the corresponding key_descriptor (or allocate it if does not exist)
        if (it == keyMap.end()) {
            auto p = keyMap.insert(std::make_pair(key, Key_Descriptor(comb_func, tuples_per_batch, batch_len, win_len, slide_len, key, &cudaStream, numSMs, max_blocks_per_sm))); // create the state of the key
            it = p.first;
        }
        Key_Descriptor &key_d = (*it).second;
        key_d.rcv_counter++;
        key_d.slide_counter++;
        result_t res = create_win_result_t<decltype(get_result_t_Lift(lift_func)), decltype(key)>(key);
        lift_func(_tuple, res);
        (key_d.pending_tuples).push_back(res);
        if (key_d.rcv_counter == win_len) { // first window when it is complete
            key_d.batchedWin++;
            uint64_t lwid = key_d.next_lwid;
            uint64_t gwid = lwid;
            if (key_d.batchedWin == 1) {
                key_d.initial_gwid = gwid;
            }
            key_d.next_lwid++;
            key_d.slide_counter = 0;
        }
        else if ((key_d.rcv_counter > win_len) && (key_d.slide_counter % slide_len == 0)) { // other windows when the slide is complete
            key_d.batchedWin++;
            uint64_t lwid = key_d.next_lwid;
            uint64_t gwid = lwid;
            if (key_d.batchedWin == 1) {
                key_d.initial_gwid = gwid;
            }
            key_d.next_lwid++;
            key_d.slide_counter = 0;
        }
        if (key_d.batchedWin == batch_len) { // check whether a new batch is ready to be computed
            waitAndFlush(); // if we have a previously launched batch, we emit its results
            if (rebuild) { // rebuild mode
                (key_d.fatgpu).build(key_d.pending_tuples, key_d.initial_gwid);
                (key_d.pending_tuples).erase((key_d.pending_tuples).begin(), (key_d.pending_tuples).begin() + batch_len * slide_len);
            }
            else { // update mode
                if (key_d.num_processed_batches == 0) { // build only for the first batch
                    (key_d.fatgpu).build(key_d.pending_tuples, key_d.initial_gwid);
                    key_d.num_processed_batches++;
                }
                else { // update otherwise
                    (key_d.fatgpu).update(key_d.pending_tuples, key_d.initial_gwid);
                    key_d.num_processed_batches++;
                }
                key_d.pending_tuples.clear(); // clear the pending tuples
            }
            key_d.batchedWin = 0;
            key_d.fatgpu.getAsyncResults(); // start acquiring the results from GPU asynchronously
            isRunningKernel = true;
            lastKeyD = &key_d;
        }
    }

    // Process a single input (time-based windows)
    void process_input_tb(tuple_t &_tuple,
                          uint64_t _timestamp,
                          uint64_t _watermark)
    {
        if (execution_mode == Execution_Mode_t::DEFAULT) {
            assert(last_time <= _watermark); // sanity check
            last_time = _watermark;
        }
        else {
            assert(last_time <= _timestamp); // sanity check
            last_time = _timestamp;
        }
        auto key = key_extr(_tuple); // get the key attribute of the input tuple
        auto it = keyMap.find(key); // find the corresponding key_descriptor (or allocate it if does not exist)
        if (it == keyMap.end()) {
            auto p = keyMap.insert(std::make_pair(key, Key_Descriptor(comb_func, tuples_per_batch, batch_len, win_len, slide_len, key, &cudaStream, numSMs, max_blocks_per_sm))); // create the state of the key
            it = p.first;
        }
        Key_Descriptor &key_d = (*it).second;
        uint64_t quantum_id = _timestamp / quantum; // compute the identifier of the quantum containing the input tuple
        // check if the tuple must be ignored
        if (quantum_id < key_d.last_quantum) {
#if defined (WF_TRACING_ENABLED)
            stats_record.inputs_ignored++;
#endif
            ignored_tuples++;
            return;
        }
        key_d.rcv_counter++;
        auto &acc_results = key_d.acc_results;
        int64_t distance = quantum_id - key_d.last_quantum;
        for (size_t i=acc_results.size(); i<=distance; i++) { // resize acc_results properly
            result_t r = create_win_result_t<decltype(get_result_t_Lift(lift_func)), decltype(key)>(key);
            acc_results.push_back(r);
        }
        result_t tmp = create_win_result_t<decltype(get_result_t_Lift(lift_func)), decltype(key)>(key);
        lift_func(_tuple, tmp);
        size_t id = quantum_id - key_d.last_quantum; // compute the identifier of the corresponding quantum
        result_t tmp2 = create_win_result_t<decltype(get_result_t_Lift(lift_func)), decltype(key)>(key);
        comb_func(acc_results[id], tmp, tmp2);
        acc_results[id] = tmp2;
        size_t n_completed = 0;
        for (size_t i=0; i<acc_results.size(); i++) { // check whether there are complete quantums by taking into account the lateness
            uint64_t final_ts = ((key_d.last_quantum+i+1) * quantum)-1;
            if (final_ts + lateness < _watermark) {
                n_completed++;
                processCompleteTBWindows(key_d, acc_results[i], key, _timestamp, _watermark);
                key_d.last_quantum++;
            }
            else {
                break;
            }
        }
        acc_results.erase(acc_results.begin(), acc_results.begin() + n_completed); // remove the accumulated results of all the complete quantums
    }

    // process a completed time-based window
    void processCompleteTBWindows(Key_Descriptor &key_d,
                                  result_t &r,
                                  key_t key,
                                  uint64_t _timestamp,
                                  uint64_t _watermark)
    {
        (key_d.pending_tuples).push_back(r);
        key_d.ts_rcv_counter++;
        key_d.slide_counter++;
        if (key_d.ts_rcv_counter == win_len) { // first window when it is complete
            key_d.batchedWin++;
            uint64_t lwid = key_d.next_lwid;
            uint64_t gwid = lwid;
            if (key_d.batchedWin == 1) {
                key_d.initial_gwid = gwid;
            }
            key_d.next_lwid++;
            key_d.slide_counter = 0;
        }
        else if ((key_d.ts_rcv_counter > win_len) && (key_d.slide_counter % slide_len == 0)) { // other windows when the slide is complete
            key_d.batchedWin++;
            uint64_t lwid = key_d.next_lwid;
            uint64_t gwid = lwid;
            if (key_d.batchedWin == 1) {
                key_d.initial_gwid = gwid;
            }
            key_d.next_lwid++;
            key_d.slide_counter = 0;
        }
        if (key_d.batchedWin == batch_len) { // check whether a new batch is ready to be computed
            waitAndFlush(); // if we have a previously launched batch, we emit its results
            if (rebuild) { // rebuild mode
                (key_d.fatgpu).build(key_d.pending_tuples, key_d.initial_gwid);
                (key_d.pending_tuples).erase((key_d.pending_tuples).begin(), (key_d.pending_tuples).begin() + batch_len * slide_len);
            }
            else { // update mode
                if (key_d.num_processed_batches == 0) { // build only for the first batch
                    (key_d.fatgpu).build(key_d.pending_tuples, key_d.initial_gwid);
                    key_d.num_processed_batches++;
                }
                else { // update otherwise
                    (key_d.fatgpu).update(key_d.pending_tuples, key_d.initial_gwid);
                    key_d.num_processed_batches++;
                }
                // clear the pending tuples
                key_d.pending_tuples.clear();
            }
            key_d.batchedWin = 0;
            key_d.fatgpu.getAsyncResults(); // start acquiring the results from GPU asynchronously
            isRunningKernel = true;
            lastKeyD = &key_d;
        }
    }

    // method to manage the EOS (utilized by the FastFlow runtime)
    void eosnotify(ssize_t id) override
    {
        if (winType == Win_Type_t::CB) { // count-based eos logic
            eosnotifyCBWindows(id);
        }
        else {
            eosnotifyTBWindows(id); // count-based eos logic
        }
        emitter->flush(this); // call the flush of the emitter
        terminated = true;
#if defined (WF_TRACING_ENABLED)
        stats_record.setTerminated();
#endif
    }

    // eosnotify with count-based windows
    void eosnotifyCBWindows(ssize_t id)
    {
        waitAndFlush(); // if we have a previously launched batch, we emit its results
        for (auto &k: keyMap) {  // iterate over all the keys
            auto key = k.first;
            Key_Descriptor &key_d = k.second;
            auto &fatgpu = key_d.fatgpu;
            std::vector<decltype(get_result_t_Lift(lift_func))> remaining_tuples;
            if (!rebuild && key_d.num_processed_batches > 0) {
                remaining_tuples = (key_d.fatgpu).getBatchedTuples();
                remaining_tuples.erase(remaining_tuples.begin(), remaining_tuples.begin() + batch_len * slide_len);
            }
            remaining_tuples.insert(remaining_tuples.end(), (key_d.pending_tuples).begin(), (key_d.pending_tuples).end());
            for (size_t wid=0; wid<key_d.batchedWin; wid++) { // for all the complete batched windows
                result_t res = create_win_result_t<decltype(get_result_t_Lift(lift_func)), decltype(key)>(key, key_d.initial_gwid + wid);
                auto it = remaining_tuples.begin();
                for (size_t i=0; i<win_len; it++, i++) {
                    comb_func(*it, res, res);
                }
                remaining_tuples.erase(remaining_tuples.begin(), remaining_tuples.begin() + slide_len);
                emitter->emit(&res, 0, last_time, last_time, this);
#if defined (WF_TRACING_ENABLED)
                stats_record.outputs_sent++;
                stats_record.bytes_sent += sizeof(result_t);
#endif
            }
            size_t numIncompletedWins = ceil(remaining_tuples.size() / (double) slide_len); // for all the incomplete windows
            for (size_t i=0; i<numIncompletedWins; i++) {
                uint64_t lwid = key_d.next_lwid;
                uint64_t gwid = lwid;
                key_d.next_lwid++;
                result_t res = create_win_result_t<decltype(get_result_t_Lift(lift_func)), decltype(key)>(key, gwid);
                for(auto it = remaining_tuples.begin(); it != remaining_tuples.end(); it++) {
                    comb_func(*it, res, res);
                }
                auto lastPos = remaining_tuples.end() <= remaining_tuples.begin() + slide_len ? remaining_tuples.end() : remaining_tuples.begin() + slide_len;
                remaining_tuples.erase(remaining_tuples.begin(), lastPos);
                emitter->emit(&res, 0, last_time, last_time, this);
#if defined (WF_TRACING_ENABLED)
                stats_record.outputs_sent++;
                stats_record.bytes_sent += sizeof(result_t);
#endif
            }
        }
    }

    // eosnotify with time-based windows
    void eosnotifyTBWindows(ssize_t id)
    {
        waitAndFlush(); // if we have a previously launched batch, we emit its results
        for (auto &k: keyMap) { // iterate over all the keys
            auto key = k.first;
            Key_Descriptor &key_d = k.second;
            auto &fatgpu = key_d.fatgpu;
            auto &acc_results = key_d.acc_results;
            for (size_t i=0; i<acc_results.size(); i++) { // add all the accumulated results
               processCompleteTBWindows(key_d, acc_results[i], key, last_time, last_time);
               key_d.last_quantum++;
            }
            waitAndFlush(); // if we have a previously launched batch, we emit its results
            std::vector<decltype(get_result_t_Lift(lift_func))> remaining_tuples;
            if (!rebuild && key_d.num_processed_batches > 0) {
                remaining_tuples = (key_d.fatgpu).getBatchedTuples();
                remaining_tuples.erase(remaining_tuples.begin(), remaining_tuples.begin() + batch_len * slide_len);
            }
            remaining_tuples.insert(remaining_tuples.end(), (key_d.pending_tuples).begin(), (key_d.pending_tuples).end());
            for (size_t wid=0; wid<key_d.batchedWin; wid++) { // for all the complete batched windows
                result_t res = create_win_result_t<decltype(get_result_t_Lift(lift_func)), decltype(key)>(key, key_d.initial_gwid + wid);
                auto it = remaining_tuples.begin();
                for (size_t i=0; i<win_len; it++, i++) {
                    comb_func(*it, res, res);
                }
                remaining_tuples.erase(remaining_tuples.begin(), remaining_tuples.begin() + slide_len);
                emitter->emit(&res, 0, last_time, last_time, this);
#if defined (WF_TRACING_ENABLED)
                stats_record.outputs_sent++;
                stats_record.bytes_sent += sizeof(result_t);
#endif
            }
            size_t numIncompletedWins = ceil(remaining_tuples.size() / (double) slide_len); // for all the incomplete windows
            for (size_t i=0; i<numIncompletedWins; i++) {
                uint64_t lwid = key_d.next_lwid;
                uint64_t gwid = lwid;
                key_d.next_lwid++;
                result_t res = create_win_result_t<decltype(get_result_t_Lift(lift_func)), decltype(key)>(key, gwid);
                for(auto it = remaining_tuples.begin(); it != remaining_tuples.end(); it++) {
                    comb_func(*it, res, res);
                }
                auto lastPos = remaining_tuples.end() <= remaining_tuples.begin() + slide_len ? remaining_tuples.end() : remaining_tuples.begin() + slide_len;
                remaining_tuples.erase(remaining_tuples.begin(), lastPos);
                emitter->emit(&res, 0, last_time, last_time, this);
#if defined (WF_TRACING_ENABLED)
                stats_record.outputs_sent++;
                stats_record.bytes_sent += sizeof(result_t);
#endif
            }
        }
    }

    // Configure the Window_Replica to receive batches instead of individual inputs
    void receiveBatches(bool _input_batching)
    {
        input_batching = _input_batching;
    }

    // Set the emitter used to route outputs from the FFAT_Aggregator replica
    void setEmitter(Basic_Emitter *_emitter)
    {
        emitter = _emitter;
    }

    // Check the termination of the Window_Replica
    bool isTerminated() const
    {
        return terminated;
    }

    // Set the execution mode of the FFAT_Aggregator replica
    void setExecutionMode(Execution_Mode_t _execution_mode)
    {
        execution_mode = _execution_mode;
    }

    // Get the number of ignored tuples
    size_t getNumIgnoredTuples() const
    {
        return ignored_tuples;
    }

#if defined (WF_TRACING_ENABLED)
    // Get a copy of the Stats_Record of the FFAT_Replica_GPU
    Stats_Record getStatsRecord() const
    {
        return stats_record;
    }
#endif
};

} // namespace wf

#endif
