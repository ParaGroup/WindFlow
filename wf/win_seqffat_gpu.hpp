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
 *  @file    win_seqffat_gpu.hpp
 *  @author  Elia Ruggeri and Gabriele Mencagli
 *  @date    16/03/2020
 *  
 *  @brief Win_SeqFFAT_GPU node executing associative windowed queries on GPU
 *         with the algorithm in the FlatFAT_GPU data structure
 *  
 *  @section Win_SeqFFAT_GPU (Description)
 *  
 *  This file implements the Win_SeqFFAT_GPU node able to execute associative windowed
 *  queries on a GPU device. The node prepares batches of input tuples sequentially on
 *  a CPU core and offloads on the GPU the parallel processing of the windows within
 *  the same batch. The algorithm is the one implemented by the FlatFAT_GPU.
 *  
 *  The template parameters tuple_t and result_t must be default constructible, with a copy
 *  constructor and a copy assignment operator, and they must provide and implement the
 *  setControlFields() and getControlFields() methods. Furthermore, in order to be copyable
 *  in a GPU-accessible memory, they must be compliant with the C++ specification for standard
 *  layout types. The third template argument comb_F_t is the type of the callable object to be
 *  used for GPU processing.
 */ 

#ifndef WIN_SEQFFAT_GPU_H
#define WIN_SEQFFAT_GPU_H

// includes
#include<deque>
#include<vector>
#include<string>
#include<unordered_map>
#include<math.h>
#include<ff/node.hpp>
#include<ff/multinode.hpp>
#include<meta.hpp>
#include<meta_gpu.hpp>
#include<flatfat_gpu.hpp>
#if defined (TRACE_WINDFLOW)
    #include<stats_record.hpp>
#endif

namespace wf {

// Win_SeqFFAT_GPU class
template<typename tuple_t, typename result_t, typename comb_F_t>
class Win_SeqFFAT_GPU: public ff::ff_minode_t<tuple_t, result_t>
{
private:
    static_assert(std::is_standard_layout<tuple_t>::value,
        "WindFlow Compilation Error - input type of a GPU operator is not a standard_layout type:\n");
    static_assert(std::is_standard_layout<result_t>::value,
        "WindFlow Compilation Error - output type of a GPU operator is not a standard_layout type:\n");
    // type of the lift function
    using winLift_func_t = std::function<void(const tuple_t &, result_t &)>;
    tuple_t tmp; // never used
    // key data type
    using key_t = typename std::remove_reference<decltype(std::get<0>(tmp.getControlFields()))>::type;
    // friendships with other classes in the library
    template<typename T1, typename T2, typename T3>
    friend class Key_FFAT_GPU;
    // struct of a key descriptor
    struct Key_Descriptor
    {
        FlatFAT_GPU<tuple_t, result_t, comb_F_t> fatgpu; // FlatFAT_GPU of this key
        std::vector<result_t> pending_tuples; // vector of pending tuples of this key
        std::deque<result_t> acc_results; // deque of acculumated results
        uint64_t cb_id; // identifier used in the count-based translation
        uint64_t last_quantum; // identifier of the last quantum
        uint64_t rcv_counter; // number of tuples received of this key
        uint64_t slide_counter; // counter of the tuples in the last slide
        uint64_t ts_rcv_counter; // counter of received tuples (count-based translation)
        uint64_t next_ids; // progressive counter (used if isRenumbering is true)
        uint64_t next_lwid;// next window to be opened of this key (lwid)
        size_t batchedWin; // number of batched windows of the key
        size_t num_processed_batches; // number of processed batches of this key
        std::vector<uint64_t> gwids; // vector of gwid of the windows in the current batch
        std::vector<uint64_t> tsWin; // vector of the final timestamp of the windows in the current batch

        // Constructor
        Key_Descriptor(winLift_func_t _winLift_func,
                       comb_F_t _winComb_func,
                       size_t _batchSize, 
                       size_t _numWindows,
                       size_t _win_len, 
                       size_t _slide_len,
                       key_t _key,
                       cudaStream_t *_cudaStream,
                       size_t _n_thread_block,
                       int _numSMs):
                       fatgpu(_winLift_func, _winComb_func, _batchSize, _numWindows, _win_len, _slide_len, _key, _cudaStream, _n_thread_block, _numSMs),
                       cb_id(0),
                       last_quantum(0),
                       rcv_counter(0),
                       slide_counter(0),
                       ts_rcv_counter(0),
                       next_ids(0),
                       next_lwid(0),
                       batchedWin(0),
                       num_processed_batches(0)
        {
            pending_tuples.reserve(_batchSize);
        }

        // move Constructor
        Key_Descriptor(Key_Descriptor &&_k):
                       fatgpu(std::move(_k.fatgpu)),
                       pending_tuples(std::move(_k.pending_tuples)),
                       acc_results(std::move(_k.acc_results)),
                       cb_id(_k.cb_id),
                       last_quantum(_k.last_quantum),
                       rcv_counter(_k.rcv_counter),
                       slide_counter(_k.slide_counter),
                       ts_rcv_counter(_k.ts_rcv_counter),
                       next_ids(_k.next_ids),
                       next_lwid(_k.next_lwid),
                       batchedWin(_k.batchedWin),
                       num_processed_batches(_k.num_processed_batches),
                       gwids(_k.gwids),
                       tsWin(_k.tsWin) {}
    };
    // CPU variables
    winLift_func_t winLift_func; // lift function
    comb_F_t winComb_func; // combine function
    uint64_t quantum; // quantum value (for time-based windows only)
    uint64_t win_len; // window length (no. of tuples or in time units)
    uint64_t slide_len; // slide length (no. of tuples or in time units)
    uint64_t triggering_delay; // triggering delay in time units (meaningful for TB windows only)
    win_type_t winType; // window type (CB or TB)
    std::string name; // string of the unique name of the node
    WinOperatorConfig config; // configuration structure of the Win_SeqFFAT_GPU node
    std::unordered_map<size_t, Key_Descriptor> keyMap; // hash table that maps keys onto descriptors
    size_t batch_len; // length of the micro-batch in terms of no. of windows
    size_t tuples_per_batch; // number of tuples per batch
    bool rebuild; // flag stating whether the FLATFAT_GPU must be built every batch or only updated
    bool isRunningKernel = false; // true if the kernel is running on the GPU, false otherwise
    Key_Descriptor *lastKeyD = nullptr; // pointer to the key descriptor of the running kernel on the GPU
    size_t ignored_tuples; // number of ignored tuples
    size_t eos_received; // number of received EOS messages
    bool terminated; // true if the replica has finished its work
    bool isRenumbering; // if true, the node assigns increasing identifiers to the input tuples (useful for count-based windows in DEFAULT mode)
    // GPU variables
    int gpu_id; // identifier of the chosen GPU device
    size_t n_thread_block; // number of threads per block
    int numSMs; // number of Stream MultiProcessors of the used GPU
    cudaStream_t cudaStream; // CUDA stream used by this Win_SeqFFAT_GPU node
#if defined (TRACE_WINDFLOW)
    Stats_Record stats_record;
    double avg_td_us = 0;
    double avg_ts_us = 0;
    volatile uint64_t startTD, startTS, endTD, endTS;
#endif

    // function to compute the gcd (std::gcd is available only in C++17)
    uint64_t gcd(uint64_t u, uint64_t v) {
        while (v != 0) {
            unsigned long r = u % v;
            u = v;
            v = r;
        }
        return u;
    };

    // Private Constructor
    Win_SeqFFAT_GPU(winLift_func_t _winLift_func,
                    comb_F_t _winComb_func,
                    uint64_t _win_len,
                    uint64_t _slide_len,
                    uint64_t _triggering_delay,
                    win_type_t _winType,
                    size_t _batch_len,
                    int _gpu_id,
                    size_t _n_thread_block,
                    bool _rebuild,
                    std::string _name,
                    WinOperatorConfig _config):
                    winLift_func(_winLift_func),
                    winComb_func(_winComb_func),
                    win_len(_win_len),
                    slide_len(_slide_len),
                    triggering_delay(_triggering_delay),
                    winType(_winType),
                    name(_name),
                    config(_config),
                    batch_len(_batch_len),
                    rebuild(_rebuild),
                    ignored_tuples(0),
                    eos_received(0),
                    terminated(false),
                    isRenumbering(false),
                    gpu_id(_gpu_id),
                    n_thread_block(_n_thread_block),
                    numSMs(0)
    {
        // check the validity of the windowing parameters
        if (win_len == 0 || slide_len == 0) {
            std::cerr << RED << "WindFlow Error: window length or slide in Win_SeqFFAT_GPU cannot be zero" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // check the use of sliding windows
        if (slide_len >= win_len) {
            std::cerr << RED << "WindFlow Error: Win_SeqFFAT_GPU can be used with sliding windows only (s<w)" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // check the validity of the batch length
        if (batch_len == 0) {
            std::cerr << RED << "WindFlow Error: batch length in Win_SeqFFAT_GPU cannot be zero" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // set the quantum value (for time-based windows only)
        if (winType == win_type_t::TB) {
            quantum = gcd(win_len, slide_len);
            win_len = win_len / quantum;
            slide_len = slide_len / quantum;
        }
        else {
            quantum = 0; // zero, quantum is never used
        }
    }

    // function to wait for the completion of the previous kernel (if any) and to flush its results
    void waitAndFlush()
    {
        if (isRunningKernel) {
            assert(lastKeyD != nullptr);
            auto *results = (lastKeyD->fatgpu).waitResults();
            for (size_t i=0; i<batch_len; i++) {
                result_t *r = new result_t();
                *r = results[i];
                r->setControlFields(std::get<0>(r->getControlFields()), (lastKeyD->gwids)[i], (lastKeyD->tsWin)[i]);
                this->ff_send_out(r);
#if defined (TRACE_WINDFLOW)
                stats_record.outputs_sent++;
                stats_record.bytes_sent += sizeof(result_t);
#endif
            }
            isRunningKernel = false;
            (lastKeyD->gwids).erase((lastKeyD->gwids).begin(), (lastKeyD->gwids).begin()+batch_len);
            (lastKeyD->tsWin).erase((lastKeyD->tsWin).begin(), (lastKeyD->tsWin).begin()+batch_len);
            lastKeyD = nullptr;
        }
    }

public:
    // Constructor
    Win_SeqFFAT_GPU(winLift_func_t _winLift_func,
                    comb_F_t _winComb_func,
                    uint64_t _win_len,
                    uint64_t _slide_len,
                    uint64_t _triggering_delay,
                    win_type_t _winType,
                    size_t _batch_len,
                    int _gpu_id,
                    size_t _n_thread_block,
                    bool _rebuild,
                    std::string _name):
                    Win_SeqFFAT_GPU(_winLift_func, _winComb_func, _win_len, _slide_len, _triggering_delay, _winType, _batch_len, _gpu_id, _n_thread_block, _rebuild, _name, WinOperatorConfig( 0, 1, _slide_len, 0, 1, _slide_len )) {}

    // svc_init method (utilized by the FastFlow runtime)
    int svc_init() override
    {
        // check the validity of the chosen GPU device
        int devicesCount = 0; // number of available GPU devices
        gpuErrChk(cudaGetDeviceCount(&devicesCount));
        if (gpu_id >= devicesCount) {
            std::cerr << RED << "WindFlow Error: chosen GPU device is not valid" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // use the chosen GPU device by this fastflow thread
        gpuErrChk(cudaSetDevice(gpu_id));
        // get the number of Stream MultiProcessors on the GPU
        gpuErrChk(cudaDeviceGetAttribute(&numSMs, cudaDevAttrMultiProcessorCount, gpu_id));
        assert(numSMs>0);
        // get the number of threads per block limit on the GPU
        int max_thread_block = 0; // maximum number of threads per block
        gpuErrChk(cudaDeviceGetAttribute(&max_thread_block, cudaDevAttrMaxThreadsPerBlock, gpu_id));
        assert(max_thread_block>0);
        // check the number of threads per block limit
        if (max_thread_block < n_thread_block) {
            std::cerr << RED << "WindFlow Error: number of threads per block exceeds the limit of the GPU (max is " << max_thread_block << ")" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // create the CUDA stream
        gpuErrChk(cudaStreamCreate(&cudaStream));
        // compute the fixed number of tuples per batch (only sliding windows here)
        tuples_per_batch = (batch_len - 1) * slide_len + win_len;
#if defined (TRACE_WINDFLOW)
        stats_record = Stats_Record(name, std::to_string(this->get_my_id()), true, true);
#endif
        return 0;
    }

    // svc method (utilized by the FastFlow runtime)
    result_t *svc(tuple_t *t) override
    {
#if defined (TRACE_WINDFLOW)
        startTS = current_time_nsecs();
        if (stats_record.inputs_received == 0) {
            startTD = current_time_nsecs();
        }
        stats_record.inputs_received++;
        stats_record.bytes_received += sizeof(tuple_t);
#endif
        // two separate logics depending on the window type
        if (winType == win_type_t::CB) {
            svcCBWindows(t);
        }
        else {
            svcTBWindows(t);
        }
#if defined (TRACE_WINDFLOW)
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

    // processing logic with count-based windows
    void svcCBWindows(tuple_t *t)
    {
        // extract the key and id fields from the input tuple
        auto key = std::get<0>(t->getControlFields()); // key
        size_t hashcode = std::hash<decltype(key)>()(key); // compute the hashcode of the key
        uint64_t id = std::get<1>(t->getControlFields()); // identifier
        // access the descriptor of the input key
        auto it = keyMap.find(key);
        if (it == keyMap.end()) {
            keyMap.insert(std::make_pair(key, Key_Descriptor(winLift_func, winComb_func, tuples_per_batch, batch_len, win_len, slide_len, key, &cudaStream, n_thread_block, numSMs)));
            it = keyMap.find(key);
#if defined (TRACE_WINDFLOW)
            (((*it).second).fatgpu).set_StatsRecord(&stats_record);
#endif
        }
        Key_Descriptor &key_d = (*it).second;
        // check if isRenumbering is enabled (used for count-based windows in DEFAULT mode)
        if (isRenumbering) {
            assert(winType == win_type_t::CB);
            id = key_d.next_ids++;
            t->setControlFields(std::get<0>(t->getControlFields()), id, std::get<2>(t->getControlFields()));
        }
        // gwid of the first window of that key assigned to this Win_SeqFFAT_GPU node
        uint64_t first_gwid_key = ((config.id_inner - (hashcode % config.n_inner) + config.n_inner) % config.n_inner) * config.n_outer + (config.id_outer - (hashcode % config.n_outer) + config.n_outer) % config.n_outer;
        key_d.rcv_counter++;
        key_d.slide_counter++;
        // convert the input tuple to a result with the lift function
        result_t res;
        res.setControlFields(key, 0, std::get<2>(t->getControlFields()));
        winLift_func(*t, res);
        (key_d.pending_tuples).push_back(res);
        // check if a new window has been fired
        if (key_d.rcv_counter == win_len) { // first window when it is complete
            key_d.batchedWin++;
            uint64_t lwid = key_d.next_lwid;
            uint64_t gwid = first_gwid_key + (lwid * config.n_outer * config.n_inner);
            (key_d.gwids).push_back(gwid);
            (key_d.tsWin).push_back(std::get<2>(t->getControlFields()));
            key_d.next_lwid++;
            key_d.slide_counter = 0;
        }
        else if ((key_d.rcv_counter > win_len) && (key_d.slide_counter % slide_len == 0)) { // other windows when the slide is complete
            key_d.batchedWin++;
            uint64_t lwid = key_d.next_lwid;
            uint64_t gwid = first_gwid_key + (lwid * config.n_outer * config.n_inner);
            (key_d.gwids).push_back(gwid);
            (key_d.tsWin).push_back(std::get<2>(t->getControlFields()));
            key_d.next_lwid++;
            key_d.slide_counter = 0;
        }
        // check whether a new batch is ready to be computed
        if (key_d.batchedWin == batch_len) {
            // if we have a previously launched batch, we emit its results
            waitAndFlush();
            // if we need to rebuild everytime the FLATFAT_GPU
            if (rebuild) {
                (key_d.fatgpu).build(key_d.pending_tuples, 0);
                (key_d.pending_tuples).erase((key_d.pending_tuples).begin(), (key_d.pending_tuples).begin() + batch_len * slide_len);
            }
            else {
                // if it is the first batch of this key, we build the FlatFAT_GPU from scratch
                if (key_d.num_processed_batches == 0) {
                    (key_d.fatgpu).build(key_d.pending_tuples, 0);
                    key_d.num_processed_batches++;
                }
                // otherwise, we update the FlatFAT_GPU with the new elements
                else {
                    (key_d.fatgpu).update(key_d.pending_tuples, 0);
                    key_d.num_processed_batches++;
                }
                // clear the pending tuples
                key_d.pending_tuples.clear();
            }
            key_d.batchedWin = 0;
            // start acquiring the results from GPU asynchronously
            key_d.fatgpu.getAsyncResults();
            isRunningKernel = true;
            lastKeyD = &key_d;
        }
        // delete the input
        delete t;
    }

    // processing logic with time-based windows
    void svcTBWindows(tuple_t *t)
    {
        // extract the key and timestamp fields from the input tuple
        auto key = std::get<0>(t->getControlFields()); // key
        uint64_t ts = std::get<2>(t->getControlFields()); // timestamp
        // access the descriptor of the input key
        auto it = keyMap.find(key);
        if (it == keyMap.end()) {
            keyMap.insert(std::make_pair(key, Key_Descriptor(winLift_func, winComb_func, tuples_per_batch, batch_len, win_len, slide_len, key, &cudaStream, n_thread_block, numSMs)));
            it = keyMap.find(key);
#if defined (TRACE_WINDFLOW)
            (((*it).second).fatgpu).set_StatsRecord(&stats_record);
#endif
        }
        Key_Descriptor &key_d = (*it).second;
        // compute the identifier of the quantum containing the input tuple
        uint64_t quantum_id = ts / quantum;
        // check if the tuple must be ignored
        if (quantum_id < key_d.last_quantum) {
#if defined (TRACE_WINDFLOW)
            stats_record.inputs_ignored++;
#endif
            ignored_tuples++;
            delete t;
            return;
        }
        key_d.rcv_counter++;
        auto &acc_results = key_d.acc_results;
        int64_t distance = quantum_id - key_d.last_quantum;
        // resize acc_results properly
        for (size_t i=acc_results.size(); i<=distance; i++){
            result_t r;
            r.setControlFields(key, key_d.cb_id, ((key_d.last_quantum+i+1) * quantum)-1);
            key_d.cb_id++;
            acc_results.push_back(r);
        }
        // add the input tuple to the correct quantum
        result_t tmp;
        tmp.setControlFields(key, 0, ts);
        winLift_func(*t, tmp);
        // compute the identifier of the corresponding quantum
        size_t id = quantum_id - key_d.last_quantum;
        result_t tmp2;
        tmp2.setControlFields(key, 0, std::max(std::get<2>((acc_results[id]).getControlFields()), std::get<2>((tmp).getControlFields())));
        winComb_func(acc_results[id], tmp, tmp2);
        acc_results[id] = tmp2;
        // check whether there are complete quantums by taking into account the triggering delay
        size_t n_completed = 0;
        for (size_t i=0; i<acc_results.size(); i++) {
            uint64_t final_ts = ((key_d.last_quantum+i+1) * quantum)-1;
            if (final_ts + triggering_delay < ts) {
                n_completed++;
                processWindows(key_d, acc_results[i]);
                key_d.last_quantum++;
            }
            else {
                break;
            }
        }
        // remove the accumulated results of all the complete quantums
        acc_results.erase(acc_results.begin(), acc_results.begin() + n_completed);
        // delete the input
        delete t;
    }

    // process a window (for time-based logic)
    void processWindows(Key_Descriptor &key_d, result_t &r)
    {
        auto key = std::get<0>(r.getControlFields()); // key
        uint64_t id = std::get<1>(r.getControlFields()); // identifier
        size_t hashcode = std::hash<decltype(key)>()(key); // compute the hashcode of the key
        // gwid of the first window of that key assigned to this Win_SeqFFAT_GPU node
        uint64_t first_gwid_key = ((config.id_inner - (hashcode % config.n_inner) + config.n_inner) % config.n_inner) * config.n_outer + (config.id_outer - (hashcode % config.n_outer) + config.n_outer) % config.n_outer;
        (key_d.pending_tuples).push_back(r);
        key_d.ts_rcv_counter++;
        key_d.slide_counter++;
        // check if a new window has been fired
        if (key_d.ts_rcv_counter == win_len) { // first window when it is complete
            key_d.batchedWin++;
            uint64_t lwid = key_d.next_lwid;
            uint64_t gwid = first_gwid_key + (lwid * config.n_outer * config.n_inner);
            (key_d.gwids).push_back(gwid);
            (key_d.tsWin).push_back(std::get<2>(r.getControlFields()));
            key_d.next_lwid++;
            key_d.slide_counter = 0;
        }
        else if ((key_d.ts_rcv_counter > win_len) && (key_d.slide_counter % slide_len == 0)) { // other windows when the slide is complete
            key_d.batchedWin++;
            uint64_t lwid = key_d.next_lwid;
            uint64_t gwid = first_gwid_key + (lwid * config.n_outer * config.n_inner);
            (key_d.gwids).push_back(gwid);
            (key_d.tsWin).push_back(std::get<2>(r.getControlFields()));
            key_d.next_lwid++;
            key_d.slide_counter = 0;
        }
        // check whether a new batch is ready to be computed
        if (key_d.batchedWin == batch_len) {
            // if we have a previously launched batch, we emit its results
            waitAndFlush();
            // if we need to rebuild everytime the FLATFAT_GPU
            if (rebuild) {
                (key_d.fatgpu).build(key_d.pending_tuples, 0);
                (key_d.pending_tuples).erase((key_d.pending_tuples).begin(), (key_d.pending_tuples).begin() + batch_len * slide_len);
            }
            else {
                // if it is the first batch of this key, we build the FlatFAT_GPU from scratch
                if (key_d.num_processed_batches == 0) {
                    (key_d.fatgpu).build(key_d.pending_tuples, 0);
                    key_d.num_processed_batches++;
                }
                // otherwise, we update the FlatFAT_GPU with the new elements
                else {
                    (key_d.fatgpu).update(key_d.pending_tuples, 0);
                    key_d.num_processed_batches++;
                }
                // clear the pending tuples
                key_d.pending_tuples.clear();
            }
            key_d.batchedWin = 0;
            // start acquiring the results from GPU asynchronously
            key_d.fatgpu.getAsyncResults();
            isRunningKernel = true;
            lastKeyD = &key_d;
        }
    }

    // method to manage the EOS (utilized by the FastFlow runtime)
    void eosnotify(ssize_t id) override
    {
        eos_received++;
        // check the number of received EOS messages
        if ((eos_received != this->get_num_inchannels()) && (this->get_num_inchannels() != 0)) { // workaround due to FastFlow
            return;
        }
        // two separate logics depending on the window type
        if (winType == win_type_t::CB) {
            eosnotifyCBWindows(id);
        }
        else {
            eosnotifyTBWindows(id);
        }
        terminated = true;
#if defined (TRACE_WINDFLOW)
        stats_record.set_Terminated();
#endif
    }

    // eosnotify with count-based windows
    void eosnotifyCBWindows(ssize_t id)
    {
        // emit results of the previously running kernel on the GPU
        waitAndFlush();
        // iterate over all the keys
        for (auto &k: keyMap) {
            // iterate over all the existing windows of the key
            auto key = k.first;
            size_t hashcode = std::hash<decltype(key)>()(key); // compute the hashcode of the key
            Key_Descriptor &key_d = k.second;
            auto &fatgpu = key_d.fatgpu;
            std::vector<result_t> remaining_tuples;
            if (!rebuild && key_d.num_processed_batches > 0) {
                remaining_tuples = (key_d.fatgpu).getBatchedTuples();
                remaining_tuples.erase(remaining_tuples.begin(), remaining_tuples.begin() + batch_len * slide_len);
            }
            remaining_tuples.insert(remaining_tuples.end(), (key_d.pending_tuples).begin( ), (key_d.pending_tuples).end());
            // for all the remaining windows
            for (auto gwid :key_d.gwids) {
                result_t *res = new result_t();
                auto it = remaining_tuples.begin();
                for (size_t i=0; i<win_len; it++, i++ ) {
                    winComb_func(*it, *res, *res);
                }
                res->setControlFields(key, gwid, std::get<2>(res->getControlFields()));
                remaining_tuples.erase(remaining_tuples.begin(), remaining_tuples.begin() + slide_len);
                this->ff_send_out(res);
#if defined (TRACE_WINDFLOW)
                stats_record.outputs_sent++;
                stats_record.bytes_sent += sizeof(result_t);
#endif
            }
            // for all the incomplete windows
            size_t numIncompletedWins = ceil(remaining_tuples.size() / (double) slide_len);
            for (size_t i=0; i<numIncompletedWins; i++) {
                uint64_t first_gwid_key = ((config.id_inner - (hashcode % config.n_inner) + config.n_inner) % config.n_inner) * config.n_outer + (config.id_outer - (hashcode % config.n_outer) + config.n_outer) % config.n_outer;
                uint64_t lwid = key_d.next_lwid;
                uint64_t gwid = first_gwid_key + (lwid * config.n_outer * config.n_inner);
                key_d.next_lwid++;
                result_t *res = new result_t();
                for(auto it = remaining_tuples.begin(); it != remaining_tuples.end(); it++) {
                    winComb_func(*it, *res, *res );
                }
                res->setControlFields(key, gwid, std::get<2>(res->getControlFields()));
                auto lastPos = remaining_tuples.end( ) <= remaining_tuples.begin( ) + slide_len ? remaining_tuples.end( ) : remaining_tuples.begin( ) + slide_len;
                remaining_tuples.erase(remaining_tuples.begin(), lastPos);
                this->ff_send_out(res);
#if defined (TRACE_WINDFLOW)
                stats_record.outputs_sent++;
                stats_record.bytes_sent += sizeof(result_t);
#endif
            }
        }
    }

    // eosnotify with time-based windows
    void eosnotifyTBWindows(ssize_t id)
    {
        // emit results of the previously running kernel on the GPU
        waitAndFlush();
        // iterate over all the keys
        for (auto &k: keyMap) {
            // iterate over all the existing windows of the key
            auto key = k.first;
            size_t hashcode = std::hash<decltype(key)>()(key); // compute the hashcode of the key
            Key_Descriptor &key_d = k.second;
            auto &fatgpu = key_d.fatgpu;
            auto &acc_results = key_d.acc_results;
            // add all the accumulated results
            for (size_t i=0; i<acc_results.size(); i++) {
               processWindows(key_d, acc_results[i]);
               key_d.last_quantum++;
            }
            // emit results of the previously running kernel on the GPU
            waitAndFlush();
            std::vector<result_t> remaining_tuples;
            if (!rebuild && key_d.num_processed_batches > 0) {
                remaining_tuples = (key_d.fatgpu).getBatchedTuples();
                remaining_tuples.erase(remaining_tuples.begin(), remaining_tuples.begin() + batch_len * slide_len);
            }
            remaining_tuples.insert(remaining_tuples.end(), (key_d.pending_tuples).begin(), (key_d.pending_tuples).end());
            // for all the remaining windows
            for (auto gwid :key_d.gwids) {
                result_t *res = new result_t();
                auto it = remaining_tuples.begin();
                for (size_t i=0; i<win_len; it++, i++ ) {
                    winComb_func(*it, *res, *res);
                }
                res->setControlFields(key, gwid, std::get<2>(res->getControlFields()));
                remaining_tuples.erase(remaining_tuples.begin(), remaining_tuples.begin() + slide_len);
                this->ff_send_out(res);
#if defined (TRACE_WINDFLOW)
                stats_record.outputs_sent++;
                stats_record.bytes_sent += sizeof(result_t);
#endif
            }
            // for all the incomplete windows
            size_t numIncompletedWins = ceil(remaining_tuples.size() / (double) slide_len);
            for (size_t i=0; i<numIncompletedWins; i++) {
                uint64_t first_gwid_key = ((config.id_inner - (hashcode % config.n_inner) + config.n_inner) % config.n_inner) * config.n_outer + (config.id_outer - (hashcode % config.n_outer) + config.n_outer) % config.n_outer;
                uint64_t lwid = key_d.next_lwid;
                uint64_t gwid = first_gwid_key + (lwid * config.n_outer * config.n_inner);
                key_d.next_lwid++;
                result_t *res = new result_t();
                for(auto it = remaining_tuples.begin(); it != remaining_tuples.end(); it++) {
                    winComb_func(*it, *res, *res );
                }
                res->setControlFields(key, gwid, std::get<2>(res->getControlFields()));
                auto lastPos = remaining_tuples.end( ) <= remaining_tuples.begin() + slide_len ? remaining_tuples.end() : remaining_tuples.begin() + slide_len;
                remaining_tuples.erase(remaining_tuples.begin(), lastPos);
                this->ff_send_out(res);
#if defined (TRACE_WINDFLOW)
                stats_record.outputs_sent++;
                stats_record.bytes_sent += sizeof(result_t);
#endif
            }
        }
    }

    // svc_end method (utilized by the FastFlow runtime)
    void svc_end() override
    {
        // destroy the CUDA stream
        gpuErrChk(cudaStreamDestroy(cudaStream));
    }

    // method to return the number of ignored tuples by this node
    size_t getNumIgnoredTuples() const
    {
        return ignored_tuples;
    }

    // method the check the termination of the replica
    bool isTerminated() const
    {
        return terminated;
    }

#if defined (TRACE_WINDFLOW)
    // method to return a copy of the Stats_Record of this node
    Stats_Record get_StatsRecord() const
    {
        return stats_record;
    }
#endif

    // method to start the node execution asynchronously
    int run(bool) override
    {
        return ff::ff_minode::run();
    }

    // method to wait the node termination
    int wait() override
    {
        return ff::ff_minode::wait();
    }
};

} // namespace wf

#endif
