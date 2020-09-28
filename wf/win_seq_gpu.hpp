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
 *  @file    win_seq_gpu.hpp
 *  @author  Gabriele Mencagli
 *  @date    16/03/2018
 *  
 *  @brief Win_Seq_GPU node executing windowed queries on GPU
 *  
 *  @section Win_Seq_GPU (Description)
 *  
 *  This file implements the Win_Seq_GPU node able to execute windowed queries on
 *  a GPU device. The node prepares batches of input tuples sequentially on a CPU
 *  core and offloads on the GPU the parallel processing of the windows within the
 *  same batch.
 *  
 *  The template parameters tuple_t and result_t must be default constructible, with a copy
 *  constructor and a copy assignment operator, and they must provide and implement the
 *  setControlFields() and getControlFields() methods. Furthermore, in order to be copyable
 *  in a GPU-accessible memory, they must be compliant with the C++ specification for standard
 *  layout types. The third template argument win_F_t is the type of the callable object to be
 *  used for GPU processing.
 */ 

#ifndef WIN_SEQ_GPU_H
#define WIN_SEQ_GPU_H

// includes
#include<vector>
#include<string>
#include<unordered_map>
#include<math.h>
#include<ff/node.hpp>
#include<ff/multinode.hpp>
#include<meta.hpp>
#include<window.hpp>
#include<meta_gpu.hpp>
#if defined (TRACE_WINDFLOW)
    #include<stats_record.hpp>
#endif
#include<stream_archive.hpp>

namespace wf {

// CUDA KERNEL: it calls the user-defined function over all the windows within a batch
template<typename win_F_t>
__global__ void ComputeBatch_Kernel(void *input_data,
                                    size_t *start,
                                    size_t *end,
                                    uint64_t *gwids,
                                    void *results,
                                    win_F_t F,
                                    size_t batch_len,
                                    char *scratchpad_memory,
                                    size_t scratchpad_size)
{
    using tuple_t = decltype(get_tuple_t_WinGPU(F));
    using result_t = decltype(get_result_t_WinGPU(F));
    int id = threadIdx.x + blockIdx.x * blockDim.x;
    int stride = blockDim.x * gridDim.x;
    // grid-stride loop
    for (size_t i=id; i<batch_len; i+=stride) {
        if (scratchpad_size > 0) {
            F(gwids[i], ((const tuple_t *) input_data) + start[i], end[i] - start[i], &((result_t *) results)[i], &scratchpad_memory[id * scratchpad_size], scratchpad_size);
        }
        else {
            F(gwids[i], ((const tuple_t *) input_data) + start[i], end[i] - start[i], &((result_t *) results)[i], nullptr, 0);
        }
    }
}

// Win_Seq_GPU class
template<typename tuple_t, typename result_t, typename win_F_t, typename input_t>
class Win_Seq_GPU: public ff::ff_minode_t<input_t, result_t>
{
private:
    // static asserts to check that input and output types must be compliant with the standard layout constraints
    static_assert(std::is_standard_layout<tuple_t>::value,
        "WindFlow Compilation Error - input type of a GPU operator is not a standard_layout type:\n");
    static_assert(std::is_standard_layout<result_t>::value,
        "WindFlow Compilation Error - output type of a GPU operator is not a standard_layout type:\n");
    // type of the stream archive used by the Win_Seq_GPU node
    using archive_t = StreamArchive<tuple_t, std::vector<tuple_t>>;
    // iterator type for accessing tuples
    using input_iterator_t = typename std::vector<tuple_t>::iterator;
    // window type used by the Win_Seq_GPU node
    using win_t = Window<tuple_t, result_t>;
    // function type to compare two tuples
    using compare_func_t = std::function<bool(const tuple_t &, const tuple_t &)>;
    tuple_t tmp; // never used
    // key data type
    using key_t = typename std::remove_reference<decltype(std::get<0>(tmp.getControlFields()))>::type;
    // friendships with other classes in the library
    template<typename T1, typename T2, typename T3, typename T4>
    friend class Win_Farm_GPU;
    template<typename T1, typename T2, typename T3, typename T4>
    friend class Key_Farm_GPU;
    template<typename T1, typename T2, typename T3, typename T4>
    friend class Pane_Farm_GPU;
    template<typename T1, typename T2, typename T3, typename T4>
    friend class Win_MapReduce_GPU;
    // struct of a key descriptor
    struct Key_Descriptor
    {
        archive_t archive; // archive of tuples of this key
        std::vector<win_t> wins; // open windows of this key
        uint64_t emit_counter; // progressive counter (used if role is PLQ or MAP)
        uint64_t next_ids; // progressive counter (used if isRenumbering is true)
        uint64_t next_lwid; // next window to be opened of this key (lwid)
        int64_t last_lwid; // last window closed of this key (lwid)
        size_t batchedWin; // number of batched windows of the key
        std::vector<size_t> start, end; // vector of initial/final positions of each window in the current batch
        std::vector<uint64_t> gwids; // vector of gwid of the windows in the current batch
        std::vector<uint64_t> tsWin; // vector of the final timestamp of the windows in the current batch
        std::optional<tuple_t> start_tuple; // optional to the first tuple of the current batch
        std::optional<tuple_t> end_tuple; // optional to the last tuple of the current batch

        // Constructor
        Key_Descriptor(compare_func_t _compare_func,
                       uint64_t _emit_counter=0):
                       archive(_compare_func),
                       emit_counter(_emit_counter),
                       next_ids(0),
                       next_lwid(0),
                       last_lwid(-1),
                       batchedWin(0)
        {
            wins.reserve(DEFAULT_VECTOR_CAPACITY);
        }

        // move Constructor
        Key_Descriptor(Key_Descriptor &&_k):
                       archive(move(_k.archive)),
                       wins(move(_k.wins)),
                       emit_counter(_k.emit_counter),
                       next_ids(_k.next_ids),
                       next_lwid(_k.next_lwid),
                       last_lwid(_k.last_lwid),
                       batchedWin(_k.batchedWin),
                       start(_k.start),
                       end(_k.end),
                       gwids(_k.gwids),
                       tsWin(_k.tsWin),
                       start_tuple(_k.start_tuple),
                       end_tuple(_k.end_tuple) {}
    };
    // CPU variables
    compare_func_t compare_func; // function to compare two tuples
    uint64_t win_len; // window length (no. of tuples or in time units)
    uint64_t slide_len; // slide length (no. of tuples or in time units)
    uint64_t triggering_delay; // triggering delay in time units (meaningful for TB windows only)
    win_type_t winType; // window type (CB or TB)
    std::string name; // string of the unique name of the node
    WinOperatorConfig config; // configuration structure of the Win_Seq_GPU node
    role_t role; // role of the Win_Seq_GPU node
    std::unordered_map<key_t, Key_Descriptor> keyMap; // hash table that maps keys onto descriptors
    std::pair<size_t, size_t> map_indexes = std::make_pair(0, 1); // indexes useful is the role is MAP
    size_t batch_len; // length of the batch in terms of no. of windows
    size_t tuples_per_batch; // number of tuples per batch
    win_F_t win_func; // function to be executed per window
    bool isRunningKernel = false; // true if the kernel is running on the GPU, false otherwise
    Key_Descriptor *lastKeyD = nullptr; // pointer to the key descriptor of the running kernel on the GPU
    size_t ignored_tuples; // number of ignored tuples
    size_t eos_received; // number of received EOS messages
    bool terminated; // true if the replica has finished its work
    bool isRenumbering; // if true, the node assigns increasing identifiers to the input tuples (useful for count-based windows in DEFAULT mode)
    size_t scratchpad_size = 0; // size of the scratchpage memory area on the GPU (one per CUDA thread)
    // memory arrays allocated in a page-locked manner on the HOST (prefix "pinned" used for them)
    tuple_t *pinned_inputs = nullptr; // array of inputs of a batch
    result_t *pinned_results = nullptr; // array of results of a batch
    size_t *pinned_start = nullptr; // array of starting identifiers
    size_t *pinned_end = nullptr; // array of ending identifiers
    uint64_t *pinned_gwids = nullptr; // array of window identifiers
    // GPU variables
    int gpu_id; // identifier of the chosen GPU device
    size_t n_thread_block; // number of threads per block
    size_t num_blocks; // number of blocks of a GPU kernel
    cudaStream_t cudaStream; // CUDA stream used by this Win_Seq_GPU node
    // arrays allocated in the global memory of the GPU (prefix "gpu" ised for them)
    tuple_t *gpu_inputs = nullptr; // array of inputs of a batch
    result_t *gpu_results = nullptr; // array of results of a batch
    size_t *gpu_start = nullptr; // array of starting identifiers
    size_t *gpu_end = nullptr; // array of ending identifiers
    uint64_t *gpu_gwids = nullptr; // array of window identifiers
    char *gpu_scratchpad = nullptr; // scratchpage memory area (one per CUDA thread)
#if defined (TRACE_WINDFLOW)
    Stats_Record stats_record;
    double avg_td_us = 0;
    double avg_ts_us = 0;
    volatile uint64_t startTD, startTS, endTD, endTS;
#endif

    // Private Constructor
    Win_Seq_GPU(win_F_t _win_func,
                uint64_t _win_len,
                uint64_t _slide_len,
                uint64_t _triggering_delay,
                win_type_t _winType,
                size_t _batch_len,
                int _gpu_id,
                size_t _n_thread_block,
                std::string _name,
                size_t _scratchpad_size,
                WinOperatorConfig _config,
                role_t _role):
                win_func(_win_func),
                win_len(_win_len),
                slide_len(_slide_len),
                triggering_delay(_triggering_delay),
                winType(_winType),
                batch_len(_batch_len),
                gpu_id(_gpu_id),
                n_thread_block(_n_thread_block),
                name(_name),
                scratchpad_size(_scratchpad_size),
                config(_config),
                role(_role),
                ignored_tuples(0),
                eos_received(0),
                terminated(false),
                isRenumbering(false)
    {
        // check the validity of the windowing parameters
        if (_win_len == 0 || _slide_len == 0) {
            std::cerr << RED << "WindFlow Error: window length or slide in Win_Seq_GPU cannot be zero" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // check the validity of the batch length
        if (_batch_len == 0) {
            std::cerr << RED << "WindFlow Error: batch length in Win_Seq_GPU cannot be zero" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // define the compare function depending on the window type
        if (winType == win_type_t::CB) {
            compare_func = [](const tuple_t &t1, const tuple_t &t2) {
                return std::get<1>(t1.getControlFields()) < std::get<1>(t2.getControlFields());
            };
        }
        else {
            compare_func = [](const tuple_t &t1, const tuple_t &t2) {
                return std::get<2>(t1.getControlFields()) < std::get<2>(t2.getControlFields());
            };
        }
    }

    // method to set the indexes useful if role is MAP
    void setMapIndexes(size_t _first, size_t _second) {
        map_indexes.first = _first;
        map_indexes.second = _second;
    }

    // function to wait for the completion of the previous kernel (if any) and to flush its results
    void waitAndFlush()
    {
        if (isRunningKernel) {
            assert(lastKeyD != nullptr);
            gpuErrChk(cudaStreamSynchronize(cudaStream));
            // transmission of results
            for (size_t i=0; i<batch_len; i++) {
                result_t *res = new result_t();
                *res = pinned_results[i];
                // special cases: role is PLQ or MAP
                if (role == role_t::MAP) {
                    res->setControlFields(std::get<0>(res->getControlFields()), lastKeyD->emit_counter, std::get<2>(res->getControlFields()));
                    lastKeyD->emit_counter += map_indexes.second;
                }
                else if (role == role_t::PLQ) {
                    auto key = std::get<0>(res->getControlFields());
                    size_t old_hashcode = std::hash<decltype(key)>()(key); // compute the hashcode of the key
                    uint64_t new_id = ((config.id_inner - (old_hashcode % config.n_inner) + config.n_inner) % config.n_inner) + (lastKeyD->emit_counter * config.n_inner);
                    res->setControlFields(std::get<0>(res->getControlFields()), new_id, std::get<2>(res->getControlFields()));
                    lastKeyD->emit_counter++;
                }
                this->ff_send_out(res);
#if defined (TRACE_WINDFLOW)
                stats_record.outputs_sent++;
                stats_record.bytes_sent += sizeof(result_t);
#endif
            }
            isRunningKernel = false;
            lastKeyD = nullptr;
        }
    }

public:
    // Constructor I
    Win_Seq_GPU(win_F_t _win_func,
                uint64_t _win_len,
                uint64_t _slide_len,
                uint64_t _triggering_delay,
                win_type_t _winType,
                size_t _batch_len,
                int _gpu_id,
                size_t _n_thread_block,
                std::string _name,
                size_t _scratchpad_size):
                Win_Seq_GPU(_win_func, _win_len, _slide_len, _triggering_delay, _winType, _batch_len, _gpu_id, _n_thread_block, _name, _scratchpad_size, WinOperatorConfig(0, 1, _slide_len, 0, 1, _slide_len), role_t::SEQ) {}

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
        int numSMs = 0; // number of SMs in the GPU
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
        // compute the number of blocks to be used by all the kernel invokations
        num_blocks = std::min((int) ceil((double) batch_len / n_thread_block), 32 * numSMs); // at most 32 blocks per SM
        // create the CUDA stream
        gpuErrChk(cudaStreamCreate(&cudaStream));
        // initialization with count-based windows
        if (winType == win_type_t::CB) {
            // compute the fixed number of tuples per batch
            if (slide_len <= win_len) { // sliding or tumbling windows
                tuples_per_batch = (batch_len - 1) * slide_len + win_len;
            }
            else { // hopping windows
                tuples_per_batch = win_len * batch_len;
            }
            // allocate the gpu_inputs array on GPU
            gpuErrChk(cudaMalloc(&gpu_inputs, tuples_per_batch * sizeof(tuple_t)));
            // allocate the pinned_inputs array on HOST
            gpuErrChk(cudaMallocHost(&pinned_inputs, tuples_per_batch * sizeof(tuple_t)));
        }
        // initialization with time-based windows
        else {
            tuples_per_batch = DEFAULT_BATCH_SIZE_TB;
            // allocate the gpu_inputs array on GPU
            gpuErrChk(cudaMalloc(&gpu_inputs, tuples_per_batch * sizeof(tuple_t)));
            // allocate the pinned_inputs array on HOST
            gpuErrChk(cudaMallocHost(&pinned_inputs, tuples_per_batch * sizeof(tuple_t)));
        }
        // allocate the gpu_start array on GPU
        gpuErrChk(cudaMalloc(&gpu_start, batch_len * sizeof(size_t)));
        // allocate the pinned_start array on HOST
        gpuErrChk(cudaMallocHost(&pinned_start, batch_len * sizeof(size_t)));
        // allocate the gpu_end array on GPU
        gpuErrChk(cudaMalloc(&gpu_end, batch_len * sizeof(size_t)));
        // allocate the pinned_end array on HOST
        gpuErrChk(cudaMallocHost(&pinned_end, batch_len * sizeof(size_t)));
        // allocate the gpu_gwids array on GPU
        gpuErrChk(cudaMalloc(&gpu_gwids, batch_len * sizeof(uint64_t)));
        // allocate the pinned_gwids array on HOST
        gpuErrChk(cudaMallocHost(&pinned_gwids, batch_len * sizeof(uint64_t)));
        // allocate the gpu_results array on GPU
        gpuErrChk(cudaMalloc(&gpu_results, batch_len * sizeof(result_t)));
        // allocate the pinned_results array on HOST
        gpuErrChk(cudaMallocHost(&pinned_results, batch_len * sizeof(result_t)));
        // allocate the gpu_scratchpad arrat on the GPU (if required)
        if (scratchpad_size > 0) {
            gpuErrChk(cudaMalloc(&gpu_scratchpad, (num_blocks * n_thread_block) * scratchpad_size));  // scratchpad_memory
        }
#if defined (TRACE_WINDFLOW)
        stats_record = Stats_Record(name, std::to_string(this->get_my_id()), true, true);
#endif
        return 0;
    }

    // svc method (utilized by the FastFlow runtime)
    result_t *svc(input_t *wt) override
    {
#if defined (TRACE_WINDFLOW)
        startTS = current_time_nsecs();
        if (stats_record.inputs_received == 0) {
            startTD = current_time_nsecs();
        }
        stats_record.inputs_received++;
        stats_record.bytes_received += sizeof(tuple_t);
#endif
        // extract the key and id/timestamp fields from the input tuple
        tuple_t *t = extractTuple<tuple_t, input_t>(wt);
        auto key = std::get<0>(t->getControlFields()); // key
        size_t hashcode = std::hash<decltype(key)>()(key); // compute the hashcode of the key
        uint64_t id = (winType == win_type_t::CB) ? std::get<1>(t->getControlFields()) : std::get<2>(t->getControlFields()); // identifier or timestamp
        // access the descriptor of the input key
        auto it = keyMap.find(key);
        if (it == keyMap.end()) {
            // create the descriptor of that key
            keyMap.insert(std::make_pair(key, Key_Descriptor(compare_func, role == role_t::MAP ? map_indexes.first : 0)));
            it = keyMap.find(key);
        }
        Key_Descriptor &key_d = (*it).second;
        // check if isRenumbering is enabled (used for count-based windows in DEFAULT mode)
        if (isRenumbering) {
            assert(winType == win_type_t::CB);
            id = key_d.next_ids++;
            t->setControlFields(std::get<0>(t->getControlFields()), id, std::get<2>(t->getControlFields()));
        }
        // gwid of the first window of that key assigned to this Win_Seq_GPU node
        uint64_t first_gwid_key = ((config.id_inner - (hashcode % config.n_inner) + config.n_inner) % config.n_inner) * config.n_outer + (config.id_outer - (hashcode % config.n_outer) + config.n_outer) % config.n_outer;
        // initial identifer/timestamp of the keyed sub-stream arriving at this Win_Seq_GPU node
        uint64_t initial_outer = ((config.id_outer - (hashcode % config.n_outer) + config.n_outer) % config.n_outer) * config.slide_outer;
        uint64_t initial_inner = ((config.id_inner - (hashcode % config.n_inner) + config.n_inner) % config.n_inner) * config.slide_inner;
        uint64_t initial_id = initial_outer + initial_inner;
        // special cases: if role is WLQ or REDUCE
        if (role == role_t::WLQ || role == role_t::REDUCE) {
            initial_id = initial_inner;
        }
        // check if the tuple must be ignored
        uint64_t min_boundary = (key_d.last_lwid >= 0) ? win_len + (key_d.last_lwid  * slide_len) : 0;
        if (id < initial_id + min_boundary) {
            if (key_d.last_lwid >= 0) {
#if defined (TRACE_WINDFLOW)
                stats_record.inputs_ignored++;
#endif
                ignored_tuples++;
            }
            deleteTuple<tuple_t, input_t>(wt);
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
        // determine the local identifier of the last window containing t
        long last_w = -1;
        // sliding or tumbling windows
        if (win_len >= slide_len) {
            last_w = ceil(((double) id + 1 - initial_id)/((double) slide_len)) - 1;
        }
        // hopping windows
        else {
            uint64_t n = floor((double) (id-initial_id) / slide_len);
            last_w = n;
            // if the tuple does not belong to at least one window assigned to this Win_Seq_GPU node
            if ((id-initial_id < n*(slide_len)) || (id-initial_id >= (n*slide_len)+win_len)) {
                // if it is not an EOS marker, we delete the tuple immediately
                if (!isEOSMarker<tuple_t, input_t>(*wt)) {
                    // delete the received tuple
                    deleteTuple<tuple_t, input_t>(wt);
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
            }
        }
        // copy the tuple into the archive of the corresponding key
        if (!isEOSMarker<tuple_t, input_t>(*wt)) {
            (key_d.archive).insert(*t);
        }
        auto &wins = key_d.wins;
        // create all the new windows that need to be opened by the arrival of t
        for (long lwid = key_d.next_lwid; lwid <= last_w; lwid++) {
            // translate the lwid into the corresponding gwid
            uint64_t gwid = first_gwid_key + (lwid * config.n_outer * config.n_inner);
            if (winType == win_type_t::CB) {
                wins.push_back(win_t(key, lwid, gwid, Triggerer_CB(win_len, slide_len, lwid, initial_id), win_type_t::CB, win_len, slide_len));
            }
            else {
                wins.push_back(win_t(key, lwid, gwid, Triggerer_TB(win_len, slide_len, lwid, initial_id, triggering_delay), win_type_t::TB, win_len, slide_len));
            }
            key_d.next_lwid++;
        }
        // evaluate all the open windows
        size_t cnt_fired = 0;
        for (auto &win: wins) {
            // if the window is fired
            if (win.onTuple(*t) == win_event_t::FIRED) {
                key_d.batchedWin++;
                key_d.last_lwid++;
                (key_d.gwids).push_back(win.getGWID());
                (key_d.tsWin).push_back(std::get<2>((win.getResult()).getControlFields()));
                // acquire from the archive the optionals to the first and the last tuple of the window
                std::optional<tuple_t> t_s = win.getFirstTuple();
                std::optional<tuple_t> t_e = win.getLastTuple();
                // empty window
                if (!t_s) {
                    if ((key_d.start).size() == 0) {
                        (key_d.start).push_back(0);
                    }
                    else {
                        (key_d.start).push_back((key_d.start).back());
                    }
                    (key_d.end).push_back((key_d.start).back());
                }
                // non-empty window
                else {
                    if (!key_d.start_tuple) {
                        key_d.start_tuple = t_s;
                    }
                    size_t start_pos = (key_d.archive).getDistance(*(key_d.start_tuple), *t_s);
                    (key_d.start).push_back(start_pos);
                    size_t end_pos = (key_d.archive).getDistance(*(key_d.start_tuple), *t_e);
                    (key_d.end).push_back(end_pos);
                }
                // the fired window is put in batched mode
                win.setBatched();
                // a new batch is complete
                if (key_d.batchedWin == batch_len) {
                    // emit results of the previously running kernel on the GPU
                    waitAndFlush();
                    // compute the initial pointer of the batch and its size (in no. of tuples)
                    const tuple_t *dataBatch;
                    size_t size_copy;
                    if (!key_d.start_tuple) { // the batch is empty
                        dataBatch = nullptr;
                        size_copy = 0;
                    }
                    else { // the batch is not empty
                        key_d.end_tuple = t_e;
                        dataBatch = (const tuple_t *) &(*((key_d.archive).getIterator(*(key_d.start_tuple))));
                        size_copy = (key_d.archive).getDistance(*(key_d.start_tuple), *(key_d.end_tuple));
                    }
                    // prepare the pinned_results array with the timestamps of the results
                    for (size_t i=0; i<batch_len; i++) {
                        pinned_results[i].setControlFields(key, key_d.gwids[i], key_d.tsWin[i]);
                    }
                    // prepare and copy pinned_start to gpu_start
                    memcpy(pinned_start, (key_d.start).data(), batch_len * sizeof(size_t));
                    gpuErrChk(cudaMemcpyAsync(gpu_start, pinned_start, batch_len * sizeof(size_t), cudaMemcpyHostToDevice, cudaStream));
                    // prepare and copy pinned_end to gpu_end
                    memcpy(pinned_end, (key_d.end).data(), batch_len * sizeof(size_t));
                    gpuErrChk(cudaMemcpyAsync(gpu_end, pinned_end, batch_len * sizeof(size_t), cudaMemcpyHostToDevice, cudaStream));
                    // prepare and copy pinned_gwids to gpu_gwids
                    memcpy(pinned_gwids, (key_d.gwids).data(), batch_len * sizeof(uint64_t));
                    gpuErrChk(cudaMemcpyAsync(gpu_gwids, pinned_gwids, batch_len * sizeof(uint64_t), cudaMemcpyHostToDevice, cudaStream));
                    // copy pinned_results to gpu_results
                    gpuErrChk(cudaMemcpyAsync(gpu_results, pinned_results, batch_len * sizeof(result_t), cudaMemcpyHostToDevice, cudaStream));
                    // count-based windows
                    if (winType == win_type_t::CB) {
                        // prepare and copy pinned_inputs to gpu_inputs
                        memcpy(pinned_inputs, dataBatch, size_copy * sizeof(tuple_t));
                        gpuErrChk(cudaMemcpyAsync(gpu_inputs, pinned_inputs, size_copy * sizeof(tuple_t), cudaMemcpyHostToDevice, cudaStream));
                    }
                    // time-based windows
                    else {
                        // simple herustics to resize the array Bin on the GPU (if required)
                        if (size_copy > tuples_per_batch) {
                            tuples_per_batch = std::max(tuples_per_batch * 2, size_copy);
                            // reallocate gpu_inputs
                            gpuErrChk(cudaFree(gpu_inputs));
                            gpuErrChk(cudaMalloc(&gpu_inputs, tuples_per_batch * sizeof(tuple_t)));
                            // reallocate pinned_inputs
                            gpuErrChk(cudaFreeHost(pinned_inputs));
                            gpuErrChk(cudaMallocHost(&pinned_inputs, tuples_per_batch * sizeof(tuple_t)));
                        }
                        else if (size_copy < tuples_per_batch / 2) {
                            tuples_per_batch = tuples_per_batch / 2;
                            // reallocate gpu_inputs
                            gpuErrChk(cudaFree(gpu_inputs));
                            gpuErrChk(cudaMalloc(&gpu_inputs, tuples_per_batch * sizeof(tuple_t)));
                            // reallocate pinned_inputs
                            gpuErrChk(cudaFreeHost(pinned_inputs));
                            gpuErrChk(cudaMallocHost(&pinned_inputs, tuples_per_batch * sizeof(tuple_t)));
                        }
                        // prepare and copy pinned_inputs to gpu_inputs
                        memcpy(pinned_inputs, dataBatch, size_copy * sizeof(tuple_t));
                        gpuErrChk(cudaMemcpyAsync(gpu_inputs, pinned_inputs, size_copy * sizeof(tuple_t), cudaMemcpyHostToDevice, cudaStream));
                    }
#if defined (TRACE_WINDFLOW)
                    stats_record.num_kernels++;
                    stats_record.bytes_copied_hd += 2 * (batch_len * sizeof(size_t)) + batch_len * sizeof(uint64_t) + batch_len * sizeof(result_t) + size_copy * sizeof(tuple_t);
                    stats_record.bytes_copied_dh += batch_len * sizeof(result_t);
#endif
                    // call the kernel on the GPU
                    cudaError_t err;
                    ComputeBatch_Kernel<win_F_t><<<num_blocks, n_thread_block, 0, cudaStream>>>(gpu_inputs, gpu_start, gpu_end, gpu_gwids, gpu_results, win_func, batch_len, gpu_scratchpad, scratchpad_size);
                    if (err = cudaGetLastError()) {
                        std::cerr << RED << "WindFlow Error: invoking the GPU kernel (ComputeBatch_Kernel) causes error -> " << err << DEFAULT_COLOR << std::endl;
                        exit(EXIT_FAILURE);
                    }
                    // start asynchronous copy of the results from GPU to pinned_results
                    gpuErrChk(cudaMemcpyAsync(pinned_results, gpu_results, batch_len * sizeof(result_t), cudaMemcpyDeviceToHost, cudaStream));
                    // purge the archive from tuples that are no longer necessary
                    if (key_d.start_tuple) {
                        (key_d.archive).purge(*(key_d.start_tuple));
                    }
                    cnt_fired += batch_len;
                    isRunningKernel = true;
                    lastKeyD = &key_d;
                    // reset data structures for the next batch
                    key_d.batchedWin = 0;
                    (key_d.start).clear();
                    (key_d.end).clear();
                    (key_d.gwids).clear();
                    (key_d.tsWin).clear();
                    key_d.start_tuple = std::nullopt;
                    key_d.end_tuple = std::nullopt;
                }
            }
        }
        // purge all the windows of the batch
        wins.erase(wins.begin(), wins.begin() + cnt_fired);
        // delete the received tuple
        deleteTuple<tuple_t, input_t>(wt);
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

    // method to manage the EOS (utilized by the FastFlow runtime)
    void eosnotify(ssize_t id) override
    {
        eos_received++;
        // check the number of received EOS messages
        if ((eos_received != this->get_num_inchannels()) && (this->get_num_inchannels() != 0)) { // workaround due to FastFlow
            return;
        }
        // emit results of the previously running kernel on the GPU
        waitAndFlush();
        // allocate on the CPU the scratchpad_memory
        char *scratchpad_memory_cpu = (char *) malloc(sizeof(char) * scratchpad_size);
        // iterate over all the keys
        for (auto &k: keyMap) {
            auto key = k.first;
            Key_Descriptor &key_d = k.second;
            auto &wins = key_d.wins;
            // iterate over all the existing windows of the key and execute them on the CPU
            for (auto &win: wins) {
                std::optional<tuple_t> t_s = win.getFirstTuple();
                std::optional<tuple_t> t_e = win.getLastTuple();
                std::pair<input_iterator_t, input_iterator_t> its;
                result_t *out = new result_t(win.getResult());
                if (t_s) { // not-empty window
                    if (t_e) { // DELAYED or BATCHED window
                        its = (key_d.archive).getWinRange(*t_s, *t_e);
                    }
                    else { // not-FIRED window
                        its = (key_d.archive).getWinRange(*t_s);
                    }
                    // call the win_func on the CPU
                    decltype(get_tuple_t_WinGPU(win_func)) *my_input_data = (decltype(get_tuple_t_WinGPU(win_func)) *) &(*(its.first));
                    // call win_func
                    win_func(win.getGWID(), my_input_data, distance(its.first, its.second), out, scratchpad_memory_cpu, scratchpad_size);
                }
                else { // empty window
                    // call win_func
                    win_func(win.getGWID(), nullptr, 0, out, scratchpad_memory_cpu, scratchpad_size);
                }
                // special cases: role is PLQ or MAP
                if (role == role_t::MAP) {
                    out->setControlFields(k.first, (k.second).emit_counter, std::get<2>(out->getControlFields()));
                    (k.second).emit_counter += map_indexes.second;
                }
                else if (role == role_t::PLQ) {
                    size_t hashcode = std::hash<key_t>()(k.first); // compute the hashcode of the key
                    uint64_t new_id = ((config.id_inner - (hashcode % config.n_inner) + config.n_inner) % config.n_inner) + ((k.second).emit_counter * config.n_inner);
                    out->setControlFields(k.first, new_id, std::get<2>(out->getControlFields()));
                    (k.second).emit_counter++;
                }
                this->ff_send_out(out);
#if defined (TRACE_WINDFLOW)
                stats_record.outputs_sent++;
                stats_record.bytes_sent += sizeof(result_t);
#endif
            }
        }
        // deallocate the scratchpad_memory on the CPU
        free(scratchpad_memory_cpu);
        terminated = true;
#if defined (TRACE_WINDFLOW)
        stats_record.set_Terminated();
#endif
    }

    // svc_end method (utilized by the FastFlow runtime)
    void svc_end() override
    {
        // deallocate the arrays on GPU
        gpuErrChk(cudaFree(gpu_inputs));
        gpuErrChk(cudaFree(gpu_results));
        gpuErrChk(cudaFree(gpu_start));
        gpuErrChk(cudaFree(gpu_end));
        gpuErrChk(cudaFree(gpu_gwids));
        if (scratchpad_size > 0) {
            gpuErrChk(cudaFree(gpu_scratchpad));
        }
        // deallocate the arrays on HOST
        gpuErrChk(cudaFreeHost(pinned_inputs));
        gpuErrChk(cudaFreeHost(pinned_results));
        gpuErrChk(cudaFreeHost(pinned_start));
        gpuErrChk(cudaFreeHost(pinned_end));
        gpuErrChk(cudaFreeHost(pinned_gwids));
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
