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
 *  @brief Win_Seq_GPU operator executing a windowed query on a CPU+GPU system
 *  
 *  @section Win_Seq_GPU (Description)
 *  
 *  This file implements the Win_Seq_GPU operator able to execute windowed queries on
 *  a heterogeneous system (CPU+GPU). The operator prepares batches of input tuples
 *  sequentially on a CPU core and offloads on the GPU the parallel processing of the
 *  windows within each batch.
 *  
 *  The template parameters tuple_t and result_t must be default constructible, with
 *  a copy constructor and copy assignment operator, and they must provide and implement
 *  the setControlFields() and getControlFields() methods. The third template argument
 *  win_F_t is the type of the callable object to be used for GPU processing.
 */ 

#ifndef WIN_SEQ_GPU_H
#define WIN_SEQ_GPU_H

/// includes
#include <vector>
#include <string>
#include <unordered_map>
#include <math.h>
#include <ff/node.hpp>
#include <ff/multinode.hpp>
#include <window.hpp>
#include <meta.hpp>
#include <stream_archive.hpp>

namespace wf {

//@cond DOXY_IGNORE

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
    using input_t = decltype(get_tuple_t(F));
    using output_t = decltype(get_result_t(F));
    int id = threadIdx.x + blockIdx.x * blockDim.x;
    int stride = blockDim.x * gridDim.x;
    // grid-stride loop over the windows within the batch
    for (size_t i=id; i<batch_len; i+=stride) {
        if (scratchpad_size > 0) {
            F(gwids[i], ((const input_t *) input_data) + start[i], end[i] - start[i], &((output_t *) results)[i], &scratchpad_memory[id * scratchpad_size], scratchpad_size);
        }
        else {
            F(gwids[i], ((const input_t *) input_data) + start[i], end[i] - start[i], &((output_t *) results)[i], nullptr, 0);
        }
    }
}

//@endcond

/** 
 *  \class Win_Seq_GPU
 *  
 *  \brief Win_Seq_GPU operator executing a windowed query on a CPU+GPU system
 *  
 *  This class implements the Win_Seq_GPU operator executing windowed queries on a heterogeneous
 *  system (CPU+GPU). The operator prepares batches of input tuples on a CPU core sequentially,
 *  and offloads the processing of all the windows within a batch on the GPU.
 */ 
template<typename tuple_t, typename result_t, typename win_F_t, typename input_t>
class Win_Seq_GPU: public ff::ff_minode_t<input_t, result_t>
{
private:
    // iterator type for accessing tuples
    using input_iterator_t = typename std::vector<tuple_t>::iterator;
    // type of the stream archive used by the Win_Seq_GPU operator
    using archive_t = StreamArchive<tuple_t, std::vector<tuple_t>>;
    // window type used by the Win_Seq_GPU operator
    using win_t = Window<tuple_t, result_t>;
    // function type to compare two tuples
    using compare_func_t = std::function<bool(const tuple_t &, const tuple_t &)>;
    tuple_t tmp; // never used
    // key data type
    using key_t = typename std::remove_reference<decltype(std::get<0>(tmp.getControlFields()))>::type;
    // friendships with other classes in the library
    template<typename T1, typename T2, typename T3, typename T4>
    friend class Win_Farm_GPU;
    template<typename T1, typename T2, typename T3>
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
    std::string name; // std::string of the unique name of the operator
    OperatorConfig config; // configuration structure of the Win_Seq_GPU operator
    role_t role; // role of the Win_Seq_GPU
    std::unordered_map<key_t, Key_Descriptor> keyMap; // hash table that maps a descriptor for each key
    std::pair<size_t, size_t> map_indexes = std::make_pair(0, 1); // indexes useful is the role is MAP
    size_t batch_len; // length of the batch in terms of no. of windows
    size_t tuples_per_batch; // number of tuples per batch (only for CB windows)
    win_F_t win_func; // function to be executed per window
    result_t *host_results = nullptr; // array of results copied back from the GPU
    bool isRunningKernel = false; // true if the kernel is running on the GPU, false otherwise
    Key_Descriptor *lastKeyD = nullptr; // pointer to the key descriptor of the running kernel on the GPU
    // GPU variables
    size_t n_thread_block; // number of threads per block
    size_t num_blocks; // number of blocks of a GPU kernel
    cudaStream_t cudaStream; // CUDA stream used by this Win_Seq_GPU
    tuple_t *Bin = nullptr; // array of tuples in the batch (allocated on the GPU)
    result_t *Bout = nullptr; // array of results of the batch (allocated on the GPU)
    size_t *gpu_start, *gpu_end = nullptr; // arrays of the starting/ending positions of each window in the batch (allocated on the GPU)
    uint64_t *gpu_gwids = nullptr; // array of the gwids of the windows in the microbatch (allocated on the GPU)
    size_t scratchpad_size = 0; // size of the scratchpage memory area on the GPU (one per CUDA thread)
    char *scratchpad_memory = nullptr; // scratchpage memory area (allocated on the GPU, one per CUDA thread)
    size_t dropped_tuples; // number of dropped tuples
    size_t eos_received; // number of received EOS messages
#if defined(TRACE_WINDFLOW)
    bool isTriggering = false;
    unsigned long rcvTuples = 0;
    unsigned long rcvTuplesTriggering = 0; // a triggering tuple activates a new batch
    double avg_td_us = 0;
    double avg_ts_us = 0;
    double avg_ts_triggering_us = 0;
    double avg_ts_non_triggering_us = 0;
    volatile unsigned long startTD, startTS, endTD, endTS;
    std::ofstream *logfile = nullptr;
#endif

    // Private Constructor
    Win_Seq_GPU(win_F_t _win_func,
                uint64_t _win_len,
                uint64_t _slide_len,
                uint64_t _triggering_delay,
                win_type_t _winType,
                size_t _batch_len,
                size_t _n_thread_block,
                std::string _name,
                size_t _scratchpad_size,
                OperatorConfig _config,
                role_t _role):
                win_func(_win_func),
                win_len(_win_len),
                slide_len(_slide_len),
                triggering_delay(_triggering_delay),
                winType(_winType),
                batch_len(_batch_len),
                n_thread_block(_n_thread_block),
                name(_name),
                scratchpad_size(_scratchpad_size),
                config(_config),
                role(_role),
                dropped_tuples(0),
                eos_received(0)
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
        // create the CUDA stream
        if (cudaStreamCreate(&cudaStream) != cudaSuccess) {
            std::cerr << RED << "WindFlow Error: cudaStreamCreate() returns error code" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // define the compare function depending on the window type
        if (winType == CB) {
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
    inline void waitAndFlush()
    {
        if (isRunningKernel) {
            assert(lastKeyD != nullptr);
            gpuErrChk(cudaStreamSynchronize(cudaStream));
            // transmission of results
            for (size_t i=0; i<batch_len; i++) {
                result_t *res = new result_t();
                *res = host_results[i];
                // special cases: role is PLQ or MAP
                if (role == MAP) {
                    res->setControlFields(std::get<0>(res->getControlFields()), lastKeyD->emit_counter, std::get<2>(res->getControlFields()));
                    lastKeyD->emit_counter += map_indexes.second;
                }
                else if (role == PLQ) {
                    auto key = std::get<0>(res->getControlFields());
                    size_t old_hashcode = std::hash<decltype(key)>()(key); // compute the hashcode of the key
                    uint64_t new_id = ((config.id_inner - (old_hashcode % config.n_inner) + config.n_inner) % config.n_inner) + (lastKeyD->emit_counter * config.n_inner);
                    res->setControlFields(std::get<0>(res->getControlFields()), new_id, std::get<2>(res->getControlFields()));
                    lastKeyD->emit_counter++;
                }
                this->ff_send_out(res);
            }
            isRunningKernel = false;
            lastKeyD = nullptr;
        }
    }

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _win_func the non-incremental window processing function (__host__ __device__ function)
     *  \param _win_len window length (in no. of tuples or in time units)
     *  \param _slide_len slide length (in no. of tuples or in time units)
     *  \param _triggering_delay (triggering delay in time units, meaningful for TB windows only otherwise it must be 0)
     *  \param _winType window type (count-based CB or time-based TB)
     *  \param _batch_len no. of windows in a batch
     *  \param _n_thread_block number of threads per block
     *  \param _name string with the unique name of the operator
     *  \param _scratchpad_size size in bytes of the scratchpad area local of a CUDA thread (pre-allocated on the global memory of the GPU)
     */ 
    Win_Seq_GPU(win_F_t _win_func,
                uint64_t _win_len,
                uint64_t _slide_len,
                uint64_t _triggering_delay,
                win_type_t _winType,
                size_t _batch_len,
                size_t _n_thread_block,
                std::string _name,
                size_t _scratchpad_size):
                Win_Seq_GPU(_win_func, _win_len, _slide_len, _triggering_delay, _winType, _batch_len, _n_thread_block, _name, _scratchpad_size, OperatorConfig(0, 1, _slide_len, 0, 1, _slide_len), SEQ)
    {}

//@cond DOXY_IGNORE

    // svc_init method (utilized by the FastFlow runtime)
    int svc_init()
    {
        int deviceID = 0; // only one GPU device supported
        int numSMs = 0; // number of SMs in the GPU
        int max_thread_block = 0; // maximum number of threads per block
        gpuErrChk(cudaDeviceGetAttribute(&numSMs, cudaDevAttrMultiProcessorCount, deviceID));
        assert(numSMs>0);
        gpuErrChk(cudaDeviceGetAttribute(&max_thread_block, cudaDevAttrMaxThreadsPerBlock, deviceID));
        assert(max_thread_block>0);
        // check the number of threads per block limit
        if (max_thread_block < n_thread_block) {
            std::cerr << RED << "WindFlow Error: number of threads per block exceeds the limit of the GPU device (" << max_thread_block << ")" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        num_blocks = std::min((int) ceil((double) batch_len / n_thread_block), 32 * numSMs); // at most 32 blocks per SM
        // initialization with count-based windows
        if (winType == CB) {
            // compute the fixed number of tuples per batch
            if (slide_len <= win_len) // sliding or tumbling windows
                tuples_per_batch = (batch_len - 1) * slide_len + win_len;
            else // hopping windows
                tuples_per_batch = win_len * batch_len;
            // allocate Bin (of fixed size) on the GPU
            gpuErrChk(cudaMalloc((tuple_t **) &Bin, tuples_per_batch * sizeof(tuple_t)));      // Bin
        }
        // initialization with time-based windows
        else {
            tuples_per_batch = DEFAULT_BATCH_SIZE_TB;
            // allocate Bin (with default size) on the GPU
            gpuErrChk(cudaMalloc((tuple_t **) &Bin, tuples_per_batch * sizeof(tuple_t))); // Bin
        }
        // allocate the other arrays on the GPU/HOST
        gpuErrChk(cudaMalloc((size_t **) &gpu_start, batch_len * sizeof(size_t)));             // gpu_start
        gpuErrChk(cudaMalloc((size_t **) &gpu_end, batch_len * sizeof(size_t)));               // gpu_end
        gpuErrChk(cudaMalloc((uint64_t **) &gpu_gwids, batch_len * sizeof(uint64_t)));         // gpu_gwids
        gpuErrChk(cudaMalloc((result_t **) &Bout, batch_len * sizeof(result_t)));              // Bout
        gpuErrChk(cudaMallocHost((void **) &host_results, batch_len * sizeof(result_t)));      // host_results
        // allocate the scratchpad on the GPU (if required)
        if (scratchpad_size > 0) {
            gpuErrChk(cudaMalloc((char **) &scratchpad_memory, (num_blocks * n_thread_block) * scratchpad_size));  // scratchpad_memory
        }
#if defined(TRACE_WINDFLOW)
        logfile = new std::ofstream();
        name += "_" + std::to_string(this->get_my_id()) + "_" + std::to_string(getpid()) + ".log";
#if defined(LOG_DIR)
        std::string filename = std::string(STRINGIFY(LOG_DIR)) + "/" + name;
        std::string log_dir = std::string(STRINGIFY(LOG_DIR));
#else
        std::string filename = "log/" + name;
        std::string log_dir = std::string("log");
#endif
        // create the log directory
        if (mkdir(log_dir.c_str(), 0777) != 0) {
            struct stat st;
            if((stat(log_dir.c_str(), &st) != 0) || !S_ISDIR(st.st_mode)) {
                std::cerr << RED << "WindFlow Error: directory for log files cannot be created" << DEFAULT_COLOR << std::endl;
                exit(EXIT_FAILURE);
            }
        }
        logfile->open(filename);
#endif
        return 0;
    }

    // svc method (utilized by the FastFlow runtime)
    result_t *svc(input_t *wt)
    {
#if defined(TRACE_WINDFLOW)
        startTS = current_time_nsecs();
        if (rcvTuples == 0)
            startTD = current_time_nsecs();
        rcvTuples++;
#endif
        // extract the key and id/timestamp fields from the input tuple
        tuple_t *t = extractTuple<tuple_t, input_t>(wt);
        auto key = std::get<0>(t->getControlFields()); // key
        size_t hashcode = std::hash<decltype(key)>()(key); // compute the hashcode of the key
        uint64_t id = (winType == CB) ? std::get<1>(t->getControlFields()) : std::get<2>(t->getControlFields()); // identifier or timestamp
        // access the descriptor of the input key
        auto it = keyMap.find(key);
        if (it == keyMap.end()) {
            // create the descriptor of that key
            keyMap.insert(std::make_pair(key, Key_Descriptor(compare_func, role == MAP ? map_indexes.first : 0)));
            it = keyMap.find(key);
        }
        Key_Descriptor &key_d = (*it).second;
        // gwid of the first window of that key assigned to this Win_Seq_GPU
        uint64_t first_gwid_key = ((config.id_inner - (hashcode % config.n_inner) + config.n_inner) % config.n_inner) * config.n_outer + (config.id_outer - (hashcode % config.n_outer) + config.n_outer) % config.n_outer;
        // initial identifer/timestamp of the keyed sub-stream arriving at this Win_Seq_GPU
        uint64_t initial_outer = ((config.id_outer - (hashcode % config.n_outer) + config.n_outer) % config.n_outer) * config.slide_outer;
        uint64_t initial_inner = ((config.id_inner - (hashcode % config.n_inner) + config.n_inner) % config.n_inner) * config.slide_inner;
        uint64_t initial_id = initial_outer + initial_inner;
        // special cases: if role is WLQ or REDUCE
        if (role == WLQ || role == REDUCE)
            initial_id = initial_inner;
        // check if the tuple must be dropped
        uint64_t min_boundary = (key_d.last_lwid >= 0) ? win_len + (key_d.last_lwid  * slide_len) : 0;
        if (id < initial_id + min_boundary) {
            if (key_d.last_lwid >= 0)
                dropped_tuples++;
            deleteTuple<tuple_t, input_t>(wt);
            return this->GO_ON;
        }
        // determine the local identifier of the last window containing t
        long last_w = -1;
        // sliding or tumbling windows
        if (win_len >= slide_len)
            last_w = ceil(((double) id + 1 - initial_id)/((double) slide_len)) - 1;
        // hopping windows
        else {
            uint64_t n = floor((double) (id-initial_id) / slide_len);
            last_w = n;
            // if the tuple does not belong to at least one window assigned to this Win_Seq
            if ((id-initial_id < n*(slide_len)) || (id-initial_id >= (n*slide_len)+win_len)) {
                // if it is not an EOS marker, we delete the tuple immediately
                if (!isEOSMarker<tuple_t, input_t>(*wt)) {
                    // delete the received tuple
                    deleteTuple<tuple_t, input_t>(wt);
                    return this->GO_ON;
                }
            }
        }
        // copy the tuple into the archive of the corresponding key
        if (!isEOSMarker<tuple_t, input_t>(*wt))
            (key_d.archive).insert(*t);
        auto &wins = key_d.wins;
        // create all the new windows that need to be opened by the arrival of t
        for (long lwid = key_d.next_lwid; lwid <= last_w; lwid++) {
            // translate the lwid into the corresponding gwid
            uint64_t gwid = first_gwid_key + (lwid * config.n_outer * config.n_inner);
            if (winType == CB)
                wins.push_back(win_t(key, lwid, gwid, Triggerer_CB(win_len, slide_len, lwid, initial_id), CB, win_len, slide_len));
            else
                wins.push_back(win_t(key, lwid, gwid, Triggerer_TB(win_len, slide_len, lwid, initial_id, triggering_delay), TB, win_len, slide_len));
            key_d.next_lwid++;
        }
        // evaluate all the open windows
        size_t cnt_fired = 0;
        for (auto &win: wins) {
            // if the window is fired
            if (win.onTuple(*t) == FIRED) {
                key_d.batchedWin++;
                key_d.last_lwid++;
                (key_d.gwids).push_back(win.getGWID());
                (key_d.tsWin).push_back(std::get<2>((win.getResult()).getControlFields()));
                // acquire from the archive the optionals to the first and the last tuple of the window
                std::optional<tuple_t> t_s = win.getFirstTuple();
                std::optional<tuple_t> t_e = win.getLastTuple();
                // empty window
                if (!t_s) {
                    if ((key_d.start).size() == 0)
                        (key_d.start).push_back(0);
                    else
                        (key_d.start).push_back((key_d.start).back());
                    (key_d.end).push_back((key_d.start).back());
                }
                // non-empty window
                else {
                    if (!key_d.start_tuple)
                        key_d.start_tuple = t_s;
                    size_t start_pos = (key_d.archive).getDistance(*(key_d.start_tuple), *t_s);
                    (key_d.start).push_back(start_pos);
                    size_t end_pos = (key_d.archive).getDistance(*(key_d.start_tuple), *t_e);
                    (key_d.end).push_back(end_pos);
                }
                // the fired window is put in batched mode
                win.setBatched();
                // a new batch is complete
                if (key_d.batchedWin == batch_len) {
#if defined(TRACE_WINDFLOW)
                    if (!isTriggering) {
                        rcvTuplesTriggering++;
                    }
                    isTriggering = true;
#endif
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
                    // prepare the host_results
                    for (size_t i=0; i < batch_len; i++) {
                        host_results[i].setControlFields(key, key_d.gwids[i], key_d.tsWin[i]);
                    }
                    // copy of the arrays on the GPU
                    gpuErrChk(cudaMemcpyAsync(gpu_start, (key_d.start).data(), batch_len * sizeof(size_t), cudaMemcpyHostToDevice, cudaStream));
                    gpuErrChk(cudaMemcpyAsync(gpu_end, (key_d.end).data(), batch_len * sizeof(size_t), cudaMemcpyHostToDevice, cudaStream));
                    gpuErrChk(cudaMemcpyAsync(gpu_gwids, (key_d.gwids).data(), batch_len * sizeof(uint64_t), cudaMemcpyHostToDevice, cudaStream));
                    gpuErrChk(cudaMemcpyAsync(Bout, host_results, batch_len * sizeof(result_t), cudaMemcpyHostToDevice, cudaStream));
                    // count-based windows
                    if (winType == CB) {
                        gpuErrChk(cudaMemcpyAsync(Bin, dataBatch, size_copy * sizeof(tuple_t), cudaMemcpyHostToDevice, cudaStream));
                    }
                    // time-based windows
                    else {
                        // simple herustics to resize the array Bin on the GPU (if required)
                        if (size_copy > tuples_per_batch) {
                            tuples_per_batch = std::max(tuples_per_batch * 2, size_copy);
                            // deallocate/allocate Bin
                            gpuErrChk(cudaFree(Bin));
                            gpuErrChk(cudaMalloc((tuple_t **) &Bin, tuples_per_batch * sizeof(tuple_t)));     // Bin
                        }
                        else if (size_copy < tuples_per_batch / 2) {
                            tuples_per_batch = tuples_per_batch / 2;
                            // deallocate/allocate Bin
                            gpuErrChk(cudaFree(Bin));
                            gpuErrChk(cudaMalloc((tuple_t **) &Bin, tuples_per_batch * sizeof(tuple_t)));     // Bin
                        }
                        // copy of the array Bin on the GPU
                        gpuErrChk(cudaMemcpyAsync(Bin, dataBatch, size_copy * sizeof(tuple_t), cudaMemcpyHostToDevice, cudaStream));
                    }
                    // call the kernel on the GPU
                    cudaError_t err;
                    ComputeBatch_Kernel<win_F_t><<<num_blocks, n_thread_block, 0, cudaStream>>>(Bin, gpu_start, gpu_end, gpu_gwids, Bout, win_func, batch_len, scratchpad_memory, scratchpad_size);
                    if (err = cudaGetLastError()) {
                        std::cerr << RED << "WindFlow Error: invoking the GPU kernel (ComputeBatch_Kernel) causes error -> " << err << DEFAULT_COLOR << std::endl;
                        exit(EXIT_FAILURE);
                    }
                    gpuErrChk(cudaMemcpyAsync(host_results, Bout, batch_len * sizeof(result_t), cudaMemcpyDeviceToHost, cudaStream));
                    // purge the archive from tuples that are no longer necessary
                    if (key_d.start_tuple)
                        (key_d.archive).purge(*(key_d.start_tuple));
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
#if defined(TRACE_WINDFLOW)
        endTS = current_time_nsecs();
        endTD = current_time_nsecs();
        double elapsedTS_us = ((double) (endTS - startTS)) / 1000;
        avg_ts_us += (1.0 / rcvTuples) * (elapsedTS_us - avg_ts_us);
        if (isTriggering)
            avg_ts_triggering_us += (1.0 / rcvTuplesTriggering) * (elapsedTS_us - avg_ts_triggering_us);
        else
            avg_ts_non_triggering_us += (1.0 / (rcvTuples - rcvTuplesTriggering)) * (elapsedTS_us - avg_ts_non_triggering_us);
        isTriggering = false;
        double elapsedTD_us = ((double) (endTD - startTD)) / 1000;
        avg_td_us += (1.0 / rcvTuples) * (elapsedTD_us - avg_td_us);
        startTD = current_time_nsecs();
#endif
        return this->GO_ON;
    }

    // method to manage the EOS (utilized by the FastFlow runtime)
    void eosnotify(ssize_t id)
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
                    if (t_e) // DELAYED or BATCHED window
                        its = (key_d.archive).getWinRange(*t_s, *t_e);
                    else // not-FIRED window
                        its = (key_d.archive).getWinRange(*t_s);
                    // call the win_func on the CPU
                    decltype(get_tuple_t(win_func)) *my_input_data = (decltype(get_tuple_t(win_func)) *) &(*(its.first));
                    // call win_func
                    win_func(win.getGWID(), my_input_data, distance(its.first, its.second), out, scratchpad_memory_cpu, scratchpad_size);
                }
                else {// empty window
                    // call win_func
                    win_func(win.getGWID(), nullptr, 0, out, scratchpad_memory_cpu, scratchpad_size);
                }
                // special cases: role is PLQ or MAP
                if (role == MAP) {
                    out->setControlFields(k.first, (k.second).emit_counter, std::get<2>(out->getControlFields()));
                    (k.second).emit_counter += map_indexes.second;
                }
                else if (role == PLQ) {
                    size_t hashcode = std::hash<key_t>()(k.first); // compute the hashcode of the key
                    uint64_t new_id = ((config.id_inner - (hashcode % config.n_inner) + config.n_inner) % config.n_inner) + ((k.second).emit_counter * config.n_inner);
                    out->setControlFields(k.first, new_id, std::get<2>(out->getControlFields()));
                    (k.second).emit_counter++;
                }
                this->ff_send_out(out);
            }
        }
        // deallocate the scratchpad_memory on the CPU
        free(scratchpad_memory_cpu);
    }

    // svc_end method (utilized by the FastFlow runtime)
    void svc_end()
    {
        // deallocate data structures allocated on the GPU
        cudaFree(Bin);
        cudaFree(gpu_start);
        cudaFree(gpu_end);
        cudaFree(gpu_gwids);
        cudaFree(Bout);
        if (scratchpad_size > 0)
            cudaFree(scratchpad_memory);
        // deallocate data structures allocated on the CPU
        cudaFreeHost(host_results);
        // destroy the CUDA stream
        cudaStreamDestroy(cudaStream);
#if defined(TRACE_WINDFLOW)
        std::ostringstream stream;
        stream << "************************************LOG************************************\n";
        stream << "No. of received tuples: " << rcvTuples << "\n";
        stream << "No. of received tuples (triggering): " << rcvTuplesTriggering << "\n";
        stream << "Average service time: " << avg_ts_us << " usec \n";
        stream << "Average service time (triggering): " << avg_ts_triggering_us << " usec \n";
        stream << "Average service time (non triggering): " << avg_ts_non_triggering_us << " usec \n";
        stream << "Average inter-departure time: " << avg_td_us << " usec \n";
        stream << "Dropped tuples: " << dropped_tuples << "\n";
        stream << "***************************************************************************\n";
        *logfile << stream.str();
        logfile->close();
        delete logfile;
#endif
    }

//@endcond

    /** 
     *  \brief Get the window type (CB or TB) utilized by the operator
     *  \return adopted windowing semantics (count- or time-based)
     */
    win_type_t getWinType() const
    {
        return winType;
    }

    /** 
     *  \brief Get the number of dropped tuples by the Win_Seq_GPU
     *  \return number of tuples dropped during the processing by the Win_Seq_GPU
     */ 
    size_t getNumDroppedTuples() const
    {
        return dropped_tuples;
    }

    /// Method to start the operator execution asynchronously
    virtual int run(bool)
    {
        return ff::ff_minode::run();
    }

    /// Method to wait the operator termination
    virtual int wait()
    {
        return ff::ff_minode::wait();
    }
};

} // namespace wf

#endif
