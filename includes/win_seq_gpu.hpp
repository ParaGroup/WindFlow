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
 *  @version 1.0
 *  
 *  @brief Win_Seq_GPU pattern executing windowed queries on a heterogeneous system (CPU+GPU)
 *  
 *  @section DESCRIPTION
 *  
 *  This file implements the Win_Seq_GPU pattern able to execute windowed queries on a heterogeneous
 *  system (CPU+GPU). The pattern prepares batches of input tuples sequentially on a CPU core and
 *  offloads on the GPU the parallel processing of the windows within each batch.
 */ 

#ifndef WIN_SEQ_GPU_H
#define WIN_SEQ_GPU_H

// includes
#include <vector>
#include <string>
#include <unordered_map>
#include <math.h>
#include <ff/node.hpp>
#include <window.hpp>
#include <builders.hpp>
#include <stream_archive.hpp>

using namespace ff;

// CUDA KERNEL: it calls the user-defined function over the windows within a micro-batch
template<typename win_F_t>
__global__ void kernelBatch(size_t key, void *input_data, size_t *start, size_t *end,
                            size_t *gwids, void *results, win_F_t F, size_t batch_len,
                            char *scratchpad_memory, size_t scratchpad_size)
{
    using input_t = decltype(get_tuple_t(F));
    using output_t = decltype(get_result_t(F));
    int id = threadIdx.x + blockIdx.x * blockDim.x;
    if (id < batch_len) {
        if (scratchpad_size > 0)
            F(key, gwids[id], ((input_t *) input_data) + start[id], &((output_t *) results)[id], end[id] - start[id], &scratchpad_memory[id * scratchpad_size]);
        else
            F(key, gwids[id], ((input_t *) input_data) + start[id], &((output_t *) results)[id], end[id] - start[id], nullptr);
    }
}

// assert function on GPU
inline void gpuAssert(cudaError_t code, const char *file, int line, bool abort=false)
{
    if (code != cudaSuccess) {
        fprintf(stderr, "GPUassert: %s %s %d\n", cudaGetErrorString(code), file, line);
        if (abort) exit(code);
    }
}

/** 
 *  \class Win_Seq_GPU
 *  
 *  \brief Win_Seq_GPU pattern executing windowed queries on heterogeneous system (CPU+GPU)
 *  
 *  This class implements the Win_Seq_GPU pattern executing windowed queries on a heterogeneous
 *  system (CPU+GPU). The pattern prepares batches of input tuples on a CPU core sequentially,
 *  and offloads the processing of all the windows within a batch on the GPU. The pattern class
 *  has four template arguments. The first is the type of the input tuples. It must be copyable
 *  and providing the getInfo() and setinfo() methods. The second is the type of the window results.
 *  It must have a default constructor and the getInfo() and setInfo() methods. The first and the
 *  second types must be POD C++ types (Plain Old Data). The third template argument is the type
 *  of the function to process per window. It must be declared in order to be executable both on
 *  the device (GPU) and on the CPU. The last template argument is used by the WindFlow run-time
 *  system and should never be utilized by the high-level programmer.
 */ 
template<typename tuple_t, typename result_t, typename win_F_t, typename input_t>
class Win_Seq_GPU: public ff_node_t<input_t, result_t>
{
private:
    // const iterator type for accessing tuples
    using const_input_iterator_t = typename vector<tuple_t>::const_iterator;
    // type of the stream archive used by the Win_Seq_GPU pattern
    using archive_t = StreamArchive<tuple_t, vector<tuple_t>>;
    // window type used by the Win_Seq_GPU pattern
    using win_t = Window<tuple_t, result_t>;
    // function type to compare two tuples
    using f_compare_t = function<bool(const tuple_t &, const tuple_t &)>;
    /// friendships with other classes in the library
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
        vector<win_t> wins; // open windows of this key
        size_t emit_counter; // progressive counter (used if role is PLQ or MAP)
        size_t rcv_counter; // number of tuples received of this key
        tuple_t last_tuple; // copy of the last tuple received of this key
        size_t next_lwid; // next window to be opened of this key (lwid)
        size_t batchedWin; // number of batched windows of the key
        vector<size_t> start, end; // vectors of initial/final positions of each window in the current micro-batch
        vector<size_t> gwids; // vector of gwid of the windows in the current micro-batch
        optional<tuple_t> start_tuple; // optional to the first tuple of the current micro-batch

        // constructor
        Key_Descriptor(f_compare_t _compare, size_t _emit_counter=0):
                       archive(_compare),
                       emit_counter(_emit_counter),
                       rcv_counter(0),
                       next_lwid(0),
                       batchedWin(0)
        {
            wins.reserve(DEFAULT_VECTOR_CAPACITY);
        }

        // move constructor
        Key_Descriptor(Key_Descriptor &&_k):
                       archive(move(_k.archive)),
                       wins(move(_k.wins)),
                       emit_counter(_k.emit_counter),
                       rcv_counter(_k.rcv_counter),
                       last_tuple(_k.last_tuple),
                       next_lwid(_k.next_lwid),
                       batchedWin(_k.batchedWin),
                       start(_k.start),
                       end(_k.end),
                       gwids(_k.gwids),
                       start_tuple(_k.start_tuple) {}

        // destructor
       ~Key_Descriptor() {}
    };
    // CPU variables
    f_compare_t compare; // function to compare two tuples
    uint64_t win_len; // window length (no. of tuples or in time units)
    uint64_t slide_len; // slide length (no. of tuples or in time units)
    win_type_t winType; // window type (CB or TB)
    string name; // string of the unique name of the pattern
    PatternConfig config; // configuration structure of the Win_Seq_GPU pattern
    role_t role; // role of the Win_Seq_GPU instance
    unordered_map<size_t, Key_Descriptor> keyMap; // hash table that maps a descriptor for each key
    pair<size_t, size_t> map_indexes = make_pair(0, 1); // indexes useful is the role is MAP
    size_t batch_len; // length of the micro-batch in terms of no. of windows (i.e. 1 window mapped onto 1 CUDA thread)
    size_t n_thread_block; // number of threads per block
    size_t tuples_per_batch; // number of tuples per batch (only for CB windows)
    win_F_t winFunction; // function to be executed per window
    result_t *host_results; // array of results copied back from the GPU
    // GPU variables
    cudaStream_t cudaStream; // CUDA stream used by this Win_Seq_GPU instance
    size_t no_thread_block; // number of CUDA threads per block
    tuple_t *Bin; // array of tuples in the micro-batch (allocated on the GPU)
    result_t *Bout; // array of results of the micro-batch (allocated on the GPU)
    size_t *gpu_start, *gpu_end; // arrays of the starting/ending positions of each window in the micro-batch (allocated on the GPU)
    size_t *gpu_gwids; // array of the gwids of the windows in the microbatch (allocated on the GPU)
    size_t scratchpad_size = 0; // size of the scratchpage memory area on the GPU (one per CUDA thread)
    char *scratchpad_memory; // scratchpage memory area (allocated on the GPU, one per CUDA thread)
#if defined(LOG_DIR)
    bool isTriggering = false;
    unsigned long rcvTuples = 0;
    unsigned long rcvTuplesTriggering = 0; // a triggering tuple activates a new batch
    double avg_td_us = 0;
    double avg_ts_us = 0;
    double avg_ts_triggering_us = 0;
    double avg_ts_non_triggering_us = 0;
    volatile unsigned long startTD, startTS, endTD, endTS;
    ofstream logfile;
#endif

    // private constructor
    Win_Seq_GPU(win_F_t _winFunction, uint64_t _win_len, uint64_t _slide_len, win_type_t _winType, size_t _batch_len, size_t _n_thread_block, string _name, size_t _scratchpad_size, PatternConfig _config, role_t _role):
            winFunction(_winFunction),
            compare([&] (const tuple_t &t1, const tuple_t &t2) { return (t1.getInfo()).second < (t2.getInfo()).second; }),
            win_len(_win_len),
            slide_len(_slide_len),
            winType(_winType),
            batch_len(_batch_len),
            n_thread_block(_n_thread_block),
            name(_name),
            scratchpad_size(_scratchpad_size),
            config(_config),
            role(_role)
    {
        // create the CUDA stream
        if (cudaStreamCreate(&cudaStream) != cudaSuccess) {
            cerr << RED << "WindFlow Error: cudaStreamCreate() returns error code" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
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
        // allocate the other arrays on the GPU
        gpuErrChk(cudaMalloc((size_t **) &gpu_start, batch_len * sizeof(size_t)));             // gpu_start
        gpuErrChk(cudaMalloc((size_t **) &gpu_end, batch_len * sizeof(size_t)));               // gpu_end
        gpuErrChk(cudaMalloc((size_t **) &gpu_gwids, batch_len * sizeof(size_t)));             // gpu_gwids
        gpuErrChk(cudaMalloc((result_t **) &Bout, batch_len * sizeof(result_t)));              // Bout
        gpuErrChk(cudaMallocHost((void **) &host_results, batch_len * sizeof(result_t)));      // host_results
        // allocate the scratchpad on the GPU (if required)
        if (scratchpad_size > 0) {
            gpuErrChk(cudaMalloc((char **) &scratchpad_memory, batch_len * scratchpad_size));  // scratchpad_memory
        }
    }

    // method to set the indexes useful if role is MAP
    void setMapIndexes(size_t _first, size_t _second) {
        map_indexes.first = _first;
        map_indexes.second = _second;
    }

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _winFunction the non-incremental window processing function (CPU/GPU function)
     *  \param _win_len window length (in no. of tuples or in time units)
     *  \param _slide_len slide length (in no. of tuples or in time units)
     *  \param _winType window type (count-based CB or time-based TB)
     *  \param _batch_len no. of windows in a batch (i.e. 1 window mapped onto 1 CUDA thread)
     *  \param _n_thread_block number of threads (i.e. windows) per block
     *  \param _name string with the unique name of the pattern
     *  \param _scratchpad_size size in bytes of the scratchpad area per CUDA thread (on the GPU)
     */ 
    Win_Seq_GPU(win_F_t _winFunction, uint64_t _win_len, uint64_t _slide_len, win_type_t _winType, size_t _batch_len, size_t _n_thread_block, string _name, size_t _scratchpad_size=0):
                Win_Seq_GPU(_winFunction, _win_len, _slide_len, _winType, _batch_len, _n_thread_block, _name, _scratchpad_size, PatternConfig(0, 1, _slide_len, 0, 1, _slide_len), SEQ) {}

    /// Destructor
    ~Win_Seq_GPU() {
        // deallocate data structures allocated on the GPU
        cudaFree(gpu_start);
        cudaFree(gpu_end);
        cudaFree(gpu_gwids);
        cudaFree(Bin);
        cudaFree(Bout);
        if (scratchpad_size > 0)
            cudaFree(scratchpad_memory);
        // deallocate data structures allocated on the CPU
        cudaFreeHost(host_results);
        // destroy the CUDA stream
        cudaStreamDestroy(cudaStream);
    }

    // svc_init method (utilized by the FastFlow runtime)
    int svc_init()
    {
#if defined(LOG_DIR)
        name += "_seq_" + to_string(ff_node_t<input_t, result_t>::get_my_id()) + ".log";
        string filename = string(STRINGIFY(LOG_DIR)) + "/" + name;
        logfile.open(filename);
#endif
        return 0;
    }

    // svc method (utilized by the FastFlow runtime)
    result_t *svc(input_t *wt)
    {
#if defined (LOG_DIR)
        startTS = current_time_nsecs();
        if (rcvTuples == 0)
            startTD = current_time_nsecs();
        rcvTuples++;
#endif
        // extract the key and id/timestamp fields from the input tuple
        tuple_t *t = extractTuple<tuple_t, input_t>(wt);
        size_t key = (t->getInfo()).first; // key
        uint64_t id = (t->getInfo()).second; // identifier or timestamp
        // access the descriptor of the input key
        auto it = keyMap.find(key);
        if (it == keyMap.end()) {
            // create the descriptor of that key
            keyMap.insert(make_pair(key, Key_Descriptor(compare, role == MAP ? map_indexes.first : 0)));
            it = keyMap.find(key);
        }
        Key_Descriptor &key_d = (*it).second;
        // check duplicate tuples
        if (key_d.rcv_counter == 0) {
            key_d.rcv_counter++;
            key_d.last_tuple = *t;
        }
        else {
            // tuples can be received only ordered by id/timestamp
            if (id <= ((key_d.last_tuple).getInfo()).second) {
                // the tuple is immediately deleted
                deleteTuple<tuple_t, input_t>(wt);
                return this->GO_ON;
            }
            else {
                key_d.rcv_counter++;
                key_d.last_tuple = *t;
            }
        }
        // gwid of the first window of that key assigned to this Win_Seq_GPU instance
        size_t first_gwid_key = ((config.id_inner - (key % config.n_inner) + config.n_inner) % config.n_inner) * config.n_outer + (config.id_outer - (key % config.n_outer) + config.n_outer) % config.n_outer;
        // initial identifer/timestamp of the keyed sub-stream arriving at this Win_Seq_GPU instance
        size_t initial_outer = ((config.id_outer - (key % config.n_outer) + config.n_outer) % config.n_outer) * config.slide_outer;
        size_t initial_inner = ((config.id_inner - (key % config.n_inner) + config.n_inner) % config.n_inner) * config.slide_inner;
        size_t initial_id = initial_outer + initial_inner;
        // special cases: if role is WLQ or REDUCE
        if (role == WLQ || role == REDUCE)
            initial_id = initial_inner;
        // if the id/timestamp of the tuple is smaller than the initial one, it must be discarded
        if (id < initial_id) {
            deleteTuple<tuple_t, input_t>(wt);
            return this->GO_ON;
        }
        // determine the local identifier of the last window containing t
        long last_w = -1;
        // sliding or tumbling windows
        if (win_len >= slide_len)
            last_w = ceil(((double) id + 1 - initial_id)/((double) slide_len)) - 1;
        // hopping windows
        else
            last_w = floor((double) (id-initial_id) / ((double) slide_len));
        // copy the tuple into the archive of the corresponding key
        //if (role != MAP || (role == MAP && !isEOSMarker<tuple_t, input_t>(*wt)))
        if (!isEOSMarker<tuple_t, input_t>(*wt))
            (key_d.archive).insert(*t);
        auto &wins = key_d.wins;
        // create all the new windows that need to be opened by the arrival of t
        for (long lwid = key_d.next_lwid; lwid <= last_w; lwid++) {
            // translate the lwid into the corresponding gwid
            size_t gwid = first_gwid_key + (lwid * config.n_outer * config.n_inner);
            if (winType == CB)
                wins.push_back(win_t(key, lwid, gwid, Triggerer_CB(win_len, slide_len, lwid, initial_id)));
            else
                wins.push_back(win_t(key, lwid, gwid, Triggerer_TB(win_len, slide_len, lwid, initial_id)));
            key_d.next_lwid++;
        }
        // evaluate all the open windows
        size_t cnt_fired = 0;
        for (auto &win: wins) {
            // if the window is fired
            if (win.onTuple(*t) == FIRED) {
                key_d.batchedWin++;
                (key_d.gwids).push_back(win.getGWID());
                // acquire from the archive the optionals to the first and the last tuple of the window
                optional<tuple_t> t_s = win.getFirstTuple();
                optional<tuple_t> t_e = win.getFiringTuple();
                pair<const_input_iterator_t, const_input_iterator_t> its;
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
                    its = (key_d.archive).getWinRange(*t_s, *t_e);
                    if (!key_d.start_tuple)
                        key_d.start_tuple = t_s;
                    size_t start_pos = (key_d.archive).getDistance(*(key_d.start_tuple), *t_s);
                    (key_d.start).push_back(start_pos);
                    size_t end_pos = (key_d.archive).getDistance(*(key_d.start_tuple))-1;
                    if(isEOSMarker<tuple_t, input_t>(*wt))
                        end_pos++;
                    (key_d.end).push_back(end_pos);
                }
                // the fired window is batched
                win.setBatched();
                // a new micro-batch is complete
                if (key_d.batchedWin == batch_len) {
#if defined(LOG_DIR)
                    rcvTuplesTriggering++;
                    isTriggering = true;
#endif
                    // compute the initial pointer of the batch and its size (in no. of tuples)
                    const tuple_t *dataBatch;
                    size_t size_copy;
                    if (!key_d.start_tuple) { // the batch is empty
                        dataBatch = nullptr;
                        size_copy = 0;
                    }
                    else { // the batch is not empty
                        dataBatch = (const tuple_t *) &(*((key_d.archive).getIterator(*(key_d.start_tuple))));
                        size_copy = (key_d.archive).getDistance(*(key_d.start_tuple)) - 1;
                        if(isEOSMarker<tuple_t, input_t>(*wt))
                            size_copy++;
                    }
                    // copy of the arrays on the GPU
                    gpuErrChk(cudaMemcpyAsync(gpu_start, (key_d.start).data(), batch_len * sizeof(size_t), cudaMemcpyHostToDevice, cudaStream));
                    gpuErrChk(cudaMemcpyAsync(gpu_end, (key_d.end).data(), batch_len * sizeof(size_t), cudaMemcpyHostToDevice, cudaStream));
                    gpuErrChk(cudaMemcpyAsync(gpu_gwids, (key_d.gwids).data(), batch_len * sizeof(size_t), cudaMemcpyHostToDevice, cudaStream));
                    // count-based windows
                    if (winType == CB) {
                        gpuErrChk(cudaMemcpyAsync(Bin, dataBatch, size_copy * sizeof(tuple_t), cudaMemcpyHostToDevice, cudaStream));   
                    }
                    // time-based windows
                    else {
                        // simple herustics to resize the array Bin on the GPU (if required)
                        if (size_copy > tuples_per_batch) {
                            tuples_per_batch = max(tuples_per_batch * 2, size_copy);
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
                    // execute the CUDA Kernel on GPU
                    int num_blocks = ceil((double) batch_len / n_thread_block);
                    kernelBatch<win_F_t><<<num_blocks, n_thread_block, 0, cudaStream>>>(key, Bin, gpu_start, gpu_end, gpu_gwids, Bout, winFunction, batch_len, scratchpad_memory, scratchpad_size);
                    gpuErrChk(cudaMemcpyAsync(host_results, Bout, batch_len * sizeof(result_t), cudaMemcpyDeviceToHost, cudaStream));
                    gpuErrChk(cudaStreamSynchronize(cudaStream));
                    // purge the archive from tuples that are no longer necessary
                    if (key_d.start_tuple)
                        (key_d.archive).purge(*(key_d.start_tuple));
                    cnt_fired += batch_len;
                    // transmission of results
                    for(size_t i=0; i<batch_len; i++) {
                        result_t *res = new result_t();
                        *res = host_results[i];
                        // special cases: role is PLQ or MAP
                        if (role == MAP) {
                            res->setInfo(key, key_d.emit_counter);
                            key_d.emit_counter += map_indexes.second;
                        }
                        else if (role == PLQ) {
                            size_t new_id = ((config.id_inner - (key % config.n_inner) + config.n_inner) % config.n_inner) + (key_d.emit_counter * config.n_inner);
                            res->setInfo(key, new_id);
                            key_d.emit_counter++;
                        }
                        this->ff_send_out(res);
                    }
                    // reset data structures for the next micro-batch
                    key_d.batchedWin = 0;
                    (key_d.start).clear();
                    (key_d.end).clear();
                    (key_d.gwids).clear();
                    key_d.start_tuple = nullopt;
                }
            }
        }
        // purge all the windows of the batch
        wins.erase(wins.begin(), wins.begin() + cnt_fired);
        // delete the received tuple
        deleteTuple<tuple_t, input_t>(wt);
#if defined(LOG_DIR)
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
        // allocate on the CPU the scratchpad_memory
        char *scratchpad_memory_cpu = (char *) malloc(sizeof(char) * scratchpad_size);
        // iterate over all the keys
        for (auto &k: keyMap) {
            size_t key = k.first;
            Key_Descriptor &key_d = k.second;
            auto &wins = key_d.wins;
            // iterate over all the existing windows of the key and execute them on the CPU
            for (auto &win: wins) {
                optional<tuple_t> t_s = win.getFirstTuple();
                optional<tuple_t> t_e = win.getFiringTuple();
                pair<const_input_iterator_t, const_input_iterator_t> its;
                result_t *out = win.getResult();
                if (t_s) { // not-empty window
                    if (t_e) // BATCHED window
                        its = (key_d.archive).getWinRange(*t_s, *t_e);
                    else // not-FIRED window
                        its = (key_d.archive).getWinRange(*t_s);
                    // call the winFunction on the CPU
                    decltype(get_tuple_t(winFunction)) *my_input_data = (decltype(get_tuple_t(winFunction)) *) &(*(its.first));
                    if (winFunction(key, win.getGWID(), my_input_data, out, distance(its.first, its.second), scratchpad_memory_cpu) != 0) {
                        cerr << RED << "Error: winFunction() call returns non-zero on CPU" << DEFAULT << endl;
                        exit(EXIT_FAILURE);
                    }
                }
                else {// empty window
                    if (winFunction(key, win.getGWID(), nullptr, out, 0, scratchpad_memory_cpu) != 0) {
                        cerr << RED << "Error: winFunction() call returns non-zero on CPU" << DEFAULT << endl;
                        exit(EXIT_FAILURE);
                    }
                }
                // special cases: role is PLQ or MAP
                if (role == MAP) {
                    out->setInfo(k.first, (k.second).emit_counter);
                    (k.second).emit_counter += map_indexes.second;
                }
                else if (role == PLQ) {
                    size_t new_id = ((config.id_inner - (k.first % config.n_inner) + config.n_inner) % config.n_inner) + ((k.second).emit_counter * config.n_inner);
                    out->setInfo(k.first, new_id);
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
#if defined (LOG_DIR)
        ostringstream stream;
        stream << "************************************LOG************************************\n";
        stream << "No. of received tuples: " << rcvTuples << "\n";
        stream << "No. of received tuples (triggering): " << rcvTuplesTriggering << "\n";
        stream << "Average service time: " << avg_ts_us << " usec \n";
        stream << "Average service time (triggering): " << avg_ts_triggering_us << " usec \n";
        stream << "Average service time (non triggering): " << avg_ts_non_triggering_us << " usec \n";
        stream << "Average inter-departure time: " << avg_td_us << " usec \n";
        stream << "***************************************************************************\n";
        logfile << stream.str();
        logfile.close();
#endif
    }

    /// Method to start the pattern execution asynchronously
    virtual int run(bool)
    {
        return ff_node::run();
    }

    /// Method to wait the pattern termination
    virtual int wait()
    {
        return ff_node::wait();
    }
};

#endif
