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
 *  @file    stats_record.hpp
 *  @author  Gabriele Mencagli
 *  
 *  @brief Record of statistics of the operator replica
 *  
 *  @section Statistics Record (Description)
 *  
 *  This file implements the record of statistics gathered by a specific
 *  replica of an operator within a WindFlow application.
 */ 

#ifndef STATS_RECORD_H
#define STATS_RECORD_H

// includes
#include<chrono>
#include<iomanip>
#include<rapidjson/prettywriter.h>
#include<basic.hpp>

namespace wf {

// class Stats_Record
class Stats_Record
{
private:
    struct tm buf; // private time structure

    // Get the current date and time in a string format
    std::string return_current_time_and_date()
    {
        auto now = std::chrono::system_clock::now();
        auto in_time_t = std::chrono::system_clock::to_time_t(now);
        std::stringstream ss;
        ss << std::put_time(localtime_r(&in_time_t, &(this->buf)), "%Y-%m-%d %X");
        return ss.str();
    }

public:
    std::string nameOP; // name of the operator
    std::string nameReplica; // name of the replica
    std::string start_time_string; // starting date and time
    std::chrono::time_point<std::chrono::system_clock> start_time; // starting time
    std::chrono::time_point<std::chrono::system_clock> end_time; // ending time
    bool terminated; // true if the replica has finished its processing
    uint64_t inputs_received = 0; // inputs received
    uint64_t inputs_ignored = 0; // number of ignored inputs
    uint64_t bytes_received = 0; // bytes received
    uint64_t outputs_sent = 0; // outputs sent
    uint64_t bytes_sent = 0; // bytes sent
    std::chrono::duration<double, std::micro> service_time; // average ideal service time (microseconds)
    std::chrono::duration<double, std::micro> eff_service_time; // effective service time (microseconds)
    bool isWinOP; // true if the replica belongs to a window-based operator
    bool isGPUReplica; // true if the replica offloads the processing on a GPU device
    /* Variables for GPU replicas */
    uint64_t num_kernels = 0; // number of kernels calls
    uint64_t bytes_copied_hd = 0; // bytes copied from Host to Device
    uint64_t bytes_copied_dh = 0; // bytes copied from Device to Host

    // Contructor I
    Stats_Record():
                 nameOP("N/A"),
                 nameReplica("N/A"),
                 start_time_string(return_current_time_and_date()),
                 start_time(std::chrono::system_clock::now()),
                 terminated(false),
                 isWinOP(false),
                 isGPUReplica(false) {}

    // Contructor II
    Stats_Record(std::string _nameOP,
                 std::string _nameReplica,
                 bool _isWinOP,
                 bool _isGPUReplica):
                 nameOP(_nameOP),
                 nameReplica(_nameReplica),
                 start_time_string(return_current_time_and_date()),
                 start_time(std::chrono::system_clock::now()),
                 terminated(false),
                 isWinOP(_isWinOP),
                 isGPUReplica(_isGPUReplica) {}

    // Set the statistics recording as terminated
    void setTerminated()
    {
        terminated = true;
        end_time = std::chrono::system_clock::now(); // save the termination time
    }

    // Append the statistics of the operator replica (in JSON format)
    void appendStats(rapidjson::PrettyWriter<rapidjson::StringBuffer> &writer)
    {
        // append the statistics of this operator replica
        writer.StartObject();
        writer.Key("Replica_id");
        writer.String(nameReplica.c_str());
        writer.Key("Starting_time");
        writer.String(start_time_string.c_str());
        std::chrono::duration<double> elapsed_seconds; // compute the execution length
        if (!terminated) {
            auto curr_time = std::chrono::system_clock::now(); // get the current time
            elapsed_seconds = curr_time - start_time;
        }
        else {
            elapsed_seconds = end_time - start_time;
        }
        writer.Key("Running_time_sec");
        writer.Double(elapsed_seconds.count());
        writer.Key("isTerminated");
        writer.Bool(terminated);
        writer.Key("Inputs_received");
        writer.Uint64(inputs_received);
        writer.Key("Bytes_received");
        writer.Uint64(bytes_received);
        if (isWinOP) {
            writer.Key("Inputs_ingored");
            writer.Uint64(inputs_ignored);
        }
        writer.Key("Outputs_sent");
        writer.Uint64(outputs_sent);
        writer.Key("Bytes_sent");
        writer.Uint64(bytes_sent);
        writer.Key("Service_time_usec");
        writer.Double(service_time.count());
        writer.Key("Eff_Service_time_usec");
        writer.Double(eff_service_time.count());
        if (isGPUReplica) {
            writer.Key("Kernels_launched");
            writer.Uint64(num_kernels);
            writer.Key("Bytes_H2D");
            writer.Uint64(bytes_copied_hd);
            writer.Key("Bytes_D2H");
            writer.Uint64(bytes_copied_dh);
        }
        writer.EndObject();
    }
};

} // namespace wf

#endif
