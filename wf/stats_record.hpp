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
 *  @file    stats_record.hpp
 *  @author  Gabriele Mencagli
 *  @date    21/04/2020
 *  
 *  @brief Record of Statistics of an operator replica
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
#include<fstream>
#include<iomanip>
#include<sstream>
#include<time.h>
#include<rapidjson/prettywriter.h>
#include<basic.hpp>

namespace wf {

// class Stats_Record
class Stats_Record
{
private:
    struct tm buf; // private time structure

    // method to get the current date and time in a string format
    std::string return_current_time_and_date()
    {
        auto now = std::chrono::system_clock::now();
        auto in_time_t = std::chrono::system_clock::to_time_t(now);
        std::stringstream ss;
        ss << std::put_time(localtime_r(&in_time_t, &(this->buf)), "%Y-%m-%d %X");
        return ss.str();
    }

public:
    std::string nameOP; // name of the operator where this replica belongs to
    std::string nameReplica; // name of the replica
    std::string start_time_string; // starting date and time of the replica
    std::chrono::time_point<std::chrono::system_clock> start_time; // starting time of the replica
    std::chrono::time_point<std::chrono::system_clock> end_time; // ending time of the replica
    bool terminated; // true if the replica has finished its processing
    uint64_t inputs_received = 0; // inputs received by the replica
    uint64_t inputs_ignored = 0; // number of ignored inputs
    uint64_t bytes_received = 0; // bytes received by the replica
    uint64_t outputs_sent = 0; // outputs sent by the replica
    uint64_t bytes_sent = 0; // bytes sent by the replica
    std::chrono::duration<double, std::micro> service_time; // average ideal service time of the replica (microseconds)
    std::chrono::duration<double, std::micro> eff_service_time; // effective service time of the replica (microseconds)
    bool isWinOP; // true if the replica belongs to a window-based operator
    bool isGPUReplica; // true if the replica offloads the processing on a GPU device
    // the following variables are meaningful if isGPUReplica is true
    uint64_t num_kernels = 0; // number of kernels calls
    uint64_t bytes_copied_hd = 0; // bytes copied from Host to Device
    uint64_t bytes_copied_dh = 0; // bytes copied from Device to Host

    // Contructor I
    Stats_Record()
    {
        nameOP = "N/A";
        nameReplica = "N/A";
        start_time_string = return_current_time_and_date();
        start_time = std::chrono::system_clock::now();
        terminated = false;
        isWinOP = false;
        isGPUReplica = false;
    }

    // Contructor II
    Stats_Record(std::string _nameOP,
                 std::string _nameReplica,
                 bool _isWinOP,
                 bool _isGPUReplica):
                 nameOP(_nameOP),
                 nameReplica(_nameReplica),
                 terminated(false),
                 isWinOP(_isWinOP),
                 isGPUReplica(_isGPUReplica)
    {
        start_time_string = return_current_time_and_date();
        start_time = std::chrono::system_clock::now();
    }

    // method to mark the replica as terminated
    void set_Terminated()
    {
        terminated = true;
        // save the termination time
        end_time = std::chrono::system_clock::now();
    }

    // method to append the statistics of the operator replica (in JSON format)
    void append_Stats(rapidjson::PrettyWriter<rapidjson::StringBuffer> &writer)
    {
        // append the statistics of this operator replica
        writer.StartObject();
        writer.Key("Replica_id");
        writer.String(nameReplica.c_str());
        writer.Key("Starting_time");
        writer.String(start_time_string.c_str());
        // compute the execution length
        std::chrono::duration<double> elapsed_seconds;
        if (!terminated) {
            // get the current time
            auto curr_time = std::chrono::system_clock::now();
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
