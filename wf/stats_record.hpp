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
 *  @brief Record of Statistics of an Operator Replica
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
#include<time.h>
#include<basic.hpp>

namespace wf {

/** 
 *  \class Stats_Record
 *  
 *  \brief Record of statistics gathered by a replica of an operator
 *  
 *  This class contains a set of statistics gathered from the beginning of the
 *  execution by a specific replica of an operator within a WindFlow application.
 */ 
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
    std::string nameOP; ///< name of the operator where this replica belongs to
    std::string nameReplica; ///< name of the replica
    std::string start_time_string; ///< starting date and time of the replica
    std::chrono::time_point<std::chrono::system_clock> start_time; ///< starting time of the replica
    uint64_t inputs_received = 0; ///< inputs received by the replica
    uint64_t bytes_received = 0; ///< bytes received by the replica
    uint64_t outputs_sent = 0; ///< outputs sent by the replica
    uint64_t bytes_sent = 0; ///< bytes sent by the replica
    std::chrono::duration<double, std::micro> service_time; ///< average ideal service time of the replica
    std::chrono::duration<double, std::micro> eff_service_time; ///< effectuve service time of the replica
    bool isGPUReplica; ///< true if the replica offloads the processing on a GPU device
    /// the following variables are meaningful if isGPUReplica is true
    uint64_t num_kernels = 0; ///< number of kernels calls
    uint64_t bytes_copied_hd = 0; ///< bytes copied from Host to Device
    uint64_t bytes_copied_dh = 0; ///< bytes copied from Device to Host

    /// Contructor I
    Stats_Record()
    {
        nameOP = "N/A";
        nameReplica = "N/A";
        start_time_string = return_current_time_and_date();
        start_time = std::chrono::system_clock::now();
        isGPUReplica = false;
    }

    /** 
     *  \brief Constructor II
     *  
     *  \param _nameOP name of the operator where this replica belongs to
     *  \param _nameReplica name of the replica
     *  \param _isGPUReplica true if the replica belongs to a GPU operator, false otherwise
     */ 
    Stats_Record(std::string _nameOP, std::string _nameReplica, bool _isGPUReplica):
                 nameOP(_nameOP),
                 nameReplica(_nameReplica),
                 isGPUReplica(_isGPUReplica)
    {
        start_time_string = return_current_time_and_date();
        start_time = std::chrono::system_clock::now();
    }

    /// Method to dump the statistics of the replica in a log file
    void dump_toFile()
    {
        std::ofstream logfile;
#if defined(LOG_DIR)
        std::string log_dir = std::string(STRINGIFY(LOG_DIR));
        std::string filename = std::string(STRINGIFY(LOG_DIR)) + "/" + std::to_string(getpid()) + "_" + nameOP + "_" + nameReplica + ".log";
#else
        std::string log_dir = std::string("log");
        std::string filename = "log/" + std::to_string(getpid()) + "_" + nameOP + "_" + nameReplica + ".log";
#endif
        // create the log directory
        if (mkdir(log_dir.c_str(), 0777) != 0) {
            struct stat st;
            if((stat(log_dir.c_str(), &st) != 0) || !S_ISDIR(st.st_mode)) {
                std::cerr << RED << "WindFlow Error: directory for log files cannot be created" << DEFAULT_COLOR << std::endl;
                exit(EXIT_FAILURE);
            }
        }
        logfile.open(filename);
        // get the ending time of this replica
        auto end_time = std::chrono::system_clock::now();
        // compute the execution length
        std::chrono::duration<double> elapsed_seconds = end_time - start_time;
        // write statistics in the log file
        std::ostringstream stream;
        stream << "***************************************************************************\n";
        stream << "Operator name: " << nameOP << "\n";
        stream << "Replica name: " << nameReplica << "\n";
        stream << "Starting date and time: " << start_time_string << "\n";
        stream << "Execution duration (seconds): " << elapsed_seconds.count() << "\n";
        stream << "\n";
        stream << "Inputs received: " << inputs_received << "\n";
        stream << "Bytes received: " << bytes_received << "\n";
        stream << "Outputs sent: " << outputs_sent << "\n";
        stream << "Bytes sent: " << bytes_sent << "\n";
        stream << "Service time (usec): " << service_time.count() << "\n";
        stream << "Effective service time (usec): " << eff_service_time.count() << "\n";
        if (isGPUReplica) {
            stream << "\n";
            stream << "Kernels launched: " << num_kernels << "\n";
            stream << "Bytes copied (Host->Device): " << bytes_copied_hd << "\n";
            stream << "Bytes copied (Device->Host): " << bytes_copied_dh << "\n";
        }
        stream << "***************************************************************************\n";
        logfile << stream.str();
        logfile.close();
    }
};

} // namespace wf

#endif
