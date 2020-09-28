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
 *  @file    monitoring.hpp
 *  @author  Gabriele Mencagli
 *  @date    02/08/2020
 *  
 *  @brief Monitoring thread used with the macro TRACE_WINDFLOW enabled
 *  
 *  @section Monitoring Thread (Description)
 *  
 *  This file implements the monitoring thread used by the WindFlow library with the
 *  TRACE_WINDFLOW macro enabled. The thread allows the PipeGraph to connect with a
 *  Web DashBoard showing the application statistics through a REST web server.
 */ 

#ifndef MONITORING_H
#define MONITORING_H

/// includes
#include<ios>
#include<fstream>
#include<errno.h>
#include<fcntl.h>
#include<netdb.h>
#include<stdlib.h>
#include<string.h>
#include<unistd.h>
#include<arpa/inet.h>
#include<basic.hpp>

namespace wf {

// function to extract the VSS and RSS of the calling process (PID)
inline void get_MemUsage(double &vss, double &rss)
{
   vss = 0.0;
   rss = 0.0;
   std::ifstream stat_stream("/proc/self/stat", std::ios_base::in); //get info from proc directory
   // create some variables to get info
   std::string pid, comm, state, ppid, pgrp, session, tty_nr;
   std::string tpgid, flags, minflt, cminflt, majflt, cmajflt;
   std::string utime, stime, cutime, cstime, priority, nice;
   std::string O, itrealvalue, starttime;
   unsigned long vsize;
   long rss_2;
   stat_stream >> pid >> comm >> state >> ppid >> pgrp >> session >> tty_nr >> tpgid 
               >> flags >> minflt >> cminflt >> majflt >> cmajflt >> utime >> stime
               >> cutime >> cstime >> priority >> nice >> O >> itrealvalue >> starttime >> vsize >> rss_2;
   stat_stream.close();
   long page_size_kb = sysconf(_SC_PAGE_SIZE) / 1024; // for x86-64 is configured to use 2MB pages
   vss = vsize / 1024.0;
   rss = rss_2 * page_size_kb;
}

// function to connect to the given ip address/hostname and port of the Web DashBoard
inline int socket_connect(const char *dashboard_machine, int port)
{
    const uint64_t MAX_HOST_NAME_LEN = 256;
    // prepare the sockaddr_in struct
    struct sockaddr_in sockAddress;
    sockAddress.sin_family = AF_INET;
    sockAddress.sin_port = htons(port);
    sockAddress.sin_addr.s_addr = inet_addr(dashboard_machine);
    // if instead of an ip we have a hostname
    struct hostent *hostEntity;
    char hnamebuf[MAX_HOST_NAME_LEN];
    if (sockAddress.sin_addr.s_addr == (u_int)-1) {
        // try to translate the hostname into an ip address
        hostEntity = gethostbyname(dashboard_machine);
        if (!hostEntity) {
            std::cerr << YELLOW << "       WindFlow Warning: gethostbyname() is not working" << DEFAULT_COLOR << std::endl;
            return -1;
        }
        else {
            sockAddress.sin_family = hostEntity->h_addrtype;
            bcopy(hostEntity->h_addr, (caddr_t) &sockAddress.sin_addr, hostEntity->h_length);
            strncpy(hnamebuf, hostEntity->h_name, sizeof(hnamebuf)-1);
        }
    }
    // opening of the TCP/IP socket with the Web DashBoard
    int s = -1;
    if ((s = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0) {
        std::cerr << YELLOW << "       WindFlow Warning: socket() call is not working" << DEFAULT_COLOR << std::endl;
        return -1;
    }
    // try 5 times to connect to the Web DashBoard
    int count = 0;
    bool isEstablished = false;
    std::cout << "       * Try connection with Web DashBoard on machine " << std::string(dashboard_machine) << " with port " << std::to_string(port) << std::endl;
    while (count < 5 && !isEstablished) {
        if (connect(s, (struct sockaddr *) &sockAddress, sizeof(struct sockaddr)) >= 0) {
            std::cout << "       * Connection " << GREEN << "established" << DEFAULT_COLOR << std::endl;
            isEstablished = true;
        }
        count++;
    }
    if (!isEstablished) {
        std::cout << "       * Connection " << RED << "refused" << DEFAULT_COLOR << std::endl;
        return -1;
    }
    return s;
}

// function to send a message over an open socket
inline int socket_send(int s, void *msg, size_t len)
{
    size_t sentBytes = 0;
    size_t sent = 0;
    char *msg_char = (char *) msg;
    // do many sends until all the bytes have been transmitted
    while (sentBytes < len) {
        sent = send(s, msg_char, len, 0);
        if (sent == -1) {
            std::cerr << YELLOW << "       WindFlow Warning: send() call is not working" << DEFAULT_COLOR << std::endl;
            return -1;
        }
        else if (sent <= len) {
            sentBytes += sent;
            msg_char += sent;
        }
    }
    return sentBytes;
}

// function to receive a message from an open socket
inline int socket_receive(int s, void *vtg, size_t len)
{
    char *vtg_char = (char *) vtg;
    // receive
    size_t recv_tot = 0;
    while(recv_tot < len) {
        size_t received = recv(s, vtg_char+recv_tot, len-recv_tot, MSG_WAITALL);
        if (received == -1) {
            std::cerr << YELLOW << "       WindFlow Warning: recv() call is not working" << DEFAULT_COLOR << std::endl;
            return -1;
        }
        if (received == 0) {
            // the client side has gracefully closed the socket
            return -1;
        }
        else recv_tot += received;
    }
    return recv_tot;
}

// class MonitoringThread
class MonitoringThread
{
private:
    PipeGraph *graph; // pointer to the pipegraph
    int s; // socket to communicate with the Web DashBoard
    int32_t identifier; // identifier given by the Web DashBoard
    volatile uint64_t start_sample_time_us; // starting time of the last sample in usec

public:
    // Constructor
    MonitoringThread(PipeGraph *_graph):
                     graph(_graph),
                     s(-1),
                     identifier(-1)
    {
        start_sample_time_us = current_time_usecs();
    }

    // function to be executed by the monitoring thread
    void operator()()
    {
        assert(graph != nullptr);
        const uint64_t DASHBOARD_SAMPLE_RATE_USEC = 1000000;
        // connect to the Web DashBoard
#if (!defined(DASHBOARD_MACHINE) and !defined(DASHBOARD_PORT))
        std::string dashboard_machine = "localhost";
        int dashboard_port = 20207;
#elif (defined(DASHBOARD_MACHINE) and !defined(DASHBOARD_PORT))
        std::string dashboard_machine = STRINGIFY(DASHBOARD_MACHINE);
        int dashboard_port = 20207;
#elif (!defined(DASHBOARD_MACHINE) and defined(DASHBOARD_PORT))
        std::string dashboard_machine = "localhost";
        int dashboard_port = DASHBOARD_PORT;
#elif (defined(DASHBOARD_MACHINE) and defined(DASHBOARD_PORT))
        std::string dashboard_machine = STRINGIFY(DASHBOARD_MACHINE);
        int dashboard_port = DASHBOARD_PORT;
#endif
        if ((s = socket_connect(dashboard_machine.c_str(), dashboard_port)) < 0) {
            std::cout << "       * Monitoring thread switched off " << std::endl;
            return;
        }
        // register the application
        if (registerApp() < 0) {
            std::cout << "       * Monitoring thread switched off " << std::endl;
            close(s);
            return;
        }
        // loop until the processing is complete
        while (!is_ended_func(graph)) {
            // if the sampling interval is complete send the new report
            if (current_time_usecs() - start_sample_time_us >= DASHBOARD_SAMPLE_RATE_USEC) {
                if (sendReport() < 0) {
                    std::cout << "       * Monitoring thread switched off " << std::endl;
                    close(s);
                    return;
                }
                start_sample_time_us = current_time_usecs();
            }
            usleep(100); // sleep for 100 useconds
        }
        // de-register the application
        if (deregisterApp() < 0) {
            std::cout << "       * Monitoring thread switched off " << std::endl;
            close(s);
            return;
        }
        close(s);
    }

    // method to register the application to the Web DashBoard
    int registerApp()
    {
        // preamble of 8 bytes (Type + Length)
        int32_t preamble[2];
        preamble[0] = htonl(0); // NEW_APP has type 0
        // get the string representing the diagram (in SVG format)
        std::string svg_str = get_diagram(graph);
        preamble[1] = htonl(svg_str.length() + 1);
        // send the preamble to the Web DashBoard
        if (socket_send(s, (void *) &preamble, 8) != 8) {
            return -1;
        }
        // send the payload to the Web DashBoard
        if (socket_send(s, (void *) svg_str.c_str(), svg_str.length() + 1) != svg_str.length() + 1) {
            return -1;
        }
        // receive the message back from the Web DashBoard
        int32_t ack[2];
        if (socket_receive(s, &ack, 8) != 8) {
            return -1;
        }
        assert(ntohl(ack[0]) == 0); // received status must always be zero
        // save the identifier given by the Web DashBoard
        identifier = ntohl(ack[1]);
        return 0;
    }

    // method to send a statistic report to the Web DashBoard
    int sendReport()
    {
        // preamble of 12 bytes (Type + Identifier + Length)
        int32_t preamble[3];
        preamble[0] = htonl(1); // NEW_REPORT has type 1
        // copy the identifier of the application in the preamble
        preamble[1] = htonl(identifier);
        // get the string representing the last report of statistics
        std::string json_str = get_stats_report(graph);
        preamble[2] = htonl(json_str.length() + 1);
        // send the preamble to the Web DashBoard
        if (socket_send(s, (void *) &preamble, 12) != 12) {
            return -1;
        }
        // send the payload to the Web DashBoard
        if (socket_send(s, (void *) json_str.c_str(), json_str.length() + 1) != json_str.length() + 1) {
            return -1;
        }
        // receive the message back from the Web DashBoard
        int32_t ack[2];
        if (socket_receive(s, &ack, 8) != 8) {
            return -1;
        }
        assert(ntohl(ack[0]) == 0); // received status must always be zero
        return 0;
    }

    // method to de-register the application from the Web DashBoard
    int deregisterApp()
    {
        // preamble of 12 bytes (Type + Identifier + Length)
        int32_t preamble[3];
        preamble[0] = htonl(2); // END_APP has type 2
        // copy the identifier of the application in the preamble
        preamble[1] = htonl(identifier);
        // get the string representing the last report of statistics
        std::string json_str = get_stats_report(graph);
        preamble[2] = htonl(json_str.length() + 1);
        // send the preamble to the Web DashBoard
        if (socket_send(s, (void *) &preamble, 12) != 12) {
            return -1;
        }
        // send the payload to the Web DashBoard
        if (socket_send(s, (void *) json_str.c_str(), json_str.length() + 1) != json_str.length() + 1) {
            return -1;
        }
        // receive the message back from the Web DashBoard
        int32_t ack[2];
        if (socket_receive(s, &ack, 8) != 8) {
            return -1;
        }
        assert(ntohl(ack[0]) == 0); // received status must always be zero
        return 0;
    }
};

} // namespace wf

#endif
