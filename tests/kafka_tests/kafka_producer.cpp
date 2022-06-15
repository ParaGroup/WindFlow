/**************************************************************************************
 *  Copyright (c) 2019- Gabriele Mencagli and Matteo Della Bartola
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

/*  
 *  Standalone program used to generate messages with a specific topic. Messages are
 *  published to Apache Kafka.
 */ 

// include
#include<iostream>
#include<windflow.hpp>
#include<kafka/windflow_kafka.hpp>

using namespace std;
using namespace wf;

// class ExDeliveryReportCb
class ExDeliveryReportCb: public RdKafka::DeliveryReportCb
{
private:
    int success = 0;
    int failed = 0;

public:
    // dr_cb method
    void dr_cb(RdKafka::Message &message)
    {
        if (message.err())
            failed++;
        else {
            success++;
        }
    }
};

// Main
int main(int argc, char* argv[])
{
    std::string broker = "localhost:9092";
    RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    std::string errstr;
    RdKafka::Producer *producer;
    ExDeliveryReportCb ex_dr_cb;
    int index = 0;
    int num_keys = 0;
    unsigned long current_time;
    unsigned long start_time;
    unsigned long app_run_time = 60 * 1000000000L; // 60 seconds
    if (conf->set("bootstrap.servers", broker, errstr) != RdKafka::Conf::CONF_OK) {
        std::cerr << errstr << std::endl;
        exit(1);
    }
    producer = RdKafka::Producer::create(conf, errstr);
    if (!producer) {
        std::cerr << "Failed to create producer: " << errstr << std::endl;
        exit(1);
    }
    start_time = current_time_nsecs();
    current_time = current_time_nsecs();
    index = 0;
    std::string msg = "message";
    volatile unsigned long start_time_main_usecs = current_time_usecs();
    std::cout << "Generating Messages for 60 seconds..." << std::endl;
    while (current_time - start_time <= (app_run_time)) {        
        RdKafka::ErrorCode err = producer->produce("input",
                                                   RdKafka::Topic::PARTITION_UA,
                                                   RdKafka::Producer::RK_MSG_COPY,
                                                   const_cast<char *>(msg.c_str()),
                                                   msg.size(),
                                                   NULL,
                                                   0,
                                                   0,
                                                   NULL);
        
        producer->poll(0);
        std::this_thread::sleep_for(std::chrono::microseconds(100));
        index++;
        current_time = current_time_nsecs();        
    }
    volatile unsigned long end_time_main_usecs = current_time_usecs();
    producer->poll(0);
    std::this_thread::sleep_for(std::chrono::milliseconds(2));
    std::cout << "...end" << std::endl;
    return 0;
}
