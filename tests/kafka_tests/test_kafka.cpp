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
 *    * MIT License: https://github.com/ParaGroup/WindFlow/blob/master/LICENSE.MIT
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
 *  Test with the use of Kafka operators.
 *  
 *  Before starting this test program, you need to download Apache Kafka and to start it
 *  using the following commands:
 *  
 *  $ <path_to_kafka>/bin/zookeeper-server-start.sh <path_to_windflow>/tests/kafka_tests/zookeeper.properties
 *  $ <path_to_kafka>/bin/kafka-server-start.sh <path_to_windflow>/tests/kafka_tests/server.properties
 *  
 *  We create the two Kafka topics used by the Kafka_Source and Kafka_Sink rispectively:
 *  
 *  $ <path_to_kafka>/bin/kafka-topics.sh --create --topic input --bootstrap-server localhost:9092
 *  $ <path_to_kafka>/bin/kafka-topics.sh --create --topic output --bootstrap-server localhost:9092
 *  
 *  To start the test, you have to run the following commands (each in a separate terminal):
 *  
 *  $ ./kafka_producer
 *  $ <path_to_kafka>/bin/kafka-console-consumer.sh --topic output --bootstrap-server localhost:9092
 *  $ ./test_kafka
 *  
 *  To reset the Kafka environment, and to clean it:
 *  
 *  $ rm -rf /tmp/kafka-logs-* /tmp/zookeeper
 */ 

// include
#include<regex>
#include<string>
#include<vector>
#include<iostream>
#include<windflow.hpp>
#include<kafka/windflow_kafka.hpp>
#include"kafka_common.hpp"

using namespace std;
using namespace wf;
using namespace chrono;

// Main
int main(int argc, char* argv[])
{
    mt19937 rng;
    rng.seed(std::random_device()());
    size_t min = 1;
    size_t max = 9;
    std::uniform_int_distribution<std::mt19937::result_type> dist_p(min, max);
    std::uniform_int_distribution<std::mt19937::result_type> dist_b(0, 10);
    // prepare the test
    PipeGraph graph("test_kafka_1", Execution_Mode_t::DEFAULT, Time_Policy_t::INGRESS_TIME);
    // prepare the MultiPipe
    KafkaSource_Functor source_functor;
    Kafka_Source source = KafkaSource_Builder(source_functor)
                                .withName("kafka_source")
                                .withOutputBatchSize(dist_b(rng))
                                .withBrokers("localhost:9092")
                                .withTopics("input")
                                .withGroupID("groupid")
                                .withAssignmentPolicy("roundrobin")
                                .withIdleness(seconds(5))
                                .withParallelism(dist_p(rng))
                                .withOffsets(-1) // -1 means from the most recent offset
                                .build();
    MultiPipe &pipe = graph.add_source(source);
    Map_Functor map_functor;
    Map map = Map_Builder(map_functor)
                        .withName("map")
                        .withParallelism(dist_p(rng))
                        .withOutputBatchSize(dist_b(rng))
                        .build();
    pipe.add(map);
    KafkaSink_Functor sink_functor;
    Kafka_Sink sink = KafkaSink_Builder(sink_functor)
                        .withName("kafka_sink")
                        .withParallelism(dist_p(rng))
                        .withBrokers("localhost:9092")
                        .build();
    pipe.add_sink(sink);
    graph.run();
    return 0;
}
