[![License: LGPL v3](https://img.shields.io/badge/License-LGPL%20v3-blue.svg)](https://www.gnu.org/licenses/lgpl-3.0)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Release](https://img.shields.io/github/release/paragroup/windflow.svg)](https://github.com/paragroup/windflow/releases/latest)
[![Donate](https://img.shields.io/badge/Donate-PayPal-green.svg)](https://paypal.me/GabrieleMencagli)

<p align="center"><img src="https://paragroup.github.io/WindFlow/img/logo_white.png" width="400" title="WindFlow Logo"></p>

# Introduction
WindFlow is a C++17 header-only library designed for parallel data stream processing on heterogeneous shared-memory architectures that combine multi-core CPUs and NVIDIA GPUs. The library offers traditional stream processing operators such as <b>map</b>, <b>flatmap</b>, <b>filter</b>, and <b>reduce</b>, as well as window-based operators. Its API enables developers to build streaming applications using the <b>MultiPipe</b> and <b>PipeGraph</b> programming constructs. The former facilitates the creation of parallel pipelines with shuffle connections, while the latter allows multiple <b>MultiPipe</b> instances to be interconnected through <b>merge</b> and <b>split</b> operations, enabling the construction of complex directed acyclic graphs of interconnected operators.

Analogous to popular stream processing engines such as Apache Storm and Apache Flink, WindFlow supports general-purpose streaming applications by allowing operators to execute user-defined code. The WindFlow runtime system is designed to efficiently operate on embedded architectures featuring low-power multi-core CPUs and integrated NVIDIA GPUs (such as the Jetson family of NVIDIA boards). Nevertheless, it also performs effectively on conventional multi-core servers equipped with discrete NVIDIA GPUs.

At the moment, WindFlow supports single-node execution. We are currently working on a distributed implementation.

The web site of the library is available at: https://paragroup.github.io/WindFlow/.

# Dependencies
The library requires the following dependencies:
* <strong>a C++ compiler</strong> with full support for C++17 (WindFlow tests have been successfully compiled with both GCC and CLANG)
* <strong>FastFlow</strong> version >= 3.0 (https://github.com/fastflow/fastflow)
* <strong>CUDA</strong> (version >= 11.5 is preferred for using operators targeting GPUs)
* <strong>libtbb-dev</strong> required by GPU operators only
* <strong>libgraphviz-dev</strong> and <strong>rapidjson-dev</strong> when compiling with -DWF_TRACING_ENABLED to report statistics and to use the Web Dashboard for monitoring purposes
* <strong>librdkafka-dev</strong> for using the integration with Kafka (special Kafka_Source and Kafka_Sink operators)
* <strong>librocksdb-dev</strong> for using the suite of persistent operators keeping their internal state in RocksDB KVS
* <strong>doxygen</strong> (to generate the documentation)

<b>Important about the FastFlow dependency</b> — After downloading FastFlow, the user must configure the library for the target multi-core environment. By default, FastFlow pins its threads to the machine’s cores. To ensure FastFlow correctly recognizes the core ordering and their correspondence to CPUs and NUMA regions, it is essential to run the script <strong>"mapping_string.sh"</strong> (located in the <tt>fastflow/ff</tt> directory) once before compiling your WindFlow programs.

# Macros
WindFlow, and its underlying level FastFlow, come with some important macros that can be used during compilation to enable specific behaviors. Some of them are reported below:
* <strong>-DWF_TRACING_ENABLED</strong> -> enables tracing (logging) at the WindFlow level (operator replicas), and allows streaming applications to continuously report statistics to a Web Dashboard (which is a separate sub-project). Outputs are also written in log files at the end of the processing
* <strong>-DTRACE_FASTFLOW</strong> -> enables tracing (logging) at the FastFlow level (raw threads and FastFlow nodes). Outputs are written in log files at the end of the processing
* <strong>-DFF_BOUNDED_BUFFER</strong> -> enables the use of bounded lock-free queues for pointer passing between threads. Otherwise, queues are unbounded (no backpressure mechanism)
* <strong>-DDEFAULT_BUFFER_CAPACITY=VALUE</strong> -> set the size of the lock-free queues capacity. The default size of the queues is of 2048 entries
* <strong>-DNO_DEFAULT_MAPPING</strong> -> if this macro is enabled, FastFlow threads are not pinned onto CPU cores, but they are scheduled by the Operating System
* <strong>-DBLOCKING_MODE</strong> -> if this macro is enabled, FastFlow queues use the blocking concurrency mode (pushing to a full queue or polling from an empty queue might suspend the underlying thread). If not set, waiting conditions are implemented by busy-waiting spin loops.

Some macros are available to configure the runtime system when GPU operators are used in your application. By default, GPU support relies on explicit CUDA memory management and overlapped data transfers, a configuration suitable for a wide range of NVIDIA GPU models. However, developers may choose to enable an alternative implementation based on CUDA unified memory by compiling with the macro <strong>-DWF_GPU_UNIFIED_MEMORY</strong>. Alternatively, the runtime system can be configured to use pinned memory on NVIDIA System-on-Chip devices (e.g., Jetson Nano and Jetson Xavier, Jeson Orin), where pinned memory is directly accessible by both the CPU and GPU without additional data copies. This mode can be enabled by compiling with the macro <strong>-DWF_GPU_PINNED_MEMORY</strong>.

# Build the Examples
WindFlow is a header-only template library. To build your applications, you need to include the main library header file, <tt>windflow.hpp</tt>. When using operators that target GPUs, you must also include the <tt>windflow_gpu.hpp</tt> header file and compile your code with the <code>nvcc</code> CUDA compiler (or with <code>clang</code> configured for CUDA support). The source code in this repository provides several examples demonstrating how to use the API and explore the library’s advanced features. These examples are located in the <tt>tests</tt> folder. To compile them:
```
    $ cd <WINDFLOW_ROOT>
    $ mkdir ./build
    $ cd build
    $ cmake ..
    $ make -j<no_cores> # compile all the tests (not the doxygen documentation)
    $ make all_cpu -j<no_cores> # compile only CPU tests
    $ make all_gpu -j<no_cores> # compile only GPU tests
    $ make docs # generate the doxygen documentation (if doxygen has been installed)
```

To use the Kafka integration, which provides specialized Source and Sink operators, the developer must include the additional header <tt>kafka/windflow_kafka.hpp</tt> and link against the <tt>librdkafka-dev</tt> library. Similarly, to enable persistent operators, you need to include the header <tt>persistent/windflow_rocksdb.hpp</tt> and link the <tt>librocksdb-dev</tt> library.

# Docker Images
Two Docker images are available in the WindFlow GitHub repository. These images include all synthetic tests, precompiled and ready to run. To build the first image (which excludes tests involving GPU operators), execute the following commands:
```
    $ cd <WINDFLOW_ROOT>
    $ cd dockerimages
    $ docker build -t windflow_nogpu -f Dockerfile_nogpu .
    $ docker run windflow_nogpu ./bin/graph_tests/test_graph_1 -r 1 -l 10000 -k 10
```
The last command runs one of the synthetic experiments (<tt>test_graph_1</tt>). You can execute any of the compiled tests in the same manner.

The second image contains all synthetic tests with GPU operators. To use your GPU device with Docker, please follow the guidelines in the following page (https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/install-guide.html). Then, you can build the image and run the container as follows:
```
    $ cd <WINDFLOW_ROOT>
    $ cd dockerimages
    $ docker build -t windflow_gpu -f Dockerfile_gpu .
    $ docker run --gpus all windflow_gpu ./bin/graph_tests_gpu/test_graph_gpu_1 -r 1 -l 10000 -k 10
```
Again, the last command runs one of the synthetic experiments (<tt>test_graph_gpu_1</tt>). You can execute any of the compiled tests in the same manner.

# Web Dashboard
WindFlow provides its own Web Dashboard for profiling and monitoring the execution of running applications. The dashboard code is located in the <tt>WINDFLOW_ROOT/dashboard</tt> subfolder. It is implemented as a Java package using Spring for the web server and React for the front-end. To start the Web Dashboard, run the following commands:
```
    cd <WINDFLOW_ROOT>/dashboard/Server
    mvn spring-boot:run
```
The web server listens on the machine’s default port <tt>8080</tt>. To change the port or other configuration parameters, you can modify the file <tt>WINDFLOW_ROOT/dashboard/Server/src/main/resources/application.properties</tt> for the Spring server (e.g., to change the HTTP port), and the file <tt>WINDFLOW_ROOT/dashboard/Server/src/main/java/com/server/CustomServer/Configuration/config.json</tt> for the internal server that receives statistical reports from WindFlow applications (e.g., to change the port used by applications to send statistics to the dashboard).

WindFlow applications compiled with the macro <strong>-DWF_TRACING_ENABLED</strong> automatically attempt to connect to the Web Dashboard and report statistics every second. By default, the applications assume that the dashboard is running on the local machine. To change the hostname or port number, developers can define the macros <strong>WF_DASHBOARD_MACHINE=hostname/ip_addr</strong> and <strong>WF_DASHBOARD_PORT=port_number</strong>.

# About the License
Starting from version 3.1.0, WindFlow is released under a dual license: <strong>LGPL-3</strong> and <strong>MIT</strong>. Developers should verify the licenses of any third-party libraries used as dependencies.

# Cite our Work
In order to cite our work, we kindly ask interested people to use the following references:
```
@article{WindFlow,
 author={Mencagli, Gabriele and Torquati, Massimo and Cardaci, Andrea and Fais, Alessandra and Rinaldi, Luca and Danelutto, Marco},
 journal={IEEE Transactions on Parallel and Distributed Systems},
 title={WindFlow: High-Speed Continuous Stream Processing With Parallel Building Blocks},
 year={2021},
 volume={32},
 number={11},
 pages={2748-2763},
 doi={10.1109/TPDS.2021.3073970}
}
```

```
@article{WindFlow-GPU,
 title = {General-purpose data stream processing on heterogeneous architectures with WindFlow},
 journal = {Journal of Parallel and Distributed Computing},
 volume = {184},
 pages = {104782},
 year = {2024},
 issn = {0743-7315},
 doi = {https://doi.org/10.1016/j.jpdc.2023.104782},
 url = {https://www.sciencedirect.com/science/article/pii/S0743731523001521},
 author = {Gabriele Mencagli and Massimo Torquati and Dalvan Griebler and Alessandra Fais and Marco Danelutto},
}
```

# Requests for Modifications
If you are using WindFlow for your purposes and you are interested in specific modifications of the API (or of the runtime system), please send an email to the maintainer.

# Contributors
The main developer and maintainer of WindFlow is [Gabriele Mencagli](mailto:gabriele.mencagli@unipi.it) (Department of Computer Science, University of Pisa, Italy).
