[![License: LGPL v3](https://img.shields.io/badge/License-LGPL%20v3-blue.svg)](https://www.gnu.org/licenses/lgpl-3.0)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Release](https://img.shields.io/github/release/paragroup/windflow.svg)](https://github.com/paragroup/windflow/releases/latest)
[![Hits](https://hits.seeyoufarm.com/api/count/incr/badge.svg?url=https%3A%2F%2Fgithub.com%2FParaGroup%2FWindFlow&count_bg=%2379C83D&title_bg=%23555555&icon=&icon_color=%23E7E7E7&title=hits&edge_flat=false)](https://hits.seeyoufarm.com)
[![Say Thanks!](https://img.shields.io/badge/Say%20Thanks-!-1EAEDB.svg)](https://saythanks.io/to/mencagli@di.unipi.it)
[![Donate](https://img.shields.io/badge/Donate-PayPal-green.svg)](https://paypal.me/GabrieleMencagli)

<p align="center"><img src="https://paragroup.github.io/WindFlow/img/logo_white.png" width="400" title="WindFlow Logo"></p>

# Introduction
WindFlow is a C++17 header-only library for parallel data stream processing targeting heterogeneous shared-memory architectures equipped with multi-core CPUs and NVIDIA GPUs. The library provides traditional stream processing operators like map, flatmap, filter, fold/reduce as well as sliding-window operators. The API allows building streaming applications through the <b>MultiPipe</b> and the <b>PipeGraph</b> programming constructs. The first is used to create parallel pipelines (with shuffle connections), while the second allows several <b>MultiPipe</b> instances to be interconnected through <b>merge</b> and <b>split</b> operations, in order to create complex directed acyclic graphs of interconnected operators.

As for the existing popular streaming engines like Apache Storm and FLink, WindFlow supports general-purpose streaming applications by enabling operators to run user-defined code. In terms of runtime system, WindFlow is suitable for embedded architectures equipped with low-power multi-core CPUs and integrated NVIDIA GPUs (like the Jetson family of NVIDIA boards). However, it works also on traditional multi-core servers equipped with discrete NVIDIA GPUs.

The web site of the library is available at: https://paragroup.github.io/WindFlow/.

# Dependencies
The library requires the following dependencies:
* <strong>a C++ compiler</strong> with full support for C++17 (WindFlow tests have been successfully compiled with both GCC and CLANG)
* <strong>FastFlow</strong> version >= 3.0 (https://github.com/fastflow/fastflow)
* <strong>CUDA</strong> (for using operators targeting GPUs)
* <strong>libtbb-dev</strong> required by GPU operators only
* <strong>libgraphviz-dev</strong> and <strong>rapidjson-dev</strong> when compiling with -DWF_TRACING_ENABLED to report statistics and using the Web Dashboard
* <strong>librdkafka-dev</strong> for using the integration with Kafka (special Kafka_Source and Kafka_Sink operators)
* <strong>doxygen</strong> (to generate the documentation)

<b>Important about the FastFlow dependency</b> -> after downloading FastFlow, the user needs to configure the library for the underlying multi-core environment. By default, FastFlow pins its threads onto the cores of the machine. To make FastFlow aware of the ordering of cores, and their correspondence in CPUs and NUMA regions, it is important to run (just one time) the script <strong>"mapping_string.sh"</strong> in the folder <tt>fastflow/ff</tt> before compiling your WindFlow programs.

# Macros
WindFlow, and its underlying level FastFlow, come with some important macros that can be used during compilation to enable specific behaviors:
* <strong>-DWF_TRACING_ENABLED</strong> -> enables tracing (logging) at the WindFlow level (operator replicas), and allows streaming applications to continuously report statistics to a Web Dashboard (which is a separate sub-project). Outputs are also written in log files at the end of the processing
* <strong>-DTRACE_FASTFLOW</strong> -> enables tracing (logging) at the FastFlow level (raw threads and FastFlow nodes). Outputs are written in log files at the end of the processing
* <strong>-DFF_BOUNDED_BUFFER</strong> -> enables the use of bounded lock-free queues for pointer passing between threads. Otherwise, queues are unbounded (no backpressure mechanism)
* <strong>-DDEFAULT_BUFFER_CAPACITY=VALUE</strong> -> set the size of the lock-free queues capacity. The default size of the queues is of 2048 entries. We suggest the users to significantly reduce this size in applications that use GPU operators (e.g., using values between 16 to 128 depending on the available GPU memory)
* <strong>-DNO_DEFAULT_MAPPING</strong> -> if set, FastFlow threads are not pinned onto CPU cores, but they are scheduled by the Operating System
* <strong>-DBLOCKING_MODE</strong> -> if set, FastFlow queues use the blocking concurrency mode (pushing to a full queue or polling from an empty queue might suspend the underlying thread). If not set, waiting conditions are implemented by busy-waiting spin loops.

Some macros are useful to configure the run-time system when GPU operators are utilized in your application. The default version of the GPU support is based on explicit CUDA memory management and overlapped data transfers, which is a version suitable for a wide range of NVIDIA GPU models. However, the developer might want to switch to a different implementation that makes use of the CUDA unified memory support. This can be done by compiling with the macro <strong>-DWF_GPU_UNIFIED_MEMORY</strong>. Alternatively, the user can configure the runtime system to use pinned memory on NVIDIA System-on-Chip devices (e.g., Jetson Nano and Jetson Xavier), where pinned memory is directly accessed by CPU and GPU without extra copies. This can be done by compiling with the macro <strong>-DWF_GPU_PINNED_MEMORY</strong>.

# Build the Examples
WindFlow is a header-only template library. To build your applications you have to include the main header of the library (<tt>windflow.hpp</tt>). For using the operators targeting GPUs, you further have to include the <tt>windflow_gpu.hpp</tt> header file and compile using the <code>nvcc</code> CUDA compiler (or through <code>clang</code> with CUDA support). The source code in this repository includes several examples that can be used to understand the use of the API and the advanced features of the library. The examples can be found in the <tt>tests</tt> folder. To compile them:
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

In order to use Kafka integration, done with special Source and Sink operators, the developer has to include the additional header <tt>kafka/windflow_kafka.hpp</tt> and properly link the library <tt>librdkafka-dev</tt>.

# Docker Images
Two Docker images are available in the WindFlow GitHub repository. The images contain all the synthetic tests compiled and ready to be executed. To build the first image (the one without tests using GPU operators) execute the following commands:
```
    $ cd <WINDFLOW_ROOT>
    $ cd dockerimages
    $ docker build -t windflow_nogpu -f Dockerfile_nogpu .
    $ docker run windflow_nogpu ./bin/graph_tests/test_graph_1 -r 1 -l 10000 -k 10
```
The last command executes one of the synthetic experiments (test_graph_1). You can execute any of the compiled tests in the same mannner.

The second image contains all synthetic tests with GPU operators. To use your GPU device with Docker, please follow the guidelines in the following page (https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/install-guide.html). Then, you can build the image and run the container as follows:
```
    $ cd <WINDFLOW_ROOT>
    $ cd dockerimages
    $ docker build -t windflow_gpu -f Dockerfile_gpu .
    $ docker run --gpus all windflow_gpu ./bin/graph_tests_gpu/test_graph_gpu_1 -r 1 -l 10000 -k 10
```
Again, the last command executes one of the synthetic experiments (test_graph_gpu_1). You can execute any of the compiled tests in the same mannner.

# Web Dashboard
WindFlow has its own Web Dashboard that can be used to profile the execution of running WindFlow applications. The dashboard code is in the sub-folder <tt>WINDFLOW_ROOT/dashboard</tt>. It is a Java package based on Spring (for the Web Server) and developed using React for the front-end part. To start the Web Dashboard run the following commands:
```
    cd <WINDFLOW_ROOT>/dashboard/Server
    mvn spring-boot:run
```
The web server listens on the default port <tt>8080</tt> of the machine. To change the port, and other configuration parameters, users can modify the configuration file <tt>WINDFLOW_ROOT/dashboard/Server/src/main/resources/application.properties</tt> for the Spring server (e.g., to change the HTTP port), and the file <tt>WINDFLOW_ROOT/dashboard/Server/src/main/java/com/server/CustomServer/Configuration/config.json</tt> for the internal server receiving reports of statistics from the WindFlow applications (e.g., to change the port used by applications to report statistics to the dashboard).

WindFlow applications compiled with the macro <strong>-DWF_TRACING_ENABLED</strong> try to connect to the Web Dashboard and report statistics to it every second. By default, the applications assume that the dashboard is running on the local machine. To change the hostname and the port number, developers can use the macros <strong>WF_DASHBOARD_MACHINE=hostname/ip_addr</strong> and <strong>WF_DASHBOARD_PORT=port_number</strong>.

# About the License
From version 3.1.0, WindFlow is released with a double license: <strong>LGPL-3</strong> and <strong>MIT</strong>. Programmers should check the licenses of the other libraries used as dependencies.

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
@inproceedings{WindFlow-GPU,
 author={Mencagli, Gabriele and Griebler, Dalvan and Danelutto, Marco},
 booktitle={2022 30th Euromicro International Conference on Parallel, Distributed and Network-based Processing (PDP)}, 
 title={Towards Parallel Data Stream Processing on System-on-Chip CPU+GPU Devices}, 
 year={2022},
 volume={},
 number={},
 pages={34-38},
 doi={10.1109/PDP55904.2022.00014}
}
```

# Requests for Modifications
If you are using WindFlow for your purposes and you are interested in specific modifications of the API (or of the runtime system), please send an email to the maintainer.

# Contributors
The main developer and maintainer of WindFlow is [Gabriele Mencagli](mailto:gabriele.mencagli@unipi.it) (Department of Computer Science, University of Pisa, Italy).
