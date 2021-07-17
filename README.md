[![License: LGPL v3](https://img.shields.io/badge/License-LGPL%20v3-blue.svg)](https://www.gnu.org/licenses/lgpl-3.0)
[![release](https://img.shields.io/github/release/paragroup/windflow.svg)](https://github.com/paragroup/windflow/releases/latest)
[![Hits](https://hits.seeyoufarm.com/api/count/incr/badge.svg?url=https%3A%2F%2Fgithub.com%2FParaGroup%2FWindFlow&count_bg=%2379C83D&title_bg=%23555555&icon=&icon_color=%23E7E7E7&title=hits&edge_flat=false)](https://hits.seeyoufarm.com)
[![Say Thanks!](https://img.shields.io/badge/Say%20Thanks-!-1EAEDB.svg)](https://saythanks.io/to/mencagli@di.unipi.it)
[![Donate](https://img.shields.io/badge/Donate-PayPal-green.svg)](https://paypal.me/GabrieleMencagli)

<p align="center"><img src="https://paragroup.github.io/WindFlow/img/logo_white.png" width="400" title="WindFlow Logo"></p>

# Introduction
WindFlow is a C++17 library for parallel data stream processing targeting heterogeneous shared-memory architectures equipped with multi-core CPUs and an NVIDIA GPUs. The library provides traditional stream processing operators like map, flatmap, filter, fold/reduce as well as sliding-window operators designed with complex parallel processing modes. The API allows building streaming applications through the <b>MultiPipe</b> and the <b>PipeGraph</b> programming constructs. The first is used to create parallel pipelines, while the second one allows several <b>MultiPipe</b> instances to be interconnected through <b>merge</b> and <b>split</b> operations, thus creating complex directed acyclic graphs of interconnected operators.

WindFlow is not thought to support streaming analytics applications only (e.g., the ones written with relational algebra query languages, as in traditional old-style DSMSs) but rather all general-purpose streaming applications can be easily supported through operators embedding user-defined custom logics. In terms of runtime system, WindFlow is particularly suitable for embedded architectures equipped with low-power multi-core CPUs and integrated NVIDIA GPUs (i.e., Jetson boards). However, it works well also on traditional multi-core servers equipped with discrete NVIDIA GPUs (e.g., Pascal/Volta models). Differently from existing research libraries for stream processing on multicores, WindFlow is thought to support real live-streaming applications, where inputs are continuously received from real-world sources, and not only offline streaming applications reading historical data already prepared in memory.

The web site of the library is available at: https://paragroup.github.io/WindFlow/.

# Dependencies
The library needs the following dependencies:
* <strong>a C++ compiler</strong> with full support to C++17 (WindFlow tests have been successfully compiled with both GCC and CLANG)
* <strong>CUDA</strong> (for using operators targeting GPUs) with support for C++17 (CUDA >= 11)
* <strong>libtbb-dev</strong> for using efficient concurrent containers needed by the operators targeting GPUs
* <strong>FastFlow</strong> version >= 3.0 (https://github.com/fastflow/fastflow)
* <strong>libgraphviz-dev</strong> and <strong>rapidjson-dev</strong> (when compiling with -DWF_TRACING_ENABLED to report statistics and using the Web Dashboard)
* <strong>doxygen</strong> (to generate the documentation)

After downloading FastFlow, the user needs to properly configure the library for the underlying multi-core environment. By default, FastFlow pins its threads onto the cores of the machine. To be sure of the ordering of cores, and to place communicating threads on sibling cores, it is important to run the script <strong>"mapping_string.sh"</strong> in the folder <tt>fastflow/ff</tt> before compiling any code using WindFlow.

# Macros
WindFlow, and its underlying level FastFlow, come with some important macros that can be used during compilation to enable specific behaviors:
* <strong>-DWF_TRACING_ENABLED</strong> -> enables tracing (logging) at the WindFlow level (operator replicas), and allows WindFlow applications to continuously report statistics to a Web Dashboard (which is a separate sub-project). Outputs are also written in log files at the end of the processing
* <strong>-DTRACE_FASTFLOW</strong> -> enables tracing (logging) at the FastFlow level (raw threads and FastFlow nodes). Outputs are written in log files at the end of the processing
* <strong>-DFF_BOUNDED_BUFFER</strong> -> enables the use of bounded lock-free queues for pointer passing between threads. Otherwise, queues are unbounded (no backpressure mechanism)
* <strong>-DDEFAULT_BUFFER_CAPACITY=VALUE</strong> -> set the size of the lock-free queues capacity in terms of pointers to objects. The default size of the queues is of 2048 entries. We suggest users to greatly reduce this size in applications using operators targeting GPUs (e.g., using sizes between 16 to 128 depending on the size of the input tuples and the batch size used)
* <strong>-DNO_DEFAULT_MAPPING</strong> -> if set, FastFlow threads are not pinned onto the CPU cores and are scheduled by the standard OS scheduling policy

# Build the Examples
WindFlow is a header-only template library. To build your applications you have to include the main header of the library (<tt>windflow.hpp</tt>). For using the operators targeting GPUs, you further have to include the <tt>windflow_gpu.hpp</tt> header file and compile using the <code>nvcc</code> CUDA compiler. The source code in this repository includes several examples that can be used to understand the use of the API and the advanced features of the library. The examples can be found in the <tt>tests</tt> folder. To compile them:
```
    cd <WINDFLOW_ROOT>
    mkdir ./build
    cd build; cmake ../
    make -j<#cores> # compile all the tests (not the doxygen documentation)
    make all_cpu -j<#cores> # compile only CPU tests
    make all_gpu -j<#cores> # compile only GPU tests
    make docs # generate the doxygen documentation (if doxygen has been installed)
```

# Web Dashboard
WindFlow has its own Web Dashboard used to monitoring and profiling the execution of running WindFlow applications. The dashboard code is in the sub-folder <tt>WINDFLOW_ROOT/dashboard</tt>. It is a Java package based on Spring (for the Web Server) and developed using React for the front-end part. To start the Web Dashboard run the following commands:
```
    cd <WINDFLOW_ROOT>/dashboard/Server
    mvn spring-boot:run
```
The web server listens on the default port <tt>8080</tt> of the machine. To change the port, and some other configuration settings, users can modify the configuration file <tt>WINDFLOW_ROOT/dashboard/Server/src/main/resources/application.properties</tt> for the Spring server (e.g., the HTTP port for connecting from a browser), and the file <tt>WINDFLOW_ROOT/dashboard/Server/src/main/java/com/server/CustomServer/Configuration/config.json</tt> for the internal server receiving reports of statistics from the connected WindFlow applications (e.g., the port used by WindFlow applications to report statistics to the dashboard).

WindFlow applications compiled with the macro <strong>-DWF_TRACING_ENABLED</strong> try to connect to the Web Dashboard and report statistics to it every second. By default, the applications assume that the dashboard is running on the local machine. To change the hostname and port number used to connect to the dashboard, developers should compile the WindFlow application with the macros <strong>WF_DASHBOARD_MACHINE=hostname/ip_addr</strong> and <strong>WF_DASHBOARD_PORT=port_number</strong>.

# About the License
WindFlow and FastFlow are released with the <strong>LGPL-3</strong> license and they are both header-only libraries. Programmers should check the licenses of the other libraries used as dependencies.

# Cite our Work
In order to cite our work, we kindly ask interested people to use the following reference:
```
@article{WF_Paper,
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

# Contributors
The main developer and maintainer of WindFlow is [Gabriele Mencagli](mailto:mencagli@di.unipi.it) (Department of Computer Science, University of Pisa, Italy).
