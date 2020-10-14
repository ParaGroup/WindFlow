[![License: LGPL v3](https://img.shields.io/badge/License-LGPL%20v3-blue.svg)](https://www.gnu.org/licenses/lgpl-3.0)
[![release](https://img.shields.io/github/release/paragroup/windflow.svg)](https://github.com/paragroup/windflow/releases/latest)
[![HitCount](http://hits.dwyl.io/paragroup/windflow.svg)](http://hits.dwyl.io/paragroup/windflow)
[![Say Thanks!](https://img.shields.io/badge/Say%20Thanks-!-1EAEDB.svg)](https://saythanks.io/to/mencagli@di.unipi.it)
[![Donate](https://img.shields.io/badge/Donate-PayPal-green.svg)](https://paypal.me/GabrieleMencagli)

<p align="center"><img src="https://paragroup.github.io/WindFlow/logo/logo_white.png" width="400" title="WindFlow Logo"></p>

# Introduction
WindFlow is a C++17 library for parallel data stream processing targeting heterogeneous shared-memory architectures equipped with multi-core CPUs and GPUs. The library provides common stream processing operators like map, flatmap, filter, fold/reduce as well as sliding-window operators designed with complex parallel processing methods. The API allows building streaming applications through the <b>MultiPipe</b> and the <b>PipeGraph</b> programming constructs. The first is used to create parallel pipelines, while the second one allows several <b>MultiPipe</b> instances to be interconnected through <b>merge</b> and <b>split</b> operations, thus creating complex directed acyclic graphs of interconnected operators.

The web site of the library is available at: https://paragroup.github.io/WindFlow/.

# Dependencies
The library needs the following dependencies:
* <strong>GNU C/C++ compiler</strong> with support for at least C++14 (recommended C++17)
* <strong>CUDA</strong> (for compiling GPU examples) with support for at least C++14 (recommended C++17)
* <strong>FastFlow</strong> version >= 3.0 (https://github.com/fastflow/fastflow)
* <strong>libgraphviz-dev</strong> and <strong>rapidjson-dev</strong> (when compiling with -DTRACE_WINDFLOW)
* <strong>doxygen</strong> (to generate the documentation)

When downloaded FastFlow, the user needs to properly configure the library for the underlying multi-core environment. By default, FastFlow pins its threads onto the cores of the machine. To be sure of the ordering of cores, and to place communicating threads on sibling cores, it is important to run the script <strong>"mapping_string.sh"</strong> in the folder <tt>fastflow/ff</tt> before compiling any code using WindFlow/FastFlow.

# Macros
WindFlow and its underlying level FastFlow come with some important macros that can be used during compilation to enable specific behaviors:
* <strong>-DTRACE_WINDFLOW</strong> -> enables tracing (logging) at the WindFlow level (operator replicas), and allows the WindFlow application to continuously report its statistics to the Web Dashboard (if it is running)
* <strong>-DTRACE_FASTFLOW</strong> -> enables tracing (logging) at the FastFlow level (raw threads and FastFlow nodes)
* <strong>-DFF_BOUNDED_BUFFER</strong> -> enables the use of bounded lock-free queues for pointer passing between threads. Otherwise, queues are unbounded (no backpressure mechanism)
* <strong>-DDEFAULT_BUFFER_CAPACITY=VALUE</strong> -> set the size of the lock-free queues capacity in terms of pointers to objects (the default size of the queues is of 2048 entries)
* <strong>-DNO_DEFAULT_MAPPING</strong> -> if set, FastFlow threads are not pinned onto the CPU cores but they are scheduled by the standard OS scheduling policy

# Build the Examples
WindFlow is a header-only template library. To build your applications you have to include the main header of the library (<tt>windflow.hpp</tt>). For using the GPU operators, you further have to include the <tt>windflow_gpu.hpp</tt> header file. The source code in this repository includes several examples that can be used to understand the use of the API and the advanced features of the library. The examples can be found in the <tt>tests</tt> folder. To compile them:
```
    cd <WINDFLOW_ROOT>
    mkdir ./build
    cd build; cmake ../
    make -j<#cores> # compile all the tests (not the doxygen documentation)
    make all_cpu -j<#cores> # compile only CPU tests
    make all_gpu -j<#cores> # compile only GPU tests
    make docs # generate the doxygen documentation (if doxygen has been installed previously)
```
WindFlow makes use of <tt>std::optional</tt> in its source code. So, it is compliant with the C++17 standard, where optionals have officially been included in the standard. However, it is possible to compile the headers of the library with a compiler supporting C++14 (where optionals are still experimental). In the <tt>tests</tt> folder:
* CPU examples are written to be compiled with a compiler supporting C++17. This reflects in the way the builder classes to instantiate operators have been used, where their template arguments are not explicitly specified (owing to the Class Template Argument Deduction feature of C++17). To compile with C++14 you have to change the use of the buiders by providing the template arguments explicitly;
* GPU examples are written to be compiled with CUDA (NVCC) compiler supporting at least C++14. In this case, builders are used by explicitly providing their template arguments, resulting in a more verbose syntax. GPU examples can be easily converted in a C++17 style and compiled with CUDA (>= 11).

Tests seem to compile well also using <tt>clang</tt>.

# Web Dashboard
From the release <tt>2.8.8</tt>, WindFlow has its own Web Dashboard used to monitor and profile the execution of WindFlow applications. The dashboard code is in the sub-folder <tt>WINDFLOW_ROOT/dashboard</tt>. It is a java package (requiring at least <tt>Java 11</tt>) based on Spring (for the Web Server) and programmed using React for the front-end. To start the Web Dashboard use the following commands:
```
    cd <WINDFLOW_ROOT>/dashboard/Server
    mvn spring-boot:run
```
The web server will listen on the port <tt>8080</tt> of the machine. To change the port, and other configuration settings, users can modify the configuration file <tt>WINDFLOW_ROOT/dashboard/Server/src/main/resources/application.properties</tt> for the Spring server, and the file <tt>WINDFLOW_ROOT/dashboard/Server/src/main/java/com/server/CustomServer/Configuration/config.json</tt> for the internal server receiving reports of statistics from the connected WindFlow applications.

WindFlow applications compiled with the macro TRACE_WINDFLOW will try to connect to the Web Dashboard and to report statistics to it every second. By default, the applications assume that the dashboard is running on the local machine. To change the hostname and port number to connect to the dashboard, developers should compile the WindFlow application with the macros <strong>DASHBOARD_MACHINE=hostname/ip_addr</strong> and <strong>DASHBOARD_PORT=port_number</strong>.

# About the License
WindFlow and FastFlow are released with the <strong>LGPL-3</strong> license and they are both header-only libraries. So, any developer who wants to use these libraries for her applications must honor Section 3 of the LGPL (she should mention "prominently" that her application uses WindFlow/FastFlow and linking the LGPL text somewhere). Please be careful that, if compiled with the TRACE_WINDFLOW macro, WindFlow needs libgraphviz and librapidjson-dev (authors should check the compatibility with their license). Furthermore, the Web Dashboard has its own dependencies, and their licenses should be checked carefully. However, the dashboard is useful for monitoring and debugging activities, and it is not used if WindFlow applications are not compiled with the TRACE_WINDFLOW macro.

# Contributors
The main developer and maintainer of WindFlow is [Gabriele Mencagli](mailto:mencagli@di.unipi.it) (Department of Computer Science, University of Pisa, Italy).
