[![License: LGPL v3](https://img.shields.io/badge/License-LGPL%20v3-blue.svg)](https://www.gnu.org/licenses/lgpl-3.0)
[![release](https://img.shields.io/github/release/paragroup/windflow.svg)](https://github.com/paragroup/windflow/releases/latest)
[![HitCount](http://hits.dwyl.io/paragroup/windflow.svg)](http://hits.dwyl.io/paragroup/windflow)
[![Say Thanks!](https://img.shields.io/badge/Say%20Thanks-!-1EAEDB.svg)](https://saythanks.io/to/mencagli@di.unipi.it)
[![Donate](https://img.shields.io/badge/Donate-PayPal-green.svg)](https://paypal.me/GabrieleMencagli)

<p align="center"><img src="https://paragroup.github.io/WindFlow/logo/logo_white.png" width="400" title="WindFlow Logo"></p>

# Introduction
WindFlow is a C++17 library for parallel data stream processing targeting heterogeneous shared-memory architectures featuring multi-core CPUs and GPU devices. The library provides common stream processing operators like map, flatmap, filter, fold/reduce as well as sliding-window operators designed with complex parallel features. The API allows building streaming applications through the <b>MultiPipe</b> and the <b>PipeGraph</b> programming constructs. The first is used to create parallel pipelines, while the second one allows several <b>MultiPipe</b> instances to be interconnected through <b>merge</b> and <b>split</b> operations, thus creating complex directed acyclic graphs of interconnected operators.

The web site of the library is available at https://paragroup.github.io/WindFlow/.

# Dependencies
The library needs the following dependencies:
* <strong>GCC</strong> (GNU Compiler Collection) with support for at least C++14 (recommended C++17)
* <strong>CUDA</strong> (for compiling GPU examples) with support for C++14
* <strong>FastFlow</strong> version >= 3.0 (https://github.com/fastflow/fastflow)
* <strong>libgraphviz-dev</strong> (only when compiling with -DGRAPHVIZ_WINDFLOW)
* <strong>doxygen</strong> (if you need to generate the documentation)

When downloaded FastFlow, it is important to properly configure it for your multi-core environment. By default, FastFlow applies pinning of its threads onto the cores of the machine. To be sure of the ordering of cores, and to place communicating threads on sibling cores, it is important to run the script <strong>"mapping_string.sh"</strong> in the folder <tt>fastflow/ff</tt> before compiling any code.

# Macros
WindFlow and its underlying level FastFlow come with some important macros that can be used during compilation to enable specific behaviors:
* <strong>-DTRACE_WINDFLOW</strong> -> enables tracing (logging) at the WindFlow level (operator replicas)
* <strong>-DGRAPHVIZ_WINDFLOW</strong> -> if set, it allows the generation of a DOT representation of the PipeGraph (.gv and .pdf files are generated with the <tt>dump_DOTGraph()</tt> method)
* <strong>-DTRACE_FASTFLOW</strong> -> enables tracing (logging) at the FastFlow level (raw threads and FastFlow nodes)
* <strong>-DFF_BOUNDED_BUFFER</strong> -> enables the use of bounded lock-free queues for pointer passing between threads (the default size of the queues is 2048). Otherwise, queues are unbounded (no backpressure mechanism)
* <strong>-DDEFAULT_BUFFER_CAPACITY=VALUE</strong> -> set the size of the lock-free queues capacity in terms of pointers to objects
* <strong>-DNO_DEFAULT_MAPPING</strong> -> if set, FastFlow threads are not pinned onto the CPU cores but they are scheduled by the standard OS scheduling policy.

# Build the Examples
WindFlow is a header-only template library. To build your applications you have to include the main header of the library (<tt>windflow.hpp</tt>). For using the GPU operators, you further have to include the <tt>windflow_gpu.hpp</tt> header file. The source code in this repository includes several examples that can be used to understand the use of the API and the advanced features of the library. The examples can be found in the <tt>tests</tt> folder. To compile them:
```
    cd <WINDFLOW_ROOT>
    mkdir ./build
    cd build; cmake ../
    make -j<#cores> # compile all the tests (not the doxygen documentation)
    make all_cpu -j<#cores> # compile only CPU tests
    make all_gpu -j<#cores> # compile only GPU tests
    make docs # generate the doxygen documentation
```
All the examples compile with <tt>gcc</tt> at least version <tt>7.5.0</tt> (with full support to C++17). The examples for GPU need <tt>CUDA</tt> at least version <tt>9.0</tt> with support for C++14 (C++17 is not currently supported by CUDA).

# Contributors
The main developer and maintainer of WindFlow is [Gabriele Mencagli](mailto:mencagli@di.unipi.it) (Department of Computer Science, University of Pisa, Italy).
