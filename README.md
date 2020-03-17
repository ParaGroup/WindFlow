[![release](https://img.shields.io/github/release/paragroup/windflow.svg)](https://github.com/paragroup/windflow/releases/latest)
[![HitCount](http://hits.dwyl.io/paragroup/windflow.svg)](http://hits.dwyl.io/paragroup/windflow)

# WindFlow

WindFlow is a C++17 library for parallel data stream processing applications targeting heterogeneous shared-memory architectures featuring multi-core CPUs and GPU devices. The library provides common stream processing operators like map, flatmap, filter, fold/reduce as well as sliding-window operators designed with complex parallel features. Applications are built through the <b>MultiPipe</b> and the <b>PipeGraph</b> programming constructs. The first is used to create parallel pipelines, while the second one allows several <b>MultiPipe</b> instances to be interconnected through <b>merge</b> and <b>split</b> operations.

The web site of the library is available at https://paragroup.github.io/WindFlow/.

# Dependencies
The library needs the following dependencies:
* <strong>GCC</strong> (GNU Compiler Collection) version >= 7.2
* <strong>CUDA</strong> >= 9 (for compiling GPU examples)
* <strong>FastFlow</strong> version >= 3.0 (https://github.com/fastflow/fastflow)

When downloaded FastFlow, it is important to properly configure the library. By default, FastFlow applies pinning of its threads onto the cores of the machine and this must be done correctly. To be sure of the ordering of cores, and to place communicating threads on sibling cores, it is important to run the script <strong>"mapping_string.sh"</strong> in the folder <tt>fastflow/ff</tt> before compiling any code using the library.

# Macros
WindFlow and its underlying level FastFlow come with some important macros that can be used during compilation to enable specific behaviors:
* <strong>-DTRACE_WINDFLOW</strong> -> enables the tracing (logging) at the WindFlow level (operator replicas)
* <strong>-DTRACE_FASTFLOW</strong> -> enables the tracing (logging) at the FastFlow level (raw threads and FastFlow nodes)
* <strong>-DFF_BOUNDED_BUFFER</strong> -> enables the use of bounded lock-free queues for pointer passing between threads. Otherwise, queues are unbounded (no backpressure mechanism)
* <strong>-DDEFAULT_BUFFER_CAPACITY=VALUE</strong> -> set the size of the lock-free queues capacity in terms of pointers
* <strong>-DNO_DEFAULT_MAPPING</strong> -> if set, FastFlow threads are not pinned onto the CPU cores but they are scheduled by the standard OS scheduling policy.

# Build the Examples
WindFlow is a header-only template library. To build your applications you have to include the main header of the library (<tt>windflow.hpp</tt>). For using the GPU operators, you further have to include <tt>windflow_gpu.hpp</tt>. To compile the examples:
* <strong>make</strong> -> generate all the examples
* <strong>make all_cpu</strong> -> generate only the examples with operators running on CPU
* <strong>male all_gpu</strong> -> generate only the examples with operators running on GPU

# Contributors
The main developer and maintainer of WindFlow is [Gabriele Mencagli](mailto:mencagli@di.unipi.it) (Department of Computer Science, University of Pisa, Italy).
