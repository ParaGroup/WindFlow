[![release](https://img.shields.io/github/release/paragroup/windflow.svg)](https://github.com/paragroup/windflow/releases/latest)
[![HitCount](http://hits.dwyl.io/paragroup/windflow.svg)](http://hits.dwyl.io/paragroup/windflow)

# WindFlow

WindFlow is a C++17 library for parallel data stream processing applications targeting heterogeneous shared-memory architectures featuring multi-core CPUs and GPU devices. The library provides common stream processing operators like map, flatmap, filter, fold/reduce as well as sliding-window operators designed with complex parallel features. Applications are built through the <b>MultiPipe</b> and the <b>PipeGraph</b> programming constructs. The first is used to create parallel pipelines, while the second one allows several <b>MultiPipe</b> instances to be interconnected through <b>merge</b> and <b>split</b> operations.

The web site of the library is available at https://paragroup.github.io/WindFlow/.

# Dependencies
The library needs the following dependencies:
* GCC (GNU Compiler Collection) version >= 7.2
* CUDA >= 9 (for compiling GPU examples)
* FastFlow version >= 3.0

# Build the Examples
WindFlow is a header-only template library. To build your applications you have to include the main header of the library (windflow.hpp). For using the GPU operators you further have to include windflow_gpu.hpp. To compile the examples provided alongside the library:
* make -> generate all the examples
* make all_cpu -> generate only the examples with operators running on CPU
* male all_gpu -> generate only the examples with operators running on GPU

# Contributors
The main developer and maintainer of WindFlow is [Gabriele Mencagli](mailto:mencagli@di.unipi.it).
