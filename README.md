[![release](https://img.shields.io/github/release/paragroup/windflow.svg)](https://github.com/paragroup/windflow/releases/latest)
[![HitCount](http://hits.dwyl.io/paragroup/windflow.svg)](http://hits.dwyl.io/paragroup/windflow)

# WindFlow

WindFlow is a C++17 library for parallel data stream processing applications targeting heterogeneous shared-memory architectures featuring multi-core CPUs and GPU devices. The library provides common stream processing operators like map, flatmap, filter, fold/reduce as well as sliding-window operators designed with complex parallel features. Applications are built through the <b>MultiPipe</b> and the <b>PipeGraph</b> programming constructs. The first is used to create parallel pipelines, while the second one allows several <b>MultiPipe</b> instances to be interconnected through <b>merge</b> and <b>split</b> operations.

The web site of the library is available at https://paragroup.github.io/WindFlow/.

# Contributors
The main developer and maintainer of WindFlow is [Gabriele Mencagli](mailto:mencagli@di.unipi.it).
