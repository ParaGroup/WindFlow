[![release](https://img.shields.io/github/release/paragroup/windflow.svg)](https://github.com/paragroup/windflow/releases/latest)
[![HitCount](http://hits.dwyl.io/paragroup/windflow.svg)](http://hits.dwyl.io/paragroup/windflow)

# WindFlow

WindFlow is a C++17 library for parallel data stream processing applications targeting heterogeneous shared-memory architectures featuring multi-core CPUs and GPU devices. The library provides common stream processing operators like map, flatmap, filter, fold/reduce as well as sliding-window operators designed with complex parallel features. Such operators are called <b>patterns</b> in the library, where each pattern is an instance of a class that can be built and connected with other pattern instances to create data-flow graphs. Applications are built through the <b>MultiPipe</b> programming construct used to create parallel pipelines that can be run on the system.

The web site of the library is available at https://paragroup.github.io/WindFlow/.

# Contributors
The main developer and maintainer of WindFlow is [Gabriele Mencagli](mailto:mencagli@di.unipi.it).
