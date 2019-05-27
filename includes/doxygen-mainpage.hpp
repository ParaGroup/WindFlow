/*! \mainpage WindFlow Library (version 1.0)
 * 
 * \section intro_sec Introduction
 * 
 * WindFlow is a C++17 library for parallel data stream processing applications 
 * targeting heterogeneous shared-memory architectures featuring multi-core CPUs
 * and GPU devices. The library provides common stream processing operators like
 * map, flatmap, filter, fold/reduce as well as sliding-window operators designed with
 * complex parallel features. Such operators are called patterns in the library, where
 * each pattern is an instance of a class that can be built and connected with other
 * pattern instances to create data-flow graphs. Applications are built through the
 * MultiPipe programming construct used to create parallel pipelines that can be run
 * on the system.
 */