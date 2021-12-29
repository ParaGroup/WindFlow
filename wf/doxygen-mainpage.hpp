/*! \mainpage WindFlow Library
 * 
 * \section intro_sec Introduction
 * 
 * WindFlow is a C++17 library for parallel data stream processing targeting heterogeneous shared-memory architectures
 * equipped with multi-core CPUs and NVIDIA GPUs. The library provides traditional stream processing operators like map,
 * flatmap, filter, fold/reduce as well as sliding-window operators. The API allows building streaming applications through
 * the MultiPipe and the PipeGraph programming constructs. The first is used to create parallel pipelines, while the second
 * one allows several MultiPipe instances to be interconnected through merge and split operations, in ordee to create complex
 * directed acyclic graphs of interconnected operators.
 * 
 * WindFlow does not support streaming analytics applications only (e.g., the ones written with relational algebra query languages,
 * as in traditional old-style DSMSs), but rather all general-purpose streaming applications can easily be supported through operators
 * with user-defined custom logics. In terms of runtime system, WindFlow is particularly suitable for embedded architectures equipped
 * with low-power multi-core CPUs and integrated NVIDIA GPUs (i.e., Jetson boards). However, it works well also on traditional multi-core
 * servers equipped with discrete NVIDIA GPUs. Differently from existing research libraries for stream processing on multicores, WindFlow
 * is designed to support real live-streaming applications, where inputs are continuously received from real-world sources, and not only
 * offline streaming applications reading historical data already prepared in memory.
 * 
 * The web site of the library is available at: https://paragroup.github.io/WindFlow/.
 */
