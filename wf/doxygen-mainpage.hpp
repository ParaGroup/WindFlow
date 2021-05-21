/*! \mainpage WindFlow Library
 * 
 * \section intro_sec Introduction
 * 
 * WindFlow is a C++17 library for parallel data stream processing applications targeting heterogeneous shared-memory architectures
 * featuring multi-core CPUs and NVIDIA GPUs. The library provides common stream processing operators like map, flatmap, filter,
 * fold/reduce as well as sliding-window operators designed with complex parallel features. Applications are built through the
 * MultiPipe and the PipeGraph programming constructs. The first is used to create parallel pipelines, while the second one allows
 * several MultiPipe instances to be interconnected through merge and split operations.
 * 
 * WindFlow is not thought to support streaming analytics applications only (e.g., the ones written with relational algebra query
 * languages, as in traditional old-style DSMSs), but rather general-purpose streaming applications can be easily supported through
 * operators embedding user-defined custom logics. In terms of runtime system, WindFlow is particularly suitable for embedded
 * architectures equipped with low-power multi-core CPUs and NVIDIA GPUs. Differently from existing research libraries for
 * stream processing on multicores, WindFlow is thought to support real live-streaming applications, where inputs are continuously
 * received from real-world sources like sensors, financial markets, and so forth.
 * 
 * The web site of the library is available at: https://paragroup.github.io/WindFlow/.
 */
