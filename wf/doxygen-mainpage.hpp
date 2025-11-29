/*! \mainpage WindFlow Library
 * 
 * \section intro_sec Introduction
 * 
 * WindFlow is a C++17 library for parallel data stream processing targeting heterogeneous shared-memory architectures
 * equipped with multi-core CPUs and NVIDIA GPUs. The library provides traditional stream processing operators such as
 * map, flatmap, filter, and reduce, as well as window-based operators. The API enables developers to build streaming
 * applications through the MultiPipe and PipeGraph programming constructs. The former is used to create parallel pipelines,
 * while the latter allows multiple MultiPipe instances to be interconnected through merge and split operations, enabling the
 * construction of complex directed acyclic graphs (DAGs) of interconnected operators.
 * 
 * WindFlow supports not only streaming analytics applications (e.g., those written with relational algebra query languages,
 * as in traditional DSMSs) but also general-purpose streaming applications that can be implemented through operators with
 * user-defined custom logic. From a runtime perspective, WindFlow is particularly suitable for embedded architectures
 * equipped with low-power multi-core CPUs and integrated NVIDIA GPUs (e.g., Jetson boards). Nevertheless, it also performs
 * effectively on conventional multi-core servers equipped with discrete NVIDIA GPUs.
 * 
 * Unlike existing research-oriented libraries for stream processing on multicores, WindFlow is designed to support real
 * live-streaming applications, where inputs are continuously received from real-world sources, rather than offline streaming
 * workloads processing preloaded historical data.
 * 
 * The official website of the library is available at: https://paragroup.github.io/WindFlow/
 */
