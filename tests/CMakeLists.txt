# Add sub-folders with CPU tests
add_subdirectory(graph_tests)
add_subdirectory(merge_tests)
add_subdirectory(split_tests)
add_subdirectory(win_tests)
add_subdirectory(join_tests)

# Add custom target for all the CPU tests
add_custom_target(all_cpu)

if(LIBRDKAFKA_FOUND AND LibRDKafka_LIBRARIES)
    # Add the sub-folder with Kafka tests
    add_subdirectory(kafka_tests)
    # Add dependencies to the all_cpu target
    add_dependencies(all_cpu graph_tests merge_tests split_tests win_tests join_tests kafka_tests)
else()
    message(STATUS "librdkafka needs to be installed to generate kafka tests")
    # Add dependencies to the all_cpu target
    add_dependencies(all_cpu graph_tests merge_tests split_tests win_tests join_tests)
endif()

if(ROCKSDB_FOUND AND ROCKSDB_LIBRARIES)
    # Add the sub-folder with RocksDB tests
    add_subdirectory(rocksdb_tests)
    # Add dependencies to the all_cpu target
    add_dependencies(all_cpu rocksdb_tests)
else()
    message(STATUS "librocksdb-dev needs to be installed to generate RocksDB tests")
endif()

# Check CUDA existence
include(CheckLanguage)
check_language(CUDA)

if(CMAKE_CUDA_COMPILER AND TBB_FOUND)
    # Add sub-folders with GPU tests
    add_subdirectory(graph_tests_gpu)
    add_subdirectory(merge_tests_gpu)
    add_subdirectory(split_tests_gpu)
    add_subdirectory(win_tests_gpu)
    # Add custom target for all the GPU tests
    add_custom_target(all_gpu)
    if(GRAPHVIZ_GVC_LIBRARY AND GRAPHVIZ_CGRAPH_LIBRARY AND RAPIDJSON_FOUND)
        add_subdirectory(miscellanea)
        # Add dependencies to the all_gpu target
        add_dependencies(all_gpu graph_tests_gpu merge_tests_gpu split_tests_gpu win_tests_gpu miscellanea)
    else()
        message(STATUS "Graphviz and RapidJson need to be installed to generate miscellanea tests")
        # Add dependencies to the all_gpu target
        add_dependencies(all_gpu graph_tests_gpu merge_tests_gpu split_tests_gpu win_tests_gpu)
    endif()
else()
    message(STATUS "CUDA and Intel TBB need to be installed to generate gpu tests")
endif()
