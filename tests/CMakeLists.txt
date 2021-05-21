# add sub-folders with CPU tests
add_subdirectory(graph_tests)
add_subdirectory(merge_tests)
if (GRAPHVIZ_GVC_LIBRARY AND GRAPHVIZ_CGRAPH_LIBRARY)
    add_subdirectory(miscellanea)
else()
    message(STATUS "Graphviz needs to be installed to generate miscellanea tests")
endif()
add_subdirectory(mp_tests_cpu)
add_subdirectory(split_tests)

# add custom target for all the CPU tests
add_custom_target(all_cpu)
if (GRAPHVIZ_GVC_LIBRARY AND GRAPHVIZ_CGRAPH_LIBRARY)
    add_dependencies(all_cpu graph_tests merge_tests miscellanea mp_tests_cpu split_tests)
else()
    add_dependencies(all_cpu graph_tests merge_tests mp_tests_cpu split_tests)
endif()

# check CUDA existence
include(CheckLanguage)
check_language(CUDA)
if(CMAKE_CUDA_COMPILER)
    # add sub-folders with GPU tests    
    add_subdirectory(mp_tests_gpu)
    # add custom target for all the GPU tests
    add_custom_target(all_gpu)
    add_dependencies(all_gpu mp_tests_gpu)
else()
    message(STATUS "CUDA needs to be installed to generate gpu tests")
endif()
