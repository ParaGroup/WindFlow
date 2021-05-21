# add sub-folders with CPU tests
add_subdirectory(graph_tests)
add_subdirectory(merge_tests)
add_subdirectory(split_tests)
add_subdirectory(win_tests)

# add custom target for all the CPU tests
add_custom_target(all_cpu)
add_dependencies(all_cpu graph_tests merge_tests split_tests win_tests)

# check CUDA existence
include(CheckLanguage)
check_language(CUDA)

if(CMAKE_CUDA_COMPILER AND TBB_TBB_FOUND)
    # add sub-folders with GPU tests
    add_subdirectory(graph_tests_gpu)
    add_subdirectory(merge_tests_gpu)
    add_subdirectory(split_tests_gpu)
    add_subdirectory(win_tests_gpu)
    if(GRAPHVIZ_GVC_LIBRARY AND GRAPHVIZ_CGRAPH_LIBRARY AND RAPIDJSON_FOUND)
        add_subdirectory(miscellanea)
        # add custom target for all the GPU tests
        add_custom_target(all_gpu)
        add_dependencies(all_gpu graph_tests_gpu merge_tests_gpu split_tests_gpu win_tests_gpu miscellanea)
    else()
        message(STATUS "Graphviz and RapidJson need to be installed to generate miscellanea tests")
        # add custom target for all the GPU tests
        add_custom_target(all_gpu)
        add_dependencies(all_gpu graph_tests_gpu merge_tests_gpu split_tests_gpu win_tests_gpu)
    endif()
else()
    message(STATUS "CUDA and Intel TBB need to be installed to generate gpu tests")
endif()
