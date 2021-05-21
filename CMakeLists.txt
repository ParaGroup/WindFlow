cmake_minimum_required(VERSION 3.18)

# project name
project(WindFlow-Tests CXX)

# check compiler (GCC)
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    if(CMAKE_CXX_COMPILER_VERSION VERSION_LESS "7.5.0")
        message(FATAL_ERROR "Insufficient gcc version (>= 7.5.0 required)")
    endif()
else()
    message(FATAL_ERROR "Compiler is not GCC")
endif()

# check build type
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "Release")
    message(STATUS "Setting build type to '${CMAKE_BUILD_TYPE}' as none was specified")
else()
    message(STATUS "Build type is '${CMAKE_BUILD_TYPE}'")
endif()

# check Git dependency
find_package(Git REQUIRED)

# check pthreads dependency
find_package(Threads REQUIRED)

# check FastFlow dependency
if(DEFINED ENV{FF_ROOT})
    set(ff_root_dir $ENV{FF_ROOT})
else()
    if(NOT EXISTS $ENV{HOME}/fastflow)
        execute_process(COMMAND ${GIT_EXECUTABLE} clone https://github.com/fastflow/fastflow.git fastflow WORKING_DIRECTORY $ENV{HOME})
    endif()
    set(ff_root_dir $ENV{HOME}/fastflow)
endif()

# check graphviz, rapidjson and tbb dependencies
set(CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake.modules)
include(${PROJECT_SOURCE_DIR}/cmake.modules/FindGraphviz.cmake)
include(${PROJECT_SOURCE_DIR}/cmake.modules/FindRapidJSON.cmake)
include(${PROJECT_SOURCE_DIR}/cmake.modules/FindTBB.cmake)

# add sub-folder with tests
add_subdirectory(tests)

# check Doxygen dependency
find_package(Doxygen OPTIONAL_COMPONENTS dot)
if (DOXYGEN_FOUND)
    # set input and output files
    set(DOXYGEN_IN ./docs/windflow-doxygen.conf)
    set(DOXYGEN_OUT ./windflow-doxygen)
    # request to configure the file
    configure_file(${DOXYGEN_IN} ${DOXYGEN_OUT} @ONLY)
    message("Doxygen build started")
    # add custom target for generating the Doxygen documentation
    add_custom_target(docs
        COMMAND ${DOXYGEN_EXECUTABLE} ${DOXYGEN_OUT}
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
        COMMENT "Generating API documentation with Doxygen"
        VERBATIM)
else (DOXYGEN_FOUND)
    message("Doxygen needs to be installed to generate the doxygen documentation")
endif (DOXYGEN_FOUND)
