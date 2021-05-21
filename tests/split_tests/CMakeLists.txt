# compiler and flags
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS_DEBUG "-g -O0")
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -finline-functions")

# macros to be provided to the compiler
add_definitions(-DFF_BOUNDED_BUFFER)

# header files of WindFlow and of FastFlow
include_directories(../../wf ${ff_root_dir})

# linking to pthread
link_libraries(pthread)

# set output directory
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ../../bin/split_tests)

# cpp files to be compiled
file(GLOB SOURCES "*.cpp")

# add a target for each cpp file and a unique target for all the tests in this folder
add_custom_target(split_tests)
foreach(testsourcefile ${SOURCES})
    get_filename_component(barename ${testsourcefile} NAME)
    string(REPLACE ".cpp" "" testname ${barename})
    add_executable(${testname} ${testsourcefile})
    add_dependencies(split_tests ${testname})
endforeach(testsourcefile ${SOURCES})
