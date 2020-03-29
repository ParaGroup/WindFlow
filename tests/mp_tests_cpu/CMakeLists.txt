# compiler and flags
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS_DEBUG "-g -O0")
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -finline-functions")

# check pthreads dependency
find_package(Threads REQUIRED)

# macros to be provided to the compiler
add_definitions(-DFF_BOUNDED_BUFFER)

# linking to the pthread library (needed by FastFlow)
SET(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -pthread")

# header files of WindFlow and of FastFlow
include_directories(../../wf ${ff_root_dir})

# set output directory
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ../../bin/mp_tests_cpu)

# cpp files to be compiled
file(GLOB SOURCES "*.cpp")

# add a target for each cpp file and a unique target for all the tests in this folder
add_custom_target(mp_tests_cpu)
foreach(testsourcefile ${SOURCES})
	get_filename_component(barename ${testsourcefile} NAME)
	string(REPLACE ".cpp" "" testname ${barename})
	add_executable(${testname} ${testsourcefile})
	add_dependencies(mp_tests_cpu ${testname})
endforeach(testsourcefile ${SOURCES})