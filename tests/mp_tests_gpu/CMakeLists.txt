# enable CUDA
enable_language(CUDA)

# check pthreads dependency
find_package(Threads REQUIRED)

# compiler and flags
set(CMAKE_CUDA_STANDARD 14)
set(CMAKE_CUDA_STANDARD_REQUIRED ON)
set(CMAKE_CUDA_FLAGS_DEBUG "-w --expt-extended-lambda -g -O0")
set(CMAKE_CUDA_FLAGS_RELEASE "-w --expt-extended-lambda -O3")

# macros to be provided to the compiler
add_definitions(-DFF_BOUNDED_BUFFER)

# header files of WindFlow and of FastFlow
include_directories(../../wf ${ff_root_dir})

# set output directory
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ../../bin/mp_tests_gpu)

# cpp files to be compiled
file(GLOB SOURCES "*.cpp")

# add a target for each cpp file and a unique target for all the tests in this folder
add_custom_target(mp_tests_gpu)
foreach(testsourcefile ${SOURCES})
	get_filename_component(barename ${testsourcefile} NAME)
	string(REPLACE ".cpp" "" testname ${barename})
	add_executable(${testname} ${testsourcefile})
	set_property(SOURCE ${testsourcefile} PROPERTY LANGUAGE CUDA)
	add_dependencies(mp_tests_gpu ${testname})
endforeach(testsourcefile ${SOURCES})
