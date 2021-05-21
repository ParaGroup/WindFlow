# enable CUDA
enable_language(CUDA)

# compiler and flags
set(CMAKE_CUDA_STANDARD 17)
set(CMAKE_CUDA_STANDARD_REQUIRED ON)
set(CMAKE_CUDA_FLAGS_DEBUG "-w --expt-extended-lambda --expt-relaxed-constexpr -g -G -O0 -Wno-deprecated-gpu-targets")
set(CMAKE_CUDA_FLAGS_RELEASE "-w --expt-extended-lambda --expt-relaxed-constexpr -O3 -Wno-deprecated-gpu-targets")

# macros to be provided to the compiler
add_definitions(-DFF_BOUNDED_BUFFER -DDEFAULT_BUFFER_CAPACITY=16)

# header files of WindFlow and of FastFlow
include_directories(../../wf ${ff_root_dir} ${TBB_TBB_INCLUDE_DIR})

# set the dynamic path to find the tbb library
link_directories(${TBB_TBB_LIBRARY_RELEASE})

# linking to pthread
link_libraries(pthread tbb)

# set output directory
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ../../bin/win_tests_gpu)

# cpp files to be compiled
file(GLOB SOURCES "*.cpp")

# add a target for each cpp file and a unique target for all the tests in this folder
add_custom_target(win_tests_gpu)
foreach(testsourcefile ${SOURCES})
    get_filename_component(barename ${testsourcefile} NAME)
    string(REPLACE ".cpp" "" testname ${barename})
    add_executable(${testname} ${testsourcefile})
    set_property(SOURCE ${testsourcefile} PROPERTY LANGUAGE CUDA)
    # change the statement below to express specific CUDA architectures to be used
	set_property(TARGET ${testname} PROPERTY CUDA_ARCHITECTURES OFF)
    add_dependencies(win_tests_gpu ${testname})
endforeach(testsourcefile ${SOURCES})
