# Enable CUDA
enable_language(CUDA)

# Compiler and flags
set(CMAKE_CUDA_STANDARD 17)
set(CMAKE_CUDA_STANDARD_REQUIRED ON)
set(CMAKE_CUDA_FLAGS_DEBUG "-w --expt-extended-lambda --expt-relaxed-constexpr -g -G -O0 -Wno-deprecated-gpu-targets")
set(CMAKE_CUDA_FLAGS_RELEASE "-w --expt-extended-lambda --expt-relaxed-constexpr -O3 -g -Wno-deprecated-gpu-targets")

# Macros to be provided to the compiler
add_definitions(-DFF_BOUNDED_BUFFER -DDEFAULT_BUFFER_CAPACITY=16 -DWF_TRACING_ENABLED)

# Header files of WindFlow, FastFlow and TBB
include_directories(${PROJECT_SOURCE_DIR}/wf ${ff_root_dir} ${TBB_TBB_INCLUDE_DIR})

# Set the dynamic path to find the tbb library
link_directories(${TBB_TBB_LIBRARY_RELEASE})

# Linking to pthread
link_libraries(pthread tbb gvc cgraph)

# Set output directory
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ../../bin/miscellanea)

# Cpp files to be compiled
file(GLOB SOURCES "*.cpp")

# Add a target for each cpp file and a unique target for all the tests in this folder
add_custom_target(miscellanea)

foreach(testsourcefile ${SOURCES})
    get_filename_component(barename ${testsourcefile} NAME)
    string(REPLACE ".cpp" "" testname ${barename})
    add_executable(${testname} ${testsourcefile})
    set_property(SOURCE ${testsourcefile} PROPERTY LANGUAGE CUDA)
    # Change the statement below to express specific CUDA architectures to be used
    set_property(TARGET ${testname} PROPERTY CUDA_ARCHITECTURES OFF)
    # set_property(TARGET ${testname} PROPERTY CUDA_ARCHITECTURES 35)
    add_dependencies(miscellanea ${testname})
endforeach(testsourcefile ${SOURCES})
