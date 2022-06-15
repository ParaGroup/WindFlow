# Compiler and flags
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS_DEBUG "-g -O0")
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -g -finline-functions")

# Macros to be provided to the compiler
add_definitions(-DFF_BOUNDED_BUFFER)

# Header files of WindFlow and FastFlow
include_directories(${PROJECT_SOURCE_DIR}/wf ${ff_root_dir})

# Linking to pthread
link_libraries(pthread)

# Set output directory
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ../../bin/merge_tests)

# Cpp files to be compiled
file(GLOB SOURCES "*.cpp")

# Add a target for each cpp file and a unique target for all the tests in this folder
add_custom_target(merge_tests)

foreach(testsourcefile ${SOURCES})
    get_filename_component(barename ${testsourcefile} NAME)
    string(REPLACE ".cpp" "" testname ${barename})
    add_executable(${testname} ${testsourcefile})
    add_dependencies(merge_tests ${testname})
endforeach(testsourcefile ${SOURCES})
