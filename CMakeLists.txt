cmake_minimum_required(VERSION 3.10.2)

# project name
project(WindFlow-Tests VERSION 2.8.0)

# check build type
if(NOT CMAKE_BUILD_TYPE)
	set(CMAKE_BUILD_TYPE "Release")
	message(STATUS "Setting build type to '${CMAKE_BUILD_TYPE}' as none was specified")
else()
	message(STATUS "Build type is '${CMAKE_BUILD_TYPE}'")
endif()

# check Git dependency
find_package(Git REQUIRED)

# check FastFlow dependency
if(DEFINED ENV{FF_ROOT})
    set(ff_root_dir $ENV{FF_ROOT})
else()
	if(NOT EXISTS $ENV{HOME}/fastflow)
		execute_process(COMMAND ${GIT_EXECUTABLE} clone https://github.com/fastflow/fastflow.git fastflow WORKING_DIRECTORY $ENV{HOME})
	endif()
	set(ff_root_dir $ENV{HOME}/fastflow)
endif()

# add sub-folder with tests
add_subdirectory(tests)

# check Doxygen dependency
find_package(Doxygen)

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
