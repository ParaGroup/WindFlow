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