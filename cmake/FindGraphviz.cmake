# Try to find Graphviz and cgraph libraries

# Check GRAPHVIZ_ROOT
if(EXISTS "$ENV{GRAPHVIZ_ROOT}")
    set(GRAPHVIZ_POSSIBLE_INCDIRS "$ENV{GRAPHVIZ_ROOT}/include" "$ENV{GRAPHVIZ_ROOT}/include/graphviz")
    set(GRAPHVIZ_POSSIBLE_LIBRARY_PATHS "$ENV{GRAPHVIZ_ROOT}/lib/release/dll" "$ENV{GRAPHVIZ_ROOT}/lib/release/lib")
endif()

# Check GRAPHVIZ_DIR
if(EXISTS "$ENV{GRAPHVIZ_DIR}")
    set(GRAPHVIZ_POSSIBLE_INCDIRS "$ENV{GRAPHVIZ_DIR}/include" "$ENV{GRAPHVIZ_DIR}/include/graphviz")
    set(GRAPHVIZ_POSSIBLE_LIBRARY_PATHS "$ENV{GRAPHVIZ_DIR}/lib/release/dll" "$ENV{GRAPHVIZ_DIR}/lib/release/lib")
endif()

if(GRAPHVIZ_DIR)
    set(GRAPHVIZ_POSSIBLE_INCDIRS "${GRAPHVIZ_DIR}/include" "${GRAPHVIZ_DIR}/include/graphviz")
    set(GRAPHVIZ_POSSIBLE_LIBRARY_PATHS "${GRAPHVIZ_DIR}/lib/release/dll" "${GRAPHVIZ_DIR}/lib/release/lib")
endif()

if(GRAPHVIZ_CGRAPH_LIBRARY)
    # In cache already
    set(Graphviz_FIND_QUIETLY TRUE)
endif()

# Use pkg-config to get the directories and then use these values in the FIND_PATH() and FIND_LIBRARY() calls
if(NOT WIN32)
    find_package(PkgConfig)
    pkg_check_modules(GRAPHVIZ_GVC_PKG gvc)
    pkg_check_modules(GRAPHVIZ_CGRAPH_PKG cgraph)
    pkg_check_modules(GRAPHVIZ_CDT_PKG cdt)
endif()

find_library(GRAPHVIZ_GVC_LIBRARY NAMES gvc libgvc
    PATHS
        ${GRAPHVIZ_POSSIBLE_LIBRARY_PATHS}
        /usr/lib
        /usr/local/lib
    HINTS
        ${GRAPHVIZ_GVC_PKG_LIBRARY_DIRS} # Generated by pkg-config
)

if(NOT(GRAPHVIZ_GVC_LIBRARY))
    message(STATUS "Could not find libgvc")
    set(GRAPHVIZ_GVC_FOUND FALSE)
else()
    set(GRAPHVIZ_GVC_FOUND TRUE)
endif()

find_library(GRAPHVIZ_CGRAPH_LIBRARY NAMES cgraph libcgraph
    PATHS
        ${GRAPHVIZ_POSSIBLE_LIBRARY_PATHS}
        /usr/lib
        /usr/local/lib
    HINTS
        ${GRAPHVIZ_CGRAPH_PKG_LIBRARY_DIRS} # Generated by pkg-config
)

if(NOT(GRAPHVIZ_CGRAPH_LIBRARY))
    message(STATUS "Could not find libcgraph")
    set(GRAPHVIZ_CGRAPH_FOUND FALSE)
else()
    set(GRAPHVIZ_CGRAPH_FOUND TRUE)
endif()

find_library(GRAPHVIZ_CDT_LIBRARY NAMES cdt libcdt
    PATHS
        ${GRAPHVIZ_POSSIBLE_LIBRARY_PATHS}
        /usr/lib
        /usr/local/lib
    HINTS
        ${GRAPHVIZ_CDT_PKG_LIBRARY_DIRS} # Generated by pkg-config
)

if(NOT(GRAPHVIZ_CDT_LIBRARY))
    message(STATUS "Could not find libcdt")
    set(GRAPHVIZ_CDT_FOUND FALSE)
else()
    set(GRAPHVIZ_CDT_FOUND TRUE)
endif()

find_path(GRAPHVIZ_INCLUDE_DIR NAMES cgraph.h
    PATHS
        ${GRAPHVIZ_POSSIBLE_INCDIRS}
        /usr/include
        /usr/include/graphviz
        /usr/local/include
        /usr/local/include/graphviz
    HINTS
        ${GRAPHVIZ_PKG_INCLUDE_DIR} # Generated by pkg-config
)

if(NOT(GRAPHVIZ_INCLUDE_DIR))
    message(STATUS "Could not find graphviz headers")
endif()

include(FindPackageHandleStandardArgs)

FIND_PACKAGE_HANDLE_STANDARD_ARGS(Graphviz_Gvc DEFAULT_MSG GRAPHVIZ_GVC_LIBRARY)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(Graphviz_Cgraph DEFAULT_MSG GRAPHVIZ_CGRAPH_LIBRARY)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(Graphviz_Cdt DEFAULT_MSG GRAPHVIZ_CDT_LIBRARY)
FIND_PACKAGE_HANDLE_STANDARD_ARGS("Graphviz Headers" DEFAULT_MSG GRAPHVIZ_INCLUDE_DIR)

# Show the POPPLER_(XPDF/QT4)_INCLUDE_DIR and POPPLER_LIBRARIES variables only in the advanced view
MARK_AS_ADVANCED(GRAPHVIZ_INCLUDE_DIR GRAPHVIZ_GVC_LIBRARY GRAPHVIZ_CGRAPH_LIBRARY GRAPHVIZ_CDT_LIBRARY)
