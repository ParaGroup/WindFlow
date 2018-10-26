# ---------------------------------------------------------------------------
#  This program is free software; you can redistribute it and/or modify
#  it under the terms of the GNU General Public License version 2 as
#  published by the Free Software Foundation.
#  
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#  
#  You should have received a copy of the GNU General Public License
#  along with this program; if not, write to the Free Software
#  Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
# ---------------------------------------------------------------------------

# Author: Gabriele Mencagli <mencagli@di.unipi.it>
# Date:   May 2018

FF_ROOT			= $(HOME)/fastflow
OUT_DIR			= ../../bin/spatial_query
INCLUDE_DIR		= ../../includes

CXX 			= /usr/local/gcc-7.2.0/bin/g++
CXXFLAGS		= -std=c++17
INCLUDES		= -I $(FF_ROOT) -I $(INCLUDE_DIR) -I .
MACROS 			= -DTRACE_FASTFLOW -DLOG_DIR="/home/mencagli/WindFlow/log" -DDIM=4
OPTFLAGS		= -g -O3 -finline-functions
LDFLAGS			= -pthread

all: spatial_query_wf spatial_query_pf spatial_query_wf+pf test_generator

spatial_query_wf: test_spatial_query_wf.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(MACROS) $(OPTFLAGS) -o $(OUT_DIR)/$@ test_spatial_query_wf.cpp $(LDFLAGS)

spatial_query_pf: test_spatial_query_pf.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(MACROS) $(OPTFLAGS) -o $(OUT_DIR)/$@ test_spatial_query_pf.cpp $(LDFLAGS)

spatial_query_wf+pf: test_spatial_query_wf+pf.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(MACROS) $(OPTFLAGS) -o $(OUT_DIR)/$@ test_spatial_query_wf+pf.cpp $(LDFLAGS)

test_generator: test_generator.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(MACROS) $(OPTFLAGS) -o $(OUT_DIR)/$@ test_generator.cpp $(LDFLAGS)

clean:
	rm -f $(OUT_DIR)/*

.DEFAULT_GOAL := all
.PHONY: all clean
