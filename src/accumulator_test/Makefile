# --------------------------------------------------------------------------
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
# Date:   February 2019

FF_ROOT			= ../../fastflow
OUT_DIR			= ../../bin/accumulator_test
INCLUDE_DIR		= ../../includes

CXX 			= /usr/local/gcc-7.2.0/bin/g++
CXXFLAGS		= -std=c++17
INCLUDES		= -I $(FF_ROOT) -I $(INCLUDE_DIR) -I .
MACROS 			= -DTRACE_FASTFLOW -DLOG_DIR="../../log"
OPTFLAGS		= -g -O3 -finline-functions
LDFLAGS			= -pthread

SRCS			:= $(wildcard *.cpp)
TGTS			:= $(basename $(SRCS))

all: $(TGTS)

%: %.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(MACROS) $(OPTFLAGS) -o $(OUT_DIR)/$@ $< $(LDFLAGS)

clean:
	rm -f $(OUT_DIR)/*

.DEFAULT_GOAL := all
.PHONY: all clean
