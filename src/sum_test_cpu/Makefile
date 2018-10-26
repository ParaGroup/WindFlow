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
# Date:   June 2017

FF_ROOT			= $(HOME)/fastflow
OUT_DIR			= ../../bin/sum_test_cpu
INCLUDE_DIR		= ../../includes

CXX 			= /usr/local/gcc-7.2.0/bin/g++
CXXFLAGS		= -std=c++17
INCLUDES		= -I $(FF_ROOT) -I $(INCLUDE_DIR) -I .
MACROS 			= -DTRACE_FASTFLOW -DLOG_DIR="../../log"
OPTFLAGS		= -g -O3 -finline-functions
LDFLAGS			= -pthread
OPT_LEVEL		= -DLEVEL=LEVEL0

SEQ_SRCS		:= $(wildcard *seq*.cpp)
SEQ_TGT			:= $(addprefix $(OUT_DIR)/, $(basename $(SEQ_SRCS)))
WF_SRCS			:= $(wildcard *wf_*.cpp)
WF_TGT			:= $(addprefix $(OUT_DIR)/, $(basename $(WF_SRCS)))
KF_SRCS			:= $(wildcard *kf_*.cpp)
KF_TGT			:= $(addprefix $(OUT_DIR)/, $(basename $(KF_SRCS)))
PF_SRCS			:= $(wildcard *_pf*.cpp)
PF_TGT			:= $(addprefix $(OUT_DIR)/, $(basename $(PF_SRCS)))
WM_SRCS			:= $(wildcard *_wm*.cpp)
WM_TGT			:= $(addprefix $(OUT_DIR)/, $(basename $(WM_SRCS)))
NESTED_SRCS		:= $(wildcard *+*.cpp)
NESTED_TGT		:= $(addprefix $(OUT_DIR)/, $(basename $(NESTED_SRCS)))

all: $(SEQ_TGT) $(WF_TGT) $(KF_TGT) $(PF_TGT) $(WM_TGT) $(NESTED_TGT) test_all_cb test_all_tb

test_all_cb: test_all_cb.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(MACROS) -DALL_TESTS $(OPTFLAGS) -o $(OUT_DIR)/test_all_cb test_all_cb.cpp $(LDFLAGS)

test_all_tb: test_all_tb.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(MACROS) -DALL_TESTS $(OPTFLAGS) -o $(OUT_DIR)/test_all_tb test_all_tb.cpp $(LDFLAGS)

$(SEQ_TGT): $(OUT_DIR)/% : %.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(MACROS) $(OPT_LEVEL) $(OPTFLAGS) -o $@ $< $(LDFLAGS)

$(WF_TGT): $(OUT_DIR)/% : %.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(MACROS) $(OPT_LEVEL) $(OPTFLAGS) -o $@ $< $(LDFLAGS)

$(KF_TGT): $(OUT_DIR)/% : %.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(MACROS) $(OPT_LEVEL) $(OPTFLAGS) -o $@ $< $(LDFLAGS)

$(PF_TGT): $(OUT_DIR)/% : %.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(MACROS) $(OPT_LEVEL) $(OPTFLAGS) -o $@ $< $(LDFLAGS)

$(WM_TGT): $(OUT_DIR)/% : %.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(MACROS) $(OPT_LEVEL) $(OPTFLAGS) -o $@ $< $(LDFLAGS)

$(NESTED_TGT): $(OUT_DIR)/% : %.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(MACROS) $(OPT_LEVEL) $(OPTFLAGS) -o $@ $< $(LDFLAGS)

clean:
	rm -f $(OUT_DIR)/*

.DEFAULT_GOAL := all
.PHONY: all clean
