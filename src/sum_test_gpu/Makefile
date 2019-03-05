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
# Date:   June 2017

FF_ROOT			= $(HOME)/fastflow
OUT_DIR			= ../../bin/sum_test_gpu
INCLUDE_DIR		= ../../includes

INCLUDES		= -I $(FF_ROOT) -I $(INCLUDE_DIR) -I .
NCXX			= nvcc
NCXXFLAGS		= -g -x cu -w -std=c++14 --expt-extended-lambda -gencode arch=compute_35,code=sm_35
MACROS 			= # -DTRACE_FASTFLOW -DLOG_DIR="/home/mencagli/WindFlow/log"
NOPTFLAGS		= -O3
NLDFLAGS      	= 
OPT_LEVEL		= -DLEVEL=LEVEL0

SEQ_SRCS		:= $(wildcard *seq*.cpp)
SEQ_TGT			:= $(basename $(SEQ_SRCS))
WF_SRCS			:= $(wildcard *wf_*.cpp)
WF_TGT			:= $(basename $(WF_SRCS))
KF_SRCS			:= $(wildcard *kf_*.cpp)
KF_TGT			:= $(basename $(KF_SRCS))
PF_SRCS			:= $(wildcard *_pf*.cpp)
PF_TGT			:= $(basename $(PF_SRCS))
WM_SRCS			:= $(wildcard *_wm*.cpp)
WM_TGT			:= $(basename $(WM_SRCS))
NESTED_SRCS		:= $(wildcard *+*.cpp)
NESTED_TGT		:= $(basename $(NESTED_SRCS))

all: $(SEQ_TGT) $(WF_TGT) $(KF_TGT) $(PF_TGT) $(WM_TGT) $(NESTED_TGT) test_all_cb test_all_tb

test_all_cb: test_all_cb.cpp
	$(NCXX) $(NCXXFLAGS) $(NOPTFLAGS) $(INCLUDES) $(MACROS) -DALL_TESTS -o $(OUT_DIR)/test_all_cb test_all_cb.cpp $(NLDFLAGS)

test_all_tb: test_all_tb.cpp
	$(NCXX) $(NCXXFLAGS) $(NOPTFLAGS) $(INCLUDES) $(MACROS) -DALL_TESTS -o $(OUT_DIR)/test_all_tb test_all_tb.cpp $(NLDFLAGS)

%: %.cpp
	$(NCXX) $(NCXXFLAGS) $(NOPTFLAGS) $(INCLUDES) $(MACROS) $(OPT_LEVEL) -o $(OUT_DIR)/$@ $< $(NLDFLAGS)

clean:
	rm -f $(OUT_DIR)/*

.DEFAULT_GOAL := all
.PHONY: all clean
