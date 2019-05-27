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
# Date:   Jan 2019

FF_ROOT			= ../../fastflow
OUT_DIR			= ../../bin/pipe_test_gpu
INCLUDE_DIR		= ../../includes

INCLUDES		= -I $(FF_ROOT) -I $(INCLUDE_DIR) -I .
NCXX			= nvcc
NCXXFLAGS		= -g -x cu -w -std=c++14 --expt-extended-lambda -gencode arch=compute_35,code=sm_35
MACROS 			= # -DTRACE_FASTFLOW -DLOG_DIR="/home/mencagli/WindFlow/log"
NOPTFLAGS		= -O3
NLDFLAGS      	= 

SRCS			:= $(wildcard *.cpp)
TGTS			:= $(basename $(SRCS))

all: $(TGTS)

%: %.cpp
	$(NCXX) $(NCXXFLAGS) $(NOPTFLAGS) $(INCLUDES) $(MACROS) -o $(OUT_DIR)/$@ $< $(NLDFLAGS)

clean:
	rm -f $(OUT_DIR)/*

.DEFAULT_GOAL := all
.PHONY: all clean
