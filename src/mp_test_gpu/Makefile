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

FF_ROOT			= $(HOME)/fastflow
OUT_DIR			= ../../bin/mp_test_gpu
INCLUDE_DIR		= ../../includes

INCLUDES		= -I $(FF_ROOT) -I $(INCLUDE_DIR) -I .
NCXX			= nvcc
NCXXFLAGS		= -g -x cu -w -std=c++14 --expt-extended-lambda
MACROS 			= -DFF_BOUNDED_BUFFER # -DLOG_DIR="../../log"
NOPTFLAGS		= -O3
NLDFLAGS      	=

SRCS			:= $(wildcard *.cpp)
TGTS			:= $(basename $(SRCS))

all: $(TGTS)

%: %.cpp checkbindir
	$(NCXX) $(NCXXFLAGS) $(NOPTFLAGS) $(INCLUDES) $(MACROS) -o $(OUT_DIR)/$@ $< $(NLDFLAGS)

checkbindir:
	@if [ ! -d $(OUT_DIR) ] ;\
	then \
		echo "creating $(OUT_DIR)";\
		mkdir -p $(OUT_DIR); \
	fi

clean:
	rm -rf $(OUT_DIR)

.DEFAULT_GOAL := all
.PHONY: all clean
