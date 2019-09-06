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

FF_ROOT	= $(HOME)/fastflow
FF_REPO	= https://github.com/fastflow/fastflow

all: fastflow
	$(MAKE) -C src

source_test: fastflow
	$(MAKE) source_test -C src

map_test: fastflow
	$(MAKE) map_test -C src

filter_test: fastflow
	$(MAKE) filter_test -C src

flatmap_test: fastflow
	$(MAKE) flatmap_test -C src

accumulator_test: fastflow
	$(MAKE) accumulator_test -C src

sum_test_cpu: fastflow
	$(MAKE) sum_test_cpu -C src

sum_test_gpu: fastflow
	$(MAKE) sum_test_gpu -C src

sink_test: fastflow
	$(MAKE) sink_test -C src

microbenchmarks: fastflow
	$(MAKE) microbenchmarks -C src

pipe_test_cpu: fastflow
	$(MAKE) pipe_test_cpu -C src

pipe_test_gpu: fastflow
	$(MAKE) pipe_test_gpu -C src

union_test: fastflow
	$(MAKE) union_test -C src

spatial_test: fastflow
	$(MAKE) spatial_test -C src

yahoo_test_cpu: fastflow
	$(MAKE) yahoo_test_cpu -C src

fastflow:
	@if [ ! -d $(FF_ROOT) ] ;\
	then \
	  echo "FastFlow does not exist, fetching"; \
	  git clone $(FF_REPO); \
fi

clean:
	$(MAKE) clean -C src

.DEFAULT_GOAL := all
.PHONY: all source_test map_test filter_test flatmap_test accumulator_test sum_test_cpu sum_test_gpu sink_test microbenchmarks pipe_test_cpu pipe_test_gpu union_test spatial_test yahoo_test_cpu clean
