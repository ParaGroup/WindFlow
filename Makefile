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

all:
	$(MAKE) -C src

source_test:
	$(MAKE) source_test -C src

map_test:
	$(MAKE) map_test -C src

filter_test:
	$(MAKE) filter_test -C src

flatmap_test:
	$(MAKE) flatmap_test -C src

accumulator_test:
	$(MAKE) accumulator_test -C src

sum_test_cpu:
	$(MAKE) sum_test_cpu -C src

sum_test_gpu:
	$(MAKE) sum_test_gpu -C src

sink_test:
	$(MAKE) sink_test -C src

microbenchmarks:
	$(MAKE) microbenchmarks -C src

pipe_test_cpu:
	$(MAKE) pipe_test_cpu -C src

pipe_test_gpu:
	$(MAKE) pipe_test_gpu -C src

union_test:
	$(MAKE) union_test -C src

spatial_test:
	$(MAKE) spatial_test -C src

yahoo_test_cpu:
	$(MAKE) yahoo_test_cpu -C src

clean:
	$(MAKE) clean -C src

.DEFAULT_GOAL := all
.PHONY: all source_test map_test filter_test flatmap_test accumulator_test sum_test_cpu sum_test_gpu sink_test microbenchmarks pipe_test_cpu pipe_test_gpu union_test spatial_test yahoo_test_cpu clean
