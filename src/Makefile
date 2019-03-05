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

all: source_test map_test filter_test flatmap_test accumulator_test sum_test_cpu sum_test_gpu sink_test microbenchmarks union_test pipe_test_cpu pipe_test_gpu spatial_test yahoo_test_cpu

source_test:
	$(MAKE) -C source_test

map_test:
	$(MAKE) -C map_test

filter_test:
	$(MAKE) -C filter_test

flatmap_test:
	$(MAKE) -C flatmap_test

accumulator_test:
	$(MAKE) -C accumulator_test

sum_test_cpu:
	$(MAKE) -C sum_test_cpu

sum_test_gpu:
	$(MAKE) -C sum_test_gpu

sink_test:
	$(MAKE) -C sink_test

microbenchmarks:
	$(MAKE) -C microbenchmarks

pipe_test_cpu:
	$(MAKE) -C pipe_test_cpu

pipe_test_gpu:
	$(MAKE) -C pipe_test_gpu

union_test:
	$(MAKE) -C union_test

spatial_test:
	$(MAKE) -C spatial_test

yahoo_test_cpu:
	$(MAKE) -C yahoo_test_cpu

clean:
	$(MAKE) clean -C source_test
	$(MAKE) clean -C map_test
	$(MAKE) clean -C filter_test
	$(MAKE) clean -C flatmap_test
	$(MAKE) clean -C accumulator_test
	$(MAKE) clean -C sum_test_cpu
	$(MAKE) clean -C sum_test_gpu
	$(MAKE) clean -C sink_test
	$(MAKE) clean -C microbenchmarks
	$(MAKE) clean -C pipe_test_cpu
	$(MAKE) clean -C pipe_test_gpu
	$(MAKE) clean -C union_test
	$(MAKE) clean -C spatial_test
	$(MAKE) clean -C yahoo_test_cpu
	
.DEFAULT_GOAL := all
.PHONY: all source_test map_test filter_test flatmap_test accumulator_test sum_test_cpu sum_test_gpu sink_test microbenchmarks union_test pipe_test_cpu pipe_test_gpu spatial_test yahoo_test_cpu clean
