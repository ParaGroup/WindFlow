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

all: mp_tests_cpu mp_tests_gpu merge_tests split_tests graph_tests

all_cpu: mp_tests_cpu merge_tests split_tests graph_tests

all_gpu: mp_tests_gpu

mp_tests_cpu:
	$(MAKE) -C mp_tests_cpu

mp_tests_gpu:
	$(MAKE) -C mp_tests_gpu

merge_tests:
	$(MAKE) -C merge_tests

split_tests:
	$(MAKE) -C split_tests

graph_tests:
	$(MAKE) -C graph_tests

clean:
	$(MAKE) clean -C mp_tests_cpu
	$(MAKE) clean -C mp_tests_gpu
	$(MAKE) clean -C merge_tests
	$(MAKE) clean -C split_tests
	$(MAKE) clean -C graph_tests

.DEFAULT_COLOR_COLOR_COLOR_GOAL := all
.PHONY: all all_cpu app_gpu mp_tests_cpu mp_tests_gpu merge_tests split_tests graph_tests clean
