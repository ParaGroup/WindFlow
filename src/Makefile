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

all: mp_test_cpu mp_test_gpu merge_test split_test graph_test yahoo_test_cpu

all_cpu: mp_test_cpu merge_test split_test graph_test yahoo_test_cpu

all_gpu: mp_test_gpu

mp_test_cpu:
	$(MAKE) -C mp_test_cpu

mp_test_gpu:
	$(MAKE) -C mp_test_gpu

merge_test:
	$(MAKE) -C merge_test

split_test:
	$(MAKE) -C split_test

graph_test:
	$(MAKE) -C graph_test

yahoo_test_cpu:
	$(MAKE) -C yahoo_test_cpu

clean:
	$(MAKE) clean -C mp_test_cpu
	$(MAKE) clean -C mp_test_gpu
	$(MAKE) clean -C merge_test
	$(MAKE) clean -C split_test
	$(MAKE) clean -C graph_test
	$(MAKE) clean -C yahoo_test_cpu

.DEFAULT_COLOR_COLOR_COLOR_GOAL := all
.PHONY: all all_cpu app_gpu mp_test_cpu mp_test_gpu merge_test split_test graph_test yahoo_test_cpu clean
