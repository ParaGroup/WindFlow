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
	$(MAKE) -C tests

all_cpu: fastflow
	$(MAKE) all_cpu -C tests

all_gpu: fastflow
	$(MAKE) all_gpu -C tests

mp_tests_cpu: fastflow
	$(MAKE) mp_tests_cpu -C tests

mp_tests_gpu: fastflow
	$(MAKE) mp_tests_gpu -C tests

merge_tests: fastflow
	$(MAKE) merge_tests -C tests

split_tests: fastflow
	$(MAKE) split_tests -C tests

graph_tests: fastflow
	$(MAKE) graph_tests -C tests

fastflow:
	@if [ ! -d $(FF_ROOT) ] ;\
	then \
	  echo "FastFlow does not exist, fetching"; \
	  git clone $(FF_REPO) $(FF_ROOT); \
fi

clean:
	$(MAKE) clean -C tests

.DEFAULT_GOAL := all
.PHONY: all all_cpu all_gpu mp_tests_cpu mp_tests_gpu merge_tests split_tests graph_tests fastflow clean
