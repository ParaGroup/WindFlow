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

mp_test_cpu: fastflow
	$(MAKE) mp_test_cpu -C src

mp_test_gpu: fastflow
	$(MAKE) mp_test_gpu -C src

merge_test: fastflow
	$(MAKE) merge_test -C src

split_test: fastflow
	$(MAKE) split_test -C src

yahoo_test_cpu: fastflow
	$(MAKE) yahoo_test_cpu -C src

fastflow:
	@if [ ! -d $(FF_ROOT) ] ;\
	then \
	  echo "FastFlow does not exist, fetching"; \
	  git clone $(FF_REPO) $(FF_ROOT); \
fi

clean:
	$(MAKE) clean -C src

.DEFAULT_GOAL := all
.PHONY: all mp_test_cpu mp_test_gpu merge_test split_test yahoo_test_cpu clean
