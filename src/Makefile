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

all: merge_test mp_test_cpu mp_test_gpu split_test yahoo_test_cpu

mp_test_cpu:
	$(MAKE) -C mp_test_cpu

mp_test_gpu:
	$(MAKE) -C mp_test_gpu

merge_test:
	$(MAKE) -C merge_test

split_test:
	$(MAKE) -C split_test

yahoo_test_cpu:
	$(MAKE) -C yahoo_test_cpu

clean:
	$(MAKE) clean -C mp_test_cpu
	$(MAKE) clean -C mp_test_gpu
	$(MAKE) clean -C merge_test
	$(MAKE) clean -C yahoo_test_cpu
	
.DEFAULT_GOAL := all
.PHONY: all merge_test mp_test_cpu mp_test_gpu split_test yahoo_test_cpu clean
