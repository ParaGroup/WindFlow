# Author: Gabriele Mencagli
# Date: 04/10/2023

FF_ROOT         = $(HOME)/fastflow
WF_INCLUDES     = $(HOME)/WindFlow-3.6.0/wf

CXX             = g++
CXXFLAGS        = -std=c++17
INCLUDES        = -I $(FF_ROOT) -I $(WF_INCLUDES) -I .
MACRO           = -DFF_BOUNDED_BUFFER -DDEFAULT_BUFFER_CAPACITY=32786
OPTFLAGS        = -g -O3
LDFLAGS         = -pthread

NVXX            = /usr/local/cuda/bin/nvcc
NVXXFLAGS       = -std=c++17 -x cu
NVOPTFLAGS      = -w --expt-extended-lambda -O3 -g -gencode arch=compute_80,code=sm_80 -Wno-deprecated-gpu-targets --expt-relaxed-constexpr

all: test_synth_gpu test_synth_gpu_keyed test_synth_gpu_delayed test_saber test_saber_v2

test_synth_gpu.o: test_synth_gpu.cpp
	$(NVXX) $(NVXXFLAGS) $(NVOPTFLAGS) $(INCLUDES) $(MACRO) $(OPTFLAGS) $< -c

test_synth_gpu: test_synth_gpu.o
	$(NVXX) test_synth_gpu.o -o test_synth_gpu

test_synth_gpu_keyed.o: test_synth_gpu_keyed.cpp
	$(NVXX) $(NVXXFLAGS) $(NVOPTFLAGS) $(INCLUDES) $(MACRO) $(OPTFLAGS) $< -c

test_synth_gpu_keyed: test_synth_gpu_keyed.o
	$(NVXX) test_synth_gpu_keyed.o -o test_synth_gpu_keyed

test_synth_gpu_delayed.o: test_synth_gpu_delayed.cpp
	$(NVXX) $(NVXXFLAGS) $(NVOPTFLAGS) $(INCLUDES) $(MACRO) $(OPTFLAGS) $< -c

test_synth_gpu_delayed: test_synth_gpu_delayed.o
	$(NVXX) test_synth_gpu_delayed.o -o test_synth_gpu_delayed

test_saber.o: test_saber.cpp
	$(NVXX) $(NVXXFLAGS) $(NVOPTFLAGS) $(INCLUDES) $(MACRO) $(OPTFLAGS) $< -c

test_saber: test_saber.o
	$(NVXX) test_saber.o -o test_saber	

test_saber_v2.o: test_saber_v2.cpp
	$(NVXX) $(NVXXFLAGS) $(NVOPTFLAGS) $(INCLUDES) $(MACRO) $(OPTFLAGS) $< -c

test_saber_v2: test_saber_v2.o
	$(NVXX) test_saber_v2.o -o test_saber_v2	

clean:
	rm -f test_synth_gpu
	rm -f test_synth_gpu_keyed
	rm -f test_synth_gpu_delayed
	rm -f test_saber
	rm -f test_saber_v2
	rm -f *.o

.DEFAULT_GOAL := all
.PHONY: all clean
