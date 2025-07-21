# Makefile for SPM Mergesort

CXX					= /opt/openmpi/bin/mpicxx -std=c++20
ifdef DEBUG
CXXFLAGS			= -O0 -g -DDEBUG
else
CXXFLAGS         	= -O3 -ffast-math -DNDEBUG 
endif
CXXFLAGS			+= -Wall # -DBLOCKING_MODE -DFF_BOUNDED_BUFFER -DNO_DEFAULT_MAPPING

ifndef FF_ROOT 
FF_ROOT				= ${HOME}/fastflow
endif

INCLUDES			= -I. -I./include -I $(FF_ROOT) -I /opt/openmpi/include/
LIBS				= -pthread -fopenmp

SOURCES				= $(wildcard *.cpp)
TARGET				= $(SOURCES:.cpp=)

all: 
	$(MAKE) $(TARGET)

mergesort: mergesort.cpp include/defines.hpp include/record.hpp include/timer.hpp include/utils.hpp
	$(CXX) $(INCLUDES) $(CXXFLAGS) $< -o mergesort $(LIBS)

record_gen: record_gen.cpp include/defines.hpp include/record.hpp include/utils.hpp
	$(CXX) $(INCLUDES) $(CXXFLAGS) $< -o record_gen $(LIBS)

clean:
	-rm -fr $(TARGET) *.o *~ *.d
clean_test_artifacts:
	-rm -f *.out *.txt *.dat
cleanall: clean clean_test_artifacts

.PHONY: all clean cleanall