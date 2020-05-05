aSHELL := /bin/bash
UNAME := $(shell uname)
LIBNAME=indigenous

FILES = /tmp/indigenous/*.cpp

LINUX_FLAGS = -shared -fPIC
LINUX_JAVA_INCLUDE= ${JAVA_HOME}/include/linux

DARWIN_FLAGS = -dynamiclib
DARWIN_JAVA_INCLUDE = ${JAVA_HOME}/include/darwin

all: compile

package:
	mvn package -f ./engine

compile: $(FILES)
ifeq ($(UNAME), Linux)
	g++ -I"${JAVA_HOME}/include" -I"$(LINUX_JAVA_INCLUDE)" -I "./indigenous" -o /tmp/$(LIBNAME).so $(LINUX_FLAGS) $(FILES)
endif
ifeq ($(UNAME), Darwin)
	g++ -I"${JAVA_HOME}/include" -I"$(DARWIN_JAVA_INCLUDE)" -I "./indigenous -o /tmp/$(LIBNAME).dylib $(DARWIN_FLAGS) $(FILES)
endif