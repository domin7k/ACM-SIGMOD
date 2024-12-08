cmake_minimum_required(VERSION 3.30)

set(This Lsd)

project(${This} C CXX)

set(CMAKE_C_STANDARD 99)
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

enable_testing()

add_subdirectory(googletest)

set(Headers
    include/core.h
)
set(Sources
    task1/core.cpp
)

add_library(${This} STATIC ${Sources} ${Headers})

add_subdirectory(test)