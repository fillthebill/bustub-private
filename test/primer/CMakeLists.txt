cmake_minimum_required(VERSION 3.20)
project(primer)

set(CMAKE_CXX_STANDARD 14)

include_directories(.)
include_directories(~/15445/bustub-private/src/include)
include_directories(~/15445/bustub-private/build/googletest-src/googletest/include)
include_directories(~/15445/bustub-private/src/include/primer/p0_starter.h)

add_executable(primer
        starter_test.cpp)


