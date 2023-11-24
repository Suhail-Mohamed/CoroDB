cmake_minimum_required(VERSION 3.10)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_BUILD_TYPE Debug)

project(DiskTest)

set(CMAKE_CXX_STANDARD 23)

include_directories(../../include)

file(GLOB SOURCES "../../src/*.cpp" "*.cpp")
list(FILTER SOURCES EXCLUDE REGEX "main.cpp")

add_executable(DiskTest ${SOURCES})
target_link_libraries(DiskTest uring)
