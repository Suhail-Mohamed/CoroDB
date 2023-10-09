cmake_minimum_required(VERSION 3.10) 
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

project(CoroDB)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_FLAGS_DEBUG "-O0")
set(CMAKE_CXX_FLAGS_RELEASE "-O2")

include_directories(include)
file(GLOB SOURCES "src/*.cpp")

add_executable(CoroDB ${SOURCES})
target_link_libraries(CoroDB uring)
