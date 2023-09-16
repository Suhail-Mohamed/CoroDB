cmake_minimum_required(VERSION 3.10) # Adjust the minimum required version if necessary
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_BUILD_TYPE Debug)

project(CoroDB)

set(CMAKE_CXX_STANDARD 23)

# Add the include directory for header files
include_directories(include)

# Add all source files in the src directory
file(GLOB SOURCES "src/*.cpp")

# Define the executable target and link the source files
add_executable(CoroDB ${SOURCES})
target_link_libraries(CoroDB uring)
