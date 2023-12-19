
cmake_minimum_required(VERSION 3.10) 
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

project(CoroDB)

set(CMAKE_CXX_STANDARD 23)

# Debug flags
# Uncomment the set of sanitizers you want to use

# Option 1: ASan and UBSan
# set(CMAKE_CXX_FLAGS_DEBUG "-O0 -g -fsanitize=address,undefined")
# set(CMAKE_EXE_LINKER_FLAGS_DEBUG "-fsanitize=address,undefined")

# Option 2: TSan and UBSan
set(CMAKE_CXX_FLAGS_DEBUG "-O0 -g -fsanitize=thread,undefined")
set(CMAKE_EXE_LINKER_FLAGS_DEBUG "-fsanitize=thread,undefined")

set(CMAKE_CXX_FLAGS_RELEASE "-O2")

include_directories(include)
file(GLOB SOURCES "src/*.cpp")

add_executable(CoroDB ${SOURCES})
target_link_libraries(CoroDB uring)

