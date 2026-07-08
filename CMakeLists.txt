cmake_minimum_required(VERSION 3.30)

project(KVCacheRaft LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# 👉 生成 clangd 需要的 compile_commands.json
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# -------------------------
# Protobuf
# -------------------------
find_package(Protobuf REQUIRED)
find_package(Boost REQUIRED COMPONENTS serialization program_options)

include_directories(
    ${PROJECT_SOURCE_DIR}/include
)
# src 下所有 .cc 都作为公共源文件
file(GLOB_RECURSE KVRAFT_SRC
    ${PROJECT_SOURCE_DIR}/src/*.cc
)

# 公共静态库：包含 src 下所有实现
add_library(kvraft_lib STATIC
    ${KVRAFT_SRC}
)


target_include_directories(kvraft_lib
    PUBLIC
    ${PROJECT_SOURCE_DIR}/include
)

target_link_libraries(kvraft_lib
    PUBLIC
    protobuf::libprotobuf
    Boost::serialization
    Boost::program_options
	muduo_net
    muduo_base
    pthread
)

# example/server.cc -> example_server
add_executable(example_server
    example/server.cc
)

target_link_libraries(example_server
    PRIVATE
    kvraft_lib
)

# example/clerk.cc -> example_clerk
add_executable(example_clerk
    example/clerk.cc
)

target_link_libraries(example_clerk
    PRIVATE
    kvraft_lib
)