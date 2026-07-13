cmake_minimum_required(VERSION 3.30)

project(KVCacheRaft LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# 👉 生成 clangd 需要的 compile_commands.json
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

set(EXECUTABLE_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/bin)

# -------------------------
# Protobuf
# -------------------------
find_package(Protobuf REQUIRED)
find_package(Boost REQUIRED COMPONENTS serialization program_options context)

include_directories(
    ${PROJECT_SOURCE_DIR}/include,
    ${PROJECT_SOURCE_DIR}/proto
    ${PROJECT_SOURCE_DIR}/submodule/coroutine/include
    ${PROJECT_SOURCE_DIR}/submodule/threadpool
)
# 公共源文件
file(GLOB_RECURSE KVRAFT_SRC
    ${PROJECT_SOURCE_DIR}/src/*.cc
    ${PROJECT_SOURCE_DIR}/proto/*.cc
)

# 添加 coroutine 子项目
add_subdirectory(submodule/coroutine)
add_subdirectory(submodule/threadpool)

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
	Boost::context
	muduo_net
    muduo_base
	mcoroutine
    mthreadpool
    pthread
)

# example/server.cc -> server
add_executable(server
    example/server.cc
)

target_link_libraries(server
    PRIVATE
    kvraft_lib
)

# example/clerk.cc -> clerk
add_executable(clerk
    example/clerk.cc
)

target_link_libraries(clerk
    PRIVATE
    kvraft_lib
)