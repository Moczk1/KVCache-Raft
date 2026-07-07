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

include_directories(
    ${PROJECT_SOURCE_DIR}/include
)

# -------------------------
# RPC / util / raft source
# -------------------------
file(GLOB_RECURSE SRC_FILES
    src/*.cc
    src/*.h
)

file(GLOB PROTO_SRCS
    proto/*.cc
)

# -------------------------
# 可执行文件
# -------------------------
add_executable(kvserver
    ${SRC_FILES}
    ${PROTO_SRCS}
)

# -------------------------
# 链接 protobuf
# -------------------------
target_link_libraries(kvserver
    PRIVATE protobuf::libprotobuf
)