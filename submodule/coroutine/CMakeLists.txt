cmake_minimum_required(VERSION 3.31)
project(my_coroutine VERSION 1.0.0 LANGUAGES CXX)

# 👉 生成 clangd 需要的 compile_commands.json
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

find_package(Threads REQUIRED)

set(CMAKE_COMPILE_WARNING_AS_ERROR ON) # 调试阶段可以先关闭

# 设置 C++ 标准
set(CMAKE_CXX_STANDARD 23) 
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# 1. 收集所有子目录中的源文件（排除各个目录下的 test.cc）
file(GLOB_RECURSE CORE_SOURCES 
    "src/thread/thread.cc"
    "src/fiber/fiber.cc"
    "src/scheduler/scheduler.cc"
    "src/ioscheduler/ioscheduler.cc"
    "src/timer/timer.cc"
    "src/hook/fd_manager.cc"
    "src/hook/hook.cc"
)

# 2. 设置头文件包含路径，这样内部互相引用时比较方便
include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

# 3. 核心：选择打包成动态库还是静态库（二选一，或者两个都建）

# # 选项 A：打包成动态链接库 .so
# add_library(moczkrin_coroutine SHARED ${LIB_SOURCES})

# 选项 B：打包成静态链接库  .a
add_library(mcoroutine STATIC ${CORE_SOURCES})
target_link_libraries(mcoroutine PUBLIC Threads::Threads ${CMAKE_DL_LIBS})


# 测试程序
# add_executable(test_fiber src/fiber/test.cc)
# target_link_libraries(test_fiber mcoroutine) # 让测试程序链接你刚生成的库

# Demo: shows Fiber + Scheduler + IOManager working together as a small HTTP server.
add_executable(demo_http_server examples/main.cc)
target_link_libraries(demo_http_server mcoroutine)


