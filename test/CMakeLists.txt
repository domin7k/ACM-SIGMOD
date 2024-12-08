cmake_minimum_required(VERSION 3.30)

set(This LsdTests)

set(Sources 
    test_core.cpp
)

add_executable(${This} ${Sources})
target_link_libraries(${This} PUBLIC
    gtest_main
    Lsd
)

add_test(
    NAME ${This}
    COMMAND ${This}
)