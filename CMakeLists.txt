project(petuum)
cmake_minimum_required(VERSION 2.8.9)

set(EXECUTABLE_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/bin)
set(LIBRARY_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/lib)

include_directories(
    ${PROJECT_SOURCE_DIR}/src
    ${PROJECT_SOURCE_DIR}/third_party/include
)

link_directories(
    ${PROJECT_SOURCE_DIR}/lib
    ${PROJECT_SOURCE_DIR}/third_party/lib
    /usr/local/lib
)

#petuum variable
set(PETUUM_CXXFLAGS
    -g
    -O3
    -std=c++11
    -Wall
    -Wno-sign-compare
    -fno-builtin-malloc
    -fno-builtin-calloc
    -fno-builtin-realloc
    -fno-builtin-free
    -fno-omit-frame-pointer
)
set(PETUUM_LDFLAGS
    pthread
    rt
    nsl
    uuid
    zmq
    boost_thread
    boost_system
    glog
    gflags
    tcmalloc
    config++
    yaml-cpp
    leveldb
)
set(PETUUM_PS_LIB
    petuum-ps
)
set(PETUUM_PS_SN_LIB
    petuum-ps-sn
)

add_subdirectory(apps)
add_subdirectory(src)
