add_definitions(${PETUUM_CXXFLAGS})
add_executable(helloworld helloworld.cpp)

target_link_libraries(helloworld
    ${PETUUM_LDFLAGS}
    ${PETUUM_PS_LIB}
)
