add_definitions(${PETUUM_CXXFLAGS})

aux_source_directory(src SRC_LIST)
add_executable(matrixfact ${SRC_LIST})

target_link_libraries(matrixfact
    ${PETUUM_LDFLAGS}
    ${PETUUM_PS_LIB}
    ${PETUUM_PS_SN_LIB}
)
