add_definitions(${PETUUM_CXXFLAGS})
#petuum-ps-common
aux_source_directory(./petuum_ps_common/client COMMON_SRC_LIST)
aux_source_directory(./petuum_ps_common/comm_bus COMMON_SRC_LIST)
aux_source_directory(./petuum_ps_common/consistency COMMON_SRC_LIST)
aux_source_directory(./petuum_ps_common/oplog COMMON_SRC_LIST)
aux_source_directory(./petuum_ps_common/server COMMON_SRC_LIST)
aux_source_directory(./petuum_ps_common/storage COMMON_SRC_LIST)
aux_source_directory(./petuum_ps_common/include COMMON_SRC_LIST)
aux_source_directory(./petuum_ps_common/thread COMMON_SRC_LIST)
aux_source_directory(./petuum_ps_common/util COMMON_SRC_LIST)

#petuum-ps
aux_source_directory(./petuum_ps/client PS_SRC_LIST)
aux_source_directory(./petuum_ps/consistency PS_SRC_LIST)
aux_source_directory(./petuum_ps/oplog PS_SRC_LIST)
aux_source_directory(./petuum_ps/server PS_SRC_LIST)
aux_source_directory(./petuum_ps/thread PS_SRC_LIST)

#petuum-ps-sn
aux_source_directory(./petuum_ps_sn/thread PS_SN_SRC_LIST)
aux_source_directory(./petuum_ps_sn/client PS_SN_SRC_LIST)
aux_source_directory(./petuum_ps_sn/consistency PS_SN_SRC_LIST)

#petuum-ps
add_library(${PETUUM_PS_LIB} STATIC ${COMMON_SRC_LIST} ${PS_SRC_LIST})
target_link_libraries(${PETUUM_PS_LIB} ${PETUUM_LDFLAGS})

#petuum-ps-sn
add_library(${PETUUM_PS_SN_LIB} STATIC ${COMMON_SRC_LIST} ${PS_SN_SRC_LIST})
target_link_libraries(${PETUUM_PS_SN_LIB} ${PETUUM_LDFLAGS})
