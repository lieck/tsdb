add_subdirectory(cache)
add_subdirectory(common)
add_subdirectory(db)
add_subdirectory(disk)
add_subdirectory(mem_table)
add_subdirectory(sstable)
add_subdirectory(struct)
add_subdirectory(snappy)

add_library(ljdb STATIC ${ALL_OBJECT_FILES} TSDBEngineImpl.cpp)

set(LJDB_LIBS
        ljdb_cache
        ljdb_mem_table
        ljdb_sstable
        ljdb_db
        ljdb_disk
        ljdb_common
        snappy
        )

target_link_libraries(ljdb ${LJDB_LIBS})

target_include_directories(
        ljdb PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
