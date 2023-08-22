add_library(
        ljdb_disk
        OBJECT
        disk_manager.cpp
)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:ljdb_disk>
        PARENT_SCOPE)