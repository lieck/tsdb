add_library(
        ljdb_mem_table
        OBJECT
        mem_table.cpp
)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:ljdb_mem_table>
        PARENT_SCOPE)