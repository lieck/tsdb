add_library(
        ljdb_sstable
        OBJECT
        block.cpp
        block_builder.cpp
        sstable.cpp
        sstable_builder.cpp
)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:ljdb_sstable>
        PARENT_SCOPE)