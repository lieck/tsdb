add_library(
        ljdb_db
        OBJECT
        background.cpp
        compaction.cpp
        format.cpp
        merger_iterator.cpp
        table.cpp
        meta_data.cpp
        two_level_iterator.cpp
)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:ljdb_db>
        PARENT_SCOPE)