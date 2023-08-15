add_library(
        ljdb_db
        OBJECT
        background.cpp
        compaction.cpp
        file_meta_data.cpp
        format.cpp
        table.cpp
        table_meta_data.cpp
        db_options.cpp
)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:ljdb_db>
        PARENT_SCOPE)