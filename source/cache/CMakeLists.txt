add_library(
        ljdb_cache
        OBJECT
        cache.cpp
        table_cache.cpp
)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:ljdb_cache>
        PARENT_SCOPE)