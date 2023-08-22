add_library(
        ljdb_common
        OBJECT
        merger_iterator.cpp
        two_level_iterator.cpp
)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:ljdb_common>
        PARENT_SCOPE)