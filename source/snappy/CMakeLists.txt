add_library(
        snappy
        OBJECT
        snappy.cpp
        snappy-sinksource.cpp
        snappy-stubs-internal.cpp
)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:snappy>
        PARENT_SCOPE)