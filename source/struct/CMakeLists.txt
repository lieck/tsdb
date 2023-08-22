add_library(
        struct
        OBJECT
        ColumnValue.cpp
        Row.cpp
        Schema.cpp
        Vin.cpp
)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:struct>
        PARENT_SCOPE)