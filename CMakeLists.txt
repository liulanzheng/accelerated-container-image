cmake_minimum_required(VERSION 3.8)

project(
  accelerated-container-image
  VERSION 0.1.0
  LANGUAGES C CXX
)

set(LIBRARY_OUTPUT_PATH "${CMAKE_SOURCE_DIR}/build/output")
set(EXECUTABLE_OUTPUT_PATH "${CMAKE_SOURCE_DIR}/build/output")

file(GLOB_RECURSE PKG_SRC "pkg/*.go")
file(GLOB SNAPSHOTTER_SRC "cmd/overlaybd-snapshotter/*.go")
add_custom_command(
    OUTPUT ${EXECUTABLE_OUTPUT_PATH}/snapshotter
    COMMAND go build -o ${EXECUTABLE_OUTPUT_PATH}/snapshotter "${CMAKE_CURRENT_SOURCE_DIR}/cmd/overlaybd-snapshotter/"
    DEPENDS ${SNAPSHOTTER_SRC} ${PKG_SRC}
)

file(GLOB CTR_SRC "cmd/ctr/*.go")
add_custom_command(
    OUTPUT ${EXECUTABLE_OUTPUT_PATH}/ctr
    COMMAND go build -o ${EXECUTABLE_OUTPUT_PATH}/ctr "${CMAKE_CURRENT_SOURCE_DIR}/cmd/ctr/"
    DEPENDS ${CTR_SRC} ${PKG_SRC}
)

add_custom_target(cmd ALL DEPENDS ${EXECUTABLE_OUTPUT_PATH}/snapshotter ${EXECUTABLE_OUTPUT_PATH}/ctr)

install(PROGRAMS ${EXECUTABLE_OUTPUT_PATH}/ctr ${EXECUTABLE_OUTPUT_PATH}/snapshotter DESTINATION /opt/overlaybd/bin)
install(FILES "cmd/overlaybd-snapshotter/config.json" DESTINATION /etc/overlaybd-snapshotter)

include(CMake/cpack.cmake)