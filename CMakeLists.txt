# cmake_minimum_required(VERSION <specify CMake version here>)
cmake_minimum_required(VERSION 3.16)
project(analyze_broker)

SET(CMAKE_BUILD_TYPE "Debug")  # Release Debug
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fPIC")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIC")

SET(ROOT_DIR "/home/work/sys/lib")
macro(SUBDIRLIST result curdir)
    file(GLOB children RELATIVE ${curdir} ${curdir}/*)
    set(dirlist "")
    foreach(child ${children})
        if(IS_DIRECTORY ${curdir}/${child})
            LIST(APPEND dirlist ${child})
        endif()
    endforeach()
    set(${result} ${dirlist})
endmacro()

SUBDIRLIST(all_sub_index ${ROOT_DIR})
foreach(child ${all_sub_index})
    list(APPEND all_include ${ROOT_DIR}/${child}/include)
    list(APPEND all_lib ${ROOT_DIR}/${child}/lib)
endforeach()

include_directories(${all_include} /home/work/sys/develop/googletest/googletest/include)
link_directories(${all_lib} /home/work/sys/develop/googletest/bin/lib)

SET(FEEDER analyze_broker_data)
aux_source_directory (./src/ COMPAREONE_ALL)
add_executable(${FEEDER} ${COMPAREONE_ALL})

target_link_libraries(${FEEDER}
        feeder coral swordfish x stdc++fs yaml-cpp clickhouse-cpp-lib-static boost_date_time boost_filesystem boost_regex boost_system  boost_chrono boost_log boost_program_options boost_thread boost_iostreams z protobuf protobuf-lite sodium zmq ssl crypto iconv pthread dl)

