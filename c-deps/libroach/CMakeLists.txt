# Copyright 2017 The Cockroach Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied. See the License for the specific language governing
# permissions and limitations under the License.

# NB: Despite CMake's portability, this build configuration makes no attempt to
# support non-GCC-like compilers.

# The CXX_STANDARD property was introduced in version 3.1.
cmake_minimum_required(VERSION 3.1 FATAL_ERROR)

project(roachlib)
SET(CMAKE_C_COMPILER "g++")
set(CMAKE_CXX_STANDARD 11)
ADD_DEFINITIONS(-std=c++11)
#link_libraries("/home/victor/workspace/native/x86_64-linux-gnu/rocksdb/librocksdb.a")

add_library(roach
  db.cc
  encoding.cc
  eventlistener.cc
  protos/cockroach/pkg/roachpb/data.pb.cc
  protos/cockroach/pkg/roachpb/internal.pb.cc
  protos/cockroach/pkg/roachpb/metadata.pb.cc
  protos/cockroach/pkg/storage/engine/enginepb/mvcc.pb.cc
  protos/cockroach/pkg/storage/engine/enginepb/rocksdb.pb.cc
  protos/cockroach/pkg/util/hlc/timestamp.pb.cc
  protos/cockroach/pkg/util/unresolved_addr.pb.cc
  insert_from_crdb.cpp include/encoding_infomation.h
  encoding_information.cpp rocksIO_op.cpp include/rocksIO_op.h select_from_libroach.cpp include/select_from_libroach.h
        include/query_info.h handle_stmt.cpp include/handle_stmt.h)
target_include_directories(roach
  PUBLIC  ./include
  PRIVATE ../protobuf/src ../rocksdb/include protos
)

add_library(roachccl
  ccl/db.cc
)



target_include_directories(roachccl
  PRIVATE ../rocksdb/include
)
target_link_libraries(roachccl roach)

set_target_properties(roach roachccl PROPERTIES
  CXX_STANDARD 11
  CXX_STANDARD_REQUIRED YES
  CXX_EXTENSIONS NO
  COMPILE_OPTIONS "-Werror;-Wall;-Wno-sign-compare;-std=c++11;"
)
