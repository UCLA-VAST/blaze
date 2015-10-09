/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "Block.h"

namespace blaze {

#define LOG_HEADER  std::string("DataBlock::") + \
                    std::string(__func__) +\
                    std::string("(): ")

void DataBlock::alloc(int64_t _size) {

  // guarantee exclusive access by outside lock
  //boost::lock_guard<DataBlock> guard(*this);

  if (!allocated) {
    size = _size;
    data = new char[size];
    allocated = true;
  }
}

void DataBlock::writeData(void* src, size_t _size) {
  if (allocated) {
    memcpy((void*)data, src, _size);
    ready = true;
  }
  else {
    throw std::runtime_error("Block memory not allocated");
  }
}
// copy data from an array with offset
void DataBlock::writeData(
    void* src, 
    size_t _size, 
    size_t offset) 
{
  if (allocated) {
    if (offset+_size > size) {
      throw std::runtime_error("Exists block size");
    }
    memcpy((void*)(data+offset), src, _size);

    if (offset + _size == size) {
      ready = true;
    }
  }
  else {
    throw std::runtime_error("Block memory not allocated");
  }
}

// write data to an array
void DataBlock::readData(void* dst, size_t size) {
  if (allocated) {
    memcpy(dst, (void*)data, size);
  }
  else {
    throw std::runtime_error("Block memory not allocated");
  }
}

void DataBlock::readFromMem(std::string path) {
  // guarantee exclusive access by outside lock
  // to avoid reading memory for the same block simutainously
  //boost::lock_guard<DataBlock> guard(*this);

  if (ready) {
    return;
  }

  boost::iostreams::mapped_file_source fin;

  //int data_length = length; 
  int data_size = size;

  fin.open(path, data_size);

  if (fin.is_open()) {
    
    void* data = (void*)fin.data();

    try {
      writeData(data, data_size);

    } catch(std::runtime_error &e) {
      throw e;
    }

    fin.close();
  }
  else {
    throw std::runtime_error("Cannot find file");
  }
}

void DataBlock::writeToMem(std::string path) {

  //int data_length = length; 
  int data_size = size;

  boost::iostreams::mapped_file_params param(path); 
  param.flags = boost::iostreams::mapped_file::mapmode::readwrite;
  param.new_file_size = data_size;
  param.length = data_size;
  boost::iostreams::mapped_file_sink fout(param);

  if (fout.is_open()) {

    try {
      readData((void*)fout.data(), data_size);
    } catch(std::runtime_error &e) {
      throw e;
    }

    fout.close();
  }
  else {
    throw std::runtime_error("Cannot find file");
  }
}

} // namespace
