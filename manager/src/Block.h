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

#ifndef BLOCK_H
#define BLOCK_H

#include <string.h>
#include <string>
#include <stdexcept>

#include <boost/smart_ptr.hpp>
#include <boost/iostreams/device/mapped_file.hpp>
#include <boost/thread/thread.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/lockable_adapter.hpp>

/*
 * base class extendable to manage memory block
 * on other memory space (e.g. FPGA device memory)
 *
 */

namespace blaze {

class DataBlock
: public boost::basic_lockable_adapter<boost::mutex>
{

public:
  // create a single output elements
  DataBlock(int _length, int64_t _size):
    length(_length), 
    num_items(1),
    size(_size),
    allocated(true),
    ready(false)
  {
    //width = _size / _length;
    data = new char[_size];

  }

  DataBlock():
    length(0), num_items(0), size(0), width(0),
    allocated(false),
    ready(false),
    data(NULL)
  {
    ;  
  }

  ~DataBlock() {
    if (allocated && !data) {
      delete data; 
    }
  }

  virtual void alloc(int64_t _size);

  // copy data from an array
  virtual void writeData(void* src, size_t _size);

  // copy data from an array with offset
  virtual void writeData(void* src, size_t _size, size_t offset);

  // write data to an array
  virtual void readData(void* dst, size_t size);

  void readFromMem(std::string path);

  void writeToMem(std::string path);

  virtual char* getData() { 
    if (allocated) {
      return data; 
    }
    else {
      return NULL;
    }
  }

  int setLength(int _length) { 
    length = _length; 
  }

  int setNumItems(int _num) { 
    num_items = _num; 
  }

  int getLength() { return length; }

  int getNumItems() { return num_items; }

  int getSize() { return size; }

  // status check of DataBlock needs to be exclusive
  bool isAllocated() { 
    boost::lock_guard<DataBlock> guard(*this);
    return allocated; 
  }
  bool isReady() { 
    boost::lock_guard<DataBlock> guard(*this);
    return ready; 
  }

protected:
  int length;       /* total number of elements */
  int num_items;    /* number of elements per data item */
  int width;        /* size of a single element */
  int64_t size;     /* byte size of all the data */
  bool allocated;
  bool ready;

private:
  char* data;
};

typedef boost::shared_ptr<DataBlock> DataBlock_ptr;

const DataBlock_ptr NULL_DATA_BLOCK;

}
#endif
