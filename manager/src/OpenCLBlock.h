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

#ifndef OPENCLBLOCK_H
#define OPENCLBLOCK_H

#include <stdio.h>
#include <string.h>
#include <string>
#include <stdexcept>

#include <boost/smart_ptr.hpp>
#include <boost/iostreams/device/mapped_file.hpp>

#include "Block.h"
#include "OpenCLEnv.h"

namespace blaze {

class OpenCLBlock : public DataBlock 
{

public:
  // create a single output elements
  OpenCLBlock(OpenCLEnv* _env, int _length, int _size):
    env(_env)
  {
    length = _length;
    num_items = 1;
    size = _size;
    ready = false;

    cl_context context = env->getContext();

    // TODO: exception handling
    data = clCreateBuffer(
        context, CL_MEM_READ_ONLY,  
        _size, NULL, NULL);

    allocated = true;
  }
  
  OpenCLBlock(OpenCLEnv* _env): DataBlock(), env(_env)
  {
    ;  
  }

  OpenCLBlock(OpenCLEnv* _env, DataBlock *block):
    env(_env) 
  {
    length = block->getLength();
    size = block->getSize();
    num_items = block->getNumItems();
    if (block->isAllocated()) {
         
      cl_context context = env->getContext();

      data = clCreateBuffer(
          context, CL_MEM_READ_ONLY,  
          size, NULL, NULL);

      allocated = true;
    }
    // if ready, copy the data over
    if (block->isReady()) {
      writeData((void*)block->getData(), size);
      ready = true;
    }
  }
  
  ~OpenCLBlock() {
    if (allocated) {
      clReleaseMemObject(data);
    }
  }

  virtual void alloc(int64_t _size);

  // copy data from an array
  virtual void writeData(void* src, size_t _size);

  // copy data from an array with offset
  virtual void writeData(void* src, size_t _size, size_t offset);

  // write data to an array
  virtual void readData(void* dst, size_t size);

  virtual char* getData() { 

    if (allocated) {
      // this is a reinterpretive cast from cl_mem* to char*
      return (char*)&data; 
    }
    else {
      return NULL;
    }
  }

private:
  cl_mem data;
  OpenCLEnv *env;

};
}

#endif
