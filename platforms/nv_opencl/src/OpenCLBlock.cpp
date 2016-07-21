#include <boost/smart_ptr.hpp>
#include <boost/iostreams/device/mapped_file.hpp>
#include <stdexcept>
#include <stdio.h>
#include <string.h>

#define LOG_HEADER  "OpenCLBlock"
#include <glog/logging.h>

#include "blaze/nv_opencl/OpenCLEnv.h"
#include "blaze/nv_opencl/OpenCLBlock.h"

namespace blaze {

OpenCLBlock::OpenCLBlock(
    std::vector<OpenCLEnv*> &_env_list, 
    int _num_items, 
    int _item_length,
    int _item_size,
    int _align_width, 
    int _flag):
  env_list(_env_list), 
  location(-1),   // uninitialized location
  DataBlock(_num_items, _item_length, _item_size, _align_width, _flag)
{ 
  num_devices = _env_list.size();
}

OpenCLBlock::OpenCLBlock(const OpenCLBlock& block): 
  DataBlock(block),
  env_list(block.env_list),
  data(block.data),
  location(block.location),
  num_devices(block.num_devices)
{
}

OpenCLBlock::~OpenCLBlock() {
  if (allocated && !copied) {
    if (flag == BLAZE_INPUT_BLOCK ||
        flag == BLAZE_OUTPUT_BLOCK) 
    {
      clReleaseMemObject(data[0]);
      delete data;
    } else {
      for (int d=0; d<num_devices; d++) {
        clReleaseMemObject(data[d]);
      } 
      delete [] data;
    }
  }
}

void OpenCLBlock::alloc() {

  if (!allocated) {
    int num_devices = env_list.size();
    cl_int err = 0;

    // figure out the device assignment
    if (flag == BLAZE_INPUT_BLOCK) {
      // for input block distribute the assignment as much as possible
      // here use thread id since each alloc() is called by comm threads
      location = getTid() % num_devices;
      
      cl_context context = env_list[location]->getContext();

      data = new cl_mem;
      data[0] = clCreateBuffer(
          context, CL_MEM_READ_ONLY,  
          size, NULL, &err);

      if (err != CL_SUCCESS) {
        LOG(ERROR) << "Cannot allocate input block, clCreateBuffer returns: "
          << err;

        throw std::runtime_error("Failed to allocate OpenCL block");
      }
      DLOG(INFO) << "Allocate input block on GPU_" << location;
    }
    else if (flag == BLAZE_SHARED_BLOCK) {
      // allocate on all devices 
      data = new cl_mem[num_devices];

      for (int d=0; d<num_devices; d++) {
        cl_context context = env_list[d]->getContext();

        data[d] = clCreateBuffer(
            context, CL_MEM_READ_ONLY,  
            size, NULL, &err);

        if (err != CL_SUCCESS) {
          LOG(ERROR) << "Cannot allocate shared block, clCreateBuffer returns: "
            << err;

          throw std::runtime_error("Failed to allocate OpenCL block");
        }       
        DLOG(INFO) << "Allocate shared block on GPU_" << d;
      }
    }
    else {
      // output block should be allocated on the same device as task execution
      // location should be assigned by dispatcher in queue manager
      if (location < 0) {
        DLOG(ERROR) << "Location should be assigned by now";
        throw std::runtime_error("Unexpected error in allocation");
      }
      cl_context context = env_list[location]->getContext();

      data = new cl_mem;
      data[0] = clCreateBuffer(
          context, CL_MEM_WRITE_ONLY,  
          size, NULL, &err);

      if (err != CL_SUCCESS) {
        LOG(ERROR) << "Cannot allocate output block, clCreateBuffer returns: "
          << err;

        throw std::runtime_error("Failed to allocate OpenCL block");
      }
      DLOG(INFO) << "Allocate output block on GPU_" << location;
    }
    allocated = true;
  }
}

void OpenCLBlock::readFromMem(std::string path) {

  boost::iostreams::mapped_file_source fin;

  fin.open(path, size);

  if (fin.is_open()) {
    
    uint64_t start_t = getUs();

    // first copy data from shared memory to a temp buffer 
    char* temp_data = new char[size];
    char* mem_ptr = (char*)fin.data();

    if (aligned) {
      for (int k=0; k<num_items; k++) {

        // element size in memory
        int data_size = item_length*data_width;

        // memcpy is parallel among all tasks
        memcpy((void*)(temp_data+k*item_size), 
            (void*)(mem_ptr+k*data_size), data_size);
      }
    }
    else {
      memcpy((void*)temp_data, (void*)mem_ptr, size);
    }

    uint64_t elapse_t = getUs() - start_t;
    DLOG(INFO) << "Read " <<
      (double)size /1024/1024 << "MB of data from mmap file takes " <<
      elapse_t << "us.";

    // then write temp buffer to FPGA, will be serialized among all tasks
    writeData(temp_data, size);

    delete [] temp_data;
    fin.close();
  }
  else {
    throw std::runtime_error(std::string("Cannot find file: ") + path);
  }
}

void OpenCLBlock::writeToMem(std::string path) {

  int data_size = size;

  boost::iostreams::mapped_file_params param(path); 
  param.flags = boost::iostreams::mapped_file::mapmode::readwrite;
  param.new_file_size = data_size;
  param.length = data_size;
  boost::iostreams::mapped_file_sink fout(param);

  if (!fout.is_open()) {
    throw fileError(std::string("Cannot write file: ") + path);
  }

  // first copy data from FPGA to a temp buffer, will be serialized among all tasks
  char* temp_data = new char[data_size];
  readData(temp_data, data_size);

  // then copy data from temp buffer to shared memory, in parallel among all tasks
  memcpy((void*)fout.data(), temp_data, data_size);

  delete [] temp_data;
  fout.close();

  // change permission
  boost::filesystem::wpath wpath(path);
  boost::filesystem::permissions(wpath, boost::filesystem::add_perms |
                                        boost::filesystem::group_read |
                                        boost::filesystem::others_read);
}

void OpenCLBlock::writeData(void* src, size_t _size) {
  if (_size > size) {
    throw std::runtime_error("Not enough space left in Block");
  }

  // lazy allocation
  alloc();

  uint64_t start_t = getUs();
  writeData(src, _size, 0);
  ready = true;

  uint64_t elapse_t = getUs() - start_t;
  DLOG(INFO) << "Writting OpenCLBlock of size " 
             << (double)size /1024/1024 << "MB takes " 
             << elapse_t << "us.";
}

void OpenCLBlock::writeData(void* src, size_t _size, size_t offset) {

  if (offset+_size > size) {
    throw std::runtime_error("Exists block size");
  }
  cl_event event;

  // lazy allocation
  alloc();

  if (flag == BLAZE_INPUT_BLOCK) {
    // get the command queue handler
    cl_command_queue command = env_list[location]->getCmdQueue();

    int err = clEnqueueWriteBuffer(
        command, data[0], CL_TRUE, offset, 
        _size, src, 0, NULL, &event);

    if (err != CL_SUCCESS) {
      DLOG(ERROR) << "clEnqueueWriteBuffer error: " << err;
      throw std::runtime_error("writeData() error");
    }
  }
  else { // must be BLAZE_SHARED_BLOCK
    for (int d=0; d<num_devices; d++) {
      // get the command queue handler
      cl_command_queue command = env_list[d]->getCmdQueue();
       
      int err = clEnqueueWriteBuffer(
          command, data[d], CL_TRUE, offset, 
          _size, src, 0, NULL, &event);

      if (err != CL_SUCCESS) {
        DLOG(ERROR) << "clEnqueueWriteBuffer error: " << err;
        throw std::runtime_error("writeData() error");
      }
    }
  }

  if (offset + _size == size) {
    ready = true;
  }
}

// write data to an array
void OpenCLBlock::readData(void* dst, size_t size) {
  if (allocated) {

    // NOTE: must be BLAZE_OUTPUT_BLOCK

    // get the command queue handler
    cl_command_queue command = env_list[location]->getCmdQueue();
    cl_event event;

    // use a lock on TaskEnv to guarantee single-thread access to command queues
    // NOTE: this is unnecessary if the OpenCL runtime is thread-safe
    //boost::lock_guard<OpenCLEnv> guard(*env);

    int err = clEnqueueReadBuffer(
      command, data[0], CL_TRUE, 0, 
      size, dst, 0, NULL, &event);

    if (err != CL_SUCCESS) {
      DLOG(ERROR) << "clEnqueueReadBuffer error: " << err;
      throw std::runtime_error("Failed to read an OpenCL block");
    }
  }
  else {
    throw std::runtime_error("Block memory not allocated");
  }
}

DataBlock_ptr OpenCLBlock::sample(char* mask) {

  // count the total number of 
  int masked_items = 0;
  for (int i=0; i<num_items; i++) {
    if (mask[i]!=0) {
      masked_items ++;
    }
  }

  OpenCLBlock* ocl_block = new OpenCLBlock(env_list,
        item_length, 
        item_size,
        aligned ? align_width : item_size);

  DataBlock_ptr block(ocl_block);

  cl_mem masked_data = *((cl_mem*)(ocl_block->getData()));

  // get the command queue handler
  cl_command_queue command = env_list[location]->getCmdQueue();
  cl_int err = 0;

  // start copy the masked data items to the new block,
  // since the current block is read-only, do not need to enforce lock
  int k=0;

  // array of cl_event to wait until all buffer copy is finished
  cl_event *events = new cl_event[num_items];

  for (int i=0; i<num_items; i++) {
    if (mask[i] != 0) {
      err = clEnqueueCopyBuffer(command, 
          data[0], masked_data,
          i*item_size, k*item_size,
          item_size, 
          0, NULL, events+k);

      if (err != CL_SUCCESS) {
        throw std::runtime_error(LOG_HEADER +
            std::string("error in clEnqueueCopyBuffer()"));
      }

      k++;
    }
  }
  err = clWaitForEvents(num_items, events);

  if (err != CL_SUCCESS) {
    throw std::runtime_error(LOG_HEADER +
        std::string("error during sampling"));
  }
  ocl_block->ready = true;

  delete [] events;

  return block;
}

char* OpenCLBlock::getData() {
  alloc();

  if (flag == BLAZE_INPUT_BLOCK ||
      flag == BLAZE_OUTPUT_BLOCK)
  {
    return (char*)data;
  }
  else {
    // location should be assigned already
    if (location < 0) {
      DLOG(ERROR) << "Location for shared block is not assigned";
      return NULL;
    }
    return (char*)&data[location];
  }
}

void OpenCLBlock::relocate(int loc) {
  if (flag == BLAZE_INPUT_BLOCK) {
    LOG(WARNING) << "relocation of input block is unsupported" ;
  }
  else {
    location = loc;  
    if (flag == BLAZE_SHARED_BLOCK) {
      DLOG(INFO) << "Assigned shared block to GPU_" << location;
    }
    else {
      DLOG(INFO) << "Assigned output block to GPU_" << location;
    }
  }
}

int OpenCLBlock::getDeviceId() {
  return location;
}

} // namespace

