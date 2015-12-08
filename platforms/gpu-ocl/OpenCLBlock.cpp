#include <glog/logging.h>

#include "OpenCLEnv.h"
#include "OpenCLBlock.h"

namespace blaze {

#define LOG_HEADER  std::string("OpenCLBlock::") + \
                    std::string(__func__) +\
                    std::string("(): ")

void OpenCLBlock::alloc() {

  if (!allocated) {
    //NOTE: assuming buffer allocation is thread-safe
    //boost::lock_guard<OpenCLBlock> guard(*env);
    cl_context context = env->getContext();
    cl_int err = 0;

    if (flag == BLAZE_INPUT_BLOCK || 
        flag == BLAZE_SHARED_BLOCK) 
    {
      data = clCreateBuffer(
          context, CL_MEM_READ_ONLY,  
          size, NULL, &err);
    } else {
      data = clCreateBuffer(
          context, CL_MEM_WRITE_ONLY,  
          size, NULL, &err);
    }

    if (err != CL_SUCCESS) {
      throw std::runtime_error("Failed to allocate OpenCL block");
    }

    allocated = true;
  }
}

void OpenCLBlock::readFromMem(std::string path) {

  boost::iostreams::mapped_file_source fin;

  fin.open(path, size);

  if (fin.is_open()) {
    
    // first copy data from shared memory to a temp buffer 
    // NOTE: here the "size" maybe aligned size, so mem size 
    // should be calculated based on length
    size_t memsize = length * data_width;

    char* temp_data = new char[memsize];

    // memcpy is parallel among all tasks
    memcpy((void*)temp_data, (void*)fin.data(), memsize);

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

  // lazy allocation
  alloc();

  int data_size = size;

  boost::iostreams::mapped_file_params param(path); 
  param.flags = boost::iostreams::mapped_file::mapmode::readwrite;
  param.new_file_size = data_size;
  param.length = data_size;
  boost::iostreams::mapped_file_sink fout(param);

  if (fout.is_open()) {

    // first copy data from FPGA to a temp buffer, will be serialized among all tasks
    char* temp_data = new char[data_size];
    readData(temp_data, data_size);

    // then copy data from temp buffer to shared memory, in parallel among all tasks
    memcpy((void*)fout.data(), temp_data, data_size);

    delete [] temp_data;
    fout.close();
  }
  else {
    throw std::runtime_error(std::string("Cannot write file: ") + path);
  }
}

void OpenCLBlock::writeData(void* src, size_t _size) {
  if (_size > size) {
    throw std::runtime_error("Not enough space left in Block");
  }

  // lazy allocation
  alloc();

  if (!aligned) {
    writeData(src, _size, 0);
    ready = true;
  }
  else { // NOTE: there is no need to support aligned data 
    // get the command queue handler
    cl_command_queue command = env->getCmdQueue();

    // lock TaskEnv for exclusive access to OpenCL command queue
    //boost::lock_guard<OpenCLEnv> guard(*env);

    // array of cl_event to wait until all buffer copy is finished
    //cl_event *events = new cl_event[num_items];

    // copy the data element-by-element since each element is aligned
    for (int k=0; k<num_items; k++) {

      // element size in memory
      int data_size = item_length*data_width;

      int err = clEnqueueWriteBuffer(
          command, data, CL_TRUE, k*item_size, 
          data_size, (void*)((char*)src+k*data_size), 
          0, NULL, NULL);

      if (err != CL_SUCCESS) {
        throw std::runtime_error("Failed to write to OpenCL block");
      }
    }  
    ready = true;
    //delete [] events;
  }
}

void OpenCLBlock::writeData(void* src, size_t _size, size_t offset) {

  if (offset+_size > size) {
    throw std::runtime_error("Exists block size");
  }

  // lazy allocation
  alloc();

  // get the command queue handler
  cl_command_queue command = env->getCmdQueue();
  cl_event event;

  // use a lock on TaskEnv to guarantee single-thread access to command queues
  // NOTE: this is unnecessary if the OpenCL runtime is thread-safe
  //boost::lock_guard<OpenCLEnv> guard(*env);
  //env->lock();

  int err = clEnqueueWriteBuffer(
      command, data, CL_TRUE, offset, 
      _size, src, 0, NULL, &event);

  if (err != CL_SUCCESS) {
    DLOG(ERROR) << "clEnqueueWriteBuffer error: " << err;
    DLOG(ERROR) << "block infomation: size=" << _size <<
      ", offset=" << offset;
    throw std::runtime_error("clEnqueueWriteBuffer error");
  }
  //env->unlock();
  //clWaitForEvents(1, &event);

  if (offset + _size == size) {
    ready = true;
  }
}

// write data to an array
void OpenCLBlock::readData(void* dst, size_t size) {
  if (allocated) {

    // get the command queue handler
    cl_command_queue command = env->getCmdQueue();
    cl_event event;

    // use a lock on TaskEnv to guarantee single-thread access to command queues
    // NOTE: this is unnecessary if the OpenCL runtime is thread-safe
    //boost::lock_guard<OpenCLEnv> guard(*env);
    //env->lock();

    int err = clEnqueueReadBuffer(
      command, data, CL_TRUE, 0, 
      size, dst, 0, NULL, &event);

    if (err != CL_SUCCESS) {
      DLOG(ERROR) << "clEnqueueReadBuffer error: " << err;
      DLOG(ERROR) << "block infomation: size=" << size;
      throw std::runtime_error("Failed to read an OpenCL block");
    }
    //env->unlock();
    //clWaitForEvents(1, &event);
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

  OpenCLBlock* ocl_block = new OpenCLBlock(env,
        item_length, 
        item_size,
        aligned ? align_width : item_size);

  DataBlock_ptr block(ocl_block);

  cl_mem masked_data = *((cl_mem*)(ocl_block->getData()));

  // get the command queue handler
  cl_command_queue command = env->getCmdQueue();
  cl_int err = 0;

  // start copy the masked data items to the new block,
  // since the current block is read-only, do not need to enforce lock
  int k=0;

  // array of cl_event to wait until all buffer copy is finished
  cl_event *events = new cl_event[num_items];

  for (int i=0; i<num_items; i++) {
    if (mask[i] != 0) {
      err = clEnqueueCopyBuffer(command, 
          data, masked_data,
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

} // namespace

