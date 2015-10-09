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

    data = clCreateBuffer(
        context, CL_MEM_READ_ONLY,  
        size, NULL, &err);

    if (err != CL_SUCCESS) {
      throw std::runtime_error("Failed to allocate OpenCL block");
    }

    allocated = true;
  }
}

void OpenCLBlock::writeData(void* src, size_t _size, size_t offset) {

  if (offset+_size > size) {
    throw std::runtime_error("Exists block size");
  }

  // lazy allocation
  alloc();

  // WriteBuffer need to be exclusive
  // lock env for this 
  boost::lock_guard<OpenCLEnv> guard(*env);

  // get the command queue handler
  cl_command_queue command = env->getCmdQueue();
  cl_event event;

  int err = clEnqueueWriteBuffer(
      command, data, CL_TRUE, offset, 
      _size, src, 0, NULL, &event);

  if (err != CL_SUCCESS) {
    throw std::runtime_error("Failed to write to OpenCL block");
  }
  clWaitForEvents(1, &event);

  if (offset + _size == size) {
    ready = true;
  }
}

// write data to an array
void OpenCLBlock::readData(void* dst, size_t size) {
  if (allocated) {

    // ReadBuffer need to be exclusive
    // lock env for this 
    boost::lock_guard<OpenCLEnv> guard(*env);
    
    // get the command queue handler
    cl_command_queue command = env->getCmdQueue();
    cl_event event;

    int err = clEnqueueReadBuffer(
      command, data, CL_TRUE, 0, 
      size, dst, 0, NULL, &event);

    if (err != CL_SUCCESS) {
      throw std::runtime_error("Failed to write to OpenCL block");
    }
    clWaitForEvents(1, &event);
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

  delete events;

  return block;
}

} // namespace

