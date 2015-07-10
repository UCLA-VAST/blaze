#ifndef BLOCK_H
#define BLOCK_H

#include <string.h>
#include <string>
#include <stdexcept>
#include <boost/smart_ptr.hpp>

/*
 * make the base class extendable to manage memory block
 * on other memory space (e.g. FPGA device memory)
 *
 */

namespace acc_runtime {

class DataBlock {

// TODO: size may be unknown until read from file
// need to provide constructor without allocating 
// memory
public:
  DataBlock(int _length, int _size):
    length(_length), 
    size(_size)
  {
    width = _size / _length;
    data = new char[_size];

    ready = false;
    allocated = true;
  }

  DataBlock():
    length(0), size(0), width(0),
    allocated(false),
    ready(false)
  {
    ;  
  }

  void alloc(int _length, int _size) {
    length = _length;
    size = _size;
    width = size / length;

    data = new char[size];

    allocated = true;
  }

  // copy data from an array
  void writeData(void* src, size_t size) {
    if (allocated) {
      memcpy((void*)data, src, size);
      ready = true;
    }
    else {
      throw std::runtime_error("Block memory not allocated");
    }
  }

  void readData(void* dst, size_t size) {
    if (allocated) {
      memcpy(dst, (void*)data, size);
    }
    else {
      throw std::runtime_error("Block memory not allocated");
    }
  }

  char* getData() { 
    if (allocated) {
      return data; 
    }
    else {
      return NULL;
    }
  }

  int getLength() { return length; }
  int getSize() { return size; }

  bool isReady() {
    return ready; 
  }

  ~DataBlock() {
    if (allocated) {
      delete data; 
    }
  }


protected:
  int length;
  int width;
  int size;
  bool allocated;
  bool ready;
  char* data;
};

typedef boost::shared_ptr<DataBlock> DataBlock_ptr;

const DataBlock_ptr NULL_DATA_BLOCK;

}
#endif
