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

  // create basic data block with one item
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
