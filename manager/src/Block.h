#ifndef BLOCK_H
#define BLOCK_H

#include <stdio.h>
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
    item_length(_length),
    item_size(_size),
    num_items(1),
    length(_length), 
    size(_size),
    allocated(true),
    ready(false)
  {
    data_width = _size / _length;
    data = new char[_size];
  }

  DataBlock():
    length(0), num_items(0), size(0), data_width(0),
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


  // allocate data aligned to a given width
  void alloc(
      int _num_items, 
      int _item_length,
      int _data_width,
      int _align_width);

  // copy data from an array, use writeData with offset as subroutine
  void writeData(void* src, size_t _size);

  // allocate data of given size
  virtual void alloc(int64_t _size);

  // copy data from an array with offset
  virtual void writeData(void* src, size_t _size, size_t offset);

  // write data to an array
  virtual void readData(void* dst, size_t size);

  // get the pointer to data
  virtual char* getData() { 
    if (allocated) {
      return data; 
    }
    else {
      return NULL;
    }
  }

  // sample the items in the block by a mask
  virtual boost::shared_ptr<DataBlock> sample(char* mask);

  void readFromMem(std::string path);
  void writeToMem(std::string path);

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
  
  int item_length;  /* number of elements per data item */
  int item_size;    /* byte size per data item */
  int num_items;    /* number of data items per data block */
  int data_width;   /* byte size per element */
  int length;       /* total number of elements */
  int64_t size;     /* total byte size of the data block */

  bool allocated;
  bool aligned;
  bool ready;

private:
  char* data;
  boost::shared_ptr<DataBlock> base_block;
};

typedef boost::shared_ptr<DataBlock> DataBlock_ptr;

const DataBlock_ptr NULL_DATA_BLOCK;

}
#endif
