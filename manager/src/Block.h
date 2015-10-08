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

  // create basic data block 
  DataBlock(
      int _num_items, 
      int _item_length,
      int _item_size,
      int _align_width = 0)
    :
    num_items(_num_items),
    item_length(_item_length),
    align_width(_align_width),
    allocated(false),
    ready(false)
  {
    data_width = _item_size / _item_length;

    if (_align_width == 0 ||
        _item_size % _align_width == 0) 
    {
      item_size = _item_size;
      aligned = false;
    }
    else {
      item_size = (_item_length*data_width + _align_width - 1) /
        _align_width * _align_width;
      aligned = true;
    }
    length = _num_items * _item_length;
    size   = _num_items * _item_size;

    if (length <= 0 || size <= 0 || data_width < 1) {
      throw std::runtime_error("Invalid parameters");
    }

    // NOTE: lazy allocation
    //data = new char[_size];
  }

  ~DataBlock() {
    if (allocated && !data) {
      delete data; 
    }
  }


  // allocate data aligned to a given width
  void alloc(int _align_width);

  // allocate data
  virtual void alloc();

  // copy data from an array, use writeData with offset as subroutine
  void writeData(void* src, size_t _size);

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

  int getNumItems() { return num_items; }
  int getItemLength() { return item_length; }
  int getItemSize() { return item_size; }
  int getLength() { return length; }
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
  int align_width;  /* align data width per data item */
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
