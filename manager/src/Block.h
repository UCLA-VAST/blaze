#ifndef BLOCK_H
#define BLOCK_H

#include <string>

#include <boost/thread/lockable_adapter.hpp>
#include <boost/thread/thread.hpp>
#include <boost/thread/mutex.hpp>

#include "Common.h"

/*
 * base class extendable to manage memory block
 * on other memory space (e.g. FPGA device memory)
 */

namespace blaze {

class DataBlock
: public boost::basic_lockable_adapter<boost::mutex>
{

public:

  // create basic data block 
  DataBlock(int _num_items, 
      int _item_length,
      int _item_size,
      int _align_width = 0,
      int _flag = BLAZE_INPUT_BLOCK);
    
  DataBlock(const DataBlock &block);

  ~DataBlock() {
    if (allocated && !data) {
      delete data; 
    }
  }

  // allocate data aligned to a given width
  void alloc(int _align_width);

  // allocate data
  virtual void alloc();

  // copy data from an array
  virtual void writeData(void* src, size_t _size);

  // copy data from an array with offset
  virtual void writeData(void* src, size_t _size, size_t offset);

  // write data to an array
  virtual void readData(void* dst, size_t size);

  // get the pointer to data
  virtual char* getData();

  // sample the items in the block by a mask
  virtual boost::shared_ptr<DataBlock> sample(char* mask);

  virtual void readFromMem(std::string path);
  virtual void writeToMem(std::string path);

  int getNumItems() { return num_items; }
  int getItemLength() { return item_length; }
  int getItemSize() { return item_size; }
  int getLength() { return length; }
  int getSize() { return size; }

  // status check of DataBlock needs to be exclusive
  bool isAllocated();
  bool isReady();

protected:
  int flag;         /* enum: input, shared, output */
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
  DataBlock_ptr base_block;
};

const DataBlock_ptr NULL_DATA_BLOCK;

}
#endif
