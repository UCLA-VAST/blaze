#include <boost/iostreams/device/mapped_file.hpp>
#include <boost/thread/thread.hpp>
#include <boost/thread/mutex.hpp>

#include "Block.h"

namespace blaze {

#define LOG_HEADER  std::string("DataBlock::") + \
                    std::string(__func__) +\
                    std::string("(): ")

void DataBlock::alloc() {
  if (!allocated) {
    data = new char[size];
    allocated = true;
  }
}

void DataBlock::writeData(void* src, size_t _size) {
  if (_size > size) {
    throw std::runtime_error("Not enough space left in Block");
  }
  if (!aligned) {
    boost::lock_guard<DataBlock> guard(*this);
    writeData(src, _size, 0);
  }
  else {
    boost::lock_guard<DataBlock> guard(*this);
    for (int k=0; k<num_items; k++) {
      int data_size = item_length*data_width;
      writeData((void*)((char*)src + k*data_size), 
          data_size, k*item_size);
    }  
    ready = true;
  }
}

// copy data from an array with offset
// this method needs to be locked from outside
void DataBlock::writeData(
    void* src, 
    size_t _size, 
    size_t _offset)
{
  if (_offset+_size > size) {
    throw std::runtime_error("Exists block size");
  }
  // lazy allocation
  alloc();

  memcpy((void*)(data+_offset), src, _size);

  if (_offset + _size == size) {
    ready = true;
  }
}

// write data to an array
void DataBlock::readData(void* dst, size_t size) {
  if (allocated) {
    memcpy(dst, (void*)data, size);
  }
  else {
    throw std::runtime_error("Block memory not allocated");
  }
}

DataBlock_ptr DataBlock::sample(char* mask) {

  // count the total number of 
  int masked_items = 0;
  for (int i=0; i<num_items; i++) {
    if (mask[i]!=0) {
      masked_items ++;
    }
  }
  
  DataBlock_ptr block(new DataBlock(
        masked_items,
        item_length, 
        item_size,
        aligned ? align_width : item_size));

  char* masked_data = block->getData();

  int k=0;
  for (int i=0; i<num_items; i++) {
    if (mask[i] != 0) {
      memcpy(masked_data+k*item_size, 
             data+i*item_size, 
             item_size);
      k++;
    }
  }
  block->ready = true;

  return block;
}

void DataBlock::readFromMem(std::string path) {

  if (ready) {
    return;
  }

  boost::iostreams::mapped_file_source fin;

  //int data_length = length; 
  int data_size = size;

  fin.open(path, data_size);

  if (fin.is_open()) {
    
    void* data = (void*)fin.data();

    writeData(data, data_size);

    fin.close();
  }
  else {
    throw std::runtime_error(std::string("Cannot find file: ") + path);
  }
}

void DataBlock::writeToMem(std::string path) {

  //int data_length = length; 
  int data_size = size;

  boost::iostreams::mapped_file_params param(path); 
  param.flags = boost::iostreams::mapped_file::mapmode::readwrite;
  param.new_file_size = data_size;
  param.length = data_size;
  boost::iostreams::mapped_file_sink fout(param);

  if (fout.is_open()) {

    readData((void*)fout.data(), data_size);

    fout.close();
  }
  else {
    throw std::runtime_error(std::string("Cannot write file: ") + path);
  }
}

} // namespace
