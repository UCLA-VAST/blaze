#include <stdio.h>
#include <string.h>
#include <stdexcept>

#include <boost/smart_ptr.hpp>
#include <boost/iostreams/device/mapped_file.hpp>

#define LOG_HEADER "Block"
#include <glog/logging.h>

#include "blaze/Block.h"

namespace blaze {

// create basic data block 
DataBlock::DataBlock(
    int _num_items, 
    int _item_length,
    int _item_size,
    int _align_width,
    int _flag):
  num_items(_num_items),
  item_length(_item_length),
  align_width(_align_width),
  flag(_flag),
  allocated(false),
  ready(false),
  copied(false),
  data(NULL)
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
  length = num_items * item_length;
  size   = num_items * item_size;

  if (length <= 0 || size <= 0 || data_width < 1) {
    throw std::runtime_error("Invalid parameters");
  }

  // NOTE: lazy allocation
  //data = new char[_size];
}

DataBlock::DataBlock(const DataBlock &block) {

  DLOG(INFO) << "Create a duplication of a block";

  flag = block.flag;
  num_items = block.num_items;
  item_length = block.item_length;
  item_size = block.item_length;
  data_width = block.data_width;
  align_width = block.align_width;
  length = block.length;
  size = block.size;

  allocated = block.allocated;
  aligned = block.aligned;
  ready = block.ready;
  copied = true;

  data = block.data;
}

DataBlock::~DataBlock() {

  if (data && !copied) 
  {
    delete data; 
  }
}

void DataBlock::alloc() {
  if (!allocated) {
    data = new char[size];
    allocated = true;
  }
}

char* DataBlock::getData() { 
  boost::lock_guard<DataBlock> guard(*this);
  alloc();
  return data; 
}

bool DataBlock::isAllocated() { 
  boost::lock_guard<DataBlock> guard(*this);
  return allocated; 
}

bool DataBlock::isReady() { 
  boost::lock_guard<DataBlock> guard(*this);
  return ready; 
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

  int data_size = size;

  boost::iostreams::mapped_file_params param(path); 
  param.flags = boost::iostreams::mapped_file::mapmode::readwrite;
  param.new_file_size = data_size;
  param.length = data_size;
  boost::iostreams::mapped_file_sink fout(param);

  if (!fout.is_open()) {
    throw fileError(std::string("Cannot write file: ") + path);
  }
  // push data to file
  readData((void*)fout.data(), data_size);
  fout.close();

  // change permission
  // NOTE: here there might be a security issue need to be addressed
  boost::filesystem::wpath wpath(path);
  boost::filesystem::permissions(wpath, boost::filesystem::add_perms |
                                        boost::filesystem::group_read |
                                        boost::filesystem::others_read);
}

} // namespace
