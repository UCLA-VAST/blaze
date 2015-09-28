#include "Block.h"

namespace blaze {

#define LOG_HEADER  std::string("DataBlock::") + \
                    std::string(__func__) +\
                    std::string("(): ")

void DataBlock::alloc(int64_t _size) {

  if (!allocated) {
    size = _size;
    data = new char[size];
    allocated = true;
  }
}

void DataBlock::alloc(
    int _num_items,
    int _item_length, 
    int _data_width,
    int _align_width) 
{
  // input checks
  if (_num_items <= 0 || 
      _item_length <= 0 || 
      _align_width <= 0 ||
      _data_width <= 0) 
  {
    throw std::runtime_error("Invalid input");
  }
  item_length = _item_length;
  num_items   = _num_items;
  data_width  = _data_width;
  length      = _item_length * num_items;

  if (item_length*data_width % _align_width == 0) {
    item_size = item_length * data_width;
    alloc(item_size * num_items);
    aligned = false;
  }
  else {
    item_size = (item_length*data_width + _align_width - 1) /
                _align_width * _align_width;
    alloc(item_size * num_items);
    aligned = true;
    printf("aligning item_size=%d to %d\n", item_length*data_width, item_size);
  }
} 

void DataBlock::writeData(void* src, size_t _size) {
  if (!allocated) {
    throw std::runtime_error("Block memory not allocated");
  }
  if (!aligned) {
    writeData(src, _size, 0);
  }
  else {
    for (int k=0; k<num_items; k++) {
      int data_size = item_length*data_width;
      writeData((void*)((char*)src + k*data_size), 
          data_size, k*item_size);
    }  
    // TODO: this part should be automatics
    ready = true;
  }
}

// copy data from an array with offset
// TODO: this is used to write data item by item, so it should be used
// to put aligned data
void DataBlock::writeData(
    void* src, 
    size_t _size, 
    size_t offset) 
{
  if (allocated) {
    if (offset+_size > size) {
      throw std::runtime_error("Exists block size");
    }
    memcpy((void*)(data+offset), src, _size);

    if (offset + _size == size) {
      ready = true;
    }
  }
  else {
    throw std::runtime_error("Block memory not allocated");
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

  int item_length = length / num_items;
  int item_size   = size / num_items;

  // count the total number of 
  int masked_items = 0;
  for (int i=0; i<num_items; i++) {
    if (mask[i]!=0) {
      masked_items ++;
    }
  }
  
  DataBlock_ptr block(new DataBlock(
        item_length*masked_items, 
        item_size*masked_items));

  block->setNumItems(masked_items);
  
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
  block->ready = false;

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

    try {
      writeData(data, data_size);

    } catch(std::runtime_error &e) {
      throw e;
    }

    fin.close();
  }
  else {
    throw std::runtime_error("Cannot find file");
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

    try {
      readData((void*)fout.data(), data_size);
    } catch(std::runtime_error &e) {
      throw e;
    }

    fout.close();
  }
  else {
    throw std::runtime_error("Cannot find file");
  }
}

} // namespace
