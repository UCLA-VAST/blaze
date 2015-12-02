#include <climits>
#include <glog/logging.h>

#include "Block.h"
#include "BlockManager.h"
#include "Platform.h"

namespace blaze {

// create a block, if no space left then return NULL
DataBlock_ptr BlockManager::create(
    int num_items, 
    int item_length,
    int item_size,
    int align_size) 
{
  int block_size;
  if (align_size > 0 && item_size % align_size != 0) {
    block_size = num_items*((item_size+align_size-1)/align_size)*align_size;
  }
  else {
    block_size = num_items*item_size;
  }
  // do not create new block if there is no space left 
  // in either cache or scratch, for safety
  if (block_size + cacheSize > maxCacheSize &&
      block_size + scratchSize > maxScratchSize)
  {
    return NULL_DATA_BLOCK;
  }
  else {
    DataBlock_ptr block = platform->createBlock(
        num_items, item_length, item_size, align_size);

    return block;
  }
}

// create a block if it does not exist in the manager
// return true if a new block is created
bool BlockManager::getAlloc(
    int64_t tag, 
    DataBlock_ptr &block,
    int num_items,
    int item_length,
    int item_size,
    int align_size)
{
  // guarantee exclusive access
  boost::lock_guard<BlockManager> guard(*this);

  if (!contains(tag)) {

    // create block never throw exception
    block = platform->createBlock(
        num_items, 
        item_length, 
        item_size, 
        align_size);

    try {
      do_add(tag, block);

      return true;
    }
    catch (std::runtime_error &e) {
      LOG(ERROR) << "Cannot to add block " << tag;
      return false;
    }
  }
  else {
    if (tag < 0) {
      block = scratchTable[tag];
    }
    else {
      block = cacheTable[tag].second;
    }
    return false;
  }
}

DataBlock_ptr BlockManager::get(int64_t tag) {

  // guarantee exclusive access
  boost::lock_guard<BlockManager> guard(*this);

  // log info
  VLOG(2) << "Requesting block " << tag;
  
  if (tag < 0) {
    if (scratchTable.find(tag) == scratchTable.end()) {
      return NULL_DATA_BLOCK;
    }
    else {
      return scratchTable[tag];
    }
  }
  else {
    if (cacheTable.find(tag) == cacheTable.end()) {
      return NULL_DATA_BLOCK;
    }
    else {
      // accumulate the access count for the block
      // NOTE: better performance if this is being
      // done asynchronously
      update(tag);

      return cacheTable[tag].second;
    }
  }
}

void BlockManager::add(
    int64_t tag, 
    DataBlock_ptr block)
{
  // guarantee exclusive access
  boost::lock_guard<BlockManager> guard(*this);
  try {
    do_add(tag, block);
  }
  catch (std::runtime_error &e) {
    // do not throw exceptions, simply log info and return 
    LOG(ERROR) << "Cannot to add block " << tag;
  }
}

void BlockManager::do_add(int64_t tag, DataBlock_ptr block) {

  // check if block already exists
  if (contains(tag)) {
    return;
  }
  if (tag < 0) { // scratch block
    if (scratchSize + block->getSize() >= maxScratchSize) {

      // cannot add because running out of space
      throw std::runtime_error("Cannot add broadcast with size: " + 
          std::to_string((long long)block->getSize())+
          ", maxsize is "+
          std::to_string((long long)maxScratchSize));
    }
    VLOG(2) << "Adding block " << tag << " to scratch";

    // add the index to cacheTable
    scratchTable.insert(std::make_pair(tag, block));

    // increase the current scratchSize
    scratchSize += block->getSize();
  }
  else {
    // log info
    VLOG(2) << "Adding block " << tag << " to cache";

    // do not add if current block is too big
    if (block->getSize() > maxCacheSize) {
      throw std::runtime_error("no space left"); 
    }

    // remove block from cache if no space left
    while (cacheSize + block->getSize() > maxCacheSize) {
      evict();
    }

    // add the index to cacheTable
    cacheTable.insert(std::make_pair(tag, std::make_pair(0, block)));

    // increase the current cacheSize
    cacheSize += block->getSize();
  }
}

void BlockManager::remove(int64_t tag) {

  // guarantee exclusive access
  boost::lock_guard<BlockManager> guard(*this);

  // can only remove scratch data
  if (tag < 0) {

    if (scratchTable.find(tag) == scratchTable.end()) {
      // no data match tag; 
      // do nothing
    }
    else {
      DataBlock_ptr block = scratchTable[tag];
      //scratchSize -= block->getSize();

      //delete block;
      scratchTable.erase(tag);

      VLOG(2) << "Removed block " << tag;
    }
  }
}

void BlockManager::evict() {

  if (cacheTable.size()==0) {
    throw std::runtime_error("no space left"); 
  }
   
  // find the block that has the least access count
  int min_val = INT_MAX;
  std::map<int64_t, std::pair<int, DataBlock_ptr> >::iterator min_idx; 
  std::map<int64_t, std::pair<int, DataBlock_ptr> >::iterator iter; 
  for (iter = cacheTable.begin(); 
       iter != cacheTable.end(); 
       iter ++)
  {
    if (iter->second.first == 0) {
      // early jump out
      min_idx = iter; 
      break;
    }
    if (min_val > iter->second.first) {
      min_val = iter->second.first;
      min_idx = iter;
    }
  }

  int size = min_idx->second.second->getSize();
  int64_t tag = min_idx->first;

  // remove the block 
  cacheTable.erase(min_idx);

  // decrease the current cacheSize
  cacheSize -= size;

  // log info
  VLOG(1) << "Evicted block " << tag;
}

// update access count for a block, 
// and then update cacheTable for new indexes
void BlockManager::update(int64_t tag) {

  // log info
  VLOG(2) << "Update block " << tag;

  cacheTable[tag].first += 1;
}

}
