#include <climits>

#include "BlockManager.h"

namespace acc_runtime {

#define LOG_HEADER  std::string("BlockManager::") + \
                    std::string(__func__) +\
                    std::string("(): ")

DataBlock_ptr BlockManager::get(int tag) {

  // guarantee exclusive access
  boost::lock_guard<BlockManager> guard(*this);

  // log info
  std::string msg = LOG_HEADER + 
                    std::string("requesting block ") +
                    std::to_string((long long int)tag);
  logger->logInfo(msg);
  
  if (cacheTable.find(tag) == cacheTable.end()) {
    return NULL_DATA_BLOCK;
  }
  else {
    // accumulate the access count for the block
    // TODO: should do it async
    update(tag);

    return cacheTable[tag].second;
  }
}

void BlockManager::add(
    int tag, 
    DataBlock_ptr block)
{
  // guarantee exclusive access
  boost::lock_guard<BlockManager> guard(*this);

  // check if block already exists
  if (cacheTable.find(tag) != cacheTable.end()) {
    return;
  }

  // log info
  std::string msg = LOG_HEADER + 
                    std::string("adding block ") +
                    std::to_string((long long int)tag);
  logger->logInfo(msg);

  while (cacheSize + block->getSize() > maxCacheSize) {

    // remove block from cache
    evict();
  }

  // add the index to cacheTable
  cacheTable.insert(
      std::make_pair(
        tag, 
        std::make_pair(0, block)
        ));

  // increase the current cacheSize
  cacheSize += block->getSize();
}


// TODO: this function seems to be useless
/*
DataBlock_ptr BlockManager::getOrAlloc(int tag, int size) {

  // guarantee exclusive access
  boost::lock_guard<BlockManager> guard(*this);

  // ISSUE: it is possible that between the block is removed 
  // while it is being used

  if (cacheTable.find(tag) == cacheTable.end()) {
    DataBlock_ptr new_block(new DataBlock());

    add(tag, new_block);
    return new_block;
  }
  else {
    DataBlock_ptr block = cacheTable[tag].second;

    update(tag);

    return block;
  }
}
*/

DataBlock_ptr BlockManager::getShared(int tag) {

  // guarantee exclusive access
  boost::lock_guard<BlockManager> guard(*this);
  
  if (scratchTable.find(tag) == scratchTable.end()) {
    return NULL_DATA_BLOCK;
  }
  else {
    return scratchTable[tag]; 
  }
}

int BlockManager::addShared(
    int tag, 
    DataBlock_ptr block)
{
  // guarantee exclusive access
  boost::lock_guard<BlockManager> guard(*this);
  
  if (scratchSize + block->getSize() >= maxScratchSize) {

    // cannot add because running out of space
    logger->logInfo(
        LOG_HEADER+
        "cannot add broadcast with size:" + 
        std::to_string((long long)block->getSize())+
        ", maxsize is "+
        std::to_string((long long)maxScratchSize));

    return -1;
  }
  else if (scratchTable.find(tag) == scratchTable.end()){
    // add new block
    scratchTable.insert(std::make_pair(tag, block));
    scratchSize += block->getSize();

    logger->logInfo(
        LOG_HEADER+
        "added broadcast block "+
        std::to_string((long long)tag)+
        " with size:" + 
        std::to_string((long long)block->getSize()));

    return 0;
  }
  else {
    // block already exist
    return 0;
  }
}

int BlockManager::removeShared(int tag) {

  // guarantee exclusive access
  boost::lock_guard<BlockManager> guard(*this);
  
  if (scratchTable.find(tag) == scratchTable.end()) {
    // no data match tag; 
    return 1;
  }
  else {
    DataBlock_ptr block = scratchTable[tag];
    scratchSize -= block->getSize();

    //delete block;
    scratchTable.erase(tag);

    return 0;
  }
}

void BlockManager::evict() {
   
  // find the block that has the least access count
  int min_val = INT_MAX;
  std::map<int, std::pair<int, DataBlock_ptr> >::iterator min_idx; 
  std::map<int, std::pair<int, DataBlock_ptr> >::iterator iter; 
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
  int tag = min_idx->first;

  // remove the block 
  cacheTable.erase(min_idx);

  // decrease the current cacheSize
  cacheSize -= size;

  // log info
  std::string msg = LOG_HEADER + 
                    std::string("evicted block ") +
                    std::to_string((long long int)tag);
  logger->logInfo(msg);
}

// update access count for a block, 
// and then update cacheTable for new indexes
void BlockManager::update(int tag) {

  // log info
  std::string msg = LOG_HEADER + 
                    std::string("updating block ") +
                    std::to_string((long long int)tag);
  logger->logInfo(msg);

  cacheTable[tag].first += 1;
}

}
