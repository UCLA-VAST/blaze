#include <climits>

#include "BlockManager.h"

namespace blaze {

#define LOG_HEADER  std::string("BlockManager::") + \
                    std::string(__func__) +\
                    std::string("(): ")

DataBlock_ptr BlockManager::create() {

  DataBlock_ptr block;
  switch (env->getType()) {
    case AccType::CPU: 
    {
      DataBlock_ptr bp(new DataBlock());
      block = bp;
      break;
    }
    case AccType::OpenCL:
    {
      DataBlock_ptr bp(new OpenCLBlock(
          dynamic_cast<OpenCLEnv*>(env)));
      block = bp;
      break;
    }
    default:
    {
      throw std::runtime_error("Invalid platform");
    }
  }
  return block;
}

// create a block if it does not exist in the manager
// return true if a new block is created
bool BlockManager::create(
    int64_t tag,
    DataBlock_ptr &block)
{
  // guarantee exclusive access
  boost::lock_guard<BlockManager> guard(*this);

  if (!contains(tag)) {
    logger->logInfo(LOG_HEADER+
        "creating block for id="+
        std::to_string(tag));

    try {
      switch (env->getType()) {
        case AccType::CPU:
          {
            DataBlock_ptr bp(new DataBlock());
            block = bp;
            break;
          }
        case AccType::OpenCL:
          {
            DataBlock_ptr bp(new OpenCLBlock(
                  dynamic_cast<OpenCLEnv*>(env)));
            block = bp;
            break;
          }
        default: ; // never end up here
      } 
      // add the block to manager
      // TODO: do not control scratch size in the experiments
      //if (scratchSize + block->getSize() >= maxScratchSize) {

      //  // cannot add because running out of space
      //  throw std::runtime_error(LOG_HEADER+
      //      "cannot add broadcast with size:" + 
      //      std::to_string((long long)block->getSize())+
      //      ", current size is "+
      //      std::to_string((long long)scratchSize));
      //}

      // add the index to cacheTable
      scratchTable.insert(std::make_pair(tag, block));

      // increase the current scratchSize
      //scratchSize += block->getSize();

      return true;
    }
    catch (std::runtime_error &e) {
      throw e;
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
  std::string msg = LOG_HEADER + 
                    std::string("requesting block ") +
                    std::to_string((long long)tag);
  logger->logInfo(msg);
  
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

  if (tag < 0) { // scratch block
    
    if (scratchTable.find(tag) != scratchTable.end()) {
      return;
    }
    if (!block->isAllocated()) {
      throw std::runtime_error(LOG_HEADER+ 
          "Block is not allocated cannot be added");
    }
    if (scratchSize + block->getSize() >= maxScratchSize) {

      // cannot add because running out of space
      throw std::runtime_error(LOG_HEADER+
          "cannot add broadcast with size:" + 
          std::to_string((long long)block->getSize())+
          ", maxsize is "+
          std::to_string((long long)maxScratchSize));
    }

    // add the index to cacheTable
    scratchTable.insert(std::make_pair(tag, block));

    // increase the current scratchSize
    scratchSize += block->getSize();
  }
  else {
    // check if block already exists
    if (cacheTable.find(tag) != cacheTable.end()) {
      return;
    }

    // log info
    logger->logInfo(LOG_HEADER + 
        std::string("adding block ") +
        std::to_string((long long int)tag) + 
        std::string(" to cache."));

    try {
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
    catch (std::runtime_error &e) {
      // do not throw exceptions, simply log info and return 
      logger->logInfo(LOG_HEADER + 
          std::string("Failed to add block ") +
          std::to_string((long long int)tag));
      return;
      //throw std::runtime_error(LOG_HEADER+
      //    "Caught exception: "+e.what());
    }
  }
}

void BlockManager::remove(int64_t tag) {

  // guarantee exclusive access
  boost::lock_guard<BlockManager> guard(*this);

  // can only remove scratch data
  if (tag < 0) {

    if (scratchTable.find(tag) == scratchTable.end()) {
      // no data match tag; 
      throw std::runtime_error(LOG_HEADER+
          "no matching tag in scratch table");
    }
    else {
      DataBlock_ptr block = scratchTable[tag];
      //scratchSize -= block->getSize();

      //delete block;
      scratchTable.erase(tag);
    }
  }
}

void BlockManager::evict() {

  if (cacheTable.size()==0) {
    throw std::runtime_error("no block left");
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
  std::string msg = LOG_HEADER + 
                    std::string("evicted block ") +
                    std::to_string((long long int)tag);
  logger->logInfo(msg);
}

// update access count for a block, 
// and then update cacheTable for new indexes
void BlockManager::update(int64_t tag) {

  // log info
  std::string msg = LOG_HEADER + 
                    std::string("updating block ") +
                    std::to_string((long long int)tag);
  logger->logInfo(msg);

  cacheTable[tag].first += 1;
}

}
