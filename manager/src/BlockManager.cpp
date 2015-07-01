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
  
  /* ISSUE: it is possible that between the block is removed 
   * while it is being used
   */

  if (cacheTable.find(tag) == cacheTable.end()) {
    return NULL_DATA_BLOCK;
  }
  else {
    // accumulate the access count for the block
    // TODO: should do it async
    update(tag);

    return cacheQueue[cacheTable[tag]].second;
  }
}

DataBlock_ptr BlockManager::getOrAlloc(int tag, int size) {

  // guarantee exclusive access
  boost::lock_guard<BlockManager> guard(*this);

  /* ISSUE: it is possible that between the block is removed 
   * while it is being used
   */

  if (cacheTable.find(tag) == cacheTable.end()) {
    DataBlock_ptr new_block(new DataBlock(tag, size));

    add(tag, new_block);
    return new_block;
  }
  else {
    DataBlock_ptr block = cacheQueue[cacheTable[tag]].second;

    update(tag);

    return block;
  }
}

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
  
  if (scratchSize + block->getSize() >= scratchSize) {
    // cannot add because running out of space
    return -1;
  }
  else if (scratchTable.find(tag) == scratchTable.end()){
    // add new block
    scratchTable.insert(std::make_pair(tag, block));
    scratchSize += block->getSize();
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

void BlockManager::add(
    int tag, 
    DataBlock_ptr block)
{

  // private method, exclusion guaranteed outside

  // log info
  std::string msg = LOG_HEADER + 
                    std::string("adding block ") +
                    std::to_string((long long int)tag);
  logger->logInfo(msg);

  while (cacheSize + block->getSize() > maxCacheSize) {

    // remove block from cache
    evict();
  }

  // add the new block to cacheQueue
  cacheQueue.push_back(std::make_pair(0, block));
  int idx = cacheQueue.size() - 1;

  // add the index to cacheTable
  cacheTable.insert(std::make_pair(tag, idx));

  // increase the current cacheSize
  cacheSize += block->getSize();
}

void BlockManager::evict() {

  // element in the back with highest priority
  std::pair<int, DataBlock_ptr> val = cacheQueue.back();

  DataBlock_ptr block = val.second;
  int tag = block->getTag();
  int size = block->getSize();

  // free block 
  //delete block;

  cacheQueue.pop_back();  

  // remove index from cacheTag
  cacheTable.erase(tag);

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

  int idx = cacheTable[tag];

  std::pair<int, DataBlock_ptr> val = cacheQueue[idx];
  int key = val.first;
  DataBlock_ptr block = val.second;

  cacheQueue.erase(cacheQueue.begin()+idx);
  
  /* find the position of the updated block, 
   * and upate all blocks' indexes after it */

  int idx_res = 0;
  int idx_lo = 0;
  int idx_hi = cacheQueue.size();

  while (idx_lo < idx_hi) {
    int idx_mid = (idx_lo + idx_hi)/2;    
    if (cacheQueue[idx_mid].first < key+1) {
      idx_hi = idx_mid;
      idx_res = idx_hi;
    }
    else {
      idx_lo = idx_mid + 1;
      idx_res = idx_lo;
    }
  }
  // insert the block in idx_mid
  cacheQueue.insert(cacheQueue.begin()+idx_res, 
                    std::make_pair(key+1, block));
  // update its idx in cacheTable
  cacheTable[tag] = idx_res;

  /* all blocks originally after the updated block will have 
   * changed index */
  for (int i=idx_res+1; i<=idx; i++) {
    DataBlock_ptr block = cacheQueue[i].second;
    int tag = block->getTag();
    cacheTable[tag] += 1;
  }
}

}
