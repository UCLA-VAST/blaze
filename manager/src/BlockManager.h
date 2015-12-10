#ifndef BLOCK_MANAGER_H
#define BLOCK_MANAGER_H

#include <map>
#include <vector>
#include <iostream>
#include <cstdint>

#include <boost/smart_ptr.hpp>
#include <boost/thread/thread.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/lockable_adapter.hpp>

#include "Common.h"

/* TODO list:
 * - guarantee an unique partition id shared by possibly multiple
 *   spark context/applications/tasks
 * - allow update the scratch size after add an empty block, if the space is run out
 *   throw an exception and let the Comm to send ACCFailure
 */

namespace blaze {

/**
 * BlockManager holds two spaces of memory
 * - scratch: for shared data across many tasks of the same stage,
 *   will be explicitly deleted after the stage finishes.
 *   Aligned with Spark broadcast
 * - cache: hold all input blocks, and is managed on a LRU basis
 */

class BlockManager 
: public boost::basic_lockable_adapter<boost::mutex>
{
public:

  BlockManager(
      Platform* _platform,
      size_t _maxCacheSize = (1L<<30), 
      size_t _maxScratchSize = (1L<<28)
      ):
    cacheSize(0), scratchSize(0),
    maxCacheSize(_maxCacheSize), 
    maxScratchSize(_maxScratchSize),
    platform(_platform)
  {;}

  // check scratch and cache table to see if a certain block exists
  virtual bool contains(int64_t tag) {
    if (tag < 0) {
      // check scratch table
      return (scratchTable.find(tag) != scratchTable.end());
    }
    else {
      // check cache table
      return (cacheTable.find(tag) != cacheTable.end());
    }
  }

  // create a block and add it to cache/scratch
  // return true if a new block is created
  virtual bool getAlloc(int64_t tag, DataBlock_ptr &block,
      int num_items, int item_length, int item_size, int align_width=0);

  // get a block from cache table or scratch table
  virtual DataBlock_ptr get(int64_t tag);

  // remove a block from scratch table
  virtual void remove(int64_t tag);

private:
  // internal cache operations
  void do_add(int64_t tag, DataBlock_ptr block);
  void evict();
  void update(int64_t tag);

  // index (tag) to scratch table 
  std::map<int64_t, DataBlock_ptr> scratchTable;

  // index (tag) to cached block and its access count
  std::map<int64_t, std::pair<int, DataBlock_ptr> > cacheTable;

  size_t maxCacheSize;
  size_t maxScratchSize;
  size_t cacheSize;
  size_t scratchSize;

  Platform* platform;
};

}

#endif
