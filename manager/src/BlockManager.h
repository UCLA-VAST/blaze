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

#include "Block.h"
#include "OpenCLBlock.h"
#include "Logger.h"
#include "TaskEnv.h"
#include "OpenCLEnv.h"

/* TODO list:
 * - guarantee an unique partition id shared by possibly multiple
 *   spark context/applications/tasks
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
      TaskEnv* _env,
      Logger* _logger,
      size_t _maxCacheSize = (1L<<30), 
      size_t _maxScratchSize = (1L<<28)
      ):
    cacheSize(0), scratchSize(0),
    maxCacheSize(_maxCacheSize), 
    maxScratchSize(_maxScratchSize),
    env(_env),
    logger(_logger)
  {
    //cacheTable    = new std::map<int, int>;
    //cacheQueue    = new std::vector<std::pair<int, DataBlock*>>;
    //scratchTable  = new std::map<int, DataBlock*>;
  }

  /* all reference in BlockManager will be automatically removed */
  //~BlockManager();

  // check scratch and cache table to see if a certain 
  // block exists
  bool contains(int64_t tag) {
    if (tag < 0) {
      // check scratch table
      return (scratchTable.find(tag) != scratchTable.end());
    }
    else {
      // check cache table
      return (cacheTable.find(tag) != cacheTable.end());
    }
  }

  // create an empty block
  DataBlock_ptr create();

  // create a block and add it to cache/scratch
  // return true if a new block is created
  bool create(
      //int length, 
      //int size,
      int64_t tag,
      DataBlock_ptr &block);

  // get a block from cache table or scratch table
  DataBlock_ptr get(int64_t tag);

  // add a block to cache table or scratch table
  void add(int64_t tag, DataBlock_ptr block);

  // remove a block from scratch table
  void remove(int64_t tag);

  // used for tests and debugging
  void printTable() {

    int i = 0;
    printf("id,\ttag,\trefcnt\n");
    std::map<int64_t, std::pair<int, DataBlock_ptr> >::iterator iter; 
    for (iter = cacheTable.begin(); 
        iter != cacheTable.end(); 
        iter ++)
    {
      int64_t tag = iter->first;
      std::pair<int, DataBlock_ptr> v = iter->second;
      printf("%d,\t%ld,\t%d\n", i, tag, v.first);
      i++;
    }
  }

private:
  // internal cache operations
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

  TaskEnv* env;
  Logger* logger;
};

typedef boost::shared_ptr<BlockManager> BlockManager_ptr;
}

#endif
