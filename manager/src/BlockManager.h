#ifndef BLOCK_MANAGER_H
#define BLOCK_MANAGER_H

#include <map>
#include <vector>
#include <iostream>

#include <boost/smart_ptr.hpp>
#include <boost/thread/thread.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/lockable_adapter.hpp>

#include "Block.h"
#include "Logger.h"

/* TODO list:
 * - guarantee an unique partition id shared by possibly multiple
 *   spark context/applications/tasks
 */

namespace acc_runtime {

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
      Logger* _logger,
      size_t _maxCacheSize = (1L<<30), 
      size_t _maxScratchSize = (1L<<28)
      ):
    cacheSize(0), scratchSize(0),
    maxCacheSize(_maxCacheSize), 
    maxScratchSize(_maxScratchSize),
    logger(_logger)
  {
    //cacheTable    = new std::map<int, int>;
    //cacheQueue    = new std::vector<std::pair<int, DataBlock*>>;
    //scratchTable  = new std::map<int, DataBlock*>;
  }

  /* all reference in BlockManager will be automatically removed */
  //~BlockManager();

  // cache access
  bool isCached(int tag) {
    if (cacheTable.find(tag) == cacheTable.end()) {
      return false;
    }
    else {
      return true;
    }
  }
  void add(int tag, DataBlock_ptr block);
  DataBlock_ptr get(int tag);
  //DataBlock_ptr alloc(int tag, int length, int width);
  //DataBlock_ptr getOrAlloc(int tag, int size);

  // scratch access
  DataBlock_ptr getShared(int tag);
  int addShared(int tag, DataBlock_ptr block);
  int removeShared(int tag);

  void printTable() {

    int i = 0;
    printf("id,\ttag,\trefcnt\n");
    std::map<int, std::pair<int, DataBlock_ptr> >::iterator iter; 
    for (iter = cacheTable.begin(); 
        iter != cacheTable.end(); 
        iter ++)
    {
      int tag = iter->first;
      std::pair<int, DataBlock_ptr> v = iter->second;
      printf("%d,\t%d,\t%d\n", i, tag, v.first);
      i++;
    }
  }

private:
  bool cmpLRU(
      const std::pair<int, DataBlock_ptr> &v1,
      const std::pair<int, DataBlock_ptr> &v2) 
  {
    return (v1.first > v2.first);
  }

  // cache operation
  void evict();
  void update(int idx);

  std::map<int, DataBlock_ptr> scratchTable;

  // index to blocks and its access time
  std::map<int, std::pair<int, DataBlock_ptr> > cacheTable;

  // maintaining blocks sorted by access count 
  //std::vector<std::pair<int, DataBlock_ptr>> cacheQueue;

  size_t maxCacheSize;
  size_t maxScratchSize;
  size_t cacheSize;
  size_t scratchSize;

  Logger* logger;
};
}

#endif
