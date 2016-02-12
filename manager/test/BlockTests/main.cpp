#include <cstdint>
#include <string>
#include <stdexcept>
#include <unistd.h>
#include <dlfcn.h>

#include <boost/thread/thread.hpp>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "Common.h"
#include "Block.h"
#include "Platform.h"
#include "BlockManager.h"
#include "PlatformManager.h"

namespace blaze {

class BlockTests : public ::testing::Test {
  protected:

    BlockTests() {

      char path[100] = "platform.so";

      if (access(path, F_OK) == -1) { 
        std::map<std::string, std::string> table;
        Platform_ptr new_platform(new Platform(table));
        platform = new_platform;
      }
      else {
        LOG(INFO) << "Opening platform implementation: "
                  << path;

        void* handle = dlopen(path, RTLD_LAZY|RTLD_GLOBAL);

        if (handle == NULL) {
          LOG(ERROR) << dlerror();
          throw std::runtime_error(dlerror());
        }

        // reset errors
        dlerror();

        // load the symbols
        Platform* (*create_func)();
        void (*destroy_func)(Platform*);

        // read the custom constructor and destructor  
        create_func = (Platform* (*)())dlsym(handle, "create");
        destroy_func = (void (*)(Platform*))dlsym(handle, "destroy");

        const char* error = dlerror();
        if (error) {
          LOG(ERROR) << error;
          throw std::runtime_error(error);
        }

        Platform_ptr new_platform(create_func(), destroy_func);
        platform = new_platform;
      }
    }

    virtual void SetUp() {
      // allocate 4MB of cache and scratch
      const int maxCacheSize = 4*1024*1024;
      const int maxScratchSize = 4*1024*1024;

      // create a new manager for each test to ensure clean start
      bman = new BlockManager(
          platform.get(),
          maxCacheSize, maxScratchSize);
      FLAGS_v = 0;
    }

    virtual void TearDown() {
      delete bman;
    }

    Platform_ptr  platform;
    BlockManager* bman;
};

TEST_F(BlockTests, CheckBasicBlock) {

  int num_items   = 4;
  int item_length = 32;
  int item_size   = item_length*sizeof(int);

  // create a new block
  DataBlock_ptr block = platform->createBlock(num_items, item_length, item_size);

  // check status right after construction
  ASSERT_EQ(num_items, block->getNumItems());
  ASSERT_EQ(item_length, block->getItemLength());
  ASSERT_EQ(item_size, block->getItemSize());
  ASSERT_EQ(num_items*item_length, block->getLength());
  ASSERT_EQ(num_items*item_size, block->getSize());
  ASSERT_EQ(false, block->isAllocated());
  ASSERT_EQ(false, block->isReady());

  // check Block::writeData()
  int* data_in = new int[num_items*item_length];
  for (int i=0; i<num_items*item_length; i++) {
    data_in[i] = i;
  }
  block->writeData((void*)data_in, item_size*num_items);

  ASSERT_EQ(true, block->isAllocated());
  ASSERT_EQ(true, block->isReady());

  // check Block::getData()
  ASSERT_NE((char*)NULL, block->getData());

  // check Block::readData()
  int* data_out = new int[num_items*item_length];
  block->readData((void*)data_out, item_size*num_items);
  for (int i=0; i<num_items*item_length; i++) {
    EXPECT_EQ(i, data_out[i]);
  }

  delete [] data_in;
  delete [] data_out;
}

TEST_F(BlockTests, CheckAlignedAlloc) {

  int num_items   = 16;
  int item_length = 123;
  int item_size   = item_length*sizeof(int);
  int align_width = 64;  // align to 512bit
  
  int align_item_size = (item_size+align_width-1)/align_width*align_width;

  DataBlock_ptr block = platform->createBlock(
      num_items, item_length, item_size, align_width);
  
  ASSERT_NE(NULL_DATA_BLOCK, block);
  ASSERT_EQ(align_item_size, block->getItemSize());
  
  int* data_in = new int[num_items*item_length];
  int* data_out = new int[num_items*align_item_size/sizeof(int)];

  for (int i=0; i<num_items*item_length; i++) {
    data_in[i] = i;
  }
  block->writeData((void*)data_in, item_size*num_items);

  ASSERT_EQ(true, block->isReady());

  block->readData((void*)data_out, align_item_size*num_items);

  // check if data is aligned 
  for (int i=0; i<num_items; i++) {
    int offset = i*align_item_size/sizeof(int);
    for (int j=0; j<item_length; j++) {
      EXPECT_EQ(i*item_length+j, data_out[offset+j]);
    }
  }

  delete [] data_in;
  delete [] data_out;
}

// TODO
TEST_F(BlockTests, CheckSampling) {
  
}

TEST_F(BlockTests, CheckBasicCache) {

  // create a new block
  DataBlock_ptr block;

  // add this block to cache
  int64_t tag = 1;
  bman->getAlloc(tag, block, 42, 1000, 1000);
  
  // check BlockManager::add(), contains()
  ASSERT_EQ(true, bman->contains(tag));
  ASSERT_EQ(false, bman->contains(tag+1));
  ASSERT_NE(NULL_DATA_BLOCK, bman->get(tag));
  ASSERT_EQ(false, bman->getAlloc(tag, block, 42, 1024, 1024));
  ASSERT_NE(NULL_DATA_BLOCK, block);
  ASSERT_EQ(42, block->getNumItems());

  // check BlockManager::getAlloc()
  tag = 2;
  ASSERT_EQ(true, bman->getAlloc(tag, block, 27, 1024, 1024));
  ASSERT_EQ(true, bman->contains(tag));
  ASSERT_NE(NULL_DATA_BLOCK, bman->get(tag));
  ASSERT_EQ(27, block->getNumItems());
}

TEST_F(BlockTests, CheckEviction) {

  DataBlock_ptr block;
  ASSERT_EQ(false, bman->getAlloc(1, block, 16, 1024*1024, 1024*1024));
  ASSERT_EQ(false, bman->contains(1));

  // add four blocks, 1MB each
  EXPECT_EQ(true, bman->getAlloc(1, block, 1024, 1024, 1024));
  boost::this_thread::sleep_for(boost::chrono::seconds(1)); 

  EXPECT_EQ(true, bman->getAlloc(2, block, 1024, 1024, 1024));
  boost::this_thread::sleep_for(boost::chrono::seconds(1)); 

  EXPECT_EQ(true, bman->getAlloc(3, block, 1024, 1024, 1024));
  boost::this_thread::sleep_for(boost::chrono::seconds(1)); 

  EXPECT_EQ(true, bman->getAlloc(4, block, 1024, 1024, 1024));
  boost::this_thread::sleep_for(boost::chrono::seconds(1)); 

  // add fifth block should evict one
  EXPECT_EQ(true, bman->getAlloc(5, block, 1024, 1024, 1024));
  boost::this_thread::sleep_for(boost::chrono::seconds(1)); 

  int count = 0;
  int64_t evicted_tag = 0;
  for (int64_t tag=1; tag<=4; tag++) {
    if (bman->get(tag) != NULL_DATA_BLOCK) {
      count ++;
    }
    else {
      evicted_tag = tag;
    }
  }
  ASSERT_EQ(3, count);
  ASSERT_EQ(false, bman->contains(evicted_tag));

  // add another one should evict the fifth one
  // since all the other three have been referenced once more
  bman->getAlloc(6, block, 42, 1000, 1000);
  for (int64_t tag=1; tag<=6; tag++) {
    if (tag == evicted_tag || tag == 5) {
      ASSERT_EQ(false, bman->contains(tag));
    }
    else {
      ASSERT_EQ(true, bman->contains(tag));
    }
  }
}

// TODO
TEST_F(BlockTests, CheckScratch) {

}


void doCreateBlocks(int tid, BlockManager* bman, int* data) {
  for (int i=0; i<8; i++) {
    DataBlock_ptr block;
    ASSERT_EQ(true, bman->getAlloc(tid*8+i, block, 1, 256, 1024));
    block->writeData((void*)data, 1024);
    ASSERT_EQ(true, block->isAllocated());
    ASSERT_EQ(true, block->isReady());
  } 
}

void doCheckBlock(int tid, BlockManager* bman, int* data) {

  DataBlock_ptr block = bman->get(tid%1024);
  ASSERT_NE(NULL_DATA_BLOCK, block);

  int data_out[256];
  block->readData(data_out, 1024);

  for (int i=0; i<256; i++) {
    EXPECT_EQ(i, data_out[i]);
  } 
}

TEST_F(BlockTests, CheckMultiThread) {

  int data_in[256];
  int data_out[256];

  for (int i=0; i<256; i++) {
    data_in[i] = i;
  }
  
  // create 128 threads, each of which creates 8 blocks
  boost::thread_group tgroup;
  for (int t=0; t<128; t++) {
    tgroup.create_thread(boost::bind(doCreateBlocks, t, bman, data_in));
  }
  tgroup.join_all();

  // check if all blocks have been added
  for (int i=0; i<1024; i++) {
    tgroup.create_thread(boost::bind(doCheckBlock, i, bman, data_in));
  }
  tgroup.join_all();
}

} // namespace blaze


int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
