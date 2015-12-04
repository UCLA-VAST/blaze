#include "Task.h"

namespace blaze {

std::string Task::getConfig(int idx, std::string key) 
{
  if (config_table.find(idx) != config_table.end() &&
      config_table[idx].find(key) != config_table[idx].end()) 
  {
    return config_table[idx][key];
  } else {
    return std::string();
  }
}

char* Task::getOutput(
    int idx, 
    int item_length, 
    int num_items,
    int data_width) 
{
  if (idx < output_blocks.size()) {
    // if output already exists, return the pointer 
    // to the existing block
    return output_blocks[idx]->getData();
  }
  else {
    // if output does not exist, create one
    DataBlock_ptr block = platform->createBlock(num_items, 
        item_length, item_length*data_width);

    output_blocks.push_back(block);

    return block->getData();
  }
}

int Task::getInputLength(int idx) { 
  if (idx < input_blocks.size() && 
      input_table.find(input_blocks[idx]) != input_table.end())
  {
    return input_table[input_blocks[idx]]->getLength(); 
  }
  else {
    throw std::runtime_error("getInputLength out of bound idx");
  }
}


int Task::getInputNumItems(int idx) { 
  if (idx < input_blocks.size() &&
      input_table.find(input_blocks[idx]) != input_table.end())
  {
    return input_table[input_blocks[idx]]->getNumItems() ; 
  }
  else {
    throw std::runtime_error("getInputNumItems out of bound idx");
  }
}

char* Task::getInput(int idx) {

  if (idx < input_blocks.size() &&
      input_table.find(input_blocks[idx]) != input_table.end())
  {
    return input_table[input_blocks[idx]]->getData();      
  }
  else {
    throw std::runtime_error("getInput out of bound idx");
  }
}

void Task::addConfig(int idx, std::string key, std::string val) {

  config_table[idx][key] = val;
}

void Task::addInputBlock(
    int64_t partition_id, 
    DataBlock_ptr block = NULL_DATA_BLOCK) 
{
  if (input_blocks.size() >= num_input) {
    throw std::runtime_error(
        "Inconsistancy between num_args in ACC Task"
        " with the number of blocks in ACCREQUEST");
  }
  // add the block to the input list
  input_blocks.push_back(partition_id);

  if (block != NULL_DATA_BLOCK) {
    // add the same block to a map table to provide fast access
    input_table.insert(std::make_pair(partition_id, block));

    // automatically trace all the blocks,
    // if all blocks are initialized with data, 
    // set the task status to READY
    if (block->isReady()) {
      num_ready ++;
      if (num_ready == num_input) {
        status = READY;
      }
    }
  }
}

void Task::inputBlockReady(int64_t partition_id, DataBlock_ptr block) {

  if (input_table.find(partition_id) == input_table.end()) {

    // add the same block to a map table to provide fast access
    input_table.insert(std::make_pair(partition_id, block));

    // assuming the block is already ready
    if (!block || !block->isReady()) {
      throw std::runtime_error("Task::inputBlockReady(): block not ready");
    }
    num_ready ++;
    if (num_ready == num_input) {
      status = READY;
    }
  }
}

DataBlock_ptr Task::getInputBlock(int64_t block_id) {
  if (input_table.find(block_id) != input_table.end()) {
    return input_table[block_id];
  }
  else {
    return NULL_DATA_BLOCK;
  }
}

// push one output block to consumer
// return true if there are more blocks to output
bool Task::getOutputBlock(DataBlock_ptr &block) {

  if (!output_blocks.empty()) {

    block = output_blocks.back();

    // assuming the blocks are controlled by consumer afterwards
    output_blocks.pop_back();

    // no more output blocks means all data are consumed
    if (output_blocks.empty()) {
      status = COMMITTED;
      return false;
    }
    return true;
  }
  else {
    return false;
  }
}

// check if all the blocks in task's input list is ready
bool Task::isReady() {

  if (status == READY) {
    return true; 
  }
  else {
    bool ready = true;
    int num_ready_curr = 0;
    for (std::map<int64_t, DataBlock_ptr>::iterator iter = input_table.begin();
        iter != input_table.end();
        iter ++)
    {
      // a block may be added but not initialized
      if (iter->second == NULL_DATA_BLOCK || !iter->second->isReady()) {
        ready = false;
        break;
      }
      num_ready_curr++;
    }
    if (ready && num_ready_curr == num_input) {
      status = READY;
      return true;
    }
    else {
      return false;
    }
  }
}

} // namespace
