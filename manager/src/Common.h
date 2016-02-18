#ifndef COMMON_H
#define COMMON_H

#include <boost/smart_ptr.hpp>
#include <cstdint>
#include <string>

namespace blaze {

// constant parameter definition
#define BLAZE_INPUT_BLOCK  0
#define BLAZE_SHARED_BLOCK 1
#define BLAZE_OUTPUT_BLOCK 2

// forward declaration of all classes
class DataBlock;
class Task;
class TaskEnv;
class Platform;
class BlockManager;
class CommManager;
class PlatformManager;
class TaskManager;
class QueueManager;

typedef boost::shared_ptr<DataBlock> DataBlock_ptr;
typedef boost::shared_ptr<Task> Task_ptr;
typedef boost::shared_ptr<TaskEnv> TaskEnv_ptr;
typedef boost::shared_ptr<Platform> Platform_ptr;
typedef boost::shared_ptr<BlockManager> BlockManager_ptr;
typedef boost::shared_ptr<TaskManager> TaskManager_ptr;
typedef boost::shared_ptr<QueueManager> QueueManager_ptr;

// common functions
uint64_t getUs();
uint64_t getMs();
uint32_t getTid();
std::string getTS();

// common file/directory operators
std::string saveFile(std::string path, const std::string &contents);
bool deleteFile(std::string path);

// parameters

} // namespace blaze

#endif
