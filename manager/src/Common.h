#ifndef COMMON_H
#define COMMON_H

#include <cstdint>
#include <string>

#include <boost/smart_ptr.hpp>

namespace blaze {

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
typedef boost::shared_ptr<Platform> Platform_ptr;
typedef boost::shared_ptr<BlockManager> BlockManager_ptr;
typedef boost::shared_ptr<TaskManager> TaskManager_ptr;
typedef boost::shared_ptr<QueueManager> QueueManager_ptr;

// common functions
uint64_t getUs();
uint64_t getMs();
uint32_t getTid();
std::string getTS();

} // namespace blaze

#endif
