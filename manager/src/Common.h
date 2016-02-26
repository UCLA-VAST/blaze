#ifndef COMMON_H
#define COMMON_H

#include <boost/smart_ptr.hpp>
#include <cstdint>
#include <iostream>
#include <stdexcept>
#include <string>

namespace blaze {

// constant parameter definition
#define BLAZE_INPUT_BLOCK  0
#define BLAZE_SHARED_BLOCK 1
#define BLAZE_OUTPUT_BLOCK 2

// forward declaration of all classes
class BlockManager;
class CommManager;
class DataBlock;
class Task;
class TaskEnv;
class TaskManager;
class Platform;
class PlatformManager;
class QueueManager;

typedef boost::shared_ptr<BlockManager> BlockManager_ptr;
typedef boost::shared_ptr<DataBlock>    DataBlock_ptr;
typedef boost::weak_ptr<DataBlock>      DataBlock_ref;
typedef boost::shared_ptr<Task>         Task_ptr;
typedef boost::shared_ptr<TaskEnv>      TaskEnv_ptr;
typedef boost::shared_ptr<TaskManager>  TaskManager_ptr;
typedef boost::weak_ptr<TaskManager>    TaskManager_ref;
typedef boost::shared_ptr<Platform>     Platform_ptr;
typedef boost::weak_ptr<Platform>       Platform_ref;
typedef boost::shared_ptr<QueueManager> QueueManager_ptr;

// common functions
uint64_t getUs();
uint64_t getMs();
uint32_t getTid();
std::string getTS();
std::string getUid();

// common file/directory operators
std::string saveFile(std::string path, const std::string &contents);
bool deleteFile(std::string path);

// parameters

// custom exceptions
class invalidParam : public std::runtime_error {
public:
  explicit invalidParam(const std::string& what_arg):
    std::runtime_error(what_arg) {;}
};

class commError : public std::runtime_error {
public:
  explicit commError(const std::string& what_arg):
    std::runtime_error(what_arg) {;}
};

} // namespace blaze

#endif
