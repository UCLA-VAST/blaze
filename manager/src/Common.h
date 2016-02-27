#ifndef COMMON_H
#define COMMON_H

#include <boost/asio.hpp>
#include <boost/filesystem.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/smart_ptr.hpp>
#include <boost/thread/lockable_adapter.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/thread.hpp>
#include <cstdint>
#include <google/protobuf/message.h>
#include <iostream>
#include <map>
#include <stdexcept>
#include <string>
#include <vector>

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

// typedef of boost smart pointer object
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

typedef boost::shared_ptr<boost::asio::io_service> ios_ptr;
typedef boost::shared_ptr<boost::asio::ip::tcp::endpoint> endpoint_ptr;
typedef boost::shared_ptr<boost::asio::ip::tcp::socket> socket_ptr;

// helper functions to get system info
uint64_t getUs();
uint64_t getMs();
uint32_t getTid();
std::string getTS();
std::string getUid();

// helper functions for socket transfer
void recv(::google::protobuf::Message&, socket_ptr);
void send(::google::protobuf::Message&, socket_ptr);

// helper functions for file/directory operations
std::string saveFile(std::string path, const std::string &contents);
std::string readFile(std::string path);
bool deleteFile(std::string path);

// parameters
static std::string nam_root_dir("/tmp/nam");

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

class fileError : public std::runtime_error {
public:
  explicit fileError(const std::string& what_arg):
    std::runtime_error(what_arg) {;}
};

class internalError : public std::runtime_error {
public:
  explicit internalError(const std::string& what_arg):
    std::runtime_error(what_arg) {;}
};
} // namespace blaze

#endif
