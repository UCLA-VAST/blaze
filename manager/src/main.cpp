#include <stdio.h>
#include <stdlib.h>
#include <string>

#include <boost/filesystem.hpp>

#include "Comm.h"
#include "QueueManager.h"
#include "BlockManager.h"
#include "TaskManager.h"

using namespace acc_runtime;

int main(int argc, char** argv) {
  
  int port = 1027;
  std::string ip_address = "127.0.0.1";

  if (argc > 1) {
    ip_address.assign(argv[1]);
  }
  if (argc > 2) {
    port = atoi(argv[2]);   
  }

  Logger logger(3);
  BlockManager block_manager(&logger);
  QueueManager queue_manager(&logger);

  boost::filesystem::path acc_dir("../../task/lib/");

  if ( !boost::filesystem::exists( acc_dir ) ) {
    printf("Cannot find any accelerators.\n");
    return -1;
  }

  boost::filesystem::directory_iterator end_iter;
  // construct a task for every accelerator in the directory
  for (boost::filesystem::directory_iterator iter(acc_dir);
       iter != end_iter; 
       ++iter)
  {
    if (iter->path().extension().compare(".so")==0) {
      // add task queue to queue manager
      try {
        queue_manager.add(
            iter->path().stem().string(), 
            acc_dir.string() + iter->path().string());
      }
      catch (std::runtime_error &e) {
        printf("%s\n", e.what());
        continue;
      }

      // get the reference to the task queue 
      TaskManager_ptr task_manager = 
        queue_manager.get(iter->path().stem().string());

      // start executor and commitor
      boost::thread executor(
          boost::bind(&TaskManager::execute, task_manager));

      boost::thread committer(
          boost::bind(&TaskManager::commit, task_manager));

    }
  }

  Comm comm(
          &block_manager, 
          &queue_manager, 
          &logger, ip_address, port);

  comm.listen();

  return 0;
}
