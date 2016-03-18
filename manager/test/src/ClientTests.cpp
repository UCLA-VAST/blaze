#define TEST_FRIENDS_LIST \
          friend class ClientTests; \
          friend class ClientTests_CheckBlockAllocation_Test; \
          friend class ClientTests_CheckPrepareRequest_Test; 

#include "TestCommon.h"
#include "Client.h"
#include "CommManager.h"
#include "PlatformManager.h"

namespace blaze {

class TestClient : public Client {
  TEST_FRIENDS_LIST
public:
  TestClient(int ni, int no): Client("test", ni, no, app_port) {;}
  void compute() {
    throw cpuCalled("");
  }
};

TEST_F(ClientTests, CheckBlockAllocation) {
  TestClient client(1, 1); 

  // valid input
  try {
    void* ptr = client.createInput(0, 4, 4, sizeof(int), BLAZE_INPUT);
    ASSERT_NE(ptr, (void*)NULL);
    ASSERT_EQ(1,   client.input_blocks.size());
    ASSERT_EQ(ptr, client.getInputPtr(0));
    ASSERT_EQ(4,   client.getInputNumItems(0));
    ASSERT_EQ(16,  client.getInputLength(0));

    // new allocation should not overwrite old one
    void* new_ptr = client.createInput(0, 16, 4, sizeof(int), BLAZE_INPUT);
    ASSERT_EQ(ptr, new_ptr);
    ASSERT_EQ(ptr, client.getInputPtr(0));
    ASSERT_EQ(4,   client.getInputNumItems(0));
    ASSERT_EQ(16,  client.getInputLength(0));
  }
  catch (std::exception &e) {
    // should not be any exception
    ASSERT_EQ(0, 1);
    LOG(ERROR) << "Valid input should not throw exceptions";
  }

  // invalid input
  try {
    // idx should not be larger than num_inputs
    void* ptr = client.createInput(1, 1, 1, sizeof(int), BLAZE_INPUT);
    ASSERT_EQ(1, 0);
  }
  catch (std::exception &e) {
    // should not be any exception
    ASSERT_EQ(0, 0);
    LOG(INFO) << "Exception caught: " << e.what();
  }
}

TEST_F(ClientTests, CheckPrepareRequest) {
  TestClient client(5, 1); 

  client.createInput(2, 6, 8, sizeof(int), BLAZE_INPUT_CACHED);
  client.createInput(1, 1, 1, sizeof(double), BLAZE_INPUT);
  client.createInput(3, 2, 4, sizeof(int), BLAZE_SHARED);
  client.createInput(0, 4, 4, sizeof(int), BLAZE_INPUT);
  client.createInput(4, 1, 1, sizeof(float), BLAZE_SHARED);

  double val1 = 0.1027;
  float  val2 = 0.528;

  double* block1 = (double*)client.getInputPtr(1);
  float* block2  = (float*) client.getInputPtr(4);

  block1[0] = val1;
  block2[0] = val2;

  // build message
  TaskMsg msg;
  client.prepareRequest(msg);

  // checkout message field
  ASSERT_EQ(msg.type(), ACCREQUEST);
  ASSERT_EQ(msg.has_app_id(), true);
  ASSERT_EQ(msg.has_acc_id(), true);
  ASSERT_EQ(msg.app_id(), client.app_id);
  ASSERT_EQ(msg.acc_id(), client.acc_id);
  ASSERT_EQ(msg.data_size(), 5); 

  // check input #1: normal block
  DataMsg dmsg = msg.data(0);
  ASSERT_GE(dmsg.partition_id(), 0);
  ASSERT_EQ(dmsg.has_cached(), true);
  ASSERT_EQ(dmsg.cached(), false);

  // check input #2: scalar block
  dmsg = msg.data(1);
  ASSERT_EQ(dmsg.has_partition_id(), false);
  ASSERT_EQ(dmsg.has_scalar_value(), true);
  uint64_t val = dmsg.scalar_value();
  ASSERT_EQ(val1, *(double*)&val);

  // check input #3: cached block
  dmsg = msg.data(2);
  ASSERT_GE(dmsg.partition_id(), 0);
  ASSERT_EQ(dmsg.has_cached(), false);

  // check input #4: broadcast block
  dmsg = msg.data(3);
  ASSERT_LT(dmsg.partition_id(), 0);

  // check input #5: scalar block
  dmsg = msg.data(4);
  ASSERT_EQ(dmsg.has_partition_id(), false);
  ASSERT_EQ(dmsg.has_scalar_value(), true);
  val = dmsg.scalar_value();
  ASSERT_EQ(val2, *(float*)&val);
}

TEST_F(ClientTests, AppTest_arrayTest) {

  std::string path("./tasks/cpu/arrayTest/arrayTest.so");
  boost::filesystem::wpath file(path);
  ASSERT_EQ(boost::filesystem::exists(file), true)
    << "Required acc task file does not exist, skipping test";

  // config manager
  ManagerConf conf;
  AccPlatform *platform = conf.add_platform();
  AccWorker *acc_worker = platform->add_acc();
  acc_worker->set_id("test");
  acc_worker->set_path(path);

  // start manager
  PlatformManager platform_manager(&conf);
  boost::shared_ptr<CommManager> comm( new AppCommManager(
        &platform_manager, "127.0.0.1", app_port)); 

  ASSERT_EQ(true, runArrayTest());  
}

TEST_F(ClientTests, AppTest_loopBack) {

  std::string path("./tasks/cpu/loopBack/loopBack.so");
  boost::filesystem::wpath file(path);
  ASSERT_EQ(boost::filesystem::exists(file), true)
    << "Required acc task file does not exist, skipping test";

  // config manager
  ManagerConf conf;
  AccPlatform *platform = conf.add_platform();
  AccWorker *acc_worker = platform->add_acc();
  acc_worker->set_id("test");
  acc_worker->set_path(path);

  // start manager
  PlatformManager platform_manager(&conf);
  boost::shared_ptr<CommManager> comm( new AppCommManager(
        &platform_manager, "127.0.0.1", app_port)); 

  ASSERT_EQ(true, runLoopBack());
}

TEST_F(ClientTests, AppTest_delay) {

  std::string path("./tasks/cpu/delay/delay.so");
  boost::filesystem::wpath file(path);
  ASSERT_EQ(boost::filesystem::exists(file), true)
    << "Required acc task file does not exist, skipping test";

  // config manager
  ManagerConf conf;
  AccPlatform *platform = conf.add_platform();
  AccWorker *acc_worker = platform->add_acc();
  acc_worker->set_id("test");
  acc_worker->set_path(path);

  // start manager
  PlatformManager platform_manager(&conf);
  boost::shared_ptr<CommManager> comm( new AppCommManager(
        &platform_manager, "127.0.0.1", app_port)); 

  ASSERT_EQ(true, runDelay());  
}
} // namespace blaze
