#define TEST_FRIENDS_LIST \
          friend class ConfigTests; \
          friend class ConfigTests_CheckCommHandler_Test; 

#include "TestCommon.h"
#include "Client.h"
#include "CommManager.h"
#include "PlatformManager.h"
#include "proto/acc_conf.pb.h"

namespace blaze {

TEST_F(ConfigTests, CheckCommHandler) {

  // settings
  std::string acc_id      = "test";
  std::string platform_id = "cpu";

  // config manager to use default CPU platform
  ManagerConf conf;
  AccPlatform *platform = conf.add_platform();

  // start manager
  PlatformManager platform_manager(&conf);
  boost::shared_ptr<AppCommManager> comm( new AppCommManager(
        &platform_manager, "127.0.0.1", app_port)); 

  TaskMsg req_msg;

  // test acc register
  req_msg.set_type(ACCREGISTER);

  AccMsg* acc_msg = req_msg.mutable_acc();

  acc_msg->set_acc_id(acc_id);
  acc_msg->set_platform_id(platform_id);
  acc_msg->set_task_impl(pathToLoopBack);

  try {
    comm->handleAccRegister(req_msg);
  } catch (std::exception &e) {
    ASSERT_EQ(true, false) << "Caught unexpected exception: " << e.what();
  }

  // accelerator should be registered
  ASSERT_EQ(true, platform_manager.accExists(acc_id));
  try {
    ASSERT_EQ(true, runLoopBack());
  } catch (cpuCalled &e) {
    ASSERT_EQ(false, true) << "Client error";
  }

  // try register another one
  acc_msg->set_task_impl(pathToArrayTest);
  try {
    comm->handleAccRegister(req_msg);
  } catch (std::exception &e) {
    ASSERT_EQ(true, true);
  }
  // accelerator should not change
  try {
    ASSERT_EQ(true, runLoopBack());
  } catch (cpuCalled &e) {
    ASSERT_EQ(false, true) << "Client error";
  }

  // test acc delete
  req_msg.set_type(ACCDELETE);

  try {
    comm->handleAccDelete(req_msg);
  } catch (std::exception &e) {
    ASSERT_EQ(true, false) << "Caught unexpected exception: " << e.what();
  }
  ASSERT_EQ(false, platform_manager.accExists(acc_id));
  try {
    runLoopBack();
  } catch (cpuCalled &e) {
    // should catch exception here
    ASSERT_EQ(true, true);
  }

  // test acc register again for ArrayTest
  req_msg.set_type(ACCREGISTER);

  try {
    comm->handleAccRegister(req_msg);
  } catch (std::exception &e) {
    ASSERT_EQ(true, false) << "Caught unexpected exception: " << e.what();
  }
  // accelerator should be registered as ArrayTest
  ASSERT_EQ(true, platform_manager.accExists(acc_id));
  try {
    ASSERT_EQ(true, runArrayTest());
  } catch (cpuCalled &e) {
    ASSERT_EQ(false, true) << "Client error";
  }
}

} // namespace blaze
