#ifndef AAL_PLATFORM_H
#define AAL_PLATFORM_H

#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <stdexcept>

#include <aalsdk/AAL.h>
#include <aalsdk/xlRuntime.h>
#include <aalsdk/service/ICCIAFU.h>
#include <aalsdk/service/ICCIClient.h>

#include "proto/acc_conf.pb.h"
#include "Platform.h"
#include "RuntimeClient.h"
#include "AALEnv.h"
#include "AALBlock.h"

#define DSM_SIZE  MB(4)

namespace blaze {

class AALPlatform : public blaze::Platform,
                    public CAASBase,
                    public IServiceClient,
                    public ICCIClient
{
public:

  AALPlatform();

  ~AALPlatform() {
    (dynamic_ptr<IAALService>(iidService, m_pAALService))->Release(TransactionID());
    m_Sem.Destroy();
    delete env;  
  }

  virtual void setupAcc(AccWorker &conf);

  virtual DataBlock_ptr createBlock() {
    DataBlock_ptr block(
        new AALBlock(static_cast<AALEnv*>(env)));  
    return block;
  }

  virtual DataBlock_ptr createBlock(size_t length, size_t size) {
    DataBlock_ptr block(
        new AALBlock(static_cast<AALEnv*>(env), length, size));  
    return block;
  }

  // <begin IServiceClient interface>
  void serviceAllocated(IBase *pServiceBase,
      TransactionID const &rTranID);

  void serviceAllocateFailed(const IEvent &rEvent);

  void serviceFreed(TransactionID const &rTranID);

  void serviceEvent(const IEvent &rEvent);
  // <end IServiceClient interface>
  
  // <ICCIClient>
  virtual void OnWorkspaceAllocated(TransactionID const &TranID,
                                    btVirtAddr           WkspcVirt,
                                    btPhysAddr           WkspcPhys,
                                    btWSSize             WkspcSize) {
    
    printf("I was called\n");
  }

  virtual void OnWorkspaceAllocateFailed(const IEvent &Event) {;}

  virtual void OnWorkspaceFreed(TransactionID const &TranID) {;}

  virtual void OnWorkspaceFreeFailed(const IEvent &Event) {;}
  // </ICCIClient>

private:

  RuntimeClient   *m_runtimeClient;
  IBase      *m_pAALService;    // The generic AAL Service interface for the AFU.
  ICCIAFU    *m_APPService;

  CSemaphore m_Sem;            // For synchronizing with the AAL runtime.
  btBool     m_Status;

  AALBlock_ptr    dsm_block;
};

extern "C" Platform* create() {
  return new AALPlatform();
}

extern "C" void destroy(Platform* p) {
  delete p;
}
} // namespace blaze

#endif
