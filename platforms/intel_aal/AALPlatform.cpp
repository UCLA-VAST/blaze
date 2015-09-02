#include "AALPlatform.h"

namespace blaze {

AALPlatform::AALPlatform() {
  // create a AAL runtime client first
  m_runtimeClient = new RuntimeClient();
  if(!m_runtimeClient->isOK()){
    throw std::runtime_error("Runtime Failed to Start");
  }

  SetSubClassInterface(iidServiceClient, dynamic_cast<IServiceClient *>(this));
  SetInterface(iidCCIClient, dynamic_cast<ICCIClient *>(this));
  m_Sem.Create(0, 1);
  m_Status = true;

  env = new AALEnv();
}

void AALPlatform::setupAcc(AccWorker &conf) {

  std::string afu_name;
  std::string afu_id;
  std::string afu_location;

  for (int i=0; i<conf.param_size(); i++) {
    if (conf.param(i).key().compare("afu_name")==0) {
      afu_name = conf.param(i).value();
    }
    else if (conf.param(i).key().compare("afu_location")==0) {
      afu_location = conf.param(i).value();
    }
    else if (conf.param(i).key().compare("afu_id")==0) {
      afu_id = conf.param(i).value();
    }
  }
  if (afu_name.empty()) {
    throw std::runtime_error("Invalid Acc configuration, afu_name expected");
  }

  NamedValueSet Manifest;
  NamedValueSet ConfigRecord;

  if (afu_location.compare("HWAFU")==0) {             /* Use FPGA hardware */

    if (afu_id.empty()) {
      throw std::runtime_error("Invalid Acc Configuration, afu_id expected");
    }
    ConfigRecord.Add(
        AAL_FACTORY_CREATE_CONFIGRECORD_FULL_SERVICE_NAME, 
        "libHWCCIAFU");
    ConfigRecord.Add(keyRegAFU_ID, afu_id);
    ConfigRecord.Add(
        AAL_FACTORY_CREATE_CONFIGRECORD_FULL_AIA_NAME, 
        "libAASUAIA");
  }
  else if (afu_location.compare("ASEAFU")==0) {         /* Use ASE based RTL simulation */

    ConfigRecord.Add(AAL_FACTORY_CREATE_CONFIGRECORD_FULL_SERVICE_NAME, "libASECCIAFU");
    ConfigRecord.Add(AAL_FACTORY_CREATE_SOFTWARE_SERVICE,true);
  }

  else {                            /* default is Software Simulator */
    ConfigRecord.Add(AAL_FACTORY_CREATE_CONFIGRECORD_FULL_SERVICE_NAME, "libSWSimCCIAFU");
    ConfigRecord.Add(AAL_FACTORY_CREATE_SOFTWARE_SERVICE, true);
  }

  Manifest.Add(AAL_FACTORY_CREATE_CONFIGRECORD_INCLUDED, ConfigRecord);
  Manifest.Add(AAL_FACTORY_CREATE_SERVICENAME, afu_name);
  //MSG("Allocating Service");

  m_runtimeClient->getRuntime()->allocService(dynamic_cast<IBase *>(this), Manifest);
  m_Sem.Wait();

  if (m_Status != true) {
    throw std::runtime_error("failed to allocate AAL Service");
  }

  ((AALEnv*)env)->context = m_APPService;

  try {
    // create an AALBlock for DSM, size is always 4MB
    AALBlock_ptr block(new AALBlock((AALEnv*)env, DSM_SIZE, DSM_SIZE));
    dsm_block = block;

    ((AALEnv*)env)->dsm = block->getWorkspace();
  }
  catch (std::exception &e) {
    throw e;
  }
}

// <begin IServiceClient interface>
void AALPlatform::serviceAllocated(
    IBase *pServiceBase, 
    TransactionID const &rTranID)
{
  m_pAALService = pServiceBase;
  if (!m_pAALService) {
    m_Status = false;
    printf("no m_pAALService allocated\n");
    return;
  }
  // Documentation says CCIAFU Service publishes ICCIAFU as subclass interface
  m_APPService = subclass_ptr<ICCIAFU>(pServiceBase);

  if(!m_APPService) {
    m_Status = false;
    printf("no m_APPService allocated\n");
    return;
  }
  m_Sem.Post(1);
}

void AALPlatform::serviceAllocateFailed(const IEvent &rEvent)
{
  IExceptionTransactionEvent * pExEvent = 
    dynamic_ptr<IExceptionTransactionEvent>(iidExTranEvent, rEvent);

  ERR("Failed to allocate a Service");
  ERR(pExEvent->Description());

  m_Status = false;
  m_Sem.Post(1);
}

void AALPlatform::serviceFreed(TransactionID const &rTranID)
{
  // Unblock Main()
  m_Sem.Post(1);
}

void AALPlatform::serviceEvent(const IEvent &rEvent)
{
  ;
}
// <end IServiceClient interface>

} // namespace blaze
