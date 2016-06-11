#include "RuntimeClient.h"

RuntimeClient::RuntimeClient() :
    m_Runtime(),        // Instantiate the AAL Runtime
    m_pRuntime(NULL),
    m_isOK(false)
{
   NamedValueSet configArgs;
   NamedValueSet configRecord;

   // Publish our interface
   SetSubClassInterface(iidRuntimeClient, dynamic_cast<IRuntimeClient *>(this));

   m_Sem.Create(0, 1);

   // Using Hardware Services requires the Remote Resource Manager Broker Service
   //  Note that this could also be accomplished by setting the environment variable
   //   XLRUNTIME_CONFIG_BROKER_SERVICE to librrmbroker
#if defined( HWAFU )
   configRecord.Add(XLRUNTIME_CONFIG_BROKER_SERVICE, "librrmbroker");
   configArgs.Add(XLRUNTIME_CONFIG_RECORD,configRecord);
#endif

   if(!m_Runtime.start(this, configArgs)){
      m_isOK = false;
      return;
   }
   m_Sem.Wait();
}

RuntimeClient::~RuntimeClient()
{
    m_Sem.Destroy();
}

btBool RuntimeClient::isOK()
{
   return m_isOK;
}


void RuntimeClient::runtimeStarted(IRuntime            *pRuntime,
                                    const NamedValueSet &rConfigParms)
 {
    // Save a copy of our runtime interface instance.
    m_pRuntime = pRuntime;
    m_isOK = true;
    m_Sem.Post(1);
 }

void RuntimeClient::end()
{
   m_Runtime.stop();
   m_Sem.Wait();
}

void RuntimeClient::runtimeStopped(IRuntime *pRuntime)
 {
    MSG("Runtime stopped");
    m_isOK = false;
    m_Sem.Post(1);
 }

void RuntimeClient::runtimeStartFailed(const IEvent &rEvent)
{
   IExceptionTransactionEvent * pExEvent = dynamic_ptr<IExceptionTransactionEvent>(iidExTranEvent, rEvent);
   ERR("Runtime start failed");
   ERR(pExEvent->Description());
}

void RuntimeClient::runtimeAllocateServiceFailed( IEvent const &rEvent)
{
   IExceptionTransactionEvent * pExEvent = dynamic_ptr<IExceptionTransactionEvent>(iidExTranEvent, rEvent);
   ERR("Runtime AllocateService failed");
   ERR(pExEvent->Description());

}

void RuntimeClient::runtimeAllocateServiceSucceeded(IBase *pClient,
                                                    TransactionID const &rTranID)
{
    MSG("Runtime Allocate Service Succeeded");
}

void RuntimeClient::runtimeEvent(const IEvent &rEvent)
{
    MSG("Generic message handler (runtime)");
}

IRuntime * RuntimeClient::getRuntime()
{
   return m_pRuntime;
}


