// Copyright (c) 2007-2015, Intel Corporation
//
// Redistribution  and  use  in source  and  binary  forms,  with  or  without
// modification, are permitted provided that the following conditions are met:
//
// * Redistributions of  source code  must retain the  above copyright notice,
//   this list of conditions and the following disclaimer.
// * Redistributions in binary form must reproduce the above copyright notice,
//   this list of conditions and the following disclaimer in the documentation
//   and/or other materials provided with the distribution.
// * Neither the name  of Intel Corporation  nor the names of its contributors
//   may be used to  endorse or promote  products derived  from this  software
//   without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
// AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING,  BUT NOT LIMITED TO,  THE
// IMPLIED WARRANTIES OF  MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
// ARE DISCLAIMED.  IN NO EVENT  SHALL THE COPYRIGHT OWNER  OR CONTRIBUTORS BE
// LIABLE  FOR  ANY  DIRECT,  INDIRECT,  INCIDENTAL,  SPECIAL,  EXEMPLARY,  OR
// CONSEQUENTIAL  DAMAGES  (INCLUDING,  BUT  NOT LIMITED  TO,  PROCUREMENT  OF
// SUBSTITUTE GOODS OR SERVICES;  LOSS OF USE,  DATA, OR PROFITS;  OR BUSINESS
// INTERRUPTION)  HOWEVER CAUSED  AND ON ANY THEORY  OF LIABILITY,  WHETHER IN
// CONTRACT,  STRICT LIABILITY,  OR TORT  (INCLUDING NEGLIGENCE  OR OTHERWISE)
// ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE,  EVEN IF ADVISED OF THE
// POSSIBILITY OF SUCH DAMAGE.
//****************************************************************************
/// @file HelloCCINLB.cpp
/// @brief Basic CCI AFU interaction.
/// @ingroup HelloCCINLB
/// @verbatim
/// Intel(R) QuickAssist Technology Accelerator Abstraction Layer Sample Application
///
///    This application is for example purposes only.
///    It is not intended to represent a model for developing commercially-deployable applications.
///    It is designed to show working examples of the AAL programming model and APIs.
///
/// AUTHORS: Joseph Grecco, Intel Corporation.
///
/// This Sample demonstrates the following:
///    - The basic structure of an AAL program using the XL APIs.
///    - The ICCI and ICCIClient interfaces of CCIAFU Service.
///    - System initialization and shutdown.
///    - Use of interface IDs (iids).
///    - Accessing object interfaces through the Interface functions.
///
/// This sample is designed to be used with the CCIAFU Service.
///
/// HISTORY:
/// WHEN:          WHO:     WHAT:
/// 06/09/2015     JG       Initial version started based on older sample code.@endverbatim
//****************************************************************************
#include <aalsdk/AAL.h>
#include <aalsdk/xlRuntime.h>
#include <aalsdk/AALLoggerExtern.h> // Logger


#include <aalsdk/service/ICCIAFU.h>
#include <aalsdk/service/ICCIClient.h>

#include <string.h>
#include <time.h>

//****************************************************************************
// UN-COMMENT appropriate #define in order to enable either Hardware or ASE.
//    DEFAULT is to use Software Simulation.
//****************************************************************************
// #define  HWAFU
#define  ASEAFU

using namespace AAL;

// Convenience macros for printing messages and errors.
#ifndef MSG
# define MSG(x) std::cout << __AAL_SHORT_FILE__ << ':' << __LINE__ << ':' << __AAL_FUNC__ << "() : " << x << std::endl
#endif // MSG
#ifndef ERR
# define ERR(x) std::cerr << __AAL_SHORT_FILE__ << ':' << __LINE__ << ':' << __AAL_FUNC__ << "() **Error : " << x << std::endl
#endif // ERR

// Print/don't print the event ID's entered in the event handlers.
#if 1
# define EVENT_CASE(x) case x : MSG(#x);
#else
# define EVENT_CASE(x) case x :
#endif

#ifndef CL
# define CL(x)                     ((x) * 64)
#endif // CL
#ifndef LOG2_CL
# define LOG2_CL                   6
#endif // LOG2_CL
#ifndef MB
# define MB(x)                     ((x) * 1024 * 1024)
#endif // MB

#define LPBK1_DSM_SIZE           MB(4)
#define CSR_AFU_DSM_BASEH        0x1a04
#define CSR_SRC_ADDR             0x1a20
#define CSR_DST_ADDR             0x1a24
#define CSR_CTL                  0x1a2c
#define CSR_CFG                  0x1a34
#define CSR_CIPUCTL              0x280
#define CSR_NUM_LINES            0x1a28
#define DSM_STATUS_TEST_COMPLETE 0x40
#define CSR_AFU_DSM_BASEL        0x1a00
#define CSR_AFU_DSM_BASEH        0x1a04

/// @addtogroup HelloCCINLB
/// @{



/// @brief   Define our Runtime client class so that we can receive the runtime started/stopped notifications.
///
/// We implement a Service client within, to handle AAL Service allocation/free.
/// We also implement a Semaphore for synchronization with the AAL runtime.
class RuntimeClient : public CAASBase,
                      public IRuntimeClient
{
public:
   RuntimeClient();
   ~RuntimeClient();

   void end();

   IRuntime* getRuntime();

   btBool isOK();

   // <begin IRuntimeClient interface>
   void runtimeStarted(IRuntime            *pRuntime,
                       const NamedValueSet &rConfigParms);

   void runtimeStopped(IRuntime *pRuntime);

   void runtimeStartFailed(const IEvent &rEvent);

   void runtimeAllocateServiceFailed( IEvent const &rEvent);

   void runtimeAllocateServiceSucceeded(IBase               *pClient,
                                        TransactionID const &rTranID);

   void runtimeEvent(const IEvent &rEvent);
   

   // <end IRuntimeClient interface>


protected:
   IRuntime        *m_pRuntime;  // Pointer to AAL runtime instance.
   Runtime          m_Runtime;   // AAL Runtime
   btBool           m_isOK;      // Status
   CSemaphore       m_Sem;       // For synchronizing with the AAL runtime.
};

///////////////////////////////////////////////////////////////////////////////
///
///  MyRuntimeClient Implementation
///
///////////////////////////////////////////////////////////////////////////////
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


 /// @brief   Define our Service client class so that we can receive Service-related notifications from the AAL Runtime.
 ///          The Service Client contains the application logic.
 ///
 /// When we request an AFU (Service) from AAL, the request will be fulfilled by calling into this interface.
 class HelloCCINLBApp : public CAASBase, public IServiceClient, public ICCIClient
 {
 public:
    enum WorkspaceType
    {
       WKSPC_DSM, ///< Device Status Memory
       WKSPC_IN,  ///< Input workspace
       WKSPC_OUT  ///< Output workspace
    };

    HelloCCINLBApp(void* dst, void* src, uint64_t size, RuntimeClient * rtc);
    ~HelloCCINLBApp();

    void run();

    // <ICCIClient>
    virtual void      OnWorkspaceAllocated(TransactionID const &TranID,
                                           btVirtAddr           WkspcVirt,
                                           btPhysAddr           WkspcPhys,
                                           btWSSize             WkspcSize);

    virtual void OnWorkspaceAllocateFailed(const IEvent &Event);

    virtual void          OnWorkspaceFreed(TransactionID const &TranID);

    virtual void     OnWorkspaceFreeFailed(const IEvent &Event);
    // </ICCIClient>

    // <begin IServiceClient interface>
    void serviceAllocated(IBase               *pServiceBase,
                          TransactionID const &rTranID);

    void serviceAllocateFailed(const IEvent        &rEvent);

    void serviceFreed(TransactionID const &rTranID);

    void serviceEvent(const IEvent &rEvent);
    // <end IServiceClient interface>



 protected:
    IBase           *m_pAALService;    // The generic AAL Service interface for the AFU.
    RuntimeClient   *m_runtimeClient;
    ICCIAFU         *m_NLBService;
    CSemaphore       m_Sem;            // For synchronizing with the AAL runtime.
    btBool           m_Status;
    btUnsignedInt    m_wsfreed;        // Simple counter used for when we free workspaces

    // Workspace info
    btVirtAddr m_DSMVirt;    ///< DSM workspace virtual address.
    btPhysAddr m_DSMPhys;    ///< DSM workspace physical address.
    btWSSize   m_DSMSize;    ///< DSM workspace size in bytes.
    btVirtAddr m_InputVirt;  ///< Input workspace virtual address.
    btPhysAddr m_InputPhys;  ///< Input workspace physical address.
    btWSSize   m_InputSize;  ///< Input workspace size in bytes.
    btVirtAddr m_OutputVirt; ///< Output workspace virtual address.
    btPhysAddr m_OutputPhys; ///< Output workspace physical address.
    btWSSize   m_OutputSize; ///< Output workspace size in bytes.

 private:
    void*      m_DstAddr;
    void*      m_SrcAddr;
    uint64_t   m_LoopbackBufferSize;
 };

///////////////////////////////////////////////////////////////////////////////
///
///  Implementation
///
///////////////////////////////////////////////////////////////////////////////
 HelloCCINLBApp::HelloCCINLBApp(void* dst, void* src, uint64_t size, RuntimeClient *rtc):
    m_pAALService(NULL),
    m_runtimeClient(rtc),
    m_NLBService(NULL),
    m_Status(true),
    m_DstAddr(dst),
    m_SrcAddr(src),
    m_LoopbackBufferSize(size)
 {
    SetSubClassInterface(iidServiceClient, dynamic_cast<IServiceClient *>(this));
    SetInterface(iidCCIClient, dynamic_cast<ICCIClient *>(this));
    m_Sem.Create(0, 1);
 }

 HelloCCINLBApp::~HelloCCINLBApp()
 {
    m_Sem.Destroy();
 }

void HelloCCINLBApp::run()
{
   cout <<"========================"<<endl;
   cout <<"= Hello CCI NLB Sample ="<<endl;
   cout <<"========================"<<endl;

   // Request our AFU.

   // NOTE: This example is bypassing the Resource Manager's configuration record lookup
   //  mechanism.  This code is work around code and subject to change. But it does
   //  illustrate the utility of having different implementations of a service all
   //  readily available and bound at run-time.
   NamedValueSet Manifest;
   NamedValueSet ConfigRecord;

#if defined( HWAFU )                /* Use FPGA hardware */

   ConfigRecord.Add(AAL_FACTORY_CREATE_CONFIGRECORD_FULL_SERVICE_NAME, "libHWCCIAFU");
   ConfigRecord.Add(keyRegAFU_ID,"C000C966-0D82-4272-9AEF-FE5F84570612");
   ConfigRecord.Add(AAL_FACTORY_CREATE_CONFIGRECORD_FULL_AIA_NAME, "libAASUAIA");

   #elif defined ( ASEAFU )         /* Use ASE based RTL simulation */

   ConfigRecord.Add(AAL_FACTORY_CREATE_CONFIGRECORD_FULL_SERVICE_NAME, "libASECCIAFU");
   ConfigRecord.Add(AAL_FACTORY_CREATE_SOFTWARE_SERVICE,true);

   #else                            /* default is Software Simulator */

   ConfigRecord.Add(AAL_FACTORY_CREATE_CONFIGRECORD_FULL_SERVICE_NAME, "libSWSimCCIAFU");
   ConfigRecord.Add(AAL_FACTORY_CREATE_SOFTWARE_SERVICE,true);

#endif

   Manifest.Add(AAL_FACTORY_CREATE_CONFIGRECORD_INCLUDED, ConfigRecord);
   Manifest.Add(AAL_FACTORY_CREATE_SERVICENAME, "Hello CCI NLB");
   MSG("Allocating Service");

   // Allocate the Service and allocate the required workspace.
   //   This happens in the background via callbacks (simple state machine).
   //   When everything is set we do the real work here in the main thread.
   m_runtimeClient->getRuntime()->allocService(dynamic_cast<IBase *>(this), Manifest);

   m_Sem.Wait();

   // If all went well run test.
   //   NOTE: If not successful we simply bail.
   //         A better design would do all appropriate clean-up.
   if(true == m_Status){

      //=============================
      // Now we have the NLB Service
      //   now we can use it
      //=============================
      MSG("Running Test");

      // Initialize the source and destination buffers
      memcpy( m_InputVirt, m_SrcAddr, m_InputSize);
      //memset( m_InputVirt,  0xAF, m_InputSize);    // Input initialized to AFter
      memset( m_OutputVirt, 0xBE, m_OutputSize);   // Output initialized to BEfore

      // Set DSM base, high then low
      m_NLBService->CSRWrite64(CSR_AFU_DSM_BASEL, m_DSMPhys);

      // Assert Device Reset
      m_NLBService->CSRWrite(CSR_CTL, 0);

      // Clear the DSM
      ::memset((void *)m_DSMVirt, 0, m_DSMSize);

      // De-assert Device Reset
      m_NLBService->CSRWrite(CSR_CTL, 1);

      // Set input workspace address
      m_NLBService->CSRWrite(CSR_SRC_ADDR, CACHELINE_ALIGNED_ADDR(m_InputPhys));

      // Set output workspace address
      m_NLBService->CSRWrite(CSR_DST_ADDR, CACHELINE_ALIGNED_ADDR(m_OutputPhys));

      // Set the number of cache lines for the test
      m_NLBService->CSRWrite(CSR_NUM_LINES, m_LoopbackBufferSize / CL(1));

      // Set the test mode
      m_NLBService->CSRWrite(CSR_CFG, 0);


      volatile bt32bitCSR *StatusAddr = (volatile bt32bitCSR *)
                                         (m_DSMVirt  + DSM_STATUS_TEST_COMPLETE);

      // Start the test
      m_NLBService->CSRWrite(CSR_CTL, 3);


      // Wait for test completion
      while( 0 == *StatusAddr ) {
         SleepMicro(100);
      }
      MSG("Done Running Test");

      // Stop the device
      m_NLBService->CSRWrite(CSR_CTL, 7);

      memcpy( m_DstAddr, m_OutputVirt, m_OutputSize);

      // Check that output buffer now contains what was in input buffer, e.g. 0xAF
      //if (int err = memcmp( m_OutputVirt, m_InputVirt, m_OutputSize)) {
      //   ERR("Output does NOT Match input, at offset " << err << "!");
      //} else {
      //   MSG("Output matches Input!");
      //}

      // Now clean up Workspaces and Release.
      //  Once again all of this is done in a simple
      //  state machine via callbacks

      // Release the Workspaces and wait for all three then Release the Service
      m_wsfreed = 0;  // Reset the counter
      m_NLBService->WorkspaceFree(m_InputVirt,  TransactionID((bt32bitInt)HelloCCINLBApp::WKSPC_IN));
      m_NLBService->WorkspaceFree(m_OutputVirt, TransactionID((bt32bitInt)HelloCCINLBApp::WKSPC_OUT));
      m_NLBService->WorkspaceFree(m_DSMVirt,    TransactionID((bt32bitInt)HelloCCINLBApp::WKSPC_DSM));
      m_Sem.Wait();
   }

   m_runtimeClient->end();
}

 // We must implement the IServiceClient interface (IServiceClient.h):

 // <begin IServiceClient interface>
 void HelloCCINLBApp::serviceAllocated(IBase               *pServiceBase,
                                       TransactionID const &rTranID)
 {
    m_pAALService = pServiceBase;
    ASSERT(NULL != m_pAALService);

    // Documentation says CCIAFU Service publishes ICCIAFU as subclass interface
    m_NLBService = subclass_ptr<ICCIAFU>(pServiceBase);

    ASSERT(NULL != m_NLBService);
    if( NULL == m_NLBService ) {
       return;
    }

    MSG("Service Allocated");

    // Allocate first of 3 Workspaces needed.  Use the TransactionID to tell which was allocated.
    //   In workspaceAllocated() callback we allocate the rest
    m_NLBService->WorkspaceAllocate(LPBK1_DSM_SIZE, TransactionID((bt32bitInt)HelloCCINLBApp::WKSPC_DSM));

 }

 void HelloCCINLBApp::serviceAllocateFailed(const IEvent        &rEvent)
 {
    IExceptionTransactionEvent * pExEvent = dynamic_ptr<IExceptionTransactionEvent>(iidExTranEvent, rEvent);
    ERR("Failed to allocate a Service");
    ERR(pExEvent->Description());
    m_Sem.Post(1);
 }

 void HelloCCINLBApp::serviceFreed(TransactionID const &rTranID)
 {
    MSG("Service Freed");
    // Unblock Main()
    m_Sem.Post(1);
 }

 // <ICCIClient>
void HelloCCINLBApp::OnWorkspaceAllocated(TransactionID const &TranID,
                                          btVirtAddr           WkspcVirt,
                                          btPhysAddr           WkspcPhys,
                                          btWSSize             WkspcSize)
{
   AutoLock(this);

   switch ( (HelloCCINLBApp::WorkspaceType)TranID.ID() ) {

      case WKSPC_DSM: {
         m_DSMVirt = WkspcVirt;
         m_DSMPhys = WkspcPhys;
         m_DSMSize = WkspcSize;
         INFO("Got DSM");
         m_NLBService->WorkspaceAllocate(m_LoopbackBufferSize, TransactionID((bt32bitInt)HelloCCINLBApp::WKSPC_IN));
      }break;
      case WKSPC_IN : {
         m_InputVirt = WkspcVirt;
         m_InputPhys = WkspcPhys;
         m_InputSize = WkspcSize;
         INFO("Got Input Workspace");

         // Now get Output workspace
         m_NLBService->WorkspaceAllocate(m_LoopbackBufferSize, TransactionID((bt32bitInt)HelloCCINLBApp::WKSPC_OUT));
      } break;
      case WKSPC_OUT : {
         m_OutputVirt = WkspcVirt;
         m_OutputPhys = WkspcPhys;
         m_OutputSize = WkspcSize;

         INFO("Got Output Workspace");

         // Got all workspaces so unblock the Run() thread
         m_Sem.Post(1);
      } break;

      default : {
         m_Status = false;
         ERR("Invalid workspace type: " << TranID.ID());
      } break;
   }
}

void HelloCCINLBApp::OnWorkspaceAllocateFailed(const IEvent &rEvent)
{
   IExceptionTransactionEvent * pExEvent = dynamic_ptr<IExceptionTransactionEvent>(iidExTranEvent, rEvent);
   ERR("OnWorkspaceAllocateFailed");
   ERR(pExEvent->Description());


   m_Status = false;
   m_Sem.Post(1);
}

void HelloCCINLBApp::OnWorkspaceFreed(TransactionID const &TranID)
{
   ERR("OnWorkspaceFreed");
   if(++m_wsfreed == 3){
      // Freed all three so now Release() the Service through the Services IAALService::Release() method
      (dynamic_ptr<IAALService>(iidService, m_pAALService))->Release(TransactionID());
   }

}

void HelloCCINLBApp::OnWorkspaceFreeFailed(const IEvent &rEvent)
{
   IExceptionTransactionEvent * pExEvent = dynamic_ptr<IExceptionTransactionEvent>(iidExTranEvent, rEvent);
   ERR("OnWorkspaceAllocateFailed");
   ERR(pExEvent->Description());
   m_Status = false;
   m_Sem.Post(1);
}


 void HelloCCINLBApp::serviceEvent(const IEvent &rEvent)
 {
    ERR("unexpected event 0x" << hex << rEvent.SubClassID());
 }
 // <end IServiceClient interface>

/// @} group HelloCCINLB

