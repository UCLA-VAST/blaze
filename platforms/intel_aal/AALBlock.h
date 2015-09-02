#ifndef AALBLOCK_H
#define AALBLOCK_H

#include <stdio.h>
#include <string.h>
#include <string>
#include <stdexcept>

#include <boost/smart_ptr.hpp>
#include <boost/iostreams/device/mapped_file.hpp>

#include <aalsdk/AAL.h>
#include <aalsdk/xlRuntime.h>
#include <aalsdk/service/ICCIAFU.h>
#include <aalsdk/service/ICCIClient.h>

#include "RuntimeClient.h"
#include "Block.h"
#include "AALEnv.h"

namespace blaze {

class AALBlock : public DataBlock, 
                 public CAASBase,
                 public ICCIClient
{

public:
  // create a single output elements
  AALBlock(AALEnv* _env, size_t _length, size_t _size):
    env(_env),
    m_Status(true)
  {
    SetInterface(iidCCIClient, dynamic_cast<ICCIClient *>(this));
    size = _size;
    length = _length;
    ready = false;
    
    try {
      alloc(size);
    }
    catch (std::exception &e) {
      throw e;
    }
  }
  
  AALBlock(AALEnv* _env): 
    DataBlock(), 
    env(_env),
    m_Status(true)
  {
    //SetInterface(iidCCIClient, dynamic_cast<ICCIClient *>(this));
  }

  AALBlock(AALEnv* _env, DataBlock *block): env(_env) 
  {
    //SetInterface(iidCCIClient, dynamic_cast<ICCIClient *>(this));
    length = block->getLength();
    size = block->getSize();
    num_items = block->getNumItems();
    m_Status = true;

    if (block->isAllocated()) {
      try {
        alloc(size);
      }
      catch (std::exception &e) {
        throw e;
      }
    }
    // if ready, copy the data over
    if (block->isReady()) {
      writeData((void*)block->getData(), size);
      ready = true;
    }
  }
  
  ~AALBlock() {
    ICCIAFU* service = env->getContext();
    if (allocated) {
      service->WorkspaceFree(data_virt, TransactionID());
      m_Sem.Wait();

      if (!m_Status) {
        printf("freeing block failed\n");
      }
    }
  }

  virtual void alloc(int64_t _size);

  // copy data from an array
  virtual void writeData(void* src, size_t _size);

  // copy data from an array with offset
  virtual void writeData(void* src, size_t _size, size_t offset);

  // write data to an array
  virtual void readData(void* dst, size_t size);

  virtual char* getData() { 

    if (allocated) {
      // this is a reinterpretive cast from cl_mem* to char*
      return (char*)data_phys; 
    }
    else {
      return NULL;
    }
  }

  AAL_Workspace getWorkspace() {
    AAL_Workspace res;
    res.virt = data_virt;
    res.phys = data_phys;
    res.size = size;

    return res;
  }

  // <ICCIClient>
  virtual void OnWorkspaceAllocated(TransactionID const &TranID,
                                    btVirtAddr           WkspcVirt,
                                    btPhysAddr           WkspcPhys,
                                    btWSSize             WkspcSize);

  virtual void OnWorkspaceAllocateFailed(const IEvent &Event);

  virtual void OnWorkspaceFreed(TransactionID const &TranID) {;}

  virtual void OnWorkspaceFreeFailed(const IEvent &Event);
  // </ICCIClient>

private:
  
  btVirtAddr  data_virt;   // workspace virtual address.
  btPhysAddr  data_phys;   // workspace physical address.

  CSemaphore  m_Sem;       // For synchronizing with the AAL runtime.
  btBool      m_Status;

  AALEnv      *env;
};

typedef boost::shared_ptr<AALBlock> AALBlock_ptr;
}

#endif
