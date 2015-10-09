#include "AALBlock.h"

namespace blaze {

void AALBlock::alloc(int64_t _size) {
  ICCIAFU* service = env->getContext();
  printf("allocating AAL Block of size %d\n", _size);

  if (!allocated) {

    service->WorkspaceAllocate(_size, TransactionID());

    m_Sem.Wait();

    if (m_Status != true) {
      throw std::runtime_error("Failed to create AAL block");
    }
    size = _size;

    allocated = true;
  }
}

void AALBlock::writeData(void* src, size_t _size) {

  if (allocated) {

    if (_size > size) {
      throw std::runtime_error("not enough space");
    }
    printf("start memcpy for %lx\n", data_virt);
    memcpy(data_virt, src, _size);
    ready = true;
  }
  else {
    throw std::runtime_error("Block memory not allocated");
  }
}

void AALBlock::writeData(void* src, size_t _size, size_t offset) {

  if (allocated) {
    if (offset+_size > size) {
      throw std::runtime_error("Exists block size");
    }

    // TODO: check data width of data_virt
    memcpy(data_virt+offset, src, _size);

    if (offset + _size == size) {
      ready = true;
    }
  }
  else {
    throw std::runtime_error("Block memory not allocated");
  }
}

// write data to an array
void AALBlock::readData(void* dst, size_t _size) {
  if (allocated) {
    if (_size > size) {
      throw std::runtime_error("not enough space");
    }
    memcpy(dst, data_virt, _size);
  }
  else {
    throw std::runtime_error("Block memory not allocated");
  }
}

// <ICCIClient>
void AALBlock::OnWorkspaceAllocated(TransactionID const &TranID,
                                    btVirtAddr           WkspcVirt,
                                    btPhysAddr           WkspcPhys,
                                    btWSSize             WkspcSize)
{
   AutoLock(this);

   data_virt = WkspcVirt;
   data_phys = WkspcPhys;

   printf("Workspace allocated\n");

   m_Sem.Post(1);
}

void AALBlock::OnWorkspaceAllocateFailed(const IEvent &rEvent)
{
   IExceptionTransactionEvent * pExEvent = 
     dynamic_ptr<IExceptionTransactionEvent>(iidExTranEvent, rEvent);
   ERR("OnWorkspaceAllocateFailed");
   ERR(pExEvent->Description());

   m_Status = false;
   m_Sem.Post(1);
}

void AALBlock::OnWorkspaceFreeFailed(const IEvent &rEvent)
{
   IExceptionTransactionEvent * pExEvent = dynamic_ptr<IExceptionTransactionEvent>(iidExTranEvent, rEvent);
   ERR("OnWorkspaceFreeFailed");
   ERR(pExEvent->Description());
   m_Status = false;
   m_Sem.Post(1);
}

}; // namespace blaze
