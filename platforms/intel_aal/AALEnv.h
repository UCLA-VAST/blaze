#ifndef AALENV_H
#define AALENV_H

#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>
#include <string>
#include <stdexcept>

#include <aalsdk/AAL.h>
#include <aalsdk/xlRuntime.h>
#include <aalsdk/service/ICCIAFU.h>
#include <aalsdk/service/ICCIClient.h>

#include "TaskEnv.h"

using namespace AAL;

namespace blaze {

typedef struct {
  btVirtAddr virt;
  btPhysAddr phys;
  btWSSize   size;
} AAL_Workspace;

class AALPlatform;

class AALEnv : public TaskEnv
{
  friend class AALPlatform;

public:
  AALEnv(): 
    TaskEnv(),
    context(NULL)
  {;}

  ICCIAFU* getContext() {
    return context;
  }

  AAL_Workspace getDSM() {
    return dsm;
  }

private:
  ICCIAFU *context;
  AAL_Workspace dsm;
};
}

#endif 
