#ifndef _FI_PROV_OPA1X_FABRIC_PROGRESS_H_ 
#define _FI_PROV_OPA1X_FABRIC_PROGRESS_H_

#include "rdma/opa1x/fi_opa1x_shm_progress.h"

#ifdef FI_OPA1X_FABRIC_HFI1
#include "rdma/opa1x/fi_opa1x_hfi1_progress.h"

#define FI_OPA1X_FABRIC_POLL_ONCE	fi_opa1x_hfi1_poll_once
#define FI_OPA1X_FABRIC_POLL_MANY	fi_opa1x_hfi1_poll_many


#endif

#endif /* _FI_PROV_OPA1X_FABRIC_PROGRESS_H_ */
