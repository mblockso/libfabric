#ifndef _FI_PROV_OPA1X_FABRIC_H_
#define _FI_PROV_OPA1X_FABRIC_H_

#include "rdma/opa1x/fi_opa1x_shm_fabric.h"

#ifdef FI_OPA1X_FABRIC_HFI1
#include "rdma/opa1x/fi_opa1x_hfi1_fabric.h"

#define FI_OPA1X_FABRIC_TX_CONNECT	fi_opa1x_hfi1_tx_connect


#endif

#endif /* _FI_PROV_OPA1X_FABRIC_H_ */
