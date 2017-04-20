#ifndef _FI_PROV_OPA1X_HFI1_FABRIC_H_
#define _FI_PROV_OPA1X_HFI1_FABRIC_H_

#ifndef FI_OPA1X_FABRIC_HFI1
#error "fabric selection #define error"
#endif

#include "rdma/opa1x/fi_opa1x_endpoint.h"

void fi_opa1x_hfi1_tx_connect (struct fi_opa1x_ep *opa1x_ep, fi_addr_t peer);


#endif /* _FI_PROV_OPA1X_HFI1_FABRIC_H_ */
