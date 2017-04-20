#ifndef _FI_PROV_OPA1X_FABRIC_TRANSPORT_H_ 
#define _FI_PROV_OPA1X_FABRIC_TRANSPORT_H_

#include "rdma/opa1x/fi_opa1x_shm_transport.h"

#define FI_OPA1X_FABRIC_HFI1

#ifdef FI_OPA1X_FABRIC_HFI1
#include "rdma/opa1x/fi_opa1x_hfi1_transport.h"

#define FI_OPA1X_FABRIC_TX_INJECT	fi_opa1x_hfi1_tx_inject
#define FI_OPA1X_FABRIC_TX_SEND_EGR	fi_opa1x_hfi1_tx_send_egr
#define FI_OPA1X_FABRIC_TX_SEND_RZV	fi_opa1x_hfi1_tx_send_rzv
#define FI_OPA1X_FABRIC_RX_RZV_RTS	fi_opa1x_hfi1_rx_rzv_rts
#define FI_OPA1X_FABRIC_RX_RZV_CTS	fi_opa1x_hfi1_rx_rzv_cts

#endif

#endif /* _FI_PROV_OPA1X_FABRIC_TRANSPORT_H_ */
