/*
 * Copyright (C) 2016 by Argonne National Laboratory.
 *
 * This software is available to you under a choice of one of two
 * licenses.  You may choose to be licensed under the terms of the GNU
 * General Public License (GPL) Version 2, available from the file
 * COPYING in the main directory of this source tree, or the
 * BSD license below:
 *
 *     Redistribution and use in source and binary forms, with or
 *     without modification, are permitted provided that the following
 *     conditions are met:
 *
 *      - Redistributions of source code must retain the above
 *        copyright notice, this list of conditions and the following
 *        disclaimer.
 *
 *      - Redistributions in binary form must reproduce the above
 *        copyright notice, this list of conditions and the following
 *        disclaimer in the documentation and/or other materials
 *        provided with the distribution.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
 * ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

#include <fi.h>
#include "rdma/opa1x/fi_opa1x.h"
#include "rdma/opa1x/fi_opa1x_internal.h"
#include "rdma/opa1x/fi_opa1x_hfi1.h"

#include "rdma/opa1x/fi_opa1x_addr.h"

int fi_opa1x_set_default_info()
{
	struct fi_info *fi;
	fi = fi_dupinfo(NULL);
	if (!fi) {
		errno = FI_ENOMEM;
		return -errno;
	}

	fi_opa1x_global.info = fi;

	*fi->tx_attr = (struct fi_tx_attr) {
		.caps		= FI_OPA1X_DEFAULT_TX_CAPS,
		.mode		= FI_OPA1X_DEFAULT_MODE,
		.op_flags	= FI_TRANSMIT_COMPLETE,
		.msg_order	= FI_OPA1X_DEFAULT_MSG_ORDER,
		.comp_order	= FI_ORDER_NONE,
		.inject_size	= FI_OPA1X_HFI1_PACKET_IMM,
		.size		= SIZE_MAX,
		.iov_limit	= SIZE_MAX,
		.rma_iov_limit  = 0
	};

	*fi->rx_attr = (struct fi_rx_attr) {
		.caps		= FI_OPA1X_DEFAULT_RX_CAPS,
		.mode		= FI_OPA1X_DEFAULT_MODE,
		.op_flags	= FI_MULTI_RECV,
		.msg_order	= FI_OPA1X_DEFAULT_MSG_ORDER,
		.comp_order	= FI_ORDER_NONE,
		.total_buffered_recv = FI_OPA1X_HFI1_PACKET_MTU + 64 /* header */,
		.size		= SIZE_MAX,
		.iov_limit	= SIZE_MAX
	};

	*fi->ep_attr = (struct fi_ep_attr) {
		.type			= FI_EP_RDM,
		.protocol		= FI_PROTO_OPA1X,
		.protocol_version	= FI_OPA1X_PROTOCOL_VERSION,
		.max_msg_size		= FI_OPA1X_MAX_MSG_SIZE,
		.msg_prefix_size	= 0,
		.max_order_raw_size	= 0,
		.max_order_war_size	= 0,
		.max_order_waw_size	= 0,
		.mem_tag_format		= FI_OPA1X_MEM_TAG_FORMAT,
		.tx_ctx_cnt		= 1,
		.rx_ctx_cnt		= 1,
		.auth_key_size		= 0,
		.auth_key		= NULL
	};

	*fi->domain_attr = (struct fi_domain_attr) {
		.domain		= NULL,
		.name		= NULL, /* TODO: runtime query for name? */
		.threading	= FABRIC_DIRECT_THREAD,
		.control_progress = FABRIC_DIRECT_PROGRESS,
		.data_progress	= FABRIC_DIRECT_PROGRESS == FI_PROGRESS_UNSPEC ? FI_PROGRESS_MANUAL : FABRIC_DIRECT_PROGRESS,
		.resource_mgmt	= FI_RM_ENABLED,
		.av_type	= FABRIC_DIRECT_AV,
		.mr_mode	= FABRIC_DIRECT_MR,
		.mr_key_size	= 2,
		.cq_data_size	= FI_OPA1X_REMOTE_CQ_DATA_SIZE,
		.cq_cnt		= SIZE_MAX,
		.ep_cnt		= SIZE_MAX,
		.tx_ctx_cnt	= 160,	/* TODO ppn */
		.rx_ctx_cnt	= 160,	/* TODO ppn */

		.max_ep_tx_ctx	= 160,	/* TODO ppn */
		.max_ep_rx_ctx	= 160,	/* TODO ppn */
		.max_ep_stx_ctx	= SIZE_MAX,
		.max_ep_srx_ctx	= 0,
		.cntr_cnt	= 0,
		.mr_iov_limit	= 1,
		.caps		= OPA1X_LOCAL_COMM_CAP | FI_REMOTE_COMM,	/* TODO: FI_SHARED_AV */
		.mode		= 0,
		.auth_key	= NULL,
		.auth_key_size	= 0,
		.max_err_data	= 0,
		.mr_cnt		= 0					/* TODO: FI_MR */
	};

	*fi->fabric_attr = (struct fi_fabric_attr) {
		.fabric		= NULL,
		.name		= strdup(FI_OPA1X_FABRIC_NAME),
		.prov_name	= NULL,
		.prov_version	= FI_OPA1X_PROVIDER_VERSION
	};

	fi->caps		= FI_OPA1X_DEFAULT_TX_CAPS | FI_OPA1X_DEFAULT_RX_CAPS;
	fi->mode		= FI_OPA1X_DEFAULT_MODE;

	fi->addr_format		= FI_ADDR_OPA1X;
	fi->src_addrlen		= sizeof(union fi_opa1x_addr);
	fi->dest_addrlen	= sizeof(union fi_opa1x_addr);
	fi->dest_addr = NULL;
	fi->next = NULL;

	return 0;
}
