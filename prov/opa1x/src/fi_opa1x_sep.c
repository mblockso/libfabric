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
#include <ofi.h>

#include "rdma/opa1x/fi_opa1x.h"
#include "rdma/opa1x/fi_opa1x_domain.h"
#include "rdma/opa1x/fi_opa1x_endpoint.h"

#include <ofi.h>
#include <ofi_enosys.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <errno.h>
#include <unistd.h>
#include <limits.h>

/* forward declaration */
int fi_opa1x_endpoint_rx_tx (struct fid_domain *dom, struct fi_info *info,
		struct fid_ep **ep, void *context, const uint8_t cx);

static int fi_opa1x_close_sep(fid_t fid)
{
	int ret;
	struct fi_opa1x_sep *opa1x_sep = container_of(fid, struct fi_opa1x_sep, ep_fid);

	ret = fi_opa1x_fid_check(fid, FI_CLASS_SEP, "scalable endpoint");
	if (ret)
		return ret;

	fi_opa1x_finalize_cm_ops(&opa1x_sep->ep_fid.fid);

	ret = fi_opa1x_ref_dec(&opa1x_sep->av->ref_cnt, "address vector");
	if (ret)
		return ret;

	ret = fi_opa1x_ref_finalize(&opa1x_sep->ref_cnt, "scalable endpoint");
	if (ret)
		return ret;

	ret = fi_opa1x_ref_dec(&opa1x_sep->domain->ref_cnt, "domain");
	if (ret)
		return ret;

	free(opa1x_sep->info->ep_attr);
	free(opa1x_sep->info);
	void * memptr = opa1x_sep->memptr;
	free(memptr);

	return 0;
}

static int fi_opa1x_control_sep(fid_t fid, int command, void *arg)
{
	struct fid_ep *ep __attribute__ ((unused));
	ep = container_of(fid, struct fid_ep, fid);
	return 0;
}

static int fi_opa1x_tx_ctx(struct fid_ep *sep, int index,
			struct fi_tx_attr *attr, struct fid_ep **tx_ep,
			void *context)
{
	int ret;
	struct fi_info info = {0};
	struct fi_tx_attr tx_attr = {0};
	struct fi_ep_attr ep_attr = {0};
	struct fi_domain_attr dom_attr = {0};
	struct fi_fabric_attr fab_attr = {0};
	struct fi_opa1x_sep *opa1x_sep;
	struct fi_opa1x_ep  *opa1x_tx_ep;


//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
	if (!sep || !attr || !tx_ep) {
//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
		errno = FI_EINVAL;
		return -errno;
	}
//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);

	opa1x_sep = container_of(sep, struct fi_opa1x_sep, ep_fid);

	uint64_t caps = attr->caps;	/* TODO - "By default, a transmit context inherits the properties of its associated endpoint. However, applications may request context specific attributes through the attr parameter." */

//fprintf(stderr, "%s:%s():%d caps = 0x%016lx\n", __FILE__, __func__, __LINE__, caps);
	if ((caps & FI_MSG || caps & FI_TAGGED) && (caps & FI_RECV)) {
//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
		FI_LOG(fi_opa1x_global.prov, FI_LOG_DEBUG, FI_LOG_DOMAIN,
				"FI_MSG|FI_TAGGED with FI_RECV capability specified for a TX context\n");
		caps &= ~FI_RECV;
	}
	caps &= ~FI_RECV;

//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
	if ((caps & FI_RMA || caps & FI_ATOMIC) && (caps & FI_REMOTE_READ || caps & FI_REMOTE_WRITE)) {
//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
		FI_LOG(fi_opa1x_global.prov, FI_LOG_DEBUG, FI_LOG_DOMAIN,
				"FI_RMA|FI_ATOMIC with FI_REMOTE_READ|FI_REMOTE_WRITE capability specified for a TX context\n");
		caps &= ~FI_REMOTE_READ;
		caps &= ~FI_REMOTE_WRITE;
	}

//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
	if (caps & FI_MSG || caps & FI_TAGGED) {
//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
		caps |= FI_SEND;
	}

//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
	if (caps & FI_RMA || caps & FI_ATOMIC) {
//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
		caps |= FI_READ;
		caps |= FI_WRITE;
	}

//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
	if (ofi_recv_allowed(caps) || ofi_rma_target_allowed(caps)) {
//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
		FI_LOG(fi_opa1x_global.prov, FI_LOG_DEBUG, FI_LOG_DOMAIN,
				"RX capabilities specified for TX context\n");
		errno = FI_EINVAL;
		return -errno;
	}

//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
	if (!ofi_send_allowed(caps) && !ofi_rma_initiate_allowed(caps)) {
//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
		FI_LOG(fi_opa1x_global.prov, FI_LOG_DEBUG, FI_LOG_DOMAIN,
				"TX capabilities not specified for TX context\n");
		errno = FI_EINVAL;
		return -errno;
	}

//fprintf(stderr, "%s:%s():%d opa1x_sep->domain->tx_count = %u\n", __FILE__, __func__, __LINE__, opa1x_sep->domain->tx_count);
	if (opa1x_sep->domain->tx_count >= fi_opa1x_domain_get_tx_max((struct fid_domain *)opa1x_sep->domain)) {
//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
		FI_LOG(fi_opa1x_global.prov, FI_LOG_DEBUG, FI_LOG_DOMAIN,
				"TX ctx count exceeded (max %u, created %u)\n",
				fi_opa1x_domain_get_tx_max((struct fid_domain *)opa1x_sep->domain), opa1x_sep->domain->tx_count);
		errno = FI_EINVAL;
		return -errno;
	}
//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);

	info.caps = caps;
	info.mode = attr->mode;

	info.tx_attr = &tx_attr;
	memcpy(info.tx_attr, attr, sizeof(*info.tx_attr));

//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
	info.ep_attr = &ep_attr;
	memcpy(info.ep_attr, opa1x_sep->info->ep_attr, sizeof(*info.ep_attr));
//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);

	info.domain_attr = &dom_attr;
	memcpy(info.domain_attr, opa1x_sep->info->domain_attr, sizeof(*info.domain_attr));
//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);

	info.fabric_attr = &fab_attr;
	memcpy(info.fabric_attr, opa1x_sep->info->fabric_attr, sizeof(*info.fabric_attr));

//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
//fprintf(stderr, "%s:%s():%d info.caps = 0x%016lx\n", __FILE__, __func__, __LINE__, info.caps);
	ret = fi_opa1x_endpoint_rx_tx((struct fid_domain *)opa1x_sep->domain,
		&info, tx_ep, context, opa1x_sep->cx);
	if (ret) {
//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
		goto err;
	}
//fprintf(stderr, "%s:%s():%d info.caps = 0x%016lx\n", __FILE__, __func__, __LINE__, info.caps);

	opa1x_tx_ep = container_of(*tx_ep, struct fi_opa1x_ep, ep_fid);
	opa1x_tx_ep->ep_fid.fid.fclass = FI_CLASS_TX_CTX;
//fprintf(stderr, "%s:%s():%d opa1x_tx_ep->tx.caps = 0x%016lx\n", __FILE__, __func__, __LINE__, opa1x_tx_ep->tx.caps);
//fprintf(stderr, "%s:%s():%d opa1x_tx_ep->rx.caps = 0x%016lx\n", __FILE__, __func__, __LINE__, opa1x_tx_ep->rx.caps);

	opa1x_tx_ep->av = opa1x_sep->av;
	fi_opa1x_ref_inc(&opa1x_tx_ep->av->ref_cnt, "address vector");

	opa1x_tx_ep->sep = container_of(sep, struct fi_opa1x_sep, ep_fid);

//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
	++ opa1x_sep->domain->tx_count;

	fi_opa1x_ref_inc(&opa1x_sep->ref_cnt, "scalable endpoint");

	attr->caps = caps;

//fprintf(stderr, "%s:%s():%d attr->caps = 0x%016lx\n", __FILE__, __func__, __LINE__, attr->caps);
	return 0;

err:
//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
	return -errno;
}

static int fi_opa1x_rx_ctx(struct fid_ep *sep, int index,
			struct fi_rx_attr *attr, struct fid_ep **rx_ep,
			void *context)
{
	//int ret;
	struct fi_info info = {0};
	struct fi_opa1x_sep *opa1x_sep;
	struct fi_opa1x_ep  *opa1x_rx_ep;

	if (!sep || !attr || !rx_ep || (index < 0) || (index >= FI_OPA1X_ADDR_SEP_RX_MAX)) {
		errno = FI_EINVAL;
		return -errno;
	}

	opa1x_sep = container_of(sep, struct fi_opa1x_sep, ep_fid);

	uint64_t caps = attr->caps;	/* TODO - "By default, a receive context inherits the properties of its associated endpoint. However, applications may request context specific attributes through the attr parameter." */

	if ((caps & FI_MSG || caps & FI_TAGGED) && (caps & FI_SEND)) {
		FI_LOG(fi_opa1x_global.prov, FI_LOG_DEBUG, FI_LOG_DOMAIN,
				"FI_MSG|FI_TAGGED with FI_SEND capability specified for a RX context\n");
		caps &= ~FI_SEND;
	}

	if ((caps & FI_RMA || caps & FI_ATOMIC) && (caps & FI_READ || caps & FI_WRITE)) {
		FI_LOG(fi_opa1x_global.prov, FI_LOG_DEBUG, FI_LOG_DOMAIN,
				"FI_RMA|FI_ATOMIC with FI_READ|FI_WRITE capability specified for a RX context\n");
		caps &= ~FI_READ;
		caps &= ~FI_WRITE;
	}

	if (caps & FI_MSG || caps & FI_TAGGED) {
		caps |= FI_RECV;
	}

	if (caps & FI_RMA || caps & FI_ATOMIC) {
		caps |= FI_REMOTE_READ;
		caps |= FI_REMOTE_WRITE;
	}

	if (ofi_send_allowed(caps) || ofi_rma_initiate_allowed(caps)) {
		FI_LOG(fi_opa1x_global.prov, FI_LOG_DEBUG, FI_LOG_DOMAIN,
				"TX capabilities specified for RX context\n");
		errno = FI_EINVAL;
		return -errno;
	}

	if (!ofi_recv_allowed(caps) && !ofi_rma_target_allowed(caps)) {
		FI_LOG(fi_opa1x_global.prov, FI_LOG_DEBUG, FI_LOG_DOMAIN,
				"RX capabilities not specified for RX context\n");
		errno = FI_EINVAL;
		return -errno;
	}

	if (opa1x_sep->domain->rx_count >= fi_opa1x_domain_get_rx_max((struct fid_domain *)opa1x_sep->domain)) {
		FI_LOG(fi_opa1x_global.prov, FI_LOG_DEBUG, FI_LOG_DOMAIN,
				"RX ctx count exceeded (max %u, created %u)\n",
				fi_opa1x_domain_get_rx_max((struct fid_domain *)opa1x_sep->domain), opa1x_sep->domain->rx_count);
		errno = FI_EINVAL;
		return -errno;
	}

	info.caps = caps;
	info.mode = attr->mode;

	info.rx_attr = calloc(1, sizeof(*info.rx_attr));
	if (!info.rx_attr) {
		errno = FI_ENOMEM;
		goto err;
	}

	info.rx_attr->caps     = caps;
	info.rx_attr->mode     = attr->mode;
	info.rx_attr->op_flags = attr->op_flags;
	info.rx_attr->msg_order = attr->msg_order;
	info.rx_attr->total_buffered_recv = attr->total_buffered_recv;
	info.rx_attr->iov_limit = attr->iov_limit;

	info.ep_attr = calloc(1, sizeof(*info.ep_attr));
	if (!info.ep_attr) {
		errno = FI_ENOMEM;
		goto err;
	}
	memcpy(info.ep_attr, opa1x_sep->info->ep_attr,
			sizeof(*info.ep_attr));

	info.domain_attr = calloc(1, sizeof(*info.domain_attr));
	if (!info.domain_attr) {
		errno = FI_ENOMEM;
		goto err;
	}
	memcpy(info.domain_attr, opa1x_sep->info->domain_attr,
			sizeof(*info.domain_attr));

	info.fabric_attr = calloc(1, sizeof(*info.fabric_attr));
	if (!info.fabric_attr) {
		errno = FI_ENOMEM;
		goto err;
	}
	memcpy(info.fabric_attr, opa1x_sep->info->fabric_attr,
			sizeof(*info.fabric_attr));

	opa1x_rx_ep = container_of(*rx_ep, struct fi_opa1x_ep, ep_fid);
	opa1x_rx_ep->ep_fid.fid.fclass = FI_CLASS_RX_CTX;

	opa1x_rx_ep->hfi = fi_opa1x_hfi1_context_open(opa1x_sep->domain->unique_job_key);

//	open_hfi1_context(opa1x_sep->domain->unique_job_key, &opa1x_rx_ep->rx.hfi);
	fi_opa1x_endpoint_rx_tx(&opa1x_sep->domain->domain_fid, &info,
		rx_ep, context, opa1x_sep->cx);

//	const int domain_rx_index = opa1x_sep->domain_hfi1_offset + index;
//	ret = fi_opa1x_endpoint_rx_tx(&opa1x_sep->domain->domain_fid, &info,
//			rx_ep, context, domain_rx_index, -1, opa1x_sep->cx);
//	if (ret) {
//		goto err;
//	}

//	opa1x_ep->rx.self.rx[index] = 
//	opa1x_domain->hfi1[domain_rx_index].info.rxe.id;

	//opa1x_sep->self.rx[index] = opa1x_ep->rx.self.rx[0];


	opa1x_sep->ep[index] = opa1x_rx_ep;


	/* check for non-contiguous rx ids */
	if (index > 0) {
		unsigned i;
		for (i=0; i<index; ++i) {
			if (opa1x_sep->ep[i]->rx.self.hfi1_rx + 1 != opa1x_sep->ep[i+1]->rx.self.hfi1_rx) {
				fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__); abort();
			}
		}
	}


	opa1x_rx_ep->sep = container_of(sep, struct fi_opa1x_sep, ep_fid);

	opa1x_rx_ep->av = opa1x_sep->av;
	fi_opa1x_ref_inc(&opa1x_rx_ep->av->ref_cnt, "address vector");

	++ opa1x_sep->domain->rx_count;

	fi_opa1x_ref_inc(&opa1x_sep->ref_cnt, "scalable endpoint");

	return 0;

err:
	if (info.fabric_attr)
		free(info.fabric_attr);
	if (info.domain_attr)
		free(info.domain_attr);
	if (info.ep_attr)
		free(info.ep_attr);
	if (info.tx_attr)
		free(info.tx_attr);
	return -errno;
}

static int fi_opa1x_bind_sep(struct fid *fid, struct fid *bfid,
		uint64_t flags)
{
	int ret = 0;
	struct fi_opa1x_sep *opa1x_sep = container_of(fid, struct fi_opa1x_sep, ep_fid);
	struct fi_opa1x_av *opa1x_av;

	if (!fid || !bfid) {
		errno = FI_EINVAL;
		return -errno;
	}

	switch (bfid->fclass) {
	case FI_CLASS_AV:
		opa1x_av = container_of(bfid, struct fi_opa1x_av, av_fid);
		fi_opa1x_ref_inc(&opa1x_av->ref_cnt, "address vector");
		opa1x_sep->av = opa1x_av;
		break;
	default:
		errno = FI_ENOSYS;
		return -errno;
	}

	return ret;
}

static struct fi_ops fi_opa1x_fi_ops = {
	.size		= sizeof(struct fi_ops),
	.close		= fi_opa1x_close_sep,
	.bind		= fi_opa1x_bind_sep,
	.control	= fi_opa1x_control_sep,
	.ops_open	= fi_no_ops_open
};

static struct fi_ops_ep fi_opa1x_sep_ops = {
	.size		= sizeof(struct fi_ops_ep),
	.cancel		= fi_no_cancel,
	.getopt		= fi_no_getopt,
	.setopt		= fi_no_setopt,
	.tx_ctx		= fi_opa1x_tx_ctx,
	.rx_ctx		= fi_opa1x_rx_ctx,
	.rx_size_left   = fi_no_rx_size_left,
	.tx_size_left   = fi_no_tx_size_left
};

int fi_opa1x_scalable_ep (struct fid_domain *domain,
	struct fi_info *info,
	struct fid_ep **sep,
	void *context)
{
	struct fi_opa1x_sep *opa1x_sep = NULL;

	if (!info || !domain) {
		errno = FI_EINVAL;
		goto err;
	}

	void * memptr = NULL;
	memptr = malloc(sizeof(struct fi_opa1x_sep)+L2_CACHE_LINE_SIZE);
	if (!memptr) {
		errno = FI_ENOMEM;
		goto err;
	}
	memset(memptr, 0, sizeof(struct fi_opa1x_sep)+L2_CACHE_LINE_SIZE);
	opa1x_sep = (struct fi_opa1x_sep *)(((uintptr_t)memptr+L2_CACHE_LINE_SIZE) & ~(L2_CACHE_LINE_SIZE-1));
	opa1x_sep->memptr = memptr;
	memptr = NULL;

	opa1x_sep->domain = (struct fi_opa1x_domain *) domain;

	opa1x_sep->ep_fid.fid.fclass	= FI_CLASS_SEP;
	opa1x_sep->ep_fid.fid.context	= context;
	opa1x_sep->ep_fid.fid.ops		= &fi_opa1x_fi_ops;
	opa1x_sep->ep_fid.ops		= &fi_opa1x_sep_ops;

	opa1x_sep->info = calloc(1, sizeof (struct fi_info));
	if (!opa1x_sep->info) {
		errno = FI_ENOMEM;
		goto err;
	}
	memcpy(opa1x_sep->info, info, sizeof (struct fi_info));
	opa1x_sep->info->next = NULL;
	opa1x_sep->info->ep_attr = calloc(1, sizeof(struct fi_ep_attr));
	if (!opa1x_sep->info->ep_attr) {
		errno = FI_ENOMEM;
		goto err;
	}
	memcpy(opa1x_sep->info->ep_attr, info->ep_attr, sizeof(struct fi_ep_attr));

	/*
	 * fi_endpoint.3
	 *
	 * "tx_ctx_cnt - Transmit Context Count
	 * 	Number of transmit contexts to associate with the endpoint. If
	 * 	not specified (0), 1 context will be assigned if the endpoint
	 * 	supports outbound transfers."
	 */
	if (0 == opa1x_sep->info->ep_attr->tx_ctx_cnt) {
		opa1x_sep->info->ep_attr->tx_ctx_cnt = 1;
	}

	/*
	 * fi_endpoint.3
	 *
	 * "rx_ctx_cnt - Receive Context Count
	 * 	Number of receive contexts to associate with the endpoint. If
	 * 	not specified, 1 context will be assigned if the endpoint
	 * 	supports inbound transfers."
	 */
	if (0 == opa1x_sep->info->ep_attr->rx_ctx_cnt) {
		opa1x_sep->info->ep_attr->rx_ctx_cnt = 1;
	}

	opa1x_sep->cx = (uint8_t)fi_opa1x_compiler_fetch_and_inc_u64(&opa1x_sep->domain->node->ep_count);

	unsigned i;
	for (i=0; i<FI_OPA1X_ADDR_SEP_RX_MAX; ++i) {
		opa1x_sep->ep[i] = NULL;
	}

//fprintf(stderr, "%s:%s():%d =========== BEFORE fi_opa1x_init_cm_ops; opa1x_sep = %p, info = %p\n", __FILE__, __func__, __LINE__, opa1x_sep, info);
	//ret = fi_opa1x_init_cm_ops(opa1x_sep, info);
	int ret = fi_opa1x_init_cm_ops(&opa1x_sep->ep_fid.fid, info);
	if (ret)
		goto err;

	fi_opa1x_ref_init(&opa1x_sep->ref_cnt, "scalable endpoint");
	fi_opa1x_ref_inc(&opa1x_sep->domain->ref_cnt, "domain");

	*sep = &opa1x_sep->ep_fid;

	return 0;
err:
	if (opa1x_sep) {
		fi_opa1x_finalize_cm_ops(&opa1x_sep->ep_fid.fid);
		if (opa1x_sep->info) {
			if (opa1x_sep->info->ep_attr)
				free(opa1x_sep->info->ep_attr);
			free(opa1x_sep->info);
		}
		memptr = opa1x_sep->memptr;
		free(memptr);
	}
	return -errno;
}
