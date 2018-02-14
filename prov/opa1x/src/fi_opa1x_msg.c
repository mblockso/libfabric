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

#include "rdma/opa1x/fi_opa1x_domain.h"
#include "rdma/opa1x/fi_opa1x_endpoint.h"
#include "rdma/opa1x/fi_opa1x.h"
#include "rdma/opa1x/fi_opa1x_internal.h"

#include <ofi_enosys.h>

ssize_t fi_opa1x_sendmsg(struct fid_ep *ep, const struct fi_msg *msg,
			uint64_t flags)
{
	FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_EP_DATA, "\n");

	struct fi_opa1x_ep * opa1x_ep = container_of(ep, struct fi_opa1x_ep, ep_fid);
	const enum fi_threading threading = opa1x_ep->threading;
	const enum fi_av_type av_type = opa1x_ep->av_type;

	return fi_opa1x_ep_tx_send(ep, msg->msg_iov, msg->iov_count,
		msg->desc, msg->addr, 0, msg->context, msg->data,
		(threading != FI_THREAD_ENDPOINT && threading != FI_THREAD_DOMAIN),	/* "lock required"? */
		av_type,
		1	/* is_msg */,
		0	/* is_contiguous */,
		1	/* override the default tx flags */,
		flags);
}

ssize_t fi_opa1x_sendv(struct fid_ep *ep, const struct iovec *iov,
			void **desc, size_t count, fi_addr_t dest_addr,
			void *context)
{
	FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_EP_DATA, "\n");

	struct fi_opa1x_ep * opa1x_ep = container_of(ep, struct fi_opa1x_ep, ep_fid);
	const enum fi_threading threading = opa1x_ep->threading;
	const enum fi_av_type av_type = opa1x_ep->av_type;

	return fi_opa1x_ep_tx_send(ep, iov, count,
		desc, dest_addr, 0, context, 0,
		(threading != FI_THREAD_ENDPOINT && threading != FI_THREAD_DOMAIN),	/* "lock required"? */
		av_type,
		1	/* is_msg */,
		0	/* is_contiguous */,
		0	/* do not override flags */,
		0);	/* flags */
}


ssize_t fi_opa1x_senddata(struct fid_ep *ep, const void *buf, size_t len, void *desc,
			uint64_t data, void *context)
{
	FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_EP_DATA, "\n");

	errno = FI_ENOSYS;
	return -errno;
}

FI_OPA1X_MSG_SPECIALIZED_FUNC(0,FI_AV_TABLE)
FI_OPA1X_MSG_SPECIALIZED_FUNC(1,FI_AV_TABLE)
FI_OPA1X_MSG_SPECIALIZED_FUNC(0,FI_AV_MAP)
FI_OPA1X_MSG_SPECIALIZED_FUNC(1,FI_AV_MAP)

#define FI_OPA1X_MSG_OPS_STRUCT_NAME(LOCK, AV)					\
	fi_opa1x_ops_msg_ ## LOCK ## _ ## AV

#define FI_OPA1X_MSG_OPS_STRUCT(LOCK,AV)					\
static struct fi_ops_msg							\
	FI_OPA1X_MSG_OPS_STRUCT_NAME(LOCK,AV) __attribute__ ((unused)) = {	\
	.size		= sizeof(struct fi_ops_msg),				\
	.recv		=							\
		FI_OPA1X_MSG_SPECIALIZED_FUNC_NAME(recv, LOCK, AV),		\
	.recvv		= fi_no_msg_recvv,					\
	.recvmsg	=							\
		FI_OPA1X_MSG_SPECIALIZED_FUNC_NAME(recvmsg, LOCK, AV),		\
	.send		=							\
		FI_OPA1X_MSG_SPECIALIZED_FUNC_NAME(send, LOCK, AV),		\
	.sendv		= fi_opa1x_sendv,					\
	.sendmsg	= fi_opa1x_sendmsg,					\
	.inject		=							\
		FI_OPA1X_MSG_SPECIALIZED_FUNC_NAME(inject, LOCK, AV),		\
	.senddata	=							\
		FI_OPA1X_MSG_SPECIALIZED_FUNC_NAME(senddata, LOCK, AV),		\
	.injectdata	=							\
		FI_OPA1X_MSG_SPECIALIZED_FUNC_NAME(injectdata, LOCK, AV),	\
}

FI_OPA1X_MSG_OPS_STRUCT(0,FI_AV_TABLE);
FI_OPA1X_MSG_OPS_STRUCT(1,FI_AV_TABLE);
FI_OPA1X_MSG_OPS_STRUCT(0,FI_AV_MAP);
FI_OPA1X_MSG_OPS_STRUCT(1,FI_AV_MAP);

static struct fi_ops_msg fi_opa1x_no_msg_ops = {
	.size		= sizeof(struct fi_ops_msg),
	.recv		= fi_no_msg_recv,
	.recvv		= fi_no_msg_recvv,
	.recvmsg	= fi_no_msg_recvmsg,
	.send		= fi_no_msg_send,
	.sendv		= fi_no_msg_sendv,
	.sendmsg	= fi_no_msg_sendmsg,
	.inject		= fi_no_msg_inject,
	.senddata	= fi_no_msg_senddata,
	.injectdata	= fi_no_msg_injectdata
};

int fi_opa1x_init_msg_ops(struct fid_ep *ep, struct fi_info *info)
{
	struct fi_opa1x_ep * opa1x_ep = container_of(ep, struct fi_opa1x_ep, ep_fid);

	if (!info || !opa1x_ep) {
		errno = FI_EINVAL;
		goto err;
	}
	if (info->caps & FI_MSG ||
			(info->tx_attr &&
			 (info->tx_attr->caps & FI_MSG))) {

		opa1x_ep->rx.min_multi_recv = sizeof(union fi_opa1x_hfi1_packet_payload);
	}

	return 0;

err:
	return -errno;
}

int fi_opa1x_enable_msg_ops(struct fid_ep *ep)
{
	struct fi_opa1x_ep * opa1x_ep = container_of(ep, struct fi_opa1x_ep, ep_fid);

	if (!opa1x_ep || !opa1x_ep->domain)
		return -FI_EINVAL;

	if (!(opa1x_ep->tx.caps & FI_MSG)) {
		/* Messaging ops not enabled on this endpoint */
		opa1x_ep->ep_fid.msg =
			&fi_opa1x_no_msg_ops;
		return 0;
	}


	switch (opa1x_ep->domain->threading) {
	case FI_THREAD_ENDPOINT:
	case FI_THREAD_DOMAIN:
	case FI_THREAD_COMPLETION:
		if (opa1x_ep->av->type == FI_AV_TABLE) {
			opa1x_ep->ep_fid.msg = &FI_OPA1X_MSG_OPS_STRUCT_NAME(0,FI_AV_TABLE);
		} else {
			opa1x_ep->ep_fid.msg = &FI_OPA1X_MSG_OPS_STRUCT_NAME(0,FI_AV_MAP);
		}
		break;
	case FI_THREAD_FID:
	case FI_THREAD_UNSPEC:
	case FI_THREAD_SAFE:
		if (opa1x_ep->av->type == FI_AV_TABLE) {
			opa1x_ep->ep_fid.msg = &FI_OPA1X_MSG_OPS_STRUCT_NAME(1,FI_AV_TABLE);
		} else {
			opa1x_ep->ep_fid.msg = &FI_OPA1X_MSG_OPS_STRUCT_NAME(1,FI_AV_MAP);
		}
		break;
	default:
		return -FI_EINVAL;
	}

	return 0;
}

int fi_opa1x_finalize_msg_ops(struct fid_ep *ep)
{
	return 0;
}
