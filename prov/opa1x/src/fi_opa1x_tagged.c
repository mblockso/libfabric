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
#include "rdma/opa1x/fi_opa1x_domain.h"
#include "rdma/opa1x/fi_opa1x_endpoint.h"
#include "rdma/opa1x/fi_opa1x_tagged.h"

#include <fi_enosys.h>

/*
 * \note The opa1x provider asserts the following mode bits which affect
 * 	the behavior of this routine:
 *
 * 	- 'FI_ASYNC_IOV' mode bit which requires the application to maintain
 * 	  the 'msg->msg_iov' iovec array until the operation completes
 *
 * 	- 'FI_LOCAL_MR' mode bit which allows the provider to ignore the 'desc'
 * 	  parameter .. no memory regions are required to access the local
 * 	  memory
 */
ssize_t fi_opa1x_trecvmsg_generic (struct fid_ep *ep,
		const struct fi_msg_tagged *msg,
		uint64_t flags,
		const int lock_required,
		const enum fi_av_type av_type)
{
	struct fi_opa1x_ep * opa1x_ep = container_of(ep, struct fi_opa1x_ep, ep_fid);
	union fi_opa1x_context * opa1x_context = NULL;

	if (msg->iov_count == 0) {
		assert(msg->context);
		assert(((uintptr_t)msg->context & 0x07ull) == 0);	/* must be 8 byte aligned */

		opa1x_context = (union fi_opa1x_context *) msg->context;
		opa1x_context->next = NULL;
		opa1x_context->src_addr = msg->addr;
		opa1x_context->flags = flags;
		opa1x_context->len = 0;
		opa1x_context->buf = NULL;
		opa1x_context->byte_counter = (uint64_t)-1;
		if ((flags & (FI_PEEK | FI_CLAIM)) != FI_CLAIM) {
			/* do not overwrite state from a previous "peek|claim" operation */
			opa1x_context->tag = msg->tag;
			opa1x_context->ignore = msg->ignore;
		}

	} else if (msg->iov_count == 1) {
		assert(msg->context);
		assert(((uintptr_t)msg->context & 0x07ull) == 0);	/* must be 8 byte aligned */

		opa1x_context = (union fi_opa1x_context *) msg->context;
		opa1x_context->next = NULL;
		opa1x_context->src_addr = msg->addr;
		opa1x_context->flags = flags;
		opa1x_context->len = msg->msg_iov[0].iov_len;
		opa1x_context->buf = msg->msg_iov[0].iov_base;
		opa1x_context->byte_counter = (uint64_t)-1;
		if ((flags & (FI_PEEK | FI_CLAIM)) != FI_CLAIM) {
			/* do not overwrite state from a previous "peek|claim" operation */
			opa1x_context->tag = msg->tag;
			opa1x_context->ignore = msg->ignore;
		}

	} else {
		assert((flags & (FI_PEEK | FI_CLAIM)) != FI_CLAIM);	/* TODO - why not? */

		struct fi_opa1x_context_ext * ext = NULL;
		posix_memalign((void**)&ext, 32, sizeof(struct fi_opa1x_context_ext));
		flags |= FI_OPA1X_CQ_CONTEXT_EXT;

		ext->opa1x_context.next = NULL;
		ext->opa1x_context.src_addr = msg->addr;
		ext->opa1x_context.flags = flags;
		ext->opa1x_context.byte_counter = (uint64_t)-1;
		ext->opa1x_context.tag = msg->tag;
		ext->opa1x_context.ignore = msg->ignore;
		ext->msg.op_context = msg->context;
		ext->msg.iov_count = msg->iov_count;
		ext->msg.iov = (struct iovec *)msg->msg_iov;

		if (IS_PROGRESS_MANUAL(opa1x_ep->domain)) {

			fi_opa1x_ep_rx_process_context(opa1x_ep,
				FI_OPA1X_EP_RX_QUEUE_KIND_TAG,
				0,	/* cancel_context */
				(union fi_opa1x_context *) ext,
				flags,
				1,	/* is_context_ext */
				lock_required,
				av_type);

			return 0;
		}
	}

	if (IS_PROGRESS_MANUAL(opa1x_ep->domain)) {

		fi_opa1x_ep_rx_process_context(opa1x_ep,
			FI_OPA1X_EP_RX_QUEUE_KIND_TAG,
			0,	/* cancel_context */
			opa1x_context,
			flags,
			0,	/* is_context_ext */
			lock_required,
			av_type);

	} else {
		abort();
#if 0
		/* the *only* difference between a 'tagged' and 'non-tagged' recv is
		 * the L2 atomic fifo used to post the receive information */
		struct l2atomic_fifo_producer * fifo = &opa1x_ep->rx.post.match[0];	/* TODO - use enum */

		while (l2atomic_fifo_produce(fifo, context_rsh3b) != 0);		/* spin loop! */
#endif
	}

	return 0;
}






























ssize_t fi_opa1x_trecvmsg(struct fid_ep *ep,
		const struct fi_msg_tagged *msg, uint64_t flags)
{
	FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_EP_DATA, "\n");

	struct fi_opa1x_ep * opa1x_ep = container_of(ep, struct fi_opa1x_ep, ep_fid);
	const enum fi_threading threading = opa1x_ep->domain->threading;
	const int lock_required =
		(threading == FI_THREAD_FID) ||
		(threading == FI_THREAD_UNSPEC) ||
		(threading == FI_THREAD_SAFE);
	const enum fi_av_type av_type = opa1x_ep->av_type;

	return fi_opa1x_trecvmsg_generic(ep, msg, flags, lock_required, av_type);
}

ssize_t fi_opa1x_tsendmsg(struct fid_ep *ep,
		const struct fi_msg_tagged *msg, uint64_t flags)
{
	FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_EP_DATA, "\n");

	struct fi_opa1x_ep * opa1x_ep = container_of(ep, struct fi_opa1x_ep, ep_fid);
	const enum fi_threading threading = opa1x_ep->threading;
	const enum fi_av_type av_type = opa1x_ep->av_type;

	const size_t niov = msg->iov_count;

	if (niov > 1) {

		/* pack !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! */
		unsigned i;
		size_t tbytes = 0;
#ifndef NDEBUG
		for (i=0; i<niov; ++i)
			tbytes += msg->msg_iov[i].iov_len;

		if (tbytes > FI_OPA1X_MAX_MSG_SIZE) {
			fprintf(stderr, "%s:%s():%d total bytes too big!\n", __FILE__, __func__, __LINE__);
			abort();
		}

		tbytes = 0;
#endif

fprintf(stderr, "%s:%s():%d FI_OPA1X_MAX_MSG_SIZE too big! (%lu)\n", __FILE__, __func__, __LINE__, FI_OPA1X_MAX_MSG_SIZE); abort();
//		uint8_t data[FI_OPA1X_MAX_MSG_SIZE];
		uint8_t data[8192];
		uint8_t *dst_ptr = data;

		for (i=0; i<niov; ++i) {

			const size_t bytes = msg->msg_iov[i].iov_len;
			memcpy((void *)dst_ptr, (void *)msg->msg_iov[i].iov_base, bytes);
			dst_ptr += bytes;
			tbytes += bytes;
		}


		return fi_opa1x_ep_tx_send(ep, data, tbytes,
			msg->desc, msg->addr, msg->tag, msg->context, msg->data,
			(threading != FI_THREAD_ENDPOINT && threading != FI_THREAD_DOMAIN),
			av_type,
			0	/* is_msg */,
			1	/* is_contiguous */,
			1	/* override flags */,
			flags);
	} else {

		return fi_opa1x_ep_tx_send(ep, msg->msg_iov[0].iov_base, msg->msg_iov[0].iov_len,
			msg->desc, msg->addr, msg->tag, msg->context, msg->data,
			(threading != FI_THREAD_ENDPOINT && threading != FI_THREAD_DOMAIN),
			av_type,
			0	/* is_msg */,
			1	/* is_contiguous */,
			1	/* override flags */,
			flags);
	}
}

FI_OPA1X_TAGGED_SPECIALIZED_FUNC(0,FI_AV_MAP)
FI_OPA1X_TAGGED_SPECIALIZED_FUNC(1,FI_AV_MAP)
FI_OPA1X_TAGGED_SPECIALIZED_FUNC(0,FI_AV_TABLE)
FI_OPA1X_TAGGED_SPECIALIZED_FUNC(1,FI_AV_TABLE)

#define FI_OPA1X_TAGGED_OPS_STRUCT_NAME(LOCK,AV)				\
	fi_opa1x_ops_tagged_ ## LOCK ## _ ## AV

#define FI_OPA1X_TAGGED_OPS_STRUCT(LOCK,AV)					\
static struct fi_ops_tagged							\
	FI_OPA1X_TAGGED_OPS_STRUCT_NAME(LOCK,AV) __attribute__ ((unused)) = {	\
	.size		= sizeof(struct fi_ops_tagged),				\
	.recv		=							\
		FI_OPA1X_TAGGED_SPECIALIZED_FUNC_NAME(trecv, LOCK, AV),		\
	.recvv		= fi_no_tagged_recvv,					\
	.recvmsg	= fi_opa1x_trecvmsg,					\
	.send		=							\
		FI_OPA1X_TAGGED_SPECIALIZED_FUNC_NAME(tsend, LOCK, AV),		\
	.sendv		= fi_no_tagged_sendv,					\
	.sendmsg	= fi_opa1x_tsendmsg,					\
	.inject 	=							\
		FI_OPA1X_TAGGED_SPECIALIZED_FUNC_NAME(tinject, LOCK, AV),	\
	.senddata	=							\
		FI_OPA1X_TAGGED_SPECIALIZED_FUNC_NAME(tsenddata, LOCK, AV),	\
	.injectdata	=							\
		FI_OPA1X_TAGGED_SPECIALIZED_FUNC_NAME(tinjectdata, LOCK, AV),	\
}

FI_OPA1X_TAGGED_OPS_STRUCT(0,FI_AV_MAP);
FI_OPA1X_TAGGED_OPS_STRUCT(1,FI_AV_MAP);
FI_OPA1X_TAGGED_OPS_STRUCT(0,FI_AV_TABLE);
FI_OPA1X_TAGGED_OPS_STRUCT(1,FI_AV_TABLE);

ssize_t fi_opa1x_tsearch(struct fid_ep *ep, uint64_t *tag,
		uint64_t ignore, uint64_t flags,
		fi_addr_t *src_addr, size_t *len, void *context)
{
	errno = FI_ENOSYS;
	return -errno;
}

static struct fi_ops_tagged fi_opa1x_no_tagged_ops = {
        .size           = sizeof(struct fi_ops_tagged),
        .recv           = fi_no_tagged_recv,
        .recvv          = fi_no_tagged_recvv,
        .recvmsg        = fi_no_tagged_recvmsg,
        .send           = fi_no_tagged_send,
        .sendv          = fi_no_tagged_sendv,
        .sendmsg        = fi_no_tagged_sendmsg,
        .inject         = fi_no_tagged_inject,
        .senddata       = fi_no_tagged_senddata,
        .injectdata     = fi_no_tagged_injectdata
};

int fi_opa1x_init_tagged_ops(struct fid_ep *ep, struct fi_info *info)
{
        if (!info || !ep)
                goto err;

        if (info->caps & FI_TAGGED ||
                        (info->tx_attr &&
                         (info->tx_attr->caps & FI_TAGGED))) {
        }

        return 0;

err:
        errno = FI_EINVAL;
        return -errno;
}

int fi_opa1x_enable_tagged_ops(struct fid_ep *ep)
{
	struct fi_opa1x_ep * opa1x_ep =
		container_of(ep, struct fi_opa1x_ep, ep_fid);

        if (!opa1x_ep || !opa1x_ep->domain)
                goto err;

        if (!(opa1x_ep->tx.caps & FI_TAGGED)) {
                /* Tagged ops not enabled on this endpoint */
                opa1x_ep->ep_fid.tagged =
                        &fi_opa1x_no_tagged_ops;
                return 0;
        }

        switch (opa1x_ep->domain->threading) {
        case FI_THREAD_ENDPOINT:
        case FI_THREAD_DOMAIN:
        case FI_THREAD_COMPLETION:
		if (opa1x_ep->av->type == FI_AV_TABLE) {
			opa1x_ep->ep_fid.tagged = &FI_OPA1X_TAGGED_OPS_STRUCT_NAME(0,FI_AV_TABLE);
		} else {
			opa1x_ep->ep_fid.tagged = &FI_OPA1X_TAGGED_OPS_STRUCT_NAME(0,FI_AV_MAP);
		}
                break;
        case FI_THREAD_FID:
        case FI_THREAD_UNSPEC:
        case FI_THREAD_SAFE:
		if (opa1x_ep->av->type == FI_AV_TABLE) {
			opa1x_ep->ep_fid.tagged = &FI_OPA1X_TAGGED_OPS_STRUCT_NAME(1,FI_AV_TABLE);
		} else {
			opa1x_ep->ep_fid.tagged = &FI_OPA1X_TAGGED_OPS_STRUCT_NAME(1,FI_AV_MAP);
		}
                break;
        default:
                opa1x_ep->ep_fid.tagged = &fi_opa1x_no_tagged_ops;
                FI_WARN(fi_opa1x_global.prov, FI_LOG_EP_DATA,
                                "Tagged ops not enabled on EP\n");
		break;
        }

	return 0;
err:
        errno = FI_EINVAL;
        return -errno;
}

int fi_opa1x_finalize_tagged_ops(struct fid_ep *ep)
{
	if (!ep) {
		return 0;
	}

	return 0;
}


#define FABRIC_DIRECT_LOCK	0

ssize_t
fi_opa1x_tinject_FABRIC_DIRECT(struct fid_ep *ep, const void *buf, size_t len,
		fi_addr_t dest_addr, uint64_t tag)
{
	FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_EP_DATA, "\n");

	return FI_OPA1X_TAGGED_SPECIALIZED_FUNC_NAME(tinject,
			FABRIC_DIRECT_LOCK,
			FABRIC_DIRECT_AV)
				(ep, buf, len, dest_addr, tag);
}

ssize_t
fi_opa1x_tsend_FABRIC_DIRECT(struct fid_ep *ep, const void *buf, size_t len,
		void *desc, fi_addr_t dest_addr, uint64_t tag, void *context)
{
	FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_EP_DATA, "\n");

	return FI_OPA1X_TAGGED_SPECIALIZED_FUNC_NAME(tsend,
			FABRIC_DIRECT_LOCK,
			FABRIC_DIRECT_AV)
				(ep, buf, len, desc, dest_addr, tag, context);
}

ssize_t
fi_opa1x_tinjectdata_FABRIC_DIRECT(struct fid_ep *ep, const void *buf, size_t len,
		uint64_t data, fi_addr_t dest_addr, uint64_t tag)
{
	FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_EP_DATA, "\n");

	return FI_OPA1X_TAGGED_SPECIALIZED_FUNC_NAME(tinjectdata,
			FABRIC_DIRECT_LOCK,
			FABRIC_DIRECT_AV)
				(ep, buf, len, data, dest_addr, tag);
}

ssize_t
fi_opa1x_tsenddata_FABRIC_DIRECT(struct fid_ep *ep, const void *buf, size_t len,
		void *desc, uint64_t data, fi_addr_t dest_addr, uint64_t tag, void *context)
{
	FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_EP_DATA, "\n");

	return FI_OPA1X_TAGGED_SPECIALIZED_FUNC_NAME(tsenddata,
			FABRIC_DIRECT_LOCK,
			FABRIC_DIRECT_AV)
				(ep, buf, len, desc, data, dest_addr, tag, context);
}

ssize_t
fi_opa1x_trecv_FABRIC_DIRECT(struct fid_ep *ep, void *buf, size_t len,
		void *desc, fi_addr_t src_addr, uint64_t tag, uint64_t ignore, void *context)
{
	FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_EP_DATA, "\n");

	return FI_OPA1X_TAGGED_SPECIALIZED_FUNC_NAME(trecv,
			FABRIC_DIRECT_LOCK,
			FABRIC_DIRECT_AV)
				(ep, buf, len, desc, src_addr, tag, ignore, context);
}
