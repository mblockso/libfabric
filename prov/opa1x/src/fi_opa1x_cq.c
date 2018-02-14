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
#include "rdma/opa1x/fi_opa1x_eq.h"
#include "rdma/opa1x/fi_opa1x.h"

#include <ofi_enosys.h>
#include <stdlib.h>

#define FI_OPA1X_DEFAULT_CQ_DEPTH (8192)
#define FI_OPA1X_MAXIMUM_CQ_DEPTH (8192)


void fi_opa1x_cq_debug(struct fi_opa1x_cq *opa1x_cq, const int line) {

	char str[2048];
	char *s = str;
	size_t len = 2047;
	int n = 0;
	union fi_opa1x_context * context = NULL;;

	n = snprintf(s, len, "%s():%d [%p] completed(%p,%p)", __func__, line, opa1x_cq, opa1x_cq->completed.head, opa1x_cq->completed.tail);
	s += n;
	len -= n;

	if (opa1x_cq->completed.head != NULL) {
		context = opa1x_cq->completed.head;
		n = snprintf(s, len, " = { %p", context); s += n; len -= n;

		context = context->next;
		while (context != NULL) {
			n = snprintf(s, len, ", %p", context); s += n; len += n;
			context = context->next;
		}
		n = snprintf(s, len, " }"); s += n; len -= n;
	}
	fprintf(stderr, "%s\n", str);

	n = 0; len = 2047; s = str; *s = 0;
	n = snprintf(s, len, "%s():%d [%p] pending(%p,%p)", __func__, line, opa1x_cq, opa1x_cq->pending.head, opa1x_cq->pending.tail); s += n; len -= n;
	if (opa1x_cq->pending.head != NULL) {
		context = opa1x_cq->pending.head;
		n = snprintf(s, len, " = { %p(%lu)", context, context->byte_counter); s += n; len -= n;

		context = context->next;
		while (context != NULL) {
			n = snprintf(s, len, ", %p(%lu)", context, context->byte_counter); s += n; len += n;
			context = context->next;
		}
		n = snprintf(s, len, " }"); s += n; len -= n;
	}

	fprintf(stderr, "%s\n", str);

	n = 0; len = 2047; s = str; *s = 0;
	n = snprintf(s, len, "%s():%d [%p] err(%p,%p)", __func__, line, opa1x_cq, opa1x_cq->err.head, opa1x_cq->err.tail); s += n; len -= n;
	if (opa1x_cq->err.head != NULL) {
		context = opa1x_cq->err.head;
		n = snprintf(s, len, " = { %p(%lu)", context, context->byte_counter); s += n; len -= n;

		context = context->next;
		while (context != NULL) {
			n = snprintf(s, len, ", %p(%lu)", context, context->byte_counter); s += n; len += n;
			context = context->next;
		}
		n = snprintf(s, len, " }"); s += n; len -= n;
	}

	fprintf(stderr, "%s\n", str);
}



static int fi_opa1x_close_cq(fid_t fid)
{
	FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_CQ, "close cq\n");

	int ret;
	struct fi_opa1x_cq *opa1x_cq =
		container_of(fid, struct fi_opa1x_cq, cq_fid);

	ret = fi_opa1x_fid_check(fid, FI_CLASS_CQ, "completion queue");
	if (ret)
		return ret;

	ret = fi_opa1x_ref_dec(&opa1x_cq->domain->ref_cnt, "domain");
	if (ret)
		return ret;

	ret = fi_opa1x_ref_finalize(&opa1x_cq->ref_cnt, "completion queue");
	if (ret)
		return ret;

	free(opa1x_cq);

	FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_CQ, "cq closed\n");
	return 0;
}

static int fi_opa1x_bind_cq(struct fid *fid, struct fid *bfid,
		uint64_t flags)
{
	FI_WARN(fi_opa1x_global.prov, FI_LOG_CQ, "unimplemented\n");
	errno = FI_ENOSYS;
	return -errno;
}

static int fi_opa1x_control_cq(fid_t fid, int command, void *arg)
{
	FI_WARN(fi_opa1x_global.prov, FI_LOG_CQ, "unimplemented\n");
	errno = FI_ENOSYS;
	return -errno;
}

static int fi_opa1x_ops_open_cq(struct fid *fid, const char *name,
		uint64_t flags, void **ops, void *context)
{
	FI_WARN(fi_opa1x_global.prov, FI_LOG_CQ, "unimplemented\n");
	errno = FI_ENOSYS;
	return -errno;
}

static struct fi_ops fi_opa1x_fi_ops = {
	.size		= sizeof(struct fi_ops),
	.close		= fi_opa1x_close_cq,
	.bind		= fi_opa1x_bind_cq,
	.control	= fi_opa1x_control_cq,
	.ops_open	= fi_opa1x_ops_open_cq
};

static ssize_t fi_opa1x_cq_read(struct fid_cq *cq, void *buf, size_t count)
{
	FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_CQ, "(begin)\n");
	int lock_required;
	int ret;
	struct fi_opa1x_cq *opa1x_cq = container_of(cq, struct fi_opa1x_cq, cq_fid);

	switch (opa1x_cq->domain->threading) {
	case FI_THREAD_ENDPOINT:
	case FI_THREAD_DOMAIN:
		lock_required = 0;
	default:
		lock_required = 1;
	}

	ret = fi_opa1x_cq_read_generic(cq, buf, count, opa1x_cq->format, lock_required);

	FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_CQ, "(end)\n");
	return ret;
}

static ssize_t
fi_opa1x_cq_readfrom(struct fid_cq *cq, void *buf, size_t count, fi_addr_t *src_addr)
{
	FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_CQ, "(begin)\n");
	int lock_required;
	int ret;
	struct fi_opa1x_cq *opa1x_cq = container_of(cq, struct fi_opa1x_cq, cq_fid);

	switch (opa1x_cq->domain->threading) {
	case FI_THREAD_ENDPOINT:
	case FI_THREAD_DOMAIN:
		lock_required = 0;
		break;
	default:
		lock_required = 1;
		break;
	}

	ret = fi_opa1x_cq_readfrom_generic(cq, buf, count, src_addr, opa1x_cq->format, lock_required);
	if (ret > 0) {
		unsigned n;
		for (n=0; n<ret; ++n) src_addr[n] = FI_ADDR_NOTAVAIL;
	}

	FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_CQ, "(end)\n");
	return ret;
}

static ssize_t
fi_opa1x_cq_readerr(struct fid_cq *cq, struct fi_cq_err_entry *buf, uint64_t flags)
{
	FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_CQ, "(begin)\n");

	struct fi_opa1x_cq *opa1x_cq = container_of(cq, struct fi_opa1x_cq, cq_fid);

	if (IS_PROGRESS_MANUAL(opa1x_cq->domain)) {

		struct fi_opa1x_context_ext * ext =
			 (struct fi_opa1x_context_ext *) opa1x_cq->err.head;

		if ((ext == NULL) || (ext->opa1x_context.byte_counter != 0)) {
			/* perhaps an in-progress truncated rendezvous receive? */
			errno = FI_EAGAIN;
			return -errno;
		}

		assert(ext->opa1x_context.flags & FI_OPA1X_CQ_CONTEXT_EXT);	/* DEBUG */

		const enum fi_threading threading = opa1x_cq->domain->threading;

		switch (threading) {
		case FI_THREAD_ENDPOINT:
		case FI_THREAD_DOMAIN:
			*buf = ext->err_entry;
			slist_remove_head((struct slist *)&opa1x_cq->err);
			free(ext);
			break;
		default:
			FI_WARN(fi_opa1x_global.prov, FI_LOG_CQ, "bad thread mode (%u)\n", threading);
			abort();
			break;
		}

	} else {
		FI_WARN(fi_opa1x_global.prov, FI_LOG_CQ, "unimplemented\n");
		abort();
	}

	FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_CQ, "(end)\n");
	return 1;
}

static ssize_t
fi_opa1x_cq_sread(struct fid_cq *cq, void *buf, size_t len, const void *cond, int timeout)
{
	FI_WARN(fi_opa1x_global.prov, FI_LOG_CQ, "unimplemented\n");
	abort();

	errno = FI_EAGAIN;
	return -errno;
}

static ssize_t
fi_opa1x_cq_sreadfrom(struct fid_cq *cq, void *buf, size_t len,
		   fi_addr_t *src_addr, const void *cond, int timeout)
{
	FI_WARN(fi_opa1x_global.prov, FI_LOG_CQ, "unimplemented\n");
	abort();

	errno = FI_EAGAIN;
	return -errno;
}

static const char *
fi_opa1x_cq_strerror(struct fid_cq *cq, int prov_errno, const void *err_data,
	       char *buf, size_t len)
{
	FI_WARN(fi_opa1x_global.prov, FI_LOG_CQ, "unimplemented\n");
	errno = FI_ENOSYS;
	return NULL;
}

int fi_opa1x_bind_ep_cq(struct fid_ep *ep,
		struct fid_cq *cq, uint64_t flags)
{
	FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_CQ, "(begin)\n");

	struct fi_opa1x_ep *opa1x_ep =
		container_of(ep, struct fi_opa1x_ep, ep_fid);

	struct fi_opa1x_cq *opa1x_cq =
		container_of(cq, struct fi_opa1x_cq, cq_fid);
//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
	if (!(flags & (FI_TRANSMIT | FI_RECV)))
		goto err;

//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
	if (flags & FI_TRANSMIT) {
//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
		fi_opa1x_ref_inc(&opa1x_cq->ref_cnt, "tx completion queue");
		opa1x_ep->tx.cq = opa1x_cq;
		opa1x_ep->tx.cq_completed_ptr = &opa1x_cq->completed;
		opa1x_ep->tx.cq_pending_ptr = &opa1x_cq->pending;
		opa1x_ep->tx.cq_err_ptr = &opa1x_cq->err;
		//opa1x_ep->tx.cq_lock_ptr = &opa1x_cq->lock;

		/* See NOTE_SELECTIVE_COMPLETION for more information */
		opa1x_ep->tx.cq_bind_flags = flags;

		const uint64_t selective_completion =
			FI_SELECTIVE_COMPLETION | FI_TRANSMIT | FI_COMPLETION;

		const uint64_t cq_flags = opa1x_ep->tx.op_flags | flags;

		opa1x_ep->tx.do_cq_completion =
			((cq_flags & selective_completion) == selective_completion) ||
			((cq_flags & (FI_SELECTIVE_COMPLETION | FI_TRANSMIT)) == FI_TRANSMIT);

	}
//fprintf(stderr, "%s:%s():%d opa1x_ep->tx.cq = %p\n", __FILE__, __func__, __LINE__, opa1x_ep->tx.cq);
//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
	if (flags & FI_RECV) {
//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
		fi_opa1x_ref_inc(&opa1x_cq->ref_cnt, "rx completion queue");
		opa1x_ep->rx.cq = opa1x_cq;
		opa1x_ep->rx.cq_completed_ptr = &opa1x_cq->completed;
		opa1x_ep->rx.cq_pending_ptr = &opa1x_cq->pending;
		opa1x_ep->rx.cq_err_ptr = &opa1x_cq->err;
		//opa1x_ep->rx.cq_lock_ptr = &opa1x_cq->lock;
	}
//fprintf(stderr, "%s:%s():%d opa1x_ep->rx.cq = %p\n", __FILE__, __func__, __LINE__, opa1x_ep->rx.cq);
	opa1x_cq->bflags = flags;

	if (FI_CLASS_RX_CTX == opa1x_ep->ep_fid.fid.fclass ||
			FI_CLASS_EP == opa1x_ep->ep_fid.fid.fclass) {
//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
		opa1x_cq->ep[(opa1x_cq->ep_bind_count)++] = opa1x_ep;
	}

//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
	if (ofi_recv_allowed(opa1x_ep->rx.caps) || ofi_rma_target_allowed(opa1x_ep->rx.caps)) {
//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
		opa1x_cq->progress.ep[(opa1x_cq->progress.ep_count)++] = opa1x_ep;
	}

	FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_CQ, "(end)\n");
	return 0;
err:
	FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_CQ, "(end, error)\n");
	errno = FI_EINVAL;
	return -errno;
}

int fi_opa1x_cq_enqueue_err (struct fi_opa1x_cq * opa1x_cq,
		struct fi_opa1x_context_ext * ext,
		const int lock_required)
{
	assert(ext->opa1x_context.flags & FI_OPA1X_CQ_CONTEXT_EXT);	/* DEBUG */
	ext->opa1x_context.next = NULL;

	const enum fi_threading threading = opa1x_cq->domain->threading;

	switch (threading) {
		case FI_THREAD_ENDPOINT:
		case FI_THREAD_DOMAIN:
			fi_opa1x_context_slist_insert_tail((union fi_opa1x_context *)ext, &opa1x_cq->err);
			break;
		default:
			fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__); abort();
			break;
	}

	return 0;
}

FI_OPA1X_CQ_SPECIALIZED_FUNC(FI_CQ_FORMAT_UNSPEC, 0)
FI_OPA1X_CQ_SPECIALIZED_FUNC(FI_CQ_FORMAT_UNSPEC, 1)
FI_OPA1X_CQ_SPECIALIZED_FUNC(FI_CQ_FORMAT_CONTEXT, 0)
FI_OPA1X_CQ_SPECIALIZED_FUNC(FI_CQ_FORMAT_CONTEXT, 1)
FI_OPA1X_CQ_SPECIALIZED_FUNC(FI_CQ_FORMAT_MSG, 0)
FI_OPA1X_CQ_SPECIALIZED_FUNC(FI_CQ_FORMAT_MSG, 1)
FI_OPA1X_CQ_SPECIALIZED_FUNC(FI_CQ_FORMAT_DATA, 0)
FI_OPA1X_CQ_SPECIALIZED_FUNC(FI_CQ_FORMAT_DATA, 1)
FI_OPA1X_CQ_SPECIALIZED_FUNC(FI_CQ_FORMAT_TAGGED, 0)
FI_OPA1X_CQ_SPECIALIZED_FUNC(FI_CQ_FORMAT_TAGGED, 1)

#define FI_OPA1X_CQ_OPS_STRUCT_NAME(FORMAT, LOCK)					\
  fi_opa1x_ops_cq_ ## FORMAT ## _ ## LOCK						\

#define FI_OPA1X_CQ_OPS_STRUCT(FORMAT, LOCK)					\
static struct fi_ops_cq								\
	FI_OPA1X_CQ_OPS_STRUCT_NAME(FORMAT, LOCK) = {				\
    .size    = sizeof(struct fi_ops_cq),					\
    .read      = FI_OPA1X_CQ_SPECIALIZED_FUNC_NAME(cq_read, FORMAT, LOCK),	\
    .readfrom  = FI_OPA1X_CQ_SPECIALIZED_FUNC_NAME(cq_readfrom, FORMAT, LOCK),	\
    .readerr   = fi_opa1x_cq_readerr,						\
    .sread     = fi_opa1x_cq_sread,						\
    .sreadfrom = fi_opa1x_cq_sreadfrom,						\
    .signal    = fi_no_cq_signal,						\
    .strerror  = fi_opa1x_cq_strerror,						\
}

FI_OPA1X_CQ_OPS_STRUCT(FI_CQ_FORMAT_UNSPEC, 0);
FI_OPA1X_CQ_OPS_STRUCT(FI_CQ_FORMAT_UNSPEC, 1);
FI_OPA1X_CQ_OPS_STRUCT(FI_CQ_FORMAT_CONTEXT, 0);
FI_OPA1X_CQ_OPS_STRUCT(FI_CQ_FORMAT_CONTEXT, 1);
FI_OPA1X_CQ_OPS_STRUCT(FI_CQ_FORMAT_MSG, 0);
FI_OPA1X_CQ_OPS_STRUCT(FI_CQ_FORMAT_MSG, 1);
FI_OPA1X_CQ_OPS_STRUCT(FI_CQ_FORMAT_DATA, 0);
FI_OPA1X_CQ_OPS_STRUCT(FI_CQ_FORMAT_DATA, 1);
FI_OPA1X_CQ_OPS_STRUCT(FI_CQ_FORMAT_TAGGED, 0);
FI_OPA1X_CQ_OPS_STRUCT(FI_CQ_FORMAT_TAGGED, 1);


static struct fi_ops_cq fi_opa1x_ops_cq_default = {
	.size		= sizeof(struct fi_ops_cq),
	.read		= fi_opa1x_cq_read,
	.readfrom	= fi_opa1x_cq_readfrom,
	.readerr	= fi_opa1x_cq_readerr,
	.signal		= fi_no_cq_signal,
	.sread		= fi_opa1x_cq_sread,
	.sreadfrom	= fi_opa1x_cq_sreadfrom,
	.strerror	= fi_opa1x_cq_strerror
};


int fi_opa1x_cq_open(struct fid_domain *dom,
		struct fi_cq_attr *attr,
		struct fid_cq **cq, void *context)
{
	FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_CQ, "open cq\n");

	int ret;
	struct fi_opa1x_cq *opa1x_cq;
	int lock_required;

	if (!attr) {
		FI_LOG(fi_opa1x_global.prov, FI_LOG_DEBUG, FI_LOG_CQ,
				"no attr supplied\n");
		errno = FI_EINVAL;
		return -errno;
	}
	ret = fi_opa1x_fid_check(&dom->fid, FI_CLASS_DOMAIN, "domain");
	if (ret)
		return ret;

	opa1x_cq = calloc(1, sizeof(*opa1x_cq));
	if (!opa1x_cq) {
		errno = FI_ENOMEM;
		goto err;
	}

	opa1x_cq->cq_fid.fid.fclass = FI_CLASS_CQ;
	opa1x_cq->cq_fid.fid.context= context;
	opa1x_cq->cq_fid.fid.ops    = &fi_opa1x_fi_ops;

	opa1x_cq->size = attr->size ? attr->size : FI_OPA1X_DEFAULT_CQ_DEPTH;

	opa1x_cq->domain = (struct fi_opa1x_domain *) dom;

	opa1x_cq->format = attr->format ? attr->format : FI_CQ_FORMAT_CONTEXT;

	fi_opa1x_context_slist_init(&opa1x_cq->pending);
	fi_opa1x_context_slist_init(&opa1x_cq->completed);
	fi_opa1x_context_slist_init(&opa1x_cq->err);

	switch (opa1x_cq->domain->threading) {
	case FI_THREAD_ENDPOINT:
	case FI_THREAD_DOMAIN:
	case FI_THREAD_COMPLETION:
		lock_required = 0;
		break;
	case FI_THREAD_FID:
	case FI_THREAD_UNSPEC:
	case FI_THREAD_SAFE:
		lock_required = 1;
		break;
	default:
		errno = FI_EINVAL;
	goto err;
	}

	if (lock_required == 0 &&
			opa1x_cq->format == FI_CQ_FORMAT_UNSPEC) {
		opa1x_cq->cq_fid.ops =
			&FI_OPA1X_CQ_OPS_STRUCT_NAME(FI_CQ_FORMAT_UNSPEC, 0);
	} else if (lock_required == 0 &&
			opa1x_cq->format == FI_CQ_FORMAT_CONTEXT) {
		opa1x_cq->cq_fid.ops =
			&FI_OPA1X_CQ_OPS_STRUCT_NAME(FI_CQ_FORMAT_CONTEXT, 0);
	} else if (lock_required == 0 &&
			opa1x_cq->format == FI_CQ_FORMAT_MSG) {
		opa1x_cq->cq_fid.ops =
			&FI_OPA1X_CQ_OPS_STRUCT_NAME(FI_CQ_FORMAT_MSG, 0);
	} else if (lock_required == 0 &&
			opa1x_cq->format == FI_CQ_FORMAT_DATA) {
		opa1x_cq->cq_fid.ops =
			&FI_OPA1X_CQ_OPS_STRUCT_NAME(FI_CQ_FORMAT_DATA, 0);
	} else if (lock_required == 0 &&
			opa1x_cq->format == FI_CQ_FORMAT_TAGGED) {
		opa1x_cq->cq_fid.ops =
			&FI_OPA1X_CQ_OPS_STRUCT_NAME(FI_CQ_FORMAT_TAGGED, 0);
	} else if (lock_required == 1 &&
			opa1x_cq->format == FI_CQ_FORMAT_UNSPEC) {
		opa1x_cq->cq_fid.ops =
			&FI_OPA1X_CQ_OPS_STRUCT_NAME(FI_CQ_FORMAT_UNSPEC, 1);
	} else if (lock_required == 1 &&
			opa1x_cq->format == FI_CQ_FORMAT_CONTEXT) {
		opa1x_cq->cq_fid.ops =
			&FI_OPA1X_CQ_OPS_STRUCT_NAME(FI_CQ_FORMAT_CONTEXT, 1);
	} else if (lock_required == 1 &&
			opa1x_cq->format == FI_CQ_FORMAT_MSG) {
		opa1x_cq->cq_fid.ops =
			&FI_OPA1X_CQ_OPS_STRUCT_NAME(FI_CQ_FORMAT_MSG, 1);
	} else if (lock_required == 1 &&
			opa1x_cq->format == FI_CQ_FORMAT_DATA) {
		opa1x_cq->cq_fid.ops =
			&FI_OPA1X_CQ_OPS_STRUCT_NAME(FI_CQ_FORMAT_DATA, 1);
	} else if (lock_required == 1 &&
			opa1x_cq->format == FI_CQ_FORMAT_TAGGED) {
		opa1x_cq->cq_fid.ops =
			&FI_OPA1X_CQ_OPS_STRUCT_NAME(FI_CQ_FORMAT_TAGGED, 1);

	} else {
		opa1x_cq->cq_fid.ops =
			&fi_opa1x_ops_cq_default;
	}

	opa1x_cq->ep_bind_count = 0;
	opa1x_cq->progress.ep_count = 0;
	unsigned i;
	for (i=0; i<64; ++i) {		/* TODO - check this array size */
		opa1x_cq->ep[i] = NULL;
		opa1x_cq->progress.ep[i] = NULL;
	}


	//fi_opa1x_ref_init(&opa1x_cq->domain->fabric->node, &opa1x_cq->ref_cnt, "completion queue");
	fi_opa1x_ref_inc(&opa1x_cq->domain->ref_cnt, "domain");

	*cq = &opa1x_cq->cq_fid;

	FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_CQ, "cq opened\n");
	return 0;
err:
	if(opa1x_cq)
		free(opa1x_cq);
	return -errno;
}


#define FABRIC_DIRECT_LOCK	0
#define FABRIC_DIRECT_CQ_FORMAT	FI_CQ_FORMAT_TAGGED

ssize_t
fi_opa1x_cq_read_FABRIC_DIRECT(struct fid_cq *cq, void *buf, size_t count)
{
	return FI_OPA1X_CQ_SPECIALIZED_FUNC_NAME(cq_read,
			FABRIC_DIRECT_CQ_FORMAT,
			FABRIC_DIRECT_LOCK)
				(cq, buf, count);
}

ssize_t
fi_opa1x_cq_readfrom_FABRIC_DIRECT(struct fid_cq *cq, void *buf, size_t count,
		fi_addr_t *src_addr)
{
	return FI_OPA1X_CQ_SPECIALIZED_FUNC_NAME(cq_readfrom,
			FABRIC_DIRECT_CQ_FORMAT,
			FABRIC_DIRECT_LOCK)
				(cq, buf, count, src_addr);
}
