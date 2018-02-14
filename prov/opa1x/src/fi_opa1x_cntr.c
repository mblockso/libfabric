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

static int fi_opa1x_close_cntr(struct fid *fid)
{
	int ret;
	struct fi_opa1x_cntr *opa1x_cntr =
		container_of(fid, struct fi_opa1x_cntr, cntr_fid);

	ret = fi_opa1x_fid_check(fid, FI_CLASS_CNTR, "counter");
	if (ret)
		return ret;

	ret = fi_opa1x_ref_dec(&opa1x_cntr->domain->ref_cnt, "domain");
	if (ret)
		return ret;

	free(opa1x_cntr->attr);
	free(opa1x_cntr);
	return 0;
}

static int fi_opa1x_bind_cntr(struct fid *fid, struct fid *bfid, uint64_t flags)
{
	errno = FI_ENOSYS;
	return -errno;
}

static uint64_t fi_opa1x_cntr_read(struct fid_cntr *cntr)
{
	struct fi_opa1x_cntr *opa1x_cntr =
		container_of(cntr, struct fi_opa1x_cntr, cntr_fid);

	const int64_t value = 0; fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__); abort();//ofi_atomic_get64(&opa1x_cntr->std);

	if (IS_PROGRESS_MANUAL(opa1x_cntr->domain)) {
		const uint64_t count = opa1x_cntr->progress.ep_count;
		uint64_t i;
		if (unlikely(opa1x_cntr->lock_required)) {
			abort();
			for (i=0; i<count; ++i) {
				fi_opa1x_ep_rx_poll(opa1x_cntr->progress.ep[i]);
			}
		} else {
			for (i=0; i<count; ++i) {
				fi_opa1x_ep_rx_poll(opa1x_cntr->progress.ep[i]);
			}
		}
	}

	return value;
}

static uint64_t fi_opa1x_cntr_readerr(struct fid_cntr *cntr)
{
//	struct fi_opa1x_cntr *opa1x_cntr =
//		container_of(cntr, struct fi_opa1x_cntr, cntr_fid);
fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__); abort();
	return 0; //ofi_atomic_get64(&opa1x_cntr->err);
}

static int fi_opa1x_cntr_add(struct fid_cntr *cntr, uint64_t value)
{
//	struct fi_opa1x_cntr *opa1x_cntr =
//		container_of(cntr, struct fi_opa1x_cntr, cntr_fid);

	fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__); abort();//ofi_atomic_add64(&opa1x_cntr->std, value);

	return 0;
}

static int fi_opa1x_cntr_set(struct fid_cntr *cntr, uint64_t value)
{
//	struct fi_opa1x_cntr *opa1x_cntr =
//		container_of(cntr, struct fi_opa1x_cntr, cntr_fid);

	fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__); abort();//ofi_atomic_set64(&opa1x_cntr->std, value);

	return 0;
}

static int
fi_opa1x_cntr_wait(struct fid_cntr *cntr, uint64_t threshold, int timeout)
{
abort();
#if 0
	struct fi_opa1x_cntr *opa1x_cntr =
		container_of(cntr, struct fi_opa1x_cntr, cntr_fid);

	uint64_t timeout_cycles = (timeout < 0) ?
		ULLONG_MAX :
		GetTimeBase() + (1600UL * 1000 * timeout);

	uint64_t current_value = 0;
	ofi_atomic64_t *std = &opa1x_cntr->std;
	do {
		current_value = ofi_atomic_get64(std);

		if (IS_PROGRESS_MANUAL(opa1x_cntr->domain)) {
			const uint64_t count = opa1x_cntr->progress.ep_count;
			uint64_t i;
			for (i=0; i<count; ++i) {
				fi_opa1x_ep_rx_hfi1_poll(&opa1x_cntr->progress.ep[i]->rx, opa1x_cntr->lock_required);
			}
		}

		if (threshold <= current_value) return 0;
	} while (GetTimeBase() < timeout_cycles);
#endif
	errno = FI_ETIMEDOUT;
	return -errno;
}

static struct fi_ops fi_opa1x_fi_ops = {
	.size		= sizeof(struct fi_ops),
	.close		= fi_opa1x_close_cntr,
	.bind		= fi_opa1x_bind_cntr,
	.control	= fi_no_control,
	.ops_open	= fi_no_ops_open
};

int fi_opa1x_bind_ep_cntr(struct fid_ep *ep,
		struct fid_cntr *cntr, uint64_t flags)
{
	struct fi_opa1x_cntr *opa1x_cntr =
		container_of(cntr, struct fi_opa1x_cntr, cntr_fid);

	struct fi_opa1x_ep *opa1x_ep =
		container_of(ep, struct fi_opa1x_ep, ep_fid);

	if (!(flags & (FI_WRITE |
			FI_READ |
			FI_SEND |
			FI_RECV |
			FI_REMOTE_READ |
			FI_REMOTE_WRITE))) {
		FI_LOG(fi_opa1x_global.prov, FI_LOG_DEBUG, FI_LOG_CQ,
				"unclear flags while binding counter\n");
		goto err;
	}

	if (flags & FI_WRITE)
		opa1x_ep->write_cntr = opa1x_cntr;
	if (flags & FI_READ)
		opa1x_ep->read_cntr = opa1x_cntr;
	if (flags & FI_SEND)
		opa1x_ep->send_cntr = opa1x_cntr;
	if (flags & FI_RECV)
		opa1x_ep->recv_cntr = opa1x_cntr;

	opa1x_cntr->ep[(opa1x_cntr->ep_bind_count)++] = opa1x_ep;

	if (ofi_recv_allowed(opa1x_ep->rx.caps) || ofi_rma_target_allowed(opa1x_ep->rx.caps)) {
		opa1x_cntr->progress.ep[(opa1x_cntr->progress.ep_count)++] = opa1x_ep;
	}

	return 0;
err:
	errno = FI_EINVAL;
	return -errno;
}

static struct fi_ops_cntr fi_opa1x_ops_cntr = {
	.size		= sizeof(struct fi_ops_cntr),
	.read		= fi_opa1x_cntr_read,
	.readerr	= fi_opa1x_cntr_readerr,
	.add		= fi_opa1x_cntr_add,
	.set		= fi_opa1x_cntr_set,
	.wait		= fi_opa1x_cntr_wait
};

int fi_opa1x_cntr_open(struct fid_domain *domain,
		struct fi_cntr_attr *attr,
		struct fid_cntr **cntr, void *context)
{
	int ret;
	struct fi_opa1x_cntr *opa1x_cntr;

	if (!attr) {
		FI_LOG(fi_opa1x_global.prov, FI_LOG_DEBUG, FI_LOG_CQ,
				"no attr supplied\n");
		errno = FI_EINVAL;
		return -errno;
	}
	ret = fi_opa1x_fid_check(&domain->fid, FI_CLASS_DOMAIN, "domain");
	if (ret)
		return ret;

	opa1x_cntr = calloc(1, sizeof(*opa1x_cntr));
	if (!opa1x_cntr) {
		errno = FI_ENOMEM;
		goto err;
	}

	opa1x_cntr->cntr_fid.fid.fclass		= FI_CLASS_CNTR;
	opa1x_cntr->cntr_fid.fid.context	= context;
	opa1x_cntr->cntr_fid.fid.ops		= &fi_opa1x_fi_ops;
	opa1x_cntr->cntr_fid.ops		= &fi_opa1x_ops_cntr;

	opa1x_cntr->domain = (struct fi_opa1x_domain *) domain;

	opa1x_cntr->threading = opa1x_cntr->domain->threading;
	opa1x_cntr->lock_required =
		(opa1x_cntr->threading == FI_THREAD_FID) ||
		(opa1x_cntr->threading == FI_THREAD_UNSPEC) ||
		(opa1x_cntr->threading == FI_THREAD_SAFE);

	/* ---- allocate and initialize the "std" and "err" counters ---- */
	//ofi_atomic_initialize64(&opa1x_cntr->std, 0);
	//ofi_atomic_initialize64(&opa1x_cntr->err, 0);

	opa1x_cntr->ep_bind_count = 0;
	opa1x_cntr->progress.ep_count = 0;
	unsigned i;
	for (i=0; i<64; ++i) {			/* TODO - check this array size */
		opa1x_cntr->ep[i] = NULL;
		opa1x_cntr->progress.ep[i] = NULL;
	}

	fi_opa1x_ref_inc(&opa1x_cntr->domain->ref_cnt, "domain");

	*cntr = &opa1x_cntr->cntr_fid;
	return 0;
err:
	if (opa1x_cntr)
		free(opa1x_cntr);
	return -errno;
}
