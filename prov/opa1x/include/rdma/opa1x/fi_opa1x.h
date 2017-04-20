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
#ifndef __FI_PROV_OPA1X_H__
#define __FI_PROV_OPA1X_H__

#include <config.h>

#include <stdbool.h>
#include <stdlib.h>
#include <assert.h>

#ifndef likely
#define likely(x) __builtin_expect((x), 1)
#endif
#ifndef unlikely
#define unlikely(x) __builtin_expect((x), 0)
#endif

#include <rdma/fabric.h>
#include <rdma/providers/fi_log.h>
#include <rdma/fi_domain.h>
#include <rdma/fi_endpoint.h>
#include <rdma/fi_cm.h>
#include <rdma/fi_eq.h>
#include <rdma/fi_errno.h>

// #define FI_OPA1X_TRACE 1

/* --- Will be exposed by fabric.h */
#define FI_OPA1X_PROTOCOL		0x0008
#define FI_OPA1X_PROTOCOL_VERSION (1)
/* --- end */

#define FI_OPA1X_PROVIDER_NAME		"opa1x"
#define FI_OPA1X_PROVIDER_VERSION	(1)
#define FI_OPA1X_DEVICE_MAX_PATH_NAME	(32)
#define FI_OPA1X_FABRIC_NAME		"OPA1"

#define FI_OPA1X_CACHE_LINE_SIZE	(64)

#define FI_OPA1X_MAX_STRLEN		(64)

#define EXIT_FAILURE 1

#define LOCAL_COMM_ENABLED

#ifdef LOCAL_COMM_ENABLED
#define OPA1X_LOCAL_COMM_CAP	(FI_LOCAL_COMM)
#else
#define OPA1X_LOCAL_COMM_CAP	(0)
#endif

struct fi_opa1x_global_data {
	struct fi_info		*info;
	struct fi_domain_attr	*default_domain_attr;
	struct fi_ep_attr	*default_ep_attr;
	struct fi_tx_attr	*default_tx_attr;
	struct fi_rx_attr	*default_rx_attr;
	struct fi_provider 	*prov;
};

extern struct fi_opa1x_global_data fi_opa1x_global;

static const uint64_t FI_OPA1X_MAX_MSG_SIZE		= ((uint64_t)-1);
static const uint64_t FI_OPA1X_MAX_PREFIX_SIZE		= (0ULL);
//static const uint64_t FI_OPA1X_INJECT_SIZE		= FI_OPA1X_HFI1_PACKET_IMM;
static const uint64_t FI_OPA1X_MAX_ORDER_RAW_SIZE	= (0ULL);
static const uint64_t FI_OPA1X_MAX_ORDER_WAR_SIZE	= (0ULL);
static const uint64_t FI_OPA1X_MAX_ORDER_WAW_SIZE	= (0ULL);
//static const size_t   FI_OPA1X_TOTAL_BUFFERED_RECV	= FI_OPA1X_HFI1_PACKET_MTU;
static const size_t   FI_OPA1X_REMOTE_CQ_DATA_SIZE	= 4;

static const uint64_t FI_OPA1X_MEM_TAG_FORMAT		= (0xFFFFFFFFFFFFFFFFULL);

#define FI_OPA1X_DEFAULT_MSG_ORDER						\
	(FI_ORDER_SAS)
	/* TODO: FI_ORDER_RAR | FI_ORDER_RAW | FI_ORDER_WAW | FI_ORDER_WAS | FI_ORDER_SAW */

#define FI_OPA1X_TXONLY_CAPS							\
	(FI_SEND | FI_READ | FI_WRITE)						/* TODO rma+atomic implementation */

#define FI_OPA1X_RXONLY_CAPS							\
	( FI_RECV | FI_DIRECTED_RECV | FI_MULTI_RECV				\
	| FI_REMOTE_READ | FI_REMOTE_WRITE)					/* TODO rma+atomic implementation */

#define FI_OPA1X_BASE_CAPS							\
	( FI_MSG | FI_TAGGED | OPA1X_LOCAL_COMM_CAP | FI_REMOTE_COMM		\
	| FI_SOURCE | FI_NAMED_RX_CTX						\
	| FI_RMA | FI_ATOMIC )							/* TODO rma+atomic implementation */

#define FI_OPA1X_DEFAULT_CAPS							\
	(FI_OPA1X_BASE_CAPS | FI_OPA1X_TXONLY_CAPS | FI_OPA1X_RXONLY_CAPS)

#define FI_OPA1X_DEFAULT_TX_CAPS						\
	(FI_OPA1X_BASE_CAPS | FI_OPA1X_TXONLY_CAPS)

#define FI_OPA1X_DEFAULT_RX_CAPS						\
	(FI_OPA1X_BASE_CAPS | FI_OPA1X_RXONLY_CAPS)

#define FI_OPA1X_DEFAULT_MODE							\
	(FI_CONTEXT2 | FI_ASYNC_IOV)



#if 0
#ifndef FABRIC_DIRECT_PROGRESS
#define FABRIC_DIRECT_PROGRESS FI_PROGRESS_UNSPEC
#endif

#define IS_PROGRESS_MANUAL(domain_ptr)						\
	((FABRIC_DIRECT_PROGRESS == FI_PROGRESS_MANUAL) ||			\
	((FABRIC_DIRECT_PROGRESS == FI_PROGRESS_UNSPEC) &&			\
		((domain_ptr)->data_progress == FI_PROGRESS_MANUAL)))

#define IS_PROGRESS_AUTO(domain_ptr)						\
	((FABRIC_DIRECT_PROGRESS == FI_PROGRESS_AUTO) ||			\
	((FABRIC_DIRECT_PROGRESS == FI_PROGRESS_UNSPEC) &&			\
		((domain_ptr)->data_progress == FI_PROGRESS_AUTO)))
#else

#ifndef FABRIC_DIRECT_PROGRESS
#define FABRIC_DIRECT_PROGRESS FI_PROGRESS_MANUAL
#endif

#define IS_PROGRESS_MANUAL(domain_ptr)	(1)
#define IS_PROGRESS_AUTO(domain_ptr)	(0)
#endif

static inline void always_assert (bool val, char *msg)
{
	if (!val) {
		FI_LOG(fi_opa1x_global.prov, FI_LOG_DEBUG, FI_LOG_FABRIC,
				"%s\n", msg);
		exit(EXIT_FAILURE);
	}
}

static inline void fi_opa1x_ref_init (int64_t *ref, char *name)
{
	*ref = 0;
	FI_LOG(fi_opa1x_global.prov, FI_LOG_DEBUG, FI_LOG_FABRIC,
			"initializing ref count for (%s) to (%d)\n",
			name, 0);

	return;
}

static inline void fi_opa1x_ref_inc (int64_t *ref, char *name)
{
	(*ref) += 1;
	return;
}

static inline int fi_opa1x_ref_dec (int64_t *ref, char *name)
{
	int64_t value = --(*ref);
	if (value < 0) {

		FI_WARN(fi_opa1x_global.prov, FI_LOG_FABRIC,
			"decrement ref for (%s) (ref_cnt %ld < 0)\n",
			name, value);

		errno = FI_EOTHER;
		return -errno;
	}
	return 0;
}

static inline int fi_opa1x_ref_finalize (int64_t *ref, char *name)
{
	int64_t value = *ref;
	if (value != 0) {
		FI_WARN(fi_opa1x_global.prov, FI_LOG_FABRIC,
			"error ref for (%s) (ref_cnt %ld != 0)\n",
			name, value);
		errno = FI_EBUSY;
		return -errno;
	}
	return 0;
}
#if 0
static inline int fi_opa1x_lock_if_required (fastlock_t *lock, const int required)
{
	if (required) fastlock_acquire(lock);
	return 0;
}

static inline int fi_opa1x_unlock_if_required (fastlock_t *lock, const int required)
{
	if (required) fastlock_release(lock);
	return 0;
}
#endif
static inline int fi_opa1x_fid_check (fid_t fid, int fid_class, char *name)
{
	if (!fid) {
		FI_LOG(fi_opa1x_global.prov, FI_LOG_DEBUG, FI_LOG_FABRIC,
				"NULL %s object", name);
		errno = FI_EINVAL;
		return -errno;
	}
	if (fid->fclass != fid_class) {
		FI_LOG(fi_opa1x_global.prov, FI_LOG_DEBUG, FI_LOG_FABRIC,
			"wrong type of object (%s) expected (%d), got (%zu)\n",
			name, fid_class, fid->fclass);
		errno = FI_EINVAL;
		return -errno;
	}
	return 0;
}

#if 1
int fi_opa1x_set_default_info(void);

int fi_opa1x_check_info(const struct fi_info *info);

int fi_opa1x_fabric(struct fi_fabric_attr *attr,
		struct fid_fabric **fabric, void *context);

int fi_opa1x_check_fabric_attr(struct fi_fabric_attr *attr);

int fi_opa1x_domain(struct fid_fabric *fabric,
		struct fi_info *info,
		struct fid_domain **dom, void *context);

int fi_opa1x_check_domain_attr(struct fi_domain_attr *attr);
int fi_opa1x_choose_domain(uint64_t caps,
		struct fi_domain_attr *domain_attr,
		struct fi_domain_attr *hints);

int fi_opa1x_alloc_default_domain_attr(struct fi_domain_attr **domain_attr);

int fi_opa1x_stx_context(struct fid_domain *domain, struct fi_tx_attr *attr,
	       struct fid_stx **stx, void *context);

int fi_opa1x_av_open(struct fid_domain *dom,
		struct fi_av_attr *attr, struct fid_av **av,
		void *context);

int fi_opa1x_cq_open(struct fid_domain *dom,
		struct fi_cq_attr *attr,
		struct fid_cq **eq, void *context);

int fi_opa1x_endpoint(struct fid_domain *dom, struct fi_info *info,
		struct fid_ep **ep, void *context);

int fi_opa1x_alloc_default_ep_attr(struct fi_ep_attr **ep_attr);

int fi_opa1x_check_ep_attr(struct fi_ep_attr *attr);

int fi_opa1x_alloc_default_tx_attr(struct fi_tx_attr **tx_attr);
int fi_opa1x_check_tx_attr(struct fi_tx_attr *attr);

int fi_opa1x_alloc_default_rx_attr(struct fi_rx_attr **rx_attr);
int fi_opa1x_check_rx_attr(struct fi_rx_attr *attr);

int fi_opa1x_scalable_ep(struct fid_domain *dom, struct fi_info *info,
		struct fid_ep **ep, void *context);

int fi_opa1x_cntr_open(struct fid_domain *domain,
		struct fi_cntr_attr *attr,
		struct fid_cntr **cntr, void *context);

int fi_opa1x_init_mr_ops(struct fid_domain *domain, struct fi_info *info);
int fi_opa1x_finalize_mr_ops(struct fid_domain *domain);

int fi_opa1x_init_rma_ops(struct fid_ep *ep, struct fi_info *info);
int fi_opa1x_enable_rma_ops(struct fid_ep *ep);
int fi_opa1x_finalize_rma_ops(struct fid_ep *ep);

int fi_opa1x_init_msg_ops(struct fid_ep *ep, struct fi_info *info);
int fi_opa1x_enable_msg_ops(struct fid_ep *ep);
int fi_opa1x_finalize_msg_ops(struct fid_ep *ep);

int fi_opa1x_init_atomic_ops(struct fid_ep *ep, struct fi_info *info);
int fi_opa1x_enable_atomic_ops(struct fid_ep *ep);
int fi_opa1x_finalize_atomic_ops(struct fid_ep *ep);

int fi_opa1x_init_tagged_ops(struct fid_ep *ep, struct fi_info *info);
int fi_opa1x_enable_tagged_ops(struct fid_ep *ep);
int fi_opa1x_finalize_tagged_ops(struct fid_ep *ep);

//int fi_opa1x_init_cm_ops(struct fi_opa1x_ep *opa1x_ep, struct fi_info *info);
int fi_opa1x_init_cm_ops(fid_t fid, struct fi_info *info);
//int fi_opa1x_finalize_cm_ops(struct fi_opa1x_ep *opa1x_ep);
int fi_opa1x_finalize_cm_ops(fid_t fid);

int fi_opa1x_bind_ep_stx(struct fid_ep *ep,
		struct fid_stx *stx, uint64_t flags);
int fi_opa1x_bind_ep_cq(struct fid_ep *ep,
		struct fid_cq *cq, uint64_t flags);
int fi_opa1x_bind_ep_cntr(struct fid_ep *ep,
		struct fid_cntr *cntr, uint64_t flags);
int fi_opa1x_bind_ep_mr(struct fid_ep *ep,
		struct fid_mr *mr, uint64_t flags);
int fi_opa1x_bind_ep_av(struct fid_ep *ep,
		struct fid_av *av, uint64_t flags);
#endif
#endif /* __FI_PROV_OPA1X_H__ */
