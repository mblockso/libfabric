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
#ifndef _FI_PROV_OPA1X_RMA_H_
#define _FI_PROV_OPA1X_RMA_H_

#include "rdma/opa1x/fi_opa1x_internal.h"

/* Macro indirection in order to support other macros as arguments
 * C requires another indirection for expanding macros since
 * operands of the token pasting operator are not expanded */

#define FI_OPA1X_RMA_SPECIALIZED_FUNC(LOCK)					\
	FI_OPA1X_RMA_SPECIALIZED_FUNC_(LOCK)

#define FI_OPA1X_RMA_SPECIALIZED_FUNC_(LOCK)					\
	static inline ssize_t							\
	fi_opa1x_writemsg_ ## LOCK						\
		(struct fid_ep *ep, const struct fi_msg_rma *msg,		\
			uint64_t flags)						\
	{									\
		return fi_opa1x_writemsg_generic(ep, msg, flags,		\
				LOCK);						\
	}									\
	static inline ssize_t							\
	fi_opa1x_writev_ ## LOCK						\
		(struct fid_ep *ep, const struct iovec *iov,			\
			void **desc, size_t count, fi_addr_t dest_addr,		\
			uint64_t addr, uint64_t key, void *context)		\
	{									\
		return fi_opa1x_writev_generic(ep, iov, desc, count,		\
				dest_addr, addr, key, context, LOCK);		\
	}									\
	static inline ssize_t							\
	fi_opa1x_write_ ## LOCK							\
		(struct fid_ep *ep, const void *buf, size_t len,		\
			void *desc, fi_addr_t dst_addr, uint64_t addr,		\
			uint64_t key, void *context)				\
	{									\
		return fi_opa1x_write_generic(ep, buf, len, desc,		\
				dst_addr, addr, key, context, LOCK);		\
	}									\
	static inline ssize_t							\
	fi_opa1x_inject_write_ ## LOCK 						\
		(struct fid_ep *ep, const void *buf, size_t len,		\
			fi_addr_t dst_addr, uint64_t addr,			\
			uint64_t key)						\
	{									\
		return fi_opa1x_inject_write_generic(ep, buf, len,		\
				dst_addr, addr, key, LOCK);			\
	}									\
	static inline ssize_t							\
	fi_opa1x_readmsg_ ## LOCK						\
		(struct fid_ep *ep, const struct fi_msg_rma *msg,		\
			uint64_t flags)						\
	{									\
		return fi_opa1x_readmsg_generic(ep, msg, flags,			\
				LOCK);						\
	}									\
	static inline ssize_t							\
	fi_opa1x_readv_ ## LOCK							\
		(struct fid_ep *ep, const struct iovec *iov,			\
			void **desc, size_t count, fi_addr_t src_addr,		\
			uint64_t addr, uint64_t key, void *context)		\
	{									\
		return fi_opa1x_writev_generic(ep, iov, desc, count,		\
				src_addr, addr, key, context, LOCK);		\
	}									\
	static inline ssize_t							\
	fi_opa1x_read_ ## LOCK							\
		(struct fid_ep *ep, void *buf, size_t len,			\
			void *desc, fi_addr_t src_addr, uint64_t addr,		\
			uint64_t key, void *context)				\
	{									\
		return fi_opa1x_read_generic(ep, buf, len, desc,		\
				src_addr, addr, key, context, LOCK);		\
	}

#define FI_OPA1X_RMA_SPECIALIZED_FUNC_NAME(TYPE, LOCK)				\
	FI_OPA1X_RMA_SPECIALIZED_FUNC_NAME_(TYPE, LOCK)

#define FI_OPA1X_RMA_SPECIALIZED_FUNC_NAME_(TYPE, LOCK)				\
		fi_opa1x_ ## TYPE ## _ ## LOCK

#ifdef __cplusplus
extern "C" {
#endif

int fi_opa1x_check_rma (struct fi_opa1x_ep *opa1x_ep);

void fi_opa1x_readv_internal (struct fi_opa1x_ep * opa1x_ep,
		const struct iovec * iov,
		const size_t niov,
		const union fi_opa1x_addr * opa1x_target_addr,
		const uint64_t * addr,
		const uint64_t * key,
		union fi_opa1x_context * opa1x_context,
		const uint64_t tx_op_flags,
		const uint64_t enable_cq,
		const uint64_t enable_cntr,
		const int lock_required);

void fi_opa1x_write_fence (struct fi_opa1x_ep * opa1x_ep,
		const uint64_t tx_op_flags,
		const union fi_opa1x_addr * opa1x_dst_addr,
		union fi_opa1x_context * opa1x_context,
		const int lock_required);

void fi_opa1x_write_internal (struct fi_opa1x_ep * opa1x_ep,
		const void * buf,
		size_t len,
		const union fi_opa1x_addr * opa1x_dst_addr,
		uint64_t addr,
		const uint64_t key,
		union fi_opa1x_context * opa1x_context,
		const uint64_t tx_op_flags,
		const uint64_t enable_cq,
		const uint64_t enable_cntr,
		const int lock_required);

ssize_t fi_opa1x_inject_write_generic(struct fid_ep *ep,
		const void *buf, size_t len, fi_addr_t dst_addr,
		uint64_t addr, uint64_t key,
		int lock_required);

ssize_t fi_opa1x_write_generic(struct fid_ep *ep,
		const void *buf, size_t len, void *desc, fi_addr_t dst_addr,
		uint64_t addr, uint64_t key, void *context,
		int lock_required);

ssize_t fi_opa1x_writev_generic(struct fid_ep *ep,
		const struct iovec *iov, void **desc, size_t count,
		fi_addr_t dst_addr, uint64_t addr, uint64_t key, void *context,
		int lock_required);

ssize_t fi_opa1x_writemsg_generic(struct fid_ep *ep,
		const struct fi_msg_rma *msg, uint64_t flags,
		int lock_required);

ssize_t fi_opa1x_read_generic(struct fid_ep *ep,
		void *buf, size_t len, void *desc, fi_addr_t src_addr,
		uint64_t addr, uint64_t key, void *context,
		int lock_required);

ssize_t fi_opa1x_readv_generic (struct fid_ep *ep,
		const struct iovec *iov, void **desc, size_t count,
		fi_addr_t src_addr, uint64_t addr, uint64_t key, void *context,
		int lock_required);

ssize_t fi_opa1x_readmsg_generic(struct fid_ep *ep,
		const struct fi_msg_rma *msg, uint64_t flags,
		int lock_required);

#ifdef __cplusplus
}
#endif

#endif /* _FI_PROV_OPA1X_RMA_H_ */
