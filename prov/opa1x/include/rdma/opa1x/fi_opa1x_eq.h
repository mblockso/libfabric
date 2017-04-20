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
#ifndef _FI_PROV_OPA1X_EQ_H_
#define _FI_PROV_OPA1X_EQ_H_

#include <assert.h>
#include <unistd.h>
#include <stdint.h>

#include "rdma/opa1x/fi_opa1x_internal.h"
#include "rdma/opa1x/fi_opa1x_endpoint.h"
#include "rdma/opa1x/fi_opa1x_hfi1.h"

/* Macro indirection in order to support other macros as arguments
 * C requires another indirection for expanding macros since
 * operands of the token pasting operator are not expanded */

#define FI_OPA1X_CQ_SPECIALIZED_FUNC(FORMAT, LOCK)				\
	FI_OPA1X_CQ_SPECIALIZED_FUNC_(FORMAT, LOCK)

#define FI_OPA1X_CQ_SPECIALIZED_FUNC_(FORMAT, LOCK)				\
	static inline ssize_t							\
	fi_opa1x_cq_read_ ## FORMAT ## _ ## LOCK				\
		(struct fid_cq *cq, void *buf, size_t count)			\
	{									\
		return fi_opa1x_cq_read_generic(cq, buf, count,			\
				FORMAT, LOCK);					\
	}									\
	static inline ssize_t							\
	fi_opa1x_cq_readfrom_ ## FORMAT ## _ ## LOCK				\
		(struct fid_cq *cq, void *buf, size_t count,			\
			fi_addr_t *src_addr)					\
	{									\
		return fi_opa1x_cq_readfrom_generic(cq, buf, count, src_addr,	\
				FORMAT, LOCK);					\
	}									\

#define FI_OPA1X_CQ_SPECIALIZED_FUNC_NAME(TYPE, FORMAT, LOCK)			\
	FI_OPA1X_CQ_SPECIALIZED_FUNC_NAME_(TYPE, FORMAT, LOCK)

#define FI_OPA1X_CQ_SPECIALIZED_FUNC_NAME_(TYPE, FORMAT, LOCK)			\
		fi_opa1x_ ## TYPE ## _ ## FORMAT ## _ ## LOCK


#ifdef __cplusplus
extern "C" {
#endif

struct fi_opa1x_cntr {
	struct fid_cntr		cntr_fid;

	uint64_t		std;	/* TODO was 'ofi_atomic64_t', but had to change for FABRIC_DIRECT */
	uint64_t		err;	/* TODO was 'ofi_atomic64_t', but had to change for FABRIC_DIRECT */

	struct {
		uint64_t		ep_count;
		struct fi_opa1x_ep	*ep[64];	/* TODO - check this array size */
	} progress;

	uint64_t		ep_bind_count;
	struct fi_opa1x_ep	*ep[64];	/* TODO - check this array size */

	struct fi_cntr_attr	*attr;
	struct fi_opa1x_domain	*domain;
	enum fi_threading	threading;
	int			lock_required;
};

/* This structure is organized in a way that minimizes cacheline use for the
 * "FI_PROGRESS_MANUAL + inject" poll scenario.
 */
struct fi_opa1x_cq {
	struct fid_cq			cq_fid;		/* must be the first field in the structure; 24 + 64 bytes */
	uint64_t			pad_0[5];

	/* == CACHE LINE == */

	struct fi_opa1x_context_slist	pending;
	struct fi_opa1x_context_slist	completed;
	struct fi_opa1x_context_slist	err;		/* 'struct fi_opa1x_context_ext' element linked list */
//	struct {
//		struct fi_opa1x_context_ext *	head;
//		struct fi_opa1x_context_ext *	tail;
//	} err;
//	struct slist			err;		/* 'struct fi_opa1x_context_ext' element linked list */

//	struct fi_opa1x_context_ext	*err_head;

//	union fi_opa1x_context		*pending_head;
//	union fi_opa1x_context		*pending_tail;
//	union fi_opa1x_context		*completed_head;
//	union fi_opa1x_context		*completed_tail;

	struct {
		uint64_t		ep_count;
		struct fi_opa1x_ep	*ep[64];	/* TODO - check this array size */
	} progress;

//	struct fi_opa1x_context_ext	*err_tail;
	uint64_t			pad_1[9];

	struct fi_opa1x_domain		*domain;
	uint64_t			bflags;		/* fi_opa1x_bind_ep_cq() */
	size_t				size;
	enum fi_cq_format		format;

	uint64_t			ep_bind_count;
	struct fi_opa1x_ep		*ep[64];		/* TODO - check this array size */

	int64_t				ref_cnt;
	//fastlock_t			lock;
};


int fi_opa1x_eq_open(struct fid_fabric *fabric, struct fi_eq_attr *attr,
		struct fid_eq **eq, void *context);


#define DUMP_ENTRY_INPUT(entry)	\
({				\
	fprintf(stderr,"%s:%s():%d entry = %p\n", __FILE__, __func__, __LINE__, (entry));					\
	fprintf(stderr,"%s:%s():%d   op_context = %p\n", __FILE__, __func__, __LINE__, (entry)->tagged.op_context);		\
	fprintf(stderr,"%s:%s():%d   flags      = 0x%016lx\n", __FILE__, __func__, __LINE__, (entry)->tagged.flags);		\
	fprintf(stderr,"%s:%s():%d   len        = %zu\n", __FILE__, __func__, __LINE__, (entry)->tagged.len);			\
	fprintf(stderr,"%s:%s():%d   buf        = %p\n", __FILE__, __func__, __LINE__, (entry)->tagged.buf);			\
	fprintf(stderr,"%s:%s():%d   ignore     = 0x%016lx\n", __FILE__, __func__, __LINE__, (entry)->recv.ignore);		\
	fprintf(stderr,"%s:%s():%d   tag        = 0x%016lx\n", __FILE__, __func__, __LINE__, (entry)->tagged.tag);		\
	fprintf(stderr,"%s:%s():%d   entry_kind = %u\n", __FILE__, __func__, __LINE__, (entry)->recv.entry_kind);		\
	fprintf(stderr,"%s:%s():%d   entry_id   = %u\n", __FILE__, __func__, __LINE__, (entry)->recv.entry_id);		\
})

int fi_opa1x_cq_enqueue_err (struct fi_opa1x_cq * opa1x_cq,
		struct fi_opa1x_context_ext * ext,
		const int lock_required);

void fi_opa1x_cq_debug (struct fi_opa1x_cq * opa1x_cq, const int line);

static inline
int fi_opa1x_cq_enqueue_pending (struct fi_opa1x_cq * opa1x_cq,
		union fi_opa1x_context * context,
		const int lock_required)
{
	if (IS_PROGRESS_MANUAL(opa1x_cq->domain)) {

		if (lock_required) { FI_WARN(fi_opa1x_global.prov, FI_LOG_CQ, "unimplemented\n"); abort(); }

		union fi_opa1x_context * tail = opa1x_cq->pending.tail;
		context->next = NULL;
		if (tail) {
			tail->next = context;
		} else {
			opa1x_cq->pending.head = context;
		}
		opa1x_cq->pending.tail = context;

	} else {
		FI_WARN(fi_opa1x_global.prov, FI_LOG_CQ, "unimplemented\n");
		abort();		
	}

	return 0;
}


static inline
int fi_opa1x_cq_enqueue_completed (struct fi_opa1x_cq * opa1x_cq,
		union fi_opa1x_context * context,
		const int lock_required)
{
	assert(0 == context->byte_counter);

	if (IS_PROGRESS_MANUAL(opa1x_cq->domain)) {

		if (lock_required) { FI_WARN(fi_opa1x_global.prov, FI_LOG_CQ, "unimplemented\n"); abort(); }

		union fi_opa1x_context * tail = opa1x_cq->completed.tail;
		context->next = NULL;
		if (tail) {

			assert(NULL != opa1x_cq->completed.head);
			tail->next = context;
			opa1x_cq->completed.tail = context;

		} else {

			assert(NULL == opa1x_cq->completed.head);
			opa1x_cq->completed.head = context;
			opa1x_cq->completed.tail = context;
		}

	} else {
		FI_WARN(fi_opa1x_global.prov, FI_LOG_CQ, "unimplemented\n");
		abort();
	}

	return 0;
}



static size_t fi_opa1x_cq_fill(uintptr_t output,
		union fi_opa1x_context * context,
		const enum fi_cq_format format)
{
	assert((context->flags & FI_OPA1X_CQ_CONTEXT_EXT)==0);
	assert(sizeof(struct fi_context2) == sizeof(union fi_opa1x_context));

	struct fi_cq_tagged_entry * entry = (struct fi_cq_tagged_entry *) output;
	switch (format) {
	case FI_CQ_FORMAT_CONTEXT:
		if ((context->flags & FI_OPA1X_CQ_CONTEXT_MULTIRECV) == 0) {	/* likely */
			entry->op_context = (void *)context;
		} else {
			entry->op_context = (void *)context->multi_recv_context;
		}
		return sizeof(struct fi_cq_entry);
		break;
	case FI_CQ_FORMAT_MSG:
		*((struct fi_cq_msg_entry *)output) = *((struct fi_cq_msg_entry *)context);
		if ((context->flags & FI_OPA1X_CQ_CONTEXT_MULTIRECV) == 0) {	/* likely */
			entry->op_context = (void *)context;
		} else {
			entry->op_context = (void *)context->multi_recv_context;
		}
		return sizeof(struct fi_cq_msg_entry);
		break;
	case FI_CQ_FORMAT_DATA:
		*((struct fi_cq_data_entry *)output) = *((struct fi_cq_data_entry *)context);
		if ((context->flags & FI_OPA1X_CQ_CONTEXT_MULTIRECV) == 0) {	/* likely */
			entry->op_context = (void *)context;
		} else {
			entry->op_context = (void *)context->multi_recv_context;
		}
		return sizeof(struct fi_cq_data_entry);
		break;
	case FI_CQ_FORMAT_TAGGED:
		*((struct fi_cq_tagged_entry *)output) = *((struct fi_cq_tagged_entry *)context);
		if ((context->flags & FI_OPA1X_CQ_CONTEXT_MULTIRECV) == 0) {	/* likely */
			entry->op_context = (void *)context;
		} else {
			entry->op_context = (void *)context->multi_recv_context;
		}
		return sizeof(struct fi_cq_tagged_entry);
		break;
	default:
		assert(0);
	}

	return 0;
}

static ssize_t fi_opa1x_cq_poll_noinline (struct fi_opa1x_cq *opa1x_cq,
		void *buf,
		size_t count,
		const enum fi_cq_format format,
		const enum fi_progress progress)
{
	if (progress == FI_PROGRESS_MANUAL) {

		/* check if the err list has anything in it and return */
		//if (unlikely(!slist_empty(&opa1x_cq->err))) {
		if (opa1x_cq->err.head != NULL) {
			errno = FI_EAVAIL;
			return -errno;
		}

	} else {
		FI_WARN(fi_opa1x_global.prov, FI_LOG_CQ, "unimplemented\n");
		abort();

	}

	ssize_t num_entries = 0;
	uintptr_t output = (uintptr_t)buf;

	/* examine each context in the pending completion queue and, if the
	 * operation is complete, initialize the cq entry in the application
	 * buffer and remove the context from the queue. */
	union fi_opa1x_context * pending_head = opa1x_cq->pending.head;
	union fi_opa1x_context * pending_tail = opa1x_cq->pending.tail;

//	ssize_t entries_to_write = count;
#if 0
	if (unlikely(!slist_empty(&opa1x_cq->pending))) {

		struct slist_entry *item, *prev;
		struct slist *list = &opa1x_cq->pending;

		for ((prev) = NULL, (item) = (list)->head; (item) && entries_to_write; (prev) = (item), (item) = (item)->next) {	/* see fi_list.h */
			union fi_opa1x_context * context = (union fi_opa1x_context *)item;
			const uint64_t byte_counter = context->byte_counter;

			if (likely(byte_counter == 0)) {
				 output += fi_opa1x_cq_fill(output, context, format);
				++num_entries;
				--entries_to_write;

				if (prev)
					prev->next = item->next;
				else
					list->head = item->next;

				if (!item->next)
					list->tail = prev;
			}
		}
	}
#endif

	if (NULL != pending_head) {
		union fi_opa1x_context * context = pending_head;
		union fi_opa1x_context * prev = NULL;
		while ((count - num_entries) > 0 && context != NULL) {

			const uint64_t byte_counter = context->byte_counter;

			if (byte_counter == 0) {
				output += fi_opa1x_cq_fill(output, context, format);
				++ num_entries;

				if (prev)
					prev->next = context->next;
				else
					/* remove the head */
					pending_head = context->next;

				if (!(context->next))
					/* remove the tail */
					pending_tail = prev;
			}
			else
				prev = context;
			context = context->next;
		}

		/* save the updated pending head and pending tail pointers */
		opa1x_cq->pending.head = pending_head;
		opa1x_cq->pending.tail = pending_tail;
	}


	if (progress == FI_PROGRESS_MANUAL) {
#if 0
		struct slist *list = &opa1x_cq->completed;
		if (!slist_empty(list)) {

			struct slist_entry * item;
			struct slist_entry * __attribute__((unused)) prev;
			slist_foreach(list, item, prev) {
				output += fi_opa1x_cq_fill(output, (union fi_opa1x_context *)item, format);
				++num_entries;
				if (--entries_to_write == 0)
					break;
			}
		}
#endif

		union fi_opa1x_context * head = opa1x_cq->completed.head;
		if (head) {
			union fi_opa1x_context * context = head;
			while ((count - num_entries) > 0 && context != NULL) {
				output += fi_opa1x_cq_fill(output, context, format);
				++ num_entries;
				context = context->next;
			}
			opa1x_cq->completed.head = context;
			if (!context) opa1x_cq->completed.tail = NULL;

		}

	} else {
		FI_WARN(fi_opa1x_global.prov, FI_LOG_CQ, "unimplemented\n");
		abort();
	}

	return num_entries;
}

void fi_opa1x_ep_rx_poll (struct fi_opa1x_ep *ep);

static inline ssize_t fi_opa1x_cq_poll_inline(struct fid_cq *cq, void *buf, size_t count,
		fi_addr_t *src_addr, const enum fi_cq_format format,
		const int lock_required)
{
	ssize_t num_entries = 0;

	struct fi_opa1x_cq *opa1x_cq = (struct fi_opa1x_cq *)cq;

	if (lock_required) { FI_WARN(fi_opa1x_global.prov, FI_LOG_CQ, "unimplemented\n"); abort(); }

	if (IS_PROGRESS_MANUAL(opa1x_cq->domain)) {
		const uint64_t count = opa1x_cq->progress.ep_count;
		uint64_t i;
		for (i=0; i<count; ++i) {
			fi_opa1x_ep_rx_poll(opa1x_cq->progress.ep[i]);
		}

		const uintptr_t tmp_eh = (const uintptr_t)opa1x_cq->err.head;
		const uintptr_t tmp_ph = (const uintptr_t)opa1x_cq->pending.head;
		const uintptr_t tmp_ch = (const uintptr_t)opa1x_cq->completed.head;

		/* check for "all empty" and return */
		if (0 == (tmp_eh | tmp_ph | tmp_ch)) {

			if (lock_required) { FI_WARN(fi_opa1x_global.prov, FI_LOG_CQ, "unimplemented\n"); abort(); }

			errno = FI_EAGAIN;
			return -errno;
		}

		/* check for "fast path" and return (something has completed, but nothing is pending and there are no errors) */
		if (tmp_ch == (tmp_eh | tmp_ph | tmp_ch)) {

			uintptr_t output = (uintptr_t) buf;
#if 0
			size_t entries_to_write = count;

			struct slist_entry * item;
			struct slist_entry * __attribute__((unused)) prev;
			struct slist *list = &opa1x_cq->completed;

			slist_foreach(list, item, prev) {
//fprintf(stderr, "%s:%s():%d ------- completed context = %p\n", __FILE__, __func__, __LINE__, item);
				output += fi_opa1x_cq_fill(output, (union fi_opa1x_context *)item, format);
				++num_entries;
				if (--entries_to_write == 0)
					break;
			}
			if (item == list->tail) {
				list->head = list->tail = NULL;
			} else {
				list->head = item->next;
			}
#endif

			union fi_opa1x_context * context = (union fi_opa1x_context *)tmp_ch;
			while ((count - num_entries) > 0 && context != NULL) {
				output += fi_opa1x_cq_fill(output, context, format);
				++ num_entries;
				context = context->next;
			}
			opa1x_cq->completed.head = context;
			if (!context) opa1x_cq->completed.tail = NULL;

			if (lock_required) { FI_WARN(fi_opa1x_global.prov, FI_LOG_CQ, "unimplemented\n"); abort(); }

//fprintf(stderr, "%s:%s():%d opa1x_cq->completed.head = %p, opa1x_cq->completed.tail = %p, num_entries = %zd\n", __FILE__, __func__, __LINE__, opa1x_cq->completed.head, opa1x_cq->completed.tail, num_entries);
			return num_entries;
		}

		num_entries = fi_opa1x_cq_poll_noinline(opa1x_cq, buf, count, format, FI_PROGRESS_MANUAL);

	} else {
		num_entries = fi_opa1x_cq_poll_noinline(opa1x_cq, buf, count, format, FI_PROGRESS_AUTO);
	}

	if (lock_required) { FI_WARN(fi_opa1x_global.prov, FI_LOG_CQ, "unimplemented\n"); abort(); }

	if (num_entries == 0) {
		errno = FI_EAGAIN;
		return -errno;
	}

	return num_entries;
}


static inline
ssize_t fi_opa1x_cq_read_generic (struct fid_cq *cq, void *buf, size_t count,
		const enum fi_cq_format format, const int lock_required)
{
	int ret;
	ret = fi_opa1x_cq_poll_inline(cq, buf, count, NULL, format, lock_required);
	return ret;
}

static inline
ssize_t fi_opa1x_cq_readfrom_generic (struct fid_cq *cq, void *buf, size_t count, fi_addr_t *src_addr,
		const enum fi_cq_format format, const int lock_required)
{
	int ret;
	ret = fi_opa1x_cq_poll_inline(cq, buf, count, src_addr, format, lock_required);
	if (ret > 0) {
		unsigned n;
		for (n=0; n<ret; ++n) src_addr[n] = FI_ADDR_NOTAVAIL;
	}

	return ret;
}

#ifdef __cplusplus
}
#endif

#endif /* _FI_PROV_OPA1X_EQ_H_ */
