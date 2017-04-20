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
#ifndef _FI_PROV_OPA1X_INTERNAL_H_
#define _FI_PROV_OPA1X_INTERNAL_H_

#include <config.h>

#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdio.h>

#define FI_OPA1X_CACHE_LINE_SIZE	(64)

#if 1
#define FI_OPA1X_CQ_CONTEXT_EXT		(0x8000000000000000ull)
#define FI_OPA1X_CQ_CONTEXT_MULTIRECV	(0x4000000000000000ull)

union fi_opa1x_context {
	struct fi_context2			context;
	struct {
		//struct slist_entry		entry;		/* fi_cq_entry::op_context */
		union fi_opa1x_context *	next;		/* fi_cq_entry::op_context */
		uint64_t			flags;		/* fi_cq_msg_entry::flags */
		size_t				len;		/* fi_cq_msg_entry::len */
		void				*buf;		/* fi_cq_data_entry::buf (unused for tagged cq's and non-multi-receive message cq's) */

		union {
			uint64_t		data;		/* fi_cq_data_entry::data; only used _after_ a message is matched */
			fi_addr_t		src_addr;	/* only used _before_ a message is matched ('FI_DIRECTED_RECEIVE') */
		};

		union {
			uint64_t		tag;		/* fi_cq_tagged_entry::tag */
			union fi_opa1x_context	*multi_recv_next;	/* only for multi-receives; which is not tagged */
		};
		union {
			uint64_t		ignore;		/* only for tagged receive */
			void 			*claim;		/* only for peek/claim */
			void			*multi_recv_context;	/* only for individual FI_MULTI_RECV's */
		};

		volatile uint64_t	byte_counter;
	};
};

struct fi_opa1x_context_slist {
	union fi_opa1x_context *	head;
	union fi_opa1x_context *	tail;
};

static inline void fi_opa1x_context_slist_init (struct fi_opa1x_context_slist* list)
{
	list->head = list->tail = NULL;
}

static inline int fi_opa1x_context_slist_empty (struct fi_opa1x_context_slist* list)
{
	return !list->head;
}

static inline void fi_opa1x_context_slist_insert_head (union fi_opa1x_context *item,
		struct fi_opa1x_context_slist* list)
{
	assert(item->next == NULL);
	if (fi_opa1x_context_slist_empty(list))
		list->tail = item;
	else
		item->next = list->head;

	list->head = item;
}

static inline void fi_opa1x_context_slist_insert_tail (union fi_opa1x_context *item,
		struct fi_opa1x_context_slist* list)
{
	assert(item->next == NULL);
	if (fi_opa1x_context_slist_empty(list))
		list->head = item;
	else
		list->tail->next = item;

	list->tail = item;
}

static inline void fi_opa1x_context_slist_debug (struct fi_opa1x_context_slist* list,
		const char * file, const char * func, const int line)
{
	char str[2048];
	char *s = str;
	*s = 0;
	size_t len = sizeof(str)-1;
	int n = 0;

	union fi_opa1x_context *item = list->head;
	while (item != NULL) {
		if (item->next == NULL) {
			n = snprintf(s, len, "%p -> %p.", item, NULL);
		} else {
			n = snprintf(s, len, "%p -> ", item);
		}
		s += n;
		len -= n;
		item = item->next;
	}
	fprintf(stderr, "%s:%s():%d %s() list = %p (%p, %p) %s\n", file, func, line, __func__, list, list->head, list->tail, str);
}


struct fi_opa1x_context_ext {
	union fi_opa1x_context		opa1x_context;
	struct fi_cq_err_entry		err_entry;
	struct {
		struct fi_context	*op_context;
		size_t			iov_count;
		struct iovec		*iov;
	} msg;
};
#endif


#ifndef MIN
#define MIN(a,b) (b^((a^b)&-(a<b)))
#endif
#ifndef MIN3
#define MIN3(a,b,c) (MIN(MIN(a,b),c))
#endif
#ifndef MIN4
#define MIN4(a,b,c,d) (MIN(MIN(a,b),MIN(c,d)))
#endif

#if 0
static inline void always_assert (bool val, char *msg)
{
	if (!val) {
		FI_LOG(fi_opa1x_global.prov, FI_LOG_DEBUG, FI_LOG_FABRIC,
				"%s\n", msg);
		exit(EXIT_FAILURE);
	}
}

static inline void fi_opa1x_ref_init (ofi_atomic32_t *ref, char *name)
{
	ofi_atomic_initialize32(ref, 0);
	FI_LOG(fi_opa1x_global.prov, FI_LOG_DEBUG, FI_LOG_FABRIC,
			"initializing ref count for (%s) to (%d)\n",
			name, 0);

	return;
}

static inline void fi_opa1x_ref_inc (ofi_atomic32_t *ref, char *name)
{
	ofi_atomic_inc32(ref);
	return;
}

static inline int fi_opa1x_ref_dec (ofi_atomic32_t *ref, char *name)
{
	int64_t value = ofi_atomic_dec32(ref);
	if (value < 0) {

		FI_WARN(fi_opa1x_global.prov, FI_LOG_FABRIC,
			"decrement ref for (%s) (ref_cnt %d < 0)\n",
			name, value);

		errno = FI_EOTHER;
		return -errno;
	}
	return 0;
}

static inline int fi_opa1x_ref_finalize (ofi_atomic32_t *ref, char *name)
{
	int64_t value = ofi_atomic_get32(ref);
	if (value != 0) {
		FI_WARN(fi_opa1x_global.prov, FI_LOG_FABRIC,
			"error ref for (%s) (ref_cnt %d != 0)\n",
			name, value);
		errno = FI_EBUSY;
		return -errno;
	}
	return 0;
}
#endif
//static inline int fi_opa1x_lock_if_required (fastlock_t *lock, const int required)
//{
//	if (required) fastlock_acquire(lock);
//	return 0;
//}
//
//static inline int fi_opa1x_unlock_if_required (fastlock_t *lock, const int required)
//{
//	if (required) fastlock_release(lock);
//	return 0;
//}
#if 0
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
			"wrong type of object (%s) expected (%d), got (%d)\n",
			name, fid_class, fid->fclass);
		errno = FI_EINVAL;
		return -errno;
	}
	return 0;
}
#endif
#endif /* _FI_PROV_OPA1X_INTERNAL_H_ */
