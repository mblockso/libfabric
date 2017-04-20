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
#ifndef _FI_PROV_OPA1X_DOMAIN_H_
#define _FI_PROV_OPA1X_DOMAIN_H_

#include <unistd.h>
#include <stdint.h>
#include <pthread.h>
#include <uuid/uuid.h>

#include "rdma/fi_domain.h"

#include "rdma/opa1x/fi_opa1x_reliability.h"


#ifdef __cplusplus
extern "C" {
#endif

struct fi_opa1x_ep;	/* forward declaration */


struct fi_opa1x_fabric {
	struct fid_fabric	fabric_fid;

	int64_t		ref_cnt;
};


struct fi_opa1x_node {
	volatile uint64_t	ep_count;
};

struct fi_opa1x_domain {
	struct fid_domain	domain_fid;
	struct fi_opa1x_fabric	*fabric;

	enum fi_threading	threading;
	enum fi_resource_mgmt	resource_mgmt;
	enum fi_mr_mode		mr_mode;
	enum fi_progress	data_progress;

	uuid_t			unique_job_key;
	char			unique_job_key_str[64];

	uint32_t		rx_count;
	uint32_t		tx_count;
	uint8_t			ep_count;

	uint64_t		num_mr_keys;


	struct fi_opa1x_reliability_service	reliability_service;
	uint8_t					reliability_rx;

	struct {
		char		name[256];
		void *		ptr;
		size_t		size;
	} util_shm;

	struct fi_opa1x_node *	node;

	int64_t		ref_cnt;
};

struct fi_opa1x_av {

	/* == CACHE LINE 0 == */

	struct fid_av		av_fid;		/* 32 bytes */
	struct fi_opa1x_domain	*domain;
	void			*map_addr;
	int64_t		ref_cnt;
	uint32_t		addr_count;
	enum fi_av_type		type;
	unsigned		ep_tx_count;

	/* == CACHE LINE 1..20 == */

	struct fi_opa1x_ep	*ep_tx[160];

	/* == ALL OTHER CACHE LINES == */

	union fi_opa1x_addr *	table_addr;
	uint64_t		rx_ctx_bits;
};

struct fi_opa1x_mr {
	struct fid_mr		mr_fid;
	struct fi_opa1x_domain	*domain;
	const void		*buf;
	size_t			len;
	size_t			offset;
	uint64_t		access;
	uint64_t		flags;
	uint64_t		cntr_bflags;
	struct fi_opa1x_cntr	*cntr;
	struct fi_opa1x_ep	*ep;
};

static inline uint32_t
fi_opa1x_domain_get_tx_max(struct fid_domain *domain) {
	return 160;
}

static inline uint32_t
fi_opa1x_domain_get_rx_max(struct fid_domain *domain) {
	return 160;
}

#ifdef __cplusplus
}
#endif

#endif /* _FI_PROV_OPA1X_DOMAIN_H_ */
