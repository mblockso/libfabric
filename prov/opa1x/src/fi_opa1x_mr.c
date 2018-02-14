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
#include "rdma/opa1x/fi_opa1x.h"
#include "rdma/opa1x/fi_opa1x_internal.h"

#include <ofi_enosys.h>

static int fi_opa1x_close_mr(fid_t fid)
{
	struct fi_opa1x_domain *opa1x_domain;
	struct fi_opa1x_mr *opa1x_mr = (struct fi_opa1x_mr *) fid;

	opa1x_domain = opa1x_mr->domain;

	if (opa1x_domain->mr_mode == FI_MR_SCALABLE) {
	int ret;
//	fi_opa1x_domain_bat_clear(opa1x_domain, opa1x_mr->mr_fid.key);

	ret = fi_opa1x_ref_dec(&opa1x_domain->ref_cnt, "domain");
	if (ret) return ret;
	}
	free(opa1x_mr);
	return 0;
}

static int fi_opa1x_bind_mr(struct fid *fid,
		struct fid *bfid, uint64_t flags)
{
	int ret;
	struct fi_opa1x_mr *opa1x_mr =
		(struct fi_opa1x_mr *) fid;
	struct fi_opa1x_cntr *opa1x_cntr;

	ret = fi_opa1x_fid_check(fid, FI_CLASS_MR, "memory region");
	if (ret)
		return ret;

	switch (bfid->fclass) {
	case FI_CLASS_CNTR:
		opa1x_cntr = (struct fi_opa1x_cntr *) bfid;
		opa1x_mr->cntr = opa1x_cntr;
		opa1x_mr->cntr_bflags = flags;
		break;
	default:
		errno = FI_ENOSYS;
		return -errno;
	}
	return 0;
}

static struct fi_ops fi_opa1x_fi_ops = {
	.size		= sizeof(struct fi_ops),
	.close		= fi_opa1x_close_mr,
	.bind		= fi_opa1x_bind_mr,
	.control	= fi_no_control,
	.ops_open	= fi_no_ops_open
};

static int fi_opa1x_mr_reg(struct fid *fid, const void *buf,
		size_t len, uint64_t access, uint64_t offset,
		uint64_t requested_key, uint64_t flags,
		struct fid_mr **mr, void *context)
{
	int ret;

	struct fi_opa1x_mr *opa1x_mr;
	struct fi_opa1x_domain *opa1x_domain;

	if (!fid || !mr) {
		errno = FI_EINVAL;
		return -errno;
	}

	ret = fi_opa1x_fid_check(fid, FI_CLASS_DOMAIN, "domain");
	if (ret) return ret;

	if (flags != 0) {
		FI_LOG(fi_opa1x_global.prov, FI_LOG_DEBUG, FI_LOG_MR,
				"Flags for fi_mr_reg must be 0\n");
		errno = FI_EINVAL;
		return -errno;
	}

	opa1x_domain = (struct fi_opa1x_domain *) container_of(fid, struct fid_domain, fid);

	if (opa1x_domain->mr_mode == FI_MR_SCALABLE) {
		if (requested_key >= opa1x_domain->num_mr_keys) {
			/* requested key is too large */
			errno = FI_EKEYREJECTED;
			return -errno;
		}
	}
	opa1x_mr = calloc(1, sizeof(*opa1x_mr));
	if (!opa1x_mr) {
		errno = FI_ENOMEM;
		return -errno;
	}

	opa1x_mr->mr_fid.fid.fclass	= FI_CLASS_MR;
	opa1x_mr->mr_fid.fid.context	= context;
	opa1x_mr->mr_fid.fid.ops		= &fi_opa1x_fi_ops;
	if (opa1x_domain->mr_mode == FI_MR_SCALABLE) {
		opa1x_mr->mr_fid.key		= requested_key;
	}
//	else if (opa1x_domain->mr_mode == FI_MR_BASIC) {

//		uint64_t paddr = 0;

//		fi_opa1x_cnk_vaddr2paddr(buf,1,&paddr);
//		opa1x_mr->mr_fid.key		= ((uint64_t)buf - paddr);
//#ifdef FI_OPA1X_TRACE
  //      fprintf(stderr,"fi_opa1x_mr_reg - FI_MR_BASIC virtual addr is 0x%016lx physical addr is 0x%016lx key is %lu  \n",(uint64_t)buf,paddr,(uint64_t)((uint64_t)buf - paddr));
//fflush(stderr);

//#endif

//	}
	opa1x_mr->buf 	= buf;
	opa1x_mr->len	= len;
	opa1x_mr->offset	= offset;
	opa1x_mr->access	= FI_SEND | FI_RECV | FI_READ | FI_WRITE | FI_REMOTE_READ | FI_REMOTE_WRITE;
	opa1x_mr->flags	= flags;
	opa1x_mr->domain  = opa1x_domain;

	if (opa1x_domain->mr_mode == FI_MR_SCALABLE) {
//		fi_opa1x_domain_bat_write(opa1x_domain, requested_key, buf, len);

		fi_opa1x_ref_inc(&opa1x_domain->ref_cnt, "domain");
	}

	*mr = &opa1x_mr->mr_fid;

	return 0;
}

int fi_opa1x_bind_ep_mr(struct fid_ep *ep,
		struct fid_mr *mr, uint64_t flags)
{
	return 0;
}

static struct fi_ops_mr fi_opa1x_mr_ops = {
	.size		= sizeof(struct fi_ops_mr),
	.reg 		= fi_opa1x_mr_reg,
	.regv 		= fi_no_mr_regv,
	.regattr	= fi_no_mr_regattr
};

int fi_opa1x_init_mr_ops(struct fid_domain *domain, struct fi_info *info)
{
	if (!domain || !info) {
		goto err;
	}

	struct fi_opa1x_domain *opa1x_domain =
		container_of(domain, struct fi_opa1x_domain, domain_fid);

	if (info->domain_attr->mr_mode == FI_MR_UNSPEC) goto err; 

	opa1x_domain->domain_fid.mr	   = &fi_opa1x_mr_ops;

	opa1x_domain->mr_mode = info->domain_attr->mr_mode;

	if (opa1x_domain->mr_mode == FI_MR_SCALABLE) {
	opa1x_domain->num_mr_keys = (1<<(8*info->domain_attr->mr_key_size));
//	opa1x_domain->bat = (struct fi_opa1x_bat_entry *) calloc(opa1x_domain->num_mr_keys, sizeof(struct fi_opa1x_bat_entry));

	}
	return 0;
err:
	errno = FI_EINVAL;
	return -errno;
}

int fi_opa1x_finalize_mr_ops(struct fid_domain *domain)
{
	struct fi_opa1x_domain *opa1x_domain =
		container_of(domain, struct fi_opa1x_domain, domain_fid);

	if (opa1x_domain->mr_mode == FI_MR_SCALABLE) {
//	free((void*)opa1x_domain->bat);
//	opa1x_domain->bat = (void*)NULL;
	opa1x_domain->num_mr_keys = 0;
	}
	return 0;
}
