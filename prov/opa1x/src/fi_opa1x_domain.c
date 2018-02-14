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


#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <errno.h>
#include <unistd.h>
#include <limits.h>
#include <fcntl.h>

#include "rdma/fabric.h"

#include "rdma/opa1x/fi_opa1x_domain.h"
#include "rdma/opa1x/fi_opa1x_internal.h"
#include "rdma/opa1x/fi_opa1x_hfi1.h"

#include <ofi_enosys.h>

#include "rdma/opa1x/fi_opa1x.h"

static int fi_opa1x_close_domain(fid_t fid)
{
	FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_DOMAIN, "close domain\n");

	int ret;
	struct fi_opa1x_domain *opa1x_domain =
		container_of(fid, struct fi_opa1x_domain, domain_fid);

	ret = fi_opa1x_fid_check(fid, FI_CLASS_DOMAIN, "domain");
	if (ret)
		return ret;

	fi_opa1x_reliability_service_fini(&opa1x_domain->reliability_service);


	ret = fi_opa1x_finalize_mr_ops(&opa1x_domain->domain_fid);
	if (ret)
		return ret;

	munmap(opa1x_domain->util_shm.ptr, opa1x_domain->util_shm.size);
	shm_unlink(opa1x_domain->util_shm.name);

	ret = fi_opa1x_ref_finalize(&opa1x_domain->ref_cnt, "domain");
	if (ret)
		return ret;

	ret = fi_opa1x_ref_dec(&opa1x_domain->fabric->ref_cnt, "fabric");
	if (ret)
		return ret;


	free(opa1x_domain);

	FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_DOMAIN, "domain closed\n");
	return 0;
}

static struct fi_ops fi_opa1x_fi_ops = {
	.size		= sizeof(struct fi_ops),
	.close		= fi_opa1x_close_domain,
	.bind		= fi_no_bind,
	.control	= fi_no_control,
	.ops_open	= fi_no_ops_open
};

static struct fi_ops_domain fi_opa1x_domain_ops = {
	.size		= sizeof(struct fi_ops_domain),
	.av_open	= fi_opa1x_av_open,
	.cq_open	= fi_opa1x_cq_open,
	.endpoint	= fi_opa1x_endpoint,
	.scalable_ep	= fi_opa1x_scalable_ep,
	.cntr_open	= fi_opa1x_cntr_open,
	.poll_open	= fi_no_poll_open,
	.stx_ctx	= fi_opa1x_stx_context,
	.srx_ctx	= fi_no_srx_context
};


int fi_opa1x_alloc_default_domain_attr(struct fi_domain_attr **domain_attr)
{
	struct fi_domain_attr *attr;

	attr = calloc(1, sizeof(*attr));
	if (!attr)
		goto err;

	uint32_t ppn = 1; /* TODO */

	const unsigned tx_ctx_cnt = 160 / ppn;
	const unsigned rx_ctx_cnt = 160 / ppn;

	attr->domain		= NULL;
	attr->name		= NULL;

	attr->threading		= FABRIC_DIRECT_THREAD;
	attr->control_progress 	= FABRIC_DIRECT_PROGRESS == FI_PROGRESS_UNSPEC ? FI_PROGRESS_MANUAL : FABRIC_DIRECT_PROGRESS;
	attr->data_progress	= FABRIC_DIRECT_PROGRESS == FI_PROGRESS_UNSPEC ? FI_PROGRESS_MANUAL : FABRIC_DIRECT_PROGRESS;
	attr->resource_mgmt	= FI_RM_DISABLED;
	attr->av_type		= FABRIC_DIRECT_AV;
	attr->mr_mode		= FABRIC_DIRECT_MR;
	attr->mr_key_size 	= 2;			/* 2^16 keys */
	attr->cq_data_size 	= FI_OPA1X_REMOTE_CQ_DATA_SIZE;
	attr->cq_cnt		= (size_t)-1;
	attr->ep_cnt		= 1;			/* TODO - what about endpoints that only use a shared receive context and a shared transmit context? */
	attr->tx_ctx_cnt	= tx_ctx_cnt;
	attr->rx_ctx_cnt	= rx_ctx_cnt;

	attr->max_ep_tx_ctx	= 160;
	attr->max_ep_rx_ctx	= FI_OPA1X_ADDR_SEP_RX_MAX;

	attr->max_ep_stx_ctx	= (size_t)-1;
	attr->max_ep_srx_ctx	= 0;
	attr->mr_iov_limit	= 1;

	*domain_attr = attr;

	return 0;
err:
	*domain_attr = NULL;
	errno = FI_ENOMEM;
	return -1;
}

int fi_opa1x_choose_domain(uint64_t caps, struct fi_domain_attr *domain_attr, struct fi_domain_attr *hints)
{
	FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_DOMAIN, "\n");

	if (!domain_attr) {
		FI_DBG(fi_opa1x_global.prov, FI_LOG_DOMAIN, "missing domain attribute structure\n");
		goto err;
	}

	*domain_attr = *fi_opa1x_global.default_domain_attr;

#ifdef FABRIC_DIRECT_ENABLED
	/* Set the data progress mode to the option used in the configure.
 	 * Ignore any setting by the application.
 	 */
	domain_attr->data_progress = FABRIC_DIRECT_PROGRESS == FI_PROGRESS_UNSPEC ? FI_PROGRESS_MANUAL : FABRIC_DIRECT_PROGRESS;

	/* Set the mr_mode to the option used in the configure.
 	 * Ignore any setting by the application - the checkinfo should have verified
 	 * it was set to the same setting.
 	 */
	domain_attr->mr_mode = FABRIC_DIRECT_MR;
#endif

	if (hints) {
		if (hints->domain) {
			struct fi_opa1x_domain *opa1x_domain = opa1x_domain = container_of(hints->domain, struct fi_opa1x_domain, domain_fid);

			domain_attr->threading		= opa1x_domain->threading;
			domain_attr->resource_mgmt	= opa1x_domain->resource_mgmt;
			domain_attr->tx_ctx_cnt		= fi_opa1x_domain_get_tx_max(hints->domain);
			domain_attr->rx_ctx_cnt		= fi_opa1x_domain_get_rx_max(hints->domain);
			domain_attr->max_ep_tx_ctx	= fi_opa1x_domain_get_tx_max(hints->domain);
			domain_attr->max_ep_rx_ctx	= fi_opa1x_domain_get_rx_max(hints->domain);
			domain_attr->max_ep_stx_ctx	= fi_opa1x_domain_get_tx_max(hints->domain);

		} else {

			if (hints->threading)		domain_attr->threading = hints->threading;
			if (hints->control_progress)	domain_attr->control_progress = hints->control_progress;
			if (hints->resource_mgmt)	domain_attr->resource_mgmt = hints->resource_mgmt;
			if (hints->av_type)		domain_attr->av_type = hints->av_type;
			if (hints->mr_key_size)		domain_attr->mr_key_size = hints->mr_key_size;
			if (hints->cq_data_size)	domain_attr->cq_data_size = hints->cq_data_size;
			if (hints->cq_cnt)		domain_attr->cq_cnt = hints->cq_cnt;
			if (hints->ep_cnt)		domain_attr->ep_cnt = hints->ep_cnt;
			if (hints->tx_ctx_cnt)		domain_attr->tx_ctx_cnt = hints->tx_ctx_cnt;
			if (hints->rx_ctx_cnt)		domain_attr->rx_ctx_cnt = hints->rx_ctx_cnt;
			if (hints->max_ep_tx_ctx)	domain_attr->max_ep_tx_ctx = hints->max_ep_tx_ctx;
			if (hints->max_ep_rx_ctx)	domain_attr->max_ep_rx_ctx = hints->max_ep_rx_ctx;
			if (hints->max_ep_stx_ctx)	domain_attr->max_ep_stx_ctx = hints->max_ep_stx_ctx;
			if (hints->max_ep_srx_ctx)	domain_attr->max_ep_srx_ctx = hints->max_ep_srx_ctx;
			if (hints->mr_iov_limit)	domain_attr->mr_iov_limit = hints->mr_iov_limit;
		}
	}

	domain_attr->name = strdup(FI_OPA1X_PROVIDER_NAME);
	if (!domain_attr->name) {
		FI_DBG(fi_opa1x_global.prov, FI_LOG_DOMAIN, "no memory\n");
		errno = FI_ENOMEM;
		return -errno;
	}

	domain_attr->cq_data_size = FI_OPA1X_REMOTE_CQ_DATA_SIZE;

	FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_DOMAIN, "\n");
	return 0;
err:
	errno = FI_EINVAL;
	return -errno;
}

int fi_opa1x_check_domain_attr(struct fi_domain_attr *attr)
{
	switch(attr->threading) {
	case FI_THREAD_UNSPEC:
	case FI_THREAD_SAFE:
	case FI_THREAD_FID:
	case FI_THREAD_ENDPOINT:
	case FI_THREAD_COMPLETION:
	case FI_THREAD_DOMAIN:
		break;
	default:
		FI_DBG(fi_opa1x_global.prov, FI_LOG_DOMAIN,
				"incorrect threading level\n");
		goto err;
	}
	if (attr->control_progress &&
			attr->control_progress != FI_PROGRESS_MANUAL) {
		FI_WARN(fi_opa1x_global.prov, FI_LOG_DOMAIN, "FI_PROGRESS_AUTO not supported\n"); abort();
	}

	if (attr->data_progress == FI_PROGRESS_UNSPEC) {
		attr->data_progress = FABRIC_DIRECT_PROGRESS == FI_PROGRESS_UNSPEC ? FI_PROGRESS_MANUAL : FABRIC_DIRECT_PROGRESS;
	}

	if (FABRIC_DIRECT_PROGRESS == FI_PROGRESS_AUTO) {
		if (attr->data_progress &&
				attr->data_progress == FI_PROGRESS_MANUAL) {
			FI_WARN(fi_opa1x_global.prov, FI_LOG_DOMAIN, "provider configured with data progress mode of FI_PROGRESS_AUTO but application specified FI_PROGRESS_MANUAL\n"); abort();
		}
	} else if (FABRIC_DIRECT_PROGRESS == FI_PROGRESS_MANUAL) {
		if (attr->data_progress &&
				attr->data_progress == FI_PROGRESS_AUTO) {
			FI_WARN(fi_opa1x_global.prov, FI_LOG_DOMAIN, "provider configured with data progress mode of FI_PROGRESS_MANUAL but application specified FI_PROGRESS_AUTO\n"); abort();
		}
	}

	if (attr->mr_mode == FI_MR_UNSPEC) {
		attr->mr_mode = FABRIC_DIRECT_MR == FI_MR_UNSPEC ? FI_MR_BASIC : FABRIC_DIRECT_MR;
	}

	if (attr->mr_key_size) {
		if (attr->mr_key_size > 2) {
			FI_DBG(fi_opa1x_global.prov, FI_LOG_DOMAIN,
					"memory key size too large\n");
			goto err;
		}
	}
	if (attr->cq_data_size) {
		if (attr->cq_data_size > FI_OPA1X_REMOTE_CQ_DATA_SIZE) {
			FI_DBG(fi_opa1x_global.prov, FI_LOG_DOMAIN,
					"max cq data supported is %zu\n",
					FI_OPA1X_REMOTE_CQ_DATA_SIZE);
			goto err;
		}
	}

	return 0;

err:
	errno = FI_EINVAL;
	return -errno;
}

int fi_opa1x_domain(struct fid_fabric *fabric,
		struct fi_info *info,
		struct fid_domain **dom, void *context)
{
	FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_DOMAIN, "open domain\n");

	int ret;
	struct fi_opa1x_domain 	*opa1x_domain = NULL;
	struct fi_opa1x_fabric 	*opa1x_fabric =
		container_of(fabric, struct fi_opa1x_fabric, fabric_fid);

	if (!info) {
		FI_WARN(fi_opa1x_global.prov, FI_LOG_DOMAIN,
				"no info supplied\n");
		errno = FI_EINVAL;
		return -errno;
	}

	ret = fi_opa1x_fid_check(&fabric->fid, FI_CLASS_FABRIC, "fabric");
	if (ret)
		return ret;

	opa1x_domain = calloc(1, sizeof(struct fi_opa1x_domain));
	if (!opa1x_domain) {
		errno = FI_ENOMEM;
		goto err;
	}


	/* fill in default domain attributes */
	opa1x_domain->threading		= fi_opa1x_global.default_domain_attr->threading;
	opa1x_domain->resource_mgmt	= fi_opa1x_global.default_domain_attr->resource_mgmt;
	opa1x_domain->data_progress	= fi_opa1x_global.default_domain_attr->data_progress;

	if (info->domain_attr) {
		if (info->domain_attr->domain) {
			FI_WARN(fi_opa1x_global.prov, FI_LOG_DOMAIN,
					"domain cannot be supplied\n");
			goto err;
		}
		ret = fi_opa1x_check_domain_attr(info->domain_attr);
		if (ret)
			goto err;
		opa1x_domain->threading = info->domain_attr->threading;
		opa1x_domain->resource_mgmt = info->domain_attr->resource_mgmt;
		if (FABRIC_DIRECT_PROGRESS == FI_PROGRESS_UNSPEC) {
			opa1x_domain->data_progress = info->domain_attr->data_progress;
		}
	}

	opa1x_domain->fabric = opa1x_fabric;

	fi_opa1x_ref_init(&opa1x_domain->ref_cnt, "domain");

	opa1x_domain->domain_fid.fid.fclass  = FI_CLASS_DOMAIN;
	opa1x_domain->domain_fid.fid.context = context;
	opa1x_domain->domain_fid.fid.ops     = &fi_opa1x_fi_ops;
	opa1x_domain->domain_fid.ops	   = &fi_opa1x_domain_ops;

	/* Todo: OFI env variables handling */
	char * env_var_uuid = getenv("FI_OPA1X_UUID");

	if (env_var_uuid) {
		strncpy(opa1x_domain->unique_job_key_str, env_var_uuid, sizeof(opa1x_domain->unique_job_key_str));
	} else {
		const char default_uuid[64] = "00112233-4455-6677-8899-aabbccddeeff";
		strncpy(opa1x_domain->unique_job_key_str, default_uuid, sizeof(opa1x_domain->unique_job_key_str));
	}

	sscanf(opa1x_domain->unique_job_key_str, "%02hhx%02hhx%02hhx%02hhx-%02hhx%02hhx-%02hhx%02hhx-%02hhx%02hhx-%02hhx%02hhx%02hhx%02hhx%02hhx%02hhx",
		&opa1x_domain->unique_job_key[0],
		&opa1x_domain->unique_job_key[1],
		&opa1x_domain->unique_job_key[2],
		&opa1x_domain->unique_job_key[3],
		&opa1x_domain->unique_job_key[4],
		&opa1x_domain->unique_job_key[5],
		&opa1x_domain->unique_job_key[6],
		&opa1x_domain->unique_job_key[7],
		&opa1x_domain->unique_job_key[8],
		&opa1x_domain->unique_job_key[9],
		&opa1x_domain->unique_job_key[10],
		&opa1x_domain->unique_job_key[11],
		&opa1x_domain->unique_job_key[12],
		&opa1x_domain->unique_job_key[13],
		&opa1x_domain->unique_job_key[14],
		&opa1x_domain->unique_job_key[15]);

	FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_DOMAIN,
		"using uuid \"%s\"%s\n", opa1x_domain->unique_job_key_str, env_var_uuid ? " from FI_OPA1X_UUID environment variable" : "");

	snprintf(opa1x_domain->util_shm.name, sizeof(opa1x_domain->util_shm.name), "/fi_opa1x_domain.%s", opa1x_domain->unique_job_key_str);
	size_t size = (sizeof(struct fi_opa1x_node) + 4096) & 0xFFFFFFFFFFFFF000u;
	opa1x_domain->util_shm.size = size;

	FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_DOMAIN,
		"shared memory name (\"%s\"), sizeof(opa1x_domain->util_shm.name) = %zu\n", opa1x_domain->util_shm.name, sizeof(opa1x_domain->util_shm.name));

	int fd = -1;
	fd = shm_open(opa1x_domain->util_shm.name, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
	if (fd < 0) {
		FI_WARN(fi_opa1x_global.prov, FI_LOG_DOMAIN,
			"unable to open shared memory (\"%s\")\n", opa1x_domain->util_shm.name);
		errno = FI_ENOMEM;
		return -errno;
	}

	if (ftruncate(fd, size) == -1) {
		FI_WARN(fi_opa1x_global.prov, FI_LOG_DOMAIN,
			"unable to set size of shared memory (%zu)\n", size);
		errno = FI_ENOMEM;
		return -errno;
	}

	opa1x_domain->util_shm.ptr =
		mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
	if (opa1x_domain->util_shm.ptr == MAP_FAILED) {
		FI_WARN(fi_opa1x_global.prov, FI_LOG_DOMAIN,
			"map of shared memory failed (%zu, PROT_READ | PROT_WRITE, MAP_SHARED)\n", size);
		errno = FI_ENOMEM;
		return -errno;
	}

	close(fd);

	opa1x_domain->node = (struct fi_opa1x_node *)opa1x_domain->util_shm.ptr;


	opa1x_domain->rx_count = 0;
	opa1x_domain->tx_count = 0;
	opa1x_domain->ep_count = 0;

	ret = fi_opa1x_init_mr_ops(&opa1x_domain->domain_fid, info);
	if (ret)
		goto err;

	opa1x_domain->reliability_rx =
		fi_opa1x_reliability_service_init(&opa1x_domain->reliability_service, opa1x_domain->unique_job_key);

	fi_opa1x_ref_inc(&opa1x_fabric->ref_cnt, "fabric");

	*dom = &opa1x_domain->domain_fid;

	FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_DOMAIN, "domain opened\n");
	return 0;

err:
	fi_opa1x_finalize_mr_ops(&opa1x_domain->domain_fid);
	if (opa1x_domain)
		free(opa1x_domain);
	return -errno;
}
