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

#include "rdma/opa1x/fi_opa1x.h"
#include "rdma/opa1x/fi_opa1x_internal.h"
#include "rdma/opa1x/fi_opa1x_hfi1.h"
#include "ofi_prov.h"

#include "rdma/opa1x/fi_opa1x_addr.h"

#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

static int fi_opa1x_init;
static int fi_opa1x_count;

int fi_opa1x_check_info(const struct fi_info *info)
{
FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_EP_DATA, "\n");

	int ret;
//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
	/* TODO: check caps, mode */

	if ((info->tx_attr) && ((info->tx_attr->caps | info->caps) != info->caps)) {
FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_EP_DATA, "info->tx_attr->caps = 0x%016lx, info->caps = 0x%016lx\n", info->tx_attr->caps, info->caps);
		FI_LOG(fi_opa1x_global.prov, FI_LOG_DEBUG, FI_LOG_FABRIC,
				"The tx_attr capabilities (0x%016lx) must be a subset of those requested of the associated endpoint (0x%016lx)",
				info->tx_attr->caps, info->caps);
//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
		goto err;
	}

FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_EP_DATA, "\n");

//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
	if ((info->rx_attr) && ((info->rx_attr->caps | info->caps) != info->caps)) {
FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_EP_DATA, "info->rx_attr->caps = 0x%016lx, info->caps = 0x%016lx, (info->rx_attr->caps | info->caps) = 0x%016lx, ((info->rx_attr->caps | info->caps) ^ info->caps) = 0x%016lx\n", info->rx_attr->caps, info->caps, (info->rx_attr->caps | info->caps), ((info->rx_attr->caps | info->caps) ^ info->caps));
		FI_LOG(fi_opa1x_global.prov, FI_LOG_DEBUG, FI_LOG_FABRIC,
				"The rx_attr capabilities (0x%016lx) must be a subset of those requested of the associated endpoint (0x%016lx)",
				info->rx_attr->caps, info->caps);
//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
		goto err;
	}

FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_EP_DATA, "\n");


//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
	switch (info->addr_format) {
	case FI_ADDR_OPA1X:
	case FI_FORMAT_UNSPEC:
		break;
	default:
//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
		FI_LOG(fi_opa1x_global.prov, FI_LOG_DEBUG, FI_LOG_FABRIC,
				"unavailable [bad info->addr_format (%u)]",
				info->addr_format);
		goto err;
	}

FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_EP_DATA, "\n");

//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
	if (info->tx_attr) {
//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
		ret = fi_opa1x_check_tx_attr(info->tx_attr);
		if (ret)
			return ret;
	}

FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_EP_DATA, "\n");

//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
	if (info->rx_attr) {
//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
		ret = fi_opa1x_check_rx_attr(info->rx_attr);
		if (ret)
			return ret;
	}

FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_EP_DATA, "\n");

//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
	if (info->ep_attr) {
//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
		ret = fi_opa1x_check_ep_attr(info->ep_attr);
		if (ret)
			return ret;
	}

FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_EP_DATA, "\n");

//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
	if (info->domain_attr) {
//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
		ret = fi_opa1x_check_domain_attr(info->domain_attr);
		if (ret)
			return ret;
	}
//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_EP_DATA, "\n");

	if (info->fabric_attr) {
//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
		ret = fi_opa1x_check_fabric_attr(info->fabric_attr);
		if (ret)
			return ret;
	}

FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_EP_DATA, "\n");

//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
	return 0;

err:
//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_EP_DATA, "\n");

	errno = FI_ENODATA;
	return -errno;
}

static int fi_opa1x_fillinfo(struct fi_info *fi, const char *node,
		const char* service, const struct fi_info *hints,
	        uint64_t flags)
{
	int ret;
	uint64_t caps;

	if (!fi)
		goto err;

	if (!hints && !node && !service)
		goto err;

	if (hints && ((hints->mode & FI_CONTEXT) != 0) && ((hints->mode & FI_CONTEXT2) == 0)) {
		FI_WARN(fi_opa1x_global.prov, FI_LOG_FABRIC,
			"FI_CONTEXT mode is not supported. Use FI_CONTEXT2 mode instead.\n");
		errno = FI_ENODATA;
		return -errno;
	}

	fi->next = NULL;
	fi->caps = FI_OPA1X_DEFAULT_CAPS;

	/* set the mode that we require */
	fi->mode = FI_ASYNC_IOV;
	fi->mode |= (FI_CONTEXT2);

	/* clear modes that we do not require */
	fi->mode &= (~FI_LOCAL_MR);
	fi->mode &= (~FI_MSG_PREFIX);

	fi->addr_format = FI_ADDR_OPA1X;
	fi->src_addrlen = sizeof(union fi_opa1x_addr);
	fi->dest_addrlen = sizeof(union fi_opa1x_addr);

	if (flags & FI_SOURCE) {
		if ((hints != NULL) && (hints->dest_addr)) {
			FI_LOG(fi_opa1x_global.prov, FI_LOG_DEBUG, FI_LOG_FABRIC,
				"cannot support dest_addr lookups now\n");
			errno = FI_ENOSYS;
			return -errno;
		}

		fi->src_addr = strdup(service);
		if (!fi->src_addr) {
			goto err;
		}
	}

	if ((hints != NULL) && (hints->dest_addr != NULL) && (((node == NULL) && (service == NULL)) || (flags & FI_SOURCE))) {

		/*
		 * man/fi_getinfo.3
		 *
		 * dest_addr - destination address
		 * If specified, indicates the destination address. This field
		 * will be ignored in hints unless the node and service
		 * parameters are NULL or FI_SOURCE flag is set. If FI_SOURCE
		 * is not specified, on output a provider shall return an
		 * address the corresponds to the indicated node and/or service
		 * fields, relative to the fabric and domain. Note that any
		 * returned address is only usable locally.
		 */

		if ((flags & FI_SOURCE) == 0) {
			if ((hints->addr_format != FI_FORMAT_UNSPEC) &&
				(hints->addr_format != FI_ADDR_OPA1X)) {

				FI_WARN(fi_opa1x_global.prov, FI_LOG_FABRIC,
					"invalid addr_format hint (%d)\n", hints->addr_format);
				errno = FI_EINVAL;
				return -errno;
			}
		}
	}

	fi->dest_addr = NULL;

	/*
	 * man/fi_fabric.3
	 *
	 * On input to fi_getinfo, a user may set this (fi_fabric_attr::fabric)
	 * to an opened fabric instance to restrict output to the given fabric.
	 * On output from fi_getinfo, if no fabric was specified, but the user
	 * has an opened instance of the named fabric, this (fi_fabric_attr::fabric)
	 * will reference the first opened instance. If no instance has been
	 * opened, this field will be NULL.
	 */

	fi->fabric_attr->name = strdup(FI_OPA1X_FABRIC_NAME);
	if (!fi->fabric_attr->name) {
		FI_LOG(fi_opa1x_global.prov, FI_LOG_DEBUG, FI_LOG_FABRIC,
				"memory allocation failed");
		goto err;
	}

	fi->fabric_attr->prov_version = FI_OPA1X_PROVIDER_VERSION;

	memcpy(fi->tx_attr, fi_opa1x_global.default_tx_attr, sizeof(*fi->tx_attr));

	if (hints->tx_attr) {

		/*
		 * man/fi_endpoint.3
		 *
		 *   fi_tx_attr::caps
		 *
		 *   "... If the caps field is 0 on input to fi_getinfo(3), the
		 *   caps value from the fi_info structure will be used."
		 */
		if (hints->tx_attr->caps) {
			fi->tx_attr->caps = hints->tx_attr->caps;
		}

		/* adjust parameters down from what requested if required */
		fi->tx_attr->op_flags = hints->tx_attr->op_flags;
	} else if (hints->caps) {
		fi->tx_attr->caps = hints->caps;
	}

	memcpy(fi->rx_attr, fi_opa1x_global.default_rx_attr, sizeof(*fi->rx_attr));
	if (hints->rx_attr) {

		/*
		 * man/fi_endpoint.3
		 *
		 *   fi_rx_attr::caps
		 *
		 *   "... If the caps field is 0 on input to fi_getinfo(3), the
		 *   caps value from the fi_info structure will be used."
		 */
		if (hints->rx_attr->caps) {
			fi->rx_attr->caps = hints->rx_attr->caps;
		}

		/* adjust parameters down from what requested if required */
		fi->rx_attr->op_flags = hints->rx_attr->op_flags;
		if (hints->rx_attr->total_buffered_recv > 0 &&
			hints->rx_attr->total_buffered_recv < fi_opa1x_global.default_rx_attr->total_buffered_recv)
				fi->rx_attr->total_buffered_recv = hints->rx_attr->total_buffered_recv;
	} else if (hints->caps) {
		fi->rx_attr->caps = hints->caps;
	}

	caps = fi->caps | fi->tx_attr->caps | fi->rx_attr->caps;

	/*
	 * man/fi_domain.3
	 *
	 * On input to fi_getinfo, a user may set this (fi_domain_attr::domain)
	 * to an opened domain instance to restrict output to the given domain.
	 * On output from fi_getinfo, if no domain was specified, but the user
	 * has an opened instance of the named domain, this (fi_domain_attr::domain)
	 * will reference the first opened instance. If no instance has been
	 * opened, this field will be NULL.
	 */

	ret = fi_opa1x_choose_domain(caps, fi->domain_attr, hints->domain_attr);
	if (ret) {
		FI_LOG(fi_opa1x_global.prov, FI_LOG_DEBUG, FI_LOG_FABRIC,
				"cannot find appropriate domain\n");
		goto err;
	}

	memcpy(fi->ep_attr, fi_opa1x_global.default_ep_attr, sizeof(*fi->ep_attr));
	if (hints->ep_attr) {
		/* adjust parameters down from what requested if required */
		fi->ep_attr->type	= hints->ep_attr->type;
		if (hints->ep_attr->max_msg_size > 0 &&
			hints->ep_attr->max_msg_size <= fi_opa1x_global.default_ep_attr->max_msg_size)
				fi->ep_attr->max_msg_size = hints->ep_attr->max_msg_size;

		if (0 != hints->ep_attr->tx_ctx_cnt && hints->ep_attr->tx_ctx_cnt <= fi->ep_attr->tx_ctx_cnt)
			fi->ep_attr->tx_ctx_cnt = hints->ep_attr->tx_ctx_cnt;	/* TODO - check */

		if (0 != hints->ep_attr->rx_ctx_cnt && hints->ep_attr->rx_ctx_cnt <= fi->ep_attr->rx_ctx_cnt)
			fi->ep_attr->rx_ctx_cnt = hints->ep_attr->rx_ctx_cnt;	/* TODO - check */
	}

	return 0;
err:
	if (fi->domain_attr->name) free(fi->domain_attr->name);
	if (fi->fabric_attr->name) free(fi->fabric_attr->name);
	if (fi->fabric_attr->prov_name) free(fi->fabric_attr->prov_name);
	errno = FI_ENODATA;
	return -errno;
}

struct fi_opa1x_global_data fi_opa1x_global;

static int fi_opa1x_getinfo(uint32_t version, const char *node,
		const char *service, uint64_t flags,
		const struct fi_info *hints, struct fi_info **info)
{
//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);

	int ret;
	struct fi_info *fi;//, *prev_fi, *curr;

//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
	if (!fi_opa1x_count) {
		errno = FI_ENODATA;
		return -errno;
	}

//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
	if (hints) {
//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
		ret = fi_opa1x_check_info(hints);
//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
		if (ret) {
			return ret;
		}
//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
		if (!(fi = fi_allocinfo())) {
			return -FI_ENOMEM;
		}
//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
//fprintf(stderr, "%s:%s():%d fi->fabric_attr->prov_name = '%s'\n", __FILE__, __func__, __LINE__, fi->fabric_attr->prov_name);
//fprintf(stderr, "%s:%s():%d hints->fabric_attr->prov_name = '%s'\n", __FILE__, __func__, __LINE__, hints->fabric_attr->prov_name);
		if (fi_opa1x_fillinfo(fi, node, service,
					hints, flags)) {
			return -errno;
		}
//fprintf(stderr, "%s:%s():%d fi->fabric_attr->prov_name = '%s'\n", __FILE__, __func__, __LINE__, fi->fabric_attr->prov_name);
		*info = fi;
//fprintf(stderr, "%s:%s():%d (*info)->fabric_attr->prov_name = '%s'\n", __FILE__, __func__, __LINE__, (*info)->fabric_attr->prov_name);
//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
	} else {
//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
		if(node || service) {
			errno = FI_ENODATA;
			return -errno;
		} else {
//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
			if (!(fi = fi_dupinfo(fi_opa1x_global.info))) {
//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
				return -FI_ENOMEM;
			}
			*info = fi;
		}
	}
//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);

//fprintf(stderr, "%s:%s():%d (*info)->fabric_attr->prov_name = '%s'\n", __FILE__, __func__, __LINE__, (*info)->fabric_attr->prov_name);
//fprintf(stderr, "%s:%s():%d (*info) = %p\n", __FILE__, __func__, __LINE__, (*info));
//fprintf(stderr, "%s:%s():%d (*info)->fabric_attr = %p\n", __FILE__, __func__, __LINE__, (*info)->fabric_attr);
//fprintf(stderr, "%s:%s():%d (*info)->fabric_attr->prov_name = %p\n", __FILE__, __func__, __LINE__, (*info)->fabric_attr->prov_name);
	return 0;
}

static void fi_opa1x_fini()
{
	always_assert(fi_opa1x_init == 1,
		"OPA1X provider finalize called before initialize\n");
	fi_freeinfo(fi_opa1x_global.info);
}

static struct fi_provider fi_opa1x_provider = {
	.name 		= FI_OPA1X_PROVIDER_NAME,
	.version 	= FI_VERSION(0, 1),
	.fi_version 	= FI_VERSION(1,6),
	.getinfo	= fi_opa1x_getinfo,
	.fabric		= fi_opa1x_fabric,
	.cleanup	= fi_opa1x_fini
};

OPA1X_INI
{
	fi_opa1x_count = 1;
	fi_opa1x_set_default_info(); // TODO: fold into fi_opa1x_set_defaults

	if (fi_opa1x_alloc_default_domain_attr(&fi_opa1x_global.default_domain_attr)) {
		return NULL;
	}

	if (fi_opa1x_alloc_default_ep_attr(&fi_opa1x_global.default_ep_attr)) {
		return NULL;
	}

	if (fi_opa1x_alloc_default_tx_attr(&fi_opa1x_global.default_tx_attr)) {
		return NULL;
	}

	if (fi_opa1x_alloc_default_rx_attr(&fi_opa1x_global.default_rx_attr)) {
		return NULL;
	}

	fi_opa1x_global.prov = &fi_opa1x_provider;

	fi_opa1x_init = 1;

	return (&fi_opa1x_provider);
}
