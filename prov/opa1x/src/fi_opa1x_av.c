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
#include "rdma/opa1x/fi_opa1x.h"

#include <ofi_enosys.h>

#include <stdio.h>
#include <arpa/inet.h>
#include <string.h>

static int fi_opa1x_close_av(fid_t fid)
{
	FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_AV, "close av\n");

	int ret;
	struct fi_opa1x_av *opa1x_av =
		container_of(fid, struct fi_opa1x_av, av_fid);

	ret = fi_opa1x_fid_check(fid, FI_CLASS_AV, "address vector");
	if (ret)
		return ret;

	if (opa1x_av->map_addr) free(opa1x_av->map_addr);

	ret = fi_opa1x_ref_dec(&opa1x_av->domain->ref_cnt, "domain");
	if (ret)
		return ret;

	ret = fi_opa1x_ref_finalize(&opa1x_av->ref_cnt, "address vector");
	if (ret)
		return ret;

	free(opa1x_av);

	FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_AV, "av closed\n");
	return 0;
}

/*
 * The 'addr' is a representation of the address - not a string
 *
 * 'flags' is allowed to be ignored
 * 'context' is not used ... what is the purpose?
 */
static int
fi_opa1x_av_insert(struct fid_av *av, const void *addr, size_t count,
	     fi_addr_t *fi_addr, uint64_t flags, void *context)
{
	struct fi_opa1x_av *opa1x_av =
		container_of(av, struct fi_opa1x_av, av_fid);

	if (!opa1x_av) {
		errno = FI_EINVAL;
		return -errno;
	}

	uint32_t n, i;
	fi_addr_t * input = (fi_addr_t *) addr;
	const unsigned ep_tx_count = opa1x_av->ep_tx_count;

	switch (opa1x_av->type) {
	case FI_AV_TABLE:
		/* The address table is internal and the application uses a
		 * 'monotonically increasing integer' to index the table and
		 * retrieve the actual internal address
		 */
		if (!addr) {
			errno = FI_ENOSYS;
			return -errno;
		} else if (opa1x_av->table_addr != NULL) {
			errno = FI_EINVAL;
			return -errno;
		} else {
			union fi_opa1x_addr * opa1x_addr =
				(union fi_opa1x_addr *) malloc(sizeof(union fi_opa1x_addr) * count);
			opa1x_av->table_addr = opa1x_addr;
			if (fi_addr != NULL) {
				for (n=0; n<count; ++n) {
					opa1x_addr[n].fi = input[n];
					fi_addr[n] = n;
					for (i=0; i<ep_tx_count; ++i) {
						fi_opa1x_ep_tx_connect(opa1x_av->ep_tx[i], opa1x_addr[n].fi);
					}
				}
			} else {
				for (n=0; n<count; ++n) {
					opa1x_addr[n].fi = input[n];
					for (i=0; i<ep_tx_count; ++i) {
						fi_opa1x_ep_tx_connect(opa1x_av->ep_tx[i], opa1x_addr[n].fi);
					}
				}
			}
		}
		break;
	case FI_AV_MAP:
		/* The address map is maintained by the application ('fi_addr') and
		 * the provider must fill in the map with the actual network
		 * address of each .
		 */
		if (!addr) {
			errno = FI_ENOSYS;
			return -errno;
		} else if (opa1x_av->table_addr != NULL) {
			fprintf(stderr, "%s:%s():%d abort\n", __FILE__, __func__, __LINE__); abort();
		} else {
			union fi_opa1x_addr * output = (union fi_opa1x_addr *) fi_addr;
			for (n=0; n<count; ++n) {
				output[n].fi = input[n];
				for (i=0; i<ep_tx_count; ++i) {
					fi_opa1x_ep_tx_connect(opa1x_av->ep_tx[i], output[n].fi);
				}
			}
		}
		break;
	default:
		errno = FI_EINVAL;
		return -errno;
	}

	opa1x_av->addr_count = count;

	return count;
}

static int
fi_opa1x_av_insertsvc(struct fid_av *av, const char *node, const char *service,
		fi_addr_t *fi_addr, uint64_t flags, void *context)
{
	struct fi_opa1x_av *opa1x_av =
		container_of(av, struct fi_opa1x_av, av_fid);

	if (!opa1x_av) {
		errno = FI_EINVAL;
		return -errno;
	}

	switch (opa1x_av->type) {
	case FI_AV_TABLE:
		/* The address table is internal and the application uses a
		 * 'monotonically increasing integer' to index the table and
		 * retrieve the actual internal address
		 */
		break;
	case FI_AV_MAP:
		/* The address map is maintained by the application ('fi_addr') and
		 * the provider must fill in the map with the actual network
		 * address of each .
		 */
		errno = FI_ENOSYS;
		return -errno;
		break;
	default:
		errno = FI_EINVAL;
		return -errno;
	}

	FI_WARN(fi_opa1x_global.prov, FI_LOG_AV, "unimplemented\n");
	abort();
	return 0;
}

/*
 * This is similar to "ranks to coords" syscall. The "node" is the string
 * representation of the torus coordinates of a node and the 't' coordinate,
 * such as "0.0.0.0.0.0", and the "service" is the string representation of
 * what could be considered a pami-style "client id". Currently, only a single
 * "service" per "node" is supported - the service parameter is ignored and
 * a svccnt != 1 is considered an error.
 *
 * If the "node" parameter is NULL, then the insert begins at coordinate
 * 0.0.0.0.0.0 and increments according to the default ABCDET map order until
 * "nodecnt" addresses have been inserted. In this respect, "nodecnt" is the
 * same as the job size.
 *
 * The opa1x provider does not support rank reorder via mapfiles.
 */
static int
fi_opa1x_av_insertsym(struct fid_av *av, const char *node, size_t nodecnt,
		const char *service, size_t svccnt,
		fi_addr_t *fi_addr, uint64_t flags, void *context)
{
	struct fi_opa1x_av *opa1x_av =
		container_of(av, struct fi_opa1x_av, av_fid);

	if (!opa1x_av) {
		errno = FI_EINVAL;
		return -errno;
	}

	if (svccnt != 1) {
		FI_WARN(fi_opa1x_global.prov, FI_LOG_AV,
			"Error. Only one 'service' per 'node' is supported by the opa1x provider\n");
		errno = FI_EINVAL;
		return -errno;
	}

	switch (opa1x_av->type) {
	case FI_AV_TABLE:
		/* The address table is internal and the application uses a
		 * 'monotonically increasing integer' to index the table and
		 * retrieve the actual internal address
		 */
		break;
	case FI_AV_MAP:
		/* The address map is maintained by the application ('fi_addr') and
		 * the provider must fill in the map with the actual network
		 * address of each .
		 */
		errno = FI_ENOSYS;
		return -errno;
		break;
	default:
		errno = FI_EINVAL;
		return -errno;
	}

	FI_WARN(fi_opa1x_global.prov, FI_LOG_AV, "unimplemented\n");
	abort();
	return 0;
}

static int
fi_opa1x_av_remove(struct fid_av *av, fi_addr_t *fi_addr, size_t count, uint64_t flags)
{
	return 0;	/* noop on opa1x */
}

static int
fi_opa1x_av_lookup(struct fid_av *av, fi_addr_t fi_addr, void *addr, size_t *addrlen)
{
	if (!addr || !addrlen) {
		errno = FI_EINVAL;
		return -errno;
	}

	struct fi_opa1x_av *opa1x_av =
		container_of(av, struct fi_opa1x_av, av_fid);

	if (opa1x_av->type == FI_AV_MAP) {

		memcpy(addr, (void*)&fi_addr, MIN(sizeof(fi_addr_t), *addrlen));

	} else {

		assert(opa1x_av->table_addr != NULL);
		memcpy(addr, (void*)&opa1x_av->table_addr[fi_addr], MIN(sizeof(union fi_opa1x_addr), *addrlen));
	}

	*addrlen = sizeof(union fi_opa1x_addr);

	return 0;
}

static const char *
fi_opa1x_av_straddr(struct fid_av *av, const void *addr,
			char *buf, size_t *len)
{
	if (!addr || !buf || !len) {
		errno = FI_EINVAL;
		return NULL;
	}

	union fi_opa1x_addr * opa1x_addr = (union fi_opa1x_addr *)addr;

	char tmp[32];
	int n = 1 + snprintf(tmp, sizeof(tmp), "%04x.%04x.%02x.%02x.%02x.%02x",
		opa1x_addr->uid.lid,
		opa1x_addr->uid.endpoint_id, opa1x_addr->rx_index,
		opa1x_addr->hfi1_rx, opa1x_addr->unused,
		opa1x_addr->reliability_rx);
	memcpy(buf, tmp, MIN(n, *len));
	*len = n;

	return buf;
}

static struct fi_ops fi_opa1x_fi_ops = {
	.size		= sizeof(struct fi_ops),
	.close		= fi_opa1x_close_av,
	.bind		= fi_no_bind,
	.control	= fi_no_control,
	.ops_open	= fi_no_ops_open
};

int fi_opa1x_bind_ep_av(struct fid_ep *ep,
		struct fid_av *av, uint64_t flags)
{
	FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_AV, "bind av\n");

	struct fi_opa1x_ep *opa1x_ep =
		container_of(ep, struct fi_opa1x_ep, ep_fid);

	struct fi_opa1x_av *opa1x_av =
		container_of(av, struct fi_opa1x_av, av_fid);

	if (opa1x_ep->av) {
		FI_DBG(fi_opa1x_global.prov, FI_LOG_AV,
			"Address vector already bound to TX endpoint\n");
		errno = FI_EINVAL;
		return -errno;
	}

	if (opa1x_ep->ep_fid.fid.fclass != FI_CLASS_EP) {
		FI_DBG(fi_opa1x_global.prov, FI_LOG_AV,
			"Wrong type of endpoint\n");
		errno = FI_EINVAL;
		return -errno;
	}

	opa1x_ep->av = opa1x_av;
	opa1x_ep->rx.av_addr = NULL;
	opa1x_ep->tx.av_addr = NULL;

	const unsigned ep_tx_max = sizeof(opa1x_av->ep_tx) / sizeof(struct fi_opa1x_ep*);
	if (opa1x_av->ep_tx_count < ep_tx_max) {
		opa1x_av->ep_tx[opa1x_av->ep_tx_count++] = opa1x_ep;
	} else {
		FI_WARN(fi_opa1x_global.prov, FI_LOG_AV, "Too many ep tx contexts (max = %u)\n", ep_tx_max); abort();
	}

	fi_opa1x_ref_inc(&opa1x_av->ref_cnt, "address vector");

	FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_AV, "av bound to ep\n");
	return 0;
}

static struct fi_ops_av fi_opa1x_av_ops = {
	.size		= sizeof(struct fi_ops_av),
	.insert		= fi_opa1x_av_insert,
	.insertsvc	= fi_opa1x_av_insertsvc,
	.insertsym	= fi_opa1x_av_insertsym,
	.remove		= fi_opa1x_av_remove,
	.lookup		= fi_opa1x_av_lookup,
	.straddr	= fi_opa1x_av_straddr
};

int fi_opa1x_av_open(struct fid_domain *dom,
		struct fi_av_attr *attr, struct fid_av **av,
		void *context)
{
	FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_AV, "open av\n");

	int ret;
	struct fi_opa1x_av *opa1x_av = NULL;

	if (!attr) {
		FI_DBG(fi_opa1x_global.prov, FI_LOG_AV, "no attr provided\n");
		errno = FI_EINVAL;
		return -errno;
	}

	ret = fi_opa1x_fid_check(&dom->fid, FI_CLASS_DOMAIN, "domain");
	if (ret)
		return ret;

	if ((attr->type == FI_AV_TABLE) && (FABRIC_DIRECT_AV != FI_AV_MAP)) {

		/* allocate the address table in-line with the av object */
		ret = posix_memalign((void**)&opa1x_av, 32, sizeof(struct fi_opa1x_av) + (attr->count * sizeof(union fi_opa1x_addr)));
		if (ret != 0) {
			errno = FI_ENOMEM;
			goto err;
		}

	} else if ((attr->type == FI_AV_MAP) && (FABRIC_DIRECT_AV != FI_AV_TABLE)) {

		opa1x_av = calloc(1, sizeof(*opa1x_av));
		if (!opa1x_av) {
			errno = FI_ENOMEM;
			goto err;
		}

	} else {
		FI_DBG(fi_opa1x_global.prov, FI_LOG_AV,
				"Unsupported AV type requested\n");
		errno = FI_EINVAL;
		return -errno;
	}

	opa1x_av->av_fid.fid.fclass = FI_CLASS_AV;
	opa1x_av->av_fid.fid.context= context;
	opa1x_av->av_fid.fid.ops    = &fi_opa1x_fi_ops;
	opa1x_av->av_fid.ops 	  = &fi_opa1x_av_ops;

	opa1x_av->domain = (struct fi_opa1x_domain *) dom;
	opa1x_av->type = attr->type;

	opa1x_av->ep_tx_count = 0;
	unsigned i, ep_tx_max = sizeof(opa1x_av->ep_tx) / sizeof(struct fi_opa1x_ep*);
	for (i=0; i<ep_tx_max; ++i)
		opa1x_av->ep_tx[i] = NULL;

	opa1x_av->map_addr = NULL;
	if (attr->name != NULL && (attr->flags & FI_READ)) {

		/* named address vector not supported */
		errno = FI_EOPNOTSUPP;
		goto err;
#if 0		
		assert(0 == attr->map_addr);
		fi_addr_t *addr = (fi_addr_t *)malloc(sizeof(fi_addr_t)*ep_count);	/* TODO - mmap this into shared memory */

		size_t n = 0;
		int i;

		for (i=0;i<ep_count;i++) {

			addr[n++] = fi_opa1x_addr_create(destination, fifo_map, base_rx);
		}

		opa1x_av->map_addr = (void *)addr;
		attr->map_addr = (void *)addr;
#endif
	}

	opa1x_av->rx_ctx_bits = attr->rx_ctx_bits;

	opa1x_av->addr_count = 0;
	opa1x_av->table_addr = NULL;

	*av = &opa1x_av->av_fid;

	fi_opa1x_ref_inc(&opa1x_av->domain->ref_cnt, "domain");

	FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_AV, "av opened\n");
	return 0;
err:
	if (opa1x_av)
		free(opa1x_av);
	return -errno;
}
