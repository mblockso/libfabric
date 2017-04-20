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
#include <fi.h>

#include "rdma/opa1x/fi_opa1x_domain.h"
#include "rdma/opa1x/fi_opa1x_endpoint.h"
#include "rdma/opa1x/fi_opa1x.h"


#include <fi_enosys.h>

#include <string.h>

int fi_opa1x_getname(fid_t fid, void *addr, size_t *addrlen)
{
//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
	if (addrlen == NULL) {
//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
		errno = FI_EINVAL;
		return -errno;
	}

	const size_t len = *addrlen;

//fprintf(stderr, "%s:%s():%d fid->fclass = %zu\n", __FILE__, __func__, __LINE__, fid->fclass);
	switch(fid->fclass) {
	case FI_CLASS_EP:

		*addrlen = sizeof(union fi_opa1x_addr);
		if (len > 0) {
			if (!addr) {
				errno = FI_EINVAL;
				return -errno;
			}

			struct fi_opa1x_ep *opa1x_ep;
			opa1x_ep = container_of(fid, struct fi_opa1x_ep, ep_fid);

			memcpy(addr, (void*)&opa1x_ep->rx.self, MIN(len, *addrlen));
			//FI_OPA1X_ADDR_DUMP(&opa1x_ep->rx.self);
		}

		if (len < sizeof(union fi_opa1x_addr)) {
			errno = FI_ETOOSMALL;
			return -errno;
		}
		break;

	case FI_CLASS_SEP:
//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
		*addrlen = sizeof(union fi_opa1x_addr);
		if (len > 0) {
//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
			if (!addr) {
//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
				errno = FI_EINVAL;
				return -errno;
			}

			struct fi_opa1x_sep *opa1x_sep;
			opa1x_sep = container_of(fid, struct fi_opa1x_sep, ep_fid);

//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
			unsigned i;
			for (i = 0; (opa1x_sep->ep[i] == NULL) && (i < FI_OPA1X_ADDR_SEP_RX_MAX); ++i);
//fprintf(stderr, "%s:%s():%d i=%u\n", __FILE__, __func__, __LINE__, i);

			if (i == FI_OPA1X_ADDR_SEP_RX_MAX) {
//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
				/* no sep rx ctx were created? */
				errno = FI_EINVAL;
				return -errno;
			}

//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
			union fi_opa1x_addr tmp;
			tmp.raw64b = 0;
			tmp.rx_index = 0;
			tmp.uid.endpoint_id = opa1x_sep->cx;
			tmp.reliability_rx = opa1x_sep->domain->reliability_rx;
			tmp.uid.lid = htons(opa1x_sep->ep[i]->hfi->lid);

			tmp.hfi1_rx = opa1x_sep->ep[i]->rx.self.hfi1_rx;
			tmp.unused = 0;

//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
			memcpy(addr, (void*)&tmp, MIN(len, *addrlen));
//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
			//FI_OPA1X_ADDR_DUMP(&tmp.fi);
//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
		}

//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
		if (len < sizeof(union fi_opa1x_addr)) {
//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
			errno = FI_ETOOSMALL;
			return -errno;
		}
		break;

	default:
//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
		errno = FI_EINVAL;
		return -errno;
	}

//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
	return 0;
}

static struct fi_ops_cm fi_opa1x_cm_ops = {
	.size		= sizeof(struct fi_ops_cm),
	.getname 	= fi_opa1x_getname,
	.getpeer 	= fi_no_getpeer,
	.connect 	= fi_no_connect,
	.listen  	= fi_no_listen,
	.accept  	= fi_no_accept,
	.reject  	= fi_no_reject,
	.shutdown 	= fi_no_shutdown,
};

int fi_opa1x_init_cm_ops(fid_t fid, struct fi_info *info)
{
//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
	if (!info) goto err;

	struct fi_opa1x_ep *opa1x_ep;
	struct fi_opa1x_sep *opa1x_sep;

//fprintf(stderr, "%s:%s():%d fid->fclass = %zu\n", __FILE__, __func__, __LINE__, fid->fclass);
	switch(fid->fclass) {
	case FI_CLASS_EP:

		opa1x_ep = container_of(fid, struct fi_opa1x_ep, ep_fid);

		if (!opa1x_ep) goto err;

		opa1x_ep->ep_fid.cm = &fi_opa1x_cm_ops;
		break;

	case FI_CLASS_SEP:

		opa1x_sep = container_of(fid, struct fi_opa1x_sep, ep_fid);

		if (!opa1x_sep) goto err;

		opa1x_sep->ep_fid.cm = &fi_opa1x_cm_ops;
		break;

	default:
		FI_WARN(fi_opa1x_global.prov, FI_LOG_FABRIC, "Abort. Unknown fid class (%zu)\n", fid->fclass);
		abort();
		break;
	}


	return 0;
err:
//fprintf(stderr, "%s:%s():%d ================================== ERROR!\n", __FILE__, __func__, __LINE__);
	errno = FI_EINVAL;
	return -errno;
}

int fi_opa1x_finalize_cm_ops(fid_t fid)
{
	return 0;
}
