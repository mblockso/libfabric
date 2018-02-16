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
#include "rdma/opa1x/fi_opa1x_eq.h"
#include "rdma/opa1x/fi_opa1x.h"
#include "rdma/opa1x/fi_opa1x_internal.h"

#include <fi_enosys.h>

#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

static int fi_opa1x_close_fabric(struct fid *fid)
{
	FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_FABRIC, "close fabric\n");

	int ret;
	struct fi_opa1x_fabric *opa1x_fabric =
		container_of(fid, struct fi_opa1x_fabric, fabric_fid);

	ret = fi_opa1x_fid_check(fid, FI_CLASS_FABRIC, "fabric");
	if (ret)
		return ret;

	ret = fi_opa1x_ref_finalize(&opa1x_fabric->ref_cnt, "fabric");
	if (ret)
		return ret;

	free(opa1x_fabric);

	FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_FABRIC, "fabric closed\n");
	return 0;
}

static struct fi_ops fi_opa1x_fi_ops = {
	.size		= sizeof(struct fi_ops),
	.close		= fi_opa1x_close_fabric,
	.bind		= fi_no_bind,
	.control	= fi_no_control,
	.ops_open	= fi_no_ops_open
};

static struct fi_ops_fabric fi_opa1x_ops_fabric = {
	.size		= sizeof(struct fi_ops_fabric),
	.domain		= fi_opa1x_domain,
	.passive_ep	= fi_no_passive_ep,
	.eq_open	= fi_opa1x_eq_open
};

int fi_opa1x_check_fabric_attr(struct fi_fabric_attr *attr)
{
	if (attr->name) {
		if (strcmp(attr->name, FI_OPA1X_FABRIC_NAME)) {
			FI_WARN(fi_opa1x_global.prov, FI_LOG_FABRIC,
					"attr->name (%s) doesn't match fabric (%s)\n",
					attr->name, FI_OPA1X_FABRIC_NAME);
			errno = FI_EINVAL;
			return -errno;
		}
	}
	if (attr->prov_version) {
		if (attr->prov_version != FI_OPA1X_PROVIDER_VERSION) {
			FI_WARN(fi_opa1x_global.prov, FI_LOG_FABRIC,
					"attr->prov_version (%u) doesn't match prov (%u) "
					"backward/forward compatibility support not implemented\n",
					attr->prov_version, FI_OPA1X_PROVIDER_VERSION);
			errno = FI_ENOSYS;
			return -errno;
		}
	}
	return 0;
}

int fi_opa1x_fabric(struct fi_fabric_attr *attr,
		struct fid_fabric **fabric, void *context)
{
	FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_FABRIC, "open fabric\n");

	int ret;
	struct fi_opa1x_fabric *opa1x_fabric;

	if (attr) {
		if (attr->fabric) {
			FI_WARN(fi_opa1x_global.prov, FI_LOG_FABRIC,
				"attr->fabric only valid on getinfo\n");
			errno = FI_EINVAL;
			return -errno;
		}

		ret = fi_opa1x_check_fabric_attr(attr);
		if (ret)
			return ret;
	}

	opa1x_fabric = calloc(1, sizeof(*opa1x_fabric));
	if (!opa1x_fabric)
		goto err;

	opa1x_fabric->fabric_fid.fid.fclass = FI_CLASS_FABRIC;
	opa1x_fabric->fabric_fid.fid.context = context;
	opa1x_fabric->fabric_fid.fid.ops = &fi_opa1x_fi_ops;
	opa1x_fabric->fabric_fid.ops = &fi_opa1x_ops_fabric;


	*fabric = &opa1x_fabric->fabric_fid;

	/* work around for imported psm2 source that wants to set thread affinity */
	setenv("HFI_NO_CPUAFFINITY", "", 0);

	fi_opa1x_ref_init(&opa1x_fabric->ref_cnt, "fabric");

	FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_FABRIC, "fabric opened\n");
	return 0;
err:
	errno = FI_ENOMEM;
	return -errno;
}
