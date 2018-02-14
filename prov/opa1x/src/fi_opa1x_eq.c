
#include <ofi.h>

#include "rdma/opa1x/fi_opa1x_fabric.h"
#include "rdma/opa1x/fi_opa1x_eq.h"

#include <ofi_enosys.h>

int fi_opa1x_eq_open(struct fid_fabric *fabric, struct fi_eq_attr *attr,
		struct fid_eq **eq, void *context)
{
	return FI_SUCCESS;
}

