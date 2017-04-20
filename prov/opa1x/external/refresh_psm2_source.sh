#!/bin/bash

srcdir=wfr-psm
#srcdir=opa-psm2
rm -rf ${srcdir}

#git clone https://github.com/01org/opa-psm2.git
git clone ssh://${USER}@git-amr-2.devtools.intel.com:29418/wfr-psm.git

cp -vf ${srcdir}/opa/opa_service.c psm2/opa/
cp -vf ${srcdir}/opa/opa_service.c psm2/opa/
cp -vf ${srcdir}/opa/opa_debug.c psm2/opa/
cp -vf ${srcdir}/opa/opa_sysfs.c psm2/opa/
cp -vf ${srcdir}/opa/opa_proto.c psm2/opa/
cp -vf ${srcdir}/opa/opa_utils.c psm2/opa/
cp -vf ${srcdir}/ptl_ips/ipserror.h psm2/ptl_ips/
cp -vf ${srcdir}/psm_log.h psm2/
cp -vf ${srcdir}/psmi_wrappers.h psm2/
cp -vf ${srcdir}/include/opa_user.h psm2/include/
cp -vf ${srcdir}/include/opa_intf.h psm2/include/
cp -vf ${srcdir}/include/linux-i386/sysdep.h psm2/include/linux-i386/
cp -vf ${srcdir}/include/linux-i386/bit_ops.h psm2/include/linux-i386/
cp -vf ${srcdir}/include/opa_common.h psm2/include/
cp -vf ${srcdir}/include/hfi1_deprecated.h psm2/include/
cp -vf ${srcdir}/include/opa_byteorder.h psm2/include/
cp -vf ${srcdir}/include/opa_udebug.h psm2/include/
cp -vf ${srcdir}/include/opa_debug.h psm2/include/
cp -vf ${srcdir}/include/opa_service.h psm2/include/
cp -vf ${srcdir}/include/psm2_mock_testing.h psm2/include/

rm -rf ${srcdir}

