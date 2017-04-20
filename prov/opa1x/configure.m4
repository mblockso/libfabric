dnl
dnl Copyright (C) 2016 by Argonne National Laboratory.
dnl
dnl This software is available to you under a choice of one of two
dnl licenses.  You may choose to be licensed under the terms of the GNU
dnl General Public License (GPL) Version 2, available from the file
dnl COPYING in the main directory of this source tree, or the
dnl BSD license below:
dnl
dnl     Redistribution and use in source and binary forms, with or
dnl     without modification, are permitted provided that the following
dnl     conditions are met:
dnl
dnl      - Redistributions of source code must retain the above
dnl        copyright notice, this list of conditions and the following
dnl        disclaimer.
dnl
dnl      - Redistributions in binary form must reproduce the above
dnl        copyright notice, this list of conditions and the following
dnl        disclaimer in the documentation and/or other materials
dnl        provided with the distribution.
dnl
dnl THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
dnl EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
dnl MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
dnl NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
dnl BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
dnl ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
dnl CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
dnl SOFTWARE.
dnl
dnl Configury specific to the libfabrics OPA1-x provider

dnl Called to configure this provider
dnl
dnl Arguments:
dnl
dnl $1: action if configured successfully
dnl $2: action if not configured successfully
dnl
AC_DEFUN([FI_OPA1X_CONFIGURE],[
	dnl Determine if we can support the opa1x provider
	opa1x_happy=0
	opa1x_direct=0

	AS_IF([test x"$enable_opa1x" != x"no"],[


		AC_MSG_CHECKING([for direct opa1x provider])
		AS_IF([test x"$enable_direct" != x"opa1x"],
			[AC_MSG_RESULT([no])],
			[AC_MSG_RESULT([yes])

			dnl AS_CASE([x$FABRIC_DIRECT_PROGRESS],
			dnl	[xauto], [FABRIC_DIRECT_PROGRESS_MODE=FI_PROGRESS_AUTO],
			dnl	[xmanual], [FABRIC_DIRECT_PROGRESS_MODE=FI_PROGRESS_MANUAL],
			dnl	[xruntime], [FABRIC_DIRECT_PROGRESS_MODE=FI_PROGRESS_UNSPEC],
			dnl	[FABRIC_DIRECT_PROGRESS_MODE=FI_PROGRESS_MANUAL])

			dnl Only FI_PROGRESS_MANUAL is supported by the opa1x provider
			FABRIC_DIRECT_PROGRESS_MODE=FI_PROGRESS_MANUAL
			AC_SUBST(fabric_direct_progress, [$FABRIC_DIRECT_PROGRESS_MODE])
			AC_DEFINE_UNQUOTED(FABRIC_DIRECT_PROGRESS, [$FABRIC_DIRECT_PROGRESS_MODE], [fabric direct progress])


			AS_CASE([x$FABRIC_DIRECT_AV],
				[xmap], [FABRIC_DIRECT_AV_MODE=FI_AV_MAP],
				[xtable], [FABRIC_DIRECT_AV_MODE=FI_AV_TABLE],
				[xruntime], [FABRIC_DIRECT_AV_MODE=FI_AV_UNSPEC],
				[FABRIC_DIRECT_AV_MODE=FI_AV_MAP])

			AC_SUBST(fabric_direct_av, [$FABRIC_DIRECT_AV_MODE])
			AC_DEFINE_UNQUOTED(FABRIC_DIRECT_AV, [$FABRIC_DIRECT_AV_MODE], [fabric direct address vector])


			AS_CASE([x$FABRIC_DIRECT_MR],
				[xscalable], [OPA1X_FABRIC_DIRECT_MR_MODE=FI_MR_SCALABLE],
				[xbasic], [OPA1X_FABRIC_DIRECT_MR_MODE=FI_MR_BASIC],
				[FABRIC_DIRECT_MR_MODE=FI_MR_SCALABLE])

			AC_SUBST(fabric_direct_mr, [$FABRIC_DIRECT_MR_MODE])
			AC_DEFINE_UNQUOTED(FABRIC_DIRECT_MR, [$FABRIC_DIRECT_MR_MODE], [fabric direct memory region])


			dnl Only FI_THREAD_ENDPOINT is supported by the opa1x provider
			FABRIC_DIRECT_THREAD_MODE=FI_THREAD_ENDPOINT

			AC_SUBST(fabric_direct_thread, [$FABRIC_DIRECT_THREAD_MODE])
			AC_DEFINE_UNQUOTED(FABRIC_DIRECT_THREAD, [$FABRIC_DIRECT_THREAD_MODE], [fabric direct thread])

			opa1x_happy=1
		])
	])

	AS_IF([test $opa1x_happy -eq 1], [$1], [$2])
])

dnl A separate macro for AM CONDITIONALS, since they cannot be invoked
dnl conditionally
AC_DEFUN([FI_OPA1X_CONDITIONALS],[
])
