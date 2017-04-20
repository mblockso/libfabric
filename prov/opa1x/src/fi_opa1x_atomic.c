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
#include "rdma/opa1x/fi_opa1x_atomic.h"
#include "rdma/opa1x/fi_opa1x_endpoint.h"
#include "rdma/opa1x/fi_opa1x.h"

#include <fi_enosys.h>

#include <complex.h>

/*
 * --------------------------- begin: rx atomics ------------------------------
 */
#define FI_OPA1X_RX_ATOMIC_SPECIALIZED_MACRO_NAME(OP)				\
	FI_OPA1X_RX_ATOMIC_SPECIALIZED_MACRO_NAME_(OP)

#define FI_OPA1X_RX_ATOMIC_SPECIALIZED_MACRO_NAME_(OP)				\
	FI_OPA1X_RX_ATOMIC_DO_ ## OP

#define FI_OPA1X_RX_ATOMIC_SPECIALIZED_FUNC(OP, DT, CTYPE)			\
	FI_OPA1X_RX_ATOMIC_SPECIALIZED_FUNC_(OP, DT, CTYPE)

#define FI_OPA1X_RX_ATOMIC_SPECIALIZED_FUNC_(OP, DT, CTYPE)			\
	void fi_opa1x_rx_atomic_ ## OP ## _ ## DT				\
		(void * buf, void * addr, size_t nbytes)			\
	{									\
		FI_OPA1X_RX_ATOMIC_SPECIALIZED_MACRO_NAME(OP)(buf, addr, CTYPE)	\
	}

#define FI_OPA1X_RX_ATOMIC_SPECIALIZED_FUNC_NAME(OP, DT)			\
	FI_OPA1X_RX_ATOMIC_SPECIALIZED_FUNC_NAME_(OP, DT)

#define FI_OPA1X_RX_ATOMIC_SPECIALIZED_FUNC_NAME_(OP, DT)			\
	fi_opa1x_rx_atomic_ ## OP ## _ ## DT


#define FI_OPA1X_RX_ATOMIC_DO_MIN(buf_, addr_, ctype)				\
	ctype * buf__ = (ctype *)buf_;						\
	ctype * addr__ = (ctype *)addr_;					\
	const size_t count = nbytes / sizeof(ctype);				\
	unsigned i;								\
	for (i=0; i<count; ++i)							\
		if (buf__[i] < addr__[i])					\
			addr__[i] = buf__[i];					\

#define FI_OPA1X_RX_ATOMIC_DO_MAX(buf_, addr_, ctype)				\
{										\
	ctype * buf__ = (ctype *)buf_;						\
	ctype * addr__ = (ctype *)addr_;					\
	const size_t count = nbytes / sizeof(ctype);				\
	unsigned i;								\
	for (i=0; i<count; ++i)							\
		if (buf__[i] > addr__[i])					\
			addr__[i] = buf__[i];					\
}

#define FI_OPA1X_RX_ATOMIC_DO_SUM(buf_, addr_, ctype)				\
{										\
	ctype * buf__ = (ctype *)buf_;						\
	ctype * addr__ = (ctype *)addr_;					\
	const size_t count = nbytes / sizeof(ctype);				\
	unsigned i;								\
	for (i=0; i<count; ++i)							\
		addr__[i] += buf__[i];						\
}

#define FI_OPA1X_RX_ATOMIC_DO_PROD(buf_, addr_, ctype)				\
{										\
	ctype * buf__ = (ctype *)buf_;						\
	ctype * addr__ = (ctype *)addr_;					\
	const size_t count = nbytes / sizeof(ctype);				\
	unsigned i;								\
	for (i=0; i<count; ++i)							\
		addr__[i] = addr__[i] * buf__[i];				\
}

#define FI_OPA1X_RX_ATOMIC_DO_LOR(buf_, addr_, ctype)				\
{										\
	ctype * buf__ = (ctype *)buf_;						\
	ctype * addr__ = (ctype *)addr_;					\
	const size_t count = nbytes / sizeof(ctype);				\
	unsigned i;								\
	for (i=0; i<count; ++i)							\
		addr__[i] = (addr__[i] || buf__[i]);				\
}

#define FI_OPA1X_RX_ATOMIC_DO_LAND(buf_, addr_, ctype)				\
{										\
	ctype * buf__ = (ctype *)buf_;						\
	ctype * addr__ = (ctype *)addr_;					\
	const size_t count = nbytes / sizeof(ctype);				\
	unsigned i;								\
	for (i=0; i<count; ++i)							\
		addr__[i] = (addr__[i] && buf__[i]);				\
}

#define FI_OPA1X_RX_ATOMIC_DO_BOR_(buf_, addr_, ctype)				\
{										\
	ctype * buf__ = (ctype *)buf_;						\
	ctype * addr__ = (ctype *)addr_;					\
	const size_t count = nbytes / sizeof(ctype);				\
	unsigned i;								\
	for (i=0; i<count; ++i)							\
		addr__[i] |= buf__[i];						\
}

#define FI_OPA1X_RX_ATOMIC_DO_BOR(buf_, addr_, ctype)				\
{										\
	if (sizeof(uint8_t) == sizeof(ctype)) {					\
		FI_OPA1X_RX_ATOMIC_DO_BOR_(buf_, addr_, uint8_t);		\
	} else if (sizeof(uint16_t) == sizeof(ctype)) {				\
		FI_OPA1X_RX_ATOMIC_DO_BOR_(buf_, addr_, uint16_t);		\
	} else if (sizeof(uint32_t) == sizeof(ctype)) {				\
		FI_OPA1X_RX_ATOMIC_DO_BOR_(buf_, addr_, uint32_t);		\
	} else if (sizeof(uint64_t) == sizeof(ctype)) {				\
		FI_OPA1X_RX_ATOMIC_DO_BOR_(buf_, addr_, uint64_t);		\
	} else if (16 == sizeof(ctype)) {					\
		FI_OPA1X_RX_ATOMIC_DO_BOR_(&(((uint64_t*)buf_)[0]), &(((uint64_t*)addr_)[0]), uint64_t);	\
		FI_OPA1X_RX_ATOMIC_DO_BOR_(&(((uint64_t*)buf_)[1]), &(((uint64_t*)addr_)[1]), uint64_t);	\
	} else if (32 == sizeof(ctype)) {					\
		FI_OPA1X_RX_ATOMIC_DO_BOR_(&(((uint64_t*)buf_)[0]), &(((uint64_t*)addr_)[0]), uint64_t);	\
		FI_OPA1X_RX_ATOMIC_DO_BOR_(&(((uint64_t*)buf_)[1]), &(((uint64_t*)addr_)[1]), uint64_t);	\
		FI_OPA1X_RX_ATOMIC_DO_BOR_(&(((uint64_t*)buf_)[2]), &(((uint64_t*)addr_)[2]), uint64_t);	\
		FI_OPA1X_RX_ATOMIC_DO_BOR_(&(((uint64_t*)buf_)[3]), &(((uint64_t*)addr_)[3]), uint64_t);	\
	} else {								\
		assert(0);							\
	}									\
}

#define FI_OPA1X_RX_ATOMIC_DO_BAND_(buf_, addr_, ctype)				\
{										\
	ctype * buf__ = (ctype *)buf_;						\
	ctype * addr__ = (ctype *)addr_;					\
	const size_t count = nbytes / sizeof(ctype);				\
	unsigned i;								\
	for (i=0; i<count; ++i)							\
		addr__[i] &= buf__[i];						\
}

#define FI_OPA1X_RX_ATOMIC_DO_BAND(buf_, addr_, ctype)				\
{										\
	if (sizeof(uint8_t) == sizeof(ctype)) {					\
		FI_OPA1X_RX_ATOMIC_DO_BAND_(buf_, addr_, uint8_t);		\
	} else if (sizeof(uint16_t) == sizeof(ctype)) {				\
		FI_OPA1X_RX_ATOMIC_DO_BAND_(buf_, addr_, uint16_t);		\
	} else if (sizeof(uint32_t) == sizeof(ctype)) {				\
		FI_OPA1X_RX_ATOMIC_DO_BAND_(buf_, addr_, uint32_t);		\
	} else if (sizeof(uint64_t) == sizeof(ctype)) {				\
		FI_OPA1X_RX_ATOMIC_DO_BAND_(buf_, addr_, uint64_t);		\
	} else if (16 == sizeof(ctype)) {					\
		FI_OPA1X_RX_ATOMIC_DO_BAND_(&(((uint64_t*)buf_)[0]), &(((uint64_t*)addr_)[0]), uint64_t);	\
		FI_OPA1X_RX_ATOMIC_DO_BAND_(&(((uint64_t*)buf_)[1]), &(((uint64_t*)addr_)[1]), uint64_t);	\
	} else if (32 == sizeof(ctype)) {					\
		FI_OPA1X_RX_ATOMIC_DO_BAND_(&(((uint64_t*)buf_)[0]), &(((uint64_t*)addr_)[0]), uint64_t);	\
		FI_OPA1X_RX_ATOMIC_DO_BAND_(&(((uint64_t*)buf_)[1]), &(((uint64_t*)addr_)[1]), uint64_t);	\
		FI_OPA1X_RX_ATOMIC_DO_BAND_(&(((uint64_t*)buf_)[2]), &(((uint64_t*)addr_)[2]), uint64_t);	\
		FI_OPA1X_RX_ATOMIC_DO_BAND_(&(((uint64_t*)buf_)[3]), &(((uint64_t*)addr_)[3]), uint64_t);	\
	} else {								\
		assert(0);							\
	}									\
}

#define FI_OPA1X_RX_ATOMIC_DO_LXOR(buf_, addr_, ctype)				\
{										\
	ctype * buf__ = (ctype *)buf_;						\
	ctype * addr__ = (ctype *)addr_;					\
	const size_t count = nbytes / sizeof(ctype);				\
	unsigned i;								\
	for (i=0; i<count; ++i)							\
		addr__[i] = ((addr__[i] && !buf__[i]) ||			\
			(!addr__[i] && buf__[i]));				\
}

#define FI_OPA1X_RX_ATOMIC_DO_BXOR_(buf_, addr_, ctype)				\
{										\
	ctype * buf__ = (ctype *)buf_;						\
	ctype * addr__ = (ctype *)addr_;					\
	const size_t count = nbytes / sizeof(ctype);				\
	unsigned i;								\
	for (i=0; i<count; ++i)							\
		addr__[i] = addr__[i] ^ buf__[i];				\
}

#define FI_OPA1X_RX_ATOMIC_DO_BXOR(buf_, addr_, ctype)				\
{										\
	if (sizeof(uint8_t) == sizeof(ctype)) {					\
		FI_OPA1X_RX_ATOMIC_DO_BXOR_(buf_, addr_, uint8_t);		\
	} else if (sizeof(uint16_t) == sizeof(ctype)) {				\
		FI_OPA1X_RX_ATOMIC_DO_BXOR_(buf_, addr_, uint16_t);		\
	} else if (sizeof(uint32_t) == sizeof(ctype)) {				\
		FI_OPA1X_RX_ATOMIC_DO_BXOR_(buf_, addr_, uint32_t);		\
	} else if (sizeof(uint64_t) == sizeof(ctype)) {				\
		FI_OPA1X_RX_ATOMIC_DO_BXOR_(buf_, addr_, uint64_t);		\
	} else if (16 == sizeof(ctype)) {			\
		FI_OPA1X_RX_ATOMIC_DO_BXOR_(&(((uint64_t*)buf_)[0]), &(((uint64_t*)addr_)[0]), uint64_t);	\
		FI_OPA1X_RX_ATOMIC_DO_BXOR_(&(((uint64_t*)buf_)[1]), &(((uint64_t*)addr_)[1]), uint64_t);	\
	} else if (32 == sizeof(ctype)) {					\
		FI_OPA1X_RX_ATOMIC_DO_BXOR_(&(((uint64_t*)buf_)[0]), &(((uint64_t*)addr_)[0]), uint64_t);	\
		FI_OPA1X_RX_ATOMIC_DO_BXOR_(&(((uint64_t*)buf_)[1]), &(((uint64_t*)addr_)[1]), uint64_t);	\
		FI_OPA1X_RX_ATOMIC_DO_BXOR_(&(((uint64_t*)buf_)[2]), &(((uint64_t*)addr_)[2]), uint64_t);	\
		FI_OPA1X_RX_ATOMIC_DO_BXOR_(&(((uint64_t*)buf_)[3]), &(((uint64_t*)addr_)[3]), uint64_t);	\
	} else {								\
		assert(0);							\
	}									\
}

#define FI_OPA1X_RX_ATOMIC_DO_ATOMIC_READ(buf_, addr_, ctype)			\
{										\
	ctype * buf__ = (ctype *)buf_;						\
	ctype * addr__ = (ctype *)addr_;					\
	const size_t count = nbytes / sizeof(ctype);				\
	unsigned i;								\
	for (i=0; i<count; ++i)							\
		buf__[i] = addr__[i];						\
}

#define FI_OPA1X_RX_ATOMIC_DO_ATOMIC_WRITE(buf_, addr_, ctype)			\
{										\
	ctype * buf__ = (ctype *)buf_;						\
	ctype * addr__ = (ctype *)addr_;					\
	const size_t count = nbytes / sizeof(ctype);				\
	unsigned i;								\
	for (i=0; i<count; ++i)							\
		addr__[i] = buf__[i];						\
}

#define FI_OPA1X_RX_ATOMIC_DO_CSWAP(buf_, addr_, ctype)				\
{										\
	ctype * buf__ = (ctype *)buf_;						\
	ctype * addr__ = (ctype *)addr_;					\
	const size_t count = nbytes / sizeof(ctype);				\
	const ctype * compare__ = &buf__[count];				\
	unsigned i;								\
	for (i=0; i<count; ++i)							\
		if (compare__[i] == addr__[i])					\
			addr__[i] = buf__[i];					\
}

#define FI_OPA1X_RX_ATOMIC_DO_CSWAP_NE(buf_, addr_, ctype)			\
{										\
	ctype * buf__ = (ctype *)buf_;						\
	ctype * addr__ = (ctype *)addr_;					\
	const size_t count = nbytes / sizeof(ctype);				\
	const ctype * compare__ = &buf__[count];				\
	unsigned i;								\
	for (i=0; i<count; ++i)							\
		if (compare__[i] != addr__[i])					\
			addr__[i] = buf__[i];					\
}

#define FI_OPA1X_RX_ATOMIC_DO_CSWAP_LE(buf_, addr_, ctype)			\
{										\
	ctype * buf__ = (ctype *)buf_;						\
	ctype * addr__ = (ctype *)addr_;					\
	const size_t count = nbytes / sizeof(ctype);				\
	const ctype * compare__ = &buf__[count];				\
	unsigned i;								\
	for (i=0; i<count; ++i)							\
		if (compare__[i] <= addr__[i])					\
			addr__[i] = buf__[i];					\
}

#define FI_OPA1X_RX_ATOMIC_DO_CSWAP_LT(buf_, addr_, ctype)			\
{										\
	ctype * buf__ = (ctype *)buf_;						\
	ctype * addr__ = (ctype *)addr_;					\
	const size_t count = nbytes / sizeof(ctype);				\
	const ctype * compare__ = &buf__[count];				\
	unsigned i;								\
	for (i=0; i<count; ++i)							\
		if (compare__[i] < addr__[i])					\
			addr__[i] = buf__[i];					\
}

#define FI_OPA1X_RX_ATOMIC_DO_CSWAP_GE(buf_, addr_, ctype)			\
{										\
	ctype * buf__ = (ctype *)buf_;						\
	ctype * addr__ = (ctype *)addr_;					\
	const size_t count = nbytes / sizeof(ctype);				\
	const ctype * compare__ = &buf__[count];				\
	unsigned i;								\
	for (i=0; i<count; ++i)							\
		if (compare__[i] >= addr__[i])					\
			addr__[i] = buf__[i];					\
}

#define FI_OPA1X_RX_ATOMIC_DO_CSWAP_GT(buf_, addr_, ctype)			\
{										\
	ctype * buf__ = (ctype *)buf_;						\
	ctype * addr__ = (ctype *)addr_;					\
	const size_t count = nbytes / sizeof(ctype);				\
	const ctype * compare__ = &buf__[count];				\
	unsigned i;								\
	for (i=0; i<count; ++i)							\
		if (compare__[i] > addr__[i])					\
			addr__[i] = buf__[i];					\
}

#define FI_OPA1X_RX_ATOMIC_DO_MSWAP_(buf_, addr_, ctype)			\
{										\
	ctype * buf__ = (ctype *)buf_;						\
	ctype * addr__ = (ctype *)addr_;					\
	const size_t count = nbytes / sizeof(ctype);				\
	const ctype * compare__ = &buf__[count];				\
	unsigned i;								\
	for (i=0; i<count; ++i)							\
		addr__[i] =							\
			(buf__[i] & compare__[i]) |				\
			(addr__[i] & ~compare__[i]);				\
}

#define FI_OPA1X_RX_ATOMIC_DO_MSWAP(buf_, addr_, ctype)				\
{										\
	if (sizeof(uint8_t) == sizeof(ctype)) {					\
		FI_OPA1X_RX_ATOMIC_DO_MSWAP_(buf_, addr_, uint8_t);		\
	} else if (sizeof(uint16_t) == sizeof(ctype)) {				\
		FI_OPA1X_RX_ATOMIC_DO_MSWAP_(buf_, addr_, uint16_t);		\
	} else if (sizeof(uint32_t) == sizeof(ctype)) {				\
		FI_OPA1X_RX_ATOMIC_DO_MSWAP_(buf_, addr_, uint32_t);		\
	} else if (sizeof(uint64_t) == sizeof(ctype)) {				\
		FI_OPA1X_RX_ATOMIC_DO_MSWAP_(buf_, addr_, uint64_t);		\
	} else if (16 == sizeof(ctype)) {					\
		FI_OPA1X_RX_ATOMIC_DO_MSWAP_(&(((uint64_t*)buf_)[0]), &(((uint64_t*)addr_)[0]), uint64_t);	\
		FI_OPA1X_RX_ATOMIC_DO_MSWAP_(&(((uint64_t*)buf_)[1]), &(((uint64_t*)addr_)[1]), uint64_t);	\
	} else if (32 == sizeof(ctype)) {					\
		FI_OPA1X_RX_ATOMIC_DO_MSWAP_(&(((uint64_t*)buf_)[0]), &(((uint64_t*)addr_)[0]), uint64_t);	\
		FI_OPA1X_RX_ATOMIC_DO_MSWAP_(&(((uint64_t*)buf_)[1]), &(((uint64_t*)addr_)[1]), uint64_t);	\
		FI_OPA1X_RX_ATOMIC_DO_MSWAP_(&(((uint64_t*)buf_)[2]), &(((uint64_t*)addr_)[2]), uint64_t);	\
		FI_OPA1X_RX_ATOMIC_DO_MSWAP_(&(((uint64_t*)buf_)[3]), &(((uint64_t*)addr_)[3]), uint64_t);	\
	} else {								\
		assert(0);							\
	}									\
}

#define FI_OPA1X_RX_ATOMIC_DO_NOOP(buf_, addr_, ctype) {}

#define FI_OPA1X_RX_ATOMIC_SPECIALIZED_FUNCS(DT, CTYPE)				\
	FI_OPA1X_RX_ATOMIC_SPECIALIZED_FUNC(MIN, DT, CTYPE);			\
	FI_OPA1X_RX_ATOMIC_SPECIALIZED_FUNC(MAX, DT, CTYPE);			\
	FI_OPA1X_RX_ATOMIC_SPECIALIZED_FUNC(SUM, DT, CTYPE);			\
	FI_OPA1X_RX_ATOMIC_SPECIALIZED_FUNC(PROD, DT, CTYPE);			\
	FI_OPA1X_RX_ATOMIC_SPECIALIZED_FUNC(LOR, DT, CTYPE);			\
	FI_OPA1X_RX_ATOMIC_SPECIALIZED_FUNC(LAND, DT, CTYPE);			\
	FI_OPA1X_RX_ATOMIC_SPECIALIZED_FUNC(BOR, DT, CTYPE);			\
	FI_OPA1X_RX_ATOMIC_SPECIALIZED_FUNC(BAND, DT, CTYPE);			\
	FI_OPA1X_RX_ATOMIC_SPECIALIZED_FUNC(LXOR, DT, CTYPE);			\
	FI_OPA1X_RX_ATOMIC_SPECIALIZED_FUNC(BXOR, DT, CTYPE);			\
	FI_OPA1X_RX_ATOMIC_SPECIALIZED_FUNC(ATOMIC_READ, DT, CTYPE);		\
	FI_OPA1X_RX_ATOMIC_SPECIALIZED_FUNC(ATOMIC_WRITE, DT, CTYPE);		\
	FI_OPA1X_RX_ATOMIC_SPECIALIZED_FUNC(CSWAP, DT, CTYPE);			\
	FI_OPA1X_RX_ATOMIC_SPECIALIZED_FUNC(CSWAP_NE, DT, CTYPE);		\
	FI_OPA1X_RX_ATOMIC_SPECIALIZED_FUNC(CSWAP_LE, DT, CTYPE);		\
	FI_OPA1X_RX_ATOMIC_SPECIALIZED_FUNC(CSWAP_LT, DT, CTYPE);		\
	FI_OPA1X_RX_ATOMIC_SPECIALIZED_FUNC(CSWAP_GE, DT, CTYPE);		\
	FI_OPA1X_RX_ATOMIC_SPECIALIZED_FUNC(CSWAP_GT, DT, CTYPE);		\
	FI_OPA1X_RX_ATOMIC_SPECIALIZED_FUNC(MSWAP, DT, CTYPE);

#define FI_OPA1X_RX_ATOMIC_SPECIALIZED_FUNCS_COMPLEX(DT, CTYPE)			\
	FI_OPA1X_RX_ATOMIC_SPECIALIZED_FUNC(SUM, DT, CTYPE);			\
	FI_OPA1X_RX_ATOMIC_SPECIALIZED_FUNC(PROD, DT, CTYPE);			\
	FI_OPA1X_RX_ATOMIC_SPECIALIZED_FUNC(LOR, DT, CTYPE);			\
	FI_OPA1X_RX_ATOMIC_SPECIALIZED_FUNC(LAND, DT, CTYPE);			\
	FI_OPA1X_RX_ATOMIC_SPECIALIZED_FUNC(BOR, DT, CTYPE);			\
	FI_OPA1X_RX_ATOMIC_SPECIALIZED_FUNC(LXOR, DT, CTYPE);			\
	FI_OPA1X_RX_ATOMIC_SPECIALIZED_FUNC(ATOMIC_READ, DT, CTYPE);		\
	FI_OPA1X_RX_ATOMIC_SPECIALIZED_FUNC(ATOMIC_WRITE, DT, CTYPE);		\
	FI_OPA1X_RX_ATOMIC_SPECIALIZED_FUNC(CSWAP, DT, CTYPE);

void fi_opa1x_rx_atomic_NOOP (void * addr, void * buf, size_t nbytes) {}

FI_OPA1X_RX_ATOMIC_SPECIALIZED_FUNCS(INT8, int8_t)
FI_OPA1X_RX_ATOMIC_SPECIALIZED_FUNCS(UINT8, uint8_t)
FI_OPA1X_RX_ATOMIC_SPECIALIZED_FUNCS(INT16, int16_t)
FI_OPA1X_RX_ATOMIC_SPECIALIZED_FUNCS(UINT16, uint16_t)
FI_OPA1X_RX_ATOMIC_SPECIALIZED_FUNCS(INT32, int32_t)
FI_OPA1X_RX_ATOMIC_SPECIALIZED_FUNCS(UINT32, uint32_t)
FI_OPA1X_RX_ATOMIC_SPECIALIZED_FUNCS(INT64, int64_t)
FI_OPA1X_RX_ATOMIC_SPECIALIZED_FUNCS(UINT64, uint64_t)
FI_OPA1X_RX_ATOMIC_SPECIALIZED_FUNCS(FLOAT, float)
FI_OPA1X_RX_ATOMIC_SPECIALIZED_FUNCS(DOUBLE, double)
FI_OPA1X_RX_ATOMIC_SPECIALIZED_FUNCS_COMPLEX(FLOAT_COMPLEX, complex float)
FI_OPA1X_RX_ATOMIC_SPECIALIZED_FUNCS_COMPLEX(DOUBLE_COMPLEX, complex double)
FI_OPA1X_RX_ATOMIC_SPECIALIZED_FUNCS(LONG_DOUBLE, long double)
FI_OPA1X_RX_ATOMIC_SPECIALIZED_FUNCS_COMPLEX(LONG_DOUBLE_COMPLEX, complex long double)

#define FI_OPA1X_RX_ATOMIC_DISPATCH_FUNC_NAMES(DT)				\
	FI_OPA1X_RX_ATOMIC_SPECIALIZED_FUNC_NAME(MIN, DT),			\
	FI_OPA1X_RX_ATOMIC_SPECIALIZED_FUNC_NAME(MAX, DT),			\
	FI_OPA1X_RX_ATOMIC_SPECIALIZED_FUNC_NAME(SUM, DT),			\
	FI_OPA1X_RX_ATOMIC_SPECIALIZED_FUNC_NAME(PROD, DT),			\
	FI_OPA1X_RX_ATOMIC_SPECIALIZED_FUNC_NAME(LOR, DT),			\
	FI_OPA1X_RX_ATOMIC_SPECIALIZED_FUNC_NAME(LAND, DT),			\
	FI_OPA1X_RX_ATOMIC_SPECIALIZED_FUNC_NAME(BOR, DT),			\
	FI_OPA1X_RX_ATOMIC_SPECIALIZED_FUNC_NAME(BAND, DT),			\
	FI_OPA1X_RX_ATOMIC_SPECIALIZED_FUNC_NAME(LXOR, DT),			\
	FI_OPA1X_RX_ATOMIC_SPECIALIZED_FUNC_NAME(BXOR, DT),			\
	FI_OPA1X_RX_ATOMIC_SPECIALIZED_FUNC_NAME(ATOMIC_READ, DT),		\
	FI_OPA1X_RX_ATOMIC_SPECIALIZED_FUNC_NAME(ATOMIC_WRITE, DT),		\
	FI_OPA1X_RX_ATOMIC_SPECIALIZED_FUNC_NAME(CSWAP, DT),			\
	FI_OPA1X_RX_ATOMIC_SPECIALIZED_FUNC_NAME(CSWAP_NE, DT),			\
	FI_OPA1X_RX_ATOMIC_SPECIALIZED_FUNC_NAME(CSWAP_LE, DT),			\
	FI_OPA1X_RX_ATOMIC_SPECIALIZED_FUNC_NAME(CSWAP_LT, DT),			\
	FI_OPA1X_RX_ATOMIC_SPECIALIZED_FUNC_NAME(CSWAP_GE, DT),			\
	FI_OPA1X_RX_ATOMIC_SPECIALIZED_FUNC_NAME(CSWAP_GT, DT),			\
	FI_OPA1X_RX_ATOMIC_SPECIALIZED_FUNC_NAME(MSWAP, DT)

#define FI_OPA1X_RX_ATOMIC_DISPATCH_FUNC_NAMES_COMPLEX(DT)			\
	fi_opa1x_rx_atomic_NOOP,						\
	fi_opa1x_rx_atomic_NOOP,						\
	FI_OPA1X_RX_ATOMIC_SPECIALIZED_FUNC_NAME(SUM, DT),			\
	FI_OPA1X_RX_ATOMIC_SPECIALIZED_FUNC_NAME(PROD, DT),			\
	FI_OPA1X_RX_ATOMIC_SPECIALIZED_FUNC_NAME(LOR, DT),			\
	FI_OPA1X_RX_ATOMIC_SPECIALIZED_FUNC_NAME(LAND, DT),			\
	FI_OPA1X_RX_ATOMIC_SPECIALIZED_FUNC_NAME(BOR, DT),			\
	FI_OPA1X_RX_ATOMIC_SPECIALIZED_FUNC_NAME(BAND, DT),			\
	FI_OPA1X_RX_ATOMIC_SPECIALIZED_FUNC_NAME(LXOR, DT),			\
	FI_OPA1X_RX_ATOMIC_SPECIALIZED_FUNC_NAME(BXOR, DT),			\
	FI_OPA1X_RX_ATOMIC_SPECIALIZED_FUNC_NAME(ATOMIC_READ, DT),		\
	FI_OPA1X_RX_ATOMIC_SPECIALIZED_FUNC_NAME(ATOMIC_WRITE, DT),		\
	FI_OPA1X_RX_ATOMIC_SPECIALIZED_FUNC_NAME(CSWAP, DT),			\
	fi_opa1x_rx_atomic_NOOP,						\
	fi_opa1x_rx_atomic_NOOP,						\
	fi_opa1x_rx_atomic_NOOP,						\
	fi_opa1x_rx_atomic_NOOP,						\
	fi_opa1x_rx_atomic_NOOP,						\
	FI_OPA1X_RX_ATOMIC_SPECIALIZED_FUNC_NAME(MSWAP, DT)

void
fi_opa1x_rx_atomic_dispatch (void * buf, void * addr, size_t nbytes,
	enum fi_datatype dt, enum fi_op op)
{
	static void (*fi_opa1x_rx_atomic_dispatch_table[FI_DATATYPE_LAST][FI_ATOMIC_OP_LAST])(void*, void*, size_t) =
	{
		{ FI_OPA1X_RX_ATOMIC_DISPATCH_FUNC_NAMES(INT8) },
		{ FI_OPA1X_RX_ATOMIC_DISPATCH_FUNC_NAMES(UINT8) },
		{ FI_OPA1X_RX_ATOMIC_DISPATCH_FUNC_NAMES(INT16) },
		{ FI_OPA1X_RX_ATOMIC_DISPATCH_FUNC_NAMES(UINT16) },
		{ FI_OPA1X_RX_ATOMIC_DISPATCH_FUNC_NAMES(INT32) },
		{ FI_OPA1X_RX_ATOMIC_DISPATCH_FUNC_NAMES(UINT32) },
		{ FI_OPA1X_RX_ATOMIC_DISPATCH_FUNC_NAMES(INT64) },
		{ FI_OPA1X_RX_ATOMIC_DISPATCH_FUNC_NAMES(UINT64) },
		{ FI_OPA1X_RX_ATOMIC_DISPATCH_FUNC_NAMES(FLOAT) },
		{ FI_OPA1X_RX_ATOMIC_DISPATCH_FUNC_NAMES(DOUBLE) },
		{ FI_OPA1X_RX_ATOMIC_DISPATCH_FUNC_NAMES_COMPLEX(FLOAT) },
		{ FI_OPA1X_RX_ATOMIC_DISPATCH_FUNC_NAMES_COMPLEX(DOUBLE) },
		{ FI_OPA1X_RX_ATOMIC_DISPATCH_FUNC_NAMES(LONG_DOUBLE) },
		{ FI_OPA1X_RX_ATOMIC_DISPATCH_FUNC_NAMES_COMPLEX(LONG_DOUBLE) }
	};

	fi_opa1x_rx_atomic_dispatch_table[dt][op](buf, addr, nbytes);
}
/*
 * --------------------------- end: rx atomics --------------------------------
 */











static inline int fi_opa1x_check_atomic(struct fi_opa1x_ep *opa1x_ep,
		enum fi_datatype dt, enum fi_op op,
		size_t count)
{
#ifdef DEBUG
	switch((int)op) {
	case FI_MIN:
	case FI_MAX:
	case FI_SUM:
	case FI_PROD:
	case FI_LOR:
	case FI_LAND:
	case FI_BOR:
	case FI_BAND:
	case FI_LXOR:
	case FI_ATOMIC_READ:
	case FI_ATOMIC_WRITE:
	case FI_CSWAP:
	case FI_CSWAP_NE:
	case FI_CSWAP_LE:
	case FI_CSWAP_LT:
	case FI_CSWAP_GE:
	case FI_CSWAP_GT:
	case FI_MSWAP:
		break;
	default:
		return -FI_EINVAL;
	}
	if (((int) dt >= FI_DATATYPE_LAST) || ((int) dt < 0))
		return -FI_EINVAL;

	if (!opa1x_ep)
		return -FI_EINVAL;
	if (opa1x_ep->state != FI_OPA1X_EP_ENABLED)
		return -FI_EINVAL;

	if (count == 0)
		return -FI_EINVAL;

	const enum fi_av_type av_type = opa1x_ep->av->av_type;

	if (av_type == FI_AV_UNSPEC)
		return -FI_EINVAL;
	if (av_type == FI_AV_MAP && opa1x_ep->av_type != FI_AV_MAP)
		return -FI_EINVAL;
	if (av_type == FI_AV_TABLE && opa1x_ep->av_type != FI_AV_TABLE)
		return -FI_EINVAL;
#endif
	return 0;
}
#if 0
static inline size_t sizeofdt(const enum fi_datatype datatype) {

	static const size_t sizeofdt[FI_DATATYPE_LAST] = {
		sizeof(int8_t),			/* FI_INT8 */
		sizeof(uint8_t),		/* FI_UINT8 */
		sizeof(int16_t),		/* FI_INT16 */
		sizeof(uint16_t),		/* FI_UINT16 */
		sizeof(int32_t),		/* FI_INT32 */
		sizeof(uint32_t),		/* FI_UINT32 */
		sizeof(int64_t),		/* FI_INT64 */
		sizeof(uint64_t),		/* FI_UINT64 */
		sizeof(float),			/* FI_FLOAT */
		sizeof(double),			/* FI_DOUBLE */
		sizeof(complex float),		/* FI_FLOAT_COMPLEX */
		sizeof(complex double),		/* FI_DOUBLE_COMPLEX */
		sizeof(long double),		/* FI_LONG_DOUBLE */
		sizeof(complex long double)	/* FI_LONG_DOUBLE_COMPLEX */
	};

	return sizeofdt[datatype];
}

static inline size_t maxcount (const enum fi_datatype datatype,
		const unsigned is_compare,
		const unsigned is_fetch) {

#define INIT_MAXCOUNT_ARRAY(maxbytes)			\
	maxbytes / sizeof(int8_t),		/* FI_INT8 */		\
	maxbytes / sizeof(uint8_t),		/* FI_UINT8 */		\
	maxbytes / sizeof(int16_t),		/* FI_INT16 */		\
	maxbytes / sizeof(uint16_t),		/* FI_UINT16 */		\
	maxbytes / sizeof(int32_t),		/* FI_INT32 */		\
	maxbytes / sizeof(uint32_t),		/* FI_UINT32 */		\
	maxbytes / sizeof(int64_t),		/* FI_INT64 */		\
	maxbytes / sizeof(uint64_t),		/* FI_UINT64 */		\
	maxbytes / sizeof(float),		/* FI_FLOAT */		\
	maxbytes / sizeof(double),		/* FI_DOUBLE */		\
	maxbytes / sizeof(complex float),	/* FI_FLOAT_COMPLEX */	\
	maxbytes / sizeof(complex double),	/* FI_DOUBLE_COMPLEX */	\
	maxbytes / sizeof(long double),		/* FI_LONG_DOUBLE */	\
	maxbytes / sizeof(complex long double)	/* FI_LONG_DOUBLE_COMPLEX */

	static const size_t maxcount[2][2][FI_DATATYPE_LAST] = {
		{
			{	/* !compare, !fetch */
				INIT_MAXCOUNT_ARRAY(FI_OPA1X_HFI1_PACKET_MTU)
			},
			{	/* !compare, fetch */
				INIT_MAXCOUNT_ARRAY((FI_OPA1X_HFI1_PACKET_MTU-sizeof(struct fi_opa1x_hfi1_fetch_metadata)))
			}
		},
		{
			{	/* compare, !fetch */
				INIT_MAXCOUNT_ARRAY(FI_OPA1X_HFI1_PACKET_MTU >> 1)
			},
			{	/* compare, fetch */
				INIT_MAXCOUNT_ARRAY(((FI_OPA1X_HFI1_PACKET_MTU >> 1)-sizeof(struct fi_opa1x_hfi1_fetch_metadata)))
			}
		}
	};

#undef INIT_MAXCOUNT_ARRAY

	return maxcount[is_compare][is_fetch][datatype];
}
#endif
static inline void fi_opa1x_atomic_fence (struct fi_opa1x_ep * opa1x_ep,
		const uint64_t tx_op_flags,
		const union fi_opa1x_addr * opa1x_dst_addr,
		union fi_opa1x_context * opa1x_context,
		const int lock_required, const enum fi_av_type av_type)
{

abort();
#if 0
	const uint64_t do_cq = ((tx_op_flags & FI_COMPLETION) == FI_COMPLETION);

	struct fi_opa1x_cntr * write_cntr = opa1x_ep->write_cntr;
	const uint64_t do_cntr = (write_cntr != 0);
	assert(do_cq || do_cntr);

		MUHWI_Descriptor_t * model = &opa1x_ep->tx.atomic.emulation.fence.mfifo_model;

		MUHWI_Descriptor_t * desc =
			fi_opa1x_spi_injfifo_tail_wait(&opa1x_ep->tx.injfifo);

		qpx_memcpy64((void*)desc, (const void*)model);

		/* set the destination torus address and fifo map */
		desc->PacketHeader.NetworkHeader.pt2pt.Destination = fi_opa1x_uid_get_destination(opa1x_dst_addr->uid.fi);

		const uint64_t fifo_map = (uint64_t) fi_opa1x_addr_get_fifo_map(opa1x_dst_addr->fi);
		desc->Torus_FIFO_Map = fifo_map;

		desc->PacketHeader.messageUnitHeader.Packet_Types.Memory_FIFO.Rec_FIFO_Id =
			fi_opa1x_addr_rec_fifo_id(opa1x_dst_addr->fi);

		/* locate the payload lookaside slot */
		void * payload =
			fi_opa1x_spi_injfifo_immediate_payload(&opa1x_ep->tx.injfifo,
				desc, &desc->Pa_Payload);

		if (do_cntr && !do_cq) {	/* likely */

			/* increment the origin fi_cntr value */

			/* copy the 'fi_atomic' counter completion descriptor
			 * model into the payload lookaside slot */
			model = &opa1x_ep->tx.atomic.emulation.fence.cntr_model;
			MUHWI_Descriptor_t * cntr_desc = (MUHWI_Descriptor_t *) payload;
			qpx_memcpy64((void*)cntr_desc, (const void*)model);

			cntr_desc->Torus_FIFO_Map = fifo_map;

			MUSPI_SetRecPayloadBaseAddressInfo(cntr_desc, write_cntr->std.batid,
				MUSPI_GetAtomicAddress(0, MUHWI_ATOMIC_OPCODE_STORE_ADD));	/* TODO - init */

		} else if (do_cq) {

			/* add the cq byte counter decrement direct-put
			 * descriptor to the tail of the rget/mfifo payload */

			/* initialize the completion entry */
			assert(opa1x_context);
			assert(((uintptr_t)opa1x_context & 0x07ull) == 0);	/* must be 8 byte aligned */
			opa1x_context->flags = FI_RMA | FI_READ;
			opa1x_context->len = 0;
			opa1x_context->buf = NULL;
			opa1x_context->byte_counter = 1;
			opa1x_context->tag = 0;

			uint64_t byte_counter_paddr = 0;
			uint32_t cnk_rc __attribute__ ((unused));
			cnk_rc = fi_opa1x_cnk_vaddr2paddr((void*)&opa1x_context->byte_counter,
					sizeof(uint64_t), &byte_counter_paddr);
			assert(cnk_rc == 0);

			/* copy the 'fi_atomic' cq completion descriptor
			 * model into the payload lookaside slot */
			model = &opa1x_ep->tx.atomic.emulation.fence.cq_model;
			MUHWI_Descriptor_t * cq_desc = (MUHWI_Descriptor_t *) payload;
			qpx_memcpy64((void*)cq_desc, (const void*)model);

			cq_desc->Torus_FIFO_Map = fifo_map;

			MUSPI_SetRecPayloadBaseAddressInfo(cq_desc,
				FI_OPA1X_MU_BAT_ID_GLOBAL, byte_counter_paddr);

			fi_opa1x_cq_enqueue_pending(opa1x_ep->send_cq, opa1x_context, lock_required);

			if (do_cntr) {

				/* increment the origin fi_cntr value */

				/* copy the 'fi_atomic' counter completion descriptor
				 * model into the payload lookaside slot */
				model = &opa1x_ep->tx.atomic.emulation.fence.cntr_model;
				MUHWI_Descriptor_t * cntr_desc = &(((MUHWI_Descriptor_t *) payload)[1]);
				qpx_memcpy64((void*)cntr_desc, (const void*)model);

				cntr_desc->Torus_FIFO_Map = fifo_map;

				MUSPI_SetRecPayloadBaseAddressInfo(cntr_desc, write_cntr->std.batid,
					MUSPI_GetAtomicAddress(0, MUHWI_ATOMIC_OPCODE_STORE_ADD));	/* TODO - init */

				desc->Message_Length += sizeof(MUHWI_Descriptor_t);
				union fi_opa1x_mu_packet_hdr * hdr = (union fi_opa1x_mu_packet_hdr *) &desc->PacketHeader;
				hdr->rma.ndesc += 1;
			}

		} else {	/* !do_cntr && !do_cq */

			assert(0);

		}

		MUSPI_InjFifoAdvanceDesc(opa1x_ep->tx.injfifo.muspi_injfifo);
#endif
}

static inline size_t fi_opa1x_atomic_internal(struct fi_opa1x_ep *opa1x_ep,
		const void *buf, size_t count, const fi_addr_t dst_addr,
		uint64_t addr, uint64_t key, enum fi_datatype datatype,
		enum fi_op op, void *context,
		const unsigned is_fetch, const void * fetch_vaddr,
		const unsigned is_compare, const void * compare_vaddr,
		const uint64_t tx_op_flags, const int lock_required, const enum fi_av_type av_type,
		const uint64_t enable_cntr, const uint64_t enable_cq,
		const unsigned is_inject)
{
	fprintf(stderr, "%s:%s():%d abort\n", __FILE__, __func__, __LINE__); abort();


	return 0;
#if 0
	assert((is_fetch==0)||(is_fetch==1));
	assert((is_compare==0)||(is_compare==1));

	const uint64_t do_cq = enable_cq && ((tx_op_flags & FI_COMPLETION) == FI_COMPLETION);
	struct fi_opa1x_cntr * write_cntr = opa1x_ep->write_cntr;
	const uint64_t do_cntr = enable_cntr && (write_cntr != 0);

	const size_t max_count = maxcount(datatype, is_compare, is_fetch);
	const size_t xfer_count = MIN(max_count,count);
	const uint32_t nbytes = (uint32_t)(sizeofdt(datatype) * xfer_count);

	MUHWI_Descriptor_t * desc =
		fi_opa1x_spi_injfifo_tail_wait(&opa1x_ep->tx.injfifo);

	qpx_memcpy64((void*)desc, (const void*)&opa1x_ep->tx.atomic.emulation.mfifo_model);

	/* set the destination torus address and fifo map */
	desc->PacketHeader.NetworkHeader.pt2pt.Destination = fi_opa1x_uid_get_destination(opa1x_dst_addr->uid.fi);
	const uint64_t fifo_map = (uint64_t) fi_opa1x_addr_get_fifo_map(opa1x_dst_addr->fi);
	desc->Torus_FIFO_Map = fifo_map;

	desc->PacketHeader.messageUnitHeader.Packet_Types.Memory_FIFO.Rec_FIFO_Id =
		fi_opa1x_addr_rec_fifo_id(opa1x_dst_addr->fi);

	const size_t max_count = maxcount(datatype, is_compare, is_fetch);
	const size_t xfer_count = MIN(max_count,count);
	const uint32_t nbytes = (uint32_t)(sizeofdt(datatype) * xfer_count);

	union fi_opa1x_mu_packet_hdr * hdr = (union fi_opa1x_mu_packet_hdr *) &desc->PacketHeader;
	hdr->atomic.dt = datatype;
	hdr->atomic.op = op;
	hdr->atomic.do_cntr = do_cntr;
	hdr->atomic.cntr_bat_id = do_cntr ? write_cntr->std.batid : -1;
	hdr->atomic.nbytes_minus_1 = nbytes - 1;
	hdr->atomic.key = (uint16_t)key;
	hdr->atomic.offset = addr;
	hdr->atomic.is_local = fi_opa1x_addr_is_local(opa1x_dst_addr->fi);

	hdr->atomic.is_fetch = is_fetch;


	if (is_inject) {	/* const expression with cause branch to compile out */

		/* locate the payload lookaside slot */
		void * payload =
			fi_opa1x_spi_injfifo_immediate_payload(&opa1x_ep->tx.injfifo,
				desc, &desc->Pa_Payload);

		desc->Message_Length = nbytes;

		if (buf) memcpy((void *)payload, (const void *)buf, nbytes);

	} else if (!is_fetch && !is_compare) {	/* const expression with cause branch to compile out */

		desc->Message_Length = nbytes;
		fi_opa1x_cnk_vaddr2paddr(buf, nbytes, &desc->Pa_Payload);

		assert(!do_cq);

	} else {

		/* locate the payload lookaside slot */
		union fi_opa1x_mu_packet_payload * payload =
			(union fi_opa1x_mu_packet_payload *)fi_opa1x_spi_injfifo_immediate_payload(&opa1x_ep->tx.injfifo,
				desc, &desc->Pa_Payload);

		/* initialize the atomic operation metadata in the packet payload */
		payload->atomic_fetch.metadata.fifo_map = fifo_map;
		payload->atomic_fetch.metadata.cq_paddr = 0;

		if (is_fetch) {
			fi_opa1x_cnk_vaddr2paddr(fetch_vaddr, nbytes,
				&payload->atomic_fetch.metadata.dst_paddr);

			/* copy the origin (source) data into the injection lookaside buffer */
			if (buf) memcpy((void*)&payload->atomic_fetch.data[0], (const void*) buf, nbytes);
			desc->Message_Length = sizeof(struct fi_opa1x_mu_fetch_metadata) +
				nbytes + nbytes * is_compare;

			if (is_compare) {
				/* copy the origin (compare) data into the injection lookaside buffer */
				memcpy((void*)&payload->atomic_fetch.data[nbytes], compare_vaddr, nbytes);
			}

			if (do_cq) {

				/* initialize the completion entry */
				assert(context);
				assert(((uintptr_t)context & 0x07ull) == 0);	/* must be 8 byte aligned */
				union fi_opa1x_context * opa1x_context = (union fi_opa1x_context *)context;
				opa1x_context->flags = 0;		/* TODO */
				opa1x_context->len = nbytes;
				opa1x_context->buf = NULL;
				opa1x_context->byte_counter = nbytes;
				opa1x_context->tag = 0;

				fi_opa1x_cnk_vaddr2paddr((const void*)&opa1x_context->byte_counter,
					sizeof(uint64_t), &payload->atomic_fetch.metadata.cq_paddr);

				fi_opa1x_cq_enqueue_pending(opa1x_ep->tx.send_cq, opa1x_context, lock_required);
			}

		} else {
			assert(0);	/* !fetch, compare */
		}
	}

	MUSPI_InjFifoAdvanceDesc(opa1x_ep->tx.injfifo.muspi_injfifo);
	return xfer_count;
#endif
}


//static inline 
ssize_t fi_opa1x_atomic_generic(struct fid_ep *ep,
		const void *buf, size_t count,
		fi_addr_t dst_addr, uint64_t addr,
		uint64_t key, enum fi_datatype datatype,
		enum fi_op op, void* context,
		const int lock_required,
		const enum fi_av_type av_type)
{
	struct fi_opa1x_ep	*opa1x_ep;

	opa1x_ep = container_of(ep, struct fi_opa1x_ep, ep_fid);

	if (lock_required) { fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__); abort(); }

	union fi_opa1x_addr opa1x_addr = { .fi = dst_addr };
	const unsigned rx_ctx_bits = opa1x_ep->tx.rx_ctx_bits;
	//const unsigned rx_index = rx_ctx_bits == 0 ? 0 : (unsigned)(dst_addr >> (64 - rx_ctx_bits));
	if (av_type == FI_AV_TABLE) {
		const unsigned av_index = FI_OPA1X_FI_ADDR_TO_AV_INDEX(dst_addr, rx_ctx_bits);
		opa1x_addr = opa1x_ep->tx.av_addr[av_index];
	}
	//const uint8_t u8_rx = opa1x_addr.hfi1_rx + rx_index;

	size_t xfer __attribute__ ((unused));
	xfer = fi_opa1x_atomic_internal(opa1x_ep, buf, count,
		opa1x_addr.fi, addr, key, datatype, op,
		context, 0, NULL, 0, NULL,
		opa1x_ep->tx.op_flags, lock_required, av_type, 0, 0, 0);
	assert(xfer == count);

	return 0;
}

static inline ssize_t fi_opa1x_atomic_writemsg_generic(struct fid_ep *ep,
		const struct fi_msg_atomic *msg, const uint64_t flags,
		const int lock_required, const enum fi_av_type av_type)
{
	struct fi_opa1x_ep *opa1x_ep;
	opa1x_ep = container_of(ep, struct fi_opa1x_ep, ep_fid);

	const enum fi_datatype datatype = msg->datatype;
	const enum fi_op op = msg->op;

#ifndef NDEBUG
	int ret = 0;
	ret = fi_opa1x_check_atomic(opa1x_ep, datatype, op, 1);
	if (ret) return ret;
#endif

	if (lock_required) { fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__); abort(); }

	union fi_opa1x_addr * opa1x_dst_addr = (union fi_opa1x_addr *)&msg->addr;

	const size_t dtsize = sizeofdt(datatype);

	size_t rma_iov_index = 0;
	const size_t rma_iov_count = msg->rma_iov_count;
	uint64_t rma_iov_dtcount = msg->rma_iov[rma_iov_index].count;
	uint64_t rma_iov_addr = msg->rma_iov[rma_iov_index].addr;
	uint64_t rma_iov_key = msg->rma_iov[rma_iov_index].key;

	size_t msg_iov_index = 0;
	const size_t msg_iov_count = msg->iov_count;
	uint64_t msg_iov_dtcount = msg->msg_iov[msg_iov_index].count;
	uintptr_t msg_iov_vaddr = (uintptr_t)msg->msg_iov[msg_iov_index].addr;

	while (msg_iov_dtcount != 0 && rma_iov_dtcount != 0) {

		const size_t count_requested = MIN(msg_iov_dtcount,rma_iov_dtcount);

		const size_t count_transfered =
			fi_opa1x_atomic_internal(opa1x_ep, (void*)msg_iov_vaddr,
				count_requested, msg->addr, rma_iov_addr,
				rma_iov_key, datatype, op, NULL,
				0, NULL, 0, NULL, flags, lock_required, av_type, 0, 0, 0);

		const size_t bytes_transfered = dtsize * count_transfered;

		msg_iov_dtcount -= count_transfered;
		msg_iov_vaddr += bytes_transfered;

		if ((msg_iov_dtcount == 0) && ((msg_iov_index+1) < msg_iov_count)) {
			++msg_iov_index;
			msg_iov_dtcount = msg->msg_iov[msg_iov_index].count;
			msg_iov_vaddr = (uintptr_t)msg->msg_iov[msg_iov_index].addr;
		}

		rma_iov_dtcount -= count_transfered;
		rma_iov_addr  += bytes_transfered;

		if ((rma_iov_dtcount == 0) && ((rma_iov_index+1) < rma_iov_count)) {
			++rma_iov_index;
			rma_iov_dtcount = msg->rma_iov[rma_iov_index].count;
			rma_iov_addr = msg->rma_iov[rma_iov_index].addr;
			rma_iov_key = msg->rma_iov[rma_iov_index].key;
		}
	}

	fi_opa1x_atomic_fence(opa1x_ep, flags, opa1x_dst_addr,
		(union fi_opa1x_context *)msg->context,
		lock_required, av_type);

	return 0;
}




static inline ssize_t fi_opa1x_atomic_readwritemsg_generic (struct fid_ep *ep,
		const struct fi_msg_atomic *msg,
		struct fi_ioc *resultv,
		const size_t result_count,
		const uint64_t flags,
		const int lock_required,
		const enum fi_av_type av_type)
{
	struct fi_opa1x_ep *opa1x_ep;
	opa1x_ep = container_of(ep, struct fi_opa1x_ep, ep_fid);

	const enum fi_datatype datatype = msg->datatype;
	const enum fi_op op = msg->op;

#ifndef NDEBUG
	int ret = 0;
	ret = fi_opa1x_check_atomic(opa1x_ep, datatype, op, 1);
	if (ret) return ret;
#endif

	if (lock_required) { fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__); abort(); }

	union fi_opa1x_addr * opa1x_dst_addr = (union fi_opa1x_addr *)&msg->addr;

	const size_t dtsize = sizeofdt(datatype);

	size_t rma_iov_index = 0;
	const size_t rma_iov_count = msg->rma_iov_count;
	uint64_t rma_iov_dtcount = msg->rma_iov[rma_iov_index].count;
	uint64_t rma_iov_addr = msg->rma_iov[rma_iov_index].addr;
	uint64_t rma_iov_key = msg->rma_iov[rma_iov_index].key;

	size_t rst_iov_index = 0;
	const size_t rst_iov_count = result_count;
	uint64_t rst_iov_dtcount = resultv[rst_iov_index].count;
	uintptr_t rst_iov_vaddr = (uintptr_t)resultv[rst_iov_index].addr;

	if (op != FI_ATOMIC_READ) {	/* likely */

		size_t msg_iov_index = 0;
		const size_t msg_iov_count = msg->iov_count;
		uint64_t msg_iov_dtcount = msg->msg_iov[msg_iov_index].count;
		uintptr_t msg_iov_vaddr = (uintptr_t)msg->msg_iov[msg_iov_index].addr;

		size_t count_requested = MIN3(msg_iov_dtcount, rma_iov_dtcount, rst_iov_dtcount);

		while (count_requested > 0) {

			const size_t count_transfered =
				fi_opa1x_atomic_internal(opa1x_ep, (void*)msg_iov_vaddr,
					count_requested, msg->addr, rma_iov_addr,
					rma_iov_key, datatype, op, NULL,
					1, (const void *)rst_iov_vaddr, 0, NULL,
					flags, lock_required, av_type, 0, 0, 0);

			const size_t bytes_transfered = dtsize * count_transfered;

			msg_iov_dtcount -= count_transfered;
			msg_iov_vaddr += bytes_transfered;

			if ((msg_iov_dtcount == 0) && ((msg_iov_index+1) < msg_iov_count)) {
				++msg_iov_index;
				msg_iov_dtcount = msg->msg_iov[msg_iov_index].count;
				msg_iov_vaddr = (uintptr_t)msg->msg_iov[msg_iov_index].addr;
			}

			rma_iov_dtcount -= count_transfered;
			rma_iov_addr  += bytes_transfered;

			if ((rma_iov_dtcount == 0) && ((rma_iov_index+1) < rma_iov_count)) {
				++rma_iov_index;
				rma_iov_dtcount = msg->rma_iov[rma_iov_index].count;
				rma_iov_addr = msg->rma_iov[rma_iov_index].addr;
				rma_iov_key = msg->rma_iov[rma_iov_index].key;
			}

			rst_iov_dtcount -= count_transfered;
			rst_iov_vaddr += bytes_transfered;

			if ((rst_iov_dtcount == 0) && ((rst_iov_index+1) < rst_iov_count)) {
				++rst_iov_index;
				rst_iov_dtcount = resultv[rst_iov_index].count;
				rst_iov_vaddr = (uintptr_t)resultv[rst_iov_index].addr;
			}

			count_requested = MIN3(msg_iov_dtcount, rma_iov_dtcount, rst_iov_dtcount);
		}

	} else {

		size_t count_requested = MIN(rma_iov_dtcount, rst_iov_dtcount);

		while (rma_iov_dtcount != 0 && rst_iov_dtcount != 0) {

			const size_t count_transfered =
				fi_opa1x_atomic_internal(opa1x_ep, NULL,
					count_requested, msg->addr, rma_iov_addr,
					rma_iov_key, datatype, op, NULL,
					1, (const void *)rst_iov_vaddr, 0, NULL,
					flags, lock_required, av_type, 0, 0, 0);

			const size_t bytes_transfered = dtsize * count_transfered;

			rma_iov_dtcount -= count_transfered;
			rma_iov_addr  += bytes_transfered;

			if ((rma_iov_dtcount == 0) && ((rma_iov_index+1) < rma_iov_count)) {
				++rma_iov_index;
				rma_iov_dtcount = msg->rma_iov[rma_iov_index].count;
				rma_iov_addr = msg->rma_iov[rma_iov_index].addr;
				rma_iov_key = msg->rma_iov[rma_iov_index].key;
			}

			rst_iov_dtcount -= count_transfered;
			rst_iov_vaddr += bytes_transfered;

			if ((rst_iov_dtcount == 0) && ((rst_iov_index+1) < rst_iov_count)) {
				++rst_iov_index;
				rst_iov_dtcount = resultv[rst_iov_index].count;
				rst_iov_vaddr = (uintptr_t)resultv[rst_iov_index].addr;
			}

			count_requested = MIN(rma_iov_dtcount, rst_iov_dtcount);
		}
	}

	fi_opa1x_atomic_fence(opa1x_ep, flags, opa1x_dst_addr,
		(union fi_opa1x_context *)msg->context,
		lock_required, av_type);

	return 0;
}

static inline ssize_t fi_opa1x_atomic_compwritemsg_generic (struct fid_ep *ep,
		const struct fi_msg_atomic *msg,
		const struct fi_ioc *comparev,
		size_t compare_count,
		struct fi_ioc *resultv,
		size_t result_count,
		uint64_t flags,
		const int lock_required,
		const enum fi_av_type av_type)
{
	struct fi_opa1x_ep *opa1x_ep;
	opa1x_ep = container_of(ep, struct fi_opa1x_ep, ep_fid);

	const enum fi_datatype datatype = msg->datatype;
	const enum fi_op op = msg->op;

#ifndef NDEBUG
	int ret = 0;
	ret = fi_opa1x_check_atomic(opa1x_ep, datatype, op, 1);
	if (ret) return ret;
#endif

	if (lock_required) { fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__); abort(); }

	union fi_opa1x_addr * opa1x_dst_addr = (union fi_opa1x_addr *)&msg->addr;

	const size_t dtsize = sizeofdt(datatype);

	size_t rma_iov_index = 0;
	const size_t rma_iov_count = msg->rma_iov_count;
	uint64_t rma_iov_dtcount = msg->rma_iov[rma_iov_index].count;
	uint64_t rma_iov_addr = msg->rma_iov[rma_iov_index].addr;
	uint64_t rma_iov_key = msg->rma_iov[rma_iov_index].key;

	size_t msg_iov_index = 0;
	const size_t msg_iov_count = msg->iov_count;
	uint64_t msg_iov_dtcount = msg->msg_iov[msg_iov_index].count;
	uintptr_t msg_iov_vaddr = (uintptr_t)msg->msg_iov[msg_iov_index].addr;

	size_t rst_iov_index = 0;
	const size_t rst_iov_count = result_count;
	uint64_t rst_iov_dtcount = resultv[rst_iov_index].count;
	uintptr_t rst_iov_vaddr = (uintptr_t)resultv[rst_iov_index].addr;

	size_t cmp_iov_index = 0;
	const size_t cmp_iov_count = compare_count;
	uint64_t cmp_iov_dtcount = comparev[cmp_iov_index].count;
	uintptr_t cmp_iov_vaddr = (uintptr_t)comparev[cmp_iov_index].addr;

	while (msg_iov_dtcount != 0 && rma_iov_dtcount != 0 && rst_iov_dtcount != 0 && cmp_iov_dtcount != 0) {

		const size_t count_requested =
			MIN4(msg_iov_dtcount,rma_iov_dtcount,rst_iov_dtcount,cmp_iov_dtcount);

		const size_t count_transfered =
			fi_opa1x_atomic_internal(opa1x_ep, (void*)msg_iov_vaddr,
				count_requested, msg->addr, rma_iov_addr,
				rma_iov_key, datatype, op, NULL,
				1, (const void *)rst_iov_vaddr, 1, (const void *)cmp_iov_vaddr,
				flags, lock_required, av_type, 0, 0, 0);

		const size_t bytes_transfered = dtsize * count_transfered;

		msg_iov_dtcount -= count_transfered;
		msg_iov_vaddr += bytes_transfered;

		if ((msg_iov_dtcount == 0) && ((msg_iov_index+1) < msg_iov_count)) {
			++msg_iov_index;
			msg_iov_dtcount = msg->msg_iov[msg_iov_index].count;
			msg_iov_vaddr = (uintptr_t)msg->msg_iov[msg_iov_index].addr;
		}

		rma_iov_dtcount -= count_transfered;
		rma_iov_addr  += bytes_transfered;

		if ((rma_iov_dtcount == 0) && ((rma_iov_index+1) < rma_iov_count)) {
			++rma_iov_index;
			rma_iov_dtcount = msg->rma_iov[rma_iov_index].count;
			rma_iov_addr = msg->rma_iov[rma_iov_index].addr;
			rma_iov_key = msg->rma_iov[rma_iov_index].key;
		}

		rst_iov_dtcount -= count_transfered;
		rst_iov_vaddr += bytes_transfered;

		if ((rst_iov_dtcount == 0) && ((rst_iov_index+1) < rst_iov_count)) {
			++rst_iov_index;
			rst_iov_dtcount = resultv[rst_iov_index].count;
			rst_iov_vaddr = (uintptr_t)resultv[rst_iov_index].addr;
		}

		cmp_iov_dtcount -= count_transfered;
		cmp_iov_vaddr += bytes_transfered;

		if ((cmp_iov_dtcount == 0) && ((cmp_iov_index+1) < cmp_iov_count)) {
			++cmp_iov_index;
			cmp_iov_dtcount = comparev[cmp_iov_index].count;
			cmp_iov_vaddr = (uintptr_t)comparev[cmp_iov_index].addr;
		}
	}

	fi_opa1x_atomic_fence(opa1x_ep, flags, opa1x_dst_addr,
		(union fi_opa1x_context *)msg->context,
		lock_required, av_type);

	return 0;
}

/*
 * Generic function to handle both fetching (1 operand) and compare
 * (2 operand) atomics.
 */
static inline ssize_t fi_opa1x_fetch_compare_atomic_generic(struct fid_ep *ep,
		const void *buf, size_t count,
		void *desc,
		const void *compare, void *compare_desc,
		void *result, void *result_desc,
		fi_addr_t dest_addr, uint64_t addr,
		uint64_t key, enum fi_datatype datatype,
		enum fi_op op, void *context,
		int lock_required, const enum fi_av_type av_type)
{
/* MPICH does NOT call fi_fetch_atomic or fi_compare_atomic so these functions
 * have not been properly tested - for now just abort and come back later
 * and implement if an application on OPA1X needs this.
 */
	abort();

	struct fi_opa1x_ep *opa1x_ep __attribute__ ((unused));
	opa1x_ep = container_of(ep, struct fi_opa1x_ep, ep_fid);

#ifndef NDEBUG
	int ret = 0;
	ret = fi_opa1x_check_atomic(opa1x_ep, datatype, op, count);
	if (ret) return ret;
#endif

	if (lock_required) { fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__); abort(); }

	return 0;

}

//static inline
ssize_t fi_opa1x_fetch_atomic_generic(struct fid_ep *ep,
		const void *buf, size_t count,
		void *desc,
		void *result, void *result_desc,
		fi_addr_t dest_addr, uint64_t addr,
		uint64_t key, enum fi_datatype datatype,
		enum fi_op op, void *context,
		const int lock_required, const enum fi_av_type av_type)
{



	return fi_opa1x_fetch_compare_atomic_generic(ep,
			buf, count, desc, NULL, NULL,
			result, result_desc, dest_addr, addr,
			key, datatype, op, context,
			lock_required, av_type);
}

//static inline 
ssize_t fi_opa1x_compare_atomic_generic(struct fid_ep *ep,
		const void *buf, size_t count, void *desc,
		const void *compare, void *compare_desc,
		void  *result, void *result_desc,
		fi_addr_t dest_addr, uint64_t addr,
		uint64_t key, enum fi_datatype datatype,
		enum fi_op op, void *context,
		const int lock_required, const enum fi_av_type av_type)
{
	return fi_opa1x_fetch_compare_atomic_generic(ep,
			buf, count, desc, compare, compare_desc,
			result, result_desc, dest_addr, addr,
			key, datatype, op, context,
			lock_required, av_type);
}

//static inline 
ssize_t fi_opa1x_inject_atomic_generic(struct fid_ep *ep,
                const void *buf, size_t count,
                fi_addr_t dest_addr, uint64_t addr, uint64_t key,
                enum fi_datatype datatype, enum fi_op op,
		const int lock_required, const enum fi_av_type av_type)
{
	struct fi_opa1x_ep *opa1x_ep;
	opa1x_ep = container_of(ep, struct fi_opa1x_ep, ep_fid);

#ifndef NDEBUG
	int ret = 0;
	ret = fi_opa1x_check_atomic(opa1x_ep, datatype, op, count);
	if (ret) return ret;
#endif

	if (lock_required) { fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__); abort(); }

	assert(dest_addr != FI_ADDR_UNSPEC);

	

	fi_opa1x_atomic_internal(opa1x_ep, buf, count,
		dest_addr, addr, key, datatype, op,
		NULL, 0, NULL, 0, NULL,
		opa1x_ep->tx.op_flags, lock_required, av_type, 1, 0, 1);

	return 0;
}









































ssize_t fi_opa1x_atomic(struct fid_ep *ep,
		const void *buf, size_t count, void *desc,
		fi_addr_t dst_addr, uint64_t addr,
		uint64_t key, enum fi_datatype datatype,
		enum fi_op op, void* context)
{
	struct fi_opa1x_ep * opa1x_ep;
	opa1x_ep = container_of(ep, struct fi_opa1x_ep, ep_fid);

	switch (opa1x_ep->threading) {
        case FI_THREAD_ENDPOINT:
        case FI_THREAD_DOMAIN:
        case FI_THREAD_COMPLETION:
		return fi_opa1x_atomic_generic(ep, buf, count, dst_addr,
			addr, key, datatype, op, context,
			0,	/* lock_required */
			opa1x_ep->av_type);
                break;
        case FI_THREAD_FID:
        case FI_THREAD_UNSPEC:
        case FI_THREAD_SAFE:
		return fi_opa1x_atomic_generic(ep, buf, count, dst_addr,
			addr, key, datatype, op, context,
			1,	/* lock_required */
			opa1x_ep->av_type);

                break;
        default:
                return -FI_EINVAL;
        }

}

ssize_t fi_opa1x_fetch_atomic(struct fid_ep *ep,
		const void *buf, size_t count,
		void *desc,
		void *result, void *result_desc,
		fi_addr_t dest_addr, uint64_t addr,
		uint64_t key, enum fi_datatype datatype,
		enum fi_op op, void *context)
{
	struct fi_opa1x_ep * opa1x_ep;
	opa1x_ep = container_of(ep, struct fi_opa1x_ep, ep_fid);

        switch (opa1x_ep->threading) {
        case FI_THREAD_ENDPOINT:
        case FI_THREAD_DOMAIN:
        case FI_THREAD_COMPLETION:
		if (opa1x_ep->av_type == FI_AV_MAP) {
			return fi_opa1x_fetch_atomic_generic(ep,
				buf, count, desc,
				result, result_desc, dest_addr, addr,
				key, datatype, op, context,
				0,	/* lock_required */
				FI_AV_MAP);
		} else {
			return fi_opa1x_fetch_atomic_generic(ep,
				buf, count, desc,
				result, result_desc, dest_addr, addr,
				key, datatype, op, context,
				0,	/* lock_required */
				FI_AV_TABLE);
		}
                break;
        case FI_THREAD_FID:
        case FI_THREAD_UNSPEC:
        case FI_THREAD_SAFE:
		if (opa1x_ep->av_type == FI_AV_MAP) {
			return fi_opa1x_fetch_atomic_generic(ep,
				buf, count, desc,
				result, result_desc, dest_addr, addr,
				key, datatype, op, context,
				1,	/* lock_required */
				FI_AV_MAP);
		} else {
			return fi_opa1x_fetch_atomic_generic(ep,
				buf, count, desc,
				result, result_desc, dest_addr, addr,
				key, datatype, op, context,
				1,	/* lock_required */
				FI_AV_TABLE);
		}
                break;
        default:
                return -FI_EINVAL;
	}

}

ssize_t fi_opa1x_compare_atomic(struct fid_ep *ep,
		const void *buf, size_t count, void *desc,
		const void *compare, void *compare_desc,
		void  *result, void *result_desc,
		fi_addr_t dest_addr, uint64_t addr,
		uint64_t key, enum fi_datatype datatype,
		enum fi_op op, void *context)
{
	struct fi_opa1x_ep * opa1x_ep;
	opa1x_ep = container_of(ep, struct fi_opa1x_ep, ep_fid);

        switch (opa1x_ep->threading) {
        case FI_THREAD_ENDPOINT:
        case FI_THREAD_DOMAIN:
        case FI_THREAD_COMPLETION:
		if (opa1x_ep->av_type == FI_AV_MAP) {
			return fi_opa1x_compare_atomic_generic(ep,
				buf, count, desc,
				compare, compare_desc,
				result, result_desc, dest_addr, addr,
				key, datatype, op, context,
				0,	/* lock_required */
				FI_AV_MAP);
		} else {
			return fi_opa1x_compare_atomic_generic(ep,
				buf, count, desc,
				compare, compare_desc,
				result, result_desc, dest_addr, addr,
				key, datatype, op, context,
				0,	/* lock_required */
				FI_AV_TABLE);
		}
                break;
        case FI_THREAD_FID:
        case FI_THREAD_UNSPEC:
        case FI_THREAD_SAFE:
		if (opa1x_ep->av_type == FI_AV_MAP) {
			return fi_opa1x_compare_atomic_generic(ep,
				buf, count, desc,
				compare, compare_desc,
				result, result_desc, dest_addr, addr,
				key, datatype, op, context,
				1,	/* lock_required */
				FI_AV_MAP);
		} else {
			return fi_opa1x_compare_atomic_generic(ep,
				buf, count, desc,
				compare, compare_desc,
				result, result_desc, dest_addr, addr,
				key, datatype, op, context,
				1,	/* lock_required */
				FI_AV_TABLE);
		}
                break;
        default:
                return -FI_EINVAL;
	}

}

ssize_t fi_opa1x_inject_atomic(struct fid_ep *ep,
                const void *buf, size_t count,
                fi_addr_t dest_addr, uint64_t addr, uint64_t key,
                enum fi_datatype datatype, enum fi_op op)
{
	struct fi_opa1x_ep * opa1x_ep;
	opa1x_ep = container_of(ep, struct fi_opa1x_ep, ep_fid);

        switch (opa1x_ep->threading) {
        case FI_THREAD_ENDPOINT:
        case FI_THREAD_DOMAIN:
        case FI_THREAD_COMPLETION:
		if (opa1x_ep->av_type == FI_AV_MAP) {
			return fi_opa1x_inject_atomic_generic(ep,
				buf, count,
				dest_addr, addr,
				key, datatype, op,
				0,	/* lock_required */
				FI_AV_MAP);
		} else {
			return fi_opa1x_inject_atomic_generic(ep,
				buf, count,
				dest_addr, addr,
				key, datatype, op,
				0,	/* lock_required */
				FI_AV_TABLE);
		}
                break;
        case FI_THREAD_FID:
        case FI_THREAD_UNSPEC:
        case FI_THREAD_SAFE:
		if (opa1x_ep->av_type == FI_AV_MAP) {
			return fi_opa1x_inject_atomic_generic(ep,
				buf, count,
				dest_addr, addr,
				key, datatype, op,
				1,	/* lock_required */
				FI_AV_MAP);
		} else {
			return fi_opa1x_inject_atomic_generic(ep,
				buf, count,
				dest_addr, addr,
				key, datatype, op,
				1,	/* lock_required */
				FI_AV_TABLE);
		}
                break;
        default:
                return -FI_EINVAL;
	}
}

ssize_t	fi_opa1x_atomicv(struct fid_ep *ep,
			const struct fi_ioc *iov, void **desc, size_t count,
			uint64_t addr, uint64_t key,
			enum fi_datatype datatype, enum fi_op op, void *context)
{
	errno = FI_ENOSYS;
	return -errno;
}

ssize_t fi_opa1x_atomic_writemsg(struct fid_ep *ep,
	const struct fi_msg_atomic *msg, uint64_t flags)
{
	struct fi_opa1x_ep * opa1x_ep;
	opa1x_ep = container_of(ep, struct fi_opa1x_ep, ep_fid);

        switch (opa1x_ep->threading) {
        case FI_THREAD_ENDPOINT:
        case FI_THREAD_DOMAIN:
        case FI_THREAD_COMPLETION:
		if (opa1x_ep->av_type == FI_AV_MAP) {
			return fi_opa1x_atomic_writemsg_generic(ep, msg, flags,
				0, FI_AV_MAP);
		} else {
			return fi_opa1x_atomic_writemsg_generic(ep, msg, flags,
				0, FI_AV_TABLE);
		}
        case FI_THREAD_FID:
        case FI_THREAD_UNSPEC:
        case FI_THREAD_SAFE:
		if (opa1x_ep->av_type == FI_AV_MAP) {
			return fi_opa1x_atomic_writemsg_generic(ep, msg, flags,
				1, FI_AV_MAP);
		} else {
			return fi_opa1x_atomic_writemsg_generic(ep, msg, flags,
				1, FI_AV_TABLE);
		}
	}

	errno = FI_EINVAL;
	return -errno;
}

ssize_t	fi_opa1x_atomic_readwritemsg(struct fid_ep *ep,
		const struct fi_msg_atomic *msg,
		struct fi_ioc *resultv,
	       	void **result_desc, size_t result_count,
		uint64_t flags)
{
	struct fi_opa1x_ep * opa1x_ep;
	opa1x_ep = container_of(ep, struct fi_opa1x_ep, ep_fid);

        switch (opa1x_ep->threading) {
        case FI_THREAD_ENDPOINT:
        case FI_THREAD_DOMAIN:
        case FI_THREAD_COMPLETION:
		if (opa1x_ep->av_type == FI_AV_MAP) {
			return fi_opa1x_atomic_readwritemsg_generic(ep, msg,
					resultv, result_count, flags,
					0, FI_AV_MAP);
		} else {
			return fi_opa1x_atomic_readwritemsg_generic(ep, msg,
					resultv, result_count, flags,
					0, FI_AV_TABLE);
		}
        case FI_THREAD_FID:
        case FI_THREAD_UNSPEC:
        case FI_THREAD_SAFE:
		if (opa1x_ep->av_type == FI_AV_MAP) {
			return fi_opa1x_atomic_readwritemsg_generic(ep, msg,
					resultv, result_count, flags,
					1, FI_AV_MAP);
		} else {
			return fi_opa1x_atomic_readwritemsg_generic(ep, msg,
					resultv, result_count, flags,
					1, FI_AV_TABLE);
		}
	}

	errno = FI_EINVAL;
	return -errno;
}

ssize_t	fi_opa1x_atomic_compwritemsg(struct fid_ep *ep,
		const struct fi_msg_atomic *msg,
		const struct fi_ioc *comparev,
	       	void **compare_desc, size_t compare_count,
		struct fi_ioc *resultv, void **result_desc,
	       	size_t result_count,
		uint64_t flags)
{
	struct fi_opa1x_ep * opa1x_ep;
	opa1x_ep = container_of(ep, struct fi_opa1x_ep, ep_fid);

        switch (opa1x_ep->threading) {
        case FI_THREAD_ENDPOINT:
        case FI_THREAD_DOMAIN:
        case FI_THREAD_COMPLETION:
		if (opa1x_ep->av_type == FI_AV_MAP) {
			return fi_opa1x_atomic_compwritemsg_generic(ep, msg,
					comparev, compare_count,
					resultv, result_count,
					flags, 0, FI_AV_MAP);
		} else {
			return fi_opa1x_atomic_compwritemsg_generic(ep, msg,
					comparev, compare_count,
					resultv, result_count,
					flags, 0, FI_AV_TABLE);
		}
        case FI_THREAD_FID:
        case FI_THREAD_UNSPEC:
        case FI_THREAD_SAFE:
		if (opa1x_ep->av_type == FI_AV_MAP) {
			return fi_opa1x_atomic_compwritemsg_generic(ep, msg,
					comparev, compare_count,
					resultv, result_count,
					flags, 1, FI_AV_MAP);
		} else {
			return fi_opa1x_atomic_compwritemsg_generic(ep, msg,
					comparev, compare_count,
					resultv, result_count,
					flags, 1, FI_AV_TABLE);
		}
	}

	errno = FI_EINVAL;
	return -errno;
}

int fi_opa1x_atomic_writevalid(struct fid_ep *ep, enum fi_datatype datatype,
		enum fi_op op, size_t *count)
{
	static size_t sizeofdt[FI_DATATYPE_LAST] = {
		sizeof(int8_t),			/* FI_INT8 */
		sizeof(uint8_t),		/* FI_UINT8 */
		sizeof(int16_t),		/* FI_INT16 */
		sizeof(uint16_t),		/* FI_UINT16 */
		sizeof(int32_t),		/* FI_INT32 */
		sizeof(uint32_t),		/* FI_UINT32 */
		sizeof(int64_t),		/* FI_INT64 */
		sizeof(uint64_t),		/* FI_UINT64 */
		sizeof(float),			/* FI_FLOAT */
		sizeof(double),			/* FI_DOUBLE */
		sizeof(complex float),		/* FI_FLOAT_COMPLEX */
		sizeof(complex double),		/* FI_DOUBLE_COMPLEX */
		sizeof(long double),		/* FI_LONG_DOUBLE */
		sizeof(complex long double)	/* FI_LONG_DOUBLE_COMPLEX */
	};

	if ((op > FI_ATOMIC_WRITE) || (datatype >= FI_DATATYPE_LAST)) {
		*count = 0;
		errno = FI_EOPNOTSUPP;
		return -errno;
	}

	*count = sizeof(union fi_opa1x_hfi1_packet_payload) / sizeofdt[datatype];
	return 0;
}

int fi_opa1x_atomic_readwritevalid(struct fid_ep *ep,
	       	enum fi_datatype datatype,
		enum fi_op op, size_t *count)
{
	static size_t sizeofdt[FI_DATATYPE_LAST] = {
		sizeof(int8_t),			/* FI_INT8 */
		sizeof(uint8_t),		/* FI_UINT8 */
		sizeof(int16_t),		/* FI_INT16 */
		sizeof(uint16_t),		/* FI_UINT16 */
		sizeof(int32_t),		/* FI_INT32 */
		sizeof(uint32_t),		/* FI_UINT32 */
		sizeof(int64_t),		/* FI_INT64 */
		sizeof(uint64_t),		/* FI_UINT64 */
		sizeof(float),			/* FI_FLOAT */
		sizeof(double),			/* FI_DOUBLE */
		sizeof(complex float),		/* FI_FLOAT_COMPLEX */
		sizeof(complex double),		/* FI_DOUBLE_COMPLEX */
		sizeof(long double),		/* FI_LONG_DOUBLE */
		sizeof(complex long double)	/* FI_LONG_DOUBLE_COMPLEX */
	};

	if ((op > FI_ATOMIC_WRITE) || (datatype >= FI_DATATYPE_LAST)) {
		*count = 0;
		errno = FI_EOPNOTSUPP;
		return -errno;
	}

	*count = (sizeof(union fi_opa1x_hfi1_packet_payload) - sizeof(struct fi_opa1x_hfi1_fetch_metadata)) / sizeofdt[datatype];
	return 0;
}

int fi_opa1x_atomic_compwritevalid(struct fid_ep *ep,
	       	enum fi_datatype datatype,
		enum fi_op op, size_t *count)
{
	static size_t sizeofdt[FI_DATATYPE_LAST] = {
		sizeof(int8_t),			/* FI_INT8 */
		sizeof(uint8_t),		/* FI_UINT8 */
		sizeof(int16_t),		/* FI_INT16 */
		sizeof(uint16_t),		/* FI_UINT16 */
		sizeof(int32_t),		/* FI_INT32 */
		sizeof(uint32_t),		/* FI_UINT32 */
		sizeof(int64_t),		/* FI_INT64 */
		sizeof(uint64_t),		/* FI_UINT64 */
		sizeof(float),			/* FI_FLOAT */
		sizeof(double),			/* FI_DOUBLE */
		sizeof(complex float),		/* FI_FLOAT_COMPLEX */
		sizeof(complex double),		/* FI_DOUBLE_COMPLEX */
		sizeof(long double),		/* FI_LONG_DOUBLE */
		sizeof(complex long double)	/* FI_LONG_DOUBLE_COMPLEX */
	};

	if ((op < FI_CSWAP) || (op >= FI_ATOMIC_OP_LAST) || (datatype >= FI_DATATYPE_LAST)) {
		*count = 0;
		errno = FI_EOPNOTSUPP;
		return -errno;
	}

	*count = (sizeof(union fi_opa1x_hfi1_packet_payload) / 2) / sizeofdt[datatype];
	return 0;
}

static struct fi_ops_atomic fi_opa1x_ops_atomic = {
	.size		= sizeof(struct fi_ops_atomic),
	.write		= fi_no_atomic_write,
	.writev		= fi_no_atomic_writev,
	.writemsg	= fi_opa1x_atomic_writemsg,
	.inject		= fi_no_atomic_inject,
	.readwrite      = fi_no_atomic_readwrite,
	.readwritev     = fi_no_atomic_readwritev,
	.readwritemsg	= fi_opa1x_atomic_readwritemsg,
	.compwrite	= fi_no_atomic_compwrite,
	.compwritev	= fi_no_atomic_compwritev,
	.compwritemsg	= fi_opa1x_atomic_compwritemsg,
	.writevalid	= fi_opa1x_atomic_writevalid,
	.readwritevalid	= fi_opa1x_atomic_readwritevalid,
	.compwritevalid = fi_opa1x_atomic_compwritevalid
};


int fi_opa1x_init_atomic_ops(struct fid_ep *ep, struct fi_info *info)
{
	struct fi_opa1x_ep * opa1x_ep;
	opa1x_ep = container_of(ep, struct fi_opa1x_ep, ep_fid);

	if (!info || !opa1x_ep)
		goto err;

	if (info->caps & FI_ATOMICS ||
			(info->tx_attr &&
			 (info->tx_attr->caps & FI_ATOMICS))) {
		opa1x_ep->ep_fid.atomic = &fi_opa1x_ops_atomic;
	}
	return 0;
err:
	errno = FI_EINVAL;
	return -errno;
}

int fi_opa1x_enable_atomic_ops(struct fid_ep *ep)
{
#if 0
	struct fi_opa1x_ep * opa1x_ep;
	opa1x_ep = container_of(ep, struct fi_opa1x_ep, ep_fid);

	if (!opa1x_ep || !opa1x_ep->domain)
		goto err;

	if (!opa1x_ep->ep_fid.atomic) {
		/* atomic ops not enabled on this endpoint */
		return 0;
	}
	/* fill in atomic formats */

	return 0;
err:
	errno = FI_EINVAL;
	return -errno;
#endif
	return 0;
}


int fi_opa1x_finalize_atomic_ops(struct fid_ep *ep)
{
	return 0;
}




#define FABRIC_DIRECT_LOCK	0

FI_OPA1X_ATOMIC_SPECIALIZED_FUNC(FABRIC_DIRECT_LOCK, FABRIC_DIRECT_AV)

ssize_t
fi_opa1x_atomic_FABRIC_DIRECT(struct fid_ep *ep, const void *buf, size_t count,
		void *desc, fi_addr_t dest_addr, uint64_t addr, uint64_t key,
		enum fi_datatype datatype, enum fi_op op, void *context)
{
	return FI_OPA1X_ATOMIC_SPECIALIZED_FUNC_NAME(atomic,
			FABRIC_DIRECT_LOCK,
			FABRIC_DIRECT_AV)
				(ep, buf, count, desc, dest_addr, addr, key,
					datatype, op, context);
}

ssize_t
fi_opa1x_inject_atomic_FABRIC_DIRECT(struct fid_ep *ep, const void *buf,
		size_t count, fi_addr_t dest_addr, uint64_t addr, uint64_t key,
		enum fi_datatype datatype, enum fi_op op)
{
	return FI_OPA1X_ATOMIC_SPECIALIZED_FUNC_NAME(inject_atomic,
			FABRIC_DIRECT_LOCK,
			FABRIC_DIRECT_AV)
				(ep, buf, count, dest_addr, addr, key,
					datatype, op);
}

ssize_t
fi_opa1x_fetch_atomic_FABRIC_DIRECT(struct fid_ep *ep, const void *buf,
		size_t count, void *desc, void *result, void *result_desc,
		fi_addr_t dest_addr, uint64_t addr, uint64_t key,
		enum fi_datatype datatype, enum fi_op op, void *context)
{
	return FI_OPA1X_ATOMIC_SPECIALIZED_FUNC_NAME(fetch_atomic,
			FABRIC_DIRECT_LOCK,
			FABRIC_DIRECT_AV)
				(ep, buf, count, desc, result, result_desc,
					dest_addr, addr, key, datatype, op,
					context);
}

ssize_t
fi_opa1x_compare_atomic_FABRIC_DIRECT(struct fid_ep *ep, const void *buf,
		size_t count, void *desc, const void *compare,
		void *compare_desc, void *result, void *result_desc,
		fi_addr_t dest_addr, uint64_t addr, uint64_t key,
		enum fi_datatype datatype, enum fi_op op, void *context)
{
	return FI_OPA1X_ATOMIC_SPECIALIZED_FUNC_NAME(compare_atomic,
			FABRIC_DIRECT_LOCK,
			FABRIC_DIRECT_AV)
				(ep, buf, count, desc, compare, compare_desc,
					result, result_desc, dest_addr, addr,
					key, datatype, op, context);
}
