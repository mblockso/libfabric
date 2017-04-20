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
#ifndef _FI_PROV_OPA1X_ADDR_H_
#define _FI_PROV_OPA1X_ADDR_H_

#include <stdio.h>
#include <stdlib.h>
#include <arpa/inet.h>		/* only for fi_opa1x_addr_dump ... */

#include "rdma/fabric.h"	/* only for 'fi_addr_t' ... which is a typedef to uint64_t */



typedef uint32_t fi_opa1x_uid_t;

union fi_opa1x_uid {

	fi_opa1x_uid_t			fi;
	struct {
		uint16_t		endpoint_id;	/* node-scoped endpoint identifier */
		uint16_t		lid;		/* fabric-scoped node identifier (big-endian) */
	} __attribute__((__packed__));
} __attribute__((__packed__));

static inline void
fi_opa1x_uid_dump (char * prefix, const union fi_opa1x_uid * const uid) {

	fprintf(stderr, "%s [%p]: %08x\n", prefix, uid, uid->fi);
	fprintf(stderr, "%s opa1x uid dump at %p (0x%08x)\n", prefix, (void*)uid, uid->fi);
	fprintf(stderr, "%s   .endpoint_id ..... %u (0x%04x)\n", prefix, uid->endpoint_id, uid->endpoint_id);
	fprintf(stderr, "%s   .lid ............. %u (0x%04x)\n", prefix, uid->lid, uid->lid);

	fflush(stderr);
}

#define FI_OPA1X_UID_DUMP(uid)							\
({										\
	char prefix[1024];							\
	snprintf(prefix, 1023, "%s:%s():%d", __FILE__, __func__, __LINE__);	\
	fi_opa1x_uid_dump(prefix, (uid));					\
})

union fi_opa1x_addr {
	fi_addr_t			fi;
	uint64_t			raw64b;
	uint32_t			raw32b[2];
	uint8_t				raw8b[8];
	struct {
		uint8_t			hfi1_rx;	/* isn't THIS the "node scoped endpoint id"? ... no, what about tx-only endpoints ... tx-only endpoints still have a hfi1 rx context associated with them */
		uint8_t			unused;
		uint8_t			reliability_rx;	/* hfi1 rx id of reliability service */
		union fi_opa1x_uid	uid;
		uint8_t			rx_index;	/* for scalable endpoints; will always be zero for "normal" endpoints */
	} __attribute__((__packed__));
} __attribute__((__packed__));

static inline void
fi_opa1x_addr_dump (char * prefix, const union fi_opa1x_addr * const addr) {

	fprintf(stderr, "%s [%p]: %08x %08x\n", prefix, addr, addr->raw32b[0], addr->raw32b[1]);
	fprintf(stderr, "%s opa1x addr dump at %p (0x%016lx)\n", prefix, (void*)addr, addr->raw64b);
	fprintf(stderr, "%s   .raw8b[8] = { %02x %02x %02x %02x  %02x %02x %02x %02x }\n", prefix, addr->raw8b[0], addr->raw8b[1], addr->raw8b[2], addr->raw8b[3], addr->raw8b[4], addr->raw8b[5], addr->raw8b[6], addr->raw8b[7]);

	fprintf(stderr, "%s   .hfi1_rx ....................................... %u\n", prefix, addr->hfi1_rx);
	fprintf(stderr, "%s   .unused ........................................ %u\n", prefix, addr->unused);
	fprintf(stderr, "%s   .reliability_rx ................................ %u\n", prefix, addr->reliability_rx);
	fprintf(stderr, "%s   .uid.endpoint_id ............................... %u\n", prefix, addr->uid.endpoint_id);
	fprintf(stderr, "%s   .uid.lid (big endian) .......................... %u (le: 0x%04hx, be: 0x%04hx)\n", prefix, addr->uid.lid, ntohs(addr->uid.lid), addr->uid.lid);
	fprintf(stderr, "%s   .rx_index ...................................... %u\n", prefix, addr->rx_index);

	fflush(stderr);
}

#define FI_OPA1X_ADDR_DUMP(addr)						\
({										\
	char prefix[1024];							\
	snprintf(prefix, 1023, "%s:%s():%d", __FILE__, __func__, __LINE__);	\
	fi_opa1x_addr_dump(prefix, (addr));					\
})

#define FI_OPA1X_ADDR_TO_HFI1_LRH_DLID(fi_addr)					\
	((fi_addr & 0x00FFFF0000000000ul) >> 24)


#define FI_OPA1X_HFI1_LRH_DLID_TO_LID(hfi1_lrh_dlid)				\
	(hfi1_lrh_dlid >> 16)


#define FI_OPA1X_ADDR_RX(fi_addr)						\
	(uint64_t)(((uint64_t)fi_addr << (56 - ((uint64_t)fi_addr >> 57))) & 0xFF00000000000000ul)

#define FI_OPA1X_FI_ADDR_TO_AV_INDEX(fi_addr, rx_bits)				\
	(fi_addr & (((uint64_t)-1) >> rx_bits))



#endif /* _FI_PROV_OPA1X_ADDR_H_ */
