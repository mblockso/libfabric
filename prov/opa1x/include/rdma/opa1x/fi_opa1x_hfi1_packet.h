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
#ifndef _FI_PROV_OPA1X_HFI1_PACKET_H_
#define _FI_PROV_OPA1X_HFI1_PACKET_H_

#include <stdio.h>
#include <stdlib.h>
#include <arpa/inet.h>		/* only for fi_opa1x_addr_dump ... */

#include "rdma/fabric.h"	/* only for 'fi_addr_t' ... which is a typedef to uint64_t */
#include "rdma/opa1x/fi_opa1x_addr.h"

#define FI_OPA1X_ADDR_SEP_RX_MAX	(4)









#define FI_OPA1X_HFI1_PACKET_MTU			(8192)
#define FI_OPA1X_HFI1_PACKET_IMM			(16)

#define FI_OPA1X_HFI_BTH_OPCODE_INVALID			(0xC0)
#define FI_OPA1X_HFI_BTH_OPCODE_RZV_CTS			(0xC1)
#define FI_OPA1X_HFI_BTH_OPCODE_RZV_DATA		(0xC2)
#define FI_OPA1X_HFI_BTH_OPCODE_RMA			(0xC3)
#define FI_OPA1X_HFI_BTH_OPCODE_ATOMIC			(0xC4)
#define FI_OPA1X_HFI_BTH_OPCODE_ACK			(0xC5)
/* opcodes (0xC5..0xFA) are unused */
#define FI_OPA1X_HFI_BTH_OPCODE_MSG_INJECT		(0xFA)
#define FI_OPA1X_HFI_BTH_OPCODE_MSG_EAGER		(0xFB)
#define FI_OPA1X_HFI_BTH_OPCODE_MSG_RZV_RTS		(0xFC)
#define FI_OPA1X_HFI_BTH_OPCODE_TAG_INJECT		(0xFD)
#define FI_OPA1X_HFI_BTH_OPCODE_TAG_EAGER		(0xFE)
#define FI_OPA1X_HFI_BTH_OPCODE_TAG_RZV_RTS		(0xFF)


#define FI_OPA1X_HFI1_PACKET_SLID(packet_hdr)				\
	(((packet_hdr).qw[0] & 0xFFFF000000000000ul) >> 48)


struct fi_opa1x_hfi1_stl_packet_hdr {

	/* == quadword 0 == */
	union {
		uint64_t		qw[1];
		uint32_t		dw[2];
		uint16_t		w[4];
		struct {
			uint16_t	flags;	/* lrh.w[0] - big-endian! */
			uint16_t	dlid;	/* lrh.w[1] - big-endian! */
			uint16_t	pktlen;	/* lrh.w[2] - big-endian! */
			uint16_t	slid;	/* lrh.w[3] - big-endian! */
		} __attribute__((packed));
	} lrh;

	/* == quadword 1 == */
	union {
		uint32_t		dw[3];
		uint16_t		w[6];
		uint8_t			hw[12];
		struct {
			uint8_t		opcode;	/* bth.hw[0] */
			uint8_t		bth_1;	/* bth.hw[1] */
			uint16_t	pkey;	/* bth.w[1]  - big-endian! */
			uint8_t		ecn;	/* bth.hw[4] (FECN, BECN, and reserved) */
			uint8_t		qp;	/* bth.hw[5] */
			uint8_t		unused; /* bth.hw[6] -----> inject::message_length, send::xfer_bytes_tail */
			uint8_t		rx;	/* bth.hw[7] */

	/* == quadword 2 == */
			uint32_t	psn;	/* bth.dw[2] ..... the 'psn' field is unused for 'eager' packets -----> reliability::psn, etc */
		} __attribute__((packed));
	} bth;

	union {
		uint32_t		dw[3];
		uint16_t		w[6];
		uint8_t			hw[12];
		struct {
			uint32_t	offset_ver_tid;	/* kdeth.dw[0]  .... the 'offset' field is unused for 'eager' packets */

	/* == quadword 3 == */
			uint16_t	jkey;		/* kdeth.w[2]  */
			uint16_t	hcrc;		/* kdeth.w[3]  */
			uint32_t	unused;		/* kdeth.dw[2] -----> immediate data (32b) */
		} __attribute__((packed));
	} kdeth;

	/* == quadword 4,5,6 == */
	uint64_t			unused[3];

} __attribute__((__packed__));



static inline
void fi_opa1x_hfi1_dump_stl_packet_hdr (struct fi_opa1x_hfi1_stl_packet_hdr * hdr,
		const char * fn, const unsigned ln) {

	const uint64_t * const qw = (uint64_t *)hdr;

	fprintf(stderr, "%s():%u ==== dump stl packet header @ %p [%016lx %016lx %016lx %016lx]\n", fn, ln, hdr, qw[0], qw[1], qw[2], qw[3]);
	fprintf(stderr, "%s():%u .lrh.flags .............     0x%04hx\n", fn, ln, hdr->lrh.flags);
	fprintf(stderr, "%s():%u .lrh.dlid ..............     0x%04hx (be: %5hu, le: %5hu)\n", fn, ln, hdr->lrh.dlid, hdr->lrh.dlid, ntohs(hdr->lrh.dlid));
	fprintf(stderr, "%s():%u .lrh.pktlen ............     0x%04hx (be: %5hu, le: %5hu)\n", fn, ln, hdr->lrh.pktlen, hdr->lrh.pktlen, ntohs(hdr->lrh.pktlen));
	fprintf(stderr, "%s():%u .lrh.slid ..............     0x%04hx (be: %5hu, le: %5hu)\n", fn, ln, hdr->lrh.slid, hdr->lrh.slid, ntohs(hdr->lrh.slid));
	fprintf(stderr, "%s():%u\n", fn, ln);
	fprintf(stderr, "%s():%u .bth.opcode ............       0x%02x \n", fn, ln, hdr->bth.opcode);
	fprintf(stderr, "%s():%u .bth.bth_1 .............       0x%02x \n", fn, ln, hdr->bth.bth_1);
	fprintf(stderr, "%s():%u .bth.pkey ..............     0x%04hx \n", fn, ln, hdr->bth.pkey);
	fprintf(stderr, "%s():%u .bth.ecn ...............       0x%02x \n", fn, ln, hdr->bth.ecn);
	fprintf(stderr, "%s():%u .bth.qp ................       0x%02x \n", fn, ln, hdr->bth.qp);
	fprintf(stderr, "%s():%u .bth.unused ............       0x%02x \n", fn, ln, hdr->bth.unused);
	fprintf(stderr, "%s():%u .bth.rx ................       0x%02x \n", fn, ln, hdr->bth.rx);
	fprintf(stderr, "%s():%u\n", fn, ln);
	fprintf(stderr, "%s():%u .bth.psn ............... 0x%08x \n", fn, ln, hdr->bth.psn);
	fprintf(stderr, "%s():%u .kdeth.offset_ver_tid .. 0x%08x\n", fn, ln, hdr->kdeth.offset_ver_tid);
	fprintf(stderr, "%s():%u\n", fn, ln);
	fprintf(stderr, "%s():%u .kdeth.jkey ............     0x%04hx\n", fn, ln, hdr->kdeth.jkey);
	fprintf(stderr, "%s():%u .kdeth.hcrc ............     0x%04hx\n", fn, ln, hdr->kdeth.hcrc);
	fprintf(stderr, "%s():%u .kdeth.unused .......... 0x%08x\n", fn, ln, hdr->kdeth.unused);

	return;
}




/**
 * \brief HFI1 packet header
 *
 * The HFI1 packet header is consumed in many places and sometimes overloaded
 * for cache and memory allocation reasons.
 */
union fi_opa1x_hfi1_packet_hdr {


	uint64_t				qw[7];

	struct fi_opa1x_hfi1_stl_packet_hdr	stl;

	struct {
		/* == quadword 0 == */
		uint16_t			reserved_0[3];
		uint16_t			slid;

		/* == quadword 1 == */
		uint64_t			reserved_1;

		/* == quadword 2 == */
		uint32_t			psn		: 24;
		uint32_t			origin_tx	:  8;
		uint8_t				origin_reliability_rx;
		uint8_t				reserved_2[3];

		/* == quadword 3,4,5,6 == */
		uint64_t			reserved_n[4];

	} __attribute__((__packed__)) reliability;


	struct {
		/* == quadword 0 == */
		uint16_t	reserved_0[3];
		uint16_t	slid;			/* used for FI_DIRECTED_RECV; identifies the node - big-endian! */

		/* == quadword 1 == */
		uint64_t	reserved_1;

		/* == quadword 2 == */
		uint8_t		reserved_2[5];
		uint8_t		origin_cx;		/* used for FI_DIRECTED_RECV; identifies the endpoint on the node */
		uint16_t	reserved_3;

		/* == quadword 3 == */
		uint32_t	reserved_4;
		uint32_t	ofi_data;		/* used for FI_RX_CQ_DATA */

		/* == quadword 4 == */
		uint64_t	reserved_5;

		/* == quadword 5 == */
		uint64_t	reserved_6;

		/* == quadword 6 == */
		uint64_t	ofi_tag;

	} __attribute__((__packed__)) match;


	struct {
		/* == quadword 0 == */
		uint64_t	reserved_0;

		/* == quadword 1 == */
		uint16_t	reserved_1[3];
		uint8_t		message_length;		/* only need 5 bits; maximum inject message size is 16 bytes */
		uint8_t		reserved_2;

		/* == quadword 2 == */
		uint64_t	reserved_3;

		/* == quadword 3 == */
		uint64_t	reserved_4;

		/* == quadword 4,5 == */
		union {
			uint8_t		app_data_u8[16];
			uint16_t	app_data_u16[8];
			uint32_t	app_data_u32[4];
			uint64_t	app_data_u64[2];
		};

		/* == quadword 6 == */
		uint64_t	reserved_6;

	} __attribute__((__packed__)) inject;


	struct {
		/* == quadword 0 == */
		uint64_t	reserved_0;

		/* == quadword 1 == */
		uint16_t	reserved_1[3];
		uint8_t		xfer_bytes_tail;	/* only need 4 bits; maximum tail size is 8 bytes (or is it 7?) */
		uint8_t		reserved_2;

		/* == quadword 2 == */
		uint64_t	reserved_3;

		/* == quadword 3 == */
		uint64_t	reserved_4;

		/* == quadword 4 == */
		uint16_t	unused[3];
		uint16_t	payload_qws_total;	/* TODO - use stl.lrh.pktlen instead (num dws); only need 11 bits; maximum number of payload qw is 10240 / 8 = 1280 */

		/* == quadword 5 == */
		uint64_t	xfer_tail;

		/* == quadword 6 == */
		uint64_t	reserved_6;

	} __attribute__((__packed__)) send;


	struct {
		/* == quadword 0 == */
		uint64_t	reserved_0;

		/* == quadword 1 == */
		uint16_t	reserved_1[3];
		uint8_t		origin_rx;
		uint8_t		reserved_2;

		/* == quadword 2 == */
		uint64_t	reserved_3;

		/* == quadword 3 == */
		uint64_t	reserved_4;

		/* == quadword 4 == */
		uint16_t	origin_rs;
		uint16_t	unused[2];
		uint16_t	niov;			/* number of non-contiguous buffers */

		/* == quadword 5 == */
		uint64_t	message_length;		/* total length in bytes of all non-contiguous buffers and immediate data */

		/* == quadword 6 == */
		uint64_t	reserved_6;

	} __attribute__((__packed__)) rendezvous;


	struct {
		/* == quadword 0 == */
		uint64_t	reserved_0;

		/* == quadword 1 == */
		uint16_t	reserved_1[3];
		uint8_t		origin_rx;
		uint8_t		reserved_2;

		/* == quadword 2 == */
		uint64_t	reserved_3;

		/* == quadword 3 == */
		uint64_t	reserved_4;

		/* == quadword 4 == */
		uint16_t	origin_rs;
		uint16_t	unused;
		uint32_t	niov;			/* number of non-contiguous buffers described in the packet payload */

		/* == quadword 5,6 == */
		uintptr_t	origin_byte_counter_vaddr;
		uintptr_t	target_byte_counter_vaddr;

	} __attribute__((__packed__)) cts;

	struct {
		/* == quadword 0 == */
		uint64_t	reserved_0;

		/* == quadword 1 == */
		uint16_t	reserved_1[3];
		uint8_t		unused_0;
		uint8_t		reserved_2;

		/* == quadword 2 == */
		uint64_t	reserved_3;

		/* == quadword 3 == */
		uint64_t	reserved_4;

		/* == quadword 4 == */
		uint32_t	unused_1;
		uint32_t	bytes;

		/* == quadword 5,6 == */
		uintptr_t	rbuf;
		uintptr_t	target_byte_counter_vaddr;

	} __attribute__((__packed__)) dput;

#if 0
	struct {
		uint64_t		reserved_0;
		uint32_t		reserved_1;
		uint16_t		reserved_2	: 10;
		uint16_t		unused_0	:  6;

		uint8_t			unused_1;
		uint8_t			reserved_3;
		uint64_t		unused_2;
		uintptr_t		context;
	} __attribute__((__packed__)) ack;

	struct {
		uint64_t		reserved_0;
		uint32_t		reserved_1;
		uint16_t		reserved_2	: 10;
		uint16_t		unused_0	:  6;

		uint8_t			ndesc;			/* 0..8 descriptors */
		uint8_t			reserved_3;
		uint64_t		nbytes		: 16;	/* 0..FI_OPA1X_HFI1_PACKET_MTU bytes */
		uint64_t		unused_2	: 11;
		uint64_t		offset		: 37;	/* FI_MR_BASIC uses virtual address as the offset */
		uint64_t		key;			/* only 16 bits needed for FI_MR_SCALABLE but need up to 34 for FI_MR_BASIC vaddr-paddr delta */
	} __attribute__((__packed__)) rma;

	struct {
		uint64_t		reserved_0;
		uint32_t		reserved_1;
		uint32_t		reserved_2	: 10;
		uint32_t		unused_0	:  5;
		uint32_t		cntr_bat_id	:  9;
		uint32_t		reserved_3	:  8;
		union {
			uint32_t		origin_raw;
//			MUHWI_Destination_t	origin;
			struct {
				uint32_t	is_fetch	:  1;
				uint32_t	dt		:  4;	/* enum fi_datatype */
				uint32_t	a		:  3;	/* only 3 bits are needed for Mira */
				uint32_t	is_local	:  1;
				uint32_t	do_cntr		:  1;
				uint32_t	b		:  4;	/* only 4 bits are needed for Mira */
				uint32_t	unused_1	:  2;
				uint32_t	c		:  4;	/* only 4 bits are needed for Mira */
				uint32_t	unused_2	:  2;
				uint32_t	d		:  4;	/* only 4 bits are needed for Mira */
				uint32_t	op		:  5;	/* enum fi_op */
				uint32_t	e		:  1;	/* only 1 bit is needed for Mira */
			} __attribute__((__packed__));
		};
		uint16_t		nbytes_minus_1;			/* only 9 bits needed */
		uint16_t		key;				/* only 16 bits needed for FI_MR_SCALABLE and not used for FI_MR_BASIC */
		uint64_t		offset;				/* FI_MR_BASIC needs 34 bits */
	} __attribute__((__packed__)) atomic;
#endif

} __attribute__((__aligned__(8)));


static inline
fi_opa1x_uid_t fi_opa1x_hfi1_packet_hdr_uid (const union fi_opa1x_hfi1_packet_hdr * const hdr) {

	const union fi_opa1x_uid uid =
	{
		.endpoint_id = hdr->match.origin_cx,	/* node-scoped endpoint id */
		.lid = hdr->match.slid			/* job-scoped node id */
	};

	return uid.fi;
}


static inline size_t
fi_opa1x_hfi1_packet_hdr_message_length (const union fi_opa1x_hfi1_packet_hdr * const hdr)
{
	size_t message_length = 0;
	switch (hdr->stl.bth.opcode) {
		case FI_OPA1X_HFI_BTH_OPCODE_MSG_INJECT:
		case FI_OPA1X_HFI_BTH_OPCODE_TAG_INJECT:
			message_length = hdr->inject.message_length;
			break;
		case FI_OPA1X_HFI_BTH_OPCODE_MSG_EAGER:
		case FI_OPA1X_HFI_BTH_OPCODE_TAG_EAGER:
			message_length = hdr->send.xfer_bytes_tail + hdr->send.payload_qws_total * sizeof(uint64_t);
			break;
		case FI_OPA1X_HFI_BTH_OPCODE_MSG_RZV_RTS:
		case FI_OPA1X_HFI_BTH_OPCODE_TAG_RZV_RTS:
			//assert(hdr->rendezvous.niov == 1);
			message_length = hdr->rendezvous.message_length;
			break;
		default:
			fprintf(stderr, "%s:%s():%d abort. hdr->stl.bth.opcode = %02x (%u)\n", __FILE__, __func__, __LINE__, hdr->stl.bth.opcode, hdr->stl.bth.opcode); abort();
			break;
	}

	return message_length;
}

static inline
void fi_opa1x_hfi1_dump_packet_hdr (const union fi_opa1x_hfi1_packet_hdr * const hdr,
		const char * fn, const unsigned ln) {

	const uint64_t * const qw = (uint64_t *)hdr;

	fprintf(stderr, "%s():%u ==== dump packet header @ %p [%016lx %016lx %016lx %016lx]\n", fn, ln, hdr, qw[0], qw[1], qw[2], qw[3]);
	fprintf(stderr, "%s():%u .stl.lrh.flags ...........     0x%04hx\n", fn, ln, hdr->stl.lrh.flags);
	fprintf(stderr, "%s():%u .stl.lrh.dlid ............     0x%04hx (be: %5hu, le: %5hu)\n", fn, ln, hdr->stl.lrh.dlid, hdr->stl.lrh.dlid, ntohs(hdr->stl.lrh.dlid));
	fprintf(stderr, "%s():%u .stl.lrh.pktlen ..........     0x%04hx (be: %5hu, le: %5hu)\n", fn, ln, hdr->stl.lrh.pktlen, hdr->stl.lrh.pktlen, ntohs(hdr->stl.lrh.pktlen));
	fprintf(stderr, "%s():%u .stl.lrh.slid ............     0x%04hx (be: %5hu, le: %5hu)\n", fn, ln, hdr->stl.lrh.slid, hdr->stl.lrh.slid, ntohs(hdr->stl.lrh.slid));
	fprintf(stderr, "%s():%u\n", fn, ln);
	fprintf(stderr, "%s():%u .stl.bth.opcode ..........     0x%02x \n", fn, ln, hdr->stl.bth.opcode);

	fprintf(stderr, "%s():%u .match.slid ..............     0x%04x \n", fn, ln, hdr->match.slid);
	fprintf(stderr, "%s():%u .match.origin_cx .........     0x%02x \n", fn, ln, hdr->match.origin_cx);
	fprintf(stderr, "%s():%u .match.ofi_data ..........     0x%08x \n", fn, ln, hdr->match.ofi_data);
	fprintf(stderr, "%s():%u .match.ofi_tag ...........     0x%016lx \n", fn, ln, hdr->match.ofi_tag);

	switch (hdr->stl.bth.opcode) {
		case FI_OPA1X_HFI_BTH_OPCODE_MSG_INJECT:
		case FI_OPA1X_HFI_BTH_OPCODE_TAG_INJECT:
			fprintf(stderr, "%s():%u .inject.message_length ...     0x%02x \n", fn, ln, hdr->inject.message_length);
			fprintf(stderr, "%s():%u .inject.app_data_u64[0] ..     0x%016lx \n", fn, ln, hdr->inject.app_data_u64[0]);
			fprintf(stderr, "%s():%u .inject.app_data_u64[1] ..     0x%016lx \n", fn, ln, hdr->inject.app_data_u64[1]);
			break;
		case FI_OPA1X_HFI_BTH_OPCODE_MSG_EAGER:
		case FI_OPA1X_HFI_BTH_OPCODE_TAG_EAGER:
			fprintf(stderr, "%s():%u .send.xfer_bytes_tail ....     0x%02x \n", fn, ln, hdr->send.xfer_bytes_tail);
			fprintf(stderr, "%s():%u .send.payload_qws_total ..     0x%04x \n", fn, ln, hdr->send.payload_qws_total);
			fprintf(stderr, "%s():%u .send.xfer_tail ..........     0x%016lx \n", fn, ln, hdr->send.xfer_tail);
			break;
		case FI_OPA1X_HFI_BTH_OPCODE_MSG_RZV_RTS:
		case FI_OPA1X_HFI_BTH_OPCODE_TAG_RZV_RTS:	/* calculate (?) total bytes to be transfered */
			break;
		default:
			fprintf(stderr, "%s():%u Unknown type \n", fn, ln);
			break;
	}

	return;
}

struct fi_opa1x_hfi1_fetch_metadata {
	uint64_t			dst_paddr;
	uint64_t			cq_paddr;
	uint64_t			fifo_map;
	uint64_t			unused;
};

union cacheline {
	uint64_t			qw[8];
	uint32_t			dw[16];
	uint8_t				byte[64];
};

struct fi_opa1x_hfi1_dput_iov {
	uintptr_t			rbuf;
	uintptr_t			sbuf;
	uint64_t			bytes;
};


union fi_opa1x_hfi1_packet_payload {
	uint8_t				byte[FI_OPA1X_HFI1_PACKET_MTU];
	union {
		struct {
			/* ==== CACHE LINE 0 ==== */

			uintptr_t	src_vaddr;
			uint64_t	src_blocks;		/* number of 64-byte data blocks to transfer */
			uint64_t	immediate_byte_count;	/* only need 3 bits (0..7 bytes) */
			uint64_t	immediate_qw_count;	/* only need 3 bits (0..7 quadwords) */
			uint64_t	immediate_block_count;	/* only need 8 bits (0..158 64B blocks) */
			uintptr_t	origin_byte_counter_vaddr;
			uint64_t	unused[2];

			/* ==== CACHE LINE 1 ==== */

			uint8_t		immediate_byte[8];
			uint64_t	immediate_qw[7];

			/* ==== CACHE LINE 2-127 ==== */

			union cacheline	immediate_block[FI_OPA1X_HFI1_PACKET_MTU/64 - 2];

		} contiguous;
	} rendezvous;

	struct {
		struct fi_opa1x_hfi1_dput_iov	iov[0];
	} cts;

	struct {
		struct fi_opa1x_hfi1_fetch_metadata	metadata;
		uint8_t				data[FI_OPA1X_HFI1_PACKET_MTU-sizeof(struct fi_opa1x_hfi1_fetch_metadata)];
	} atomic_fetch;
} __attribute__((__aligned__(32)));





struct fi_opa1x_hfi1_ue_packet {
	struct fi_opa1x_hfi1_ue_packet *	next;
	union fi_opa1x_hfi1_packet_hdr		hdr;
	union fi_opa1x_hfi1_packet_payload	payload;
} __attribute__((__packed__));

struct fi_opa1x_hfi1_ue_packet_slist {
	struct fi_opa1x_hfi1_ue_packet *	head;
	struct fi_opa1x_hfi1_ue_packet *	tail;
};

static inline void fi_opa1x_hfi1_ue_packet_slist_init (struct fi_opa1x_hfi1_ue_packet_slist* list)
{
	list->head = list->tail = NULL;
}

static inline int fi_opa1x_hfi1_ue_packet_slist_empty (struct fi_opa1x_hfi1_ue_packet_slist* list)
{
	return !list->head;
}

static inline void fi_opa1x_hfi1_ue_packet_slist_insert_head (struct fi_opa1x_hfi1_ue_packet *item,
		struct fi_opa1x_hfi1_ue_packet_slist* list)
{
	if (fi_opa1x_hfi1_ue_packet_slist_empty(list))
		list->tail = item;
	else
		item->next = list->head;

	list->head = item;
}

static inline void fi_opa1x_hfi1_ue_packet_slist_insert_tail (struct fi_opa1x_hfi1_ue_packet *item,
		struct fi_opa1x_hfi1_ue_packet_slist* list)
{
	if (fi_opa1x_hfi1_ue_packet_slist_empty(list))
		list->head = item;
	else
		list->tail->next = item;

	list->tail = item;
}














#endif /* _FI_PROV_OPA1X_HFI1_PACKET_H_ */
