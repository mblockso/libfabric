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
#ifndef _FI_PROV_OPA1X_HFI1_H_
#define _FI_PROV_OPA1X_HFI1_H_

#include "rdma/opa1x/fi_opa1x_hfi1_packet.h"

#include <stdlib.h>
#include <stdio.h>

#include <arpa/inet.h>

#include "rdma/fi_errno.h"	// only for FI_* errno return codes
#include "rdma/fabric.h" // only for 'fi_addr_t' ... which is a typedef to uint64_t

#include <uuid/uuid.h>

// #define FI_OPA1X_TRACE 1

#define KDETH_VERSION				(0x1)



#define FI_OPA1X_HFI1_RHF_EGRBFR_INDEX_SHIFT	(16)		/* a.k.a. "HFI_RHF_EGRBFR_INDEX_SHIFT" */
#define FI_OPA1X_HFI1_RHF_EGRBFR_INDEX_MASK	(0x7FF)		/* a.k.a. "HFI_RHF_EGRBFR_INDEX_MASK" */
#define FI_OPA1X_HFI1_PBC_VL_MASK		(0xf)		/* a.k.a. "HFI_PBC_VL_MASK" */
#define FI_OPA1X_HFI1_PBC_VL_SHIFT		(12)		/* a.k.a. "HFI_PBC_VL_SHIFT" */
#define FI_OPA1X_HFI1_PBC_SC4_SHIFT		(4)		/* a.k.a. "HFI_PBC_SC4_SHIFT" */
#define FI_OPA1X_HFI1_PBC_SC4_MASK		(0x1)		/* a.k.a. "HFI_PBC_SC4_MASK" */
#define FI_OPA1X_HFI1_PBC_DCINFO_SHIFT		(30)		/* a.k.a. "HFI_PBC_DCINFO_SHIFT" */
#define FI_OPA1X_HFI1_LRH_BTH			(0x0002)	/* a.k.a. "HFI_LRH_BTH" */
#define FI_OPA1X_HFI1_LRH_SL_MASK		(0xf)		/* a.k.a. "HFI_LRH_SL_MASK" */
#define FI_OPA1X_HFI1_LRH_SL_SHIFT		(4)		/* a.k.a. "HFI_LRH_SL_SHIFT" */
#define FI_OPA1X_HFI1_LRH_SC_MASK		(0xf)		/* a.k.a. "HFI_LRH_SC_MASK" */
#define FI_OPA1X_HFI1_LRH_SC_SHIFT		(12)		/* a.k.a. "HFI_LRH_SC_SHIFT" */
#define FI_OPA1X_HFI1_DEFAULT_P_KEY		(0x8001)	/* a.k.a. "HFI_DEFAULT_P_KEY" */
#define FI_OPA1X_HFI1_KHDR_KVER_SHIFT		(30)		/* a.k.a. "HFI_KHDR_KVER_SHIFT" */


static inline
uint32_t fi_opa1x_addr_calculate_base_rx (const uint32_t process_id, const uint32_t processes_per_node) {

abort();
	return 0;
}

struct fi_opa1x_hfi1_txe_scb {

	union {
		uint64_t		qw0;	/* a.k.a. 'struct hfi_pbc' */
		//struct hfi_pbc		pbc;
	};
	union fi_opa1x_hfi1_packet_hdr	hdr;

} __attribute__((__aligned__(8)));


struct fi_opa1x_hfi1_rxe_hdr {

	union fi_opa1x_hfi1_packet_hdr	hdr;
	uint64_t			rhf;

} __attribute__((__aligned__(64)));




#define HFI_TXE_CREDITS_COUNTER(credits)	((credits.raw16b[0] >> 0) & 0x07FFu)
#define HFI_TXE_CREDITS_STATUS(credits)		((credits.raw16b[0] >> 11) & 0x01u)
#define HFI_TXE_CREDITS_DUETOPBC(credits)	((credits.raw16b[0] >> 12) & 0x01u)
#define HFI_TXE_CREDITS_DUETOTHRESHOLD(credits)	((credits.raw16b[0] >> 13) & 0x01u)
#define HFI_TXE_CREDITS_DUETOERR(credits)	((credits.raw16b[0] >> 14) & 0x01u)
#define HFI_TXE_CREDITS_DUETOFORCE(credits)	((credits.raw16b[0] >> 15) & 0x01u)
union fi_opa1x_hfi1_txe_credits {

	uint16_t		raw16b[4];
	uint64_t		raw64b;

	struct {
		uint16_t	Counter				: 11;	/* use macros to access */
		uint16_t	Status				:  1;
		uint16_t	CreditReturnDueToPbc		:  1;
		uint16_t	CreditReturnDueToThreshold	:  1;
		uint16_t	CreditReturnDueToErr		:  1;
		uint16_t	CreditReturnDueToForce		:  1;

		uint16_t	pad[3];
	} __attribute__((packed));
};

#define FI_OPA1X_HFI1_DUMP_TXE_CREDITS(credits)	\
	fi_opa1x_hfi1_dump_txe_credits(credits, __FILE__, __func__, __LINE__);

static inline void fi_opa1x_hfi1_dump_txe_credits (union fi_opa1x_hfi1_txe_credits * credits,
		const char * file, const char * func, unsigned line)
{
	fprintf(stderr, "%s:%s():%d === dump hfi1 txe credits ===\n", file, func, line);
	fprintf(stderr, "%s:%s():%d .raw64b ...................... 0x%016lx\n", file, func, line, credits->raw64b);
	fprintf(stderr, "%s:%s():%d .Counter ..................... %hu\n", file, func, line, credits->Counter);
	fprintf(stderr, "%s:%s():%d .Status ...................... %hu\n", file, func, line, credits->Status);
	fprintf(stderr, "%s:%s():%d .CreditReturnDueToPbc ........ %hu\n", file, func, line, credits->CreditReturnDueToPbc);
	fprintf(stderr, "%s:%s():%d .CreditReturnDueToThreshold .. %hu\n", file, func, line, credits->CreditReturnDueToThreshold);
	fprintf(stderr, "%s:%s():%d .CreditReturnDueToErr ........ %hu\n", file, func, line, credits->CreditReturnDueToErr);
	fprintf(stderr, "%s:%s():%d .CreditReturnDueToForce ...... %hu\n", file, func, line, credits->CreditReturnDueToForce);
}





/* This 'state' information will update on each txe pio operation */
union fi_opa1x_hfi1_pio_state {

	uint64_t			qw0;

	struct {
		uint16_t		fill_counter;
		uint16_t		free_counter_shadow;
		uint16_t		scb_head_index;
		uint16_t		credits_total;	/* yeah, yeah .. THIS field is static, but there was an unused halfword at this spot, so .... */
	};
};

/* This 'static' information will not change after it is set by the driver
 * and can be safely copied into other structures to improve cache layout */
struct fi_opa1x_hfi1_pio_static {
	volatile uint64_t *		scb_sop_first;
	volatile uint64_t *		scb_first;

	/* pio credit return address. The HFI TXE periodically updates this
	 * host memory location with the current credit state. To avoid cache
	 * thrashing software should read from this location sparingly. */
	union {
		volatile uint64_t *				credits_addr;
		volatile union fi_opa1x_hfi1_txe_credits *	credits;
	};
};

/* This 'state' information will update on each txe sdma operation */
union fi_opa1x_hfi1_sdma_state {

	uint64_t			qw0;

//	struct {
//		uint16_t		pio_fill_counter;
//		uint16_t		pio_free_counter_shadow;
//		uint16_t		pio_scb_head_index;
//		uint16_t		unused;
//	};
};

/* This 'static' information will not change after it is set by the driver
 * and can be safely copied into other structures to improve cache layout */
struct fi_opa1x_hfi1_sdma_static {
	uint16_t			available_counter;
	uint16_t			fill_index;
	uint16_t			done_index;
	uint16_t			queue_size;
	struct hfi1_sdma_comp_entry *	completion_queue;
};


struct fi_opa1x_hfi1_rxe_state {

	struct {
		uint64_t		head;
		uint32_t		rhf_seq;
	} __attribute__((__packed__)) hdrq;

	struct {
		uint32_t		countdown;
	} __attribute__((__packed__)) egrq;

} __attribute__((__packed__));

struct fi_opa1x_hfi1_rxe_static {

	struct {
		uint32_t *		base_addr;
		uint32_t		rhf_off;
		int32_t			rhf_notail;


		uint32_t		elemsz;
		uint32_t		elemlast;
		uint32_t		elemcnt;

		uint32_t *		rhf_base;


		volatile uint64_t *	head_register;
		volatile uint64_t *	tail_register;

	} hdrq;


	struct {
		uint32_t *		base_addr;
		uint32_t		elemsz;


		volatile uint64_t *	head_register;
		volatile uint64_t *	tail_register;

	} egrq;

	volatile uint64_t *		uregbase;
	uint8_t				id;		/* hfi receive context id [0..159] */
};




struct fi_opa1x_hfi1_context {

	struct {
		union fi_opa1x_hfi1_pio_state		pio;
		union fi_opa1x_hfi1_sdma_state		sdma;
		struct fi_opa1x_hfi1_rxe_state		rxe;
	} state;

	struct {
		struct fi_opa1x_hfi1_pio_static		pio;
		struct fi_opa1x_hfi1_sdma_static	sdma;
		struct fi_opa1x_hfi1_rxe_static		rxe;

	} info;

	int				fd;
	uint16_t			lid;
	//struct _hfi_ctrl *		ctrl;
	//struct hfi1_user_info_dep	user_info;
	uint32_t			hfi_unit;
	uint32_t			hfi_port;
	uint64_t			gid_hi;
	uint64_t			gid_lo;
	uint16_t			mtu;
	uint8_t				bthqp;
	uint16_t			jkey;
	uint16_t			send_ctxt;

	uint16_t			sl2sc[32];
	uint16_t			sc2vl[32];
	uint64_t			sl;
	uint64_t			sc;
	uint64_t			vl;

	uint64_t			runtime_flags;

};



#ifdef NDEBUG
#define FI_OPA1X_HFI1_CHECK_CREDITS_FOR_ERROR(credits_addr)
#else
#define FI_OPA1X_HFI1_CHECK_CREDITS_FOR_ERROR(credits_addr)	\
	fi_opa1x_hfi1_check_credits_for_error(credits_addr, __FILE__, __func__, __LINE__);
#endif

static inline void fi_opa1x_hfi1_check_credits_for_error (volatile uint64_t * credits_addr, const char * file, const char * func, unsigned line)
{
	const uint64_t credit_return = *credits_addr;
	if ((credit_return & 0x0000000000004800ul) != 0) {
		fprintf(stderr, "%s:%s():%d ########### PIO SEND ERROR!\n", file, func, line);
		fi_opa1x_hfi1_dump_txe_credits((union fi_opa1x_hfi1_txe_credits *)credits_addr, file, func, line);
		abort();
	}

	return;
}


#define FI_OPA1X_HFI1_CREDITS_IN_USE(pio_state)										\
	((pio_state.fill_counter - pio_state.free_counter_shadow) & 0x07FFu)

#define FI_OPA1X_HFI1_UPDATE_CREDITS(pio_state, pio_credits_addr)							\
	{														\
		volatile uint64_t * credits_addr = (uint64_t *)(pio_credits_addr);					\
		const uint64_t credit_return = *credits_addr;								\
		pio_state.free_counter_shadow = (uint16_t)(credit_return & 0x00000000000007FFul);			\
	}

#define FI_OPA1X_HFI1_WAIT_FOR_CREDIT(credits_in_use, pio_state, pio_credits_addr)					\
	{														\
		if (unlikely(credits_in_use == pio_state.credits_total)) {						\
			volatile uint64_t * credits_addr = (uint64_t *)(pio_credits_addr);				\
			do {												\
				const uint64_t credit_return = *credits_addr;						\
				assert((credit_return & 0x0000000000004800ul) == 0);					\
				pio_state.free_counter_shadow = (uint16_t)(credit_return & 0x00000000000007FFul);	\
				credits_in_use = (pio_state.fill_counter - pio_state.free_counter_shadow) & 0x07FFu;	\
			} while (credits_in_use == pio_state.credits_total);						\
		}													\
	}

#define FI_OPA1X_HFI1_PIO_SCB_HEAD(pio_scb_base, pio_state)								\
	((pio_scb_base) + (pio_state.scb_head_index << 3))

#define FI_OPA1X_HFI1_SINGLE_CREDIT_AVAILABLE(pio_state)								\
	(((pio_state.fill_counter - pio_state.free_counter_shadow) & 0x07FFu) < pio_state.credits_total)

#define FI_OPA1X_HFI1_CONTIGUOUS_CREDITS_AVAILABLE(credits_in_use, pio_state)						\
	MIN((pio_state.credits_total - credits_in_use), (uint16_t)(pio_state.credits_total - pio_state.scb_head_index))

#define FI_OPA1X_HFI1_AVAILABLE_CREDITS(pio_state)									\
	(pio_state.credits_total - ((pio_state.fill_counter - pio_state.free_counter_shadow) & 0x07FFu))

#define FI_OPA1X_HFI1_CONSUME_SINGLE_CREDIT(pio_state)									\
	{														\
		pio_state.scb_head_index = (pio_state.scb_head_index + 1) *						\
			(pio_state.credits_total != (pio_state.scb_head_index + 1));					\
		pio_state.fill_counter = (pio_state.fill_counter + 1) & 0x00000000000007FFul;				\
	}

#define FI_OPA1X_HFI1_CONSUME_CREDITS(pio_state, count)									\
	{														\
		pio_state.scb_head_index = (pio_state.scb_head_index + count) *						\
			(pio_state.credits_total != (pio_state.scb_head_index + count));				\
		pio_state.fill_counter = (pio_state.fill_counter + count) & 0x00000000000007FFul;			\
	}


struct fi_opa1x_hfi1_context * fi_opa1x_hfi1_context_open (uuid_t unique_job_key);

//int open_hfi1_context (uuid_t unique_job_key,
//		struct fi_opa1x_hfi1_context * context);

int init_hfi1_rxe_state (struct fi_opa1x_hfi1_context * context,
		struct fi_opa1x_hfi1_rxe_state * rxe_state);


//static inline void fi_opa1x_hfi1_checks ()
//{
//	assert(sizeof(union fi_opa1x_hfi1_packet_hdr) == sizeof(MUHWI_PacketHeader_t));
	//assert(sizeof(union fi_opa1x_addr) == sizeof(fi_addr_t));
//	assert(sizeof(union fi_opa1x_hfi1_descriptor) == sizeof(MUHWI_Descriptor_t));
//}

#endif /* _FI_PROV_OPA1X_HFI1_H_ */
