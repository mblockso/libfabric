#ifndef _FI_PROV_OPA1X_HFI1_TRANSPORT_H_
#define _FI_PROV_OPA1X_HFI1_TRANSPORT_H_

#ifndef FI_OPA1X_FABRIC_HFI1
#error "fabric selection #define error"
#endif

#include "rdma/opa1x/fi_opa1x_hfi1.h"


void fi_opa1x_hfi1_rx_rzv_rts (struct fi_opa1x_ep_rx *rx,
		const void * const hdr, const void * const payload,
		const uint8_t u8_rx, const uint64_t niov,
		uintptr_t origin_byte_counter_vaddr,
		uintptr_t target_byte_counter_vaddr,
		const uintptr_t dst_vaddr,
		const uintptr_t src_vaddr,
		const uint64_t nbytes_to_transfer,
		const unsigned is_intranode);

void fi_opa1x_hfi1_rx_rzv_cts (struct fi_opa1x_ep_rx * const rx,
		const void * const hdr, const void * const payload,
		const uint8_t u8_rx, const uint32_t niov,
		const struct fi_opa1x_hfi1_dput_iov * const dput_iov,
		const uintptr_t target_byte_counter_vaddr,
		uint64_t * origin_byte_counter,
		const unsigned is_intranode);


static inline
ssize_t fi_opa1x_hfi1_tx_inject (struct fi_opa1x_ep_tx *tx,
		const void *buf, size_t len, fi_addr_t dest_addr, uint64_t tag,
		const uint32_t data, int lock_required, const unsigned is_msg,
		const uint64_t dest_rx) {

	const union fi_opa1x_addr addr = { .fi = dest_addr };

#ifdef LOCAL_COMM_ENABLED
	if (unlikely(tx->inject.hdr.stl.lrh.slid == addr.uid.lid)) {

		return fi_opa1x_shm_tx_inject(tx, buf, len, dest_addr, tag, data,
			lock_required, is_msg, dest_rx);
	}
#endif

	FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_EP_DATA,
		"===================================== INJECT, HFI (begin)\n");

	/* first check for sufficient credits to inject the entire packet */

	union fi_opa1x_hfi1_pio_state pio_state;
	uint64_t * const pio_state_ptr = (uint64_t*)tx->pio_state;
	pio_state.qw0 = *pio_state_ptr;		/* likely cache miss */

	if (unlikely(FI_OPA1X_HFI1_AVAILABLE_CREDITS(pio_state) < 1)) {
		FI_OPA1X_HFI1_UPDATE_CREDITS(pio_state, tx->pio_credits_addr);
		if (FI_OPA1X_HFI1_AVAILABLE_CREDITS(pio_state) < 1) {
			return -FI_EAGAIN;
		}
	}

	const uint64_t bth_rx = dest_rx << 56;
	const uint64_t lrh_dlid = FI_OPA1X_ADDR_TO_HFI1_LRH_DLID(addr.fi);

#ifndef SKIP_RELIABILITY_PROTOCOL_TX
	const uint64_t psn = fi_opa1x_reliability_tx_next_psn(tx->reliability_state, addr.uid.lid, dest_rx);
#else
	const uint64_t psn = 0;
#endif
	if (lock_required) { fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__); abort(); }

	volatile uint64_t * const scb =
		FI_OPA1X_HFI1_PIO_SCB_HEAD(tx->pio_scb_sop_first, pio_state);

	uint64_t tmp[8];

	tmp[0] = scb[0] = tx->inject.qw0;
	tmp[1] = scb[1] = tx->inject.hdr.qw[0] | lrh_dlid;

	tmp[2] = scb[2] = tx->inject.hdr.qw[1] | bth_rx | (len << 48) |
			(is_msg ? /* compile-time constant expression */
				(uint64_t)FI_OPA1X_HFI_BTH_OPCODE_MSG_INJECT :
				(uint64_t)FI_OPA1X_HFI_BTH_OPCODE_TAG_INJECT);

	tmp[3] = scb[3] = tx->inject.hdr.qw[2] | psn;
	tmp[4] = scb[4] = tx->inject.hdr.qw[3] | (((uint64_t)data) << 32);

	switch (len) {
		case 0:
			tmp[5] = scb[5] = 0;
			tmp[6] = scb[6] = 0;
			break;
		case 1:
			tmp[5] = 0;
			*((uint8_t*)&tmp[5]) = *((uint8_t*)buf);
			scb[5] = tmp[5];
			tmp[6] = scb[6] = 0;
			break;
		case 2:
		case 3:
		case 4:
		case 5:
		case 6:
		case 7:
			tmp[5] = 0;
			memcpy((void*)&tmp[5], buf, len);
			scb[5] = tmp[5];
			tmp[6] = scb[6] = 0;
			break;
		case 8:
			tmp[5] = scb[5] = *((uint64_t*)buf);
			tmp[6] = scb[6] = 0;
			break;
		case 9:
		case 10:
		case 11:
		case 12:
		case 13:
		case 14:
		case 15:
			tmp[6] = 0;
			memcpy((void*)&tmp[5], buf, len);
			scb[5] = tmp[5];
			scb[6] = tmp[6];
			break;
		case 16:
			tmp[5] = scb[5] = *((uint64_t*)buf);
			tmp[6] = scb[6] = *((uint64_t*)buf+1);
			break;
		default:
			fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__); abort();
			break;
	}

	tmp[7] = scb[7] = tag;

	fi_opa1x_compiler_msync_writes();

	FI_OPA1X_HFI1_CHECK_CREDITS_FOR_ERROR(tx->pio_credits_addr);

	/* consume one credit */
	FI_OPA1X_HFI1_CONSUME_SINGLE_CREDIT(pio_state);

	/* save the updated txe state */
	*pio_state_ptr = pio_state.qw0;

#ifndef SKIP_RELIABILITY_PROTOCOL_TX
	/*
	 * compose replay buffer and register
	 */
	struct fi_opa1x_reliability_tx_replay * replay = 
		fi_opa1x_reliability_tx_replay_allocate(tx->reliability_state);

	replay->scb.qw0 = tmp[0];
	replay->scb.hdr.qw[0] = tmp[1];
	replay->scb.hdr.qw[1] = tmp[2];
	replay->scb.hdr.qw[2] = tmp[3];
	replay->scb.hdr.qw[3] = tmp[4];
	replay->scb.hdr.qw[4] = tmp[5];
	replay->scb.hdr.qw[5] = tmp[6];
	replay->scb.hdr.qw[6] = tmp[7];

	fi_opa1x_reliability_tx_replay_register_no_update(tx->reliability_state, addr.uid.lid, addr.reliability_rx, dest_rx, psn, replay);
#endif
	FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_EP_DATA,
		"===================================== INJECT, HFI (end)\n");

	return FI_SUCCESS;
}

static inline
ssize_t fi_opa1x_hfi1_tx_send_egr (struct fi_opa1x_ep_tx *tx,
		const void *buf, size_t len, void *desc,
		fi_addr_t dest_addr, uint64_t tag, void* context,
		const uint32_t data, int lock_required,
		const unsigned is_msg, const unsigned is_contiguous,
		const unsigned override_flags, uint64_t tx_op_flags,
		const uint64_t dest_rx)
{
	const union fi_opa1x_addr addr = { .fi = dest_addr };

#ifdef LOCAL_COMM_ENABLED
	if (unlikely(tx->send.hdr.stl.lrh.slid == addr.uid.lid)) {

		return fi_opa1x_shm_tx_send_egr(tx, buf, len,
				desc, addr.fi, tag, context, data,
				lock_required, is_msg, is_contiguous,
				override_flags, tx_op_flags, dest_rx);
	}
#endif
	assert(lock_required == 0);

	FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_EP_DATA,
		"===================================== SEND, HFI -- EAGER (begin)\n");

	/* first check for sufficient credits to inject the entire packet */

	union fi_opa1x_hfi1_pio_state pio_state;
	uint64_t * const pio_state_ptr = (uint64_t*)tx->pio_state;
	pio_state.qw0 = *pio_state_ptr;		/* likely cache miss */

	const size_t xfer_bytes_tail = len & 0x07ul;
	const size_t payload_qws_total = len >> 3;
	const size_t payload_qws_tail = payload_qws_total & 0x07ul;

	uint16_t full_block_credits_needed = (uint16_t)(payload_qws_total >> 3);

	const uint16_t total_credits_needed =
		1 +				/* packet header */
		full_block_credits_needed +	/* full payload blocks */
		(payload_qws_tail > 0);		/* partial payload block */

	uint64_t total_credits_available = FI_OPA1X_HFI1_AVAILABLE_CREDITS(pio_state);
	if (unlikely(total_credits_available < total_credits_needed)) {
		FI_OPA1X_HFI1_UPDATE_CREDITS(pio_state, tx->pio_credits_addr);
		total_credits_available = FI_OPA1X_HFI1_AVAILABLE_CREDITS(pio_state);
		if (total_credits_available < total_credits_needed) {
			return -FI_EAGAIN;
		}
	}

	const uint64_t bth_rx = ((uint64_t)dest_rx) << 56;
	const uint64_t lrh_dlid = FI_OPA1X_ADDR_TO_HFI1_LRH_DLID(dest_addr);

#ifndef SKIP_RELIABILITY_PROTOCOL_TX
	const uint64_t psn = fi_opa1x_reliability_tx_next_psn(tx->reliability_state, addr.uid.lid, dest_rx);
#else
	const uint64_t psn = 0;
#endif

	/*
	 * Write the 'start of packet' (hw+sw header) 'send control block'
	 * which will consume a single pio credit.
	 */
	const uint64_t pbc_dws =
		2 +			/* pbc */
		2 +			/* lhr */
		3 +			/* bth */
		9 +			/* kdeth; from "RcvHdrSize[i].HdrSize" CSR */
		(payload_qws_total << 1);

	const uint16_t lrh_dws = htons(pbc_dws-1);	/* does not include pbc (8 bytes), but does include icrc (4 bytes) */

	volatile uint64_t * const scb =
		FI_OPA1X_HFI1_PIO_SCB_HEAD(tx->pio_scb_sop_first, pio_state);

	uint64_t tmp[8];

	tmp[0] = scb[0] = tx->send.qw0 | pbc_dws;
	tmp[1] = scb[1] = tx->send.hdr.qw[0] | lrh_dlid | ((uint64_t)lrh_dws << 32);

	tmp[2] = scb[2] = tx->send.hdr.qw[1] | bth_rx | (xfer_bytes_tail << 48) |
		(is_msg ?
			(uint64_t)FI_OPA1X_HFI_BTH_OPCODE_MSG_EAGER :
			(uint64_t)FI_OPA1X_HFI_BTH_OPCODE_TAG_EAGER);

	tmp[3] = scb[3] = tx->send.hdr.qw[2] | psn;
	tmp[4] = scb[4] = tx->send.hdr.qw[3] | (((uint64_t)data) << 32);
	tmp[5] = scb[5] = tx->send.hdr.qw[4] | (payload_qws_total << 48);

	/* only if is_contiguous */
	if (likely(len > 7)) {
		/* safe to blindly qw-copy the first portion of the source buffer */
		tmp[6] = scb[6] = *((uint64_t *)buf);
	} else {
		tmp[6] = 0;
		memcpy((void*)&tmp[6], buf, xfer_bytes_tail);
		scb[6] = tmp[6];
	}

	tmp[7] = scb[7] = tag;

	/* consume one credit for the packet header */
	--total_credits_available;
	FI_OPA1X_HFI1_CONSUME_SINGLE_CREDIT(pio_state);

	/*
	 * write the payload "send control block(s)"
	 */

	const uint16_t contiguous_credits_until_wrap =
		(uint16_t)(pio_state.credits_total - pio_state.scb_head_index);

	const uint16_t contiguous_credits_available =
		MIN(total_credits_available, contiguous_credits_until_wrap);


	uint64_t * buf_qws = (uint64_t*)((uintptr_t)buf + xfer_bytes_tail);


	if (full_block_credits_needed > 0) {

		volatile uint64_t * scb_payload =
			FI_OPA1X_HFI1_PIO_SCB_HEAD(tx->pio_scb_first, pio_state);

		const uint16_t contiguous_full_blocks_to_write =
			MIN(full_block_credits_needed, contiguous_credits_available);

		uint16_t i;
		for (i=0; i<contiguous_full_blocks_to_write; ++i) {
			scb_payload[0] = buf_qws[0];
			scb_payload[1] = buf_qws[1];
			scb_payload[2] = buf_qws[2];
			scb_payload[3] = buf_qws[3];
			scb_payload[4] = buf_qws[4];
			scb_payload[5] = buf_qws[5];
			scb_payload[6] = buf_qws[6];
			scb_payload[7] = buf_qws[7];
			scb_payload += 8;
			buf_qws += 8;
		}

		full_block_credits_needed -= contiguous_full_blocks_to_write;
		FI_OPA1X_HFI1_CONSUME_CREDITS(pio_state, contiguous_full_blocks_to_write);
	}

	if (unlikely(full_block_credits_needed > 0)) {

		/*
		 * handle wrap condition
		 */

		volatile uint64_t * scb_payload =
			FI_OPA1X_HFI1_PIO_SCB_HEAD(tx->pio_scb_first, pio_state);

		uint16_t i;
		for (i=0; i<full_block_credits_needed; ++i) {
			scb_payload[0] = buf_qws[0];
			scb_payload[1] = buf_qws[1];
			scb_payload[2] = buf_qws[2];
			scb_payload[3] = buf_qws[3];
			scb_payload[4] = buf_qws[4];
			scb_payload[5] = buf_qws[5];
			scb_payload[6] = buf_qws[6];
			scb_payload[7] = buf_qws[7];
			scb_payload += 8;
			buf_qws += 8;
		}

		FI_OPA1X_HFI1_CONSUME_CREDITS(pio_state, full_block_credits_needed);
	}

	if (payload_qws_tail > 0) {

		volatile uint64_t * scb_payload =
			FI_OPA1X_HFI1_PIO_SCB_HEAD(tx->pio_scb_first, pio_state);

		unsigned i = 0;
		for (; i<payload_qws_tail; ++i) {
			scb_payload[i] = buf_qws[i];
		}

		for (; i<8; ++i) {
			scb_payload[i] = 0;
		}

		FI_OPA1X_HFI1_CONSUME_SINGLE_CREDIT(pio_state);
	}

	fi_opa1x_compiler_msync_writes();
	FI_OPA1X_HFI1_CHECK_CREDITS_FOR_ERROR(tx->pio_credits_addr);

#ifndef SKIP_RELIABILITY_PROTOCOL_TX
	/*
	 * compose replay buffer and register
	 */
	struct fi_opa1x_reliability_tx_replay * replay =
		fi_opa1x_reliability_tx_replay_allocate(tx->reliability_state);

	replay->scb.qw0 = tmp[0];
	replay->scb.hdr.qw[0] = tmp[1];
	replay->scb.hdr.qw[1] = tmp[2];
	replay->scb.hdr.qw[2] = tmp[3];
	replay->scb.hdr.qw[3] = tmp[4];
	replay->scb.hdr.qw[4] = tmp[5];
	replay->scb.hdr.qw[5] = tmp[6];
	replay->scb.hdr.qw[6] = tmp[7];

	buf_qws = (uint64_t*)((uintptr_t)buf + xfer_bytes_tail);
	uint64_t * payload = replay->payload;
	size_t i;
	for (i=0; i<payload_qws_total; i++) {
		payload[i] = buf_qws[i];
	}

	fi_opa1x_reliability_tx_replay_register_no_update(tx->reliability_state, addr.uid.lid, addr.reliability_rx, dest_rx, psn, replay);
#endif

	/* update the shared hfi txe state */
	*pio_state_ptr = pio_state.qw0;

	FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_EP_DATA,
		"===================================== SEND, HFI -- EAGER (end)\n");

	return FI_SUCCESS;
}

ssize_t fi_opa1x_hfi1_tx_send_rzv (struct fi_opa1x_ep_tx *tx,
		const void *buf, size_t len, void *desc,
		fi_addr_t dest_addr, uint64_t tag, void* context,
		const uint32_t data, int lock_required,
		const unsigned is_msg, const unsigned is_contiguous,
		const unsigned override_flags, uint64_t tx_op_flags,
		const uint64_t dest_rx,
		const uintptr_t origin_byte_counter_vaddr,
		uint64_t *origin_byte_counter_value);

#endif /* _FI_PROV_OPA1X_HFI1_TRANSPORT_H_ */
