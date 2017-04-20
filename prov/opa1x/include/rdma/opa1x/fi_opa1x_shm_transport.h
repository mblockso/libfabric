#ifndef _FI_PROV_OPA1X_SHM_TRANSPORT_H_
#define _FI_PROV_OPA1X_SHM_TRANSPORT_H_

#include "rdma/opa1x/fi_opa1x_shm.h"

void fi_opa1x_shm_rx_rzv_rts (struct fi_opa1x_ep_rx *rx,
		const void * const hdr, const void * const payload,
		const uint8_t u8_rx, const uint64_t niov,
		uintptr_t origin_byte_counter_vaddr,
		uintptr_t target_byte_counter_vaddr,
		const uintptr_t dst_vaddr,
		const uintptr_t src_vaddr,
		const uint64_t nbytes_to_transfer,
		const unsigned is_intranode);


static inline
ssize_t fi_opa1x_shm_tx_inject (struct fi_opa1x_ep_tx *tx,
		const void *buf, size_t len, fi_addr_t dest_addr, uint64_t tag,
		const uint32_t data, int lock_required, const unsigned is_msg,
		const uint64_t dest_rx)
{
	FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_EP_DATA,
		"===================================== INJECT, SHM (begin)\n");

	union fi_opa1x_addr addr;
	addr.raw64b = (uint64_t)dest_addr;

	int global_ticket = 0;

	fi_opa1x_shm_fifo_t * tx_fifo = tx->shm.connections[addr.hfi1_rx].segment_ptr;

	global_ticket = fi_opa1x_shm_x86_atomic_fetch_and_add_int(&(tx_fifo->ticket), 1);

	fi_opa1x_shm_packet_t * packet_ptr = &tx_fifo->packets[global_ticket % FI_OPA1X_SHM_FIFO_SIZE];

	/* FI_LOG(fi_opa1x_global.prov, FI_LOG_DEBUG, FI_LOG_FABRIC, */
	/* 	   "Injecting data over SHM to %u context (slot = %d)\n", addr.hfi1_rx, global_ticket % FI_OPA1X_SHM_FIFO_SIZE); */

	unsigned spin_count = 0;
	while (packet_ptr->is_busy) {
		/* Spin wait */
				if (spin_count++ > 1000) {
					FI_WARN(fi_opa1x_global.prov, FI_LOG_EP_DATA, "shm fifo stuck!!!!!!!!!!!!\n");
					abort();
				}
		fi_opa1x_shm_x86_pause();
	}

	const uint64_t bth_rx = ((uint64_t)dest_rx) << 56;
	const uint64_t lrh_dlid = FI_OPA1X_ADDR_TO_HFI1_LRH_DLID(addr.fi);

	const uint64_t psn = 0;

	if (lock_required) { fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__); abort(); }

	union fi_opa1x_hfi1_packet_hdr * hfi1_packet_hdr = (union fi_opa1x_hfi1_packet_hdr *)&packet_ptr->hdr;

	uint64_t * const scb_shm = (uint64_t *)hfi1_packet_hdr; /* scb_shm is shifted on 8 bytes */

	scb_shm[0] = tx->inject.hdr.qw[0] | lrh_dlid;

	scb_shm[1] = tx->inject.hdr.qw[1] | bth_rx | (len << 48) |
			(is_msg ? /* compile-time constant expression */
				(uint64_t)FI_OPA1X_HFI_BTH_OPCODE_MSG_INJECT :
				(uint64_t)FI_OPA1X_HFI_BTH_OPCODE_TAG_INJECT);

	scb_shm[2] = tx->inject.hdr.qw[2] | psn;
	scb_shm[3] = tx->inject.hdr.qw[3] | (((uint64_t)data) << 32);

	switch (len) {
		case 0:
			break;
		case 1:
			*((uint8_t*)&scb_shm[4]) = *((uint8_t*)buf);
			break;
		case 2:
			*((uint16_t*)&scb_shm[4]) = *((uint16_t*)buf);
			break;
		case 3:
			memcpy((void*)&scb_shm[4], buf, len);
			break;
		case 4:
			*((uint32_t*)&scb_shm[4]) = *((uint32_t*)buf);
			break;
		case 5:
		case 6:
		case 7:
			memcpy((void*)&scb_shm[4], buf, len);
			break;
		case 8:
			scb_shm[4] = *((uint64_t*)buf);
			break;
		case 9:
		case 10:
		case 11:
		case 12:
		case 13:
		case 14:
		case 15:
			memcpy((void*)&scb_shm[4], buf, len);
			break;
		case 16:
			scb_shm[4] = *((uint64_t*)buf);
			scb_shm[5] = *((uint64_t*)buf+1);
			break;
		default:
			fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__); abort();
			break;
	}

	scb_shm[6] = tag;
#if 0
	hfi1_packet_hdr->stl.bth.opcode	= is_msg ?
		(uint64_t)FI_OPA1X_HFI_BTH_OPCODE_MSG_INJECT :
		(uint64_t)FI_OPA1X_HFI_BTH_OPCODE_TAG_INJECT;

	hfi1_packet_hdr->match.slid      = htons(tx->stx->hfi->lid);
	hfi1_packet_hdr->match.origin_cx = tx->inject.hdr.match.origin_cx;
	hfi1_packet_hdr->match.ofi_data  = data;
	hfi1_packet_hdr->match.ofi_tag   = tag;

	fi_opa1x_shm_memcpy(hfi1_packet_hdr->inject.app_data_u8, buf, len);
	hfi1_packet_hdr->inject.message_length = len;
#endif /* 0 */

	/* fi_opa1x_hfi1_dump_packet_hdr((struct fi_opa1x_hfi1_stl_packet_hdr *)&packet_ptr->hdr, __func__, __LINE__); */

	fi_opa1x_shm_compiler_barrier();
	packet_ptr->is_busy = 1;


	FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_EP_DATA,
		"===================================== INJECT, SHM (end)\n");

	return FI_SUCCESS;
}

static inline
ssize_t fi_opa1x_shm_tx_send_egr (struct fi_opa1x_ep_tx *tx,
		const void *buf, size_t len, void *desc,
		fi_addr_t dest_addr, uint64_t tag, void* context,
		const uint32_t data, int lock_required,
		const unsigned is_msg, const unsigned is_contiguous,
		const unsigned override_flags, uint64_t tx_op_flags,
		const uint64_t dest_rx)
{
	FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_EP_DATA,
		"===================================== SEND, SHM -- EAGER (begin)\n");

	union fi_opa1x_addr addr;
	addr.raw64b = (uint64_t)dest_addr;

	int global_ticket = 0;

	fi_opa1x_shm_fifo_t * tx_fifo = tx->shm.connections[addr.hfi1_rx].segment_ptr;

	global_ticket = fi_opa1x_shm_x86_atomic_fetch_and_add_int(&(tx_fifo->ticket), 1);

	fi_opa1x_shm_packet_t * packet_ptr = &tx_fifo->packets[global_ticket % FI_OPA1X_SHM_FIFO_SIZE]; 

	/* FI_LOG(fi_opa1x_global.prov, FI_LOG_DEBUG, FI_LOG_FABRIC, */
	/* 	   "Sending data over SHM to %u context (slot = %d), global slot = %d\n", */
	/* 	   addr.hfi1_rx, global_ticket % FI_OPA1X_SHM_FIFO_SIZE, tx_fifo->ticket % FI_OPA1X_SHM_FIFO_SIZE); */

	unsigned spin_count = 0;
	while (packet_ptr->is_busy) {
		/* Spin wait */
		fi_opa1x_shm_x86_pause();
		if (spin_count++ > 1000) {
			FI_WARN(fi_opa1x_global.prov, FI_LOG_EP_DATA, "shm fifo stuck!!!!!!!!!!!!\n");
			abort();
		}
	}

	const uint64_t psn = 0;

	const uint64_t bth_rx = ((uint64_t)dest_rx) << 56;
	const uint64_t lrh_dlid = FI_OPA1X_ADDR_TO_HFI1_LRH_DLID(addr.fi);

	size_t xfer_len = len;

	union fi_opa1x_hfi1_packet_hdr * hfi1_packet_hdr = (union fi_opa1x_hfi1_packet_hdr *)&packet_ptr->hdr;

	/* single packet eager */
	const size_t xfer_bytes_tail   = xfer_len & 0x07ul;
	const size_t payload_qws_total = xfer_len >> 3;

	const uint64_t pbc_dws =
		2 +			/* pbc */
		2 +			/* lhr */
		3 +			/* bth */
		9 +			/* kdeth; from "RcvHdrSize[i].HdrSize" CSR */
		(payload_qws_total << 1);

	const uint16_t lrh_dws = htons(pbc_dws - 1);

	uint64_t * const scb_shm = (uint64_t *)hfi1_packet_hdr; /* scb_shm is shifted on 8 bytes */

	scb_shm[0] = tx->send.hdr.qw[0] | lrh_dlid | ((uint64_t)lrh_dws << 32);
	scb_shm[1] = tx->send.hdr.qw[1] | bth_rx | (xfer_bytes_tail << 48) |
		(is_msg ?
		    (uint64_t)FI_OPA1X_HFI_BTH_OPCODE_MSG_EAGER :
		    (uint64_t)FI_OPA1X_HFI_BTH_OPCODE_TAG_EAGER);
	scb_shm[2] = tx->send.hdr.qw[2] | psn;
	scb_shm[3] = tx->send.hdr.qw[3] | (((uint64_t)data) << 32);
	scb_shm[4] = tx->send.hdr.qw[4] | (payload_qws_total << 48);

	if (is_contiguous) {
		if (likely(xfer_len > 7)) {
			/* safe to blindly qw-copy the first portion of the source buffer */
			scb_shm[5] = *((uint64_t *)buf);
		} else {
			memcpy((void*)&scb_shm[5], buf, xfer_bytes_tail);
		}
	} else {
		fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__); abort();
	}

	scb_shm[6] = tag;

	uint64_t * buf_qws = (uint64_t*)((uint8_t *)buf + xfer_bytes_tail);
	uint64_t * scb_payload = (uint64_t *)packet_ptr->payload;

	memcpy((void *)scb_payload, buf_qws, payload_qws_total * 8);

	fi_opa1x_shm_compiler_barrier();
	packet_ptr->is_busy = 1;

	FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_EP_DATA,
		"===================================== SEND, SHM -- EAGER (end)\n");

	return FI_SUCCESS;
}


static inline
ssize_t fi_opa1x_shm_tx_send_rzv (struct fi_opa1x_ep_tx *tx,
		const void *buf, size_t len, void *desc,
		fi_addr_t dest_addr, uint64_t tag, void* context,
		const uint32_t data, int lock_required,
		const unsigned is_msg, const unsigned is_contiguous,
		const unsigned override_flags, uint64_t tx_op_flags,
		const uint64_t dest_rx,
		const uintptr_t origin_byte_counter_vaddr,
		uint64_t *origin_byte_counter_value)
{
	FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_EP_DATA,
		"===================================== SEND, SHM -- EAGER (begin)\n");

	union fi_opa1x_addr addr;
	addr.raw64b = (uint64_t)dest_addr;

	int global_ticket = 0;

	fi_opa1x_shm_fifo_t * tx_fifo = tx->shm.connections[addr.hfi1_rx].segment_ptr;

	global_ticket = fi_opa1x_shm_x86_atomic_fetch_and_add_int(&(tx_fifo->ticket), 1);

	fi_opa1x_shm_packet_t * packet_ptr = &tx_fifo->packets[global_ticket % FI_OPA1X_SHM_FIFO_SIZE];

	/* FI_LOG(fi_opa1x_global.prov, FI_LOG_DEBUG, FI_LOG_FABRIC, */
	/* 	   "Sending data over SHM to %u context (slot = %d), global slot = %d\n", */
	/* 	   addr.hfi1_rx, global_ticket % FI_OPA1X_SHM_FIFO_SIZE, tx_fifo->ticket % FI_OPA1X_SHM_FIFO_SIZE); */

	unsigned spin_count = 0;
	while (packet_ptr->is_busy) {
		/* Spin wait */
		fi_opa1x_shm_x86_pause();
		if (spin_count++ > 1000) {
			FI_WARN(fi_opa1x_global.prov, FI_LOG_EP_DATA, "shm fifo stuck!!!!!!!!!!!!\n");
			abort();
		}
	}

	union fi_opa1x_hfi1_packet_hdr * hfi1_packet_hdr = (union fi_opa1x_hfi1_packet_hdr *)&packet_ptr->hdr;

	const uint64_t max_immediate_block_count = 2;

	const uint64_t immediate_byte_count = len & 0x0007ul;
	const uint64_t immediate_qw_count = (len >> 3) & 0x0007ul;
	const uint64_t immediate_block_count = MIN((len >> 6), max_immediate_block_count);
	const uint64_t immediate_total = immediate_byte_count +
		immediate_qw_count * sizeof(uint64_t) +
		immediate_block_count * sizeof(union cacheline);

	assert(((len - immediate_total) & 0x003Fu) == 0);

	*origin_byte_counter_value = len - immediate_total;

	hfi1_packet_hdr->stl.bth.opcode = is_msg ?
		(uint64_t)FI_OPA1X_HFI_BTH_OPCODE_MSG_RZV_RTS :
		(uint64_t)FI_OPA1X_HFI_BTH_OPCODE_TAG_RZV_RTS;

	hfi1_packet_hdr->match.slid = tx->rzv.hdr.match.slid;
	hfi1_packet_hdr->match.origin_cx = tx->rzv.hdr.match.origin_cx;
	hfi1_packet_hdr->match.ofi_data = data;
	hfi1_packet_hdr->match.ofi_tag = tag;

	hfi1_packet_hdr->rendezvous.origin_rx = tx->rzv.hdr.rendezvous.origin_rx;
	hfi1_packet_hdr->rendezvous.niov = 1;
	hfi1_packet_hdr->rendezvous.message_length = len;

	hfi1_packet_hdr->reliability.origin_reliability_rx = tx->rzv.hdr.reliability.origin_reliability_rx;

	union fi_opa1x_hfi1_packet_payload * payload =
		(union fi_opa1x_hfi1_packet_payload *)packet_ptr->payload;

	payload->rendezvous.contiguous.src_vaddr = (uintptr_t)buf + immediate_total;
	payload->rendezvous.contiguous.src_blocks = (len - immediate_total) >> 6;
	payload->rendezvous.contiguous.immediate_byte_count = immediate_byte_count;
	payload->rendezvous.contiguous.immediate_qw_count = immediate_qw_count;
	payload->rendezvous.contiguous.immediate_block_count = immediate_block_count;
	payload->rendezvous.contiguous.origin_byte_counter_vaddr = origin_byte_counter_vaddr;

	uint8_t *sbuf = (uint8_t *)buf;
	if (immediate_byte_count > 0) {
		memcpy((void*)payload->rendezvous.contiguous.immediate_byte, (const void*)sbuf, immediate_byte_count);
		sbuf += immediate_byte_count;
	}

	uint64_t * sbuf_qw = (uint64_t *)sbuf;
	unsigned i=0;
	for (i=0; i<immediate_qw_count; ++i) {
		payload->rendezvous.contiguous.immediate_qw[i] = sbuf_qw[i];
	}

	sbuf_qw += immediate_qw_count;
	memcpy((void*)payload->rendezvous.contiguous.immediate_block, (const void*)sbuf_qw, immediate_block_count*sizeof(union cacheline));

	fi_opa1x_shm_compiler_barrier();
	packet_ptr->is_busy = 1;

	FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_EP_DATA,
		"===================================== SEND, SHM -- EAGER (begin)\n");

	return FI_SUCCESS;
}


static inline
void fi_opa1x_shm_rx_rzv_cts (struct fi_opa1x_ep_rx *rx,
		const void * const hdr, const void * const payload,
		const uint8_t u8_rx, const uint64_t niov,
		const struct fi_opa1x_hfi1_dput_iov * const dput_iov,
		uintptr_t target_byte_counter_vaddr,
		uint64_t * origin_byte_counter,
		const unsigned is_intranode)
{
	FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_EP_DATA,
		"===================================== RECV, SHM -- RENDEZVOUS CTS (begin)\n");


	/* write one or more 'rendezvous dput' shm packet */

	FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_EP_DATA, "write one or more 'rendezvous dput' shm packet\n");

	int global_ticket = 0;

	fi_opa1x_shm_fifo_t * tx_fifo = rx->tx.shm.connections[u8_rx].segment_ptr;

	unsigned i;
	for (i=0; i<niov; ++i) {
		uint8_t * sbuf = (uint8_t *)dput_iov[i].sbuf;
		uintptr_t rbuf = dput_iov[i].rbuf;
		uint64_t bytes_to_send = dput_iov[i].bytes;

		while (bytes_to_send > 0) {

			uint64_t bytes_to_send_in_this_packet =
				bytes_to_send > FI_OPA1X_SHM_PAYLOAD_SIZE ?
				FI_OPA1X_SHM_PAYLOAD_SIZE : bytes_to_send;

			FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_EP_DATA, "i = %u, bytes_to_send = %lu, bytes_to_send_in_this_packet = %lu\n", i, bytes_to_send, bytes_to_send_in_this_packet);

			global_ticket = fi_opa1x_shm_x86_atomic_fetch_and_add_int(&(tx_fifo->ticket), 1);

			fi_opa1x_shm_packet_t * packet_ptr = &tx_fifo->packets[global_ticket % FI_OPA1X_SHM_FIFO_SIZE];

			FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_EP_DATA,
				   "Injecting data over SHM to %u context (slot = %d)\n", u8_rx, global_ticket % FI_OPA1X_SHM_FIFO_SIZE);

			unsigned spin_count = 0;
			while (packet_ptr->is_busy) {
				/* Spin wait */
				fi_opa1x_shm_x86_pause();
				if (spin_count++ > 100000) {
					FI_WARN(fi_opa1x_global.prov, FI_LOG_EP_DATA, "shm fifo stuck (%u) !!!!!!!!!!!!\n", spin_count);
					abort();
				}
			}
			FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_EP_DATA, "write shm rzv data packet\n");

			union fi_opa1x_hfi1_packet_hdr * hfi1_packet_hdr =
				(union fi_opa1x_hfi1_packet_hdr *)&packet_ptr->hdr;

			hfi1_packet_hdr->stl.bth.opcode = FI_OPA1X_HFI_BTH_OPCODE_RZV_DATA;
			hfi1_packet_hdr->dput.target_byte_counter_vaddr = target_byte_counter_vaddr;
			hfi1_packet_hdr->dput.rbuf = rbuf;
			hfi1_packet_hdr->dput.bytes = bytes_to_send_in_this_packet;

			memcpy(packet_ptr->payload, sbuf, bytes_to_send_in_this_packet);

			sbuf += bytes_to_send_in_this_packet;
			rbuf += bytes_to_send_in_this_packet;
			bytes_to_send -= bytes_to_send_in_this_packet;
			*origin_byte_counter -= bytes_to_send_in_this_packet;

			fi_opa1x_shm_compiler_barrier();

			packet_ptr->is_busy = 1;
		}
	}

	FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_EP_DATA,
		"===================================== RECV, SHM -- RENDEZVOUS CTS (end)\n");
}


#endif /* _FI_PROV_OPA1X_SHM_TRANSPORT_H_ */
