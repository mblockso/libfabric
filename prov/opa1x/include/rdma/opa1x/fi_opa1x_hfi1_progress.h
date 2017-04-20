#ifndef _FI_PROV_OPA1X_HFI1_PROGRESS_H_
#define _FI_PROV_OPA1X_HFI1_PROGRESS_H_

#ifndef FI_OPA1X_FABRIC_HFI1
#error "fabric selection #define error"
#endif

#include "rdma/opa1x/fi_opa1x_hfi1.h"

#ifdef LOCAL_COMM_ENABLED
#include "rdma/opa1x/fi_opa1x_shm_progress.h"
#endif

/*
 * ============================================================================
 *                      THIS IS THE HFI POLL FUNCTION
 * ============================================================================
 */
static inline
unsigned fi_opa1x_hfi1_poll_once (struct fi_opa1x_ep_rx * rx,
		const int lock_required) {

	const uint64_t hdrq_offset = rx->state.hdrq.head & 0x000000000000FFE0ul;

	volatile uint32_t * rhf_ptr = (uint32_t *)rx->hdrq.rhf_base + hdrq_offset;
	const uint32_t rhf_lsb = rhf_ptr[0];
	const uint32_t rhf_msb = rhf_ptr[1];

	/*
	 * The RHF.RcvSeq field is located in bits [31:28] and values are in
	 * the range of (1..13) inclusive. A new packet is available when the
	 * expected sequence number in the next header queue element matches
	 * the RHF.RcvSeq field.
	 *
	 * Instead of shifting and masking the RHF bits to read the sequence
	 * number in the range of 1..13 (or, 0x1..0xD) use only a bit mask to
	 * obtain the RHF sequence in the range of 0x10000000..0xD0000000.
	 * In this scheme the expected sequence number is incremented by
	 * 0x10000000 instead of 0x1.
	 */
	const uint32_t rhf_seq = rx->state.hdrq.rhf_seq;

	/*
	 * Check for receive errors
	 */
	if (unlikely((rhf_msb & 0xFFE00000u) != 0)) {

#ifndef SKIP_RELIABILITY_PROTOCOL_RX

#define HFI1_RHF_ICRCERR	(0x80000000u)
#define HFI1_RHF_ECCERR		(0x20000000u)
#define HFI1_RHF_TIDERR		(0x08000000u)
#define HFI1_RHF_DCERR		(0x00800000u)
#define HFI1_RHF_DCUNCERR	(0x00400000u)

		if ((rhf_msb & (HFI1_RHF_ICRCERR | HFI1_RHF_ECCERR | HFI1_RHF_TIDERR | HFI1_RHF_DCERR | HFI1_RHF_DCUNCERR)) != 0) {

			/* drop this packet and allow reliability protocol to retry */
			if (rhf_seq == (rhf_lsb & 0xF0000000u)) {

#ifdef OPA1X_RELIABILITY_DEBUG
				const uint64_t hdrq_offset_dws = (rhf_msb >> 12) & 0x01FFu;

				uint32_t * pkt = (uint32_t *)rhf_ptr -
					32 +	/* header queue entry size in dw */
					2 +	/* rhf field size in dw */
					hdrq_offset_dws;

				const union fi_opa1x_hfi1_packet_hdr * const hdr =
					(union fi_opa1x_hfi1_packet_hdr *)pkt;

				fprintf(stderr, "%s:%s():%d drop this packet and allow reliability protocol to retry, psn = %lu\n", __FILE__, __func__, __LINE__, hdr->reliability.psn);
#endif
				if ((rhf_lsb & 0x00008000u) == 0x00008000u) {

					/* "consume" this egrq element */
					const uint32_t egrbfr_index = (rhf_lsb >> FI_OPA1X_HFI1_RHF_EGRBFR_INDEX_SHIFT) & FI_OPA1X_HFI1_RHF_EGRBFR_INDEX_MASK;
					const uint32_t last_egrbfr_index = rx->egrq.last_egrbfr_index;
					if (unlikely(last_egrbfr_index != egrbfr_index)) {
						*rx->egrq.head_register = last_egrbfr_index;
						rx->egrq.last_egrbfr_index = egrbfr_index;
					}
				}

				/* "consume" this hdrq element */
				rx->state.hdrq.rhf_seq = (rhf_seq < 0xD0000000u) * rhf_seq + 0x10000000u;
				rx->state.hdrq.head = hdrq_offset + 32;	/* 32 dws == 128 bytes, the maximum header queue entry size */
				if (unlikely((hdrq_offset & 0x7FFFul) == 0x0020ul)) {
					*rx->hdrq.head_register = hdrq_offset - 32;
				}

				/* TODO - immediately send nack? */
#if 0
			} else {
				/*
				 * TODO - this may not actually be an unrecoverable
				 * error as ignoring the case where the expected
				 * rhf sequence number is not received may mean that
				 * headers were silently dropped within the HFI (?!)
				 *
				 * Removing this check/abort allows the 'pingping'
				 * test to complete successfully.
				 */
				fprintf(stderr, "%s:%s():%d === RECEIVE ERROR: rhf_msb = 0x%08x, rhf_lsb = 0x%08x\n", __FILE__, __func__, __LINE__, rhf_msb, rhf_lsb);
				fprintf(stderr, "%s:%s():%d === RHF.ICRCErr    = %u\n", __FILE__, __func__, __LINE__, (rhf_msb >> 31) & 0x01u);
				fprintf(stderr, "%s:%s():%d === RHF.Reserved   = %u\n", __FILE__, __func__, __LINE__, (rhf_msb >> 30) & 0x01u);
				fprintf(stderr, "%s:%s():%d === RHF.EccErr     = %u\n", __FILE__, __func__, __LINE__, (rhf_msb >> 29) & 0x01u);
				fprintf(stderr, "%s:%s():%d === RHF.LenErr     = %u\n", __FILE__, __func__, __LINE__, (rhf_msb >> 28) & 0x01u);
				fprintf(stderr, "%s:%s():%d === RHF.TIDErr     = %u\n", __FILE__, __func__, __LINE__, (rhf_msb >> 27) & 0x01u);
				fprintf(stderr, "%s:%s():%d === RHF.RcvTypeErr = %u\n", __FILE__, __func__, __LINE__, (rhf_msb >> 24) & 0x07u);
				fprintf(stderr, "%s:%s():%d === RHF.DcErr      = %u\n", __FILE__, __func__, __LINE__, (rhf_msb >> 23) & 0x01u);
				fprintf(stderr, "%s:%s():%d === RHF.DcUncErr   = %u\n", __FILE__, __func__, __LINE__, (rhf_msb >> 22) & 0x01u);
				fprintf(stderr, "%s:%s():%d === RHF.KHdrLenErr = %u\n", __FILE__, __func__, __LINE__, (rhf_msb >> 21) & 0x01u);

				abort();
#endif
			}
			return 1;
		}
#endif



		fprintf(stderr, "%s:%s():%d === RECEIVE ERROR: rhf_msb = 0x%08x, rhf_lsb = 0x%08x\n", __FILE__, __func__, __LINE__, rhf_msb, rhf_lsb);
		fprintf(stderr, "%s:%s():%d === RHF.ICRCErr    = %u\n", __FILE__, __func__, __LINE__, (rhf_msb >> 31) & 0x01u);
		fprintf(stderr, "%s:%s():%d === RHF.Reserved   = %u\n", __FILE__, __func__, __LINE__, (rhf_msb >> 30) & 0x01u);
		fprintf(stderr, "%s:%s():%d === RHF.EccErr     = %u\n", __FILE__, __func__, __LINE__, (rhf_msb >> 29) & 0x01u);
		fprintf(stderr, "%s:%s():%d === RHF.LenErr     = %u\n", __FILE__, __func__, __LINE__, (rhf_msb >> 28) & 0x01u);
		fprintf(stderr, "%s:%s():%d === RHF.TIDErr     = %u\n", __FILE__, __func__, __LINE__, (rhf_msb >> 27) & 0x01u);
		fprintf(stderr, "%s:%s():%d === RHF.RcvTypeErr = %u\n", __FILE__, __func__, __LINE__, (rhf_msb >> 24) & 0x07u);
		fprintf(stderr, "%s:%s():%d === RHF.DcErr      = %u\n", __FILE__, __func__, __LINE__, (rhf_msb >> 23) & 0x01u);
		fprintf(stderr, "%s:%s():%d === RHF.DcUncErr   = %u\n", __FILE__, __func__, __LINE__, (rhf_msb >> 22) & 0x01u);
		fprintf(stderr, "%s:%s():%d === RHF.KHdrLenErr = %u\n", __FILE__, __func__, __LINE__, (rhf_msb >> 21) & 0x01u);

		abort();
	}

	if (rhf_seq == (rhf_lsb & 0xF0000000u)) {

		const uint64_t hdrq_offset_dws = (rhf_msb >> 12) & 0x01FFu;

		uint32_t * pkt = (uint32_t *)rhf_ptr -
			32 +	/* header queue entry size in dw */
			2 +	/* rhf field size in dw */
			hdrq_offset_dws;

		const union fi_opa1x_hfi1_packet_hdr * const hdr =
			(union fi_opa1x_hfi1_packet_hdr *)pkt;

#ifndef SKIP_RELIABILITY_PROTOCOL_RX

		if (unlikely(FI_OPA1X_RELIABILITY_RX_DROP_PACKET(&rx->reliability.state))) {
			rx->state.hdrq.rhf_seq = (rhf_seq < 0xD0000000u) * rhf_seq + 0x10000000u;
			rx->state.hdrq.head = hdrq_offset + 32;	/* 32 dws == 128 bytes, the maximum header queue entry size */

			if ((rhf_lsb & 0x00008000u) == 0x00008000u) {	/* eager */
				const uint32_t egrbfr_index = (rhf_lsb >> FI_OPA1X_HFI1_RHF_EGRBFR_INDEX_SHIFT) & FI_OPA1X_HFI1_RHF_EGRBFR_INDEX_MASK;
				const uint32_t last_egrbfr_index = rx->egrq.last_egrbfr_index;
				if (unlikely(last_egrbfr_index != egrbfr_index)) {
					*rx->egrq.head_register = last_egrbfr_index;
					rx->egrq.last_egrbfr_index = egrbfr_index;
				}
			}

			return 0;
		}

		/*
		 * Check for 'reliability' exceptions
		 */
		const uint64_t slid = hdr->stl.lrh.slid;
		const uint64_t origin_tx = hdr->reliability.origin_tx;
		const uint64_t psn = hdr->reliability.psn;
		if (unlikely(fi_opa1x_reliability_rx_check(&rx->reliability.state, slid, origin_tx, psn) == FI_OPA1X_RELIABILITY_EXCEPTION)) {

			if ((rhf_lsb & 0x00008000u) != 0x00008000u) {
				/* no payload */
				fi_opa1x_reliability_rx_exception(&rx->reliability.state, slid, origin_tx, psn, (void*)rx, hdr, NULL);

			} else {
				/* has payload */
				const uint32_t egrbfr_index = (rhf_lsb >> FI_OPA1X_HFI1_RHF_EGRBFR_INDEX_SHIFT) & FI_OPA1X_HFI1_RHF_EGRBFR_INDEX_MASK;
				const uint32_t egrbfr_offset = rhf_msb & 0x0FFFu;
				const uint8_t * const payload =
					(uint8_t *)((uintptr_t) rx->egrq.base_addr +
					egrbfr_index * rx->egrq.elemsz +
					egrbfr_offset * 64);

				assert(payload!=NULL);
				fi_opa1x_reliability_rx_exception(&rx->reliability.state, slid, origin_tx, psn, (void*)rx, hdr, payload);

				const uint32_t last_egrbfr_index = rx->egrq.last_egrbfr_index;
				if (unlikely(last_egrbfr_index != egrbfr_index)) {
					*rx->egrq.head_register = last_egrbfr_index;
					rx->egrq.last_egrbfr_index = egrbfr_index;
				}
			}

			rx->state.hdrq.rhf_seq = (rhf_seq < 0xD0000000u) * rhf_seq + 0x10000000u;
			rx->state.hdrq.head = hdrq_offset + 32;	/* 32 dws == 128 bytes, the maximum header queue entry size */

			/*
			 * Notify the hfi that this packet has been processed ..
			 * but only do this every 1024 hdrq elements because the hdrq
			 * size is 2048 and the update is expensive.
			 */
			if (unlikely((hdrq_offset & 0x7FFFul) == 0x0020ul)) {
				*rx->hdrq.head_register = hdrq_offset - 32;
			}

			return 1;	/* one packet was processed - even though it was a "reliability event" packet */
		}
#endif

		const uint8_t opcode = hdr->stl.bth.opcode;

		FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_EP_DATA, "================ received a packet from the fabric\n");

		if ((rhf_lsb & 0x00008000u) != 0x00008000u) {

			/* "header only" packet - no payload */

			if (likely(opcode == FI_OPA1X_HFI_BTH_OPCODE_TAG_INJECT)) {

				fi_opa1x_ep_rx_process_header(rx, hdr, NULL, 0,
					FI_OPA1X_EP_RX_QUEUE_KIND_TAG,
					FI_OPA1X_HFI_BTH_OPCODE_TAG_INJECT,
					0, /* is_intranode */
					lock_required);

			} else if (opcode > FI_OPA1X_HFI_BTH_OPCODE_TAG_INJECT) {	/* all other "tag" packets */

				fi_opa1x_ep_rx_process_header_tag(rx, hdr, NULL, 0, opcode, 0, lock_required);

			} else {

				fi_opa1x_ep_rx_process_header_msg(rx, hdr, NULL, 0, opcode, 0, lock_required);
			}

		} else {
			/* "eager" packet - has payload */

			const uint32_t egrbfr_index = (rhf_lsb >> FI_OPA1X_HFI1_RHF_EGRBFR_INDEX_SHIFT) & FI_OPA1X_HFI1_RHF_EGRBFR_INDEX_MASK;
			const uint32_t egrbfr_offset = rhf_msb & 0x0FFFu;
			const uint8_t * const payload =
				(uint8_t *)((uintptr_t) rx->egrq.base_addr +
				egrbfr_index * rx->egrq.elemsz +
				egrbfr_offset * 64);

			assert(payload!=NULL);

			/* reported in LRH as the number of 4-byte words in the packet; header + payload + icrc */
			const uint16_t lrh_pktlen_le = ntohs(hdr->stl.lrh.pktlen);
			const size_t total_bytes_to_copy = (lrh_pktlen_le - 1) * 4;	/* do not copy the trailing icrc */
			const size_t payload_bytes_to_copy = total_bytes_to_copy - sizeof(union fi_opa1x_hfi1_packet_hdr);

			if (likely(opcode == FI_OPA1X_HFI_BTH_OPCODE_TAG_EAGER)) {

				fi_opa1x_ep_rx_process_header(rx, hdr,
					(const union fi_opa1x_hfi1_packet_payload * const) payload,
					payload_bytes_to_copy,
					FI_OPA1X_EP_RX_QUEUE_KIND_TAG,
					FI_OPA1X_HFI_BTH_OPCODE_TAG_EAGER,
					0, /* is_intranode */
					lock_required);

			} else if (opcode > FI_OPA1X_HFI_BTH_OPCODE_TAG_EAGER) {	/* all other "tag" packets */

				fi_opa1x_ep_rx_process_header_tag(rx, hdr, payload, payload_bytes_to_copy, opcode, 0, lock_required);

			} else {

				fi_opa1x_ep_rx_process_header_msg(rx, hdr, payload, payload_bytes_to_copy, opcode, 0, lock_required);
			}

			const uint32_t last_egrbfr_index = rx->egrq.last_egrbfr_index;
			if (unlikely(last_egrbfr_index != egrbfr_index)) {
				*rx->egrq.head_register = last_egrbfr_index;
				rx->egrq.last_egrbfr_index = egrbfr_index;
			}
		}

		rx->state.hdrq.rhf_seq = (rhf_seq < 0xD0000000u) * rhf_seq + 0x10000000u;
		rx->state.hdrq.head = hdrq_offset + 32;	/* 32 dws == 128 bytes, the maximum header queue entry size */

		/*
		 * Notify the hfi that this packet has been processed ..
		 * but only do this every 1024 hdrq elements because the hdrq
		 * size is 2048 and the update is expensive.
		 */
		if (unlikely((hdrq_offset & 0x7FFFul) == 0x0020ul)) {
			*rx->hdrq.head_register = hdrq_offset - 32;
		}

		return 1;	/* one packet was processed */
	}

	return 0;
}



static inline
void fi_opa1x_hfi1_poll_many (struct fi_opa1x_ep_rx * rx,
		const int lock_required) {

	static const unsigned hfi1_poll_max = 100;
	unsigned hfi1_poll_count = 0;
	unsigned packets = 0;

#ifdef LOCAL_COMM_ENABLED
	fi_opa1x_shm_poll_once(rx, 0);
#endif
	do {
		packets = fi_opa1x_hfi1_poll_once(rx, 0);
	} while ((packets > 0) && (hfi1_poll_count++ < hfi1_poll_max));

	return;
}





#endif /* _FI_PROV_OPA1X_HFI1_PROGRESS_H_ */
