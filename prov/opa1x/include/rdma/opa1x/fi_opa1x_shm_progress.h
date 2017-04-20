#ifndef _FI_PROV_OPA1X_SHM_PROGRESS_H_ 
#define _FI_PROV_OPA1X_SHM_PROGRESS_H_

/*
 * ============================================================================
 *                      THIS IS THE SHM POLL FUNCTION
 * ============================================================================
 */
static inline
void fi_opa1x_shm_poll_once(struct fi_opa1x_ep_rx * rx,
		 const int lock_required)
{
	fi_opa1x_shm_packet_t * packet_ptr = rx->shm_poll.next_packet_ptr;

	/* fprintf(stderr, "Check incoming on %d, segment %p\n", rx->shm.local_ticket % FI_OPA1X_SHM_FIFO_SIZE, rx_fifo); */
	/* unsigned int i = 0; */
	/* for (i = 0; i < 2048; i++) { */
	/* 	if (rx_fifo->guard0[i] != 0xFF){ */
	/* 		fprintf(stderr, "Guard0 is broken: 0x%02X instead of 0x%02X on %d position\n", */
	/* 				rx_fifo->guard0[i], 0xFF, i); */
	/* 	} */
	/* } */
	/* usleep(1000); */

	if (packet_ptr->is_busy) {
		/* fprintf(stderr, "Got something on slot %d\n", rx->shm.local_ticket % FI_OPA1X_SHM_FIFO_SIZE); */

		const union fi_opa1x_hfi1_packet_hdr * const hdr =
			(union fi_opa1x_hfi1_packet_hdr *)&packet_ptr->hdr;

		const uint8_t opcode = hdr->stl.bth.opcode;

		if (likely(opcode >= FI_OPA1X_HFI_BTH_OPCODE_TAG_INJECT)) {
			fi_opa1x_ep_rx_process_header_tag(rx, hdr, packet_ptr->payload, FI_OPA1X_SHM_PAYLOAD_SIZE, opcode, 1, lock_required);
		} else {
			fi_opa1x_ep_rx_process_header_msg(rx, hdr, packet_ptr->payload, FI_OPA1X_SHM_PAYLOAD_SIZE, opcode, 1, lock_required);
		}

		fi_opa1x_shm_compiler_barrier();
		packet_ptr->is_busy = 0;

		/* fprintf(stderr, "Release %d slot\n", rx->shm.local_ticket % FI_OPA1X_SHM_FIFO_SIZE); */
		rx->shm.local_ticket++;
		/* fprintf(stderr, "Switch to %d slot\n", rx->shm.local_ticket % FI_OPA1X_SHM_FIFO_SIZE); */

		fi_opa1x_shm_fifo_t * rx_fifo = rx->shm.segment_ptr;
		rx->shm_poll.next_packet_ptr = &rx_fifo->packets[rx->shm.local_ticket % FI_OPA1X_SHM_FIFO_SIZE];
	}
}

static inline
void fi_opa1x_shm_poll_many(struct fi_opa1x_ep_rx * rx,
		const int lock_required)
{
	fi_opa1x_shm_poll_once(rx, lock_required);
}

#endif /* _FI_PROV_OPA1X_SHM_PROGRESS_H_ */
