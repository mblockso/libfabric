#include <stdint.h>
#include <sys/uio.h>

#include <ofi.h>

#include "rdma/opa1x/fi_opa1x_domain.h"
#include "rdma/opa1x/fi_opa1x_endpoint.h"
#include "rdma/opa1x/fi_opa1x.h"
#include "rdma/opa1x/fi_opa1x_shm.h"

#include <inttypes.h>
#include <sys/ipc.h>
#include <sys/shm.h>

void fi_opa1x_ep_tx_shm_init (struct fi_opa1x_ep *opa1x_ep) {
	int err = 0;

	int segment_fd = 0;
	void * segment_ptr = NULL;

	size_t segment_size = sizeof(fi_opa1x_shm_fifo_t);

	opa1x_ep->rx.shm.segment_ptr  = NULL;
	opa1x_ep->rx.shm.segment_size = 0;
	opa1x_ep->rx.shm.local_ticket = 0;

	snprintf(opa1x_ep->rx.shm.segment_key, FI_OPA1X_SHM_SEGMENT_NAME_MAX_LENGTH,
			 FI_OPA1X_SHM_SEGMENT_NAME_PREFIX "%s.%02x",
			 opa1x_ep->domain->unique_job_key_str,
			opa1x_ep->rx.self.hfi1_rx);
	segment_fd = shm_open(opa1x_ep->rx.shm.segment_key, O_RDWR | O_CREAT | O_EXCL, 0600);

	if (segment_fd == -1) {

		fprintf(stderr, "%s:%s():%d Unable to open shared memory segment '%s'. Perhaps a previous job crashed. An alternative is to use the FI_OPA1X_UUID environment variable\n", __FILE__, __func__, __LINE__, opa1x_ep->rx.shm.segment_key); abort();

		FI_LOG(fi_opa1x_global.prov, FI_LOG_DEBUG, FI_LOG_FABRIC,
			   "Cannot create shm object: %s errno=%s\n",
			   opa1x_ep->rx.shm.segment_key, strerror(errno));
		err = errno;
		goto err_out_close_ep;
	}

	errno = 0;

	if (ftruncate(segment_fd, segment_size) == -1) {
		FI_LOG(fi_opa1x_global.prov, FI_LOG_DEBUG, FI_LOG_FABRIC,
			   "Cannot set size of shm segment: %s errno=%s\n",
			   opa1x_ep->rx.shm.segment_key, strerror(errno));
		err = errno;
		goto err_out_close_ep;
	}

	errno = 0;

	segment_ptr = mmap(NULL, segment_size,
					   PROT_READ | PROT_WRITE, MAP_SHARED, segment_fd, 0);

	if (segment_ptr == MAP_FAILED) {
		FI_LOG(fi_opa1x_global.prov, FI_LOG_DEBUG, FI_LOG_FABRIC, "mmap failed: %s\n", strerror(errno));
		err = errno;
		goto err_out_close_ep;
	}

	close(segment_fd); /* safe to close now */

	memset(segment_ptr, 0, segment_size);
	/* memset(((fi_opa1x_shm_fifo_t *)segment_ptr)->guard0, 0xFF, 2048); */
	fi_opa1x_shm_compiler_barrier();

	opa1x_ep->rx.shm.segment_ptr  = segment_ptr;
	opa1x_ep->rx.shm.segment_size = segment_size;

	int i = 0;

	for (i = 0; i < FI_OPA1X_SHM_MAX_CONN_NUM; i++) {
		opa1x_ep->tx.shm.connections[i].segment_ptr  = NULL;
		opa1x_ep->tx.shm.connections[i].segment_size = 0;
		opa1x_ep->rx.tx.shm.connections[i].segment_ptr  = NULL;
		opa1x_ep->rx.tx.shm.connections[i].segment_size = 0;
	}

	fi_opa1x_shm_fifo_t * rx_fifo = opa1x_ep->rx.shm.segment_ptr;
	opa1x_ep->rx.shm_poll.next_packet_ptr = &rx_fifo->packets[opa1x_ep->rx.shm.local_ticket % FI_OPA1X_SHM_FIFO_SIZE];

	FI_LOG(fi_opa1x_global.prov, FI_LOG_DEBUG, FI_LOG_FABRIC,
		   "SHM initialization passed. Segment (%s), (%p)\n",
		   opa1x_ep->rx.shm.segment_key,
		   opa1x_ep->rx.shm.segment_ptr);

 normal_exit:

	return;

 err_out_close_ep:

	FI_LOG(fi_opa1x_global.prov, FI_LOG_DEBUG, FI_LOG_FABRIC,
		   "Initialization failed: %s\n", strerror(err));

	goto normal_exit;
}

void fi_opa1x_shm_tx_connect (struct fi_opa1x_ep *opa1x_ep, fi_addr_t peer) {
	int err = 0;

	int segment_fd = 0;
	void * segment_ptr = NULL;

	union fi_opa1x_addr addr;
	addr.raw64b = (uint64_t)peer;

	char segment_key[FI_OPA1X_SHM_SEGMENT_NAME_MAX_LENGTH];
	size_t segment_size = sizeof(fi_opa1x_shm_fifo_t);

	snprintf(segment_key, FI_OPA1X_SHM_SEGMENT_NAME_MAX_LENGTH,
			 FI_OPA1X_SHM_SEGMENT_NAME_PREFIX "%s.%02x",
			 opa1x_ep->domain->unique_job_key_str,
			addr.hfi1_rx);

	segment_fd = shm_open(segment_key, O_RDWR, 0600);

	if (segment_fd == -1) {
		FI_LOG(fi_opa1x_global.prov, FI_LOG_DEBUG, FI_LOG_FABRIC,
			   "Cannot open shm object: %s errno=%s\n", segment_key, strerror(errno));
		err = errno;
		goto err_out_close_ep;
	}

	errno = 0;

	segment_ptr = mmap(NULL, segment_size,
					   PROT_READ | PROT_WRITE, MAP_SHARED, segment_fd, 0);

	if (segment_ptr == MAP_FAILED) {
		FI_LOG(fi_opa1x_global.prov, FI_LOG_DEBUG, FI_LOG_FABRIC, "mmap failed: %s\n", strerror(errno));
		err = errno;
		goto err_out_close_ep;
	}

	close(segment_fd); /* safe to close now */

	opa1x_ep->tx.shm.connections[addr.hfi1_rx].segment_ptr  = segment_ptr;
	opa1x_ep->tx.shm.connections[addr.hfi1_rx].segment_size = segment_size;
	opa1x_ep->rx.tx.shm.connections[addr.hfi1_rx].segment_ptr  = segment_ptr;
	opa1x_ep->rx.tx.shm.connections[addr.hfi1_rx].segment_size = segment_size;

	FI_LOG(fi_opa1x_global.prov, FI_LOG_DEBUG, FI_LOG_FABRIC,
		   "SHM connection to %u context passed. Segment (%s), %d, (%p)\n",
		   addr.hfi1_rx, segment_key, segment_fd, segment_ptr);

 normal_exit:

	return;

 err_out_close_ep:

	FI_LOG(fi_opa1x_global.prov, FI_LOG_DEBUG, FI_LOG_FABRIC,
		   "Connection failed: %s\n", strerror(err));

	goto normal_exit;
}

void fi_opa1x_ep_tx_shm_finalize (struct fi_opa1x_ep *opa1x_ep) {

	unsigned int conn_id = 0;

	for (conn_id = 0; conn_id < FI_OPA1X_SHM_MAX_CONN_NUM; conn_id++) {
		if (opa1x_ep->tx.shm.connections[conn_id].segment_ptr != NULL) {
			munmap(opa1x_ep->tx.shm.connections[conn_id].segment_ptr,
				   opa1x_ep->tx.shm.connections[conn_id].segment_size);
		}
	}

	munmap(opa1x_ep->rx.shm.segment_ptr,
		   opa1x_ep->rx.shm.segment_size);
	
	shm_unlink(opa1x_ep->rx.shm.segment_key);

	return;
}

void fi_opa1x_shm_rx_rzv_rts (struct fi_opa1x_ep_rx *rx,
		const void * const hdr, const void * const payload,
		const uint8_t u8_rx, const uint64_t niov,
		uintptr_t origin_byte_counter_vaddr,
		uintptr_t target_byte_counter_vaddr,
		const uintptr_t dst_vaddr,
		const uintptr_t src_vaddr,
		const uint64_t nbytes_to_transfer,
		const unsigned is_intranode)
{
	/* write a 'rendezvous cts' shm packet */

	int global_ticket = 0;

	fi_opa1x_shm_fifo_t * tx_fifo = rx->tx.shm.connections[u8_rx].segment_ptr;

	global_ticket = fi_opa1x_shm_x86_atomic_fetch_and_add_int(&(tx_fifo->ticket), 1);

	fi_opa1x_shm_packet_t * packet_ptr = &tx_fifo->packets[global_ticket % FI_OPA1X_SHM_FIFO_SIZE];

	FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_EP_DATA,
		   "Injecting data over SHM to %u context (slot = %d)\n", u8_rx, global_ticket % FI_OPA1X_SHM_FIFO_SIZE);

	unsigned spin_count = 0;
	while (packet_ptr->is_busy) {
		/* Spin wait */
		fi_opa1x_shm_x86_pause();
				if (spin_count++ > 1000) {
					FI_WARN(fi_opa1x_global.prov, FI_LOG_EP_DATA, "shm fifo stuck!!!!!!!!!!!!\n");
					abort();
				}
	}

	union fi_opa1x_hfi1_packet_hdr * hfi1_packet_hdr =
		(union fi_opa1x_hfi1_packet_hdr *)&packet_ptr->hdr;

	hfi1_packet_hdr->stl.bth.opcode = FI_OPA1X_HFI_BTH_OPCODE_RZV_CTS;

	hfi1_packet_hdr->cts.origin_rx = rx->tx.cts.hdr.cts.origin_rx;
	hfi1_packet_hdr->cts.niov = niov;
	hfi1_packet_hdr->cts.origin_byte_counter_vaddr = origin_byte_counter_vaddr;
	hfi1_packet_hdr->cts.target_byte_counter_vaddr = target_byte_counter_vaddr;

	struct fi_opa1x_hfi1_dput_iov * dput_iov = (struct fi_opa1x_hfi1_dput_iov *)packet_ptr->payload;

	dput_iov->rbuf = dst_vaddr;
	dput_iov->sbuf = src_vaddr;
	dput_iov->bytes = nbytes_to_transfer;

	FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_EP_DATA,
		   "Filling packet for %u\n", u8_rx);

	fi_opa1x_shm_compiler_barrier();

	packet_ptr->is_busy = 1;

	FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_EP_DATA,
		   "Notifing %u\n", u8_rx);

}
