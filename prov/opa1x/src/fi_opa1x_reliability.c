
#ifndef likely
#define likely(x) __builtin_expect((x), 1)
#endif
#ifndef unlikely
#define unlikely(x) __builtin_expect((x), 0)
#endif

#include "rdma/opa1x/fi_opa1x_reliability.h"

#include "rdma/opa1x/fi_opa1x_compiler.h"
#include "rdma/opa1x/fi_opa1x_timer.h"
#include "rdma/opa1x/fi_opa1x_atomic_fifo.h"

#include "rdma/opa1x/fi_opa1x_shm.h"

#include "rbtree.h"

#include <pthread.h>
#include <unistd.h>	/* sleep */
#include <inttypes.h>

#include "rdma/opa1x/fi_opa1x_hfi1.h"

/* #define SKIP_RELIABILITY_PROTOCOL_RX_IMPL */
/* #define SKIP_RELIABILITY_PROTOCOL_TX_IMPL */

#include <execinfo.h>

#ifndef MIN
#define MIN(a,b) (b^((a^b)&-(a<b)))
#endif

/* #define DONT_BLOCK_REPLAY_ALLOCATE */

struct fi_opa1x_reliability_ticketlock {
	volatile int32_t next;
	volatile int32_t serving;
} __attribute__((__packed__));

static inline int32_t
fi_opa1x_reliability_x86_atomic_fetch_and_add_int(volatile int32_t * var, int32_t val) {

	__asm__ __volatile__ ("lock ; xadd %0,%1"
			      : "=r" (val), "=m" (*var)
			      :  "0" (val),  "m" (*var));
	return val;
}

static inline
void fi_opa1x_reliability_ticketlock_acquire (struct fi_opa1x_reliability_ticketlock * lock) {

	int32_t ticket = fi_opa1x_reliability_x86_atomic_fetch_and_add_int(&lock->next, 1);
	while (lock->serving != ticket);
}

static inline
void fi_opa1x_reliability_ticketlock_release (struct fi_opa1x_reliability_ticketlock * lock) {

	volatile int32_t * var = &lock->serving;
	__asm__ __volatile__ ("lock ; incl %0" :"=m" (*var) :"m" (*var));
};


static inline
void dump_backtrace () {

	fprintf(stderr, "==== BACKTRACE ====\n");
	void * addr[100];
	backtrace_symbols_fd(addr, backtrace(addr, 100), 2);
	fprintf(stderr, "==== BACKTRACE ====\n");
	

#if 0
	char ** names;
	names = backtrace_symbols(addr, count);

	fprintf(stderr, "got %zu stack frames\n", count);

	unsigned i;
	for (i=0; i<count; ++i) {
		fprintf(stderr, "  [%016p] %s\n", addr[i], names[i]);
	}

	free(names);
#endif
	return;
}

#define RX_CMD	(0x0000000000000008ul)
#define TX_CMD	(0x0000000000000010ul)

union fi_opa1x_reliability_service_flow_key {
	uint64_t		value;
	uint32_t		value32b[2];
	struct {
		uint32_t	slid	: 24;
		uint32_t	tx	:  8;
		uint32_t	dlid	: 24;
		uint32_t	rx	:  8;
	} __attribute__((__packed__));
};

struct fi_opa1x_reliability_rx_uepkt {
	struct fi_opa1x_reliability_rx_uepkt *	prev;
	struct fi_opa1x_reliability_rx_uepkt *	next;
	uint64_t				psn;
	uint64_t				unused_0[5];

	/* == CACHE LINE == */

	uint64_t				unused_1;
	union fi_opa1x_hfi1_packet_hdr		hdr;	/* 56 bytes */

	/* == CACHE LINE == */

	uint8_t					payload[0];

} __attribute__((__packed__));

struct fi_opa1x_reliability_flow {
	struct fi_opa1x_reliability_ticketlock		lock;
	uint64_t					next_psn;
	union fi_opa1x_reliability_service_flow_key	key;
	struct fi_opa1x_reliability_rx_uepkt *		uepkt;
};

struct fi_opa1x_reliability_service_range {
	uint64_t		begin;
	uint64_t		end;
};


#define FI_OPA1X_HFI_BTH_OPCODE_RELIABILITY_EGR		(0xFC)
#define FI_OPA1X_HFI_BTH_OPCODE_RELIABILITY_NACK	(0xFD)
#define FI_OPA1X_HFI_BTH_OPCODE_RELIABILITY_ACK		(0xFE)
#define FI_OPA1X_HFI_BTH_OPCODE_RELIABILITY_PING	(0xFF)

union fi_opa1x_reliability_service_hfi1_packet_hdr {

	uint64_t						qw[7];

	struct fi_opa1x_hfi1_stl_packet_hdr			stl;

	struct {
		/* == quadword 0 == */
		uint16_t					reserved_0[3];
		uint16_t					slid;

		/* == quadword 1 == */
		uint64_t					reserved_1;

		/* == quadword 2 == */
		uint32_t					range_count;		/* stl.bth.psn */
		uint8_t						origin_reliability_rx;	/* stl.kdeth.offset */
		uint8_t						reserved_2[3];

		/* == quadword 3 == */
		uint32_t					reserved_3;
		uint32_t					unused;			/* stl.kdeth.unused */

		/* == quadword 4,5,6 == */
		uint64_t					psn_count;
		uint64_t					psn_start;
		union fi_opa1x_reliability_service_flow_key	key;

	} __attribute__((__packed__)) reliability;
};

union fi_opa1x_reliability_service_hfi1_packet_payload {

	uint8_t						byte[10240];	/* max MTU size */

	struct fi_opa1x_reliability_service_range	range[10240/sizeof(struct fi_opa1x_reliability_service_range)];
};






struct fi_opa1x_reliability_tx_replay_internal {
	struct fi_opa1x_reliability_tx_replay_internal *next;
	struct fi_opa1x_reliability_tx_replay_internal *prev;
	volatile uint64_t				active;
	uint64_t					psn;

	union fi_opa1x_reliability_service_flow_key	key;		/* 8 bytes */
	uint16_t					rs;
	uint16_t					nack_count;
	uint32_t					unused_0;

	volatile uint64_t *				cc_ptr;
	uint64_t					cc_dec;

	/* --- MUST BE 64 BYTE ALIGNED --- */
	struct fi_opa1x_reliability_tx_replay		buffer;

} __attribute__((__packed__));

struct fi_opa1x_reliability_tx_replay_internal_large {
	struct fi_opa1x_reliability_tx_replay_internal	replay;
	uint64_t					payload[1024+8];
} __attribute__((__aligned__(64)));


union fi_opa1x_reliability_service_internal {
	struct fi_opa1x_reliability_service			service;

	struct {
		struct fi_opa1x_atomic_fifo			fifo;		/* 27 qws = 216 bytes */
		uint16_t					usec_max;
		uint8_t						fifo_max;
		uint8_t						hfi1_max;
		uint8_t						unused[5];

		struct {
			union fi_opa1x_timer_state		timer;		/*  2 qws =  16 bytes */
			RbtHandle				flow;		/*  1 qw  =   8 bytes */
			uint64_t				timestamp;

	/* == CACHE LINE == */

			struct {
				union fi_opa1x_hfi1_pio_state *	pio_state;
				volatile uint64_t *		pio_scb_sop_first;
				volatile uint64_t *		pio_credits_addr;
				volatile uint64_t *		pio_scb_first;
				struct fi_opa1x_hfi1_txe_scb	ping_model;	/* first 4 qws of this scb model are in 'CACHE LINE x' */
				struct fi_opa1x_hfi1_txe_scb	ack_model;	/* first 4 qws of this scb model are in 'CACHE LINE y' */
				struct fi_opa1x_hfi1_txe_scb	nack_model;	/* first 4 qws of this scb model are in 'CACHE LINE z' */

				uint64_t			unused_cacheline[4];
			} hfi1;
		} tx;

	/* == CACHE LINE == */

		struct {

			RbtHandle				flow;		/*  1 qw  =   8 bytes */
			struct {

				struct fi_opa1x_hfi1_rxe_state	state;		/*  2 qws =  16 bytes */

				struct {
					uint32_t *		rhf_base;
					volatile uint64_t *	head_register;
				} hdrq;

				/* -- not critical; can be moved to another cacheline */

				struct {
					uint32_t *		base_addr;
					uint32_t		elemsz;
					uint32_t		last_egrbfr_index;
					volatile uint64_t *	head_register;
				} egrq;
			} hfi1;
		} rx;

		struct fi_opa1x_hfi1_context *	context;
		volatile uint64_t		enabled;
		volatile uint64_t		active;
		pthread_t			thread;
		int				is_backoff_enabled;
		uint64_t			backoff_period;
	};
};

int fi_opa1x_reliability_compare (void *a, void *b) {

	const uintptr_t a_key = (uintptr_t)a;
	const uintptr_t b_key = (uintptr_t)b;

	if (a_key > b_key) return 1;
	if (a_key < b_key) return -1;

	return 0;
}








union fi_opa1x_reliability_tx_state_internal {

	struct fi_opa1x_reliability_tx_state	state;
	struct {
		struct {
			struct fi_opa1x_atomic_fifo_producer	fifo;
			struct fi_opa1x_reliability_tx_replay_internal_large *	large;
			uint64_t						head;
		} replay;

		struct {
			RbtHandle			rbtree;
			uint32_t *			value_memory;
			uint64_t			value_free_count;
		} psn;

		uint32_t				lid_be;
		uint64_t				tx;
	} __attribute__((__aligned__(64)));
};

/*
 * NOT THREAD-SAFE
 *
 * must acquire the flow lock, via fi_opa1x_reliability_ticketlock_acquire(),
 * before reading the uepkt queue.
 */
static inline
void dump_flow_rx (struct fi_opa1x_reliability_flow * flow, const int line) {

	const uint64_t key = flow->key.value;
	uint64_t next_psn = flow->next_psn;

	char debug[2048];
	char * str = debug;
	int size = sizeof(debug)-1;

	debug[0] = 0;
	if (flow->uepkt == NULL) {

		int c = snprintf(str, size, "(empty)");
		str += c;
		size -= c;

	} else {

		struct fi_opa1x_reliability_rx_uepkt * head = flow->uepkt;	/* read again now that queue is locked */

		int c = snprintf(str, size, "%08lu", head->psn);
		str += c;
		size -= c;

		uint64_t start_psn = head->psn;
		uint64_t stop_psn = start_psn;

		struct fi_opa1x_reliability_rx_uepkt * uepkt = head->next;
		while (uepkt != head) {
			if (uepkt->psn != (stop_psn + 1)) {

				if (start_psn != stop_psn) {
					c = snprintf(str, size, "..%08lu, %08lu", stop_psn, uepkt->psn);
				} else {
					c = snprintf(str, size, ", %08lu", uepkt->psn);
				}
				str += c;
				size -= c;

				start_psn = stop_psn = uepkt->psn;

			} else if (uepkt->next == head) {
				if (start_psn != uepkt->psn) {
					c = snprintf(str, size, "..%08lu", uepkt->psn);
				} else {
					c = snprintf(str, size, ", %08lu", uepkt->psn);
				}
				str += c;
				size -= c;

			} else {
				stop_psn++;
			}

			uepkt = uepkt->next;
		}

	}

	if (line) {
		fprintf(stderr, "flow__ %016lx (%d) next_psn = %lu, list: %s\n", key, line, next_psn, debug);
	} else {
		fprintf(stderr, "flow__ %016lx next_psn = %lu, list: %s\n", key, next_psn, debug);
	}
}


static inline
void dump_flow_list (uint64_t key, struct fi_opa1x_reliability_tx_replay_internal * head, int line) {

	char debug[2048];
	char * str = debug;
	int size = sizeof(debug)-1;

	debug[0] = 0;

	if (!head) {

		int c = snprintf(str, size, "(empty)");
		str += c;
		size -= c;

	} else {

		int c = snprintf(str, size, "%08lu", head->psn);
		str += c;
		size -= c;

		uint64_t next_psn = head->psn + 1;
		struct fi_opa1x_reliability_tx_replay_internal * replay = head->next;
		while (replay != head) {

			if (replay->psn == next_psn) {
				if (replay->next == head) {

					c = snprintf(str, size, "..%08lu", next_psn);
					str += c;
					size -= c;
				}
				next_psn += 1;
			} else {

				c = snprintf(str, size, "..%08lu, %08lu", next_psn, replay->psn);
				str += c;
				size -= c;
				next_psn = replay->psn + 1;
			}

			replay = replay->next;
		}
	}

	if (line) {
		fprintf(stderr, "flow__ %016lx (%d) list: %s\n", key, line, debug);
	} else {
		fprintf(stderr, "flow__ %016lx list: %s\n", key, debug);
	}
}


static inline
void fi_reliability_service_print_replay_ring (struct fi_opa1x_reliability_tx_replay_internal * head,
		const char * func, const int line) {

	fprintf(stderr, "%s():%d == head = %p\n", func, line, head);
	if (head == NULL) return;

	struct fi_opa1x_reliability_tx_replay_internal * tmp = head;

	do {
		fprintf(stderr, "%s():%d ==  ->    %p (p:%p, n:%p, psn:%lu)\n", func, line, tmp, tmp->prev, tmp->next, tmp->psn);
		tmp = tmp->next;
	} while (tmp != head);

	fprintf(stderr, "%s():%d == tail = %p\n", func, line, head->prev);

	return;
}

static inline
void fi_reliability_service_process_command (union fi_opa1x_reliability_service_internal * internal,
		struct fi_opa1x_reliability_tx_replay_internal * replay) { 

	const uintptr_t key = replay->key.value;

	void * itr = NULL;

#ifdef OPA1X_RELIABILITY_DEBUG
	fprintf(stderr, "(tx) packet %016lx %08lu posted.\n", key, replay->psn);
#endif

	/* search for existing unack'd flows */
	itr = rbtFind(internal->tx.flow, (void*)key);
	if (unlikely((itr == NULL))) {

		/* did not find an existing flow */
		replay->prev = replay;
		replay->next = replay;

		rbtInsert(internal->tx.flow, (void*)key, (void*)replay);

	} else {

		struct fi_opa1x_reliability_tx_replay_internal ** value_ptr =
			(struct fi_opa1x_reliability_tx_replay_internal **) rbtValuePtr(internal->tx.flow, itr);

		struct fi_opa1x_reliability_tx_replay_internal * head = *value_ptr;

		if (head == NULL) {

			/* the existing flow does not have any un-ack'd replay buffers */
			replay->prev = replay;
			replay->next = replay;
			*value_ptr = replay;

		} else {

			/* insert this replay at the end of the list */
			replay->prev = head->prev;
			replay->next = head;
			head->prev->next = replay;
			head->prev = replay;
		}
	}

	return;
}


static inline
void fi_opa1x_reliability_service_hfi1_inject_generic (union fi_opa1x_reliability_service_internal * service,
		const uint64_t key, const uint64_t dlid, const uint64_t reliability_rx,
		const uint64_t psn_start, const uint64_t psn_count,
		const uint64_t opcode) {

	union fi_opa1x_hfi1_pio_state pio_state;
	uint64_t * const pio_state_ptr = (uint64_t*)service->tx.hfi1.pio_state;
	pio_state.qw0 = *pio_state_ptr;

	FI_OPA1X_HFI1_UPDATE_CREDITS(pio_state, service->tx.hfi1.pio_credits_addr);
	if (!FI_OPA1X_HFI1_SINGLE_CREDIT_AVAILABLE(pio_state)) {

		/*
		 * no credits available
		 *
		 * DO NOT BLOCK - instead, drop this request and allow the
		 * reliability protocol to time out and retransmit
		 */
#ifdef OPA1X_RELIABILITY_DEBUG
		if (opcode == FI_OPA1X_HFI_BTH_OPCODE_RELIABILITY_PING) {
			fprintf(stderr, "(tx) flow__ %016lx inj ping dropped; no credits\n", key);
		} else if (opcode == FI_OPA1X_HFI_BTH_OPCODE_RELIABILITY_ACK) {
			fprintf(stderr, "(rx) flow__ %016lx inj ack dropped; no credits\n", key);
		} else if (opcode == FI_OPA1X_HFI_BTH_OPCODE_RELIABILITY_NACK) {
			fprintf(stderr, "(rx) flow__ %016lx inj nack dropped; no credits\n", key);
		} else {
			fprintf(stderr, "%s:%s():%d bad opcode (%lu) .. abort\n", __FILE__, __func__, __LINE__, opcode);
		}
#endif
		return;
	}
#ifdef OPA1X_RELIABILITY_DEBUG
//	fprintf(stderr, "%s():%d psn_start = %lu, psn_count = %lu, opcode = %lu\n", __func__, __LINE__, psn_start, psn_count, opcode);

	const uint64_t psn_stop = psn_start + psn_count - 1;

	if (psn_start > psn_stop) {
		if (opcode == FI_OPA1X_HFI_BTH_OPCODE_RELIABILITY_PING) {
			fprintf(stderr, "%s:%s():%d (%016lx) invalid inject ping; psn_start = %lu, psn_count = %lu, psn_stop = %lu\n", __FILE__, __func__, __LINE__, key, psn_start, psn_count, psn_stop);
		} else if (opcode == FI_OPA1X_HFI_BTH_OPCODE_RELIABILITY_ACK) {
			fprintf(stderr, "%s:%s():%d (%016lx) invalid inject ack; psn_start = %lu, psn_count = %lu, psn_stop = %lu\n", __FILE__, __func__, __LINE__, key, psn_start, psn_count, psn_stop);
		} else if (opcode == FI_OPA1X_HFI_BTH_OPCODE_RELIABILITY_NACK) {
			fprintf(stderr, "%s:%s():%d (%016lx) invalid inject nack; psn_start = %lu, psn_count = %lu, psn_stop = %lu\n", __FILE__, __func__, __LINE__, key, psn_start, psn_count, psn_stop);
		} else {
			fprintf(stderr, "%s:%s():%d bad opcode (%lu) .. abort\n", __FILE__, __func__, __LINE__, opcode);
		}
		abort();
	}

	if (opcode == FI_OPA1X_HFI_BTH_OPCODE_RELIABILITY_PING) {
		fprintf(stderr, "(tx) flow__ %016lx inj ping %08lu..%08lu\n", key, psn_start, psn_stop);
	} else if (opcode == FI_OPA1X_HFI_BTH_OPCODE_RELIABILITY_ACK) {
		fprintf(stderr, "(rx) flow__ %016lx inj ack %08lu..%08lu\n", key, psn_start, psn_stop);
	} else if (opcode == FI_OPA1X_HFI_BTH_OPCODE_RELIABILITY_NACK) {
		fprintf(stderr, "(rx) flow__ %016lx inj nack %08lu..%08lu\n", key, psn_start, psn_stop);
	} else {
		fprintf(stderr, "%s:%s():%d bad opcode (%lu) .. abort\n", __FILE__, __func__, __LINE__, opcode);
	}
#endif

	volatile uint64_t * const scb =
		FI_OPA1X_HFI1_PIO_SCB_HEAD(service->tx.hfi1.pio_scb_sop_first, pio_state);

	const uint64_t lrh_dlid = dlid << 16;
	const uint64_t bth_rx = reliability_rx << 56;

	const struct fi_opa1x_hfi1_txe_scb * const model =	/* constant compile-time expression */
			opcode == FI_OPA1X_HFI_BTH_OPCODE_RELIABILITY_PING ?
				&service->tx.hfi1.ping_model :
				( opcode == FI_OPA1X_HFI_BTH_OPCODE_RELIABILITY_ACK ?
					&service->tx.hfi1.ack_model :
					&service->tx.hfi1.nack_model );

	//uint64_t tmp[8];
	//tmp[0] =
		scb[0] = model->qw0;
	//tmp[1] =
		scb[1] = model->hdr.qw[0] | lrh_dlid;
	//tmp[2] =
		scb[2] = model->hdr.qw[1] | bth_rx;
	//tmp[3] =
		scb[3] = model->hdr.qw[2];
	//tmp[4] =
		scb[4] = model->hdr.qw[3];
	//tmp[5] =
		scb[5] = psn_count;				/* reliability.psn_count */
	//tmp[6] =
		scb[6] = psn_start;				/* reliability.psn_start */
	//tmp[7] =
		scb[7] = key;					/* reliability.key */

	//fprintf(stderr, "%s():%d pbc: 0x%016lx\n", __func__, __LINE__, tmp[0]);
	//fi_opa1x_hfi1_dump_stl_packet_hdr((struct fi_opa1x_hfi1_stl_packet_hdr *)&tmp[1], __func__, __LINE__);

	fi_opa1x_compiler_msync_writes();

	FI_OPA1X_HFI1_CHECK_CREDITS_FOR_ERROR((service->tx.hfi1.pio_credits_addr));

	/* consume one credit for the packet header */
	FI_OPA1X_HFI1_CONSUME_SINGLE_CREDIT(pio_state);

	/* save the updated txe state */
	*pio_state_ptr = pio_state.qw0;
}


static inline
void fi_opa1x_reliability_service_hfi1_receive_ping (union fi_opa1x_reliability_service_internal * service,
	const union fi_opa1x_reliability_service_hfi1_packet_hdr * const hdr) {

	//fprintf(stderr, "%s():%d hdr->reliability.key = %016lx\n", __func__, __LINE__, hdr->reliability.key.value);
	const uint64_t key = hdr->reliability.key.value;
	const uint64_t dlid = (uint64_t)hdr->stl.lrh.slid;
	const uint64_t rx = (uint64_t)hdr->reliability.origin_reliability_rx;
	//const uint64_t npkts_tx = hdr->reliability.psn_count;
	//const uint64_t last_psn_tx = npkts_tx - 1;

#ifdef OPA1X_RELIABILITY_DEBUG
	fprintf(stderr, "(rx) flow__ %016lx rcv ping %08lu..%08lu\n", key, hdr->reliability.psn_start, hdr->reliability.psn_start + npkts_tx - 1);
#endif
	void * itr = NULL;
	itr = rbtFind(service->rx.flow, (void*)key);

	if (unlikely((itr == NULL))) {

		/* did not find this flow .... send NACK for psn 0 */
		fi_opa1x_reliability_service_hfi1_inject_generic(service,
				key, dlid, rx,
				0,	/* psn_start */
				1,	/* psn_count */
				FI_OPA1X_HFI_BTH_OPCODE_RELIABILITY_NACK);
		return;
	}

	struct fi_opa1x_reliability_flow ** value_ptr =
		(struct fi_opa1x_reliability_flow **) rbtValuePtr(service->rx.flow, itr);

	struct fi_opa1x_reliability_flow * flow = *value_ptr;

	uint64_t ping_psn_count = hdr->reliability.psn_count;
	uint64_t ping_start_psn = hdr->reliability.psn_start;
	const uint64_t ping_stop_psn = ping_start_psn + ping_psn_count - 1;

	struct fi_opa1x_reliability_service_range ping;
	ping.begin = ping_start_psn;
	ping.end = ping_stop_psn;


	if (likely(flow->uepkt == NULL)) {

		/* fast path - no unexpected packets were received */

		//uint64_t ack_start_psn = 0;
		uint64_t ack_stop_psn = flow->next_psn - 1;

//		fprintf(stderr, "%s():%d ping = (%lu, %lu, %lu), ack = (%lu, %lu, %lu)\n", __func__, __LINE__,
//			ping_start_psn, ping_stop_psn, ping_psn_count,
//			ack_start_psn, ack_stop_psn, ack_stop_psn - ack_start_psn + 1);

		if (ping_start_psn <= ack_stop_psn) {

			/* need to ack some, or all, packets in the range
			 * requested by the ping */

			const uint64_t ack_count = ack_stop_psn - ping_start_psn + 1;

//			fprintf(stderr, "%s():%d fast first ack; ping_start_psn = %lu, ack_count = %lu\n", __func__, __LINE__, ping_start_psn, ack_count);

			fi_opa1x_reliability_service_hfi1_inject_generic(service,
					key, dlid, rx,
					ping_start_psn,		/* psn_start */
					ack_count,		/* psn_count */
					FI_OPA1X_HFI_BTH_OPCODE_RELIABILITY_ACK);

			uint64_t update_count = MIN(ack_count, ping_psn_count);	/* do not underflow 'ping_psn_count' */

			ping_start_psn += update_count;
			ping_psn_count -= update_count;
		}


//		fprintf(stderr, "%s():%d ping = (%lu, %lu, %lu)\n", __func__, __LINE__,
//			ping_start_psn, ping_stop_psn, ping_psn_count);

		if (ping_psn_count > 0) {

			/* no unexpected packets have been received; nack the remaining
			 * portion of the range requested by the ping and return */

//			fprintf(stderr, "%s():%d first (and last) nack; ping_start_psn = %lu, ping_psn_count = %lu\n",
//				__func__, __LINE__, ping_start_psn, ping_psn_count);

			fi_opa1x_reliability_service_hfi1_inject_generic(service,
					key, dlid, rx,
					ping_start_psn,		/* psn_start */
					ping_psn_count,		/* psn_count */
					FI_OPA1X_HFI_BTH_OPCODE_RELIABILITY_NACK);
		}

//		fprintf(stderr, "%s():%d fast ping response done\n", __func__, __LINE__);
		return;
	}














	fi_opa1x_reliability_ticketlock_acquire(&flow->lock);			/* LOCK */

	fi_opa1x_compiler_msync_reads();


	const uint64_t flow_next_psn = flow->next_psn;

	/*dump_flow_rx(flow, __LINE__);*/


	/*
	 * odd index == nack range
	 * even index == ack range
	 */

	const unsigned range_max = 10;
	struct fi_opa1x_reliability_service_range range[range_max];

	unsigned range_count = 1;

	/* initial ack range */
	range[0].begin = 0;
	range[0].end = flow_next_psn - 1;

	const struct fi_opa1x_reliability_rx_uepkt * const head = flow->uepkt;	/* read head again now that queue is locked; avoid race */


//	fprintf(stderr, "%s():%d ping = (%lu, %lu), range[0] = (%lu, %lu), range_count = %u\n", __func__, __LINE__,
//			ping.begin, ping.end, range[0].begin, range[0].end, range_count);

	if (head == NULL) {

		range_count = 2;
		range[1].begin = flow_next_psn;
		range[1].end = (uint64_t)-1;

//		fprintf(stderr, "%s():%d ping = (%lu, %lu), range[%u] = (%lu, %lu), range_count = %u\n", __func__, __LINE__,
//				ping.begin, ping.end, range_count-1, range[range_count-1].begin, range[range_count-1].end, range_count);
	} else {

		struct fi_opa1x_reliability_rx_uepkt * uepkt =
			(struct fi_opa1x_reliability_rx_uepkt *) head;

		/* initial nack range */
		range[range_count].begin = range[range_count-1].end + 1;
		range[range_count].end = uepkt->psn - 1;
		range_count++;
//		fprintf(stderr, "%s():%d ping = (%lu, %lu), range[%u] = (%lu, %lu), range_count = %u\n", __func__, __LINE__,
//				ping.begin, ping.end, range_count-1, range[range_count-1].begin, range[range_count-1].end, range_count);

		/* start next ack range */
		range[range_count].begin = uepkt->psn;
		range[range_count].end = uepkt->psn;
		uepkt = uepkt->next;
//		fprintf(stderr, "%s():%d ping = (%lu, %lu), range[%u] = (%lu, %lu), range_count = %u, uepkt = %p, head = %p\n", __func__, __LINE__,
//				ping.begin, ping.end, range_count, range[range_count].begin, range[range_count].end, range_count, uepkt, head);

		while ((uepkt != head) && (range_count < range_max)) {

			if (uepkt->psn == (range[range_count].end + 1)) {
				range[range_count].end++;
//				fprintf(stderr, "%s():%d ping = (%lu, %lu), range[%u] = (%lu, %lu), range_count = %u, uepkt = %p, head = %p\n", __func__, __LINE__,
//						ping.begin, ping.end, range_count, range[range_count].begin, range[range_count].end, range_count, uepkt, head);
			} else {
				/* nack range */
				range_count++;
				range[range_count].begin = range[range_count-1].end + 1;
				range[range_count].end = uepkt->psn - 1;

//				fprintf(stderr, "%s():%d ping = (%lu, %lu), range[%u] = (%lu, %lu), range_count = %u, range_max = %u\n", __func__, __LINE__,
//						ping.begin, ping.end, range_count, range[range_count].begin, range[range_count].end, range_count, range_max);

				if (range_count < range_max) {
					/* start next ack range */
					range_count++;
					range[range_count].begin = uepkt->psn;
					range[range_count].end = uepkt->psn;
//					fprintf(stderr, "%s():%d ping = (%lu, %lu), range[%u] = (%lu, %lu), range_count = %u, range_max = %u\n", __func__, __LINE__,
//							ping.begin, ping.end, range_count, range[range_count].begin, range[range_count].end, range_count, range_max);
				}
			}
			uepkt = uepkt->next;
		}

		range_count++;

//		fprintf(stderr, "%s():%d range_count = %u, range_max = %u, uepkt = %p, head = %p\n", __func__, __LINE__,
//				range_count, range_max, uepkt, head);

		if ((uepkt == head) && (range_count < range_max)) {

			/* tail nack range */
			range[range_count].begin = range[range_count-1].end + 1;
			range[range_count].end = (uint64_t)-1;
			range_count++;
//			fprintf(stderr, "%s():%d ping = (%lu, %lu), range[%u] = (%lu, %lu), range_count = %u, range_max = %u\n", __func__, __LINE__,
//					ping.begin, ping.end, range_count, range[range_count].begin, range[range_count].end, range_count, range_max);
		}
	}

	fi_opa1x_reliability_ticketlock_release(&flow->lock);			/* UNLOCK */

	/* first ack range begins at psn 0 */
	unsigned index = 0;
	uint64_t ping_count = ping.end - ping.begin + 1;


//	fprintf(stderr, "%s():%d ping = (%lu, %lu), index = %u, ping_count = %lu, range_count = %u\n", __func__, __LINE__,
//			ping.begin, ping.end, index, ping_count, range_count);
	

	while ((ping_count > 0) && (index < range_count)) {

//		fprintf(stderr, "%s():%d ping = (%lu, %lu), range[%u] = (%lu, %lu)\n", __func__, __LINE__,
//				ping.begin, ping.end, index, range[index].begin, range[index].end);

		if (ping.begin <= range[index].end) {
			const uint64_t start = ping.begin;
			const uint64_t stop = MIN(ping.end, range[index].end);
			const uint64_t count = stop - start + 1;

			if ((index & 0x01u) == 0) {

				/* even index == ack */
//				fprintf(stderr, "%s():%d ack = (%lu, %lu), count = %lu\n", __func__, __LINE__, start, stop, count);

				fi_opa1x_reliability_service_hfi1_inject_generic(service,
						key, dlid, rx,
						start,		/* psn_start */
						count,		/* psn_count */
						FI_OPA1X_HFI_BTH_OPCODE_RELIABILITY_ACK);
			} else {

				/* odd index == nack */
//				fprintf(stderr, "%s():%d nack = (%lu, %lu), count = %lu\n", __func__, __LINE__, start, stop, count);

				fi_opa1x_reliability_service_hfi1_inject_generic(service,
						key, dlid, rx,
						start,		/* psn_start */
						count,		/* psn_count */
						FI_OPA1X_HFI_BTH_OPCODE_RELIABILITY_NACK);
			}

			ping.begin += count;
			ping_count -= count;
		}

		index++;
	}

//	fprintf(stderr, "%s():%d ping response done\n", __func__, __LINE__);

	return;
}

static inline
void fi_opa1x_reliability_service_hfi1_receive_ack (union fi_opa1x_reliability_service_internal * service,
	const union fi_opa1x_reliability_service_hfi1_packet_hdr * const hdr) {

	const uint64_t key = hdr->reliability.key.value;
	const uint64_t psn_count = hdr->reliability.psn_count;
	const uint64_t start_psn = hdr->reliability.psn_start;
	const uint64_t stop_psn = start_psn + psn_count - 1;

	//fprintf(stderr, "%s():%d key = %016lx, start_psn = %lu, psn_count = %lu, stop_psn = %lu\n", __func__, __LINE__, key, start_psn, psn_count, stop_psn);

#ifdef OPA1X_RELIABILITY_DEBUG
	fprintf(stderr, "(tx) flow__ %016lx rcv ack  %08lu..%08lu\n", key, start_psn, stop_psn);
#endif
	void * itr = NULL;

	/* search for existing unack'd flows */
	itr = rbtFind(service->tx.flow, (void*)key);
	if (likely((itr != NULL))) {

		struct fi_opa1x_reliability_tx_replay_internal ** value_ptr =
			(struct fi_opa1x_reliability_tx_replay_internal **) rbtValuePtr(service->tx.flow, itr);

		struct fi_opa1x_reliability_tx_replay_internal * head = *value_ptr;

//fprintf(stderr, "%s():%d psn_count = %lu, start_psn = %lu, stop_psn = %lu\n", __func__, __LINE__, psn_count, start_psn, stop_psn);
		if (unlikely(head == NULL)) {

//fprintf(stderr, "%s():%d\n", __func__, __LINE__);
			/*
			 * there are no unack'd elements in the replay queue;
			 * do nothing and return
			 */
			return;
		}

		struct fi_opa1x_reliability_tx_replay_internal * tail = head->prev;

		/*
		 * check for "fast path" - retire all elements in the queue
		 */
//fprintf(stderr, "%s():%d head = %p, head->psn = %lu\n", __func__, __LINE__, head, head->psn);
//fprintf(stderr, "%s():%d tail = %p, tail->psn = %lu\n", __func__, __LINE__, tail, tail->psn);
		if ((head->psn >= start_psn) && (tail->psn <= stop_psn)) {

			/* retire all queue elements */
			*value_ptr = NULL;
//fprintf(stderr, "%s():%d *value_ptr = %p\n", __func__, __LINE__, *value_ptr);

			struct fi_opa1x_reliability_tx_replay_internal * next = NULL;
			struct fi_opa1x_reliability_tx_replay_internal * tmp = head;

			do {
//fprintf(stderr, "%s():%d tmp = %p\n", __func__, __LINE__, tmp);
#ifdef OPA1X_RELIABILITY_DEBUG
				fprintf(stderr, "(tx) packet %016lx %08lu retired.\n", key, tmp->psn);
#endif
				next = tmp->next;

				const uint64_t dec = tmp->cc_dec;
				volatile uint64_t * cc_ptr = tmp->cc_ptr;
				*cc_ptr -= dec;

				tmp->active = 0;
				tmp = next;

			} while (tmp != head);
//fprintf(stderr, "%s():%d\n", __func__, __LINE__);

			return;
		}

		/*
		 * find the first replay to ack
		 */

		struct fi_opa1x_reliability_tx_replay_internal * start = head;
//fprintf(stderr, "%s():%d head = %p, head->psn = %lu\n", __func__, __LINE__, head, head->psn);
//fprintf(stderr, "%s():%d tail = %p, tail->psn = %lu\n", __func__, __LINE__, tail, tail->psn);
//fprintf(stderr, "%s():%d start = %p, start->psn = %lu\n", __func__, __LINE__, start, start->psn);
		while ((start->psn < start_psn) && (start != tail)) {
//fprintf(stderr, "%s():%d start = %p, start->psn = %lu\n", __func__, __LINE__, start, start->psn);
			start = start->next;
		}

//fprintf(stderr, "%s():%d start = %p, start->psn = %lu\n", __func__, __LINE__, start, start->psn);
		if (unlikely(start->psn < start_psn)) {

//fprintf(stderr, "%s():%d\n", __func__, __LINE__);
			/*
			 * all elements in replay queue are 'younger' than the
			 * first psn to retire; do nothing and return
			 */
			return;
		}

		/*
		 * find the last replay to ack
		 */

		struct fi_opa1x_reliability_tx_replay_internal * stop = start;
//fprintf(stderr, "%s():%d head  = %p, head->psn  = %lu\n", __func__, __LINE__, head, head->psn);
//fprintf(stderr, "%s():%d tail  = %p, tail->psn  = %lu\n", __func__, __LINE__, tail, tail->psn);
//fprintf(stderr, "%s():%d start = %p, start->psn = %lu\n", __func__, __LINE__, start, start->psn);
//fprintf(stderr, "%s():%d stop  = %p, stop->psn  = %lu\n", __func__, __LINE__, stop, stop->psn);
		while ((stop->next != head) && (stop->next->psn <= stop_psn)) {
//fprintf(stderr, "%s():%d stop  = %p, stop->psn  = %lu\n", __func__, __LINE__, stop, stop->psn);
			stop = stop->next;
		}
//fprintf(stderr, "%s():%d stop  = %p, stop->psn  = %lu\n", __func__, __LINE__, stop, stop->psn);

		if (unlikely(stop->psn > stop_psn)) {

//fprintf(stderr, "%s():%d\n", __func__, __LINE__);
			/*
			 * all elements in the replay queue are 'older' than the
			 * last psn to retire; do nothing an return
			 */
			return;
		}


		const struct fi_opa1x_reliability_tx_replay_internal * const halt = stop->next;

		if (start == head) {
			*value_ptr = (struct fi_opa1x_reliability_tx_replay_internal *)halt;
		}
//fprintf(stderr, "%s():%d *value_ptr = %p\n", __func__, __LINE__, *value_ptr);

		/* remove the psn range to ack from the queue */
		start->prev->next = stop->next;
		stop->next->prev = start->prev;

		/*
		 * retire all replay packets between start and stop, inclusive
		 */

		struct fi_opa1x_reliability_tx_replay_internal * tmp = start;
		do {
//fprintf(stderr, "%s():%d tmp = %p\n", __func__, __LINE__, tmp);
#ifdef OPA1X_RELIABILITY_DEBUG
			fprintf(stderr, "(tx) packet %016lx %08lu retired.\n", key, tmp->psn);
#endif
			struct fi_opa1x_reliability_tx_replay_internal * next = tmp->next;

			const uint64_t dec = tmp->cc_dec;
			volatile uint64_t * cc_ptr = tmp->cc_ptr;
			*cc_ptr -= dec;

			tmp->active = 0;
			tmp = next;

		} while (tmp != halt);
//fprintf(stderr, "%s():%d\n", __func__, __LINE__);
	}
//fprintf(stderr, "%s():%d\n", __func__, __LINE__);

	fi_opa1x_compiler_msync_writes();
}



static inline
void fi_opa1x_reliability_service_do_replay (union fi_opa1x_reliability_service_internal * service,
		struct fi_opa1x_reliability_tx_replay * replay) {

	/* reported in LRH as the number of 4-byte words in the packet; header + payload + icrc */
	const uint16_t lrh_pktlen_le = ntohs(replay->scb.hdr.stl.lrh.pktlen);
	const size_t total_bytes_to_copy = (lrh_pktlen_le - 1) * 4;	/* do not copy the trailing icrc */
	const size_t payload_bytes_to_copy = total_bytes_to_copy - sizeof(union fi_opa1x_hfi1_packet_hdr);

	uint16_t payload_credits_needed =
		(payload_bytes_to_copy >> 6) +				/* number of full 64-byte blocks of payload */
		((payload_bytes_to_copy & 0x000000000000003Ful) != 0);	/* number of partial 64-byte blocks of payload */

	union fi_opa1x_hfi1_pio_state pio_state;
	uint64_t * const pio_state_ptr = (uint64_t*)service->tx.hfi1.pio_state;
	pio_state.qw0 = *pio_state_ptr;

	FI_OPA1X_HFI1_UPDATE_CREDITS(pio_state, service->tx.hfi1.pio_credits_addr);

#ifdef OPA1X_RELIABILITY_DEBUG
	union fi_opa1x_reliability_service_flow_key key;
	key.slid = (uint32_t)replay->scb.hdr.stl.lrh.slid;
	key.tx = (uint32_t)replay->scb.hdr.reliability.origin_tx;
	key.dlid = (uint32_t)replay->scb.hdr.stl.lrh.dlid;
	key.rx = (uint32_t)replay->scb.hdr.stl.bth.rx;

	//fprintf(stderr, "%s():%d FI_OPA1X_HFI1_AVAILABLE_CREDITS(pio_state) = %u, payload_credits_needed = %u\n", __func__, __LINE__, FI_OPA1X_HFI1_AVAILABLE_CREDITS(pio_state), payload_credits_needed);
#endif


	/*
	 * if not enough credits are available, spin a few time and wait for
	 * more credits to free up. if the replay has 8192 bytes of payload
	 * then it will need 129 credits in total, but the total number of
	 * credits is around 160.
	 *
	 * it is ok to pause, it is not ok to block
	 */
	const uint16_t total_credits_needed = payload_credits_needed + 1;
	uint16_t total_credits_available = FI_OPA1X_HFI1_AVAILABLE_CREDITS(pio_state);
	unsigned loop = 0;
	while ((total_credits_available < total_credits_needed) && (loop++ < 1000)) {
		FI_OPA1X_HFI1_UPDATE_CREDITS(pio_state, service->tx.hfi1.pio_credits_addr);
		total_credits_available = FI_OPA1X_HFI1_AVAILABLE_CREDITS(pio_state);
	}

	if (FI_OPA1X_HFI1_AVAILABLE_CREDITS(pio_state) < total_credits_needed) {

		/*
		 * not enough credits available
		 *
		 * DO NOT BLOCK - instead, drop this request and allow the
		 * reliability protocol to time out and try again
		 */
#ifdef OPA1X_RELIABILITY_DEBUG
		fprintf(stderr, "(tx) packet %016lx %08u replay dropped (no credits)\n", key.value, (uint32_t)replay->scb.hdr.reliability.psn);
#endif
		return;
	}

#ifdef OPA1X_RELIABILITY_DEBUG
	fprintf(stderr, "(tx) packet %016lx %08u replay injected\n", key.value, (uint32_t)replay->scb.hdr.reliability.psn);
#endif

	volatile uint64_t * const scb =
		FI_OPA1X_HFI1_PIO_SCB_HEAD(service->tx.hfi1.pio_scb_sop_first, pio_state);

	//uint64_t tmp[8];
	//tmp[0] =
		scb[0] = replay->scb.qw0;
	//tmp[1] =
		scb[1] = replay->scb.hdr.qw[0];
	//tmp[2] =
		scb[2] = replay->scb.hdr.qw[1];
	//tmp[3] =
		scb[3] = replay->scb.hdr.qw[2];
	//tmp[4] =
		scb[4] = replay->scb.hdr.qw[3];
	//tmp[5] =
		scb[5] = replay->scb.hdr.qw[4];
	//tmp[6] =
		scb[6] = replay->scb.hdr.qw[5];
	//tmp[7] =
		scb[7] = replay->scb.hdr.qw[6];

	//fprintf(stderr, "%s():%d pbc: 0x%016lx\n", __func__, __LINE__, tmp[0]);
	//fi_opa1x_hfi1_dump_stl_packet_hdr((struct fi_opa1x_hfi1_stl_packet_hdr *)&tmp[1], __func__, __LINE__);

	fi_opa1x_compiler_msync_writes();

	FI_OPA1X_HFI1_CHECK_CREDITS_FOR_ERROR((service->tx.hfi1.pio_credits_addr));
//fi_opa1x_hfi1_check_credits_for_error(service->tx.hfi1.pio_credits_addr, __FILE__, __func__, __LINE__);

	/* consume one credit for the packet header */
	--total_credits_available;
	FI_OPA1X_HFI1_CONSUME_SINGLE_CREDIT(pio_state);

	uint64_t * buf_qws = replay->payload;

	while (payload_credits_needed > 0) {

		volatile uint64_t * scb_payload =
			FI_OPA1X_HFI1_PIO_SCB_HEAD(service->tx.hfi1.pio_scb_first, pio_state);

		const uint16_t contiguous_scb_until_wrap =
			(uint16_t)(pio_state.credits_total - pio_state.scb_head_index);

		const uint16_t contiguous_credits_available =
			MIN(total_credits_available, contiguous_scb_until_wrap);

		const uint16_t contiguous_full_blocks_to_write =
			MIN(payload_credits_needed, contiguous_credits_available);

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
//fi_opa1x_compiler_msync_writes();

//FI_OPA1X_HFI1_CHECK_CREDITS_FOR_ERROR((service->tx.hfi1.pio_credits_addr));
//fi_opa1x_hfi1_check_credits_for_error(service->tx.hfi1.pio_credits_addr, __FILE__, __func__, __LINE__);

		payload_credits_needed -= contiguous_full_blocks_to_write;
		total_credits_available -= contiguous_full_blocks_to_write;
		FI_OPA1X_HFI1_CONSUME_CREDITS(pio_state, contiguous_full_blocks_to_write);
	}

	/* save the updated txe state */
	*pio_state_ptr = pio_state.qw0;

	fi_opa1x_compiler_msync_writes();
}


static inline
void fi_opa1x_reliability_service_hfi1_receive_nack (union fi_opa1x_reliability_service_internal * service,
	const union fi_opa1x_reliability_service_hfi1_packet_hdr * const hdr) {

	const uint64_t key = hdr->reliability.key.value;
	const uint64_t psn_count = hdr->reliability.psn_count;
	const uint64_t start_psn = hdr->reliability.psn_start;
	const uint64_t stop_psn = start_psn + psn_count - 1;

	uint64_t inject_count = 0;
	const uint64_t inject_max = 10;

	if (start_psn > stop_psn) {
		fprintf(stderr, "%s:%s():%d (%016lx) invalid nack received; start_psn = %lu, psn_count = %lu, stop_psn = %lu\n",
			__FILE__, __func__, __LINE__, key, start_psn, psn_count, stop_psn);
		abort();
	}


#ifdef OPA1X_RELIABILITY_DEBUG
	fprintf(stderr, "(tx) flow__ %016lx rcv nack %08lu..%08lu\n", key, start_psn, stop_psn);
#endif
	void * itr = NULL;

	/* search for existing unack'd flows */
	itr = rbtFind(service->tx.flow, (void*)key);
	if (unlikely((itr == NULL))) {

		/*
		 * the flow identified by the key is invalid ...?
		 */
		fprintf(stderr, "%s:%s():%d invalid key (%016lx)\n", __FILE__, __func__, __LINE__, key);
		abort();
	}

	struct fi_opa1x_reliability_tx_replay_internal ** value_ptr =
		(struct fi_opa1x_reliability_tx_replay_internal **) rbtValuePtr(service->tx.flow, itr);

	struct fi_opa1x_reliability_tx_replay_internal * head = *value_ptr;

//fprintf(stderr, "%s():%d head = %p\n", __func__, __LINE__, head);
	if (unlikely(head == NULL)) {
//fprintf(stderr, "%s():%d head = %p\n", __func__, __LINE__, head);

		/*
		 * there are no unack'd elements in the replay queue;
		 * do nothing and return
		 */
		return;
	}

	struct fi_opa1x_reliability_tx_replay_internal * tail = head->prev;

	/*
	 * find the first replay to retransmit
	 */

	struct fi_opa1x_reliability_tx_replay_internal * start = head;
//fprintf(stderr, "%s():%d tail = %p, start = %p, start->psn = %lu, start_psn = %lu\n", __func__, __LINE__, tail, start, start->psn, start_psn);
	while ((start->psn < start_psn) && (start != tail)) {
//fprintf(stderr, "%s():%d tail = %p, start = %p, start->psn = %lu, start_psn = %lu\n", __func__, __LINE__, tail, start, start->psn, start_psn);
		start = start->next;
	}
//fprintf(stderr, "%s():%d tail = %p, start = %p, start->psn = %lu, start_psn = %lu\n", __func__, __LINE__, tail, start, start->psn, start_psn);

	if (unlikely(start->psn < start_psn)) {

		/*
		 * all elements in replay queue are 'younger' than the
		 * first psn to retransmit; do nothing and return
		 */
//fprintf(stderr, "%s():%d\n", __func__, __LINE__);
		return;
	}

	/*
	 * find the last replay to retransmit
	 */

	struct fi_opa1x_reliability_tx_replay_internal * stop = start;
//fprintf(stderr, "%s():%d head = %p, stop = %p, stop->next = %p, start->next->psn = %lu, stop_psn = %lu\n", __func__, __LINE__, head, stop, stop->next, stop->next->psn, stop_psn);
	while ((stop->next != head) && (stop->next->psn <= stop_psn)) {
//fprintf(stderr, "%s():%d head = %p, stop = %p, stop->next = %p, start->next->psn = %lu, stop_psn = %lu\n", __func__, __LINE__, head, stop, stop->next, stop->next->psn, stop_psn);
		stop = stop->next;
	}
//fprintf(stderr, "%s():%d head = %p, stop = %p, stop->next = %p, start->next->psn = %lu, stop_psn = %lu\n", __func__, __LINE__, head, stop, stop->next, stop->next->psn, stop_psn);

	if (unlikely(stop->psn > stop_psn)) {

		/*
		 * all elements in the replay queue are 'older' than the
		 * last psn to retransmit; do nothing an return
		 */
//fprintf(stderr, "%s():%d\n", __func__, __LINE__);
		return;
	}

	const struct fi_opa1x_reliability_tx_replay_internal * const halt = stop->next;
	struct fi_opa1x_reliability_tx_replay_internal * replay = start;

	do {
		if ((inject_count < inject_max) && ((replay->nack_count)++ > 0)) {
			inject_count++;
			fi_opa1x_reliability_service_do_replay(service, &replay->buffer);
#ifdef OPA1X_RELIABILITY_DEBUG
		} else {
			union fi_opa1x_reliability_service_flow_key key;
			key.slid = (uint32_t)replay->buffer.scb.hdr.stl.lrh.slid;
			key.tx = (uint32_t)replay->buffer.scb.hdr.reliability.origin_tx;
			key.dlid = (uint32_t)replay->buffer.scb.hdr.stl.lrh.dlid;
			key.rx = (uint32_t)replay->buffer.scb.hdr.stl.bth.rx;

			fprintf(stderr, "(tx) packet %016lx %08u replay skipped (nack count)\n", key.value, (uint32_t)replay->psn);
#endif
		}
		replay = replay->next;

	} while (replay != halt);
//fprintf(stderr, "%s():%d\n", __func__, __LINE__);
}


void fi_opa1x_reliability_service_hfi1_receive_egr (union fi_opa1x_reliability_service_internal * service,
	const union fi_opa1x_reliability_service_hfi1_packet_hdr * const hdr, const unsigned count,
	const struct fi_opa1x_reliability_service_range * const range) {


	fprintf(stderr, "%s():%d\n", __func__, __LINE__);
	abort();
}




static inline
unsigned fi_opa1x_reliability_service_poll_hfi1 (union fi_opa1x_reliability_service_internal * service) {

	const uint64_t hdrq_offset = service->rx.hfi1.state.hdrq.head & 0x000000000000FFE0ul;
	volatile uint32_t * rhf_ptr = (uint32_t *)service->rx.hfi1.hdrq.rhf_base + hdrq_offset;
	const uint32_t rhf_lsb = rhf_ptr[0];
	const uint32_t rhf_msb = rhf_ptr[1];

	/*
	 * Check for receive errors
	 */
	if (unlikely((rhf_msb & 0xFFE00000u) != 0)) {

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
	const uint32_t rhf_seq = service->rx.hfi1.state.hdrq.rhf_seq;
	if (rhf_seq == (rhf_lsb & 0xF0000000u)) {

		const uint64_t hdrq_offset_dws = (rhf_msb >> 12) & 0x01FFu;

		uint32_t * pkt = (uint32_t *)rhf_ptr -
			32 +	/* header queue entry size in dw */
			2 +	/* rhf field size in dw */
			hdrq_offset_dws;

		const union fi_opa1x_reliability_service_hfi1_packet_hdr * const hdr =
			(union fi_opa1x_reliability_service_hfi1_packet_hdr *)pkt;

		const uint8_t opcode = hdr->stl.bth.opcode;

		if (opcode == FI_OPA1X_HFI_BTH_OPCODE_RELIABILITY_PING) {

			assert((rhf_lsb & 0x00008000u) != 0x00008000u);	/* ping packets NEVER have 'eager' payload data */

			/* "header only" packet - no payload */
			fi_opa1x_reliability_service_hfi1_receive_ping(service, hdr);

		} else if (opcode == FI_OPA1X_HFI_BTH_OPCODE_RELIABILITY_ACK) {

			assert((rhf_lsb & 0x00008000u) != 0x00008000u);

			/* "header only" packet - no payload */
			fi_opa1x_reliability_service_hfi1_receive_ack(service, hdr);

		} else if (opcode == FI_OPA1X_HFI_BTH_OPCODE_RELIABILITY_NACK) {

			assert((rhf_lsb & 0x00008000u) != 0x00008000u);

			/* "header only" packet - no payload */
			fi_opa1x_reliability_service_hfi1_receive_nack(service, hdr);

		} else {

			assert((rhf_lsb & 0x00008000u) == 0x00008000u);
			assert(opcode == FI_OPA1X_HFI_BTH_OPCODE_RELIABILITY_EGR);

			/* "eager" packet - has payload */

			const uint32_t egrbfr_index = (rhf_lsb >> FI_OPA1X_HFI1_RHF_EGRBFR_INDEX_SHIFT) & FI_OPA1X_HFI1_RHF_EGRBFR_INDEX_MASK;
			const uint32_t egrbfr_offset = rhf_msb & 0x0FFFu;
			const struct fi_opa1x_reliability_service_range * const payload =
				(struct fi_opa1x_reliability_service_range *)((uintptr_t) service->rx.hfi1.egrq.base_addr +
				egrbfr_index * service->rx.hfi1.egrq.elemsz +
				egrbfr_offset * 64);

			assert(payload!=NULL);

			fi_opa1x_reliability_service_hfi1_receive_egr(service, hdr, hdr->reliability.range_count, payload);

			const uint32_t last_egrbfr_index = service->rx.hfi1.egrq.last_egrbfr_index;
			if (unlikely(last_egrbfr_index != egrbfr_index)) {
				*service->rx.hfi1.egrq.head_register = last_egrbfr_index;
				service->rx.hfi1.egrq.last_egrbfr_index = egrbfr_index;
			}
		}

		service->rx.hfi1.state.hdrq.rhf_seq = (rhf_seq < 0xD0000000u) * rhf_seq + 0x10000000u;
		service->rx.hfi1.state.hdrq.head = hdrq_offset + 32;	/* 32 dws == 128 bytes, the maximum header queue entry size */

		/*
		 * Notify the hfi that this packet has been processed ..
		 * but only do this every 1024 hdrq elements because the hdrq
		 * size is 2048 and the update is expensive.
		 */
		if (unlikely((hdrq_offset & 0x7FFFul) == 0x0020ul)) {
			*service->rx.hfi1.hdrq.head_register = hdrq_offset - 32;
		}

		return 1;	/* one packet was processed */
	}

	return 0;
}


static inline
void fi_reliability_service_ping_remote (union fi_opa1x_reliability_service_internal * service) {

	/* for each flow in the rbtree ... */
	RbtIterator itr = rbtBegin(service->tx.flow);

	while (itr) {

		struct fi_opa1x_reliability_tx_replay_internal ** value_ptr =
			(struct fi_opa1x_reliability_tx_replay_internal **)rbtValuePtr(service->tx.flow, itr);

		struct fi_opa1x_reliability_tx_replay_internal * head = *value_ptr;

		if (likely(head != NULL)) {

			const uint64_t key = head->key.value;
			const uint64_t dlid = (uint64_t)head->key.dlid;
			const uint64_t rx = (uint64_t)head->rs;

			uint64_t psn_start = head->psn;
			uint64_t psn_count = 1;

			struct fi_opa1x_reliability_tx_replay_internal * replay = head->next;

			while (replay != head) {

				if (replay->psn == (psn_start + psn_count)) {
					++psn_count;
				} else {
					fi_opa1x_reliability_service_hfi1_inject_generic(service,
							key, dlid, rx,
							psn_start,		/* psn_start */
							psn_count,		/* psn_count */
							FI_OPA1X_HFI_BTH_OPCODE_RELIABILITY_PING);
					psn_start = replay->psn;
					psn_count = 1;
				}
				replay = replay->next;

			}

			fi_opa1x_reliability_service_hfi1_inject_generic(service,
					key, dlid, rx,
					psn_start,
					psn_count,
					FI_OPA1X_HFI_BTH_OPCODE_RELIABILITY_PING);
#if 0
			/*
			 * TEST ONLY - retire all replay buffers now
			 */

			*value_ptr = NULL;
			head->prev->next = NULL;
			do {
				struct fi_opa1x_reliability_tx_replay_internal * next = head->next;

				head->next = NULL;
				head->prev = NULL;
				head->active = 0;
				head = next;

			} while (head != NULL);
#endif
		}

		/* advance to the next dlid */
		itr = rbtNext(service->tx.flow, itr);
	}
}


static inline
void fi_opa1x_reliability_service_poll (union fi_opa1x_reliability_service_internal * internal) {

	/* process incoming tx replay packets */
	struct fi_opa1x_atomic_fifo * fifo = &internal->fifo;

	double elapsed_usec;
	union fi_opa1x_timer_stamp timestamp;
	union fi_opa1x_timer_state * timer = &internal->tx.timer;
	fi_opa1x_timer_now(&timestamp, timer);

	const double   usec_max = (double)((uint64_t)internal->usec_max);
	const unsigned fifo_max = (unsigned) internal->fifo_max;
	const unsigned hfi1_max = (unsigned) internal->hfi1_max;

	volatile uint64_t * enabled_ptr = &internal->enabled;

	uint64_t spin_count = 0;

	while (*enabled_ptr) {

		elapsed_usec = fi_opa1x_timer_elapsed_usec(&timestamp, timer);
		if (unlikely(elapsed_usec > usec_max)) {

			fi_reliability_service_ping_remote(internal);

			/* reset the timer */
			fi_opa1x_timer_now(&timestamp, timer);
		}

		unsigned count = 0;
		uint64_t data = 0;
		while ((count++ < fifo_max) && (0 == fi_opa1x_atomic_fifo_consume(fifo, &data))) {

			if (likely((data & TX_CMD) != 0)) {

				/* process this replay buffer */
				struct fi_opa1x_reliability_tx_replay_internal * replay =
					(struct fi_opa1x_reliability_tx_replay_internal *) (data & ~TX_CMD);

				fi_reliability_service_process_command(internal, replay);

			} else if (data & RX_CMD) {

				/* process this new rx flow */
				struct fi_opa1x_reliability_flow * flow =
					(struct fi_opa1x_reliability_flow *) (data & ~RX_CMD);

				rbtInsert(internal->rx.flow, (void*)flow->key.value, (void*)flow);

			} else {
				fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__); abort();
			}

		}

		count = 0;
		while ((count++ < hfi1_max) && (0 != fi_opa1x_reliability_service_poll_hfi1(internal)));

		if (internal->is_backoff_enabled) {
			fi_opa1x_shm_x86_pause();
			spin_count++;
			if (spin_count % internal->backoff_period == 0) {
				sched_yield();
			}
		}
	}

	return;
}

void fi_opa1x_reliability_service_cleanup (union fi_opa1x_reliability_service_internal * internal) {


	/*
	 * the application must not care about any un-acked replay packets;
	 * mark all flows as complete
	 */
	RbtIterator itr = rbtBegin(internal->tx.flow);
	while (itr) {

		struct fi_opa1x_reliability_tx_replay_internal ** value_ptr =
			(struct fi_opa1x_reliability_tx_replay_internal **)rbtValuePtr(internal->tx.flow, itr);

		struct fi_opa1x_reliability_tx_replay_internal * head = *value_ptr;

		if (likely(head != NULL)) {
			struct fi_opa1x_reliability_tx_replay_internal * tail = head->prev;

			tail->next = NULL;
			do {

				struct fi_opa1x_reliability_tx_replay_internal * next = head->next;

				const uint64_t dec = head->cc_dec;
				volatile uint64_t * cc_ptr = head->cc_ptr;
				*cc_ptr -= dec;

				head->next = NULL;
				head->prev = NULL;
				head->active = 0;

				head = next;

			} while (head != NULL);

			*value_ptr = NULL;
		}

		/* advance to the next dlid */
		itr = rbtNext(internal->tx.flow, itr);
	}

	/*
	 * process, and respond to, any incoming packets from remote
	 * reliability services until no packets are received
	 */

	union fi_opa1x_timer_stamp timestamp;
	union fi_opa1x_timer_state * timer = &internal->tx.timer;
	fi_opa1x_timer_now(&timestamp, timer);

	unsigned n = 0;
	while (fi_opa1x_timer_elapsed_usec(&timestamp, timer) < 10000.0) {

		n = fi_opa1x_reliability_service_poll_hfi1(internal);
		if (n > 0) {
			/* reset the timer */
			fi_opa1x_timer_now(&timestamp, timer);
		}
	}
}

void * pthread_start_routine (void * arg) {

	union fi_opa1x_reliability_service_internal * internal =
		(union fi_opa1x_reliability_service_internal *)arg;

	internal->active = 1;
	while (internal->enabled > 0) {
		fi_opa1x_reliability_service_poll(internal);
	}
	fi_opa1x_reliability_service_cleanup(internal);
	internal->active = 0;

	return NULL;
}


uint8_t fi_opa1x_reliability_service_init (struct fi_opa1x_reliability_service * service, uuid_t unique_job_key) {

	if (sizeof(struct fi_opa1x_reliability_service) < sizeof(union fi_opa1x_reliability_service_internal)) {
		fprintf(stderr, "%s:%s():%d sizeof(struct fi_opa1x_reliability_service) = %zu, sizeof(union fi_opa1x_reliability_service_internal) = %zu\n", __FILE__, __func__, __LINE__, sizeof(struct fi_opa1x_reliability_service), sizeof(union fi_opa1x_reliability_service_internal)); abort();
	}

	union fi_opa1x_reliability_service_internal * internal =
		(union fi_opa1x_reliability_service_internal *)service;

	internal->context = fi_opa1x_hfi1_context_open(unique_job_key);
	struct fi_opa1x_hfi1_context * hfi1 = internal->context;
	init_hfi1_rxe_state(hfi1, &internal->rx.hfi1.state);

	/*
	 * COPY the rx static information from the hfi context structure.
	 * This is to improve cache layout.
	 */
	internal->rx.hfi1.hdrq.rhf_base = hfi1->info.rxe.hdrq.rhf_base;
	internal->rx.hfi1.hdrq.head_register = hfi1->info.rxe.hdrq.head_register;
	internal->rx.hfi1.egrq.base_addr = hfi1->info.rxe.egrq.base_addr;
	internal->rx.hfi1.egrq.elemsz = hfi1->info.rxe.egrq.elemsz;
	internal->rx.hfi1.egrq.last_egrbfr_index = 0;
	internal->rx.hfi1.egrq.head_register = hfi1->info.rxe.egrq.head_register;


	/* the 'state' fields will change after every tx operation */
	internal->tx.hfi1.pio_state = &hfi1->state.pio;

	/* the 'info' fields do not change; the values can be safely copied */
	internal->tx.hfi1.pio_scb_sop_first = hfi1->info.pio.scb_sop_first;
	internal->tx.hfi1.pio_scb_first = hfi1->info.pio.scb_first;
	internal->tx.hfi1.pio_credits_addr = hfi1->info.pio.credits_addr;

	/* 'ping' pio send model */
	{
		/* PBC */
		const uint64_t pbc_dws =
			2 +	/* pbc */
			2 +	/* lrh */
			3 +	/* bth */
			9;	/* kdeth; from "RcvHdrSize[i].HdrSize" CSR */

		internal->tx.hfi1.ping_model.qw0 = (0 | pbc_dws |
			((hfi1->vl & FI_OPA1X_HFI1_PBC_VL_MASK) << FI_OPA1X_HFI1_PBC_VL_SHIFT) |
			(((hfi1->sc >> FI_OPA1X_HFI1_PBC_SC4_SHIFT) & FI_OPA1X_HFI1_PBC_SC4_MASK) << FI_OPA1X_HFI1_PBC_DCINFO_SHIFT));

		/* LRH */
		internal->tx.hfi1.ping_model.hdr.stl.lrh.flags =
			htons(FI_OPA1X_HFI1_LRH_BTH |
			((hfi1->sl & FI_OPA1X_HFI1_LRH_SL_MASK) << FI_OPA1X_HFI1_LRH_SL_SHIFT) |
			((hfi1->sc & FI_OPA1X_HFI1_LRH_SC_MASK) << FI_OPA1X_HFI1_LRH_SC_SHIFT));

		internal->tx.hfi1.ping_model.hdr.stl.lrh.dlid = 0;			/* set at runtime */
		internal->tx.hfi1.ping_model.hdr.stl.lrh.pktlen = htons(pbc_dws-1);	/* does not include pbc (8 bytes), but does include icrc (4 bytes) */
		internal->tx.hfi1.ping_model.hdr.stl.lrh.slid = htons(hfi1->lid);

		/* BTH */
		internal->tx.hfi1.ping_model.hdr.stl.bth.opcode = FI_OPA1X_HFI_BTH_OPCODE_RELIABILITY_PING;
		internal->tx.hfi1.ping_model.hdr.stl.bth.bth_1 = 0;
		internal->tx.hfi1.ping_model.hdr.stl.bth.pkey = htons(FI_OPA1X_HFI1_DEFAULT_P_KEY);
		internal->tx.hfi1.ping_model.hdr.stl.bth.ecn = 0;
		internal->tx.hfi1.ping_model.hdr.stl.bth.qp = hfi1->bthqp;
		internal->tx.hfi1.ping_model.hdr.stl.bth.unused = 0;
		internal->tx.hfi1.ping_model.hdr.stl.bth.rx = 0;			/* set at runtime */

		/* KDETH */
		internal->tx.hfi1.ping_model.hdr.stl.kdeth.offset_ver_tid = KDETH_VERSION << FI_OPA1X_HFI1_KHDR_KVER_SHIFT;
		internal->tx.hfi1.ping_model.hdr.stl.kdeth.jkey = hfi1->jkey;
		internal->tx.hfi1.ping_model.hdr.stl.kdeth.hcrc = 0;
		internal->tx.hfi1.ping_model.hdr.stl.kdeth.unused = 0;

		/* reliability service */
		union fi_opa1x_reliability_service_hfi1_packet_hdr * hdr =
			(union fi_opa1x_reliability_service_hfi1_packet_hdr *)&internal->tx.hfi1.ping_model.hdr;

		hdr->reliability.origin_reliability_rx = internal->context->info.rxe.id;
		hdr->reliability.range_count = 0;
		hdr->reliability.unused = 0;
		hdr->reliability.psn_count = 0;
		hdr->reliability.psn_start = 0;
		hdr->reliability.key.value = 0;
	}

	/* 'ack' pio send model */
	{
		internal->tx.hfi1.ack_model = internal->tx.hfi1.ping_model;
		internal->tx.hfi1.ack_model.hdr.stl.bth.opcode = FI_OPA1X_HFI_BTH_OPCODE_RELIABILITY_ACK;
	}

	/* 'nack' pio send model */
	{
		internal->tx.hfi1.nack_model = internal->tx.hfi1.ping_model;
		internal->tx.hfi1.nack_model.hdr.stl.bth.opcode = FI_OPA1X_HFI_BTH_OPCODE_RELIABILITY_NACK;
	}


	fi_opa1x_atomic_fifo_init(&internal->fifo, 1024*16);

	fi_opa1x_timer_init(&internal->tx.timer);

	internal->tx.flow = rbtNew(fi_opa1x_reliability_compare);
	internal->rx.flow = rbtNew(fi_opa1x_reliability_compare);

	char * env = getenv("OPA1X_DEBUG");
	int is_debug = 0;
	if (env) {
		is_debug = 1;
	}

	env = getenv("RELIABILITY_SERVICE_BACKOFF_PERIOD");
	internal->is_backoff_enabled = 0;
	internal->backoff_period = 1;
	if (env) {
		unsigned long period = strtoul(env, NULL, 10);
		if (is_debug) {
			fprintf(stderr, "%s():%d RELIABILITY_SERVICE_BACKOFF_PERIOD = '%s' (%lu)\n", __func__, __LINE__, env, period);
		}
		internal->is_backoff_enabled = 1;
		internal->backoff_period=(uint64_t)period;
	}

	env = getenv("RELIABILITY_SERVICE_USEC_MAX");
	internal->usec_max = 600;
	if (env) {
		unsigned long usec = strtoul(env, NULL, 10);
		if (is_debug) {
			fprintf(stderr, "%s():%d RELIABILITY_SERVICE_USEC_MAX = '%s' (%lu)\n", __func__, __LINE__, env, usec);
		}
		internal->usec_max = (uint16_t)usec;
	}

	env = getenv("RELIABILITY_SERVICE_FIFO_MAX");
	internal->fifo_max = 1;
	if (env) {
		unsigned long max = strtoul(env, NULL, 10);
		if (is_debug) {
			fprintf(stderr, "%s():%d RELIABILITY_SERVICE_FIFO_MAX = '%s' (%lu)\n", __func__, __LINE__, env, max);
		}
		internal->fifo_max = (uint8_t)max;
	}

	env = getenv("RELIABILITY_SERVICE_HFI1_MAX");
	internal->hfi1_max = 1;
	if (env) {
		unsigned long max = strtoul(env, NULL, 10);
		if (is_debug) {
			fprintf(stderr, "%s():%d RELIABILITY_SERVICE_HFI1_MAX = '%s' (%lu)\n", __func__, __LINE__, env, max);
		}
		internal->hfi1_max = (uint8_t)max;
	}

	int local_ranks = 1;
	int local_rank_id = 0;
	int is_local_rank_mode = 0;

	env = getenv("RELIABILITY_SERVICE_MPI_LOCALRANK_MODE");

	if (env) {
		char * local_ranks_env = getenv("MPI_LOCALNRANKS");
		char * local_rank_id_env = getenv("MPI_LOCALRANKID");

		if (local_ranks_env && local_rank_id_env) {
			is_local_rank_mode = 1;
			local_ranks = (int)strtoul(local_ranks_env, NULL, 10);
			local_rank_id = (int)strtoul(local_rank_id_env, NULL, 10);
		}
	}

	pthread_attr_t attr;
	pthread_attr_init(&attr);

	env = getenv("RELIABILITY_SERVICE_CPU");

	if (env) {
		long cpu_num = sysconf(_SC_NPROCESSORS_ONLN);
		cpu_set_t cpu_set;

		CPU_ZERO(&cpu_set);

		unsigned long cpu_id = 0;
		unsigned long cpu_id_range_begin = 0;
		unsigned long cpu_id_range_end = 0;

		char * service_cpu_str_save = NULL;
		char * service_cpu_sub_str_save = NULL;

		char * service_cpu_sub_str = NULL;
		char * service_cpu_sub_str_iter = NULL;

		char * service_cpu_str = strdup(env);
		char * service_cpu_str_iter = strtok_r(service_cpu_str, ",", &service_cpu_str_save);

		while (service_cpu_str_iter != NULL) {
			service_cpu_sub_str = strdup(service_cpu_str_iter);
			service_cpu_sub_str_iter = strtok_r(service_cpu_sub_str, "-", &service_cpu_sub_str_save);

			cpu_id_range_begin = strtoul(service_cpu_sub_str_iter, NULL, 10);
			cpu_id_range_end = cpu_id_range_begin;

			service_cpu_sub_str_iter = strtok_r(NULL, "-", &service_cpu_sub_str_save);

			if (service_cpu_sub_str_iter) {
				cpu_id_range_end = strtoul(service_cpu_sub_str_iter, NULL, 10);
			}

			for (cpu_id = cpu_id_range_begin; cpu_id <= cpu_id_range_end; cpu_id++) {
				CPU_SET(cpu_id, &cpu_set);
			}

			service_cpu_str_iter = strtok_r(NULL, ",", &service_cpu_str_save);
			free(service_cpu_sub_str);
		}

		free(service_cpu_str);

		if (is_local_rank_mode) {
			int cpu_num_used_total = CPU_COUNT(&cpu_set);
			int cpu_num_used_max = 1;
			int cpu_num_used = 0;

			if (local_ranks < cpu_num_used_total) {
				cpu_num_used_max = cpu_num_used_total / local_ranks;
			}

			int cpu_num_used_offset = local_rank_id % cpu_num_used_total;

			if (is_debug) {
				fprintf(stderr, "%s():%d cpu_num_used_offset = %d, cpu_num_used_max = %d, cpu_num_used_total = %d\n",
						__func__, __LINE__, cpu_num_used_offset, cpu_num_used_max, cpu_num_used_total);
			}

			for (cpu_id = 0; cpu_id < cpu_num; cpu_id++) {
				if (CPU_ISSET(cpu_id, &cpu_set)) {
					if (cpu_num_used_offset) {
						CPU_CLR(cpu_id, &cpu_set); /* clear head */
						cpu_num_used_offset--;
					} else {
						if (cpu_num_used != cpu_num_used_max) {
							cpu_num_used++; /* leave body */
						} else {
							CPU_CLR(cpu_id, &cpu_set); /* clear tail */
						}
					}
				}
			}
		}

		const int cpu_mask_chunk_bits_size = 64; /* uint64_t bits */
		const int cpu_mask_chunk_hex_size = cpu_mask_chunk_bits_size / 4;

		int cpu_mask_chunk_num  = cpu_num / cpu_mask_chunk_bits_size + (cpu_num % cpu_mask_chunk_bits_size ? 1 : 0);

		uint64_t cpu_mask[cpu_mask_chunk_num];
		memset(cpu_mask, 0, sizeof(cpu_mask));

		int i = 0;
		int j = 0;

		for (i = 0; i < cpu_mask_chunk_num; i++) {
			for (j = 0; j < cpu_mask_chunk_bits_size; j++) {
				cpu_mask[i] |= CPU_ISSET(i * cpu_mask_chunk_bits_size + j, &cpu_set) << j;
			}
		}

		char cpu_mask_str[cpu_mask_chunk_num * (cpu_mask_chunk_hex_size + 1 /* space */) + 1 /* null */];

		for (i = 0; i < cpu_mask_chunk_num; i++) {
			sprintf(cpu_mask_str + i * (cpu_mask_chunk_hex_size + 1), "%016" PRIX64 " ", cpu_mask[i]);
		}

		cpu_mask_str[cpu_mask_chunk_num * cpu_mask_chunk_hex_size] = '\0';

		if (is_debug) {
			fprintf(stderr, "%s():%d RELIABILITY_SERVICE_CPU: (%s) & (rank_mode = %s) == (%s)\n", __func__, __LINE__, env,
					is_local_rank_mode ? "TRUE" : "FALSE", cpu_mask_str);
		}

		size_t cpu_set_size = CPU_ALLOC_SIZE(cpu_num);
		pthread_attr_setaffinity_np(&attr, cpu_set_size, &cpu_set);
	}

	internal->enabled = 1;
	internal->active = 0;
	int rc = pthread_create(&internal->thread, &attr, pthread_start_routine, (void *)internal);
	if (rc != 0) {
		fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__); abort();
	}

	return internal->context->info.rxe.id;
}


void fi_opa1x_reliability_service_fini (struct fi_opa1x_reliability_service * service) {

	union fi_opa1x_reliability_service_internal * internal =
		(union fi_opa1x_reliability_service_internal *)service;

	internal->enabled = 0;
	fi_opa1x_compiler_msync_writes();
	while (internal->active != 0) {
		fi_opa1x_compiler_msync_reads();
	}

	fi_opa1x_atomic_fifo_fini(&internal->fifo);

	return;
}


union fi_opa1x_reliability_rx_state_internal {

	struct fi_opa1x_reliability_rx_state	state;
	struct {
		struct {
			struct fi_opa1x_atomic_fifo_producer	fifo;
		} service;

		void (*process_fn)(void *, const union fi_opa1x_hfi1_packet_hdr * const, const uint8_t * const);
		RbtHandle			rbtree;
		uint32_t			rx;
		uint16_t			drop_count;
		uint16_t			drop_mask;
		uint32_t			lid_be;
	};
};


void fi_opa1x_reliability_rx_init (struct fi_opa1x_reliability_rx_state * state, const uint8_t rx,
		struct fi_opa1x_reliability_service * service,
		void (*process_fn)(void *, const union fi_opa1x_hfi1_packet_hdr * const, const uint8_t * const)) {

//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
	if (sizeof(union fi_opa1x_reliability_rx_state_internal) != sizeof(struct fi_opa1x_reliability_rx_state)) {
		fprintf(stderr, "%s:%s():%d sizeof(union fi_opa1x_reliability_rx_state_internal) = %zu, sizeof(struct fi_opa1x_reliability_rx_state) = %zu\n", __FILE__, __func__, __LINE__, sizeof(union fi_opa1x_reliability_rx_state_internal), sizeof(struct fi_opa1x_reliability_rx_state)); abort();
	}

	union fi_opa1x_reliability_rx_state_internal * internal_state =
		(union fi_opa1x_reliability_rx_state_internal *) state;

	union fi_opa1x_reliability_service_internal * internal_service =
		(union fi_opa1x_reliability_service_internal *) service;

	fi_opa1x_atomic_fifo_producer_init(&internal_state->service.fifo, &internal_service->fifo);

	internal_state->process_fn = process_fn;
	internal_state->rbtree = rbtNew(fi_opa1x_reliability_compare);
	internal_state->rx = rx;
	internal_state->drop_count = 0;
	internal_state->drop_mask = 0x00FF;	/* default: drop every 256'th packet */
	internal_state->lid_be = (uint32_t)htons(internal_service->context->lid);

	char * env = getenv("RELIABILITY_SERVICE_DROP_PACKET_MASK");
	if (env) {
		uint16_t mask = (uint16_t)strtoul(env, NULL, 16);
		fprintf(stderr, "%s():%d RELIABILITY_SERVICE_DROP_PACKET_MASK = '%s' (0x%04hx)\n", __func__, __LINE__, env, mask);
		internal_state->drop_mask = mask;
	}

	return;
}

void fi_opa1x_reliability_rx_fini (struct fi_opa1x_reliability_rx_state * state) {

	union fi_opa1x_reliability_rx_state_internal * internal_state =
		(union fi_opa1x_reliability_rx_state_internal *) state;

	fi_opa1x_atomic_fifo_producer_fini(&internal_state->service.fifo);

	/* TODO - delete rbtree and flows, but first have to notify
	 * reliability service of the tear-down */
}

uint16_t fi_opa1x_reliability_rx_drop_packet (struct fi_opa1x_reliability_rx_state * state) {

	union fi_opa1x_reliability_rx_state_internal * internal =
		(union fi_opa1x_reliability_rx_state_internal *) state;

	const uint16_t tmp = internal->drop_count & internal->drop_mask;

if (tmp == 0)
	fprintf(stderr, "%s:%s():%d %s packet %hu\n", __FILE__, __func__, __LINE__, (tmp == 0) ? "drop" : "keep", internal->drop_count);

	internal->drop_count = tmp + 1;
	return !tmp;
}

/*
 * returns !0 if this packet is expected (success)
 * returns 0 on exception
 */
unsigned fi_opa1x_reliability_rx_check (struct fi_opa1x_reliability_rx_state * state,
		uint64_t slid, uint64_t origin_tx, uint32_t psn) {

#ifdef SKIP_RELIABILITY_PROTOCOL_RX_IMPL
	return FI_OPA1X_RELIABILITY_EXPECTED;
#else
	union fi_opa1x_reliability_rx_state_internal * internal =
		(union fi_opa1x_reliability_rx_state_internal *) state;

	struct fi_opa1x_reliability_flow *flow;

	void *itr, *key_ptr;

	union fi_opa1x_reliability_service_flow_key key;
	key.slid = slid;
	key.tx = origin_tx;
	key.dlid = internal->lid_be;
	key.rx = internal->rx;

	itr = rbtFind(internal->rbtree, (void*)key.value);
	if (likely((itr != NULL))) {
		rbtKeyValue(internal->rbtree, itr, &key_ptr, (void **)&flow);

		if ((flow->next_psn == psn) && (flow->uepkt == NULL)) {
#ifdef OPA1X_RELIABILITY_DEBUG
			fprintf(stderr, "(rx) packet %016lx %08u received.\n", key.value, psn);
#endif
			flow->next_psn += 1;
			return FI_OPA1X_RELIABILITY_EXPECTED;
		}
	}

	return FI_OPA1X_RELIABILITY_EXCEPTION;
#endif
}




void fi_opa1x_reliability_rx_exception (struct fi_opa1x_reliability_rx_state * state,
		uint64_t slid, uint64_t origin_tx, uint32_t psn,
		void * arg, const union fi_opa1x_hfi1_packet_hdr * const hdr, const uint8_t * const payload) {

	/* reported in LRH as the number of 4-byte words in the packet; header + payload + icrc */
	const uint16_t lrh_pktlen_le = ntohs(hdr->stl.lrh.pktlen);
	const size_t total_bytes_to_copy = (lrh_pktlen_le - 1) * 4;	/* do not copy the trailing icrc */
	const size_t payload_bytes_to_copy = total_bytes_to_copy - sizeof(union fi_opa1x_hfi1_packet_hdr);

	union fi_opa1x_reliability_rx_state_internal * internal =
		(union fi_opa1x_reliability_rx_state_internal *) state;

	union fi_opa1x_reliability_service_flow_key key;
	key.slid = slid;
	key.tx = origin_tx;
	key.dlid = internal->lid_be;
	key.rx = internal->rx;

//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
	void * itr = rbtFind(internal->rbtree, (void*)key.value);
	if (unlikely(itr == NULL)) {

		if (psn != 0) {
//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);

			/* the first packet in this flow was not delivered.
			 * do not create a new rbtree flow node and drop this
			 * packet
			 */

			fprintf(stderr, "%s:%s():%d first packet in flow exception!\n", __FILE__, __func__, __LINE__);

			/* TODO - send nack ? */

			return;
		}

		/* allocate a new rbtree node and insert */

		/* TODO - allocate from a pool of flow objects instead for better memory utilization */
		int rc __attribute__ ((unused));
		struct fi_opa1x_reliability_flow * flow = NULL;
		rc = posix_memalign((void **)&flow, 32, sizeof(*flow));
		assert(rc==0);

		flow->next_psn = 1;
		flow->key.value = key.value;
		flow->uepkt = NULL;
		flow->lock.next = 0;
		flow->lock.serving = 0;

		rbtInsert(internal->rbtree, (void*)key.value, (void*)flow);

#ifdef OPA1X_RELIABILITY_DEBUG
		fprintf(stderr, "(rx) packet %016lx %08u received.\n", key.value, psn);
#endif
//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
		internal->process_fn(arg, hdr, payload);
//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);

		fi_opa1x_atomic_fifo_produce(&internal->service.fifo, (uint64_t)flow | RX_CMD);

//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
		return;
	}

	struct fi_opa1x_reliability_flow ** value_ptr =
		(struct fi_opa1x_reliability_flow **) rbtValuePtr(internal->rbtree, itr);

	struct fi_opa1x_reliability_flow * flow = *value_ptr;

	uint64_t next_psn = flow->next_psn;

//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
	if (psn < next_psn) {

		/*
		 * old packet .. drop it
		 */
#ifdef OPA1X_RELIABILITY_DEBUG
		fprintf(stderr, "(rx) packet %016lx %08u dropped (duplicate).\n", key.value, psn);
#endif
		return;
	}

//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
	if (next_psn == psn) {

		/*
		 * deliver this packet and the next contiguous sequence of
		 * previously queued unexpected packets
		 */

#ifdef OPA1X_RELIABILITY_DEBUG
		fprintf(stderr, "(rx) packet %016lx %08u received (process out-of-order).\n", key.value, psn);
#endif
//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
		internal->process_fn(arg, hdr, payload);
//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
		next_psn += 1;

//fprintf(stderr, "%s:%s():%d flow->next_psn = %lu, next_psn = %lu\n", __FILE__, __func__, __LINE__, flow->next_psn, next_psn);
		flow->next_psn = next_psn;

		struct fi_opa1x_reliability_rx_uepkt * head = flow->uepkt;
		if (head != NULL) {
//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);

			fi_opa1x_reliability_ticketlock_acquire(&flow->lock);
//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);

			head = flow->uepkt;	/* check again now that lock is acquired */

			struct fi_opa1x_reliability_rx_uepkt * uepkt = head;

			while ((uepkt != NULL) && (next_psn == uepkt->psn)) {

				internal->process_fn(arg, &uepkt->hdr, uepkt->payload);
#ifdef OPA1X_RELIABILITY_DEBUG
				fprintf(stderr, "(rx) packet %016lx %08lu delivered.\n", key.value, next_psn);
#endif
				next_psn += 1;

				struct fi_opa1x_reliability_rx_uepkt * next = uepkt->next;
				if (next == uepkt) {
					/* only one element in the list */
					assert(uepkt->prev == uepkt);
					next = NULL;
				}

				uepkt->prev->next = uepkt->next;
				uepkt->next->prev = uepkt->prev;
				free(uepkt);
				head = next;
				uepkt = next;
			};

			flow->uepkt = head;

//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
			
//fprintf(stderr, "%s:%s():%d flow->next_psn = %lu, next_psn = %lu\n", __FILE__, __func__, __LINE__, flow->next_psn, next_psn);
			flow->next_psn = next_psn;
			fi_opa1x_reliability_ticketlock_release(&flow->lock);
//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
		}

//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);


	} else if (flow->uepkt == NULL) {

//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
		/*
		 * add the out-of-order packet to the empty unexpected queue
		 */

		struct fi_opa1x_reliability_rx_uepkt * uepkt = NULL;

		int rc __attribute__ ((unused));
		rc = posix_memalign((void **)&uepkt, 64,
			sizeof(*uepkt) + payload_bytes_to_copy);
		assert(rc==0);

		uepkt->prev = uepkt;
		uepkt->next = uepkt;
		uepkt->psn = psn;
		memcpy((void*)&uepkt->hdr, hdr, sizeof(union fi_opa1x_hfi1_packet_hdr));

		if (payload_bytes_to_copy > 0)
			memcpy((void*)&uepkt->payload[0], (const void *)payload, payload_bytes_to_copy);

//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
		fi_opa1x_reliability_ticketlock_acquire(&flow->lock);
//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);

		flow->uepkt = uepkt;

//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
		fi_opa1x_reliability_ticketlock_release(&flow->lock);
//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);

#ifdef OPA1X_RELIABILITY_DEBUG
		fprintf(stderr, "(rx) packet %016lx %08u queued.\n", key.value, psn);
#endif

		/* TODO - nack flow->psn ? */

	} else {
		/*
		 * insert the out-of-order packet into the unexpected queue;
		 * check for duplicates
		 *
		 * generally if one packet is received out-of-order with psn 'N'
		 * then the next packet received out-of-order will be psn 'N+1'.
		 *
		 * search the unexpected queue in reverse to find the insert
		 * point for this packet.
		 */
		struct fi_opa1x_reliability_rx_uepkt * head = flow->uepkt;
		struct fi_opa1x_reliability_rx_uepkt * tail = head->prev;
		struct fi_opa1x_reliability_rx_uepkt * uepkt = tail;

//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
		do {
			const uint64_t uepkt_psn = uepkt->psn;

//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
			if (uepkt_psn < psn) {
//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);

				/* insert after this element */
				struct fi_opa1x_reliability_rx_uepkt * tmp = NULL;

				int rc __attribute__ ((unused));
				rc = posix_memalign((void **)&tmp, 64,
					sizeof(*tmp) + payload_bytes_to_copy);
				assert(rc==0);

				tmp->prev = uepkt;
				tmp->next = uepkt->next;
				tmp->psn = psn;
				memcpy((void*)&tmp->hdr, hdr, sizeof(union fi_opa1x_hfi1_packet_hdr));
				if (payload_bytes_to_copy > 0)
					memcpy((void*)&tmp->payload[0], (const void *)payload, payload_bytes_to_copy);

//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
				fi_opa1x_reliability_ticketlock_acquire(&flow->lock);
//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);

				uepkt->next->prev = tmp;
				uepkt->next = tmp;

//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
				fi_opa1x_reliability_ticketlock_release(&flow->lock);
//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);

#ifdef OPA1X_RELIABILITY_DEBUG
				fprintf(stderr, "(rx) packet %016lx %08u queued.\n", key.value, psn);
#endif
//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
				break;

			} else if (uepkt_psn == psn) {

//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
				/* drop this duplicate */
#ifdef OPA1X_RELIABILITY_DEBUG
				fprintf(stderr, "(rx) packet %016lx %08u dropped (duplicate).\n", key.value, psn);
#endif
				break;

			}

//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
			/* move forward */
			uepkt = uepkt->prev;
//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);

		} while (uepkt != head);
//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
	}
//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);

	return;
}





// #########################################################################


#define FI_OPA1X_RELIABILITY_TX_REPLAY_BLOCKSIZE	(2048)


void fi_opa1x_reliability_tx_init (struct fi_opa1x_reliability_tx_state * state, uint8_t tx,
		struct fi_opa1x_reliability_service * service) {

	if (sizeof(struct fi_opa1x_reliability_service) != sizeof(union fi_opa1x_reliability_service_internal)) {
		fprintf(stderr, "%s:%s():%d abort\n", __FILE__, __func__, __LINE__); abort();
	}
	if (sizeof(struct fi_opa1x_reliability_tx_state) != sizeof(union fi_opa1x_reliability_tx_state_internal)) {
		fprintf(stderr, "%s:%s():%d abort\n", __FILE__, __func__, __LINE__); abort();
	}

	union fi_opa1x_reliability_tx_state_internal * internal_state =
		(union fi_opa1x_reliability_tx_state_internal *) state;

	union fi_opa1x_reliability_service_internal * internal_service =
		(union fi_opa1x_reliability_service_internal *) service;

	internal_state->tx = tx;	/* context->send_ctxt */
	internal_state->lid_be = (uint32_t)htons(internal_service->context->lid);



	void * block = NULL;
	int i, rc __attribute__ ((unused));
	rc = posix_memalign((void **)&block, 64,
		sizeof(struct fi_opa1x_reliability_tx_replay_internal_large) * FI_OPA1X_RELIABILITY_TX_REPLAY_BLOCKSIZE);
	assert(rc==0);

	internal_state->replay.large =
		(struct fi_opa1x_reliability_tx_replay_internal_large *) block;

	for (i=0; i<FI_OPA1X_RELIABILITY_TX_REPLAY_BLOCKSIZE; ++i) {
		internal_state->replay.large[i].replay.active = 0;
	}
	internal_state->replay.head = 0;

	fi_opa1x_atomic_fifo_producer_init(&internal_state->replay.fifo, &internal_service->fifo);

	internal_state->psn.rbtree = rbtNew(fi_opa1x_reliability_compare);
	internal_state->psn.value_memory = NULL;
	internal_state->psn.value_free_count = 0;

	return;
}


void fi_opa1x_reliability_tx_fini (struct fi_opa1x_reliability_tx_state * state) {

	union fi_opa1x_reliability_tx_state_internal * internal_state =
		(union fi_opa1x_reliability_tx_state_internal *) state;

	/* wait until all replay buffers are ack'd because the reliability
	 * service maintains pointers to unack'd reply buffers and can't
	 * free until service is finished
	 */

	unsigned i;
	for (i=0; i<FI_OPA1X_RELIABILITY_TX_REPLAY_BLOCKSIZE; ++i) {
		while(internal_state->replay.large[i].replay.active != 0) {
			fi_opa1x_compiler_msync_reads();
		}
	}

	fi_opa1x_atomic_fifo_producer_fini(&internal_state->replay.fifo);

	/* TODO - delete rbtree */

	free(internal_state->replay.large);

}


uint32_t fi_opa1x_reliability_tx_next_psn (struct fi_opa1x_reliability_tx_state * state,
		uint64_t lid, uint64_t rx) {

#ifdef SKIP_RELIABILITY_PROTOCOL_TX_IMPL
	return 0;
#else
	union fi_opa1x_reliability_tx_state_internal * internal_state =
		(union fi_opa1x_reliability_tx_state_internal *) state;

	uint32_t psn = 0;

	union fi_opa1x_reliability_service_flow_key key;
	key.slid = internal_state->lid_be;
	key.tx = internal_state->tx;
	key.dlid = lid;
	key.rx = rx;

	void * itr = rbtFind(internal_state->psn.rbtree, (void*)key.value);
	if (!itr) {
		uintptr_t value = 1;
		rbtInsert(internal_state->psn.rbtree, (void*)key.value, (void*)value);

	} else {
		uintptr_t * const psn_ptr = (uintptr_t *)rbtValuePtr(internal_state->psn.rbtree, itr);
		const uintptr_t psn_value = *psn_ptr;

		psn = (uint32_t) psn_value;
		*psn_ptr = psn_value + 1;

		if (psn == 0x00FFFFFFu) {	/* TODO - how to handle overflow? */
			fprintf(stderr, "%s:%s():%d psn overflow\n", __FILE__, __func__, __LINE__);
			abort();
		}
	}

	return psn;
#endif
}


struct fi_opa1x_reliability_tx_replay *
fi_opa1x_reliability_tx_replay_allocate(struct fi_opa1x_reliability_tx_state * state) {

	union fi_opa1x_reliability_tx_state_internal * internal_state =
		(union fi_opa1x_reliability_tx_state_internal *) state;

	const uint64_t head = internal_state->replay.head;
	internal_state->replay.head = (head + 1) & (FI_OPA1X_RELIABILITY_TX_REPLAY_BLOCKSIZE-1);

	struct fi_opa1x_reliability_tx_replay_internal * replay = (struct fi_opa1x_reliability_tx_replay_internal *)&internal_state->replay.large[head];

#ifndef SKIP_RELIABILITY_PROTOCOL_TX_IMPL
	/*
	 * FIXME
	 *
	 * DO NOT BLOCK - the fabric may be congested and require this thread
	 * to advance its receive context to pull packets off the wire so the
	 * reliability service threads can inject replay packets, etc!
	 */
#ifdef DONT_BLOCK_REPLAY_ALLOCATE
	unsigned loop = 0;
#endif
	while (replay->active) {
#ifdef DONT_BLOCK_REPLAY_ALLOCATE
		if (++loop > 100000) {
			fprintf(stderr, "%s:%s():%d abort! abort!\n", __FILE__, __func__, __LINE__);
			abort();
		}
#endif
		fi_opa1x_compiler_msync_writes();
	}
	replay->active = 1;
#endif

	return &replay->buffer;
}

void fi_opa1x_reliability_tx_replay_register_no_update (struct fi_opa1x_reliability_tx_state * state,
		const uint16_t dlid, const uint8_t rs, const uint8_t rx, const uint64_t psn,
		struct fi_opa1x_reliability_tx_replay * replay) {

#ifndef SKIP_RELIABILITY_PROTOCOL_TX_IMPL
	union fi_opa1x_reliability_tx_state_internal * internal_state =
		(union fi_opa1x_reliability_tx_state_internal *) state;

	struct fi_opa1x_reliability_tx_replay_internal * internal_replay =
		container_of(replay, struct fi_opa1x_reliability_tx_replay_internal, buffer);

	internal_replay->key.dlid = dlid;
	internal_replay->key.rx = rx;
	internal_replay->key.tx = internal_state->tx;
	internal_replay->key.slid = internal_state->lid_be;
	internal_replay->rs = rs;
	internal_replay->psn = psn;
	internal_replay->nack_count = 0;

	internal_replay->cc_ptr = &internal_replay->cc_dec;
	internal_replay->cc_dec = 0;

#ifndef NDEBUG
	if ((uint64_t)internal_replay & TX_CMD) { fprintf(stderr, "%s():%d abort\n", __func__, __LINE__); abort(); }
#endif
	/*
	 * ok to block .. the reliability service is completely non-blocking
	 * and will always consume from this atomic fifo
	 */
	fi_opa1x_atomic_fifo_produce(&internal_state->replay.fifo, (uint64_t)internal_replay | TX_CMD);
#endif
	return;
}

void fi_opa1x_reliability_tx_replay_register_with_update (struct fi_opa1x_reliability_tx_state * state,
		const uint16_t dlid, const uint8_t rs, const uint8_t rx, const uint64_t psn,
		struct fi_opa1x_reliability_tx_replay * replay, uint64_t * payload_replay,
		size_t payload_qws_replay, size_t payload_qws_immediate,
		volatile uint64_t * counter, uint64_t value) {

#ifndef SKIP_RELIABILITY_PROTOCOL_TX_IMPL
	union fi_opa1x_reliability_tx_state_internal * internal_state =
		(union fi_opa1x_reliability_tx_state_internal *) state;

	struct fi_opa1x_reliability_tx_replay_internal * internal_replay =
		container_of(replay, struct fi_opa1x_reliability_tx_replay_internal, buffer);

	internal_replay->key.dlid = dlid;
	internal_replay->key.rx = rx;
	internal_replay->key.tx = internal_state->tx;
	internal_replay->key.slid = internal_state->lid_be;
	internal_replay->rs = rs;
	internal_replay->psn = psn;
	internal_replay->nack_count = 0;

	internal_replay->cc_ptr = counter;
	internal_replay->cc_dec = value;

#ifndef NDEBUG
	if ((uint64_t)internal_replay & TX_CMD) { fprintf(stderr, "%s():%d abort\n", __func__, __LINE__); abort(); }
#endif
	/*
	 * ok to block .. the reliability service is completely non-blocking
	 * and will always consume from this atomic fifo
	 */
	fi_opa1x_atomic_fifo_produce(&internal_state->replay.fifo, (uint64_t)internal_replay | TX_CMD);
#endif
	return;
}
