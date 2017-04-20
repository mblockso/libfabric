#ifndef _FI_PROV_OPA1X_RELIABILITY_H_
#define _FI_PROV_OPA1X_RELIABILITY_H_

#include "rdma/opa1x/fi_opa1x_hfi1.h"



/* #define SKIP_RELIABILITY_PROTOCOL_RX */
/* #define SKIP_RELIABILITY_PROTOCOL_TX */
/* #define OPA1X_RELIABILITY_DEBUG */
/* #define DO_RELIABILITY_TEST */

#ifdef DO_RELIABILITY_TEST
#define FI_OPA1X_RELIABILITY_RX_DROP_PACKET(x)	fi_opa1x_reliability_rx_drop_packet(x)
#else
#define FI_OPA1X_RELIABILITY_RX_DROP_PACKET(x)	(0)
#endif



struct fi_opa1x_reliability_service {
	uint64_t	reserved[96];
} __attribute__((__aligned__(64)));

/*
 * Initialize the reliability service - and pthread
 *
 * \return reliability service hfi1 rx identifier
 */
uint8_t fi_opa1x_reliability_service_init (struct fi_opa1x_reliability_service * service, uuid_t unique_job_key);
void fi_opa1x_reliability_service_fini (struct fi_opa1x_reliability_service * service);







#define FI_OPA1X_RELIABILITY_EXCEPTION	(0)
#define FI_OPA1X_RELIABILITY_EXPECTED	(1)




struct fi_opa1x_reliability_rx_state {
	uint64_t	reserved[16];
};

void fi_opa1x_reliability_rx_init (struct fi_opa1x_reliability_rx_state * state, const uint8_t rx,
		struct fi_opa1x_reliability_service * service,
		void (*process_fn)(void * arg, const union fi_opa1x_hfi1_packet_hdr * const hdr, const uint8_t * const payload));

void fi_opa1x_reliability_rx_fini (struct fi_opa1x_reliability_rx_state * state);

uint16_t fi_opa1x_reliability_rx_drop_packet (struct fi_opa1x_reliability_rx_state * state);

unsigned fi_opa1x_reliability_rx_check (struct fi_opa1x_reliability_rx_state * state,
		uint64_t slid, uint64_t origin_tx, uint32_t psn);


void fi_opa1x_reliability_rx_exception (struct fi_opa1x_reliability_rx_state * state,
		uint64_t slid, uint64_t origin_tx, uint32_t psn,
		void * arg, const union fi_opa1x_hfi1_packet_hdr * const hdr, const uint8_t * const payload);


struct fi_opa1x_reliability_tx_state {
	uint64_t	reserved[32];
};

struct fi_opa1x_reliability_tx_replay {
	struct fi_opa1x_hfi1_txe_scb	scb;
	uint64_t			payload[0];
} __attribute__((__aligned__(64)));


void fi_opa1x_reliability_tx_init (struct fi_opa1x_reliability_tx_state * state, uint8_t tx,
		struct fi_opa1x_reliability_service * service);

void fi_opa1x_reliability_tx_fini (struct fi_opa1x_reliability_tx_state * state);

uint32_t fi_opa1x_reliability_tx_next_psn (struct fi_opa1x_reliability_tx_state * state,
		uint64_t lid, uint64_t rx);

struct fi_opa1x_reliability_tx_replay *
fi_opa1x_reliability_tx_replay_allocate(struct fi_opa1x_reliability_tx_state * state);

void fi_opa1x_reliability_tx_replay_register_no_update (struct fi_opa1x_reliability_tx_state * state,
		const uint16_t dlid, const uint8_t rs, const uint8_t rx, const uint64_t psn,
		struct fi_opa1x_reliability_tx_replay * replay);

void fi_opa1x_reliability_tx_replay_register_with_update (struct fi_opa1x_reliability_tx_state * state,
		const uint16_t dlid, const uint8_t rs, const uint8_t rx, const uint64_t psn,
		struct fi_opa1x_reliability_tx_replay * replay, uint64_t * payload_replay,
		size_t payload_qws_replay, size_t payload_qws_immediate,
		volatile uint64_t * counter, uint64_t value);


#endif /* _FI_PROV_OPA1X_RELIABILITY_H_ */
