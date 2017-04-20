#ifndef _FI_PROV_OPA1X_SHM_H_
#define _FI_PROV_OPA1X_SHM_H_

#include "rdma/opa1x/fi_opa1x_hfi1.h"

#define fi_opa1x_shm_compiler_barrier() __asm__ __volatile__ ( "" ::: "memory" )

#define fi_opa1x_shm_x86_mfence() __asm__ __volatile__ ( "mfence" )
#define fi_opa1x_shm_x86_sfence() __asm__ __volatile__ ( "sfence" )
#define fi_opa1x_shm_x86_lfence() __asm__ __volatile__ ( "lfence" )
#define fi_opa1x_shm_x86_pause()  __asm__ __volatile__ ( "pause"  )

static inline void
fi_opa1x_shm_x86_atomic_int_inc(int * var) {
	__asm__ __volatile__ ("lock ; incl %0" :"=m" (*var) :"m" (*var));
	return;
}

static inline int
fi_opa1x_shm_x86_atomic_fetch_and_add_int(volatile int * var, int val) {
	__asm__ __volatile__ ("lock ; xadd %0,%1"
			      : "=r" (val), "=m" (*var)
			      :  "0" (val),  "m" (*var));
	return val;
}

static inline int
fi_opa1x_shm_x86_atomic_int_cas(int * var, int old_value, int new_value) {
	int prev_value;

	__asm__ __volatile__ ("lock ; cmpxchg %3,%4" : "=a" (prev_value), "=m" (*var) : "0" (old_value), "q" (new_value), "m" (*var));

	return prev_value;
}

#define FI_OPA1X_SHM_MAX_CONN_NUM (UINT8_MAX)

#define FI_OPA1X_SHM_SEGMENT_NAME_MAX_LENGTH (512)
#define FI_OPA1X_SHM_SEGMENT_NAME_PREFIX "/hfi1.shm."

struct fi_opa1x_shm_connection {
	void * segment_ptr;
	size_t segment_size;
};

struct fi_opa1x_shm_ep_tx {
	struct fi_opa1x_shm_connection connections[FI_OPA1X_SHM_MAX_CONN_NUM];
};

struct fi_opa1x_shm_ep_rx {
	char   segment_key[FI_OPA1X_SHM_SEGMENT_NAME_MAX_LENGTH];
	void * segment_ptr;
	size_t segment_size;
	int    local_ticket;
};


union fi_opa1x_shm_packet_hdr {
	uint64_t			qw[7];
	union fi_opa1x_hfi1_packet_hdr	hfi1;
} __attribute__((__aligned__(8)));

#define FI_OPA1X_SHM_PAYLOAD_SIZE (FI_OPA1X_HFI1_PACKET_MTU)

typedef struct {
	volatile uint64_t             is_busy;
	union fi_opa1x_shm_packet_hdr hdr;
	uint8_t                       payload[FI_OPA1X_SHM_PAYLOAD_SIZE];
} fi_opa1x_shm_packet_t;

struct fi_opa1x_shm_poll_state {
	void *   next_packet_ptr;
};

#define FI_OPA1X_SHM_FIFO_SIZE (1024)

typedef struct {
	/* uint8_t guard0[2048]; */
	union {
		volatile int ticket;
		uint8_t pad[64];
	};
	fi_opa1x_shm_packet_t packets[FI_OPA1X_SHM_FIFO_SIZE];
} fi_opa1x_shm_fifo_t;

static inline void
fi_opa1x_shm_memcpy(void * dst, const void * src, size_t len) {
	size_t nl = len >> 2;
	__asm__ __volatile__ ("\
	cld;\
	rep; movsl;\
	mov %3,%0;\
	rep; movsb"\
	: "+c" (nl), "+S" (src), "+D" (dst)	\
	: "r" (len & 3));
}

/* forward declarations */
struct fi_opa1x_ep;
struct fi_opa1x_ep_rx;
//struct fi_opa1x_ep_tx;

void fi_opa1x_ep_tx_shm_init (struct fi_opa1x_ep *opa1x_ep);

void fi_opa1x_ep_tx_shm_finalize (struct fi_opa1x_ep *opa1x_ep);


#endif /* _FI_PROV_OPA1X_SHM_H_ */
