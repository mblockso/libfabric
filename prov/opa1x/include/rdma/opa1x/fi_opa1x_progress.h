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
#ifndef _FI_PROV_OPA1X_PROGRESS_H_
#define _FI_PROV_OPA1X_PROGRESS_H_

#define MAX_ENDPOINTS	(128)	/* TODO - get this value from somewhere else */

struct fi_opa1x_ep;
struct fi_opa1x_domain;
union fi_opa1x_context;

struct fi_opa1x_progress {

	uint64_t			tag_ep_count;
	uint64_t			msg_ep_count;
	uint64_t			all_ep_count;
	volatile uint64_t		enabled;
	struct l2atomic_fifo_consumer	consumer;
	uint64_t			pad_0[8];

	/* == L2 CACHE LINE == */

	struct fi_opa1x_ep		*tag_ep[MAX_ENDPOINTS];
	struct fi_opa1x_ep		*msg_ep[MAX_ENDPOINTS];
	struct fi_opa1x_ep		*all_ep[MAX_ENDPOINTS];

	/* == L2 CACHE LINE == */

	volatile uint64_t		active;
	struct l2atomic_fifo_producer	producer;
	struct fi_opa1x_domain		*opa1x_domain;
	pthread_t			pthread;
	uint64_t			pad_1[10];

} __attribute__((__aligned__(L2_CACHE_LINE_SIZE)));

int fi_opa1x_progress_init (struct fi_opa1x_domain *opa1x_domain, const uint64_t max_threads);
int fi_opa1x_progress_enable (struct fi_opa1x_domain *opa1x_domain, const unsigned id);
int fi_opa1x_progress_disable (struct fi_opa1x_domain *opa1x_domain, const unsigned id);
int fi_opa1x_progress_fini (struct fi_opa1x_domain *opa1x_domain);

int fi_opa1x_progress_ep_enable (struct fi_opa1x_progress *thread, struct fi_opa1x_ep *opa1x_ep);
int fi_opa1x_progress_ep_disable (struct fi_opa1x_ep *opa1x_ep);

#endif /* _FI_PROV_OPA1X_PROGRESS_H_ */
