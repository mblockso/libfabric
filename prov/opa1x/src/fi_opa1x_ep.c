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
#include "rdma/fabric.h"

#include <fi.h>

#include "rdma/opa1x/fi_opa1x_domain.h"
#include "rdma/opa1x/fi_opa1x_endpoint.h"
#include "rdma/opa1x/fi_opa1x_eq.h"
#include "rdma/opa1x/fi_opa1x.h"

#include <fi_enosys.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <errno.h>
#include <unistd.h>
#include <limits.h>

#include "rdma/opa1x/fi_opa1x_fabric.h"

#define FI_OPA1X_EP_RX_UEPKT_BLOCKSIZE (256)

static int fi_opa1x_close_stx_nofree(struct fi_opa1x_stx *opa1x_stx)
{
	int ret;

//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
	ret = fi_opa1x_ref_finalize(&opa1x_stx->ref_cnt, "shared context");
	if (ret)
		return ret;

	fi_opa1x_reliability_tx_fini(&opa1x_stx->reliability.state);

	ret = fi_opa1x_ref_dec(&opa1x_stx->domain->ref_cnt, "domain");
	if (ret)
		return ret;

//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
	return 0;
}

static int fi_opa1x_close_stx(fid_t fid)
{
	FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_EP_DATA, "close stx\n");

	int ret;
	ret = fi_opa1x_fid_check(fid, FI_CLASS_STX_CTX, "shared context");
	if (ret)
		return ret;

	struct fi_opa1x_stx *opa1x_stx =
		container_of(fid, struct fi_opa1x_stx, stx_fid);

	ret = fi_opa1x_close_stx_nofree(opa1x_stx);
	if (ret)
		return ret;

	free(opa1x_stx);

	FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_EP_DATA, "stx closed\n");
	return 0;
}

int fi_opa1x_bind_ep_stx(struct fid_ep *ep,
		struct fid_stx *stx, uint64_t flags)
{
	FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_EP_DATA, "bind stx to ep\n");

	if (!ep || !stx)
		goto err;

	struct fi_opa1x_ep *opa1x_ep = container_of(ep, struct fi_opa1x_ep, ep_fid);
	struct fi_opa1x_stx *opa1x_stx = container_of(stx, struct fi_opa1x_stx, stx_fid);

	opa1x_ep->tx.stx = opa1x_stx;

	fi_opa1x_ref_inc(&opa1x_stx->ref_cnt, "shared context");

	FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_EP_DATA, "stx bound to ep\n");
	return 0;

err:
	return -errno;
}

static struct fi_ops fi_opa1x_stx_ops = {
	.size		= sizeof(struct fi_ops),
	.close		= fi_opa1x_close_stx,
	.bind		= fi_no_bind,
	.control	= fi_no_control,
	.ops_open	= fi_no_ops_open
};

static struct fi_ops_ep fi_opa1x_stx_ep_ops = {
	.size		= sizeof(struct fi_ops_ep),
	.cancel		= fi_no_cancel,
	.getopt		= fi_no_getopt,
	.setopt		= fi_no_setopt,
	.tx_ctx		= fi_no_tx_ctx,
	.rx_ctx		= fi_no_rx_ctx
};


void fi_opa1x_ep_tx_model_init (struct fi_opa1x_hfi1_context * hfi,
		const uint8_t reliability_rx,
		const uint8_t origin_cx,
		struct fi_opa1x_hfi1_txe_scb * inject,
		struct fi_opa1x_hfi1_txe_scb * send,
		struct fi_opa1x_hfi1_txe_scb * rendezvous) {

	/*
	 * fi_send*() model - eager
	 */

	/* PBC data */
	send->qw0 = (0 |
		0 /* length_dws */ |
		((hfi->vl & FI_OPA1X_HFI1_PBC_VL_MASK) << FI_OPA1X_HFI1_PBC_VL_SHIFT) |
		(((hfi->sc >> FI_OPA1X_HFI1_PBC_SC4_SHIFT) & FI_OPA1X_HFI1_PBC_SC4_MASK) << FI_OPA1X_HFI1_PBC_DCINFO_SHIFT));

	send->qw0 = 0;	/* "pbc" FIXME ?? */

	/* LRH header */
	send->hdr.stl.lrh.flags =
		htons(FI_OPA1X_HFI1_LRH_BTH |
			((hfi->sl & FI_OPA1X_HFI1_LRH_SL_MASK) << FI_OPA1X_HFI1_LRH_SL_SHIFT) |
			((hfi->sc & FI_OPA1X_HFI1_LRH_SC_MASK) << FI_OPA1X_HFI1_LRH_SC_SHIFT));

	send->hdr.stl.lrh.dlid = 0;		/* set at runtime */
	send->hdr.stl.lrh.pktlen = 0;		/* set at runtime */
	send->hdr.stl.lrh.slid = htons(hfi->lid);

	/* BTH header */
	send->hdr.stl.bth.opcode = 0;
	send->hdr.stl.bth.bth_1 = 0;
	send->hdr.stl.bth.pkey = htons(FI_OPA1X_HFI1_DEFAULT_P_KEY);
	send->hdr.stl.bth.ecn = 0;
	send->hdr.stl.bth.qp = hfi->bthqp;
	send->hdr.stl.bth.unused = 0;
	send->hdr.stl.bth.rx = 0;		/* set at runtime */

	send->hdr.reliability.psn = 0;
	send->hdr.reliability.origin_tx = hfi->send_ctxt;

	/* KDETH header */
	send->hdr.stl.kdeth.offset_ver_tid = KDETH_VERSION << FI_OPA1X_HFI1_KHDR_KVER_SHIFT;	/* no flags */
	send->hdr.stl.kdeth.jkey = hfi->jkey;
	send->hdr.stl.kdeth.hcrc = 0;
	send->hdr.stl.kdeth.unused = 0;

	/* OFI header */
	send->hdr.match.origin_cx = origin_cx;
	send->hdr.match.ofi_data = 0;		/* set at runtime */
	send->hdr.match.ofi_tag = 0;		/* set at runtime */


	/*
	 * fi_send*() model - rendezvous
	 */
	*rendezvous = *send;
	rendezvous->hdr.rendezvous.origin_rs = reliability_rx;


	/*
	 * fi_inject() model
	 */
	const uint32_t inject_pbc_dws =
		2 +	/* pbc */
		2 +	/* lhr */
		3 +	/* bth */
		9;	/* kdeth; from "RcvHdrSize[i].HdrSize" CSR */

	inject->qw0 = (0 |
		inject_pbc_dws /* length_dws */ |
		((hfi->vl & FI_OPA1X_HFI1_PBC_VL_MASK) << FI_OPA1X_HFI1_PBC_VL_SHIFT) |
		(((hfi->sc >> FI_OPA1X_HFI1_PBC_SC4_SHIFT) & FI_OPA1X_HFI1_PBC_SC4_MASK) << FI_OPA1X_HFI1_PBC_DCINFO_SHIFT));

	/* clone from send model, then adjust */
	inject->hdr = send->hdr;

	/* does not include pbc (8 bytes), but does include icrc (4 bytes) */
	inject->hdr.stl.lrh.pktlen = htons(inject_pbc_dws-1);

	/* specified at runtime */
	inject->hdr.inject.message_length = 0;
	inject->hdr.inject.app_data_u64[0] = 0;
	inject->hdr.inject.app_data_u64[1] = 0;
}


int fi_opa1x_stx_init (struct fi_opa1x_domain *opa1x_domain, struct fi_tx_attr *attr,
		struct fi_opa1x_stx *opa1x_stx, void *context) {

	opa1x_stx->stx_fid.fid.fclass = FI_CLASS_STX_CTX;
	opa1x_stx->stx_fid.fid.context= context;
	opa1x_stx->stx_fid.fid.ops    = &fi_opa1x_stx_ops;
	opa1x_stx->stx_fid.ops 	    = &fi_opa1x_stx_ep_ops;

	opa1x_stx->domain = opa1x_domain;

	opa1x_stx->attr = attr ? *attr : *fi_opa1x_global.default_tx_attr;


	/*
	 * open the hfi1 context
	 */
	opa1x_stx->hfi = fi_opa1x_hfi1_context_open(opa1x_domain->unique_job_key);

	/*
	 * initialize the reliability service
	 */
	fi_opa1x_reliability_tx_init(&opa1x_stx->reliability.state, opa1x_stx->hfi->send_ctxt, &opa1x_domain->reliability_service);

	/*
	 * initialize the models
	 */
	fi_opa1x_ep_tx_model_init(opa1x_stx->hfi, opa1x_domain->reliability_rx, (uint8_t)-1,
		&opa1x_stx->model.inject, &opa1x_stx->model.send, &opa1x_stx->model.rzv);

	fi_opa1x_ref_inc(&opa1x_domain->ref_cnt, "domain");
	fi_opa1x_ref_init(&opa1x_stx->ref_cnt, "shared transmit context");

	//fprintf(stderr, "%s():%u inject scb model @ %p; pbc = 0x%016lx\n", __func__, __LINE__, &opa1x_stx->model.inject, opa1x_stx->model.inject.qw0);
	//fi_opa1x_hfi1_dump_stl_packet_hdr(&opa1x_stx->model.inject.hdr.stl, __func__, __LINE__);

	//fprintf(stderr, "%s():%u   send scb model @ %p; pbc = 0x%016lx\n", __func__, __LINE__, &opa1x_stx->model.send, opa1x_stx->model.send.qw0);
	//fi_opa1x_hfi1_dump_stl_packet_hdr(&opa1x_stx->model.send.hdr.stl, __func__, __LINE__);

	//fprintf(stderr, "%s():%u    rzv scb model @ %p; pbc = 0x%016lx\n", __func__, __LINE__, &opa1x_stx->model.rzv, opa1x_stx->model.rzv.qw0);
	//fi_opa1x_hfi1_dump_stl_packet_hdr(&opa1x_stx->model.rzv.hdr.stl, __func__, __LINE__);

	return 0;
}

int fi_opa1x_stx_context(struct fid_domain *domain, struct fi_tx_attr *attr,
	       struct fid_stx **stx, void *context)
{
	int ret;
	errno = 0;
	struct fi_opa1x_stx *opa1x_stx = NULL;
	struct fi_opa1x_domain *opa1x_domain =
		container_of(domain, struct fi_opa1x_domain, domain_fid);

	if (!domain || !stx) {
		errno = FI_EINVAL;
		return -errno;
	}

	ret = fi_opa1x_fid_check(&domain->fid, FI_CLASS_DOMAIN, "domain");
	if (ret)
		return ret;

	opa1x_stx = calloc(1, sizeof *opa1x_stx);
	if (!opa1x_stx) {
		errno = FI_ENOMEM;
		goto err;
	}

	if (fi_opa1x_stx_init(opa1x_domain, attr, opa1x_stx, context)) {
		errno = FI_EOTHER;
		goto err;
	}

	*stx = &opa1x_stx->stx_fid;
	return FI_SUCCESS;

err:
	if (opa1x_stx) {
		free(opa1x_stx);
		opa1x_stx = NULL;
	}
	return -errno;
}

static int fi_opa1x_close_ep(fid_t fid)
{ 
	FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_EP_DATA, "close ep\n");

	if (!fid) {
		FI_LOG(fi_opa1x_global.prov, FI_LOG_DEBUG, FI_LOG_FABRIC,
				"NULL ep object");
		errno = FI_EINVAL;
		return -errno;
	}

	if (fid->fclass != FI_CLASS_EP &&
			fid->fclass != FI_CLASS_TX_CTX &&
			fid->fclass != FI_CLASS_RX_CTX) {
		FI_LOG(fi_opa1x_global.prov, FI_LOG_DEBUG, FI_LOG_FABRIC,
			"wrong type of object. expected (FI_CLASS_EP), got (%zu)\n",
			fid->fclass);
		errno = FI_EINVAL;
		return -errno;
	}

	int ret;
	struct fi_opa1x_ep *opa1x_ep = container_of(fid, struct fi_opa1x_ep, ep_fid);

#ifdef LOCAL_COMM_ENABLED
	/* Disable SHM */
	fi_opa1x_ep_tx_shm_finalize(opa1x_ep);
#endif

	ret = fi_opa1x_ref_dec(&opa1x_ep->domain->ref_cnt, "domain");
	if (ret)
		return ret;

	/* av is only valid/required if tx capability is enabled */
	if (opa1x_ep->av) {
		ret = fi_opa1x_ref_dec(&opa1x_ep->av->ref_cnt, "address vector");
		if (ret) return ret;
	}

	if (opa1x_ep->sep) {
		ret = fi_opa1x_ref_dec(&opa1x_ep->sep->ref_cnt, "scalable endpoint");
		if (ret) return ret;
	}

	if (opa1x_ep->tx.stx) {
		ret = fi_opa1x_ref_dec(&opa1x_ep->tx.stx->ref_cnt, "shared tx context");
		if (ret) return ret;

		if (opa1x_ep->tx.stx == &opa1x_ep->tx.exclusive_stx) {
			ret = fi_opa1x_close_stx_nofree(opa1x_ep->tx.stx);
			if (ret) return ret;
		}
		opa1x_ep->tx.stx = NULL;
	} else {
		fi_opa1x_reliability_tx_fini(opa1x_ep->tx.reliability_state);
	}

	if (opa1x_ep->tx.cq) {
		ret = fi_opa1x_ref_dec(&opa1x_ep->tx.cq->ref_cnt, "completion queue");
		if (ret) return ret;
	}
	if (opa1x_ep->rx.cq) {
		ret = fi_opa1x_ref_dec(&opa1x_ep->rx.cq->ref_cnt, "completion queue");
		if (ret) return ret;
	}

	if (ofi_recv_allowed(opa1x_ep->rx.caps) ||
		ofi_rma_target_allowed(opa1x_ep->rx.caps)) {
		fi_opa1x_reliability_rx_fini(&opa1x_ep->rx.reliability.state);
	}

	fi_opa1x_finalize_cm_ops(&opa1x_ep->ep_fid.fid);
	fi_opa1x_finalize_msg_ops(&opa1x_ep->ep_fid);
	fi_opa1x_finalize_rma_ops(&opa1x_ep->ep_fid);
	fi_opa1x_finalize_tagged_ops(&opa1x_ep->ep_fid);
	fi_opa1x_finalize_atomic_ops(&opa1x_ep->ep_fid);

	void *mem = opa1x_ep->mem;
	free(mem);

	FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_EP_DATA, "ep closed\n");

	return 0;
}

static int fi_opa1x_bind_ep(struct fid *fid, struct fid *bfid,
		uint64_t flags)
{
	if (!bfid) return 0;

	int ret = 0;
	struct fi_opa1x_ep *opa1x_ep = container_of(fid, struct fi_opa1x_ep, ep_fid);

	switch (bfid->fclass) {
	case FI_CLASS_CNTR:
		ret = fi_opa1x_bind_ep_cntr(&opa1x_ep->ep_fid,
				container_of(bfid, struct fid_cntr, fid), flags);
		if (ret)
			goto err;
		break;
	case FI_CLASS_CQ:
		ret = fi_opa1x_bind_ep_cq(&opa1x_ep->ep_fid,
				container_of(bfid, struct fid_cq, fid), flags);
		if (ret)
			goto err;
		break;
	case FI_CLASS_AV:
		ret = fi_opa1x_bind_ep_av(&opa1x_ep->ep_fid,
				container_of(bfid, struct fid_av, fid), flags);
		if (ret)
			goto err;
		break;
	case FI_CLASS_MR:
		ret = fi_opa1x_bind_ep_mr(&opa1x_ep->ep_fid,
				container_of(bfid, struct fid_mr, fid), flags);
		if (ret)
			goto err;
		break;
	case FI_CLASS_STX_CTX:
		ret = fi_opa1x_bind_ep_stx(&opa1x_ep->ep_fid,
				container_of(bfid, struct fid_stx, fid),
				flags);
		if (ret)
			goto err;
		break;
	default:
		errno = FI_ENOSYS;
		goto err;
	}
	return ret;
err:
	return -errno;
}

static int fi_opa1x_check_ep(struct fi_opa1x_ep *opa1x_ep)
{


	switch (opa1x_ep->ep_fid.fid.fclass) {
	case FI_CLASS_EP:
		if (!opa1x_ep->av) {
			FI_DBG(fi_opa1x_global.prov, FI_LOG_EP_DATA, "no AV supplied");
			goto err;
		}
		break;
	case FI_CLASS_RX_CTX:
	case FI_CLASS_TX_CTX:
		if (!opa1x_ep->sep) {
			FI_DBG(fi_opa1x_global.prov, FI_LOG_EP_DATA, "no scalable endpoint supplied");
			goto err;
		}
		opa1x_ep->av = opa1x_ep->sep->av;
		if (!opa1x_ep->sep->av) {
			FI_DBG(fi_opa1x_global.prov, FI_LOG_EP_DATA, "no AV supplied on scalable endpoint");
			goto err;
		}
		break;
	default:
		FI_DBG(fi_opa1x_global.prov, FI_LOG_EP_DATA,
				"Invalid EP class %lu",
				opa1x_ep->ep_fid.fid.fclass);
		goto err;
	}

	if (!opa1x_ep->domain) {
		FI_DBG(fi_opa1x_global.prov, FI_LOG_EP_DATA, "no domain supplied\n");
		goto err;
	}

	return 0;
err:
	errno = FI_EINVAL;
	return -errno;
}

static int fi_opa1x_ep_tx_init (struct fi_opa1x_ep *opa1x_ep,
		struct fi_opa1x_domain *opa1x_domain)
{
	FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_EP_DATA, "tx init\n");

	assert(opa1x_ep);
	assert(opa1x_domain);
	//assert(opa1x_ep->tx.state == FI_OPA1X_EP_UNINITIALIZED);

	struct fi_opa1x_hfi1_context * hfi = NULL;
	if (opa1x_ep->tx.stx) {

		assert(opa1x_domain == opa1x_ep->tx.stx->domain);

		hfi = opa1x_ep->tx.stx->hfi;

		opa1x_ep->tx.reliability_state = &opa1x_ep->tx.stx->reliability.state;

		/* the 'state' fields will change after every tx operation -
		 * and may be shared between multiple ofi tx contexts */
		opa1x_ep->tx.pio_state = &hfi->state.pio;

		/*
		 * initialize the models from the shared transmit context; must
		 * specialize these models with the "origin endpoint base rx"!
		 */

		opa1x_ep->tx.inject = opa1x_ep->tx.stx->model.inject;
		opa1x_ep->tx.send = opa1x_ep->tx.stx->model.send;
		opa1x_ep->tx.rzv = opa1x_ep->tx.stx->model.rzv;

	} else {
		/*
		 * A shared transmit context was not provided; create an
		 * "exclusive" shared transmit context for use by only this
		 * endpoint transmit context
		 */

		hfi = opa1x_ep->hfi;

		opa1x_ep->tx.reliability_state = &opa1x_ep->tx.reliability_state_exclusive;
		fi_opa1x_reliability_tx_init(opa1x_ep->tx.reliability_state,
			hfi->send_ctxt,
			&opa1x_domain->reliability_service);

		/*
		 * the 'state' fields will change after every tx operation;
		 * ok to copy from the hfi object because this tx context is
		 * not shared
		 */
		opa1x_ep->tx.pio_state_exclusive.qw0 = hfi->state.pio.qw0;
		opa1x_ep->tx.pio_state = &opa1x_ep->tx.pio_state_exclusive;

		/* initialize the models */
		fi_opa1x_ep_tx_model_init(hfi,
			opa1x_ep->reliability_rx,
			opa1x_ep->cx,
			&opa1x_ep->tx.inject,
			&opa1x_ep->tx.send,
			&opa1x_ep->tx.rzv);
	}

	opa1x_ep->tx.inject.hdr.reliability.origin_reliability_rx = opa1x_ep->reliability_rx;
	opa1x_ep->tx.inject.hdr.match.origin_cx = opa1x_ep->cx;


	opa1x_ep->tx.rzv.hdr.reliability.origin_reliability_rx = opa1x_ep->reliability_rx;
	opa1x_ep->tx.rzv.hdr.match.origin_cx = opa1x_ep->cx;
	opa1x_ep->tx.rzv.hdr.rendezvous.origin_rx = hfi->info.rxe.id;

	// these 3 lines should move to ep init ?
	opa1x_ep->threading = (uint32_t) opa1x_domain->threading;
	opa1x_ep->av_type = (uint32_t) opa1x_ep->av->type;
	opa1x_ep->mr_mode = (uint32_t) opa1x_domain->mr_mode;

	/* the 'state' fields will change after every tx operation - and may be
	 * shared between multiple ofi tx contexts */

	/* the 'info' fields do not change; the values can be safely copied */
	opa1x_ep->tx.pio_scb_sop_first = hfi->info.pio.scb_sop_first;
	opa1x_ep->tx.pio_scb_first = hfi->info.pio.scb_first;
	opa1x_ep->tx.pio_credits_addr = hfi->info.pio.credits_addr;

#ifdef LOCAL_COMM_ENABLED
	fi_opa1x_ep_tx_shm_init(opa1x_ep);
#endif

	FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_EP_DATA, "tx init'd\n");
	return 0;
}

static int fi_opa1x_ep_rx_init (struct fi_opa1x_ep *opa1x_ep)
{
	FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_EP_DATA, "rx init\n");
	//assert(FI_SHARED_CONTEXT != opa1x_ep->rx.index);

	struct fi_opa1x_domain * opa1x_domain = opa1x_ep->domain;

	/*
	 * open the hfi1 context
	 */
	struct fi_opa1x_hfi1_context * hfi1 = opa1x_ep->hfi;
	init_hfi1_rxe_state(hfi1, &opa1x_ep->rx.state);

	/*
	 * COPY the rx static information from the hfi context structure.
	 * This is to improve cache layout.
	 */
	opa1x_ep->rx.hdrq.rhf_base = hfi1->info.rxe.hdrq.rhf_base;
	opa1x_ep->rx.hdrq.head_register = hfi1->info.rxe.hdrq.head_register;
	opa1x_ep->rx.egrq.base_addr = hfi1->info.rxe.egrq.base_addr;
	opa1x_ep->rx.egrq.elemsz = hfi1->info.rxe.egrq.elemsz;
	opa1x_ep->rx.egrq.last_egrbfr_index = 0;
	opa1x_ep->rx.egrq.head_register = hfi1->info.rxe.egrq.head_register;

	/*
	 * initialize the reliability service
	 */
	fi_opa1x_reliability_rx_init(&opa1x_ep->rx.reliability.state, hfi1->info.rxe.id,
		&opa1x_domain->reliability_service,
		fi_opa1x_ep_rx_reliability_process_packet);

	opa1x_ep->rx.tx.reliability_state = &opa1x_ep->tx.reliability_state_exclusive;


	opa1x_ep->rx.self.raw64b = 0;
	opa1x_ep->rx.self.uid.lid = htons(hfi1->lid);
	opa1x_ep->rx.self.hfi1_rx = hfi1->info.rxe.id;
	opa1x_ep->rx.self.uid.endpoint_id = opa1x_ep->cx;
	opa1x_ep->rx.self.rx_index = 0;
	opa1x_ep->rx.self.unused = 0;
	opa1x_ep->rx.self.reliability_rx = opa1x_domain->reliability_rx;

	//FI_OPA1X_ADDR_DUMP(&opa1x_ep->rx.self.fi);

	opa1x_ep->rx.slid = opa1x_ep->rx.self.uid.lid;	/* copied for better cache layout */

	/*
	 * initialize tx for acks, etc
	 */

	/* the 'state' fields will change after every tx operation */
	opa1x_ep->rx.tx.pio_state = &opa1x_ep->tx.pio_state_exclusive;

	/* the 'info' fields do not change; the values can be safely copied */
	opa1x_ep->rx.tx.pio_scb_sop_first = hfi1->info.pio.scb_sop_first;
	opa1x_ep->rx.tx.pio_scb_first = hfi1->info.pio.scb_first;
	opa1x_ep->rx.tx.pio_credits_addr = hfi1->info.pio.credits_addr;
	{	/* rendezvous CTS packet model */

		/* PBC data */
		opa1x_ep->rx.tx.cts.qw0 = (0 |
			0 /* length_dws */ |
			((hfi1->vl & FI_OPA1X_HFI1_PBC_VL_MASK) << FI_OPA1X_HFI1_PBC_VL_SHIFT) |
			(((hfi1->sc >> FI_OPA1X_HFI1_PBC_SC4_SHIFT) & FI_OPA1X_HFI1_PBC_SC4_MASK) << FI_OPA1X_HFI1_PBC_DCINFO_SHIFT));

		/* LRH header */
		opa1x_ep->rx.tx.cts.hdr.stl.lrh.flags =
			htons(FI_OPA1X_HFI1_LRH_BTH |
				((hfi1->sl & FI_OPA1X_HFI1_LRH_SL_MASK) << FI_OPA1X_HFI1_LRH_SL_SHIFT) |
				((hfi1->sc & FI_OPA1X_HFI1_LRH_SC_MASK) << FI_OPA1X_HFI1_LRH_SC_SHIFT));

		opa1x_ep->rx.tx.cts.hdr.stl.lrh.dlid = 0;		/* set at runtime */
		opa1x_ep->rx.tx.cts.hdr.stl.lrh.pktlen = 0;		/* set at runtime */
		opa1x_ep->rx.tx.cts.hdr.stl.lrh.slid = htons(hfi1->lid);

		/* BTH header */
		opa1x_ep->rx.tx.cts.hdr.stl.bth.opcode = FI_OPA1X_HFI_BTH_OPCODE_RZV_CTS;
		opa1x_ep->rx.tx.cts.hdr.stl.bth.bth_1 = 0;
		opa1x_ep->rx.tx.cts.hdr.stl.bth.pkey = htons(FI_OPA1X_HFI1_DEFAULT_P_KEY);
		opa1x_ep->rx.tx.cts.hdr.stl.bth.ecn = 0;
		opa1x_ep->rx.tx.cts.hdr.stl.bth.qp = hfi1->bthqp;
		opa1x_ep->rx.tx.cts.hdr.stl.bth.unused = 0;
		opa1x_ep->rx.tx.cts.hdr.stl.bth.rx = 0;		/* set at runtime */

		opa1x_ep->rx.tx.cts.hdr.reliability.psn = 0;
		opa1x_ep->rx.tx.cts.hdr.reliability.origin_tx = hfi1->send_ctxt;

		/* KDETH header */
		opa1x_ep->rx.tx.cts.hdr.stl.kdeth.offset_ver_tid = KDETH_VERSION << FI_OPA1X_HFI1_KHDR_KVER_SHIFT;	/* no flags */
		opa1x_ep->rx.tx.cts.hdr.stl.kdeth.jkey = hfi1->jkey;
		opa1x_ep->rx.tx.cts.hdr.stl.kdeth.hcrc = 0;
		opa1x_ep->rx.tx.cts.hdr.stl.kdeth.unused = 0;

		/* OFI header */
		opa1x_ep->rx.tx.cts.hdr.cts.origin_rx = hfi1->info.rxe.id;
		opa1x_ep->rx.tx.cts.hdr.cts.origin_rs = opa1x_domain->reliability_rx;
	}

	{	/* rendezvous DPUT packet model */

		/* tagged model */
		opa1x_ep->rx.tx.dput = opa1x_ep->rx.tx.cts;
		opa1x_ep->rx.tx.dput.hdr.stl.bth.opcode = FI_OPA1X_HFI_BTH_OPCODE_RZV_DATA;
	}

	FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_EP_DATA, "rx init'd\n");
	return 0;
}

static int fi_opa1x_open_command_queues(struct fi_opa1x_ep *opa1x_ep)
{
	struct fi_opa1x_domain *opa1x_domain;

	if (!opa1x_ep) {
		errno = FI_EINVAL;
		return -errno;
	}

	opa1x_domain = opa1x_ep->domain;

	if (opa1x_ep->hfi != NULL) { FI_WARN(fi_opa1x_global.prov, FI_LOG_EP_DATA, "hfi context already initialized\n"); abort(); }

	opa1x_ep->hfi = fi_opa1x_hfi1_context_open(opa1x_domain->unique_job_key);

	FI_INFO(fi_opa1x_global.prov, FI_LOG_EP_DATA, "HFI1 PIO credits: %u\n", opa1x_ep->hfi->state.pio.credits_total);

	if (ofi_recv_allowed(opa1x_ep->rx.caps) || ofi_rma_target_allowed(opa1x_ep->rx.caps)) {

		/* verify there is a completion queue associated with the rx context */
		if (!opa1x_ep->rx.cq) {
			FI_WARN(fi_opa1x_global.prov, FI_LOG_EP_DATA,
				"No completion queue bound to receive context");
			goto err;
		}

		if (0 != fi_opa1x_ep_rx_init(opa1x_ep)) {
			FI_WARN(fi_opa1x_global.prov, FI_LOG_EP_DATA,
				"Error during rx context initialization");
			goto err;
		}
	}

	if (ofi_send_allowed(opa1x_ep->tx.caps) || ofi_rma_initiate_allowed(opa1x_ep->tx.caps)) {

		/* verify there is a completion queue associated with the tx context */
		if (!opa1x_ep->tx.cq) {
			FI_LOG(fi_opa1x_global.prov, FI_LOG_DEBUG, FI_LOG_EP_DATA,
				"No completion queue bound to send context");
			goto err;
		}

		/* verify there is a shared tx context associated with the endpoint - if so configured */
//		if (FI_SHARED_CONTEXT == opa1x_ep->tx.index && !opa1x_ep->tx.stx) {
//			FI_LOG(fi_opa1x_global.prov, FI_LOG_DEBUG, FI_LOG_EP_DATA,
//				"No shared tx context bound to endpoint as configured");
//			goto err;
//		}


		if (fi_opa1x_ep_tx_init(opa1x_ep, opa1x_domain)) {
			FI_WARN(fi_opa1x_global.prov, FI_LOG_EP_DATA,
				"Too many tx contexts");
			goto err;
		}
	}

	return 0;
err:
	return -1;
}

static int fi_opa1x_enable_ep(struct fid_ep *ep)
{
	FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_EP_DATA, "enable ep\n");

	int ret;
	struct fi_opa1x_ep *opa1x_ep = container_of(ep, struct fi_opa1x_ep, ep_fid);
	ret = fi_opa1x_check_ep(opa1x_ep);
	if (ret) {
		FI_WARN(fi_opa1x_global.prov, FI_LOG_EP_DATA,
				"ep enable failed\n");
		return -errno;
	}

	ret = fi_opa1x_open_command_queues(opa1x_ep);
	if (ret) {
		FI_WARN(fi_opa1x_global.prov, FI_LOG_EP_DATA,
				"failed to assign command queues\n");
		return -errno;
	}

	ret = fi_opa1x_enable_msg_ops(ep);
	if (ret) {
		FI_WARN(fi_opa1x_global.prov, FI_LOG_EP_DATA,
			"failed to enable msg ops\n");
		return -errno;
	}

	ret = fi_opa1x_enable_rma_ops(ep);
	if (ret) {
		FI_WARN(fi_opa1x_global.prov, FI_LOG_EP_DATA,
			"failed to enable rma ops\n");
		return -errno;
	}

	ret = fi_opa1x_enable_atomic_ops(ep);
	if (ret) {
		FI_WARN(fi_opa1x_global.prov, FI_LOG_EP_DATA,
			"failed to enable rma ops\n");
		return -errno;
	}

	ret = fi_opa1x_enable_tagged_ops(ep);
	if (ret) {
		FI_WARN(fi_opa1x_global.prov, FI_LOG_EP_DATA,
			"failed to enable rma ops\n");
		return -errno;
	}

	opa1x_ep->state = FI_OPA1X_EP_INITITALIZED_ENABLED;

	FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_EP_DATA, "ep enabled\n");
	return 0;
}

static int fi_opa1x_control_ep(fid_t fid, int command, void *arg)
{
	struct fid_ep *ep;
	ep = container_of(fid, struct fid_ep, fid);

	switch (command) {
	case FI_ENABLE:
		return fi_opa1x_enable_ep(ep);
	default:
		return -FI_ENOSYS;
	}

	return 0;
}

static int fi_opa1x_getopt_ep(fid_t fid, int level, int optname,
			void *optval, size_t *optlen)
{
	struct fi_opa1x_ep *opa1x_ep = container_of(fid, struct fi_opa1x_ep, ep_fid);

	if (level != FI_OPT_ENDPOINT)
		return -FI_ENOPROTOOPT;

	switch (optname) {
	case FI_OPT_MIN_MULTI_RECV:
		*(size_t *)optval = opa1x_ep->rx.min_multi_recv;
		*optlen = sizeof(size_t);
		break;
	case FI_OPT_CM_DATA_SIZE:
		*(size_t *)optval = 0;
		*optlen = sizeof(size_t);
		break;
	default:
		return -FI_ENOPROTOOPT;
	}

	return 0;
}

static int fi_opa1x_setopt_ep(fid_t fid, int level, int optname,
			const void *optval, size_t optlen)
{
	struct fi_opa1x_ep *opa1x_ep = container_of(fid, struct fi_opa1x_ep, ep_fid);

	if (level != FI_OPT_ENDPOINT)
		return -FI_ENOPROTOOPT;

	switch (optname) {
	case FI_OPT_MIN_MULTI_RECV:
		opa1x_ep->rx.min_multi_recv = *(size_t *)optval;
		//opa1x_ep->rx.min_multi_recv = opa1x_ep->rx.min_multi_recv;
		break;

	default:
		return -FI_ENOPROTOOPT;
	}

	return 0;
}


int fi_opa1x_ep_rx_cancel (struct fi_opa1x_ep_rx * rx,
		const enum fi_opa1x_ep_rx_queue_kind kind,
		const union fi_opa1x_context * cancel_context,
		const int lock_required) {

	FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_EP_DATA, "(begin)\n");

	/*
	 * search the match queue for this context
	 */

	union fi_opa1x_context * prev = NULL;
	union fi_opa1x_context * item = rx->queue[kind].mq.head;
	while (item) {

		const uint64_t is_context_ext = item->flags & FI_OPA1X_CQ_CONTEXT_EXT;
		const uint64_t compare_context = is_context_ext ?
			(uint64_t)(((struct fi_opa1x_context_ext *)item)->msg.op_context) :
			(uint64_t)item;

		if ((uintptr_t)cancel_context == compare_context) {
			if (prev)
				prev->next = item->next;
			else
				rx->queue[kind].mq.head = item->next;

			if (!item->next)
				rx->queue[kind].mq.tail = prev;

			struct fi_opa1x_context_ext * ext = NULL;
			if (cancel_context->flags & FI_OPA1X_CQ_CONTEXT_EXT) {
				ext = (struct fi_opa1x_context_ext *)cancel_context;
			} else {
				posix_memalign((void**)&ext, 32, sizeof(struct fi_opa1x_context_ext));
				ext->opa1x_context.flags = FI_OPA1X_CQ_CONTEXT_EXT;
			}

			ext->opa1x_context.byte_counter = 0;
			ext->opa1x_context.next = NULL;
			ext->err_entry.op_context = (void *)cancel_context;
			ext->err_entry.flags = cancel_context->flags;
			ext->err_entry.len = 0;
			ext->err_entry.buf = 0;
			ext->err_entry.data = 0;
			ext->err_entry.tag = cancel_context->tag;
			ext->err_entry.olen = 0;
			ext->err_entry.err = FI_ECANCELED;
			ext->err_entry.prov_errno = 0;
			ext->err_entry.err_data = NULL;

			if (lock_required) { fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__); abort(); }
			fi_opa1x_context_slist_insert_tail((union fi_opa1x_context*)ext, rx->cq_err_ptr);

			FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_EP_DATA, "(end) canceled\n");
			return FI_ECANCELED;
		}

		prev = item;
		item = item->next;
	}	

	/* context not found in 'kind' match queue */
	FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_EP_DATA, "(end) not found\n");
	return 0;
}




static
ssize_t fi_opa1x_cancel(fid_t fid, void *context)
{
	struct fi_opa1x_ep *opa1x_ep = container_of(fid, struct fi_opa1x_ep, ep_fid);

	if (IS_PROGRESS_MANUAL(opa1x_ep->domain)) {
		const enum fi_threading threading = opa1x_ep->domain->threading;
		const int lock_required =
			(threading == FI_THREAD_FID) ||
			(threading == FI_THREAD_UNSPEC) ||
			(threading == FI_THREAD_SAFE);

		if (opa1x_ep->rx.caps & FI_MSG) {
			fi_opa1x_ep_rx_cancel(&opa1x_ep->rx,
				FI_OPA1X_EP_RX_QUEUE_KIND_MSG,
				(const union fi_opa1x_context *) context,
				lock_required);
				
		}

		if (opa1x_ep->rx.caps & FI_TAGGED) {
			fi_opa1x_ep_rx_cancel(&opa1x_ep->rx,
				FI_OPA1X_EP_RX_QUEUE_KIND_TAG,
				(const union fi_opa1x_context *) context,
				lock_required);
		}

	} else {
		abort();
	}

	return 0;
}

static struct fi_ops fi_opa1x_fi_ops = {
	.size		= sizeof(struct fi_ops),
	.close		= fi_opa1x_close_ep,
	.bind		= fi_opa1x_bind_ep,
	.control	= fi_opa1x_control_ep,
	.ops_open	= fi_no_ops_open
};

static struct fi_ops_ep fi_opa1x_ep_ops = {
	.size		= sizeof(struct fi_ops_ep),
	.cancel		= fi_opa1x_cancel,
	.getopt		= fi_opa1x_getopt_ep,
	.setopt		= fi_opa1x_setopt_ep,
	.tx_ctx		= fi_no_tx_ctx,
	.rx_ctx		= fi_no_rx_ctx,
	.rx_size_left   = fi_no_rx_size_left,
	.tx_size_left   = fi_no_tx_size_left
};

int fi_opa1x_alloc_default_rx_attr(struct fi_rx_attr **rx_attr)
{
	struct fi_rx_attr *attr;

	attr = calloc(1, sizeof(*attr));
	if (!attr)
		goto err;

	attr->caps 	= FI_MSG | FI_RMA | FI_TAGGED | FI_ATOMIC | FI_RECV | FI_REMOTE_READ | FI_REMOTE_WRITE | FI_NAMED_RX_CTX | FI_DIRECTED_RECV | FI_MULTI_RECV | FI_SOURCE;
	attr->mode 	= FI_CONTEXT2 | FI_ASYNC_IOV;
	attr->op_flags 	= 0;
	attr->msg_order = FI_OPA1X_DEFAULT_MSG_ORDER;
	attr->comp_order = FI_ORDER_NONE;
	attr->total_buffered_recv = FI_OPA1X_HFI1_PACKET_MTU;//FI_OPA1X_TOTAL_BUFFERED_RECV;
	attr->size 	= SIZE_MAX; //FI_OPA1X_RX_SIZE;
	attr->iov_limit = SIZE_MAX;

	*rx_attr = attr;

	return 0;
err:
	errno = FI_EINVAL;
	return -errno;
}

int fi_opa1x_check_rx_attr(struct fi_rx_attr *attr)
{
	/* TODO: more error checking of rx_attr */
#ifdef TODO
	if (attr->total_buffered_recv > FI_OPA1X_TOTAL_BUFFERED_RECV) {
		FI_LOG(fi_opa1x_global.prov, FI_LOG_DEBUG, FI_LOG_EP_DATA,
				"unavailable [bad total_buffered_recv (%lu)]",
				attr->total_buffered_recv);
		goto err;
	}
#endif
	if (attr->comp_order && attr->comp_order == FI_ORDER_STRICT) {
		FI_WARN(fi_opa1x_global.prov, FI_LOG_EP_DATA,
				"unavailable [bad rx comp_order (%lx)] ",
				attr->comp_order);
		goto err;
	}

	return 0;
err:
	errno = FI_EINVAL;
	return -errno;
}

int fi_opa1x_alloc_default_tx_attr(struct fi_tx_attr **tx_attr)
{
	struct fi_tx_attr *attr;

	attr = calloc(1, sizeof(*attr));
	if (!attr)
		goto err;

	attr->caps	= FI_MSG | FI_RMA | FI_TAGGED | FI_ATOMIC | FI_SEND | FI_READ | FI_WRITE;
	attr->mode	= FI_CONTEXT2 | FI_ASYNC_IOV;
	attr->op_flags	= FI_TRANSMIT_COMPLETE;
	attr->msg_order	= FI_OPA1X_DEFAULT_MSG_ORDER;
	attr->comp_order = FI_ORDER_NONE;
	attr->inject_size = FI_OPA1X_HFI1_PACKET_IMM;//FI_OPA1X_INJECT_SIZE;
	attr->size	= SIZE_MAX; //FI_OPA1X_TX_SIZE;
	attr->iov_limit = SIZE_MAX;
	attr->rma_iov_limit = 1;

	*tx_attr = attr;

	return 0;
err:
	errno = FI_EINVAL;
	return -errno;
}

int fi_opa1x_check_tx_attr(struct fi_tx_attr *attr)
{
	if (attr->inject_size > FI_OPA1X_HFI1_PACKET_IMM) {
		FI_LOG(fi_opa1x_global.prov, FI_LOG_DEBUG, FI_LOG_EP_DATA,
				"unavailable [bad inject_size (%lu)]",
				attr->inject_size);
		goto err;
	}
	/* TODO: more error checking of tx_attr */

	if (attr->comp_order && attr->comp_order == FI_ORDER_STRICT) {
		FI_LOG(fi_opa1x_global.prov, FI_LOG_DEBUG, FI_LOG_EP_DATA,
				"unavailable [bad tx comp_order (%lx)] ",
				attr->comp_order);
		goto err;
       }

	return 0;
err:
	errno = FI_EINVAL;
	return -errno;
}

int fi_opa1x_alloc_default_ep_attr(struct fi_ep_attr **ep_attr)
{
	struct fi_ep_attr *attr;

	attr = calloc(1, sizeof(*attr));
	if (!attr)
		goto err;

	attr->type		= FI_EP_RDM;
	attr->protocol		= FI_OPA1X_PROTOCOL;
	attr->protocol_version	= FI_OPA1X_PROTOCOL_VERSION;
	attr->max_msg_size	= FI_OPA1X_MAX_MSG_SIZE;
	attr->msg_prefix_size 	= 0;
	attr->max_order_raw_size= FI_OPA1X_MAX_ORDER_RAW_SIZE;
	attr->max_order_war_size= FI_OPA1X_MAX_ORDER_WAR_SIZE;
	attr->max_order_waw_size= FI_OPA1X_MAX_ORDER_WAW_SIZE;
	attr->mem_tag_format 	= FI_OPA1X_MEM_TAG_FORMAT;
	attr->tx_ctx_cnt	= 1;//tx_ctx_cnt;
	attr->rx_ctx_cnt	= 1;//rx_ctx_cnt;

	*ep_attr = attr;

	return 0;
err:
	errno = FI_EINVAL;
	return -errno;
}

int fi_opa1x_check_ep_attr(struct fi_ep_attr *attr)
{
	switch(attr->protocol) {
		case FI_PROTO_UNSPEC:
		case FI_OPA1X_PROTOCOL:
			break;
		default:
			FI_LOG(fi_opa1x_global.prov, FI_LOG_DEBUG, FI_LOG_EP_DATA,
					"unavailable [bad protocol (%u)]",
					attr->protocol);
			goto err;
	}
	if (attr->max_msg_size > FI_OPA1X_MAX_MSG_SIZE) {
		FI_LOG(fi_opa1x_global.prov, FI_LOG_DEBUG, FI_LOG_EP_DATA,
				"unavailable [bad max_msg_size (%lu)]",
				attr->max_msg_size);
		goto err;
	}
	if (attr->max_order_raw_size > FI_OPA1X_MAX_ORDER_RAW_SIZE) {
		FI_LOG(fi_opa1x_global.prov, FI_LOG_DEBUG, FI_LOG_EP_DATA,
				"unavailable [bad max_order_raw_size (%lu)",
				attr->max_order_raw_size);
		goto err;
	}
	if (attr->max_order_war_size > FI_OPA1X_MAX_ORDER_WAR_SIZE) {
		FI_LOG(fi_opa1x_global.prov, FI_LOG_DEBUG, FI_LOG_EP_DATA,
				"unavailable [bad max_order_war_size (%lu)",
				attr->max_order_war_size);
		goto err;
	}
	if (attr->max_order_waw_size > FI_OPA1X_MAX_ORDER_WAW_SIZE) {
		FI_LOG(fi_opa1x_global.prov, FI_LOG_DEBUG, FI_LOG_EP_DATA,
				"unavailable [bad max_order_waw_size (%lu)",
				attr->max_order_waw_size);
		goto err;
	}
	if (attr->mem_tag_format &&
			attr->mem_tag_format & ~FI_OPA1X_MEM_TAG_FORMAT) {
		FI_LOG(fi_opa1x_global.prov, FI_LOG_DEBUG, FI_LOG_EP_DATA,
				"unavailable [bad mem_tag_format (%lx)",
				attr->mem_tag_format);
		goto err;
	}
	/* TODO: what msg orders do we not support? */

	return 0;
err:
	errno = FI_EINVAL;
	return -errno;
}

int fi_opa1x_endpoint_rx_tx (struct fid_domain *dom, struct fi_info *info,
		//struct fid_ep **ep, void *context, const ssize_t rx_index, const ssize_t tx_index, const uint8_t cx)
		struct fid_ep **ep, void *context, const uint8_t cx)
{
	FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_EP_DATA, "(begin)\n");

	int ret;
	struct fi_opa1x_ep *opa1x_ep = NULL;
	struct fi_opa1x_domain *opa1x_domain = NULL;

	if (!info || !dom) {
		FI_WARN(fi_opa1x_global.prov, FI_LOG_EP_DATA,
				"no info/domain supplied\n");
		errno = FI_EINVAL;
		goto err;
	}

FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_EP_DATA, "\n");

	ret = fi_opa1x_fid_check(&dom->fid, FI_CLASS_DOMAIN, "domain");
	if (ret) return ret;

FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_EP_DATA, "\n");

//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
	ret = fi_opa1x_check_info(info);
	if (ret)
		return ret;

FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_EP_DATA, "\n");

//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
	void *mem = NULL;
	mem = malloc(sizeof(struct fi_opa1x_ep) + FI_OPA1X_CACHE_LINE_SIZE);
	if (!mem) {
		FI_LOG(fi_opa1x_global.prov, FI_LOG_DEBUG, FI_LOG_EP_DATA,
				"no memory for endpoint");
		errno = FI_ENOMEM;
		goto err;
	}
	opa1x_ep = (struct fi_opa1x_ep *)(((uintptr_t)mem + FI_OPA1X_CACHE_LINE_SIZE) & ~(FI_OPA1X_CACHE_LINE_SIZE - 1));
	memset(opa1x_ep, 0, sizeof(struct fi_opa1x_ep));
	opa1x_ep->mem = mem;

FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_EP_DATA, "\n");

	opa1x_ep->ep_fid.fid.fclass  = FI_CLASS_EP;
	opa1x_ep->ep_fid.fid.context = context;
	opa1x_ep->ep_fid.fid.ops     = &fi_opa1x_fi_ops;
	opa1x_ep->ep_fid.ops 	   = &fi_opa1x_ep_ops;

	opa1x_domain = container_of(dom, struct fi_opa1x_domain, domain_fid);
	opa1x_ep->domain = opa1x_domain;

	/* set during bind of completion queue to the rx/tx */
//fprintf(stderr, "%s:%s():%d cq_completed_ptr set to NULL!\n", __FILE__, __func__, __LINE__);
	opa1x_ep->rx.cq = NULL;
	opa1x_ep->rx.cq_pending_ptr = NULL;
	opa1x_ep->rx.cq_completed_ptr = NULL;
	opa1x_ep->rx.cq_err_ptr = NULL;
	//opa1x_ep->rx.cq_lock_ptr = NULL;

	opa1x_ep->rx.queue[FI_OPA1X_EP_RX_QUEUE_KIND_TAG].ue.head = NULL;	
	opa1x_ep->rx.queue[FI_OPA1X_EP_RX_QUEUE_KIND_TAG].ue.tail = NULL;	
	opa1x_ep->rx.queue[FI_OPA1X_EP_RX_QUEUE_KIND_MSG].ue.head = NULL;	
	opa1x_ep->rx.queue[FI_OPA1X_EP_RX_QUEUE_KIND_MSG].ue.tail = NULL;	
//fprintf(stderr, "%s:%s():%d opa1x_ep = %p\n", __FILE__, __func__, __LINE__, opa1x_ep);
//fprintf(stderr, "%s:%s():%d &opa1x_ep->rx = %p\n", __FILE__, __func__, __LINE__, &opa1x_ep->rx);
	//fprintf(stderr, "%s:%s():%d &opa1x_ep->rx.queue[%d].ue = %p\n", __FILE__, __func__, __LINE__, FI_OPA1X_EP_RX_QUEUE_KIND_MSG, &opa1x_ep->rx.queue[FI_OPA1X_EP_RX_QUEUE_KIND_MSG].ue);


//fprintf(stderr, "%s:%s():%d offsetof(struct fi_opa1x_ep, rx) = %zu\n", __FILE__, __func__, __LINE__, offsetof(struct fi_opa1x_ep, rx));


	fi_opa1x_context_slist_init(&opa1x_ep->rx.queue[FI_OPA1X_EP_RX_QUEUE_KIND_TAG].mq);
	fi_opa1x_context_slist_init(&opa1x_ep->rx.queue[FI_OPA1X_EP_RX_QUEUE_KIND_MSG].mq);

	opa1x_ep->rx.ue_free_pool.head = NULL;
	opa1x_ep->rx.ue_free_pool.tail = NULL;

	opa1x_ep->tx.cq = NULL;
	opa1x_ep->tx.cq_pending_ptr = NULL;
	opa1x_ep->tx.cq_completed_ptr = NULL;
	opa1x_ep->tx.cq_err_ptr = NULL;
	//opa1x_ep->tx.cq_lock_ptr = NULL;


	opa1x_ep->cx = cx;
	opa1x_ep->reliability_rx = opa1x_domain->reliability_rx;
//fprintf(stderr, "%s:%s():%d ### opa1x_ep->cx = %u\n", __FILE__, __func__, __LINE__, opa1x_ep->cx);

FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_EP_DATA, "\n");

//fprintf(stderr, "%s:%s():%d =========== BEFORE fi_opa1x_init_cm_ops; opa1x_ep = %p, info = %p\n", __FILE__, __func__, __LINE__, opa1x_ep, info);
	//ret = fi_opa1x_init_cm_ops(opa1x_ep, info);
	ret = fi_opa1x_init_cm_ops(&opa1x_ep->ep_fid.fid, info);
	if (ret)
		goto err;

FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_EP_DATA, "\n");

//fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__);
	ret = fi_opa1x_init_msg_ops(&opa1x_ep->ep_fid, info);
	if (ret)
		goto err;

FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_EP_DATA, "\n");

	ret = fi_opa1x_init_rma_ops(&opa1x_ep->ep_fid, info);
	if (ret)
		goto err;

FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_EP_DATA, "\n");

	ret = fi_opa1x_init_tagged_ops(&opa1x_ep->ep_fid, info);
	if (ret)
		goto err;

FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_EP_DATA, "\n");

	ret = fi_opa1x_init_atomic_ops(&opa1x_ep->ep_fid, info);
	if (ret)
		goto err;

FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_EP_DATA, "\n");

//	opa1x_ep->rx.index = rx_index;
//fprintf(stderr, "%s:%s():%d, info->rx_attr = %p\n", __FILE__, __func__, __LINE__, info->rx_attr);
//if (info->rx_attr) fprintf(stderr, "%s:%s():%d, info->rx_attr->caps = 0x%016lx\n", __FILE__, __func__, __LINE__, info->rx_attr->caps);
//else fprintf(stderr, "%s:%s():%d, info->caps = 0x%016lx\n", __FILE__, __func__, __LINE__, info->caps);
	opa1x_ep->rx.caps = info->rx_attr ? info->rx_attr->caps : info->caps;
//fprintf(stderr, "%s:%s():%d, opa1x_ep->rx.caps = 0x%016lx\n", __FILE__, __func__, __LINE__, opa1x_ep->rx.caps);
//	opa1x_ep->rx.caps |= FI_RECV;
//fprintf(stderr, "%s:%s():%d, opa1x_ep->rx.caps = 0x%016lx\n", __FILE__, __func__, __LINE__, opa1x_ep->rx.caps);
	opa1x_ep->rx.mode = info->rx_attr ? info->rx_attr->mode : 0;
	opa1x_ep->rx.op_flags = info->rx_attr ? info->rx_attr->op_flags : 0;
	opa1x_ep->rx.total_buffered_recv = info->rx_attr ?
			info->rx_attr->total_buffered_recv : 0;

	//opa1x_ep->tx.index = tx_index;
	opa1x_ep->tx.caps = info->tx_attr ? info->tx_attr->caps : info->caps;
	opa1x_ep->tx.mode = info->tx_attr ? info->tx_attr->mode : 0;
	opa1x_ep->tx.op_flags = info->tx_attr ? info->tx_attr->op_flags : 0;

	opa1x_ep->tx.cq = NULL;
	opa1x_ep->tx.cq_bind_flags = 0;
	opa1x_ep->tx.do_cq_completion = 0;

	opa1x_ep->tx.stx = NULL;




FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_EP_DATA, "\n");

	fi_opa1x_ref_inc(&opa1x_domain->ref_cnt, "domain");

	*ep = &opa1x_ep->ep_fid;

	FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_EP_DATA, "(end)\n");
	return 0;
err:
	fi_opa1x_finalize_cm_ops(&opa1x_ep->ep_fid.fid);
	fi_opa1x_finalize_msg_ops(&opa1x_ep->ep_fid);
	fi_opa1x_finalize_rma_ops(&opa1x_ep->ep_fid);
	fi_opa1x_finalize_tagged_ops(&opa1x_ep->ep_fid);
	fi_opa1x_finalize_atomic_ops(&opa1x_ep->ep_fid);
	if (opa1x_domain)
		fi_opa1x_ref_dec(&opa1x_domain->ref_cnt, "domain");
	if (opa1x_ep)
		free(opa1x_ep->mem);

	FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_EP_DATA, "(end - error)\n");
	return -errno;
}

int fi_opa1x_endpoint (struct fid_domain *dom, struct fi_info *info,
		struct fid_ep **ep, void *context)
{
	struct fi_opa1x_domain *opa1x_domain =
		container_of(dom, struct fi_opa1x_domain, domain_fid);

	uint8_t cx = (uint8_t)fi_opa1x_compiler_fetch_and_inc_u64(&opa1x_domain->node->ep_count);

	return fi_opa1x_endpoint_rx_tx(dom, info, ep, context, cx);
}









int fi_opa1x_ep_tx_check (struct fi_opa1x_ep_tx * tx, enum fi_av_type av_type)
{
#ifdef DEBUG
	if (!tx)
		return -FI_EINVAL;
	if (tx->state != FI_OPA1X_TX_ENABLED)
		return -FI_EINVAL;

	if (av_type == FI_AV_UNSPEC)
		return -FI_EINVAL;
	if (av_type == FI_AV_MAP && tx->av_type != FI_MAP)
		return -FI_EINVAL;
	if (av_type == FI_AV_TABLE && tx->av_type != FI_TABLE)
		return -FI_EINVAL;

	/* currently, only FI_AV_TABLE is supported */
	if (av_type == FI_AV_MAP)
		return -FI_ENOSYS;
	if (av_type != FI_AV_MAP)
		return -FI_EINVAL;
#endif
	return 0;
}


/* rx_op_flags is only checked for FI_PEEK | FI_CLAIM | FI_MULTI_RECV;
 * rx_op_flags is only used if FI_PEEK | FI_CLAIM;
 * is_context_ext is only used if FI_PEEK | iovec;
 *
 * The "normal" data movement functions, such as fi_[t]recv(), can safely
 * specify '0' for rx_op_flags, and is_context_ext, in order to reduce code path.
 *
 * See `fi_opa1x_ep_rx_process_context()`
 */
__attribute__((noinline))
void fi_opa1x_ep_rx_process_context_noinline (struct fi_opa1x_ep * opa1x_ep,
		const enum fi_opa1x_ep_rx_queue_kind kind,
		union fi_opa1x_context * context,
		const uint64_t rx_op_flags, const uint64_t is_context_ext,
		const int lock_required, const enum fi_av_type av_type) {

	FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_EP_DATA, "(begin)\n");

	if (rx_op_flags & FI_PEEK) {

		if (av_type == FI_AV_TABLE) {	/* constant compile-time expression */
			const fi_addr_t original_src_addr = context->src_addr;
			if (likely(original_src_addr != FI_ADDR_UNSPEC)) {
				context->src_addr = opa1x_ep->rx.av_addr[original_src_addr].fi;
			}
		}

		/*
		 * search the unexpected packet queue
		 */

		struct fi_opa1x_hfi1_ue_packet * uepkt = opa1x_ep->rx.queue[kind].ue.head;
		struct fi_opa1x_hfi1_ue_packet * prev = NULL;

		while (uepkt != NULL) {

			FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_EP_DATA, "rx_op_flags & FI_PEEK searching unexpected queue\n");

#ifdef FI_OPA1X_TRACE
#endif
			if (is_match(&uepkt->hdr, context)) {

				context->len = fi_opa1x_hfi1_packet_hdr_message_length(&uepkt->hdr);
				context->tag = uepkt->hdr.match.ofi_tag;
				context->data = uepkt->hdr.match.ofi_data;
				context->byte_counter = 0;

				if (rx_op_flags & FI_CLAIM) {	/* both FI_PEEK and FI_CLAIM were specified */

					assert((rx_op_flags & FI_OPA1X_CQ_CONTEXT_EXT) == 0);

					context->claim = (struct fi_opa1x_hfi1_ue_packet *)uepkt;

					/* remove this item from the list */
					if (prev)
						prev->next = uepkt->next;
					else
						opa1x_ep->rx.queue[kind].ue.head = uepkt->next;

					if (!uepkt->next)
						opa1x_ep->rx.queue[kind].ue.tail = prev;
				}

				fi_opa1x_context_slist_insert_tail(context, opa1x_ep->rx.cq_completed_ptr);
				return;
			}


			/* advance to the next item in the list */
			prev = uepkt;
			uepkt = uepkt->next;
		}

		/*
		 * did not find a match for this "peek"; notify the application
		 * via completion queue error entry
		 */

		struct fi_opa1x_context_ext * ext = NULL;
		if (is_context_ext) {
			ext = (struct fi_opa1x_context_ext *)context;
			assert((ext->opa1x_context.flags & FI_OPA1X_CQ_CONTEXT_EXT) != 0);
		} else {
			posix_memalign((void**)&ext, 32, sizeof(struct fi_opa1x_context_ext));
			ext->opa1x_context.flags = rx_op_flags | FI_OPA1X_CQ_CONTEXT_EXT;
		}

		ext->err_entry.op_context = context;
		ext->err_entry.flags = rx_op_flags;
		ext->err_entry.len = 0;
		ext->err_entry.buf = 0;
		ext->err_entry.data = 0;
		ext->err_entry.tag = 0;
		ext->err_entry.olen = 0;
		ext->err_entry.err = FI_ENOMSG;
		ext->err_entry.prov_errno = 0;
		ext->err_entry.err_data = NULL;
		ext->opa1x_context.byte_counter = 0;

		FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_EP_DATA, "no match found on unexpected queue posting error\n");

		fi_opa1x_cq_enqueue_err(opa1x_ep->rx.cq, ext, lock_required);

	} else if (rx_op_flags & FI_CLAIM) {

		assert((rx_op_flags & FI_OPA1X_CQ_CONTEXT_EXT) == 0);
		FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_EP_DATA, "rx_op_flags & FI_CLAIM complete receive operation\n");

		/* only FI_CLAIM was specified
		 *
		 * this occurs after a previous FI_PEEK + FI_CLAIM
		 * operation has removed an unexpected packet from
		 * the queue and saved a pointer to it in the context
		 *
		 * complete the receive for this "claimed" message ... */

		struct fi_opa1x_hfi1_ue_packet * claimed_pkt = context->claim;

		const unsigned is_intranode = (claimed_pkt->hdr.stl.lrh.slid == opa1x_ep->rx.slid);

		complete_receive_operation(&opa1x_ep->rx,
			&claimed_pkt->hdr,
			(union fi_opa1x_hfi1_packet_payload *)&claimed_pkt->payload,
			claimed_pkt->hdr.match.ofi_tag,
			context,
			claimed_pkt->hdr.stl.bth.opcode,
			0,	/* is_context_ext */
			0,	/* is_multi_receive */
			is_intranode,
			lock_required);

		/* ... and prepend the claimed uepkt to the ue free list. */

		fi_opa1x_hfi1_ue_packet_slist_insert_head(claimed_pkt, &opa1x_ep->rx.ue_free_pool);

	} else if ((kind == FI_OPA1X_EP_RX_QUEUE_KIND_MSG) && (rx_op_flags & FI_MULTI_RECV)) {

		if (av_type == FI_AV_TABLE) {	/* constant compile-time expression */
			if (likely(context->src_addr != FI_ADDR_UNSPEC)) {
				context->src_addr = opa1x_ep->rx.av_addr[context->src_addr].fi;
			}
		}

		/*
		 * search the unexpected packet queue
		 */

		struct fi_opa1x_hfi1_ue_packet * uepkt = opa1x_ep->rx.queue[kind].ue.head;
		struct fi_opa1x_hfi1_ue_packet * prev = NULL;

		while (uepkt != NULL) {

			if (is_match(&uepkt->hdr, context)) {

				/* verify that there is enough space available in
				 * the multi-receive buffer for the incoming data */

				const size_t recv_len = context->len;
				const size_t send_len = fi_opa1x_hfi1_packet_hdr_message_length(&uepkt->hdr);

				if (send_len > recv_len) {

					/* not enough space available in the multi-receive
					 * buffer; continue as if "a match was not found"
					 * and advance to the next ue header */
					prev = uepkt;
					uepkt = uepkt->next;

				} else {
					const unsigned is_intranode = (uepkt->hdr.stl.lrh.slid == opa1x_ep->rx.slid);

					/* the 'context->len' field will be updated to the
					 * new multi-receive buffer free space as part of
					 * the receive completion */
					complete_receive_operation(&opa1x_ep->rx,
						&uepkt->hdr,
						(union fi_opa1x_hfi1_packet_payload *)&uepkt->payload,
						uepkt->hdr.match.ofi_tag,
						context,
						uepkt->hdr.stl.bth.opcode,
						0,	/* is_context_ext */
						1,	/* is_multi_receive */
						is_intranode,
						lock_required);

					/* remove this item from the ue list and prepend
					 * the (now) completed uepkt to the ue free list. */
					if (prev)
						prev->next = uepkt->next;
					else
						opa1x_ep->rx.queue[kind].ue.head = uepkt->next;

					if (!uepkt->next)
						opa1x_ep->rx.queue[kind].ue.tail = prev;

					fi_opa1x_hfi1_ue_packet_slist_insert_head(uepkt, &opa1x_ep->rx.ue_free_pool);

					if (context->len < opa1x_ep->rx.min_multi_recv) {
						/* after processing this message there is not
						 * enough space available in the multi-receive
						 * buffer to receive the *next* message; break
						 * from the loop and post a 'FI_MULTI_RECV'
						 * event to the completion queue. */

						context->byte_counter = 0;
	
						if (lock_required) { fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__); abort(); }
						fi_opa1x_context_slist_insert_tail(context, opa1x_ep->rx.cq_completed_ptr);

						return;
					}
				}
			}
		}

		/*
		 * no unexpected headers were matched; add this match
		 * information to the appropriate match queue
		 */
		fi_opa1x_context_slist_insert_tail(context, &opa1x_ep->rx.queue[kind].mq);
	}

	FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_EP_DATA, "(end)\n");
	return;
}



#ifndef INLINE_FI_OPA1X_EP_RX_PROCESS_CONTEXT

/* rx_op_flags is only checked for FI_PEEK | FI_CLAIM | FI_MULTI_RECV
 * rx_op_flags is only used if FI_PEEK | FI_CLAIM | cancel_context
 * is_context_ext is only used if FI_PEEK | cancel_context | iovec
 *
 * The "normal" data movement functions, such as fi_[t]recv(), can safely
 * specify '0' for cancel_context, rx_op_flags, and is_context_ext, in
 * order to reduce code path.
 *
 * TODO - use payload pointer? keep data in hfi eager buffer as long
 * as possible to avoid memcpy?
 */
int fi_opa1x_ep_rx_process_context (struct fi_opa1x_ep * opa1x_ep,
		const enum fi_opa1x_ep_rx_queue_kind kind,
		const uint64_t cancel_context, union fi_opa1x_context * context,
		const uint64_t rx_op_flags, const uint64_t is_context_ext,
		const int lock_required, const enum fi_av_type av_type) {

	FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_EP_DATA, "\n");

	if (cancel_context) {	/* branch should compile out */
		FI_WARN(fi_opa1x_global.prov, FI_LOG_EP_DATA, "unimplemented; abort\n"); abort();

		const uint64_t compare_context = is_context_ext ?
			(uint64_t)(((struct fi_opa1x_context_ext *)context)->msg.op_context) :
			(uint64_t)context;

		if (compare_context == cancel_context) {

			struct fi_opa1x_context_ext * ext;
			if (is_context_ext) {
				ext = (struct fi_opa1x_context_ext *)context;
			} else {
				posix_memalign((void**)&ext, 32, sizeof(struct fi_opa1x_context_ext));
				ext->opa1x_context.flags = FI_OPA1X_CQ_CONTEXT_EXT;
			}

			ext->opa1x_context.byte_counter = 0;
			ext->err_entry.op_context = (void *)cancel_context;
			ext->err_entry.flags = rx_op_flags;
			ext->err_entry.len = 0;
			ext->err_entry.buf = 0;
			ext->err_entry.data = 0;
			ext->err_entry.tag = context->tag;
			ext->err_entry.olen = 0;
			ext->err_entry.err = FI_ECANCELED;
			ext->err_entry.prov_errno = 0;
			ext->err_entry.err_data = NULL;

			/* post an 'error' completion event for the canceled receive */
			if (lock_required) { fprintf(stderr, "%s:%s():%d\n", __FILE__, __func__, __LINE__); abort(); }
			fi_opa1x_context_slist_insert_tail((union fi_opa1x_context*)ext, opa1x_ep->rx.cq_err_ptr);

			return FI_ECANCELED;
		}
	}

	if (likely((rx_op_flags & (FI_PEEK | FI_CLAIM | FI_MULTI_RECV)) == 0)) {

		/*
		 * search the unexpected packet queue
		 */
		FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_EP_DATA,
			"searching unexpected queue\n");

		struct fi_opa1x_hfi1_ue_packet *uepkt = opa1x_ep->rx.queue[kind].ue.head;

		if (uepkt) {
			FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_EP_DATA,
				"uepkt = %p\n", uepkt);

			if (is_match(&uepkt->hdr, context)) {

				FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_EP_DATA,
					"found a match\n");

				const unsigned is_intranode = (uepkt->hdr.stl.lrh.slid == opa1x_ep->rx.slid);

				complete_receive_operation(&opa1x_ep->rx,
					&uepkt->hdr,
					&uepkt->payload,
					uepkt->hdr.match.ofi_tag,
					context,
					uepkt->hdr.stl.bth.opcode,
					0,	/* is_context_ext */
					0,	/* is_multi_receive */
					is_intranode,
					lock_required);

				/* remove */
				opa1x_ep->rx.queue[kind].ue.head = uepkt->next;
				if (!uepkt->next) opa1x_ep->rx.queue[kind].ue.tail = NULL;

				/* add uepkt to ue free pool */
				if (!opa1x_ep->rx.ue_free_pool.head) opa1x_ep->rx.ue_free_pool.tail = uepkt;
				uepkt->next = opa1x_ep->rx.ue_free_pool.head;
				opa1x_ep->rx.ue_free_pool.head = uepkt;

				FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_EP_DATA, "\n");
				return 0;

			} else {
				struct fi_opa1x_hfi1_ue_packet *prev = uepkt;
				uepkt = uepkt->next;

				while (uepkt) {
					FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_EP_DATA,
						"uepkt = %p\n", uepkt);

					if (is_match(&uepkt->hdr, context)) {

						FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_EP_DATA,
							"found a match\n");

						const unsigned is_intranode = (uepkt->hdr.stl.lrh.slid == opa1x_ep->rx.slid);

						complete_receive_operation(&opa1x_ep->rx,
							&uepkt->hdr,
							&uepkt->payload,
							uepkt->hdr.match.ofi_tag,
							context,
							uepkt->hdr.stl.bth.opcode,
							0,	/* is_context_ext */
							0,	/* is_multi_receive */
							is_intranode,
							lock_required);

						/* remove */
						prev->next = uepkt->next;
						if (!uepkt->next) opa1x_ep->rx.queue[kind].ue.tail = prev;

						/* add uepkt to ue free pool */
						if (!opa1x_ep->rx.ue_free_pool.head) opa1x_ep->rx.ue_free_pool.tail = uepkt;
						uepkt->next = opa1x_ep->rx.ue_free_pool.head;
						opa1x_ep->rx.ue_free_pool.head = uepkt;

						FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_EP_DATA, "\n");
						return 0;
					}
					prev = uepkt;
					uepkt = uepkt->next;
				}
			}
		}

		FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_EP_DATA,
			"nothing found on unexpected queue; adding to match queue\n");

		/*
		 * no unexpected headers were matched; add this match information
		 * (context) to the appropriate match queue
		 */
		context->next = NULL;
		if (!opa1x_ep->rx.queue[kind].mq.tail) {
			opa1x_ep->rx.queue[kind].mq.head = context;
			opa1x_ep->rx.queue[kind].mq.tail = context;
		} else {
			opa1x_ep->rx.queue[kind].mq.tail->next = context;
			opa1x_ep->rx.queue[kind].mq.tail = context;

		}

	} else {

		/*
		 * Not for critical path: peek, or claim, or multi-receive
		 * context information
		 */
		FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_EP_DATA,
			"process peek, claim, or multi-receive context\n");

		fi_opa1x_ep_rx_process_context_noinline(opa1x_ep, kind,
			context, rx_op_flags, is_context_ext, lock_required, av_type);
	}

	FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_EP_DATA, "\n");
	return 0;
}
#endif /* INLINE_FI_OPA1X_EP_RX_PROCESS_CONTEXT */

void fi_opa1x_ep_rx_process_header_tag (struct fi_opa1x_ep_rx * const rx,
		const union fi_opa1x_hfi1_packet_hdr * const hdr,
		const uint8_t * const payload,
		const size_t payload_bytes,
		const uint8_t opcode,
		const unsigned is_intranode,
		const int lock_required) {

	fi_opa1x_ep_rx_process_header(rx, hdr,
		(const union fi_opa1x_hfi1_packet_payload * const )payload,
		payload_bytes,
		FI_OPA1X_EP_RX_QUEUE_KIND_TAG,
		opcode,
		is_intranode,
		lock_required);
	return;
}

void fi_opa1x_ep_rx_process_header_msg (struct fi_opa1x_ep_rx * const rx,
		const union fi_opa1x_hfi1_packet_hdr * const hdr,
		const uint8_t * const payload,
		const size_t payload_bytes,
		const uint8_t opcode,
		const unsigned is_intranode,
		const int lock_required) {

	fi_opa1x_ep_rx_process_header(rx, hdr,
		(const union fi_opa1x_hfi1_packet_payload * const )payload,
		payload_bytes,
		FI_OPA1X_EP_RX_QUEUE_KIND_MSG,
		opcode,
		is_intranode,
		lock_required);
	return;
}

void fi_opa1x_ep_rx_reliability_process_packet (void * arg,
		const union fi_opa1x_hfi1_packet_hdr * const hdr,
		const uint8_t * const payload) {

	FI_DBG_TRACE(fi_opa1x_global.prov, FI_LOG_EP_DATA, "================ received a packet from the reliability service\n");

	struct fi_opa1x_ep_rx * rx = (struct fi_opa1x_ep_rx *)arg;

	const uint8_t opcode = hdr->stl.bth.opcode;

	/* reported in LRH as the number of 4-byte words in the packet; header + payload + icrc */
	const uint16_t lrh_pktlen_le = ntohs(hdr->stl.lrh.pktlen);
	const size_t total_bytes = (lrh_pktlen_le - 1) * 4;	/* do not copy the trailing icrc */
	const size_t payload_bytes = total_bytes - sizeof(union fi_opa1x_hfi1_packet_hdr);

	if (likely(opcode >= FI_OPA1X_HFI_BTH_OPCODE_TAG_INJECT)) {

		fi_opa1x_ep_rx_process_header(rx, hdr,
			(const union fi_opa1x_hfi1_packet_payload * const) payload,
			payload_bytes,
			FI_OPA1X_EP_RX_QUEUE_KIND_TAG,
			opcode,
			0,	/* is_intranode */
			0);	/* lock_required - TODO */
	} else {

		fi_opa1x_ep_rx_process_header(rx, hdr,
			(const union fi_opa1x_hfi1_packet_payload * const) payload,
			payload_bytes,
			FI_OPA1X_EP_RX_QUEUE_KIND_MSG,
			opcode,
			0,	/* is_intranode */
			0);	/* lock_required - TODO */
	}
}

static inline
void fi_opa1x_ep_rx_append_ue (struct fi_opa1x_ep_rx * const rx,
		const union fi_opa1x_hfi1_packet_hdr * const hdr,
		const union fi_opa1x_hfi1_packet_payload * const payload,
		const size_t payload_bytes,
		const enum fi_opa1x_ep_rx_queue_kind kind) {

	if (unlikely(fi_opa1x_hfi1_ue_packet_slist_empty(&rx->ue_free_pool))) {

		/*
		 * the unexpected packet free list is empty - allocate
		 * another block of unexpected packets
		 */
		struct fi_opa1x_hfi1_ue_packet * block = NULL;

		int i, rc __attribute__ ((unused));
		rc = posix_memalign((void **)&block, 32,
			sizeof(struct fi_opa1x_hfi1_ue_packet) *
			FI_OPA1X_EP_RX_UEPKT_BLOCKSIZE);
		assert(rc==0);

		for (i=0; i<FI_OPA1X_EP_RX_UEPKT_BLOCKSIZE; ++i)
			//slist_insert_tail(&block[i].entry, ue_free_pool);
			fi_opa1x_hfi1_ue_packet_slist_insert_tail(&block[i], &rx->ue_free_pool);
	}

	/* pop the free list, copy the packet, and add to the unexpected queue */
	struct fi_opa1x_hfi1_ue_packet *uepkt = rx->ue_free_pool.head;
	if (rx->ue_free_pool.head == rx->ue_free_pool.tail) {
		rx->ue_free_pool.head = rx->ue_free_pool.tail = NULL;
	} else {
		rx->ue_free_pool.head = uepkt->next;
	}

	memcpy((void *)&uepkt->hdr, (const void *)hdr, sizeof(union fi_opa1x_hfi1_packet_hdr));

	if (payload != NULL) {

		memcpy((void *)&uepkt->payload.byte[0], payload, payload_bytes);
	}

	uepkt->next = NULL;
	fi_opa1x_hfi1_ue_packet_slist_insert_tail(uepkt,  &rx->queue[kind].ue);

	return;
}

void fi_opa1x_ep_rx_append_ue_msg (struct fi_opa1x_ep_rx * const rx,
		const union fi_opa1x_hfi1_packet_hdr * const hdr,
		const union fi_opa1x_hfi1_packet_payload * const payload,
		const size_t payload_bytes) {

	fi_opa1x_ep_rx_append_ue(rx, hdr, payload, payload_bytes, FI_OPA1X_EP_RX_QUEUE_KIND_MSG);
}

void fi_opa1x_ep_rx_append_ue_tag (struct fi_opa1x_ep_rx * const rx,
		const union fi_opa1x_hfi1_packet_hdr * const hdr,
		const union fi_opa1x_hfi1_packet_payload * const payload,
		const size_t payload_bytes) {

	fi_opa1x_ep_rx_append_ue(rx, hdr, payload, payload_bytes, FI_OPA1X_EP_RX_QUEUE_KIND_TAG);
}

void fi_opa1x_ep_tx_connect (struct fi_opa1x_ep *opa1x_ep, fi_addr_t peer)
{
	opa1x_ep->rx.av_addr = opa1x_ep->av->table_addr;
	opa1x_ep->tx.av_addr = opa1x_ep->av->table_addr;
	opa1x_ep->rx.av_count = opa1x_ep->av->addr_count;
	opa1x_ep->tx.av_count = opa1x_ep->av->addr_count;

	FI_OPA1X_FABRIC_TX_CONNECT(opa1x_ep, peer);

	return;
}



#define FABRIC_DIRECT_LOCK	0

FI_OPA1X_MSG_SPECIALIZED_FUNC(FABRIC_DIRECT_LOCK, FABRIC_DIRECT_AV)

ssize_t
fi_opa1x_send_FABRIC_DIRECT(struct fid_ep *ep, const void *buf, size_t len,
		void *desc, fi_addr_t dest_addr, void *context)
{
	return FI_OPA1X_MSG_SPECIALIZED_FUNC_NAME(send,
			FABRIC_DIRECT_LOCK,
			FABRIC_DIRECT_AV)
				(ep, buf, len, desc, dest_addr, context);
}

ssize_t
fi_opa1x_recv_FABRIC_DIRECT(struct fid_ep *ep, void *buf, size_t len,
		void *desc, fi_addr_t src_addr, void *context)
{
	return FI_OPA1X_MSG_SPECIALIZED_FUNC_NAME(recv,
			FABRIC_DIRECT_LOCK,
			FABRIC_DIRECT_AV)
				(ep, buf, len, desc, src_addr, context);
}

ssize_t
fi_opa1x_inject_FABRIC_DIRECT(struct fid_ep *ep, const void *buf, size_t len,
		fi_addr_t dest_addr)
{
	return FI_OPA1X_MSG_SPECIALIZED_FUNC_NAME(inject,
			FABRIC_DIRECT_LOCK,
			FABRIC_DIRECT_AV)
				(ep, buf, len, dest_addr);
}

ssize_t
fi_opa1x_recvmsg_FABRIC_DIRECT(struct fid_ep *ep, const struct fi_msg *msg,
		uint64_t flags)
{
	return FI_OPA1X_MSG_SPECIALIZED_FUNC_NAME(recvmsg,
			FABRIC_DIRECT_LOCK,
			FABRIC_DIRECT_AV)
				(ep, msg, flags);
}

ssize_t
fi_opa1x_senddata_FABRIC_DIRECT(struct fid_ep *ep, const void *buf, size_t len,
		void *desc, uint64_t data, fi_addr_t dest_addr, void *context)
{
	return FI_OPA1X_MSG_SPECIALIZED_FUNC_NAME(senddata,
			FABRIC_DIRECT_LOCK,
			FABRIC_DIRECT_AV)
				(ep, buf, len, desc, data, dest_addr, context);
}

ssize_t
fi_opa1x_injectdata_FABRIC_DIRECT(struct fid_ep *ep, const void *buf,
		size_t len, uint64_t data, fi_addr_t dest_addr)
{
	return FI_OPA1X_MSG_SPECIALIZED_FUNC_NAME(injectdata,
			FABRIC_DIRECT_LOCK,
			FABRIC_DIRECT_AV)
				(ep, buf, len, data, dest_addr);
}
