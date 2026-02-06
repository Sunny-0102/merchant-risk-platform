#!/usr/bin/env python3
import argparse, os, uuid, hashlib, random
from datetime import datetime, timedelta, timezone
import numpy as np
import pandas as pd

def sha256_hex(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def u(prefix: str) -> str:
    return f"{prefix}{uuid.uuid4().hex}"

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--merchants", type=int, default=5000)
    ap.add_argument("--events", type=int, default=5_000_000)
    ap.add_argument("--history-days", type=int, default=365)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--drift-days-ago", type=int, default=45, help="inject distribution shift starting this many days ago")
    ap.add_argument("--write-parquet", action="store_true")
    ap.add_argument("--make-realtime-tail", action="store_true", help="writes last 60 minutes to realtime_events.jsonl")
    args = ap.parse_args()

    os.makedirs(args.out, exist_ok=True)
    rng = np.random.default_rng(args.seed)
    random.seed(args.seed)
    now = datetime.now(timezone.utc)
    drift_start = now - timedelta(days=args.drift_days_ago)

    NONE = np.array(None, dtype=object)
    drift_start_ts = pd.Timestamp(drift_start)
    recent_cutoff = pd.Timestamp(now - timedelta(days=14))

    # ===== dim_merchant =====
    segments = ["SMB","MID_MARKET","ENTERPRISE"]
    risk_tiers = ["LOW","MEDIUM","HIGH"]
    kyc_statuses = ["VERIFIED","PENDING","REJECTED"]
    account_statuses = ["ACTIVE","SUSPENDED","CLOSED"]

    # MCC examples; expand as needed
    mcc_pool = [
        ("5411","Grocery Stores"),
        ("5812","Eating Places/Restaurants"),
        ("5732","Electronics Stores"),
        ("5999","Misc Retail"),
        ("7011","Lodging/Hotels"),
        ("4111","Transport"),
        ("4900","Utilities"),
    ]

    merchant_ids = [f"m_{i:06d}" for i in range(1, args.merchants + 1)]
    merchant_segment = rng.choice(segments, size=args.merchants, p=[0.72,0.23,0.05])
    risk_tier = rng.choice(risk_tiers, size=args.merchants, p=[0.72,0.22,0.06])
    kyc_status = rng.choice(kyc_statuses, size=args.merchants, p=[0.9,0.08,0.02])
    account_status = np.array([
        rng.choice(account_statuses, p=[0.85,0.12,0.03]) if rt=="HIGH"
        else rng.choice(account_statuses, p=[0.95,0.04,0.01])
        for rt in risk_tier
    ])

    mcc = rng.choice([x[0] for x in mcc_pool], size=args.merchants, replace=True)
    industry_name = [dict(mcc_pool)[x] for x in mcc]

    countries = ["US","CA","GB","DE","FR","AU","IN","BR","MX","JP"]
    merchant_country = rng.choice(countries, size=args.merchants, p=[0.82,0.03,0.03,0.02,0.02,0.02,0.02,0.02,0.01,0.01])
    us_states = ["NY","CA","TX","FL","IL","WA","MA","NJ","PA","GA","NC","VA","AZ","CO","MI"]
    state = np.where(merchant_country=="US", rng.choice(us_states, size=args.merchants), NONE)
    tz_us = ["America/New_York","America/Chicago","America/Denver","America/Los_Angeles"]
    tz_other = ["Europe/London","Europe/Berlin","Asia/Tokyo"]
    timezone_str = np.where(merchant_country=="US",
                            rng.choice(tz_us, size=args.merchants, p=[0.4,0.25,0.1,0.25]),
                            rng.choice(tz_other, size=args.merchants))

    onboard_days = rng.integers(30, 1200, size=args.merchants)
    onboarding_date = [(now.date() - timedelta(days=int(x))) for x in onboard_days]

    seg_scale = np.array([1.0 if s=="SMB" else (2.3 if s=="MID_MARKET" else 5.5) for s in merchant_segment])
    avg_order_value_usd_est = rng.lognormal(mean=3.6, sigma=0.55, size=args.merchants) * seg_scale
    avg_daily_txn_est = (rng.poisson(lam=25*seg_scale, size=args.merchants) + 1).astype(int)

    dim_merchant = pd.DataFrame({
        "merchant_id": merchant_ids,
        "merchant_name": [f"Merchant_{i:06d}" for i in range(1, args.merchants+1)],
        "onboarding_date": onboarding_date,
        "merchant_segment": merchant_segment,
        "mcc": mcc,
        "industry_name": industry_name,
        "risk_tier": risk_tier,
        "kyc_status": kyc_status,
        "account_status": account_status,
        "country": merchant_country,
        "state": state,
        "timezone": timezone_str,
        "fee_plan_id": rng.choice(["FP_BASIC","FP_PLUS","FP_CUSTOM"], size=args.merchants, p=[0.7,0.25,0.05]),
        "processor_fee_rate": np.round(np.where(merchant_segment=="ENTERPRISE", 0.022,
                                     np.where(merchant_segment=="MID_MARKET", 0.025, 0.029)), 4),
        "fixed_fee": 0.30,
        "settlement_schedule": rng.choice(["T1","T2","T3"], size=args.merchants, p=[0.55,0.35,0.10]),
        "payout_frequency": rng.choice(["DAILY","WEEKLY"], size=args.merchants, p=[0.8,0.2]),
        "payout_method": rng.choice(["BANK","WALLET"], size=args.merchants, p=[0.92,0.08]),
        "reserve_rate": np.round(np.where(risk_tier=="HIGH", 0.07, np.where(risk_tier=="MEDIUM", 0.03, 0.0)), 3),
        "avg_order_value_usd_est": np.round(avg_order_value_usd_est, 2),
        "avg_daily_txn_est": avg_daily_txn_est,
        "created_at_utc": now.isoformat()
    })

    # ===== fact_payment_events =====
    event_types = [
        "CHECKOUT.ORDER.APPROVED",
        "PAYMENT.AUTHORIZATION.CREATED",
        "PAYMENT.AUTHORIZATION.VOIDED",
        "PAYMENT.CAPTURE.COMPLETED",
        "PAYMENT.CAPTURE.DENIED",
        "PAYMENT.CAPTURE.REFUNDED",
        "CUSTOMER.DISPUTE.CREATED",
        "CUSTOMER.DISPUTE.RESOLVED",
        "PAYOUTS.PAYOUT.COMPLETED"
    ]

    currencies = ["USD","CAD","GBP","EUR","AUD"]
    currency_p = [0.90,0.03,0.03,0.03,0.01]

    merchant_idx = rng.integers(0, args.merchants, size=args.events)
    mid = np.array(merchant_ids, dtype=object)[merchant_idx]
    m_risk = np.array(risk_tier, dtype=object)[merchant_idx]
    m_seg = np.array(merchant_segment, dtype=object)[merchant_idx]
    m_mcc = np.array(mcc, dtype=object)[merchant_idx]
    m_country = np.array(merchant_country, dtype=object)[merchant_idx]

    # time distribution (recent heavier)
    mix = rng.random(args.events)
    sec_old = rng.integers(0, args.history_days*24*3600, size=args.events)
    sec_recent = rng.integers(0, 14*24*3600, size=args.events)
    sec_back = np.where(mix < 0.7, sec_recent, sec_old)
    event_time = pd.to_datetime([now - timedelta(seconds=int(s)) for s in sec_back], utc=True)

    # amounts conditioned on merchant AOV
    aov = avg_order_value_usd_est[merchant_idx]
    amount = rng.lognormal(mean=np.log(np.maximum(aov,1)), sigma=0.65, size=args.events)
    amount = np.clip(amount, 1.0, 7000.0)
    currency = rng.choice(currencies, size=args.events, p=currency_p)

    # synthetic FX rate (very rough)
    fx = np.where(currency=="USD", 1.0,
         np.where(currency=="CAD", 0.74,
         np.where(currency=="GBP", 1.27,
         np.where(currency=="EUR", 1.09, 0.66))))
    amount_usd = amount * fx

    # event type distribution with drift
    base_probs = np.array([0.10,0.08,0.03,0.62,0.04,0.07,0.03,0.02,0.01])
    chosen = []
    for i in range(args.events):
        p = base_probs.copy()
        # risk conditioning
        if m_risk[i] == "HIGH":
            p[4] += 0.04; p[5] += 0.03; p[6] += 0.02; p[7] += 0.01; p[3] -= 0.08
        elif m_risk[i] == "MEDIUM":
            p[4] += 0.015; p[5] += 0.01; p[6] += 0.005; p[3] -= 0.03

        # concept drift: after drift_start, refunds/disputes increase for certain MCCs
        if event_time[i] >= drift_start_ts and m_mcc[i] in ("5812","5999"):
            p[5] += 0.02  # refunds
            p[6] += 0.01  # disputes created
            p[3] -= 0.03

        p = np.clip(p, 0.001, None)
        p = p / p.sum()
        chosen.append(rng.choice(event_types, p=p))
    event_type = np.array(chosen, dtype=object)

    # ids
    event_id = np.array([u("evt_") for _ in range(args.events)], dtype=object)
    order_id = np.array([u("ord_") for _ in range(args.events)], dtype=object)
    authorization_id = np.where(np.isin(event_type, ["PAYMENT.AUTHORIZATION.CREATED","PAYMENT.AUTHORIZATION.VOIDED"]),
                                np.array([u("aut_") for _ in range(args.events)], dtype=object), NONE)
    capture_id = np.where(np.isin(event_type, ["PAYMENT.CAPTURE.COMPLETED","PAYMENT.CAPTURE.DENIED","PAYMENT.CAPTURE.REFUNDED"]),
                          np.array([u("cap_") for _ in range(args.events)], dtype=object), NONE)
    refund_id = np.where(event_type=="PAYMENT.CAPTURE.REFUNDED",
                         np.array([u("rfd_") for _ in range(args.events)], dtype=object), NONE)
    dispute_id = np.where(np.isin(event_type, ["CUSTOMER.DISPUTE.CREATED","CUSTOMER.DISPUTE.RESOLVED"]),
                          np.array([u("dsp_") for _ in range(args.events)], dtype=object), NONE)
    payout_id = np.where(event_type=="PAYOUTS.PAYOUT.COMPLETED",
                         np.array([u("pyo_") for _ in range(args.events)], dtype=object), NONE)

    # status
    status = np.full(args.events, "UNKNOWN", dtype=object)
    status[event_type=="PAYMENT.CAPTURE.COMPLETED"] = "COMPLETED"
    status[event_type=="PAYMENT.CAPTURE.DENIED"] = "DENIED"
    status[event_type=="PAYMENT.CAPTURE.REFUNDED"] = "REFUNDED"
    status[event_type=="CHECKOUT.ORDER.APPROVED"] = "APPROVED"
    status[event_type=="PAYMENT.AUTHORIZATION.CREATED"] = "AUTHORIZED"
    status[event_type=="PAYMENT.AUTHORIZATION.VOIDED"] = "VOIDED"
    status[event_type=="CUSTOMER.DISPUTE.CREATED"] = "DISPUTE_OPEN"
    status[event_type=="CUSTOMER.DISPUTE.RESOLVED"] = "DISPUTE_CLOSED"
    status[event_type=="PAYOUTS.PAYOUT.COMPLETED"] = "PAID_OUT"

    is_refund = (event_type=="PAYMENT.CAPTURE.REFUNDED")
    is_dispute = np.isin(event_type, ["CUSTOMER.DISPUTE.CREATED","CUSTOMER.DISPUTE.RESOLVED"])
    is_denied = (event_type=="PAYMENT.CAPTURE.DENIED")

    # payer/device/ip hashes
    payer_id_hash = np.array([sha256_hex("payer|" + uuid.uuid4().hex) for _ in range(args.events)], dtype=object)
    device_id_hash = np.array([sha256_hex("dev|" + uuid.uuid4().hex) for _ in range(args.events)], dtype=object)
    ip_hash = np.array([sha256_hex("ip|" + str(int(x))) for x in rng.integers(0, 2**32-1, size=args.events)], dtype=object)

    # payer country (mostly local)
    payer_country = np.array([
        rng.choice(["US","CA","MX","GB"], p=[0.9,0.05,0.03,0.02]) if mc=="US"
        else rng.choice(countries)
        for mc in m_country
    ], dtype=object)

    payment_method = rng.choice(["CARD","BANK_TRANSFER","WALLET","BNPL"], size=args.events, p=[0.72,0.08,0.15,0.05])
    card_brand = np.where(payment_method=="CARD",
                          rng.choice(["VISA","MASTERCARD","AMEX","DISCOVER"], size=args.events, p=[0.52,0.34,0.1,0.04]),
                          NONE)
    channel = rng.choice(["ECOM","POS","INVOICE","SUBSCRIPTION"], size=args.events, p=[0.62,0.18,0.1,0.1])
    card_present = (channel=="POS")
    three_ds_result = np.where(channel=="ECOM", rng.choice(["AUTHENTICATED","ATTEMPTED","NOT_SUPPORTED"], size=args.events, p=[0.55,0.25,0.20]), NONE)

    avs_result = rng.choice(["Y","N","A","Z","U"], size=args.events, p=[0.80,0.08,0.05,0.04,0.03])
    cvv_result = rng.choice(["M","N","P","U"], size=args.events, p=[0.86,0.06,0.05,0.03])

    # fee and net
    fee_rate_base = np.where(m_seg=="ENTERPRISE", 0.022, np.where(m_seg=="MID_MARKET", 0.025, 0.029))
    fee_rate_risk = np.where(m_risk=="HIGH", 0.006, np.where(m_risk=="MEDIUM", 0.002, 0.0))
    fee_rate = np.clip(fee_rate_base + fee_rate_risk + rng.normal(0,0.0015,size=args.events), 0.015, 0.07)
    fixed_fee = np.where(currency=="USD", 0.30, 0.35)
    fee_amount = np.round(amount_usd * fee_rate + fixed_fee, 2)

    refund_amount = np.where(is_refund, np.round(amount_usd * rng.choice([0.25,0.5,1.0], size=args.events, p=[0.15,0.25,0.6]),2), 0.0)
    dispute_amount = np.where(is_dispute, np.round(amount_usd * rng.choice([0.5,1.0], size=args.events, p=[0.2,0.8]),2), 0.0)
    dispute_fee = np.where(is_dispute, np.round(rng.choice([15,25,35], size=args.events, p=[0.5,0.35,0.15]),2), 0.0)

    net_amount = np.round(amount_usd - fee_amount - refund_amount - (dispute_fee*0.2), 2)

    # settlement/payout lags
    settlement_lag = np.where(status=="COMPLETED", rng.integers(1, 72, size=args.events), NONE)
    payout_lag = np.where(status=="COMPLETED", rng.integers(12, 120, size=args.events), NONE)
    settlement_time = []
    payout_time = []
    for i in range(args.events):
        if settlement_lag[i] is None:
            settlement_time.append(pd.NaT); payout_time.append(pd.NaT)
        else:
            settlement_time.append(event_time[i] + pd.Timedelta(hours=int(settlement_lag[i])))
            payout_time.append(event_time[i] + pd.Timedelta(hours=int(payout_lag[i])))
    settlement_time = pd.to_datetime(settlement_time, utc=True)
    payout_time = pd.to_datetime(payout_time, utc=True)

    is_cross_border = (payer_country != m_country)

    # labels
    label_bad_outcome = ((is_denied) | (is_dispute) | (refund_amount > 0)).astype(int)
    label_anomaly = np.zeros(args.events, dtype=int)
    # simple anomaly: huge orders in last 14 days
    recent_mask = event_time >= recent_cutoff
    label_anomaly[recent_mask & (amount_usd > np.percentile(amount_usd[recent_mask], 99.7))] = 1
    label_dispute_lost = (is_dispute & (rng.random(args.events) < np.where(m_risk=="HIGH", 0.45, np.where(m_risk=="MEDIUM", 0.30, 0.18)))).astype(int)

    fact = pd.DataFrame({
        "event_id": event_id,
        "event_type": event_type,
        "event_time_utc": event_time,
        "merchant_id": mid,
        "order_id": order_id,
        "authorization_id": authorization_id,
        "capture_id": capture_id,
        "refund_id": refund_id,
        "dispute_id": dispute_id,
        "payout_id": payout_id,
        "settlement_id": None,
        "status": status,
        "amount": np.round(amount,2),
        "currency": currency,
        "fx_rate_to_usd": np.round(fx,4),
        "amount_usd": np.round(amount_usd,2),
        "fee_amount": fee_amount,
        "net_amount": net_amount,
        "refund_amount": refund_amount,
        "dispute_amount": dispute_amount,
        "dispute_fee": dispute_fee,
        "payer_id_hash": payer_id_hash,
        "payer_country": payer_country,
        "merchant_country": m_country,
        "merchant_mcc": m_mcc,
        "payment_method": payment_method,
        "card_brand": card_brand,
        "channel": channel,
        "card_present": card_present.astype(bool),
        "three_ds_result": three_ds_result,
        "device_id_hash": device_id_hash,
        "ip_hash": ip_hash,
        "avs_result": avs_result,
        "cvv_result": cvv_result,
        "settlement_time_utc": settlement_time,
        "payout_time_utc": payout_time,
        "settlement_lag_hours": pd.Series(settlement_lag).astype("float"),
        "payout_lag_hours": pd.Series(payout_lag).astype("float"),
        "is_refund": is_refund.astype(bool),
        "is_dispute": is_dispute.astype(bool),
        "is_denied": is_denied.astype(bool),
        "is_cross_border": is_cross_border.astype(bool),
        "source_system": "SYNTHETIC",
        "ingested_at_utc": now.isoformat(),
        "trace_id": [u("tr_") for _ in range(args.events)],
        "correlation_id": [u("cor_") for _ in range(args.events)],
        "raw_event_version": 1,
        "label_bad_outcome": label_bad_outcome,
        "label_anomaly": label_anomaly,
        "label_dispute_lost": label_dispute_lost,
        "drift_regime_id": (event_time >= drift_start_ts).astype(int)
    })

    # write outputs
    dim_merchant.to_csv(os.path.join(args.out, "dim_merchant.csv"), index=False)
    fact.to_csv(os.path.join(args.out, "fact_payment_events.csv"), index=False)

    if args.write_parquet:
        dim_merchant.to_parquet(os.path.join(args.out, "dim_merchant.parquet"), index=False)
        fact.to_parquet(os.path.join(args.out, "fact_payment_events.parquet"), index=False)

    # realtime tail
    if args.make_realtime_tail:
        tail_start = now - timedelta(minutes=60)
        tail = fact[fact["event_time_utc"] >= pd.Timestamp(tail_start)].sort_values("event_time_utc")
        out_path = os.path.join(args.out, "realtime_events.jsonl")
        tail.to_json(out_path, orient="records", lines=True, date_format="iso")
        print(f"Wrote realtime tail: {out_path} ({len(tail)} events)")

    print("DONE")
    print(f"dim_merchant rows: {len(dim_merchant)}")
    print(f"fact_payment_events rows: {len(fact)}")

if __name__ == "__main__":
    main()
