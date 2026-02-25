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
    ap.add_argument("--merchants", type=int, default=500)
    ap.add_argument("--events", type=int, default=50000)
    ap.add_argument("--history-days", type=int, default=180)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--drift-days-ago", type=int, default=45)
    ap.add_argument("--write-parquet", action="store_true")
    ap.add_argument("--make-realtime-tail", action="store_true")
    args = ap.parse_args()

    os.makedirs(args.out, exist_ok=True)
    rng = np.random.default_rng(args.seed)
    random.seed(args.seed)
    now = datetime.now(timezone.utc)
    drift_start = now - timedelta(days=args.drift_days_ago)

    # ===== dim_merchant =====
    segments = ["SMB","MID_MARKET","ENTERPRISE"]
    risk_tiers = ["LOW","MEDIUM","HIGH"]
    kyc_statuses = ["VERIFIED","PENDING","REJECTED"]
    account_statuses = ["ACTIVE","SUSPENDED","CLOSED"]

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
    state = np.where(merchant_country=="US", rng.choice(us_states, size=args.merchants), None)
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

    mix = rng.random(args.events)
    sec_old = rng.integers(0, args.history_days*24*3600, size=args.events)
    sec_recent = rng.integers(0, 14*24*3600, size=args.events)
    sec_back = np.where(mix < 0.7, sec_recent, sec_old)
    event_time = pd.to_datetime([now - timedelta(seconds=int(s)) for s in sec_back], utc=True)

    aov = avg_order_value_usd_est[merchant_idx]
    amount = rng.lognormal(mean=np.log(np.maximum(aov,1)), sigma=0.65, size=args.events)
    amount = np.clip(amount, 1.0, 7000.0)
    currency = rng.choice(currencies, size=args.events, p=currency_p)

    fx = np.where(currency=="USD", 1.0,
         np.where(currency=="CAD", 0.74,
         np.where(currency=="GBP", 1.27,
         np.where(currency=="EUR", 1.09, 0.66))))
    amount_usd = amount * fx

    base_probs = np.array([0.10,0.08,0.03,0.62,0.04,0.07,0.03,0.02,0.01])
    chosen = []
    for i in range(args.events):
        p = base_probs.copy()
        if m_risk[i] == "HIGH":
            p[4] += 0.04; p[5] += 0.03; p[6] += 0.02; p[7] += 0.01; p[3] -= 0.08
        elif m_risk[i] == "MEDIUM":
            p[4] += 0.015; p[5] += 0.01; p[6] += 0.005; p[3] -= 0.03
        if event_time[i].to_pydatetime() >= drift_start and m_mcc[i] in ("5812","5999"):
            p[5] += 0.02
            p[6] += 0.01
            p[3] -= 0.03
        p = np.clip(p, 0.001, None)
        p = p / p.sum()
        chosen.append(rng.choice(event_types, p=p))
    event_type = np.array(chosen, dtype=object)

    event_id = np.array([u("evt_") for _ in range(args.events)], dtype=object)
    order_id = np.array([u("ord_") for _ in range(args.events)], dtype=object)

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

    payer_id_hash = np.array([sha256_hex("payer|" + uuid.uuid4().hex) for _ in range(args.events)], dtype=object)

    label_bad_outcome = ((is_denied) | (is_dispute) | (is_refund)).astype(int)

    fact = pd.DataFrame({
        "event_id": event_id,
        "event_type": event_type,
        "event_time_utc": event_time,
        "merchant_id": mid,
        "order_id": order_id,
        "status": status,
        "amount_usd": np.round(amount_usd,2),
        "payer_id_hash": payer_id_hash,
        "merchant_country": m_country,
        "merchant_mcc": m_mcc,
        "source_system": "SYNTHETIC",
        "ingested_at_utc": now.isoformat(),
        "label_bad_outcome": label_bad_outcome,
        "drift_regime_id": (event_time >= pd.Timestamp(drift_start)).astype(int)
    })

    dim_merchant.to_csv(os.path.join(args.out, "dim_merchant.csv"), index=False)
    fact.to_csv(os.path.join(args.out, "fact_payment_events.csv"), index=False)

    if args.make_realtime_tail:
        tail_start = now - timedelta(minutes=60)
        tail = fact[fact["event_time_utc"] >= pd.Timestamp(tail_start)].sort_values("event_time_utc")
        out_path = os.path.join(args.out, "realtime_events.jsonl")
        tail.to_json(out_path, orient="records", lines=True, date_format="iso")
        print(f"Wrote realtime tail: {out_path} ({len(tail)} events)")

    print("DONE")
    print("dim_merchant rows:", len(dim_merchant))
    print("fact_payment_events rows:", len(fact))

if __name__ == "__main__":
    main()
