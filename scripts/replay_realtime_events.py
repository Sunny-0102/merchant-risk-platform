#!/usr/bin/env python3
import argparse
import json
import time
import urllib.request
import urllib.error

def post_json(url: str, body: dict, timeout: float = 10.0) -> dict:
    data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        url=url,
        data=data,
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))

def main():
    ap = argparse.ArgumentParser(description="Replay jsonl events into POST /ingest/raw")
    ap.add_argument("--file", default="/Users/sunny/Desktop/Projects/data/synth/realtime_events.jsonl")
    ap.add_argument("--url", default="http://localhost:8000/ingest/raw")
    ap.add_argument("--source", default="SYNTH_STREAM")
    ap.add_argument("--rps", type=float, default=50.0, help="requests per second (throttle)")
    ap.add_argument("--limit", type=int, default=0, help="0 = all lines, else only first N")
    ap.add_argument("--timeout", type=float, default=10.0)
    args = ap.parse_args()

    sleep_s = 0.0 if args.rps <= 0 else 1.0 / args.rps

    sent = 0
    ok = 0
    failed = 0

    with open(args.file, "r", encoding="utf-8") as f:
        for line in f:
            if args.limit and sent >= args.limit:
                break
            line = line.strip()
            if not line:
                continue

            payload = json.loads(line)
            body = {"source": args.source, "payload": payload}

            try:
                resp = post_json(args.url, body, timeout=args.timeout)
                ok += 1 if resp.get("status") == "ok" else 0
            except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError) as e:
                failed += 1
                # minimal retry once
                try:
                    time.sleep(0.2)
                    resp = post_json(args.url, body, timeout=args.timeout)
                    ok += 1 if resp.get("status") == "ok" else 0
                except Exception:
                    pass

            sent += 1
            if sleep_s > 0:
                time.sleep(sleep_s)

            if sent % 200 == 0:
                print(f"sent={sent} ok={ok} failed={failed}")

    print(f"DONE sent={sent} ok={ok} failed={failed}")

if __name__ == "__main__":
    main()
