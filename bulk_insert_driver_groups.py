import csv
import os
import time
import random
import argparse
from typing import List, Set, Tuple
import requests

# --- Webfleet extern endpoint ---
URL = "https://csv.webfleet.com/extern"

# --- Credentials (as requested, baked in) ---
AUTH = {
    "account": "Omexom-nz",
    "apikey": "01896d58-2271-47af-9e4f-e146cc2be7b4",
    "username": "franciswynne",
    "password": "@wynn5Fr4nc1s",
    "lang": "en"
}

# Pacing to respect 10 requests/minute
BASE_SLEEP_BETWEEN_CALLS = 6.2

# Retry/backoff settings
MAX_RETRIES_PER_ITEM = 8
BACKOFF_BASE_SECONDS = 4.0
BACKOFF_CAP_SECONDS = 90.0
JITTER_MIN = 0.4
JITTER_MAX = 1.2

RATE_LIMIT_HINTS = (
    "rate limit",
    "too many requests",
    "request limit",
    "exceeded",
    "throttl",
    "6002",
    "limit exceeded",
)

def is_rate_limited(resp: requests.Response) -> bool:
    if resp is None:
        return False
    if resp.status_code in (429, 503):
        return True
    body = (resp.text or "").lower()
    for hint in RATE_LIMIT_HINTS:
        if hint in body:
            return True
    return False

def is_temporary_server_error(resp: requests.Response) -> bool:
    return resp is not None and resp.status_code >= 500

def read_groups(csv_path: str) -> List[str]:
    groups: List[str] = []
    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        rdr = csv.DictReader(f)
        if not rdr.fieldnames or "drivergroupname" not in rdr.fieldnames:
            raise ValueError("CSV must have a 'drivergroupname' column.")
        for row in rdr:
            name = (row.get("drivergroupname") or "").strip()
            if not name:
                continue
            if (name.startswith('"') and name.endswith('"')) or (name.startswith("'") and name.endswith("'")):
                name = name[1:-1].strip()
            groups.append(name)
    return groups

def validate_and_dedupe(groups: List[str]) -> Tuple[List[str], List[str]]:
    seen: Set[str] = set()
    clean: List[str] = []
    skipped: List[str] = []
    for g in groups:
        if g.lower().startswith("sys$"):
            skipped.append(f"SKIP sys$ rule: {g}")
            continue
        if g in seen:
            skipped.append(f"SKIP duplicate: {g}")
            continue
        seen.add(g)
        clean.append(g)
    return clean, skipped

def get_with_retries(params: dict) -> Tuple[bool, str, str]:
    """
    Returns (success, response_text, note)
    - success True if created OR already exists
    - note 'exists' if detected, else '' or error detail
    """
    attempt = 0
    last_exc: Exception | None = None
    while attempt <= MAX_RETRIES_PER_ITEM:
        attempt += 1
        resp = None
        try:
            resp = requests.get(URL, params=params, timeout=25)
            text = (resp.text or "").strip()
            lower = text.lower()
            ok = resp.ok and ("error" not in lower)
            exists = ("already exists" in lower) or ("exists already" in lower)
            if ok or exists:
                return True, text, ("exists" if exists else "")
            if is_rate_limited(resp) or is_temporary_server_error(resp):
                # backoff handled below
                pass
            else:
                return False, text, ""
        except requests.RequestException as e:
            last_exc = e
        # Backoff before next attempt
        backoff = min(BACKOFF_BASE_SECONDS * (2 ** (attempt - 1)), BACKOFF_CAP_SECONDS)
        jitter = random.uniform(JITTER_MIN, JITTER_MAX)
        wait = backoff + jitter
        print(f"   Retry {attempt}/{MAX_RETRIES_PER_ITEM} after {wait:.1f}s (rate/temporary issue)")
        time.sleep(wait)
    if last_exc is not None:
        return False, "", f"exception: {last_exc}"
    return False, "", "retry_exhausted"

def insert_driver_group_get(group: str) -> Tuple[bool, str, str]:
    params = dict(AUTH)
    params["action"] = "insertDriverGroup"
    params["drivergroupname"] = group
    return get_with_retries(params)

def main() -> None:
    import sys
    ap = argparse.ArgumentParser(description="Bulk insert Webfleet driver groups from CSV (GET method, with backoff).")
    ap.add_argument("--csv", required=True, help="Path to CSV with 'drivergroupname' column.")
    ap.add_argument("--results", default="driver_groups_results.csv", help="Output results CSV path.")
    ap.add_argument("--dry-run", action="store_true", help="Validate only; no API calls.")
    ap.add_argument("--resume", action="store_true", help="Skip names already marked Success in results.")
    ap.add_argument("--sleep", type=float, default=BASE_SLEEP_BETWEEN_CALLS, help="Seconds between successful calls.")
    args = ap.parse_args()

    raw = read_groups(args.csv)
    groups, skipped = validate_and_dedupe(raw)

    # Resume support
    already_done: Set[str] = set()
    append_mode = False
    if args.resume and os.path.exists(args.results):
        with open(args.results, newline="", encoding="utf-8") as f:
            rdr = csv.DictReader(f)
            if rdr.fieldnames and "drivergroupname" in rdr.fieldnames and "status" in rdr.fieldnames:
                for r in rdr:
                    if (r.get("status") or "").lower() == "success":
                        gname = (r.get("drivergroupname") or "").strip()
                        if gname:
                            already_done.add(gname)
        append_mode = True

    with open(args.results, "a" if append_mode else "w", newline="", encoding="utf-8") as outf:
        wr = csv.DictWriter(outf, fieldnames=["drivergroupname", "status", "response", "note"])
        if not append_mode:
            wr.writeheader()

        # upfront skips
        for s in skipped:
            wr.writerow({
                "drivergroupname": s.split(": ", 1)[-1],
                "status": "skipped",
                "response": "",
                "note": s
            })

        if args.dry_run:
            print(f"[DRY RUN] Valid groups: {len(groups)} | Skipped upfront: {len(skipped)} | Results -> {args.results}")
            return

        processed = 0
        for g in groups:
            if args.resume and g in already_done:
                wr.writerow({"drivergroupname": g, "status": "skipped", "response": "", "note": "resume: already success"})
                continue

            print(f"GET inserting: {g}")
            success, response_text, note = insert_driver_group_get(g)

            if success:
                wr.writerow({"drivergroupname": g, "status": "success", "response": response_text, "note": note})
                print(f"  OK  -> {response_text[:160].replace('\\n', ' ')}")
                time.sleep(max(0.0, args.sleep))
            else:
                wr.writerow({"drivergroupname": g, "status": "error", "response": response_text, "note": note})
                print(f"  ERR -> {(note or response_text)[:160].replace('\\n', ' ')}")

            processed += 1

    print("Done. Check results file for details.")

if __name__ == "__main__":
    main()
