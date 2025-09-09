#!/usr/bin/env python3
import os
import time
import json
import logging
from typing import List, Dict, Tuple, Optional
import requests
from supabase import create_client, Client
from dotenv import load_dotenv

# === Confit Env ===
load_dotenv()

# =========================
# Configuration
# =========================
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_ANON_KEY = os.getenv("SUPABASE_ANON_KEY")
SUPABASE_TABLE = os.getenv("PREORDER_OFFER_TABLE")

STOQ_API_BASE = os.getenv("STOQ_API_BASE")
STOQ_API_ACCESS_KEY = os.getenv("STOQ_API_ACCESS_KEY")

PAGE_SIZE = 1000

# Retry settings for Stoq DELETE calls
MAX_RETRIES = 3
INITIAL_BACKOFF_SEC = 1.0
TIMEOUT_SEC = 30.0

# Logging
logging.basicConfig(
    level="INFO",
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("stoq-delete-offers")

# =========================
# Supabase helpers
# =========================
def fetch_offer_ids_to_delete() -> List[str]:
    url = f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}"
    headers = {
        "apikey": SUPABASE_ANON_KEY,
        "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
        "Accept": "application/json",
        "Prefer": "count=exact"
    }

    from_idx = 0
    out: List[str] = []
    seen = set()

    while True:
        to_idx = from_idx + PAGE_SIZE - 1
        params = {
            "select": "stoq_offer_id",
            "status": "eq.delete",
            "stoq_offer_id": "not.is.null"
        }
        range_header = {"Range": f"{from_idx}-{to_idx}"}

        resp = requests.get(url, headers={**headers, **range_header}, params=params, timeout=TIMEOUT_SEC)
        if resp.status_code not in (200, 206):
            raise RuntimeError(f"Supabase fetch error: {resp.status_code} {resp.text}")

        rows = resp.json()
        if not rows:
            break

        for row in rows:
            oid = str(row.get("stoq_offer_id") or "").strip()
            if oid and oid not in seen:
                seen.add(oid)
                out.append(oid)

        if len(rows) < PAGE_SIZE:
            break
        from_idx += PAGE_SIZE

    logger.info(f"Found {len(out)} stoq_offer_id(s) with status='delete'")
    return out


def update_supabase_status(offer_id: str, new_status: str = "delete-completed") -> bool:
    """
    PATCH Supabase row where stoq_offer_id = offer_id to new_status.
    """
    url = f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}"
    headers = {
        "apikey": SUPABASE_ANON_KEY,
        "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Prefer": "return=representation"  # return updated row
    }
    params = {"stoq_offer_id": f"eq.{offer_id}"}
    payload = {"status": new_status}

    resp = requests.patch(url, headers=headers, params=params, json=payload, timeout=TIMEOUT_SEC)
    if resp.status_code not in (200, 204):
        logger.error(f"[{offer_id}] Failed to update Supabase status -> {resp.status_code} {resp.text}")
        return False
    logger.info(f"[{offer_id}] Supabase status updated to '{new_status}'")
    return True

# =========================
# Stoq API delete with retries
# =========================
def delete_stoq_offer(offer_id: str) -> Tuple[bool, Optional[Dict], Optional[str]]:
    if not STOQ_API_ACCESS_KEY:
        raise RuntimeError("Missing STOQ_API_ACCESS_KEY")

    url = f"{STOQ_API_BASE}/{offer_id}"
    headers = {
        "X-Auth-Token": STOQ_API_ACCESS_KEY,
        "Accept": "application/json",
        "Content-Type": "application/json",
    }

    backoff = INITIAL_BACKOFF_SEC
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.delete(url, headers=headers, timeout=TIMEOUT_SEC)
        except requests.RequestException as e:
            err = f"network_error: {e}"
            logger.warning(f"[{offer_id}] Attempt {attempt}/{MAX_RETRIES} -> {err}")
            if attempt < MAX_RETRIES:
                time.sleep(backoff)
                backoff *= 2
                continue
            return False, None, err

        if resp.status_code in (200, 204):
            try:
                body = resp.json() if resp.text else None
            except ValueError:
                body = None
            logger.info(f"[{offer_id}] Deleted from Stoq.")
            return True, body, None

        if resp.status_code == 404:
            logger.info(f"[{offer_id}] Already deleted (404). Treating as success.")
            return True, None, None

        if resp.status_code == 429 or (500 <= resp.status_code <= 599):
            logger.warning(f"[{offer_id}] Attempt {attempt}/{MAX_RETRIES} -> HTTP {resp.status_code}. Retrying…")
            if attempt < MAX_RETRIES:
                time.sleep(backoff)
                backoff *= 2
                continue

        err = f"http_error {resp.status_code}: {resp.text}"
        logger.error(f"[{offer_id}] {err}")
        return False, None, err

    return False, None, "unknown_error"

# =========================
# Main
# =========================
def main():
    offer_ids = fetch_offer_ids_to_delete()
    if not offer_ids:
        logger.info("No offers to delete. Exiting.")
        return

    successes: List[str] = []
    failures: List[Dict[str, str]] = []
    update_failures: List[str] = []

    for idx, offer_id in enumerate(offer_ids, start=1):
        logger.info(f"({idx}/{len(offer_ids)}) Deleting Stoq offer {offer_id} …")
        ok, _, err = delete_stoq_offer(offer_id)
        if ok:
            successes.append(offer_id)
            # update Supabase status
            if not update_supabase_status(offer_id, "delete-completed"):
                update_failures.append(offer_id)
        else:
            failures.append({"stoq_offer_id": offer_id, "error": err or "unknown_error"})

    logger.info("==== Summary ====")
    logger.info(f"Total: {len(offer_ids)} | Deleted: {len(successes)} | Failed: {len(failures)} | Supabase update failed: {len(update_failures)}")

    if failures or update_failures:
        out_path = "stoq_delete_failures.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(
                {"stoq_failures": failures, "supabase_update_failures": update_failures},
                f, indent=2, ensure_ascii=False
            )
        logger.info(f"Wrote failures to {out_path}")
    else:
        logger.info("All offers deleted and Supabase updated 🎉")

    ## Delete from Table.
    supabase = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)
    resp = (
        supabase
        .table(SUPABASE_TABLE)
        .delete()
        .eq("status", "delete-completed")  # WHERE status = 'delete-completed'
        .execute()
    )
    print("Romved from table - delete-completed")

if __name__ == "__main__":
    main()
