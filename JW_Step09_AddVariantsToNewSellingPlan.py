import os
import time
import math
import json
from typing import List, Dict, Any, Tuple
import requests
from supabase import create_client, Client
from datetime import datetime
from dotenv import load_dotenv

# === Confit Env ===
load_dotenv()

# ========= Config (env-driven) =========
STOQ_API_ACCESS_KEY = os.getenv("STOQ_API_ACCESS_KEY")
STOQ_API_BASE = os.getenv("STOQ_API_BASE")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_ANON_KEY")
OFFERS_TABLE = os.getenv("PREORDER_OFFER_TABLE")
VARIANTS_TABLE = os.getenv("PREORDER_VARIANT_TABLE")

# batching & retry
CHUNK_SIZE = 50          # how many variant ids per POST
MAX_RETRIES = 3
INITIAL_BACKOFF = 1.0  # seconds
TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "30"))  # seconds

# whether to flip offer status to 'done' once all chunks post OK
STOQ_PREORDER_MAX_COUNT = None
MARK_DONE_ON_SUCCESS = "true"

OUTPUT_DIR = "./"

def save_skipped_invalid(offer_id: str, plan_id: str, variant_ids: list[int], note: str = "") -> str:
    """Write the skipped_invalid list to a JSON file and return the path."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    fname = f"skipped_{offer_id}_{ts}.json" if not note else f"skipped_{offer_id}_{note}_{ts}.json"
    path = os.path.join(OUTPUT_DIR, fname)
    payload = {
        "offer_id": offer_id,
        "plan_id": plan_id,
        "count": len(variant_ids),
        "variant_ids": sorted({int(v) for v in variant_ids}),
        "generated_at": datetime.now().isoformat(),
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    print(f"📄 Saved skipped_invalid ({len(variant_ids)}) -> {path}")
    return path

def make_supabase() -> Client:
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise RuntimeError("Missing SUPABASE_URL or SUPABASE_KEY env vars.")
    return create_client(SUPABASE_URL, SUPABASE_KEY)

def fetch_offers_to_insert(sb: Client) -> List[Dict[str, Any]]:
    resp = sb.table(OFFERS_TABLE).select("*").eq("status", "insert").execute()
    return resp.data or []

def fetch_variant_ids_for_offer(sb: Client, offer_id: Any) -> List[int]:
    resp = sb.table(VARIANTS_TABLE).select("variant_id").eq("offer_id", offer_id).execute()
    rows = resp.data or []
    ids = []
    for r in rows:
        v = r.get("variant_id")
        if v is None:
            continue
        try:
            ids.append(int(v))
        except (TypeError, ValueError):
            pass
    return sorted(set(ids))

def chunked(lst: List[Any], size: int) -> List[List[Any]]:
    return [lst[i:i+size] for i in range(0, len(lst), size)]

def _post_with_retries(url: str, headers: Dict[str, str], payload: Dict[str, Any]) -> requests.Response:
    body = json.dumps(payload)
    backoff = INITIAL_BACKOFF
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.post(url, headers=headers, data=body, timeout=TIMEOUT)
            if 200 <= resp.status_code < 300:
                return resp
            if resp.status_code in (429, 500, 502, 503, 504) and attempt < MAX_RETRIES:
                time.sleep(backoff); backoff *= 2
                continue
            return resp
        except requests.RequestException:
            if attempt == MAX_RETRIES:
                raise
            time.sleep(backoff); backoff *= 2
    return resp  # type: ignore

def stoq_add_variants(plan_id: str, variant_ids: List[int]) -> Tuple[bool, Dict[str, Any]]:
    """
    - Adds variants in chunks.
    - If 422 with 'existing_variants' -> strip and retry remainder (your current behavior).
    - If generic 422, recursively bisect the chunk to isolate & skip bad IDs.
    """
    if not variant_ids:
        return True, {"message": "No variants to add."}

    url = f"{STOQ_API_BASE}/{plan_id}/add_variant"
    headers = {
        "X-Auth-Token": STOQ_API_ACCESS_KEY,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    known_existing = set()
    skipped_invalid: List[int] = []

    def payload_for(ids: List[int]) -> Dict[str, Any]:
        p = {"shopify_variant_ids": ids}
        # if STOQ_PREORDER_MAX_COUNT:
        #     # Only include if provided; leave out otherwise
        #     p["preorder_max_count"] = int(STOQ_PREORDER_MAX_COUNT)
        return p

    def _add_chunk_or_bisect(batch: List[int]) -> Tuple[bool, List[int]]:
        """
        Try to add 'batch'. If generic 422, bisect to find bad IDs.
        Returns (success, skipped_ids).
        """
        if not batch:
            return True, []

        resp = _post_with_retries(url, headers, payload_for(batch))

        if 200 <= resp.status_code < 300:
            return True, []

        if resp.status_code == 422:
            # Try to parse specific 'existing_variants'
            try:
                data = resp.json()
            except ValueError:
                data = {}

            existing = set(data.get("existing_variants") or [])
            if existing:
                # Strip known existing and retry once with remainder
                remainder = [v for v in batch if v not in existing]
                if not remainder:
                    return True, []
                resp2 = _post_with_retries(url, headers, payload_for(remainder))
                if 200 <= resp2.status_code < 300:
                    return True, []
                # If still failing 422 without details, fall through to bisect
                if resp2.status_code != 422:
                    # Non-422, treat as hard fail for this remainder
                    return False, remainder
                # Re-parse for a second pass; if still nothing, bisect remainder
                try:
                    data2 = resp2.json()
                except ValueError:
                    data2 = {}
                existing2 = set(data2.get("existing_variants") or [])
                if existing2:
                    remainder2 = [v for v in remainder if v not in existing2]
                    if not remainder2:
                        return True, []
                    # final attempt
                    resp3 = _post_with_retries(url, headers, payload_for(remainder2))
                    if 200 <= resp3.status_code < 300:
                        return True, []
                    # If still failing, go to bisect on remainder2
                    batch_to_bisect = remainder2
                else:
                    batch_to_bisect = remainder
            else:
                # Generic 422 -> need to bisect the batch
                batch_to_bisect = batch

            # Bisect strategy: if single ID fails, mark it skipped; else split and recurse
            if len(batch_to_bisect) == 1:
                bad = batch_to_bisect[0]
                return True, [bad]
            mid = len(batch_to_bisect) // 2
            left, right = batch_to_bisect[:mid], batch_to_bisect[mid:]

            ok_left, skipped_left = _add_chunk_or_bisect(left)
            ok_right, skipped_right = _add_chunk_or_bisect(right)
            return (ok_left and ok_right), skipped_left + skipped_right

        # Non-2xx non-422 -> hard fail this batch
        return False, batch

    total_chunks = math.ceil(len(variant_ids) / CHUNK_SIZE)
    for idx, raw_batch in enumerate(chunked(variant_ids, CHUNK_SIZE), start=1):
        batch = [v for v in raw_batch if v not in known_existing]
        if not batch:
            print(f"Plan {plan_id}: chunk {idx}/{total_chunks} skipped (already present).")
            continue

        ok, skipped = _add_chunk_or_bisect(batch)
        if skipped:
            skipped_invalid.extend(skipped)
            # Remember these as "bad" to avoid on later chunks
            known_existing |= set(skipped)  # reuse set to filter them out

        if ok:
            added = len(batch) - len(skipped)
            status_msg = []
            if added > 0: status_msg.append(f"added {added}")
            if skipped:   status_msg.append(f"skipped {len(skipped)} invalid")
            msg = ", ".join(status_msg) if status_msg else "no-op"
            print(f"Plan {plan_id}: chunk {idx}/{total_chunks} OK — {msg}.")
            continue

        # Hard failure: report and stop
        return False, {
            "error": "http_error",
            "status": 422,
            "message": "Unresolvable validation error in chunk even after bisection.",
            "failed_chunk": batch,
            "skipped_so_far": skipped_invalid[:200],
        }

    return True, {
        "message": "All chunks processed.",
        "skipped_invalid_variants": sorted(set(skipped_invalid)),
        "note": "Skipped are likely deleted/not-eligible/other validation failures."
    }

def mark_offer_done(sb: Client, offer_id: Any):
    sb.table(OFFERS_TABLE).update({"status": "insert-completed"}).eq("id", offer_id).execute()

def main():
    if not STOQ_API_ACCESS_KEY:
        raise RuntimeError("Missing STOQ_API_ACCESS_KEY.")

    sb = make_supabase()
    offers = fetch_offers_to_insert(sb)
    print(f"Found {len(offers)} offers with status='insert'.")

    failures = []
    succeeded = 0

    for offer in offers:
        offer_id = offer.get("id")
        plan_id = offer.get("stoq_offer_id")
        if not plan_id:
            print(f"Offer {offer_id}: missing stoq_selling_plan_id; skipping.")
            continue

        ids = fetch_variant_ids_for_offer(sb, offer_id)
        print(f"\nProcessing '{offer.get("internal_name")}' offer {offer_id} plan {plan_id} with {len(ids)} variants...")
        ok, info = stoq_add_variants(plan_id, ids)
        if ok:
            succeeded += 1
            print(f"~~ Offer {offer_id} completed: {info}")
            if MARK_DONE_ON_SUCCESS:
                try:
                    mark_offer_done(sb, offer_id)
                    print(f"Offer {offer_id} marked as 'done'.")
                except Exception as e:
                    print(f"##️ Failed to update status for {offer_id}: {e}")
        else:
            print(f"@@ Offer {offer_id} failed: {info}")
            failures.append({"offer_id": offer_id, "plan_id": plan_id, "details": info})

    print("\n==== Summary ====")
    print(f"Succeeded: {succeeded} / {len(offers)}")
    if failures:
        print(json.dumps(failures, indent=2))

if __name__ == "__main__":
    main()
