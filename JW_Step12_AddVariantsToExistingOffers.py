import json
import os, httpx
from typing import List, Dict, Iterable, Tuple, Any, Optional, Callable
import requests
from supabase import create_client, Client
from datetime import datetime, UTC
import time as pytime
from dotenv import load_dotenv

# === Config Env ===
load_dotenv()

STOQ_API_BASE = os.getenv("STOQ_API_BASE")  # e.g. https://app.stoqapp.com/api/v1/external/preorders
STOQ_API_ACCESS_KEY = os.getenv("STOQ_API_ACCESS_KEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_ANON_KEY")

# Batching & retries
ADD_BATCH_SIZE = 50        # tune if needed
REMOVE_BATCH_SIZE = 10      # tune if needed - small batches to avoid silent errors
REQUEST_TIMEOUT = 30       # seconds per request
RETRY_TIMES = 3
REMOVE_MAX_RETRIES = 2
RETRY_BASE_SLEEP = 1.0     # seconds (exponential backoff)
REMOVE_RATE_SLEEP = 0.5    # seconds between remove calls
RATE_LIMIT_SLEEP = 0.15    # small pause between requests

DEFAULT_HEADERS = {
    "X-Auth-Token": STOQ_API_ACCESS_KEY or "",
    "Accept": "application/json",
    "Content-Type": "application/json",
}

def get_stoq_offer_ids_from_db(sb: Client) -> List[Tuple[Optional[str], Optional[str]]]:
    """
    Fetch all (stoq_offer_id, id) pairs for rows matching the internal_name.
    Returns a list of tuples. If no rows, returns an empty list.
    """
    response = (
        sb.table("stoq_selling_plan_offers")
          .select("stoq_offer_id, id", "internal_name")
          # .eq("internal_name", "Preorder-16wks-40")
          .eq("status", "update")
          .execute()
    )

    results: List[Tuple[Optional[str], Optional[str], Optional[str]]] = []
    if response.data:
        for row in response.data:
            results.append(( row.get("stoq_offer_id"), row.get("id"), row.get("internal_name") ))
    return results

def make_supabase() -> Client:
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise RuntimeError("Missing SUPABASE_URL or SUPABASE_KEY env vars.")
    return create_client(SUPABASE_URL, SUPABASE_KEY)

def get_variants_from_stoq_api(stoq_offer_id: str) -> List[str]:
    if not STOQ_API_BASE:
        raise RuntimeError("Missing STOQ_API_BASE env var.")
    url = f"{STOQ_API_BASE}/{stoq_offer_id}/product_variants"
    with httpx.Client(timeout=30) as client:
        r = client.get(url, headers={"X-Auth-Token": STOQ_API_ACCESS_KEY})
        r.raise_for_status()
        data = r.json()
        variant_ids = [str(v["shopify_variant_id"]) for v in data.get("product_variants", [])]
    return variant_ids

def fetch_variants_for_offer_ids(sb: Client, table: str, offer_ids: Iterable[str]) -> List[str]:
    """
    Fetch variant_ids for a set of offer_ids in chunks using .in_ filter.
    Returns a flat list of variant_id strings.
    """
    offer_ids = list(offer_ids)
    if not offer_ids:
        return []

    out: List[str] = []
    CHUNK = 1000
    for i in range(0, len(offer_ids), CHUNK):
        chunk = offer_ids[i:i+CHUNK]
        resp = (
            sb.table(table)
              .select("variant_id")
              .in_("offer_id", chunk)
              .execute()
        )
        if resp.data:
            out.extend(str(row["variant_id"]) for row in resp.data if "variant_id" in row)
    return out

def stoq_headers() -> Dict[str, str]:
    if not STOQ_API_ACCESS_KEY:
        raise RuntimeError("Missing STOQ_API_ACCESS_KEY environment variable.")
    return {
        "X-Auth-Token": STOQ_API_ACCESS_KEY,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

def with_retries(callable_fn, *args, **kwargs) -> Tuple[bool, Optional[requests.Response], Optional[dict], Optional[str]]:
    """
    Run a requests call with retries.
    Returns: (ok, response, json_body_or_None, error_message_or_None)
    """
    last_err = None
    for attempt in range(1, RETRY_TIMES + 1):
        try:
            resp: requests.Response = callable_fn(*args, **kwargs)
            if 200 <= resp.status_code < 300:
                try:
                    body = resp.json() if resp.content else None
                except ValueError:
                    body = None
                return True, resp, body, None
            else:
                try:
                    _ = resp.json()  # parse to help visibility if needed
                except ValueError:
                    pass
                last_err = f"HTTP {resp.status_code}: {resp.text[:500]}"
        except requests.RequestException as e:
            last_err = f"RequestException: {e}"

        if attempt < RETRY_TIMES:
            sleep_s = RETRY_BASE_SLEEP * (2 ** (attempt - 1))
            pytime.sleep(sleep_s)
    # failed after retries
    return False, None, None, last_err

def _coerce_ids_to_int(ids: Iterable[str | int]) -> List[int]:
    ok, bad = [], []
    for x in ids:
        try:
            ok.append(int(x))
        except (ValueError, TypeError):
            bad.append(x)
    if bad:
        print(f"## Skipping non-numeric IDs ({len(bad)}): {bad[:10]}{' ...' if len(bad) > 10 else ''}")
    return ok

def _post_with_retries(
    url: str,
    payload: Dict[str, Any],
    headers: Dict[str, str],
    *,
    timeout: int = 60,
    max_retries: int = 3,
    base_backoff: float = 1.0,
) -> requests.Response:
    """
    POST with retries for transient failures (network, 429, 5xx, timeouts).
    Does NOT retry 4xx except 409/422 where caller wants to inspect.
    """
    attempt = 0
    while True:
        try:
            resp = requests.post(url, headers=headers, data=json.dumps(payload), timeout=timeout)
        except requests.exceptions.RequestException:
            attempt += 1
            if attempt > max_retries:
                raise
            pytime.sleep(base_backoff * (2 ** (attempt - 1)))
            continue

        if resp.status_code in (429,) or 500 <= resp.status_code < 600:
            attempt += 1
            if attempt > max_retries:
                return resp
            ra = resp.headers.get("Retry-After")
            if ra:
                try:
                    delay = float(ra)
                except ValueError:
                    delay = base_backoff * (2 ** (attempt - 1))
            else:
                delay = base_backoff * (2 ** (attempt - 1))
            pytime.sleep(delay)
            continue

        return resp

def _is_job_in_progress(err: Optional[str]) -> bool:
    t = (err or "").lower()
    return "another job is in progress" in t or "job is in progress" in t

def _try_add_chunk_with_lock_wait(
    stoq_offer_id: str,
    ids: List[int],
    *,
    preorder_max_count: Optional[int],
    timeout: int,
    max_retries: int,
    base_backoff: float,
    # lock wait controls
    lock_wait_seconds: int = 180,      # total wait budget for a locked offer
    lock_backoff_start: float = 1.0,   # initial sleep when locked
    lock_backoff_factor: float = 1.8,  # growth factor
    lock_backoff_cap: float = 12.0     # max sleep between probes
) -> Tuple[bool, Optional[Dict[str, Any]], Optional[str]]:
    """
    Calls _try_add_chunk but, if the offer is 'job in progress' locked (409),
    it patiently waits with exponential backoff until the lock clears or the
    lock_wait_seconds budget is exhausted.
    """
    import time as _t
    start = _t.time()
    sleep_s = lock_backoff_start

    while True:
        ok, body, err = _try_add_chunk(
            stoq_offer_id, ids,
            preorder_max_count=preorder_max_count,
            timeout=timeout, max_retries=max_retries, base_backoff=base_backoff
        )
        if ok:
            return True, body, None

        # If it's a lock, wait and retry this SAME chunk (don't split).
        if _is_job_in_progress(err):
            if _t.time() - start >= lock_wait_seconds:
                # Give the caller the lock error if we timed out
                return False, None, err
            _t.sleep(sleep_s)
            sleep_s = min(lock_backoff_cap, sleep_s * lock_backoff_factor)
            continue

        # Non-lock error -> let caller handle (e.g., 422 split logic)
        return False, None, err

def _try_add_chunk(
    stoq_offer_id: str,
    ids: List[int],
    *,
    preorder_max_count: Optional[int],
    timeout: int,
    max_retries: int,
    base_backoff: float,
) -> Tuple[bool, Optional[Dict[str, Any]], Optional[str]]:
    """
    Attempt to add a chunk once (with transient retries inside).
    Returns (ok, json_or_none, err_text_or_none).
    """
    if not STOQ_API_BASE:
        raise RuntimeError("Missing STOQ_API_BASE env var.")
    url = f"{STOQ_API_BASE}/{stoq_offer_id}/add_variant"
    payload = {"shopify_variant_ids": ids}
    if preorder_max_count is not None:
        payload["preorder_max_count"] = preorder_max_count

    resp = _post_with_retries(
        url, payload, DEFAULT_HEADERS, timeout=timeout,
        max_retries=max_retries, base_backoff=base_backoff
    )

    if 200 <= resp.status_code < 300:
        try:
            return True, resp.json() if resp.text else {}, None
        except ValueError:
            return True, {}, None

    if resp.status_code in (409, 422):
        return False, None, resp.text

    return False, None, f"HTTP {resp.status_code}: {resp.text}"

def _divide_and_conquer_on_422(
    stoq_offer_id: str,
    ids: List[int],
    *,
    preorder_max_count: Optional[int],
    timeout: int,
    max_retries: int,
    base_backoff: float,
    skipped_invalid: List[int],
    added_ok: List[int],
) -> None:
    """
    For a set of ids that produced 422 as a group, split into halves recursively.
    Any single ID that still triggers 422 is considered 'skipped/invalid/already-added'.
    """
    if not ids:
        return

    if len(ids) == 1:
        skipped_invalid.append(ids[0])
        return

    mid = len(ids) // 2
    left = ids[:mid]
    right = ids[mid:]

    ok, _, err = _try_add_chunk_with_lock_wait(
        stoq_offer_id, left,
        preorder_max_count=preorder_max_count,
        timeout=timeout, max_retries=max_retries, base_backoff=base_backoff,
        lock_wait_seconds=180,
        lock_backoff_start=1.0,
        lock_backoff_factor=1.8,
        lock_backoff_cap=12.0
    )
    if ok:
        added_ok.extend(left)
    else:
        _divide_and_conquer_on_422(
            stoq_offer_id, left,
            preorder_max_count=preorder_max_count,
            timeout=timeout, max_retries=max_retries, base_backoff=base_backoff,
            skipped_invalid=skipped_invalid, added_ok=added_ok
        )

    ok, _, err = _try_add_chunk_with_lock_wait(
        stoq_offer_id, left,
        preorder_max_count=preorder_max_count,
        timeout=timeout, max_retries=max_retries, base_backoff=base_backoff,
        lock_wait_seconds=180,
        lock_backoff_start=1.0,
        lock_backoff_factor=1.8,
        lock_backoff_cap=12.0
    )
    if ok:
        added_ok.extend(right)
    else:
        _divide_and_conquer_on_422(
            stoq_offer_id, right,
            preorder_max_count=preorder_max_count,
            timeout=timeout, max_retries=max_retries, base_backoff=base_backoff,
            skipped_invalid=skipped_invalid, added_ok=added_ok
        )

def call_stoq_add_variants(
    stoq_offer_id: str,
    variant_ids: List[int],
    *,
    preorder_max_count: Optional[int] = None,
    chunk_size: int = 75,
    timeout: int = 90,
    max_retries: int = 3,
    base_backoff: float = 1.0,
    write_skipped_file: bool = True,
    skipped_filename_prefix: str = "skipped_invalid_add",
) -> Tuple[bool, Dict[str, Any], Optional[str]]:
    """
    Robust add-variants.
    Returns (overall_ok, summary_dict, err_message_or_none).
    """
    total = len(variant_ids)
    added_ok: List[int] = []
    skipped_invalid: List[int] = []
    first_error: Optional[str] = None

    for i in range(0, total, chunk_size):
        chunk = variant_ids[i : i + chunk_size]

        ok, _, err = _try_add_chunk_with_lock_wait(
            stoq_offer_id, chunk,
            preorder_max_count=preorder_max_count,
            timeout=timeout, max_retries=max_retries, base_backoff=base_backoff,
            lock_wait_seconds=180,  # tune as needed
            lock_backoff_start=1.0,
            lock_backoff_factor=1.8,
            lock_backoff_cap=12.0
        )

        if ok:
            added_ok.extend(chunk)
            continue

        if err and ("422" in err or "already" in err.lower() or "unprocessable" in err.lower()):
            _divide_and_conquer_on_422(
                stoq_offer_id, chunk,
                preorder_max_count=preorder_max_count,
                timeout=timeout, max_retries=max_retries, base_backoff=base_backoff,
                skipped_invalid=skipped_invalid, added_ok=added_ok
            )
            if first_error is None:
                first_error = f"Some IDs could not be added (likely already present). Example error: {err[:200]}"
            continue

        if first_error is None:
            first_error = err or "Unknown error while adding variants."

    summary: Dict[str, Any] = {
        "offer_id": stoq_offer_id,
        "requested_count": total,
        "added_ok_count": len(added_ok),
        "skipped_invalid_count": len(skipped_invalid),
        "added_ok_sample": added_ok[:10],
        "skipped_invalid_sample": skipped_invalid[:10],
    }

    if write_skipped_file and skipped_invalid:
        ts = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
        fname = f"{skipped_filename_prefix}_{stoq_offer_id}_{ts}.json"
        with open(fname, "w", encoding="utf-8") as f:
            json.dump({
                "offer_id": stoq_offer_id,
                "skipped_invalid_variant_ids": skipped_invalid
            }, f, indent=2)
        summary["skipped_file"] = fname

    overall_ok = (len(added_ok) > 0) and (len(skipped_invalid) == 0 or len(added_ok) >= 1)
    return overall_ok, summary, first_error

def main():
    sb = make_supabase()

    offer_ids = get_stoq_offer_ids_from_db(sb)

    for offer_id, plan_id, internal_nm in offer_ids:
        print("Offer ID:", offer_id, "Plan ID:", plan_id, "Internal Name:", internal_nm)

        if not offer_id or not plan_id:
            raise RuntimeError("Offer not found for internal_name 'Preorder-20wks-40'.")

        # Variants List from Stoq API (strings)
        variant_ids_from_api = get_variants_from_stoq_api(offer_id)
        print("API variant count:", len(variant_ids_from_api))

        # Variants List from DB (strings)
        variants_from_db = fetch_variants_for_offer_ids(sb, "stoq_selling_plan_variants", {plan_id})
        print("DB variant count:", len(variants_from_db))

        # Compare two lists
        set_api = set(variant_ids_from_api)
        set_db = set(variants_from_db)

        to_remove = sorted(list(set_api - set_db))  # present on STOQ, not in DB
        to_add    = sorted(list(set_db - set_api))  # in DB, missing on STOQ

        print("--> To Remove (API only):", to_remove[:10], "…" if len(to_remove) > 10 else "")
        print("Count:", len(to_remove))
        print("--> To Add (DB only):", to_add[:10], "…" if len(to_add) > 10 else "")
        print("Count:", len(to_add))

        # Ensure ints for API calls
        to_remove_int = _coerce_ids_to_int(to_remove)
        to_add_int    = _coerce_ids_to_int(to_add)

        print(f"$$ Plan for offer {offer_id}: remove {len(to_remove_int)}, add {len(to_add_int)}")

        # Collect flags/errors from API bodies
        error_remove_rows: List[dict] = []
        error_add_rows: List[dict] = []
        skipped_invalid_rows: List[dict] = []

        def handle_body_flags(action: str, stoq_offer_id: str, internal_name: str, body: dict, sent_batch: List[int]):
            if not isinstance(body, dict):
                return
            si = body.get("skipped_invalid")
            if isinstance(si, list):
                for vid in si:
                    skipped_invalid_rows.append({
                        "action": action, "offer_id": stoq_offer_id, "variant_id": vid,
                        "internal_name": internal_name, "reason": "skipped_invalid"
                    })
            failed_ids = body.get("failed_variant_ids")
            if isinstance(failed_ids, list):
                for vid in failed_ids:
                    (error_add_rows if action == "add" else error_remove_rows).append({
                        "action": action, "offer_id": stoq_offer_id, "variant_id": vid,
                        "internal_name": internal_name, "error": "failed_variant_ids"
                    })
            err_s = body.get("error")
            if err_s and not si and not failed_ids:
                for vid in sent_batch:
                    (error_add_rows if action == "add" else error_remove_rows).append({
                        "action": action, "offer_id": stoq_offer_id, "variant_id": vid,
                        "internal_name": internal_name, "error": f"api_error: {err_s}"
                    })

        # 2) Add variants in DB but missing on STOQ
        print("Start Add Variants...")
        if to_add_int:
            ok_add, summary_add, err_add = call_stoq_add_variants(
                offer_id,
                to_add_int,
                # preorder_max_count=<optional int>,
                chunk_size=75,
                timeout=90,
                max_retries=3,
                base_backoff=1.0,
                write_skipped_file=True,
                skipped_filename_prefix="skipped_invalid_add",
            )
            print("++ ADD result -> ok:", ok_add)
            if err_add:
                print("ADD error:", err_add[:500])
            if summary_add:
                print("ADD summary:", {k: summary_add.get(k) for k in (
                    "requested_count", "added_ok_count", "skipped_invalid_count",
                    "added_ok_sample", "skipped_invalid_sample", "skipped_file"
                )})
        else:
            print("++ ADD skipped (nothing to add)")


if __name__ == "__main__":
    main()
