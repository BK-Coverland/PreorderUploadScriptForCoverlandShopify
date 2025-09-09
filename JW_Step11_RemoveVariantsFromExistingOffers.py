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
OFFER_TABLE = os.getenv("PREORDER_OFFER_TABLE")
VARIANT_TABLE = os.getenv("PREORDER_VARIANT_TABLE")

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
        sb.table(OFFER_TABLE)
          .select("stoq_offer_id, id", "internal_name")
          # .eq("internal_name", "Preorder-20wks-60")
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

def chunked(seq, size):
    for i in range(0, len(seq), size):
        yield seq[i:i+size]

def _coerce_ids_to_int(ids: Iterable[str | int]) -> List[int]:
    ok, bad = [], []
    for x in ids:
        try:
            ok.append(int(x))
        except (ValueError, TypeError):
            bad.append(x)
    if bad:
        print(f"️## Skipping non-numeric IDs ({len(bad)}): {bad[:10]}{' ...' if len(bad) > 10 else ''}")
    return ok

def _retry_remove_batch(stoq_offer_id: str, batch: List[int], max_retries: int = REMOVE_MAX_RETRIES) -> Tuple[bool, Optional[dict], Optional[str]]:
    """Retry a full-batch remove; return (ok, body, err)."""
    backoff = 1.0
    for attempt in range(max_retries + 1):
        ok, body, err = call_stoq_remove_variants(stoq_offer_id, batch)
        if ok:
            return True, body, None
        transient = (err and any(t in err for t in ("429", "502", "503", "504", "timeout", "connection", "Temporary")))
        if attempt < max_retries and transient:
            pytime.sleep(backoff)
            backoff *= 2
            continue
        return False, body, err

def remove_variants_chunked(
    stoq_offer_id: str,
    vids: List[str | int],
    *,
    stoq_to_internal: Optional[Dict[str, str]] = None,
    batch_size: int = REMOVE_BATCH_SIZE,
    on_body_flags: Optional[Callable[[str, str, str, dict, List[int]], None]] = None,
) -> Dict[str, Any]:
    """
    Chunked removal of variants from a STOQ offer.
    - Coerces IDs to int
    - Retries batch on transient errors
    - Falls back to per-item removal on hard errors
    - Optional callback `on_body_flags(action, offer_id, internal_name, body, sent_batch)`
    - Returns summary dict
    """
    internal_name = (stoq_to_internal or {}).get(stoq_offer_id, "")
    unique_vids = sorted(set(_coerce_ids_to_int(vids)))
    total = len(unique_vids)

    removed_ok: List[int] = []
    failed: List[dict] = []
    called_total = 0

    for batch in chunked(unique_vids, batch_size):
        called_total += len(batch)
        ok, body, err = _retry_remove_batch(stoq_offer_id, batch)

        print(f"Called 'remove_variants' for {stoq_offer_id}: {called_total}/{total}")

        if ok:
            if on_body_flags:
                on_body_flags("remove", stoq_offer_id, internal_name, body or {}, batch)
            removed_ok.extend(batch)
            pytime.sleep(REMOVE_RATE_SLEEP)
            continue

        print(f"## Batch remove failed; falling back to per-item. Error: {str(err)[:200] if err else 'unknown'}")
        for vid in batch:
            ok1, body1, err1 = call_stoq_remove_variants(stoq_offer_id, [vid])
            if ok1:
                if on_body_flags:
                    on_body_flags("remove", stoq_offer_id, internal_name, body1 or {}, [vid])
                removed_ok.append(vid)
            else:
                failed.append({
                    "action": "remove",
                    "offer_id": stoq_offer_id,
                    "variant_id": vid,
                    "internal_name": internal_name,
                    "error": err1 or "unknown_error",
                })
            pytime.sleep(REMOVE_RATE_SLEEP)

    print(f" Removed from offer {stoq_offer_id}: requested={total}, removed_ok={len(removed_ok)}, failed={len(failed)}")
    return {
        "offer_id": stoq_offer_id,
        "internal_name": internal_name,
        "requested": total,
        "removed_ok_count": len(removed_ok),
        "failed_count": len(failed),
        "removed_ok_sample": removed_ok[:10],
        "failed_sample": failed[:5],
        "failed": failed,
    }

def call_stoq_remove_variants(stoq_offer_id: str, variant_ids: List[int]) -> Tuple[bool, Optional[dict], Optional[str]]:
    """
    DELETE /{stoq_offer_id}/remove_variant with batching & retries.
    Returns (ok, json_body, err_msg)
    """
    if not STOQ_API_BASE:
        raise RuntimeError("Missing STOQ_API_BASE env var.")
    url = f"{STOQ_API_BASE}/{stoq_offer_id}/remove_variant"
    payload = {"shopify_variant_ids": variant_ids}
    ok, resp, body, err = with_retries(
        requests.delete,
        url,
        headers=stoq_headers(),
        json=payload,
        timeout=REQUEST_TIMEOUT,
    )
    pytime.sleep(RATE_LIMIT_SLEEP)
    return ok, body, err

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

    ok, _, err = _try_add_chunk(
        stoq_offer_id, left,
        preorder_max_count=preorder_max_count,
        timeout=timeout, max_retries=max_retries, base_backoff=base_backoff
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

    ok, _, err = _try_add_chunk(
        stoq_offer_id, right,
        preorder_max_count=preorder_max_count,
        timeout=timeout, max_retries=max_retries, base_backoff=base_backoff
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

        ok, _, err = _try_add_chunk(
            stoq_offer_id, chunk,
            preorder_max_count=preorder_max_count,
            timeout=timeout, max_retries=max_retries, base_backoff=base_backoff
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
    # Supabase Client
    sb = make_supabase()

    # Get offer Ids
    offer_ids = get_stoq_offer_ids_from_db(sb)

    for offer_id, plan_id, internal_nm in offer_ids:
        print("Offer ID:", offer_id, "Plan ID:", plan_id, "Internal Name:", internal_nm)

        if not offer_id or not plan_id:
            raise RuntimeError("Offer not found for internal_name 'Preorder-20wks-40'.")

        # Variants List from Stoq API (strings)
        variant_ids_from_api = get_variants_from_stoq_api(offer_id)
        print("API variant count:", len(variant_ids_from_api))

        # Variants List from DB (strings)
        variants_from_db = fetch_variants_for_offer_ids(sb, VARIANT_TABLE, {plan_id})
        print("DB variant count:", len(variants_from_db))

        # Compare two lists
        set_api = set(variant_ids_from_api)
        set_db = set(variants_from_db)

        to_remove = sorted(list(set_api - set_db))  # present on STOQ, not in DB
        to_add    = sorted(list(set_db - set_api))  # in DB, missing on STOQ

        print("➡️ To Remove (API only):", to_remove[:10], "…" if len(to_remove) > 10 else "")
        print("Count:", len(to_remove))
        print("➡️ To Add (DB only):", to_add[:10], "…" if len(to_add) > 10 else "")
        print("Count:", len(to_add))

        # Ensure ints for API calls
        to_remove_int = _coerce_ids_to_int(to_remove)
        to_add_int    = _coerce_ids_to_int(to_add)

        print(f"🧮 Plan for offer {offer_id}: remove {len(to_remove_int)}, add {len(to_add_int)}")

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

        # 1) Remove variants not in DB but present on STOQ
        print("Start Remove Variants...")
        remove_summary = remove_variants_chunked(
            stoq_offer_id=offer_id,
            vids=to_remove,  # list[str]
            on_body_flags=handle_body_flags
        )
        print(remove_summary)

if __name__ == "__main__":
    main()
