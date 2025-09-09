import os
import time
import json
import math
import requests
from typing import Iterable, List, Set, Dict, Tuple, Optional
from dotenv import load_dotenv

# === Config Env ===
load_dotenv()

# ================== CONFIG ==================
ACCESS_TOKEN = os.getenv("SHOPIFY_ADMIN_TOKEN")
ENDPOINT = f"https://8461ca-35.myshopify.com/admin/api/2025-04/graphql.json"
TARGET_PROFILE_ID = os.getenv("SHOPIFY_DELIVERY_PROFILE_ID")

# --- Supabase REST ---
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_ANON_KEY")

# Supabase paging
SB_PAGE_SIZE = 1000  # postgrest page size via Range header
SB_TABLE = os.getenv("PREORDER_VARIANT_TABLE")  # columns: id (uuid), offer_id (uuid), variant_id (text)

# Shopify tuning
BATCH_SIZE = 50
MAX_RETRIES = 4
BACKOFF_BASE = 1.6  # seconds
TIMEOUT_SEC = 60

# File with one GID per line, like: gid://shopify/ProductVariant/123...
VARIANT_IDS_FILE = "variant_ids.txt"

# ============================================
def _post_graphql(query: str, variables: dict) -> dict:
    for attempt in range(1, MAX_RETRIES + 1):
        resp = requests.post(
            ENDPOINT,
            headers={
                "X-Shopify-Access-Token": ACCESS_TOKEN,
                "Content-Type": "application/json",
            },
            json={"query": query, "variables": variables},
            timeout=TIMEOUT_SEC,
        )
        if resp.status_code == 200:
            data = resp.json()
            if "errors" in data:
                raise RuntimeError(f"GraphQL errors: {data['errors']}")
            return data

        if resp.status_code in (429, 500, 502, 503, 504) and attempt < MAX_RETRIES:
            time.sleep(BACKOFF_BASE * attempt)
            continue

        raise RuntimeError(f"HTTP {resp.status_code}: {resp.text}")

    raise RuntimeError("Unexpected fallthrough in _post_graphql retries")

# ---------- Query: enumerate existing variant members in the profile ----------
PROFILE_ITEMS_QUERY = """
query ProfileItems($id: ID!, $cursor: String) {
  deliveryProfile(id: $id) {
    id
    profileItems(first: 100, after: $cursor) {
      edges {
        cursor
        node {
          variants(first: 250) {
            edges { node { id } }
            pageInfo { hasNextPage }
          }
        }
      }
      pageInfo { hasNextPage }
    }
  }
}
"""

def get_existing_variant_ids_in_profile(profile_id: str) -> Set[str]:
    existing: Set[str] = set()
    cursor: Optional[str] = None

    while True:
        data = _post_graphql(PROFILE_ITEMS_QUERY, {"id": profile_id, "cursor": cursor})
        dp = data["data"]["deliveryProfile"]
        if not dp:
            raise ValueError(f"DeliveryProfile not found: {profile_id}")

        edges = dp["profileItems"]["edges"]
        for edge in edges:
            v_conn = edge["node"]["variants"]
            for v_edge in v_conn["edges"]:
                existing.add(v_edge["node"]["id"])
            # >250 per product is unlikely (standard products max 100 variants)

        if dp["profileItems"]["pageInfo"]["hasNextPage"]:
            cursor = edges[-1]["cursor"]
        else:
            break

    return existing

# ---------- Mutation: ASSOCIATE variants to the delivery profile ----------
DELIVERY_PROFILE_UPDATE_ASSOCIATE = """
mutation deliveryProfileUpdate($id: ID!, $profile: DeliveryProfileInput!) {
  deliveryProfileUpdate(id: $id, profile: $profile) {
    profile { id }
    userErrors { field message }
  }
}
"""

def chunked(iterable: Iterable[str], n: int) -> Iterable[List[str]]:
    batch: List[str] = []
    for item in iterable:
        batch.append(item)
        if len(batch) >= n:
            yield batch
            batch = []
    if batch:
        yield batch

def add_variants_to_profile(profile_id: str, variant_ids: List[str]) -> Tuple[int, List[Dict]]:
    added_count = 0
    errors: List[Dict] = []

    for idx, batch in enumerate(chunked(variant_ids, BATCH_SIZE), start=1):
        variables = {"id": profile_id, "profile": {"variantsToAssociate": batch}}
        try:
            data = _post_graphql(DELIVERY_PROFILE_UPDATE_ASSOCIATE, variables)
        except Exception as e:
            errors.append({"batch_index": idx, "memberIds": batch, "error": str(e)})
            continue

        payload = data["data"]["deliveryProfileUpdate"]
        uerrs = payload.get("userErrors") or []
        if uerrs:
            errors.append({"batch_index": idx, "memberIds": batch, "userErrors": uerrs})
        else:
            added_count += len(batch)

        time.sleep(0.25)  # polite pacing

    return added_count, errors


def remove_variants_from_profile(profile_id: str, variant_ids: List[str]) -> Tuple[int, List[Dict]]:
    removed_count = 0
    errors: List[Dict] = []

    for idx, batch in enumerate(chunked(variant_ids, BATCH_SIZE), start=1):
        variables = {"id": profile_id, "profile": {"variantsToDissociate": batch}}
        try:
            data = _post_graphql(DELIVERY_PROFILE_UPDATE_ASSOCIATE, variables)
        except Exception as e:
            errors.append({"op": "dissociate", "batch_index": idx, "memberIds": batch, "error": str(e)})
            continue

        payload = data["data"]["deliveryProfileUpdate"]
        uerrs = payload.get("userErrors") or []
        if uerrs:
            errors.append({"op": "dissociate", "batch_index": idx, "memberIds": batch, "userErrors": uerrs})
        else:
            removed_count += len(batch)

        time.sleep(0.25)  # polite pacing

    return removed_count, errors

# ---------- Supabase: load variant_ids ----------
def _sb_headers() -> Dict[str, str]:
    return {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

def load_variant_ids_from_supabase() -> List[str]:
    """
    Pulls non-null variant_id values from stoq_selling_plan_variants with pagination.
    """
    url = f"{SUPABASE_URL}/rest/v1/{SB_TABLE}?select=variant_id&variant_id=not.is.null"
    headers = _sb_headers()

    collected: List[str] = []
    page = 0

    while True:
        start = page * SB_PAGE_SIZE
        end = start + SB_PAGE_SIZE - 1
        r = requests.get(url, headers={**headers, "Range": f"{start}-{end}"}, timeout=TIMEOUT_SEC)

        if r.status_code not in (200, 206):
            raise RuntimeError(f"Supabase HTTP {r.status_code}: {r.text}")

        rows = r.json()
        if not rows:
            break

        for row in rows:
            vid = str(row.get("variant_id") or "").strip()
            if vid:
                collected.append(vid)

        if len(rows) < SB_PAGE_SIZE:
            break

        page += 1

    return collected

def to_variant_gid(value: str) -> str:
    if value.startswith("gid://shopify/ProductVariant/"):
        return value
    if value.isdigit():
        return f"gid://shopify/ProductVariant/{value}"
    return value  # GraphQL will reject invalids; they'll appear in userErrors

def main():
    # 0) Load from Supabase and de-duplicate
    print("Loading variant IDs from Supabase…")
    raw_ids = load_variant_ids_from_supabase()
    before_dedupe = len(raw_ids)
    normalized = [to_variant_gid(v) for v in raw_ids]
    # keep order, remove dupes
    unique_variant_ids = list(dict.fromkeys(normalized))
    print(f"Loaded {before_dedupe} rows; unique normalized = {len(unique_variant_ids)}")

    desired_set = set(unique_variant_ids)

    # Edge: nothing to sync
    if not unique_variant_ids:
        print("No variant IDs found in Supabase; will remove all variants from the profile.")
        desired_set = set()

    # 1) Fetch variants already in the target profile
    print("Fetching existing variant members from target profile…")
    existing = get_existing_variant_ids_in_profile(TARGET_PROFILE_ID)
    print(f"Existing members: {len(existing)}")

    # 2) Compute work sets
    to_add = [vid for vid in unique_variant_ids if vid not in existing]
    to_remove = [vid for vid in existing if vid not in desired_set]

    print(f"Variants to remove (not in DB set): {len(to_remove)}")
    print(f"Variants to add (after skip): {len(to_add)}")

    # 3) Apply changes (remove first for strict “only-from-DB” invariant)
    removed_count, remove_errors = (0, [])
    if to_remove:
        print(f"Removing in batches of {BATCH_SIZE}…")
        removed_count, remove_errors = remove_variants_from_profile(TARGET_PROFILE_ID, to_remove)

    added_count, add_errors = (0, [])
    if to_add:
        print(f"Adding in batches of {BATCH_SIZE}…")
        added_count, add_errors = add_variants_to_profile(TARGET_PROFILE_ID, to_add)

    # 4) Summary
    errors = (remove_errors or []) + (add_errors or [])
    summary = {
        "target_profile_id": TARGET_PROFILE_ID,
        "supabase_rows": before_dedupe,
        "unique_input_ids": len(unique_variant_ids),
        "already_in_profile": len(existing & set(unique_variant_ids)),
        "attempted_add": len(to_add),
        "attempted_remove": len(to_remove),
        "added_count": added_count,
        "removed_count": removed_count,
        "batches_add": math.ceil(len(to_add) / BATCH_SIZE) if to_add else 0,
        "batches_remove": math.ceil(len(to_remove) / BATCH_SIZE) if to_remove else 0,
        "errors_count": len(errors),
    }
    print(json.dumps(summary, indent=2))

    if errors:
        report_path = "delivery_profile_update_errors.json"
        with open(report_path, "w", encoding="utf-8") as f:
            json.dump(errors, f, indent=2)
        print(f"\nSaved error report: {report_path}")

if __name__ == "__main__":
    main()