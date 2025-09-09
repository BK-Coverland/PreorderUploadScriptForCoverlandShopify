import os
import csv
import math
import time
from typing import Dict, List, Optional, Tuple

import httpx
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from supabase import create_client, Client as SupabaseClient

# --------------------------
# Environment & Constants
# --------------------------
load_dotenv()

SHOPIFY_SHOP = os.getenv("SHOPIFY_SHOP")
SHOPIFY_ADMIN_TOKEN = os.getenv("SHOPIFY_ADMIN_TOKEN")

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_ANON_KEY")

SUPABASE_SCHEMA = "public"
SUPABASE_TABLE = os.getenv("PREORDER_BATCH_VARIANT_TABLE")

COL_VARIANT_ID = "variant_id"
PAGE_SIZE = 5000
BATCH_SIZE = 250
REQUEST_TIMEOUT = 30
OUTPUT_CSV = "shopify_variant_check_report.csv"

if not SHOPIFY_SHOP or not SHOPIFY_ADMIN_TOKEN:
    raise SystemExit("Missing SHOPIFY_SHOP or SHOPIFY_ADMIN_TOKEN in .env")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise SystemExit("Missing SUPABASE_URL or SUPABASE_KEY in .env")

GRAPHQL_URL = f"https://{SHOPIFY_SHOP}/admin/api/2025-04/graphql.json"

NODES_QUERY = """
query Nodes($ids: [ID!]!) {
  nodes(ids: $ids) {
    __typename
    ... on ProductVariant {
      id
    }
  }
}
"""

class ShopifyError(Exception):
    pass

def to_gid(variant_id: int | str) -> str:
    """Convert numeric variant_id to Shopify GID."""
    return f"gid://shopify/ProductVariant/{int(variant_id)}"

def build_headers() -> Dict[str, str]:
    return {
        "X-Shopify-Access-Token": SHOPIFY_ADMIN_TOKEN,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

@retry(
    retry=retry_if_exception_type(ShopifyError),
    wait=wait_exponential(multiplier=1.0, min=1, max=20),
    stop=stop_after_attempt(5),
)
def shopify_graphql(client: httpx.Client, query: str, variables: Dict) -> Dict:
    resp = client.post(GRAPHQL_URL, json={"query": query, "variables": variables}, timeout=REQUEST_TIMEOUT)
    if resp.status_code == 429:
        raise ShopifyError("Rate limited (429)")
    if resp.status_code >= 500:
        raise ShopifyError(f"Shopify server error {resp.status_code}: {resp.text}")
    data = resp.json()
    if "errors" in data and data["errors"]:
        raise ShopifyError(f"GraphQL errors: {data['errors']}")
    return data["data"]

def supabase_client() -> SupabaseClient:
    return create_client(SUPABASE_URL, SUPABASE_KEY)

def fetch_variant_ids_page(sb: SupabaseClient, limit: int, offset: int) -> List[int]:
    schema = sb.schema(SUPABASE_SCHEMA) if SUPABASE_SCHEMA else sb
    res = schema.table(SUPABASE_TABLE).select(COL_VARIANT_ID).range(offset, offset + limit - 1).execute()
    out: List[int] = []
    for row in (res.data or []):
        v = row.get(COL_VARIANT_ID)
        if v is None:
            continue
        try:
            out.append(int(v))
        except (ValueError, TypeError):
            continue
    return out

def fetch_all_variant_ids(sb: SupabaseClient, page_size: int) -> List[int]:
    all_ids: List[int] = []
    offset = 0
    while True:
        page = fetch_variant_ids_page(sb, page_size, offset)
        if not page:
            break
        all_ids.extend(page)
        offset += page_size
        if len(page) < page_size:
            break
    return all_ids

def chunked(seq, size):
    for i in range(0, len(seq), size):
        yield seq[i : i + size]

def delete_variant_ids(sb: SupabaseClient, ids: List[int]):
    """Delete given variant_ids from Supabase table."""
    if not ids:
        return
    schema = sb.schema(SUPABASE_SCHEMA) if SUPABASE_SCHEMA else sb
    for chunk in chunked(ids, 500):  # safe batch deletes
        schema.table(SUPABASE_TABLE).delete().in_(COL_VARIANT_ID, chunk).execute()

def main():
    sb = supabase_client()
    print("Fetching numeric variant_ids from Supabase...")
    variant_ids = fetch_all_variant_ids(sb, PAGE_SIZE)
    if not variant_ids:
        print("No variant_ids found. Exiting.")
        return

    print(f"Total variant_ids to verify: {len(variant_ids)}")

    headers = build_headers()
    http = httpx.Client(headers=headers, timeout=REQUEST_TIMEOUT)

    not_found_ids: List[int] = []

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["variant_id", "status"])
        writer.writeheader()

        for batch in chunked(variant_ids, BATCH_SIZE):
            gid_list = [to_gid(v) for v in batch]
            try:
                data = shopify_graphql(http, NODES_QUERY, {"ids": gid_list})
            except ShopifyError as e:
                raise

            for v_id, node in zip(batch, data["nodes"]):
                status = "FOUND" if (node is not None and node.get("__typename") == "ProductVariant") else "NOT_FOUND"
                writer.writerow({"variant_id": v_id, "status": status})
                if status == "NOT_FOUND":
                    not_found_ids.append(v_id)

            time.sleep(0.15)

    http.close()

    if not_found_ids:
        print(f"Deleting {len(not_found_ids)} NOT_FOUND variant_ids from Supabase...")
        delete_variant_ids(sb, not_found_ids)
        print("Deletion complete.")
        print("Check Varification File -- shopify_variant_check_report.csv")

    print(f"Done. CSV written: {OUTPUT_CSV}")

if __name__ == "__main__":
    main()
