import os
import requests
import json
import time
from datetime import datetime, timezone
# from dateutil.relativedelta import relativedelta
from supabase import create_client, Client
from dotenv import load_dotenv

# === Confit Env ===
load_dotenv()

# --- Supabase credentials ---
STOQ_API_ACCESS_KEY = os.getenv("STOQ_API_ACCESS_KEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_ANON_KEY")
TABLE_NAME = os.getenv("PREORDER_OFFER_TABLE")
HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Accept-Profile": "size_chart",
}

# throttle to be nice to Stoq API
RATE_LIMIT_SECONDS = 0.5

# Default fields you wanted to “keep as-is”
DEFAULT_SELLING_PLAN_FIELDS = {
    "enabled": True,  # turn True later after variants uploaded
    "delivery_type": "asap",
    "pricing_type": "fixed_amount",
    "preorder_button_text": "Preorder",
    "preorder_button_description": "Note: This is a preorder. Items will ship based on the estimated delivery date.",
    "discount_text": "Save {{ discount }}",
    "preorder_tags": "STOQ-preorder",
    "product_variants_source": "custom",
    "markets_enabled": True,
    "show_fulfillment_timeline": True,
    "shopify_market_ids": [46048411815, 46048379047],
}

# ======== Clients ========
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# ======== Helpers ========
def build_payload_from_row(row: dict) -> dict:
    """
    Map row columns to Stoq payload fields and keep other defaults unchanged.
    Required mappings per your instructions:
      - name            <- row.internal_name
      - internal_name   <- row.internal_name
      - delivery_at     <- row.shipping_text
      - pricing_amount  <- row.discount_amount
      - shipping_text   <- row.shipping_text
    """
    internal_name = row.get("internal_name")
    shipping_text = row.get("shipping_text")
    discount_amount = row.get("discount_amount")

    if internal_name is None or shipping_text is None or discount_amount is None:
        missing = [k for k, v in {
            "internal_name": internal_name,
            "shipping_text": shipping_text,
            "discount_amount": discount_amount,
        }.items() if v is None]
        raise ValueError(f"Row is missing required fields: {missing}. Row: {row}")

    payload = {
        "selling_plan": {
            # mapped fields
            "name": internal_name,
            "internal_name": internal_name,
            "delivery_at": shipping_text,    # as requested (note: not a date)
            "pricing_amount": int(discount_amount),
            "shipping_text": shipping_text,
            # keep other fields as-is
            **DEFAULT_SELLING_PLAN_FIELDS,
        }
    }
    return payload

def create_stoq_offer(payload: dict) -> dict:
    url = "https://app.stoqapp.com/api/v1/external/preorders"
    headers = {
        "X-Auth-Token": STOQ_API_ACCESS_KEY,
        "Content-Type": "application/json",
    }
    resp = requests.post(url, headers=headers, data=json.dumps(payload), timeout=60)
    # Raise on non-2xx so we can catch & log nicely
    resp.raise_for_status()
    try:
        return resp.json()
    except ValueError:
        # Not JSON (unexpected) – return raw text
        return {"_raw": resp.text}

# ======== Main ========
def main():
    # 1) Fetch rows
    ###res = supabase.table(TABLE_NAME).select("*").execute()
    res = (
        supabase
        .table(TABLE_NAME)
        .select("*")
        .eq("status", "insert")
        .execute()
    )
    rows = res.data or []

    print(rows)
    print(f"Found {len(rows)} rows in '{TABLE_NAME}'.")

    success = 0
    failed = 0
    failures = []

    # 2) Loop & create offers
    for idx, row in enumerate(rows, start=1):
        try:
            payload = build_payload_from_row(row)
            data = create_stoq_offer(payload)

            # Extract top-level ID from Stoq API response
            stoq_offer_id = data.get("id")
            if not stoq_offer_id:
                raise ValueError(f"No 'id' found in response: {data}")

            # Update Supabase with the created Stoq offer ID
            supabase.table(TABLE_NAME).update({"stoq_offer_id": stoq_offer_id}).eq("id", row["id"]).execute()

            success += 1
            print(f"[{idx}/{len(rows)}] ~~ Created offer for internal_name='{row.get('internal_name')}', stoq_offer_id={stoq_offer_id}")
            print(json.dumps(data, indent=2))
        except requests.HTTPError as e:
            failed += 1
            body = e.response.text if e.response is not None else str(e)
            failures.append((row.get("internal_name"), f"HTTPError {e.response.status_code if e.response else ''}", body))
            print(f"[{idx}/{len(rows)}] @@ HTTP error for internal_name='{row.get('internal_name')}': {body}")
        except Exception as e:
            failed += 1
            failures.append((row.get("internal_name"), "Exception", str(e)))
            print(f"[{idx}/{len(rows)}] @@ Error for internal_name='{row.get('internal_name')}': {e}")
        finally:
            if RATE_LIMIT_SECONDS:
                time.sleep(RATE_LIMIT_SECONDS)

    # 3) Summary
    print("\n=== Summary ===")
    print(f"Success: {success}")
    print(f"Failed : {failed}")
    if failures:
        print("\nFailures detail:")
        for name, kind, info in failures:
            print(f"- {name}: {kind} -> {info[:400]}")  # truncate long messages

if __name__ == "__main__":
    main()
