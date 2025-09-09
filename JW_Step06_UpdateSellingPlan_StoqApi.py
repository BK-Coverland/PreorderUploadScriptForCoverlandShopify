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
BATCH_TABLE_NAME = os.getenv("PREORDER_BATCH_OFFER_TABLE")
HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Accept-Profile": "size_chart",
}

# throttle to be nice to Stoq API
RATE_LIMIT_SECONDS = 0.5

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
            # "delivery_at": shipping_text,    # as requested (note: not a date)
            "pricing_amount": int(discount_amount),
            "shipping_text": shipping_text,
        }
    }
    return payload

def update_stoq_offer(payload: dict, plan_id) -> dict:
    url = f"https://app.stoqapp.com/api/v1/external/preorders/{plan_id}"
    headers = {
        "X-Auth-Token": STOQ_API_ACCESS_KEY,
        "Content-Type": "application/json",
    }
    resp = requests.put(url, headers=headers, data=json.dumps(payload), timeout=60)
    resp.raise_for_status()
    try:
        return resp.json()
    except ValueError:
        # Not JSON (unexpected) – return raw text
        return {"_raw": resp.text}


# ======== Main ========
def main():
    # 1) Fetch rows
    ### Get Data from Batch Table
    res = (
        supabase
        .table(TABLE_NAME)
        .select("*")
        .eq("status", "update")
        .execute()
    )
    rows = res.data or []

    print(f"Found {len(rows)} rows in '{TABLE_NAME}'.")

    success = 0
    failed = 0
    failures = []

    # 2) Loop & create offers
    for idx, row in enumerate(rows, start=1):
        try:
            print(idx, row)
            res = supabase.table(BATCH_TABLE_NAME).select("internal_name, shipping_text, discount_amount").eq("internal_name", row["internal_name"]).execute()
            for scr_row in res.data:
                supabase.table(TABLE_NAME).update({
                    "shipping_text": scr_row["shipping_text"],
                    "discount_amount": scr_row["discount_amount"]
                }).eq("internal_name", scr_row["internal_name"]).execute()

                ## Update Information on Stoq App
                payload = build_payload_from_row(scr_row)
                stoq_offer_id = str(row["stoq_offer_id"])
                data = update_stoq_offer(payload, stoq_offer_id)

                success += 1
                print(f"[{idx}/{len(rows)}] ~~ Created offer for internal_name='{row.get('internal_name')}', stoq_offer_id={stoq_offer_id}")
                print(json.dumps(data, indent=2))
        except Exception as e:
            print(e)
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
