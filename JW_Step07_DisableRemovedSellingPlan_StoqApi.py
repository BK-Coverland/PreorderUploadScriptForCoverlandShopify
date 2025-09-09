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

# ======== Clients ========
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# ======== Helpers ========
def update_stoq_offer(Stoq_selling_plan_id) -> dict:
    resp = requests.put(
        f"https://app.stoqapp.com/api/v1/external/preorders/{Stoq_selling_plan_id}",
        headers={"X-Auth-Token": STOQ_API_ACCESS_KEY, "Content-Type": "application/json"},
        data=json.dumps({
            "selling_plan": {
                "enabled": False,
            }
        })
    )
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
    res = (
        supabase
        .table(TABLE_NAME)
        .select("*")
        .eq("status", "delete")
        .execute()
    )
    rows = res.data or []

    print(rows)
    print(f"Found {len(rows)} rows in '{TABLE_NAME}'.")

    success = 0
    failed = 0
    failures = []

    # 2) Loop & update offers as Disabled
    for idx, row in enumerate(rows, start=1):
        try:
            stoq_offer_id = row.get("stoq_offer_id")
            data = update_stoq_offer(stoq_offer_id)

            success += 1
            print(f"[{idx}/{len(rows)}] ~~ Disabled offer for internal_name='{row.get('internal_name')}', stoq_offer_id={stoq_offer_id}")
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
