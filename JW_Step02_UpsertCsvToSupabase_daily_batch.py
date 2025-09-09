import os
import re
import uuid
import logging
import pandas as pd
from supabase import create_client
from typing import Tuple
from dotenv import load_dotenv

# === Confit Env ===
load_dotenv()

# === Minimal Error Logging ===
logging.basicConfig(level=logging.ERROR, format="%(levelname)s: %(message)s")

# Matches strings like: Preorder-<name>-0902-30
MMDD_TRAILER = re.compile(
    r'^(?P<prefix>Preorder-)(?P<name>.*?)(?P<trailer>-\d{4}-[^-]+)$'
)

# Helper Function
def parse_preorder(s: str) -> Tuple[str, str]:
    """
    Returns (name, date) from a string like:
      - 'Preorder-100-CA-Seat-0902-30'  -> ('100-CA-Seat', '0902')
      - 'Preorder-16wks-40'             -> ('16wks-40', '16wks')
      - 'Preorder-20wks-40'             -> ('20wks-40', '20wks')
    """
    s = s.strip()

    # Case 1: ends with -MMDD-<something>
    m = MMDD_TRAILER.match(s)
    if m:
        name = m.group('name').strip('-')
        # trailer looks like '-0902-30' -> date is the 1st element after splitting by '-'
        trailer = m.group('trailer')  # e.g., '-0902-30'
        date = trailer.split('-')[1]  # '0902'
        return name, date

    # Case 2: general fallback (e.g., 'Preorder-16wks-40')
    # Take everything after 'Preorder-' as name; date = second-last token
    if s.startswith("Preorder-"):
        rest = s[len("Preorder-"):]
        parts = rest.split('-')
        date = parts[-2] if len(parts) >= 2 else ''
        name = rest
        return name, date

    # If no 'Preorder-' prefix, be graceful
    parts = s.split('-')
    date = parts[-2] if len(parts) >= 2 else ''
    return s, date

# === Supabase Credentials ===
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_ANON_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# === Settings ===
selling_plan_table_name = os.getenv("PREORDER_OFFER_TABLE")
offer_table_name = os.getenv("PREORDER_BATCH_OFFER_TABLE")
variant_table_name = os.getenv("PREORDER_BATCH_VARIANT_TABLE")
file_path = os.getenv("PREORDER_TARGET_CSV")
file_pairs = [
    ("preorder_offers_Car PREORDER.csv", "preorder_variants_Car PREORDER.csv"),
    ("preorder_offers_Front PREORDER.csv", "preorder_variants_Front PREORDER.csv"),
    ("preorder_offers_Full PREORDER.csv", "preorder_variants_Full PREORDER.csv"),
]

# === Load and Merge Files ===
all_offers = []
all_variants = []
internal_name_to_uuid = {}

for offer_file, variant_file in file_pairs:
    df_offers = pd.read_csv(os.path.join(file_path, offer_file))
    df_variants = pd.read_csv(os.path.join(file_path, variant_file))
    local_id_map = {}

    for _, row in df_offers.iterrows():
        original_id = row["id"]
        name = row["internal_name"].strip()
        container_name, container_arrival_mmdd = parse_preorder(name)
        if name not in internal_name_to_uuid:
            uid = str(uuid.uuid4())
            internal_name_to_uuid[name] = uid
            local_id_map[original_id] = uid
            all_offers.append({
                "id": uid,
                "internal_name": name,
                "shipping_text": row["shipping_text"],
                "discount_amount": int(row["discount_amount"]),
                "stoq_offer_id": None,
                "container_name": container_name,
                "container_arrival_mmdd": container_arrival_mmdd
            })
        else:
            local_id_map[original_id] = internal_name_to_uuid[name]

    for _, row in df_variants.iterrows():
        offer_id = local_id_map.get(row["offer_id"])
        if offer_id:
            variant_id = str(row["variant_id"]).strip()
            if variant_id.endswith(".0"):
                variant_id = variant_id[:-2]
            all_variants.append({
                "id": str(uuid.uuid4()),
                "offer_id": offer_id,
                "variant_id": variant_id
            })

# === Supabase Upload ===
# Prepare Tables before CSV upload
try:
    # 1. Cleare status value.
    resp = (
        supabase
        .table(selling_plan_table_name)
        .update({"status": None})
        .neq("status", None)   # equivalent to WHERE status IS NOT NULL
        .execute()
    )
    print("Offer Table Status Set Null")

    # 2. Truncate daily_batch Tables
    resp = supabase.rpc("truncate_stoq_selling_plan_offers_daily_batch").execute()
    print("Daily Batch Tables Truncated")

except Exception as e:
    logging.error(f"!! Preparing Error: {e}")

# Upsert Offers
try:
    supabase.table(offer_table_name).upsert(all_offers, on_conflict=["internal_name"]).execute()
except Exception as e:
    logging.error(f"!! Offer upsert failed: {e}")

# Replace Variants Per Offer
offer_ids = set(v["offer_id"] for v in all_variants)

for oid in offer_ids:
    try:
        supabase.table(variant_table_name).delete().eq("offer_id", oid).execute()
        batch = [v for v in all_variants if v["offer_id"] == oid]
        if batch:
            supabase.table(variant_table_name).insert(batch).execute()
    except Exception as e:
        logging.error(f"!! Variant refresh failed for offer {oid}: {e}")
