import os
import uuid
from supabase import create_client, Client
from dotenv import load_dotenv

# === Confit Env ===
load_dotenv()

# === Supabase credentials ===
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_ANON_KEY")
TARGET_OFFERS  = os.getenv("PREORDER_OFFER_TABLE")
SOURCE_OFFERS  = os.getenv("PREORDER_BATCH_OFFER_TABLE")
TARGET_VARIANTS = os.getenv("PREORDER_VARIANT_TABLE")
SOURCE_VARIANTS = os.getenv("PREORDER_BATCH_VARIANT_TABLE")

def main():
    # Create Supabase client
    sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

    # Step 1. Fetch all offers from source and target
    source_offers = sb.table(SOURCE_OFFERS).select("id, internal_name").execute().data
    target_offers = sb.table(TARGET_OFFERS).select("id, internal_name").execute().data

    # Map internal_name → id for quick lookup
    target_map = {row["internal_name"]: row["id"] for row in target_offers}

    inserts = []

    # Step 2. Loop through each source offer
    for s_offer in source_offers:
        internal_name = s_offer["internal_name"]
        source_offer_id = s_offer["id"]

        if internal_name not in target_map:
            print(f"️## Skipping {internal_name}, no matching target offer.")
            continue

        target_offer_id = target_map[internal_name]

        # Step 3. Get variant_ids from source_variants
        source_variants = sb.table(SOURCE_VARIANTS).select("variant_id").eq("offer_id", source_offer_id).execute().data

        for v in source_variants:
            inserts.append({
                "id": str(uuid.uuid4()),        # gen_random_uuid()
                "offer_id": target_offer_id,    # link to target_offers
                "variant_id": v["variant_id"]   # from source_variants
            })

        # Before inserting, Delete all offers from target variants
        sb.table(TARGET_VARIANTS).delete().eq("offer_id", target_offer_id).execute()

    # Step 4. Insert into target_variants in batches
    BATCH_SIZE = 1000
    for i in range(0, len(inserts), BATCH_SIZE):
        chunk = inserts[i:i+BATCH_SIZE]
        sb.table(TARGET_VARIANTS).insert(chunk).execute()
        print(f"~~ Inserted {len(chunk)} rows")

    print("🎉 Done syncing variants.")

if __name__ == "__main__":
    main()
