import os
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

# === Confit Env ===
load_dotenv()

### Load source CSV
## List of Source CSV files. Downloaded from Jinwon's Google sheet. 2 Seat Covers and 1 Car Covers
# https://docs.google.com/spreadsheets/d/1iOGWkYE2SuBYJo0w2fPP4iaiC-EroUc9xphxlR9hrQ0/edit?gid=0#gid=0
# https://docs.google.com/spreadsheets/d/14xKkO_G7u9rAQqNolxAgXsUNE7KiMVh2hrK3Z7lCcDE/edit?gid=0#gid=0
source_file_name_list_str = os.getenv("SOURCE_FILE_NAME_LIST")
source_file_name_list = source_file_name_list_str.split(",") if source_file_name_list_str else []
print(source_file_name_list)
suffix_list_str = os.getenv("SUFFIX_LIST")
suffix_list = suffix_list_str.split(",") if suffix_list_str else []
print(suffix_list)

for idx, val in enumerate(source_file_name_list):
    source_file_name = val
    suffix = suffix_list[idx]
    source_folder = os.getenv("PREORDER_SOURCE_CSV")
    source_csv = f"{source_folder}{source_file_name}.csv"
    df_raw = pd.read_csv(source_csv, skiprows=2, header=None)

    # === LOAD HEADER (first row) ===
    with open(source_csv, "r", encoding="utf-8") as f:
        header_parts = f.readline().strip().split(",")

    # Parse pairs: column B onward
    offer_pairs = []
    for i in range(1, len(header_parts[1:]), 2):
        internal_name = header_parts[i].strip()
        shipping_text = header_parts[i + 1].strip() if (i + 1) < len(header_parts) else ""
        offer_pairs.append((internal_name, shipping_text))

    # === LOAD VARIANT DATA ===
    df_raw = pd.read_csv(source_csv, skiprows=2, header=None)

    # === MAP OFFER INDICES TO UUID-STYLE ID (index + 1) ===
    offer_index_to_id = {i: i + 1 for i in range(len(offer_pairs))}

    # === COLLECT VALID VARIANTS ===
    valid_offer_ids = set()
    variants_data = []

    for row in df_raw.itertuples(index=False):
        row_vals = list(row)
        for col in range(1, len(row_vals) - 1, 2):  # start from column B
            variant = row_vals[col]
            discount = row_vals[col + 1]
            offer_idx = (col - 1) // 2
            offer_id = offer_index_to_id.get(offer_idx)

            if pd.notna(variant) and str(variant).strip().upper() != "#N/A" and offer_id:
                try:
                    variant_str = str(variant)
                    if variant_str.endswith(".0"):
                        variant_str = variant_str[:-2]

                    variants_data.append({
                        "offer_id": offer_id,
                        "variant_id": variant_str,
                        "discount_amount": float(discount) if pd.notna(discount) else None
                    })
                    valid_offer_ids.add(offer_id)
                except Exception:
                    continue

    # SAVE preorder_variants.csv
    df_variants = pd.DataFrame(variants_data)
    target_folder = os.getenv("PREORDER_TARGET_CSV")
    df_variants.to_csv(f"{target_folder}preorder_variants_{suffix}.csv", index=False)

    # === FIRST DISCOUNT MAPPING ===
    first_discounts = (
        df_variants.groupby("offer_id")["discount_amount"].first().round(2).to_dict()
    )

    # === FORMATTER ===
    def format_mmdd(date_str):
        try:
            return datetime.strptime(date_str.strip(), "%m/%d/%Y").strftime("%m%d")
        except ValueError:
            return "0000"

    # === GENERATE OFFERS CSV ===
    offers_output = []
    for idx, (name, raw_date) in enumerate(offer_pairs):
        offer_id = idx + 1
        if offer_id not in valid_offer_ids:
            continue

        discount = first_discounts.get(offer_id)
        internal_name = name
        shipping_text_out = raw_date

        if discount is not None:
            discount_str = str(int(discount)) if discount == int(discount) else f"{discount:.1f}"

            if "Weeks" in name:
                # e.g., "16 Weeks" → Preorder-16wks-40
                try:
                    week_num = int(name.replace(" Weeks", "").strip())
                    internal_name = f"Preorder-{week_num}wks-{discount_str}"
                    shipping_text_out = f"{week_num * 7} days after checkout"
                except ValueError:
                    shipping_text_out = "0 days after checkout"
            else:
                # Uniform formatting for all other cases
                internal_name = f"Preorder-{name.replace('#', '')}-{format_mmdd(raw_date)}-{discount_str}"
                try:
                    dt = datetime.strptime(raw_date.strip(), "%m/%d/%Y")
                    shipping_text_out = dt.strftime("%d %b %Y")
                except ValueError:
                    shipping_text_out = raw_date

        offers_output.append({
            "id": offer_id,
            "internal_name": internal_name,
            "shipping_text": shipping_text_out,
            "discount_amount": discount
        })

    # SAVE preorder_offers.csv
    df_offers = pd.DataFrame(offers_output)
    df_offers.to_csv(f"{target_folder}preorder_offers_{suffix}.csv", index=False)

    print("~~ Done. Files saved:")
    print(f"- preorder_offers_{suffix}.csv")
    print(f"- preorder_variants_{suffix}.csv")