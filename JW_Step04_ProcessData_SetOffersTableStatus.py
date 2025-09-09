import os
from typing import Iterable, List, Dict, Tuple
import logging
import pandas as pd
from supabase import create_client, Client
from dotenv import load_dotenv


# === Confit Env ===
load_dotenv()

# === Minimal Error Logging ===
logging.basicConfig(level=logging.ERROR, format="%(levelname)s: %(message)s")

# ================= Config =================
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_ANON_KEY")

# Tables
TARGET_OFFERS  = os.getenv("PREORDER_OFFER_TABLE")
SOURCE_OFFERS  = os.getenv("PREORDER_BATCH_OFFER_TABLE")
TARGET_VARIANTS = os.getenv("PREORDER_VARIANT_TABLE")
SOURCE_VARIANTS = os.getenv("PREORDER_BATCH_VARIANT_TABLE")

# Keys and columns
KEY_COLS     = ("container_name", "discount_amount")
UPDATE_COLS  = ("internal_name", "container_arrival_mmdd")
TARGET_PK    = "id"

# Variants foreign key column (adjust to your schema if different)
VAR_FK_COL = "offer_id"

# Batch sizes (tune to your data sizes)
UPSERT_BATCH_SIZE = 1000
INSERT_BATCH_SIZE = 1000
UPDATE_STATUS_BATCH_SIZE = 5000

# ================= Helpers =================
def supabase_client() -> Client:
    return create_client(SUPABASE_URL, SUPABASE_KEY)

def chunked(iterable: Iterable, size: int) -> Iterable[list]:
    chunk = []
    for item in iterable:
        chunk.append(item)
        if len(chunk) >= size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk

def fetch_all(sb: Client, table: str, select: str = "*") -> List[Dict]:
    """
    Fetch all rows from a table without pagination.
    If tables are huge, replace with a .range() pager.
    """
    res = sb.table(table).select(select).execute()
    return res.data or []

def df_key_series(df: pd.DataFrame, key_cols=KEY_COLS) -> pd.Series:
    return df[list(key_cols)].astype(str).agg("§§".join, axis=1)

def compute_diffs(target_df: pd.DataFrame, source_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Returns (to_delete, to_add, to_update):
    - to_delete: rows in target but NOT in source   (by KEY_COLS)
    - to_add:    rows in source but NOT in target   (by KEY_COLS)
    - to_update: rows in both where UPDATE_COLS differ (source -> target)
                 Columns: [TARGET_PK, *KEY_COLS, *UPDATE_COLS] where values are the source values to apply
    """
    tgt = target_df.copy()
    src = source_df.copy()
    # normalize to strings for comparison safety
    tgt = tgt.astype(str).fillna("")
    src = src.astype(str).fillna("")

    tgt["_key"] = df_key_series(tgt)
    src["_key"] = df_key_series(src)

    tgt_keys = set(tgt["_key"])
    src_keys = set(src["_key"])

    # to_delete
    to_delete = tgt[~tgt["_key"].isin(src_keys)].drop(columns=["_key"]).reset_index(drop=True)

    # to_add
    to_add = src[~src["_key"].isin(tgt_keys)].drop(columns=["_key"]).reset_index(drop=True)

    # to_update: inner merge + compare update columns
    merged = tgt.merge(
        src[[*KEY_COLS, *UPDATE_COLS]],
        on=list(KEY_COLS),
        how="inner",
        suffixes=("_tgt", "_src"),
    )
    if len(merged) == 0:
        to_update = pd.DataFrame(columns=[TARGET_PK, *KEY_COLS, *UPDATE_COLS])
        return to_delete, to_add, to_update

    # any UPDATE_COL differs?
    diff_mask = False
    for c in UPDATE_COLS:
        ct, cs = f"{c}_tgt", f"{c}_src"
        m = merged[ct] != merged[cs]
        diff_mask = m if isinstance(diff_mask, bool) else (diff_mask | m)

    changed = merged[diff_mask]
    if len(changed) == 0:
        to_update = pd.DataFrame(columns=[TARGET_PK, *KEY_COLS, *UPDATE_COLS])
        return to_delete, to_add, to_update

    # ensure TARGET_PK present (could be suffixed)
    if TARGET_PK not in changed.columns:
        pk_candidates = [c for c in changed.columns if c.endswith("_tgt") and c.split("_tgt")[0] == TARGET_PK]
        if pk_candidates:
            changed[TARGET_PK] = changed[pk_candidates[0]]

    # shape to: id + keys + new values (from source)
    out_cols = [TARGET_PK, *KEY_COLS, *[f"{c}_src" for c in UPDATE_COLS]]
    to_update = (
        changed[out_cols]
        .rename(columns={f"{c}_src": c for c in UPDATE_COLS})
        .reset_index(drop=True)
    )
    return to_delete, to_add, to_update


# ================= DB Writers =================
def apply_updates_null(sb: Client) -> None:
    """
    Bulk UPDATE (no inserts) via RPC. Sets status='update'.
    """
    resp = (
        sb
        .table(TARGET_OFFERS)
        .update({"status": "update"})
        .is_("status", None)  # WHERE status IS NULL
        .execute()
    )
    print(f"Applied updates for unchanged offers")


def apply_updates(sb: Client, to_update: pd.DataFrame) -> None:
    """
    Bulk UPDATE (no inserts) via RPC. Sets status='update'.
    """
    if to_update.empty:
        print("No updates to apply.")
        return

    payload = [
        {
            "id": str(r["id"]),
            "internal_name": r["internal_name"],
            "container_arrival_mmdd": r["container_arrival_mmdd"],
        }
        for _, r in to_update.iterrows()
    ]

    resp = sb.rpc("stoq_offers_bulk_update", {"p_rows": payload}).execute()
    updated = resp.data or 0
    print(f"Applied updates: {updated} rows")


def apply_soft_deletes(sb: Client, to_delete: pd.DataFrame) -> None:
    """
    Mark rows as status='delete' in live offers. One mass update per ID batch.
    """
    if to_delete.empty:
        print("No deletes to mark.")
        return

    ids = to_delete[TARGET_PK].tolist()
    total = 0
    for batch in chunked(ids, UPDATE_STATUS_BATCH_SIZE):
        resp = (
            sb.table(TARGET_OFFERS)
              .update({"status": "delete"})
              .in_(TARGET_PK, batch)
              .execute()
        )
        total += len(batch)
    print(f"Marked deletes: {total} rows")

def insert_offers_and_variants(
    sb: Client,
    to_add_keys: pd.DataFrame,
    full_source_offers: pd.DataFrame,
) -> None:
    """
    Insert new offers from source into target with status='insert',
    then insert related variants from SOURCE_VARIANTS into TARGET_VARIANTS
    remapping the foreign key to the new target offer IDs.

    Strategy:
      1) Find the full source offer rows matching to_add keys.
      2) Insert into target (status='insert'), in batches.
      3) Re-fetch the just-inserted target offers by KEY_COLS to get their new IDs.
      4) Build mapping: (container_name, discout_amout) -> target_id
      5) Fetch variants from source whose parent is in the to_add set.
         - We join SOURCE_VARIANTS to SOURCE_OFFERS (in memory) via source offer id
           to bring in KEY_COLS, then map to target_id and insert.
    """
    if to_add_keys.empty:
        print("No inserts to apply.")
        return

    # Step 1: isolate full source offers for the to_add keys
    full_source_offers = full_source_offers.copy()
    full_source_offers["_key"] = df_key_series(full_source_offers)
    to_add_keys = to_add_keys.copy()
    to_add_keys["_key"] = df_key_series(to_add_keys)

    src_to_insert = full_source_offers[full_source_offers["_key"].isin(set(to_add_keys["_key"]))].drop(columns=["_key"])
    if src_to_insert.empty:
        print("No matching full source rows found for toAdd keys.")
        return

    # Add status='insert'
    src_to_insert = src_to_insert.copy()
    src_to_insert["status"] = "insert"

    # Step 2: insert offers in batches
    # Remove PK if present (let DB assign) — keep PK if yours is not serial (adjust as needed)
    # If your PK is serial/bigint, drop it to avoid collisions:
    if TARGET_PK in src_to_insert.columns:
        # Only drop if the PKs are from the SOURCE (not valid in target)
        src_to_insert = src_to_insert.drop(columns=[TARGET_PK])

    offer_rows = src_to_insert.to_dict(orient="records")
    inserted_total = 0
    for batch in chunked(offer_rows, INSERT_BATCH_SIZE):
        sb.table(TARGET_OFFERS).insert(batch).execute()
        inserted_total += len(batch)
    print(f"Inserted offers: {inserted_total} rows")

    # Step 3: re-fetch inserted target offers by keys to get new IDs
    refetch_cols = [TARGET_PK, *KEY_COLS]
    target_now = pd.DataFrame(fetch_all(sb, TARGET_OFFERS, select=",".join(refetch_cols)))
    if target_now.empty:
        print("Refetch of target offers returned no rows; cannot map IDs for variants.")
        return
    target_now["_key"] = df_key_series(target_now)

    key_to_target_id = dict(zip(target_now["_key"], target_now[TARGET_PK]))

    # Step 4: build set of keys just inserted
    just_added_keys = set(df_key_series(to_add_keys))
    # Map keys -> target_id (filter to just added)
    just_added_key_to_id = {k: key_to_target_id[k] for k in just_added_keys if k in key_to_target_id}

    # --- Variants insert ---
    # Step 5a: fetch minimal SOURCE_OFFERS to map source offer id -> key
    # We need source offer PK to tie SOURCE_VARIANTS rows to their parent offer
    src_offers_min = pd.DataFrame(fetch_all(sb, SOURCE_OFFERS, select=f"id,{','.join(KEY_COLS)}"))
    if src_offers_min.empty or VAR_FK_COL is None:
        print("Skipping variants insert (missing source offers or VAR_FK_COL not set).")
        return
    src_offers_min["_key"] = df_key_series(src_offers_min)

    # We only care about source offers that are in to_add
    src_offers_min = src_offers_min[src_offers_min["_key"].isin(just_added_keys)]
    if src_offers_min.empty:
        print("No source offers match the toAdd keys for variants.")
        return

    # Step 5b: fetch SOURCE_VARIANTS for those parent IDs
    parent_ids = src_offers_min["id"].tolist()
    src_vars_all: List[Dict] = []
    for batch in chunked(parent_ids, 1000):
        # PostgREST 'in' filter
        resp = sb.table(SOURCE_VARIANTS).select("*").in_(VAR_FK_COL, batch).execute()
        src_vars_all.extend(resp.data or [])

    if not src_vars_all:
        print("No source variants to insert.")
        return

    src_vars_df = pd.DataFrame(src_vars_all)
    # Join variants -> source offers to attach keys
    src_vars_join = src_vars_df.merge(
        src_offers_min[["id", "_key"]],
        left_on=VAR_FK_COL,
        right_on="id",
        how="left",
        suffixes=("", "_src_offer")
    )

    # Map to target offer id
    def map_target_id(row):
        k = row["_key"]
        return just_added_key_to_id.get(k, None)

    src_vars_join["__target_offer_id"] = src_vars_join.apply(map_target_id, axis=1)
    src_vars_join = src_vars_join[~src_vars_join["__target_offer_id"].isna()].copy()

    if src_vars_join.empty:
        print("No variants mapped to target offers (check keys / FK).")
        return

    # Prepare variant rows for insert:
    # - set FK column to new target id
    # - drop old PK if present (let DB assign)
    # - drop helper columns
    var_rows = src_vars_join.copy()
    var_rows[VAR_FK_COL] = var_rows["__target_offer_id"].astype(str)

    drop_cols = []
    if "id" in var_rows.columns:
        drop_cols.append("id")  # drop source variant PK
    drop_cols += ["_key", "__target_offer_id", "id_src_offer"]
    drop_cols = [c for c in drop_cols if c in var_rows.columns]
    var_rows = var_rows.drop(columns=drop_cols, errors="ignore")

    var_records = var_rows.to_dict(orient="records")
    inserted_vars_total = 0
    for batch in chunked(var_records, INSERT_BATCH_SIZE):
        sb.table(TARGET_VARIANTS).insert(batch).execute()
        inserted_vars_total += len(batch)

    print(f"Inserted variants: {inserted_vars_total} rows")


# ================= Orchestrator =================
def main():
    try:
        sb = supabase_client()

        # Pull full data (you can restrict columns for faster diff; here we fetch all for inserts)
        target_rows_all = fetch_all(sb, TARGET_OFFERS, select="*")
        source_rows_all = fetch_all(sb, SOURCE_OFFERS, select="*")

        if not source_rows_all and not target_rows_all:
            print("Both tables are empty; nothing to do.")
            return

        target_df_all = pd.DataFrame(target_rows_all)
        source_df_all = pd.DataFrame(source_rows_all)

        # Minimal frames for diffing (keys + update cols + PK)
        # Ensure missing columns exist (empty) to avoid KeyErrors
        for col in [*KEY_COLS, *UPDATE_COLS, TARGET_PK]:
            if col not in target_df_all.columns:
                target_df_all[col] = ""
        for col in [*KEY_COLS, *UPDATE_COLS]:
            if col not in source_df_all.columns:
                source_df_all[col] = ""

        target_min = target_df_all[[TARGET_PK, *KEY_COLS, *UPDATE_COLS]].copy()
        source_min = source_df_all[[*KEY_COLS, *UPDATE_COLS]].copy()

        # Compute diffs
        to_delete, to_add_keys, to_update = compute_diffs(target_min, source_min)

        print(f"Diff summary -> delete: {len(to_delete)}, add: {len(to_add_keys)}, update: {len(to_update)}")

        # 1) Updates (set fields and status='update')
        apply_updates(sb, to_update)

        # 2) Inserts (offers + variants; status='insert')
        insert_offers_and_variants(sb, to_add_keys, full_source_offers=source_df_all)

        # 3) Soft deletes (status='delete')
        apply_soft_deletes(sb, to_delete)

        # 3) Fill as update for unchanged offers (status='update')
        apply_updates_null(sb)

        print("~~ Sync completed.")
    except Exception as e:
        logging.error(f"!! Step4--Setup Offers Table Status failed: {e}")


if __name__ == "__main__":
    main()
