import os
import pandas as pd
from supabase import create_client, Client
from datetime import datetime
from dotenv import load_dotenv
import json

# Load environment variables
load_dotenv("config/supabase.env")

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
    raise ValueError("Missing SUPABASE_URL or SUPABASE_SERVICE_KEY in environment variables.")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

print("🔁 Fetching data from Supabase...")
dn = pd.DataFrame(supabase.table("dn").select("*").execute().data)
sales = pd.DataFrame(supabase.table("sales").select("*").execute().data)
groups = pd.DataFrame(supabase.table("dn_groups").select("*").execute().data)

if dn.empty or sales.empty or groups.empty:
    raise Exception("One or more required tables are empty.")

sales["date"] = pd.to_datetime(sales["date"])
sales["month"] = sales["date"].dt.to_period("M")

# Merge dn and sales
merged = sales.merge(dn, left_on="dn_id", right_on="id", suffixes=("_sale", "_dn"))
merged["keywords"] = merged["keywords"].apply(lambda x: x if isinstance(x, list) else [])

def matches_group(row, filters):
    if filters.get("tld") and row["tld"] != filters["tld"]:
        return False
    if filters.get("word_count") and row["word_count"] != filters["word_count"]:
        return False
    if filters.get("starts_with"):
        return any(kw.lower().startswith(filters["starts_with"].lower()) for kw in row["keywords"])
    if filters.get("ends_with"):
        return any(kw.lower().endswith(filters["ends_with"].lower()) for kw in row["keywords"])
    return True

updates = []
now = datetime.utcnow().isoformat()

print("🔍 Calculating trends for each group...")
for _, group in groups.iterrows():
    try:
        filters = json.loads(group["filters"])
    except:
        continue
    group_df = merged[merged.apply(lambda row: matches_group(row, filters), axis=1)]
    if group_df.empty:
        continue

    grouped = group_df.groupby("month")["price_adjusted"].agg(["count", "mean"]).reset_index()
    grouped["growth_pct"] = grouped["mean"].pct_change() * 100
    for _, row in grouped.iterrows():
        updates.append({
            "keyword_group": f'2word-com-{filters.get("starts_with", filters.get("ends_with", "unknown"))}' if filters.get("starts_with") else f'2word-com-ends-with-{filters.get("ends_with", "unknown")}',
            "volume": int(row["count"]),
            "avg_price": float(row["mean"]),
            "growth_pct": float(row["growth_pct"]) if pd.notnull(row["growth_pct"]) else None,
            "time_range": str(row["month"])
        })

print(f"📤 Uploading {len(updates)} trend entries to Supabase...")
for trend in updates:
    supabase.table("trends").upsert(trend, on_conflict=["keyword_group", "time_range"]).execute()

print("✅ Trends updated successfully.")
