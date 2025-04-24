import os
import pandas as pd
from supabase import create_client, Client
from datetime import datetime, timedelta
import json

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

print("Connected to Supabase")

# Load dn_groups table
dn_groups = supabase.table("dn_groups").select("id, name, filters").execute().data

# Load dn table
dn_data = supabase.table("dn").select("id, dn_name, tld, word_count, dn_type").execute().data

# For each group, filter matching domains and calculate metrics
trend_rows = []

def matches_filter(domain, filters):
    name = domain["dn_name"].lower()
    word_count = domain.get("word_count", 0)
    tld = domain.get("tld", "")

    if filters.get("tld") and filters["tld"].lower() != tld.lower():
        return False
    if filters.get("word_count") and filters["word_count"] != word_count:
        return False
    if filters.get("starts_with") and not name.startswith(filters["starts_with"].lower()):
        return False
    if filters.get("ends_with") and not name.endswith(filters["ends_with"].lower()):
        return False

    return True

for group in dn_groups:
    group_name = group["name"]
    group_id = group["id"]
    filters = group.get("filters", {})

    matching_ids = [d["id"] for d in dn_data if matches_filter(d, filters)]
    if not matching_ids:
        continue

    # Fetch sales
    sales_resp = supabase.table("sales").select("price_adjusted, date").in_("dn_id", matching_ids).execute()
    sales = pd.DataFrame(sales_resp.data)
    if sales.empty:
        continue

    # Convert date
    sales["date"] = pd.to_datetime(sales["date"])
    sales.sort_values("date", inplace=True)

    # Overall metrics
    total_volume = len(sales)
    total_avg_price = sales["price_adjusted"].mean()
    total_median_price = sales["price_adjusted"].median()
    total_top_price = sales["price_adjusted"].max()

    # Rolling 6-month window based on current date
    end_date = pd.Timestamp.today()
    start_6mo = end_date - pd.DateOffset(months=6)
    start_prev_6mo = start_6mo - pd.DateOffset(months=6)

    window_now = sales[sales["date"] >= start_6mo]
    window_prev = sales[(sales["date"] >= start_prev_6mo) & (sales["date"] < start_6mo)]

    if not window_now.empty:
        avg_price_rolling_6mo = window_now["price_adjusted"].mean()
        median_price_6mo = window_now["price_adjusted"].median()
        top_price_6mo = window_now["price_adjusted"].max()
        total_volume_rolling_6mo = len(window_now)
        avg_prev = window_prev["price_adjusted"].mean() if not window_prev.empty else 0
        if avg_prev:
            growth_pct_6mo = ((avg_price_rolling_6mo - avg_prev) / avg_prev) * 100
        else:
            growth_pct_6mo = None
    else:
        avg_price_rolling_6mo = None
        median_price_6mo = None
        top_price_6mo = None
        total_volume_rolling_6mo = 0
        growth_pct_6mo = None

    # Prepare row
    trend_rows.append({
        "group_name": group_name,
        "total_volume": total_volume,
        "total_avg_price": round(total_avg_price, 2),
        "total_median_price": round(total_median_price, 2),
        "total_top_price": round(total_top_price, 2),
        "avg_price_rolling_6mo": round(avg_price_rolling_6mo, 2) if avg_price_rolling_6mo else None,
        "median_price_6mo": round(median_price_6mo, 2) if median_price_6mo else None,
        "top_price_6mo": round(top_price_6mo, 2) if top_price_6mo else None,
        "total_volume_rolling_6mo": total_volume_rolling_6mo,
        "growth_pct_6mo": round(growth_pct_6mo, 2) if growth_pct_6mo else None,
        "time_range": end_date.strftime("%Y-%m")
    })

# Replace data in trends table
if trend_rows:
    supabase.table("trends").delete().gt("created_at", "1970-01-01").execute()
    supabase.table("trends").insert(trend_rows).execute()

print("✅ Trends updated with dynamic filter matching!")
