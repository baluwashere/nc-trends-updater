import os
import pandas as pd
from supabase import create_client, Client
from datetime import datetime, timedelta

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

print("Connected to Supabase")

# Load dn_groups table
dn_groups = supabase.table("dn_groups").select("id, keyword_group, criteria").execute().data

# For each group, fetch matching sales and calculate metrics
trend_rows = []

for group in dn_groups:
    keyword_group = group["keyword_group"]
    group_id = group["id"]

    # Fetch domains in group
    dn_resp = supabase.table("dn").select("id").eq("group_id", group_id).execute()
    domain_ids = [d["id"] for d in dn_resp.data]
    if not domain_ids:
        continue

    # Fetch sales
    sales_resp = supabase.table("sales").select("price_adjusted, date").in_("dn_id", domain_ids).execute()
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

    # Rolling 6-month window
    end_date = sales["date"].max()
    start_6mo = end_date - pd.DateOffset(months=6)
    window_now = sales[sales["date"] >= start_6mo]
    start_prev_6mo = start_6mo - pd.DateOffset(months=6)
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
        "keyword_group": keyword_group,
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
