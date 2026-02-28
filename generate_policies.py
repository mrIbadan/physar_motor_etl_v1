import os
import uuid
import random
from datetime import date
from dateutil.relativedelta import relativedelta
from supabase import create_client, Client

SUPABASE_URL = "https://jxonjddldsakvxqklaqd.supabase.co"
SUPABASE_KEY = "sb_secret_rZT7TG1WXbuazTIM9T53Rg_dtKkfI3s"
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def get_next_policy_start() -> int:
    resp = supabase.table("policies").select("policy_id").order("policy_id", desc=True).limit(1).execute()
    rows = getattr(resp, "data", []) or []
    if not rows: return 1
    try:
        return int(rows[0]["policy_id"].split("_")[1]) + 1
    except:
        return 1

def fetch_unconverted_quotes():
    """
    Checks the DB to see which quotes already have policies.
    """
    # 1. Get IDs of quotes that are already policies
    p_resp = supabase.table("policies").select("quote_id").execute()
    already_converted = set(row["quote_id"] for row in (getattr(p_resp, "data", []) or []))

    # 2. Get all quotes (You might want to add .limit(1000) here later for speed)
    q_resp = supabase.table("quotes").select("*").execute()
    all_quotes = getattr(q_resp, "data", []) or []

    # 3. Filter
    return [q for q in all_quotes if q["quote_id"] not in already_converted]

def build_policy_rows(conversion_rate: float = 0.10):
    unconverted = fetch_unconverted_quotes()
    if not unconverted:
        print("Everything is already converted.")
        return []

    # IMPROVED LOGIC: Ensure 10% probability even for 1 quote
    policies_to_make = []
    for quote in unconverted:
        if random.random() <= conversion_rate:
            policies_to_make.append(quote)
    
    if not policies_to_make:
        return []

    start_index = get_next_policy_start()
    final_policies = []

    for i, quote in enumerate(policies_to_make):
        start_date = date.fromisoformat(quote["start_date"])
        gwp = float(quote["quoted_total_premium"])
        
        final_policies.append({
            "policy_id": f"p_{(start_index + i):07d}",
            "quote_id": quote["quote_id"],
            "customer_uuid": quote["customer_uuid"],
            "policy_start_date": start_date.isoformat(),
            "policy_end_date": (start_date + relativedelta(years=1)).isoformat(),
            "cover_type": quote.get("cover_type"),
            "payment_frequency": quote.get("payment_frequency"),
            "car_make": quote.get("car_make"),
            "car_model": quote.get("car_model"),
            "gross_written_premium": gwp,
            "commission_amount": round(gwp * 0.15, 2),
            "ipt_amount": round(gwp * 0.12, 2),
            "total_payable": round(gwp * 1.12, 2),
            "status": "Active",
        })
    return final_policies

def main():
    policies = build_policy_rows(conversion_rate=0.10)
    if not policies:
        print("No conversions this run.")
        return

    try:
        supabase.table("policies").insert(policies).execute()
        print(f"✅ Successfully created {len(policies)} policies.")
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()
