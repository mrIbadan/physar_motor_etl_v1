import os
import random
from datetime import datetime, date, timedelta, timezone
from dateutil.relativedelta import relativedelta
from supabase import create_client, Client

# CONFIG
URL = "https://jxonjddldsakvxqklaqd.supabase.co"
KEY = "sb_secret_rZT7TG1WXbuazTIM9T53Rg_dtKkfI3s"
supabase: Client = create_client(URL, KEY)

def get_next_policy_start():
    try:
        resp = supabase.table("policies").select("policy_id").order("policy_id", desc=True).limit(1).execute()
        rows = getattr(resp, "data", []) or []
        if not rows: return 1
        return int(rows[0]["policy_id"].split("_")[1]) + 1
    except:
        return 1

def main():
    # 1. Fetch 'Quoted' leads
    resp = supabase.table("quotes").select("*").eq("status", "Quoted").execute()
    unprocessed = getattr(resp, "data", []) or []
    
    if not unprocessed:
        print("No new quotes to process.")
        return

    policies_to_insert = []
    p_idx = get_next_policy_start()
    
    # Use timezone-aware 'now' to match Supabase
    now = datetime.now(timezone.utc)

    for quote in unprocessed:
        # A. EXPIRY CHECK (30 Minute Window)
        # We ensure created_at is parsed as a UTC-aware object
        created_at_str = quote["created_at"].replace('Z', '+00:00')
        created_at = datetime.fromisoformat(created_at_str)
        
        # If created_at is naive, we force it to UTC (Supabase usually sends offset)
        if created_at.tzinfo is None:
            created_at = created_at.replace(tzinfo=timezone.utc)

        if now > created_at + timedelta(minutes=30):
            supabase.table("quotes").update({
                "status": "Expired", 
                "rejection_reason": "30m timeout"
            }).eq("quote_id", quote["quote_id"]).execute()
            continue

        # B. RATING ENGINE
        if quote.get("credit_score", 0) < 450:
            supabase.table("quotes").update({
                "status": "Declined", 
                "rejection_reason": "Low Credit Score"
            }).eq("quote_id", quote["quote_id"]).execute()
            continue

        # C. CONVERSION (10%)
        if random.random() <= 0.10:
            supabase.table("quotes").update({"status": "Converted"}).eq("quote_id", quote["quote_id"]).execute()
            
            start_dt = date.fromisoformat(quote["start_date"])
            gwp = float(quote.get("quoted_total_premium", 0))
            
            policies_to_insert.append({
                "policy_id": f"p_{p_idx:07d}",
                "quote_id": quote["quote_id"],
                "customer_uuid": quote["customer_uuid"],
                "policy_start_date": start_dt.isoformat(),
                "policy_end_date": (start_dt + relativedelta(years=1)).isoformat(),
                "gross_written_premium": gwp,
                "total_payable": round(gwp * 1.12, 2),
                "status": "Active"
            })
            p_idx += 1
        else:
            # D. ABANDONED (The 90% who didn't click buy)
            supabase.table("quotes").update({"status": "Abandoned"}).eq("quote_id", quote["quote_id"]).execute()

    if policies_to_insert:
        supabase.table("policies").insert(policies_to_insert).execute()
        print(f"✅ Created {len(policies_to_insert)} policies.")
    else:
        print("Processed runs, no new policies created.")

if __name__ == "__main__":
    main()
