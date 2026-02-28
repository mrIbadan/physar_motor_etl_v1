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
    try: return int(rows[0]["policy_id"].split("_")[1]) + 1
    except: return 1

def fetch_unprocessed_quotes():
    # Only pull quotes that haven't been decided yet
    resp = supabase.table("quotes").select("*").eq("status", "Quoted").execute()
    return getattr(resp, "data", []) or []

def build_policy_rows(conversion_rate: float = 0.10):
    unprocessed = fetch_unprocessed_quotes()
    if not unprocessed: return []

    policies_to_insert = []
    start_index = get_next_policy_start()

    for quote in unprocessed:
        # --- PHASE 1: THE RATING ENGINE (Rejections) ---
        if quote.get("credit_score", 0) < 450 or quote.get("number_of_ccjs", 0) > 1:
            supabase.table("quotes").update({"status": "Rejected"}).eq("quote_id", quote["quote_id"]).execute()
            continue

        # --- PHASE 2: CONVERSION CHANCE ---
        if random.random() <= conversion_rate:
            # Mark Quote as Converted
            supabase.table("quotes").update({"status": "Converted"}).eq("quote_id", quote["quote_id"]).execute()
            
            start_date = date.fromisoformat(quote["start_date"])
            gwp = float(quote["quoted_total_premium"])
            
            policies_to_insert.append({
                "policy_id": f"p_{(start_index + len(policies_to_insert)):07d}",
                "quote_id": quote["quote_id"],
                "customer_uuid": quote["customer_uuid"],
                "policy_start_date": start_date.isoformat(),
                "policy_end_date": (start_date + relativedelta(years=1)).isoformat(),
                "gross_written_premium": gwp,
                "total_payable": round(gwp * 1.12, 2),
                "status": "Active",
            })
        else:
            # User Abandoned (did not follow up)
            supabase.table("quotes").update({"status": "Abandoned"}).eq("quote_id", quote["quote_id"]).execute()

    return policies_to_insert

def main():
    policies = build_policy_rows(conversion_rate=0.10)
    if policies:
        supabase.table("policies").insert(policies).execute()
        print(f"✅ Created {len(policies)} policies.")
    else:
        print("No quotes converted this run.")

if __name__ == "__main__":
    main()
