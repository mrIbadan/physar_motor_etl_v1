import os
import random
from datetime import date
from dateutil.relativedelta import relativedelta  # pip install python-dateutil

from supabase import create_client, Client

# ---------- 1. CONNECTION ----------
SUPABASE_URL = "https://jxonjddldsakvxqklaqd.supabase.co"
SUPABASE_KEY = "sb_secret_rZT7TG1WXbuazTIM9T53Rg_dtKkfI3s"

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


# ---------- 2. HELPERS ----------
def get_next_policy_start() -> int:
    """Get the next policy sequence number from policies.policy_id."""
    resp = (
        supabase.table("policies")
        .select("policy_id")
        .order("policy_id", desc=True)
        .limit(1)
        .execute()
    )
    rows = getattr(resp, "data", []) or []
    if not rows:
        return 1

    last_id = rows[0]["policy_id"]  # e.g. "p_0000123"
    try:
        return int(last_id.split("_")[1]) + 1
    except Exception:
        return 1


def fetch_unique_customers():
    """Fetch unique customer_uuid from quotes."""
    resp = supabase.table("quotes").select("customer_uuid").execute()
    rows = getattr(resp, "data", []) or []

    unique = {}
    for r in rows:
        unique[r["customer_uuid"]] = True
    return list(unique.keys())


def fetch_quote_for_customer(customer_uuid: str):
    """Pick one quote for the customer (e.g. latest created_at)."""
    resp = (
        supabase.table("quotes")
        .select(
            "uuid, quote_id, customer_uuid, start_date, "
            "quoted_total_premium, payment_frequency"
        )
        .eq("customer_uuid", customer_uuid)
        .order("created_at", desc=True)
        .limit(1)
        .execute()
    )
    rows = getattr(resp, "data", []) or []
    return rows[0] if rows else None


def build_policy_rows(conversion_rate: float = 0.10):
    """
    Build policy rows for ~conversion_rate of customers based on their quotes.
    """
    customers = fetch_unique_customers()
    if not customers:
        print("No customers found in quotes table.")
        return []

    num_to_convert = max(1, int(len(customers) * conversion_rate))
    if num_to_convert > len(customers):
        num_to_convert = len(customers)

    selected_customers = random.sample(customers, num_to_convert)

    start_policy_index = get_next_policy_start()
    current_index = start_policy_index

    policies = []

    for cust_uuid in selected_customers:
        quote = fetch_quote_for_customer(cust_uuid)
        if not quote:
            continue

        start_date_str = quote["start_date"]
        if not start_date_str:
            continue

        start_date = date.fromisoformat(start_date_str)
        end_date = start_date + relativedelta(years=1)

        premium = float(quote["quoted_total_premium"])

        policy = {
            "customer_uuid": quote["customer_uuid"],
            "quote_uuid": quote["uuid"],
            "quote_id": quote["quote_id"],
            "policy_id": f"p_{current_index:07d}",
            "policy_start_date": start_date.isoformat(),
            "policy_end_date": end_date.isoformat(),
            "status": "Active",
            "written_premium": premium,
            "current_premium": premium,
            "currency": "GBP",
            "payment_frequency": quote.get("payment_frequency"),
            "payment_method": "Card",  # demo default
        }
        policies.append(policy)
        current_index += 1

    return policies


# ---------- 3. ENTRY POINT ----------
def main():
    policies = build_policy_rows(conversion_rate=0.10)
    if not policies:
        print("No policies to insert.")
        return

    print(f"Inserting {len(policies)} policies...")
    try:
        supabase.table("policies").insert(policies).execute()
        print("✅ Policies inserted.")
    except Exception as e:
        print(f"❌ Failed to insert policies: {e}")


if __name__ == "__main__":
    main()
