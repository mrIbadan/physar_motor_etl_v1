import random
from datetime import date

from dateutil.relativedelta import relativedelta
from supabase import create_client, Client

# ---------- 1. CONNECTION ----------
SUPABASE_URL = "https://jxonjddldsakvxqklaqd.supabase.co"
SUPABASE_KEY = "sb_secret_rZT7TG1WXbuazTIM9T53Rg_dtKkfI3s"

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ---------- 2. CONSTANTS ----------

REJECTION_REASONS = [
    "User Cancelled / Did not accept",
    "User Declined due to UW rules",
]


# ---------- 3. HELPERS ----------

def get_next_policy_start() -> int:
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

    last_id = rows[0]["policy_id"]
    try:
        return int(last_id.split("_")[1]) + 1
    except Exception:
        return 1


def fetch_unconverted_quotes():
    resp = supabase.table("policies").select("quote_id").execute()
    already_converted = set(
        row["quote_id"] for row in (getattr(resp, "data", []) or []) if row.get("quote_id")
    )

    resp = (
        supabase.table("quotes")
        .select(
            "uuid, quote_id, customer_uuid, start_date, "
            "quoted_total_premium, payment_frequency, "
            "cover_type, vehicle_usage, car_make, car_model, abi_group"
        )
        .eq("status", "Quoted")
        .execute()
    )
    all_quotes = getattr(resp, "data", []) or []
    return [q for q in all_quotes if q["quote_id"] not in already_converted]


def mark_rejected_quotes(converted_quote_ids: set[str]):
    if not converted_quote_ids:
        return

    resp = (
        supabase.table("quotes")
        .select("quote_id")
        .eq("status", "Quoted")
        .execute()
    )
    rows = getattr(resp, "data", []) or []
    if not rows:
        return

    for r in rows:
        qid = r["quote_id"]
        if qid in converted_quote_ids:
            continue
        supabase.table("quotes").update({
            "status": "Rejected",
            "rejection_reason": random.choice(REJECTION_REASONS),
        }).eq("quote_id", qid).execute()


# ---------- 4. CORE LOGIC ----------

def build_policy_rows(conversion_rate: float = 0.10):
    unconverted = fetch_unconverted_quotes()

    if not unconverted:
        print("No unconverted quotes found — nothing to do.")
        return []

    policies = []
    current_index = get_next_policy_start()

    for quote in unconverted:
        if random.random() > conversion_rate:
            continue

        start_date_str = quote.get("start_date")
        if not start_date_str:
            continue

        start_date = date.fromisoformat(start_date_str)
        end_date = start_date + relativedelta(years=1)

        gwp = float(quote["quoted_total_premium"])
        commission = round(gwp * 0.15, 2)
        ipt = round(gwp * 0.12, 2)
        total = round(gwp + ipt, 2)

        cover_type = quote.get("cover_type") or "Comprehensive"
        payment_frequency = quote.get("payment_frequency") or "Annual"
        vehicle_usage = quote.get("vehicle_usage") or "Social, domestic & pleasure"
        car_make = quote.get("car_make") or "Unknown"
        car_model = quote.get("car_model") or "Unknown"
        abi_group = quote.get("abi_group") or 1

        policy = {
            "policy_id": f"p_{current_index:07d}",
            "quote_id": quote["quote_id"],
            "customer_uuid": quote["customer_uuid"],
            "policy_start_date": start_date.isoformat(),
            "policy_end_date": end_date.isoformat(),
            "cover_type": cover_type,
            "payment_frequency": payment_frequency,
            "vehicle_usage": vehicle_usage,
            "car_make": car_make,
            "car_model": car_model,
            "abi_group": abi_group,
            "gross_written_premium": gwp,
            "commission_amount": commission,
            "ipt_amount": ipt,
            "total_payable": total,
            "status": "Active",
        }
        policies.append(policy)
        current_index += 1

    return policies


# ---------- 5. ENTRY POINT ----------

def main():
    CONVERSION_RATE = 0.10

    policies = build_policy_rows(conversion_rate=CONVERSION_RATE)

    if not policies:
        print("Quotes processed, but none converted to policies this run (probabilistic 10%).")
        return

    print(f"Converting {len(policies)} policies...")
    try:
        supabase.table("policies").insert(policies).execute()
        print("✅ Success: New policies added.")

        converted_quote_ids = {p["quote_id"] for p in policies}
        mark_rejected_quotes(converted_quote_ids)

    except Exception as e:
        print(f"❌ Database Error: {e}")


if __name__ == "__main__":
    main()
