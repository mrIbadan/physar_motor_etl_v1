import random
from datetime import date, timedelta

from supabase import create_client, Client

# ---------- 1. CONNECTION ----------
SUPABASE_URL = "https://jxonjddldsakvxqklaqd.supabase.co"
SUPABASE_KEY = "sb_secret_rZT7TG1WXbuazTIM9T53Rg_dtKkfI3s"

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

CLAIM_TYPES = [
    "Accidental damage",
    "Third-party property",
    "Bodily injury",
    "Theft",
    "Fire",
    "Windscreen",
    "Vandalism",
]

FAULT_TYPES = ["Fault", "Non-fault", "Split"]


# ---------- 2. HELPERS ----------
def get_next_claim_start() -> int:
    """Get next claim sequence from claims.claim_reference."""
    resp = (
        supabase.table("claims")
        .select("claim_reference")
        .order("claim_reference", desc=True)
        .limit(1)
        .execute()
    )
    rows = getattr(resp, "data", []) or []
    if not rows:
        return 1

    last_id = rows[0]["claim_reference"]  # e.g. "c_0000123"
    try:
        return int(last_id.split("_")[1]) + 1
    except Exception:
        return 1


def pick_random_policy():
    """Pick a random policy from policies table."""
    resp = (
        supabase.table("policies")
        .select(
            "policy_uuid, customer_uuid, quote_uuid, policy_id, "
            "policy_start_date, policy_end_date, current_premium"
        )
        .limit(1000)
        .execute()
    )
    rows = getattr(resp, "data", []) or []
    if not rows:
        return None
    return random.choice(rows)


def sample_claim_amount(claim_type: str, premium: float) -> float:
    """Sample a rough incurred amount based on type and premium."""
    if claim_type == "Windscreen":
        return round(random.uniform(100, 400), 2)
    if claim_type in ("Theft", "Fire"):
        return round(random.uniform(0.5, 2.0) * premium, 2)
    if claim_type == "Bodily injury":
        return round(random.uniform(0.5, 3.0) * premium, 2)
    # Accidental damage, TP property, Vandalism
    return round(random.uniform(0.2, 1.5) * premium, 2)


def build_one_claim():
    policy = pick_random_policy()
    if not policy:
        print("No policies found; cannot create claim.")
        return None

    claim_index = get_next_claim_start()
    claim_ref = f"c_{claim_index:07d}"

    claim_type = random.choice(CLAIM_TYPES)
    fault = random.choices(FAULT_TYPES, weights=[0.6, 0.3, 0.1])[0]

    start = date.fromisoformat(policy["policy_start_date"])
    end = date.fromisoformat(policy["policy_end_date"])
    duration_days = (end - start).days or 1

    loss_date = start + timedelta(days=random.randint(0, duration_days - 1))
    reported_date = loss_date + timedelta(days=random.randint(0, 14))

    premium = float(policy["current_premium"])
    incurred = sample_claim_amount(claim_type, premium)

    # Simple paid vs outstanding split
    if random.random() < 0.5:
        paid = incurred
        outstanding = 0.0
        status = "Closed"
        settlement_date = reported_date + timedelta(days=random.randint(1, 60))
    else:
        paid = round(incurred * random.uniform(0.0, 0.7), 2)
        outstanding = round(incurred - paid, 2)
        status = "Open"
        settlement_date = None

    return {
        "policy_uuid": policy["policy_uuid"],
        "customer_uuid": policy["customer_uuid"],
        "quote_uuid": policy.get("quote_uuid"),
        "policy_id": policy["policy_id"],
        "claim_reference": claim_ref,
        "insurer_claim_number": None,
        "claim_type": claim_type,
        "fault": fault,
        "claim_description": f"Synthetic {claim_type} claim ({fault})",
        "loss_date": loss_date.isoformat(),
        "reported_date": reported_date.isoformat(),
        "settlement_date": settlement_date.isoformat() if settlement_date else None,
        "incurred_amount": incurred,
        "paid_amount": paid,
        "outstanding_amount": outstanding,
        "status": status,
    }


# ---------- 3. ENTRY POINT ----------
def main():
    claim = build_one_claim()
    if not claim:
        return

    print(f"Inserting claim {claim['claim_reference']} for policy {claim['policy_id']}...")
    try:
        supabase.table("claims").insert(claim).execute()
        print("✅ Claim inserted.")
    except Exception as e:
        print(f"❌ Failed to insert claim: {e}")


if __name__ == "__main__":
    main()
