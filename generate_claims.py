import os
import uuid
import random
from datetime import datetime, timedelta, date

from supabase import create_client, Client

# ---------- 1. CONNECTION ----------
SUPABASE_URL = "https://jxonjddldsakvxqklaqd.supabase.co"
SUPABASE_KEY = "sb_secret_rZT7TG1WXbuazTIM9T53Rg_dtKkfI3s"

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

CAUSES = [
    "Accident - at fault",
    "Accident - non-fault",
    "Theft",
    "Fire",
    "Weather",
    "Vandalism",
]
FAULT_OPTIONS = ["Fault", "Non-fault", "Split"]
CLAIM_TYPES = ["Own damage", "TP property", "Injury", "Windscreen"]
HANDLERS = ["Alex", "Jamie", "Taylor", "Jordan", "Morgan"]


# ---------- 2. HELPERS ----------
def pick_random_policy() -> dict:
    """
    Pick a random recent policy from public.policies.

    Expects policies schema:
      uuid (pk), policy_id, policy_start_date, policy_end_date.
    """
    resp = (
        supabase.table("policies")
        .select("uuid, policy_id, policy_start_date, policy_end_date")
        .order("created_at", desc=True)
        .limit(100)
        .execute()
    )
    policies = getattr(resp, "data", []) or []
    if not policies:
        raise RuntimeError("No policies found to attach claims to")
    return random.choice(policies)


def build_one_claim() -> dict:
    """
    Build a single synthetic claim linked to a policy by policy_id.
    Matches public.claims schema above.
    """
    policy = pick_random_policy()

    start = date.fromisoformat(policy["policy_start_date"])
    end = date.fromisoformat(policy["policy_end_date"])
    days_span = (end - start).days or 1
    loss_date = start + timedelta(days=random.randint(0, days_span))
    report_date = loss_date + timedelta(days=random.randint(0, 14))

    # Sample incurred based on claim type (no premium dependency)
    claim_type = random.choice(CLAIM_TYPES)
    if claim_type == "Windscreen":
        incurred = round(random.uniform(100, 400), 2)
    elif claim_type in ("Theft", "Fire"):
        incurred = round(random.uniform(500, 5000), 2)
    elif claim_type == "Injury":
        incurred = round(random.uniform(1000, 15000), 2)
    else:
        incurred = round(random.uniform(300, 8000), 2)

    # Simple paid vs outstanding split
    if random.random() < 0.5:
        paid = incurred
        outstanding = 0.0
        claim_status = "Closed"
        settlement_date = report_date + timedelta(days=random.randint(1, 60))
    else:
        paid = round(incurred * random.uniform(0.0, 0.7), 2)
        outstanding = round(incurred - paid, 2)
        claim_status = "Open"
        settlement_date = None

    claim_seq = random.randint(1_000_000, 9_999_999)

    return {
        "uuid": str(uuid.uuid4()),
        "claim_id": f"c_{claim_seq:07d}",

        # FK to policies.policy_id
        "policy_id": policy["policy_id"],

        "loss_date": loss_date.isoformat(),
        "report_date": report_date.isoformat(),
        "claim_status": claim_status,
        "cause_of_loss": random.choice(CAUSES),
        "fault": random.choice(FAULT_OPTIONS),
        "claim_type": claim_type,
        "description": f"Synthetic {claim_type} claim",

        "incurred_amount": incurred,
        "paid_amount": paid,
        "outstanding_amount": outstanding,

        "handler_name": random.choice(HANDLERS),
        "settlement_date": (
            settlement_date.isoformat() if settlement_date else None
        ),

        "created_at": datetime.utcnow().isoformat(),
    }


# ---------- 3. ENTRY POINT ----------
def main(total_claims: int = 5) -> None:
    claims: list[dict] = []
    for _ in range(total_claims):
        try:
            claims.append(build_one_claim())
        except RuntimeError as e:
            print(f"⚠️ Skipping claim build: {e}")
            break

    if not claims:
        print("No claims to insert.")
        return

    print(f"Inserting {len(claims)} claims...")
    try:
        resp = supabase.table("claims").insert(claims).execute()
        print(
            f"✅ Inserted {len(claims)} claims "
            f"(status: {getattr(resp, 'status_code', 'unknown')})"
        )
    except Exception as e:
        print(f"❌ Failed to insert claims: {e}")


if __name__ == "__main__":
    total = int(os.getenv("TOTAL_CLAIMS", "5"))
    main(total_claims=total)
