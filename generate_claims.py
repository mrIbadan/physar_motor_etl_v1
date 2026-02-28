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

# ---------- BEHAVIOUR RULES ----------
MIN_DAYS_AFTER_START_FOR_CLAIM = 30       # no claims in first month
MAX_CLAIMS_PER_POLICY_PER_YEAR = 3        # hard cap per policy per year
P_CLAIMING_POLICY = 0.2                   # ~20% of policies have any claims at all
P_SECOND_CLAIM_GIVEN_FIRST = 0.3          # given a 1st claim, 30% chance of a 2nd
P_THIRD_CLAIM_GIVEN_SECOND = 0.1          # given a 2nd, 10% chance of a 3rd


# ---------- 2. HELPERS ----------

def fetch_candidate_policies(limit: int = 500) -> list[dict]:
    """
    Fetch policies that are eligible for claims:
    - Active.
    - Started at least MIN_DAYS_AFTER_START_FOR_CLAIM days ago.
    """
    cutoff_date = (date.today() - timedelta(days=MIN_DAYS_AFTER_START_FOR_CLAIM)).isoformat()

    resp = (
        supabase.table("policies")
        .select("uuid, policy_id, policy_start_date, policy_end_date, status")
        .lte("policy_start_date", cutoff_date)
        .eq("status", "Active")
        .order("policy_start_date", desc=True)
        .limit(limit)
        .execute()
    )
    return getattr(resp, "data", []) or []


def sample_claim_count_for_policy() -> int:
    """
    Decide how many claims a policy should get in its policy year.
    - Most policies: 0 claims.
    - Some: 1 claim.
    - Few: 2–3 claims.
    Never more than MAX_CLAIMS_PER_POLICY_PER_YEAR.
    """
    if random.random() > P_CLAIMING_POLICY:
        return 0

    count = 1
    if count < MAX_CLAIMS_PER_POLICY_PER_YEAR and random.random() < P_SECOND_CLAIM_GIVEN_FIRST:
        count += 1
    if count < MAX_CLAIMS_PER_POLICY_PER_YEAR and random.random() < P_THIRD_CLAIM_GIVEN_SECOND:
        count += 1
    return count


def build_one_claim_for_policy(policy: dict) -> dict:
    """
    Build a single claim for a given policy.
    - Loss date is at least MIN_DAYS_AFTER_START_FOR_CLAIM after start.
    - Loss is somewhere within the policy year.
    """
    start = date.fromisoformat(policy["policy_start_date"])
    end = date.fromisoformat(policy["policy_end_date"])

    # Earliest possible loss = start + MIN_DAYS_AFTER_START_FOR_CLAIM
    earliest_loss = start + timedelta(days=MIN_DAYS_AFTER_START_FOR_CLAIM)
    if earliest_loss > end:
        # Fallback: if policy shorter than delay (unlikely), allow anywhere in policy
        earliest_loss = start

    days_span = max((end - earliest_loss).days, 1)
    loss_date = earliest_loss + timedelta(days=random.randint(0, days_span))
    report_date = loss_date + timedelta(days=random.randint(0, 14))

    claim_type = random.choice(CLAIM_TYPES)

    # Sample incurred based on claim type
    if claim_type == "Windscreen":
        incurred = round(random.uniform(100, 400), 2)
    elif claim_type in ("Theft", "Fire"):
        incurred = round(random.uniform(500, 5000), 2)
    elif claim_type == "Injury":
        incurred = round(random.uniform(1000, 15000), 2)
    else:
        incurred = round(random.uniform(300, 8000), 2)

    # Paid vs outstanding split
    if random.random() < 0.6:
        paid = incurred
        outstanding = 0.0
        claim_status = "Closed"
        settlement_date = report_date + timedelta(days=random.randint(1, 120))
    else:
        paid = round(incurred * random.uniform(0.0, 0.7), 2)
        outstanding = round(incurred - paid, 2)
        claim_status = "Open"
        settlement_date = None

    claim_seq = random.randint(1_000_000, 9_999_999)

    return {
        "uuid": str(uuid.uuid4()),
        "claim_id": f"c_{claim_seq:07d}",
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

def main(max_policies_to_sample: int = 50) -> None:
    """
    Each run:
    - Take up to max_policies_to_sample eligible policies.
    - For each, decide 0–3 claims using the rules above.
    - Insert all generated claims.
    """
    policies = fetch_candidate_policies(limit=max_policies_to_sample)
    if not policies:
        print("No eligible policies found for claims.")
        return

    claims: list[dict] = []

    for policy in policies:
        n_claims = sample_claim_count_for_policy()
        if n_claims == 0:
            continue
        for _ in range(n_claims):
            claims.append(build_one_claim_for_policy(policy))

    if not claims:
        print("No claims to insert after sampling.")
        return

    print(f"Inserting {len(claims)} claims across {len(policies)} policies...")
    try:
        resp = supabase.table("claims").insert(claims).execute()
        print(
            f"✅ Inserted {len(claims)} claims "
            f"(status: {getattr(resp, 'status_code', 'unknown')})"
        )
    except Exception as e:
        print(f"❌ Failed to insert claims: {e}")


if __name__ == "__main__":
    max_policies = int(os.getenv("MAX_POLICIES_FOR_CLAIMS", "50"))
    main(max_policies_to_sample=max_policies)
