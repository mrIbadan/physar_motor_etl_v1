import uuid
import random
from datetime import datetime, date, timedelta

from faker import Faker
from supabase import create_client, Client

# ---------- 1. CONNECTION ----------
SUPABASE_URL = "https://jxonjddldsakvxqklaqd.supabase.co"
SUPABASE_KEY = "sb_secret_rZT7TG1WXbuazTIM9T53Rg_dtKkfI3s"

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
fake = Faker("en_GB")

# ---------- 2. CONSTANTS & DICTIONARIES ----------

CAR_DATA = {
    "Ford": ["Fiesta", "Focus", "Puma", "Kuga"],
    "Tesla": ["Model 3", "Model Y", "Model S", "Model X"],
    "BMW": ["1 Series", "3 Series", "5 Series", "X1", "X3", "X5"],
    "Volkswagen": ["Golf", "Polo", "ID.3", "Passat", "Tiguan"],
    "Audi": ["A1", "A3", "A4", "A5", "Q3", "Q5"],
    "Mercedes-Benz": ["A-Class", "C-Class", "E-Class", "GLA", "GLC"],
    "Nissan": ["Micra", "Qashqai", "Juke"],
    "Toyota": ["Yaris", "Corolla", "RAV4"],
    "Vauxhall": ["Corsa", "Astra", "Insignia"],
}

COVER_TYPES = ["Comprehensive", "Third Party", "Third Party, Fire and Theft"]
SEX_OPTIONS = ["Male", "Female"]
MARITAL_STATUS = ["Single", "Married", "Divorced", "Widowed"]
NATIONALITIES = ["UK", "EU", "Other"]
EMPLOYMENT_STATUS = ["Employed", "Self-Employed", "Student", "Retired", "Unemployed"]
VEHICLE_USAGE = ["Social, domestic & pleasure", "SDP + commuting", "Business use"]
PARKING_OPTIONS = ["Garage", "Driveway", "On street", "Car park"]
LICENCE_TYPES = ["Full UK", "Provisional", "EU", "International"]
PAYMENT_FREQUENCY = ["Annual", "Monthly"]
TRANSMISSION = ["Manual", "Automatic"]

EMAIL_DOMAINS = [
    "youmail.com",
    "randommail.com",
    "testmail.net",
    "demoemail.org",
    "maildemo.io",
    "sampleinbox.co",
    "fakedomain.test",
]

# ---------- 3. HELPERS ----------

def get_next_quote_start() -> int:
    """
    Continue quote_id from the highest existing q_XXXXXXX.
    """
    try:
        resp = (
            supabase.table("quotes")
            .select("quote_id")
            .order("quote_id", desc=True)
            .limit(1)
            .execute()
        )
        rows = getattr(resp, "data", []) or []
        if not rows:
            return 1
        last_id = rows[0]["quote_id"]  # e.g. "q_0000123"
        return int(last_id.split("_")[1]) + 1
    except Exception:
        return 1


def random_start_date() -> str:
    offset = random.randint(1, 30)
    return (date.today() + timedelta(days=offset)).isoformat()


def build_random_email() -> str:
    first = fake.first_name().lower()
    last = fake.last_name().lower()
    num = random.randint(1, 9999)
    sep = random.choice([".", "_", ""])
    domain = random.choice(EMAIL_DOMAINS)
    return f"{first}{sep}{last}{num}@{domain}"


def generate_quote(i: int) -> dict:
    make = random.choice(list(CAR_DATA.keys()))
    model = random.choice(CAR_DATA[make])

    # credit_score bands: 15% high risk, 65% standard, 20% prime
    credit_score = random.choices(
        [
            random.randint(300, 500),  # high risk
            random.randint(501, 750),  # standard
            random.randint(751, 900),  # prime
        ],
        weights=[15, 65, 20],
    )[0]

    return {
        "uuid": str(uuid.uuid4()),
        "quote_id": f"q_{i:07d}",
        "customer_uuid": str(uuid.uuid4()),
        "title": random.choice(["Mr", "Ms", "Mrs", "Dr"]),
        "first_name": fake.first_name(),
        "last_name": fake.last_name(),
        "email_address": build_random_email(),
        "date_of_birth": fake.date_of_birth(minimum_age=18, maximum_age=75).isoformat(),
        "sex": random.choice(SEX_OPTIONS),
        "nationality": random.choice(NATIONALITIES),
        "marital_status": random.choice(MARITAL_STATUS),
        "employment_status": random.choice(EMPLOYMENT_STATUS),
        "job_title": fake.job(),
        "occupation": fake.job(),
        "car_make": make,
        "car_model": model,
        "abi_group": random.randint(1, 50),
        "transmission": random.choice(TRANSMISSION),
        "vehicle_usage": random.choice(VEHICLE_USAGE),
        "parking": random.choice(PARKING_OPTIONS),
        "driving_licence_type": random.choice(LICENCE_TYPES),
        "driving_licence_years": random.randint(1, 57),
        "licence_issue_country": "UK",
        "number_of_ncd_years": random.choice(list(range(10))),
        "number_of_past_claims": random.choices([0, 1, 2, 3], weights=[75, 15, 7, 3])[0],
        "number_of_ccjs": random.choices([0, 1, 2], weights=[92, 6, 2])[0],
        "medical_conditions": random.choice(
            [None, "None", "Diabetes", "Heart condition", "Epilepsy"]
        ),
        "cover_type": random.choice(COVER_TYPES),
        "start_date": random_start_date(),
        "estimated_annual_mileage": random.randint(3000, 20000),
        "payment_frequency": random.choice(PAYMENT_FREQUENCY),
        "personal_injury_cover": random.choice([True, False]),
        "breakdown_cover": random.choice([True, False]),
        "courtesy_car": random.choice([True, False]),
        "quoted_total_premium": float(round(random.uniform(350.00, 2500.00), 2)),
        "status": "Quoted",
        "created_at": datetime.utcnow().isoformat(),
        "credit_score": credit_score,
        # rejection_reason is left to default NULL; will be set in generate_policies.py for rejects.
    }

# ---------- 4. MAIN EXECUTION ----------

def main():
    start_idx = get_next_quote_start()
    data = [generate_quote(i) for i in range(start_idx, start_idx + 10)]
    try:
        supabase.table("quotes").insert(data).execute()
        print(f"✅ Successfully generated quotes q_{start_idx:07d} to q_{start_idx+9:07d}")
    except Exception as e:
        print(f"❌ Database Insert Error: {e}")

if __name__ == "__main__":
    main()
