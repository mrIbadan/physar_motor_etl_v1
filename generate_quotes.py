import os
import uuid
import random
from datetime import datetime, date, timedelta

from faker import Faker
from supabase import create_client, Client

# ---------- 1. CONNECTION (hardcoded for now) ----------
SUPABASE_URL = "https://jxonjddldsakvxqklaqd.supabase.co"
SUPABASE_KEY = "sb_secret_rZT7TG1WXbuazTIM9T53Rg_dtKkfI3s"

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ---------- 2. DATA GENERATION SETUP ----------
fake = Faker("en_GB")

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
SEX_OPTIONS = [True, False]
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


def random_start_date() -> str:
    """Start date within ±30 days of today, as ISO string."""
    offset = random.randint(-30, 30)
    return (date.today() + timedelta(days=offset)).isoformat()


def build_random_email() -> str:
    """Create a fake email on made-up domains (never null)."""
    first = fake.first_name().lower()
    last = fake.last_name().lower()
    num = random.randint(1, 9999)
    sep = random.choice([".", "_", ""])
    domain = random.choice(EMAIL_DOMAINS)
    return f"{first}{sep}{last}{num}@{domain}"


def generate_quote(i: int) -> dict:
    """Generate one quote row matching the public.quotes schema."""
    make = random.choice(list(CAR_DATA.keys()))
    model = random.choice(CAR_DATA[make])

    customer_uuid = uuid.uuid4()

    return {
        # Required IDs
        "uuid": str(uuid.uuid4()),                  # primary key
        "quote_id": f"q_{i:07d}",                   # unique text ID
        "customer_uuid": str(customer_uuid),        # stable per customer

        # Person & demographics
        "title": random.choice(["Mr", "Ms", "Mrs", "Dr"]),
        "first_name": fake.first_name(),
        "last_name": fake.last_name(),
        "email_address": build_random_email(),
        "date_of_birth": fake.date_of_birth(
            minimum_age=18, maximum_age=75
        ).isoformat(),
        "sex": random.choice(SEX_OPTIONS),
        "nationality": random.choice(NATIONALITIES),
        "marital_status": random.choice(MARITAL_STATUS),
        "employment_status": random.choice(EMPLOYMENT_STATUS),
        "job_title": fake.job(),
        "occupation": fake.job(),

        # Vehicle & usage
        "car_make": make,
        "car_model": model,
        "abi_group": random.randint(1, 50),
        "transmission": random.choice(TRANSMISSION),
        "vehicle_usage": random.choice(VEHICLE_USAGE),
        "parking": random.choice(PARKING_OPTIONS),

        # Driving & risk
        "driving_licence_type": random.choice(LICENCE_TYPES),
        "driving_licence_years": random.randint(1, 57),
        "licence_issue_country": "UK",
        "number_of_ncd_years": random.choice([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]),
        "number_of_past_claims": random.choices(
            [0, 1, 2, 3], weights=[75, 15, 7, 3]
        )[0],
        "number_of_ccjs": random.choices(
            [0, 1, 2], weights=[92, 6, 2]
        )[0],
        "medical_conditions": random.choice(
            [None, "None", "Diabetes", "Heart condition", "Epilepsy"]
        ),

        # Cover & pricing
        "cover_type": random.choice(COVER_TYPES),
        "start_date": random_start_date(),
        "estimated_annual_mileage": random.randint(3000, 20000),
        "payment_frequency": random.choice(PAYMENT_FREQUENCY),
        "personal_injury_cover": random.choice([True, False]),
        "breakdown_cover": random.choice([True, False]),
        "courtesy_car": random.choice([True, False]),
        "quoted_total_premium": float(
            round(random.uniform(350.00, 2500.00), 2)
        ),

        # Status & audit
        "status": "Quoted",
        "created_at": datetime.utcnow().isoformat(),  # timestamptz-compatible
    }


def main(total_records: int = 10, batch_size: int = 10) -> None:
    """Generate and insert synthetic quotes into Supabase."""
    print(f"Generating {total_records} records...")
    data = [generate_quote(i) for i in range(1, total_records + 1)]

    for i in range(0, len(data), batch_size):
        batch = data[i : i + batch_size]
        try:
            response = supabase.table("quotes").insert(batch).execute()
            print(
                f"✅ Successfully ingested batch: {i} to {i + len(batch)} "
                f"(status: {getattr(response, 'status_code', 'unknown')})"
            )
        except Exception as e:
            print(f"❌ Batch failed at index {i}: {e}")


if __name__ == "__main__":
    total = int(os.getenv("TOTAL_RECORDS", "10"))
    batch = int(os.getenv("BATCH_SIZE", "10"))
    main(total_records=total, batch_size=batch)
