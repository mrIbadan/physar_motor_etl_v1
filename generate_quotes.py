import os
import uuid
import random
from datetime import datetime
from faker import Faker
from supabase import create_client, Client

# ---------- 1. CONNECTION (hardcoded for now) ----------
# These are the values you already tested successfully
SUPABASE_URL = "https://jxonjddldsakvxqklaqd.supabase.co"
SUPABASE_KEY = "sb_secret_rZT7TG1WXbuazTIM9T53Rg_dtKkfI3s"

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ---------- 2. DATA GENERATION SETUP ----------
fake = Faker("en_GB")

CAR_DATA = {
    "Ford": ["Fiesta", "Focus", "Puma"],
    "Tesla": ["Model 3", "Model Y"],
    "BMW": ["3 Series", "5 Series", "X5"],
    "Volkswagen": ["Golf", "Polo", "ID.3"],
    "Audi": ["A3", "A4", "Q5"],
}

def generate_quote():
    make = random.choice(list(CAR_DATA.keys()))
    model = random.choice(CAR_DATA[make])

    return {
        "uuid": str(uuid.uuid4()),
        "customer_uuid": str(uuid.uuid4()),

        # Profile Data
        "first_name": fake.first_name(),
        "last_name": fake.last_name(),
        "email_address": fake.ascii_free_email(),
        "date_of_birth": fake.date_of_birth(minimum_age=18, maximum_age=75).isoformat(),
        "postcode": fake.postcode(),
        "employment_status": random.choice(
            ["Employed", "Self-Employed", "Student", "Retired"]
        ),
        "occupation": fake.job(),

        # Risk factors
        "number_of_ccjs": random.choices([0, 1, 2], weights=[92, 6, 2])[0],
        "number_of_past_claims": random.choices(
            [0, 1, 2, 3], weights=[75, 15, 7, 3]
        )[0],

        # Rating input & output
        "car_make": make,
        "car_model": model,
        "quoted_total_premium": float(round(random.uniform(350.00, 2500.00), 2)),
        "status": "Quoted",
        "created_at": datetime.utcnow().isoformat(),
    }

def main(total_records=1000, batch_size=500):
    print(f"Generating {total_records} records...")
    data = [generate_quote() for _ in range(total_records)]

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
    # allow overrides via env for later CI use
    total = int(os.getenv("TOTAL_RECORDS", "1000"))
    batch = int(os.getenv("BATCH_SIZE", "500"))
    main(total_records=total, batch_size=batch)
