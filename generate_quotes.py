import os
import uuid
import random
from datetime import datetime, date, timedelta
from faker import Faker
from supabase import create_client, Client

SUPABASE_URL = "https://jxonjddldsakvxqklaqd.supabase.co"
SUPABASE_KEY = "sb_secret_rZT7TG1WXbuazTIM9T53Rg_dtKkfI3s"
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
fake = Faker("en_GB")

# ... [CAR_DATA, COVER_TYPES, etc. dictionaries remain the same] ...

def generate_quote(i: int) -> dict:
    make = random.choice(list(CAR_DATA.keys()))
    model = random.choice(CAR_DATA[make])
    
    # Realistic UK Credit Score (300-900)
    credit_score = random.choices(
        [random.randint(300, 550), random.randint(551, 750), random.randint(751, 900)],
        weights=[15, 65, 20]
    )[0]

    return {
        "uuid": str(uuid.uuid4()),
        "quote_id": f"q_{i:07d}",
        "customer_uuid": str(uuid.uuid4()),
        "title": random.choice(["Mr", "Ms", "Mrs", "Dr"]),
        "first_name": fake.first_name(),
        "last_name": fake.last_name(),
        "email_address": f"{fake.first_name().lower()}@{random.choice(['testmail.com', 'demo.io'])}",
        "date_of_birth": fake.date_of_birth(minimum_age=18, maximum_age=75).isoformat(),
        "car_make": make,
        "car_model": model,
        "credit_score": credit_score,
        "number_of_ccjs": random.choices([0, 1, 2], weights=[92, 6, 2])[0],
        "quoted_total_premium": float(round(random.uniform(350.00, 2500.00), 2)),
        "status": "Quoted",
        "start_date": (date.today() + timedelta(days=random.randint(-30, 30))).isoformat(),
        "created_at": datetime.utcnow().isoformat(),
        # ... [Include other fields like transmission, usage, etc. as per your original script]
    }

def get_next_quote_start() -> int:
    resp = supabase.table("quotes").select("quote_id").order("quote_id", desc=True).limit(1).execute()
    rows = getattr(resp, "data", []) or []
    if not rows: return 1
    try: return int(rows[0]["quote_id"].split("_")[1]) + 1
    except: return 1

def main(total_records: int = 10, batch_size: int = 10) -> None:
    start_index = get_next_quote_start()
    data = [generate_quote(i) for i in range(start_index, start_index + total_records)]
    for i in range(0, len(data), batch_size):
        supabase.table("quotes").insert(data[i : i + batch_size]).execute()
    print(f"✅ Generated {total_records} quotes.")

if __name__ == "__main__":
    main(total=10)
