import os
import uuid
import random
from datetime import datetime, date, timedelta, timezone
from faker import Faker
from supabase import create_client, Client

# CONFIG
URL = "https://jxonjddldsakvxqklaqd.supabase.co"
KEY = "sb_secret_rZT7TG1WXbuazTIM9T53Rg_dtKkfI3s"
supabase: Client = create_client(URL, KEY)
fake = Faker("en_GB")

def generate_quote(i):
    # DEFINED INSIDE TO KILL THE NAMEERROR
    CAR_DATA = {
        "Ford": ["Fiesta", "Focus", "Puma"],
        "Tesla": ["Model 3", "Model Y"],
        "BMW": ["3 Series", "5 Series"],
        "Audi": ["A3", "Q5"]
    }
    
    make = random.choice(list(CAR_DATA.keys()))
    model = random.choice(CAR_DATA[make])
    
    # 80% have good credit, 20% are "Risk"
    credit_score = random.choices([random.randint(300, 500), random.randint(501, 900)], weights=[20, 80])[0]

    return {
        "uuid": str(uuid.uuid4()),
        "quote_id": f"q_{i:07d}",
        "customer_uuid": str(uuid.uuid4()),
        "first_name": fake.first_name(),
        "last_name": fake.last_name(),
        "email_address": f"{fake.first_name().lower()}@testmail.com",
        "date_of_birth": fake.date_of_birth(minimum_age=18, maximum_age=75).isoformat(),
        "car_make": make,
        "car_model": model,
        "credit_score": credit_score,
        "number_of_ccjs": random.choices([0, 1, 2], weights=[90, 8, 2])[0],
        "quoted_total_premium": float(round(random.uniform(400.0, 2000.0), 2)),
        "status": "Quoted",
        "start_date": (date.today() + timedelta(days=7)).isoformat(),
        "created_at": datetime.now(timezone.utc).isoformat()
    }

def main():
    # Get last ID
    resp = supabase.table("quotes").select("quote_id").order("quote_id", desc=True).limit(1).execute()
    rows = getattr(resp, "data", []) or []
    start_idx = (int(rows[0]["quote_id"].split("_")[1]) + 1) if rows else 1
    
    # Generate 10
    data = [generate_quote(i) for i in range(start_idx, start_idx + 10)]
    supabase.table("quotes").insert(data).execute()
    print(f"✅ Created 10 quotes starting at q_{start_idx:07d}")

if __name__ == "__main__":
    main()
