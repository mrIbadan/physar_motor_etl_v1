import os
import uuid
import random
from datetime import datetime, date, timedelta
from faker import Faker
from supabase import create_client, Client

# ---------- 1. CONFIG & CONNECTION ----------
SUPABASE_URL = "https://jxonjddldsakvxqklaqd.supabase.co"
SUPABASE_KEY = "sb_secret_rZT7TG1WXbuazTIM9T53Rg_dtKkfI3s"
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
fake = Faker("en_GB")

# ---------- 2. CONSTANTS (The Missing Data) ----------
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
VEHICLE_USAGE = ["Social, domestic & pleasure", "SDP + commuting", "Business use"]
PAYMENT_FREQUENCY = ["Annual", "Monthly"]

# ---------- 3. HELPERS ----------

def get_next_quote_start() -> int:
    try:
        resp = supabase.table("quotes").select("quote_id").order("quote_id", desc=True).limit(1).execute()
        rows = getattr(resp, "data", []) or []
        if not rows: return 1
        return int(rows[0]["quote_id"].split("_")[1]) + 1
    except:
        return 1

def generate_quote(i: int) -> dict:
    make = random.choice(list(CAR_DATA.keys()))
    model = random.choice(CAR_DATA[make])
    
    # Generate credit score (300-900)
    credit_score = random.choices(
        [random.randint(300, 500), random.randint(501, 750), random.randint(751, 900)],
        weights=[15, 65, 20]
    )[0]

    return {
        "uuid": str(uuid.uuid4()),
        "quote_id": f"q_{i:07d}",
        "customer_uuid": str(uuid.uuid4()),
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
        "cover_type": random.choice(COVER_TYPES),
        "vehicle_usage": random.choice(VEHICLE_USAGE),
        "payment_frequency": random.choice(PAYMENT_FREQUENCY),
        "start_date": (date.today() + timedelta(days=random.randint(1, 30))).isoformat(),
        "created_at": datetime.utcnow().isoformat(),
    }

# ---------- 4. EXECUTION ----------

def main():
    start_idx = get_next_quote_start()
    # Generate 10 new quotes
    data = [generate_quote(i) for i in range(start_idx, start_idx + 10)]
    
    try:
        supabase.table("quotes").insert(data).execute()
        print(f"✅ Successfully generated 10 quotes (q_{start_idx:07d} to q_{start_idx+9:07d})")
    except Exception as e:
        print(f"❌ Error inserting quotes: {e}")

if __name__ == "__main__":
    main()
