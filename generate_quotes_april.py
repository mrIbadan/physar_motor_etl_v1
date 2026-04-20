import os
import uuid
import random
import json
import math
from datetime import datetime, date, timedelta
import numpy as np
import pandas as pd
from faker import Faker
from supabase import create_client, Client

# ---------- 0. CONFIG ----------
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
fake = Faker("en_GB")

TOTAL_RECORDS = int(os.getenv("TOTAL_RECORDS", "50"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "10"))

# ---------- 1. DATA GENERATION ----------
def calculate_conversion_probability(ncd, credit_band):
    w = ncd * 35
    if credit_band == "Excellent": w += 45
    logit = -3.0 + (0.02 * w)
    # Clipping to prevent math from becoming Infinity
    logit = max(-15, min(15, logit))
    return float(1.0 / (1.0 + math.exp(-logit)))

def generate_quote_row(i):
    age = random.randint(18, 75)
    credit_band = random.choice(['Excellent', 'Good', 'Fair', 'Poor'])
    ncd = random.randint(0, 15)
    
    return {
        "uuid": str(uuid.uuid4()),
        "quote_id": f"q_{i:07d}",
        "customer_uuid": str(uuid.uuid4()),
        "title": random.choice(["Mr", "Mrs", "Miss", "Ms", "Dr"]),
        "first_name": fake.first_name(),
        "last_name": fake.last_name(),
        "email_address": f"{fake.first_name().lower()}@example.com",
        "date_of_birth": (date.today() - timedelta(days=age*365)).isoformat(),
        "sex": random.choice(["Male", "Female"]),
        "nationality": "UK",
        "marital_status": random.choice(["Single", "Married", "Divorced"]),
        "employment_status": "Employed",
        "occupation": "Professional",
        "job_title": "Manager",
        "home_owner": random.choice([True, False]),
        "car_make": random.choice(["Ford", "Tesla", "BMW", "Audi"]),
        "car_model": "Model X",
        "abi_group": random.randint(1, 50),
        "transmission": random.choice(["Manual", "Automatic"]),
        "estimated_annual_mileage": random.randint(4000, 12000),
        "quoted_total_premium": round(float(random.uniform(400, 1200)), 2),
        "status": "Pending",
        "conversion_probability": calculate_conversion_probability(ncd, credit_band),
        "postcode_risk_band": random.randint(1, 10),
        "number_of_ncd_years": ncd,
        "credit_score_band": credit_band,
        "cover_type": "Comprehensive",
        "has_blackbox": random.choice([True, False]),
        "driving_licence_type": "Full UK",
        "driving_licence_years": random.randint(1, 40),
        "payment_frequency": "Annual",
        "rejection_reason": None,
        "created_at": datetime.utcnow().isoformat()
    }

# ---------- 2. THE ERROR ISOLATOR ----------
def validate_batch_json(batch):
    """Checks every value for JSON compliance before we even talk to Supabase."""
    for row_idx, row in enumerate(batch):
        for key, value in row.items():
            try:
                json.dumps({key: value})
            except (ValueError, OverflowError):
                print(f"\n💥 FOUND THE ARSEHOLE!")
                print(f"Row Index: {row_idx}")
                print(f"Quote ID: {row.get('quote_id')}")
                print(f"Column: '{key}'")
                print(f"Value: {value} (Type: {type(value)})")
                print(f"Check if value is Inf or NaN: {math.isinf(value) if isinstance(value, float) else 'N/A'}")
                return False
    return True

# ---------- 3. MAIN ----------
def main():
    print(f"🚀 Initializing push: {TOTAL_RECORDS} records.")
    
    # Generate data
    data = [generate_quote_row(i) for i in range(1, TOTAL_RECORDS + 1)]
    
    # Sort and Status Logic
    df = pd.DataFrame(data)
    df = df.sort_values("conversion_probability", ascending=False).reset_index(drop=True)
    
    n_accept = int(0.10 * len(df))
    df["status"] = "Rejected"
    df.loc[:n_accept-1, "status"] = "Accepted"
    df.loc[df["status"] == "Rejected", "rejection_reason"] = "Risk/Price Mismatch"

    # Convert to pure Python list of dicts
    records = df.to_dict(orient="records")

    for i in range(0, len(records), BATCH_SIZE):
        batch = records[i:i+BATCH_SIZE]
        
        # --- NEW: ISOLATION CHECK ---
        if not validate_batch_json(batch):
            print("🛑 Stopping script because invalid data was found.")
            return

        try:
            # Pushes to Supabase
            supabase.table("quotes").insert(batch).execute()
            print(f"✅ Batch {i//BATCH_SIZE + 1} pushed.")
        except Exception as e:
            print(f"❌ DATABASE ERROR on Batch {i}: {e}")
            break

if __name__ == "__main__":
    main()
