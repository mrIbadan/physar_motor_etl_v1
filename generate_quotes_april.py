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
    """Calculates probability with strict logit clipping."""
    w = ncd * 35
    if credit_band == "Excellent": w += 45
    logit = -3.0 + (0.02 * w)
    # Clip to avoid Inf
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

# ---------- 2. THE SERIALIZATION GUARDIAN ----------
def safe_json_value(v):
    """Forces every single value into a JSON-compliant standard Python type."""
    if v is None:
        return None
    if isinstance(v, (bool, str)):
        return v
    if isinstance(v, (int, np.integer)):
        return int(v)
    if isinstance(v, (float, np.floating)):
        if math.isinf(v) or math.isnan(v):
            return None
        # Limit precision to prevent float-point errors
        return round(float(v), 6)
    return str(v)

# ---------- 3. MAIN ----------
def main():
    print(f"🚀 Generating {TOTAL_RECORDS} records...")
    
    # 1. Generate using pure list of dicts first
    raw_data = [generate_quote_row(i) for i in range(1, TOTAL_RECORDS + 1)]
    
    # 2. Use Pandas ONLY for the sorting/status logic
    df = pd.DataFrame(raw_data)
    df = df.sort_values("conversion_probability", ascending=False).reset_index(drop=True)
    
    n_accept = int(0.10 * len(df))
    df["status"] = "Rejected"
    df.loc[:n_accept-1, "status"] = "Accepted"
    df.loc[df["status"] == "Rejected", "rejection_reason"] = "Risk/Price Mismatch"

    # 3. BRUTE FORCE CONVERSION back to standard Python types
    # This strips away ALL NumPy metadata that breaks JSON
    records = []
    for _, row in df.iterrows():
        clean_row = {str(k): safe_json_value(v) for k, v in row.items()}
        records.append(clean_row)

    # 4. PUSH
    for i in range(0, len(records), BATCH_SIZE):
        batch = records[i:i+BATCH_SIZE]
        try:
            # Final validation check
            json.dumps(batch)
            
            supabase.table("quotes").insert(batch).execute()
            print(f"✅ Batch {i//BATCH_SIZE + 1} pushed.")
        except Exception as e:
            print(f"❌ FATAL ERROR on Batch {i}: {e}")
            # Identify exact problematic key
            for row in batch:
                for k, v in row.items():
                    try:
                        json.dumps(v)
                    except:
                        print(f"🚨 ILLEGAL KEY: {k} | VALUE: {v} | TYPE: {type(v)}")
            break

if __name__ == "__main__":
    main()
