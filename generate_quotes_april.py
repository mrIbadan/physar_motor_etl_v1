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

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Missing SUPABASE_URL or SUPABASE_KEY secrets!")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
fake = Faker("en_GB")

TOTAL_RECORDS = int(os.getenv("TOTAL_RECORDS", "50"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "10"))

# ---------- 1. HELPERS ----------
def calculate_conversion_probability(ncd: int, credit_band: str) -> float:
    """Calculates probability with strict clipping and standard float conversion."""
    try:
        w = ncd * 35
        if credit_band == "Excellent": w += 45
        logit = max(-20, min(20, -3.0 + (0.02 * w)))
        # Force conversion to standard Python float immediately
        return float(1.0 / (1.0 + np.exp(-logit)))
    except:
        return 0.01

def generate_quote_row(i: int, customer_uuid: str) -> dict:
    age = random.randint(18, 75)
    credit_band = random.choice(['Excellent', 'Good', 'Fair', 'Poor'])
    ncd = random.randint(0, 15)
    make = random.choice(["Ford", "Tesla", "BMW", "Volkswagen", "Audi", "Nissan"])
    
    # CRITICAL: Every numeric value is wrapped in int() or float() 
    # to prevent NumPy JSON serialization errors in GitHub Actions.
    return {
        "uuid": str(uuid.uuid4()),
        "quote_id": f"q_{i:07d}",
        "customer_uuid": str(customer_uuid),
        "title": random.choice(["Mr", "Mrs", "Miss", "Ms", "Dr"]),
        "first_name": fake.first_name(),
        "last_name": fake.last_name(),
        "email_address": f"{fake.first_name().lower()}@testmail.com",
        "date_of_birth": (date.today() - timedelta(days=age*365)).isoformat(),
        "sex": random.choice(["Male", "Female"]),
        "nationality": "UK",
        "marital_status": random.choice(["Single", "Married", "Divorced", "Widowed"]),
        "employment_status": random.choice(["Employed", "Self-Employed", "Retired"]),
        "occupation": "Professional",
        "job_title": random.choice(["Manager", "Engineer", "Consultant", "Director"]),
        "home_owner": bool(random.choice([True, False])),
        "car_make": make,
        "car_model": "Model X",
        "abi_group": int(random.randint(1, 50)),
        "transmission": random.choice(["Manual", "Automatic"]),
        "estimated_annual_mileage": int(random.randint(4000, 12000)),
        "quoted_total_premium": float(round(random.uniform(400, 1200), 2)),
        "status": "Pending",
        "conversion_probability": calculate_conversion_probability(ncd, credit_band),
        "postcode_risk_band": int(random.randint(1, 10)),
        "number_of_ncd_years": int(ncd),
        "credit_score_band": credit_band,
        "cover_type": random.choice(["Comprehensive", "Third Party"]),
        "has_blackbox": bool(random.choice([True, False])),
        "driving_licence_type": "Full UK",
        "driving_licence_years": int(random.randint(1, 40)),
        "payment_frequency": random.choice(["Annual", "Monthly"]),
        "rejection_reason": None,
        "created_at": datetime.utcnow().isoformat()
    }

# ---------- 2. MAIN EXECUTION ----------
def main():
    print(f"🚀 Generating {TOTAL_RECORDS} records for GitHub Actions...")
    
    # 1. Generate Rows
    rows = [generate_quote_row(idx, str(uuid.uuid4())) for idx in range(1, TOTAL_RECORDS + 1)]
    
    # 2. Status Logic via Pandas
    df = pd.DataFrame(rows)
    df = df.sort_values("conversion_probability", ascending=False).reset_index(drop=True)
    
    n_accept = int(0.10 * len(df))
    df["status"] = "Rejected"
    df.loc[:n_accept-1, "status"] = "Accepted"
    df.loc[df["status"] == "Rejected", "rejection_reason"] = "Risk Model Outlier"

    # 3. THE "GITHUB ACTION" SPECIAL SANITIZATION
    # We convert the entire dataframe to standard Python types to kill NumPy errors
    records = []
    for record in df.to_dict(orient="records"):
        sanitized_record = {}
        for k, v in record.items():
            # If it's a null/NaN, force to None
            if pd.isna(v):
                sanitized_record[k] = None
            # If it's a NumPy int, force to Python int
            elif isinstance(v, (np.integer, int)):
                sanitized_record[k] = int(v)
            # If it's a NumPy float, force to Python float
            elif isinstance(v, (np.floating, float)):
                if math.isinf(v) or math.isnan(v):
                    sanitized_record[k] = None
                else:
                    sanitized_record[k] = float(v)
            else:
                sanitized_record[k] = v
        records.append(sanitized_record)

    # 4. PUSH TO SUPABASE
    for i in range(0, len(records), BATCH_SIZE):
        batch = records[i:i+BATCH_SIZE]
        try:
            # Final JSON validation before sending
            json_batch = json.dumps(batch)
            
            response = supabase.table("quotes").insert(batch).execute()
            if response.data:
                print(f"✅ Batch {i//BATCH_SIZE + 1} pushed successfully.")
        except Exception as e:
            print(f"❌ SERIALIZATION ERROR on Batch {i}: {e}")
            # Identify the specific column causing the issue
            for item in batch:
                for k, v in item.items():
                    try:
                        json.dumps(v)
                    except:
                        print(f"🚨 ILLEGAL VALUE in column '{k}': {v} (Type: {type(v)})")
            break

if __name__ == "__main__":
    main()
