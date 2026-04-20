import os
import uuid
import random
from datetime import datetime, date, timedelta
from typing import Dict, Any, Tuple

import numpy as np
import pandas as pd
from faker import Faker
from supabase import create_client, Client

# ---------- 0. CONFIG FROM ENV ----------
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("SUPABASE_URL and SUPABASE_KEY must be set in GitHub Secrets")

# Initialize Supabase Client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
fake = Faker("en_GB")

# Control constants
TOTAL_RECORDS = int(os.getenv("TOTAL_RECORDS", "50"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "10"))
TARGET_CONVERSION_RATE = 0.10  # 10%

# ---------- 1. DATA CONSTANTS ----------
CAR_DATA = {
    "Ford": ["Fiesta", "Focus", "Puma", "Kuga"],
    "Tesla": ["Model 3", "Model Y", "Model S", "Model X"],
    "BMW": ["1 Series", "3 Series", "5 Series", "X1", "X3", "X5"],
    "Volkswagen": ["Golf", "Polo", "ID.3", "Passat", "Tiguan"],
    "Audi": ["A1", "A3", "A4", "A5", "Q3", "Q5"],
    "Mercedes-Benz": ["A-Class", "C-Class", "E-Class", "GLA", "GLC"],
}

EMAIL_DOMAINS = ["youmail.com", "randommail.com", "testmail.net", "demoemail.org"]
REJECTION_REASONS = ["User Cancelled / Did not accept", "User Declined due to UW rules"]

# ---------- 2. HELPERS ----------

def generate_realistic_occupation() -> Tuple[str, str, str]:
    industries = {
        "Technology": {"Software Engineer": ["Frontend Dev", "Backend Dev"], "Data Scientist": ["Analyst"]},
        "Healthcare": {"Doctor": ["GP", "Surgeon"], "Nurse": ["Staff Nurse"]},
        "Finance": {"Accountant": ["Tax Accountant"], "Banker": ["Investment Banker"]}
    }
    industry = random.choice(list(industries.keys()))
    occupation = random.choice(list(industries[industry].keys()))
    job_title = random.choice(industries[industry][occupation])
    return industry, occupation, job_title

def calculate_conversion_probability(ncd: int, credit_band: str) -> float:
    w = ncd * 35
    if credit_band == "Excellent": w += 45
    logit = -3.0 + (0.02 * w)
    prob = 1.0 / (1.0 + np.exp(-logit))
    # Forces standard Python float to prevent JSON compliance errors
    return float(np.nan_to_num(prob, nan=0.01))

def get_next_quote_start() -> int:
    try:
        resp = supabase.table("quotes").select("quote_id").order("quote_id", desc=True).limit(1).execute()
        rows = resp.data or []
        if not rows: return 1
        return int(rows[0]["quote_id"].split("_")[1]) + 1
    except: return random.randint(1000, 9999)

# ---------- 3. ROW GENERATION (SYNCED WITH SQL SCHEMA) ----------

def generate_quote_row(i: int, customer_uuid: str) -> Dict[str, Any]:
    age = random.randint(18, 75)
    make = random.choice(list(CAR_DATA.keys()))
    industry, occupation, job_title = generate_realistic_occupation()
    credit_band = random.choice(['Excellent', 'Good', 'Fair'])
    ncd = random.randint(0, 9)
    
    # Premium Logic
    postcode_risk = random.randint(1, 10)
    base_premium = 500.0
    risk_multiplier = max(0.5, min(3.0, 1.0 + (postcode_risk - 5) * 0.05))
    total_premium = float(round(base_premium * risk_multiplier * random.uniform(0.9, 1.1), 2))

    # Calculate Probability
    conv_prob = calculate_conversion_probability(ncd, credit_band)

    # Dictionary keys MUST match Supabase Column Names exactly
    return {
        "uuid": str(uuid.uuid4()),
        "quote_id": f"q_{i:07d}",
        "customer_uuid": str(customer_uuid),
        "first_name": fake.first_name(),
        "last_name": fake.last_name(),
        "email_address": f"{fake.first_name().lower()}@{random.choice(EMAIL_DOMAINS)}",
        "date_of_birth": (date.today() - timedelta(days=age*365)).isoformat(),
        "car_make": make,
        "car_model": random.choice(CAR_DATA[make]),
        "quoted_total_premium": total_premium,
        "status": "Pending",  # Overwritten in batch logic
        "conversion_probability": conv_prob,
        "created_at": datetime.utcnow().isoformat(),
        "postcode_risk_band": postcode_risk,
        "occupation": occupation,
        "job_title": job_title,
        "abi_group": random.randint(1, 50),
        "transmission": random.choice(["Manual", "Automatic"]),
        "employment_status": random.choice(["Employed", "Self-Employed"]),
        "marital_status": random.choice(["Single", "Married", "Divorced"]),
        "number_of_ncd_years": ncd,
        "credit_score_band": credit_band,
        "estimated_annual_mileage": random.randint(4000, 15000),
        "cover_type": random.choice(["Comprehensive", "Third Party"]),
        "has_blackbox": random.choice([True, False]),
        "home_owner": random.choice([True, False]),
        "sex": random.choice(["Male", "Female"]),
        "nationality": "UK",
        "driving_licence_type": "Full UK",
        "driving_licence_years": random.randint(1, 40),
        "payment_frequency": random.choice(["Annual", "Monthly"])
    }

# ---------- 4. BATCH LOGIC & CLEANING ----------

def generate_quotes_batch(total_records: int) -> pd.DataFrame:
    start_idx = get_next_quote_start()
    rows = [generate_quote_row(idx, str(uuid.uuid4())) for idx in range(start_idx, start_idx + total_records)]
    
    df = pd.DataFrame(rows)
    
    # Sort and Apply Conversion Logic
    df = df.sort_values("conversion_probability", ascending=False).reset_index(drop=True)
    n_accept = int(TARGET_CONVERSION_RATE * len(df))
    df["status"] = "Rejected"
    df.loc[:n_accept-1, "status"] = "Accepted"
    
    # Fill rejection reasons for the rejected ones
    df.loc[df["status"] == "Rejected", "rejection_reason"] = [random.choice(REJECTION_REASONS) for _ in range(len(df) - n_accept)]
    
    # CRITICAL CLEANING: Ensure no NaN/Inf for JSON compliance
    # We replace math errors with 0 or None so Supabase doesn't reject the block
    df = df.replace([np.inf, -np.inf], np.nan).where(pd.notnull(df), None)
    
    return df

# ---------- 5. THE PUSH LOGIC ----------

def main():
    print(f"🚀 Initializing push to Supabase: {TOTAL_RECORDS} records.")
    df = generate_quotes_batch(TOTAL_RECORDS)
    
    # Convert dataframe to list of dictionaries
    records = df.to_dict(orient="records")

    for i in range(0, len(records), BATCH_SIZE):
        batch = records[i:i+BATCH_SIZE]
        try:
            # ------------------------------------------------------------
            # TARGET TABLE: "quotes"
            # TARGET SCHEMA: "public" (Default)
            # ------------------------------------------------------------
            response = supabase.table("quotes").insert(batch).execute()
            # ------------------------------------------------------------
            
            if response.data:
                print(f"✅ BATCH SUCCESS: Inserted rows {i} to {i+len(batch)-1}")
            else:
                print(f"⚠️ BATCH EMPTY: Supabase received the batch but saved 0 rows. Check RLS.")
                
        except Exception as e:
            print(f"❌ CRITICAL ERROR on batch {i}: {e}")
            # If the first batch fails, stop the script to prevent 50 separate error logs
            break

if __name__ == "__main__":
    main()
