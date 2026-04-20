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

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
fake = Faker("en_GB")

# Volume control from GitHub Actions env or defaults
TOTAL_RECORDS = int(os.getenv("TOTAL_RECORDS", "50"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "10"))
TARGET_CONVERSION_RATE = 0.10  # 10%

# ---------- 1. CONSTANTS ----------
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
EMPLOYMENT_STATUS_OLD = ["Employed", "Self-Employed", "Student", "Retired", "Unemployed"]
VEHICLE_USAGE_OLD = ["Social, domestic & pleasure", "SDP + commuting", "Business use"]
PARKING_OPTIONS_OLD = ["Garage", "Driveway", "On street", "Car park"]
LICENCE_TYPES = ["Full UK", "Provisional", "EU", "International"]
PAYMENT_FREQUENCY = ["Annual", "Monthly"]
TRANSMISSION = ["Manual", "Automatic"]

EMAIL_DOMAINS = ["youmail.com", "randommail.com", "testmail.net", "demoemail.org"]
REJECTION_REASONS = ["User Cancelled / Did not accept", "User Declined due to UW rules"]

# ---------- 2. HELPERS ----------

def generate_realistic_occupation_job_title() -> Tuple[str, str, str]:
    industries = {
        "Technology": {"Software Engineer": ["Frontend Dev", "Backend Dev"], "Data Scientist": ["Analyst"]},
        "Healthcare": {"Doctor": ["GP", "Surgeon"], "Nurse": ["Staff Nurse"]},
        "Finance": {"Accountant": ["Tax Accountant"], "Banker": ["Investment Banker"]}
    }
    industry = random.choice(list(industries.keys()))
    occupation = random.choice(list(industries[industry].keys()))
    job_title = random.choice(industries[industry][occupation])
    return industry, occupation, job_title

def get_occupation_risk_class(occupation: str) -> int:
    low_risk = ['Accountant', 'Software Engineer', 'Doctor']
    if occupation in low_risk: return 1
    return 3

def calculate_conversion_probability(row: Dict[str, Any]) -> float:
    w = 0.0
    w += row.get("number_of_ncd_years", 0) * 35
    if row.get("credit_score_band") == "Excellent": w += 45
    
    logit = -3.0 + (0.02 * w)
    prob = 1.0 / (1.0 + np.exp(-logit))
    # FIX: Ensure result is a standard Python float, not a NumPy float
    return float(np.nan_to_num(prob, nan=0.01))

def get_next_quote_start() -> int:
    try:
        resp = supabase.table("quotes").select("quote_id").order("quote_id", desc=True).limit(1).execute()
        rows = resp.data or []
        if not rows: return 1
        return int(rows[0]["quote_id"].split("_")[1]) + 1
    except: return random.randint(1000, 9999)

# ---------- 3. ROW GENERATION ----------

def generate_quote_row(i: int, customer_uuid: str) -> Dict[str, Any]:
    age = random.randint(18, 75)
    make = random.choice(list(CAR_DATA.keys()))
    
    # Basic math for premium
    postcode_risk = random.randint(1, 10)
    base_premium = 500.0
    risk_multiplier = max(0.5, min(3.0, 1.0 + (postcode_risk - 5) * 0.05))
    total_premium = float(round(base_premium * risk_multiplier * random.uniform(0.9, 1.1), 2))

    base_row = {
        "number_of_ncd_years": random.randint(0, 9),
        "credit_score_band": random.choice(['Excellent', 'Good', 'Fair']),
        "age": age
    }
    
    conv_prob = calculate_conversion_probability(base_row)

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
        "status": "Pending", # Will be overwritten by batch logic
        "conversion_probability": conv_prob,
        "created_at": datetime.utcnow().isoformat(),
        "postcode_risk_band": postcode_risk
    }

# ---------- 4. BATCH LOGIC & CLEANING ----------

def generate_quotes_batch(total_records: int) -> pd.DataFrame:
    start_idx = get_next_quote_start()
    rows = [generate_quote_row(idx, str(uuid.uuid4())) for idx in range(start_idx, start_idx + total_records)]
    
    df = pd.DataFrame(rows)
    # Enforce global conversion rate
    df = df.sort_values("conversion_probability", ascending=False).reset_index(drop=True)
    n_accept = int(TARGET_CONVERSION_RATE * len(df))
    df["status"] = "Rejected"
    df.loc[:n_accept-1, "status"] = "Accepted"
    
    # CRITICAL FIX: Sanitize for JSON compliance (Removes Inf/NaN)
    df = df.replace([np.inf, -np.inf], np.nan).fillna(0)
    
    return df

def main():
    print(f"🚀 Starting generation for {TOTAL_RECORDS} records...")
    df = generate_quotes_batch(TOTAL_RECORDS)
    
    # Drop helper columns before sending to Supabase
    records = df.to_dict(orient="records")

    for i in range(0, len(records), BATCH_SIZE):
        batch = records[i:i+BATCH_SIZE]
        try:
            supabase.table("quotes").insert(batch).execute()
            print(f"✅ Successfully inserted batch {i} to {i+len(batch)-1}")
        except Exception as e:
            print(f"❌ JSON/Database Error on batch {i}: {e}")
            # Stop if we hit a fatal error
            break

if __name__ == "__main__":
    main()
