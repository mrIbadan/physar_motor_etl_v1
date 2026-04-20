import os
import uuid
import random
import json
import math
from datetime import datetime, date, timedelta
from typing import Dict, Any, Tuple

import numpy as np
import pandas as pd
from faker import Faker
from supabase import create_client, Client

# ---------- 0. CONFIG ----------
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("SUPABASE_URL and SUPABASE_KEY secrets are missing!")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
fake = Faker("en_GB")

TOTAL_RECORDS = int(os.getenv("TOTAL_RECORDS", "50"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "10"))
TARGET_CONVERSION_RATE = 0.10

# ---------- 1. HELPERS ----------

def calculate_conversion_probability(ncd: int, credit_band: str) -> float:
    """Calculates probability with safety clipping to prevent Infinity."""
    try:
        w = ncd * 35
        if credit_band == "Excellent": w += 45
        logit = -3.0 + (0.02 * w)
        
        # HARD CLIP: Prevents the math from exploding into Infinity
        logit = max(-20, min(20, logit))
        
        prob = 1.0 / (1.0 + np.exp(-logit))
        return float(prob)
    except:
        return 0.01

def generate_quote_row(i: int) -> Dict[str, Any]:
    age = random.randint(18, 75)
    credit_band = random.choice(['Excellent', 'Good', 'Fair', 'Poor'])
    ncd = random.randint(0, 15)
    
    # Premium math
    base = 500.0
    premium = float(round(base * random.uniform(0.5, 3.0), 2))

    return {
        "uuid": str(uuid.uuid4()),
        "quote_id": f"q_{i:07d}",
        "first_name": fake.first_name(),
        "last_name": fake.last_name(),
        "email_address": f"{fake.first_name().lower()}@example.com",
        "date_of_birth": (date.today() - timedelta(days=age*365)).isoformat(),
        "car_make": random.choice(["Ford", "Tesla", "BMW", "Audi"]),
        "quoted_total_premium": premium,
        "status": "Pending",
        "conversion_probability": calculate_conversion_probability(ncd, credit_band),
        "created_at": datetime.utcnow().isoformat(),
        "number_of_ncd_years": ncd,
        "credit_score_band": credit_band,
        "abi_group": random.randint(1, 50),
        "transmission": random.choice(["Manual", "Automatic"]),
        "has_blackbox": random.choice([True, False])
    }

# ---------- 2. BATCH & ISOLATION LOGIC ----------

def main():
    print(f"🔍 Starting Generation: {TOTAL_RECORDS} records")
    
    # Generate data
    raw_rows = [generate_quote_row(i) for i in range(1, TOTAL_RECORDS + 1)]
    
    # Assign Statuses based on probability
    df = pd.DataFrame(raw_rows)
    df = df.sort_values("conversion_probability", ascending=False).reset_index(drop=True)
    n_accept = int(TARGET_CONVERSION_RATE * len(df))
    df["status"] = "Rejected"
    df.loc[:n_accept-1, "status"] = "Accepted"
    
    # Final Sanitization: Force everything to JSON-safe types
    # This replaces NaN/Inf with None (SQL NULL)
    df = df.replace([np.inf, -np.inf], np.nan)
    df = df.where(pd.notnull(df), None)
    
    records = df.to_dict(orient="records")

    # BATCH PUSH WITH DIAGNOSTICS
    for i in range(0, len(records), BATCH_SIZE):
        batch = records[i:i+BATCH_SIZE]
        
        # --- THE ISOLATION TEST ---
        # Before we push, we try to JSON-encode the batch locally.
        # This will tell us if the data is broken before it hits the network.
        try:
            json.dumps(batch) 
        except ValueError as e:
            print(f"💥 FATAL JSON MISMATCH in Batch starting at index {i}!")
            for row in batch:
                for k, v in row.items():
                    if isinstance(v, float) and (math.isinf(v) or math.isnan(v)):
                        print(f"🚨 ILLEGAL VALUE FOUND: Quote {row['quote_id']} | Column '{k}' | Value: {v}")
            break # Stop here so we can read the log

        # --- THE ACTUAL PUSH ---
        try:
            response = supabase.table("quotes").insert(batch).execute()
            if response.data:
                print(f"✅ Batch {i//BATCH_SIZE + 1} pushed successfully.")
        except Exception as e:
            print(f"❌ DATABASE ERROR on Batch {i}: {e}")
            break

if __name__ == "__main__":
    main()
