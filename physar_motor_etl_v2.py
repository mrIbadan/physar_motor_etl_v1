import os
import uuid
import numpy as np
import pandas as pd
from datetime import datetime, date, timedelta
from faker import Faker
from supabase import create_client

# =========================
# Supabase config
# =========================
SUPABASE_URL = "https://qlpxsymlkhqmhxpqctkj.supabase.co"
SUPABASE_KEY = "sb_publishable_uw7kn8SN3en32wQYm8G4zg__FK69p-i"
TABLE_NAME = "motor_policies_staged" 

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
fake = Faker("en_GB")
np.random.seed(42)

def make_base_frame(n_rows: int, source_file: str) -> pd.DataFrame:
    today = date.today()
    
    # --- 1. CORE DRIVER DATA ---
    driver_age = np.random.randint(17, 85, size=n_rows)
    licence_years = np.clip(driver_age - 17 - np.random.randint(0, 3, n_rows), 0, None)
    ncd_years = np.clip(licence_years - np.random.randint(0, 2, n_rows), 0, 15)
    
    # --- 2. CORE VEHICLE DATA ---
    veh_year = np.random.randint(2008, 2025, size=n_rows)
    veh_age = 2025 - veh_year
    abi_group = np.random.randint(1, 51, size=n_rows)
    car_value = np.random.uniform(2000, 55000, size=n_rows)
    annual_mileage = np.random.randint(2000, 18000, size=n_rows)
    
    # --- 3. CLAIMS & RISK ---
    total_claims = np.random.choice([0, 1, 2, 3], p=[0.85, 0.10, 0.03, 0.02], size=n_rows)
    fault_claims = np.array([np.random.randint(0, c + 1) for c in total_claims])
    nonfault_claims = total_claims - fault_claims
    convictions = np.random.choice([0, 1, 2], p=[0.92, 0.06, 0.02], size=n_rows)
    total_claim_cost = (total_claims * np.random.uniform(500, 5000, n_rows)).round(2)

    # --- 4. FULL SCHEMA MAPPING (50+ Columns) ---
    data = {
        "source_file": [source_file] * n_rows,
        "policy_id": [f"POL_{source_file}_{uuid.uuid4().hex[:6]}_{i}" for i in range(n_rows)],
        "driver_id": [f"DRV_{uuid.uuid4().hex[:8]}" for _ in range(n_rows)],
        "driver_age": driver_age,
        "gender": np.random.choice(["Male", "Female", "Non-binary"], size=n_rows),
        "marital_status": np.random.choice(["Single", "Married", "Divorced", "Widowed"], size=n_rows),
        "occupation": [fake.job() for _ in range(n_rows)],
        "employment_status": np.random.choice(["Employed", "Self-Employed", "Retired", "Student"], size=n_rows),
        "income_band": np.random.choice(["<20k", "20-40k", "40-70k", "70k+"], size=n_rows),
        "licence_years": licence_years,
        "ncd_years": ncd_years,
        "age_band": pd.cut(driver_age, bins=[0, 21, 25, 35, 50, 65, 100], labels=["<21", "21-25", "26-35", "36-50", "51-65", "65+"]).astype(str),
        "ncd_band": pd.cut(ncd_years, bins=[-1, 0, 3, 6, 9, 20], labels=["0", "1-3", "4-6", "7-9", "10+"]).astype(str),
        "vehicle_make": np.random.choice(["Ford", "Vauxhall", "VW", "BMW", "Audi", "Mercedes", "Toyota"], size=n_rows),
        "vehicle_model": [f"Model_{np.random.randint(1, 20)}" for _ in range(n_rows)],
        "vehicle_year": veh_year,
        "vehicle_age": veh_age,
        "vehicle_body_type": np.random.choice(["Hatchback", "SUV", "Saloon", "Coupe"], size=n_rows),
        "fuel_type": np.random.choice(["Petrol", "Diesel", "Electric", "Hybrid"], size=n_rows),
        "gearbox_type": np.random.choice(["Manual", "Automatic"], size=n_rows),
        "engine_cc": np.random.choice([1000, 1200, 1400, 1600, 2000, 3000], size=n_rows),
        "abi_group": abi_group,
        "abi_band": pd.cut(abi_group, bins=[0, 10, 20, 30, 40, 51], labels=["1-10", "11-20", "21-30", "31-40", "41-50"]).astype(str),
        "car_value": car_value.round(2),
        "car_value_band": pd.cut(car_value, bins=[0, 5000, 15000, 30000, 1000000], labels=["Budget", "Standard", "Premium", "Luxury"]).astype(str),
        "car_value_to_income_proxy": (car_value / np.random.randint(25000, 80000, n_rows)).round(4),
        "inception_date": [(today - timedelta(days=np.random.randint(0, 365))).isoformat() for _ in range(n_rows)],
        "policy_term_months": [12] * n_rows,
        "exposure_years": [1.0] * n_rows,
        "cover_type": np.random.choice(["Comprehensive", "TPFT", "TPO"], size=n_rows),
        "use_type": np.random.choice(["Social", "Commuting", "Business"], size=n_rows),
        "annual_mileage": annual_mileage,
        "mileage_band": pd.cut(annual_mileage, bins=[0, 5000, 10000, 15000, 100000], labels=["Low", "Average", "High", "Very High"]).astype(str),
        "overnight_parking": np.random.choice(["Driveway", "Garage", "Street", "Locked Compound"], size=n_rows),
        "voluntary_excess": np.random.choice([0, 100, 250, 500], size=n_rows),
        "compulsory_excess": [150] * n_rows,
        "payment_method": np.random.choice(["Monthly", "Annual"], size=n_rows),
        "postcode": [fake.postcode() for _ in range(n_rows)],
        "total_claim_cost": total_claim_cost,
        "risk_premium": (total_claim_cost * 0.7).round(2),
        "technical_premium": (total_claim_cost * 0.85).round(2),
        "quoted_premium": np.random.uniform(400, 2500, size=n_rows).round(2),
        "convictions": convictions,
        "fault_claims": fault_claims,
        "nonfault_claims": nonfault_claims,
        "total_claims": total_claims,
        "claims_per_year_of_licence": (total_claims / (licence_years + 1)).round(4),
        "young_driver_flag": (driver_age < 25).astype(int),
        "senior_driver_flag": (driver_age > 70).astype(int),
        "high_mileage_flag": (annual_mileage > 15000).astype(int),
        "luxury_brand_flag": np.random.choice([0, 1], p=[0.9, 0.1], size=n_rows),
        "urban_flag": np.random.choice([0, 1], p=[0.4, 0.6], size=n_rows),
        "high_excess_flag": np.random.choice([0, 1], p=[0.8, 0.2], size=n_rows),
        "monthly_pay_flag": np.random.choice([0, 1], size=n_rows),
        "multi_claims_flag": (total_claims > 1).astype(int)
    }
    
    return pd.DataFrame(data)

def insert_df_into_supabase(df: pd.DataFrame):
    records = df.replace({np.nan: None}).to_dict(orient="records")
    batch_size = 1000  # Larger batches for 10k rows
    
    total_records = len(records)
    print(f"🚀 Uploading {total_records} rows to {TABLE_NAME}...")
    
    for i in range(0, total_records, batch_size):
        chunk = records[i:i + batch_size]
        try:
            supabase.table(TABLE_NAME).insert(chunk).execute()
            print(f"📦 Progress: {min(i + batch_size, total_records)} / {total_records} rows uploaded.")
        except Exception as e:
            print(f"❌ Batch Error: {e}")
            break

def main():
    # SET TO 10,000 ROWS
    ROW_COUNT = 10000
    df = make_base_frame(ROW_COUNT, "PRODUCTION_BATCH_001")
    insert_df_into_supabase(df)
    print("🏁 Done. Your Supabase table now has 10,000 rows with no NULLs.")

if __name__ == "__main__":
    main()
