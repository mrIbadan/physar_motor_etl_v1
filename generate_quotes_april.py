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

SUPABASE_URL = os.getenv("https://nykdyzhihohynrujxzgi.supabase.co")
SUPABASE_KEY = os.getenv("sb_publishable_rXmxD30iR2zcNS7HfDQtYQ_O8tuChCa")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("SUPABASE_URL and SUPABASE_KEY must be set in the environment")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
fake = Faker("en_GB")

TOTAL_RECORDS = int(os.getenv("TOTAL_RECORDS", "10"))
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

EMAIL_DOMAINS = [
    "youmail.com",
    "randommail.com",
    "testmail.net",
    "demoemail.org",
    "maildemo.io",
    "sampleinbox.co",
    "fakedomain.test",
]

REJECTION_REASONS = [
    "User Cancelled / Did not accept",
    "User Declined due to UW rules",
]

rng = np.random.default_rng(42)
random.seed(42)

# ---------- 2. OCCUPATION HELPERS ----------

OCCUPATION_MAPPINGS = {
    "Finance": {
        "Accountant": ["Financial Accountant", "Management Accountant", "Tax Accountant"],
        "Financial Analyst": ["Investment Analyst", "Risk Analyst", "Credit Analyst"],
        "Banker": ["Investment Banker", "Private Banker", "Corporate Banker"],
        "Insurance Broker": ["Commercial Broker", "Personal Lines Broker"],
        "Financial Advisor": ["Wealth Manager", "Pension Advisor", "Investment Advisor"],
    },
    "Healthcare": {
        "Doctor": ["GP", "Surgeon", "Pediatrician", "Cardiologist"],
        "Nurse": ["Staff Nurse", "Senior Nurse", "Practice Nurse", "Theatre Nurse"],
        "Dentist": ["General Dentist", "Orthodontist", "Oral Surgeon"],
        "Pharmacist": ["Community Pharmacist", "Hospital Pharmacist"],
        "Physiotherapist": ["Sports Physio", "Paediatric Physio"],
    },
    "Retail": {
        "Store Manager": ["Retail Manager", "Department Manager", "Store Supervisor"],
        "Sales Assistant": ["Sales Associate", "Customer Assistant", "Shop Assistant"],
        "Merchandiser": ["Visual Merchandiser", "Product Merchandiser"],
        "Buyer": ["Assistant Buyer", "Category Buyer", "Senior Buyer"],
        "Cashier": ["Checkout Operator", "Customer Service Cashier"],
    },
    "Construction": {
        "Electrician": ["Domestic Electrician", "Industrial Electrician"],
        "Plumber": ["Residential Plumber", "Commercial Plumber", "Gas Engineer"],
        "Carpenter": ["Joinery Specialist", "Kitchen Fitter"],
        "Builder": ["General Builder", "Site Foreman", "Construction Manager"],
        "Architect": ["Residential Architect", "Commercial Architect"],
    },
    "Education": {
        "Teacher": ["Primary Teacher", "Secondary Teacher", "Supply Teacher"],
        "Lecturer": ["University Lecturer", "FE College Lecturer"],
        "Headteacher": ["Primary Head", "Secondary Head", "Academy Head"],
        "Teaching Assistant": ["Learning Support", "SEN TA", "Higher Level TA"],
        "Librarian": ["School Librarian", "Public Librarian"],
    },
    "Technology": {
        "Software Engineer": ["Frontend Dev", "Backend Dev", "Full Stack Dev"],
        "Data Scientist": ["ML Engineer", "Data Analyst", "BI Developer"],
        "IT Manager": ["Infrastructure Manager", "Cloud Manager"],
        "Product Manager": ["Technical PM", "Agile PM", "Digital PM"],
        "DevOps Engineer": ["Cloud Engineer", "Platform Engineer"],
    },
    "Manufacturing": {
        "Production Manager": ["Factory Manager", "Shift Manager", "Operations Manager"],
        "Quality Control": ["QA Inspector", "QC Technician", "Compliance Officer"],
        "Maintenance Engineer": ["Mechanical Engineer", "Electrical Engineer"],
        "Machine Operator": ["CNC Operator", "Press Operator", "Assembly Line"],
        "Logistics Coordinator": ["Supply Chain Coordinator", "Warehouse Manager"],
    },
    "Hospitality": {
        "Hotel Manager": ["General Manager", "Operations Manager", "Front Office Manager"],
        "Chef": ["Head Chef", "Sous Chef", "Pastry Chef", "Line Cook"],
        "Bartender": ["Mixologist", "Bar Manager", "Cocktail Specialist"],
        "Restaurant Manager": ["F&B Manager", "Venue Manager"],
        "Event Coordinator": ["Wedding Coordinator", "Conference Planner"],
    },
    "Transport": {
        "HGV Driver": ["Class 1 Driver", "Class 2 Driver", "Articulated Driver"],
        "Van Driver": ["Delivery Driver", "Courier", "Multi-drop Driver"],
        "Taxi Driver": ["Private Hire", "Black Cab", "Airport Transfer"],
        "Logistics Manager": ["Fleet Manager", "Distribution Manager"],
        "Courier": ["Same Day Courier", "Express Courier"],
    },
    "Public Sector": {
        "Police Officer": ["Constable", "Detective", "Traffic Officer"],
        "Firefighter": ["Watch Manager", "Crew Manager", "Fire Safety Officer"],
        "Civil Servant": ["Policy Advisor", "Case Worker", "Administrative Officer"],
        "Social Worker": ["Children's Social Worker", "Adult Social Worker"],
        "Local Government": ["Council Officer", "Planning Officer", "Housing Officer"],
    },
}

def generate_realistic_occupation_job_title() -> Tuple[str, str, str]:
    industry = random.choice(list(OCCUPATION_MAPPINGS.keys()))
    occupation = random.choice(list(OCCUPATION_MAPPINGS[industry].keys()))
    job_title = random.choice(OCCUPATION_MAPPINGS[industry][occupation])
    return industry, occupation, job_title

def get_occupation_risk_class(occupation: str) -> int:
    high_risk = ['HGV Driver', 'Taxi Driver', 'Courier', 'Builder', 'Electrician', 'Chef', 'Bartender']
    medium_high = ['Plumber', 'Carpenter', 'Police Officer', 'Firefighter', 'Machine Operator']
    medium = ['Sales Assistant', 'Cashier', 'Van Driver', 'Restaurant Manager']
    low_medium = ['Store Manager', 'Teacher', 'Nurse', 'Pharmacist', 'Production Manager']
    low_risk = ['Accountant', 'Software Engineer', 'Doctor', 'Lecturer', 'Financial Advisor', 'Data Scientist']
    if occupation in high_risk:
        return 5
    if occupation in medium_high:
        return 4
    if occupation in medium:
        return 3
    if occupation in low_medium:
        return 2
    if occupation in low_risk:
        return 1
    return 3

# ---------- 3. Conversion helper ----------

def calculate_conversion_probability(row: Dict[str, Any]) -> float:
    w = 0.0
    # NCD
    ncd = row.get("number_of_ncd_years", 0)
    w += ncd * 35
    if row.get("number_of_past_claims", 0) == 0 and row.get("years_claim_free", 0) >= 5:
        w += 50
    # Credit
    credit_band = row.get("credit_score_band", "Fair")
    if credit_band == "Excellent":
        w += 45
    elif credit_band in ["Poor", "Very Poor"]:
        w -= 35
    # Age
    age = row.get("age", 40)
    if 35 <= age <= 50:
        w += 40
    elif age < 25:
        w -= 60
    # Mileage
    mileage = row.get("estimated_annual_mileage", 12000)
    if mileage < 8000:
        w += 25
    elif mileage > 20000:
        w -= 20
    # Parking
    if row.get("parking") == "Garage":
        w += 30
    # Postcode risk
    if row.get("postcode_risk_band", 5) >= 8:
        w -= 30
    # Occupation risk
    occ_risk = row.get("occupation_risk_class", 3)
    if occ_risk == 1:
        w += 25
    # Telematics / security / homeowner / multi-car
    if row.get("has_blackbox", False):
        w += 15
    if row.get("has_dashcam", False):
        w += 10
    if row.get("security_devices", "None") != "None":
        w += 15
    if row.get("home_owner", False):
        w += 20
    if row.get("number_of_cars_household", 1) > 1:
        w += 20

    intercept = -3.0
    scale = 0.02
    logit = intercept + scale * w
    prob = 1.0 / (1.0 + np.exp(-logit))
    return float(np.clip(prob, 0.0001, 0.8))

# ---------- 4. Helpers ----------

def get_next_quote_start() -> int:
    try:
        resp = (
            supabase.table("quotes")
            .select("quote_id")
            .order("quote_id", desc=True)
            .limit(1)
            .execute()
        )
        rows = getattr(resp, "data", []) or []
        if not rows:
            return 1
        last_id = rows[0]["quote_id"]
        return int(last_id.split("_")[1]) + 1
    except Exception:
        return 1

def random_start_date() -> str:
    offset = random.randint(1, 30)
    return (date.today() + timedelta(days=offset)).isoformat()

def build_random_email() -> str:
    first = fake.first_name().lower()
    last = fake.last_name().lower()
    num = random.randint(1, 9999)
    sep = random.choice([".", "_", ""])
    domain = random.choice(EMAIL_DOMAINS)
    return f"{first}{sep}{last}{num}@{domain}"

# ---------- 5. One quote row (same user can appear multiple times) ----------

def generate_quote_row(i: int, customer_uuid: str) -> Dict[str, Any]:
    age = random.randint(18, 75)
    dob = fake.date_of_birth(minimum_age=18, maximum_age=75)
    sex = random.choice(SEX_OPTIONS)
    marital_status = random.choice(MARITAL_STATUS)
    nationality = random.choice(NATIONALITIES)

    industry, occupation, job_title = generate_realistic_occupation_job_title()
    occupation_risk_class = get_occupation_risk_class(occupation)
    employment_status = random.choice(EMPLOYMENT_STATUS_OLD)

    make = random.choice(list(CAR_DATA.keys()))
    model = random.choice(CAR_DATA[make])
    abi_group = random.randint(1, 50)
    transmission = random.choice(TRANSMISSION)
    vehicle_usage = random.choice(VEHICLE_USAGE_OLD)
    parking = random.choice(PARKING_OPTIONS_OLD)

    driving_licence_years = random.randint(1, max(1, age - 17))
    number_of_ncd_years = random.choice(list(range(10)))
    number_of_past_claims = random.choices([0, 1, 2, 3], weights=[75, 15, 7, 3])[0]
    years_claim_free = 0 if number_of_past_claims > 0 else random.randint(
        0, min(10, driving_licence_years)
    )
    number_of_ccjs = random.choices([0, 1, 2], weights=[92, 6, 2])[0]

    credit_band = random.choices(
        ['Excellent', 'Good', 'Fair', 'Poor', 'Very Poor'],
        weights=[0.15, 0.35, 0.30, 0.15, 0.05]
    )[0]
    credit_score_value = {
        'Excellent': random.randint(720, 850),
        'Good': random.randint(670, 719),
        'Fair': random.randint(580, 669),
        'Poor': random.randint(300, 579),
        'Very Poor': random.randint(300, 450)
    }[credit_band]

    has_dashcam = random.random() < 0.30
    has_blackbox = random.random() < 0.20
    security_devices = random.choice(['None', 'Alarm', 'Immobilizer', 'Alarm & Immobilizer', 'Tracker'])
    home_owner = random.random() < 0.60
    number_of_cars_household = random.randint(1, 3)
    postcode_risk_band = random.choices(
        list(range(1, 11)),
        weights=[0.05, 0.08, 0.10, 0.12, 0.15, 0.15, 0.12, 0.10, 0.08, 0.05]
    )[0]

    cover_type = random.choice(COVER_TYPES)
    start_date = random_start_date()
    estimated_annual_mileage = random.randint(3000, 20000)
    payment_frequency = random.choice(PAYMENT_FREQUENCY)
    personal_injury_cover = random.choice([True, False])
    breakdown_cover = random.choice([True, False])
    courtesy_car = random.choice([True, False])

    base_row = {
        "number_of_ncd_years": number_of_ncd_years,
        "number_of_past_claims": number_of_past_claims,
        "years_claim_free": years_claim_free,
        "credit_score_band": credit_band,
        "age": age,
        "estimated_annual_mileage": estimated_annual_mileage,
        "parking": parking,
        "postcode_risk_band": postcode_risk_band,
        "occupation_risk_class": occupation_risk_class,
        "has_blackbox": has_blackbox,
        "has_dashcam": has_dashcam,
        "security_devices": security_devices,
        "home_owner": home_owner,
        "number_of_cars_household": number_of_cars_household,
    }
    conv_prob = calculate_conversion_probability(base_row)
    converted = random.random() < conv_prob

    # simple risk-based premium
    base_premium = 500.0
    risk_multiplier = 1.0
    risk_multiplier += (postcode_risk_band - 5) * 0.05
    risk_multiplier += number_of_past_claims * 0.15
    risk_multiplier -= number_of_ncd_years * 0.02
    risk_multiplier = max(0.5, min(3.0, risk_multiplier))
    quoted_total_premium = float(round(base_premium * risk_multiplier * random.uniform(0.9, 1.1), 2))

    if converted:
        status = "Accepted"
        rejection_reason = None
    else:
        status = "Rejected"
        rejection_reason = random.choice(REJECTION_REASONS)

    return {
        "uuid": str(uuid.uuid4()),
        "quote_id": f"q_{i:07d}",
        "customer_uuid": customer_uuid,
        "title": random.choice(["Mr", "Ms", "Mrs", "Dr"]),
        "first_name": fake.first_name(),
        "last_name": fake.last_name(),
        "email_address": build_random_email(),
        "date_of_birth": dob.isoformat(),
        "sex": sex,
        "nationality": nationality,
        "marital_status": marital_status,
        "employment_status": employment_status,
        "job_title": job_title,
        "occupation": occupation,
        "car_make": make,
        "car_model": model,
        "abi_group": abi_group,
        "transmission": transmission,
        "vehicle_usage": vehicle_usage,
        "parking": parking,
        "driving_licence_type": random.choice(LICENCE_TYPES),
        "driving_licence_years": driving_licence_years,
        "licence_issue_country": "UK",
        "number_of_ncd_years": number_of_ncd_years,
        "number_of_past_claims": number_of_past_claims,
        "number_of_ccjs": number_of_ccjs,
        "medical_conditions": random.choice(
            [None, "None", "Diabetes", "Heart condition", "Epilepsy"]
        ),
        "cover_type": cover_type,
        "start_date": start_date,
        "estimated_annual_mileage": estimated_annual_mileage,
        "payment_frequency": payment_frequency,
        "personal_injury_cover": personal_injury_cover,
        "breakdown_cover": breakdown_cover,
        "courtesy_car": courtesy_car,
        "quoted_total_premium": quoted_total_premium,
        "status": status,
        "rejection_reason": rejection_reason,
        "created_at": datetime.utcnow().isoformat(),
        "credit_score": credit_score_value,
        "occupation_risk_class": occupation_risk_class,
        "credit_score_band": credit_band,
        "home_owner": home_owner,
        "postcode_risk_band": postcode_risk_band,
        "has_blackbox": has_blackbox,
        "has_dashcam": has_dashcam,
        "security_devices": security_devices,
        "conversion_probability": conv_prob,
    }

# ---------- 6. Batch generate & enforce global conversion rate ----------

def generate_quotes_batch(total_records: int) -> pd.DataFrame:
    start_idx = get_next_quote_start()
    rows = []
    for idx in range(start_idx, start_idx + total_records):
        # simulate same customer shopping around: reuse UUID sometimes
        if random.random() < 0.2 and rows:
            customer_uuid = random.choice(rows)["customer_uuid"]
        else:
            customer_uuid = str(uuid.uuid4())
        row = generate_quote_row(idx, customer_uuid)
        rows.append(row)

    df = pd.DataFrame(rows)
    df["raw_prob"] = df["conversion_probability"]
    df = df.sort_values("raw_prob", ascending=False).reset_index(drop=True)

    n_accept = int(TARGET_CONVERSION_RATE * len(df))
    df["status"] = "Rejected"
    df.loc[:n_accept-1, "status"] = "Accepted"
    df["rejection_reason"] = df["rejection_reason"].where(df["status"] == "Rejected", None)
    print(f"Enforced Accepted rate: {(df['status'] == 'Accepted').mean():.2%} on {len(df)} rows")
    return df

# ---------- 7. Main for GitHub Actions ----------

def main():
    df = generate_quotes_batch(TOTAL_RECORDS)
    records = df.drop(columns=["raw_prob"]).to_dict(orient="records")

    # Insert in batches to avoid size limits
    for i in range(0, len(records), BATCH_SIZE):
        batch = records[i:i+BATCH_SIZE]
        try:
            supabase.table("quotes").insert(batch).execute()
            print(f"Inserted batch {i}–{i+len(batch)-1}")
        except Exception as e:
            print(f"Insert error on batch {i}: {e}")
            break

if __name__ == "__main__":
    main()
