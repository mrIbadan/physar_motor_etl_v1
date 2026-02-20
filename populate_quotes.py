import os
import uuid
import random
from datetime import datetime, timedelta
from faker import Faker
from supabase import create_client, Client

# Initialize Faker with UK locale to generate valid postcodes and names
fake = Faker('en_GB')

# 1. AUTHENTICATION BLOCK
# Using Service Role Key to bypass Row Level Security (RLS) for administrative seeding
URL = os.environ.get("https://jxonjddldsakvxqklaqd.supabase.co") # URL
KEY = os.environ.get("sb_publishable_ZESybNf1JTKEusRTqDnoaQ_SLVec74A") ' Key

if not URL or not KEY:
    raise ValueError("Environment variables SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY not set.")

supabase: Client = create_client(URL, KEY)

# 2. DOMAIN LOGIC MAPPINGS
# These ensure the synthetic data reflects real insurance book characteristics
CAR_DATA = {
    'Ford': ['Fiesta', 'Focus', 'Puma'],
    'Tesla': ['Model 3', 'Model Y'],
    'BMW': ['3 Series', '5 Series', 'X5'],
    'Volkswagen': ['Golf', 'Polo', 'ID.3'],
    'Audi': ['A3', 'A4', 'Q5']
}

def generate_quote():
    """Constructs a single quote record mapping to the Supabase SQL schema."""
    make = random.choice(list(CAR_DATA.keys()))
    model = random.choice(CAR_DATA[make])
    
    # We use isoformat() for dates to ensure Postgres DATE/TIMESTAMPTZ compatibility
    return {
        "uuid": str(uuid.uuid4()),
        "customer": str(uuid.uuid4()),
        
        # Profile Data
        "first_name": fake.first_name(),
        "last_name": fake.last_name(),
        "email_address": fake.ascii_free_email(),
        "date_of_birth": fake.date_of_birth(minimum_age=18, maximum_age=75).isoformat(),
        "postcode": fake.postcode(),
        "employment_status": random.choice(['Employed', 'Self-Employed', 'Student', 'Retired']),
        "occupation": fake.job(),
        
        # Risk Factors: Weighted to reflect a real-world imbalanced distribution
        # Most customers have 0 CCJs; only a small percentage have 1 or 2.
        "number_of_ccjs": random.choices([0, 1, 2], weights=[92, 6, 2])[0],
        "number_of_past_claims": random.choices([0, 1, 2, 3], weights=[75, 15, 7, 3])[0],

        # Rating Input & Output
        "car_make": make,
        "car_model": model,
        "quoted_total_premium": float(round(random.uniform(350.00, 2500.00), 2)),
        "status": "Quoted",
        "created_at": datetime.now().isoformat()
    }

def main(total_records=1000, batch_size=500):
    """Executes the ingestion with batching to prevent API timeouts."""
    print(f"Generating {total_records} records...")
    data = [generate_quote() for _ in range(total_records)]
    
    for i in range(0, len(data), batch_size):
        batch = data[i : i + batch_size]
        try:
            # .insert() handles the list of dictionaries as a bulk operation
            supabase.table("quotes").insert(batch).execute()
            print(f"✅ Successfully ingested batch: {i} to {i + len(batch)}")
        except Exception as e:
            print(f"❌ Batch failed at index {i}: {e}")

if __name__ == "__main__":
    main()
