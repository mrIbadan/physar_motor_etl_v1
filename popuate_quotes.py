import os
import uuid
import random
from datetime import datetime, timedelta
from faker import Faker
from supabase import create_client, Client

# 1. Initialize Faker (UK Locale) and Supabase
fake = Faker('en_GB')

# Ensure these are set in your GitHub Secrets
URL = os.environ.get("SUPABASE_URL")
KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY") 

if not URL or not KEY:
    raise ValueError("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY environment variables.")

supabase: Client = create_client(URL, KEY)

def generate_random_quotes(count=1000):
    quotes = []
    
    # Selection lists for realistic data distribution
    titles = ['Mr', 'Mrs', 'Miss', 'Ms', 'Dr']
    employment_statuses = ['Employed', 'Self-Employed', 'Student', 'Retired', 'Unemployed']
    licence_types = ['Full UK', 'EU', 'Provisional']
    car_makes = {
        'Ford': ['Fiesta', 'Focus', 'Puma'],
        'Tesla': ['Model 3', 'Model Y'],
        'BMW': ['3 Series', '5 Series', 'X5'],
        'Volkswagen': ['Golf', 'Polo', 'ID.3'],
        'Audi': ['A3', 'A4', 'Q5'],
        'Toyota': ['Yaris', 'Corolla', 'RAV4']
    }
    transmission_types = ['Manual', 'Automatic']
    usages = ['Social, Domestic & Pleasure', 'Commuting', 'Business Use']
    parkings = ['Driveway', 'Garage', 'On Street', 'Private Car Park']
    cover_types = ['Comprehensive', 'Third Party, Fire & Theft', 'Third Party Only']
    payment_frequencies = ['Monthly', 'Annual']

    print(f"--- Starting generation of {count} insurance quotes ---")

    for i in range(count):
        make = random.choice(list(car_makes.keys()))
        model = random.choice(car_makes[make])
        
        # Create a single quote record mapping exactly to your SQL schema
        quote = {
            "uuid": str(uuid.uuid4()),
            "customer": str(uuid.uuid4()),
            
            # Customer Profile
            "title": random.choice(titles),
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "date_of_birth": fake.date_of_birth(minimum_age=18, maximum_age=75).isoformat(),
            "email_address": fake.ascii_free_email(),
            "sex": random.choice([True, False]),
            "marital_status": random.choice(['Single', 'Married', 'Divorced', 'Widowed']),
            "nationality": "British",
            "employment_status": random.choice(employment_statuses),
            "occupation": fake.job(),
            "job_title": fake.job(),
            "medical_conditions": random.choice([None, "None", "Diabetes", "Vision Impairment"]),
            "number_of_ccjs": random.choices([0, 1, 2], weights=[90, 8, 2])[0],

            # Licence Details
            "driving_licence_type": random.choice(licence_types),
            "driving_licence_years": random.randint(0, 40),
            "driving_licence_issue_country": "United Kingdom",

            # Vehicle Details
            "car_make": make,
            "car_model": model,
            "transmission": random.choice(transmission_types),
            "vehicle_usage": random.choice(usages),
            "parking": random.choice(parkings),
            "estimated_annual_mileage": random.randint(2000, 15000),
            "number_of_cars_in_household": random.randint(1, 3),

            # Rating Factors
            "postcode": fake.postcode(),
            "number_of_ncd_years": random.randint(0, 15),
            "number_of_past_claims": random.choices([0, 1, 2, 3], weights=[70, 20, 7, 3])[0],

            # Cover & Add-ons
            "cover_type": random.choice(cover_types),
            "start_date": (datetime.now() + timedelta(days=random.randint(1, 28))).date().isoformat(),
            "payment_frequency": random.choice(payment_frequencies),
            "personal_injury_cover": random.choice([True, False]),
            "breakdown_cover": random.choice([True, False]),
            "courtesy_car": random.choice([True, False]),
            
            # Financial Output
            "quoted_total_premium": float(round(random.uniform(300.00, 2500.00), 2)),
            "status": "Quoted"
        }
        quotes.append(quote)

    # 2. Batch Insert (500 per request is optimal for Supabase)
    batch_size = 500
    for j in range(0, len(quotes), batch_size):
        current_batch = quotes[j:j + batch_size]
        try:
            response = supabase.table("quotes").insert(current_batch).execute()
            print(f"Successfully inserted rows {j} to {j + len(current_batch)}")
        except Exception as e:
            print(f"Error inserting batch: {e}")

if __name__ == "__main__":
    generate_random_quotes(1000)
