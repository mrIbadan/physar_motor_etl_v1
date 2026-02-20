import os
import uuid
import random
from datetime import datetime, timedelta
from faker import Faker
from supabase import create_client, Client

# Initialize Faker with UK locale for realistic UK postcodes/names
fake = Faker('en_GB')

# Configuration from Environment Variables (GitHub Secrets)
URL = os.environ.get("SUPABASE_URL")
KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY") 

if not URL or not KEY:
    raise ValueError("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY.")

supabase: Client = create_client(URL, KEY)
