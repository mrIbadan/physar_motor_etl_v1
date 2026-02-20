# physar_motor_etl_v1
Process to save data daily via Github Actions to Supabase

# Physar Rating Engine Seeder

This repository contains the automated data pipeline for populating the Physar Pricing Model with synthetic insurance data.

## Data Strategy
To support senior-level modeling for **Price Adequacy** and **Risk Segmentation**, the seeder generates 1,000 records with the following characteristics:
- **Imbalanced Risk Factors:** Uses weighted distributions for Claims and CCJs to reflect real-world credit/risk volatility.
- **Relational Consistency:** Ensures Car Models are mapped correctly to Manufacturers.
- **Production MLOps:** Integrated with GitHub Actions for automated, repeatable data seeding.

## Tech Stack
- **Database:** Supabase (Postgres)
- **Language:** Python 3.10
- **Libraries:** `supabase-py`, `faker`
- **CI/CD:** GitHub Actions

## Setup
1. Clone the repo and run `pip install -r requirements.txt`.
2. Configure GitHub Secrets for `SUPABASE_URL` and `SUPABASE_SERVICE_ROLE_KEY`.
3. Trigger the workflow via the **Actions** tab in GitHub.
