name: Seed Quotes, Policies, and Claims

on:
  schedule:
    - cron: "*/10 6-18 * * *"   # every 10 minutes 06:00–18:59 UTC
  workflow_dispatch:

jobs:
  run-quotes:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install faker supabase python-dateutil

      - name: Run quote generator
        run: python generate_quotes.py

  run-policies:
    runs-on: ubuntu-latest
    needs: run-quotes      # ensure quotes run first
    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install supabase python-dateutil

      - name: Run policy generator
        run: python generate_policies.py

  run-claims:
    runs-on: ubuntu-latest
    needs: run-policies     # ensure policies exist before claims
    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install supabase

      - name: Run claim generator
        run: python generate_claims.py
