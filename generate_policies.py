def apply_mta(policy_uuid: str, new_premium: float, effective_date: date, reason: str):
    # 1) Fetch current policy
    resp = supabase.table("policies").select("*").eq("policy_uuid", policy_uuid).limit(1).execute()
    row = resp.data[0]
    old_premium = float(row["current_premium"])

    # 2) Update policy record
    supabase.table("policies").update(
        {"current_premium": new_premium, "updated_at": datetime.utcnow().isoformat()}
    ).eq("policy_uuid", policy_uuid).execute()

    # 3) Insert transaction row
    supabase.table("policy_transactions").insert(
        {
            "policy_uuid": policy_uuid,
            "transaction_type": "MTA",
            "transaction_effective_date": effective_date.isoformat(),
            "old_premium": old_premium,
            "new_premium": new_premium,
            "premium_delta": new_premium - old_premium,
            "change_reason": reason,
        }
    ).execute()
