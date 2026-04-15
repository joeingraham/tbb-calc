"""
TBB Calculator — Data Foundation
Pulls baseline actuals from Kusto and saves as JSON for the dashboard.
"""
import subprocess, json, datetime
import pandas as pd
import numpy as np

CLUSTER = "https://gh-analytics.eastus.kusto.windows.net"

def get_token():
    r = subprocess.run(
        ["az", "account", "get-access-token", "--resource", CLUSTER,
         "--query", "accessToken", "-o", "tsv"],
        capture_output=True, text=True, check=True, shell=True
    )
    return r.stdout.strip()

def query(kql, db, token):
    import requests
    resp = requests.post(
        f"{CLUSTER}/v1/rest/query",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json={"db": db, "csl": kql}, timeout=300
    )
    resp.raise_for_status()
    data = resp.json()
    if "Tables" not in data or not data["Tables"]:
        return pd.DataFrame()
    t = data["Tables"][0]
    return pd.DataFrame(t["Rows"], columns=[c["ColumnName"] for c in t["Columns"]])

token = get_token()
baseline = {}

# ── 1. Current user counts + COGS + tokens by SKU (latest 28d) ───────
print("1. User counts, COGS, tokens by SKU (T28)...")
df = query("""
copilot_daily_aggregations_v3
| extend d = todatetime(day)
| where d >= ago(28d)
| where categorized_plan != ""
| where cleaned_integration == ""
| where summarized_integration == ""
| where cleaned_model_name == ""
| where hosting_provider == ""
| where hosting_type == ""
| where categorized_interaction_type == ""
| where isnull(is_automode_request)
| where billable_owner_is_staff == false
| summarize
    total_cogs = sum(todouble(usd_cogs)),
    avg_daily_users = avg(todouble(daily_unique_users)),
    total_requests = sum(todouble(requests)),
    total_output_tokens = sum(todouble(output_tokens)),
    total_input_tokens = sum(todouble(input_tokens_from_model)),
    total_cached_tokens = sum(todouble(input_tokens_cached_from_model)),
    days = dcount(day)
    by categorized_plan
""", db="data_science", token=token)
print(f"   → {len(df)} SKUs")

sku_map = {
    "Pro Plus": "Pro+", "Pro": "Pro", "Business": "Business",
    "Enterprise": "Enterprise", "FREE_LIMITED_COPILOT": "Free",
    "COMPLIMENTARY_EDU": "Edu", "COMPLIMENTARY_OTHER": "Other",
    "UNKNOWN": "Unknown", "MISSING_PLAN": "Missing",
}

sku_data = {}
for _, row in df.iterrows():
    sku = sku_map.get(row["categorized_plan"], row["categorized_plan"])
    days = float(row["days"])
    if days == 0:
        continue
    sku_data[sku] = {
        "avg_daily_users": float(row["avg_daily_users"]),
        "daily_cogs": float(row["total_cogs"]) / days,
        "daily_requests": float(row["total_requests"]) / days,
        "daily_output_tokens": float(row["total_output_tokens"]) / days,
        "daily_input_tokens": float(row["total_input_tokens"]) / days,
        "daily_cached_tokens": float(row["total_cached_tokens"]) / days,
        "cogs_per_user_day": float(row["total_cogs"]) / days / float(row["avg_daily_users"]) if float(row["avg_daily_users"]) > 0 else 0,
        "requests_per_user_day": float(row["total_requests"]) / days / float(row["avg_daily_users"]) if float(row["avg_daily_users"]) > 0 else 0,
        "tokens_per_user_day": (float(row["total_output_tokens"]) + float(row["total_input_tokens"])) / days / float(row["avg_daily_users"]) if float(row["avg_daily_users"]) > 0 else 0,
    }
baseline["sku_actuals"] = sku_data

# ── 2. Revenue by SKU (latest 28d) ───────────────────────────────────
print("2. Revenue by SKU (T28)...")
rev = query("""
copilot_revenue_daily
| extend d = todatetime(day)
| where d >= ago(28d) and is_staff_owned == false
| extend sku = case(
    product == "Copilot Pro", "Pro",
    product == "Copilot Pro Plus", "Pro+",
    product == "Copilot Business", "Business",
    product == "Copilot Enterprise", "Enterprise",
    product == "Copilot Standalone", "Free",
    product startswith "Copilot for Individuals", "Pro",
    "Other")
| summarize
    total_rev = sum(todouble(daily_revenue)),
    total_users = dcount(user_dotcom_id),
    days = dcount(day)
    by sku, revenue_type
""", db="copilot", token=token)
print(f"   → {len(rev)} rows")

rev_data = {}
for _, row in rev.iterrows():
    sku = row["sku"]
    rtype = row["revenue_type"]
    days = float(row["days"])
    if sku not in rev_data:
        rev_data[sku] = {"seat_rev_daily": 0, "overage_rev_daily": 0, "total_users": 0}
    if "seat" in str(rtype).lower() or "subscription" in str(rtype).lower():
        rev_data[sku]["seat_rev_daily"] += float(row["total_rev"]) / days
    else:
        rev_data[sku]["overage_rev_daily"] += float(row["total_rev"]) / days
    rev_data[sku]["total_users"] = max(rev_data[sku]["total_users"], int(row["total_users"]))
baseline["rev_actuals"] = rev_data

# ── 3. Usage distribution (percentiles) via PRU data ─────────────────
print("3. Premium request usage distributions...")
pru = query("""
premium_request_usage
| extend d = todatetime(day)
| where d >= ago(28d) and billable_owner_is_staff == false
| extend plan = case(
    categorized_plan == "Pro Plus", "Pro+",
    categorized_plan == "Pro", "Pro",
    categorized_plan == "Business", "Business",
    categorized_plan == "Enterprise", "Enterprise",
    "Other")
| where plan in ("Pro", "Pro+")
| summarize
    total_premium = sum(todouble(premium_requests_consumed)),
    total_overage = sum(todouble(overage_requests_consumed))
    by plan, analytics_tracking_id
""", db="copilot", token=token)
print(f"   → {len(pru)} user-level rows")

pru_dist = {}
for plan in ["Pro", "Pro+"]:
    p = pru[pru["plan"] == plan].copy()
    p["total_premium"] = pd.to_numeric(p["total_premium"], errors="coerce")
    p["total_overage"] = pd.to_numeric(p["total_overage"], errors="coerce")
    p["total"] = p["total_premium"] + p["total_overage"]

    if len(p) > 0:
        percs = p["total"].quantile([0, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 1.0]).to_dict()
        pru_dist[plan] = {
            "user_count": len(p),
            "zero_usage_pct": float((p["total"] == 0).mean() * 100),
            "mean_pru_28d": float(p["total"].mean()),
            "median_pru_28d": float(p["total"].median()),
            "percentiles": {f"p{int(k*100)}": float(v) for k, v in percs.items()},
            "pct_with_overage": float((p["total_overage"] > 0).mean() * 100),
            "mean_overage_28d": float(p["total_overage"].mean()),
        }
baseline["pru_distribution"] = pru_dist

# ── 4. COGS by interaction type (what's expensive) ───────────────────
print("4. COGS by interaction type (T28)...")
interact = query("""
copilot_daily_aggregations_v3
| extend d = todatetime(day)
| where d >= ago(28d)
| where categorized_plan == ""
| where cleaned_integration == ""
| where summarized_integration == ""
| where cleaned_model_name == ""
| where hosting_provider == ""
| where hosting_type == ""
| where categorized_interaction_type != ""
| where isnull(is_automode_request)
| where billable_owner_is_staff == false
| summarize
    cogs = sum(todouble(usd_cogs)),
    users = sum(todouble(daily_unique_users)),
    requests = sum(todouble(requests)),
    days = dcount(day)
    by categorized_interaction_type
""", db="data_science", token=token)
print(f"   → {len(interact)} interaction types")

interact_data = {}
for _, row in interact.iterrows():
    it = row["categorized_interaction_type"]
    days = float(row["days"])
    if days > 0:
        interact_data[it] = {
            "daily_cogs": float(row["cogs"]) / days,
            "daily_users": float(row["users"]) / days,
            "daily_requests": float(row["requests"]) / days,
            "cogs_per_request": float(row["cogs"]) / float(row["requests"]) if float(row["requests"]) > 0 else 0,
            "share_of_cogs": 0,  # calculated below
        }
total_cogs = sum(v["daily_cogs"] for v in interact_data.values())
for it in interact_data:
    interact_data[it]["share_of_cogs"] = interact_data[it]["daily_cogs"] / total_cogs * 100 if total_cogs > 0 else 0
baseline["interaction_types"] = interact_data

# ── 5. COGS by model (for efficiency roadmap) ────────────────────────
print("5. COGS by model (T28)...")
models = query("""
copilot_daily_aggregations_v3
| extend d = todatetime(day)
| where d >= ago(28d)
| where categorized_plan == ""
| where cleaned_integration == ""
| where summarized_integration == ""
| where cleaned_model_name != ""
| where hosting_provider == ""
| where hosting_type == ""
| where categorized_interaction_type == ""
| where isnull(is_automode_request)
| where billable_owner_is_staff == false
| summarize
    cogs = sum(todouble(usd_cogs)),
    requests = sum(todouble(requests)),
    output_tokens = sum(todouble(output_tokens)),
    input_tokens = sum(todouble(input_tokens_from_model)),
    cached_tokens = sum(todouble(input_tokens_cached_from_model)),
    days = dcount(day)
    by cleaned_model_name
| order by cogs desc
""", db="data_science", token=token)
print(f"   → {len(models)} models")

model_data = {}
for _, row in models.iterrows():
    m = row["cleaned_model_name"]
    days = float(row["days"])
    if days > 0 and float(row["cogs"]) > 1000:
        total_tokens = float(row["output_tokens"]) + float(row["input_tokens"])
        model_data[m] = {
            "daily_cogs": float(row["cogs"]) / days,
            "cogs_per_request": float(row["cogs"]) / float(row["requests"]) if float(row["requests"]) > 0 else 0,
            "cogs_per_mtokens": float(row["cogs"]) / (total_tokens / 1e6) if total_tokens > 0 else 0,
            "cache_hit_rate": float(row["cached_tokens"]) / float(row["input_tokens"]) * 100 if float(row["input_tokens"]) > 0 else 0,
            "daily_requests": float(row["requests"]) / days,
        }
total_model_cogs = sum(v["daily_cogs"] for v in model_data.values())
for m in model_data:
    model_data[m]["share_of_cogs"] = model_data[m]["daily_cogs"] / total_model_cogs * 100 if total_model_cogs > 0 else 0
baseline["model_mix"] = model_data

# ── 6. DAU by plan type (engagement table) ───────────────────────────
print("6. DAU by plan type (latest)...")
dau = query("""
copilot_unified_engagement_aggregated
| where day >= ago(7d)
| where Metric == "Daily Active Users"
| where aggregation_name == "Copilot Total"
| where copilot_product_feature == "All"
| where copilot_product_pillar == "All"
| where editor == "All"
| where free_user_type == "All"
| where product_sku != "All"
| where language_id == "All"
| where user_plan_type == "All"
| summarize dau = avg(todouble(Count)) by product_sku
| order by dau desc
""", db="copilot", token=token)
print(f"   → {len(dau)} product SKUs")
baseline["dau_by_sku"] = {row["product_sku"]: float(row["dau"]) for _, row in dau.iterrows()}

# ── 7. Estimated ARR by product ──────────────────────────────────────
print("7. Estimated ARR (latest)...")
arr = query("""
copilot_estimated_arr
| where day >= ago(7d) and is_org_staff_owned == false
| summarize arr = avg(estimated_arr), seats = avg(billed_seats) by product
| order by arr desc
""", db="copilot", token=token)
print(f"   → {len(arr)} products")
arr_data = {}
for _, row in arr.iterrows():
    arr_data[row["product"]] = {
        "arr": float(row["arr"]),
        "seats": float(row["seats"]),
        "arr_per_seat_month": float(row["arr"]) / float(row["seats"]) / 12 if float(row["seats"]) > 0 else 0,
    }
baseline["arr_by_product"] = arr_data

# ── Save ──────────────────────────────────────────────────────────────
baseline["generated_at"] = datetime.datetime.now().isoformat()
baseline["data_window"] = "trailing 28 days"

out = r"C:\Users\joeingraham\tbb-calc\baseline_data.json"
with open(out, "w") as f:
    json.dump(baseline, f, indent=2, default=str)

print(f"\n✅ Baseline data saved → {out}")

# Print summary
print("\n═══ BASELINE SUMMARY ═══")
for sku in ["Pro", "Pro+"]:
    if sku in sku_data:
        s = sku_data[sku]
        r = rev_data.get(sku, {})
        rev_daily = r.get("seat_rev_daily", 0) + r.get("overage_rev_daily", 0)
        print(f"\n  {sku}:")
        print(f"    Avg DAU:          {s['avg_daily_users']:,.0f}")
        print(f"    COGS/user/day:    ${s['cogs_per_user_day']:.2f}")
        print(f"    Revenue/user/day: ${rev_daily / s['avg_daily_users']:.2f}" if s['avg_daily_users'] > 0 else "    Revenue/user/day: N/A")
        print(f"    Requests/user/day:{s['requests_per_user_day']:.1f}")
        print(f"    Tokens/user/day:  {s['tokens_per_user_day']:,.0f}")
    if sku in pru_dist:
        p = pru_dist[sku]
        print(f"    Zero usage %:     {p['zero_usage_pct']:.1f}%")
        print(f"    Mean PRU/28d:     {p['mean_pru_28d']:.0f}")
        print(f"    Median PRU/28d:   {p['median_pru_28d']:.0f}")
        print(f"    P95 PRU/28d:      {p['percentiles'].get('p95', 0):.0f}")
        print(f"    P99 PRU/28d:      {p['percentiles'].get('p99', 0):.0f}")
        print(f"    Overage users %:  {p['pct_with_overage']:.1f}%")

print("\n  Top interaction types by COGS share:")
sorted_interact = sorted(interact_data.items(), key=lambda x: x[1]["daily_cogs"], reverse=True)[:5]
for it, v in sorted_interact:
    print(f"    {it:30s}  {v['share_of_cogs']:.1f}%  (${v['cogs_per_request']:.4f}/req)")

print("\n  Top models by COGS share:")
sorted_models = sorted(model_data.items(), key=lambda x: x[1]["daily_cogs"], reverse=True)[:5]
for m, v in sorted_models:
    print(f"    {m:30s}  {v['share_of_cogs']:.1f}%  (${v['cogs_per_mtokens']:.2f}/Mtok, {v['cache_hit_rate']:.0f}% cache)")
