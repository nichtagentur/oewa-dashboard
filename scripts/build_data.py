import csv
import json
from pathlib import Path
from collections import defaultdict

BASE = Path("/home/student/Data/oewa-data")
OUT = Path("/home/student/Projects/oewa-dashboard/data.json")

COMBINED = BASE / "oewa_combined_all.csv"
MISSING = BASE / "MISSING_DATA_REPORT.csv"


def read_semicolon(path):
    with path.open(newline="", encoding="utf-8") as fh:
        reader = csv.reader(fh, delimiter=";")
        header = next(reader)
        for row in reader:
            if not row:
                continue
            if len(row) != len(header):
                # Skip malformed rows
                continue
            yield dict(zip(header, row))


def to_int(value):
    try:
        return int(float(value))
    except Exception:
        return None


def to_float(value):
    try:
        return float(value)
    except Exception:
        return None


rows = []
months = set()
for r in read_semicolon(COMBINED):
    r["unique_users"] = to_int(r.get("unique_users"))
    r["unique_clients"] = to_int(r.get("unique_clients"))
    r["visits"] = to_int(r.get("visits"))
    r["impressions"] = to_int(r.get("impressions"))
    r["impressions_at_percent"] = to_float(r.get("impressions_at_percent"))
    r["usetime_seconds"] = to_int(r.get("usetime_seconds"))
    r["impressions_per_visits"] = to_float(r.get("impressions_per_visits"))
    rows.append(r)
    months.add(r.get("month"))

months = sorted(m for m in months if m)
latest_month = months[-1] if months else None

# Category totals by month (impressions)
cat_month_totals = defaultdict(lambda: defaultdict(int))
for r in rows:
    if not r.get("month") or r.get("impressions") is None:
        continue
    cat = r.get("category")
    cat_month_totals[cat][r["month"]] += r["impressions"]

category_series = []
for cat, by_month in sorted(cat_month_totals.items()):
    series = []
    for m in months:
        series.append({"month": m, "value": by_month.get(m, 0)})
    category_series.append({"category": cat, "series": series})

# Top 10 by impressions (latest month)
latest_rows = [r for r in rows if r.get("month") == latest_month and r.get("impressions") is not None]
latest_rows.sort(key=lambda r: r["impressions"], reverse=True)

seen = set()
unique_top = []
for r in latest_rows:
    key = (r.get("name"), r.get("category"))
    if key in seen:
        continue
    seen.add(key)
    unique_top.append({
        "name": r.get("name"),
        "category": r.get("category"),
        "type": r.get("type"),
        "unique_users": r.get("unique_users"),
        "net_reach": r.get("net_reach"),
        "impressions": r.get("impressions"),
    })
    if len(unique_top) >= 10:
        break

# Engagement scatter for top 30 by impressions (latest month)
scatter = []
for r in latest_rows[:30]:
    if r.get("usetime_seconds") is None or r.get("impressions_per_visits") is None:
        continue
    scatter.append({
        "name": r.get("name"),
        "category": r.get("category"),
        "usetime_seconds": r.get("usetime_seconds"),
        "impressions_per_visits": r.get("impressions_per_visits"),
        "impressions": r.get("impressions"),
    })

# Missing data heatmap counts by month and category
missing_counts = defaultdict(lambda: defaultdict(int))
missing_months = set()
for r in read_semicolon(MISSING):
    m = r.get("month")
    c = r.get("category")
    if not m or not c:
        continue
    missing_counts[c][m] += 1
    missing_months.add(m)

missing_months = sorted(missing_months)
missing_by_category = []
for cat, by_month in sorted(missing_counts.items()):
    series = []
    for m in missing_months:
        series.append({"month": m, "value": by_month.get(m, 0)})
    missing_by_category.append({"category": cat, "series": series})

output = {
    "latest_month": latest_month,
    "months": months,
    "category_series": category_series,
    "top_latest": unique_top,
    "scatter_latest": scatter,
    "missing_months": missing_months,
    "missing_by_category": missing_by_category,
}

OUT.write_text(json.dumps(output, ensure_ascii=True, indent=2), encoding="utf-8")
print(f"Wrote {OUT}")
