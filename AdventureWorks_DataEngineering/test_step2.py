import sys
sys.path.insert(0, 'src')

from src.ingestion import extract_all
from src.validation import validate_all
from src.transformation import transform_all
from src.olap import build_olap, olap_rollup, olap_slice

print("=== STEP 2 END-TO-END TEST ===")
raw  = extract_all()
val  = validate_all(raw)
trn  = transform_all(val["valid"])
olap_data = build_olap(trn)
fact = olap_data["fact_sales"]

rollup = olap_rollup(fact, olap_data["dim_date"])
print("\nYearly Roll-up:")
print(rollup["yearly"].to_string(index=False))

slice_r     = olap_slice(fact, olap_data["dim_product"], olap_data["dim_date"], "Black")
total_sales = slice_r["TotalSales"].sum()
print(f"\nSlice (color=Black): {len(slice_r)} months, Total = ${total_sales:,.0f}")

print("\n=== ALL MODULES PASSED ===")
