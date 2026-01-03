import sqlite3
import os

# Path to database
db_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'electronics_dw.db')
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Expected schema
expected_tables = {
    'customer_dim': ['customer_key', 'gender', 'customer_name', 'city', 'state_code', 'state', 'zip_code', 'country', 'continent', 'birthday'],
    'product_dim': ['product_key', 'product_name', 'brand', 'color', 'category_key', 'subcategory_key', 'unit_cost_usd', 'unit_price_usd'],
    'product_category_dim': ['category_key', 'category'],
    'product_subcategory_dim': ['subcategory_key', 'subcategory'],
    'store_dim': ['store_key', 'store_name', 'country', 'state', 'square_meters', 'open_date'],
    'exchange_rates_dim': ['date', 'currency_code', 'exchange'],
    'sales_fact': ['order_number', 'customer_key', 'product_key', 'store_key', 'order_date', 'delivery_date', 'quantity', 'unit_cost_usd', 'unit_price_usd', 'total_revenue', 'profit', 'currency_code']
}

print("=" * 60)
print("DATA WAREHOUSE SCHEMA VERIFICATION")
print("=" * 60)

all_passed = True

for table, expected_cols in expected_tables.items():
    cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table}'")
    if cursor.fetchone():
        cursor.execute(f"PRAGMA table_info({table})")
        actual_cols = [col[1] for col in cursor.fetchall()]
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        row_count = cursor.fetchone()[0]
        
        # Check columns
        missing = set(expected_cols) - set(actual_cols)
        if missing:
            print(f"❌ {table}: Missing columns: {missing}")
            all_passed = False
        else:
            print(f"✅ {table}: {row_count:,} rows, {len(actual_cols)} columns")
    else:
        print(f"❌ {table}: TABLE NOT FOUND")
        all_passed = False

print("=" * 60)
if all_passed:
    print("✅ ALL TABLES VERIFIED SUCCESSFULLY!")
else:
    print("❌ SOME ISSUES FOUND - See above")

conn.close()
