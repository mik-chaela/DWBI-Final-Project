import sqlite3
import os

db_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'electronics_dw.db')
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Check data availability
cursor.execute("""
    SELECT 
        strftime('%Y', order_date) as year, 
        COUNT(*) as total_orders,
        SUM(CASE WHEN delivery_date IS NOT NULL THEN 1 ELSE 0 END) as with_delivery
    FROM sales_fact 
    GROUP BY year 
    ORDER BY year
""")
print("Year | Total Orders | With Delivery Date")
print("-" * 45)
for r in cursor.fetchall():
    print(f"{r[0]} | {r[1]:>12} | {r[2]:>18}")

conn.close()
