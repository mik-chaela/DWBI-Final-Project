import pandas as pd
import os
from sqlalchemy import create_engine

# --- PHASE 1: EXTRACTION ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def load_data():
    print("Extracting data from CSVs...")
    sales = pd.read_csv(os.path.join(BASE_DIR, "data", "Sales.csv"), encoding="latin1")
    customers = pd.read_csv(os.path.join(BASE_DIR, "data", "Customers.csv"), encoding="latin1")
    products = pd.read_csv(os.path.join(BASE_DIR, "data", "Products.csv"), encoding="latin1")
    stores = pd.read_csv(os.path.join(BASE_DIR, "data", "Stores.csv"), encoding="latin1")
    exchange = pd.read_csv(os.path.join(BASE_DIR, "data", "Exchange_Rates.csv"), encoding="latin1")
    return sales, customers, products, stores, exchange

# --- PHASE 2: TRANSFORMATION ---
def transform_data(sales, customers, products, stores, exchange):
    print("Transforming data...")

    # Standardize Dates
    sales['Order Date'] = pd.to_datetime(sales['Order Date'])
    sales['Delivery Date'] = pd.to_datetime(sales['Delivery Date'])
    
    # Create Dimensions
    cat_dim = products[['Category']].drop_duplicates().reset_index(drop=True)
    cat_dim['CategoryKey'] = cat_dim.index + 1

    sub_dim = products[['Subcategory', 'Category']].drop_duplicates().reset_index(drop=True)
    sub_dim['SubcategoryKey'] = sub_dim.index + 1

    # Product_Dim (Essential for Question A)
    prod_dim = products.copy()

    # Create Sales_Fact (Math for Business Questions)
    sales_fact = pd.merge(sales, products[['ProductKey', 'Unit Price USD', 'Unit Cost USD']], on='ProductKey', how='left')
    sales_fact['Total_Revenue_USD'] = sales_fact['Quantity'] * sales_fact['Unit Price USD']
    sales_fact['Delivery_Time_Days'] = (sales_fact['Delivery Date'] - sales_fact['Order Date']).dt.days
    sales_fact['Order_Year'] = sales_fact['Order Date'].dt.year

    return cat_dim, sub_dim, prod_dim, sales_fact

# --- PHASE 3: LOADING ---
def load_to_mysql(tables_dict):
    print("Connecting to MySQL...")
    user = "root"
    password = ""
    host = "localhost"
    port = 3307
    db = "electronics_dw"
    
    engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{db}')

    for table_name, df in tables_dict.items():
        # Using method='multi' can be more stable for large loads
        print(f"Loading {table_name}...")
        df.to_sql(table_name.lower(), engine, if_exists='replace', index=False, chunksize=1000)
    
    print("\nSUCCESS! All tables loaded.")

if __name__ == "__main__":
    s_raw, c_raw, p_raw, st_raw, ex_raw = load_data()
    cat, sub, prod, fact = transform_data(s_raw, c_raw, p_raw, st_raw, ex_raw)
    
    to_load = {
        'product_category_dim': cat,
        'product_subcategory_dim': sub,
        'product_dim': prod,
        'sales_fact': fact,
        'store_dim': st_raw
    }
    load_to_mysql(to_load)