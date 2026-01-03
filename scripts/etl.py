import pandas as pd
import os
from sqlalchemy import create_engine

# --- PHASE 1: EXTRACTION ---
# Getting the base directory to ensure paths work on any machine
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def load_data():
    print("Extracting data from CSVs...")
    # Loading files with latin1 encoding to avoid special character errors
    sales = pd.read_csv(os.path.join(BASE_DIR, "data", "Sales.csv"), encoding="latin1")
    customers = pd.read_csv(os.path.join(BASE_DIR, "data", "Customers.csv"), encoding="latin1")
    products = pd.read_csv(os.path.join(BASE_DIR, "data", "Products.csv"), encoding="latin1")
    stores = pd.read_csv(os.path.join(BASE_DIR, "data", "Stores.csv"), encoding="latin1")
    exchange = pd.read_csv(os.path.join(BASE_DIR, "data", "Exchange_Rates.csv"), encoding="latin1")
    
    return sales, customers, products, stores, exchange

# --- PHASE 2: TRANSFORMATION ---
def transform_data(sales, customers, products, stores, exchange):
    print("Transforming data and building Dimensions/Fact tables...")

    # 1. Standardize Date Formats
    sales['Order Date'] = pd.to_datetime(sales['Order Date'])
    sales['Delivery Date'] = pd.to_datetime(sales['Delivery Date'])
    customers['Birthday'] = pd.to_datetime(customers['Birthday'])
    stores['Open Date'] = pd.to_datetime(stores['Open Date'])
    exchange['Date'] = pd.to_datetime(exchange['Date'])

    # 2. Build Dimensions
    # Product_Category_Dim
    product_category_dim = products[['Category']].drop_duplicates().reset_index(drop=True)
    product_category_dim['CategoryKey'] = product_category_dim.index + 1

    # Product_Subcategory_Dim
    product_subcategory_dim = products[['Subcategory', 'Category']].drop_duplicates().reset_index(drop=True)
    product_subcategory_dim['SubcategoryKey'] = product_subcategory_dim.index + 1

    # Customer_Dim (Keep essential columns)
    customer_dim = customers[['CustomerKey', 'Gender', 'Name', 'City', 'State', 'Country', 'Birthday']]

    # Store_Dim
    store_dim = stores[['StoreKey', 'Country', 'State', 'Square Meters', 'Open Date']]

    # 3. Build Sales_Fact Table (Calculating values for your Business Questions)
    # Merge Sales with Products to get Price and Cost for Revenue calculation
    sales_fact = pd.merge(sales, products[['ProductKey', 'Unit Price USD', 'Unit Cost USD', 'Brand', 'Subcategory']], on='ProductKey', how='left')

    # Metric: Revenue and Cost
    sales_fact['Total_Revenue_USD'] = sales_fact['Quantity'] * sales_fact['Unit Price USD']
    sales_fact['Total_Cost_USD'] = sales_fact['Quantity'] * sales_fact['Unit Cost USD']

    # Metric: Delivery Time in Days (Question B)
    sales_fact['Delivery_Time_Days'] = (sales_fact['Delivery Date'] - sales_fact['Order Date']).dt.days

    # Metric: Seasonality (Question D - Defining Peak as Nov/Dec for electronics)
    sales_fact['Season'] = sales_fact['Order Date'].dt.month.apply(lambda x: 'Peak' if x in [11, 12] else 'Off-Peak')

    # Metric: Month/Year for growth analysis (Question C)
    sales_fact['Order_Month'] = sales_fact['Order Date'].dt.month
    sales_fact['Order_Year'] = sales_fact['Order Date'].dt.year

    return product_category_dim, product_subcategory_dim, customer_dim, store_dim, sales_fact

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
        print(f"Loading {table_name}...")
        df.to_sql(table_name, engine, if_exists='replace', index=False)
    
    print("Success! Data Warehouse is ready.")

# --- EXECUTION ---
if __name__ == "__main__":
    # Extract
    sales_df, cust_df, prod_df, store_df, ex_df = load_data()
    
    # Transform
    cat_dim, sub_dim, cust_dim, st_dim, s_fact = transform_data(sales_df, cust_df, prod_df, store_df, ex_df)
    
    # Load
    tables_to_load = {
        'Product_Category_Dim': cat_dim,
        'Product_Subcategory_Dim': sub_dim,
        'Customer_Dim': cust_dim,
        'Store_Dim': st_dim,
        'Sales_Fact': s_fact
    }
    
    load_to_mysql(tables_to_load)