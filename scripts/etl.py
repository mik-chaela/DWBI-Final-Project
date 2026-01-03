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
def clean_currency(value):
    """Remove $ symbol, commas, and whitespace from currency strings."""
    if pd.isna(value):
        return 0.0
    if isinstance(value, str):
        return float(value.replace('$', '').replace(',', '').strip())
    return float(value)

def transform_data(sales, customers, products, stores, exchange):
    print("Transforming data...")

    # Clean currency columns in products
    products['Unit Cost USD'] = products['Unit Cost USD'].apply(clean_currency)
    products['Unit Price USD'] = products['Unit Price USD'].apply(clean_currency)

    # ========== CUSTOMER_DIM ==========
    customer_dim = customers[['CustomerKey', 'Gender', 'Name', 'City', 'State Code', 
                               'State', 'Zip Code', 'Country', 'Continent', 'Birthday']].copy()
    customer_dim.columns = ['customer_key', 'gender', 'customer_name', 'city', 'state_code',
                            'state', 'zip_code', 'country', 'continent', 'birthday']

    # ========== PRODUCT_CATEGORY_DIM ==========
    product_category_dim = products[['CategoryKey', 'Category']].drop_duplicates().reset_index(drop=True)
    product_category_dim.columns = ['category_key', 'category']

    # ========== PRODUCT_SUBCATEGORY_DIM ==========
    product_subcategory_dim = products[['SubcategoryKey', 'Subcategory']].drop_duplicates().reset_index(drop=True)
    product_subcategory_dim.columns = ['subcategory_key', 'subcategory']

    # ========== PRODUCT_DIM ==========
    product_dim = products[['ProductKey', 'Product Name', 'Brand', 'Color', 
                            'CategoryKey', 'SubcategoryKey', 'Unit Cost USD', 'Unit Price USD']].copy()
    product_dim.columns = ['product_key', 'product_name', 'brand', 'color',
                           'category_key', 'subcategory_key', 'unit_cost_usd', 'unit_price_usd']

    # ========== STORE_DIM ==========
    store_dim = stores[['StoreKey', 'Country', 'State', 'Square Meters', 'Open Date']].copy()
    store_dim['store_name'] = 'Store ' + store_dim['StoreKey'].astype(str)  # Generate placeholder names
    store_dim = store_dim[['StoreKey', 'store_name', 'Country', 'State', 'Square Meters', 'Open Date']]
    store_dim.columns = ['store_key', 'store_name', 'country', 'state', 'square_meters', 'open_date']

    # ========== EXCHANGE_RATES_DIM ==========
    exchange_rates_dim = exchange[['Date', 'Currency', 'Exchange']].copy()
    exchange_rates_dim.columns = ['date', 'currency_code', 'exchange']

    # ========== SALES_FACT ==========
    # Standardize dates
    sales['Order Date'] = pd.to_datetime(sales['Order Date'])
    sales['Delivery Date'] = pd.to_datetime(sales['Delivery Date'])
    
    # Merge with products to get prices
    sales_fact = pd.merge(
        sales, 
        products[['ProductKey', 'Unit Price USD', 'Unit Cost USD']], 
        on='ProductKey', 
        how='left'
    )
    
    # Calculate derived columns
    sales_fact['total_revenue'] = sales_fact['Quantity'] * sales_fact['Unit Price USD']
    sales_fact['profit'] = sales_fact['Quantity'] * (sales_fact['Unit Price USD'] - sales_fact['Unit Cost USD'])
    
    # Select and rename columns
    sales_fact = sales_fact[['Order Number', 'CustomerKey', 'ProductKey', 'StoreKey',
                              'Order Date', 'Delivery Date', 'Quantity', 
                              'Unit Cost USD', 'Unit Price USD', 'total_revenue', 'profit', 'Currency Code']]
    sales_fact.columns = ['order_number', 'customer_key', 'product_key', 'store_key',
                          'order_date', 'delivery_date', 'quantity',
                          'unit_cost_usd', 'unit_price_usd', 'total_revenue', 'profit', 'currency_code']

    return {
        'customer_dim': customer_dim,
        'product_category_dim': product_category_dim,
        'product_subcategory_dim': product_subcategory_dim,
        'product_dim': product_dim,
        'store_dim': store_dim,
        'exchange_rates_dim': exchange_rates_dim,
        'sales_fact': sales_fact
    }

# --- PHASE 3: LOADING ---
def load_to_sqlite(tables_dict):
    print("Connecting to SQLite...")
    db_path = os.path.join(BASE_DIR, "data", "electronics_dw.db")
    engine = create_engine(f'sqlite:///{db_path}')

    for table_name, df in tables_dict.items():
        print(f"Loading {table_name}...")
        df.to_sql(table_name.lower(), engine, if_exists='replace', index=False, chunksize=1000)
    
    print("\nSUCCESS! All tables loaded.")

if __name__ == "__main__":
    s_raw, c_raw, p_raw, st_raw, ex_raw = load_data()
    tables = transform_data(s_raw, c_raw, p_raw, st_raw, ex_raw)
    load_to_sqlite(tables)