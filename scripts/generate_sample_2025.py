import pandas as pd
from datetime import datetime, timedelta
import random

print("Starting data preprocessing...")

# Read original orders data
print("\n1. Reading orders.csv...")
orders_df = pd.read_csv(r'C:\Users\Admin\PROJECT\Project_instacart\data\orders.csv')
print(f"   Original orders: {len(orders_df)}")

# Generate date range (60 days starting Nov 10, 2025)
start_date = datetime(2025, 11, 10)
num_days = 60

# Assign random dates to orders
print("\n2. Assigning synthetic dates to orders...")
dates = []
for _ in range(len(orders_df)):
    random_day = random.randint(0, num_days - 1)
    order_date = start_date + timedelta(days=random_day)
    dates.append(order_date.strftime('%Y-%m-%d'))

orders_df['order_date'] = dates

# Keep only 50 orders per date
print("\n3. Sampling 50 orders per date...")
orders_sampled = orders_df.groupby('order_date').head(50).reset_index(drop=True)
print(f"   Sampled orders: {len(orders_sampled)}")
print(f"   Date range: {orders_sampled['order_date'].min()} to {orders_sampled['order_date'].max()}")

# Get the order_ids we're keeping
sampled_order_ids = set(orders_sampled['order_id'])

# Filter order_products to only include our sampled orders
print("\n4. Filtering order_products__prior.csv...")
order_products_prior = pd.read_csv(r'C:\Users\Admin\PROJECT\Project_instacart\data\order_products__prior.csv')
print(f"   Original order_products: {len(order_products_prior)}")
order_products_filtered = order_products_prior[order_products_prior['order_id'].isin(sampled_order_ids)]
print(f"   Filtered order_products: {len(order_products_filtered)}")

# Save processed files
print("\n5. Saving processed files...")
orders_output = r'C:\Users\Admin\PROJECT\Project_instacart\data\orders_with_dates_2025.csv'
order_products_output = r'C:\Users\Admin\PROJECT\Project_instacart\data\order_products_filtered_2025.csv'

orders_sampled.to_csv(orders_output, index=False)
order_products_filtered.to_csv(order_products_output, index=False)

print(f"\n✅ Processing complete!")
print(f"\nCreated files:")
print(f"  - {orders_output}")
print(f"  - {order_products_output}")
print(f"\nData summary:")
print(f"  Total orders: {len(orders_sampled)}")
print(f"  Total order items: {len(order_products_filtered)}")
print(f"  Unique dates: {orders_sampled['order_date'].nunique()}")
print(f"  Orders per date: ~50")
print(f"\nNote: products.csv, aisles.csv, and departments.csv remain unchanged (reference data)")