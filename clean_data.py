import pandas as pd

INPUT_FILE = "data/sample_orders.csv"
OUTPUT_FILE = "data/cleaned_orders.csv"

print("=" * 70)
print("FOOD DELIVERY DATA CLEANING")
print("=" * 70)

# Load dataset
df = pd.read_csv(INPUT_FILE)

print(f"Original rows    : {len(df)}")
print(f"Original columns : {len(df.columns)}")

# Remove duplicate records
before = len(df)
df = df.drop_duplicates()
duplicates_removed = before - len(df)

# Remove rows with missing values
before = len(df)
df = df.dropna()
missing_removed = before - len(df)

# Convert numeric columns
df["order_value"] = pd.to_numeric(df["order_value"], errors="coerce")
df["distance_km"] = pd.to_numeric(df["distance_km"], errors="coerce")
df["estimated_delivery_min"] = pd.to_numeric(
    df["estimated_delivery_min"],
    errors="coerce"
)

# Convert timestamp
df["event_timestamp"] = pd.to_datetime(
    df["event_timestamp"],
    errors="coerce"
)

# Remove rows made invalid by conversion
df = df.dropna()

# Save cleaned data
df.to_csv(OUTPUT_FILE, index=False)

print(f"Duplicates removed: {duplicates_removed}")
print(f"Missing rows removed: {missing_removed}")
print(f"Cleaned rows      : {len(df)}")
print(f"Output file       : {OUTPUT_FILE}")
print("=" * 70)
print("DATA CLEANING COMPLETED SUCCESSFULLY")
print("=" * 70)