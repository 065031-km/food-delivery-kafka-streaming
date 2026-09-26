import pandas as pd

INPUT_FILE = "data/consumed_orders.csv"
OUTPUT_FILE = "data/analytics_summary.csv"

print("=" * 70)
print("FOOD DELIVERY ANALYTICS")
print("=" * 70)

# Load consumed Kafka data
df = pd.read_csv(INPUT_FILE)

print(f"Total records: {len(df)}")

# ------------------------------------------------------------
# Basic statistics
# ------------------------------------------------------------

print("\n--- BASIC STATISTICS ---")

print(f"Total order value: {df['order_value'].sum():.2f}")
print(f"Average order value: {df['order_value'].mean():.2f}")
print(f"Average distance: {df['distance_km'].mean():.2f} km")
print(
    f"Average estimated delivery: "
    f"{df['estimated_delivery_min'].mean():.2f} min"
)

# ------------------------------------------------------------
# Orders by location
# ------------------------------------------------------------

print("\n--- ORDERS BY LOCATION ---")

orders_by_location = (
    df.groupby("location")
      .size()
      .sort_values(ascending=False)
)

print(orders_by_location)

# ------------------------------------------------------------
# Orders by cuisine
# ------------------------------------------------------------

print("\n--- ORDERS BY CUISINE ---")

orders_by_cuisine = (
    df.groupby("cuisine")
      .size()
      .sort_values(ascending=False)
)

print(orders_by_cuisine)

# ------------------------------------------------------------
# Orders by event type
# ------------------------------------------------------------

print("\n--- EVENTS BY TYPE ---")

events_by_type = (
    df.groupby("event_type")
      .size()
      .sort_values(ascending=False)
)

print(events_by_type)

# ------------------------------------------------------------
# Payment method analysis
# ------------------------------------------------------------

print("\n--- PAYMENT METHODS ---")

payment_analysis = (
    df.groupby("payment_method")
      .agg(
          orders=("order_id", "count"),
          total_value=("order_value", "sum"),
          average_value=("order_value", "mean")
      )
      .sort_values("orders", ascending=False)
)

print(payment_analysis)

# ------------------------------------------------------------
# Traffic analysis
# ------------------------------------------------------------

print("\n--- TRAFFIC LEVEL ---")

traffic_analysis = (
    df.groupby("traffic_level")
      .agg(
          events=("event_id", "count"),
          average_delivery_time=("estimated_delivery_min", "mean"),
          average_distance=("distance_km", "mean")
      )
      .sort_values("events", ascending=False)
)

print(traffic_analysis)

# ------------------------------------------------------------
# Weather analysis
# ------------------------------------------------------------

print("\n--- WEATHER ---")

weather_analysis = (
    df.groupby("weather")
      .agg(
          events=("event_id", "count"),
          average_delivery_time=("estimated_delivery_min", "mean")
      )
      .sort_values("events", ascending=False)
)

print(weather_analysis)

# ------------------------------------------------------------
# Save summary
# ------------------------------------------------------------

summary = pd.DataFrame({
    "metric": [
        "total_records",
        "total_order_value",
        "average_order_value",
        "average_distance_km",
        "average_estimated_delivery_min"
    ],
    "value": [
        len(df),
        df["order_value"].sum(),
        df["order_value"].mean(),
        df["distance_km"].mean(),
        df["estimated_delivery_min"].mean()
    ]
})

summary.to_csv(OUTPUT_FILE, index=False)

print("\n" + "=" * 70)
print("ANALYTICS COMPLETED SUCCESSFULLY")
print("=" * 70)
print(f"Output file: {OUTPUT_FILE}")