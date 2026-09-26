import csv
import random
from datetime import datetime, timedelta

# ============================================================
# FOOD DELIVERY KAFKA STREAMING PROJECT
# SAMPLE DATA GENERATOR
# ============================================================

OUTPUT_FILE = "data/sample_orders.csv"

NUMBER_OF_ORDERS = 100

locations = [
    "Delhi",
    "Gurugram",
    "Noida",
    "Ghaziabad",
    "Faridabad"
]

cuisines = [
    "North Indian",
    "South Indian",
    "Chinese",
    "Biryani",
    "Pizza",
    "Fast Food"
]

payment_methods = [
    "UPI",
    "Credit Card",
    "Debit Card",
    "Wallet",
    "Cash"
]

traffic_levels = [
    "Low",
    "Medium",
    "High",
    "Severe"
]

weather_conditions = [
    "Clear",
    "Cloudy",
    "Rain",
    "Heavy Rain"
]

event_definitions = [
    ("customer_app", "ORDER_PLACED"),
    ("restaurant_platform", "RESTAURANT_ACCEPTED"),
    ("delivery_partner_app", "RIDER_ASSIGNED"),
    ("delivery_partner_app", "ORDER_PICKED_UP"),
    ("delivery_partner_app", "ORDER_DELIVERED")
]

fields = [
    "event_id",
    "event_timestamp",
    "event_source",
    "event_type",
    "order_id",
    "customer_id",
    "restaurant_id",
    "location",
    "cuisine",
    "order_value",
    "payment_method",
    "distance_km",
    "estimated_delivery_min",
    "traffic_level",
    "weather",
    "delivery_status"
]


def generate_dataset():

    records = []

    # Starting point for simulated events
    start_time = datetime(2026, 8, 31, 12, 0, 0)

    event_counter = 1

    for order_number in range(1, NUMBER_OF_ORDERS + 1):

        # Generate common order information
        order_id = f"ORD{100000 + order_number}"
        customer_id = f"CUS{random.randint(1000, 9999)}"
        restaurant_id = f"REST{random.randint(100, 999)}"

        location = random.choice(locations)
        cuisine = random.choice(cuisines)

        order_value = round(random.uniform(150, 2500), 2)

        payment_method = random.choice(payment_methods)

        distance_km = round(random.uniform(1.0, 15.0), 2)

        estimated_delivery_min = random.randint(20, 60)

        traffic_level = random.choice(traffic_levels)
        weather = random.choice(weather_conditions)

        # Create timestamps for the order lifecycle
        order_start = start_time + timedelta(
            minutes=random.randint(0, 300)
        )

        event_times = [
            order_start,
            order_start + timedelta(minutes=random.randint(2, 5)),
            order_start + timedelta(minutes=random.randint(5, 10)),
            order_start + timedelta(minutes=random.randint(15, 25)),
            order_start + timedelta(minutes=random.randint(25, 60))
        ]

        delivery_statuses = [
            "PLACED",
            "CONFIRMED",
            "RIDER_ASSIGNED",
            "PICKED_UP",
            "DELIVERED"
        ]

        # Generate five events for each order
        for index, ((event_source, event_type), event_time) in enumerate(
            zip(event_definitions, event_times)
        ):

            record = {
                "event_id": f"EVT{event_counter:06d}",
                "event_timestamp": event_time.isoformat(),
                "event_source": event_source,
                "event_type": event_type,
                "order_id": order_id,
                "customer_id": customer_id,
                "restaurant_id": restaurant_id,
                "location": location,
                "cuisine": cuisine,
                "order_value": order_value,
                "payment_method": payment_method,
                "distance_km": distance_km,
                "estimated_delivery_min": estimated_delivery_min,
                "traffic_level": traffic_level,
                "weather": weather,
                "delivery_status": delivery_statuses[index]
            }

            records.append(record)

            event_counter += 1

    # Randomise event arrival order to simulate streaming
    random.shuffle(records)

    # Save records as CSV
    with open(
        OUTPUT_FILE,
        "w",
        newline="",
        encoding="utf-8"
    ) as file:

        writer = csv.DictWriter(
            file,
            fieldnames=fields
        )

        writer.writeheader()
        writer.writerows(records)

    print("=" * 65)
    print("FOOD DELIVERY SAMPLE DATA GENERATOR")
    print("=" * 65)
    print(f"Orders generated : {NUMBER_OF_ORDERS}")
    print(f"Events generated : {len(records)}")
    print("Events per order : 5")
    print(f"Output file      : {OUTPUT_FILE}")
    print("=" * 65)
    print("Dataset generation completed successfully.")


if __name__ == "__main__":
    generate_dataset()