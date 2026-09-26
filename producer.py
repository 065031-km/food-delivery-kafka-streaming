import csv
import json
import time
from kafka import KafkaProducer
from kafka.errors import KafkaError

# ============================================================
# FOOD DELIVERY KAFKA PRODUCER
# ============================================================

KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "food_delivery_events"
CSV_FILE = "data/sample_orders.csv"

STREAM_INTERVAL = 1  # seconds

# ============================================================
# REQUIRED FIELDS
# ============================================================

REQUIRED_FIELDS = [
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

VALID_PAYMENT_METHODS = {
    "UPI",
    "Credit Card",
    "Debit Card",
    "Wallet",
    "Cash"
}

VALID_TRAFFIC_LEVELS = {
    "Low",
    "Medium",
    "High",
    "Severe"
}

VALID_WEATHER = {
    "Clear",
    "Cloudy",
    "Rain",
    "Heavy Rain"
}


# ============================================================
# VALIDATE DATA
# ============================================================

def validate_record(record):

    # Check that all required fields exist
    for field in REQUIRED_FIELDS:
        if not record.get(field):
            return False, f"Missing field: {field}"

    # Validate numeric fields
    try:
        order_value = float(record["order_value"])
        distance_km = float(record["distance_km"])
        estimated_delivery = int(record["estimated_delivery_min"])
    except ValueError:
        return False, "Invalid numeric value"

    # Business validation rules
    if order_value < 0:
        return False, "Order value cannot be negative"

    if distance_km < 0:
        return False, "Distance cannot be negative"

    if estimated_delivery <= 0:
        return False, "Estimated delivery time must be positive"

    if record["payment_method"] not in VALID_PAYMENT_METHODS:
        return False, "Invalid payment method"

    if record["traffic_level"] not in VALID_TRAFFIC_LEVELS:
        return False, "Invalid traffic level"

    if record["weather"] not in VALID_WEATHER:
        return False, "Invalid weather condition"

    return True, "Valid"


# ============================================================
# CREATE KAFKA PRODUCER
# ============================================================

def create_producer():

    producer = KafkaProducer(

        bootstrap_servers=[KAFKA_BROKER],

        # Wait for acknowledgement from Kafka
        acks="all",

        # Retry failed transmissions
        retries=5,

        # Small batching delay
        linger_ms=10
    )

    return producer


# ============================================================
# MAIN PRODUCER FUNCTION
# ============================================================

def stream_events():

    print("=" * 75)
    print("FOOD DELIVERY KAFKA PRODUCER")
    print("=" * 75)

    print(f"Kafka Broker : {KAFKA_BROKER}")
    print(f"Kafka Topic  : {KAFKA_TOPIC}")
    print(f"Data Source  : {CSV_FILE}")
    print(f"Frequency    : 1 event every {STREAM_INTERVAL} second")
    print("=" * 75)

    producer = None

    total_records = 0
    successful_records = 0
    failed_records = 0

    try:

        # Connect to Kafka
        producer = create_producer()

        print("Kafka connection established successfully.")
        print("Starting event streaming...\n")

        # Open CSV dataset
        with open(
            CSV_FILE,
            "r",
            encoding="utf-8",
            newline=""
        ) as file:

            reader = csv.DictReader(file)

            # Process one event at a time
            for record in reader:

                total_records += 1

                # ------------------------------------------------
                # Validate event
                # ------------------------------------------------

                is_valid, validation_message = validate_record(record)

                if not is_valid:

                    failed_records += 1

                    print(
                        f"[{total_records:03d}] "
                        f"INVALID | "
                        f"{validation_message}"
                    )

                    continue

                # ------------------------------------------------
                # Convert numeric values
                # ------------------------------------------------

                record["order_value"] = float(record["order_value"])
                record["distance_km"] = float(record["distance_km"])
                record["estimated_delivery_min"] = int(
                    record["estimated_delivery_min"]
                )

                # ------------------------------------------------
                # Send event to Kafka
                # ------------------------------------------------

                message_key = record["order_id"].encode("utf-8")
                message_value = json.dumps(record).encode("utf-8")

                future = producer.send(
                    KAFKA_TOPIC,
                    key=message_key,
                    value=message_value
                )

                try:

                    metadata = future.get(timeout=10)

                    successful_records += 1

                    print(
                        f"[{total_records:03d}/500] "
                        f"SUCCESS | "
                        f"Event: {record['event_id']} | "
                        f"Order: {record['order_id']} | "
                        f"Type: {record['event_type']} | "
                        f"Location: {record['location']} | "
                        f"Partition: {metadata.partition} | "
                        f"Offset: {metadata.offset}"
                    )

                except KafkaError as error:

                    failed_records += 1

                    print(
                        f"[{total_records:03d}] "
                        f"KAFKA ERROR | "
                        f"{error}"
                    )

                # ------------------------------------------------
                # Simulate real-time event arrival
                # ------------------------------------------------

                time.sleep(STREAM_INTERVAL)

    except FileNotFoundError:

        print("\nERROR: CSV file was not found.")
        print(f"Expected file: {CSV_FILE}")

    except KeyboardInterrupt:

        print("\n\nStreaming interrupted by user.")

    except Exception as error:

        print(f"\nUnexpected error: {error}")

    finally:

        # --------------------------------------------------------
        # Close Kafka producer safely
        # --------------------------------------------------------

        if producer is not None:

            producer.flush()
            producer.close()

        print("\n" + "=" * 75)
        print("STREAMING SUMMARY")
        print("=" * 75)

        print(f"Records processed : {total_records}")
        print(f"Successfully sent : {successful_records}")
        print(f"Failed records    : {failed_records}")

        print("=" * 75)


# ============================================================
# PROGRAM ENTRY POINT
# ============================================================

if __name__ == "__main__":
    stream_events()