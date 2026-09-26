import json
import csv
from kafka import KafkaConsumer
from pymongo import MongoClient, ASCENDING, DESCENDING

# ============================================================
# FOOD DELIVERY KAFKA CONSUMER
# KAFKA → CSV + MONGODB
# ============================================================

KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "food_delivery_events"

OUTPUT_FILE = "data/consumed_orders.csv"

# ============================================================
# MONGODB CONFIGURATION
# ============================================================

MONGO_URI = "mongodb://admin:admin123@127.0.0.1:27017/?authSource=admin&authMechanism=SCRAM-SHA-256"
MONGO_DATABASE = "food_delivery"

# ============================================================
# MONGODB CONNECTION
# ============================================================

def create_mongodb_connection():

    client = MongoClient(MONGO_URI)

    db = client[MONGO_DATABASE]

    events_collection = db["events"]
    orders_collection = db["orders"]

    # --------------------------------------------------------
    # Indexes for faster Grafana queries
    # --------------------------------------------------------

    events_collection.create_index(
        [("event_id", ASCENDING)],
        unique=True
    )

    events_collection.create_index(
        [("event_timestamp", DESCENDING)]
    )

    events_collection.create_index(
        [("order_id", ASCENDING)]
    )

    events_collection.create_index(
        [("event_type", ASCENDING)]
    )

    orders_collection.create_index(
        [("order_id", ASCENDING)],
        unique=True
    )

    orders_collection.create_index(
        [("event_timestamp", DESCENDING)]
    )

    orders_collection.create_index(
        [("location", ASCENDING)]
    )

    orders_collection.create_index(
        [("cuisine", ASCENDING)]
    )

    return client, db, events_collection, orders_collection


# ============================================================
# KAFKA CONSUMER
# ============================================================

def create_consumer():

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="food_delivery_consumer_group_v2",
        value_deserializer=lambda x: json.loads(
            x.decode("utf-8")
        )
    )

    return consumer


# ============================================================
# UPDATE ORDER IN MONGODB
# ============================================================

def update_order_collection(orders_collection, record):

    order_id = record.get("order_id")

    if not order_id:
        return

    # --------------------------------------------------------
    # Store the latest state of the order
    # --------------------------------------------------------

    orders_collection.update_one(
        {"order_id": order_id},
        {
            "$set": record,
            "$setOnInsert": {
                "order_id": order_id
            }
        },
        upsert=True
    )


# ============================================================
# SAVE CSV
# ============================================================

def save_to_csv(records):

    if not records:
        return

    fieldnames = [
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

    with open(
        OUTPUT_FILE,
        "w",
        newline="",
        encoding="utf-8"
    ) as file:

        writer = csv.DictWriter(
            file,
            fieldnames=fieldnames
        )

        writer.writeheader()

        for record in records:

            writer.writerow({
                field: record.get(field, "")
                for field in fieldnames
            })


# ============================================================
# MAIN CONSUMER
# ============================================================

def consume_events():

    print("=" * 75)
    print("FOOD DELIVERY KAFKA CONSUMER")
    print("KAFKA → CSV + MONGODB")
    print("=" * 75)

    print(f"Kafka Broker : {KAFKA_BROKER}")
    print(f"Kafka Topic  : {KAFKA_TOPIC}")
    print(f"Output File  : {OUTPUT_FILE}")
    print(f"MongoDB      : {MONGO_DATABASE}")
    print("=" * 75)

    # --------------------------------------------------------
    # Connect to MongoDB
    # --------------------------------------------------------

    try:

        mongo_client, db, events_collection, orders_collection = (
            create_mongodb_connection()
        )

        # Test MongoDB connection
        mongo_client.admin.command("ping")

        print("\nMongoDB connection successful.")
        print(f"Database : {MONGO_DATABASE}")
        print("Collections : events, orders")

    except Exception as error:

        print("\nMongoDB connection failed.")
        print(f"Error: {error}")

        return

    # --------------------------------------------------------
    # Create Kafka consumer
    # --------------------------------------------------------

    consumer = create_consumer()

    records = []

    print("\nReading events from Kafka...\n")

    try:

        for message in consumer:

            record = message.value

            records.append(record)

            # ------------------------------------------------
            # Save event to MongoDB
            # ------------------------------------------------

            try:

                events_collection.update_one(
                    {
                        "event_id": record.get("event_id")
                    },
                    {
                        "$set": record,
                        "$setOnInsert": {
                            "kafka_partition": message.partition,
                            "kafka_offset": message.offset
                        }
                    },
                    upsert=True
                )

                # --------------------------------------------
                # Update current order state
                # --------------------------------------------

                update_order_collection(
                    orders_collection,
                    record
                )

            except Exception as mongo_error:

                print(
                    f"MongoDB error for event "
                    f"{record.get('event_id')}: {mongo_error}"
                )

            # ------------------------------------------------
            # Display event
            # ------------------------------------------------

            print(
                f"Event: {record.get('event_id')} | "
                f"Order: {record.get('order_id')} | "
                f"Type: {record.get('event_type')} | "
                f"Location: {record.get('location')} | "
                f"Partition: {message.partition} | "
                f"Offset: {message.offset}"
            )

            # ------------------------------------------------
            # Stop after 500 events
            # ------------------------------------------------

            if len(records) >= 500:
                break

    except KeyboardInterrupt:

        print("\nConsumer interrupted.")

    except Exception as error:

        print(f"\nConsumer error: {error}")

    finally:

        consumer.close()
        mongo_client.close()

    # ========================================================
    # SAVE CONSUMED DATA TO CSV
    # ========================================================

    save_to_csv(records)

    # ========================================================
    # SUMMARY
    # ========================================================

    print("\n" + "=" * 75)
    print("CONSUMER SUMMARY")
    print("=" * 75)

    print(f"Records consumed : {len(records)}")
    print(f"CSV file         : {OUTPUT_FILE}")
    print("MongoDB database : food_delivery")
    print("MongoDB events   : events")
    print("MongoDB orders   : orders")

    print("=" * 75)


# ============================================================
# PROGRAM ENTRY
# ============================================================

if __name__ == "__main__":
    consume_events()