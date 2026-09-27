import os
import json
import csv
from datetime import datetime, timezone

from kafka import KafkaConsumer
from pymongo import MongoClient, ASCENDING, DESCENDING
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv


# ============================================================
# LOAD ENVIRONMENT VARIABLES
# ============================================================

load_dotenv()


# ============================================================
# CONFIGURATION
# ============================================================

KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "food_delivery_events"

OUTPUT_FILE = "data/consumed_orders.csv"

MYSQL_HOST = "localhost"
from dotenv import load_dotenv

load_dotenv()
MYSQL_PORT = 3306
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "food_delivery")
MYSQL_USER = os.getenv("MYSQL_USER", "fooddelivery")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "fooddelivery123")

MONGO_URI = os.getenv("MONGODB_URI")
MONGO_DATABASE = "food_delivery"

CONSUMER_GROUP = "food_delivery_intelligence_v1"

MAX_EVENTS = 500


# ============================================================
# MYSQL CONNECTION
# ============================================================

def create_mysql_connection():

    connection = mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        database=MYSQL_DATABASE,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD
    )

    return connection


# ============================================================
# MONGODB CONNECTION
# ============================================================

def create_mongodb_connection():

    if not MONGO_URI:
        raise ValueError("MONGODB_URI is missing from .env")

    client = MongoClient(MONGO_URI)

    db = client[MONGO_DATABASE]

    events_collection = db["events"]
    orders_collection = db["orders"]
    alerts_collection = db["alerts"]

    # --------------------------------------------------------
    # EVENT INDEXES
    # --------------------------------------------------------

    events_collection.create_index(
        [("event_id", ASCENDING)],
        unique=True
    )

    events_collection.create_index(
        [("event_timestamp", DESCENDING)]
    )

    events_collection.create_index(
        [("ingested_at", DESCENDING)]
    )

    events_collection.create_index(
        [("order_id", ASCENDING)]
    )

    events_collection.create_index(
        [("event_type", ASCENDING)]
    )

    # --------------------------------------------------------
    # ORDER INDEXES
    # --------------------------------------------------------

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

    # --------------------------------------------------------
    # ALERT INDEXES
    # --------------------------------------------------------

    alerts_collection.create_index(
        [("order_id", ASCENDING)]
    )

    alerts_collection.create_index(
        [("alert_type", ASCENDING)]
    )

    alerts_collection.create_index(
        [("severity", ASCENDING)]
    )

    return (
        client,
        db,
        events_collection,
        orders_collection,
        alerts_collection
    )


# ============================================================
# KAFKA CONSUMER
# ============================================================

def create_consumer():

    consumer = KafkaConsumer(

        KAFKA_TOPIC,

        bootstrap_servers=[KAFKA_BROKER],

        auto_offset_reset="earliest",

        enable_auto_commit=True,

        group_id=CONSUMER_GROUP,

        value_deserializer=lambda x: json.loads(
            x.decode("utf-8")
        )
    )

    return consumer


# ============================================================
# SAFE DATETIME CONVERSION
# ============================================================

def parse_datetime(value):

    if not value:
        return None

    try:

        value = str(value).replace("Z", "")

        return datetime.fromisoformat(value)

    except Exception:

        return None


# ============================================================
# DATA QUALITY CHECK
# ============================================================

def validate_record(record):

    required_fields = [
        "event_id",
        "event_timestamp",
        "event_type",
        "order_id"
    ]

    missing_fields = [
        field
        for field in required_fields
        if not record.get(field)
    ]

    if missing_fields:

        return (
            False,
            "MISSING_REQUIRED_FIELDS",
            f"Missing fields: {', '.join(missing_fields)}"
        )

    if record.get("order_value") is not None:

        try:

            if float(record["order_value"]) < 0:

                return (
                    False,
                    "INVALID_ORDER_VALUE",
                    "Order value cannot be negative"
                )

        except ValueError:

            return (
                False,
                "INVALID_ORDER_VALUE",
                "Order value is not numeric"
            )

    if record.get("distance_km") is not None:

        try:

            if float(record["distance_km"]) < 0:

                return (
                    False,
                    "INVALID_DISTANCE",
                    "Distance cannot be negative"
                )

        except ValueError:

            return (
                False,
                "INVALID_DISTANCE",
                "Distance is not numeric"
            )

    return True, None, None


# ============================================================
# MYSQL RAW EVENT INSERT
# ============================================================

def insert_raw_event(
    connection,
    record,
    partition,
    offset
):

    query = """

    INSERT IGNORE INTO food_delivery_events (

        event_id,
        event_timestamp,
        event_source,
        event_type,
        order_id,
        customer_id,
        restaurant_id,
        location,
        cuisine,
        order_value,
        payment_method,
        distance_km,
        estimated_delivery_min,
        traffic_level,
        weather,
        delivery_status,
        kafka_partition,
        kafka_offset,
        ingested_at

    )

    VALUES (

        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
        %s,%s,%s,%s,%s,%s,%s,%s,%s

    )

    """

    cursor = connection.cursor()

    cursor.execute(
        query,
        (
            record.get("event_id"),
            parse_datetime(record.get("event_timestamp")),
            record.get("event_source"),
            record.get("event_type"),
            record.get("order_id"),
            record.get("customer_id"),
            record.get("restaurant_id"),
            record.get("location"),
            record.get("cuisine"),
            record.get("order_value"),
            record.get("payment_method"),
            record.get("distance_km"),
            record.get("estimated_delivery_min"),
            record.get("traffic_level"),
            record.get("weather"),
            record.get("delivery_status"),
            partition,
            offset,
            datetime.now(timezone.utc).replace(tzinfo=None)
        )
    )

    connection.commit()

    cursor.close()


# ============================================================
# UPDATE CURRENT ORDER STATE
# ============================================================

def update_current_order(
    connection,
    record
):

    order_id = record.get("order_id")

    if not order_id:
        return

    query = """

    INSERT INTO orders_current (

        order_id,
        customer_id,
        restaurant_id,
        location,
        cuisine,
        order_value,
        payment_method,
        distance_km,
        estimated_delivery_min,
        current_event_type,
        delivery_status,
        first_event_timestamp,
        last_event_timestamp

    )

    VALUES (

        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s

    )

    ON DUPLICATE KEY UPDATE

        customer_id = VALUES(customer_id),

        restaurant_id = VALUES(restaurant_id),

        location = VALUES(location),

        cuisine = VALUES(cuisine),

        order_value = VALUES(order_value),

        payment_method = VALUES(payment_method),

        distance_km = VALUES(distance_km),

        estimated_delivery_min = VALUES(estimated_delivery_min),

        current_event_type = VALUES(current_event_type),

        delivery_status = VALUES(delivery_status),

        last_event_timestamp = VALUES(last_event_timestamp)

    """

    event_time = parse_datetime(
        record.get("event_timestamp")
    )

    cursor = connection.cursor()

    cursor.execute(
        query,
        (
            order_id,
            record.get("customer_id"),
            record.get("restaurant_id"),
            record.get("location"),
            record.get("cuisine"),
            record.get("order_value"),
            record.get("payment_method"),
            record.get("distance_km"),
            record.get("estimated_delivery_min"),
            record.get("event_type"),
            record.get("delivery_status"),
            event_time,
            event_time
        )
    )

    connection.commit()

    cursor.close()


# ============================================================
# GET ORDER PLACED DETAILS
# ============================================================

def get_order_placed_data(
    connection,
    order_id
):

    query = """

    SELECT
        event_timestamp,
        estimated_delivery_min,
        order_value,
        customer_id,
        restaurant_id,
        location,
        cuisine,
        payment_method,
        distance_km

    FROM food_delivery_events

    WHERE order_id = %s

      AND event_type = 'ORDER_PLACED'

    ORDER BY event_timestamp ASC

    LIMIT 1

    """

    cursor = connection.cursor(
        dictionary=True
    )

    cursor.execute(
        query,
        (order_id,)
    )

    result = cursor.fetchone()

    cursor.close()

    return result


# ============================================================
# CREATE DELIVERY METRICS
# ============================================================

def create_delivery_metrics(
    connection,
    record
):

    if record.get("event_type") != "ORDER_DELIVERED":

        return

    order_id = record.get("order_id")

    delivered_time = parse_datetime(
        record.get("event_timestamp")
    )

    placed_data = get_order_placed_data(
        connection,
        order_id
    )

    if not placed_data:

        return

    placed_time = placed_data["event_timestamp"]

    if not placed_time or not delivered_time:

        return

    actual_minutes = (
        delivered_time - placed_time
    ).total_seconds() / 60

    estimated_minutes = (
        float(
            placed_data["estimated_delivery_min"]
        )
        if placed_data["estimated_delivery_min"]
        is not None
        else None
    )

    delay_minutes = None
    on_time_flag = None

    if estimated_minutes is not None:

        delay_minutes = (
            actual_minutes -
            estimated_minutes
        )

        on_time_flag = (
            1 if delay_minutes <= 0
            else 0
        )

    query = """

    INSERT INTO order_metrics (

        order_id,
        customer_id,
        restaurant_id,
        location,
        cuisine,
        order_value,
        payment_method,
        distance_km,
        estimated_delivery_min,
        actual_delivery_min,
        delay_min,
        on_time_flag,
        final_status,
        order_timestamp,
        delivery_timestamp

    )

    VALUES (

        %s,%s,%s,%s,%s,%s,%s,%s,
        %s,%s,%s,%s,%s,%s,%s

    )

    ON DUPLICATE KEY UPDATE

        actual_delivery_min =
            VALUES(actual_delivery_min),

        delay_min =
            VALUES(delay_min),

        on_time_flag =
            VALUES(on_time_flag),

        final_status =
            VALUES(final_status),

        delivery_timestamp =
            VALUES(delivery_timestamp)

    """

    cursor = connection.cursor()

    cursor.execute(
        query,
        (
            order_id,
            placed_data["customer_id"],
            placed_data["restaurant_id"],
            placed_data["location"],
            placed_data["cuisine"],
            placed_data["order_value"],
            placed_data["payment_method"],
            placed_data["distance_km"],
            estimated_minutes,
            actual_minutes,
            delay_minutes,
            on_time_flag,
            record.get("delivery_status"),
            placed_time,
            delivered_time
        )
    )

    connection.commit()

    cursor.close()


# ============================================================
# ALERT ENGINE
# ============================================================

def generate_alerts(
    connection,
    record
):

    order_id = record.get("order_id")

    if not order_id:
        return

    alerts = []

    traffic = str(
        record.get("traffic_level", "")
    ).lower()

    weather = str(
        record.get("weather", "")
    ).lower()

    try:
        distance = float(
            record.get("distance_km", 0)
        )
    except:
        distance = 0

    try:
        order_value = float(
            record.get("order_value", 0)
        )
    except:
        order_value = 0

    # --------------------------------------------------------
    # TRAFFIC ALERT
    # --------------------------------------------------------

    if traffic in ["high", "severe"]:

        severity = (
            "CRITICAL"
            if traffic == "severe"
            else "HIGH"
        )

        alerts.append(
            (
                "SEVERE_TRAFFIC",
                "TRAFFIC",
                severity,
                f"Traffic level: {traffic}"
            )
        )

    # --------------------------------------------------------
    # WEATHER ALERT
    # --------------------------------------------------------

    if weather in [
        "heavy rain",
        "storm",
        "severe"
    ]:

        alerts.append(
            (
                "HEAVY_RAIN",
                "WEATHER",
                "HIGH",
                f"Weather condition: {weather}"
            )
        )

    # --------------------------------------------------------
    # LONG DISTANCE
    # --------------------------------------------------------

    if distance >= 10:

        alerts.append(
            (
                "LONG_DISTANCE",
                "DELIVERY",
                "MEDIUM",
                f"Delivery distance: {distance} km"
            )
        )

    # --------------------------------------------------------
    # HIGH VALUE ORDER
    # --------------------------------------------------------

    if order_value >= 2000:

        alerts.append(
            (
                "HIGH_VALUE_ORDER",
                "COMMERCIAL",
                "MEDIUM",
                f"Order value: {order_value}"
            )
        )

    # --------------------------------------------------------
    # STORE ALERTS
    # --------------------------------------------------------

    for alert_type, category, severity, description in alerts:

        query = """

        SELECT alert_id

        FROM delivery_alerts

        WHERE order_id = %s

          AND alert_type = %s

        LIMIT 1

        """

        cursor = connection.cursor()

        cursor.execute(
            query,
            (
                order_id,
                alert_type
            )
        )

        exists = cursor.fetchone()

        cursor.close()

        if exists:
            continue

        insert_query = """

        INSERT INTO delivery_alerts (

            order_id,
            alert_type,
            category,
            severity,
            status,
            description,
            triggered_at

        )

        VALUES (

            %s,%s,%s,%s,'OPEN',%s,%s

        )

        """

        cursor = connection.cursor()

        cursor.execute(
            insert_query,
            (
                order_id,
                alert_type,
                category,
                severity,
                description,
                parse_datetime(
                    record.get("event_timestamp")
                )
            )
        )

        connection.commit()

        cursor.close()


# ============================================================
# DATA QUALITY LOGGING
# ============================================================

def log_quality_issue(
    connection,
    record,
    issue_type,
    description
):

    query = """

    INSERT INTO data_quality_log (

        event_id,
        issue_type,
        issue_description,
        severity

    )

    VALUES (

        %s,%s,%s,%s

    )

    """

    cursor = connection.cursor()

    cursor.execute(
        query,
        (
            record.get("event_id"),
            issue_type,
            description,
            "HIGH"
        )
    )

    connection.commit()

    cursor.close()


# ============================================================
# MONGODB EVENT
# ============================================================

def save_mongodb_event(
    events_collection,
    record,
    partition,
    offset
):

    event_document = dict(record)

    event_document["kafka_partition"] = partition

    event_document["kafka_offset"] = offset

    event_document["ingested_at"] = datetime.now(
        timezone.utc
    )

    events_collection.update_one(

        {
            "event_id": record.get("event_id")
        },

        {
            "$set": event_document
        },

        upsert=True
    )


# ============================================================
# MONGODB CURRENT ORDER
# ============================================================

def update_mongodb_order(
    orders_collection,
    record
):

    order_id = record.get("order_id")

    if not order_id:
        return

    record_copy = dict(record)

    record_copy["updated_at"] = datetime.now(
        timezone.utc
    )

    orders_collection.update_one(

        {
            "order_id": order_id
        },

        {
            "$set": record_copy
        },

        upsert=True
    )


# ============================================================
# MONGODB ALERT
# ============================================================

def save_mongodb_alert(
    alerts_collection,
    record
):

    traffic = str(
        record.get("traffic_level", "")
    ).lower()

    weather = str(
        record.get("weather", "")
    ).lower()

    order_id = record.get("order_id")

    if traffic not in ["high", "severe"] and \
       weather not in ["heavy rain", "storm", "severe"]:

        return

    alert_type = (
        "SEVERE_TRAFFIC"
        if traffic in ["high", "severe"]
        else "HEAVY_RAIN"
    )

    severity = (
        "CRITICAL"
        if traffic == "severe"
        else "HIGH"
    )

    alert = {

        "order_id": order_id,

        "alert_type": alert_type,

        "severity": severity,

        "status": "OPEN",

        "triggered_at": parse_datetime(
            record.get("event_timestamp")
        ),

        "created_at": datetime.now(
            timezone.utc
        )
    }

    alerts_collection.update_one(

        {
            "order_id": order_id,
            "alert_type": alert_type
        },

        {
            "$set": alert
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

                field:
                record.get(field, "")

                for field in fieldnames

            })


# ============================================================
# MAIN CONSUMER
# ============================================================

def consume_events():

    print("=" * 80)

    print(
        "FOOD DELIVERY STREAMING INTELLIGENCE PLATFORM"
    )

    print(
        "KAFKA → MYSQL + MONGODB + CSV"
    )

    print("=" * 80)

    print(
        f"Kafka Broker : {KAFKA_BROKER}"
    )

    print(
        f"Kafka Topic  : {KAFKA_TOPIC}"
    )

    print(
        f"MySQL DB     : {MYSQL_DATABASE}"
    )

    print(
        f"MongoDB DB   : {MONGO_DATABASE}"
    )

    print("=" * 80)


    # ========================================================
    # MYSQL
    # ========================================================

    try:

        mysql_connection = (
            create_mysql_connection()
        )

        print(
            "\nMySQL connection successful."
        )

    except Error as error:

        print(
            "\nMySQL connection failed:"
        )

        print(error)

        return


    # ========================================================
    # MONGODB
    # ========================================================

    try:

        (
            mongo_client,
            mongo_db,
            events_collection,
            orders_collection,
            alerts_collection
        ) = create_mongodb_connection()

        mongo_client.admin.command(
            "ping"
        )

        print(
            "MongoDB connection successful."
        )

    except Exception as error:

        print(
            "\nMongoDB connection failed:"
        )

        print(error)

        mysql_connection.close()

        return


    # ========================================================
    # KAFKA
    # ========================================================

    try:

        consumer = create_consumer()

    except Exception as error:

        print(
            "\nKafka connection failed:"
        )

        print(error)

        mysql_connection.close()

        mongo_client.close()

        return


    records = []

    print(
        "\nReading events from Kafka...\n"
    )


    # ========================================================
    # PROCESS EVENTS
    # ========================================================

    try:

        for message in consumer:

            record = message.value

            records.append(record)


            # ------------------------------------------------
            # DATA QUALITY
            # ------------------------------------------------

            valid, issue_type, description = (
                validate_record(record)
            )

            if not valid:

                log_quality_issue(

                    mysql_connection,

                    record,

                    issue_type,

                    description

                )

                print(
                    f"DATA QUALITY ISSUE: "
                    f"{record.get('event_id')} "
                    f"| {issue_type}"
                )


            # ------------------------------------------------
            # MYSQL RAW EVENT
            # ------------------------------------------------

            try:

                insert_raw_event(

                    mysql_connection,

                    record,

                    message.partition,

                    message.offset

                )

            except Exception as error:

                print(
                    f"MySQL raw event error: "
                    f"{error}"
                )


            # ------------------------------------------------
            # CURRENT ORDER STATE
            # ------------------------------------------------

            try:

                update_current_order(

                    mysql_connection,

                    record

                )

            except Exception as error:

                print(
                    f"Order state error: "
                    f"{error}"
                )


            # ------------------------------------------------
            # DELIVERY METRICS
            # ------------------------------------------------

            try:

                create_delivery_metrics(

                    mysql_connection,

                    record

                )

            except Exception as error:

                print(
                    f"SLA metric error: "
                    f"{error}"
                )


            # ------------------------------------------------
            # ALERT ENGINE
            # ------------------------------------------------

            try:

                generate_alerts(

                    mysql_connection,

                    record

                )

            except Exception as error:

                print(
                    f"Alert engine error: "
                    f"{error}"
                )


            # ------------------------------------------------
            # MONGODB EVENT
            # ------------------------------------------------

            try:

                save_mongodb_event(

                    events_collection,

                    record,

                    message.partition,

                    message.offset

                )

                update_mongodb_order(

                    orders_collection,

                    record

                )

                save_mongodb_alert(

                    alerts_collection,

                    record

                )

            except Exception as error:

                print(
                    f"MongoDB error: {error}"
                )


            # ------------------------------------------------
            # DISPLAY
            # ------------------------------------------------

            print(

                f"Event: "
                f"{record.get('event_id')} | "

                f"Order: "
                f"{record.get('order_id')} | "

                f"Type: "
                f"{record.get('event_type')} | "

                f"Location: "
                f"{record.get('location')} | "

                f"Partition: "
                f"{message.partition} | "

                f"Offset: "
                f"{message.offset}"

            )


            # ------------------------------------------------
            # STOP AFTER DATASET
            # ------------------------------------------------

            if len(records) >= MAX_EVENTS:

                break


    except KeyboardInterrupt:

        print(
            "\nConsumer interrupted."
        )


    except Exception as error:

        print(
            f"\nConsumer error: {error}"
        )


    finally:

        consumer.close()

        mysql_connection.close()

        mongo_client.close()


    # ========================================================
    # CSV
    # ========================================================

    save_to_csv(records)


    # ========================================================
    # SUMMARY
    # ========================================================

    print(
        "\n" + "=" * 80
    )

    print(
        "CONSUMER SUMMARY"
    )

    print(
        "=" * 80
    )

    print(
        f"Records consumed : "
        f"{len(records)}"
    )

    print(
        f"CSV file         : "
        f"{OUTPUT_FILE}"
    )

    print(
        "MySQL tables     : "
        "events, orders, metrics, alerts, quality"
    )

    print(
        "MongoDB          : "
        "events, orders, alerts"
    )

    print(
        "=" * 80
    )


# ============================================================
# PROGRAM ENTRY
# ============================================================

if __name__ == "__main__":

    consume_events()