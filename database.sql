CREATE DATABASE IF NOT EXISTS food_delivery;

USE food_delivery;

CREATE TABLE IF NOT EXISTS food_delivery_events (
    id INT AUTO_INCREMENT PRIMARY KEY,
    event_id VARCHAR(100) NOT NULL UNIQUE,
    event_timestamp DATETIME NULL,
    event_source VARCHAR(100),
    event_type VARCHAR(100),
    order_id VARCHAR(100),
    customer_id VARCHAR(100),
    restaurant_id VARCHAR(100),
    location VARCHAR(150),
    cuisine VARCHAR(100),
    order_value DECIMAL(12,2),
    payment_method VARCHAR(50),
    distance_km DECIMAL(10,2),
    estimated_delivery_min DECIMAL(10,2),
    traffic_level VARCHAR(50),
    weather VARCHAR(100),
    delivery_status VARCHAR(100),
    kafka_partition INT,
    kafka_offset BIGINT,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    INDEX idx_event_timestamp(event_timestamp),
    INDEX idx_event_type(event_type),
    INDEX idx_order_id(order_id),
    INDEX idx_location(location),
    INDEX idx_cuisine(cuisine),
    INDEX idx_restaurant(restaurant_id),
    INDEX idx_ingested_at(ingested_at)
);


CREATE TABLE IF NOT EXISTS orders_current (
    order_id VARCHAR(100) PRIMARY KEY,
    customer_id VARCHAR(100),
    restaurant_id VARCHAR(100),
    location VARCHAR(150),
    cuisine VARCHAR(100),
    order_value DECIMAL(12,2),
    payment_method VARCHAR(50),
    distance_km DECIMAL(10,2),
    estimated_delivery_min DECIMAL(10,2),
    current_event_type VARCHAR(100),
    delivery_status VARCHAR(100),
    first_event_timestamp DATETIME NULL,
    last_event_timestamp DATETIME NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ON UPDATE CURRENT_TIMESTAMP
);


CREATE TABLE IF NOT EXISTS order_metrics (
    order_id VARCHAR(100) PRIMARY KEY,
    customer_id VARCHAR(100),
    restaurant_id VARCHAR(100),
    location VARCHAR(150),
    cuisine VARCHAR(100),
    order_value DECIMAL(12,2),
    payment_method VARCHAR(50),
    distance_km DECIMAL(10,2),
    estimated_delivery_min DECIMAL(10,2),
    actual_delivery_min DECIMAL(10,2),
    delay_min DECIMAL(10,2),
    on_time_flag TINYINT,
    final_status VARCHAR(100),
    order_timestamp DATETIME NULL,
    delivery_timestamp DATETIME NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ON UPDATE CURRENT_TIMESTAMP
);


CREATE TABLE IF NOT EXISTS delivery_alerts (
    alert_id INT AUTO_INCREMENT PRIMARY KEY,
    order_id VARCHAR(100),
    alert_type VARCHAR(100),
    category VARCHAR(100),
    severity VARCHAR(30),
    status VARCHAR(30),
    description TEXT,
    triggered_at DATETIME,
    resolved_at DATETIME NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    INDEX idx_alert_order(order_id),
    INDEX idx_alert_type(alert_type),
    INDEX idx_alert_severity(severity),
    INDEX idx_alert_status(status),
    INDEX idx_alert_time(triggered_at)
);


CREATE TABLE IF NOT EXISTS data_quality_log (
    quality_id INT AUTO_INCREMENT PRIMARY KEY,
    event_id VARCHAR(100),
    issue_type VARCHAR(100),
    issue_description TEXT,
    severity VARCHAR(30),
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    INDEX idx_quality_event(event_id),
    INDEX idx_quality_issue(issue_type)
);


CREATE OR REPLACE VIEW latest_order_status AS
SELECT *
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY order_id
               ORDER BY event_timestamp DESC, id DESC
           ) AS rn
    FROM food_delivery_events
) ranked
WHERE rn = 1;