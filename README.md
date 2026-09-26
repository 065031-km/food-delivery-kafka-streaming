\# Food Delivery Kafka Streaming



Real-time food-delivery streaming pipeline using Python and Apache Kafka.



\## Overview



This project simulates a real-time food-delivery event streaming system. It generates and processes 500 sample food-delivery events and publishes them to Apache Kafka.



\## Technologies



\- Python

\- Apache Kafka

\- Docker

\- Pandas

\- CSV

\- JSON



\## Kafka Configuration



Kafka Broker: localhost:9092



Kafka Topic: food\_delivery\_events



Partitions: 3



\## Dataset



The sample dataset contains 500 food-delivery event records.



Events include:



\- ORDER\_PLACED

\- RESTAURANT\_ACCEPTED

\- RIDER\_ASSIGNED

\- ORDER\_PICKED\_UP

\- ORDER\_DELIVERED



The dataset contains information about customers, restaurants, locations, cuisines, order values, payment methods, delivery distance, estimated delivery time, traffic, weather, and delivery status.



\## Producer



The producer reads sample\_orders.csv, validates each record, converts it to JSON, and sends it to the food\_delivery\_events Kafka topic.



The order\_id is used as the Kafka message key.



Events are streamed at one event per second to simulate real-time data.



\## Consumer



The consumer reads events from Kafka and stores them in:



data/consumed\_orders.csv



\## Data Cleaning



The clean\_data.py script cleans the consumed data and creates:



data/cleaned\_orders.csv



\## Analytics



The analytics script calculates statistics including:



\- Total order value

\- Average order value

\- Average delivery distance

\- Average estimated delivery time

\- Orders by location

\- Orders by cuisine

\- Payment methods

\- Traffic conditions

\- Weather conditions



\## Results



The producer successfully processed 500 records.



Successfully sent: 500



Failed records: 0

