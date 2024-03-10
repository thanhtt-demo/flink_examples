Create Confluent Kafka development cluster and load data into Kafak

1) Launch Confluent Kafka Docker Environment

In same directory as docker-compose.yaml file from course materials run the following.


docker-compose up -d


2) Create Kafka source topic named productsales

docker exec -it broker kafka-topics --create \
    --bootstrap-server localhost:9092 \
    --topic productsales


3) Download the Kafka Connector Jar using HTTPie HTTP Shell Client

Run the following from the same directory as the kafka_sink.py file downloaded from course materials


http --download https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka_2.11/1.13.0/flink-sql-connector-kafka_2.11-1.13.0.jar


4) Run kafka_sink.py program

python kafka_source.py


5) In another terminal / cmd prompt start a kafka console producer to write data to productsales topic

docker exec -it broker kafka-console-producer \
    --bootstrap-server localhost:9092 \
    --topic productsales


6) Paste the following into the console producer then CTRL + C to exit

{"seller_id": "LNK", "product": "Toothbrush", "quantity": 22, "product_price": 3.99, "sales_date": "2021-07-01"}
{"seller_id": "LNK", "product": "Dental Floss", "quantity": 17, "product_price": 1.99, "sales_date": "2021-07-01"}
{"seller_id": "LNK", "product": "Toothpaste", "quantity": 8, "product_price": 4.99, "sales_date": "2021-07-01"}
{"seller_id": "OMA", "product": "Toothbrush", "quantity": 29, "product_price": 3.99, "sales_date": "2021-07-01"}
{"seller_id": "OMA", "product": "Toothpaste", "quantity": 9, "product_price": 4.99, "sales_date": "2021-07-01"}
{"seller_id": "OMA", "product": "Dental Floss", "quantity": 23, "product_price": 1.99, "sales_date": "2021-07-01"}
{"seller_id": "LNK", "product": "Toothbrush", "quantity": 25, "product_price": 3.99, "sales_date": "2021-07-02"}
{"seller_id": "LNK", "product": "Dental Floss", "quantity": 16, "product_price": 1.99, "sales_date": "2021-07-02"}
{"seller_id": "LNK", "product": "Toothpaste", "quantity": 9, "product_price": 4.99, "sales_date": "2021-07-02"}
{"seller_id": "OMA", "product": "Toothbrush", "quantity": 32, "product_price": 3.99, "sales_date": "2021-07-02"}
{"seller_id": "OMA", "product": "Toothpaste", "quantity": 13, "product_price": 4.99, "sales_date": "2021-07-02"}
{"seller_id": "OMA", "product": "Dental Floss", "quantity": 18, "product_price": 1.99, "sales_date": "2021-07-02"}
{"seller_id": "LNK", "product": "Toothbrush", "quantity": 20, "product_price": 3.99, "sales_date": "2021-07-03"}
{"seller_id": "LNK", "product": "Dental Floss", "quantity": 15, "product_price": 1.99, "sales_date": "2021-07-03"}
{"seller_id": "LNK", "product": "Toothpaste", "quantity": 11, "product_price": 4.99, "sales_date": "2021-07-03"}
{"seller_id": "OMA", "product": "Toothbrush", "quantity": 31, "product_price": 3.99, "sales_date": "2021-07-03"}
{"seller_id": "OMA", "product": "Toothpaste", "quantity": 10, "product_price": 4.99, "sales_date": "2021-07-03"}
{"seller_id": "OMA", "product": "Dental Floss", "quantity": 21, "product_price": 1.99, "sales_date": "2021-07-03"}


7) Run kafka-console-consumer to be sure data was loaded into source topic productsales

docker exec -it broker kafka-console-consumer --from-beginning \
    --bootstrap-server localhost:9092 \
    --topic productsales


8) Clean up docker based Kafka environment

Run the following from the same directory as docker-compose.yaml file downloaded from course materials


docker-compose down -v
