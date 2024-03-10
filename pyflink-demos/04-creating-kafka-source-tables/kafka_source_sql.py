import logging
import sys

from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaProducer, KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.formats.json import JsonRowSerializationSchema, JsonRowDeserializationSchema
from pyflink.table import StreamTableEnvironment, DataTypes, EnvironmentSettings
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.table.descriptors import Schema

# Make sure that the Kafka cluster is started and the topic 'test_json_topic' is
# created before executing this job.
def write_to_kafka(env):
    type_info = Types.ROW_NAMED(['id', 'name', 'score'],[Types.INT(), Types.STRING(),Types.INT()])

    ds = env.from_collection(
        [(1, 'Thanh', 10), (2, 'Hang', 3), (3, 'Dau', 5)],
        type_info=type_info)

    serialization_schema = JsonRowSerializationSchema.Builder() \
        .with_type_info(type_info) \
        .build()
    
    kafka_producer = FlinkKafkaProducer(
        topic='kafka_pyflink_src',
        serialization_schema=serialization_schema,
        producer_config={'bootstrap.servers': 'localhost:9092', 'group.id': 'producer-from-pyflink'}
    )

    # note that the output type of ds must be RowTypeInfo
    ds.add_sink(kafka_producer)
    env.execute()

# Using SQL
def read_from_kafka(env, tbl_env:StreamTableEnvironment):
    
    tbl_env.execute_sql("""
        CREATE TABLE KafkaTable (
            `partition` BIGINT METADATA VIRTUAL,
            `offset` BIGINT METADATA VIRTUAL,
            id INT,
            name STRING,
            score INT
            ) WITH (
            'connector' = 'kafka',
            'topic' = 'kafka_pyflink_src',
            'properties.bootstrap.servers' = '10.9.18.10:9092',
            'properties.group.id' = 'consumer-py-flink',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json'
            )
            """
        )
    
    tbl_env.execute_sql("""
        CREATE TABLE KafkaTableCate (
            `partition` BIGINT METADATA VIRTUAL,
            `offset` BIGINT METADATA VIRTUAL,
            name STRING,
            que STRING
            ) WITH (
            'connector' = 'kafka',
            'topic' = 'kafka_pyflink_src_cate',
            'properties.bootstrap.servers' = '10.9.18.10:9092',
            'properties.group.id' = 'consumer-py-flink-cate',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json'
            )
            """
        )
    
    # tbl_kafka = tbl_env.from_path('KafkaTable')
    # tbl_kafka.print_schema()
    # tbl_env.execute_sql("""
    #                     SELECT name, COUNT(*) AS cnt 
    #                     FROM KafkaTable GROUP BY name 
    #                     """).print()

    print('='*100)

    # tbl_env.execute_sql("""
    #                     SELECT DISTINCT name, score
    #                     FROM KafkaTable
    #                     WHERE score < 5
    #                     """).print()

    tbl_env.execute_sql("""
                        SELECT t.name, c.que
                        FROM KafkaTable t
                        JOIN KafkaTableCate c
                        ON t.name = c.name
                        """
                        ).print()

    env.execute("kafka-source-demo-pyflink")


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    env = StreamExecutionEnvironment.get_execution_environment()
    settings = EnvironmentSettings.new_instance()\
                      .in_streaming_mode()\
                      .build()

    # create table environment
    tbl_env = StreamTableEnvironment.create(stream_execution_environment=env,
                                            environment_settings=settings)

    env.add_jars("file:////home/ec2-user/flink/code/pyflink-demos/04-creating-kafka-source-tables/flink-sql-connector-kafka-3.1.0-1.18.jar")
    # env.add_jars("file:////home/ec2-user/flink/code/pyflink-demos/04-creating-kafka-source-tables/flink-connector-kafka-3.1.0-1.18.jar")

    # print("start writing data to kafka")
    write_to_kafka(env)

    print("start reading data from kafka")
    read_from_kafka(env, tbl_env)