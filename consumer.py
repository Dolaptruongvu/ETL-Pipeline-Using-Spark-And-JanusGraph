from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.streaming import StreamingQueryListener
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.process.anonymous_traversal import traversal
from gremlin_python.process.graph_traversal import __
from gremlin_python.process.traversal import Order
from janusgraph_python.driver.serializer import JanusGraphSONSerializersV3d0
import os
import collections
import asyncio

# Set the correct event loop policy for Windows
if os.name == 'nt':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# Set PySpark environment variables
os.environ['PYSPARK_PYTHON'] = 'D:\\Anaconda\\envs\\etl_py39\\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = 'D:\\Anaconda\\envs\\etl_py39\\python.exe'

# Workaround for MutableMapping error
collections.MutableMapping = collections.abc.MutableMapping


# Connect to JanusGraph
connection = DriverRemoteConnection(
    'ws://localhost:8182/gremlin', 'g',
    message_serializer=JanusGraphSONSerializersV3d0()
)
g = traversal().with_remote(connection)



spark = SparkSession.builder \
        .appName("KafkatoJanus") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
        .getOrCreate()

schema = StructType([
    StructField('src',StringType(),True),
    StructField('dst',StringType(),True)
])

kafka_stream = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "172.27.16.1:9093") \
            .option("subscribe","station_topic") \
            .option("startingOffsets", "latest") \
            .load()

stream_df = kafka_stream.selectExpr("CAST(value AS STRING)") \
            .select(from_json(col("value"),schema).alias("data")) \
            .select("data.*")

# Drop Edges
# g.E().drop().iterate()

# Drop Vertices
# g.V().drop().iterate()

def add_to_janus(batch_df,batch_id):
    data = batch_df.collect()
    print("Starting to process data from DataFrame...")
    for row in data:
        src = row['src']
        dst = row['dst']
        
        try:
            
            print(f"Processing row: src = {src}, dst = {dst}")
            
            # Check if the source station exists
            if not g.V().has('station', 'name', src).hasNext():
                print(f"Adding vertex for source station: {src}")
                g.addV('station').property('name', src).next()
            
            # Check if the destination station exists
            if not g.V().has('station', 'name', dst).hasNext():
                print(f"Adding vertex for destination station: {dst}")
                g.addV('station').property('name', dst).next()

            # Create an edge
            if not g.V().has('station','name',src).out('connected_to').has('station','name',dst).hasNext():
                print(f"Creating edge from {src} to {dst}")
                g.V().has('station', 'name', src).addE('connected_to').to(__.V().has('station', 'name', dst)).next()
            else:
                print(f"Edge from {src} to {dst} already exists, skipping creation.")

        except Exception as e:
            print(f"Error processing src: {src}, dst: {dst} - {e}")


stream_query = stream_df.writeStream \
               .outputMode("append") \
               .foreachBatch(add_to_janus) \
               .start()

stream_query.awaitTermination()

try:
    print("Closing JanusGraph connection...")
    connection.close()
    print("JanusGraph connection closed.")
except Exception as e:
    print(f"Error closing JanusGraph connection: {e}")