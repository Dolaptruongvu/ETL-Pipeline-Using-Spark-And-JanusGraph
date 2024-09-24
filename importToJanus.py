import os
import collections
import asyncio
from pyspark.sql import SparkSession
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from janusgraph_python.driver.serializer import JanusGraphSONSerializersV3d0
from gremlin_python.process.anonymous_traversal import traversal
from gremlin_python.process.graph_traversal import __


# Set the correct event loop policy for Windows
if os.name == 'nt':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# Set PySpark environment variables
os.environ['PYSPARK_PYTHON'] = 'D:\\Anaconda\\envs\\etl_py39\\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = 'D:\\Anaconda\\envs\\etl_py39\\python.exe'

# Workaround for MutableMapping error
collections.MutableMapping = collections.abc.MutableMapping

# Initialize SparkSession
spark = SparkSession.builder.appName("ReadCSVToJanusGraph").getOrCreate()

# Read CSV with utf-8 encoding
csv_file_path = './dataset/london_tube_map.csv'
print(f"Reading CSV file: {csv_file_path} with UTF-8 encoding...")

spark_df = spark.read.option("header", "true").option("encoding", "utf-8").csv(csv_file_path)
print(f"CSV file loaded successfully. Number of rows: {spark_df.count()}")


print("Displaying the first 5 rows of the DataFrame:")
spark_df.show(5, truncate=False)

# Connect to JanusGraph
connection = DriverRemoteConnection(
    'ws://localhost:8182/gremlin', 'g',
    message_serializer=JanusGraphSONSerializersV3d0()
)
g = traversal().with_remote(connection)

# Load data from DataFrame into JanusGraph
print("Starting to process data from DataFrame...")
for row in spark_df.collect():
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
        print(f"Creating edge from {src} to {dst}")
        g.V().has('station', 'name', src).addE('connected_to').to(__.V().has('station', 'name', dst)).next()

    except Exception as e:
        print(f"Error processing src: {src}, dst: {dst} - {e}")


try:
    print("Closing JanusGraph connection...")
    connection.close()
    print("JanusGraph connection closed.")
except Exception as e:
    print(f"Error closing JanusGraph connection: {e}")


try:
    print("Stopping Spark session...")
    spark.stop()
    print("Spark session stopped.")
except Exception as e:
    print(f"Error stopping Spark session: {e}")

print("Data has been successfully loaded into JanusGraph.")
