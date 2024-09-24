import asyncio
import os
import collections
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from janusgraph_python.driver.serializer import JanusGraphSONSerializersV3d0
from gremlin_python.process.anonymous_traversal import traversal

# Set the correct event loop policy for Windows
if os.name == 'nt':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# Set PySpark environment variables
os.environ['PYSPARK_PYTHON'] = 'D:\\Anaconda\\envs\\etl_py39\\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = 'D:\\Anaconda\\envs\\etl_py39\\python.exe'

# Workaround for MutableMapping error
collections.MutableMapping = collections.abc.MutableMapping


data = [
    {'src': 1, 'dst': 2},
    {'src': 2, 'dst': 3}
]

# Connect to JanusGraph
connection = DriverRemoteConnection(
    'ws://localhost:8182/gremlin', 'g',
    message_serializer=JanusGraphSONSerializersV3d0())

g = traversal().with_remote(connection)

# Check and add the vertex "hercules" if it doesn't exist
if not g.V().has("demigod", "name", "hercules").hasNext():
    g.addV("demigod").property("name", "hercules").property("age", 30).next()

# Query Hercules' age
hercules_age = g.V().has("demigod", "name", "hercules").values("age").next()
print(f"Hercules age: {hercules_age}")
