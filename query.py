import asyncio
import os
import collections
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from janusgraph_python.driver.serializer import JanusGraphSONSerializersV3d0
from gremlin_python.process.anonymous_traversal import traversal
from gremlin_python.process.graph_traversal import __
from gremlin_python.process.traversal import Order
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
    message_serializer=JanusGraphSONSerializersV3d0())

g = traversal().with_remote(connection)

# Source and dest example to find shortest path
source_station = 'Oxford_Circus'
dest_station = 'West_Croydon'

# loop and find the shortest path
shortest_path = g.V().has('station','name',source_station).repeat(__.out('connected_to').simplePath()).until(__.has('station','name',dest_station)).path().limit(2).toList()

# loop and show paths
print("shortest path :")
for path in shortest_path:
    print(path)
    for step in path:
        if isinstance(step, dict):  # Vertex is represented as a dictionary
            print("Vertex:", step)
        else:
            step_vertex_details = g.V(step.id).valueMap(True).next()
            vertex_name = step_vertex_details.get('name',['N/a'])[0]
            print(f"Edge -> Vertex: {vertex_name}")

# find three prominent stations
print("prominent stations :")

prominent_stations = g.V().hasLabel('station').order().by(__.both().count(),Order.desc).limit(3).valueMap(True).toList()

#show prominent stations
for station in prominent_stations:
    print(station.get('name','N/a')[0])



connection.close()