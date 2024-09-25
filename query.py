import asyncio
import os
import collections
import graphistry
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from janusgraph_python.driver.serializer import JanusGraphSONSerializersV3d0
from gremlin_python.process.anonymous_traversal import traversal
from gremlin_python.process.graph_traversal import __
from gremlin_python.process.traversal import Order
import pandas as pd
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

# Register Graphistry
graphistry.register(api=3, username='dolaptruongvu5', password='', protocol='https', server='hub.graphistry.com')
# Source and dest example to find shortest path
source_station = 'Oxford_Circus'
dest_station = 'West_Croydon'

# loop and find the shortest path
shortest_path = g.V().has('station','name',source_station).repeat(__.out('connected_to').simplePath()).until(__.has('station','name',dest_station)).path().limit(1).toList()


edges_data =[]
vertices_data = set()
# loop and show paths
print("shortest path :")
for path in shortest_path:
    print(path)
    for i in range(len(path)-1):
        # print(step)
        src_vertex_details = g.V(path[i].id).valueMap(True).next()
        dst_vertex_details = g.V(path[i+1].id).valueMap(True).next()
        # print(step_vertex_details)
        src_name = src_vertex_details.get('name',['N/a'])[0]
        dst_name = dst_vertex_details.get('name',['N/a'])[0]
        
        # Add src and dst to vertices set
        vertices_data.add(src_name)
        vertices_data.add(dst_name)
        # Add edge between src and dst
        edges_data.append({'src': src_name, 'dst': dst_name})

        print(f"Edge -> Vertex:{dst_name}")
        

# find three prominent stations
# Create a separate list for prominent stations
prominent_vertices_data = []

print("prominent stations :")

# find prominent station
prominent_stations = g.V().hasLabel('station').order().by(__.both().count(),Order.desc).limit(3).valueMap(True).toList()

prominent_array = []
#show prominent stations
for station in prominent_stations:
    station_name = station.get('name', ['N/a'])[0]
    prominent_array.append(station_name)
    print(station_name)

# create edges for prominent stations
edges_prominent_data = []
for i in range(len(prominent_array)-1):
    edges_prominent_data.append({'src':prominent_array[i],'dst':prominent_array[i+1]})

# show prominent stations
for edge in edges_prominent_data:
    print(f"Edge -> Vertex: {edge['src']} -> {edge['dst']}")

edges_data = pd.DataFrame(edges_data)
prominent_vertices_df = pd.DataFrame(prominent_vertices_data)

# set title for shortest path 
edges_data['description'] = "Connection between stations" 
graphistry.bind(source='src',destination='dst') \
          .edges(edges_data) \
          .plot() 

# convert to Dataframe for edges_prominent_data
edges_prominent_data = pd.DataFrame(edges_prominent_data)

# set title for 3 prominent stations
edges_prominent_data['description'] = "ranking of prominent stations" 
graphistry.bind(source='src',destination='dst') \
          .edges(edges_prominent_data) \
          .plot() 

connection.close()