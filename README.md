# ETL with Spark and JanusGraph Report Document

## I. JanusGraph and Spark Setup

I first ran JanusGraph on Docker using the following command:
```bash
docker run -d --name janusgraph-default -p 8182:8182 janusgraph/janusgraph:latest
```

Then, I cloned and installed janusgraph-python from the following link: https://github.com/JanusGraph/janusgraph-python.

After successfully cloning the repository, I navigated to the folder and installed the required packages by running:
```bash
pip install .
```

Next, I installed PySpark using the command:
```bash
pip install pyspark
```

I used Python version 3.10.14 for the entire setup.

Finally, I installed the Gremlin Python library with the following command:
```bash
pip install gremlinpython
```

## II. Python Scripts Setup

I created two main scripts to perform the tasks of finding the shortest path and identifying the most prominent station:

importToJanus.py: This script uses Spark to read the CSV dataset and import the data into JanusGraph.
query.py: This script uses Gremlin to query JanusGraph, find the shortest path between stations, and identify the most prominent station (the one with the most connections in and out).

