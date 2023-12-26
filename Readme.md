# LDBC Converter
This project is one of three projects that constitute my masters thesis.
1. [Graph Access Service](https://github.com/adityachandla/graph_access_service)
2. [Graph Algorithm Service](https://github.com/adityachandla/graph_algorithm_service)
3. LDBC Converter(This repository)

The data converter converts graph data from LDBC to one of the required binary formats. This binary data is then uploaded to AWS S3. The graph access service provides an interface to interact with the graph stored in S3. The Graph algorithm service uses the interface exposed by the graph access service and accesses the performance of various graph algorithms.
