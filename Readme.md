# LDBC Converter
This project is one of three projects that constitute my masters thesis.
1. [Graph Access Service](https://github.com/adityachandla/graph_access_service)
2. [Graph Algorithm Service](https://github.com/adityachandla/graph_algorithm_service)
3. LDBC Converter(This repository)

The data converter converts graph data from LDBC to one of the required binary formats. This binary data is then uploaded to AWS S3. The graph access service provides an interface to interact with the graph stored in S3. The Graph algorithm service uses the interface exposed by the graph access service and accesses the performance of various graph algorithms.

## Mappings for SF-1
#### Node Ids
While converting the data, we assign a single identifier to all types of nodes. For SF-1, the following table depicts the identifiers ranges for each node type. 
| NodeType | start | end |
| -------- | ----- | --- |
|Person| 0|  10,294|
|Post| 10,295|  1,131,520|
|Comment| 1,131,521| 2,870,958|
|Forum| 2,870,959| 2,971,785 |
|Tag| 2,971,786 |2,987,865|
|TagClass| 2,987,866| 2,987,936 |
|Organization| 2,987,937 | 2,995,891 |
|Place| 2,995,892 | 2,997,351 |

### Relationship Ids
Each relationship is also assigned an integer label. For SF-1, this is the mapping from relationship description to an integer label:
| Relationship | Label |
| ------------ | ----- |
| Forum's Tag | 1 |
| Place is part of place | 2 |
| Person works at company | 3 |
| Person studies at univeristy | 4 |
| Person has interest in tag | 5 |
| Comment creator | 6 |
| Comment location | 7 |
| Comment parent post | 8 |
| Comment parent comment | 9 |
| Post creator | 10 |
| Post location | 11 |
| Post container forum | 12 |
| Tag class | 13 |
| Comment tag | 14 |
| Tag subclass of Tag | 15 |
| Person likes post | 16 |
| Forum moderator person | 17 |
| Organization location | 18 |
| Post Tag | 19 |
| Forum Member | 20 |
| Person Knows | 21 |
| Person likes comment | 22 |
| Person location | 23 |
