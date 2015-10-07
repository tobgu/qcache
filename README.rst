======
QCache
======

.. image:: https://badge.fury.io/py/qcache.png
    :target: http://badge.fury.io/py/qcache

.. image:: https://travis-ci.org/tobgu/qcache.png?branch=master
        :target: https://travis-ci.org/tobgu/qcache

.. image:: https://pypip.in/d/qcache/badge.png
        :target: https://crate.io/packages/qcache?version=latest


In memory cache server with analytical query capabilities

Features
--------

* TODO

Requirements
------------

- Python == 2.7 for now

License
-------

MIT licensed. See the bundled `LICENSE <https://github.com/tobgu/qcache/blob/master/LICENSE>`_ file for more details.


TODO
----
x * Query language for filtering, sorting and pagination
x * Support both JSON and CSV input/output
* Cache eviction
  - By age for mutable data
  - By size (number of lines and or bytes)
  - LRU eviction
* Assure that memory usage is stable over time

* Call the right server using some sort of stable hashing?
  - Use a fixed number of cache servers to start with
  - Cluster configuration, list of servers. May have different weights depending on configuration.
  - Client side or server side proxying
* Discovery of dead servers is done when a request to the server is required

* Stream data into dataframe rather than waiting for complete input
* Streaming proxy
* Configurable URL prefix
* Implement both GET and POST to query (using .../q/)
* Make it possible to execute multiple queries in one request (qs=,/qs/)
* Make it possible to do explicit evict by DELETE
* Allow post with data and query in one request, this will guarantee progress
  as long as the dataset fits in memory. {"query": ..., dataset: ...}
* Counters available at special URL (cache hits direct and indirect, misses, dataset size distribution, exception count)
* Counters to influx DB
* Exceptions to Sentry
* SSL and basic authentication
* Possibility to specify indexes when uploading data (how do the indexes affect size? write performance? read performance?)

Links
-----
* http://stackoverflow.com/questions/23886030/how-to-post-a-very-long-url-using-python-requests-module
* http://stackoverflow.com/questions/18089667/how-to-estimate-how-much-memory-a-pandas-dataframe-will-need
* http://stackoverflow.com/questions/16524545/how-to-write-a-web-proxy-in-python
* https://groups.google.com/forum/#!topic/python-tornado/TB_6oKBmdlA
* http://stackoverflow.com/questions/16626058/what-is-the-performance-impact-of-non-unique-indexes-in-pandas

Configuration file
------------------
* Maximum size
 - Get the size of data frame: df.values.nbytes + df.index.nbytes + df.columns.nbytes
* Maximum age
 - Seconds
* List of hosts in the "cluster"
 - IP address and port number

Using cURL to test
------------------
* time curl -X POST --data-binary @my_csv2.csv http://localhost:8888/url_prefix/big
* curl localhost:8888/url_prefix/big

Query examples
==============

Select all
----------
{}


Projection
----------
{"select": ["foo", "bar"]}

Aggregation, max, min and so on.

Not specifying select means "select *"

Filtering
---------
Lisp style prefix notation

Exact:
{"where": ["==" "foo" 1]}

Comparison:
{"where": ["<" "foo" 1]}
!=, <=, <, >, >=

In:
{"where": ["in" "foo" [1, 2]]}

Clauses:
{"where": ["&" [">" "foo" 1],
               ["==" "bar" 2]]}
&, |

Negation:
{"where": ["!" ["=" "foo"  1]]}


Ordering
--------
{"order_by": ["foo"]}    Asc
{"order_by": ["-foo"]}   Desc


Offset
------
{"offset": 5}


Limit
-----
{"limit": 10}


Group by
--------
{"group_by": ["foo"]}


API examples using curl
-----------------------
curl -G localhost:8888/url_prefix/fairlybig --data-urlencode "q={\"select\": [[\"count\"]], \"where\": [\"<\", \"baz\", 99999999999915],  \"offset\": 100, \"limit\": 50}"
curl -G localhost:8888/url_prefix/fairlybig --data-urlencode "q={\"select\": [[\"count\"]], \"where\": [\"in\", \"baz\", [779889,8958854,8281368,6836605,3080972,4072649,7173075,4769116,4766900,4947128,7314959,683531,6395813,7834211,12051932,3735224,12368089,9858334,4424629,4155280]],  \"offset\": 0, \"limit\": 50}"
curl -G localhost:8888/url_prefix/fairlybig --data-urlencode "q={\"where\": [\"==\", \"foo\", \"\\\"95d9f671\\\"\"],  \"offset\": 0, \"limit\": 50}"
curl -G localhost:8888/url_prefix/fairlybig --data-urlencode "q={\"select\": [[\"max\", \"baz\"]],  \"offset\": 0, \"limit\": 500000000000}"
curl -X POST --data-binary @fairly_big.csv http://localhost:8888/url_prefix/fairlybig
