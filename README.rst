======
QCache
======

.. image:: https://travis-ci.org/tobgu/qcache.png?branch=master
        :target: https://travis-ci.org/tobgu/qcache

.. image:: https://pypip.in/d/qcache/badge.png
        :target: https://crate.io/packages/qcache?version=latest

.. _Memcached: http://memcached.org/

QCache is a key-table cache, an in memory cache server with analytical query capabilities.

While the more commonly known key-value caches (such as Memcached_) lets you fetch a value
based on a key QCache lets you run queries against a table based on a key.

**********
Motivation
**********
You are working with table data that you want to run flexible queries against but do not want to
load them into an SQL database or similar because of any of the following:

- The operational cost and complexity of bringing in an SQL server
- The tables do not have a homogeneous format
- The data is short lived
- Not all data available is ever used, you only want to use resources on demand
- You want to treat queries as data and build them dynamically using data structures
  that you are used to (dictionaries and lists or objects and arrays depending on your
  language background)
- ...


.. _QCache-client: https://github.com/tobgu/qcache-client

********
Features
********
- Simple, single process, server.
- Expressive JSON-based query language with format and features similar to SQL SELECT.
- Support for JSON or CSV input and output format
- Performant query performance on tables as large as 10 x 1000000 cells out of the box
- No need for table definitions, tables are created dynamically based on the data inserted
- Statistics about hit and miss count, query and insert performance and more available
  through HTTP API
- Scales linearly in query capacity with the number of servers. A python client library that
  uses consistent hashing for key distribution among servers is available
  here QCache-client_. More clients are welcome!


************
Requirements
************
- Python == 2.7 for now


************
Installation
************
.. code::

   pip install qcache

*******
Running
*******
.. code::

   qcache

This will start qcache on the default port using the default cache size. To get help on available parameters:

.. code::

   qcache --help


*******
License
*******

MIT licensed. See the bundled `LICENSE <https://github.com/tobgu/qcache/blob/master/LICENSE>`_ file for more details.

**************
Query examples
**************
Below are examples of the major features of the query language. A JSON object is used to
describe the query. The query should be URL encoded and passed in using the 'q' GET-parameter.

Like so:
```
http://localhost:8888/qcache/datasets/<dataset_key>?q=<URL-encoded-query>
```

Select all
==========
An empty object will return all rows in the table:

.. code:: python

   {}

Projection
==========
.. code:: python

   {"select": ["foo", "bar"]}

Not specifying select is equivalent to SELECT * in SQL

Filtering
=========
Filtering is done using Lisp style prefix notation for the comparison operators. This
makes it simple to parse and build queries dynamically since no rules for operator precedence
ever need to be applied.

Comparison
----------
.. code:: python

   {"where": ["<" "foo" 1]}

The following operators are supported:

.. code::

   ==, !=, <=, <, >, >=

In
--
.. code:: python

   {"where": ["in" "foo" [1, 2]]}


Clauses
-------
.. code:: python

   {"where": ["&" [">" "foo" 1],
                  ["==" "bar" 2]]}

The following operators are supported:

.. code
   &, |


Negation
--------
.. code:: python

   {"where": ["!" ["=" "foo"  1]]}


Ordering
========

Ascending

.. code:: python

   {"order_by": ["foo"]}


Descending

.. code:: python

   {"order_by": ["-foo"]}   Desc


Offset
======
Great for paging long results!

.. code:: python

   {"offset": 5}


Limit
=====
Great for paging long results!

.. code:: python

   {"limit": 10}


Group by
========
.. code:: python

   {"group_by": ["foo"]}


Aggregation
===========
Aggregation is done as part of the select, just like in SQL.

.. code:: python

   {"select": ["foo" ["sum" "bar"]],
    "group_by": ["foo"]}


Distinct
========
Distinct has its own query clause unlike in SQL.

.. code:: python

   {"select": ["foo" "bar"],
    "distinct": ["foo"]}


All together now!
=================
A slightly more elaborate example. Get the top 10 foo:s with most bar:s.

.. code:: python

   {"select": ["foo" ["sum" "bar"]],
    "where": [">" "bar" 0],
    "order_by": ["-bar"],
    "group_by": ["foo"],
    "limit": 10}


***********************
API examples using curl
***********************
Upload table data to cache (a 404 will be returned if querying on a key that does not exist).

.. code::

   curl -X POST --data-binary @my_csv.csv http://localhost:8888/qcache/dataset/my-key


Query table

.. code::

   curl -G localhost:8888/qcache/dataset/my-key --data-urlencode "q={\"select\": [[\"count\"]], \"where\": [\"<\", \"baz\", 99999999999915],  \"offset\": 100, \"limit\": 50}"
   curl -G localhost:8888/qcache/dataset/my-key --data-urlencode "q={\"select\": [[\"count\"]], \"where\": [\"in\", \"baz\", [779889,8958854,8281368,6836605,3080972,4072649,7173075,4769116,4766900,4947128,7314959,683531,6395813,7834211,12051932,3735224,12368089,9858334,4424629,4155280]],  \"offset\": 0, \"limit\": 50}"
   curl -G localhost:8888/qcache/dataset/my-key --data-urlencode "q={\"where\": [\"==\", \"foo\", \"\\\"95d9f671\\\"\"],  \"offset\": 0, \"limit\": 50}"
   curl -G localhost:8888/qcache/dataset/my-key --data-urlencode "q={\"select\": [[\"max\", \"baz\"]],  \"offset\": 0, \"limit\": 500000000000}"

*************
More examples
*************
Right now the documentation is very immature. Please look at the tests in the project or QCache-client_
for further guidance. If you still have questions don't hesitate to contact the author or write an issue!

*************
Data encoding
*************
Just use UTF-8 when uploading data and in queries and you'll be fine. All responses are UTF-8.
No other codecs are supported.

**************************
Performance & dimensioning
**************************
Since QCache is single thread, single process, the way to scale capacity is by adding more servers.
If you have 8 Gb of ram available on a 4 core machine don't start one server using all 8 Gb. Instead
start 4 servers with 2 Gb memory each or even 8 servers with 1 Gb each. Assign them to different ports
and use a client library to do the key balancing between them. That way you will have 4 - 8 times the
query capacity.

QCache is ideal for container deployment. Start one container running one QCache instance.

Expect a memory overhead of about 20% - 30% of the configured cache size for querying and table loading.
To be on the safe side you should probably assume a 50% overhead. Eg. if you have 3 Gb available set the
cache size to 2 Gb.

When choosing between CSV and JSON as upload format prefer CSV as the amount of data can be large and it's
more compact and faster to insert than JSON.

For query responses prefer JSON as the amount of data is often small and it's easier to work with than CSV.

.. _Pandas: http://pandas.pydata.org/
.. _NumPy: http://www.numpy.org/
.. _Numexpr: https://github.com/pydata/numexpr
.. _Tornado: http://www.tornadoweb.org/en/stable/

***********************************
Standing on the shoulders of giants
***********************************
QCache makes heavy use of the fantastic python libraries Pandas_, NumPy_, Numexpr_ and Tornado_.


********************************
Ideas for coming features & work
********************************
* Improve documentation
* Stream data into dataframe rather than waiting for complete input
* Streaming proxy
* Configurable URL prefix
* Implement both GET and POST to query (using .../q/)
* Make it possible to execute multiple queries in one request (qs=,/qs/)
* Allow post with data and query in one request, this will guarantee progress
  as long as the dataset fits in memory. {"query": ..., dataset: ...}
* Counters available at special URL (cache hits direct and indirect, misses, dataset size distribution, exception count)
* Exceptions to Sentry?
* SSL and basic authentication
* Possibility to specify indexes when uploading data (how do the indexes affect size? write performance? read performance?)
* Possibility to upload files as a way to prime the cache without taking up memory.
* Namespaces for more diverse statistics based on namespace?
* Docker container with QCache pre-installed
* Publish performance numbers
