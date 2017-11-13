======
QCache
======

.. image:: https://travis-ci.org/tobgu/qcache.png?branch=master
    :target: https://travis-ci.org/tobgu/qcache

.. image:: https://badge.fury.io/py/qcache.svg
    :target: https://badge.fury.io/py/qcache

.. image:: http://codecov.io/github/tobgu/qcache/coverage.svg?branch=master
    :target: http://codecov.io/github/tobgu/qcache?branch=master

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
- Expensive JOINs are required to create the table.
- ...

Or, you are building server software and want to add the possibility for your clients to run
queries directly against the data without the need for dreadful translations between a REST
interface with some home grown filter language.


.. _QCache-client: https://github.com/tobgu/qcache-client
.. _Go-QCache-client: https://github.com/tobgu/go-qcache-client

********
Features
********
- Simple, single thread, single process, server.
- Expressive JSON-based query language with format and features similar to SQL SELECT. Queries
  are data that can easily be transformed or enriched.
- Support for JSON or CSV input and output format
- Performant queries on tables as large as 10 x 1000000 cells out of the box
- No need for table definitions, tables are created dynamically based on the data inserted
- Statistics about hit and miss count, query and insert performance and more available
  through HTTP API
- Scales linearly in query capacity with the number of servers. A python client library that
  uses consistent hashing for key distribution among servers is available
  here QCache-client_. There's also a basic Go client here Go-QCache-client_.
  More clients are welcome!


************
Requirements
************
Python 2.7 (2.7.9+ if using TLS) for now


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


******
Docker
******
You can also get the latest version as a Docker image. This is probably the easiest way to try it out if you
are running Linux or if you have Docker Machine installed.

.. code::

   docker run -p 9401:9401 tobgu/qcache


*******
License
*******
MIT licensed. See the bundled `LICENSE <https://github.com/tobgu/qcache/blob/master/LICENSE>`_ file for more details.

**************
Query examples
**************
Below are examples of the major features of the query language. A JSON object is used to
describe the query. The query should be URL encoded and passed in using the 'q' GET-parameter.

The query language uses LISP-style prefix notation for simplicity. This makes it easy
to parse and build queries dynamically since no rules for operator precedence
ever need to be applied.

Like so:
`http://localhost:8888/qcache/datasets/<dataset_key>?q=<URL-encoded-query>`

You can also POST queries as JSON against:
`http://localhost:8888/qcache/datasets/<dataset_key>/q/`

This is a good alternative to GET if your queries are too large to fit in the query string.

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

Column aliasing
---------------
.. code:: python

   {"select": [["=", "foo", "bar"]]}

This will rename column bar to foo in the result.

You can also make more elaborate calculations in the aliasing expression.

.. code:: python

   {"select": [["=", "baz", ["+", ["*", "bar", 2], "foo"]]]

As well as simple constant assignments.

.. code:: python

   {"select": [["=", "baz", 55]]}


Filtering
=========

Comparison
----------
.. code:: python

   {"where": ["<", "foo", 1]}

The following operators are supported:

.. code::

   ==, !=, <=, <, >, >=

In
--
.. code:: python

   {"where": ["in", "foo", [1, 2]]}


Like/ilike
----------
Like and ilike are used for string matching and work similar to LIKE in SQL. Like is case sensitive
while ilike is case insensitive. In addition to string matching using % as wildcard like/ilike also
supports regexps.

.. code:: python

   {"where": ["like", "foo", "'%bar%'"]}


Bitwise operators
-----------------
There are two operators for bitwise filtering on integers: `all_bits` and `any_bits`.

* all_bits - evaluates to true if all bits in the supplied argument are set in value tested against.
* any_bits - evaluates to true if any bits in the supplied argument are set in value tested agains.

.. code:: python

   {"where": ["any_bits", "foo", 31]}


Clauses
-------
.. code:: python

   {"where": ["&", [">", "foo", 1],
                   ["==", "bar", 2]]}

The following operators are supported:

.. code::

   &, |


Negation
--------
.. code:: python

   {"where": ["!", ["==", "foo",  1]]}


Ordering
========

Ascending

.. code:: python

   {"order_by": ["foo"]}


Descending

.. code:: python

   {"order_by": ["-foo"]}


Offset
======
Great for pagination of long results!

.. code:: python

   {"offset": 5}


Limit
=====
Great for pagination of long results!

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

   {"select": ["foo" ["sum", "bar"]],
    "group_by": ["foo"]}


Distinct
========
Distinct has its own query clause unlike in SQL.

.. code:: python

   {"select": ["foo", "bar"],
    "distinct": ["foo"]}


Sub queries using from
======================
Filter, transform and select your data in multiple steps.

.. code:: python

    {"select": [["=", "foo_pct", ["*", 100, ["/", "foo", "bar"]]]],
     "from": {"select": ["foo", ["sum", "bar"]],
              "group_by": ["foo"]}}


Sub queries using in
====================
Filter your data using the result of a query as filter input.

.. code:: python

    {"where", ["in", "foo", {"where": ["==", "bar", 10]}]}


All together now!
=================
A slightly more elaborate example. Get the top 10 foo:s with most bar:s.

.. code:: python

   {"select": ["foo", ["sum", "bar"]],
    "where": [">", "bar", 0],
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


***************************
Custom request HTTP headers
***************************

There are a couple of custom HTTP headers that can be used to control the behaviour of Q-Cache.

Posting tables
==============

X-QCache-types
--------------
QCache will usually recognize the data types of submitted data automatically. There may be times when
strings are mistaken for numbers because all of the data submitted for a column in a dataset happens
to be in numbers.

This header makes it possible to explicitly type column to be a string to. In the example below columns
foo and bar are both typed to string.

.. code::

   X-QCache-types: foo=string;bar=string

Explicitly setting the type to string is only relevant when submitting data in CSV. With JSON the data
has an unambiguous (well...) data type that is used by QCache.

Enums
-----
The `X-QCache-types` header can also be used to specify columns with enum types.

.. code::

   X-QCache-types: foo=enum;bar=enum

Enums are a good way to store low cardinality string columns space efficiently. They can be compared
for equality and inequality but currently do not have a well defined order so filtering by
larger than and less than is not possible for example.


X-QCache-stand-in-columns
-------------------------
It may be that your submitted data varies a little from dataset to dataset with respect to the columns
available in the dataset. You still want to be able to query the datasets in the same way and make
some assumptions of which columns that are available. This header lets you do that.

In the below example column foo will be set to 10 in case it does not exist in the submitted data. bar will
be set to the value of the baz column if it is not submitted.

This header can be used in request both for storing and querying data.

.. code::

   X-QCache-stand-in-columns: foo=10;bar=baz


Query responses
===============

X-QCache-unsliced-length
------------------------
This header is added to responses and states how many rows the total filtered result was before applying
any limits or offsets for pagination.

.. code::

   X-QCache-unsliced-length: 8324


*************
More examples
*************
Please look at the tests in the project or QCache-client_ for some further examples of queries.
The unit tests in this project is also a good source for examples.

If you still have questions don't hesitate to contact the author or write an issue!

**********
Statistics
**********

.. code::

   http://localhost:8888/qcache/statistics

A get against the above endpoint will return a JSON object containing cache statistics,
hit & miss count, query & upload duration. Statistics are reset when querying.

*************
Data encoding
*************
Just use UTF-8 when uploading data and in queries and you'll be fine. All responses are UTF-8.
No other codecs are supported.

****************
Data compression
****************
QCache supports request and response body compression with LZ4 or GZIP using standard HTTP headers.

In a query request set the following header to receive a compressed response:

.. code::

   Accept-Encoding: lz4,gzip


The response will contain the following header indicating the used encoding

.. code::

   Content-Encoding: lz4

LZ4 will always be preferred if present.

The above header should also be set indicating the compression algorithm if you are
submitting compressed data.


**************************
Performance & dimensioning
**************************
Since QCache is single thread, single process, the way to scale capacity is by adding more servers.
If you have 8 Gb of ram available on a 4 core machine don't start one server using all 8 Gb. Instead
start 4 servers with 2 Gb memory each or even 8 servers with 1 Gb each or 16 servers with 512 Mb each.
depending on your use case. Assign them to different ports and use a client library to do the key
balancing between them. That way you will have 4 - 16 times the query capacity.

QCache is ideal for container deployment. Start one container running one QCache instance.

Expect a memory overhead of about 20% - 30% of the configured cache size for querying and table loading.
To be on the safe side you should probably assume a 50% overhead. Eg. if you have 3 Gb available set the
cache size to 2 Gb.

When choosing between CSV and JSON as upload format prefer CSV as the amount of data can be large and it's
more compact and faster to insert than JSON.

For query responses prefer JSON as the amount of data is often small and it's easier to work with than CSV.

.. _Pandas: http://pandas.pydata.org/
.. _NumPy: http://www.numpy.org/
.. _Tornado: http://www.tornadoweb.org/en/stable/

***********************************
Standing on the shoulders of giants
***********************************
QCache makes heavy use of the fantastic python libraries Pandas_, NumPy_ and Tornado_.


*********************
Ideas for coming work
*********************
These may or may not be realized, it's far from sure that all of the ideas are good.

* Improve documentation
* Stream data into dataframe rather than waiting for complete input, chunked HTTP upload or similar.
* Streaming proxy to allow clients to only know about one endpoint.
* Configurable URL prefix to allow being mounted at arbitrary position behind a proxy.
* Make it possible to execute multiple queries and return multiple responses in one request (qs=,/qs/).
* Allow post with data and query in one request, this will guarantee progress
  as long as the dataset fits in memory. {"query": ..., "dataset": ...}
* Possibility to specify indexes when uploading data (how do the indexes affect size? write performance? read performance?)
* Possibility to upload files as a way to prime the cache without taking up memory.
* Namespaces for more diverse statistics based on namespace?
* Publish performance numbers
* Other table formats in addition to CSV and JSON?
* Break out all things dataframe into an own package and provide possibility to update
  and insert into dataframe based on predicate just like querying is done now.
* Investigate type hints for pandas categorials on enum-like values to improve storage
  layout and filter speed. Check new import options from CSV when Pandas 0.19 is available.
* Support math functions as part of the where clause (see pandas expr.py/ops.py)
* Some kind of light weight joining? Could create dataset groups that all are allocated to
  the same cache. Sub queries could then be used to query datasets based on data selected
  from other datasets in the same dataset group.

************
Contributing
************
Want to contribute? That's great!

If you experience problems please log them on GitHub. If you want to contribute code,
please fork the code and submit a pull request.

If you intend to implement major features or make major changes please raise an issue
so that we can discuss it first.

Running tests
=============
.. code::

   pip install -r dev-requirements.txt
   invoke test
