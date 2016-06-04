Changelog
=========

0.4.2 (2016-06-04)
------------------
* Additional statistics to measure for how long data remains in the cache before it's evicted.
* Bump dependency versions of Pandas, Numexpr and Tornado.

0.4.1 (2016-01-31)
------------------
* Provide the duration for which statistics were collected and statistics buffer size

0.4.0 (2016-01-24)
------------------
* Sub query support with new 'from' clause
* Column aliasing + support for calculated columns
* Error message improvements

0.3.0 (2015-12-23)
------------------
* Accepts conjunctions and disjunctions with only one clause
* Accept POST queries, good for large queries
* Improved performance for "in" queries, up to 30x faster for large lists

0.2.1 (2015-12-15)
------------------
* More efficient cache size tracking
* Check against unknown query clauses

0.2.0 (2015-12-06)
------------------
* Report the unsliced result length as part of the result, nice for pagination for example
* Use connection pooling
* SSL and basic auth support

0.1.0 (2015-10-25)
------------------
* First release that actually does something sensible.

0.0.1 (2015-10-15)
------------------
* First release on PyPI.
