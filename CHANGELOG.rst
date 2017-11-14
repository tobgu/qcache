Changelog
=========
0.9.0 (2017-11-14)
------------------
* Numexpr filter engine is not available anymore, only Pandas. Numexpr is no longer a requirement of qcache.
  NB! Major backwards incompatibility
* Fix #12, like now ignores NaN
* Fix #13, only empty string is considered as NaN when reading CSV
* Fix #8, integer standins remain integers

0.8.1 (2017-04-06)
------------------
* Bump Pandas to 0.19.2

0.8.0 (2017-01-08)
------------------
* Support client cert verification

0.7.2 (2016-12-18)
------------------
* Fix #10 & #11, minor statistics improvements

0.7.1 (2016-11-30)
------------------
* Fix #9, df overwritten by mistake

0.7.0 (2016-11-09)
------------------
* Compression using LZ4 or GZIP in requests and responses (#3)
* Sub queries in "in" filter (#7)
* Enum type based on Pandas category type (#6)
* Support for stand in columns in queries (#5)
* Additional metrics/statistics for complete request duration for stores and queries
* Update size estimates to do deep inspection of objects contained in dataframe. This should
  be more accurate than the previous shallow inspection.
* Update Pandas to 0.19.1
* Update Tornado to 4.4.2

0.6.1 (2016-09-18)
------------------
* Fix packaging, the new qcache.qframe package was missing from the submitted package.

0.6.0 (2016-09-18)
------------------
* New filter engine based on Pandas rather than Numexpr. This enables new types of filters in the where
  clause (see below). By default the old engine is still used but the new one can be enabled either
  by default on server startup or on a per-query basis by setting the new 'X-QCache-filter-engine' header
  to 'pandas'.
* New bitwise filters in the 'pandas' filter engine, 'all_bits' and 'any_bits'.
* New string filters, 'like' and 'ilike' which corresponds roughly to LIKE in SQL with the addition
  of regex support. 'like' is case sensitive while 'ilike' is case insensitive.

0.5.0 (2016-06-19)
------------------
* New header when uploading data, 'X-QCache-stand-in-columns', that let you specify default values
  for columns that may not be present in the uploaded data.

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
