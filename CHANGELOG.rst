Changelog
=========

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
