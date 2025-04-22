====================================
FDB - The Python driver for Firebird
====================================

|docs| |version| |python| |hatch| |downloads| |rank|

Home_ * Documentation_ * `Bug Reports`_ * Source_

FDB is a `Python`_ library package that implements `Python Database API 2.0`_-compliant support for the open source relational
database `Firebird`_ ®. In addition to the minimal feature set of the standard Python DB API, FDB also exposes the entire native
(old-style) client API of the database engine. Notably:

* Automatic data conversion from strings on input.
* Automatic input/output conversions of textual data between UNICODE and database character sets.
* Support for prepared SQL statements.
* Multiple independent transactions per single connection.
* All transaction parameters that Firebird supports, including table access specifications.
* Distributed transactions.
* Firebird BLOB support, including support for stream BLOBs.
* Support for Firebird Events.
* Support for Firebird ARRAY data type.
* Support for all Firebird Services

FDB also contains extensive collection of submodules that simplify various Firebird-related tasks. Notably:

* Database schema
* Firebird monitoring tables
* Parsing Firebird trace & audit logs
* Parsing Firebird server log
* Parsing Firebird gstat utility output

|donate|

.. _Python: http://python.org
.. _Python Database API 2.0: http://www.python.org/dev/peps/pep-0249/
.. _Firebird: http://www.firebirdsql.org
.. _Bug Reports: http://tracker.firebirdsql.org/browse/PYFB
.. _Home: http://www.firebirdsql.org/en/devel-python-driver/
.. _Source: https://github.com/FirebirdSQL/fdb
.. _Documentation: http://fdb.readthedocs.io/en/v2.0/

.. |donate| image:: https://www.firebirdsql.org/img/donate/donate_to_firebird.gif
    :alt: Contribute to the development
    :scale: 100%
    :target: https://www.firebirdsql.org/en/donate/

.. |docs| image:: https://readthedocs.org/projects/fdb/badge/?version=v2.0
    :alt: Documentation Status
    :scale: 100%
    :target: http://fdb.readthedocs.io/en/v2.0/

.. |version| image:: https://img.shields.io/pypi/v/fdb.svg
    :alt: Version
    :scale: 100%
    :target: https://pypi.org/project/fdb

.. |python| image:: https://img.shields.io/pypi/pyversions/fdb.svg
    :alt: Python version
    :scale: 100%
    :target: https://pypi.org/project/fdb

.. |hatch| image:: https://img.shields.io/badge/%F0%9F%A5%9A-Hatch-4051b5.svg
    :alt: Hatch
    :scale: 100%
    :target: https://pypi.org/project/fdb

.. |downloads| image:: https://img.shields.io/pypi/dm/fdb
    :alt: Downloads
    :scale: 100%
    :target: https://pypi.org/project/fdb

.. |rank| image:: https://img.shields.io/librariesio/sourcerank/pypi/fdb
    :alt: rank
    :scale: 100%
    :target: https://libraries.io/pypi/fdb
