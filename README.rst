FDB is a Python library package that implements Python Database API 2.0-compliant support for the open source relational
database Firebird®. In addition to the minimal feature set of the standard Python DB API, FDB also exposes the entire native
client API of the database engine. Notably:

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

FDB is implemented on top of Firebird client library using ctypes, and currently uses only traditional Firebird API.

FDB works with Firebird 2.0 and newer, and Python 2.7 and 3.4+.

FDB is free – covered by a permissive BSD-style license that both commercial and noncommercial users should find agreeable.

FDB is replacement for discontinued KInterbasDB library, and as such it's designed to be as much compatible
with KInterbasDB as possible, but there are some differences. See FDB documentation for full description
of these differences.

