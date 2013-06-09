Bank
====

Bank is a middleware for accessing SQL databases.

Warning
-------

This project is currently in very early beta and lacks features expected
from normal MySQL usage. While it has been proven in production, it is
not ready for general consumption just yet.

Goals
-----

Bank aims to provide a **consistent** interface to SQL databases.
That means that the same code can be used to perform queries on
MySQL, PostgreSQL or any other database.

Bank features a **worker pool** that is **always connected** and
will **automatically reconnect** when the connection is lost.

Bank drivers favor **binary protocols** rather than text, and
do **no SQL manipulation**, preferring prepared statements when
queries have parameters.

Getting Started
---------------

Bank requires Erlang R16B+.

There is no documentation at this time.

Support
-------

 *  Official IRC Channel: #ninenines on irc.freenode.net
 *  [Mailing Lists](http://lists.ninenines.eu)
 *  [Commercial Support](http://ninenines.eu/support)
