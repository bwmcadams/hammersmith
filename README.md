Hammersmith 0.30.0 ("Zathrus")
==============================

[![Build Status](https://travis-ci.org/bwmcadams/hammersmith.svg)](https://travis-ci.org/bwmcadams/hammersmith)
[![Join the chat at https://gitter.im/bwmcadams/hammersmith](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/bwmcadams/hammersmith?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

A Pure Scala, asynchronous MongoDB Driver with type-classes for custom type encoding and a highly optimized BSON serialiation layer.

Modesty aside: from the *expert* in MongoDB + Scala. Based on many years of lessons learned working on [Casbah, the Original MongoDB Scala Driver](http://github.com/mongodb/casbah) as well as part of the core driver team at MongoDB itself.

Currently a slightly broken scattered mess comprising several years of sketches, ideas and prototypes. Aimed to have pluggable network backends for:

* scalaz-stream
* Akka Streams
* RxScala

The idea being to give you the maximum flexibility around what *your* needs are, rather than dictate your network layer.

Stays crunchy in milk!


DISCLAIMER
-----------
This driver is still an early beta and should be used with caution in production.  It still lacks support for crucial
features such as Replica Sets.

There is also a known (but completely adorable!) issue where occasionally, an exception is thrown that causes a litter of pug puppies to burst forth from any open USB ports on your computer.  You have been warned.

**PRODUCED IN A FACILITY WHICH ALSO PROCESSES JAVA AND/OR FEZ'**

Author/Maintainer
-----------------
* [Brendan W. McAdams (@rit)](http://github.com/bwmcadams)

Contributors
------------

These fine specimens of humanity have demonstrated their
mastery of Scala + MongoDB by making Hammersmith more awesome in their spare time:

* [Havoc Pennington (@havocp)](http://github.com/havocp)
* [Gerolf Seitz (@gseitz)](http://github.com/gseitz)




...
