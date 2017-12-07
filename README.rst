dsxspark
========

This is tooling for deploying a spark cluster on a cloud. It was designed
with the intent of running from inside the context of IBM Data Science
Experience, but it is generic enough that it can be used anywhere.

Setup
=====

The first step to running dsxspark is to install it into your python
environment. This can easily be done by running these 2 commands::

  $ git clone --recurse-submodules https://github.com/ibm-dev-incubator/dsxspark.git
  $ pip install -U ./dsxspark

This will install dsxspark and it's requirements into your pythone environment.
But, before you can run dsxspark in any context you need to setup your
softlayer credentials on the system. You can read the python softlayer module
docs on how to do this here:

http://softlayer-python.readthedocs.io/en/latest/config_file.html

Once the softlayer api configuration file is working you can test it with the
slcli command. For example listing all the virtual servers::

  $ slcli virtual list

If this works it will list all the servers created. Once your credentials config
file is working you can move on to running dsxspark.
