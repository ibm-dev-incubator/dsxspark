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


This will install dsxspark and it's requirements into your python environment.

If running from inside the DSX jupyter notebook run the pip command above like::

    import sys
    {sys.executable} -m pip install $PATH_TO_DSXSPARK

Which will ensure dsxspark gets installed properly in the jupyter environment.

Softlayer
---------

But, before you can run dsxspark in any context you need to setup your
softlayer credentials on the system. You can read the python softlayer module
docs on how to do this here:

http://softlayer-python.readthedocs.io/en/latest/config_file.html

Once the softlayer api configuration file is working you can test it with the
slcli command. For example listing all the virtual servers::

  $ slcli virtual list

If this works it will list all the servers created. Once your credentials config
file is working you can move on to running dsxspark.

OpenStack
---------
Before you can run dsxspark with an OpenStack cloud you need to setup your
OpenStack credentials on the system. dsxspark uses os-client-config to read
the credentials for OpenStack. The full documentation on setting up credentials
on your system can be found here:

https://docs.openstack.org/os-client-config/latest/user/configuration.html

Deploying a Spark Cluster
=========================

Softlayer
---------

To deploy a cluster with dsxspark you first need to create a ``SLSparkCluster``
object. This will track the nodes being used and handle the lifcycle of the
cluster's servers. To do this simply create a class like::

    from dsxspark import launch_cluster as lc

    cluster = lc.SLSparkCluster(10, 'MyBeautifulCluster', cpus=4, memory=16384,
                                ssh_keys=['12345'])

Then after the cluster object has been created you can deploy it with::

    cluster.deploy_cluster()

After this finished (it will take some time) a spark cluster will be deployed
and running. The master node will be ``MyBeautifulCluster01``.

Deleting a Spark Cluster
''''''''''''''''''''''''

If after deploying your spark cluster you wish to tear it down and delete the
virtual servers running it you can do this at any time by calling::

    cluster.collapse_cluster()

OpenStack
---------

To deploy a cluster with dsxspark you first need to create a ``OSSparkCluster``
object. This will track the nodes being used and handle the lifcycle of the
cluster's servers. To do this simply create a class like::

    from dsxspark import launch_cluster as lc

    cluster = lc.OSSparkCluster(10, 'MyBeautifulCluster', image='centos7',
                                flavor='m1.large', remote_user='centos',
                                ssh_key='desktop')

Then after the cluster object has been created you can deploy it with::

    cluster.deploy_cluster()

After this finishes (it will take some time) a 10 node spark cluster will be
deployed and running. The master node will be ``MyBeautifulCluster01``.

Deleting a Spark Cluster
''''''''''''''''''''''''

If after deploying your spark cluster you wish to tear it down and delete the
servers running it you can do this at any time by calling::

    cluster.collapse_cluster()
