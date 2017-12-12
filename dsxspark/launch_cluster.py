# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import copy
import os
import socket
import tempfile
import time
import threading

import paramiko
import SoftLayer as sl
import shade

from dsxspark import exceptions
from dsxspark import runner
from dsxspark import write_inventory

PLAYBOOK_PATH = os.path.join(
    os.path.abspath(os.path.dirname(__file__)), 'playbooks')
LAUNCH_PLAYBOOK = os.path.join(PLAYBOOK_PATH, 'sl_launch.yml')
DESTROY_PLAYBOOK = os.path.join(PLAYBOOK_PATH, 'sl_destroy.yml')
PREPARE_PLAYBOOK = os.path.join(PLAYBOOK_PATH, 'sl_prepare_node.yml')
SETUP_SPARK_PLAYBOOK = os.path.join(PLAYBOOK_PATH,
                                    'setup-spark-standalone.yml')
START_SPARK_PLAYBOOK = os.path.join(PLAYBOOK_PATH, 'start_spark.yml')
SPARK_DEPLOY_DIR = os.path.join(
    os.path.abspath(os.path.dirname(__file__)), 'spark-cluster-install')


class SLSparkCluster(object):
    def __init__(self, server_count, cluster_name, cpus=4, memory=16384,
                 disk_size=25, domain_name='spark.test', datacenter='dal10',
                 ssh_keys=None):
        """Deploy a spark cluster on softlayer

        :param str hostname: The base hostname to use for all nodes. Must not
            contain any special characters, except for '.' and '-'.
        :param str domain_name: The domain name to use for all clusters. Must
            be in the form 'Domain.TLD' and no special characters can be used
            except for '.' and '-'
        :param int cpus: The number of cpus to use for the nodes
        :param int memory: An integer representing the memory size in MB to
            use for the launched VMs. Must be one of: 1024, 2048, 4096,
            6144, 8192, 12288, 16384, 32768, 49152, 65536, 131072, 247808
        :param str datacenter: A string for which data center to launch the
            nodes in. Valid choices are ams01, ams03, che01, dal01, dal05,
            dal06, dal09, dal10, fra02, hkg02, hou02, lon02, mel01, mex01,
            mil01, mon01, osl01, par01, sjc01, sjc03, sao01, sea01, sng01,
            syd01, tok02, tor01, wdc01, wdc04
        :param list ssh_keys: A list of numeric ids for each ssh key to use
            on the server.

        """
        self.cluster_name = cluster_name
        self.server_count = server_count
        self.datacenter = datacenter
        self.domain_name = domain_name
        ssh_keys = ssh_keys or []
        self.extra_vars = {
            'disk_size': disk_size,
            'memory': memory,
            'cpus': cpus,
            'ssh_keys': ','.join(ssh_keys),
            "cluster_name": self.cluster_name,
            "domain": self.domain_name,
            'datacenter': self.datacenter,
        }
        self.inventory_file = os.path.join(
            tempfile.gettempdir(), 'spark_inv_%s.ini' % self.cluster_name)
        self.launcher_treads = []
        self.deployed = False
        self.master = {}
        self.nodes = []

    def _get_ip_addr(self, hostname):
        sl_client = sl.create_client_from_env()
        vs = sl.VSManager(sl_client)
        server = vs.list_instances(hostname=hostname)[0]
        return server['primaryIpAddress']

    def _launch_worker(self, worker_number, master=None):
        hostname = self.cluster_name + '%02d' % worker_number
        worker_vars = copy.deepcopy(self.extra_vars)
        worker_vars['hostname'] = hostname
        runner.run_playbook_subprocess(LAUNCH_PLAYBOOK, worker_vars)
        ip_addr = self._get_ip_addr(hostname)
        if master:
            self._write_master_inventory(hostname, ip_addr)
        else:
            self._write_node_inventory(hostname, ip_addr, worker_number)

    def _write_node_inventory(self, hostname, ip_addr, node_num):
        self.nodes.append({'hostname': hostname, 'ip_addr': ip_addr,
                           'node_num': node_num})

    def _write_master_inventory(self, hostname, ip_addr):
        self.master = {'hostname': hostname, 'ip_addr': ip_addr}
        self.master_ip = ip_addr

    def deploy_cluster(self):
        if self.deployed:
            print("This cluster is already deployed.")
            return
        worker = threading.Thread(target=self._launch_worker, args=(1,),
                                  kwargs={'master': True})
        worker.start()
        self.launcher_treads.append(worker)
        for node in range(2, self.server_count + 1):
            worker_thread = threading.Thread(target=self._launch_worker,
                                             args=(node,))
            worker_thread.start()
            self.launcher_treads.append(worker_thread)
        for worker in self.launcher_treads:
            worker.join()
        self.deployed = True
        with open(self.inventory_file, 'w') as inv_file:
            write_inventory.write_inventory(inv_file, self.master, self.nodes)
        self.launcher_treads = []
        runner.run_playbook_subprocess(PREPARE_PLAYBOOK,
                                       inventory=self.inventory_file)
        cwd = os.getcwd()
        os.chdir(SPARK_DEPLOY_DIR)
        runner.run_playbook_subprocess('./setup-spark-standalone.yml',
                                       inventory=self.inventory_file)
        os.chdir(cwd)
        runner.run_playbook_subprocess(START_SPARK_PLAYBOOK,
                                       extra_vars={'domain': self.domain_name},
                                       inventory=self.inventory_file)

    def _delete_worker(self, worker_number):
        hostname = self.cluster_name + '%02d' % worker_number
        worker_vars = copy.deepcopy(self.extra_vars)
        worker_vars['hostname'] = hostname
        runner.run_playbook_subprocess(DESTROY_PLAYBOOK, worker_vars)

    def collapse_cluster(self):
        for node in range(self.server_count + 1):
            self._delete_worker(node)


class OSSparkCluster(object):
    def __init__(self, server_count, cluster_name, flavor, image,
                 ssh_key=None, cloud_name=None, remote_user='centos'):
        """Deploy a spark cluster on an OpenStack cloud

        :param int server_count: The number of servers to use in the cluster
        :param str cluster_name:
        :param flavor: The flavor to use for the servers. It can either be the
            name or the id. This will be used for all servers in the cluster
        :param image: The image to use for the servers. It can either be the
            name or the id. This will be used for all servers in the cluster.
            This should be a CentOS or RHEL image, this is what the ansible
            playbooks for deploying spark expect.
        :param str ssh_key: The ssh key name to use for the server.
        :param str cloud_name: An optional cloud name used to specify which
            cloud to use if multiple are present in your cloud config.
        :param str remote_user: The username to use for ssh
        """


        self.cluster_name = cluster_name
        self.server_count = server_count
        self.image = image
        self.flavor = flavor
        self.ssh_key = ssh_key
        self.cloud = shade.openstack_cloud(cloud=cloud_name)
        self.remote_user = remote_user
        self.servers = []
        self.inventory_file = os.path.join(
            tempfile.gettempdir(), 'spark_inv_%s.ini' % self.cluster_name)
        self.launcher_treads = []
        self.deployed = False
        self.master = {}
        self.nodes = []

    def _check_ssh(self, ip_addr):
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        count = 0
        sleep_time = 5
        while count < 3:
            try:
                client.connect(ip_addr, username=self.remote_user)
                return
            except (EOFError, socket.error, socket.timeout,
                    paramiko.SSHException):
                client.close()
                time.sleep(sleep_time)
                sleep_time = sleep_time * sleep_time
            count = count + 1
        raise exceptions.SSHTimeOut('SSH never became available')

    def _launch_worker(self, worker_number, master=None):
        hostname = self.cluster_name + '%02d' % worker_number
        server = self.cloud.create_server(hostname, image=self.image,
                                          flavor=self.flavor,
                                          key_name=self.ssh_key,
                                          wait=True)
        self.servers.append(server)
        if server['interface_ip']:
            ip_addr = server['interface_ip']
        else:
            ip_addr = shade.meta.get_server_ip(server)
        if master:
            self._write_master_inventory(hostname, ip_addr)
        else:
            self._write_node_inventory(hostname, ip_addr, worker_number)
        self._check_ssh(ip_addr)

    def deploy_cluster(self):
        if self.deployed:
            print("This cluster is already deployed.")
            return
        worker = threading.Thread(target=self._launch_worker, args=(1,),
                                  kwargs={'master': True})
        worker.start()
        self.launcher_treads.append(worker)
        for node in range(2, self.server_count + 1):
            worker_thread = threading.Thread(target=self._launch_worker,
                                             args=(node,))
            worker_thread.start()
            self.launcher_treads.append(worker_thread)
        for worker in self.launcher_treads:
            worker.join()
        self.deployed = True
        with open(self.inventory_file, 'w') as inv_file:
            write_inventory.write_inventory(inv_file, self.master, self.nodes,
                                            remote_user=self.remote_user)
        self.launcher_treads = []
        runner.run_playbook_subprocess(PREPARE_PLAYBOOK,
                                       inventory=self.inventory_file)
        cwd = os.getcwd()
        os.chdir(SPARK_DEPLOY_DIR)
        runner.run_playbook_subprocess(SETUP_SPARK_PLAYBOOK,
                                       inventory=self.inventory_file)
        os.chdir(cwd)
        runner.run_playbook_subprocess(START_SPARK_PLAYBOOK,
                                       extra_vars={'domain': ''},
                                       inventory=self.inventory_file)

    def _write_node_inventory(self, hostname, ip_addr, node_num):
        self.nodes.append({'hostname': hostname, 'ip_addr': ip_addr,
                           'node_num': node_num})

    def _write_master_inventory(self, hostname, ip_addr):
        self.master = {'hostname': hostname, 'ip_addr': ip_addr}
        self.master_ip = ip_addr

    def collapse_cluster(self):
        for server in self.servers:
            self.cloud.delete_server(server['id'])
